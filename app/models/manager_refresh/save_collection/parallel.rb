module ManagerRefresh::SaveCollection
  class Parallel
    extend EmsRefresh::SaveInventoryHelper
    extend ManagerRefresh::SaveCollection::Helper

    class << self
      def save_collections(ems, dto_collections)
        verify_dto_collections(dto_collections)
        dto_collections    = dto_collections.values
        edges, fixed_edges = build_edges(dto_collections)
        # byebug
        assert_fixed_graph(fixed_edges)
        acyclic_edges, feedback_edges = build_feedback_and_acyclic_edge_set(edges, fixed_edges)
        # byebug
        build_directed_acyclic_graph(dto_collections, feedback_edges)

        layers          = topological_sort(dto_collections, acyclic_edges)

        sorted_graph_log = "Topological sorting of manager #{""} with ---nodes---:\n#{dto_collections.join("\n")}\n"
        sorted_graph_log += "---edges---:\n#{edges.map { |x| "<#{x.first}, #{x.last}>"}.join("\n")}\n"
        sorted_graph_log += "---resulted in these layers processable in parallel:"

        layers.each_with_index do |layer, index|
          sorted_graph_log += "\n----- Layer #{index} -----: \n#{layer.join("\n")}"
        end

        _log.info(sorted_graph_log)

        # saver_pool = ManagerRefresh::SaveCollection::ParallelInventorySaver.pool(size: 10)

        layers.each_with_index do |layer, index|
          _log.info("Saving manager #{""} Layer #{index}")
          # saver_pool.save_in_parallel(layer)
          layer.each do |dto_collection|
            # todo refactor the saving method out
            save_dto_inventory(ems, dto_collection) unless dto_collection.saved?
          end
          _log.info("Saved manager #{""} Layer #{index}")
        end

        _log.info("All layers of manager #{""} saved!")
      end

      private

      def verify_dto_collections(dto_collections)
        dto_collections.each do |_key, dto_collection|
          unless dto_collection.is_a? ::ManagerRefresh::DtoCollection
            raise "A ManagerRefresh::SaveInventory needs a DtoCollection object, it got: #{dto_collection.inspect}"
          end
        end
      end

      def assert_fixed_graph(fixed_edges)
        fixed_edges.each do |edge|
          raise "Cycle in fixed graph detected" if detect_cycle(edge, fixed_edges - [edge])
        end
      end

      def build_feedback_and_acyclic_edge_set(edges, fixed_edges)
        edges = edges.dup
        acyclic_edges = fixed_edges.dup
        feedback_edge_set = []

        while edges.present?
          edge = edges.pop
          if detect_cycle(edge, acyclic_edges)
            feedback_edge_set << edge
          else
            acyclic_edges << edge
          end
        end

        return acyclic_edges, feedback_edge_set
      end

      def build_directed_acyclic_graph(vertices, feedback_edge_set)
        vertices.each do |dto_collection|
          feedback_dependencies = feedback_edge_set.select { |e| e.second == dto_collection }.map(&:first)
          attrs = dto_collection.dependency_attributes_for(feedback_dependencies)

          # Todo first dup the dto_collection, then blacklist it in original and whitelist it in the second one
          unless attrs.blank?
            dto_collection.blacklist_attributes!(attrs)
          end
        end
      end

      def detect_cycle(edge, acyclic_edges)
        # Test if adding edge creates a cycle, ew will traverse the graph from edge Vertice, through all it's
        # dependencies
        starting_vertice = edge.second
        edges            = [edge] + acyclic_edges
        traverse_dependecies([starting_vertice], starting_vertice, edges, vertice_edges(edges, starting_vertice))
      end

      def traverse_dependecies(traversed_vertices, current_vertice, edges, dependencies)
        traversed_vertices << current_vertice

        dependencies.each do |vertice_edge|
          vertice = vertice_edge.first
          if traversed_vertices.include?(vertice)
            # raise "Cycle from #{current_vertice} to #{vertice}"
            return true
          end
          return true if traverse_dependecies(traversed_vertices, vertice, edges, vertice_edges(edges, vertice))
        end

        false
      end

      def vertice_edges(edges, vertice)
        edges.select { |e| e.second == vertice}
      end

      def build_edges(dto_collections)
        edges       = []
        fixed_edges = []
        dto_collections.each do |dto_collection|
          dto_collection.dependencies.each do |dependency|
            fixed_edges << [dependency, dto_collection] if dto_collection.fixed_dependencies.include?(dependency)
            edges       << [dependency, dto_collection]
          end
        end
        return edges, fixed_edges
      end

      def topological_sort(vertices, edges)
        # Topological sort of the graph of the DTO collections to find the right order of saving DTO collections and
        # identify what DTO collections can be saved in parallel.

        # The expected input here is the directed acyclic Graph G (dto_collections), consisting of Vertices(Nodes) V and
        # Edges E:
        # G = (V, E)
        #
        # The directed edge is defined as (u, v), where u is the dependency of v, i.e. arrow comes from u to v:
        # (u, v) ∈ E and  u,v ∈ V
        #
        # S0 is a layer that has no dependencies:
        # S0 = { v ∈ V ∣ ∀u ∈ V.(u,v) ∉ E}
        #
        # Si+1 is a layer whose dependencies are in the sum of the previous layers from i to 0, cannot write
        # mathematical sum using U in text editor, so there is an alternative format using _(sum)
        # Si+1 = { v ∈ V ∣ ∀u ∈ V.(u,v) ∈ E → u ∈ _(sum(S0..Si))_ }
        #
        # Then each Si can have their Vertices(DTO collections) processed in parallel. This algorithm cannot
        # identify independent sub-graphs inside of the layers Si, so we can make the processing even more effective.
        #

        nodes          = vertices.dup
        sets           = []
        i              = 0
        sets[0], nodes = nodes.partition { |v| !edges.detect { |e| e.second == v } }

        max_depth = 1000
        while nodes.present?
          i         += 1
          max_depth -= 1
          if max_depth <= 0
            raise "Max depth reached while doing topological sort of nodes #{vertices} and edges #{edges}, your "\
                  "graph probably contains a cycle"
          end

          set, nodes = nodes.partition { |v| edges.select { |e| e.second == v }.all? { |e| sets.flatten.include?(e.first) } }
          if set.blank?
            raise "Blank dependency set while doing topological sort of nodes #{vertices} and edges #{edges}, your"\
                  " graph probably contains a cycle"
          end

          sets[i] = set
        end

        sets
      end
    end
  end
end
