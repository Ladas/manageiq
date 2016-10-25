module ManagerRefresh::SaveCollection
  class Parallel
    extend EmsRefresh::SaveInventoryHelper
    extend ManagerRefresh::SaveCollection::Helper

    class << self
      def save_collections(dto_collections)
        verify_dto_collections(dto_collections)
        dto_collections = dto_collections.values
        edges           = build_edges(dto_collections)
        layers          = topological_sort(dto_collections, edges)

        sorted_graph_log = "Topological sorting of manager #{""} with ---nodes---:\n#{dto_collections.join("\n")}\n"
        sorted_graph_log += "---edges---:\n#{edges.map { |x| "<#{x.first}, #{x.last}>"}.join("\n")}\n"
        sorted_graph_log += "---resulted in these layers processable in parallel:"

        layers.each_with_index do |layer, index|
          sorted_graph_log += "\n----- Layer #{index} -----: \n#{layer.join("\n")}"
        end

        _log.info(sorted_graph_log)

        saver_pool = ManagerRefresh::SaveCollection::ParallelInventorySaver.pool(size: 10)

        layers.each_with_index do |layer, index|
          _log.info("Saving manager #{""} Layer #{index}")
          saver_pool.save_in_parallel(layer)
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

      def build_edges(dto_collections)
        edges = []
        dto_collections.each do |dto_collection|
          dto_collection.dependencies.each do |dependency|
            edges << [dependency, dto_collection]
          end
        end
        edges
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
