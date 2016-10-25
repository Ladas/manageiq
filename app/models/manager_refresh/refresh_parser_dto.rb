module ManagerRefresh
  class RefreshParserDto
    def process_dto_collection(collection, key)
      collection.each do |item|
        uid, new_result = yield(item)
        next if uid.nil?

        dto = @data[key].new_dto(new_result)
        @data[key] << dto
      end
    end

    def node(name, options = {})
      parser_node = create_parser_node(name, options[:strategy])
      # comment 2: allow any node values as attribute:
      options.each do |n, v|
        next if n == :strategy
        parser_node.send(n, v)
      end
      # comment 1:
      parser_node.parse "get_#{name}"
      parser_node.klass name.camelcase
      yield parser_node if block_given?

      # comment 3: (this is probably overkill)
      register_dto parser_node.dto
    end

    def add_dto_collection(model_class, association, manager_ref = nil)
      @data[association] = ::ManagerRefresh::DtoCollection.new(model_class,
                                                               :parent      => @ems,
                                                               :association => association,
                                                               :manager_ref => manager_ref)
    end

    def add_cloud_manager_db_cached_dto(model_class, association, manager_ref = nil)
      @data[association] = ::ManagerRefresh::DtoCollection.new(model_class,
                                                               :parent      => @ems.parent_manager,
                                                               :association => association,
                                                               :manager_ref => manager_ref,
                                                               :strategy    => :local_db_cache_all)
    end
  end
end
