require 'celluloid/current'

module ManagerRefresh::SaveCollection
  class ParallelInventorySaver
    include Vmdb::Logging
    include Celluloid
    include EmsRefresh::SaveInventoryHelper
    include ManagerRefresh::SaveCollection::Helper

    def save(inventory_collection, signal_lambda)
      _log.info("Saving to DB #{inventory_collection}")
      ActiveRecord::Base.connection_pool.with_connection do
        save_inventory_object_inventory(inventory_collection)
      end
      _log.info("Saved to DB #{inventory_collection}")
      signal_lambda.call(inventory_collection)
      _log.info("Saved to DB signalled #{inventory_collection}")
    end

    # def save_in_parallel(layer)
    #   layer.each do |inventory_collection|
    #     condition = Celluloid::Condition.new
    #
    #     signal_lambda = lambda do |param|
    #       condition.signal(param)
    #     end
    #
    #     async.save(inventory_collection, signal_lambda)
    #     _log.info("----Saving---- #{inventory_collection}")
    #     inventory_collection_returned = condition.wait
    #     _log.info("----Saved---- #{inventory_collection}")
    #     _log.info("----Saved and returned ---- #{inventory_collection_returned}")
    #   end
    # end

    def save_in_parallel(layer)
      conditions = []
      layer.each do |inventory_collection|
        conditions << condition = Celluloid::Condition.new

        signal_lambda = lambda do |param|
          condition.signal(param)
        end

        async.save(inventory_collection, signal_lambda)
        _log.info("----Saving---- #{inventory_collection}")
      end

      conditions.each do |condition|
        inventory_collection = condition.wait
        _log.info("----Saved---- #{inventory_collection}")
      end
    end
  end
end
