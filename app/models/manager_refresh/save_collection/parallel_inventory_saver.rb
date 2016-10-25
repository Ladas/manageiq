require 'celluloid/current'

module ManagerRefresh::SaveCollection
  class ParallelInventorySaver
    include Vmdb::Logging
    include Celluloid
    include EmsRefresh::SaveInventoryHelper
    include ManagerRefresh::SaveCollection::Helper

    def save(dto_collection, signal_lambda)
      _log.info("Saving to DB #{dto_collection}")
      ActiveRecord::Base.connection_pool.with_connection do
        save_dto_inventory(dto_collection)
      end
      _log.info("Saved to DB #{dto_collection}")
      signal_lambda.call(dto_collection)
      _log.info("Saved to DB signalled #{dto_collection}")
    end

    # def save_in_parallel(layer)
    #   layer.each do |dto_collection|
    #     condition = Celluloid::Condition.new
    #
    #     signal_lambda = lambda do |param|
    #       condition.signal(param)
    #     end
    #
    #     async.save(dto_collection, signal_lambda)
    #     _log.info("----Saving---- #{dto_collection}")
    #     dto_collection_returned = condition.wait
    #     _log.info("----Saved---- #{dto_collection}")
    #     _log.info("----Saved and returned ---- #{dto_collection_returned}")
    #   end
    # end

    def save_in_parallel(layer)
      conditions = []
      layer.each do |dto_collection|
        conditions << condition = Celluloid::Condition.new

        signal_lambda = lambda do |param|
          condition.signal(param)
        end

        async.save(dto_collection, signal_lambda)
        _log.info("----Saving---- #{dto_collection}")
      end

      conditions.each do |condition|
        dto_collection = condition.wait
        _log.info("----Saved---- #{dto_collection}")
      end
    end
  end
end

