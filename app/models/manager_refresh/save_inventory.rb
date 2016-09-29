module ManagerRefresh
  class SaveInventory
    extend EmsRefresh::SaveInventoryHelper

    class << self
      def save_inventory(ems, hashes)
        _log.info("#{log_header(ems)} Saving EMS Inventory...Start")

        hashes.each do |key, dto_collection|
          save_collection(ems, key, dto_collection, hashes)
        end

        _log.info("#{log_header(ems)} Saving EMS Inventory...Complete")
        ems
      end

      def save_collection(parent, key, dto_collection, hashes)
        return if dto_collection.is_a? Array
        return if dto_collection.saved?

        if dto_collection.saveable?(hashes)
          save_dto_inventory_multi(parent.send(key),
                               dto_collection,
                               :use_association,
                               dto_collection.manager_ref,
                               key)
          store_ids_for_new_dto_records(parent.send(key), dto_collection, dto_collection.manager_ref)
          dto_collection.saved = true
        else
          dto_collection.dependencies.each do |dependency_key|
            save_collection(parent, dependency_key, hashes[dependency_key], hashes)
          end

          save_dto_inventory_multi(parent.send(key),
                               dto_collection,
                               :use_association,
                               dto_collection.manager_ref,
                               key)
          store_ids_for_new_dto_records(parent.send(key), dto_collection, dto_collection.manager_ref)
          dto_collection.saved = true
        end
      end

      private
      def log_header(ems)
        "EMS: [#{ems.name}], id: [#{ems.id}]"
      end
    end
  end
end
