module SkeletalRefresh
  class SaveInventory
    extend EmsRefresh::SaveInventoryHelper

    class << self
      def save_inventory(ems, hashes)
        hashes.each do |key, dto_collection|
          save_collection(ems, key, dto_collection, hashes)
        end
      end

      def save_collection(parent, key, dto_collection, hashes)
        return if dto_collection.is_a? Array
        return if dto_collection.saved?

        if dto_collection.saveable?(hashes)
          save_inventory_multi(parent.send(key),
                               dto_collection,
                               :use_association,
                               dto_collection.provider_uuid_attributes,
                               key)
          store_ids_for_new_records(parent.send(key), dto_collection, dto_collection.provider_uuid_attributes)
          dto_collection.saved = true
        else
          dto_collection.dependencies.each do |dependency_key|
            save_collection(parent, dependency_key, hashes[dependency_key], hashes)
          end

          save_inventory_multi(parent.send(key),
                               dto_collection,
                               :use_association,
                               dto_collection.provider_uuid_attributes,
                               key)
          store_ids_for_new_records(parent.send(key), dto_collection, dto_collection.provider_uuid_attributes)
          dto_collection.saved = true
        end
      end
    end
  end
end
