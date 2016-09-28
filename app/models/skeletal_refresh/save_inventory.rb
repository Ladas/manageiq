module SkeletalRefresh
  class SaveInventory
    extend EmsRefresh::SaveInventoryHelper

    class << self
      def save_inventory(ems, hashes)
        hashes.each do |key, value|
          save_collection(ems, key, value)
        end
      end

      def save_collection(parent, collection, data)
        save_inventory_multi(parent.send(collection),
                             data,
                             :use_association,
                             [:ems_ref],
                             collection)
        store_ids_for_new_records(parent.send(collection), data, :ems_ref)
      end
    end
  end
end
