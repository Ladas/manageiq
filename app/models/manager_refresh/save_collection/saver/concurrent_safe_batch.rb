module ManagerRefresh::SaveCollection
  module Saver
    class ConcurrentSafeBatch < ManagerRefresh::SaveCollection::Saver::Base
      private

      def save!(inventory_collection, association)
        attributes_index        = {}
        inventory_objects_index = {}
        all_attribute_keys      = Set.new

        inventory_collection.each do |inventory_object|
          attributes = inventory_object.attributes(inventory_collection)
          index      = inventory_object.manager_uuid

          attributes_index[index]        = attributes
          inventory_objects_index[index] = inventory_object
          all_attribute_keys.merge(attributes.keys)
        end

        inventory_collection_size = inventory_collection.size
        deleted_counter           = 0
        created_counter           = 0
        updated_counter           = 0
        _log.info("*************** PROCESSING #{inventory_collection} of size #{inventory_collection_size} *************")
        # Records that are in the DB, we will be updating or deleting them.
        hashes_for_update = []
        records_for_destroy = []

        association.find_in_batches do |batch|
          batch.each do |record|
            next unless assert_distinct_relation(record)

            index = inventory_collection.object_index_with_keys(unique_index_keys, record)

            inventory_object = inventory_objects_index.delete(index)
            hash             = attributes_index.delete(index)

            if inventory_object.nil?
              # Record was found in the DB but not sent for saving, that means it doesn't exist anymore and we should
              # delete it from the DB.
              if inventory_collection.delete_allowed?
                records_for_destroy << record
                deleted_counter += 1
              end
            else
              # Record was found in the DB and sent for saving, we will be updating the DB.
              next unless assert_referential_integrity(hash, inventory_object)
              inventory_object.id = record.id

              record.assign_attributes(hash.except(:id, :type))
              if !inventory_collection.check_changed? || record.changed?
                hashes_for_update << record.attributes.symbolize_keys
              end
            end
          end

          # Persist in batches
          if hashes_for_update.size >= 1000
            update_records!(inventory_collection, all_attribute_keys, hashes_for_update)
            updated_counter += hashes_for_update.count

            hashes_for_update = []
          end

          # Destroy in batches
          if records_for_destroy.size >= 1000
            destroy_records(records)
            records_for_destroy = []
          end
        end

        # Persist the last batch
        update_records!(inventory_collection, all_attribute_keys, hashes_for_update)
        updated_counter += hashes_for_update.count

        # Destroy the last batch
        destroy_records(records_for_destroy)
        records_for_destroy = [] # Cleanup so GC can release it

        all_attribute_keys << :type if inventory_collection.supports_sti?
        # Records that were not found in the DB but sent for saving, we will be creating these in the DB.
        if inventory_collection.create_allowed?
          inventory_objects_index.each_slice(1000) do |batch|
            create_records!(inventory_collection, all_attribute_keys, batch, attributes_index)
            created_counter += batch.size
          end
        end
        _log.info("*************** PROCESSED #{inventory_collection}, created=#{created_counter}, "\
                  "updated=#{updated_counter}, deleted=#{deleted_counter} *************")
      end

      def destroy_records(records)
        ActiveRecord::Base.transaction do
          records.each do |record|
            delete_record!(inventory_collection, record)
          end
        end
      end

      def update_records!(inventory_collection, all_attribute_keys, hashes)
        return if hashes.blank?

        all_attribute_keys_array = all_attribute_keys.to_a
        table_name               = inventory_collection.model_class.table_name

        update_query = %{
          UPDATE #{table_name}
            SET
              #{all_attribute_keys_array.map {|key| "#{key} = the_values.#{key}"}.join(",")}
          FROM (
            VALUES
              #{hashes.map {|hash| "(#{all_attribute_keys_array.map {|x| quote(hash[x])}.join(", ")})"}.join(",")}
          ) AS the_values (#{all_attribute_keys_array.join(", ")})
          WHERE #{inventory_collection.unique_index_columns.map {|x| "the_values.#{x} = #{table_name}.#{x}"}.join(" AND ")}
        }

        # TODO(lsmola) do we want to exclude the ems_id from the UPDATE clause? Otherwise it might be difficult to change
        # the ems_id as a cross manager migration, since ems_id should be there as part of the insert. The attempt of
        # changing ems_id could lead to putting it back by a refresh.

        # This conditional will avoid rewriting new data by old data. But we want it only when remote_data_timestamp is a
        # part of the data, since for the fake records, we just want to update ems_ref.
        if all_attribute_keys.include?(:remote_data_timestamp) # include? on Set is O(1)
          update_query += %{
             AND (the_values.remote_data_timestamp IS NULL OR (the_values.remote_data_timestamp > #{table_name}.remote_data_timestamp))
          }
        end

        ActiveRecord::Base.connection.execute(update_query)
      end

      # def update_records1(inventory_collection, all_attribute_keys, batch, attributes_index)
      #   all_attribute_keys_array = all_attribute_keys.to_a
      #   indexed_inventory_objects = {}
      #   hashes = []
      #   batch.each do |index, inventory_object|
      #     hash = attributes_index.delete(index)
      #     next unless assert_referential_integrity(hash, inventory_object)
      #     hash[:type] = inventory_collection.model_class.name if inventory_collection.supports_sti? && hash[:type].blank?
      #     hashes << hash
      #     # Index on Unique Key values, so we can easily fill in the :id later
      #     indexed_inventory_objects[inventory_collection.unique_index_columns.map { |x| hash[x] }] = inventory_object
      #   end
      #
      #   return if hashes.blank?
      #
      #   # UPDATE mytable
      #   # SET
      #   # mytext = myvalues.mytext,
      #   #   myint = myvalues.myint
      #   # FROM (
      #   #        VALUES
      #   # (1, 'textA', 99),
      #   #   (2, 'textB', 88),
      #   #   ...
      #   # ) AS myvalues (mykey, mytext, myint)
      #   # WHERE mytable.mykey = myvalues.mykey
      #
      #   table_name   = inventory_collection.model_class.table_name
      #   update_query = %{
      #     UPDATE #{table_name}
      #       SET
      #   }
      #   update_query += all_attribute_keys_array.map do |key|
      #     %{(
      #       #{table_name}.#{key} = thevalues.#{key}
      #     )}
      #   end
      #   update_query += %{
      #     FROM (
      #       VALUES
      #   }
      #   update_query += hashes.map do |hash|
      #     %{(
      #       #{all_attribute_keys_array.map { |x| quote(hash[x]) }.join(", ")}
      #     )}
      #   end.join(",")
      #   update_query += %{
      #     ) AS the_values (#{all_attribute_keys_array.join(", ")})
      #   }
      #   update_query += %{
      #     WHERE #{inventory_collection.unique_index_columns.map { |x| "the_values.#{x}=#{table_name}.#{x}" }}
      #   }
      #
      #   # TODO(lsmola) do we want to exclude the ems_id from the UPDATE clause? Otherwise it might be difficult to change
      #   # the ems_id as a cross manager migration, since ems_id should be there as part of the insert. The attempt of
      #   # changing ems_id could lead to putting it back by a refresh.
      #
      #   # This conditional will avoid rewriting new data by old data. But we want it only when remote_data_timestamp is a
      #   # part of the data, since for the fake records, we just want to update ems_ref.
      #   if all_attribute_keys.include?(:remote_data_timestamp) # include? on Set is O(1)
      #     update_query += %{
      #       , the_values.remote_data_timestamp IS NULL OR (the_values.remote_data_timestamp > #{table_name}.remote_data_timestamp)
      #     }
      #   end
      #
      #   ActiveRecord::Base.connection.execute(update_query)
      # end

      # UPDATE mytable
      # SET
      # mytext = myvalues.mytext,
      #   myint = myvalues.myint
      # FROM (
      #        VALUES
      # (1, 'textA', 99),
      #   (2, 'textB', 88),
      #   ...
      # ) AS myvalues (mykey, mytext, myint)
      # WHERE mytable.mykey = myvalues.mykey

      def create_records!(inventory_collection, all_attribute_keys, batch, attributes_index)
        all_attribute_keys_array = all_attribute_keys.to_a
        indexed_inventory_objects = {}
        hashes = []
        batch.each do |index, inventory_object|
          hash = inventory_collection.model_class.new(attributes_index.delete(index)).attributes.symbolize_keys
          next unless assert_referential_integrity(hash, inventory_object)

          hashes << hash
          # Index on Unique Key values, so we can easily fill in the :id later
          indexed_inventory_objects[inventory_collection.unique_index_columns.map { |x| hash[x] }] = inventory_object
        end

        return if hashes.blank?

        table_name   = inventory_collection.model_class.table_name
        insert_query = %{
          INSERT INTO #{table_name} (#{all_attribute_keys_array.join(", ")})
            VALUES
        }
        insert_query += hashes.map do |hash|
          %{(
            #{all_attribute_keys_array.map { |x| quote(hash[x]) }.join(", ")}
          )}
        end.join(",")
        insert_query += %{
          ON CONFLICT (#{inventory_collection.unique_index_columns.join(", ")})
            DO
              UPDATE
                SET #{all_attribute_keys_array.map { |x| "#{x} = EXCLUDED.#{x}" }.join(", ")}
        }
        # TODO(lsmola) do we want to exclude the ems_id from the UPDATE clause? Otherwise it might be difficult to change
        # the ems_id as a cross manager migration, since ems_id should be there as part of the insert. The attempt of
        # changing ems_id could lead to putting it back by a refresh.

        # This conditional will avoid rewriting new data by old data. But we want it only when remote_data_timestamp is a
        # part of the data, since for the fake records, we just want to update ems_ref.
        if all_attribute_keys.include?(:remote_data_timestamp) # include? on Set is O(1)
          insert_query += %{
            WHERE EXCLUDED.remote_data_timestamp IS NULL OR (EXCLUDED.remote_data_timestamp > #{table_name}.remote_data_timestamp)
          }
        end

        ActiveRecord::Base.connection.execute(insert_query)
        # TODO(lsmola) we need to do the mapping only if this IC has dependents/dependees
        map_ids_to_inventory_objects(inventory_collection, indexed_inventory_objects, hashes)
      end

      def map_ids_to_inventory_objects(inventory_collection, indexed_inventory_objects, hashes)
        cond = hashes.map do |hash|
          "(#{inventory_collection.unique_index_columns.map { |x| quote(hash[x]) }.join(", ")})"
        end.join(",")
        select_query = "(#{inventory_collection.unique_index_columns.join(", ")}) IN (#{cond})"

        inventory_collection.model_class.where(select_query).find_each do |inserted_record|
          inventory_object = indexed_inventory_objects[inventory_collection.unique_index_columns.map { |x| inserted_record.public_send(x) }]
          inventory_object.id = inserted_record.id if inventory_object
        end
      end
    end
  end
end
