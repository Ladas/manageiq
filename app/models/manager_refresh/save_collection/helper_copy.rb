# module ManagerRefresh::SaveCollection
#   module Helper
#     def save_inventory_object_inventory(ems, inventory_collection)
#       _log.info("Synchronizing #{ems.name} collection #{inventory_collection} of size #{inventory_collection.size} to"\
#                 " the database")
#
#       if inventory_collection.custom_save_block.present?
#         _log.info("Synchronizing #{ems.name} collection #{inventory_collection} using a custom save block")
#         inventory_collection.custom_save_block.call(ems, inventory_collection)
#       else
#         save_inventory(inventory_collection)
#       end
#       _log.info("Synchronized #{ems.name} collection #{inventory_collection}")
#       inventory_collection.saved = true
#     end
#
#     private
#
#     def save_inventory(inventory_collection)
#       # If we have not data to save and delete is not allowed, we can just skip
#       return if inventory_collection.data.blank? && !inventory_collection.delete_allowed?
#
#       inventory_collection.parent.reload if inventory_collection.parent
#       association = inventory_collection.db_collection_for_comparison
#
#       save_inventory_collection!(inventory_collection, association)
#     end
#
#     def save_inventory_collection!(inventory_collection, association)
#       attributes_index        = {}
#       inventory_objects_index = {}
#       inventory_collection.each do |inventory_object|
#         attributes = inventory_object.attributes(inventory_collection)
#         index      = inventory_object.manager_uuid
#
#         attributes_index[index]        = attributes
#         inventory_objects_index[index] = inventory_object
#       end
#
#       unique_index_keys      = inventory_collection.manager_ref_to_cols
#       unique_db_indexes      = Set.new
#       unique_db_primary_keys = Set.new
#
#       inventory_collection_size = inventory_collection.size
#       deleted_counter           = 0
#       created_counter           = 0
#       _log.info("*************** PROCESSING #{inventory_collection} of size #{inventory_collection_size} *************")
#       # Records that are in the DB, we will be updating or deleting them.
#       # ActiveRecord::Base.transaction do
#       # puts "#{association.count} === #{inventory_collection.data.count} #{inventory_collection}"
#       association.find_each do |record|
#         index = inventory_collection.object_index_with_keys(unique_index_keys, record)
#         if unique_db_primary_keys.include?(record.id) # Include on Set is O(1)
#           # Change the InventoryCollection's :association or :arel parameter to return distinct results. The :through
#           # relations can return the same record multiple times. We don't want to do SELECT DISTINCT by default, since
#           # it can be very slow.
#           if Rails.env.production?
#             _log.warn("Please update :association or :arel for #{inventory_collection} to return a DISTINCT result. "\
#                         " The duplicate value is being ignored.")
#             next
#           else
#             raise("Please update :association or :arel for #{inventory_collection} to return a DISTINCT result. ")
#           end
#         elsif unique_db_indexes.include?(index) # Include on Set is O(1)
#           # We have a duplicate in the DB, destroy it. A find_each method does automatically .order(:id => :asc)
#           # so we always keep the oldest record in the case of duplicates.
#           _log.warn("A duplicate record was detected and destroyed, inventory_collection: '#{inventory_collection}', "\
#                       "record: '#{record}', duplicate_index: '#{index}'")
#           record.destroy
#         else
#           unique_db_indexes << index
#           unique_db_primary_keys << record.id
#         end
#
#         inventory_object = inventory_objects_index.delete(index)
#         hash             = attributes_index.delete(index)
#
#         if inventory_object.nil?
#           # Record was found in the DB but not sent for saving, that means it doesn't exist anymore and we should
#           # delete it from the DB.
#           deleted_counter += 1 if delete_record!(inventory_collection, record)
#         else
#           # Record was found in the DB and sent for saving, we will be updating the DB.
#           update_record!(inventory_collection, record, hash, inventory_object)
#         end
#       end
#       # end
#
#       # Records that were not found in the DB but sent for saving, we will be creating these in the DB.
#       if inventory_collection.create_allowed?
#         # ActiveRecord::Base.transaction do
#           inventory_objects_index.each do |index, inventory_object|
#             hash = attributes_index.delete(index)
#             create_record!(inventory_collection, hash, inventory_object)
#             created_counter += 1
#           end
#         # end
#       end
#       _log.info("*************** PROCESSED #{inventory_collection}, created=#{created_counter}, "\
#                 "updated=#{inventory_collection_size - created_counter}, deleted=#{deleted_counter} *************")
#     end
#
#     def delete_record!(inventory_collection, record)
#       return false unless inventory_collection.delete_allowed?
#       record.public_send(inventory_collection.delete_method)
#       true
#     end
#
#     def update_record!(inventory_collection, record, hash, inventory_object)
#       if inventory_collection.strategy == :stream_data
#         sql_update(inventory_object, hash, record)
#       else
#         record.assign_attributes(hash.except(:id, :type))
#         record.save if !inventory_collection.check_changed? || record.changed?
#
#         inventory_object.id = record.id
#       end
#     end
#
#     def create_record!(inventory_collection, hash, inventory_object)
#       if inventory_collection.strategy == :stream_data
#         hash[:type] = inventory_collection.model_class.name if inventory_collection.supports_sti? && hash[:type].blank?
#         sql_insert(inventory_object, hash)
#       else
#         record = inventory_collection.model_class.create!(hash.except(:id))
#
#         inventory_object.id = record.id
#       end
#     end
#
#     def sql_insert(inventory_object, hash)
#       table_name = inventory_object.inventory_collection.model_class.table_name
#       insert_query = %{
#         INSERT INTO #{table_name} (#{hash.keys.join(", ")})
#           VALUES
#             (
#               #{hash.values.map { |x| ActiveRecord::Base.connection.quote(x)}.join(", ") }
#             )
#           ON CONFLICT (#{inventory_object.inventory_collection.unique_index_columns.join(", ")})
#             DO
#               UPDATE
#                 SET #{hash.keys.map { |x| "#{x} = EXCLUDED.#{x}" }.join(", ")}
#         }
#       # TODO(lsmola) do we want to exclude the ems_id from the UPDATE clause? Otherwise it might be difficult to change
#       # the ems_id as a cross manager migration, since ems_id should be there as part of the insert. The attempt of
#       # changing ems_id could lead to putting it back by a refresh.
#
#       # This conditional will avoid rewriting new data by old data. But we want it only when remote_data_timestamp is a
#       # part of the data, since for the fake records, we just want to update ems_ref.
#       if hash[:remote_data_timestamp].present?
#         insert_query += %{
#           WHERE EXCLUDED.remote_data_timestamp IS NULL OR (EXCLUDED.remote_data_timestamp > #{table_name}.remote_data_timestamp)
#         }
#       end
#       result_id = ActiveRecord::Base.connection.insert_sql(insert_query)
#       inventory_object.id = result_id
#     end
#
#     def sql_update(inventory_object, hash, record)
#       record.assign_attributes(hash.except(:id, :type))
#
#       # TODO(lsmola) ignore all N:M relations, since we use pure SQL, all N:M needs to be modeled as a separate IC, or
#       # can we process those automatically? Using a convention? But still, it needs to be a separate IC, to have
#       # efficient saving.
#       hash.reject! { |key, value| value.kind_of?(Array) }
#
#       if !inventory_object.inventory_collection.check_changed? || record.changed?
#         update_query = inventory_object.inventory_collection.model_class.where(:id => record.id)
#         if hash[:remote_data_timestamp]
#           timestamp_field = inventory_object.inventory_collection.model_class.arel_table[:remote_data_timestamp]
#           update_query = update_query.where(timestamp_field.lt(hash[:remote_data_timestamp]))
#         end
#
#         update_query.update_all(hash)
#       end
#
#       inventory_object.id = record.id
#     end
#   end
# end
