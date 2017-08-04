module ManagerRefresh::SaveCollection
  module Saver
    module SqlHelper
      def unique_index_columns
        inventory_collection.unique_index_columns
      end

      def on_conflict_update
        true
      end

      # TODO(lsmola) all below methods should be rewritten to arel, but we need to first extend arel to be able to do
      # this
      def build_insert_set_cols(key)
        "#{quote_column_name(key)} = EXCLUDED.#{quote_column_name(key)}"
      end

      def build_insert_query(all_attribute_keys, hashes)
        all_attribute_keys_array = all_attribute_keys.to_a
        table_name               = inventory_collection.model_class.table_name
        values                   = hashes.map do |hash|
          "(#{all_attribute_keys_array.map { |x| quote(hash[x], x) }.join(",")})"
        end.join(",")
        col_names = all_attribute_keys_array.map { |x| quote_column_name(x) }.join(",")

        insert_query = %{
          INSERT INTO #{table_name} (#{col_names})
            VALUES
              #{values}
        }

        if on_conflict_update
          insert_query += %{
            ON CONFLICT (#{unique_index_columns.map { |x| quote_column_name(x) }.join(",")})
              DO
                UPDATE
                  SET #{all_attribute_keys_array.map { |key| build_insert_set_cols(key) }.join(", ")}
          }
        end

        # TODO(lsmola) do we want to exclude the ems_id from the UPDATE clause? Otherwise it might be difficult to change
        # the ems_id as a cross manager migration, since ems_id should be there as part of the insert. The attempt of
        # changing ems_id could lead to putting it back by a refresh.
        # TODO(lsmola) should we add :deleted => false to the update clause? That should handle a reconnect, without a
        # a need to list :deleted anywhere in the parser. We just need to check that a model has the :deleted attribute

        # This conditional will avoid rewriting new data by old data. But we want it only when remote_data_timestamp is a
        # part of the data, since for the fake records, we just want to update ems_ref.
        if supports_remote_data_timestamp?(all_attribute_keys)
          insert_query += %{
            WHERE EXCLUDED.remote_data_timestamp IS NULL OR (EXCLUDED.remote_data_timestamp > #{table_name}.remote_data_timestamp)
          }
        end

        insert_query += %{
          RETURNING "id",#{unique_index_columns.map { |x| quote_column_name(x) }.join(",")}
        }

        insert_query
      end

      def build_update_set_cols(key)
        "#{quote_column_name(key)} = updated_values.#{quote_column_name(key)}"
      end

      def quote_column_name(key)
        ActiveRecord::Base.connection.quote_column_name(key)
      end

      def build_update_query(all_attribute_keys, hashes)
        # We want to ignore type and create timestamps when updating
        all_attribute_keys_array = all_attribute_keys.to_a.delete_if { |x| %i(type created_at created_on).include?(x) }
        all_attribute_keys_array << :id
        table_name               = inventory_collection.model_class.table_name

        values = hashes.map! do |hash|
          "(#{all_attribute_keys_array.map { |x| quote(hash[x], x, inventory_collection) }.join(",")})"
        end.join(",")
        # Wuuuuuuuut this takes like 3.5 minutes instead of the 9s the maps and passing array multiple times using
        # values = ""
        # hashes.each do |hash|
        #   values << "("
        #   all_attribute_keys_array.each do |key|
        #     values << quote(hash[key], key, inventory_collection)
        #     values << ","
        #   end
        #   values[-1] = ""
        #   values << ")"
        #   values << ","
        # end
        # values[-1] = ""

        update_query = %{
          UPDATE #{table_name}
            SET
              #{all_attribute_keys_array.map { |key| build_update_set_cols(key) }.join(",")}
          FROM (
            VALUES
              #{values}
          ) AS updated_values (#{all_attribute_keys_array.map { |x| quote_column_name(x) }.join(",")})
          WHERE updated_values.id = #{table_name}.id
        }

        # TODO(lsmola) do we want to exclude the ems_id from the UPDATE clause? Otherwise it might be difficult to change
        # the ems_id as a cross manager migration, since ems_id should be there as part of the insert. The attempt of
        # changing ems_id could lead to putting it back by a refresh.

        # This conditional will avoid rewriting new data by old data. But we want it only when remote_data_timestamp is a
        # part of the data, since for the fake records, we just want to update ems_ref.
        if supports_remote_data_timestamp?(all_attribute_keys)
          update_query += %{
            AND (updated_values.remote_data_timestamp IS NULL OR (updated_values.remote_data_timestamp > #{table_name}.remote_data_timestamp))
          }
        end
        update_query
      end

      def build_multi_selection_query(hashes)
        inventory_collection.build_multi_selection_condition(hashes, unique_index_columns)
      end

      def quote(value, name = nil, used_inventory_collection = nil)
        # TODO(lsmola) needed only because UPDATE FROM VALUES needs a specific PG typecasting, remove when fixed in PG
        if used_inventory_collection.nil?
          ActiveRecord::Base.connection.quote(value)
        else
          quote_and_pg_type_cast(value, name)
        end
      rescue TypeError => e
        _log.error("Can't quote value: #{value}, of :#{name} and #{inventory_collection}")
        raise e
      end

      def quote_and_pg_type_cast(value, name)
        pg_type_cast(
          ActiveRecord::Base.connection.quote(value),
          pg_type(name)
        )
      end

      def pg_type_cast(value, sql_type)
        if sql_type.nil?
          value
        else
          "#{value}::#{sql_type}"
        end
      end

      def pg_type(name)
        @pg_types_cache[name]
      end

      def collect_pg_types!(all_attribute_keys)
        @pg_types_cache = {}
        all_attribute_keys.each do |key|
          @pg_types_cache[key] = inventory_collection.model_class.columns_hash[key.to_s]
                                   .try(:sql_type_metadata)
                                   .try(:instance_values)
                                   .try(:[], "sql_type")
        end
      end
    end
  end
end
