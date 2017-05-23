module EmsRefresh::SaveInventoryHelperBackup
  def collect_inventory_object_inventory_with_findkey(inventory_object, new_records, updated_records, record_index)
    # Find the record and add if to bucket for creation or update
    uuid = inventory_object.manager_uuid
    # found = record_index.fetch(inventory_object.data)
    found = record_index[uuid]
    if found.nil?
      new_records << inventory_object.data
    else
      updated_records[found.id] = inventory_object.data
      record_index.delete(uuid)
    end
  end

  def save_inventory_object_inventory_multi_batch_doh(association, inventory_collection, deletes, find_key, child_keys = [], extra_keys = [], disconnect = false)
    association.reset
    # Fetch all dependencies in the data
    inventory_collection.data.map(&:attributes)
    # inventory_collection.each(&:attributes)

    # record_index = TypedIndex.new(association, find_key)
    record_index = {}
    # TODO(lsmola) would be nice to select only few cols, but we would need to convert association to col name
    # selected = [:id] + inventory_collection.manager_ref
    # selected << :type if inventory_collection.model_class.new.respond_to? :type
    # association.select(selected).find_each do |record|
    # association.find_each do |record|
    #   record_index[inventory_collection.object_index(record)] = record
    # end

    deletes         = []
    new_records     = inventory_collection.to_hash.clone
    updated_records = {}
    _log.info("PROCESSING #{inventory_collection}")

    association.find_each do |record|
      uuid = inventory_collection.object_index(record)
      found = new_records.delete(uuid)
      if found
        updated_records[record.id] = found.data
      else
        deletes << record
      end
    end

    # new_records     = []
    # updated_records = {}
    # _log.info("PROCESSING #{inventory_collection}")
    # inventory_collection.each do |inventory_object|
    #   # byebug if inventory_collection.model_class == FirewallRule && !record_index.blank?
    #   collect_inventory_object_inventory_with_findkey(inventory_object, new_records, updated_records, record_index)
    # end
    _log.info("PROCESSED #{inventory_collection}")

    # Delete the items no longer found
    unless deletes.blank?
      type = association.proxy_association.reflection.name
      _log.info("[#{type}] Deleting #{log_format_deletes(deletes)}")
      disconnect ? deletes.each(&:disconnect_inv) : delete_inventory_multi(inventory_collection, association, deletes)
    end

    unless updated_records.blank?
      _log.info("UPDATING BATCH size #{updated_records.size} of #{inventory_collection}")
      inventory_collection.model_class.transaction do
        inventory_collection.model_class.update(updated_records.keys, updated_records.values)
      end
      _log.info("UPDATED BATCH #{inventory_collection}")
    end

    _log.info("ACTUAL CREATING BATCH size #{inventory_collection.size} #{inventory_collection}")
    # Add the new items
    association_meta_info = inventory_collection.parent.class.reflect_on_association(inventory_collection.association)
    if association_meta_info.options[:through].blank?
      inventory_collection.model_class.transaction do
        association.push(new_records.values.map { |x| association.build(x.data.except(:id)) })
      end
    else
      inventory_collection.model_class.transaction do
        inventory_collection.model_class.create(new_records.values.map(&:data))
      end
    end
  end
end
