class AddUniqueIndexAndTimestampToVms < ActiveRecord::Migration[5.0]
  def change
    # add_column :vms, :remote_data_timestamp, :datetime
    #
    # add_index :vms, [:ems_id, :ems_ref], :unique => true
    #
    #
    # add_column :hardwares, :remote_data_timestamp, :datetime
    # # TODO delete the old index if going this way, will need down migration?
    # remove_index :hardwares, :vm_or_template_id
    # add_index :hardwares, [:vm_or_template_id], :unique => true, :name => "index_hardwares_on_vm_or_template_id"
    #
    # add_column :availability_zones, :remote_data_timestamp, :datetime
    #
    # add_index :availability_zones, [:ems_id, :ems_ref], :unique => true
    #
    # add_column :flavors, :remote_data_timestamp, :datetime
    #
    # add_index :flavors, [:ems_id, :ems_ref], :unique => true
    #
    # add_column :orchestration_stacks, :remote_data_timestamp, :datetime
    #
    # add_index :orchestration_stacks, [:ems_id, :ems_ref], :unique => true
    #
    # add_column :authentications, :remote_data_timestamp, :datetime
    # # TODO ems_ref?
    # add_index :authentications, [:resource_type, :resource_id, :name, :authtype, :userid], :unique => true,
    #           :name => "index_authentications_with_unique_index"
  end
end
