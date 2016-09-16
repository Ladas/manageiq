class CreateServiceResourceDependencyMappings < ActiveRecord::Migration[5.0]
  def change
    create_table :service_resource_dependency_mappings do |t|
      t.belongs_to :service_resource, :type => :bigint, :index => {:name => 'service_resource_dependency_mappings_resource_index'}
      t.belongs_to :service_resource_dependency, :type => :bigint, :index => {:name => 'service_resource_dependency_mappings_resource_dep_task_index'}
    end

    add_index :service_resource_dependency_mappings,
              [:service_resource_id, :service_resource_dependency_id],
              :name   => 'service_resource_dependency_mappings_index',
              :unique => true
  end
end
