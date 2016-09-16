class CreateMiqRequestTaskDependencyMappings < ActiveRecord::Migration[5.0]
  def change
    create_table :miq_request_task_dependency_mappings do |t|
      t.belongs_to :miq_request_task, :type => :bigint, :index => {:name => 'miq_request_task_dependency_mapping_task_index'}
      t.belongs_to :miq_request_task_dependency, :type => :bigint, :index => {:name => 'miq_request_task_dependency_mapping_task_dependency_index'}
    end

    add_index :miq_request_task_dependency_mappings,
              [:miq_request_task_id, :miq_request_task_dependency_id],
              :name   => 'miq_request_task_dependency_mapping_index',
              :unique => true
  end
end
