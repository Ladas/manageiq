class MiqRequestTaskDependencyMapping < ApplicationRecord
  belongs_to :miq_request_task
  belongs_to :miq_request_task_dependency, :class_name => "MiqRequestTask"
end
