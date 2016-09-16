class ServiceResourceDependencyMapping < ApplicationRecord
  belongs_to :service_resource
  belongs_to :service_resource_dependency, :class_name => "ServiceResource"
end
