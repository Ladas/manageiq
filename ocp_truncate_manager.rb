manager_name = ARGV[0] || "ocp_manager_small"

ActiveRecord::Base.logger = Logger.new(STDOUT)
ems = ExtManagementSystem.find_by(
  :name => manager_name,
)

ems.container_images.delete_all
ems.container_image_registries.delete_all
