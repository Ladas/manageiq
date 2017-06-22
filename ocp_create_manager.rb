manager_name = ARGV[0] || "ocp_manager_small"
host_name    = ARGV[1] || "ocp-manager-small.redhat.com"

token = 'token'.freeze
ems   = ManageIQ::Providers::Openshift::ContainerManager.create(
  :name     => manager_name,
  :zone     => Zone.first,
  :hostname => host_name,
  :port     => 443
)
ems.update_authentication(:bearer => {:auth_key => token, :save => true})
