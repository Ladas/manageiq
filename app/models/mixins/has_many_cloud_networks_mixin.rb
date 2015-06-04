module HasManyCloudNetworksMixin
  extend ActiveSupport::Concern

  included do
    has_many :floating_ips,    :foreign_key => :ems_id, :dependent => :destroy
    has_many :security_groups, :foreign_key => :ems_id, :dependent => :destroy
    has_many :cloud_networks,  :foreign_key => :ems_id, :dependent => :destroy
  end
end
