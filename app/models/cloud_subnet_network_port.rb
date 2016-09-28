class CloudSubnetNetworkPort < ApplicationRecord
  class DtoCollection < ::DtoCollection
    class Dto < ::DtoCollection::Dto
      attr_accessor :address, :cloud_subnet, :network_port
    end
  end

  self.table_name = "cloud_subnets_network_ports"

  belongs_to :cloud_subnet
  belongs_to :network_port
end
