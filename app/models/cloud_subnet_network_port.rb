class CloudSubnetNetworkPort < ApplicationRecord
  class DtoCollection < ::DtoCollection

    def dependencies
      [:cloud_subnets, :network_ports]
    end

    def provider_uuid_attributes
      [:address, :cloud_subnet, :network_port]
    end

    class Dto < ::DtoCollection::Dto
      attr_accessor :address, :cloud_subnet, :network_port

      def provider_uuid_attributes
        [:address, :cloud_subnet, :network_port]
      end
    end
  end

  self.table_name = "cloud_subnets_network_ports"

  belongs_to :cloud_subnet
  belongs_to :network_port
end
