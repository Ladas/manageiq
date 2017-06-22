require 'manageiq_performance'

def persister_class
  # ManageIQ::Providers::Kubernetes::Inventory::Persister::ContainerManager
  ManageIQ::Providers::Kubernetes::Inventory::Persister::ContainerManagerStream
end

def generate_batches_od_data(ems_name:, total_elements:, batch_size: 1000)
  ems       = ExtManagementSystem.find_by(:name => ems_name)
  persister = persister_class.new(
    ems
  )
  count     = 1

  persister, count = process_entity(ems, :container_image_registries, persister, count, total_elements, batch_size)

  # Send or update the rest which is batch smaller than the batch size
  # send_or_update(ems, :key_pair, persister, :rest, batch_size)
  send_or_update(ems, :container_image_registries, persister, :rest, batch_size)
end

def process_entity(ems, entity_name, starting_persister, starting_count, total_elements, batch_size)
  persister = starting_persister
  count     = starting_count

  (1..total_elements).each do |index|
    send("parse_#{entity_name.to_s}", index, persister)
    persister, count = send_or_update(ems, entity_name, persister, count, batch_size)
  end

  return persister, count
end

def send_or_update(ems, entity_name, persister, count, batch_size)
  if count == :rest || count >= batch_size
    ############################ Replace by sending to kafka and use the saving code on the other side START #########
    # persister = ManagerRefresh::Inventory::Persister.from_yaml(persister.to_yaml)

    _, timings = Benchmark.realtime_block(:ems_refresh) do
      ManagerRefresh::SaveInventory.save_inventory(
        persister.manager,
        persister.inventory_collections
      )
    end

    $log.info "#{ems.id} LADAS_BENCH #{timings.inspect}"
    ############################ Replace by sending to kafka and use the saving code on the other side END ###########

    # And and create new persistor so the old one with data can be GCed
    return_persister = persister_class.new(
      ems
    )
    return_count     = 1
  else
    return_persister = persister

    addition = case entity_name
               when :vm
                 2
               else
                 1
               end

    return_count = count + addition
  end

  return return_persister, return_count
end

def parse_container_image_registries(index, persister)
  image_registry = persister.container_image_registries.build(
    :name => "name_#{index}",
    :host => "host_#{index}_name",
    :port => "443",
  )

  parse_container_images(index, persister, image_registry)
end

def parse_container_images(index, persister, image_registry)
  persister.container_images.build(
    # :container_image_registry => persister.container_image_registries.lazy_find("host_#{index}_name__443"),
    :container_image_registry => image_registry,
    :name                     => "name_#{index}",
    :image_ref                => "container_image_ref_#{index}"
  )
end

manager_name   = ARGV[0] || "ocp_manager_small"
total_elements = ARGV[1].try(:to_i) || 10
batch_size     = ARGV[2].try(:to_i) || 2

ActiveRecord::Base.logger = Logger.new(STDOUT)

# ManageIQPerformance.profile do
_, timings = Benchmark.realtime_block(:ems_total_refresh) do
  generate_batches_od_data(
    :ems_name       => "ocp_manager_small",
    :total_elements => total_elements,
    :batch_size     => batch_size
  )
end
$log.info "aws_ems LADAS_TOTAL_BENCH 1st refresh #{timings.inspect}"
# end

puts "finished"
# while (1) do
#   sleep(1000)
# end
