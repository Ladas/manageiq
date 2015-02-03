require "spec_helper"

describe Metric::CiMixin::Capture::Openstack do
  require File.expand_path(File.join(File.dirname(__FILE__),
                                     %w(.. .. .. .. tools openstack_data openstack_data_test_helper)))

  before :each do
    MiqRegion.seed
    _guid, _server, @zone = EvmSpecHelper.create_guid_miq_server_zone

    @mock_meter_list = OpenstackMeterListData.new
    @mock_stats_data = OpenstackMetricStatsData.new

    @ems_openstack = FactoryGirl.create(:ems_openstack_infra, :zone => @zone)
    @ems_openstack.stub(:list_meters).and_return(
      OpenstackApiResult.new(@mock_meter_list.list_meters("resource_counters")),
      OpenstackApiResult.new(@mock_meter_list.list_meters("metadata_counters")))

    @host = FactoryGirl.create(:host_openstack_infra, :ext_management_system => @ems_openstack)
    @host.stub(:perf_init_openstack).and_return(@ems_openstack)
  end

  context "with standard interval data" do
    before :each do
      @ems_openstack.stub(:get_statistics) do |name, _options|
        OpenstackApiResult.new(@mock_stats_data.get_statistics(name))
      end
    end

    it "treats openstack timestamp as UTC" do
      ts_as_utc = api_time_as_utc(@mock_stats_data.get_statistics("hardware.system_stats.cpu.util").last)
      _counters, values_by_id_and_ts = @host.perf_collect_metrics_openstack("perf_capture_data_openstack_infra",
                                                                            "realtime")
      ts = Time.parse(values_by_id_and_ts[@host.ems_ref].keys.sort.last)

      ts_as_utc.should eq ts
    end

    it "translates cumulative meters into discrete values" do
      counter_info = Metric::Capture::OpenstackInfra::COUNTER_INFO.find do |c|
        c[:vim_style_counter_key] == "disk_usage_rate_average"
      end

      # the next 4 steps are test comparison data setup

      # 1. grab the 3rd-to-last, 2nd-to-last and last API results for disk read/writes
      # need 3rd-to-last to get the interval for the 2nd-to-last values
      *_, read_bytes_prev, read_bytes1, read_bytes2 = @mock_stats_data.get_statistics(
          "hardware.system_stats.io.outgoing.blocks")
      *_, write_bytes_prev, write_bytes1, write_bytes2 = @mock_stats_data.get_statistics(
          "hardware.system_stats.io.incoming.blocks")

      read_ts_prev = api_duration_time_as_utc(read_bytes_prev)
      write_ts_prev = api_duration_time_as_utc(write_bytes_prev)
      read_val_prev = read_bytes_prev["avg"]
      write_val_prev = write_bytes_prev["avg"]

      # 2. calculate the disk_usage_rate_average for the 2nd-to-last API result
      read_ts1 = api_duration_time_as_utc(read_bytes1)
      read_val1 = read_bytes1["avg"]
      write_ts1 = api_duration_time_as_utc(write_bytes1)
      write_val1 = write_bytes1["avg"]
      disk_val1 = counter_info[:calculation].call(
        {
          "hardware.system_stats.io.incoming.blocks" => read_val1 - read_val_prev,
          "hardware.system_stats.io.outgoing.blocks" => write_val1 - write_val_prev
        },
        {
          "hardware.system_stats.io.incoming.blocks" => read_ts1 - read_ts_prev,
          "hardware.system_stats.io.outgoing.blocks" => write_ts1 - write_ts_prev,
        })

      # 3. calculate the disk_usage_rate_average for the last API result
      read_ts2 = api_duration_time_as_utc(read_bytes2)
      read_val2 = read_bytes2["avg"]
      write_ts2 = api_duration_time_as_utc(write_bytes2)
      write_val2 = write_bytes2["avg"]
      disk_val2 = counter_info[:calculation].call(
        {
            "hardware.system_stats.io.incoming.blocks" => read_val2 - read_val1 ,
            "hardware.system_stats.io.outgoing.blocks" => write_val2 - write_val1
        },
        {
            "hardware.system_stats.io.incoming.blocks" => read_ts2 - read_ts1,
            "hardware.system_stats.io.outgoing.blocks" => write_ts2 - write_ts1,
        })

      # get the actual values from the method
      _, values_by_id_and_ts = @host.perf_collect_metrics_openstack("perf_capture_data_openstack_infra", "realtime")
      values_by_ts = values_by_id_and_ts[@host.ems_ref]

      # make sure that the last calculated value is the same as the discrete values
      # calculated in step #2 and #3 above
      *_, result = values_by_ts

      read_ts1_period = api_time_as_utc(read_bytes1)
      read_ts2_period = api_time_as_utc(read_bytes2)
      result[read_ts1_period.iso8601]["disk_usage_rate_average"].should eq disk_val1
      result[read_ts2_period.iso8601]["disk_usage_rate_average"].should eq disk_val2
    end
  end

  context "with irregular interval data" do
    before do
      @ems_openstack.stub(:get_statistics) do |name, _options|
        OpenstackApiResult.new(@mock_stats_data.get_statistics(name, "irregular_interval"))
      end

      @orig_log = $log
      $log = double.as_null_object
    end

    after do
      $log = @orig_log
    end

    it "normalizes irregular intervals into 20sec intervals" do
      # tbd
    end

    it "logs when capture intervals are too small" do
      $log.should_receive(:warn).with(/Capture interval invalid/).at_least(:once)
      @vm.perf_collect_metrics_openstack("realtime")
    end
  end

  def api_time_as_utc(api_result)
    period_end = api_result["period_end"]
    period_end << "Z" if period_end.size == 19
    Time.parse(period_end)
  end

  def api_duration_time_as_utc(api_result)
    duration_end = api_result["duration_end"]
    duration_end << "Z" if duration_end.size == 19
    Time.parse(duration_end)
  end
end
