require "spec_helper"

describe Metric::CiMixin::Capture::Openstack do
  require File.expand_path(File.join(File.dirname(__FILE__), %w{.. .. .. .. tools openstack_data openstack_data_test_helper}))

  before :each do
    MiqRegion.seed
    guid, server, @zone = EvmSpecHelper.create_guid_miq_server_zone

    @mock_meter_list = OpenstackMeterListData.new
    @mock_stats_data = OpenstackMetricStatsData.new

    @ems_openstack = FactoryGirl.create(:ems_openstack, :zone => @zone)
    @ems_openstack.stub(:list_meters).and_return(
        OpenstackApiResult.new(@mock_meter_list.list_meters("resource_counters")),
        OpenstackApiResult.new(@mock_meter_list.list_meters("metadata_counters")))

    @vm = FactoryGirl.create(:vm_perf_openstack, :ext_management_system => @ems_openstack)
    @vm.stub(:perf_init_openstack).and_return(@ems_openstack)
  end

  SECOND_COLLECTION_PERIOD_START = '2013-08-28T12:02:00'
  COLLECTION_OVERLAP_PERIOD      = 20.minutes

  context "second collection period from 2 collection periods total, start of this period has incomplete stat" do
    before do
      @ems_openstack.stub(:get_statistics) do |name, _options|
        second_collection_period = filter_statistics(@mock_stats_data.get_statistics(name, "multiple_collection_periods"),
                                                     '>',
                                                     SECOND_COLLECTION_PERIOD_START,
                                                     COLLECTION_OVERLAP_PERIOD)

        OpenstackApiResult.new(second_collection_period)
      end

      @orig_log = $log
      $log = double.as_null_object
    end

    after do
      $log = @orig_log
    end

    ###################################################################################################################
    # Complete scenario doc is placed in first collection period

    it "2. period of the net_usage_rate_average should fill stat that hasn't been collected in last period" do
      counter_info = Metric::Capture::Openstack::COUNTER_INFO.find do |c|
        c[:vim_style_counter_key] == "net_usage_rate_average"
      end

      # grab read bytes and write bytes data, these values are pulled directly from
      # spec/tools/openstack_data/openstack_perf_data/multiple_collection_periods.yml
      read_bytes =  filter_statistics(@mock_stats_data.get_statistics("network.incoming.bytes", "multiple_collection_periods"),
                                      '>',
                                      SECOND_COLLECTION_PERIOD_START,
                                      COLLECTION_OVERLAP_PERIOD)

      write_bytes = filter_statistics(@mock_stats_data.get_statistics("network.outgoing.bytes", "multiple_collection_periods"),
                                      '>',
                                      SECOND_COLLECTION_PERIOD_START,
                                      COLLECTION_OVERLAP_PERIOD)
      # Drop fist element of write bytes, as that is the incomplete stat
      write_bytes.shift

      _, values_by_id_and_ts = @vm.perf_collect_metrics_openstack("perf_capture_data_openstack", "realtime")
      values_by_ts = values_by_id_and_ts[@vm.ems_ref]
      ts_keys = values_by_ts.keys.sort

      # make computation of stats for comparison
      avg_stat1 = make_calculation(counter_info, "network.incoming.bytes", "network.outgoing.bytes", write_bytes, read_bytes, 0, 1)
      avg_stat2 = make_calculation(counter_info, "network.incoming.bytes", "network.outgoing.bytes", write_bytes, read_bytes, 1, 2)
      avg_stat3 = make_calculation(counter_info, "network.incoming.bytes", "network.outgoing.bytes", write_bytes, read_bytes, 2, 3)
      avg_stat4 = make_calculation(counter_info, "network.incoming.bytes", "network.outgoing.bytes", write_bytes, read_bytes, 3, 4)
      avg_stat5 = make_calculation(counter_info, "network.incoming.bytes", "network.outgoing.bytes", write_bytes, read_bytes, 4, 5)

      # Ensure that the first 30 statistics match nil. This happens because there is one more stat of cpu_util, but
      # net usage stat has been incomplete, so we store nil values, those nil values has been filled in previous
      # collecting period.
      # !!!!!!! It's up to saving mechanism to not overwrite the old values with nil. !!!!!!!!!!!
      (0..20).each { |i| values_by_ts[ts_keys[i]]["net_usage_rate_average"].should eq nil }
      # ensure that the next 30 statistics match avg_stat1
      (21..50).each { |i| values_by_ts[ts_keys[i]]["net_usage_rate_average"].should eq avg_stat1 }
      # ensure that the next 30 statistics match avg_stat2
      (51..80).each { |i| values_by_ts[ts_keys[i]]["net_usage_rate_average"].should eq avg_stat2 }
      # ensure that the next 30 statistics match avg_stat3
      (81..110).each { |i| values_by_ts[ts_keys[i]]["net_usage_rate_average"].should eq avg_stat3 }
      # ensure that the next 31 statistics match avg_stat4
      (111..140).each { |i| values_by_ts[ts_keys[i]]["net_usage_rate_average"].should eq avg_stat4 }
      # ensure that the next 30 statistics match avg_stat5
      (141..170).each { |i| values_by_ts[ts_keys[i]]["net_usage_rate_average"].should eq avg_stat5 }

      # ensure total number of stats is correct, 151 data stats and 30 nil stats
      ts_keys.count.should eq 171
    end
  end



  def filter_statistics(stats, op, date, subtract_by=nil)
    filter_date = parse_datetime(date)
    filter_date -= subtract_by if subtract_by
    stats.select { |x| x['period_end'].send(op, filter_date) }
  end

  def make_calculation(counter_info, counter_one, counter_two, write_stats, read_stats, first_index, second_index)
    calc = counter_info[:calculation].call(
        {
            counter_one => write_stats[second_index]['avg'] - write_stats[first_index]['avg'],
            counter_two => read_stats[second_index]['avg'] - read_stats[first_index]['avg'],
        },
        {
            counter_one => api_duration_time_as_utc(write_stats[second_index]) - api_duration_time_as_utc(write_stats[first_index]),
            counter_two => api_duration_time_as_utc(read_stats[second_index]) - api_duration_time_as_utc(read_stats[first_index]),
        })
  end

  def api_time_as_utc(api_result)
    period_end = api_result["period_end"]
    parse_datetime(period_end)
  end

  def api_duration_time_as_utc(api_result)
    duration_end = api_result["duration_end"]
    parse_datetime(duration_end)
  end

  def parse_datetime(datetime)
    datetime << "Z" if datetime.size == 19
    Time.parse(datetime)
  end
end
