module Metric::CiMixin::Capture::OpenstackBase
  def perf_collect_metrics_openstack(capture_data_method, interval_name, start_time = nil, end_time = nil)
    target = "[#{self.class.name}], [#{id}], [#{name}]"
    log_header = "MIQ(#{self.class.name}.perf_collect_metrics_openstack) [#{interval_name}] for: #{target}"

    end_time   ||= Time.now
    end_time     = end_time.utc
    start_time ||= end_time - 4.hours # 4 hours for symmetry with VIM
    start_time   = start_time.utc

    $log.debug "#{log_header} start_time: #{start_time}, end_time: #{end_time}"

    begin
      @perf_ems = perf_init_openstack
      send(capture_data_method, start_time, end_time)
    rescue Exception => err
      $log.error("#{log_header} Unhandled exception during perf data collection: [#{err}], class: [#{err.class}]")
      $log.error("#{log_header}   Timings at time of error: #{Benchmark.current_realtime.inspect}")
      $log.log_backtrace(err)
      raise
    ensure
      perf_release_openstack
    end
  end

  def perf_init_openstack
    raise "No EMS defined" if ext_management_system.nil?

    metering_service, _ = Benchmark.realtime_block(:connect) do
      ext_management_system.connect(:service => "Metering")
    end
    metering_service
  end

  def perf_release_openstack
    @perf_ems = nil
  end

  def perf_capture_data_openstack_base(metric_capture_module, start_time, end_time, resource_filter, metadata_filter)
    # some meters can get gathered by directly querying for the "resource_id",
    #   but other meters can only be gathered by examining the
    #   "metadata.instance_id"
    log_header = "MIQ(#{self.class.name}.perf_collect_data_openstack) [#{start_time} - #{end_time}]"
    $log.debug "#{log_header} start_time: #{start_time}, end_time: #{end_time}"

    instance_filter = resource_filter
    if resource_filter
      $log.debug "#{log_header} getting resource counters using resource filter: #{resource_filter}"
      counters, _ = Benchmark.realtime_block(:capture_counters) do
        @perf_ems.list_meters([instance_filter]).body
      end
      counters.each { |m| m[:instance_filter] = instance_filter }
    else
      $log.debug "#{log_header} no resource filter provided"
      counters = []
    end

    instance_filter = metadata_filter
    if metadata_filter
      $log.debug "#{log_header} getting metadata counters using metadata filter: #{metadata_filter}"
      meta_counters, _ = Benchmark.realtime_block(:capture_meta_counters) do
        @perf_ems.list_meters([instance_filter]).body
      end
    else
      $log.debug "#{log_header} no metadata filter provided"
      meta_counters = []
    end

    meta_counters.each { |m| m[:instance_filter] = instance_filter }
    counters += meta_counters

    counters.select! { |c| metric_capture_module::COUNTER_NAMES.include? c["name"] }

    # TODO(lsmola) we can't be sure Ceilometer will be set with this value configured. If period of collecting the
    # of the data will be bigger, we can have 'holes' in the 20s aligned data stream. So this value should be inferred
    # from the data itself.
    # For now hardcoding to 10 minutes which is Ceilometer default pipeline setting
    data_collecting_period = 10.minutes

    # We will have to account for the fact that each counter can be configured
    # for individual capture intervals ... the out-of-box default is 10min
    start_time -= data_collecting_period * 2

    # Remove seconds from the start_time and end_time, so we always start at 00s and continue on aligned 20s steps
    start_time -= start_time.sec
    end_time -= end_time.sec

    metrics_by_counter_name = {}
    counters.each do |c|
      metrics = metrics_by_counter_name[c["name"]] = {}

      # For now, this logic just mirrors how we capture Amazon CloudWatch data
      # (see amazon.rb)
      (start_time..end_time).step_value(1.day).each_cons(2) do |st, et|
        filter = [{"field" => "timestamp", "op" => "lt", "value" => et.iso8601},
                  {"field" => "timestamp", "op" => "gt", "value" => st.iso8601},
                  c[:instance_filter]]
        statistics, _ = Benchmark.realtime_block(:capture_counter_values) do
          # try to capture for every 20s over the timeframe ... however, the
          # server can be configured for any arbitrary capture interval
          # we'll deal with that below
          options = {'period' => 20, 'q' => filter}
          @perf_ems.get_statistics(c["name"], options).body
        end

        # This is a pretty bad hack to work around a problem with the timestamp
        #   values that come back from ceilometer.  The timestamps come back
        #   without a timezone specifier, e.g.: "2013-08-23T20:06:09".
        #   The time value is actually in UTC, but there's nothing about the
        #   string which indicates that.
        # This hack looks at the length of the string and tries to determine if
        #   the timezone information is missing.  If so, it appends "Z" (zulu
        #   time) to the string to indicate UTC before it is parsed.  This will
        #   force a UTC timezone in order to keep the value consistent with what
        #   was intended--but not indicated--by ceilometer.
        # http://lists.openstack.org/pipermail/openstack-dev/2012-November/002235.html

        statistics.each do |s|
          # Period end has always alligned 20s interval, we just have to make sure the start_time is aligned to 20s.
          # We are enforcing this by removing seconds from start_time, so it always start at 00s.
          period_end = s["period_end"]
          duration_end = s["duration_end"]
          period_end << "Z" if period_end.size == 19
          duration_end << "Z" if duration_end.size == 19
          timestamp = Time.parse(period_end)
          duration_end = Time.parse(duration_end)
          metrics[timestamp] = {:avg => s["avg"], :duration_end => duration_end}
        end
      end
    end

    # TODO(lsmola) check that first statistic is already saved in the database. If not log.warn that there is hole
    # in the data due to missing data in Ceilometer. Only if this is not the first metrics collection,

    # TODO(lsmola) since it actually saves all counters at once, and we don't know if samples of all those counters
    # will be aligned. We need to obtain the overlapping period of start and merge those samples together, so they
    # are not rewritten by nil values of another collection period. This should probably go to saving algorithm

    #require 'byebug'; byebug

    counter_values_by_ts = {}
    metric_capture_module::COUNTER_INFO.each do |i|
      timestamps = i[:openstack_counters].collect { |c| metrics_by_counter_name[c].try(:keys) }
      timestamps = timestamps.flatten.compact.uniq.sort

      aggregate = []
      beginning_of_the_collection_period = nil
      multi_counter_aligned_start = nil
      multi_counter_aligned_end = nil
      # Guard for multicounter search for all counters data.
      multi_counter_aligned_start_guard = nil
      multi_counter_metrics = nil
      last_multi_counter_metrics = nil
      multi_counter_intervals = nil
      timestamps.each_cons(2) do |last_period, period|
        beginning_of_the_collection_period ||= last_period
        multi_counter_aligned_start        ||= last_period
        multi_counter_aligned_end          ||= period
        multi_counter_aligned_start_guard  ||= last_period
        multi_counter_aligned_start_guard    = period if multi_counter_aligned_start_guard=='initialize_with_period'

        multi_counter_metrics              ||= {}
        last_multi_counter_metrics         ||= {}
        multi_counter_intervals            ||= {}

        metrics = {}
        i[:openstack_counters].each { |c| metrics[c] = metrics_by_counter_name.fetch_path(c, period) }

        if i[:openstack_counters].count == 1
          # We have only one counter, we can pass the values and intervals to calculation in simplified manner, which
          # is compatible with previous version. We are passing values, not hashes

          if metric_capture_module.diff_meter? i[:openstack_counters]
            last_metrics = {}
            i[:openstack_counters].each { |c| last_metrics[c] = metrics_by_counter_name.fetch_path(c, last_period) }
            # Interval is related only to diff metrics
            intervals = {}
            i[:openstack_counters].each { |c| intervals[c] = metrics[c][:duration_end] - last_metrics[c][:duration_end] }
            diff_metrics_avg = {}
            i[:openstack_counters].each { |c| diff_metrics_avg[c] = metrics[c][:avg] - last_metrics[c][:avg] }

            value = i[:calculation].call(diff_metrics_avg.values.first, intervals.values.first)
          else
            value = i[:calculation].call(metrics.values.first[:avg], nil)
          end
        else
          # We have more counters in calculation. We have to make sure all counters have values present. It can
          # happen that data of related counters are not collected in the same 20s window. So we will try to collect
          # all of the data for each counter.
          # Important Facts:
          # 1. It can happen, that we will not have all samples for all counters, when we are near the borders of the
          # collecting timeframe. We will attempt to find related data half of the data_collecting_period far. That
          # should help us to avoid joining with the data from 2 Ceilometer collecting periods. If we will not find all
          # data samples of all the counters, it's better to throw the data away, than storing not precise value.
          # 2. If we will throw some incomplete data away, it's very likely it will be collected in next collecting
          # period, because start_time is always moved one data_collecting_period back, to collect the pieces.
          # 3. If collecting of the data takes longer, than half of the data_collecting_period, scale your Ceilometer.
          # 4. Make sure the related counters are collected with same interval in Ceilometer pipeline, or this
          # algorithm might not be able to match them together.

          # 2013-08-28 11:10:20
          #require 'byebug'; byebug
          if i[:openstack_counters].include? "network.incoming.bytes"
            #require 'byebug'; byebug
          end

          if metric_capture_module.diff_meter? i[:openstack_counters]
            # If these are diff meters, I need to make sure I first collect all data samples of last_metrics, then I can
            # start to collect metrics

            if !all_multi_counter_metrics_available?(i, last_multi_counter_metrics)
              # All last multicounter data are not available, try to capture them
              last_metrics = {}

              if last_period - multi_counter_aligned_start_guard > data_collecting_period / 2
                # If we haven't found all the data samples of all the counters half of the data_collecting_period away
                # from the first data sample, just throw away everything and start over, cause we can't find it in this
                # timeframe.
                if beginning_of_the_collection_period == multi_counter_aligned_start
                  # If this is at the start of the whole collection period, it's not considered error. Due to
                  # overlapping of the collections periods, incomplete stat on the begining had to be collected as part
                  # of the last collection period.
                  # That means we are moving the multi_counter_aligned_start so the incomplete period is entirely
                  # skipped.
                  multi_counter_aligned_start = last_period
                else
                  # Not beginning, the data are corrupted or missing. In order to avoid holes in saved data, this period
                  # will be filled by data operiodf next periods.
                  $log.warn("#{log_header} [#{self.name}] Distance of the multiple streams of data is invalid. It "\
                            "exceeded half of the Ceilometer collection period in "\
                            "#{multi_counter_aligned_start_guard} - #{last_period} for counters" \
                            "#{i[:openstack_counters].to_s} It can be caused by different pipeline configuration "\
                            "period for each related sample. Or you need to scale Ceilometer because the samples"\
                            "collection is overloaded.")
                end
                multi_counter_metrics      = {}
                last_multi_counter_metrics = {}
                # Moving to guard to another period cause the old one had incomplete data for all counters. We will
                # try to collect all data for all counters in next guard period.
                multi_counter_aligned_start_guard = last_period
              end

              i[:openstack_counters].each { |c| last_metrics[c] = metrics_by_counter_name.fetch_path(c, last_period) }
              all_last_metrics_available = process_multi_counter_metrics(i, last_metrics, last_multi_counter_metrics)

              if !all_last_metrics_available
                # Reseting multi_counter_aligned_end, which will be initialized to period in next period
                multi_counter_aligned_end = nil
                # All last_multi_counter data are not available, lets move to another period and try to capture them
                next
              else
                # Move guard when last_period is complete
                multi_counter_aligned_start_guard = period
              end
            end
          end

          if period - multi_counter_aligned_start_guard > data_collecting_period / 2
            # The data are corrupted or missing. In order to avoid holes in saved data, this period will be filled
            # by data of next periods.
            $log.warn("#{log_header} [#{self.name}] Distance of the multiple streams of data is invalid. It "\
                      "exceeded half of the Ceilometer collection period in "\
                      "(#{multi_counter_aligned_start_guard}, #{last_period}> for counters" \
                      "#{i[:openstack_counters].to_s} It can be caused by different pipeline configuration "\
                      "period for each related sample. Or you need to scale Ceilometer because the samples"\
                      "collection is overloaded.")

            # We haven't found all the data samples of all the counters half of the data_collecting_period away
            # from the first data sample, just throw away everything and start over, cause we can't find it in this
            # timeframe. We can keep last_multi_counter_metrics, cause that has been completed.
            multi_counter_metrics      = {}
            # Moving to guard to another period cause the old one had incomplete data for all counters. We will try
            # to collect all data for all counters in next guard period.
            multi_counter_aligned_start_guard = period
            # Moving also aligned end, meaning the incomplete stat has been skipped, but will be filled by value of the
            # next if we are not in the end of collection period.
            multi_counter_aligned_end = period
          end

          # We are ready for fetching multi_counter_metrics
          all_metrics_available = process_multi_counter_metrics(i, metrics, multi_counter_metrics)

          if all_metrics_available
            if metric_capture_module.diff_meter? i[:openstack_counters]
              # I have both multi_counter_metrics and multi_counter_metrics full of data for each counter, I can compute
              # diff_metrics and intervals
              metrics_avg = {}
              i[:openstack_counters].each { |c| metrics_avg[c] = multi_counter_metrics[c][:avg] - last_multi_counter_metrics[c][:avg] }
              metrics_intervals = {}
              i[:openstack_counters].each { |c| metrics_intervals[c] = multi_counter_metrics[c][:duration_end] - last_multi_counter_metrics[c][:duration_end] }
            else
              metrics_avg = {}
              metrics_avg = i[:openstack_counters].each { |c| metrics_avg[c] = multi_counter_metrics[c][:avg] }
              # We care about intervals only for diff metrics
              metrics_intervals = nil
            end

            # If we have found all data samples for all counters, we can compute the value and continue to store it
            # across the whole <multi_counter_aligned_start, period> interval
            value = i[:calculation].call(metrics_avg, metrics_intervals)
          else
            # Keeping multi_counter_aligned_start and already obtained multi_counter metrics and intervals and thus
            # expanding the interval for finding all values.
            next
          end
        end

        # For (temporary) symmetry with VIM API we create 20-second intervals.
        (multi_counter_aligned_start + 20.seconds..multi_counter_aligned_end).step_value(20.seconds).each do |ts_item|
          counter_values_by_ts.store_path(ts_item.iso8601, i[:vim_style_counter_key], value)
        end
        
        # Moving multi_counter_aligned_start to next period, this period has been already covered
        multi_counter_aligned_start = multi_counter_aligned_end
        # Moving also guard to next period, will be initialized when next period begins
        multi_counter_aligned_start_guard = 'initialize_with_period'
        # Reset multi_counter_aligned_end, which will be initialized to period in next period
        multi_counter_aligned_end   = nil
        # Moving completed multi_counter_metrics to last_multi_counter_metrics and nullifying multi_counter_metrics,
        # so they can be collected for next period.
        last_multi_counter_metrics = multi_counter_metrics
        multi_counter_metrics = {}
      end
    end
    #require 'byebug'; byebug

    counters_by_id              = {ems_ref => metric_capture_module::VIM_STYLE_COUNTERS}
    counter_values_by_id_and_ts = {ems_ref => counter_values_by_ts}
    return counters_by_id, counter_values_by_id_and_ts
  end

  def process_multi_counter_metrics(i, metrics, multi_counter_metrics)
    # We have to make sure all counters have metric values present.
    i[:openstack_counters].each do |c|
      unless metrics.fetch_path(c, :avg).blank?
        # Always overwriting the multicounter metrics. So if new value comes, it will overwrite the old one, that is
        # possible orphan from another Ceilometer collectiong period.
        multi_counter_metrics[c] = metrics[c]
      end
    end
    all_multi_counter_metrics_available? i, multi_counter_metrics
  end

  def all_multi_counter_metrics_available?(i, multi_counter_metrics)
    all_metrics_available = true
    i[:openstack_counters].each do |c|
      if multi_counter_metrics.fetch_path(c, :avg).blank?
        all_metrics_available = false
      end
    end
    all_metrics_available
  end

end
