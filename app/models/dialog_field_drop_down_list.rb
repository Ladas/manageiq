class DialogFieldDropDownList < DialogFieldSortedItem
  def initialize_with_values(dialog_values)
    if load_values_on_init?
      raw_values
      @value = value_from_dialog_fields(dialog_values) || default_value
    else
      @raw_values = initial_values
    end
  end

  def show_refresh_button?
    !!show_refresh_button
  end

  def multi_value?
    return true if options[:force_multi_value].present? && options[:force_multi_value] != "null" && options[:force_multi_value]
  end

  def force_multi_value=(setting)
    options[:force_multi_value] = setting
  end

  def initial_values
    [[nil, "<None>"]]
  end

  def refresh_json_value(checked_value)
    @raw_values = @default_value = nil

    refreshed_values = values

    if checked_value.is_a?(Array) && (refreshed_values.collect { |value_pair| value_pair[0].to_s } & checked_value).present?
      @value = refreshed_values.collect { |value_pair| value_pair[0].to_s } & checked_value
    elsif refreshed_values.collect { |value_pair| value_pair[0].to_s }.include?(checked_value)
      @value = checked_value
    else
      @value = @default_value
    end
    {:refreshed_values => refreshed_values, :checked_value => @value, :read_only => read_only?, :visible => visible?}
  end

  def automate_output_value
    return super unless multi_value?
    if @value.is_a?(Integer)
      a = [@value]
    elsif @value.is_a?(Array)
      a = @value
    else
      a = @value.blank? ? [] : @value.chomp.split(',')
    end
    automate_values = a.first.kind_of?(Integer) ? a.map(&:to_i) : a
    MiqAeEngine.create_automation_attribute_array_value(automate_values)
  end

  private

  def load_values_on_init?
    return true unless show_refresh_button
    load_values_on_init
  end

  def raw_values
    @raw_values ||= dynamic ? values_from_automate : super
    @default_value ||= sort_data(@raw_values).first.first if @raw_values
    self.value ||= @default_value

    @raw_values
  end
end
