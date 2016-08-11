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
    return true if options[:force_multi_value].present? && options[:force_multi_value] != "null"
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

    if refreshed_values.collect { |value_pair| value_pair[0].to_s }.include?(checked_value)
      @value = checked_value
    else
      @value = @default_value
    end

    {:refreshed_values => refreshed_values, :checked_value => @value, :read_only => read_only?, :visible => visible?}
  end

  def automate_output_value
    return super unless multi_value?
    a = @value.chomp.split(',')
    automate_values = a.first.kind_of?(Integer) ? a.map(&:to_i) : a
    MiqAeEngine.create_automation_attribute_array_value(automate_values)
  end

  def automate_key_name
    return super unless multi_value?
    MiqAeEngine.create_automation_attribute_array_key(super)
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
