class DtoCollection
  include ActiveModel::Model
  attr_accessor :saved

  class Dto
    def initialize(data)
      @data = data
    end

    def provider_uuid
      provider_uuid_attributes.map { |attribute| @data[attribute].to_s }.join("__")
    end

    def provider_uuid_attributes
      [:ems_ref]
    end

    def object
      @data[:_object]
    end

    def attributes
      @data.transform_values! do |value|
        if value.is_a? DtoCollection::LazyDto
          value.load
        else
          value
        end
      end
    end
  end

  class LazyDto
    def initialize(ems_ref, dto_collection)
      @ems_ref = ems_ref
      @dto_collection = dto_collection
    end

    def to_s
      @ems_ref
    end

    def load
      @dto_collection.find(to_s).try!(:object)
    end
  end

  include Enumerable
  def initialize
    @data = []
    @data_index = {}
    @saved = false
  end

  def saved?
    @saved
  end

  def saveable?(hashes)
    dependencies.all? do |dep|
      hashes[dep].saved?
    end
  end

  def dependencies
    []
  end

  def <<(dto)
    @data_index[dto.provider_uuid] = dto
    @data << dto
  end

  def find(ems_ref)
    @data_index[ems_ref]
  end

  def provider_uuid_attributes
    [:ems_ref]
  end

  def lazy_find(ems_ref)
    LazyDto.new(ems_ref, self)
  end

  def new_dto(hash)
    self.class::Dto.new(hash)
  end

  def each(*args, &block)
    @data.each(*args, &block)
  end
end
