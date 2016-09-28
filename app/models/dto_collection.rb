class DtoCollection
  class Dto
    include ActiveModel::Model

    def provider_uuid
      provider_uuid_attributes.map{|attribute| send(attribute)}.join("__")
    end

    def provider_uuid_attributes
      [:ems_ref]
    end
  end

  include Enumerable
  def initialize
    @data = []
    @data_index = {}
  end

  def <<(dto)
    @data_index[dto.provider_uuid] = dto
    @data << dto
  end


  def provider_uuid_attributes
    [:ems_ref]
  end

  def lazy_find(ems_ref)
    ->(ems_ref) {
      @data_index[ems_ref]
    }
  end

  def new_dto(hash)
    self.class::Dto.new(hash)
  end

  def each(*args, &block)
    @data.each(*args, &block)
  end
end
