module MiqAeMethodService
  class MiqAeServiceOrchestrationTemplateHot < MiqAeServiceOrchestrationTemplate
    CREATE_ATTRIBUTES = [:name, :description, :content, :draft, :orderable, :ems_id]

    def self.create(options = {})
      attributes = options.symbolize_keys.slice(*CREATE_ATTRIBUTES)
      attributes[:remote_proxy] = true

      ar_method { MiqAeServiceOrchestrationTemplateHot.wrap_results(OrchestrationTemplateHot.create!(attributes)) }
    end

    def self.destroy(id)
      hot = OrchestrationTemplateHot.find(id)
      hot.remote_proxy = true
      hot.destroy
    end
  end
end