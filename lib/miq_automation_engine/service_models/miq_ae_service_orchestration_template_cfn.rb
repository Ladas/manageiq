module MiqAeMethodService
  class MiqAeServiceOrchestrationTemplateCfn < MiqAeServiceOrchestrationTemplate
    CREATE_ATTRIBUTES = [:name, :description, :content, :draft, :orderable, :ems_id]

    def self.create(options = {})
      attributes = options.symbolize_keys.slice(*CREATE_ATTRIBUTES)
      attributes[:remote_proxy] = true

      ar_method { MiqAeServiceOrchestrationTemplateCfn.wrap_results(OrchestrationTemplateCfn.create!(attributes)) }
    end

    def self.destroy(id)
      cfn = OrchestrationTemplateCfn.find(id)
      cfn.remote_proxy = true
      cfn.destroy
    end
  end
end