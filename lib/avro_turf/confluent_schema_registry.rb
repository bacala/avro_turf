require 'schema_registry'

class AvroTurf::ConfluentSchemaRegistry

  def initialize(url, logger: Logger.new($stdout))
    @logger = logger
    @client = SchemaRegistry::Client.new(url)
  end

  def fetch(id)
    @logger.info "Fetching schema with id #{id}"
    @client.schema(id)
  end
 
  def fetch_id(subject, schema)
    @logger.info "Fetching id with subject #{subject} and schema."
    SchemaRegistry::Subject.new(@client, subject.to_s).verify_schema(schema).id
  end

  def register(subject, schema)
    @logger.info "Registered schema for subject `#{subject}`"
    SchemaRegistry::Subject.new(@client, subject.to_s).register_schema(schema)
  end

  # List all subjects
  def subjects
    @client.subjects
  end

  # List all versions for a subject
  def subject_versions(subject)
    SchemaRegistry::Subject.new(@client, subject.to_s).versions
  end

  # Get a specific version for a subject
  def subject_version(subject, version = 'latest')
    SchemaRegistry::Subject.new(@client, subject.to_s).version("latest")
  end

  # Check if a schema exists. Returns nil if not found.
  def check(subject, schema)
    SchemaRegistry::Subject.new(@client, subject.to_s).schema_registered?(schema)
  end

  # Check if a schema is compatible with the stored version.
  # Returns:
  # - true if compatible
  # - nil if the subject or version does not exist
  # - false if incompatible
  # http://docs.confluent.io/3.1.2/schema-registry/docs/api.html#compatibility
  def compatible?(subject, schema, version = 'latest')
    SchemaRegistry::Subject.new(@client, subject.to_s).compatible?(schema, version)
  end
  
  # Get global config
  def global_config
    @client.request(:get, "/config")
  end

  # Update global config
  def update_global_config(config)
    @client.request(:put, "/config",  body: config.to_json)
  end

  # Get config for subject
  def subject_config(subject)
    @client.request(:get, "/config/#{subject}")
  end

  # Update config for subject
  def update_subject_config(subject, config)
    @client.request(:put, "/config/#{subject}", body: config.to_json)
  end

end
