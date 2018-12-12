# A cache for the CachedConfluentSchemaRegistry.
# Simply stores the schemas and ids in in-memory hashes.
#
# ids_by_schema:
#   key = subject + schema
#   value = id
#
# schemas_by_id:
#   key = id
#   value = schema

class AvroTurf::InMemoryCache

  def initialize
    @schemas_by_id = {}
    @ids_by_schema = {}
  end

  def lookup_by_id(id)
    @schemas_by_id[id]
  end

  def store_by_id(id, schema)
    @schemas_by_id[id] = schema
  end

  def lookup_by_schema(subject, schema)
    key = subject + schema.to_s
    @ids_by_schema[key]
  end

  def store_by_schema(subject, schema, id)
    key = subject + schema.to_s
    @ids_by_schema[key] = id
  end
end
