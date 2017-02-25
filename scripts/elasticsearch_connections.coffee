elasticsearch   = require "elasticsearch"

module.exports = {
  scpr_es: new elasticsearch.Client(host:process.env.ELASTICSEARCH_SERVER),
  es_client: new elasticsearch.Client(host:process.env.LOGSTASH_SERVER)
}
