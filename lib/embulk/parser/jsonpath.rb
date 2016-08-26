Embulk::JavaPlugin.register_parser(
  "jsonpath", "org.embulk.parser.jsonpath.JsonpathParserPlugin",
  File.expand_path('../../../../classpath', __FILE__))
