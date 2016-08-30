require 'json'
require 'jsonpath'
module Embulk
  module Guess

    class Jsonpath < TextGuessPlugin
      Plugin.register_guess("jsonpath", self)

      def guess_text(config, sample_text)
        parser_config = config.param("parser",:hash)
        json_path = parser_config.param("root",:string,default: "$")
        json = JsonPath.new(json_path).on(sample_text).first
        if( json.kind_of?(Array) )
          row = json.first
          raise RuntimeError,"Can't guess row data must be hash" unless row.kind_of?(Hash)
          columns = Embulk::Guess::SchemaGuess.from_hash_records(json).map do |c|
            column = {name: c.name, type: c.type}
            column[:format] = c.format if c.format
            column
          end
          parser_guessed = {"type" => "jsonpath"}
          parser_guessed["columns"] = columns
          return {"parser" => parser_guessed}
        else
          raise RuntimeError,"Can't guess specified path(#{json_path})"
        end

      end
    end
  end
end
