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

        # The rest of code expect a data type like `[ {}, {}, {}]`.
        # If JSONPath specifies a Hash object, it needs to pack a hash object into an array.
        # ex. { "name": "bob", "age":24" } -> [ { "name": "bob", "age":24 } ]
        json = [json] unless json.is_a?(Array)

        no_hash = json.find{ |j| !j.kind_of?(Hash) }
        raise RuntimeError,"Can't exec guess. The row data must be hash." if no_hash
        columns = Embulk::Guess::SchemaGuess.from_hash_records(json).map do |c|
          column = {name: c.name, type: c.type}
          column[:format] = c.format if c.format
          column
        end
        parser_guessed = {"type" => "jsonpath"}
        parser_guessed["columns"] = columns
        return {"parser" => parser_guessed}
      end
    end
  end
end
