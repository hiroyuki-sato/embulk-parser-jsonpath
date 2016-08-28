#!/usr/bin/env ruby 

require 'faker'
require 'json'

results = []
1.upto(100){ |n|
  record = {
    'name' => Faker::Name.name,
    'city' => Faker::Address.city,
    'street_name' => Faker::Address.street_name,
    'zip_code' => Faker::Address.zip_code,
    'registered_at' => Faker::Time.between(DateTime.now - 1825,DateTime.now - 30).strftime("%Y-%m-%d %H:%M:%S"),
    'vegetarian' => Faker::Boolean.boolean(0.2),
    'age' => Faker::Number.between(20,80).to_i,
    'ratio' => Faker::Number.decimal(2, 3).to_f
  }
  results << record
}

data = {
  "count" => 100,
  "page" => 1,
  "results" => results
}

print data.to_json
