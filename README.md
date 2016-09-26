# Jsonpath parser plugin for Embulk

The JSON with [JSONPath](http://goessner.net/articles/JsonPath/) parser plugin for the Embulk.

## Overview

* **Plugin type**: parser
* **Guess supported**: yes (A JSON data size supports up to 32KB. [#476](https://github.com/embulk/embulk/issues/476))

## Configuration

* **type**: Specify this parser as jsonpath
* **columns**: Specify column name and type. See below (array, required)
* **root**: Specify data path with JSONPath. It must be Array object (string, default:'$') ([detail](https://github.com/jayway/JsonPath#operators))
* **stop_on_invalid_record**: Stop bulk load transaction if a file includes invalid record (such as invalid timestamp) (boolean, default: false)
* **default_timezone**: Default timezone of the timestamp (string, default: UTC)
* **default_timestamp_format**: Default timestamp format of the timestamp (string, default: `%Y-%m-%d %H:%M:%S.%N %z`)
* **default_typecast**: Specify whether to cast values automatically to the specified types or not (boolean, default: true)

## Example

```json
{
  "count": 100,
  "page": 1,
  "results": [
    {
      "name": "Hugh Rutherford",
      "city": "Mitchellfurt",
      "street_name": "Ondricka Island",
      "zip_code": "75232",
      "registered_at": "2015-09-09 05:28:45",
      "vegetarian": true,
      "age": 44,
      "ratio": 79.092
    },
    {
      "name": "Miss Carmella Bashirian",
      "city": "Madilynchester",
      "street_name": "Rhea Walks",
      "zip_code": "44398",
      "registered_at": "2014-07-01 04:25:27",
      "vegetarian": true,
      "age": 73,
      "ratio": 50.608
    }]
}
```


```yaml
in:
  type: any file input plugin type
  parser:
    type: jsonpath
    root: "$.results"
    default_timezone: "Asia/Tokyo"
    columns:
      - { name: "name",          type: string }
      - { name: "city",          type: string }
      - { name: "street_name",   type: string }
      - { name: "zip_code",      type: string }
      - { name: "registered_at", type: timestamp, format: "%Y-%m-%d %H:%M:%S" }
      - { name: "vegetarian",    type: boolean }
      - { name: "age",           type: long }
      - { name: "ratio",         type: double }
```

Preview results

```text
*************************** 1 ***************************
         name (   string) : Hugh Rutherford
         city (   string) : Mitchellfurt
  street_name (   string) : Ondricka Island
     zip_code (   string) : 75232
registered_at (timestamp) : 2015-09-08 20:28:45 UTC
   vegetarian (  boolean) : true
          age (     long) : 44
        ratio (   double) : 79.092
*************************** 2 ***************************
         name (   string) : Miss Carmella Bashirian
         city (   string) : Madilynchester
  street_name (   string) : Rhea Walks
     zip_code (   string) : 44398
registered_at (timestamp) : 2014-06-30 19:25:27 UTC
   vegetarian (  boolean) : true
          age (     long) : 73
        ratio (   double) : 50.608
```

```
$ embulk gem install embulk-parser-jsonpath
$ embulk guess -g jsonpath config.yml -o guessed.yml
```

## Build

```
$ ./gradlew gem  # -t to watch change of files and rebuild continuously
```

## Acknowledgment

I would like to express my special thanks to the developers of [embulk-parser-jsonl](https://github.com/shun0102/embulk-parser-jsonl) and [embulk-filter-typecast](https://github.com/sonots/embulk-filter-typecast) projects.

Almost codes copied from this project.
