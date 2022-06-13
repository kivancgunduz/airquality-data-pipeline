import os
import ndjson

import singer


schema = {
  "properties": {
    "date": {
      "type": "object",
      "properties": {
        "utc": {
          "type": "string"
        },
        "local": {
          "type": "string"
        }
      },
      "required": [
        "utc",
        "local"
      ]
    },
    "parameter": {"type": "string"},
    "value": {"type": "number"},
    "unit": {"type": "string"},
    "averagingPeriod": {
      "type": "object",
      "properties": {
        "unit": {
          "type": "string"
        },
        "value": {
          "type": "integer"
        }
      },
      "required": [
        "unit",
        "value"
      ]
    },
    "location": {"type": "string"},
    "city": {"type": "string"},
    "country": {"type": "string"},
    "coordinates": {
      "type": "object",
      "properties": {
        "latitude": {
          "type": "number"
        },
        "longitude": {
          "type": "number"
        }
      },
      "required": [
        "latitude",
        "longitude"
      ]
    },
    "attribution": {
      "type": "array",
      "items": [
        {
          "type": "object",
          "properties": {
            "name": {
              "type": "string"
            },
            "url": {
              "type": "string"
            }
          },
          "required": [
            "name",
            "url"
          ]
        }
      ]
    },
    "sourceName": {"type": "string"},
    "sourceType": {"type": "string"},
    "mobile": {"type": "boolean"},
    },
  "$schema": "http://json-schema.org/draft-04/schema#"
}


def main():
    # Write the schema to stdout
    singer.write_schema(stream_name='openaq', schema=schema, key_properties=[])
    for root, dirs, files in os.walk(r'./ingest/google_drive'):
        for file in files:
            if file.endswith('.ndjson'):
                with open(os.path.join(root, file), 'rb') as f:
                    data = ndjson.loads(f.read())
                    # Write the data to stdout
                    singer.write_records(stream_name='openaq', records=(data))


if __name__ == '__main__':
    main()
