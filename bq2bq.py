from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery

import json
from datetime import datetime

# https://beam.apache.org/get-started/quickstart-py/
def parse_user(element):
  # INSERT dataset.table (name, json_text)
  # VALUES('taro', '{"key1": "value1", "key2": "value2"}')
  name = element['name']
  json_text = element['json_text']

  json_dict = json.loads(json_text)
  value1 = json_dict['key1']
  value2 = json_dict['key2']

  return {
    'name': name,
    'attr': {
      'key1': value1,
      'key2': value2
    }
  }


def run(argv=None):
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      '--runner=DirectRunner',
      '--project=<project-id>',
      '--staging_location=gs://<bucket_path>/staging',
      '--temp_location=gs://<bucket_path>/temp',
      '--job_name=<job_name>',
  ])

  table_schema = bigquery.TableSchema()
  name_schema = bigquery.TableFieldSchema()
  name_schema.name = 'name'
  name_schema.type = 'string'
  name_schema.mode = 'nullable'
  table_schema.fields.append(name_schema)
  
  attr_schema = bigquery.TableFieldSchema()
  attr_schema.name = 'attr'
  attr_schema.type = 'record'
  attr_schema.mode = 'nullable'
  key1 = bigquery.TableFieldSchema()
  key1.name = 'key1'
  key1.type = 'string'
  key1.mode = 'nullable'
  attr_schema.fields.append(key1)
  key2 = bigquery.TableFieldSchema()
  key2.name = 'key2'
  key2.type = 'string'
  key2.mode = 'nullable'
  attr_schema.fields.append(key2)
  table_schema.fields.append(attr_schema)

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:
    query = 'SELECT name, json_text FROM dataset.table'
    (p | 'read' >> beam.io.Read(beam.io.BigQuerySource(project='project-id', use_standard_sql=False, query=query))
        | 'modify' >> beam.Map(parse_user)
        | 'write' >> beam.io.Write(beam.io.BigQuerySink(
        'dataset.dist_table',
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE))
    )

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
