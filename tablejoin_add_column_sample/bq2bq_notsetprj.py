# coding: utf-8
from __future__ import absolute_import

import argparse
import logging
#import re
#from past.builtins import unicode
import json
from datetime import datetime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery


def data_etl(element):
  """
  element
  id	name	cc	cost	cost_doll	rate_yen	rate_doll_yen	
  3, カレー, UK, 7.0, 6.8, 143.0, 110.0
  output
  id, name, cc, cost, cost_doll, cost_yen, cost_doll_yen
  3, カレー, UK, 7.0, 6.8, 1001(7*143), 748(6.8*110)
  """
  id = element['id']
  name = element['name']
  cc = element['cc']
  cost = element['cost']
  cost_doll = element['cost_doll']
  rate_yen = element['rate_yen']
  rate_doll_yen = element['rate_doll_yen']

  # Calculation
  cost_yen = cost * rate_yen
  cost_doll_yen = cost_doll * rate_doll_yen

  return {
    'id': id,
    'name': name,
    'cc': cc,
    'cost': cost,
    'cost_doll': cost_doll,
    'cost_yen': cost_yen,
    'cost_doll_yen': cost_doll_yen,
  }


def get_schema():
  """
  #output table
   CREATE TABLE `[project id].[dataset id].output_table` -- beam CREATE_IF_NEEDED
    (id INT64, name STRING, cc STRING,
    cost FLOAT64, cost_doll FLOAT64,
    cost_yen FLOAT64, cost_doll_yen FLOAT64);
  """
  schema_info_dict = {
    'id': 'integer',
    'name': 'string',
    'cc': 'string',
    'cost': 'float',
    'cost_doll': 'float',
    'cost_yen': 'float',
    'cost_doll_yen': 'float'
  }

  table_schema = bigquery.TableSchema()
  for cname, ctype in schema_info_dict.items():
    schema = bigquery.TableFieldSchema()
    schema.name = cname
    schema.type = ctype
    schema.mode = 'nullable'
    table_schema.fields.append(schema)

  return table_schema


def run(argv=None):
  project_id = '[project id]'
  bucket_name = '[bucket name]'
  job_name = 'bq2bq-python-tablejoin-addcolumn-test'
  dataset = '[dataset id]'

  # Setting
  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
      '--runner=DataflowRunner',
      '--project={}'.format(project_id),
      '--staging_location=gs://{}/staging'.format(bucket_name),
      '--temp_location=gs://{}/temp'.format(bucket_name),
      '--job_name={}'.format(job_name),
  ])

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  # Query
  """
  #input table 1
   CREATE TABLE `[project id].[dataset id].input_table1`
    (id INT64, name STRING, cc STRING, cost FLOAT64, cost_doll FLOAT64);
    INSERT INTO `[project id].[dataset id].input_table1`
    VALUES
    (1, 'たまご', 'UK', 2.0, 1.8),
    (2, 'うどん', 'UK', 3.0, 2.8),
    (3, 'カレー', 'UK', 7.0, 6.8);
  #input table 2
   CREATE TABLE `[project id].[dataset id].input_table2`
    (id INT64, cc_name STRING, cc STRING, rate_yen FLOAT64, rate_doll_yen FLOAT64);
    INSERT INTO `[project id].[dataset id].input_table2`
    VALUES
    (1, 'イギリス', 'UK', 143.0, 110.0);
  """
  query = """
SELECT
  A.id, A.name, A.cc, A.cost, A.cost_doll, B.rate_yen, B.rate_doll_yen
FROM
  `{0}.{1}.input_table1` AS A
 INNER JOIN
  `{0}.{1}.input_table2` AS B
  ON A.cc = B.cc
""".format(project_id, dataset)
  output_table_name = '{0}.output_table'.format(dataset)
  print(query)

  # Pipeline
  with beam.Pipeline(options=pipeline_options) as p:
    (p | 'read' >> beam.io.Read(
        beam.io.BigQuerySource(
          project=project_id, 
          use_standard_sql=True, 
          query=query)
      )
      | 'modify' >> beam.Map(data_etl)
      | 'write' >> beam.io.Write(
        beam.io.BigQuerySink(
          output_table_name,
          schema=get_schema(),
          create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
          write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE)
        )
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
