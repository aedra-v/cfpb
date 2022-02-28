from flask import Flask
from flask import request
from flask import jsonify #double check why
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd #pre-emptive load to play with df
import numpy as np
import json #double check why
import io #double check why
import os #pre-emptive load

headers = {
	'Access-Control-Allow-Origin': '*',
	'Access-Control-Allow-Methods': 'POST',
	'Access-Control-Max-Age': '1000'
}

def hello_content(request):
	fields = {}
	data = request.form.to_dict()
	for field in data:
		fields[field] = data[field]
		print('Processed field: %s' % field)
	packagejson = json.dumps(fields, indent = 4)
	json_object = json.loads(packagejson)
	
	table_schema = {
        	'name': 'url',
		'type': 'STRING',
		'mode': 'NULLABLE'
		}, {
		'name': 'data_html',
		'type': 'STRING',
		'mode': 'NULLABLE'
		}, {
		'name': 'data_simple',
		'type': 'STRING',
          	'mode': 'NULLABLE'
		}, {
		'name': 'prov',
		'type': 'STRING',
          	'mode': 'NULLABLE'
		}, {
		'name': 'key',
		'type': 'STRING',
		'mode': 'NULLABLE'
	}
	
	project_id = 'artaeum-py'
	dataset_id =  'kd_dump'
	table_id = 'artaeum-py:kd_dump.uto-intel-import'

	client = bigquery.Client()

	job_config = bigquery.LoadJobConfig()
	job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
	job_config.schema = format_schema(table_schema)
	job = client.load_table_from_json(json_object, table, job_config = job_config)
	print(job.result())
	
	return (jsonify(success='true',), 200, headers)