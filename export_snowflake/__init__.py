#!/usr/bin/env python3

import argparse
import io
import json
import traceback
import logging
import os
import sys
import boto3
import time

from singer import get_logger
from datetime import datetime, timedelta

from export_snowflake.file_formats import csv
from export_snowflake.file_formats import parquet
from export_snowflake import stream_utils

from export_snowflake.db_sync import DbSync
from export_snowflake.file_format import FileFormatTypes
from export_snowflake.exceptions import (
    SymonException
)

LOGGER = get_logger('export_snowflake')

# Tone down snowflake.connector log noise by only outputting warnings and higher level messages
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

# for symon error logging
ERROR_START_MARKER = '[target_error_start]'
ERROR_END_MARKER = '[target_error_end]'

LOCAL_SCHEMA_FILE_PATH = 'local_schema.json'

def get_snowflake_statics(config):
    """Retrieve common Snowflake items will be used multiple times

    Params:
        config: configuration dictionary

    Returns:
        tuple of retrieved items: table_cache, file_format_type
    """
    db = DbSync(config)
    # The file format is detected at DbSync init time
    file_format_type = db.file_format.file_format_type
    
    return file_format_type

def direct_transfer_data_from_s3_to_snowflake(config, o, file_format_type):    
    # retrieve the schema file from s3 bucket
    # if the table doesn't exist, create the new table based on the schema
    s3_client = boto3.client('s3')

    bucket = config["bucket"]
    prefix = config["prefix"]
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)

    schema_file = None
    for obj in response['Contents']:
        # for both python and spark parquet_to_csv steps, we always have a schema.json with the schema details
        if obj['Key'].endswith('.json'):
            schema_file = obj['Key']
    
    if schema_file is None:
        raise Exception("Schema file not found in the s3 bucket")

    s3_client.download_file(bucket, schema_file, LOCAL_SCHEMA_FILE_PATH)

    f = open(LOCAL_SCHEMA_FILE_PATH)
    schema = json.load(f)

    # generate the schema object for DbSync
    stream = config["stream"]
    o = {
        "stream": stream,
        "schema": {"properties": schema['fields']},
        "key_properties": config["key_columns"]
    }
    
    db_sync = DbSync(config, o, None, file_format_type)

    try:
        transfer_start_time = time.time()

        # check if the schema(Snowflake table) exists and create it if not
        db_sync.create_schema_if_not_exists()
        
        # sync_table will check the schema with the table in Snowflake, if there's a miss matching in columns, raise an error
        db_sync.sync_table()
        
        # generate a new stage with stream in Snowflake
        # the stage will be an external stage that points to the s3
        # the following merge query will be processed directly against the external stage
        stage_generation_query = db_sync.generate_temporary_external_s3_stage(bucket, prefix, config['s3_credentials'])
        
        # after creating the external stage, we could load the file directly from the s3 to Snowflake
        # need to specify the patterns in the s3 bucket to filter out the target csv.gz files
        db_sync.load_file(stage_generation_query)
        
        transfer_end_time = time.time()
        stream = config["stream"]
        LOGGER.info(f"Elapsed time usage for {stream} is {transfer_end_time - transfer_start_time}")
    except Exception as e:
        LOGGER.error(f"Error occurred in direct_transfer_data_from_s3_to_snowflake: {e}")
        raise e
    finally:
        # remove the schema file from local
        os.remove(LOCAL_SCHEMA_FILE_PATH)
        
        # Snowflake will only remove the external stage object, the s3 bucket and files will remain
        db_sync.remove_external_s3_stage()

def main():
    """Main function"""
    try:
        error_info = None
        arg_parser = argparse.ArgumentParser()
        arg_parser.add_argument('-c', '--config', help='Config file')
        args = arg_parser.parse_args()

        if args.config:
            with open(args.config, encoding="utf8") as config_input:
                config = json.load(config_input)
        else:
            config = {}

        # get file_format details from snowflake
        file_format_type = get_snowflake_statics(config)
        
        direct_transfer_data_from_s3_to_snowflake(config, None, file_format_type)

        LOGGER.debug("Exiting normally")
    except SymonException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'code': e.code,
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }

        if e.details is not None:
            error_info['details'] = e.details
        raise
    except BaseException as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_info = {
            'message': traceback.format_exception_only(exc_type, exc_value)[-1],
            'traceback': "".join(traceback.format_tb(exc_traceback))
        }
        raise
    finally:
        if error_info is not None:
            try:
                error_file_path = args.config.get('error_file_path', None)
                if error_file_path is not None:
                    try:
                        with open(error_file_path, 'w', encoding='utf-8') as fp:
                            json.dump(error_info, fp)
                    except:
                        pass
                # log error info as well in case file is corrupted
                error_info_json = json.dumps(error_info)
                error_start_marker = args.config.get('error_start_marker', ERROR_START_MARKER)
                error_end_marker = args.config.get('error_end_marker', ERROR_END_MARKER)
                LOGGER.info(f'{error_start_marker}{error_info_json}{error_end_marker}')
            except:
                # error occurred before args was parsed correctly, log the error
                error_info_json = json.dumps(error_info)
                LOGGER.info(f'{ERROR_START_MARKER}{error_info_json}{ERROR_END_MARKER}')


if __name__ == '__main__':
    main()
