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
import snowflake.connector
from datetime import datetime, timedelta, date

from singer import get_logger

from export_snowflake.file_formats import csv
from export_snowflake.file_formats import parquet
from export_snowflake import stream_utils

from export_snowflake.db_sync import DbSync
from export_snowflake.file_format import FileFormatTypes
from export_snowflake.exceptions import (
    SymonException
)

# PyArrow for efficient columnar date clamping
import s3fs
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc

LOGGER = get_logger('export_snowflake')

# S3 filesystem for PyArrow operations
s3_file_system = s3fs.S3FileSystem()

# Pandas Timestamp boundaries - dates at or beyond these are sentinel values
PANDAS_MIN_DATE = date(1677, 9, 22)
PANDAS_MAX_DATE = date(2262, 4, 10)

# Snowflake-safe boundary dates
SNOWFLAKE_MIN_DATE = date(1900, 1, 1)
SNOWFLAKE_MAX_DATE = date(9999, 12, 31)

# Tone down snowflake.connector log noise by only outputting warnings and higher level messages
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

# for symon error logging
ERROR_START_MARKER = '[target_error_start]'
ERROR_END_MARKER = '[target_error_end]'

LOCAL_SCHEMA_FILE_PATH = 'local_schema.json'


def clamp_out_of_bound_dates(table: pa.Table) -> pa.Table:
    """
    Replace pandas boundary dates with Snowflake-safe dates.
    
    Uses PyArrow for efficient columnar processing.
    For timestamp columns: converts to date32 (date-only) to avoid overflow issues.
    """
    columns_modified = []
    new_columns = {}
    
    for i, field in enumerate(table.schema):
        col = table.column(i)
        col_type = field.type
        
        # Handle timestamp columns
        if pa.types.is_timestamp(col_type):
            try:
                date_col = pc.cast(col, pa.date32())
                
                min_mask = pc.less_equal(date_col, pa.scalar(PANDAS_MIN_DATE, type=pa.date32()))
                max_mask = pc.greater_equal(date_col, pa.scalar(PANDAS_MAX_DATE, type=pa.date32()))
                
                min_count = pc.sum(pc.cast(min_mask, pa.int64())).as_py() or 0
                max_count = pc.sum(pc.cast(max_mask, pa.int64())).as_py() or 0
                
                if min_count > 0 or max_count > 0:
                    sf_min_date = pa.scalar(SNOWFLAKE_MIN_DATE, type=pa.date32())
                    sf_max_date = pa.scalar(SNOWFLAKE_MAX_DATE, type=pa.date32())
                    
                    new_col = pc.if_else(min_mask, sf_min_date, date_col)
                    new_col = pc.if_else(max_mask, sf_max_date, new_col)
                    new_columns[field.name] = (new_col, pa.field(field.name, pa.date32()))
                    columns_modified.append(f"{field.name} (timestamp→date: {min_count} min, {max_count} max)")
            except Exception as e:
                LOGGER.warning(f"Could not clamp dates in timestamp column {field.name}: {e}")
        
        # Handle date columns
        elif pa.types.is_date(col_type):
            try:
                min_mask = pc.less_equal(col, pa.scalar(PANDAS_MIN_DATE, type=col_type))
                max_mask = pc.greater_equal(col, pa.scalar(PANDAS_MAX_DATE, type=col_type))
                
                min_count = pc.sum(pc.cast(min_mask, pa.int64())).as_py() or 0
                max_count = pc.sum(pc.cast(max_mask, pa.int64())).as_py() or 0
                
                if min_count > 0 or max_count > 0:
                    sf_min_date = pa.scalar(SNOWFLAKE_MIN_DATE, type=col_type)
                    sf_max_date = pa.scalar(SNOWFLAKE_MAX_DATE, type=col_type)
                    
                    new_col = pc.if_else(min_mask, sf_min_date, col)
                    new_col = pc.if_else(max_mask, sf_max_date, new_col)
                    new_columns[field.name] = (new_col, field)
                    columns_modified.append(f"{field.name} (date: {min_count} min, {max_count} max)")
            except Exception as e:
                LOGGER.warning(f"Could not clamp dates in date column {field.name}: {e}")
    
    if columns_modified:
        LOGGER.info(f"Clamped out-of-bound dates in columns: {columns_modified}")
        for col_name, (new_col, new_field) in new_columns.items():
            col_idx = table.schema.get_field_index(col_name)
            table = table.set_column(col_idx, new_field, new_col)
    
    return table


def process_parquet_dates(bucket, prefix):
    """
    Process parquet files in S3 to clamp out-of-bound dates.
    Modifies files in place before Snowflake reads them.
    """
    s3_path = f's3://{bucket}/{prefix}'
    
    try:
        # List all parquet files
        all_files = s3_file_system.ls(s3_path)
        parquet_files = [f for f in all_files if f.endswith('.parquet')]
        
        if not parquet_files:
            LOGGER.debug(f"No parquet files found in {s3_path}")
            return
        
        LOGGER.info(f"Processing {len(parquet_files)} parquet file(s) for date clamping")
        
        for file_path in parquet_files:
            parquet_path = f's3://{file_path}'
            try:
                # Read parquet as Arrow Table
                table = pq.read_table(parquet_path, filesystem=s3_file_system)
                
                # Clamp dates
                table = clamp_out_of_bound_dates(table)
                
                # Write back to same location
                pq.write_table(table, parquet_path, filesystem=s3_file_system)
                LOGGER.debug(f"Processed dates in: {parquet_path}")
            except Exception as e:
                LOGGER.warning(f"Could not process dates in {parquet_path}: {e}")
    except Exception as e:
        LOGGER.warning(f"Could not process parquet dates in {s3_path}: {e}")


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
    remove_temp_external_stage = False

    try:
        transfer_start_time = time.time()

        # Clamp out-of-bound dates in parquet files before Snowflake reads them
        # This uses PyArrow for efficient columnar processing
        if file_format_type == FileFormatTypes.PARQUET:
            process_parquet_dates(bucket, prefix)

        # check if the schema(Snowflake table) exists and create it if not
        db_sync.create_schema_if_not_exists()
        
        # sync_table will check the schema with the table in Snowflake, if there's a miss matching in columns, raise an error
        db_sync.sync_table()
        
        # generate a new stage with stream in Snowflake
        # the stage will be an external stage that points to the s3
        # the following merge query will be processed directly against the external stage
        stage_generation_query = db_sync.generate_temporary_external_s3_stage(bucket, prefix, config.get('s3_credentials', None), config.get('storage_integration', None))
        
        # after creating the external stage, we could load the file directly from the s3 to Snowflake
        # need to specify the patterns in the s3 bucket to filter out the target csv.gz files
        db_sync.load_file(stage_generation_query)
        
        transfer_end_time = time.time()
        stream = config["stream"]
        LOGGER.info(f"Elapsed time usage for {stream} is {transfer_end_time - transfer_start_time}")
    except snowflake.connector.errors.ProgrammingError as e:
        err_msg = str(e)
        storage_integration = config.get('storage_integration', '').upper()
        if f"Location" in err_msg and "not allowed by integration" in err_msg:
            s3_allowed_location = f"s3://{bucket}/{prefix[:prefix.rfind('/') + 1]}"
            raise SymonException(f'Snowflake storage integration "{storage_integration}" must include "{s3_allowed_location}" in S3_ALLOWED_LOCATIONS.', "snowflake.clientError")
        if f"Insufficient privileges to operate on integration" in err_msg:
            raise SymonException(f'USAGE privilege is missing on storage integration "{storage_integration}".', "snowflake.clientError")
        raise
    except Exception as e:
        LOGGER.error(f"Error occurred in direct_transfer_data_from_s3_to_snowflake: {e}")
        raise e
    finally:
        # remove the schema file from local
        os.remove(LOCAL_SCHEMA_FILE_PATH)
        
        # Snowflake will only remove the external stage object, the s3 bucket and files will remain
        try:
            db_sync.remove_external_s3_stage()
        except Exception as e:
            LOGGER.error(f"Error occurred while removing external stage: {e}")
            pass

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
                error_file_path = config.get('error_file_path', None)
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
