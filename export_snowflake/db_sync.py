import json
import sys
import snowflake.connector
import re
import time
import os

from typing import List, Dict, Union, Tuple, Set
from singer import get_logger
from export_snowflake import flattening
from export_snowflake import stream_utils
from export_snowflake.file_format import FileFormat, FileFormatTypes

from export_snowflake.exceptions import TooManyRecordsException, SymonException
from export_snowflake.upload_clients.s3_upload_client import S3UploadClient
from export_snowflake.upload_clients.snowflake_upload_client import SnowflakeUploadClient


def validate_config(config):
    """Validate configuration"""
    errors = []
    s3_required_config_keys = [
        'account',
        'dbname',
        'user',
        'password',
        'warehouse',
        's3_bucket',
        'stage',
        'file_format'
    ]

    snowflake_required_config_keys = [
        'account',
        'dbname',
        'user',
        'password',
        'warehouse',
        'file_format'
    ]

    required_config_keys = []

    # Use external stages if both s3_bucket and stage defined
    if config.get('s3_bucket', None) and config.get('stage', None):
        required_config_keys = s3_required_config_keys
    # Use table stage if none s3_bucket and stage defined
    elif not config.get('s3_bucket', None) and not config.get('stage', None):
        required_config_keys = snowflake_required_config_keys
    else:
        errors.append("Only one of 's3_bucket' or 'stage' keys defined in config. "
                      "Use both of them if you want to use an external stage when loading data into snowflake "
                      "or don't use any of them if you want ot use table stages.")

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append(f"Required key is missing from config: [{k}]")

    # Check target schema config
    config_default_target_schema = config.get('default_target_schema', None)
    config_schema_mapping = config.get('schema_mapping', None)
    if not config_default_target_schema and not config_schema_mapping:
        errors.append("Neither 'default_target_schema' (string) nor 'schema_mapping' (object) keys set in config.")

    # Check if archive load files option is using external stages
    archive_load_files = config.get('archive_load_files', False)
    if archive_load_files and not config.get('s3_bucket', None):
        errors.append('Archive load files option can be used only with external s3 stages. Please define s3_bucket.')

    return errors


def column_type(schema_property):
    """Take a specific schema property and return the snowflake equivalent column type"""
    property_type = schema_property['type']
    property_format = schema_property['format'] if 'format' in schema_property else None
    col_type = 'text'
    if 'object' in property_type or 'array' in property_type:
        col_type = 'variant'

    elif property_format == 'date':
        col_type = 'date'
    elif property_format == 'time':
        col_type = 'time'
    elif property_format == 'binary':
        col_type = 'binary'
    elif 'number' in property_type:
        col_type = 'float'
    elif 'integer' in property_type and 'string' in property_type:
        col_type = 'text'
    elif 'integer' in property_type:
        col_type = 'number'
    elif 'boolean' in property_type:
        col_type = 'boolean'
    elif 'date-time' in property_type:
        # Every date-time JSON value is currently mapped to TIMESTAMP_NTZ
        col_type = 'timestamp_ntz'

    return col_type


def column_trans(schema_property):
    """Generate SQL transformed columns syntax"""
    property_type = schema_property['type']
    col_trans = ''
    if 'object' in property_type or 'array' in property_type:
        col_trans = 'parse_json'
    elif schema_property.get('format') == 'binary':
        col_trans = 'to_binary'

    return col_trans


def safe_column_name(name):
    """Generate SQL friendly column name"""
    return f'"{name}"'.upper()


def json_element_name(name):
    """Generate SQL friendly semi structured element reference name"""
    return f'"{name}"'


def column_clause(name, schema_property):
    """Generate DDL column name with column type string"""
    return f'{safe_column_name(name)} {column_type(schema_property)}'


def primary_column_names(stream_schema_message):
    """Generate list of SQL friendly PK column names"""
    return [safe_column_name(p) for p in stream_schema_message['key_properties']]


# pylint: disable=invalid-name
def create_query_tag(query_tag_pattern: str, database: str = None, schema: str = None, table: str = None) -> str:
    """
    Generate a string to tag executed queries in Snowflake.
    Replaces tokens `schema` and `table` with the appropriate values.

    Example with tokens:
        'Loading data into {schema}.{table}'

    Args:
        query_tag_pattern:
        database: optional value to replace {{database}} token in query_tag_pattern
        schema: optional value to replace {{schema}} token in query_tag_pattern
        table: optional value to replace {{table}} token in query_tag_pattern

    Returns:
        String if query_tag_patter defined otherwise None
    """
    if not query_tag_pattern:
        return None

    query_tag = query_tag_pattern

    # replace tokens, taking care of json formatted value compatibility
    for k, v in {
        '{{database}}': json.dumps(database.strip('"')).strip('"') if database else None,
        '{{schema}}': json.dumps(schema.strip('"')).strip('"') if schema else None,
        '{{table}}': json.dumps(table.strip('"')).strip('"') if table else None
    }.items():
        if k in query_tag:
            query_tag = query_tag.replace(k, v or '')

    return query_tag


# pylint: disable=too-many-public-methods,too-many-instance-attributes
class DbSync:
    """DbSync class"""

    def __init__(self, connection_config, stream_schema_message=None, table_cache=None, file_format_type=None):
        """
            connection_config:      Snowflake connection details

            stream_schema_message:  An instance of the DbSync class is typically used to load
                                    data only from a certain singer tap stream.

                                    The stream_schema_message holds the destination schema
                                    name and the JSON schema that will be used to
                                    validate every RECORDS messages that comes from the stream.
                                    Schema validation happening before creating CSV and before
                                    uploading data into Snowflake.

                                    If stream_schema_message is not defined that we can use
                                    the DbSync instance as a generic purpose connection to
                                    Snowflake and can run individual queries. For example
                                    collecting catalog informations from Snowflake for caching
                                    purposes.
        """
        self.connection_config = connection_config
        self.stream_schema_message = stream_schema_message
        self.table_cache = table_cache

        # logger to be used across the class's methods
        self.logger = get_logger('export_snowflake')

        # Validate connection configuration
        config_errors = validate_config(connection_config)

        # Exit if config has errors
        if len(config_errors) > 0:
            self.logger.error('Invalid configuration:\n   * %s', '\n   * '.join(config_errors))
            sys.exit(1)

        if self.connection_config.get('stage', None):
            stage = stream_utils.stream_name_to_dict(self.connection_config['stage'], separator='.')
            if not stage['schema_name']:
                self.logger.error(
                    "The named external stage object in config has to use the <schema>.<stage_name> format.")
                sys.exit(1)

        self.schema_name = None
        self.grantees = None
        self.file_format = FileFormat(self.connection_config['file_format'], self.query, file_format_type)

        if not self.connection_config.get('stage') and self.file_format.file_format_type == FileFormatTypes.PARQUET:
            self.logger.error("Table stages with Parquet file format is not supported. "
                              "Use named stages with Parquet file format or table stages with CSV files format")
            sys.exit(1)

        # Init stream schema pylint: disable=line-too-long
        if self.stream_schema_message is not None:
            #  Define target schema name.
            #  --------------------------
            #  Target schema name can be defined in multiple ways:
            #
            #   1: 'default_target_schema' key  : Target schema is the same for every incoming stream if
            #                                     not specified explicitly for a given stream in
            #                                     the `schema_mapping` object
            #   2: 'schema_mapping' key         : Target schema defined explicitly for a given stream.
            #                                     Example config.json:
            #                                           "schema_mapping": {
            #                                               "my_tap_stream_id": {
            #                                                   "target_schema": "my_snowflake_schema",
            #                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                               }
            #                                           }
            config_default_target_schema = self.connection_config.get('default_target_schema', '').strip()
            config_schema_mapping = self.connection_config.get('schema_mapping', {})

            stream_name = stream_schema_message['stream']
            stream_schema_name = stream_utils.stream_name_to_dict(stream_name)['schema_name']
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.schema_name = config_schema_mapping[stream_schema_name].get('target_schema')
            elif config_default_target_schema:
                self.schema_name = config_default_target_schema

            if not self.schema_name:
                raise Exception(
                    "Target schema name not defined in config. "
                    "Neither 'default_target_schema' (string) nor 'schema_mapping' (object) defines "
                    f"target schema for {stream_name} stream.")

            #  Define grantees
            #  ---------------
            #  Grantees can be defined in multiple ways:
            #
            #   1: 'default_target_schema_select_permissions' key  : USAGE and SELECT privileges will be granted on every table to a given role
            #                                                       for every incoming stream if not specified explicitly
            #                                                       in the `schema_mapping` object
            #   2: 'target_schema_select_permissions' key          : Roles to grant USAGE and SELECT privileges defined explicitly
            #                                                       for a given stream.
            #                                                       Example config.json:
            #                                                           "schema_mapping": {
            #                                                               "my_tap_stream_id": {
            #                                                                   "target_schema": "my_snowflake_schema",
            #                                                                   "target_schema_select_permissions": [ "role_with_select_privs" ]
            #                                                               }
            #                                                           }
            self.grantees = self.connection_config.get('default_target_schema_select_permissions')
            if config_schema_mapping and stream_schema_name in config_schema_mapping:
                self.grantees = config_schema_mapping[stream_schema_name].get('target_schema_select_permissions',
                                                                              self.grantees)

            self.flatten_schema = flattening.flatten_schema(stream_schema_message['schema'])

        # Use external stage
        if connection_config.get('s3_bucket', None):
            self.upload_client = S3UploadClient(connection_config)
        # Use table stage
        else:
            self.upload_client = SnowflakeUploadClient(connection_config, self)

    def open_connection(self):
        """Open snowflake connection"""
        stream = None
        if self.stream_schema_message:
            stream = self.stream_schema_message['stream']
        
        try:
            return snowflake.connector.connect(
                user=self.connection_config['user'],
                password=self.connection_config['password'],
                account=self.connection_config['account'],
                database=self.connection_config['dbname'],
                warehouse=self.connection_config['warehouse'],
                role=self.connection_config.get('role', None),
                autocommit=True,
                session_parameters={
                    # Quoted identifiers should be case sensitive
                    'QUOTED_IDENTIFIERS_IGNORE_CASE': 'FALSE',
                    'QUERY_TAG': create_query_tag(self.connection_config.get('query_tag'),
                                                database=self.connection_config['dbname'],
                                                schema=self.schema_name,
                                                table=self.table_name(stream, False, True))
                }
            )
        except snowflake.connector.errors.DatabaseError as e:
            if 'Incorrect username or password was specified' in str(e):
                raise SymonException("The username or password provided is incorrect. Please check and try again.", "snowflake.clientError")
            raise
        except snowflake.connector.errors.ForbiddenError as e:
            if 'Failed to connect to DB. Verify the account name is correct' in str(e):
                raise SymonException("Sorry, we couldn't connect to the database. Please check the Snowflake URL and try again.", "snowflake.clientError")
            raise

    def query(self, query: Union[str, List[str]], params: Dict = None, max_records=0) -> List[Dict]:
        """Run an SQL query in snowflake"""
        result = []

        if params is None:
            params = {}
        else:
            if 'LAST_QID' in params:
                self.logger.warning('LAST_QID is a reserved prepared statement parameter name, '
                                    'it will be overridden with each executed query!')

        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:

                # Run every query in one transaction if query is a list of SQL
                if isinstance(query, list):
                    self.logger.debug('Starting Transaction')
                    cur.execute("START TRANSACTION")
                    queries = query
                else:
                    queries = [query]

                qid = None

                # pylint: disable=invalid-name
                for q in queries:

                    # update the LAST_QID
                    params['LAST_QID'] = qid

                    self.logger.debug("Running query: '%s' with Params %s", q, params)
                    try:
                        cur.execute(q, params)
                    except snowflake.connector.errors.ProgrammingError as e:
                        message = str(e)
                        if 'No active warehouse selected in the current session' in message:
                            raise SymonException(f'The warehouse provided is incorrect. Please ensure it is correct and you are authorized to access it.', 'snowflake.clientError')
                        if 'This session does not have a current database' in message:
                            raise SymonException(f'The database provided is incorrect. Please ensure it is correct and you are authorized to access it.', 'snowflake.clientError')
                        raise

                    qid = cur.sfqid

                    # Raise exception if returned rows greater than max allowed records
                    if 0 < max_records < cur.rowcount:
                        raise TooManyRecordsException(
                            f"Query returned too many records. This query can return max {max_records} records")

                    result = cur.fetchall()

        return result

    def table_name(self, stream_name, is_temporary, without_schema=False):
        """Generate target table name"""
        if not stream_name:
            return None

        stream_dict = stream_utils.stream_name_to_dict(stream_name)
        table_name = stream_dict['table_name']
        sf_table_name = table_name.replace('.', '_').replace('-', '_').lower()

        if is_temporary:
            sf_table_name = f'{sf_table_name}_temp'

        if without_schema:
            return f'"{sf_table_name.upper()}"'

        return f'{self.schema_name}."{sf_table_name.upper()}"'

    def record_primary_key_string(self, record):
        """Generate a unique PK string in the record"""
        if len(self.stream_schema_message['key_properties']) == 0:
            return None
        # Symon: records are not nested, no need to flatten
        # flatten = flattening.flatten_record(record, self.flatten_schema, max_level=self.data_flattening_max_level)

        # Symon: replaced variable flatten with record in lines below
        key_props = []
        for key_prop in self.stream_schema_message['key_properties']:
            if key_prop not in record or record[key_prop] is None:
                raise SymonException(
                    f'Primary key "{key_prop}" does not exist in record or is null. '
                    f"Available fields: {list(record.keys())}",
                    "snowflake.clientError"
                )

            key_props.append(str(record[key_prop]))

        return ','.join(key_props)

    def put_to_stage(self, file, stream, temp_dir=None):
        """Upload file to snowflake stage"""
        return self.upload_client.upload_file(file, stream, temp_dir)

    def delete_from_stage(self, stream, s3_key):
        """Delete file from snowflake stage"""
        self.upload_client.delete_object(stream, s3_key)

    def copy_to_archive(self, s3_source_key, s3_archive_key, s3_archive_metadata):
        """
        Copy file from snowflake stage to archive.

        s3_source_key: The s3 key to copy, assumed to exist in the bucket configured as 's3_bucket'

        s3_archive_key: The key to use in archive destination. This will be prefixed with the config value
                        'archive_load_files_s3_prefix'. If none is specified, 'archive' will be used as the prefix.

                        As destination bucket, the config value 'archive_load_files_s3_bucket' will be used. If none is
                        specified, the bucket configured as 's3_bucket' will be used.

        s3_archive_metadata: This dict will be merged with any metadata in the source file.

        """
        source_bucket = self.connection_config.get('s3_bucket')

        # Get archive s3_bucket from config, or use same bucket if not specified
        archive_bucket = self.connection_config.get('archive_load_files_s3_bucket', source_bucket)

        # Determine prefix to use in archive s3 bucket
        default_archive_prefix = 'archive'
        archive_prefix = self.connection_config.get('archive_load_files_s3_prefix', default_archive_prefix)
        prefixed_archive_key = f'{archive_prefix}/{s3_archive_key}'

        copy_source = f'{source_bucket}/{s3_source_key}'

        self.logger.info('Copying %s to archive location %s', copy_source, prefixed_archive_key)
        self.upload_client.copy_object(copy_source, archive_bucket, prefixed_archive_key, s3_archive_metadata)

    def get_stage_name(self, stream):
        """Generate snowflake stage name"""
        stage = self.connection_config.get('stage', None)
        if stage:
            return stage

        table_name = self.table_name(stream, False, without_schema=True)
        return f"{self.schema_name}.%{table_name}"

    def load_file(self, stage_generation_query):
        """Load a supported file type from snowflake stage into target table"""
        stream = self.stream_schema_message['stream']

        # Get list if columns with types
        columns_with_trans = [
            {
                "name": safe_column_name(name),
                "json_element_name": json_element_name(name),
                "trans": column_trans(schema)
            }
            for (name, schema) in self.flatten_schema.items()
        ]

        inserts = 0
        updates = 0

        # Insert or Update with MERGE command if primary key defined
        if len(self.stream_schema_message['key_properties']) > 0:
            try:
                inserts, updates = self._load_file_merge(
                    stream=stream,
                    columns_with_trans=columns_with_trans,
                    stage_generation_query=stage_generation_query
                )
            except Exception as ex:
                self.logger.error(
                    'Error while executing MERGE query for table "%s" in stream "%s"',
                    self.table_name(stream, False), stream
                )
                raise ex

        # Insert only with COPY command if no primary key
        # we will never execute COPY command as KEY columns is always mandatory
        else:
            try:
                inserts, updates = (
                    self._load_file_copy(
                        stream=stream,
                        columns_with_trans=columns_with_trans
                    ),
                    0,
                )
            except Exception as ex:
                self.logger.error(
                    'Error while executing COPY query for table "%s" in stream "%s"',
                    self.table_name(stream, False), stream
                )
                raise ex

        self.logger.info(
            'Loading into %s: %s',
            self.table_name(stream, False),
            json.dumps({'inserts': inserts, 'updates': updates})
        )

    def use_default_schema(self):
        return f"USE SCHEMA {self.schema_name}"

    def get_export_task_id(self):
        prefix = self.connection_config.get('prefix')
        # result is the export task id from the prefix
        # id is generated from base64url, so might contain '-' characters, replace it with '_'
        result = os.path.basename(prefix).replace('-', '_')
        
        return result
    
    def get_temporary_stage_name(self):
        """Generate snowflake stage name"""
        export_task_id = self.get_export_task_id()
        
        return f"export_{export_task_id}_stage"
    
    def generate_temporary_external_s3_stage(self, bucket, prefix, s3_credentials):
        temp_stage_name = self.get_temporary_stage_name()
        destination_url = f"s3://{bucket}/{prefix}"
        

        stage_generation_query = self.file_format.formatter.create_stage_generation_sql(
            stage_name=temp_stage_name,
            url=destination_url,
            aws_key_id=s3_credentials['accessKeyID'],
            aws_secret_key=s3_credentials['secretKey'],
            aws_session_token=s3_credentials.get('sessionToken', None),
            file_format_name=self.connection_config['file_format']
        )
                
        # temporary stage will only exist during the connection cursor session
        # only execute the generation query later during the data loading session
        return stage_generation_query

    def remove_external_s3_stage(self):
        temp_stage_name = self.get_temporary_stage_name()
        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                stage_removal_query = f"DROP STAGE IF EXISTS {temp_stage_name}"
                
                # need to point to the correct schema before creating the stage
                default_schema_query = self.use_default_schema()
                cur.execute(default_schema_query)

                self.logger.debug('Removing temporary external stage: %s', stage_removal_query)
                cur.execute(stage_removal_query)
        return
    
    def _load_file_merge(self, stream, columns_with_trans, stage_generation_query) -> Tuple[int, int]:
        # MERGE does insert and update
        inserts = 0
        updates = 0
        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                merge_sql = self.file_format.formatter.create_merge_sql(
                    table_name=self.table_name(stream, False),
                    stage_name=self.get_temporary_stage_name(),
                    file_format_name=self.connection_config['file_format'],
                    columns=columns_with_trans,
                    pk_merge_condition=self.primary_key_merge_condition()
                )
                
                # need to point to the correct schema before creating the stage
                default_schema_query = self.use_default_schema()
                cur.execute(default_schema_query)
                
                # execute the temporary stage generation query
                self.logger.debug('Temporary external stage generation query: %s', stage_generation_query)
                cur.execute(stage_generation_query)
                
                self.logger.debug('Running query: %s', merge_sql)
                cur.execute(merge_sql)
                # Get number of inserted and updated records
                results = cur.fetchall()
                if len(results) > 0:
                    inserts = results[0].get('number of rows inserted', 0)
                    updates = results[0].get('number of rows updated', 0)
        return inserts, updates

    def _load_file_copy(self, stream, columns_with_trans) -> int:
        # COPY does insert only
        inserts = 0
        with self.open_connection() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                copy_sql = self.file_format.formatter.create_copy_sql(
                    table_name=self.table_name(stream, False),
                    stage_name=self.get_stage_name(stream),
                    file_format_name=self.connection_config['file_format'],
                    columns=columns_with_trans
                )
                self.logger.debug('Running query: %s', copy_sql)
                cur.execute(copy_sql)
                # Get number of inserted records - COPY does insert only
                results = cur.fetchall()
                if len(results) > 0:
                    inserts = results[0].get('rows_loaded', 0)
        return inserts

    def primary_key_merge_condition(self):
        """Generate SQL join condition on primary keys for merge SQL statements"""
        stream_schema_message = self.stream_schema_message
        names = primary_column_names(stream_schema_message)
        return ' AND '.join([f's.{c} = t.{c}' for c in names])

    def column_names(self):
        """Get list of columns in the schema"""
        return [safe_column_name(name) for name in self.flatten_schema]

    def create_table_query(self, is_temporary=False):
        """Generate CREATE TABLE SQL"""
        stream_schema_message = self.stream_schema_message
        columns = [
            column_clause(
                name,
                schema
            )
            for (name, schema) in self.flatten_schema.items()
        ]

        primary_key = []
        if len(stream_schema_message.get('key_properties', [])) > 0:
            pk_list = ', '.join(primary_column_names(stream_schema_message))
            primary_key = [f"PRIMARY KEY({pk_list})"]

        p_temp = 'TEMP ' if is_temporary else ''
        p_table_name = self.table_name(stream_schema_message['stream'], is_temporary)
        p_columns = ', '.join(columns + primary_key)
        p_extra = 'data_retention_time_in_days = 0 ' if is_temporary else 'data_retention_time_in_days = 1 '
        return f'CREATE {p_temp}TABLE IF NOT EXISTS {p_table_name} ({p_columns}) {p_extra}'

    def grant_usage_on_schema(self, schema_name, grantee):
        """Grant usage on schema"""
        query = f"GRANT USAGE ON SCHEMA {schema_name} TO ROLE {grantee}"
        self.logger.info("Granting USAGE privilege on '%s' schema to '%s'... %s", schema_name, grantee, query)
        self.query(query)

    # pylint: disable=invalid-name
    def grant_select_on_all_tables_in_schema(self, schema_name, grantee):
        """Grant select on all tables in schema"""
        query = f"GRANT SELECT ON ALL TABLES IN SCHEMA {schema_name} TO ROLE {grantee}"
        self.logger.info(
            "Granting SELECT ON ALL TABLES privilege on '%s' schema to '%s'... %s", schema_name, grantee, query)
        self.query(query)

    @classmethod
    def grant_privilege(cls, schema, grantees, grant_method):
        """Grant privileges on target schema"""
        if isinstance(grantees, list):
            for grantee in grantees:
                grant_method(schema, grantee)
        elif isinstance(grantees, str):
            grant_method(schema, grantees)

    def delete_rows(self, stream):
        """Hard delete rows from target table"""
        table = self.table_name(stream, False)
        query = f"DELETE FROM {table} WHERE _sdc_deleted_at IS NOT NULL"
        self.logger.info("Deleting rows from '%s' table... %s", table, query)
        self.logger.info('DELETE %d', len(self.query(query)))

    def create_schema_if_not_exists(self):
        """Create target schema if not exists"""
        schema_name = self.schema_name
        schema_rows = 0

        # table_cache is an optional pre-collected list of available objects in snowflake
        if self.table_cache:
            schema_rows = list(filter(lambda x: x['SCHEMA_NAME'] == schema_name.upper(), self.table_cache))
        # Query realtime if not pre-collected
        else:
            schema_rows = self.query(f"SHOW SCHEMAS LIKE '{schema_name.upper()}'")

        if len(schema_rows) == 0:
            try:
                query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
                self.logger.info("Schema '%s' does not exist. Creating... %s", schema_name, query)
                self.query(query)
            except snowflake.connector.errors.ProgrammingError as e:
                dbname_upper = self.connection_config['dbname'].upper()
                schema_name_upper = schema_name.upper()
                message = str(e)
                # No privilege to create schema. However, this error could be raised if user doesn't have usage privilege on the existing schema 
                # as the schema will not be returned SHOW query. 
                if 'Insufficient privileges to operate on database' in message:
                    raise SymonException(f'CREATE SCHEMA privilege on database "{dbname_upper}" is missing. If the schema "{schema_name_upper}" already exists in the database "{dbname_upper}", please ensure you have USAGE privilege on the schema.', "snowflake.clientError")
                if f"Schema '{schema_name.upper()}' already exists, but current role has no privileges on it" in message:
                    raise SymonException(f'USAGE privilege on schema "{schema_name.upper()}" is missing.', "snowflake.clientError")
                raise

            self.grant_privilege(schema_name, self.grantees, self.grant_usage_on_schema)

            # Refresh columns cache if required
            if self.table_cache:
                self.table_cache = self.get_table_columns(table_schemas=[self.schema_name])

    def get_tables(self, table_schemas=None):
        """Get list of tables of certain schema(s) from snowflake metadata"""
        tables = []
        if table_schemas:
            for schema in table_schemas:
                queries = []

                # Get tables in schema
                show_tables = f"SHOW TERSE TABLES IN SCHEMA {self.connection_config['dbname']}.{schema}"

                # Convert output of SHOW TABLES to table
                select = """
                    SELECT "schema_name" AS schema_name
                          ,"name"        AS table_name
                      FROM TABLE(RESULT_SCAN(%(LAST_QID)s))
                """
                queries.extend([show_tables, select])

                # Run everything in one transaction
                try:
                    tables = self.query(queries, max_records=99999)

                # Catch exception when schema not exists and SHOW TABLES throws a ProgrammingError
                # Regexp to extract snowflake error code and message from the exception message
                # Do nothing if schema not exists
                except snowflake.connector.errors.ProgrammingError as exc:
                    if not re.match(r'002043 \(02000\):.*\n.*does not exist.*', str(sys.exc_info()[1])):
                        raise exc
        else:
            raise Exception("Cannot get table columns. List of table schemas empty")

        return tables

    def get_table_columns(self, table_schemas=None):
        """Get list of columns and tables of certain schema(s) from snowflake metadata"""
        table_columns = []
        if table_schemas:
            for schema in table_schemas:
                queries = []

                # Get column data types by SHOW COLUMNS
                show_columns = f"SHOW COLUMNS IN SCHEMA {self.connection_config['dbname']}.{schema}"

                # Convert output of SHOW COLUMNS to table and insert results into the cache COLUMNS table
                #
                # ----------------------------------------------------------------------------------------
                # Character and numeric columns display their generic data type rather than their defined
                # data type (i.e. TEXT for all character types, FIXED for all fixed-point numeric types,
                # and REAL for all floating-point numeric types).
                # Further info at https://docs.snowflake.net/manuals/sql-reference/sql/show-columns.html
                # ----------------------------------------------------------------------------------------
                select = """
                    SELECT "schema_name" AS schema_name
                          ,"table_name"  AS table_name
                          ,"column_name" AS column_name
                          ,CASE PARSE_JSON("data_type"):type::varchar
                             WHEN 'FIXED' THEN 'NUMBER'
                             WHEN 'REAL'  THEN 'FLOAT'
                             ELSE PARSE_JSON("data_type"):type::varchar
                           END data_type
                      FROM TABLE(RESULT_SCAN(%(LAST_QID)s))
                """

                queries.extend([show_columns, select])

                # Run everything in one transaction
                try:
                    columns = self.query(queries, max_records=99999)

                    if not columns:
                        self.logger.warning('No columns discovered in the schema "%s"',
                                            f"{self.connection_config['dbname']}.{schema}")
                    else:
                        table_columns.extend(columns)

                # Catch exception when schema not exists and SHOW COLUMNS throws a ProgrammingError
                # Regexp to extract snowflake error code and message from the exception message
                # Do nothing if schema not exists
                except snowflake.connector.errors.ProgrammingError as exc:
                    if not re.match(r'002003 \(02000\):.*\n.*does not exist or not authorized.*',
                                    str(sys.exc_info()[1])):
                        raise exc

        else:
            raise Exception("Cannot get table columns. List of table schemas empty")

        return table_columns

    def refresh_table_cache(self):
        """Refreshes the internal table cache"""
        self.table_cache = self.get_table_columns([self.schema_name])

    def validate_columns(self):
        """Validates the columns in stream to match the columns in the target table according to the schema"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name = self.table_name(stream, False, True)
        all_table_columns = []

        if self.table_cache:
            all_table_columns = self.table_cache
        else:
            all_table_columns = self.get_table_columns(table_schemas=[self.schema_name])

        # Find the specific table
        columns = list(filter(lambda x: x['SCHEMA_NAME'] == self.schema_name.upper() and
                                        f'"{x["TABLE_NAME"].upper()}"' == table_name,
                              all_table_columns))

        columns_dict = {column['COLUMN_NAME'].upper(): column for column in columns}

        # check if column data type matches between exporting data and target table
        columns_with_type_mismatch = [
            name.upper()
            for (name, properties_schema) in self.flatten_schema.items()
            if name.upper() in columns_dict and
               columns_dict[name.upper()]['DATA_TYPE'].upper() != column_type(properties_schema).upper() and
               # Allow Symon date type to be compatible to any date, timestamp type in snowflake     
               not (columns_dict[name.upper()]['DATA_TYPE'].upper() in ['DATE', 'TIMESTAMP', 'TIMESTAMP_TZ', 'TIMESTAMP_LTZ'] and column_type(properties_schema).upper() == 'TIMESTAMP_NTZ') and
               # Allow Symon number type to be compatible with any number type in snowflake
               not (columns_dict[name.upper()]['DATA_TYPE'].upper() == 'NUMBER' and column_type(properties_schema).upper() == 'FLOAT')
        ]
        if len(columns_with_type_mismatch) > 0:
            raise SymonException(f'Exported data includes columns that have different data types in the Snowflake table {table_name}: {columns_with_type_mismatch}', 'snowflake.clientError')

        stream_columns = [column.upper() for column in self.flatten_schema]

        columns_extra = [column for column in stream_columns if column not in columns_dict]
        if len(columns_extra) > 0:
            raise SymonException(f'Exported data includes columns that are not present in the Snowflake table {table_name}: {columns_extra}', 'snowflake.clientError')
        
        columns_missing = [column for column in columns_dict if column not in stream_columns]
        if len(columns_missing) > 0:
            raise SymonException(f'Exported data is missing columns that are present in the Snowflake table {table_name}: {columns_missing}', 'snowflake.clientError')


    def drop_column(self, column_name, stream):
        """Drops column from an existing table"""
        drop_column = f"ALTER TABLE {self.table_name(stream, False)} DROP COLUMN {column_name}"
        self.logger.info('Dropping column: %s', drop_column)
        self.query(drop_column)

    def version_column(self, column_name, stream):
        """Versions a column in an existing table"""
        p_table_name = self.table_name(stream, False)
        p_column_name = column_name.replace("\"", "")
        p_ver_time = time.strftime("%Y%m%d_%H%M")

        version_column = f"ALTER TABLE {p_table_name} RENAME COLUMN {column_name} TO \"{p_column_name}_{p_ver_time}\""
        self.logger.info('Versioning column: %s', version_column)
        self.query(version_column)

    def add_column(self, column, stream):
        """Adds a new column to an existing table"""
        add_column = f"ALTER TABLE {self.table_name(stream, False)} ADD COLUMN {column}"
        self.logger.info('Adding column: %s', add_column)
        self.query(add_column)

    def sync_table(self):
        """Creates or alters the target table according to the schema"""
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message['stream']
        table_name_upper = self.table_name(stream, False, True)
        table_name_with_schema = self.table_name(stream, False)
        schema_name_upper = self.schema_name.upper()

        if self.table_cache:
            found_tables = list(filter(lambda x: x['SCHEMA_NAME'] == schema_name_upper and
                                                 f'"{x["TABLE_NAME"].upper()}"' == table_name_upper,
                                       self.table_cache))
        else:
            found_tables = [table for table in (self.get_tables([schema_name_upper]))
                            if f'"{table["TABLE_NAME"].upper()}"' == table_name_upper]

        if len(found_tables) == 0:
            try:
                query = self.create_table_query()
                self.logger.info('Table %s does not exist. Creating...', table_name_with_schema)
                self.logger.debug('Create table with query %s', query)
                self.query(query)
            except snowflake.connector.errors.ProgrammingError as e:
                # No privilege to create table. However, this error could be raised if user doesn't have select or ownership privilege on the existing table
                # as the table will not be returned from get_tables call. We need ownership privilege on updating existing table.
                message = str(e)
                if 'Insufficient privileges to operate on schema' in message:
                    raise SymonException(f'CREATE TABLE privilege on schema "{schema_name_upper}" is missing. If the table {table_name_upper} already exists in the schema "{schema_name_upper}", please ensure you have OWNERSHIP privilege on the table.', "snowflake.clientError")
                # if table exists, ownership privilege is needed on table as we use internal table stage and perform ddl
                if f"Table '{table_name_upper[1:-1]}' already exists, but current role has no privileges on it" in message:
                    raise SymonException(f'OWNERSHIP privilege on table {table_name_upper} is missing.', "snowflake.clientError")
                raise
                
            self.grant_privilege(self.schema_name, self.grantees, self.grant_select_on_all_tables_in_schema)

            # Refresh columns cache if required
            if self.table_cache:
                self.table_cache = self.get_table_columns(table_schemas=[self.schema_name])
        else:
            self.logger.info('Table %s exists', table_name_with_schema)
            try:
                self.validate_columns()
            except snowflake.connector.errors.ProgrammingError as e:
                if f"Insufficient privileges to operate on table" in str(e):
                    raise SymonException(f'OWNERSHIP privilege on table {table_name_upper} is missing.', "snowflake.clientError")
                raise

        self._refresh_table_pks()

    def _refresh_table_pks(self):
        """
        Refresh table PK constraints by either dropping or adding PK based on changes to `key_properties` of the
        stream schema.
        The non-nullability of PK column is also dropped.
        """
        table_name = self.table_name(self.stream_schema_message['stream'], False)
        current_pks = self._get_current_pks()
        new_pks = set(pk.upper() for pk in self.stream_schema_message.get('key_properties', []))

        queries = []

        self.logger.debug('Table: %s, Current PKs: %s | New PKs: %s ',
                          self.stream_schema_message['stream'],
                          current_pks,
                          new_pks
                          )

        if not new_pks and current_pks:
            self.logger.info('Table "%s" currently has PK constraint, but we need to drop it.', table_name)
            queries.append(f'alter table {table_name} drop primary key;')

        elif new_pks != current_pks:
            self.logger.info('Changes detected in pk columns of table "%s", need to refresh PK.', table_name)
            pk_list = ', '.join([safe_column_name(col) for col in new_pks])

            if current_pks:
                queries.append(f'alter table {table_name} drop primary key;')

            queries.append(f'alter table {table_name} add primary key({pk_list});')

        # For now, we don't wish to enforce non-nullability on the pk columns
        for pk in current_pks.union(new_pks):
            queries.append(f'alter table {table_name} alter column {safe_column_name(pk)} drop not null;')

        try:
            self.query(queries)
        except snowflake.connector.errors.ProgrammingError as e:
            if 'Insufficient privileges to operate on table' in str(e):
                table_name_quote_removed = table_name.split('.')[1][1:-1]
                # ownership privilege required for ddl
                raise SymonException(f'OWNERSHIP privilege on table "{table_name_quote_removed.upper()}" is missing.', "snowflake.clientError")
            raise e

    def _get_current_pks(self) -> Set[str]:
        """
        Finds the stream's current Pk in Snowflake.
        Returns: Set of pk columns, in upper case. Empty means table has no PK
        """
        table_name = self.table_name(self.stream_schema_message['stream'], False)

        show_query = f"show primary keys in table {self.connection_config['dbname']}.{table_name};"

        columns = set()
        try:
            columns = self.query(show_query)

        # Catch exception when schema not exists and SHOW TABLES throws a ProgrammingError
        # Regexp to extract snowflake error code and message from the exception message
        # Do nothing if schema not exists
        except snowflake.connector.errors.ProgrammingError as exc:
            if not re.match(r'002043 \(02000\):.*\n.*does not exist.*', str(sys.exc_info()[1])):
                raise exc

        return set(col['column_name'] for col in columns)
