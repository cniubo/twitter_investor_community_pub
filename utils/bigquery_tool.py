from google.cloud import bigquery
from google.oauth2 import service_account
import platform
import os
import yaml
import logging


#logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
SO = platform.system()


def _client_bq(credentials, scopes):
    credentials = service_account.Credentials.from_service_account_file(credentials=credentials, scopes=scopes)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    logging.info('Client created.')

    return client


def _load_schema(spath=None, db=None, table=None, so=SO):

    if so == 'Windows':
        schema_path = os.path.join(spath + "\\" + db.title() + "\\" + table + ".yml")
    elif so == 'Linux':
        schema_path = os.path.join(spath + "/" + db.title() + "/" + table + ".yml")

    with open(schema_path) as file:
        body = yaml.load(file, Loader=yaml.FullLoader)

    schema = []
    for key in body[0]:
        schema_element = bigquery.SchemaField(key, body[0][key].upper())
        schema.append(schema_element)

    return schema


def send_query_bq(qpath=None, location=None, db=None, query_string=None, query_name=None, serverdate=None, so=SO):
    client = _client_bq()
    # given query
    if query_string is None:
        logging.warning("Query not informed. Let's check out the indicated file.")
        if so == 'Windows':
            query_path = os.path.join(qpath + "\\" + db + "\\" + query_name + ".sql")
        elif so == 'Linux':
            query_path = os.path.join(qpath + "/" + db + "/" + query_name + ".sql")
        query = open(query_path, 'r').read().replace('serverdate', serverdate)
    else:
        # given details query path
        query = query_string

    logging.info(query)
    query_job = client.query(query, location=location)
    df = query_job.to_dataframe()

    return df


def delete_data_bq(df, field, table, datefield, serverdate):
    logging.warning('Data is about to be deleted...')
    if field is not None:
        if type(df[field].values[0]) not in ('int', 'float', 'str'):
            unwanted = str(list(set(df[field].astype('str').values))).replace('[', '(').replace(']', ')')
        else:
            unwanted = str(list(set(df[field].values))).replace('[', '(').replace(']', ')')

    if datefield is None:
        query = '''DELETE `{}` WHERE {} in {}'''.format(table, field, unwanted)
    elif field is None:
        query = '''DELETE `{}` WHERE cast({} as date) = date "{}"'''.format(table, datefield, serverdate)
    else:
        query = '''DELETE `{}` WHERE {} in {} and cast({} as date) = date "{}"'''.format(table, field, unwanted,
                                                                                         datefield, serverdate)
    logging.info(query)

    send_query_bq(query_string=query)


def load_data_bq(load_schema=True, spath=None, env='lab', project=None, db=None, table=None, df=None, update_field=None,
                 update_datefield=None, serverdate=None):

    client = _client_bq()
    # tables from datalake in bigQuery do not follow the standard pattern (legacy) so the next block is required

    table_id = (project + "." + env + '_' + db + "." + table).lower()

    if load_schema is True:
        schema = _load_schema(spath=spath, db=db, table=table)

        job_config = bigquery.LoadJobConfig(
            schema=schema
            # BigQuery appends loaded rows to an existing table by default, but with WRITE_TRUNCATE write
            # disposition it replaces the table with the loaded data.
            # write_disposition="WRITE_TRUNCATE",
        )

    else:
        job_config = bigquery.LoadJobConfig()

    # delete repeated data if required
    if (update_field is not None) or (update_datefield is not None):
        logging.warning("Deleting repeated data before loading the df.")
        delete_data_bq(df=df, field=update_field, datefield=update_datefield, table=table_id, serverdate=serverdate)
    # loading df to bigquery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)  # Make an API request.

    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    logging.info("Loaded {} rows and {} columns to {}".format(table.num_rows, len(table.schema), table_id))