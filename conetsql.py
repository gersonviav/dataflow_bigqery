import logging
import argparse
import pyodbc
from apache_beam import Create, DoFn, Pipeline,ParDo
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


def get_table_names(element, db_host, db_name, db_user, db_password):
    # Conexión a la base de datos
    connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};SERVER={db_host};DATABASE={db_name};UID={db_user};PWD={db_password};"
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Consulta para obtener las tablas
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' and TABLE_NAME  in ('LMS_PROGRAMACION_ACTIVIDAD','LMS_PROGRAMACION_PROGRAMA','LMS_PROGRAMA','LMS_ACTIVIDAD')")
    tables = cursor.fetchall()

    cursor.close()
    connection.close()

    for table in tables:
        yield table


def process_element(element, db_host, db_name, db_user, db_password):
    table_name = element[0]
    # Lógica de procesamiento para conectarse a la base de datos SQL
    connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};SERVER={db_host};DATABASE={db_name};UID={db_user};PWD={db_password};"
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Consulta a la base de datos
    query = f"SELECT * FROM {table_name}"
    cursor.execute(query)
    results = cursor.fetchall()

   

    # Procesamiento de los datos obtenidos de la consulta
    column_names = [column[0] for column in cursor.description]
    processed_data = []
    for row in results:
        processed_data.append(dict(zip(column_names, row)))
    cursor.close()
    connection.close()
    yield table_name, processed_data


def write_to_bigquery(element, output_table,temp_location,project):
    table_name, data = element
    # Escribir los datos en BigQuery
    data | f'Write {table_name} to Table' >> WriteToBigQuery(
        table=project+':'+output_table + '.' + table_name,
        schema='SCHEMA_AUTODETECT',
        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        custom_gcs_temp_location=temp_location
        )


def main():
    # Configuración de los argumentos de línea de comandos utilizando argparse
    parser = argparse.ArgumentParser(description='Process data from SQL')
    parser.add_argument('--runner', dest='runner', required=True)
    parser.add_argument('--project', dest='project', required=True)
    parser.add_argument('--region', dest='region', required=True)
    parser.add_argument('--job_name', dest='job_name', required=True)
    parser.add_argument('--staging_location', dest='staging_location', required=True)
    parser.add_argument('--temp_location', dest='temp_location', required=True)

    parser.add_argument('--db_host', required=True, help='Database host')
    parser.add_argument('--db_name', required=True, help='Database name')
    parser.add_argument('--db_user', required=True, help='Database username')
    parser.add_argument('--db_password', required=True, help='Database password')
    parser.add_argument('--requirements_file', required=True, help='requirements')
    parser.add_argument('--setup_file', required=True, help='setup file')
    parser.add_argument('--sdk_container', required=True, help='container')
    parser.add_argument('--network', required=True, help='network')
    parser.add_argument('--subnetwork', required=True, help='network')
    parser.add_argument('--service_account', required=True, help='service_account')
    parser.add_argument('--template_location', required=True, help='template_location')
    parser.add_argument('--output_table', dest='output_table', required=True,
                        help=('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE '
                              'or DATASET.TABLE'))

    args = parser.parse_args()

    # Configuración de las opciones del pipeline de Apache Beam
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True

    # Creación del pipeline
    p = Pipeline(options=options)

    # Obtener la lista de tablas de la base de datos
    tables = p | 'Get Table Names' >> Create([None]) | ParDo(get_table_names, args.db_host, args.db_name,
                                                             args.db_user, args.db_password)

    # Ejecutar el proceso para cada tabla y escribir los resultados en BigQuery
    processed_data = tables | 'Process Tables' >> ParDo(process_element, args.db_host, args.db_name,
                                                         args.db_user, args.db_password)
    processed_data | 'Write to BigQuery' >> ParDo(write_to_bigquery, args.output_table , args.temp_location,args.project)

    # Ejecución del pipeline
    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
