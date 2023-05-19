import logging
import argparse
import pyodbc
from apache_beam import CombineGlobally, Create, ParDo, Pipeline
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition

def process_element(element,db_host,db_name,db_user,db_password):
    # Aquí puedes realizar la lógica de procesamiento para conectarte a la base de datos SQL
    # y realizar las operaciones necesarias con los datos

    # Ejemplo de conexión a una base de datos SQL mediante pyodbc
    connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};SERVER={db_host};DATABASE={db_name};UID={db_user};PWD={db_password};"
    connection = pyodbc.connect(connection_string)
    cursor = connection.cursor()

    # Ejemplo de consulta a la base de datos
    cursor.execute("SELECT  top 10 * FROM LMS_ACTIVIDAD")
    results = cursor.fetchall()
    # Obtener los nombres de columna
    column_names = [column[0] for column in cursor.description]
    # Ejemplo de procesamiento de los datos obtenidos de la consulta
    for row in results:
        yield dict(zip(column_names, row))

    # Ejemplo de impresión de los resultados
    logging.info('row', results)

    # Aquí puedes realizar cualquier otra operación o lógica de procesamiento necesaria

    # Cierre de la conexión a la base de datos
    cursor.close()
    connection.close()



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
    parser.add_argument('--service_account', required=True, help='network')
    parser.add_argument('--output_table', dest='output_table', required=True, 
                                           help=('Output BigQuery table for results specified as: PROJECT:DATASET.TABLE ' 
                                                                                                       'or DATASET.TABLE'))
    
    args = parser.parse_args()

    # Configuración de las opciones del pipeline de Apache Beam
    options = PipelineOptions()
    options.view_as(SetupOptions).save_main_session = True

    # Creación del pipeline
    p = Pipeline(options=options)

    # Creación de la fuente de datos utilizando el rango especificado
    start = 0
    end = 1
    source = p | 'From {} to {}'.format(start, end) >> Create(list(range(start, end)))

    processed_data = source | 'Process' >> ParDo(process_element, args.db_host, args.db_name, args.db_user, args.db_password)
    #escrituro a bigquery
    processed_data | 'Write to Table' >> WriteToBigQuery(
                               table=args.output_table,
                               schema = 'SCHEMA_AUTODETECT',
                               write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                               create_disposition=BigQueryDisposition.CREATE_IF_NEEDED)
                 
    # total_sum = processed_data | 'Sum' >> CombineGlobally(sum)

    # Ejecución del pipeline
    p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()
