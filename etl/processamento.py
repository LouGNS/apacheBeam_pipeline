import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyarrow as pa
import csv
from datetime import datetime
import pytz

def preprocess_data(element):
    element['DT_CARGA'] = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

    # Convertendo para os tipos apropriados
    if 'cpf' in element:
        element['cpf'] = int(element['cpf'])
    if 'numeroConta' in element:
        element['numeroConta'] = int(element['numeroConta'])
    if 'numeroCartao' in element:
        element['numeroCartao'] = int(element['numeroCartao'])
    
    return element

def run():
    print("Pipeline execution started")
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    # Caminhos absolutos para os arquivos CSV
    ranking_file_path = 'C:/Users/User/OneDrive/Documentos/Luiz Gustavo/Teste Banco ABC/Teste-Banco-ABC/output/ranking_clientes.csv'
    resultado_file_path = 'C:/Users/User/OneDrive/Documentos/Luiz Gustavo/Teste Banco ABC/Teste-Banco-ABC/output/resultado_query.csv'

    def read_csv(file_path, schema):
        return (
            p
            | f'Read {file_path}' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
            | f'Parse {file_path}' >> beam.Map(lambda line: dict(zip(schema, next(csv.reader([line])))))
            | f'Convert Data Types {file_path}' >> beam.Map(lambda row: {
                col: int(row[col]) if col in ['cpf', 'numeroConta', 'numeroCartao'] and row[col].isdigit() else row[col]
                for col in row
            })
        )

    ranking_schema = ['cpf', 'numeroConta', 'numeroCartao', 'ranking']
    resultado_schema = ['nome', 'cpf', 'email']

    # Pipeline para dados de ranking_clientes
    ranking_clients = (
        read_csv(ranking_file_path, ranking_schema)
        | 'Preprocess Ranking Data' >> beam.Map(preprocess_data)
    )
    ranking_schema_arrow = pa.schema([
        ('cpf', pa.int64()),
        ('numeroConta', pa.int64()),
        ('numeroCartao', pa.int64()),
        ('ranking', pa.string()),
        ('DT_CARGA', pa.string())
    ])
    ranking_clients | 'Write ranking to Parquet' >> beam.io.WriteToParquet(
        'output/ranking_clientes.parquet',
        schema=ranking_schema_arrow,
        file_name_suffix='.parquet'
    )

    # Pipeline para dados de resultado_query
    resultado_query = (
        read_csv(resultado_file_path, resultado_schema)
        | 'Preprocess Resultado Data' >> beam.Map(preprocess_data)
    )
    resultado_schema_arrow = pa.schema([
        ('nome', pa.string()),
        ('cpf', pa.int64()),
        ('email', pa.string()),
        ('DT_CARGA', pa.string())
    ])
    resultado_query | 'Write resultado to Parquet' >> beam.io.WriteToParquet(
        'output/resultado_query.parquet',
        schema=resultado_schema_arrow,
        file_name_suffix='.parquet'
    )

    print("Pipeline step completed")
    
    p.run().wait_until_finish()
    print("Pipeline execution finished")

if __name__ == '__main__':
    run()
