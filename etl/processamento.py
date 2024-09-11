import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import unicodedata
from datetime import datetime
import pytz

# Função para remover duplicados
def remove_duplicates(element):
    return element[0], element[1]


# Função para remover acentuação e padronizar maiúsculas
def preprocess_string(s):
    nfkd = unicodedata.normalize('NFKD', s)
    return u"".join([c for c in nfkd if not unicodedata.combining(c)]).upper().strip()


# Função para pré-processar os dados
def preprocess_data(row):
    row['nome'] = preprocess_string(row['nome'])
    row['email'] = preprocess_string(row['email'])
    row['cidade'] = preprocess_string(row['cidade'])
    return row


# Função para adicionar a coluna DT_CARGA
def add_load_date(element):
    utc_now = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    element['DT_CARGA'] = utc_now
    return element


# Função para salvar o dataframe em formato parquet
def write_parquet(data, output_path):
    table = pa.Table.from_pandas(pd.DataFrame(data))
    pq.write_table(table, output_path)


# Definir o pipeline
def run_pipeline():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        # Lendo os CSVs
        resultado = (
            p
            | 'Ler resultado_query CSV' >> beam.io.ReadFromText('output/resultado_query.csv', skip_header_lines=1)
            | 'Dividir CSV Resultado' >> beam.Map(lambda line: dict(zip(['cpf', 'nome', 'email', 'cidade'], line.split(','))))
        )
        
        ranking = (
            p
            | 'Ler ranking_query CSV' >> beam.io.ReadFromText('output/ranking_query.csv', skip_header_lines=1)
            | 'Dividir CSV Ranking' >> beam.Map(lambda line: dict(zip(['cpf', 'ranking'], line.split(','))))
        )
        
        # Pré-processamento dos dados
        resultado = (
            resultado
            | 'Remover duplicados' >> beam.Distinct()
            | 'Pré-processar strings' >> beam.Map(preprocess_data)
            | 'Adicionar DT_CARGA' >> beam.Map(add_load_date)
        )
        
        # Salvando os resultados em parquet
        resultado | 'Escrever Parquet Resultado' >> beam.Map(write_parquet, 'output/resultado_query_processed.parquet')
        ranking | 'Escrever Parquet Ranking' >> beam.Map(write_parquet, 'output/ranking_query_processed.parquet')


if __name__ == '__main__':
    run_pipeline()