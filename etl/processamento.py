import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import unicodedata
from datetime import datetime
import pytz

# Função para remover acentuação e espaços em branco e padronizar em maiúsculas
def preprocess_text(text):
    if text:
        text = text.strip()  # Remove espaços em branco
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')  # Remove acentuação
        return text.upper()  # Padroniza em maiúsculas
    return text

# Função de pré-processamento
def preprocess_data(element):
    # Converte todas as colunas para string e aplica preprocessamento
    element['nome'] = preprocess_text(str(element.get('nome', '')))
    element['cpf'] = str(element.get('cpf', ''))
    element['email'] = preprocess_text(str(element.get('email', '')))
    element['numeroConta'] = str(element.get('numeroConta', ''))
    element['numeroCartao'] = str(element.get('numeroCartao', ''))
    element['ranking'] = str(element.get('ranking', ''))

    # Adiciona a coluna DT_CARGA com a data e horário atual em UTC
    element['DT_CARGA'] = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

    return element

# Função para definir o esquema dos dados
def get_ranking_schema():
    return {
        'fields': [
            {'name': 'cpf', 'type': 'INT64'},
            {'name': 'numeroConta', 'type': 'INT64'},
            {'name': 'numeroCartao', 'type': 'INT64'},
            {'name': 'ranking', 'type': 'STRING'}
        ]
    }

def get_resultado_schema():
    return {
        'fields': [
            {'name': 'nome', 'type': 'STRING'},
            {'name': 'cpf', 'type': 'INT64'},
            {'name': 'email', 'type': 'STRING'},
            {'name': 'DT_CARGA', 'type': 'STRING'}
        ]
    }

def run():
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    # Caminhos absolutos para os arquivos CSV
    ranking_file_path = 'C:/Users/User/OneDrive/Documentos/Luiz Gustavo/Teste Banco ABC/Teste-Banco-ABC/output/ranking_clientes.csv'
    resultado_file_path = 'C:/Users/User/OneDrive/Documentos/Luiz Gustavo/Teste Banco ABC/Teste-Banco-ABC/output/resultado_query.csv'

    # Lê e processa o arquivo ranking_clientes.csv
    ranking_clients = (
        p
        | 'Read ranking_clientes.csv' >> beam.io.ReadFromText(ranking_file_path, skip_header_lines=1)
        | 'Parse ranking_clientes' >> beam.Map(lambda line: dict(zip(['cpf', 'numeroConta', 'numeroCartao', 'ranking'], next(csv.reader([line])))))
    )

    # Lê e processa o arquivo resultado_query.csv
    resultado_query = (
        p
        | 'Read resultado_query.csv' >> beam.io.ReadFromText(resultado_file_path, skip_header_lines=1)
        | 'Parse resultado_query' >> beam.Map(lambda line: dict(zip(['nome', 'cpf', 'email'], next(csv.reader([line])))))
        | 'Preprocess data' >> beam.Map(preprocess_data)
    )

    # Salva a tabela ranking_clientes no formato Parquet
    ranking_clients | 'Write ranking to Parquet' >> beam.io.WriteToParquet(
        'output/ranking_clientes.parquet',
        schema=get_ranking_schema(),
        file_name_suffix='.parquet',
        shard_name_template='',
    )

    # Salva a tabela resultado_query no formato Parquet
    resultado_query | 'Write resultado to Parquet' >> beam.io.WriteToParquet(
        'output/resultado_query.parquet',
        schema=get_resultado_schema(),
        file_name_suffix='.parquet',
        shard_name_template='',
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
