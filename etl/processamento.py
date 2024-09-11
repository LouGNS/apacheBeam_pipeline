import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import unicodedata
from datetime import datetime
import pytz
from apache_beam.io import ReadFromText, WriteToParquet
from apache_beam.io.parquetio import _Schema

# Função para remover acentuação e espaços em branco e padronizar em maiúsculas
def preprocess_text(text):
    if text:
        text = text.strip()  # Remove espaços em branco
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')  # Remove acentuação
        return text.upper()  # Padroniza em maiúsculas
    return text

# Função de pré-processamento
def preprocess_data(element):
    # Preprocessa as colunas de cada arquivo
    element['nome'] = preprocess_text(element.get('nome', ''))
    element['cpf'] = element.get('cpf', '')
    element['email'] = preprocess_text(element.get('email', ''))
    element['numeroConta'] = element.get('numeroConta', '')
    element['numeroCartao'] = element.get('numeroCartao', '')
    element['ranking'] = element.get('ranking', '')

    # Adiciona a coluna DT_CARGA com a data e horário atual em UTC
    element['DT_CARGA'] = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

    return element

# Função para remover duplicatas com base no CPF
def remove_duplicates(elements):
    seen_cpfs = set()
    unique_elements = []
    for element in elements:
        cpf = element['cpf']
        if cpf not in seen_cpfs:
            seen_cpfs.add(cpf)
            unique_elements.append(element)
    return unique_elements

# Definindo o esquema dos dados
def get_schema():
    return {
        'nome': 'STRING',
        'cpf': 'STRING',
        'email': 'STRING',
        'numeroConta': 'STRING',
        'numeroCartao': 'STRING',
        'ranking': 'STRING',
        'DT_CARGA': 'STRING'
    }

def run():
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    # Caminhos absolutos para os arquivos CSV
    ranking_file_path = 'C:/Users/User/OneDrive/Documentos/Luiz Gustavo/Teste Banco ABC/Teste-Banco-ABC/output/ranking_clientes.csv'
    resultado_file_path = 'C:/Users/User/OneDrive/Documentos/Luiz Gustavo/Teste Banco ABC/Teste-Banco-ABC/output/resultado_query.csv'

    # Lê os arquivos CSV e aplica as transformações
    ranking_clients = (
        p
        | 'Read ranking_clientes.csv' >> beam.io.ReadFromText(ranking_file_path, skip_header_lines=1)
        | 'Parse ranking_clientes' >> beam.Map(lambda line: dict(zip(['cpf', 'numeroConta', 'numeroCartao', 'ranking'], next(csv.reader([line])))))
    )

    resultado_query = (
        p
        | 'Read resultado_query.csv' >> beam.io.ReadFromText(resultado_file_path, skip_header_lines=1)
        | 'Parse resultado_query' >> beam.Map(lambda line: dict(zip(['nome', 'cpf', 'email'], next(csv.reader([line])))))
        | 'Preprocess data' >> beam.Map(preprocess_data)
    )

    # Junta os dois datasets pelo CPF e remove duplicatas
    merged_data = (
        {'ranking': ranking_clients, 'resultado': resultado_query}
        | 'Merge datasets' >> beam.CoGroupByKey()
        | 'Remove duplicates' >> beam.FlatMap(remove_duplicates)
    )

    # Salva os dados processados no formato Parquet com esquema
    merged_data | 'Write to Parquet' >> beam.io.WriteToParquet(
        'output/processed_data.parquet',
        schema=get_schema()
    )

    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()
