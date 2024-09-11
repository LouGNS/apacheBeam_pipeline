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