#este código foi feito para testar a escrita e gravação de arquivos parquet do apache beam
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyarrow as pa
from datetime import datetime
import pytz

def preprocess_data(element):
    element['DT_CARGA'] = datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
    return element

def run():
    print("Pipeline execution started")
    options = PipelineOptions()
    p = beam.Pipeline(options=options)

    # Simulando dados
    data = [
        {'cpf': 12345678901, 'numeroConta': 987654321, 'numeroCartao': 1234567812345678, 'ranking': 'A'},
        {'cpf': 23456789012, 'numeroConta': 876543210, 'numeroCartao': 2345678923456789, 'ranking': 'B'}
    ]

    schema = pa.schema([
        ('cpf', pa.int64()),
        ('numeroConta', pa.int64()),
        ('numeroCartao', pa.int64()),
        ('ranking', pa.string()),
        ('DT_CARGA', pa.string())
    ])

    # Pipeline
    result = (
        p
        | 'Create Data' >> beam.Create(data)
        | 'Preprocess Data' >> beam.Map(preprocess_data)
        | 'Write to Parquet' >> beam.io.WriteToParquet(
            'output/test_data.parquet',
            schema=schema,
            file_name_suffix='.parquet'
        )
    )

    print("Pipeline step completed")
    
    p.run().wait_until_finish()
    print("Pipeline execution finished")

if __name__ == '__main__':
    run()
