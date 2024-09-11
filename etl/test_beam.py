import apache_beam as beam

with beam.Pipeline() as pipeline:
    (
        pipeline
        | 'Start' >> beam.Create([1, 2, 3, 4, 5])
        | 'Print' >> beam.Map(print)
    )
