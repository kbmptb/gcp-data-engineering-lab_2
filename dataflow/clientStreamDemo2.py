import csv
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


class ParseTransaction(beam.DoFn):

    def process(self, line):

        try:

            row = next(csv.reader([line]))

            ID = row[0]
            NOM = row[1]
            PRENOM = row[2]
            NUM_TEL = row[3]
            ADRESSE = row[4]

            yield (ID, NOM, PRENOM, NUM_TEL, ADRESSE)

        except Exception as e:
            print(f"Erreur de parsing : {e}")


class FormatForBigQuery(beam.DoFn):

    def process(self, element):

        ID, NOM, PRENOM, NUM_TEL, ADRESSE = element

        yield {
            "ID": int(ID),
            "NOM": NOM,
            "PRENOM": PRENOM,
            "NUM_TEL": NUM_TEL,
            "ADRESSE": ADRESSE
        }


def run():

    options = PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:

        (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(
                "gs://clients_21092026/clients_21092026.csv",
                skip_header_lines=1
            )
            | "Parse CSV" >> beam.ParDo(ParseTransaction())
            | "Filter ID >= 50" >> beam.Filter(
                lambda row: int(row[0]) >= 50
            )
            | "Format BigQuery Row" >> beam.ParDo(FormatForBigQuery())
            | "Write To BigQuery" >> beam.io.WriteToBigQuery(
                table="client_stream_demo",
                dataset="dataflow_demo_dev",
                project="project-a0c99e7b-8c47-4075-b97",
                schema={
                    "fields": [
                        {
                            "name": "ID",
                            "type": "INT64",
                            "mode": "REQUIRED"
                        },
                        {
                            "name": "NOM",
                            "type": "STRING",
                            "mode": "REQUIRED"
                        },
                        {
                            "name": "PRENOM",
                            "type": "STRING",
                            "mode": "REQUIRED"
                        },
                        {
                            "name": "NUM_TEL",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        },
                        {
                            "name": "ADRESSE",
                            "type": "STRING",
                            "mode": "NULLABLE"
                        }
                    ]
                },
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )


if __name__ == "__main__":
    run()