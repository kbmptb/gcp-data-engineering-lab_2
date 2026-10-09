import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import (
    FixedWindows,
    SlidingWindows,
    Sessions
)

from apache_beam.io.gcp.bigquery import WriteToBigQuery

from datetime import datetime


# =====================================================
# 1. PARSE CSV
# =====================================================
#
# Transforme chaque ligne CSV en dictionnaire Python.
#
# Exemple :
#
# "1,C001,Electronics,120.50,2026-10-01T09:01:00"
#
# devient :
#
# {
#   "order_id": "1",
#   "customer_id": "C001",
#   "category": "Electronics",
#   "amount": 120.50,
#   "event_time": "2026-10-01T09:01:00"
# }
#
# =====================================================

class ParseCSVFn(beam.DoFn):

    def process(self, line):

        fields = line.split(",")

        yield {
            "order_id": fields[0],
            "customer_id": fields[1],
            "category": fields[2],
            "amount": float(fields[3]),
            "event_time": fields[4]
        }


# =====================================================
# 2. PARDO
# =====================================================
#
# Ajout d'une colonne amount_ttc.
#
# Beam utilise ParDo pour des traitements ligne
# par ligne plus complexes qu'un simple Map().
#
# =====================================================

class CalculateVATFn(beam.DoFn):

    def process(self, record):

        record["amount_ttc"] = round(
            record["amount"] * 1.20,
            2
        )

        yield record


# =====================================================
# 3. AJOUT DU TIMESTAMP EVENT TIME
# =====================================================
#
# Les fenêtres Beam fonctionnent sur un timestamp.
#
# On convertit event_time en timestamp Beam.
#
# =====================================================

class AddEventTimestampFn(beam.DoFn):

    def process(self, record):

        dt = datetime.fromisoformat(
            record["event_time"]
        )

        yield beam.window.TimestampedValue(
            record,
            dt.timestamp()
        )


# =====================================================
# OPTIONS DATAFLOW
# =====================================================

options = PipelineOptions(
    save_main_session=True
)

# =====================================================
# PIPELINE
# =====================================================

with beam.Pipeline(options=options) as pipeline:

    # -------------------------------------------------
    # 1. Lecture CSV
    # -------------------------------------------------

    orders = (

        pipeline

        | "Read CSV"
        >> beam.io.ReadFromText(
            "gs://bucket280926-demo-dev/input/customer_orders_1000.csv",
            skip_header_lines=1
        )

        | "Parse CSV"
        >> beam.ParDo(ParseCSVFn())

    )

    # -------------------------------------------------
    # 2. ParDo
    # -------------------------------------------------

    enriched_orders = (

        orders

        | "Calculate VAT"
        >> beam.ParDo(CalculateVATFn())

    )

    # -------------------------------------------------
    # 3. Filter
    # -------------------------------------------------
    #
    # On conserve uniquement les commandes
    # supérieures à 100 euros.
    #
    # -------------------------------------------------

    high_value_orders = (

        enriched_orders

        | "Filter Orders >100"
        >> beam.Filter(
            lambda x: x["amount"] > 100
        )

    )

    # -------------------------------------------------
    # 4. GroupByKey
    # -------------------------------------------------
    #
    # Regroupe toutes les commandes
    # d'un même client.
    #
    # Sortie :
    #
    # C001 -> [120,65,30]
    #
    # -------------------------------------------------

    grouped_orders = (

        enriched_orders

        | "Key By Customer"
        >> beam.Map(
            lambda x:
            (
                x["customer_id"],
                x["amount"]
            )
        )

        | "Group By Customer"
        >> beam.GroupByKey()

    )

    # -------------------------------------------------
    # 5. CombinePerKey
    # -------------------------------------------------
    #
    # Calcule le montant total dépensé
    # par client.
    #
    # Sortie :
    #
    # C001 -> 215
    #
    # -------------------------------------------------

    customer_sales = (

        enriched_orders

        | "Amount By Customer"
        >> beam.Map(
            lambda x:
            (
                x["customer_id"],
                x["amount"]
            )
        )

        | "Sum Amount Per Customer"
        >> beam.CombinePerKey(sum)

        | "Format Customer Sales"
        >> beam.Map(
            lambda x: {
                "customer_id": x[0],
                "total_amount": x[1]
            }
        )

    )

    # -------------------------------------------------
    # Préparation Windowing
    # -------------------------------------------------

    timestamped_orders = (

        enriched_orders

        | "Add Event Timestamp"
        >> beam.ParDo(
            AddEventTimestampFn()
        )

    )

    # -------------------------------------------------
    # 6. FIXED WINDOW
    # -------------------------------------------------
    #
    # Fenêtre fixe de 10 minutes.
    #
    # Exemple :
    #
    # 09:00 -> 09:10
    # 09:10 -> 09:20
    #
    # -------------------------------------------------

    fixed_window_sales = (

        timestamped_orders

        | "Fixed Window"
        >> beam.WindowInto(
            FixedWindows(600)
        )

        | "Fixed Key"
        >> beam.Map(
            lambda x:
            (
                x["category"],
                x["amount"]
            )
        )

        | "Fixed Sum"
        >> beam.CombinePerKey(sum)

        | "Format Fixed Results"
        >> beam.Map(
            lambda x: {
                "category": x[0],
                "total_amount": x[1]
            }
        )

    )

    # -------------------------------------------------
    # 7. SLIDING WINDOW
    # -------------------------------------------------
    #
    # Fenêtre :
    #
    # taille 10 min
    # glissement 5 min
    #
    # Les mêmes événements peuvent
    # appartenir à plusieurs fenêtres.
    #
    # -------------------------------------------------

    sliding_window_sales = (

        timestamped_orders

        | "Sliding Window"
        >> beam.WindowInto(
            SlidingWindows(
                size=600,
                period=300
            )
        )

        | "Sliding Key"
        >> beam.Map(
            lambda x:
            (
                x["category"],
                x["amount"]
            )
        )

        | "Sliding Sum"
        >> beam.CombinePerKey(sum)

        | "Format Sliding"
        >> beam.Map(
            lambda x: {
                "category": x[0],
                "total_amount": x[1]
            }
        )

    )

    # -------------------------------------------------
    # 8. SESSION WINDOW
    # -------------------------------------------------
    #
    # Gap de session :
    # 15 minutes
    #
    # Si un client est inactif
    # pendant plus de 15 minutes,
    # Beam démarre une nouvelle session.
    #
    # -------------------------------------------------

    session_sales = (

        timestamped_orders

        | "Session Window"
        >> beam.WindowInto(
            Sessions(
                gap_size=900
            )
        )

        | "Session Key"
        >> beam.Map(
            lambda x:
            (
                x["customer_id"],
                1
            )
        )

        | "Count Orders Per Session"
        >> beam.CombinePerKey(sum)

        | "Format Session"
        >> beam.Map(
            lambda x: {
                "customer_id": x[0],
                "order_count": x[1]
            }
        )

    )

    # =================================================
    # BIGQUERY OUTPUTS
    # =================================================

    enriched_orders | "BQ Orders Enriched" >> WriteToBigQuery(
        table="dataflow_demo_dev.orders_enriched",
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

    customer_sales | "BQ Customer Sales" >> WriteToBigQuery(
        table="dataflow_demo_dev.customer_sales",
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

    fixed_window_sales | "BQ Fixed Window" >> WriteToBigQuery(
        table="dataflow_demo_dev.fixed_window_sales",
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

    sliding_window_sales | "BQ Sliding Window" >> WriteToBigQuery(
        table="dataflow_demo_dev.sliding_window_sales",
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )

    session_sales | "BQ Session Window" >> WriteToBigQuery(
        table="dataflow_demo_dev.session_sales",
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
    )