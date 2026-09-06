import dlt
# --------------------------------------------------
# 1. STAGING TABLE
# --------------------------------------------------
@dlt.table
def dimdate_stg():
    df = spark.readStream.table('silvercatalog.silverschema.dimdate')
    return df

# --------------------------------------------------
# 2. TARGET STREAMING TABLE
# --------------------------------------------------

dlt.create_streaming_table("goldcatalog.goldschema.dimdate")

# --------------------------------------------------
# 3. SCD TYPE 2 AUTO CDC
# --------------------------------------------------

dlt.create_auto_cdc_flow(
    target="goldcatalog.goldschema.dimdate",
    source="dimdate_stg",
    keys=["date_key"],
    sequence_by="date",
    stored_as_scd_type=2,
    track_history_except_column_list=None,
    name=None,
    once=False
)