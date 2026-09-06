import dlt
# --------------------------------------------------
# 1. STAGING TABLE
# --------------------------------------------------
@dlt.table
def factstream_stg():
    df = spark.readStream.table('silvercatalog.silverschema.factstream')
    return df

# --------------------------------------------------
# 2. TARGET STREAMING TABLE
# --------------------------------------------------

dlt.create_streaming_table("goldcatalog.goldschema.factstream")

# --------------------------------------------------
# 3. SCD TYPE 2 AUTO CDC
# --------------------------------------------------

dlt.create_auto_cdc_flow(
    target="goldcatalog.goldschema.factstream",
    source="factstream_stg",
    keys=["stream_id"],
    sequence_by="stream_timestamp",
    stored_as_scd_type=1,
    track_history_except_column_list=None,
    name=None,
    once=False
)