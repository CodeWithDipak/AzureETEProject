import dlt

#--------------------
# EXPECTATIONS
#-------------------

expectations = {
    "rule_1" : "user_id IS NOT NULL"
}

# --------------------------------------------------
# 1. STAGING TABLE
# --------------------------------------------------

@dlt.table
@dlt.expect_all_or_drop(expectations)
def dimuser_stg():
    df = spark.readStream.table('silvercatalog.silverschema.dimuser')
    return df

# --------------------------------------------------
# 2. TARGET STREAMING TABLE
# --------------------------------------------------

dlt.create_streaming_table(
    name = "goldcatalog.goldschema.dimuser",
    expect_all_or_drop = expectations
)

# --------------------------------------------------
# 3. SCD TYPE 2 AUTO CDC
# --------------------------------------------------

dlt.create_auto_cdc_flow(
    target="goldcatalog.goldschema.dimuser",
    source="dimuser_stg",
    keys=["user_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2,
    track_history_except_column_list=None,
    name=None,
    once=False
)