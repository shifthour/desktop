"""IndvBePopHlthRelValPkey – converted from IBM DataStage shared container.

COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
    Primary-key shared container used in IhmAnalyticsIndvBePopHlthRelValExtr.

ANNOTATIONS:
    • This container is used in:
      IhmAnalyticsIndvBePopHlthRelValExtr
      These programs need to be re-compiled when logic changes
    • Assign primary surrogate key
"""

# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

def run_IndvBePopHlthRelValPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack required runtime parameters
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]

    # ------------------------------------------------------------------
    # Lookup – dummy table replacing hashed file hf_indv_be_pop_hlth_rel_val
    # ------------------------------------------------------------------
    dummy_table = f"{IDSOwner}.dummy_hf_indv_be_pop_hlth_rel_val"

    extract_query = f"""
        SELECT
            INDV_BE_KEY,
            POP_HLTH_REL_VAL_ID,
            AS_OF_DT_SK,
            SRC_SYS_CD_SK,
            CRT_RUN_CYC_EXCTN_SK,
            INDV_BE_POP_HLTH_REL_VAL_SK
        FROM {dummy_table}
    """

    df_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ------------------------------------------------------------------
    # Join input stream with lookup table
    # ------------------------------------------------------------------
    df_join = (
        df_Transform.alias("transform")
        .join(
            df_lkup.alias("lkup"),
            (col("transform.INDV_BE_KEY")         == col("lkup.INDV_BE_KEY")) &
            (col("transform.RELATIVE_VALUE_ID")   == col("lkup.POP_HLTH_REL_VAL_ID")) &
            (col("transform.AS_OF_DT_SK")         == col("lkup.AS_OF_DT_SK")) &
            (col("transform.SRC_SYS_CD_SK")       == col("lkup.SRC_SYS_CD_SK")),
            "left"
        )
    )

    # ------------------------------------------------------------------
    # Add surrogate-key and other derived columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_join
        .withColumn("SK", col("lkup.INDV_BE_POP_HLTH_REL_VAL_SK"))
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("lkup.INDV_BE_POP_HLTH_REL_VAL_SK").isNull(), lit(CurrRunCycle))
            .otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'SK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Build Key output link
    # ------------------------------------------------------------------
    df_Key = (
        df_enriched.select(
            col("transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("transform.DISCARD_IN").alias("DISCARD_IN"),
            col("transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("transform.ERR_CT").alias("ERR_CT"),
            col("transform.RECYCLE_CT").alias("RECYCLE_CT"),
            col("transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("SK").alias("INDV_BE_POP_HLTH_REL_VAL_SK"),
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("transform.INDV_BE_KEY").alias("INDV_BE_KEY"),
            col("transform.RELATIVE_VALUE_ID").alias("RELATIVE_VALUE_ID"),
            col("transform.AS_OF_DT_SK").alias("AS_OF_DT_SK"),
            col("transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            col("transform.POP_HLTH_REL_VAL_TX").alias("POP_HLTH_REL_VAL_TX")
        )
    )

    # ------------------------------------------------------------------
    # Prepare insert/update set for dummy table
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .filter(col("lkup.INDV_BE_POP_HLTH_REL_VAL_SK").isNull())
        .select(
            col("transform.INDV_BE_KEY").alias("INDV_BE_KEY"),
            col("transform.RELATIVE_VALUE_ID").alias("POP_HLTH_REL_VAL_ID"),
            col("transform.AS_OF_DT_SK").alias("AS_OF_DT_SK"),
            col("transform.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("SK").alias("INDV_BE_POP_HLTH_REL_VAL_SK")
        )
    )

    (
        df_updt.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", dummy_table)
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # Return the single output stream
    # ------------------------------------------------------------------
    return df_Key