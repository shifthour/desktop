# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: ExcdPkey
MODIFICATIONS:
Developer      Date         Project/Altiris #   Change Description
-------------- ------------ ------------------- ------------------------------------------------------------
Ralph Tucker   10/4/07      15                  added documentation for NascoClmLnExtr
Ralph Tucker   2012-01-24   TTR-1252            Removed Display and Element values for standards

This container is used by:
    FctsExCdExtr
    NascoExCdExtr
    NascoClmLnExtr
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def run_ExcdPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Executes the logic of the ExcdPkey shared container.

    Parameters
    ----------
    df_Transform : DataFrame
        Input stream corresponding to the "Transform" link.
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Output stream corresponding to the "Key" link.
    """

    # --------------------------------------------------
    # Unpack required parameters exactly once
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]

    # JDBC configuration for IDS (assumed database for dummy tables)
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    # --------------------------------------------------
    # Read dummy table replacing hash-file hf_excd
    # --------------------------------------------------
    lkup_query = """
        SELECT
            SRC_SYS_CD               AS SRC_SYS_CD_lkp,
            EXCD_ID                  AS EXCD_ID_lkp,
            CRT_RUN_CYC_EXCTN_SK     AS CRT_RUN_CYC_EXCTN_SK_lkp,
            EXCD_SK                  AS EXCD_SK_lkp
        FROM dummy_hf_excd
    """

    df_hf_excd_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", lkup_query)
            .load()
    )

    # --------------------------------------------------
    # Join input with lookup
    # --------------------------------------------------
    df_joined = (
        df_Transform.alias("t")
        .join(
            df_hf_excd_lkup.alias("lkup"),
            (
                (F.col("t.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD_lkp")) &
                (F.col("t.EXCD_ID")    == F.col("lkup.EXCD_ID_lkp"))
            ),
            "left"
        )
    )

    # --------------------------------------------------
    # Derive transformation columns
    # --------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn(
            "SK",
            F.when(F.trim(F.col("t.EXCD_ID")) == F.lit("UNK"), F.lit(1))
             .when(F.trim(F.col("t.EXCD_ID")) == F.lit("NA"),  F.lit(0))
             .when(F.length(F.trim(F.col("lkup.EXCD_SK_lkp"))) == 0, F.lit(None).cast("long"))
             .otherwise(F.col("lkup.EXCD_SK_lkp"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(F.col("lkup.EXCD_SK_lkp").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # --------------------------------------------------
    # Surrogate key generation (replaces KeyMgtGetNextValueConcurrent)
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

    # --------------------------------------------------
    # Build Key output link
    # --------------------------------------------------
    df_key = (
        df_enriched.select(
            F.col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("t.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("t.DISCARD_IN").alias("DISCARD_IN"),
            F.col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("t.ERR_CT").alias("ERR_CT"),
            F.col("t.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("SK").alias("EXCD_SK"),
            F.when(F.col("t.EXCD_ID").isNull(), F.lit("    ")).otherwise(F.col("t.EXCD_ID")).alias("EXCD_ID"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("t.EXCD_HC_ADJ_CD").alias("EXCD_HC_ADJ_CD"),
            F.col("t.EXCD_PT_LIAB_IND").alias("EXCD_PT_LIAB_IND"),
            F.col("t.EXCD_PROV_ADJ_CD").alias("EXCD_PROV_ADJ_CD"),
            F.col("t.EXCD_REMIT_REMARK_CD").alias("EXCD_REMIT_REMARK_CD"),
            F.col("t.EXCD_STS").alias("EXCD_STS"),
            F.col("t.EXCD_TYPE").alias("EXCD_TYPE"),
            F.col("t.EXCD_LONG_TX1").alias("EXCD_LONG_TX1"),
            F.col("t.EXCD_LONG_TX2").alias("EXCD_LONG_TX2"),
            F.col("t.EXCD_SH_TX").alias("EXCD_SH_TX")
        )
    )

    # --------------------------------------------------
    # Build update stream for dummy_hf_excd (where lookup failed)
    # --------------------------------------------------
    df_updt = (
        df_enriched.filter(F.col("lkup.EXCD_ID_lkp").isNull())
        .select(
            F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.when(F.col("t.EXCD_ID").isNull(), F.lit("    ")).otherwise(F.col("t.EXCD_ID")).alias("EXCD_ID"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("EXCD_SK")
        )
    )

    # --------------------------------------------------
    # Write updates back to dummy table
    # --------------------------------------------------
    (
        df_updt.write
            .format("jdbc")
            .mode("append")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", "dummy_hf_excd")
            .save()
    )

    # --------------------------------------------------
    # Return the Key output stream
    # --------------------------------------------------
    return df_key
# COMMAND ----------