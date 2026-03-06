
# Databricks notebook: CopyOfExcdPkey – converted from IBM DataStage shared container
# JobName        : CopyOfExcdPkey
# JobType        : Server Job
# JobCategory    : DS_Integrate
# FolderPath     : Shared Containers
# Description    :
# MODIFICATIONS:
# Developer                Date                 Project/Altiris #         Change Description                                                           Development Project      Code Reviewer          Date Reviewed
# ------------------       -------------------- ------------------------  -----------------------------------------------------------------------     ------------------------ ----------------------- ----------------------------
# Ralph Tucker             10/4/07             15                        added documentation for NascoClmLnExtr                                      devlIDS30                Steph Goddard          10/4/07
# Ralph Tucker             2012-01-24          TTR-1252                  Removed Display and Element values for standards                             IntegrateCurDevl         SAndrew                2012-02-04
#
# Annotation  :
# This container is used by:
#
# FctsExCdExtr
# NascoExCdExtr
# NascoClmLnExtr

# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, trim, when

# COMMAND ----------
def run_CopyOfExcdPkey(
        df_Transform: DataFrame,
        params: Dict
) -> DataFrame:
    """
    Shared-container logic translated to PySpark.
    Parameters
    ----------
    df_Transform : DataFrame
        Input stream corresponding to the DataStage link 'Transform'.
    params : dict
        Runtime parameters and JDBC / path configs.

    Returns
    -------
    DataFrame
        Output stream corresponding to the DataStage link 'Key'.
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle   = params["CurrRunCycle"]
    adls_path      = params["adls_path"]

    # ------------------------------------------------------------------
    # Read lookup hashed file (as Parquet – scenario c)
    # ------------------------------------------------------------------
    df_lkup = (
        spark.read.parquet(f"{adls_path}/CopyOfExcdPkey_lkup.parquet")
    )

    # ------------------------------------------------------------------
    # Join input with lookup
    # ------------------------------------------------------------------
    join_cond = [
        col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
        col("Transform.EXCD_ID")   == col("lkup.EXCD_ID")
    ]

    df_join = (
        df_Transform.alias("Transform")
        .join(df_lkup.alias("lkup"), join_cond, "left")
    )

    # ------------------------------------------------------------------
    # Derive transformation columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_join
        # SK derivation (Surrogate key placeholder)
        .withColumn(
            "SK",
            when(trim(col("Transform.EXCD_ID")) == lit("UNK"), lit(1))
            .when(trim(col("Transform.EXCD_ID")) == lit("NA"),  lit(0))
            .when(
                col("lkup.EXCD_SK").isNull() | (trim(col("lkup.EXCD_SK")) == lit("")),
                lit(None)
            )
            .otherwise(col("lkup.EXCD_SK"))
        )
        # NewCrtRunCycExtcnSk derivation
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("lkup.EXCD_SK").isNull(), lit(CurrRunCycle))
            .otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
        )
        # Further column calculations / mappings
        .withColumn(
            "EXCD_ID",
            when(col("Transform.EXCD_ID").isNull(), lit("    "))
            .otherwise(col("Transform.EXCD_ID"))
        )
        .withColumn("CRT_RUN_CYC_EXCTN_SK", col("NewCrtRunCycExtcnSk"))
        .withColumn("EXCD_SK",               col("SK"))
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    )

    # ------------------------------------------------------------------
    # Surrogate-key generation (replaces KeyMgtGetNextValueConcurrent)
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'EXCD_SK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Build Key output DataFrame
    # ------------------------------------------------------------------
    df_key = df_enriched.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "EXCD_SK",
        "EXCD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "EXCD_HC_ADJ_CD",
        "EXCD_PT_LIAB_IND",
        "EXCD_PROV_ADJ_CD",
        "EXCD_REMIT_REMARK_CD",
        "EXCD_STS",
        "EXCD_TYPE",
        "EXCD_LONG_TX1",
        "EXCD_LONG_TX2",
        "EXCD_SH_TX"
    )

    # ------------------------------------------------------------------
    # updt link – rows where lookup did not find a match
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .filter(col("lkup.EXCD_ID").isNull())
        .select(
            "SRC_SYS_CD",
            "EXCD_ID",
            "CRT_RUN_CYC_EXCTN_SK",
            "EXCD_SK"
        )
    )

    # Deduplicate before writing
    df_updt = dedup_sort(
        df_updt,
        partition_cols=["SRC_SYS_CD", "EXCD_ID"],
        sort_cols=[("CRT_RUN_CYC_EXCTN_SK", "D")]
    )

    # Write as Parquet (scenario c)
    write_files(
        df_updt,
        f"{adls_path}/CopyOfExcdPkey_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return Key output stream
    # ------------------------------------------------------------------
    return df_key
# COMMAND ----------
