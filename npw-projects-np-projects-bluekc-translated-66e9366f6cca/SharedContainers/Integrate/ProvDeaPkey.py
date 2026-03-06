# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
ProvDeaPkey – Shared Container
COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

CALLED BY:   EnclarityProvDeaExtr
DESCRIPTION: Container used for primary keying of the provider DEA jobs

MODIFICATIONS:
Developer                    Date            Change Description                               Project #        Code Reviewer          Date Reviewed
------------------           ------------    ---------------------------------------------    -------------    --------------------  -------------
Kalyan Neelam                2009-10-28      Original Programming                            TTR-525           Steph Goddard          2009-10-29
Kalyan Neelam                2015-10-30      Added new columns                               5403/TFS1048      Bhoomi Dasari          2015-11-09

Annotation:
This container is used in EnclarityProvDeaExtr
"""
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple

# COMMAND ----------
def run_ProvDeaPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the ProvDeaPkey shared-container logic.

    Parameters
    ----------
    df_Transform : DataFrame
        Upstream input stream mapped to the Container link “Transform”.
    params : dict
        Runtime properties and JDBC configurations already supplied by the caller.

    Returns
    -------
    DataFrame
        Output stream corresponding to the Container link “key”.
    """
    # ------------------------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    RunCycle            = params["RunCycle"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    ids_secret_name     = params["ids_secret_name"]

    # ------------------------------------------------------------------
    # Scenario-b hashed-file replacement – dummy table I/O
    # ------------------------------------------------------------------
    dummy_table_name = "<dummy_hf_prov_dea>"

    extract_query = f"""
    SELECT
        DEA_NO                         AS DEA_NO_lkp,
        CRT_RUN_CYC_EXCTN_SK           AS CRT_RUN_CYC_EXCTN_SK_lkp,
        PROV_DEA_SK                    AS PROV_DEA_SK_lkp
    FROM {dummy_table_name}
    """

    df_InkDEA = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ------------------------------------------------------------------
    # PrimaryKey transformer logic
    # ------------------------------------------------------------------
    join_expr = df_Transform["DEA_NO"] == df_InkDEA["DEA_NO_lkp"]

    df_enriched = (
        df_Transform.alias("t")
        .join(df_InkDEA.alias("lkp"), join_expr, "left")
        .withColumn("DEACheck",
            F.when(
                (F.length(F.trim(F.regexp_replace(F.col("t.DEA_NO"), "0", " "))) == 0) |
                (F.col("t.DEA_NO") == F.lit("UNK")),
                F.lit(1)
            ).when(
                F.col("t.DEA_NO") == F.lit("NA"),
                F.lit(2)
            ).otherwise(F.lit(3))
        )
        # Pre-sequence surrogate-key placeholder
        .withColumn(
            "PROV_DEA_SK",
            F.when(F.col("DEACheck") == 1, F.lit(0))
             .when(F.col("DEACheck") == 2, F.lit(1))
             .when(F.col("lkp.PROV_DEA_SK_lkp").isNull(), F.lit(None))
             .otherwise(F.col("lkp.PROV_DEA_SK_lkp"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(F.col("lkp.PROV_DEA_SK_lkp").isNull(), F.lit(RunCycle))
             .otherwise(F.col("lkp.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn(
            "DEARecycleSK",
            F.when(F.col("lkp.PROV_DEA_SK_lkp").isNull(), F.lit(RunCycle))
             .otherwise(F.col("lkp.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # ------------------------------------------------------------------
    # Surrogate-key assignment (replaces KeyMgtGetNextValueConcurrent)
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PROV_DEA_SK",<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Output link “key”
    # ------------------------------------------------------------------
    select_cols_key = [
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "PROV_DEA_SK",
        "DEA_NO",
        "NewCrtRunCycExtcnSk",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CMN_PRCT_CD",
        "CLM_TRANS_ADD_IN",
        "EFF_DT_SK",
        "TERM_DT_SK",
        "PROV_NM",
        "ADDR_LN_1",
        "ADDR_LN_2",
        "CITY_NM",
        "PROV_DEA_ST_CD",
        "POSTAL_CD",
        "NTNL_PROV_ID",
        "NTNL_PROV_ID_SPEC_DESC",
        "NTNL_PROV_ID_PROV_TYP_DESC",
        "NTNL_PROV_ID_PROV_CLS_DESC"
    ]

    df_key = df_enriched.select(*select_cols_key)

    # ------------------------------------------------------------------
    # Output link “DEAInsert” – insert rows into dummy table
    # ------------------------------------------------------------------
    df_insert = (
        df_enriched
        .filter(
            (F.col("PROV_DEA_SK") != 0) &
            (F.col("lkp.PROV_DEA_SK_lkp").isNull())
        )
        .select(
            F.trim(F.col("DEA_NO")).alias("DEA_NO"),
            F.col("DEARecycleSK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("PROV_DEA_SK")
        )
    )

    if df_insert.count() > 0:
        (
            df_insert.write
                .format("jdbc")
                .option("url", ids_jdbc_url)
                .option("dbtable", dummy_table_name)
                .mode("append")
                .options(**ids_jdbc_props)
                .save()
        )

    # ------------------------------------------------------------------
    # Return to caller
    # ------------------------------------------------------------------
    return df_key
# COMMAND ----------