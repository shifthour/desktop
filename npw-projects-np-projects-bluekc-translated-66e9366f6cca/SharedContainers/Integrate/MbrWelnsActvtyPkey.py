# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Job Name   : MbrWelnsActvtyPkey
Job Type   : Server Job
Category   : DS_Integrate
Folder     : Shared Containers

Description:
Creates Primary key for IDS MBR_WELNS_ACTVTY table
Used in jobs:
IdsMbrWelnsActvtyAddtlExtr, HealthwaysMbrWelnsActvtyExtr,
HallmarkInsightsMbrWelnsActvtyExtr, GrpRptdActvtyMbrWelnsActvtyExtr

Annotations:
Assign primary surrogate key
These programs need to be re-compiled when logic changes
"""
from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit
# COMMAND ----------
def run_MbrWelnsActvtyPkey(df_Transform: DataFrame, params: Dict) -> DataFrame:
    # ---- parameter unpacking ----
    CurrRunCycle        = params["CurrRunCycle"]
    CurrDateTime        = params["CurrDateTime"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    RunID               = params["RunID"]
    jdbc_url            = params["jdbc_url"]
    jdbc_props          = params["jdbc_props"]

    # ---- read dummy table replacing hashed file ----
    extract_query = """
        SELECT
            MBR_UNIQ_KEY            AS MBR_UNIQ_KEY_lkp,
            WELNS_ACTVTY_ID         AS WELNS_ACTVTY_ID_lkp,
            WELNS_ACTVTY_DT_SK      AS WELNS_ACTVTY_DT_SK_lkp,
            SEQ_NO                  AS SEQ_NO_lkp,
            SRC_SYS_CD              AS SRC_SYS_CD_lkp,
            CRT_RUN_CYC_EXCTN_SK    AS CRT_RUN_CYC_EXCTN_SK_lkp,
            MBR_WELNS_ACTVTY_SK     AS MBR_WELNS_ACTVTY_SK_lkp
        FROM dummy_hf_mbr_welns_actvty
    """
    df_hf_mbr_welns_actvty_lkp = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ---- join lookup ----
    join_expr = (
        (df_Transform["MBR_UNIQ_KEY"]         == df_hf_mbr_welns_actvty_lkp["MBR_UNIQ_KEY_lkp"]) &
        (df_Transform["WELNS_ACTVTY_ID"]      == df_hf_mbr_welns_actvty_lkp["WELNS_ACTVTY_ID_lkp"]) &
        (df_Transform["WELNS_ACTVTY_DT_SK"]   == df_hf_mbr_welns_actvty_lkp["WELNS_ACTVTY_DT_SK_lkp"]) &
        (df_Transform["SEQ_NO"]               == df_hf_mbr_welns_actvty_lkp["SEQ_NO_lkp"]) &
        (df_Transform["SRC_SYS_CD"]           == df_hf_mbr_welns_actvty_lkp["SRC_SYS_CD_lkp"])
    )

    df_enriched = (
        df_Transform.alias("transform")
        .join(df_hf_mbr_welns_actvty_lkp.alias("lkup"), join_expr, "left")
    )

    # ---- derive columns ----
    df_enriched = (
        df_enriched
        .withColumn("MBR_WELNS_ACTVTY_SK", col("MBR_WELNS_ACTVTY_SK_lkp"))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("MBR_WELNS_ACTVTY_SK_lkp").isNull(), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(CurrRunCycle))
    )

    # ---- surrogate key generation ----
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_WELNS_ACTVTY_SK",<schema>,<secret_name>)

    # ---- prepare updates to dummy table ----
    df_updt = (
        df_enriched
        .filter(col("MBR_WELNS_ACTVTY_SK_lkp").isNull())
        .select(
            "MBR_UNIQ_KEY",
            "WELNS_ACTVTY_ID",
            "WELNS_ACTVTY_DT_SK",
            "SEQ_NO",
            "SRC_SYS_CD",
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            "MBR_WELNS_ACTVTY_SK"
        )
    )

    (
        df_updt.write.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("dbtable", "dummy_hf_mbr_welns_actvty")
            .mode("append")
            .save()
    )

    # ---- final key output ----
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
        "MBR_WELNS_ACTVTY_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_UNIQ_KEY",
        "WELNS_ACTVTY_DT_SK",
        "WELNS_ACTVTY_ID",
        "SEQ_NO",
        "SRC_SYS_CD_SK",
        "MBR_SK",
        "ACTVTY_SELF_RPTD_IN",
        "WELNS_ACTVTY_SENT_IN",
        "WELNS_ACTVTY_RPTD_DT_SK",
        "WELNS_ACTVTY_SENT_DT_SK",
        "WELNS_ACTVTY_BNKD_PT_NO",
        "ACTVTY_PT_QTY",
        "VNDR_ORDER_ID",
        "WELNS_ACTVTY_NM",
        "PT_APRV_AS_EXCPT_IN"
    )

    return df_key
# COMMAND ----------