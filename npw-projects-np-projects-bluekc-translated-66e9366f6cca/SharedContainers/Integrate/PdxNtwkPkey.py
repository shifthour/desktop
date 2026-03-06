# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: PdxNtwkPkey
------------------------------------------------------------------------------------------------------------------------
* VC LOGS *
^1_1 04/01/09 11:15:41 Batch  15067_40549 INIT bckcetl ids20 dsadm dsadm
^1_1 09/29/08 15:06:40 Batch  14883_54404 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 09/29/08 14:52:18 Batch  14883_53540 INIT bckcett testIDS dsadm bls for sa
^1_1 09/18/08 13:36:30 Batch  14872_48995 PROMOTE bckcett testIDS u03651 steph for Sharon
^1_1 09/18/08 13:26:42 Batch  14872_48429 INIT bckcett devlIDSnew u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Pharmacy Network job

CALLED BY : ESIPdxNtwkExtr and NABPPdxNtwkExtr
------------------------------------------------------------------------------------------------------------------------
This container is used in:
NABPPdxNtwkExtr
               ESIPdxNtwkExtr                                        
These programs need to be re-compiled when logic changes

Assign primary surrogate key
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_PdxNtwkPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Shared-container logic translated from DataStage to PySpark.
    Parameters
    ----------
    df_Transform : DataFrame
        Input link `Transform` from parent job.
    params : dict
        Runtime parameters supplied by the invoking notebook / pipeline.

    Returns
    -------
    DataFrame
        Output link `Key` – enriched with surrogate keys and flags.
    """

    # --------------------------------------------------
    # Unpack required runtime parameters (exactly once)
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    # --------------------------------------------------
    # Lookup against dummy table <dummy_hf_pdx_ntwk>
    # --------------------------------------------------
    lookup_query = """
        SELECT
              SRC_SYS_CD            AS SRC_SYS_CD_lkp
            , PROV_ID               AS PROV_ID_lkp
            , PDX_NTWK_CD           AS PDX_NTWK_CD_lkp
            , CRT_RUN_CYC_EXCTN_SK  AS CRT_RUN_CYC_EXCTN_SK_lkp
            , PDX_NTWK_SK           AS PDX_NTWK_SK_lkp
        FROM dummy_hf_pdx_ntwk
    """

    df_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", lookup_query)
            .load()
    )

    # --------------------------------------------------
    # Join source with lookup
    # --------------------------------------------------
    join_expr = (
        (F.col("src.SRC_SYS_CD")  == F.col("lk.SRC_SYS_CD_lkp")) &
        (F.col("src.PROV_ID")     == F.col("lk.PROV_ID_lkp")) &
        (F.col("src.PDX_NTWK_CD") == F.col("lk.PDX_NTWK_CD_lkp"))
    )

    df_joined = (
        df_Transform.alias("src")
            .join(df_lkup.alias("lk"), join_expr, "left")
    )

    # --------------------------------------------------
    # Derive columns, surrogate key placeholder
    # --------------------------------------------------
    df_enriched = (
        df_joined
            .withColumn(
                "PDX_NTWK_SK",
                F.when(
                    F.col("lk.PDX_NTWK_SK_lkp").isNotNull(),
                    F.col("lk.PDX_NTWK_SK_lkp")
                )
            )
            .withColumn(
                "CRT_RUN_CYC_EXCTN_SK",
                F.when(
                    F.col("lk.CRT_RUN_CYC_EXCTN_SK_lkp").isNotNull(),
                    F.col("lk.CRT_RUN_CYC_EXCTN_SK_lkp")
                ).otherwise(F.lit(CurrRunCycle))
            )
            .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
            # pass-through columns from source
            .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.col("src.JOB_EXCTN_RCRD_ERR_SK"))
            .withColumn("INSRT_UPDT_CD",           F.col("src.INSRT_UPDT_CD"))
            .withColumn("DISCARD_IN",              F.col("src.DISCARD_IN"))
            .withColumn("PASS_THRU_IN",            F.col("src.PASS_THRU_IN"))
            .withColumn("FIRST_RECYC_DT",          F.col("src.FIRST_RECYC_DT"))
            .withColumn("ERR_CT",                  F.col("src.ERR_CT"))
            .withColumn("RECYCLE_CT",              F.col("src.RECYCLE_CT"))
            .withColumn("SRC_SYS_CD",              F.col("src.SRC_SYS_CD"))
            .withColumn("PRI_KEY_STRING",          F.col("src.PRI_KEY_STRING"))
            .withColumn("PROV_ID",                 F.col("src.PROV_ID"))
            .withColumn("PDX_NTWK_CD",             F.col("src.PDX_NTWK_CD"))
            .withColumn("DIR_IN",                  F.col("src.DIR_IN"))
            .withColumn("EFF_DT",                  F.col("src.EFF_DT"))
            .withColumn("TERM_DT",                 F.col("src.TERM_DT"))
            .withColumn("PDX_NTWK_PRFRD_PROV_IN",  F.col("src.PDX_NTWK_PRFRD_PROV_IN"))
            .withColumn("ALWS_90_DAY_RX_IN",       F.col("src.ALWS_90_DAY_RX_IN"))
    )

    # --------------------------------------------------
    # Populate missing surrogate keys
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"PDX_NTWK_SK",<schema>,<secret_name>)

    # --------------------------------------------------
    # Prepare update rows for dummy table
    # --------------------------------------------------
    df_updt = (
        df_enriched
            .filter(F.col("lk.PDX_NTWK_SK_lkp").isNull())   # IsNull(lkup.PDX_NTWK_SK)
            .select(
                "SRC_SYS_CD",
                "PROV_ID",
                "PDX_NTWK_CD",
                F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
                "PDX_NTWK_SK"
            )
    )

    # --------------------------------------------------
    # Write back to dummy table (append)
    # --------------------------------------------------
    (
        df_updt.write.format("jdbc")
            .mode("append")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", "dummy_hf_pdx_ntwk")
            .save()
    )

    # --------------------------------------------------
    # Select final columns for output link `Key`
    # --------------------------------------------------
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
        "PDX_NTWK_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROV_ID",
        "PDX_NTWK_CD",
        "DIR_IN",
        "EFF_DT",
        "TERM_DT",
        "PDX_NTWK_PRFRD_PROV_IN",
        "ALWS_90_DAY_RX_IN"
    )

    return df_key