
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Job Name      : IncmTransPK
Job Category  : DS_Integrate
Folder Path   : Shared Containers/PrimaryKey

DESCRIPTION:   
Shared container used for Primary Keying of Income Transaction job

CALLED BY : PSIncmTransExtr

VC LOG HISTORY (truncated):
* 02/06/09 PROMOTE bckcetl ids20 dsadm rc for steph
* 01/23/09 PROMOTE bckcett testIDS u03651 steph - primary key
* 01/08/09 INIT bckcett devlIDS u03651 steffy

COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

MODIFICATIONS:
Developer      Date         Change Description                       Project #   Code Reviewer   Date Reviewed
Parik          2008-08-19   Initial program                          3567        Steph Goddard   08/22/2008
"""

# This container is used in:
#   PSIncmTransExtr
#
# Hash file (hf_incm_trans_allcol) cleared in calling program
# join primary key info with table info
# update primary key table (K_INCM_TRANS) with new keys created today
# primary key hash file only contains current run keys and is cleared before writing
# SQL joins temp table with key table to assign known keys
# Temp table is truncated before load and runstats done after load
# Load IDS temp. table

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_IncmTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Translated shared-container logic from DataStage job IncmTransPK.
    
    Parameters
    ----------
    df_AllCol : DataFrame
        Incoming rows containing all Income-Transaction columns.
    df_Transform : DataFrame
        Incoming rows (SRC_SYS_CD_SK, INCM_TRANS_CK, ACCTG_DT_SK) for key assignment.
    params : dict
        Runtime parameters and secrets (see notebook guidelines).
    
    Returns
    -------
    DataFrame
        Final “Key” output stream after primary-key enrichment and merge.
    """

    # ------------------------------------------------------------------
    # Unpack parameters  (exactly once, no $-prefixed suffix variables)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_incm_trans_allcol  (Scenario-a intermediate hash file)
    #   Replace with dedup logic
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "INCM_TRANS_CK", "ACCTG_DT_SK"],
        []
    )

    # ------------------------------------------------------------------
    # Stage: K_INCM_TRANS_TEMP  (DB logic recreated with Spark join)
    # ------------------------------------------------------------------
    query_k_incm_trans = (
        f"SELECT INCM_TRANS_SK, SRC_SYS_CD_SK, INCM_TRANS_CK, "
        f"ACCTG_DT_SK, CRT_RUN_CYC_EXCTN_SK "
        f"FROM {IDSOwner}.K_INCM_TRANS"
    )

    df_k_incm_trans = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", query_k_incm_trans)
        .load()
    )

    join_expr = (
        (F.col("w.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("w.INCM_TRANS_CK") == F.col("k.INCM_TRANS_CK")) &
        (F.col("w.ACCTG_DT_SK") == F.col("k.ACCTG_DT_SK"))
    )

    df_W_Extract = (
        df_Transform.alias("w")
        .join(df_k_incm_trans.alias("k"), join_expr, "left")
        .select(
            F.when(F.col("k.INCM_TRANS_SK").isNull(), F.lit(-1))
             .otherwise(F.col("k.INCM_TRANS_SK")).alias("INCM_TRANS_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.INCM_TRANS_CK"),
            F.col("w.ACCTG_DT_SK"),
            F.when(F.col("k.INCM_TRANS_SK").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey  (Transformer logic)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("INCM_TRANS_SK") == F.lit(-1), F.lit("I"))
             .otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    )

    # Surrogate-key management (mandatory placeholder call)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "INCM_TRANS_SK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # Prepare output links from PrimaryKey transformer
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "INCM_TRANS_CK",
        "ACCTG_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "INCM_TRANS_SK"
    )

    df_newkeys = (
        df_enriched
        .filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "INCM_TRANS_CK",
            "ACCTG_DT_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "INCM_TRANS_SK"
        )
    )

    df_keys = df_enriched.select(
        "INCM_TRANS_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "INCM_TRANS_CK",
        "ACCTG_DT_SK",
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        "CRT_RUN_CYC_EXCTN_SK"
    )

    # ------------------------------------------------------------------
    # Stage: hf_incm_trans  (Scenario-c → write Parquet)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/IncmTransPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: K_INCM_TRANS  (Sequential file write)
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_INCM_TRANS.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: Merge  (join Keys ↔ AllColOut)
    # ------------------------------------------------------------------
    merge_join_expr = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.INCM_TRANS_CK") == F.col("k.INCM_TRANS_CK")) &
        (F.col("all.ACCTG_DT_SK") == F.col("k.ACCTG_DT_SK"))
    )

    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), merge_join_expr, "left")
    )

    df_Key = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN").alias("DISCARD_IN"),
        F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("all.ERR_CT").alias("ERR_CT"),
        F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("k.INCM_TRANS_SK").alias("INCM_TRANS_SK"),
        F.col("all.INCM_TRANS_CK").alias("INCM_TRANS_CK"),
        F.col("all.ACCTG_DT_SK").alias("ACCTG_DT_SK"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.GRP").alias("GRP"),
        F.col("all.PROD").alias("PROD"),
        F.col("all.SUBGRP").alias("SUBGRP"),
        F.col("all.BILL_ENTY_CK").alias("BILL_ENTY_CK"),
        F.col("all.FNCL_LOB").alias("FNCL_LOB"),
        F.col("all.ORIG_FNCL_LOB").alias("ORIG_FNCL_LOB"),
        F.col("all.RVSED_FNCL_LOB").alias("RVSED_FNCL_LOB"),
        F.col("all.INCM_TRANS_LOB_CD").alias("INCM_TRANS_LOB_CD"),
        F.col("all.FIRST_YR_IN").alias("FIRST_YR_IN"),
        F.col("all.BILL_DUE_DT").alias("BILL_DUE_DT"),
        F.col("all.CRT_DT").alias("CRT_DT"),
        F.col("all.POSTING_DT").alias("POSTING_DT"),
        F.col("all.BILL_CMPNT_ID").alias("BILL_CMPNT_ID")
    )

    return df_Key
