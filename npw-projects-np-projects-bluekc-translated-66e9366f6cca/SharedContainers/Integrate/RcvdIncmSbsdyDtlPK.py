# Databricks notebook source
# MAGIC %run ./Utility

# COMMAND ----------

# MAGIC %run ./Routine_Functions

# COMMAND ----------

"""
RcvdIncmSbsdyDtlPK
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Mbr Risk Mesr job
CALLED BY :  FctsBCBSRcvdIncmSbsdyDtlExtr

MODIFICATIONS:
Developer           Date                         Change Description
Santosh Bokka       2014-04-16   Initial program
Santosh Bokka       2014-06-17   Updated PROD_ID,RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD in natural key columns
Abhiram Dasarathy   2015-10-26   Business logic changes
Abhiram Dasarathy   2015-12-14   Added CMS_ENR_PAYMT_UNIQ_KEY
Abhiram Dasarathy   2016-01-28   Primary key value changes
Abhiram Dasarathy   2016-02-10   Structure change for K-tables

Annotations:
Used by FctsBCBSRcvdIncmSbsdyDtlExtr
IDS Primary Key Container for RcvdIncmSbsdyDtl
Hash file (hf_rcvd_incm_sbsdy_dtl_allcol) cleared in calling job
SQL joins temp table with key table to assign known keys
Temp table is truncated before load and runstat done after load
Load IDS temp. table
join primary key info with table info
update primary key table (RCVD_INCM_SBSDY_DTL) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
Assign primary surrogate key
"""

from pyspark.sql import DataFrame, functions as F

# COMMAND ----------

def run_RcvdIncmSbsdyDtlPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the RcvdIncmSbsdyDtlPK shared-container logic.
    
    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream corresponding to link 'AllCol'.
    df_Transform : DataFrame
        Input stream corresponding to link 'Transform'.
    params : dict
        Runtime parameters and JDBC configurations.
    
    Returns
    -------
    DataFrame
        Output stream corresponding to link 'Key'.
    """
    # --------------------------------------------------
    # Unpack parameters (only once)
    CurrRunCycle      = params["CurrRunCycle"]
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # --------------------------------------------------
    # Common key columns
    key_cols = [
        "CMS_ENR_PAYMT_UNIQ_KEY",
        "CMS_ENR_PAYMT_AMT_SEQ_NO",
        "RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD",
        "RCVD_INCM_SBSDY_ACCT_ACTVTY_CD",
        "SRC_SYS_CD",
    ]
    # --------------------------------------------------
    # hf_rcvd_incm_sbsdy_dtl_allcol  (scenario a) – deduplicate
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=key_cols,
        sort_cols=[]
    )
    # --------------------------------------------------
    # hf_rcvdincmsbsdydtl_pk_dedupe  (scenario a) – deduplicate
    df_ktemp = dedup_sort(
        df_Transform,
        partition_cols=key_cols,
        sort_cols=[]
    )
    # --------------------------------------------------
    # Read existing keys from database (IDS)
    extract_query_keys = f"""
        SELECT  RCVD_INCM_SBSDY_DTL_SK,
                CMS_ENR_PAYMT_UNIQ_KEY,
                CMS_ENR_PAYMT_AMT_SEQ_NO,
                RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD,
                RCVD_INCM_SBSDY_ACCT_ACTVTY_CD,
                SRC_SYS_CD,
                CRT_RUN_CYC_EXCTN_SK
        FROM    {IDSOwner}.K_RCVD_INCM_SBSDY_DTL
    """
    df_existing_keys = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query_keys)
            .load()
    )
    # --------------------------------------------------
    # Join temp keys with existing keys to produce W_Extract
    join_expr = [
        df_ktemp[col] == df_existing_keys[col] for col in key_cols
    ]
    df_matched = (
        df_ktemp.alias("w")
        .join(df_existing_keys.alias("k"), join_expr, "inner")
        .select(
            F.col("k.RCVD_INCM_SBSDY_DTL_SK"),
            *[F.col(f"w.{c}") for c in key_cols],
            F.col("k.CRT_RUN_CYC_EXCTN_SK")
        )
    )
    df_unmatched = (
        df_ktemp.alias("w2")
        .join(df_existing_keys.alias("k2"), join_expr, "left_anti")
        .select(
            F.lit(-1).alias("RCVD_INCM_SBSDY_DTL_SK"),
            *[F.col(f"w2.{c}") for c in key_cols],
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    df_extract = df_matched.unionByName(df_unmatched)
    # --------------------------------------------------
    # PrimaryKey transformer logic
    df_enriched = (
        df_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("RCVD_INCM_SBSDY_DTL_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("RCVD_INCM_SBSDY_DTL_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)
    # Persist updated surrogate key in main column
    df_enriched = df_enriched.withColumn("RCVD_INCM_SBSDY_DTL_SK", F.col("svSK"))
    # --------------------------------------------------
    # updt link – all rows
    df_updt = df_enriched.select(
        *key_cols,
        "CRT_RUN_CYC_EXCTN_SK",
        "RCVD_INCM_SBSDY_DTL_SK"
    )
    # --------------------------------------------------
    # NewKeys link – inserts only
    df_new_keys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            *key_cols,
            "CRT_RUN_CYC_EXCTN_SK",
            "RCVD_INCM_SBSDY_DTL_SK"
        )
    )
    # --------------------------------------------------
    # Keys link – for merge
    df_keys = df_enriched.select(
        "RCVD_INCM_SBSDY_DTL_SK",
        *key_cols,
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # --------------------------------------------------
    # Persist hashed-file (scenario c) as parquet
    path_updt = f"{adls_path}/RcvdIncmSbsdyDtlPK_updt.parquet"
    write_files(
        df_updt,
        path_updt,
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )
    # --------------------------------------------------
    # Persist sequential file for new keys
    path_seq = f"{adls_path}/load/K_RCVD_INCM_SBSDY_DTL.dat"
    write_files(
        df_new_keys,
        path_seq,
        ",",
        "overwrite",
        False,
        True,
        "\"",
        None
    )
    # --------------------------------------------------
    # Merge transformer – combine AllColOut with Keys
    join_expr_merge = [
        df_allcol_dedup[f"{c}"] == df_keys[f"{c}"] for c in key_cols
    ]
    df_output = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr_merge, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("all.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.RCVD_INCM_SBSDY_DTL_SK"),
            F.col("all.CMS_ENR_PAYMT_UNIQ_KEY"),
            F.col("all.CMS_ENR_PAYMT_AMT_SEQ_NO"),
            F.col("all.RCVD_INCM_SBSDY_DTL_PAYMT_TYP_CD"),
            F.col("all.RCVD_INCM_SBSDY_ACCT_ACTVTY_CD"),
            F.col("all.SRC_SYS_CD_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.BILL_ENTY_UNIQ_KEY"),
            F.col("all.CLS_PLN_ID"),
            F.col("all.FEE_DSCNT_SK"),
            F.col("all.FNCL_LOB_SK"),
            F.col("all.QHP_SK"),
            F.col("all.RCVD_INCM_SBSDY_LOB_CD_SK"),
            F.col("all.FIRST_YR_IN"),
            F.col("all.BILL_DUE_DT_SK"),
            F.col("all.CRT_DT_SK"),
            F.col("all.POSTED_DT_SK"),
            F.col("all.ERN_INCM_AMT"),
            F.col("all.RVNU_RCVD_AMT"),
            F.col("all.BILL_GRP_BILL_ENTY_UNIQ_KEY"),
            F.col("all.EXCH_MBR_ID"),
            F.col("all.EXCH_ASG_POL_ID"),
            F.col("all.EXCH_ASG_SUB_ID"),
            F.col("all.GL_NO"),
            F.col("all.PROD_ID"),
            F.col("all.PROD_BILL_CMPNT_ID"),
            F.col("all.QHP_ID")
        )
    )
    # --------------------------------------------------
    return df_output

# COMMAND ----------