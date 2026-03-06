# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ---------- 

# COMMAND ----------
"""
ComsnIncmRcpntPK – Shared container used for Primary Keying of Comm Bill Incm Rcpnt job.

VC LOGS
^1_1 03/04/09 10:05:04 Batch  15039_36308 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 03/04/09 09:57:49 Batch  15039_35874 INIT bckcett testIDS dsadm BLS FOR SA
^1_1 01/13/09 09:00:06 Batch  14989_32412 PROMOTE bckcett testIDS u03651 steph for Sharon
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Comm Bill Incm Rcpnt job

CALLED BY : FctsBillIncmRcptExtr

PROCESSING:    

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-09-22             Initial program                                                               3567 Primary Key    devlIDS                                  Steph Goddard          10/03/2008

Annotations:
1. IDS Primary Key Container for Commission Bill Incm Rcpnt
2. Used by FctsBillIncmRcptExtr
3. primary key hash file only contains current run keys and is cleared before writing
4. update primary key table (K_BILL_INCM_RCPT) with new keys created today
5. Assign primary surrogate key
6. Hash file (hf_bill_incm_rcpt_allcol) cleared in calling job
7. join primary key info with table info
8. Load IDS temp. table
9. Temp table is tuncated before load and runstat done after load
10. SQL joins temp table with key table to assign known keys
"""

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_ComsnIncmRcpntPK(
    df_AllCol: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes shared-container logic for ComsnIncmRcpntPK.
    
    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream corresponding to link “AllCol”.
    params : dict
        Runtime parameters and environmental values already supplied by the caller.
    
    Returns
    -------
    DataFrame
        Output stream corresponding to link “Key”.
    """

    # ------------------------------------------------------------------
    # Unpack required parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle         = params["CurrRunCycle"]
    SrcSysCd             = params.get("SrcSysCd", "")
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Step-1  : replicate hashed-file “hf_bill_incm_rcpt_allcol” (scenario a)
    #           using deduplication on key columns
    # ------------------------------------------------------------------
    df_allcol_out = dedup_sort(
        df_AllCol,
        partition_cols=[
            "SRC_SYS_CD_SK",
            "BLEI_CK",
            "BLBL_DUE_DT",
            "BLCN_ID",
            "BLAC_CREATE_DTM"
        ],
        sort_cols=[("BLAC_CREATE_DTM", "D")]
    )

    # ------------------------------------------------------------------
    # Step-2  : obtain W_Extract dataset from IDS via JDBC
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT  k.BILL_INCM_RCPT_SK,
            w.SRC_SYS_CD_SK,
            w.BILL_ENTY_UNIQ_KEY,
            w.BILL_DUE_DT_SK,
            w.BILL_CNTR_ID,
            w.CRT_DTM,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_BILL_INCM_RCPT_TEMP w,
         {IDSOwner}.K_BILL_INCM_RCPT      k
    WHERE w.SRC_SYS_CD_SK          = k.SRC_SYS_CD_SK
      AND w.BILL_ENTY_UNIQ_KEY     = k.BILL_ENTY_UNIQ_KEY
      AND w.BILL_DUE_DT_SK         = k.BILL_DUE_DT_SK
      AND w.BILL_CNTR_ID           = k.BILL_CNTR_ID
      AND w.CRT_DTM                = k.CRT_DTM
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.BILL_ENTY_UNIQ_KEY,
           w2.BILL_DUE_DT_SK,
           w2.BILL_CNTR_ID,
           w2.CRT_DTM,
           {CurrRunCycle}
    FROM {IDSOwner}.K_BILL_INCM_RCPT_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_BILL_INCM_RCPT k2
        WHERE w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
          AND w2.BILL_DUE_DT_SK      = k2.BILL_DUE_DT_SK
          AND w2.BILL_ENTY_UNIQ_KEY  = k2.BILL_ENTY_UNIQ_KEY
          AND w2.BILL_CNTR_ID        = k2.BILL_CNTR_ID
          AND w2.CRT_DTM             = k2.CRT_DTM
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # ------------------------------------------------------------------
    # Step-3  : PrimaryKey transformer logic – enrich and derive columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    F.when(F.col("BILL_INCM_RCPT_SK") == F.lit(-1), F.lit("I"))
                     .otherwise(F.lit("U")))
        .withColumn("SRC_SYS_CD", F.lit("FACETS"))
        .withColumn("svBillCntrId", F.trim(F.col("BILL_CNTR_ID")))
        .withColumn("svSK",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None))
                     .otherwise(F.col("BILL_INCM_RCPT_SK")))
        .withColumn("svCrtRunCycExctnSk",
                    F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
                     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")))
    )

    # ------------------------------------------------------------------
    # Step-4  : surrogate-key generation
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,svSK,<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Step-5  : updt (for hf_bill_incm_rcpt) & new keys (for K_BILL_INCM_RCPT.dat)
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched.select(
            F.col("SRC_SYS_CD"),
            "BILL_ENTY_UNIQ_KEY",
            "BILL_DUE_DT_SK",
            F.col("svBillCntrId").alias("BILL_CNTR_ID"),
            "CRT_DTM",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("BILL_INCM_RCPT_SK")
        )
    )

    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
                   .select(
                       "SRC_SYS_CD_SK",
                       "BILL_ENTY_UNIQ_KEY",
                       "BILL_DUE_DT_SK",
                       F.col("svBillCntrId").alias("BILL_CNTR_ID"),
                       "CRT_DTM",
                       F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
                       F.col("svSK").alias("BILL_INCM_RCPT_SK")
                   )
    )

    # ------------------------------------------------------------------
    # Step-6  : Keys dataset for downstream merge
    # ------------------------------------------------------------------
    df_keys = (
        df_enriched.select(
            F.col("svSK").alias("BILL_INCM_RCPT_SK"),
            "SRC_SYS_CD_SK",
            "BILL_ENTY_UNIQ_KEY",
            "BILL_DUE_DT_SK",
            F.col("svBillCntrId").alias("BILL_CNTR_ID"),
            "CRT_DTM",
            "SRC_SYS_CD",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Step-7  : Merge logic – join AllColOut with Keys
    # ------------------------------------------------------------------
    join_expr = (
        (F.col("AllColOut.SRC_SYS_CD_SK") == F.col("Keys.SRC_SYS_CD_SK")) &
        (F.col("AllColOut.BLEI_CK")       == F.col("Keys.BILL_ENTY_UNIQ_KEY")) &
        (F.col("AllColOut.BLBL_DUE_DT")   == F.col("Keys.BILL_DUE_DT_SK")) &
        (F.col("AllColOut.BLCN_ID")       == F.col("Keys.BILL_CNTR_ID")) &
        (F.col("AllColOut.BLAC_CREATE_DTM") == F.col("Keys.CRT_DTM"))
    )

    df_merge = (
        df_allcol_out.alias("AllColOut")
                     .join(df_keys.alias("Keys"), join_expr, "left")
                     .select(
                         F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK"),
                         F.col("Keys.INSRT_UPDT_CD"),
                         F.col("AllColOut.DISCARD_IN"),
                         F.col("AllColOut.PASS_THRU_IN"),
                         F.col("AllColOut.FIRST_RECYC_DT"),
                         F.col("AllColOut.ERR_CT"),
                         F.col("AllColOut.RECYCLE_CT"),
                         F.col("Keys.SRC_SYS_CD"),
                         F.col("AllColOut.PRI_KEY_STRING"),
                         F.col("Keys.BILL_INCM_RCPT_SK"),
                         F.col("AllColOut.BLEI_CK").alias("BILL_ENTY_UNIQ_KEY"),
                         F.col("AllColOut.BLBL_DUE_DT").alias("BILL_DUE_DT_SK"),
                         F.col("AllColOut.BLCN_ID").alias("BILL_CNTR_ID"),
                         F.col("AllColOut.BLAC_CREATE_DTM").alias("CRT_DTM"),
                         F.col("Keys.CRT_RUN_CYC_EXCTN_SK"),
                         F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
                         F.col("AllColOut.CSPI_ID"),
                         F.col("AllColOut.PMFA_ID"),
                         F.col("AllColOut.BLAC_ACCT_CAT"),
                         F.col("AllColOut.PDPD_ID"),
                         F.col("AllColOut.ACGL_ACTIVITY"),
                         F.col("AllColOut.ACGL_TYPE"),
                         F.col("AllColOut.BLAC_SOURCE"),
                         F.col("AllColOut.BLAC_TYPE"),
                         F.col("AllColOut.LOBD_ID"),
                         F.col("AllColOut.BLAC_YR1_IND").alias("FIRST_YR_IN"),
                         F.col("AllColOut.CRT_DT_SK"),
                         F.col("AllColOut.BLAC_POSTING_DT").alias("POSTED_DT_SK"),
                         F.col("AllColOut.ERN_INCM_AMT"),
                         F.col("AllColOut.RVNU_RCVD_AMT"),
                         F.col("AllColOut.ACAD_GEN_LDGR_NO").alias("GL_NO"),
                         F.col("AllColOut.PDBL_ID").alias("PROD_BILL_CMPNT_ID")
                     )
    )

    # ------------------------------------------------------------------
    # Step-8  : Persist outputs – parquet & sequential file
    # ------------------------------------------------------------------
    parquet_path_updt = f"{adls_path}/ComsnIncmRcpntPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    seq_file_path = f"{adls_path}/load/K_BILL_INCM_RCPT.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return final output DataFrame
    # ------------------------------------------------------------------
    return df_merge

# COMMAND ----------