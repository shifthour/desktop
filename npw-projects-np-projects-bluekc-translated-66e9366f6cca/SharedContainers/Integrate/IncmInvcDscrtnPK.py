
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# Databricks notebook source
"""
Shared Container  : IncmInvcDscrtnPK
Description       : Shared container used for Primary Keying of Income Invoice Discretion job
Called By         : FctsIncomeInvcDscrtnExtr

MODIFICATIONS:
Developer            Date            Change Description
------------------   -------------   ----------------------------------------------------------
Bhoomi Dasari        2008-09-28      Initial program
Kimberly Doty        2010-09-30      Added DPNDT_CT, SUB_CT, SELF_BILL_LIFE_IN
Goutham Kalidindi    2021-03-24      Changed datatype length for BLIV_ID

ANNOTATIONS:
- IDS Primary Key Container for Income Invoice Discretion
- Used by FctsIncomeInvcDscrtnExtr
- primary key hash file only contains current run keys and is cleared before writing
- update primary key table (K_INVC_DSCRTN) with new keys created today
- Assign primary surrogate key
- Hash file (hf_invc_dscrtn_allcol) cleared in calling job
- join primary key info with table info
- Load IDS temp. table
- Temp table is truncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Databricks notebook source
def run_IncmInvcDscrtnPK(
    df_AllCol: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the primary-key generation logic originally implemented in the
    DataStage shared container “IncmInvcDscrtnPK”.
    
    Parameters
    ----------
    df_AllCol : DataFrame
        Incoming stream carrying all columns for invoice discretion records.
    params : dict
        Runtime parameters and pre-resolved environment values.
    
    Returns
    -------
    DataFrame
        Enriched stream containing primary keys and related metadata.
    """

    # ------------------------------------------------------------------
    # Unpack required parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # SCENARIO-A HASHED FILE  : hf_invc_dscrtn_allcol
    # Replace with deduplication logic
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "BILL_INVC_ID", "BLDI_SEQ_NO"],
        [("SRC_SYS_CD_SK", "A")]
    ).withColumn(
        "SEQ_NO",
        F.col("BLDI_SEQ_NO")
    )

    # ------------------------------------------------------------------
    # Load IDS temporary table : K_INVC_DSCRTN_TEMP
    # ------------------------------------------------------------------
    df_temp_load = (
        df_allcol_dedup
        .select("SRC_SYS_CD_SK", "BILL_INVC_ID", "SEQ_NO")
        .distinct()
    )

    # Truncate temp table before load
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_INVC_DSCRTN_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # Append data into temp table
    (
        df_temp_load
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbTable", f"{IDSOwner}.K_INVC_DSCRTN_TEMP")
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # Extract known and unknown keys (W_Extract link)
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT
        k.INVC_DSCRTN_SK,
        w.SRC_SYS_CD_SK,
        w.BILL_INVC_ID,
        w.SEQ_NO,
        k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_INVC_DSCRTN_TEMP w
    JOIN {IDSOwner}.K_INVC_DSCRTN      k
      ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
     AND w.BILL_INVC_ID  = k.BILL_INVC_ID
     AND w.SEQ_NO        = k.SEQ_NO
    UNION
    SELECT
        -1                                    AS INVC_DSCRTN_SK,
        w2.SRC_SYS_CD_SK,
        w2.BILL_INVC_ID,
        w2.SEQ_NO,
        {CurrRunCycle}                        AS CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_INVC_DSCRTN_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
          FROM {IDSOwner}.K_INVC_DSCRTN k2
         WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
           AND w2.BILL_INVC_ID  = k2.BILL_INVC_ID
           AND w2.SEQ_NO        = k2.SEQ_NO
    )
    """

    df_w_extract = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ------------------------------------------------------------------
    # PRIMARY KEY TRANSFORMATION
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("INVC_DSCRTN_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
    )

    # Initialize surrogate key column
    df_enriched = (
        df_enriched
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("INVC_DSCRTN_SK"))
        )
    )

    # Surrogate-key generation for new rows
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # Other derivations
    df_enriched = (
        df_enriched
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svBillInvcId",
            F.trim(F.col("BILL_INVC_ID"))
        )
    )

    # ------------------------------------------------------------------
    # Split outputs from PrimaryKey transformer
    # ------------------------------------------------------------------
    # updt link (for hf_invc_dscrtn)
    df_updt = (
        df_enriched.select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svBillInvcId").alias("BILL_INVC_ID"),
            F.col("SEQ_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("INVC_DSCRTN_SK")
        )
    )

    # NewKeys link (for sequential file)
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            F.col("svBillInvcId").alias("BILL_INVC_ID"),
            "SEQ_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("INVC_DSCRTN_SK")
        )
    )

    # Keys link (for merge with all-column stream)
    df_keys = (
        df_enriched.select(
            F.col("svSK").alias("INVC_DSCRTN_SK"),
            "SRC_SYS_CD_SK",
            F.col("svBillInvcId").alias("BILL_INVC_ID"),
            "SEQ_NO",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # MERGE WITH ALL-COLUMN DATA
    # ------------------------------------------------------------------
    df_merge = (
        df_allcol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            ["SRC_SYS_CD_SK", "BILL_INVC_ID", "SEQ_NO"],
            "left"
        )
    )

    df_key = (
        df_merge.select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.INVC_DSCRTN_SK").alias("INVC_SK"),
            F.col("k.BILL_INVC_ID"),
            F.col("k.SEQ_NO").alias("BLDI_SEQ_NO"),
            F.col("all.CLS_ID"),
            F.col("all.CLS_PLN_ID"),
            F.col("all.GRGR_ID"),
            F.col("all.FEE_DSCNT_ID"),
            F.col("all.PROD_ID"),
            F.col("all.PROD_BILL_CMPNT_ID"),
            F.col("all.SUBGRP_ID"),
            F.col("all.INVC_DSCRTN_BILL_DISP_CD"),
            F.col("all.INVC_DSCRTN_PRM_FEE_CD"),
            F.col("all.DUE_DT"),
            F.col("all.INVC_DSCRTN_BEG_DT_SK"),
            F.col("all.INVC_DSCRTN_END_DT_SK"),
            F.col("all.DPNDT_PRM_AMT"),
            F.col("all.FEE_DSCNT_AMT"),
            F.col("all.SUB_PRM_AMT"),
            F.col("all.DSCRTN_MO_QTY"),
            F.col("all.DSCRTN_DESC"),
            F.col("all.DSCRTN_PRSN_ID_TX"),
            F.col("all.DSCRTN_SH_DESC"),
            F.col("all.SBSB_CK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.DPNDT_CT"),
            F.col("all.SUB_CT"),
            F.col("all.SELF_BILL_LIFE_IN")
        )
    )

    # ------------------------------------------------------------------
    # EGRESS: WRITE SEQUENTIAL FILE & PARQUET (HASHED FILE REPLACEMENT)
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_INVC_DSCRTN.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    write_files(
        df_updt,
        f"{adls_path}/IncmInvcDscrtnPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_key
