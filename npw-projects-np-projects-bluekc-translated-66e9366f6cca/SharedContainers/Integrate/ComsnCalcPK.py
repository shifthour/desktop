
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
ComsnCalcPK – Shared container used for Primary Keying of Comm Calc job

* VC LOGS *
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:   Shared container used for Primary Keying of Comm Calc job
CALLED BY : FctsComsnCalcExtr

IDS Primary Key Container for Commission Calc
Used by  FctsComsnCalcExtr
Hash file (hf_comsn_calc_allcol) cleared in calling job
SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
Load IDS temp. table
join primary key info with table info
update primary key table (K_COMSN_CALC) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
Assign primary surrogate key
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# COMMAND ----------
def run_ComsnCalcPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # Unpack parameters -----------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------------------------

    # Stage: hf_comsn_calc_allcol (scenario a - dedup only) ------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "COMSN_BILL_REL_UNIQ_KEY", "COMSN_CALC_LOB_CD_SK", "COMSN_SCHD_TIER_UNIQ_KEY"],
        [("SRC_SYS_CD_SK", "D")]
    )

    # Stage: K_COMSN_CALC_TEMP -----------------------------------------------------------
    # Load incoming rows into temporary table
    df_Transform.select(
        "SRC_SYS_CD_SK",
        "COMSN_BILL_REL_UNIQ_KEY",
        "COMSN_CALC_LOB_CD",
        "COMSN_SCHD_TIER_UNIQ_KEY"
    ).write.mode("append").format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("dbtable", f"{IDSOwner}.K_COMSN_CALC_TEMP") \
        .save()

    # Run runstats
    after_sql = f"""CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_COMSN_CALC_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')"""
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # Extract query (W_Extract) ----------------------------------------------------------
    extract_query = f"""
    SELECT  k.COMSN_CALC_SK,
            w.SRC_SYS_CD_SK,
            w.COMSN_BILL_REL_UNIQ_KEY,
            w.COMSN_CALC_LOB_CD,
            w.COMSN_SCHD_TIER_UNIQ_KEY,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_COMSN_CALC_TEMP w,
         {IDSOwner}.K_COMSN_CALC k
    WHERE w.SRC_SYS_CD_SK              = k.SRC_SYS_CD_SK
      AND w.COMSN_BILL_REL_UNIQ_KEY    = k.COMSN_BILL_REL_UNIQ_KEY
      AND w.COMSN_CALC_LOB_CD          = k.COMSN_CALC_LOB_CD
      AND w.COMSN_SCHD_TIER_UNIQ_KEY   = k.COMSN_SCHD_TIER_UNIQ_KEY
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.COMSN_BILL_REL_UNIQ_KEY,
           w2.COMSN_CALC_LOB_CD,
           w2.COMSN_SCHD_TIER_UNIQ_KEY,
           {CurrRunCycle}
    FROM {IDSOwner}.K_COMSN_CALC_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_COMSN_CALC k2
        WHERE w2.SRC_SYS_CD_SK            = k2.SRC_SYS_CD_SK
          AND w2.COMSN_CALC_LOB_CD        = k2.COMSN_CALC_LOB_CD
          AND w2.COMSN_BILL_REL_UNIQ_KEY  = k2.COMSN_BILL_REL_UNIQ_KEY
          AND w2.COMSN_SCHD_TIER_UNIQ_KEY = k2.COMSN_SCHD_TIER_UNIQ_KEY
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # Stage: PrimaryKey Transformer ------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", F.when(F.col("COMSN_CALC_SK") == -1, F.lit("I")).otherwise(F.lit("U")))
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn("svSK", F.col("COMSN_CALC_SK"))
    )

    # SurrogateKeyGen invocation ---------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # Continue derived columns after surrogate key generation
    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnSk",
        F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )

    # updt link (to hf_comsn_calc) -------------------------------------------------------
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "COMSN_BILL_REL_UNIQ_KEY",
        "COMSN_CALC_LOB_CD",
        "COMSN_SCHD_TIER_UNIQ_KEY",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("COMSN_CALC_SK")
    )

    # Write to parquet for hf_comsn_calc
    write_files(
        df_updt,
        f"{adls_path}/ComsnCalcPK_updt.parquet",
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # NewKeys link (to sequential file) --------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "COMSN_BILL_REL_UNIQ_KEY",
            "COMSN_CALC_LOB_CD",
            "COMSN_SCHD_TIER_UNIQ_KEY",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("COMSN_CALC_SK")
        )
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_COMSN_CALC.dat",
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # Keys link --------------------------------------------------------------------------
    df_keys = df_enriched.select(
        F.col("svSK").alias("COMSN_CALC_SK"),
        "SRC_SYS_CD_SK",
        "COMSN_BILL_REL_UNIQ_KEY",
        "COMSN_CALC_LOB_CD",
        "COMSN_SCHD_TIER_UNIQ_KEY",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # Stage: Merge Transformer -----------------------------------------------------------
    join_expr = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.COMSN_BILL_REL_UNIQ_KEY") == F.col("k.COMSN_BILL_REL_UNIQ_KEY")) &
        (F.col("all.COMSN_SCHD_TIER_UNIQ_KEY") == F.col("k.COMSN_SCHD_TIER_UNIQ_KEY"))
    )

    df_merge = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
    )

    df_Key = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN").alias("DISCARD_IN"),
        F.col("all.PASS_THRU_IN").alias("FIRST_RECYC_DT"),
        F.col("all.FIRST_RECYC_DT").alias("PASS_THRU_IN"),
        F.col("all.ERR_CT").alias("ERR_CT"),
        F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("k.COMSN_CALC_SK").alias("COMSN_CALC_SK"),
        F.col("all.COMSN_BILL_REL_UNIQ_KEY").alias("COMSN_BILL_REL_UNIQ_KEY"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.COMSN_CALC_LOB_CD_SK").alias("LOBD_ID"),
        F.col("all.COMSN_SCHD_TIER_UNIQ_KEY").alias("COMSN_SCHD_TIER_UNIQ_KEY"),
        F.col("all.FNCL_COMSN_RPTNG_PRCS_IN").alias("FNCL_COMSN_RPTNG_PRCS_IN"),
        F.col("all.CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("all.CALC_COMSN_AMT").alias("CALC_COMSN_AMT"),
        F.col("all.CALC_COMSN_ADV_AMT").alias("CALC_COMSN_ADV_AMT"),
        F.col("all.COMSN_BSS_AMT").alias("COMSN_BSS_AMT"),
        F.col("all.OVRD_AMT").alias("OVRD_AMT"),
        F.col("all.INCM_AMT").alias("INCM_AMT"),
        F.col("all.BILL_CT").alias("BILL_CT")
    )

    return df_Key
# COMMAND ----------
