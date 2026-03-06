# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 


DESCRIPTION:   Shared container used for Primary Keying of Claim Line Description

PROCESSING:    Called by FctsClmLnDescExtr




MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Ralph Tucker     2008-08-08               Initial program                                                               3657 Primary Key    devlIDS                                 Steph Goddrad          08/11/2008
Rick Henry         2012-05-01               added diag code to end of file                                      4896 ICD                                                               SAndrew                   2012-05-17
"""

# Temp table is tuncated before load and runstat done after load
# Load IDS temp. table
# Writing Sequential File to ../key
# Assign primary surrogate key
# join primary key info with table info
# update primary key table (K_CLM_LN_DIAG) with new keys created today
# primary key hash file only contains current run keys and is cleared before writing
# SQL joins temp table with key table to assign known keys
# This container is used in:
# FctsClmLnDiagExtr
# NascoClmLnDiagExtr
#
# These programs need to be re-compiled when logic changes
# IDS Primary Key Container for Claim Line Description
# Hash file (hf_clm_ln_desc_allcol) cleared in the calling program


def run_ClmLnDiagPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    PySpark translation of DataStage shared container ClmLnDiagPK.
    """
    # --------------------------------------------------
    # Parameter unpacking
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    SrcSysCd            = params["SrcSysCd"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # --------------------------------------------------
    # 1. Remove duplicates (scenario a hash-file replacement)
    # --------------------------------------------------
    df_allcol_dedup = df_AllCol.dropDuplicates([
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_DIAG_ORDNL_CD"
    ])
    # --------------------------------------------------
    # 2. Load temp table K_CLM_LN_DIAG_TEMP
    # --------------------------------------------------
    truncate_query = f"TRUNCATE TABLE {IDSOwner}.K_CLM_LN_DIAG_TEMP"
    execute_dml(truncate_query, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_LN_DIAG_ORDNL_CD"
        )
        .write
        .mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_LN_DIAG_TEMP")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_PAYMT_SUM_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)
    # --------------------------------------------------
    # 3. Extract keys and unknowns (W_Extract)
    # --------------------------------------------------
    extract_query = f"""
    SELECT k.CLM_LN_DIAG_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_LN_SEQ_NO,
           w.CLM_LN_DIAG_ORDNL_CD,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_CLM_LN_DIAG_TEMP w
      JOIN {IDSOwner}.K_CLM_LN_DIAG k
        ON w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
       AND w.CLM_ID              = k.CLM_ID
       AND w.CLM_LN_SEQ_NO       = k.CLM_LN_SEQ_NO
       AND w.CLM_LN_DIAG_ORDNL_CD= k.CLM_LN_DIAG_ORDNL_CD

    UNION

    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_LN_SEQ_NO,
           w2.CLM_LN_DIAG_ORDNL_CD,
           {CurrRunCycle}
      FROM {IDSOwner}.K_CLM_LN_DIAG_TEMP w2
     WHERE NOT EXISTS (
           SELECT 1
             FROM {IDSOwner}.K_CLM_LN_DIAG k2
            WHERE w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
              AND w2.CLM_ID              = k2.CLM_ID
              AND w2.CLM_LN_SEQ_NO       = k2.CLM_LN_SEQ_NO
              AND w2.CLM_LN_DIAG_ORDNL_CD= k2.CLM_LN_DIAG_ORDNL_CD
     )
    """
    df_w_extract = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # --------------------------------------------------
    # 4. PrimaryKey transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CLM_LN_DIAG_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("CLM_LN_DIAG_SK") == F.lit(-1), F.lit(None)).otherwise(F.col("CLM_LN_DIAG_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("CLM_LN_DIAG_SK") == F.lit(-1), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # Updt link
    df_updt = (
        df_enriched
        .select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("CLM_LN_DIAG_SK").alias("CLM_ID"),
            F.col("CLM_LN_SEQ_NO"),
            F.col("CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD_SK"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_LN_DIAG_SK")
        )
    )

    # NewKeys link
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_LN_DIAG_ORDNL_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_LN_DIAG_SK")
        )
    )

    # Keys link
    df_keys = (
        df_enriched
        .select(
            F.col("svSK").alias("CLM_LN_DIAG_SK"),
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CLM_LN_DIAG_ORDNL_CD",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # --------------------------------------------------
    # 5. Write targets (hash-file -> parquet, seq file)
    # --------------------------------------------------
    parquet_path_updt = f"{adls_path}/ClmLnDiagPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='\"',
        nullValue=None
    )

    seq_path = f"{adls_path}/load/K_CLM_LN_DIAG.dat"
    write_files(
        df_newkeys,
        seq_path,
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='\"',
        nullValue=None
    )
    # --------------------------------------------------
    # 6. Merge transformer
    # --------------------------------------------------
    join_cond = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.CLM_ID")        == F.col("k.CLM_ID")) &
        (F.col("all.CLM_LN_SEQ_NO") == F.col("k.CLM_LN_SEQ_NO")) &
        (F.col("all.CLM_LN_DIAG_ORDNL_CD") == F.col("k.CLM_LN_DIAG_ORDNL_CD"))
    )

    df_merge = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_cond, "left")
    )

    df_Key = (
        df_merge
        .select(
            F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.CLM_LN_DIAG_SK"),
            F.col("all.CLM_ID"),
            F.col("all.CLM_LN_SEQ_NO"),
            F.col("all.CLM_LN_DIAG_ORDNL_CD"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.DIAG_CD"),
            F.col("all.DIAG_CD_TYP_CD")
        )
    )
    # --------------------------------------------------
    # 7. Return container output
    # --------------------------------------------------
    return df_Key