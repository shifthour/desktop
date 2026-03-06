# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
MedAlertPK – Shared container used for Primary Keying of MedAlert job
--------------------------------------------------------------------
* VC LOGS *
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION: Shared container used for Primary Keying of MedAlert job
CALLED BY : ImpProMedAlertExtr

PROCESSING NOTES:
  • primary key hash file only contains current run keys and is cleared before writing
  • update primary key table (K_MED_ALERT) with new keys created today
  • Assign primary surrogate key
  • Hash file (hf_med_alert_allcol) cleared in calling job
  • join primary key info with table info
  • Load IDS temp. table
  • Temp table is truncated before load and runstat done after load
  • SQL joins temp table with key table to assign known keys

MODIFICATIONS:
Developer           Date              Change Description
------------------  ----------------  --------------------------------------------------------------
Bhoomi Dasari       2008-08-25        Initial program
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, lit

def run_MedAlertPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the MedAlertPK shared-container logic.
    Parameters
    ----------
    df_AllCol   : DataFrame – container input link “AllCol”
    df_Transform: DataFrame – container input link “Transform”
    params      : dict      – runtime parameters (jdbc configs, paths, etc.)
    Returns
    -------
    DataFrame – container output link “Key”
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    SrcSysCd          = params["SrcSysCd"]
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # 1. Replace intermediate hash file (hf_med_alert_allcol) with dedup
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "MED_ALERT_ID"],
        sort_cols=[("SRC_SYS_CD_SK", "A")]
    )

    # ------------------------------------------------------------------
    # 2. Load K_MED_ALERT_TEMP table
    # ------------------------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_MED_ALERT_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform
        .select("SRC_SYS_CD_SK", "MED_ALERT_ID")
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MED_ALERT_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MED_ALERT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # 3. Extract data (W_Extract)
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.MED_ALERT_SK,
                w.SRC_SYS_CD_SK,
                w.MED_ALERT_ID,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_MED_ALERT_TEMP w,
             {IDSOwner}.K_MED_ALERT k
        WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
          AND w.MED_ALERT_ID  = k.MED_ALERT_ID
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.MED_ALERT_ID,
               {CurrRunCycle}
        FROM {IDSOwner}.K_MED_ALERT_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_MED_ALERT k2
            WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
              AND w2.MED_ALERT_ID  = k2.MED_ALERT_ID
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
    # 4. PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("MED_ALERT_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn(
            "svSK",
            when(col("svInstUpdt") == lit("I"), lit(None)).otherwise(col("MED_ALERT_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svMedAlertId",
            trim(col("MED_ALERT_ID"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # updt link (to hf_med_alert)
    df_updt = (
        df_enriched.select(
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            col("svMedAlertId").alias("MED_ALERT_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("MED_ALERT_SK")
        )
    )

    # NewKeys link (to sequential file)
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svMedAlertId").alias("MED_ALERT_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("MED_ALERT_SK")
        )
    )

    # Keys link (for merge)
    df_keys = (
        df_enriched.select(
            col("svSK").alias("MED_ALERT_SK"),
            col("SRC_SYS_CD_SK"),
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            col("svMedAlertId").alias("MED_ALERT_ID"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # 5. Merge transformer logic
    # ------------------------------------------------------------------
    join_expr = (
        (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
        (col("all.MED_ALERT_ID")  == col("k.MED_ALERT_ID"))
    )

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD"),
            col("all.DISCARD_IN"),
            col("all.PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT"),
            col("all.ERR_CT"),
            col("all.RECYCLE_CT"),
            col("k.SRC_SYS_CD"),
            col("all.PRI_KEY_STRING"),
            col("k.MED_ALERT_SK"),
            col("all.MED_ALERT_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.BCBSKC_CLNCL_PGM_TYP_CD"),
            col("all.MED_ALERT_TYP_CD_SK"),
            col("all.SH_DESC")
        )
    )

    # ------------------------------------------------------------------
    # 6. Persist outputs
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_MED_ALERT.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"'
    )

    write_files(
        df_updt,
        f"{adls_path}/MedAlertPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key