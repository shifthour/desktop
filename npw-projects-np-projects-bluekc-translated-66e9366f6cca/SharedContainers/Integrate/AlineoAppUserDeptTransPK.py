# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
AlineoAppUserDeptTransPK – Primary Key Shared Container
Copyright 2010 Blue Cross/Blue Shield of Kansas City

PROCESSING: Primary Key Shared Container for HSTD_APP_USER_DEPT_TRANS 

MODIFICATIONS:
Developer            Date         Project/Altiris #   Change Description
------------------   ----------   ------------------   ------------------
Bhoomi Dasari        2010-07-19   4297/Alineo-2        Original Programming
Santosh Bokka        2014-03-28   TFS-1357             Changed DEPT_ID length to 40

Annotations (original DataStage):
- SQL joins temp table with key table to assign known keys
- Temp table is truncated before load and runstats done after load
- Load IDS temp. table
- join primary key info with table info
- update primary key table (K_HSTD_APP_USER_DEPT_TRANS) with new keys created today
- primary key hash file only contains current run keys and is cleared before writing
- Assign primary surrogate key
- Alineo HSTD_APP_USER_DEPT_TRANS Primary Key Shared Container
- This container is used in:
  AlineoHstdAppUserDeptExtr
  These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_AlineoAppUserDeptTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the AlineoAppUserDeptTransPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream that carries all columns for hash file hf_hstd_appuser_dept_trans_allcol.
    df_Transform : DataFrame
        Input stream that is staged into {IDSOwner}.K_HSTD_APP_USER_DEPT_TRANS_TEMP.
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Output stream “Key” produced by the Merge stage.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (only once)
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
    # Replace intermediate hashed file hf_hstd_appuser_dept_trans_allcol
    # with deduplication logic (scenario a)
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "HSTD_USER_ID", "ROW_EFF_DT_SK", "DEPT_ID"],
        [("ROW_EFF_DT_SK", "D")]
    )

    # ------------------------------------------------------------------
    # Stage df_Transform into {IDSOwner}.K_HSTD_APP_USER_DEPT_TRANS_TEMP
    # ------------------------------------------------------------------
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_HSTD_APP_USER_DEPT_TRANS_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_HSTD_APP_USER_DEPT_TRANS_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_HSTD_APP_USER_DEPT_TRANS_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------------------------
    # Extract data set “W_Extract” from temp & primary key tables
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.HSTD_APP_USER_DEPT_T_SK,
                w.SRC_SYS_CD_SK,
                w.HSTD_USER_ID,
                w.DEPT_ID,
                w.ROW_EFF_DT_SK,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_HSTD_APP_USER_DEPT_TRANS_TEMP w,
             {IDSOwner}.K_HSTD_APP_USER_DEPT_TRANS k
        WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
          AND w.HSTD_USER_ID   = k.HSTD_USER_ID
          AND w.DEPT_ID        = k.DEPT_ID
          AND w.ROW_EFF_DT_SK  = k.ROW_EFF_DT_SK
        UNION
        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.HSTD_USER_ID,
                w2.DEPT_ID,
                w2.ROW_EFF_DT_SK,
                {CurrRunCycle}
        FROM {IDSOwner}.K_HSTD_APP_USER_DEPT_TRANS_TEMP w2
        WHERE NOT EXISTS (
              SELECT 1
              FROM {IDSOwner}.K_HSTD_APP_USER_DEPT_TRANS k2
              WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
                AND w2.HSTD_USER_ID   = k2.HSTD_USER_ID
                AND w2.DEPT_ID        = k2.DEPT_ID
                AND w2.ROW_EFF_DT_SK  = k2.ROW_EFF_DT_SK
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
    # PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("HSTD_APP_USER_DEPT_T_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "HSTD_APP_USER_DEPT_T_SK",
            F.when(F.col("INSRT_UPDT_CD") == F.lit("I"), F.lit(None)).otherwise(F.col("HSTD_APP_USER_DEPT_T_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("INSRT_UPDT_CD") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # ------------------------------------------------------------------
    # Surrogate key generation
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'HSTD_APP_USER_DEPT_T_SK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # Split outputs from PrimaryKey transformer
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        "HSTD_USER_ID",
        "DEPT_ID",
        "ROW_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "HSTD_APP_USER_DEPT_T_SK"
    )

    df_newkeys = (
        df_enriched.filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            "HSTD_USER_ID",
            "DEPT_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "HSTD_APP_USER_DEPT_T_SK"
        )
    )

    df_keys = df_enriched.select(
        "HSTD_APP_USER_DEPT_T_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "HSTD_USER_ID",
        "DEPT_ID",
        "ROW_EFF_DT_SK",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )

    # ------------------------------------------------------------------
    # Merge stage – join AllColOut with Keys
    # ------------------------------------------------------------------
    allcol = df_allcol_dedup.alias("AllColOut")
    keys   = df_keys.alias("Keys")

    join_cond = (
        (allcol["SRC_SYS_CD_SK"] == keys["SRC_SYS_CD_SK"]) &
        (allcol["HSTD_USER_ID"]  == keys["HSTD_USER_ID"])  &
        (allcol["DEPT_ID"]       == keys["DEPT_ID"])       &
        (allcol["ROW_EFF_DT_SK"] == keys["ROW_EFF_DT_SK"])
    )

    df_Key = (
        allcol.join(keys, join_cond, "left")
        .select(
            allcol["JOB_EXCTN_RCRD_ERR_SK"],
            keys["INSRT_UPDT_CD"],
            allcol["DISCARD_IN"],
            allcol["PASS_THRU_IN"],
            allcol["FIRST_RECYC_DT"],
            allcol["ERR_CT"],
            allcol["RECYCLE_CT"],
            keys["SRC_SYS_CD"],
            allcol["PRI_KEY_STRING"],
            keys["HSTD_APP_USER_DEPT_T_SK"],
            allcol["HSTD_USER_ID"],
            allcol["DEPT_ID"],
            allcol["ROW_EFF_DT_SK"],
            keys["CRT_RUN_CYC_EXCTN_SK"],
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            allcol["DEL_IN"],
            allcol["IS_MGR_IN"],
            allcol["APP_USER_DEPT_CRT_DT_SK"],
            allcol["ROW_TERM_DT_SK"],
            allcol["DEPT_NAME"].alias("DEPT_NM"),
            allcol["EFF_DATE"],
            allcol["TERM_DATE"],
            allcol["DEPT_APPL_USERDEPT_CREATED"],
            allcol["DEPT_APPL_USERDEPT_MODIFIED"]
        )
    )

    # ------------------------------------------------------------------
    # Write outputs to their respective destinations
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/AlineoAppUserDeptTransPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_HSTD_APP_USER_DEPT_TRANS.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return the container’s single output stream
    # ------------------------------------------------------------------
    return df_Key