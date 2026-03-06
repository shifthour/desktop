# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
AlineoIndvBePopHlthPgmDiagPK  – Primary-Key shared container

Copyright 2010 Blue Cross/Blue Shield of Kansas City

PROCESSING: Primary Key Shared Container for INDV_BE_POP_HLTH_PGM_DIAG_TRANS 

MODIFICATIONS:
Developer                 Date            Project/Altiris #      Change Description
------------------------  --------------  --------------------   ---------------------------------------------------
Bhoomi Dasari             2010-07-21      4297/Alineo-2          Original Programming
Abhiram Dasarathy         2012-04-27      4735 - Alineo 3.2      Modified the job to reflect the Upgrade changes
Santosh Bokka             2014-03-10      TFS-3421               Changed input Sql Type and Length for DIAG_CD_SK in
                                                                hf_indv_bepop_hlth_diag_trans_allcol

Annotations:
- Assign primary surrogate key
- SQL joins temp table with key table to assign known keys
- Temp table is truncated before load and runstat done after load
- Load IDS temp. table
- Join primary key info with table info
- Update primary key table (K_HSTD_APP_USER_DEPT_TRANS) with new keys created today
- Primary key hash file only contains current run keys and is cleared before writing
- This container is used in:  AlineoIndvBePopHlthPgmDiagTransExtr
  (These programs need to be re-compiled when logic changes)
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_AlineoIndvBePopHlthPgmDiagPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # 1. Unpack run-time parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    SrcSysCdSk          = params["SrcSysCdSk"]

    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # 2. Replace intermediate hash-file  (scenario a)  with de-duplication
    # ------------------------------------------------------------------
    partition_cols = [
        "SRC_SYS_CD_SK",
        "POP_HLTH_PGM_ENR_ID",
        "COND_CD",
        "DIAG_CD",
        "PGM_DIAG_CD_TYP_CD",
        "ROW_EFF_DT_SK"
    ]
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols,
        []                           # no specific sort columns defined
    )

    # ------------------------------------------------------------------
    # 3. Load TEMP table  K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS_TEMP
    # ------------------------------------------------------------------
    truncate_sql = (
        f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', "
        f"'K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS_TEMP')"
    )
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS_TEMP")
        .mode("append")
        .save()
    )

    runstats_sql = (
        "CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------------------------
    # 4. Extract W_Extract stream from the database
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.INDV_BE_POP_HLTH_PGM_DIAG_T_SK,
                w.SRC_SYS_CD_SK,
                w.POP_HLTH_PGM_ENR_ID,
                w.PGM_COND_CD           AS COND_CD,
                w.PGM_DIAG_CD           AS DIAG_CD,
                w.PGM_DIAG_CD_TYP_CD    AS DIAG_CD_TYP_CD,
                w.ROW_EFF_DT_SK,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS_TEMP w
        JOIN {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS      k
          ON w.SRC_SYS_CD_SK        = k.SRC_SYS_CD_SK
         AND w.POP_HLTH_PGM_ENR_ID   = k.POP_HLTH_PGM_ENR_ID
         AND w.PGM_COND_CD           = k.PGM_COND_CD
         AND w.PGM_DIAG_CD           = k.PGM_DIAG_CD
         AND w.PGM_DIAG_CD_TYP_CD    = k.PGM_DIAG_CD_TYP_CD
         AND w.ROW_EFF_DT_SK         = k.ROW_EFF_DT_SK
        UNION
        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.POP_HLTH_PGM_ENR_ID,
                w2.PGM_COND_CD          AS COND_CD,
                w2.PGM_DIAG_CD          AS DIAG_CD,
                w2.PGM_DIAG_CD_TYP_CD   AS DIAG_CD_TYP_CD,
                w2.ROW_EFF_DT_SK,
                {CurrRunCycle}
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS k2
            WHERE w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
              AND w2.POP_HLTH_PGM_ENR_ID = k2.POP_HLTH_PGM_ENR_ID
              AND w2.PGM_COND_CD         = k2.PGM_COND_CD
              AND w2.PGM_DIAG_CD         = k2.PGM_DIAG_CD
              AND w2.PGM_DIAG_CD_TYP_CD  = k2.PGM_DIAG_CD_TYP_CD
              AND w2.ROW_EFF_DT_SK       = k2.ROW_EFF_DT_SK
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
    # 5. Primary-Key Transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(
                F.col("INDV_BE_POP_HLTH_PGM_DIAG_T_SK") == -1,
                F.lit("I")
            ).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(
                F.col("svInstUpdt") == F.lit("I"),
                F.lit(None)
            ).otherwise(F.col("INDV_BE_POP_HLTH_PGM_DIAG_T_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(
                F.col("svInstUpdt") == F.lit("I"),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # 6. Prepare output links from PrimaryKey transformer
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched.select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "POP_HLTH_PGM_ENR_ID",
            "COND_CD",
            "DIAG_CD",
            "DIAG_CD_TYP_CD",
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
            "svSK".alias("INDV_BE_POP_HLTH_PGM_DIAG_T_SK")
        )
    )

    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "POP_HLTH_PGM_ENR_ID",
            "COND_CD",
            "DIAG_CD",
            "DIAG_CD_TYP_CD",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK"),
            "svSK".alias("INDV_BE_POP_HLTH_PGM_DIAG_T_SK")
        )
    )

    df_Keys = (
        df_enriched.select(
            "svSK".alias("INDV_BE_POP_HLTH_PGM_DIAG_T_SK"),
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "POP_HLTH_PGM_ENR_ID",
            "COND_CD",
            "DIAG_CD",
            "DIAG_CD_TYP_CD",
            "ROW_EFF_DT_SK",
            "svInstUpdt".alias("INSRT_UPDT_CD"),
            "svCrtRunCycExctnSk".alias("CRT_RUN_CYC_EXCTN_SK")
        )
        .withColumn(
            "PGM_DIAG_CD_TYP_CD",
            F.col("DIAG_CD_TYP_CD")
        )
    )

    # ------------------------------------------------------------------
    # 7. Merge transformer logic
    # ------------------------------------------------------------------
    join_expr = (
        (F.col("all.SRC_SYS_CD_SK")      == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.POP_HLTH_PGM_ENR_ID")== F.col("k.POP_HLTH_PGM_ENR_ID")) &
        (F.col("all.COND_CD")            == F.col("k.COND_CD")) &
        (F.col("all.DIAG_CD")            == F.col("k.DIAG_CD")) &
        (F.col("all.PGM_DIAG_CD_TYP_CD") == F.col("k.PGM_DIAG_CD_TYP_CD")) &
        (F.col("all.ROW_EFF_DT_SK")      == F.col("k.ROW_EFF_DT_SK"))
    )

    df_Key = (
        df_AllColOut.alias("all")
        .join(df_Keys.alias("k"), join_expr, "left")
        .select(
            "all.JOB_EXCTN_RCRD_ERR_SK",
            "k.INSRT_UPDT_CD",
            "all.DISCARD_IN",
            "all.PASS_THRU_IN",
            "all.FIRST_RECYC_DT",
            "all.ERR_CT",
            "all.RECYCLE_CT",
            "k.SRC_SYS_CD",
            "all.PRI_KEY_STRING",
            "k.INDV_BE_POP_HLTH_PGM_DIAG_T_SK",
            "all.POP_HLTH_PGM_ENR_ID",
            "all.COND_CD",
            "all.DIAG_CD",
            "all.PGM_DIAG_CD_TYP_CD",
            "all.ROW_EFF_DT_SK",
            "k.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "all.COND_CD_SK",
            "all.DIAG_CD_SK",
            "all.DEL_IN",
            "all.PRI_DIAG_IN",
            "all.PGM_DIAG_CRT_DT_SK",
            "all.ROW_TERM_DT_SK",
            "all.INDV_BE_KEY",
            "all.ENROLLMENT_DX_CREATED",
            "all.ENROLLMENT_DX_MODIFIED",
            "all.PGM_DIAG_CD_TYP_CD_SRC_CD"
        )
    )

    # ------------------------------------------------------------------
    # 8. Persist NewKeys sequential file
    # ------------------------------------------------------------------
    newkeys_path = f"{adls_path}/load/K_INDV_BE_POP_HLTH_PGM_DIAG_TRANS.dat"
    write_files(
        df_NewKeys,
        newkeys_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # 9. Persist updt stream (scenario c hash-file) as parquet
    # ------------------------------------------------------------------
    updt_parquet_path = f"{adls_path}/AlineoIndvBePopHlthPgmDiagPK_updt.parquet"
    write_files(
        df_updt,
        updt_parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # 10. Return container output link(s)
    # ------------------------------------------------------------------
    return df_Key