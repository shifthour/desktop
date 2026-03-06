# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def run_PseudoClmLnLoadPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> tuple[DataFrame, DataFrame]:
    """
    Pseudo Claim Line Primary Key Container
    -------------------------------------------------------------------------
    Assign / Create primary SK for IDS claim line tables with natural key of
    source system, claim ID, and sequence number.

    VC LOGS
    ^1_2 03/10/09 09:25:21 ...
    -------------------------------------------------------------------------
    Stage Descriptions:
      • hf_pseudo_clm_ln_allcol – intermediate hash file of all claim-line
        columns (source & target)
      • K_CLM_LN_TEMP          – DB2 temp table for key resolution
      • PrimaryKey             – transformer assigning surrogate keys
      • Merge                  – merges new keys with full column set
      • hf_clm_ln              – hash-file target (persisted as parquet)
      • K_CLM_LN               – sequential file of newly generated keys
    """
    # ------------------------------------------------------------------
    # Unpack runtime parameters (each key exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage: hf_pseudo_clm_ln_allcol  (scenario a – intermediate hash file)
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_LN_SEQ_NO"],
        []
    )
    # ------------------------------------------------------------------
    # Stage: K_CLM_LN_TEMP  – create / load temp table
    # ------------------------------------------------------------------
    drop_sql = f"DROP TABLE {IDSOwner}.K_CLM_LN_TEMP"
    create_sql = f"""
        CREATE TABLE {IDSOwner}.K_CLM_LN_TEMP (
            SRC_SYS_CD_SK    INTEGER     NOT NULL,
            CLM_ID           VARCHAR(20) NOT NULL,
            CLM_LN_SEQ_NO    INTEGER     NOT NULL,
            PRIMARY KEY (SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO)
        )
    """
    execute_dml(drop_sql, ids_jdbc_url, ids_jdbc_props)
    execute_dml(create_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform
        .select("SRC_SYS_CD_SK", "CLM_ID", "CLM_LN_SEQ_NO")
        .write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_LN_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_CLM_LN_TEMP "
        f"on key columns with distribution on key columns "
        f"and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------------------------
    # Stage: K_CLM_LN_TEMP  – extract union-query result (W_Extract)
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT k.CLM_LN_SK,
               w.SRC_SYS_CD_SK,
               w.CLM_ID,
               w.CLM_LN_SEQ_NO,
               k.CRT_RUN_CYC_EXCTN_SK
          FROM {IDSOwner}.K_CLM_LN_TEMP w,
               {IDSOwner}.K_CLM_LN       k
         WHERE w.SRC_SYS_CD_SK   = k.SRC_SYS_CD_SK
           AND w.CLM_ID          = k.CLM_ID
           AND w.CLM_LN_SEQ_NO   = k.CLM_LN_SEQ_NO
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.CLM_ID,
               w2.CLM_LN_SEQ_NO,
               {CurrRunCycle}
          FROM {IDSOwner}.K_CLM_LN_TEMP w2
         WHERE NOT EXISTS (
               SELECT 1
                 FROM {IDSOwner}.K_CLM_LN k2
                WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
                  AND w2.CLM_ID        = k2.CLM_ID
                  AND w2.CLM_LN_SEQ_NO = k2.CLM_LN_SEQ_NO )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # ------------------------------------------------------------------
    # Stage: PrimaryKey  – surrogate-key assignment
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INST_UPDT",
            F.when(F.col("CLM_LN_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)
    df_enriched = (
        df_enriched
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(
                F.col("INST_UPDT") == "I",
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_Keys = df_enriched.select(
        "CLM_LN_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "INST_UPDT"
    )
    # ------------------------------------------------------------------
    # Stage: Merge
    # ------------------------------------------------------------------
    join_cols = ["CLM_ID", "CLM_LN_SEQ_NO"]
    df_merge_all = (
        df_AllCol_dedup.alias("all")
        .join(df_Keys.alias("k"), join_cols, "left")
    )

    # Output link: NewKeys  (Keys.INST_UPDT = 'I')
    df_NewKeys = (
        df_Keys
        .filter(F.col("INST_UPDT") == "I")
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_LN_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "CLM_LN_SK"
        )
    )

    # Output link: updt  (to hf_clm_ln)
    df_updt = df_merge_all.select(
        F.col("all.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("k.CLM_ID").alias("CLM_ID"),
        F.col("k.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("k.CLM_LN_SK").alias("CLM_LN_SK")
    )
    # ------------------------------------------------------------------
    # Stage: K_CLM_LN  – sequential file write
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CLM_LN.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Stage: hf_clm_ln  – parquet write (scenario c hash file)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/PseudoClmLnLoadPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    return df_NewKeys, df_updt