# Databricks utility notebooks
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
PseudoClmLoadPK
---------------
Assign primary surrogate key
Pseudo Claim Primary Key Container
Load IDS temp. table
SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
primary key hash file only contains current run keys and is cleared before writing
update primary key table (K_CLM) with new keys created today
Hash file cleared by calling job
Used by

PCTAPseudoClmPKExtr

VC LOGS
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 02/02/09 12:31:59 Batch  15009_45122 PROMOTE bckcetl ids20 dsadm bls for brent
^1_1 02/02/09 12:25:43 Batch  15009_44747 INIT bckcett testIDS dsadm bls for brent
^1_1 12/16/08 21:43:11 Batch  14961_78198 PROMOTE bckcett testIDS u03651 steph for Brent
^1_1 12/16/08 21:41:20 Batch  14961_78085 INIT bckcett devlIDS u03651 steffy

Copyright 2008 Blue Cross/Blue Shield of Kansas City

CALLED BY:  PCTAPseudoClmPKExtr

PROCESSING:   Assign primary surrogate key to input records

MODIFICATIONS:
Developer                Date                 Project/Altiris #      Change Description
------------------       -------------------- ------------------------ -----------------------------------------------------------------------
Brent Leland             2008-12-03           3567 Primary Key        Original Programming.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_PseudoClmLoadPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> tuple[DataFrame, DataFrame]:
    # --------------------------------------------------
    # Unpack parameters
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # --------------------------------------------------
    # Stage: hf_pseudo_clm_allcol  (scenario a)
    # Replace intermediate hash file with deduplication
    # --------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID"],
        []
    )

    # --------------------------------------------------
    # Stage: K_CLM_TEMP  (DB2Connector)
    # --------------------------------------------------
    drop_sql   = f"DROP TABLE IF EXISTS {IDSOwner}.K_CLM_TEMP"
    create_sql = (
        f"CREATE TABLE {IDSOwner}.K_CLM_TEMP ("
        "SRC_SYS_CD_SK INT NOT NULL,"
        "CLM_ID VARCHAR(18) NOT NULL,"
        "PRIMARY KEY (SRC_SYS_CD_SK, CLM_ID))"
    )
    execute_dml(drop_sql,   ids_jdbc_url, ids_jdbc_props)
    execute_dml(create_sql, ids_jdbc_url, ids_jdbc_props)

    # Load data into K_CLM_TEMP
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_TEMP")
        .mode("append")
        .save()
    )

    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_CLM_TEMP "
        "on key columns with distribution on key columns "
        "and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    # --------------------------------------------------
    # Output link: W_Extract
    # --------------------------------------------------
    extract_query = f"""
    SELECT k.CLM_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_TEMP w,
         {IDSOwner}.K_CLM k
    WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
      AND w.CLM_ID        = k.CLM_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CLM_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CLM k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID        = k2.CLM_ID
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # Stage: PrimaryKey transformer
    # --------------------------------------------------
    df_enriched = df_W_Extract.withColumn(
        "INST_UPDT",
        F.when(F.col("CLM_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
    )
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'CLM_SK',
        <schema>,
        <secret_name>
    )
    df_enriched = df_enriched.withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("INST_UPDT") == 'I', F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )
    df_Keys = df_enriched.select(
        "CLM_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "INST_UPDT"
    )

    # --------------------------------------------------
    # Stage: Merge transformer
    # --------------------------------------------------
    join_expr = (
        (df_AllColOut["SRC_SYS_CD_SK"] == df_Keys["SRC_SYS_CD_SK"]) &
        (df_AllColOut["CLM_ID"] == df_Keys["CLM_ID"])
    )
    df_join = (
        df_Keys.alias("Keys")
        .join(df_AllColOut.alias("AllColOut"), join_expr, "left")
    )

    # Output link: NewKeys  (constraint INST_UPDT = 'I')
    df_NewKeys = (
        df_join.filter(F.col("Keys.INST_UPDT") == 'I')
        .select(
            F.col("Keys.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("Keys.CLM_ID").alias("CLM_ID"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("Keys.CLM_SK").alias("CLM_SK")
        )
    )

    # Output link: updt
    df_updt = (
        df_join.select(
            F.col("AllColOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("Keys.CLM_ID").alias("CLM_ID"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("Keys.CLM_SK").alias("CLM_SK")
        )
    )

    # --------------------------------------------------
    # Stage: K_CLM (sequential file write)
    # --------------------------------------------------
    file_path_newkeys = f"{adls_path}/load/K_CLM.dat"
    write_files(
        df_NewKeys,
        file_path_newkeys,
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: hf_clm (scenario c -> parquet)
    # --------------------------------------------------
    file_path_updt = f"{adls_path}/PseudoClmLoadPK_updt.parquet"
    write_files(
        df_updt,
        file_path_updt,
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # --------------------------------------------------
    # Return container outputs
    # --------------------------------------------------
    return df_NewKeys, df_updt