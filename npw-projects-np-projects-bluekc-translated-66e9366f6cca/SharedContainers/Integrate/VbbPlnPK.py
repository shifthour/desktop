# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
JobName      : VbbPlnPK
JobType      : Server Job
JobCategory  : DS_Integrate
FolderPath   : Shared Containers/PrimaryKey
Description  : COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
               PROCESSING: Shared container used for Primary Keying of Claim Prov

Annotations
-----------
join primary key info with table info
update primary key table (K_VBB_PLN) with new keys created today
Assign primary surrogate key
primary key hash file NOT cleared before writing. Used in Fkey routine GetFkeyVbbPln
Temp table is tuncated before load and runstat done after load
SQL joins temp table with key table to assign known keys
Load IDS temp. table
Hash file (hf_vbb_pln_allcol) cleared in the calling program - IdsVbbPlnExtr
Hashfile cleared in calling program
This container is used in:
IdsVbbPlnExtr

These programs need to be re-compiled when logic changes
"""

def run_VbbPlnPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack parameters
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Hashed-file: hf_vbb_pln_allcol  (scenario a – intermediate hash)
    #              -> deduplicate on keys and pass through
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["VBB_PLN_UNIQ_KEY", "SRC_SYS_CD_SK"],
        []
    )
    # ------------------------------------------------------------------
    # Hashed-file: hf_vbbpln_transform_dedupe (scenario a – intermediate)
    #              -> deduplicate on keys
    # ------------------------------------------------------------------
    df_dedupe = dedup_sort(
        df_Transform,
        ["VBB_PLN_UNIQ_KEY", "SRC_SYS_CD_SK"],
        []
    )
    # ------------------------------------------------------------------
    # Database : IDS.K_VBB_PLN_TEMP  (truncate & load)
    # ------------------------------------------------------------------
    create_stmt = (
        f"CREATE TABLE {IDSOwner}.K_VBB_PLN_TEMP ("
        "VBB_PLN_UNIQ_KEY INTEGER NOT NULL, "
        "SRC_SYS_CD_SK INTEGER NOT NULL, "
        "PRIMARY KEY (VBB_PLN_UNIQ_KEY, SRC_SYS_CD_SK) )"
    )
    try:
        execute_dml(
            f"DROP TABLE {IDSOwner}.K_VBB_PLN_TEMP",
            ids_jdbc_url,
            ids_jdbc_props
        )
    except Exception:
        pass
    execute_dml(create_stmt, ids_jdbc_url, ids_jdbc_props)
    (
        df_dedupe.write
        .mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_VBB_PLN_TEMP")
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_VBB_PLN_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ------------------------------------------------------------------
    # Extract W_Extract dataset
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.VBB_PLN_SK,
                w.VBB_PLN_UNIQ_KEY,
                w.SRC_SYS_CD_SK,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM    {IDSOwner}.K_VBB_PLN_TEMP w,
                {IDSOwner}.K_VBB_PLN k
        WHERE   w.SRC_SYS_CD_SK      = k.SRC_SYS_CD_SK
        AND     w.VBB_PLN_UNIQ_KEY   = k.VBB_PLN_UNIQ_KEY
        UNION
        SELECT  -1,
                w2.VBB_PLN_UNIQ_KEY,
                w2.SRC_SYS_CD_SK,
                {CurrRunCycle}
        FROM    {IDSOwner}.K_VBB_PLN_TEMP w2
        WHERE   NOT EXISTS (
                SELECT  k2.VBB_PLN_SK
                FROM    {IDSOwner}.K_VBB_PLN k2
                WHERE   w2.SRC_SYS_CD_SK     = k2.SRC_SYS_CD_SK
                AND     w2.VBB_PLN_UNIQ_KEY  = k2.VBB_PLN_UNIQ_KEY )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # ------------------------------------------------------------------
    # PrimaryKey Transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("VBB_PLN_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    )
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "VBB_PLN_SK",
        <schema>,
        <secret_name>
    )
    df_enriched = (
        df_enriched
        .withColumn("svSK", F.col("VBB_PLN_SK"))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    # updt link ---------------------------------------------------------
    df_updt = (
        df_enriched.select(
            "VBB_PLN_UNIQ_KEY",
            F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("VBB_PLN_SK")
        )
    )
    write_files(
        df_updt,
        f"{adls_path}/VbbPlnPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # NewKeys link ------------------------------------------------------
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "VBB_PLN_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("VBB_PLN_SK")
        )
    )
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_VBB_PLN.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # Keys link ---------------------------------------------------------
    df_Keys = (
        df_enriched.select(
            F.col("svSK").alias("VBB_PLN_SK"),
            "VBB_PLN_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            "SRC_SYS_CD",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # ------------------------------------------------------------------
    # Merge Transformer
    # ------------------------------------------------------------------
    join_expr = (
        (F.col("all.VBB_PLN_UNIQ_KEY") == F.col("k.VBB_PLN_UNIQ_KEY")) &
        (F.col("all.SRC_SYS_CD_SK")   == F.col("k.SRC_SYS_CD_SK"))
    )
    df_Key = (
        df_AllCol_dedup.alias("all")
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
            "k.VBB_PLN_SK",
            "all.VBB_PLN_UNIQ_KEY",
            "all.SRC_SYS_CD_SK",
            "k.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "all.VBB_MDL_CD",
            "all.VBB_PLN_CAT_CD",
            "all.VBB_PLN_PROD_CAT_CD",
            "all.VBB_PLN_TERM_RSN_CD",
            "all.VBB_PLN_STRT_YR_IN",
            "all.VBB_PLN_STRT_YR_NO",
            "all.VBB_PLN_STRT_DT_SK",
            "all.VBB_PLN_END_DT_SK",
            "all.VBB_PLN_TERM_DT_SK",
            "all.SRC_SYS_CRT_DTM",
            "all.SRC_SYS_UPDT_DTM",
            "all.VBB_PLN_NM"
        )
    )
    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key