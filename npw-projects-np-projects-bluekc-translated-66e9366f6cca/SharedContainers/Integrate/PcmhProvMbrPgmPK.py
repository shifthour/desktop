# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: PcmhProvMbrPgmPK
Copyright 2011 Blue Cross and Blue Shield of Kansas City

Called by:
    MddatacorPcmhMbrPgmExtr 

Processing:
    *

Control Job Rerun Information: 
    Previous Run Successful:    What needs to happen before a special run can occur?
    Previous Run Aborted:       Restart, no other steps necessary

Modifications:
Developer        Date         Altiris #   Change Description
---------------  -----------  ----------  -------------------
Ralph Tucker     2011-05-03   4663        Original program

Annotations:
- IDS Primary Key Container for PCMH PROV MBR PGM
- Used by MddatacorPcmhProvMbrPgmExtr
- SQL joins temp table with key table to assign known keys
- Temp table is truncated before load and runstat done after load
- Load IDS temp. table
- join primary key info with table info
- Hashfile cleared every time the job runs
- update primary key table (K_PCMH_PROV_MBR_PGM) with new keys created today
- primary key hash file only contains current run keys
- Assign primary surrogate key
- Writing Sequential File to /extr
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_PcmhProvMbrPgmPK(
    df_AllColl: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # --------------------------------------------------
    # Unpack parameters
    # --------------------------------------------------
    CurrRunCycle           = params["CurrRunCycle"]
    IDSOwner               = params["IDSOwner"]
    ids_secret_name        = params["ids_secret_name"]
    SrcSysCd               = params["SrcSysCd"]
    SrcSysCdSk             = params["SrcSysCdSk"]
    FilePath               = params.get("FilePath", "")
    ids_jdbc_url           = params["ids_jdbc_url"]
    ids_jdbc_props         = params["ids_jdbc_props"]
    adls_path              = params["adls_path"]
    adls_path_raw          = params["adls_path_raw"]
    adls_path_publish      = params["adls_path_publish"]
    # --------------------------------------------------
    # Stage: hf_pcmh_prov_mbr_pgm_allcol (Scenario a)
    # Deduplicate incoming AllColl link
    # --------------------------------------------------
    partition_cols = [
        "SRC_SYS_CD_SK",
        "PROV_ID",
        "MBR_UNIQ_KEY",
        "PCMH_PGM_ID",
        "ROW_EFF_DT_SK"
    ]
    sort_cols = [("SRC_SYS_CD_SK", "A")]
    df_AllColl_dedup = dedup_sort(df_AllColl, partition_cols, sort_cols)

    # --------------------------------------------------
    # Stage: K_PCMH_PROV_MBR_PGM_TEMP  (Insert data, then extract)
    # --------------------------------------------------
    before_sql = (
        f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_PCMH_PROV_MBR_PGM_TEMP')"
    )
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    df_Transform_subset = (
        df_Transform.select(
            "PROV_ID",
            "MBR_UNIQ_KEY",
            "PCMH_PGM_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK"
        )
    )
    (
        df_Transform_subset.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_PCMH_PROV_MBR_PGM_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_PCMH_PROV_MBR_PGM_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
SELECT
    w.PROV_ID,
    w.MBR_UNIQ_KEY,
    w.PCMH_PGM_ID,
    w.ROW_EFF_DT_SK,
    w.SRC_SYS_CD_SK,
    k.CRT_RUN_CYC_EXCTN_SK,
    k.PCMH_PROV_MBR_PGM_SK
FROM {IDSOwner}.K_PCMH_PROV_MBR_PGM_TEMP w
JOIN {IDSOwner}.K_PCMH_PROV_MBR_PGM k
  ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
 AND w.PROV_ID       = k.PROV_ID
 AND w.MBR_UNIQ_KEY  = k.MBR_UNIQ_KEY
 AND w.PCMH_PGM_ID   = k.PCMH_PGM_ID
 AND w.ROW_EFF_DT_SK = k.ROW_EFF_DT_SK
UNION
SELECT
    w2.PROV_ID,
    w2.MBR_UNIQ_KEY,
    w2.PCMH_PGM_ID,
    w2.ROW_EFF_DT_SK,
    w2.SRC_SYS_CD_SK,
    {CurrRunCycle},
    -1
FROM {IDSOwner}.K_PCMH_PROV_MBR_PGM_TEMP w2
WHERE NOT EXISTS (
    SELECT 1
    FROM {IDSOwner}.K_PCMH_PROV_MBR_PGM k2
    WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
      AND w2.PROV_ID       = k2.PROV_ID
      AND w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
      AND w2.PCMH_PGM_ID   = k2.PCMH_PGM_ID
      AND w2.ROW_EFF_DT_SK = k2.ROW_EFF_DT_SK
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
    # Stage: PrimaryKey (Transformer)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("PCMH_PROV_MBR_PGM_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "PCMH_PROV_MBR_PGM_SK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("PCMH_PROV_MBR_PGM_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "PCMH_PROV_MBR_PGM_SK",
        <schema>,
        <secret_name>
    )

    # --------------------------------------------------
    # Links from PrimaryKey
    # --------------------------------------------------
    # updt link
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "PROV_ID",
        "MBR_UNIQ_KEY",
        "PCMH_PGM_ID",
        "ROW_EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "PCMH_PROV_MBR_PGM_SK"
    )

    write_files(
        df_updt,
        f"{adls_path}/PcmhProvMbrPgmPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # NewKeys link
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "PROV_ID",
            "MBR_UNIQ_KEY",
            "PCMH_PGM_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "PCMH_PROV_MBR_PGM_SK"
        )
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/{FilePath}/load/K_PCMH_PROV_MBR_PGM.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # Keys link
    df_Keys = df_enriched.select(
        "PCMH_PROV_MBR_PGM_SK",
        "SRC_SYS_CD_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "PROV_ID",
        "MBR_UNIQ_KEY",
        "PCMH_PGM_ID",
        "ROW_EFF_DT_SK",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        "CRT_RUN_CYC_EXCTN_SK"
    )

    # --------------------------------------------------
    # Stage: Merge
    # --------------------------------------------------
    join_expr = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.PROV_ID")       == F.col("k.PROV_ID")) &
        (F.col("all.MBR_UNIQ_KEY")  == F.col("k.MBR_UNIQ_KEY")) &
        (F.col("all.PCMH_PGM_ID")   == F.col("k.PCMH_PGM_ID")) &
        (F.col("all.ROW_EFF_DT_SK") == F.col("k.ROW_EFF_DT_SK"))
    )

    df_merge = (
        df_AllColl_dedup.alias("all")
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
            "k.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "k.PCMH_PROV_MBR_PGM_SK",
            "all.PROV_ID",
            "all.MBR_UNIQ_KEY",
            "all.PCMH_PGM_ID",
            "all.ROW_EFF_DT_SK",
            "all.MBR_SK",
            "all.PCMH_PGM_SK",
            "all.PROV_SK",
            "all.PCMH_PROV_MBR_SK",
            "all.PROV_GRP_PROV_SK",
            "all.ROW_TERM_DT_SK"
        )
    )

    # --------------------------------------------------
    # Return
    # --------------------------------------------------
    return df_merge