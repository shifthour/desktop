# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
DataStage Shared Container: PcmhProvMbrPK
Used by
MddatacorPcmhProvExtr
Update primary key table (K_PCMH_PROV) with new keys created today
SQL joins temp table with key table to assign known keys
Temp table is tuncated before load and runstat done after load
Load IDS temp. table
Join primary key info with table info
Hashed file cleared everytime the job runs
IDS Primary Key Container for PCMH PROV
Primary key hash file only contains current run keys
Assign primary surrogate key
"""

from pyspark.sql import DataFrame, functions as F


def run_PcmhProvMbrPK(df_AllColl: DataFrame, df_Transform: DataFrame, params: dict) -> DataFrame:
    # --------------------------------------------------
    # Unpack parameters
    # --------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    RunID = params["RunID"]
    CurrDate = params["CurrDate"]
    FirstOfMonth = params["FirstOfMonth"]
    SrcSysCd = params["SrcSysCd"]
    SrcSysCdSk = params["SrcSysCdSk"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # --------------------------------------------------
    # Stage: hf_pcmh_prov_mbr_allcol (intermediate hash file replacement)
    # --------------------------------------------------
    partition_cols = ["PROV_ID", "MBR_UNIQ_KEY", "ROW_EFF_DT_SK", "SRC_SYS_CD_SK"]
    df_AllColOut = dedup_sort(df_AllColl, partition_cols, [])

    # --------------------------------------------------
    # Stage: K_PCMH_PROV_MBR_TEMP (load temp table)
    # --------------------------------------------------
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_PCMH_PROV_MBR_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_PCMH_PROV_MBR_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MED_MGT_NOTE_TX_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
SELECT
       k.PCMH_PROV_MBR_SK,
       w.PROV_ID,
       w.MBR_UNIQ_KEY,
       w.ROW_EFF_DT_SK,
       w.SRC_SYS_CD_SK,
       k.CRT_RUN_CYC_EXCTN_SK
FROM
       {IDSOwner}.K_PCMH_PROV_MBR_TEMP w,
       {IDSOwner}.K_PCMH_PROV_MBR k
WHERE
       w.PROV_ID = k.PROV_ID
   AND w.MBR_UNIQ_KEY = k.MBR_UNIQ_KEY
   AND w.ROW_EFF_DT_SK = k.ROW_EFF_DT_SK
   AND w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
UNION
SELECT
       -1,
       w2.PROV_ID,
       w2.MBR_UNIQ_KEY,
       w2.ROW_EFF_DT_SK,
       w2.SRC_SYS_CD_SK,
       {CurrRunCycle}
FROM
       {IDSOwner}.K_PCMH_PROV_MBR_TEMP w2
WHERE NOT EXISTS (
       SELECT k2.PCMH_PROV_MBR_SK
       FROM   {IDSOwner}.K_PCMH_PROV_MBR k2
       WHERE  w2.PROV_ID = k2.PROV_ID
         AND  w2.MBR_UNIQ_KEY = k2.MBR_UNIQ_KEY
         AND  w2.ROW_EFF_DT_SK = k2.ROW_EFF_DT_SK
         AND  w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
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
    # Stage: PrimaryKey (transformer)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract.withColumn(
            "svInstUpdt",
            F.when(F.col("PCMH_PROV_MBR_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )

    df_enriched = SurrogateKeyGen(
        df_enriched, <DB sequence name>, key_col, <schema>, <secret_name>
    )

    df_enriched = df_enriched.withColumn("svSK", F.col("PCMH_PROV_MBR_SK"))

    # updt link
    df_updt = df_enriched.select(
        "PROV_ID",
        "MBR_UNIQ_KEY",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("PCMH_PROV_MBR_SK"),
    )

    # NewKeys link (constraint: svInstUpdt = 'I')
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I")).select(
            "PROV_ID",
            "MBR_UNIQ_KEY",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("PCMH_PROV_MBR_SK"),
        )
    )

    # Keys link
    df_keys = df_enriched.select(
        F.col("svSK").alias("PCMH_PROV_MBR_SK"),
        "PROV_ID",
        "MBR_UNIQ_KEY",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    )

    # --------------------------------------------------
    # Stage: hf_pcmh_prov_mbr (hash file write as parquet)
    # --------------------------------------------------
    path_updt = f"{adls_path}/PcmhProvMbrPK_updt.parquet"
    write_files(
        df_updt,
        path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )

    # --------------------------------------------------
    # Stage: K_PCMH_PROV_MBR (sequential file write)
    # --------------------------------------------------
    path_seq = f"{adls_path}/load/K_PCMH_PROV_MBR.dat"
    write_files(
        df_NewKeys,
        path_seq,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None,
    )

    # --------------------------------------------------
    # Stage: Merge (transformer)
    # --------------------------------------------------
    df_Key = (
        df_AllColOut.alias("all")
        .join(
            df_keys.alias("k"),
            (F.col("all.PROV_ID") == F.col("k.PROV_ID"))
            & (F.col("all.MBR_UNIQ_KEY") == F.col("k.MBR_UNIQ_KEY"))
            & (F.col("all.ROW_EFF_DT_SK") == F.col("k.ROW_EFF_DT_SK"))
            & (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")),
            "left",
        )
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN").alias("DISCARD_IN"),
            F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("all.ERR_CT").alias("ERR_CT"),
            F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("k.PCMH_PROV_MBR_SK").alias("PCMH_PROV_MBR_SK"),
            F.col("k.PROV_ID").alias("PROV_ID"),
            F.col("k.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            F.col("k.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
            F.col("all.MBR_SK").alias("MBR_SK"),
            F.col("all.PCMH_PROV_SK").alias("PCMH_PROV_SK"),
            F.col("all.PROV_SK").alias("PROV_SK"),
            F.col("all.PROV_GRP_PROV_ID").alias("PROV_GRP_PROV_ID"),
            F.col("all.PROV_GRP_PROV_SK").alias("PROV_GRP_PROV_SK"),
            F.col("all.ROW_TERM_DT_SK").alias("ROW_TERM_DT_SK"),
            F.col("all.PCMH_MSTR_PATN_ID").alias("PCMH_MSTR_PATN_ID"),
            F.col("all.PCMH_PATN_ID").alias("PCMH_PATN_ID"),
        )
    )

    # --------------------------------------------------
    # Return container output
    # --------------------------------------------------
    return df_Key