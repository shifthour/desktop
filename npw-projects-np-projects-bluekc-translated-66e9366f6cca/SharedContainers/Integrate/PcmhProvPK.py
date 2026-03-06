# Databricks notebook source
# MAGIC %run ./Utility

# COMMAND ----------

# MAGIC %run ./Routine_Functions

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql import functions as F

# COMMAND ----------
def run_PcmhProvPK(
    df_AllColl: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-Container: PcmhProvPK
    Used by
    MddatacorPcmhProvExtr
    SQL joins temp table with key table to assign known keys
    Temp table is truncated before load and runstat done after load
    Load IDS temp. table
    Join primary key info with table info
    Hashed file cleared everytime the job runs
    Update primary key table (K_PCMH_PROV) with new keys created today
    Primary key hash file only contains current run keys
    Assign primary surrogate key
    """

    # --------------------------------------------------
    # Unpack parameters
    CurrRunCycle          = params["CurrRunCycle"]
    SrcSysCd              = params["SrcSysCd"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]
    FilePath              = params.get("FilePath", "")
    # --------------------------------------------------
    # Step 1: Replace intermediate hash file (hf_pcmh_prov_allcol) with dedup logic
    df_allcoll_dedup = df_AllColl.dropDuplicates(
        ["PROV_ID", "ROW_EFF_DT_SK", "SRC_SYS_CD_SK"]
    )

    # --------------------------------------------------
    # Step 2: Load K_PCMH_PROV_TEMP table
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_PCMH_PROV_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_PCMH_PROV_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MED_MGT_NOTE_TX_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # --------------------------------------------------
    # Step 3: Extract W_Extract data
    extract_query = f"""
    SELECT
           k.PCMH_PROV_SK,
           w.PROV_ID,
           w.ROW_EFF_DT_SK,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM
           {IDSOwner}.K_PCMH_PROV_TEMP w,
           {IDSOwner}.K_PCMH_PROV k
    WHERE
           w.PROV_ID        = k.PROV_ID
       AND w.ROW_EFF_DT_SK  = k.ROW_EFF_DT_SK
       AND w.SRC_SYS_CD_SK  = k.SRC_SYS_CD_SK
    UNION
    SELECT
           -1,
           w2.PROV_ID,
           w2.ROW_EFF_DT_SK,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle}
    FROM
           {IDSOwner}.K_PCMH_PROV_TEMP w2
    WHERE NOT EXISTS (
           SELECT 1
           FROM   {IDSOwner}.K_PCMH_PROV k2
           WHERE  w2.PROV_ID       = k2.PROV_ID
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
    # Step 4: Transformer PrimaryKey logic
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("PCMH_PROV_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        key_col="PCMH_PROV_SK",
        <schema>,
        <secret_name>
    )

    # --------------------------------------------------
    # Step 5: Build outputs from PrimaryKey transformer
    df_updt = (
        df_enriched.select(
            "PROV_ID",
            "ROW_EFF_DT_SK",
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "PCMH_PROV_SK"
        )
    )

    df_newkeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I"))
        .select(
            "PROV_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "PCMH_PROV_SK"
        )
    )

    df_keys = (
        df_enriched.select(
            "PCMH_PROV_SK",
            "PROV_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # Step 6: Write hashed-file output (scenario c) as Parquet
    parquet_updt_path = f"{adls_path}/PcmhProvPK_updt.parquet"
    write_files(
        df_updt,
        parquet_updt_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Step 7: Write sequential file K_PCMH_PROV.dat
    seq_file_path = f"{adls_path}/load/K_PCMH_PROV.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Step 8: Merge transformer logic
    join_expr = [
        df_allcoll_dedup["PROV_ID"] == df_keys["PROV_ID"],
        df_allcoll_dedup["ROW_EFF_DT_SK"] == df_keys["ROW_EFF_DT_SK"],
        df_allcoll_dedup["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"]
    ]

    df_final = (
        df_allcoll_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("k.PCMH_PROV_SK").alias("PCMH_PROV_SK"),
            col("k.PROV_ID").alias("PROV_ID"),
            col("k.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
            col("all.PROV_SK").alias("PROV_SK"),
            col("all.ROW_TERM_DT_SK").alias("ROW_TERM_DT_SK"),
            col("all.PCMH_CLNCN_ID").alias("PCMH_CLNCN_ID")
        )
    )

    # --------------------------------------------------
    return df_final

# COMMAND ----------