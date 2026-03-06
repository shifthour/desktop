# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
ComsnBillEntyPK – IDS Primary Key Container for Commission Bill Enty
Used by FctsBillEntyExtr
Hash file (hf_bill_enty_allcol) cleared in calling job
SQL joins temp table with key table to assign known keys
Temp table is truncated before load and runstat done after load
Load IDS temp. table, join primary key info with table info,
update primary key table (K_BILL_ENTY) with new keys created today,
primary key hash file only contains current run keys and is cleared before writing,
Assign primary surrogate key
"""
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when
from pyspark.sql import functions as F

# COMMAND ----------
def run_ComsnBillEntyPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack parameters
    CurrRunCycle              = params["CurrRunCycle"]
    SrcSysCd                  = params["SrcSysCd"]
    IDSOwner                  = params["IDSOwner"]
    ids_secret_name           = params["ids_secret_name"]
    ids_jdbc_url              = params["ids_jdbc_url"]
    ids_jdbc_props            = params["ids_jdbc_props"]
    adls_path                 = params["adls_path"]
    adls_path_raw             = params["adls_path_raw"]
    adls_path_publish         = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # ------------------------------------------------------------------
    # hf_bill_enty_allcol – intermediate hash file → dedup
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "BILL_ENTY"],
        sort_cols=[]
    )
    # ------------------------------------------------------------------
    # K_BILL_ENTY_TEMP – stage temp table load
    execute_dml(
        f"DROP TABLE IF EXISTS {IDSOwner}.K_BILL_ENTY_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )
    execute_dml(
        f"""CREATE TABLE {IDSOwner}.K_BILL_ENTY_TEMP (
                SRC_SYS_CD_SK INTEGER NOT NULL,
                BILL_ENTY_UNIQ_KEY VARCHAR(<…>) NOT NULL,
                PRIMARY KEY (SRC_SYS_CD_SK, BILL_ENTY_UNIQ_KEY)
            )""",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform
        .select(
            col("SRC_SYS_CD_SK"),
            col("BILL_ENTY_UNIQ_KEY")
        )
        .write.mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_BILL_ENTY_TEMP")
        .save()
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_BILL_ENTY_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ------------------------------------------------------------------
    # W_Extract – pull keys / new rows
    extract_query = f"""
    SELECT k.BILL_ENTY_SK,
           w.SRC_SYS_CD_SK,
           w.BILL_ENTY_UNIQ_KEY,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_BILL_ENTY_TEMP w,
         {IDSOwner}.K_BILL_ENTY k
    WHERE w.SRC_SYS_CD_SK     = k.SRC_SYS_CD_SK
      AND w.BILL_ENTY_UNIQ_KEY = k.BILL_ENTY_UNIQ_KEY
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.BILL_ENTY_UNIQ_KEY,
           {CurrRunCycle}
    FROM {IDSOwner}.K_BILL_ENTY_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.BILL_ENTY_SK
        FROM {IDSOwner}.K_BILL_ENTY k2
        WHERE w2.SRC_SYS_CD_SK     = k2.SRC_SYS_CD_SK
          AND w2.BILL_ENTY_UNIQ_KEY = k2.BILL_ENTY_UNIQ_KEY
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
    # PrimaryKey Transformer
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("BILL_ENTY_SK") == -1, lit("I")).otherwise(lit("U"))
        )
        .withColumn(
            "svSK",
            when(col("BILL_ENTY_SK") == -1, F.lit(None).cast("long")).otherwise(col("BILL_ENTY_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("BILL_ENTY_SK") == -1, lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("SrcSysCd", lit(SrcSysCd))
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # ------------------------------------------------------------------
    # updt link → hf_bill_enty (parquet)
    df_updt = df_enriched.select(
        col("SrcSysCd").alias("SRC_SYS_CD"),
        col("BILL_ENTY_UNIQ_KEY"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("BILL_ENTY_SK")
    )
    write_files(
        df_updt,
        f"{adls_path}/ComsnBillEntyPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # ------------------------------------------------------------------
    # NewKeys link – sequential file
    df_NewKeys = (
        df_enriched
        .filter(col("svInstUpdt") == "I")
        .select(
            col("SRC_SYS_CD_SK"),
            col("BILL_ENTY_UNIQ_KEY"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("BILL_ENTY_SK")
        )
    )
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_BILL_ENTY.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Keys link – for merge
    df_Keys = df_enriched.select(
        col("svSK").alias("BILL_ENTY_SK"),
        col("SRC_SYS_CD_SK"),
        col("BILL_ENTY_UNIQ_KEY"),
        col("SrcSysCd").alias("SRC_SYS_CD"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )
    # ------------------------------------------------------------------
    # Merge Transformer – produce Key output
    df_Key = (
        df_AllColOut.alias("all")
        .join(
            df_Keys.alias("k"),
            (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
            (col("all.BILL_ENTY") == col("k.BILL_ENTY_UNIQ_KEY")),
            "left"
        )
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
            col("k.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
            col("all.BILL_ENTY").alias("BILL_ENTY_UNIQ_KEY"),
            col("all.GRP_ID").alias("GRP_ID"),
            col("all.SUBGRP_ID").alias("SUBGRP_ID"),
            col("all.SUB_ID").alias("SUB_ID"),
            col("all.BILL_ENTY_LVL_CD").alias("BILL_ENTY_LVL_CD"),
            col("all.BILL_ENTY_LVL_UNIQ_KEY").alias("BILL_ENTY_LVL_UNIQ_KEY"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.SUB_CK").alias("SUB_CK")
        )
    )
    # ------------------------------------------------------------------
    return df_Key
# COMMAND ----------