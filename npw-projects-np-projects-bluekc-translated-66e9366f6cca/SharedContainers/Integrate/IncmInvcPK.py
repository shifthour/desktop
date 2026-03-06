
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container  : IncmInvcPK
Job Type          : Server Job
Folder Path       : Shared Containers/PrimaryKey
Description       : Shared container used for Primary Keying of Income Invoice job
Annotations       : 
    - IDS Primary Key Container for Income Invoice
    - Used by FctsIncomeInvcExtr
    - primary key hash file only contains current run keys and is cleared before writing
    - update primary key table (K_INVC) with new keys created today
    - Assign primary surrogate key
    - Hash file (hf_invc_allcol) cleared in calling job
    - join primary key info with table info
    - Load IDS temp. table
    - Temp table is truncated before load and runstat done after load
    - SQL joins temp table with key table to assign known keys
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, trim

def run_IncmInvcPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # unpack parameters – exactly once
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
    # 1. Replace intermediate hash file hf_invc_allcol (scenario a)
    # ------------------------------------------------------------------
    key_cols_hash = ["SRC_SYS_CD_SK", "BILL_INVC_ID"]
    df_allcol_out = df_AllCol.dropDuplicates(key_cols_hash)
    # ------------------------------------------------------------------
    # 2. Process K_INVC_TEMP – write then extract
    # ------------------------------------------------------------------
    temp_table = f"{IDSOwner}.K_INVC_TEMP"
    execute_dml(f"TRUNCATE TABLE {temp_table}", ids_jdbc_url, ids_jdbc_props)
    (
        df_Transform
        .select("SRC_SYS_CD_SK", "BILL_INVC_ID")
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table)
        .mode("append")
        .save()
    )
    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {temp_table} on key columns with distribution "
        f"on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)
    extract_query = f"""
        SELECT  k.INVC_SK,
                w.SRC_SYS_CD_SK,
                w.BILL_INVC_ID,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_INVC_TEMP w
        JOIN {IDSOwner}.K_INVC k
          ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
         AND w.BILL_INVC_ID  = k.BILL_INVC_ID
        UNION
        SELECT -1 AS INVC_SK,
               w2.SRC_SYS_CD_SK,
               w2.BILL_INVC_ID,
               {CurrRunCycle} AS CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_INVC_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_INVC k2
            WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
              AND w2.BILL_INVC_ID  = k2.BILL_INVC_ID
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
    # 3. PrimaryKey Transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("INVC_SK") == lit(-1), lit("I")).otherwise(lit("U")))
        .withColumn("svBillInvcId", trim(col("BILL_INVC_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svSK",
            when(col("svInstUpdt") == lit("I"), lit(None)).otherwise(col("INVC_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # ------------------------------------------------------------------
    # 3a. updt link (to hf_invc – scenario c, parquet)
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .withColumn("SRC_SYS_CD", lit(SrcSysCd))
        .withColumnRenamed("svBillInvcId", "BILL_INVC_ID")
        .withColumnRenamed("svCrtRunCycExctnSk", "CRT_RUN_CYC_EXCTN_SK")
        .withColumnRenamed("svSK", "INVC_SK")
        .select("SRC_SYS_CD", "BILL_INVC_ID", "CRT_RUN_CYC_EXCTN_SK", "INVC_SK")
    )
    write_files(
        df_updt,
        f"{adls_path}/IncmInvcPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # 3b. NewKeys link (sequential file)
    # ------------------------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .withColumnRenamed("svBillInvcId", "BILL_INVC_ID")
        .withColumnRenamed("svCrtRunCycExctnSk", "CRT_RUN_CYC_EXCTN_SK")
        .withColumnRenamed("svSK", "INVC_SK")
        .select("SRC_SYS_CD_SK", "BILL_INVC_ID", "CRT_RUN_CYC_EXCTN_SK", "INVC_SK")
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_INVC.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # 3c. Keys link (for merge)
    # ------------------------------------------------------------------
    df_keys = (
        df_enriched
        .withColumn("SRC_SYS_CD", lit(SrcSysCd))
        .withColumnRenamed("svBillInvcId", "BILL_INVC_ID")
        .withColumnRenamed("svCrtRunCycExctnSk", "CRT_RUN_CYC_EXCTN_SK")
        .withColumnRenamed("svSK", "INVC_SK")
        .withColumnRenamed("svInstUpdt", "INSRT_UPDT_CD")
        .select(
            "INVC_SK",
            "SRC_SYS_CD_SK",
            "BILL_INVC_ID",
            "SRC_SYS_CD",
            "INSRT_UPDT_CD",
            "CRT_RUN_CYC_EXCTN_SK"
        )
    )
    # ------------------------------------------------------------------
    # 4. Merge Transformer logic
    # ------------------------------------------------------------------
    join_cond = [
        df_allcol_out["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"],
        df_allcol_out["BILL_INVC_ID"] == df_keys["BILL_INVC_ID"]
    ]
    df_key_output = (
        df_allcol_out.alias("all")
        .join(df_keys.alias("k"), join_cond, "left")
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
            col("k.INVC_SK"),
            col("all.BILL_INVC_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.BILL_ENTY"),
            col("all.INVC_TYP_CD"),
            col("all.BILL_DUE_DT"),
            col("all.BILL_END_DT"),
            col("all.CRT_DT"),
            col("all.CUR_RCRD_IN")
        )
    )
    # ------------------------------------------------------------------
    # 5. return container output link(s)
    # ------------------------------------------------------------------
    return df_key_output
