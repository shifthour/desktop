# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
JobName        : IncmFeeDscntPK
JobType        : Server Job
FolderPath     : Shared Containers/PrimaryKey

DESCRIPTION:
Shared container used for Primary Keying of Income Fee Discount job
CALLED BY : FctsIncomeFeeDscntExtr

ANNOTATIONS:
IDS Primary Key Container for Income Fee Discount
Used by  FctsIncomeFeeDscntExtr
Hash file (hf_fee_dscnt_allcol) cleared in calling job
SQL joins temp table with key table to assign known keys
Temp table is truncated before load and runstat done after load
Load IDS temp. table
join primary key info with table info
update primary key table (K_FEE_DSCNT) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
Assign primary surrogate key
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, trim, lit
from pyspark.sql import functions as F

def run_IncmFeeDscntPK(
        df_AllCol: DataFrame,
        df_Transform: DataFrame,
        params: dict
) -> DataFrame:
    # --------------------------------------------------
    # Un-pack parameters exactly once
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    SrcSysCd          = params["SrcSysCd"]
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]
    adls_path         = params["adls_path"]
    adls_path_raw     = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # --------------------------------------------------
    # Replace intermediate hash-file (hf_fee_dscnt_allcol) with direct DataFrame flow + dedup
    # --------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "FEE_DSCNT_ID"],
        [("FEE_DSCNT_ID", "A")]
    )
    # --------------------------------------------------
    # Load temp table {IDSOwner}.K_FEE_DSCNT_TEMP
    # --------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_FEE_DSCNT_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform
        .select("SRC_SYS_CD_SK", "FEE_DSCNT_ID")
        .write
        .mode("append")
        .jdbc(
            url=ids_jdbc_url,
            table=f"{IDSOwner}.K_FEE_DSCNT_TEMP",
            properties=ids_jdbc_props
        )
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_FEE_DSCNT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # --------------------------------------------------
    # Extract W_Extract (equivalent to DataStage output of K_FEE_DSCNT_TEMP stage)
    # --------------------------------------------------
    w_extract_query = f"""
        SELECT  k.FEE_DSCNT_SK,
                w.SRC_SYS_CD_SK,
                w.FEE_DSCNT_ID,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_FEE_DSCNT_TEMP w
        JOIN {IDSOwner}.K_FEE_DSCNT k
              ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
             AND w.FEE_DSCNT_ID  = k.FEE_DSCNT_ID
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.FEE_DSCNT_ID,
               {CurrRunCycle}
        FROM {IDSOwner}.K_FEE_DSCNT_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_FEE_DSCNT k2
            WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
              AND w2.FEE_DSCNT_ID  = k2.FEE_DSCNT_ID
        )
    """
    df_w_extract = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", w_extract_query)
             .load()
    )
    # --------------------------------------------------
    # Primary-Key transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "FEE_DSCNT_SK",
            when(col("FEE_DSCNT_SK") == -1, None).otherwise(col("FEE_DSCNT_SK"))
        )
        .withColumn(
            "svInstUpdt",
            when(col("FEE_DSCNT_SK").isNull(), lit("I")).otherwise(lit("U"))
        )
        .withColumn("svFeeDscntId", trim(col("FEE_DSCNT_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == "I", lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("SrcSysCd", lit(SrcSysCd))
    )
    # Surrogate-key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"FEE_DSCNT_SK",<schema>,<secret_name>)
    df_enriched = df_enriched.withColumn("svSK", col("FEE_DSCNT_SK"))
    # --------------------------------------------------
    # Build individual output DataFrames
    # --------------------------------------------------
    # 1. updt (to hashed file hf_fee_dscnt – now parquet)
    df_updt = (
        df_enriched
        .select(
            col("SrcSysCd").alias("SRC_SYS_CD"),
            col("svFeeDscntId").alias("FEE_DSCNT_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("FEE_DSCNT_SK")
        )
    )
    # write parquet for hf_fee_dscnt
    write_files(
        df_updt,
        f"{adls_path}/IncmFeeDscntPK_updt.parquet",
        mode="overwrite",
        is_pqruet=True
    )
    # 2. NewKeys (only inserts) – sequential file
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            col("svFeeDscntId").alias("FEE_DSCNT_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("FEE_DSCNT_SK")
        )
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_FEE_DSCNT.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\""
    )
    # 3. Keys – for merge/lookup
    keys_df = (
        df_enriched
        .select(
            col("svSK").alias("FEE_DSCNT_SK"),
            "SRC_SYS_CD_SK",
            col("svFeeDscntId").alias("FEE_DSCNT_ID"),
            col("SrcSysCd").alias("SRC_SYS_CD"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # --------------------------------------------------
    # Merge transformer: combine AllColOut with Keys
    # --------------------------------------------------
    df_key = (
        df_allcol_dedup.alias("all")
        .join(
            keys_df.alias("k"),
            (col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK")) &
            (col("all.FEE_DSCNT_ID")  == col("k.FEE_DSCNT_ID")),
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
            col("k.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
            col("all.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
            col("all.FNCL_LOB").alias("FNCL_LOB_CD"),
            col("all.FEE_DSCNT_CD").alias("FEE_DSCNT_CD"),
            col("all.FEE_DSCNT_LOB_CD").alias("FEE_DSCNT_LOB_CD"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
        )
    )
    # --------------------------------------------------
    # Return the final DataFrame (Key output)
    # --------------------------------------------------
    return df_key