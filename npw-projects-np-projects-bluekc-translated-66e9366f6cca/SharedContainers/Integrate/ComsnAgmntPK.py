# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
Shared container converted from IBM DataStage job 'ComsnAgmntPK'

IDS Primary Key Container for Commission Agmnt  
Used by: FctsComsnAgmntExtr  

Processing overview:  
 1. Deduplicate incoming “AllCol” stream.  
 2. Query IDS temp/key tables to get existing / new surrogate keys.  
 3. Generate new surrogate keys when required.  
 4. Persist updates to hashed-file replacement (parquet) and write new keys
    to a sequential file.  
 5. Produce the key-enriched output stream.  
"""

def run_ComsnAgmntPK(
        df_AllCol: DataFrame,
        params: dict
) -> DataFrame:
    # --------------------------------------------------
    # Unpack runtime parameters
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]
    # --------------------------------------------------
    # hf_comsn_agmnt_allcol  – scenario a (intermediate hash file)
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=[
            "SRC_SYS_CD_SK",
            "COMSN_ARGMT_ID",
            "EFF_DT",
            "AGNT_ID"
        ],
        sort_cols=[("SRC_SYS_CD_SK", "A")]
    )
    # --------------------------------------------------
    # K_COMSN_AGMNT_TEMP  – build W_Extract
    extract_query = f"""
    SELECT  k.COMSN_AGMNT_SK,
            w.SRC_SYS_CD_SK,
            w.COMSN_ARGMT_ID,
            w.EFF_DT_SK,
            w.AGNT_ID,
            k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_COMSN_AGMNT_TEMP w,
           {IDSOwner}.K_COMSN_AGMNT       k
     WHERE w.SRC_SYS_CD_SK  = k.SRC_SYS_CD_SK
       AND w.COMSN_ARGMT_ID = k.COMSN_ARGMT_ID
       AND w.EFF_DT_SK      = k.EFF_DT_SK
       AND w.AGNT_ID        = k.AGNT_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.COMSN_ARGMT_ID,
           w2.EFF_DT_SK,
           w2.AGNT_ID,
           {CurrRunCycle}
      FROM {IDSOwner}.K_COMSN_AGMNT_TEMP w2
     WHERE NOT EXISTS (
           SELECT 1
             FROM {IDSOwner}.K_COMSN_AGMNT k2
            WHERE w2.SRC_SYS_CD_SK  = k2.SRC_SYS_CD_SK
              AND w2.EFF_DT_SK      = k2.EFF_DT_SK
              AND w2.COMSN_ARGMT_ID = k2.COMSN_ARGMT_ID
              AND w2.AGNT_ID        = k2.AGNT_ID )
    """
    df_w_extract = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )
    # --------------------------------------------------
    # PrimaryKey transformer – enrich & generate surrogate key
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("COMSN_AGMNT_SK") == F.lit(-1), F.lit("I"))
             .otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit("FACETS"))
        .withColumn("svComsnArgmtId", F.trim(F.col("COMSN_ARGMT_ID")))
        .withColumn("svAgntId",      F.trim(F.col("AGNT_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    # surrogate key generation (special placeholder call)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'COMSN_AGMNT_SK',<schema>,<secret_name>)
    # --------------------------------------------------
    # Streams produced by PrimaryKey stage
    df_updt = (
        df_enriched.select(
            F.lit(SrcSysCd)           .alias("SRC_SYS_CD"),
            F.col("svComsnArgmtId")   .alias("COMSN_ARGMT_ID"),
            F.col("EFF_DT_SK"),
            F.col("svAgntId")         .alias("AGNT_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("COMSN_AGMNT_SK")
        )
    )
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svComsnArgmtId")   .alias("COMSN_ARGMT_ID"),
            F.col("EFF_DT_SK"),
            F.col("svAgntId")         .alias("AGNT_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("COMSN_AGMNT_SK")
        )
    )
    df_keys = (
        df_enriched.select(
            F.col("COMSN_AGMNT_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("svComsnArgmtId")   .alias("COMSN_ARGMT_ID"),
            F.col("EFF_DT_SK"),
            F.col("svAgntId")         .alias("AGNT_ID"),
            F.lit(SrcSysCd)           .alias("SRC_SYS_CD"),
            F.col("svInstUpdt")       .alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # --------------------------------------------------
    # hf_comsn_agmnt  – scenario c (write parquet)
    write_files(
        df_updt,
        f"{adls_path}/ComsnAgmntPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )
    # K_COMSN_AGMNT sequential file  (new keys)
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_COMSN_AGMNT.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # --------------------------------------------------
    # Merge stage – left-join AllColOut with Keys
    join_expr = [
        df_allcol_dedup["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"],
        df_allcol_dedup["COMSN_ARGMT_ID"] == df_keys["COMSN_ARGMT_ID"],
        df_allcol_dedup["AGNT_ID"]        == df_keys["AGNT_ID"]
    ]
    df_final = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.COMSN_AGMNT_SK"),
            F.col("all.COMSN_ARGMT_ID"),
            F.col("all.EFF_DT"),
            F.col("all.AGNT_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
            F.col("all.AGNT"),
            F.col("all.COMSN_ARGMT"),
            F.col("all.COMSN_SCHD"),
            F.col("all.COMSN_AGMNT_PT_OF_SCHD_CD"),
            F.col("all.COMSN_AGMNT_TERM_RSN_CD"),
            F.col("all.TERM_DT"),
            F.col("all.SCHD_FCTR")
        )
    )
    # --------------------------------------------------
    return df_final
# COMMAND ----------