# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared-Container : LabRsltPK
Description      : Shared container used for Primary Keying of Lab Result
Called by        : LabCorpLabRsltExtr, QuestLabRsltExtr, FlatFileLabRsltExtr

Assign primary surrogate key
Load IDS temp. table
join primary key info with table info
update primary key table (K_LAB_RSLT) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
SQL joins temp table with key table to assign known keys
Hash file (hf_lab_rslt_allcol and hf_lab_rslt_transform) cleared in the calling program - LabCorpLabRsltExtr/QuestLabRsltExtr/FlatFileLabRsltExtr
This container is used in:
LabCorpLabRsltExtr
QuestLabRsltExtr
FlatFileLabRsltExtr

These programs need to be re-compiled when logic changes
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# COMMAND ----------
def run_LabRsltPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ---------------------------------------------------------
    # Unpack required run-time parameters exactly once
    # ---------------------------------------------------------
    IDSOwner        = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    CurrRunCycle    = params["CurrRunCycle"]
    SrcSysCd        = params["SrcSysCd"]
    ids_jdbc_url    = params["ids_jdbc_url"]
    ids_jdbc_props  = params["ids_jdbc_props"]
    adls_path       = params["adls_path"]
    adls_path_raw   = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # ---------------------------------------------------------
    # Hashed-file: hf_lab_rslt_allcol  (scenario a – intermediate)
    #              remove duplicates on full primary-key
    # ---------------------------------------------------------
    allcol_key_cols = [
        "MBR_UNIQ_KEY","SVC_DT_SK","PROC_CD","PROC_CD_TYP_CD","LOINC_CD",
        "DIAG_CD_1","DIAG_CD_TYP_CD_1","RSLT_ID","ORDER_TST_ID",
        "PATN_ENCNTR_ID","SRC_SYS_CD"
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        allcol_key_cols,
        []
    )
    # ---------------------------------------------------------
    # Hashed-file: hf_lab_rslt_transform  (scenario a – intermediate)
    #              remove duplicates on full primary-key
    # ---------------------------------------------------------
    transform_key_cols = [
        "MBR_UNIQ_KEY","SVC_DT_SK","PROC_CD","PROC_CD_TYP_CD","LOINC_CD",
        "DIAG_CD_1","DIAG_CD_TYP_CD_1","RSLT_ID","ORDER_TST_ID",
        "PATN_ENCNTR_ID","SRC_SYS_CD_SK"
    ]
    df_Transform_dedup = dedup_sort(
        df_Transform,
        transform_key_cols,
        []
    )
    # ---------------------------------------------------------
    # Truncate temp table  &  load new data
    # ---------------------------------------------------------
    execute_dml(
        f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_LAB_RSLT_TEMP')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    (
        df_Transform_dedup
        .write
        .mode("append")
        .jdbc(url=ids_jdbc_url, table=f"{IDSOwner}.K_LAB_RSLT_TEMP", properties=ids_jdbc_props)
    )
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_LAB_RSLT_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ---------------------------------------------------------
    # Build W_Extract (union query reproduced exactly)
    # ---------------------------------------------------------
    extract_query = f"""
    SELECT
         k.LAB_RSLT_SK,
         w.MBR_UNIQ_KEY,
         w.SVC_DT_SK,
         w.PROC_CD,
         w.PROC_CD_TYP_CD,
         w.LOINC_CD,
         w.DIAG_CD_1,
         w.DIAG_CD_TYP_CD_1,
         w.RSLT_ID,
         w.ORDER_TST_ID,
         w.PATN_ENCNTR_ID,
         w.SRC_SYS_CD_SK,
         k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_LAB_RSLT_TEMP w
    JOIN {IDSOwner}.K_LAB_RSLT k
      ON w.MBR_UNIQ_KEY     = k.MBR_UNIQ_KEY
     AND w.SVC_DT_SK        = k.SVC_DT_SK
     AND w.PROC_CD          = k.PROC_CD
     AND w.PROC_CD_TYP_CD   = k.PROC_CD_TYP_CD
     AND w.LOINC_CD         = k.LOINC_CD
     AND w.DIAG_CD_1        = k.DIAG_CD_1
     AND w.DIAG_CD_TYP_CD_1 = k.DIAG_CD_TYP_CD_1
     AND w.RSLT_ID          = k.RSLT_ID
     AND w.ORDER_TST_ID     = k.ORDER_TST_ID
     AND w.PATN_ENCNTR_ID   = k.PATN_ENCNTR_ID
     AND w.SRC_SYS_CD_SK    = k.SRC_SYS_CD_SK
    UNION
    SELECT
         -1,
         w2.MBR_UNIQ_KEY,
         w2.SVC_DT_SK,
         w2.PROC_CD,
         w2.PROC_CD_TYP_CD,
         w2.LOINC_CD,
         w2.DIAG_CD_1,
         w2.DIAG_CD_TYP_CD_1,
         w2.RSLT_ID,
         w2.ORDER_TST_ID,
         w2.PATN_ENCNTR_ID,
         w2.SRC_SYS_CD_SK,
         {CurrRunCycle}
    FROM {IDSOwner}.K_LAB_RSLT_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_LAB_RSLT k2
        WHERE w2.MBR_UNIQ_KEY     = k2.MBR_UNIQ_KEY
          AND w2.SVC_DT_SK        = k2.SVC_DT_SK
          AND w2.PROC_CD          = k2.PROC_CD
          AND w2.PROC_CD_TYP_CD   = k2.PROC_CD_TYP_CD
          AND w2.LOINC_CD         = k2.LOINC_CD
          AND w2.DIAG_CD_1        = k2.DIAG_CD_1
          AND w2.DIAG_CD_TYP_CD_1 = k2.DIAG_CD_TYP_CD_1
          AND w2.RSLT_ID          = k2.RSLT_ID
          AND w2.ORDER_TST_ID     = k2.ORDER_TST_ID
          AND w2.PATN_ENCNTR_ID   = k2.PATN_ENCNTR_ID
          AND w2.SRC_SYS_CD_SK    = k2.SRC_SYS_CD_SK
    )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # ---------------------------------------------------------
    # Primary-key derivations
    # ---------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("LAB_RSLT_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None).cast(IntegerType()))
             .otherwise(F.col("LAB_RSLT_SK").cast(IntegerType()))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle).cast(IntegerType()))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK").cast(IntegerType()))
        )
    )
    # Populate missing surrogate keys
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)
    # ---------------------------------------------------------
    # Build updt (hash-file sink), newkeys (seq-file sink), and keys (for Merge)
    # ---------------------------------------------------------
    df_updt = (
        df_enriched
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .select(
            "MBR_UNIQ_KEY","SVC_DT_SK","PROC_CD","PROC_CD_TYP_CD","LOINC_CD",
            "DIAG_CD_1","DIAG_CD_TYP_CD_1","RSLT_ID","ORDER_TST_ID","PATN_ENCNTR_ID",
            "SRC_SYS_CD","svCrtRunCycExctnSk","svSK"
        )
        .withColumnRenamed("svCrtRunCycExctnSk","CRT_RUN_CYC_EXCTN_SK")
        .withColumnRenamed("svSK","LAB_RLST_SK")
    )
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(
            "MBR_UNIQ_KEY","SVC_DT_SK","PROC_CD","PROC_CD_TYP_CD","LOINC_CD",
            "DIAG_CD_1","DIAG_CD_TYP_CD_1","RSLT_ID","ORDER_TST_ID","PATN_ENCNTR_ID",
            "SRC_SYS_CD_SK","svCrtRunCycExctnSk","svSK"
        )
        .withColumnRenamed("svCrtRunCycExctnSk","CRT_RUN_CYC_EXCTN_SK")
        .withColumnRenamed("svSK","LAB_RSLT_SK")
    )
    df_keys = (
        df_enriched
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn("DIAG_CD", F.col("DIAG_CD_1"))
        .withColumn("DIAG_CD_TYP_CD", F.col("DIAG_CD_TYP_CD_1"))
        .select(
            F.col("svSK").alias("LAB_RSLT_SK"),
            "SRC_SYS_CD_SK","SRC_SYS_CD",
            "MBR_UNIQ_KEY","SVC_DT_SK","PROC_CD","PROC_CD_TYP_CD","LOINC_CD",
            "DIAG_CD","DIAG_CD_TYP_CD",
            "RSLT_ID","ORDER_TST_ID","PATN_ENCNTR_ID",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # ---------------------------------------------------------
    # Write hash-file sink (parquet) & sequential-file sink (CSV)
    # ---------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/LabRsltPK_updt.parquet",
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_LAB_RSLT.dat",
        ",",
        "overwrite",
        False,
        True,
        "\"",
        None
    )
    # ---------------------------------------------------------
    # Merge transformer logic
    # ---------------------------------------------------------
    join_expr = (
        (F.col("all.MBR_UNIQ_KEY") == F.col("k.MBR_UNIQ_KEY")) &
        (F.col("all.SVC_DT_SK")     == F.col("k.SVC_DT_SK"))   &
        (F.col("all.PROC_CD")       == F.col("k.PROC_CD"))     &
        (F.col("all.PROC_CD_TYP_CD")== F.col("k.PROC_CD_TYP_CD")) &
        (F.col("all.LOINC_CD")      == F.col("k.LOINC_CD"))    &
        (F.col("all.DIAG_CD_1")     == F.col("k.DIAG_CD"))     &
        (F.col("all.DIAG_CD_TYP_CD_1") == F.col("k.DIAG_CD_TYP_CD")) &
        (F.col("all.RSLT_ID")       == F.col("k.RSLT_ID"))     &
        (F.col("all.ORDER_TST_ID")  == F.col("k.ORDER_TST_ID"))&
        (F.col("all.PATN_ENCNTR_ID")== F.col("k.PATN_ENCNTR_ID"))
    )
    df_load = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("all.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("all.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.LAB_RSLT_SK"),
            F.col("k.MBR_UNIQ_KEY"),
            F.col("k.SVC_DT_SK"),
            F.col("k.PROC_CD"),
            F.col("k.PROC_CD_TYP_CD"),
            F.col("k.LOINC_CD"),
            F.col("k.DIAG_CD").alias("DIAG_CD_1"),
            F.col("k.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD_1"),
            F.col("k.RSLT_ID"),
            F.col("k.ORDER_TST_ID"),
            F.col("k.PATN_ENCNTR_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.DIAG_CD_2"),
            F.col("all.DIAG_CD_3"),
            F.col("all.SVRC_PROV_ID"),
            F.col("all.NORM_RSLT_IN"),
            F.col("all.ORDER_DT_SK"),
            F.col("all.SRC_SYS_EXTR_DT_SK"),
            F.col("all.NUM_RSLT_VAL"),
            F.col("all.RSLT_NORM_HI_VAL"),
            F.col("all.RSLT_NORM_LOW_VAL"),
            F.col("all.ORDER_TST_NM"),
            F.col("all.RSLT_DESC"),
            F.col("all.RSLT_LONG_DESC_1"),
            F.col("all.RSLT_LONG_DESC_2"),
            F.col("all.RSLT_MESR_UNIT_DESC"),
            F.col("all.RSLT_RNG_DESC"),
            F.col("all.SPCMN_ID"),
            F.col("all.SRC_SYS_ORDER_PROV_ID"),
            F.col("all.SRC_SYS_PROC_CD_TX"),
            F.col("all.TX_RSLT_VAL"),
            F.col("all.ORDER_PROV_SRC_SYS_CD")
        )
    )
    # ---------------------------------------------------------
    # return final container output stream
    # ---------------------------------------------------------
    return df_load
# COMMAND ----------