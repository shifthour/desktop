# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME: UwsBioMesrClsExtr
# MAGIC 
# MAGIC CALLED BY:  UwsBioMesrClsExtrSeq
# MAGIC 
# MAGIC PROCESSING:  Extracts data from UWS BIO_MESR_CLS table and creates a file that is used by the IdsBioMesrClsFkey job.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Raja Gummadi                12-19-2011      4765  - CDM                    Original Programming                                                                            IntegrateCurDevl           SAndrew                      2012-01-12

# MAGIC Balancing Snapshot
# MAGIC Extract UWS Data
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
UWSOwner = get_widget_value('UWSOwner','')
uws_secret_name = get_widget_value('uws_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
RunID = get_widget_value('RunID','')
SourceSK = get_widget_value('SourceSK','')

# Read config for UWS (ODBC/JDBC)
jdbc_url_uws, jdbc_props_uws = get_db_config(uws_secret_name)

# ----------------------------------------------------------------------------
# Stage: hf_bio_mesr_cls_lkup (CHashedFileStage) - Scenario C => read parquet
# ----------------------------------------------------------------------------
df_hf_bio_mesr_cls_lkup = spark.read.parquet("hf_bio_mesr_cls_lkup.parquet")

# ----------------------------------------------------------------------------
# Stage: BIO_MESR_CLS (CODBCStage)
# Two output pins:
#   1) BioMesrCls -> (select from UWSOwner.BIO_MESR_CLS)
#   2) BioClsRank -> (select from UWSOwner.BIO_CLS_RANK)
# According to guidelines for parameterized "WHERE ... = ?", read entire table.
# ----------------------------------------------------------------------------

# 1) BioMesrCls
extract_query_biomesrcls = (
    f"SELECT BIO_MESR_CLS.BIO_MESR_TYP_CD, BIO_MESR_CLS.GNDR_CD, "
    f"BIO_MESR_CLS.AGE_RNG_MIN_YR_NO, BIO_MESR_CLS.AGE_RNG_MAX_YR_NO, "
    f"BIO_MESR_CLS.BIO_MESR_RNG_LOW_NO, BIO_MESR_CLS.BIO_MESR_RNG_HI_NO, "
    f"BIO_MESR_CLS.EFF_DT_SK, BIO_MESR_CLS.TERM_DT_SK, "
    f"BIO_MESR_CLS.BIO_CLS_CD, BIO_MESR_CLS.USER_ID, BIO_MESR_CLS.LAST_UPDT_DT_SK "
    f"FROM {UWSOwner}.BIO_MESR_CLS BIO_MESR_CLS"
)
df_BIO_MESR_CLS_BioMesrCls = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_biomesrcls)
    .load()
)

# 2) BioClsRank
extract_query_bioclsrank = (
    f"SELECT BIO_CLS_RANK.BIO_CLS_CD, BIO_CLS_RANK.BIO_CLS_RANK_NO, "
    f"BIO_CLS_RANK.TERM_DT_SK "
    f"FROM {UWSOwner}.BIO_CLS_RANK BIO_CLS_RANK"
)
df_BIO_MESR_CLS_BioClsRank = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_bioclsrank)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Strip (CTransformerStage)
# Input: df_BIO_MESR_CLS_BioMesrCls (pin "BioMesrCls")
# Output pin "Uws_Input"
# ----------------------------------------------------------------------------
df_Strip_Uws_Input = (
    df_BIO_MESR_CLS_BioMesrCls
    .withColumn("BIO_MESR_TYP_CD", trim(F.col("BIO_MESR_TYP_CD")))
    .withColumn("GNDR_CD", trim(F.col("GNDR_CD")))
    .withColumn("AGE_RNG_MIN_YR_NO", F.col("AGE_RNG_MIN_YR_NO"))
    .withColumn("AGE_RNG_MAX_YR_NO", F.col("AGE_RNG_MAX_YR_NO"))
    .withColumn("BIO_MESR_RNG_LOW_NO", F.col("BIO_MESR_RNG_LOW_NO"))
    .withColumn("BIO_MESR_RNG_HI_NO", F.col("BIO_MESR_RNG_HI_NO"))
    .withColumn("EFF_DT_SK", trim(F.col("EFF_DT_SK")))
    .withColumn("TERM_DT_SK", trim(F.col("TERM_DT_SK")))
    .withColumn("BIO_CLS_CD", trim(F.col("BIO_CLS_CD")))
    .withColumn("USER_ID", trim(F.col("USER_ID")))
    .withColumn("LAST_UPDT_DT_SK", trim(F.col("LAST_UPDT_DT_SK")))
    .select(
        F.col("BIO_MESR_TYP_CD").alias("BIO_MESR_TYP_CD"),
        F.col("GNDR_CD").alias("GNDR_CD"),
        F.col("AGE_RNG_MIN_YR_NO").alias("AGE_RNG_MIN_YR_NO"),
        F.col("AGE_RNG_MAX_YR_NO").alias("AGE_RNG_MAX_YR_NO"),
        F.col("BIO_MESR_RNG_LOW_NO").alias("BIO_MESR_RNG_LOW_NO"),
        F.col("BIO_MESR_RNG_HI_NO").alias("BIO_MESR_RNG_HI_NO"),
        F.col("EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("BIO_CLS_CD").alias("BIO_CLS_CD"),
        F.col("USER_ID").alias("USER_ID"),
        F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
    )
)

# ----------------------------------------------------------------------------
# Stage: hf_bio_cls_rank_temp (CHashedFileStage) - Scenario C => write parquet
# Input: df_BIO_MESR_CLS_BioClsRank (pin "BioClsRank")
# Output pin "Ref"
# ----------------------------------------------------------------------------
df_hf_bio_cls_rank_temp = df_BIO_MESR_CLS_BioClsRank.select(
    F.col("BIO_CLS_CD"),
    F.col("BIO_CLS_RANK_NO"),
    F.col("TERM_DT_SK")
)

write_files(
    df_hf_bio_cls_rank_temp,
    "hf_bio_cls_rank_temp.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# We will keep df_hf_bio_cls_rank_temp in memory for the reference join:
# (No re-read from parquet is strictly necessary in this script, but scenario C requires a write.)

# ----------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# Primary link: df_Strip_Uws_Input (alias "Uws_Input")
# Reference link: df_hf_bio_cls_rank_temp (alias "Ref"), left join on BIO_CLS_CD
# Output pin: "Transform"
# ----------------------------------------------------------------------------
df_BusinessRules = (
    df_Strip_Uws_Input.alias("Uws_Input")
    .join(
        df_hf_bio_cls_rank_temp.alias("Ref"),
        F.col("Uws_Input.BIO_CLS_CD") == F.col("Ref.BIO_CLS_CD"),
        "left"
    )
)

df_BusinessRules = (
    df_BusinessRules
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit("BCBSKC"))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            ";",
            F.col("Uws_Input.BIO_MESR_TYP_CD"),
            F.col("Uws_Input.GNDR_CD"),
            F.col("Uws_Input.AGE_RNG_MIN_YR_NO"),
            F.col("Uws_Input.AGE_RNG_MAX_YR_NO"),
            F.col("Uws_Input.BIO_MESR_RNG_LOW_NO"),
            F.col("Uws_Input.BIO_MESR_RNG_HI_NO"),
            F.col("Uws_Input.EFF_DT_SK")
        )
    )
    .withColumn("BIO_MESR_CLS_SK", F.lit(0))
    .withColumn("BIO_MESR_TYP_CD", F.col("Uws_Input.BIO_MESR_TYP_CD"))
    .withColumn("GNDR_CD", F.col("Uws_Input.GNDR_CD"))
    .withColumn("AGE_RNG_MIN_YR_NO", F.col("Uws_Input.AGE_RNG_MIN_YR_NO"))
    .withColumn("AGE_RNG_MAX_YR_NO", F.col("Uws_Input.AGE_RNG_MAX_YR_NO"))
    .withColumn("BIO_MESR_RNG_LOW_NO", F.col("Uws_Input.BIO_MESR_RNG_LOW_NO"))
    .withColumn("BIO_MESR_RNG_HI_NO", F.col("Uws_Input.BIO_MESR_RNG_HI_NO"))
    .withColumn("EFF_DT_SK", F.col("Uws_Input.EFF_DT_SK"))
    .withColumn("SRC_SYS_CD_SK", F.lit(SourceSK))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "TERM_DT_SK",
        F.when(
            F.col("Uws_Input.TERM_DT_SK").isNull() | (F.length(F.col("Uws_Input.TERM_DT_SK")) == 0),
            F.lit("2199-12-31")
        ).otherwise(F.col("Uws_Input.TERM_DT_SK"))
    )
    .withColumn("BIO_CLS_CD", F.col("Uws_Input.BIO_CLS_CD"))
    .withColumn(
        "BIO_CLS_RANK_NO",
        F.when(
            (F.col("Ref.BIO_CLS_CD").isNotNull()) & (F.col("Ref.TERM_DT_SK") == F.lit("2199-12-31")),
            trim(F.col("Ref.BIO_CLS_RANK_NO"))
        ).otherwise(F.lit(999))
    )
    .withColumn("USER_ID", F.col("Uws_Input.USER_ID"))
    .withColumn("LAST_UPDT_DT_SK", F.col("Uws_Input.LAST_UPDT_DT_SK"))
)

# ----------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage)
# Reference link: df_hf_bio_mesr_cls_lkup as "lkup" (left join)
# Primary link: df_BusinessRules as "Transform"
# Stage variables:
#   SK => if lkup.BIO_MESR_CLS_SK is null then KeyMgtGetNextValueConcurrent else lkup.BIO_MESR_CLS_SK
#   NewCrtRunCycExtcnSk => if lkup.BIO_MESR_CLS_SK is null then CurrRunCycle else lkup.CRT_RUN_CYC_EXCTN_SK
# Outputs: 
#   "Key" (all rows)
#   "updt" (rows where lkup.BIO_MESR_CLS_SK is null)
# ----------------------------------------------------------------------------
df_PrimaryKey = (
    df_BusinessRules.alias("Transform")
    .join(
        df_hf_bio_mesr_cls_lkup.alias("lkup"),
        [
            F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"),
            F.col("Transform.BIO_MESR_TYP_CD") == F.col("lkup.BIO_MESR_TYP_CD"),
            F.col("Transform.GNDR_CD") == F.col("lkup.GNDR_CD"),
            F.col("Transform.AGE_RNG_MIN_YR_NO") == F.col("lkup.AGE_RNG_MIN_YR_NO"),
            F.col("Transform.AGE_RNG_MAX_YR_NO") == F.col("lkup.AGE_RNG_MAX_YR_NO"),
            F.col("Transform.BIO_MESR_RNG_LOW_NO") == F.col("lkup.BIO_MESR_RNG_LOW_NO"),
            F.col("Transform.BIO_MESR_RNG_HI_NO") == F.col("lkup.BIO_MESR_RNG_HI_NO"),
            F.col("Transform.EFF_DT_SK") == F.col("lkup.EFF_DT_SK")
        ],
        "left"
    )
)

df_enriched = df_PrimaryKey.withColumn(
    "BIO_MESR_CLS_SK",
    F.when(F.col("lkup.BIO_MESR_CLS_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.BIO_MESR_CLS_SK"))
).withColumn(
    "NewCrtRunCycExtcnSk",
    F.when(F.col("lkup.BIO_MESR_CLS_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

# SurrogateKeyGen call for "BIO_MESR_CLS_SK" because KeyMgtGetNextValueConcurrent was used
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"BIO_MESR_CLS_SK",<schema>,<secret_name>)

df_enriched = df_enriched.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(CurrRunCycle)
)

# Now prepare the "Key" link output (all rows in df_enriched), with final columns in correct order:
df_Key = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("BIO_MESR_CLS_SK").alias("BIO_MESR_CLS_SK"),
    F.col("BIO_MESR_TYP_CD").alias("BIO_MESR_TYP_CD"),
    F.col("GNDR_CD").alias("GNDR_CD"),
    F.col("AGE_RNG_MIN_YR_NO").alias("AGE_RNG_MIN_YR_NO"),
    F.col("AGE_RNG_MAX_YR_NO").alias("AGE_RNG_MAX_YR_NO"),
    F.col("BIO_MESR_RNG_LOW_NO").alias("BIO_MESR_RNG_LOW_NO"),
    F.col("BIO_MESR_RNG_HI_NO").alias("BIO_MESR_RNG_HI_NO"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("BIO_CLS_CD").alias("BIO_CLS_CD"),
    F.col("BIO_CLS_RANK_NO").alias("BIO_CLS_RANK_NO"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK")
)

# For char columns in the final output, apply rpad as required by length:
df_Key = (
    df_Key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
)

# The "updt" link => rows where lkup.BIO_MESR_CLS_SK is null
df_updt = df_enriched.filter(F.col("lkup.BIO_MESR_CLS_SK").isNull()).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BIO_MESR_TYP_CD").alias("BIO_MESR_TYP_CD"),
    F.col("GNDR_CD").alias("GNDR_CD"),
    F.col("AGE_RNG_MIN_YR_NO").alias("AGE_RNG_MIN_YR_NO"),
    F.col("AGE_RNG_MAX_YR_NO").alias("AGE_RNG_MAX_YR_NO"),
    F.col("BIO_MESR_RNG_LOW_NO").alias("BIO_MESR_RNG_LOW_NO"),
    F.col("BIO_MESR_RNG_HI_NO").alias("BIO_MESR_RNG_HI_NO"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("BIO_MESR_CLS_SK").alias("BIO_MESR_CLS_SK")
)

# ----------------------------------------------------------------------------
# Stage: BioMesrClsFileOut (CSeqFileStage)
# Input pin "Key" => df_Key
# Write to ADLS path with .dat, preserving columns in order
# The path is "key/UwsBioMesrClsExtr.BioMesrCls.dat.#RunID#", which goes to f"{adls_path}/key/UwsBioMesrClsExtr.BioMesrCls.dat.{RunID}"
# ----------------------------------------------------------------------------
file_out_path = f"{adls_path}/key/UwsBioMesrClsExtr.BioMesrCls.dat.{RunID}"
write_files(
    df_Key,
    file_out_path,
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# ----------------------------------------------------------------------------
# Stage: hf_bio_mesr_cls (CHashedFileStage) - Scenario C => write parquet
# Input pin "updt" => df_updt
# Filename "hf_bio_mesr_cls"
# ----------------------------------------------------------------------------
write_files(
    df_updt,
    "hf_bio_mesr_cls.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# ----------------------------------------------------------------------------
# Stage: BIO_MESR_CLS1 (CODBCStage)
# Output pin: BioMesrCls => read entire {UWSOwner}.BIO_MESR_CLS again for needed columns
# ----------------------------------------------------------------------------
extract_query_biomesrcls1 = (
    f"SELECT BIO_MESR_CLS.BIO_MESR_TYP_CD, BIO_MESR_CLS.GNDR_CD, "
    f"BIO_MESR_CLS.AGE_RNG_MIN_YR_NO, BIO_MESR_CLS.AGE_RNG_MAX_YR_NO, "
    f"BIO_MESR_CLS.BIO_MESR_RNG_LOW_NO, BIO_MESR_CLS.BIO_MESR_RNG_HI_NO, "
    f"BIO_MESR_CLS.EFF_DT_SK, BIO_MESR_CLS.TERM_DT_SK, BIO_MESR_CLS.BIO_CLS_CD, "
    f"BIO_MESR_CLS.USER_ID, BIO_MESR_CLS.LAST_UPDT_DT_SK "
    f"FROM {UWSOwner}.BIO_MESR_CLS BIO_MESR_CLS"
)
df_BIO_MESR_CLS1_BioMesrCls = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_uws)
    .options(**jdbc_props_uws)
    .option("query", extract_query_biomesrcls1)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Transform (CTransformerStage), input pin "BioMesrCls" => df_BIO_MESR_CLS1_BioMesrCls
# Output pin "Snapshot"
# ----------------------------------------------------------------------------
df_Transform_Snapshot = (
    df_BIO_MESR_CLS1_BioMesrCls
    .withColumn("BIO_MESR_TYP_CD", trim(F.col("BIO_MESR_TYP_CD")))
    .withColumn("GNDR_CD", trim(F.col("GNDR_CD")))
    .withColumn("AGE_RNG_MIN_YR_NO", trim(F.col("AGE_RNG_MIN_YR_NO")))
    .withColumn("AGE_RNG_MAX_YR_NO", trim(F.col("AGE_RNG_MAX_YR_NO")))
    .withColumn("BIO_MESR_RNG_LOW_NO", trim(F.col("BIO_MESR_RNG_LOW_NO")))
    .withColumn("BIO_MESR_RNG_HI_NO", trim(F.col("BIO_MESR_RNG_HI_NO")))
    .withColumn("EFF_DT_SK", trim(F.col("EFF_DT_SK")))
    .select(
        F.col("BIO_MESR_TYP_CD").alias("BIO_MESR_TYP_CD"),
        F.col("GNDR_CD").alias("GNDR_CD"),
        F.col("AGE_RNG_MIN_YR_NO").alias("AGE_RNG_MIN_YR_NO"),
        F.col("AGE_RNG_MAX_YR_NO").alias("AGE_RNG_MAX_YR_NO"),
        F.col("BIO_MESR_RNG_LOW_NO").alias("BIO_MESR_RNG_LOW_NO"),
        F.col("BIO_MESR_RNG_HI_NO").alias("BIO_MESR_RNG_HI_NO"),
        F.col("EFF_DT_SK").alias("EFF_DT_SK"),
        F.lit(SourceSK).alias("SRC_SYS_CD_SK")
    )
)

# ----------------------------------------------------------------------------
# Stage: Snapshot (CSeqFileStage), input pin "Snapshot" => df_Transform_Snapshot
# Write to "load/B_BIO_MESR_CLS.dat" => f"{adls_path}/load/B_BIO_MESR_CLS.dat"
# ----------------------------------------------------------------------------
file_snapshot_path = f"{adls_path}/load/B_BIO_MESR_CLS.dat"
write_files(
    df_Transform_Snapshot,
    file_snapshot_path,
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)