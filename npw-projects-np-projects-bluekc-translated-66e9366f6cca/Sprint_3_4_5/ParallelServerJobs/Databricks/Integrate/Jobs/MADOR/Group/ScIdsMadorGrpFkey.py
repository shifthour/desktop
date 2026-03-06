# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sethuraman Rajendran     03/30/2018      5839- MADOR                 Originally Programmed                            IntegrateDev2            Kalyan Neelam             2018-06-14
# MAGIC Abhiram Dasarathy	         2019-12-12	F-114877		Changed the GRP_ID to GRP_ID_1 in Lkup    IntegrateDev2            Jaideep Mankala         12/13/2019

# MAGIC Look up with CD_MPPNG table for foreign key values.
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

job_name = "ScIdsMadorGrpFkey"

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Stage: GRP (DB2ConnectorPX)
# Read entire table #$IDSOwner#.GRP, renaming GRP_ID -> SRC_DRVD_LKUP_VAL
extract_query_grp = f"SELECT GRP_ID AS SRC_DRVD_LKUP_VAL, GRP_SK FROM {IDSOwner}.GRP"
df_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_grp)
    .load()
)

# Stage: CD_MPPNG (DB2ConnectorPX)
# Read #$IDSOWNER#.CD_MPPNG
extract_query_cd_mppng = f"""
SELECT SRC_DRVD_LKUP_VAL, CD_MPPNG_SK
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_SYS_CD = 'IDS'
  AND SRC_CLCTN_CD = 'IDS'
  AND SRC_DOMAIN_NM = 'STATE'
  AND TRGT_CLCTN_CD = 'IDS'
  AND TRGT_DOMAIN_NM = 'STATE'
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_cd_mppng)
    .load()
)

# Stage: seq_GRP_MA_DOR_Pkey (PxSequentialFile) - Read from file
schema_pkey = StructType([
    StructField("GRP_ID", StringType(), False),
    StructField("TAX_YR", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("AS_OF_DTM", TimestampType(), False),
    StructField("GRP_NM", StringType(), False),
    StructField("GRP_CNTCT_FIRST_NM", StringType(), False),
    StructField("GRP_CNTCT_MIDINIT", StringType(), True),
    StructField("GRP_CNTCT_LAST_NM", StringType(), False),
    StructField("GRP_ADDR_LN_1", StringType(), False),
    StructField("GRP_ADDR_LN_2", StringType(), True),
    StructField("GRP_ADDR_LN_3", StringType(), True),
    StructField("GRP_ADDR_CITY_NM", StringType(), False),
    StructField("GRP_ADDR_ZIP_CD_5", StringType(), False),
    StructField("GRP_ADDR_ZIP_CD_4", StringType(), True),
    StructField("GRP_ST_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP_MA_DOR_SK", IntegerType(), False)
])

file_path_pkey = f"{adls_path}/key/GRP_MA_DOR.{SrcSysCd}.pkey.{RunID}.dat"
df_pkey = (
    spark.read
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .schema(schema_pkey)
    .csv(file_path_pkey)
)

# Stage: LkupFkey (PxLookup): df_pkey left join df_CD_MPPNG
df_LkupFkey = (
    df_pkey.alias("Pkey_Out")
    .join(
        df_CD_MPPNG.alias("lkup"),
        F.col("Pkey_Out.GRP_ST_CD") == F.col("lkup.SRC_DRVD_LKUP_VAL"),
        "left"
    )
    .select(
        F.col("lkup.CD_MPPNG_SK").alias("STATE_CD_SK"),
        F.col("Pkey_Out.GRP_ID").alias("GRP_ID"),
        F.col("Pkey_Out.TAX_YR").alias("TAX_YR"),
        F.col("Pkey_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Pkey_Out.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("Pkey_Out.GRP_NM").alias("GRP_NM"),
        F.col("Pkey_Out.GRP_CNTCT_FIRST_NM").alias("GRP_CNTCT_FIRST_NM"),
        F.col("Pkey_Out.GRP_CNTCT_MIDINIT").alias("GRP_CNTCT_MIDINIT"),
        F.col("Pkey_Out.GRP_CNTCT_LAST_NM").alias("GRP_CNTCT_LAST_NM"),
        F.col("Pkey_Out.GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
        F.col("Pkey_Out.GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
        F.col("Pkey_Out.GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
        F.col("Pkey_Out.GRP_ADDR_CITY_NM").alias("GRP_ADDR_CITY_NM"),
        F.col("Pkey_Out.GRP_ADDR_ZIP_CD_5").alias("GRP_ADDR_ZIP_CD_5"),
        F.col("Pkey_Out.GRP_ADDR_ZIP_CD_4").alias("GRP_ADDR_ZIP_CD_4"),
        F.col("Pkey_Out.GRP_ST_CD").alias("GRP_ST_CD"),
        F.col("Pkey_Out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Pkey_Out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Pkey_Out.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK")
    )
)

# Stage: LkupGrp (PxLookup): df_LkupFkey left join df_GRP
df_LkupGrp = (
    df_LkupFkey.alias("LkupOut1")
    .join(
        df_GRP.alias("lkup"),
        F.col("LkupOut1.GRP_ST_CD") == F.col("lkup.SRC_DRVD_LKUP_VAL"),
        "left"
    )
    .select(
        F.col("LkupOut1.STATE_CD_SK").alias("STATE_CD_SK"),
        F.col("LkupOut1.GRP_ID").alias("GRP_ID"),
        F.col("LkupOut1.TAX_YR").alias("TAX_YR"),
        F.col("LkupOut1.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("LkupOut1.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("LkupOut1.GRP_NM").alias("GRP_NM"),
        F.col("LkupOut1.GRP_CNTCT_FIRST_NM").alias("GRP_CNTCT_FIRST_NM"),
        F.col("LkupOut1.GRP_CNTCT_MIDINIT").alias("GRP_CNTCT_MIDINIT"),
        F.col("LkupOut1.GRP_CNTCT_LAST_NM").alias("GRP_CNTCT_LAST_NM"),
        F.col("LkupOut1.GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
        F.col("LkupOut1.GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
        F.col("LkupOut1.GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
        F.col("LkupOut1.GRP_ADDR_CITY_NM").alias("GRP_ADDR_CITY_NM"),
        F.col("LkupOut1.GRP_ADDR_ZIP_CD_5").alias("GRP_ADDR_ZIP_CD_5"),
        F.col("LkupOut1.GRP_ADDR_ZIP_CD_4").alias("GRP_ADDR_ZIP_CD_4"),
        F.col("LkupOut1.GRP_ST_CD").alias("GRP_ST_CD"),
        F.col("LkupOut1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LkupOut1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("LkupOut1.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
        F.col("lkup.GRP_SK").alias("GRP_SK")
    )
)

# Stage: xfm_CheckLkpResults (CTransformerStage)
# We create row_number to emulate @INROWNUM logic, then derive stage variables.
w = Window.orderBy(F.lit("A"))
df_numbered = df_LkupGrp.withColumn("_row_num_", F.row_number().over(w))

df_enriched = (
    df_numbered
    .withColumn(
        "SvFKeyLkpCheck",
        F.when(
            (F.col("STATE_CD_SK").isNull()) | (F.trim(F.col("STATE_CD_SK")) == '0'),
            "Y"
        ).otherwise("N")
    )
    .withColumn(
        "SvFKeyLkpCheckGrpSk",
        F.when(
            (F.col("GRP_SK").isNull()) | (F.trim(F.col("GRP_SK")) == '0'),
            "Y"
        ).otherwise("N")
    )
)

# Output link: Lnk_Main (no filter, but with transformations)
df_Lnk_Main = df_enriched.select(
    F.col("GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.when(
        (F.trim(F.when(F.col("GRP_SK").isNotNull(), F.col("GRP_SK")).otherwise(0)) == '0'),
        F.lit(1)
    ).otherwise(F.col("GRP_SK")).alias("GRP_SK"),
    F.col("GRP_NM").alias("GRP_NM"),
    F.col("GRP_CNTCT_FIRST_NM").alias("GRP_CNTCT_FIRST_NM"),
    F.col("GRP_CNTCT_MIDINIT").alias("GRP_CNTCT_MIDINIT"),
    F.col("GRP_CNTCT_LAST_NM").alias("GRP_CNTCT_LAST_NM"),
    F.col("GRP_ADDR_LN_1").alias("GRP_ADDR_LN_1"),
    F.col("GRP_ADDR_LN_2").alias("GRP_ADDR_LN_2"),
    F.col("GRP_ADDR_LN_3").alias("GRP_ADDR_LN_3"),
    F.col("GRP_ADDR_CITY_NM").alias("GRP_ADDR_CITY_NM"),
    F.when(
        F.col("SvFKeyLkpCheck") == 'Y',
        F.lit(1862)
    ).otherwise(F.col("STATE_CD_SK")).alias("GRP_ADDR_ST_CD_SK"),
    F.col("GRP_ADDR_ZIP_CD_5").alias("GRP_ADDR_ZIP_CD_5"),
    F.col("GRP_ADDR_ZIP_CD_4").alias("GRP_ADDR_ZIP_CD_4")
)

# Output link: NA (constraint row_number=1, literal columns)
df_NA = (
    df_enriched.filter(F.col("_row_num_") == 1)
    .select(
        F.lit(1).alias("GRP_MA_DOR_SK"),
        F.lit("NA").alias("GRP_ID"),
        F.lit("NA").alias("TAX_YR"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        current_timestamp().alias("AS_OF_DTM"),
        F.lit(1).alias("GRP_SK"),
        F.lit("NA").alias("GRP_NM"),
        F.lit("NA").alias("GRP_CNTCT_FIRST_NM"),
        F.lit("NA").alias("GRP_CNTCT_MIDINIT"),
        F.lit("NA").alias("GRP_CNTCT_LAST_NM"),
        F.lit("NA").alias("GRP_ADDR_LN_1"),
        F.lit("NA").alias("GRP_ADDR_LN_2"),
        F.lit("NA").alias("GRP_ADDR_LN_3"),
        F.lit("NA").alias("GRP_ADDR_CITY_NM"),
        F.lit(1).alias("GRP_ADDR_ST_CD_SK"),
        F.lit("NA").alias("GRP_ADDR_ZIP_CD_5"),
        F.lit("NA").alias("GRP_ADDR_ZIP_CD_4")
    )
)

# Output link: UNK (constraint row_number=1, literal columns)
df_UNK = (
    df_enriched.filter(F.col("_row_num_") == 1)
    .select(
        F.lit(0).alias("GRP_MA_DOR_SK"),
        F.lit("UNK").alias("GRP_ID"),
        F.lit("UNK").alias("TAX_YR"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        current_timestamp().alias("AS_OF_DTM"),
        F.lit(0).alias("GRP_SK"),
        F.lit("UNK").alias("GRP_NM"),
        F.lit("UNK").alias("GRP_CNTCT_FIRST_NM"),
        F.lit("UNK").alias("GRP_CNTCT_MIDINIT"),
        F.lit("UNK").alias("GRP_CNTCT_LAST_NM"),
        F.lit("UNK").alias("GRP_ADDR_LN_1"),
        F.lit("UNK").alias("GRP_ADDR_LN_2"),
        F.lit("UNK").alias("GRP_ADDR_LN_3"),
        F.lit("UNK").alias("GRP_ADDR_CITY_NM"),
        F.lit(0).alias("GRP_ADDR_ST_CD_SK"),
        F.lit("UNK").alias("GRP_ADDR_ZIP_CD_5"),
        F.lit("UNK").alias("GRP_ADDR_ZIP_CD_4")
    )
)

# Output link: Lnk_K_FKey_Fail (constraint SvFKeyLkpCheck='Y')
df_Lnk_K_FKey_Fail = (
    df_enriched.filter(F.col("SvFKeyLkpCheck") == 'Y')
    .select(
        F.col("GRP_MA_DOR_SK").alias("PRI_SK"),
        F.concat_ws(":", F.col("GRP_ID"), F.col("TAX_YR"), F.col("SRC_SYS_CD")).alias("PRI_NAT_KEY_STRING"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(job_name).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("GROUP").alias("PHYSCL_FILE_NM"),
        F.concat_ws(":", F.col("GRP_ID"), F.col("TAX_YR"), F.col("SRC_SYS_CD"), F.col("GRP_ST_CD")).alias("FRGN_NAT_KEY_STRING"),
        F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Output link: DSLink65 (constraint SvFKeyLkpCheckGrpSk='Y')
df_DSLink65 = (
    df_enriched.filter(F.col("SvFKeyLkpCheckGrpSk") == 'Y')
    .select(
        F.col("GRP_MA_DOR_SK").alias("PRI_SK"),
        F.concat_ws(":", F.col("GRP_ID"), F.col("TAX_YR"), F.col("SRC_SYS_CD")).alias("PRI_NAT_KEY_STRING"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(job_name).alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("GROUP").alias("PHYSCL_FILE_NM"),
        F.concat_ws(":", F.col("GRP_ID"), F.col("TAX_YR"), F.col("SRC_SYS_CD"), F.col("GRP_ST_CD")).alias("FRGN_NAT_KEY_STRING"),
        F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Stage: Funnel_63 - union Lnk_K_FKey_Fail, DSLink65
# Columns: PRI_SK, PRI_NAT_KEY_STRING, SRC_SYS_CD_SK, JOB_NM, ERROR_TYP, PHYSCL_FILE_NM, FRGN_NAT_KEY_STRING, FIRST_RECYC_TS, JOB_EXCTN_SK
df_funnel_63 = df_Lnk_K_FKey_Fail.unionByName(df_DSLink65)

# Stage: seq_FkeyFailedFile (PxSequentialFile) - write funnel_63
# Before writing, apply rpad if we consider columns as varchar/char. The metadata for these columns was not explicitly char(...) in the job,
# but instructions say to rpad if char or varchar. We do not know lengths, so use 255 as a fallback.
df_funnel_63_out = (
    df_funnel_63
    .select(
        F.rpad(F.col("PRI_SK").cast(StringType()), 255, " ").alias("PRI_SK"),
        F.rpad("PRI_NAT_KEY_STRING", 255, " ").alias("PRI_NAT_KEY_STRING"),
        F.rpad(F.col("SRC_SYS_CD_SK").cast(StringType()), 255, " ").alias("SRC_SYS_CD_SK"),
        F.rpad("JOB_NM", 255, " ").alias("JOB_NM"),
        F.rpad("ERROR_TYP", 255, " ").alias("ERROR_TYP"),
        F.rpad("PHYSCL_FILE_NM", 255, " ").alias("PHYSCL_FILE_NM"),
        F.rpad("FRGN_NAT_KEY_STRING", 255, " ").alias("FRGN_NAT_KEY_STRING"),
        "FIRST_RECYC_TS",
        F.col("JOB_EXCTN_SK")
    )
)

file_path_fkey_fail = f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{job_name}.dat"
write_files(
    df_funnel_63_out,
    file_path_fkey_fail,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Stage: Funnel_66 - union Lnk_Main, UNK, NA
df_funnel_66 = df_Lnk_Main.unionByName(df_UNK).unionByName(df_NA)

# Stage: seq_GRP_MA_DOR_Fkey (PxSequentialFile) - write funnel_66
# We must rpad char/varchar columns. Known char columns: TAX_YR(4), GRP_CNTCT_MIDINIT(1), GRP_ADDR_ZIP_CD_5(5), GRP_ADDR_ZIP_CD_4(4).
# Other columns that are varchar get a 255 fallback. Integers/timestamps remain as is.
df_funnel_66_out = (
    df_funnel_66
    .select(
        F.col("GRP_MA_DOR_SK"),
        F.rpad("GRP_ID", 255, " ").alias("GRP_ID"),
        F.rpad("TAX_YR", 4, " ").alias("TAX_YR"),
        F.rpad("SRC_SYS_CD", 255, " ").alias("SRC_SYS_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "AS_OF_DTM",
        F.col("GRP_SK"),
        F.rpad("GRP_NM", 255, " ").alias("GRP_NM"),
        F.rpad("GRP_CNTCT_FIRST_NM", 255, " ").alias("GRP_CNTCT_FIRST_NM"),
        F.rpad("GRP_CNTCT_MIDINIT", 1, " ").alias("GRP_CNTCT_MIDINIT"),
        F.rpad("GRP_CNTCT_LAST_NM", 255, " ").alias("GRP_CNTCT_LAST_NM"),
        F.rpad("GRP_ADDR_LN_1", 255, " ").alias("GRP_ADDR_LN_1"),
        F.rpad("GRP_ADDR_LN_2", 255, " ").alias("GRP_ADDR_LN_2"),
        F.rpad("GRP_ADDR_LN_3", 255, " ").alias("GRP_ADDR_LN_3"),
        F.rpad("GRP_ADDR_CITY_NM", 255, " ").alias("GRP_ADDR_CITY_NM"),
        F.col("GRP_ADDR_ST_CD_SK"),
        F.rpad("GRP_ADDR_ZIP_CD_5", 5, " ").alias("GRP_ADDR_ZIP_CD_5"),
        F.rpad("GRP_ADDR_ZIP_CD_4", 4, " ").alias("GRP_ADDR_ZIP_CD_4")
    )
)

file_path_fkey = f"{adls_path}/load/GRP_MA_DOR.{SrcSysCd}.{RunID}.dat"
write_files(
    df_funnel_66_out,
    file_path_fkey,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)