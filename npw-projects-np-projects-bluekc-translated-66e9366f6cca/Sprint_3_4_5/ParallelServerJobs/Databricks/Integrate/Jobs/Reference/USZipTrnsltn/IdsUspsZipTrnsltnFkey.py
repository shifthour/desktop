# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2005, 2007, 2008, 2009, 2010 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:IdsReferenceSeq
# MAGIC                     
# MAGIC 
# MAGIC Processing:
# MAGIC                     
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                                               Project/                                                                                                                                                                 Code                   Date
# MAGIC Developer                                 Date                      Altiris #                                              Change Description                                                                                     Reviewer            Reviewed
# MAGIC --------------------------               -------------------                  -------------                            -----------------------------------------------------------------------------------------------------------------                    ------------------------  -------------------
# MAGIC 
# MAGIC Karthik Chintalapani            04-27-2012                4784 SC Claims                                        Original programming                                                                              Bhoomi Dasari    07/19/2012

# MAGIC Foreign key process for USPS_ZIP_TRNSLTN
# MAGIC File from FctsUspsZipTrnsltnExtr
# MAGIC Write sequential file to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','FctsUspsZipTrnsltnExtr.UspsZip.dat')
Logging = get_widget_value('Logging','Y')
CurrRunCycle = get_widget_value('CurrRunCycle','')

schema_UspsZip = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(10,0), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("USPS_ZIP_TRNSLTN_SK", IntegerType(), nullable=False),
    StructField("ZIP_CD_5", StringType(), nullable=False),
    StructField("ZIP_LN_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ST_CD", StringType(), nullable=False),
    StructField("USPS_FCLTY_CD", StringType(), nullable=False),
    StructField("ZIP_RCRD_TYP_CD", StringType(), nullable=False),
    StructField("CITY_NM", StringType(), nullable=False),
    StructField("CITY_SH_NM", StringType(), nullable=False),
    StructField("CNTY_FIPS_NO", StringType(), nullable=False),
    StructField("CNTY_NM", StringType(), nullable=False),
    StructField("PRFRD_POSTAL_NM", StringType(), nullable=False),
    StructField("PRFRD_ZIP_LN_ID", StringType(), nullable=False),
    StructField("ZIP_FNC_ID", StringType(), nullable=False)
])

df_UspsZip = (
    spark.read
    .options(header=False, sep=",", quote="\"")
    .schema(schema_UspsZip)
    .csv(f"{adls_path}/key/{InFile}")
)

df_ForeignKeyTransformed = (
    df_UspsZip
    .withColumn("svPassThru", F.col("PASS_THRU_IN"))
    .withColumn("SrcSysCdSk", GetFkeyCodes(F.lit("IDS"), F.col("USPS_ZIP_TRNSLTN_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), Logging))
    .withColumn("svStCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("USPS_ZIP_TRNSLTN_SK"), F.lit("STATE"), F.col("ST_CD"), Logging))
    .withColumn("svFcltyCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("USPS_ZIP_TRNSLTN_SK"), F.lit("USPS FACILITY"), F.col("USPS_FCLTY_CD"), Logging))
    .withColumn("svZpRdTpCdSk", GetFkeyCodes(F.col("SRC_SYS_CD"), F.col("USPS_ZIP_TRNSLTN_SK"), F.lit("USPS RECORD TYPE"), F.col("ZIP_RCRD_TYP_CD"), Logging))
    .withColumn("svErrCount", GetFkeyErrorCnt(F.col("USPS_ZIP_TRNSLTN_SK")))
)

df_ForeignKey_Fkey = (
    df_ForeignKeyTransformed
    .filter((F.col("svErrCount") == 0) | (F.col("svPassThru") == 'Y'))
    .select(
        F.col("USPS_ZIP_TRNSLTN_SK").alias("USPS_ZIP_TRNSLTN_SK"),
        F.col("ZIP_CD_5").alias("ZIP_CD_5"),
        F.col("ZIP_LN_ID").alias("ZIP_LN_ID"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svStCdSk").alias("ST_CD_SK"),
        F.col("svFcltyCdSk").alias("USPS_FCLTY_CD_SK"),
        F.col("svZpRdTpCdSk").alias("ZIP_RCRD_TYP_CD_SK"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.col("CITY_SH_NM").alias("CITY_SH_NM"),
        F.col("CNTY_FIPS_NO").alias("CNTY_FIPS_NO"),
        F.col("CNTY_NM").alias("CNTY_NM"),
        F.col("PRFRD_POSTAL_NM").alias("PRFRD_POSTAL_NM"),
        F.col("PRFRD_ZIP_LN_ID").alias("PRFRD_ZIP_LN_ID"),
        F.col("ZIP_FNC_ID").alias("ZIP_FNC_ID")
    )
)

df_ForeignKey_DefaultUNK = (
    df_ForeignKeyTransformed
    .limit(1)
    .select(
        F.lit(0).alias("USPS_ZIP_TRNSLTN_SK"),
        F.lit("UNK").alias("ZIP_CD_5"),
        F.lit("UNK").alias("ZIP_LN_ID"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("ST_CD_SK"),
        F.lit(0).alias("USPS_FCLTY_CD_SK"),
        F.lit(0).alias("ZIP_RCRD_TYP_CD_SK"),
        F.lit("UNK").alias("CITY_NM"),
        F.lit("UNK").alias("CITY_SH_NM"),
        F.lit("UNK").alias("CNTY_FIPS_NO"),
        F.lit("UNK").alias("CNTY_NM"),
        F.lit("UNK").alias("PRFRD_POSTAL_NM"),
        F.lit("UNK").alias("PRFRD_ZIP_LN_ID"),
        F.lit("UNK").alias("ZIP_FNC_ID")
    )
)

df_ForeignKey_DefaultNA = (
    df_ForeignKeyTransformed
    .limit(1)
    .select(
        F.lit(1).alias("USPS_ZIP_TRNSLTN_SK"),
        F.lit("NA").alias("ZIP_CD_5"),
        F.lit("NA").alias("ZIP_LN_ID"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("ST_CD_SK"),
        F.lit(1).alias("USPS_FCLTY_CD_SK"),
        F.lit(1).alias("ZIP_RCRD_TYP_CD_SK"),
        F.lit("NA").alias("CITY_NM"),
        F.lit("NA").alias("CITY_SH_NM"),
        F.lit("NA").alias("CNTY_FIPS_NO"),
        F.lit("NA").alias("CNTY_NM"),
        F.lit("NA").alias("PRFRD_POSTAL_NM"),
        F.lit("NA").alias("PRFRD_ZIP_LN_ID"),
        F.lit("NA").alias("ZIP_FNC_ID")
    )
)

df_ForeignKey_recycle = (
    df_ForeignKeyTransformed
    .filter(F.col("svErrCount") > 0)
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("svErrCount").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("USPS_ZIP_TRNSLTN_SK").alias("USPS_ZIP_TRNSLTN_SK"),
        F.col("ZIP_CD_5").alias("ZIP_CD_5"),
        F.col("ZIP_LN_ID").alias("ZIP_LN_ID"),
        F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ST_CD").alias("ST_CD"),
        F.col("USPS_FCLTY_CD").alias("USPS_FCLTY_CD"),
        F.col("ZIP_RCRD_TYP_CD").alias("ZIP_RCRD_TYP_CD"),
        F.col("CITY_NM").alias("CITY_NM"),
        F.col("CITY_SH_NM").alias("CITY_SH_NM"),
        F.col("CNTY_FIPS_NO").alias("CNTY_FIPS_NO"),
        F.col("CNTY_NM").alias("CNTY_NM"),
        F.col("PRFRD_POSTAL_NM").alias("PRFRD_POSTAL_NM"),
        F.col("PRFRD_ZIP_LN_ID").alias("PRFRD_ZIP_LN_ID"),
        F.col("ZIP_FNC_ID").alias("ZIP_FNC_ID")
    )
)

df_ForeignKey_recycle_rpad = (
    df_ForeignKey_recycle
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("ZIP_CD_5", F.rpad(F.col("ZIP_CD_5"), 5, " "))
)

df_ForeignKey_recycle_final = df_ForeignKey_recycle_rpad.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "USPS_ZIP_TRNSLTN_SK",
    "ZIP_CD_5",
    "ZIP_LN_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ST_CD",
    "USPS_FCLTY_CD",
    "ZIP_RCRD_TYP_CD",
    "CITY_NM",
    "CITY_SH_NM",
    "CNTY_FIPS_NO",
    "CNTY_NM",
    "PRFRD_POSTAL_NM",
    "PRFRD_ZIP_LN_ID",
    "ZIP_FNC_ID"
)

write_files(
    df_ForeignKey_recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

col_order_collector = [
    "USPS_ZIP_TRNSLTN_SK",
    "ZIP_CD_5",
    "ZIP_LN_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ST_CD_SK",
    "USPS_FCLTY_CD_SK",
    "ZIP_RCRD_TYP_CD_SK",
    "CITY_NM",
    "CITY_SH_NM",
    "CNTY_FIPS_NO",
    "CNTY_NM",
    "PRFRD_POSTAL_NM",
    "PRFRD_ZIP_LN_ID",
    "ZIP_FNC_ID"
]

df_Collector_Fkey = df_ForeignKey_Fkey.select(col_order_collector)
df_Collector_DefaultUNK = df_ForeignKey_DefaultUNK.select(col_order_collector)
df_Collector_DefaultNA = df_ForeignKey_DefaultNA.select(col_order_collector)

df_Collector = df_Collector_Fkey.unionByName(df_Collector_DefaultUNK).unionByName(df_Collector_DefaultNA)

df_Collector_rpad = df_Collector.withColumn("ZIP_CD_5", F.rpad(F.col("ZIP_CD_5"), 5, " "))

df_Collector_final = df_Collector_rpad.select(col_order_collector)

write_files(
    df_Collector_final,
    f"{adls_path}/load/USPS_ZIP_TRNSLTN.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)