# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME: IdsMbrBioMesrRsltFkey
# MAGIC 
# MAGIC CALLED BY: IdsMbrBioMesrRsltLoadSeq
# MAGIC 
# MAGIC PROCESSING:   Creates a File "MBR_BIO_MESR_RSLT.dat" after performing FKey Lookups to load the data into table MBR_BIO_MESR_RSLT
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:             
# MAGIC                                                                                                                                                                                    
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                            Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                                                      ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Lakshmi Devagiri            11-12-2020      RA                                   Original Programming                                                                            IntegrateDev2		Harsha Ravuri	2020-11-12

# MAGIC Set all foreign surragote keys
# MAGIC Merge source data with default rows
# MAGIC Writing Sequential File to /load
# MAGIC Read common record format file from extract job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
from pyspark.sql.functions import col, lit, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')

if "landing" in InFile.lower():
    path_MbrBioMesrRsltCf = f"{adls_path_raw}/{InFile}"
elif "external" in InFile.lower():
    path_MbrBioMesrRsltCf = f"{adls_path_publish}/{InFile}"
else:
    path_MbrBioMesrRsltCf = f"{adls_path}/{InFile}"

schema_MbrBioMesrRsltCf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("MBR_BIO_MESR_RSLT_SK", IntegerType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("BIO_MESR_SRC_TYP_CD", StringType(), False),
    StructField("BIO_MESR_SRC_SUBTYP_CD", StringType(), False),
    StructField("BIO_MESR_SRC_DT_SK", StringType(), False),
    StructField("BIO_MESR_TYP_CD", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("MBR_SK", IntegerType(), False),
    StructField("BIO_MESR_SRC_SUBTYP_CD_SK", IntegerType(), False),
    StructField("BIO_MESR_SRC_TYP_CD_SK", IntegerType(), False),
    StructField("BIO_MESR_TYP_CD_SK", IntegerType(), False),
    StructField("MBR_GNDR_CD_SK", IntegerType(), False),
    StructField("BIO_MESR_RSLT_NO", DecimalType(38,10), False),
    StructField("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK", IntegerType(), False),
    StructField("SRC_BIO_MESR_RSLT_NO", DecimalType(38,10), False),
    StructField("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK", IntegerType(), False)
])

df_MbrBioMesrRsltCf = (
    spark.read
    .schema(schema_MbrBioMesrRsltCf)
    .option("header", "false")
    .option("quote", "\"")
    .option("escape", "\"")
    .option("sep", ",")
    .csv(path_MbrBioMesrRsltCf)
)

df_PurgeTrn = df_MbrBioMesrRsltCf.withColumn("PassThru", col("PASS_THRU_IN")).withColumn("ErrCount", GetFkeyErrorCnt(col("MBR_BIO_MESR_RSLT_SK")))

df_ClsFkeyOut = df_PurgeTrn.filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y"))).select(
    col("MBR_BIO_MESR_RSLT_SK").alias("MBR_BIO_MESR_RSLT_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("BIO_MESR_SRC_TYP_CD").alias("BIO_MESR_SRC_TYP_CD"),
    col("BIO_MESR_SRC_SUBTYP_CD").alias("BIO_MESR_SRC_SUBTYP_CD"),
    col("BIO_MESR_SRC_DT_SK").alias("BIO_MESR_SRC_DT_SK"),
    col("BIO_MESR_TYP_CD").alias("BIO_MESR_TYP_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("BIO_MESR_SRC_SUBTYP_CD_SK").alias("BIO_MESR_SRC_SUBTYP_CD_SK"),
    col("BIO_MESR_SRC_TYP_CD_SK").alias("BIO_MESR_SRC_TYP_CD_SK"),
    col("BIO_MESR_TYP_CD_SK").alias("BIO_MESR_TYP_CD_SK"),
    col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    col("BIO_MESR_RSLT_NO").alias("BIO_MESR_RSLT_NO"),
    col("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK").alias("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK"),
    col("SRC_BIO_MESR_RSLT_NO").alias("SRC_BIO_MESR_RSLT_NO"),
    col("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK").alias("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK")
)

df_lnkRecycle = df_PurgeTrn.filter(col("ErrCount") > lit(0)).select(
    GetRecycleKey(col("MBR_BIO_MESR_RSLT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), 255, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), 255, " ").alias("PRI_KEY_STRING"),
    col("MBR_BIO_MESR_RSLT_SK").alias("MBR_BIO_MESR_RSLT_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("BIO_MESR_SRC_TYP_CD"), 255, " ").alias("BIO_MESR_SRC_TYP_CD"),
    rpad(col("BIO_MESR_SRC_SUBTYP_CD"), 255, " ").alias("BIO_MESR_SRC_SUBTYP_CD"),
    rpad(col("BIO_MESR_SRC_DT_SK"), 10, " ").alias("BIO_MESR_SRC_DT_SK"),
    rpad(col("BIO_MESR_TYP_CD"), 255, " ").alias("BIO_MESR_TYP_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("BIO_MESR_SRC_SUBTYP_CD_SK").alias("BIO_MESR_SRC_SUBTYP_CD_SK"),
    col("BIO_MESR_SRC_TYP_CD_SK").alias("BIO_MESR_SRC_TYP_CD_SK"),
    col("BIO_MESR_TYP_CD_SK").alias("BIO_MESR_TYP_CD_SK"),
    col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    col("BIO_MESR_RSLT_NO").alias("BIO_MESR_RSLT_NO"),
    col("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK").alias("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK"),
    col("SRC_BIO_MESR_RSLT_NO").alias("SRC_BIO_MESR_RSLT_NO"),
    col("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK").alias("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK")
)

write_files(
    df_lnkRecycle,
    "hf_recycle.parquet",
    ',',
    'overwrite',
    True,
    True,
    '"',
    None
)

df_DefaultUNKtemp = df_PurgeTrn.withColumn("rownum_unk", row_number().over(Window.orderBy(lit(1))))
df_DefaultUNK = df_DefaultUNKtemp.filter(col("rownum_unk") == lit(1)).select(
    lit(0).alias("MBR_BIO_MESR_RSLT_SK"),
    lit(0).alias("MBR_UNIQ_KEY"),
    rpad(lit("UNK"), 255, " ").alias("BIO_MESR_SRC_TYP_CD"),
    rpad(lit("UNK"), 255, " ").alias("BIO_MESR_SRC_SUBTYP_CD"),
    rpad(lit("1753-01-01"), 10, " ").alias("BIO_MESR_SRC_DT_SK"),
    rpad(lit("UNK"), 255, " ").alias("BIO_MESR_TYP_CD"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("GRP_SK"),
    lit(0).alias("MBR_SK"),
    lit(0).alias("BIO_MESR_SRC_SUBTYP_CD_SK"),
    lit(0).alias("BIO_MESR_SRC_TYP_CD_SK"),
    lit(0).alias("BIO_MESR_TYP_CD_SK"),
    lit(0).alias("MBR_GNDR_CD_SK"),
    lit(0).alias("BIO_MESR_RSLT_NO"),
    lit(0).alias("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK"),
    lit(0).alias("SRC_BIO_MESR_RSLT_NO"),
    lit(0).alias("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK")
)

df_DefaultNATemp = df_PurgeTrn.withColumn("rownum_na", row_number().over(Window.orderBy(lit(1))))
df_DefaultNA = df_DefaultNATemp.filter(col("rownum_na") == lit(1)).select(
    lit(1).alias("MBR_BIO_MESR_RSLT_SK"),
    lit(1).alias("MBR_UNIQ_KEY"),
    rpad(lit("NA"), 255, " ").alias("BIO_MESR_SRC_TYP_CD"),
    rpad(lit("NA"), 255, " ").alias("BIO_MESR_SRC_SUBTYP_CD"),
    rpad(lit("1753-01-01"), 10, " ").alias("BIO_MESR_SRC_DT_SK"),
    rpad(lit("NA"), 255, " ").alias("BIO_MESR_TYP_CD"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("GRP_SK"),
    lit(1).alias("MBR_SK"),
    lit(1).alias("BIO_MESR_SRC_SUBTYP_CD_SK"),
    lit(1).alias("BIO_MESR_SRC_TYP_CD_SK"),
    lit(1).alias("BIO_MESR_TYP_CD_SK"),
    lit(1).alias("MBR_GNDR_CD_SK"),
    lit(1).alias("BIO_MESR_RSLT_NO"),
    lit(1).alias("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK"),
    lit(1).alias("SRC_BIO_MESR_RSLT_NO"),
    lit(1).alias("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK")
)

df_Collector = df_ClsFkeyOut.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

df_Final = df_Collector.select(
    col("MBR_BIO_MESR_RSLT_SK").alias("MBR_BIO_MESR_RSLT_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("BIO_MESR_SRC_TYP_CD"), 255, " ").alias("BIO_MESR_SRC_TYP_CD"),
    rpad(col("BIO_MESR_SRC_SUBTYP_CD"), 255, " ").alias("BIO_MESR_SRC_SUBTYP_CD"),
    rpad(col("BIO_MESR_SRC_DT_SK"), 10, " ").alias("BIO_MESR_SRC_DT_SK"),
    rpad(col("BIO_MESR_TYP_CD"), 255, " ").alias("BIO_MESR_TYP_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("BIO_MESR_SRC_SUBTYP_CD_SK").alias("BIO_MESR_SRC_SUBTYP_CD_SK"),
    col("BIO_MESR_SRC_TYP_CD_SK").alias("BIO_MESR_SRC_TYP_CD_SK"),
    col("BIO_MESR_TYP_CD_SK").alias("BIO_MESR_TYP_CD_SK"),
    col("MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    col("BIO_MESR_RSLT_NO").alias("BIO_MESR_RSLT_NO"),
    col("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK").alias("BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK"),
    col("SRC_BIO_MESR_RSLT_NO").alias("SRC_BIO_MESR_RSLT_NO"),
    col("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK").alias("SRC_BIO_MESR_RSLT_UNIT_OF_MESR_CD_SK")
)

write_files(
    df_Final,
    f"{adls_path}/load/MBR_BIO_MESR_RSLT.dat",
    ',',
    'overwrite',
    False,
    False,
    '"',
    None
)