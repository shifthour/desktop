# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC 
# MAGIC PROCESSING:   Assigns Foreign Keys for the IDS table VBB_RWRD.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                         Date                 	Project/Altiris #	Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                       --------------------	               -----------------------	-----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Karthik Chintalapani         2013-04-30                      4963 VBB Phase III     Initial Programming                                                                      IntegrateNewDevl       Bhoomi Dasari           5/17/2013

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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","")
Logging = get_widget_value("Logging","")
RunID = get_widget_value("RunID","")

schema_IdsVbbPlnCmpntDpndcExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("VBB_PLN_CMPNT_DPNDC_SK", IntegerType(), False),
    StructField("VBB_PLN_UNIQ_KEY", IntegerType(), False),
    StructField("VBB_CMPNT_UNIQ_KEY", IntegerType(), False),
    StructField("RQRD_VBB_CMPNT_UNIQ_KEY", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("INPR_ID_DEP", IntegerType(), False),
    StructField("INPR_ID", IntegerType(), False),
    StructField("HIPL_ID", IntegerType(), False),
    StructField("MUTLLY_XCLSVE_DPNDC_IN", StringType(), False),
    StructField("SRC_SYS_CRT_DTM", TimestampType(), False),
    StructField("SRC_SYS_UPDT_DTM", TimestampType(), False),
    StructField("WAIT_PERD_DAYS_NO", IntegerType(), False)
])

df_key = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsVbbPlnCmpntDpndcExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_foreignkey = (
    df_key
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("VBB_PLN_CMPNT_DPNDC_SK")))
    .withColumn("SvSrcSysCrtDt", FORMAT.DATE(F.col("SRC_SYS_CRT_DTM"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")))
    .withColumn("SvSrcSysUpdDt", FORMAT.DATE(F.col("SRC_SYS_UPDT_DTM"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")))
    .withColumn("SvReqdVbbCmpntSk", GetFkeyVbbCmpnt(F.col("SRC_SYS_CD"), F.col("VBB_PLN_CMPNT_DPNDC_SK"), F.col("INPR_ID_DEP"), Logging))
    .withColumn("SvVbbCmpntSk", GetFkeyVbbCmpnt(F.col("SRC_SYS_CD"), F.col("VBB_PLN_CMPNT_DPNDC_SK"), F.col("INPR_ID"), Logging))
    .withColumn("SvVbbPlnSk", GetFkeyVbbPln(F.col("SRC_SYS_CD"), F.col("VBB_PLN_CMPNT_DPNDC_SK"), F.col("HIPL_ID"), Logging))
)

df_fkey = (
    df_foreignkey
    .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == "Y"))
    .select(
        F.col("VBB_PLN_CMPNT_DPNDC_SK").alias("VBB_PLN_CMPNT_DPNDC_SK"),
        F.col("VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
        F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
        F.col("RQRD_VBB_CMPNT_UNIQ_KEY").alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("SvReqdVbbCmpntSk").alias("RQRD_VBB_CMPNT_SK"),
        F.col("SvVbbCmpntSk").alias("VBB_CMPNT_SK"),
        F.col("SvVbbPlnSk").alias("VBB_PLN_SK"),
        F.col("MUTLLY_XCLSVE_DPNDC_IN").alias("MUTLLY_XCLSVE_DPNDC_IN"),
        F.col("SvSrcSysCrtDt").alias("SRC_SYS_CRT_DTM"),
        F.col("SvSrcSysUpdDt").alias("SRC_SYS_UPDT_DTM"),
        F.col("WAIT_PERD_DAYS_NO").alias("WAIT_PERD_DAYS_NO")
    )
)

df_recycle = (
    df_foreignkey
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("VBB_PLN_CMPNT_DPNDC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("VBB_PLN_CMPNT_DPNDC_SK").alias("VBB_PLN_CMPNT_DPNDC_SK"),
        F.col("VBB_PLN_UNIQ_KEY").alias("VBB_PLN_UNIQ_KEY"),
        F.col("VBB_CMPNT_UNIQ_KEY").alias("VBB_CMPNT_UNIQ_KEY"),
        F.col("RQRD_VBB_CMPNT_UNIQ_KEY").alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("INPR_ID_DEP").alias("RQRD_VBB_CMPNT_SK"),
        F.col("INPR_ID").alias("VBB_CMPNT_SK"),
        F.col("HIPL_ID").alias("VBB_PLN_SK"),
        F.col("MUTLLY_XCLSVE_DPNDC_IN").alias("MUTLLY_XCLSVE_DPNDC_IN"),
        F.col("SRC_SYS_CRT_DTM").alias("SRC_SYS_CRT_DTM"),
        F.col("SRC_SYS_UPDT_DTM").alias("SRC_SYS_UPDT_DTM"),
        F.col("WAIT_PERD_DAYS_NO").alias("WAIT_PERD_DAYS_NO")
    )
)

df_recycle_with_rpad = (
    df_recycle
    .withColumn("INSRT_UPDT_CD", rpad("INSRT_UPDT_CD", 10, " "))
    .withColumn("DISCARD_IN", rpad("DISCARD_IN", 1, " "))
    .withColumn("PASS_THRU_IN", rpad("PASS_THRU_IN", 1, " "))
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", F.lit("<...>"), F.lit(" ")))
    .withColumn("PRI_KEY_STRING", rpad("PRI_KEY_STRING", F.lit("<...>"), F.lit(" ")))
    .withColumn("MUTLLY_XCLSVE_DPNDC_IN", rpad("MUTLLY_XCLSVE_DPNDC_IN", 1, " "))
)

df_recycle_final = df_recycle_with_rpad.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "VBB_PLN_CMPNT_DPNDC_SK",
    "VBB_PLN_UNIQ_KEY",
    "VBB_CMPNT_UNIQ_KEY",
    "RQRD_VBB_CMPNT_UNIQ_KEY",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "RQRD_VBB_CMPNT_SK",
    "VBB_CMPNT_SK",
    "VBB_PLN_SK",
    "MUTLLY_XCLSVE_DPNDC_IN",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "WAIT_PERD_DAYS_NO"
)

write_files(
    df_recycle_final,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

w = Window.orderBy(F.lit(1))

df_foreignkey_rn = df_foreignkey.withColumn("RN", F.row_number().over(w))

df_defaultunk = df_foreignkey_rn.filter(F.col("RN") == 1).select(
    F.lit(0).alias("VBB_PLN_CMPNT_DPNDC_SK"),
    F.lit(0).alias("VBB_PLN_UNIQ_KEY"),
    F.lit(0).alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(0).alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("RQRD_VBB_CMPNT_SK"),
    F.lit(0).alias("VBB_CMPNT_SK"),
    F.lit(0).alias("VBB_PLN_SK"),
    F.lit("N").alias("MUTLLY_XCLSVE_DPNDC_IN"),
    F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_CRT_DTM"),
    F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_UPDT_DTM"),
    F.lit(0).alias("WAIT_PERD_DAYS_NO")
)

df_defaultna = df_foreignkey_rn.filter(F.col("RN") == 1).select(
    F.lit(1).alias("VBB_PLN_CMPNT_DPNDC_SK"),
    F.lit(1).alias("VBB_PLN_UNIQ_KEY"),
    F.lit(1).alias("VBB_CMPNT_UNIQ_KEY"),
    F.lit(1).alias("RQRD_VBB_CMPNT_UNIQ_KEY"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("RQRD_VBB_CMPNT_SK"),
    F.lit(1).alias("VBB_CMPNT_SK"),
    F.lit(1).alias("VBB_PLN_SK"),
    F.lit("N").alias("MUTLLY_XCLSVE_DPNDC_IN"),
    F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_CRT_DTM"),
    F.lit("1753-01-01-00.00.00.000000").alias("SRC_SYS_UPDT_DTM"),
    F.lit(1).alias("WAIT_PERD_DAYS_NO")
)

df_collect = df_fkey.unionByName(df_defaultunk).unionByName(df_defaultna)

df_collect2 = df_collect.withColumn(
    "MUTLLY_XCLSVE_DPNDC_IN",
    rpad("MUTLLY_XCLSVE_DPNDC_IN", 1, " ")
)

df_final = df_collect2.select(
    "VBB_PLN_CMPNT_DPNDC_SK",
    "VBB_PLN_UNIQ_KEY",
    "VBB_CMPNT_UNIQ_KEY",
    "RQRD_VBB_CMPNT_UNIQ_KEY",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "RQRD_VBB_CMPNT_SK",
    "VBB_CMPNT_SK",
    "VBB_PLN_SK",
    "MUTLLY_XCLSVE_DPNDC_IN",
    "SRC_SYS_CRT_DTM",
    "SRC_SYS_UPDT_DTM",
    "WAIT_PERD_DAYS_NO"
)

write_files(
    df_final,
    f"{adls_path}/load/VBB_PLN_CMPNT_DPNDC.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)