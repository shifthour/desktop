# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2017 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : BlueKCCommonRightFaxAPIPostSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING : Job gets count for pending Letters to Fax.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                               Date                         Project                                   Change Description            Development Project          Code Reviewer                   Date Reviewed       
# MAGIC ------------------------------              ------------------        ----------------------             ---------------------------------------       ------------------------------\(9)----------------------------------         -------------------------------        
# MAGIC Bharani Chalamalasetty      04/01/2020             Fullfillment                       Original Programming               OutboundDev3                    Jaideep Mankala              04/20/2020
# MAGIC 
# MAGIC Bharani Chalamalasetty      07/20/2020            US243985         Updated Code to Get Count of Waiting     OutboundDev3                   Jaideep Mankala                 07/22/2020
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)or Processing Fax Status
# MAGIC \(9)\(9)\(9)\(9)\(9)         
# MAGIC Saranya A                                2021-06-29            US370660         Modified Job to process NextPage and RightFax    OutboundDev3          Jaideep Mankala                 06/30/2021
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)jobs in Parallel


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


# Retrieve parameter values
CommTrnsmsnOwner = get_widget_value('CommTrnsmsnOwner','')
commtrnsmsn_secret_name = get_widget_value('commtrnsmsn_secret_name','')

# Define schema and read the Status file
status_schema = StructType([
    StructField("COMM_SK", IntegerType(), False),
    StructField("COMM_SRC_SYS_CD", StringType(), False),
    StructField("COMM_TRGT_SYS_CD", StringType(), False),
    StructField("COMM_STTUS_CD", StringType(), False),
    StructField("COMM_TYP_CD", StringType(), False),
    StructField("COMM_STTUS_CMNT_TX", StringType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False),
    StructField("COMM_SENT_DTM", TimestampType(), False)
])

df_Status = (
    spark.read.format("csv")
    .schema(status_schema)
    .option("sep", "$$")
    .option("quote", "\"")
    .option("header", "false")
    .option("nullValue", None)
    .load(f"{adls_path_publish}/external/GuidingCareRightFaxStatusData.txt")
)

# Read from database (COMM_TRNSMSN stage)
jdbc_url, jdbc_props = get_db_config(commtrnsmsn_secret_name)
extract_query = f"""
SELECT COMM_SK, SUBSCRIBERID
FROM (
    SELECT COMM_SK,
           ltrim(rtrim(COMM_JOB_ID)) AS SUBSCRIBERID
    FROM {CommTrnsmsnOwner}.[COMM]
    WHERE (
         ( COMM_STTUS_CD in ('WAITING','PROCESSING') and COMM_TYP_CD='FAXSTS')
         OR
         ( COMM_TRGT_SYS_CD = 'RGHTFAX' and COMM_TYP_CD = 'FAXRQST'  and COMM_STTUS_CD = 'SUCCESS' )
    )
) A
"""

df_COMM_TRNSMSN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Transformer xfm_Required_fields
df_xfm_Required_fields = df_COMM_TRNSMSN.select(
    F.col("COMM_SK"),
    F.lit("NA").alias("FIRSTNAME"),
    F.lit("NA").alias("LASTNAME"),
    F.lit("NA").alias("MIDDLENAME"),
    F.lit("NA").alias("DESTINATION"),
    F.lit("NA").alias("ATTACHMENTS"),
    F.lit("NA").alias("LETTERQUEUEID"),
    F.lit("NA").alias("TEMPLATENAME"),
    EReplace(F.col("SUBSCRIBERID"), "$", "").alias("SUBSCRIBERID"),
    F.lit(" ").alias("Dummy")
)

# LookUp stage (left join on COMM_SK) with two output links
df_lookup = df_xfm_Required_fields.alias("data").join(
    df_Status.alias("lkup"),
    F.col("data.COMM_SK") == F.col("lkup.COMM_SK"),
    how="left"
)

df_lnk_xfm1 = df_lookup.select(
    F.col("data.COMM_SK").alias("COMM_SK"),
    F.col("data.FIRSTNAME").alias("FIRSTNAME"),
    F.col("data.LASTNAME").alias("LASTNAME"),
    F.col("data.MIDDLENAME").alias("MIDDLENAME"),
    F.col("data.DESTINATION").alias("DESTINATION"),
    F.col("data.ATTACHMENTS").alias("ATTACHMENTS"),
    F.col("data.LETTERQUEUEID").alias("LETTERQUEUEID"),
    F.col("data.TEMPLATENAME").alias("TEMPLATENAME"),
    F.col("data.SUBSCRIBERID").alias("SUBSCRIBERID"),
    F.col("data.Dummy").alias("Dummy")
)

df_lnk_xfm = df_lookup.select(
    F.col("data.COMM_SK").alias("COMM_SK"),
    F.col("data.FIRSTNAME").alias("FIRSTNAME"),
    F.col("data.LASTNAME").alias("LASTNAME"),
    F.col("data.MIDDLENAME").alias("MIDDLENAME"),
    F.col("data.DESTINATION").alias("DESTINATION"),
    F.col("data.ATTACHMENTS").alias("ATTACHMENTS"),
    F.col("data.LETTERQUEUEID").alias("LETTERQUEUEID"),
    F.col("data.TEMPLATENAME").alias("TEMPLATENAME"),
    F.col("data.SUBSCRIBERID").alias("SUBSCRIBERID"),
    F.col("data.Dummy").alias("Dummy")
)

# Peek_19 stage (show 10 rows)
df_lnk_xfm1.limit(10).show()

# PendingData stage
df_final = df_lnk_xfm.select(
    "COMM_SK",
    "FIRSTNAME",
    "LASTNAME",
    "MIDDLENAME",
    "DESTINATION",
    "ATTACHMENTS",
    "LETTERQUEUEID",
    "TEMPLATENAME",
    "SUBSCRIBERID",
    "Dummy"
)

# For varchar/char columns, use rpad(...) with <...>; type lengths unknown -> manual remediation.
df_final = df_final.withColumn("FIRSTNAME", F.rpad(F.col("FIRSTNAME"), <...>, " "))
df_final = df_final.withColumn("LASTNAME", F.rpad(F.col("LASTNAME"), <...>, " "))
df_final = df_final.withColumn("MIDDLENAME", F.rpad(F.col("MIDDLENAME"), <...>, " "))
df_final = df_final.withColumn("DESTINATION", F.rpad(F.col("DESTINATION"), <...>, " "))
df_final = df_final.withColumn("ATTACHMENTS", F.rpad(F.col("ATTACHMENTS"), <...>, " "))
df_final = df_final.withColumn("LETTERQUEUEID", F.rpad(F.col("LETTERQUEUEID"), <...>, " "))
df_final = df_final.withColumn("TEMPLATENAME", F.rpad(F.col("TEMPLATENAME"), <...>, " "))
df_final = df_final.withColumn("SUBSCRIBERID", F.rpad(F.col("SUBSCRIBERID"), <...>, " "))
df_final = df_final.withColumn("Dummy", F.rpad(F.col("Dummy"), <...>, " "))

write_files(
    df_final,
    f"{adls_path_publish}/external/GuidingCareRightFaxPendingData.txt",
    delimiter="$",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)