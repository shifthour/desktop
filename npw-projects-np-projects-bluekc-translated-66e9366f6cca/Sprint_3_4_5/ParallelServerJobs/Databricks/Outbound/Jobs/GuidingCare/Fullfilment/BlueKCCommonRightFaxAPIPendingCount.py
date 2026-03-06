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
# MAGIC Saranya A                                2021-06-29            US370660         Modified Job to process NextPage and RightFax    OutboundDev3          Jaideep Mankala                 06/30/2021
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)jobs in Parallel
# MAGIC 
# MAGIC Jaideep Mankala               01/02/2024            US603879            Updated code to resend faxes when file not found     OutboundDev3\(9)Harsha Ravuri\(9)01/17/2024   
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)on first attempt


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


CommTrnsmsnOwner = get_widget_value('CommTrnsmsnOwner','')
commtrnsmsn_secret_name = get_widget_value('commtrnsmsn_secret_name','')
ParamFilePath = get_widget_value('ParamFilePath','')

schema_Update = StructType([
    StructField("COMM_SK", IntegerType(), False),
    StructField("COMM_SRC_SYS_CD", StringType(), False),
    StructField("COMM_TRGT_SYS_CD", StringType(), False),
    StructField("COMM_STTUS_CD", StringType(), False),
    StructField("COMM_TYP_CD", StringType(), False),
    StructField("COMM_STTUS_CMNT_TX", StringType(), False),
    StructField("COMM_JOB_ID", StringType(), False),
    StructField("COMM_SENT_DTM", TimestampType(), False),
    StructField("LAST_UPDT_DTM", TimestampType(), False)
])

df_Update = (
    spark.read
    .option("header", "false")
    .option("sep", "$$")
    .option("quote", "\"")
    .option("nullValue", None)
    .schema(schema_Update)
    .csv(f"{adls_path_publish}/external/GuidingCareRightFaxInitiateData.txt")
)

extract_query_COMM_TRNSMSN = """
SELECT COMM_SK, FIRSTNAME, LASTNAME, MIDDLENAME, cast(DESTINATION as varchar(20)) as DESTINATION, ATTACHMENTS, LETTERQUEUEID, TEMPLATENAME, SUBSCRIBERID
FROM 
(
SELECT COMM_SK, 
       coalesce((ltrim(rtrim(PROV_FIRST_NM))),'') AS FIRSTNAME, 
       coalesce((ltrim(rtrim(PROV_LAST_NM))),'') AS LASTNAME,
       coalesce((ltrim(rtrim(PROV_MID_NM))),'') AS MIDDLENAME,
       case when PROV_ADDR_FAX_OVRD_NO is null or len(ltrim(rtrim(PROV_ADDR_FAX_OVRD_NO)))=0 then ltrim(rtrim(PROV_ADDR_FAX_NO)) else ltrim(rtrim(PROV_ADDR_FAX_OVRD_NO)) end as DESTINATION, 
       ltrim(rtrim(COMM_FILE_NM)) AS ATTACHMENTS, 
       ltrim(rtrim(COMM_ID)) AS LETTERQUEUEID,
       ltrim(rtrim(COMM_TMPLT_NM)) AS TEMPLATENAME, 
       ltrim(rtrim(MBR_ID)) AS SUBSCRIBERID,
       ROW_NUMBER() OVER(PARTITION BY COMM_JOB_ID ORDER BY CRT_DTM) AS ROWNUM
FROM #$CommTrnsmsnOwner#.[COMM]
WHERE COMM_STTUS_CD='FAXRCVD' and COMM_TYP_CD='ALTRSTAFILE'
)  A 

UNION

SELECT COMM_SK, FIRSTNAME, LASTNAME, MIDDLENAME, cast(DESTINATION as varchar(20)) as DESTINATION, ATTACHMENTS, LETTERQUEUEID, TEMPLATENAME, SUBSCRIBERID
FROM 
(
SELECT COMM_SK, 
       coalesce((ltrim(rtrim(PROV_FIRST_NM))),'') AS FIRSTNAME, 
       coalesce((ltrim(rtrim(PROV_LAST_NM))),'') AS LASTNAME,
       coalesce((ltrim(rtrim(PROV_MID_NM))),'') AS MIDDLENAME,
       case when PROV_ADDR_FAX_OVRD_NO is null or len(ltrim(rtrim(PROV_ADDR_FAX_OVRD_NO)))=0 then ltrim(rtrim(PROV_ADDR_FAX_NO)) else ltrim(rtrim(PROV_ADDR_FAX_OVRD_NO)) end as DESTINATION, 
       ltrim(rtrim(COMM_FILE_NM)) AS ATTACHMENTS, 
       ltrim(rtrim(COMM_ID)) AS LETTERQUEUEID,
       ltrim(rtrim(COMM_TMPLT_NM)) AS TEMPLATENAME, 
       ltrim(rtrim(MBR_ID)) AS SUBSCRIBERID,
       ROW_NUMBER() OVER(PARTITION BY COMM_JOB_ID ORDER BY CRT_DTM) AS ROWNUM
FROM dbo.[COMM]
WHERE COMM_STTUS_CD='FAILED' and COMM_TYP_CD='FAXRQST' and COMM_STTUS_CMNT_TX = 'File Not Found'
AND CRT_DTM > DATEADD(HH,-2, CURRENT_TIMESTAMP)
)  A
""".replace("#$CommTrnsmsnOwner#", CommTrnsmsnOwner)

jdbc_url_COMM_TRNSMSN, jdbc_props_COMM_TRNSMSN = get_db_config(commtrnsmsn_secret_name)

df_COMM_TRNSMSN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_COMM_TRNSMSN)
    .options(**jdbc_props_COMM_TRNSMSN)
    .option("query", extract_query_COMM_TRNSMSN)
    .load()
)

df_xfm_Required_fields = (
    df_COMM_TRNSMSN
    .withColumn("COMM_SK", col("COMM_SK"))
    .withColumn("FIRSTNAME", Ereplace(col("FIRSTNAME"), "$", " "))
    .withColumn("LASTNAME", Ereplace(col("LASTNAME"), "$", " "))
    .withColumn("MIDDLENAME", Ereplace(col("MIDDLENAME"), "$", " "))
    .withColumn("DESTINATION", Ereplace(col("DESTINATION"), "$", " "))
    .withColumn("ATTACHMENTS", Ereplace(col("ATTACHMENTS"), "$", " "))
    .withColumn("LETTERQUEUEID", Ereplace(col("LETTERQUEUEID"), "$", " "))
    .withColumn("TEMPLATENAME", Ereplace(col("TEMPLATENAME"), "$", " "))
    .withColumn("SUBSCRIBERID", Ereplace(col("SUBSCRIBERID"), "$", " "))
    .withColumn("Dummy", F.lit(" "))
)

df_lookUpAll = (
    df_xfm_Required_fields.alias("data")
    .join(df_Update.alias("lkup"), col("data.COMM_SK") == col("lkup.COMM_SK"), "left")
)

df_ln1 = df_lookUpAll.select(
    col("data.COMM_SK").alias("COMM_SK"),
    col("data.FIRSTNAME").alias("FIRSTNAME"),
    col("data.LASTNAME").alias("LASTNAME"),
    col("data.MIDDLENAME").alias("MIDDLENAME"),
    col("data.DESTINATION").alias("DESTINATION"),
    col("data.ATTACHMENTS").alias("ATTACHMENTS"),
    col("data.LETTERQUEUEID").alias("LETTERQUEUEID"),
    col("data.TEMPLATENAME").alias("TEMPLATENAME"),
    col("data.SUBSCRIBERID").alias("SUBSCRIBERID"),
    col("data.Dummy").alias("Dummy")
)

df_ln1.show(10, False)

df_recipientsData = (
    df_lookUpAll
    .withColumn("FIRSTNAME", rpad(col("data.FIRSTNAME"), <...>, " "))
    .withColumn("LASTNAME", rpad(col("data.LASTNAME"), <...>, " "))
    .withColumn("MIDDLENAME", rpad(col("data.MIDDLENAME"), <...>, " "))
    .withColumn("DESTINATION", rpad(col("data.DESTINATION"), <...>, " "))
    .withColumn("ATTACHMENTS", rpad(col("data.ATTACHMENTS"), <...>, " "))
    .withColumn("LETTERQUEUEID", rpad(col("data.LETTERQUEUEID"), <...>, " "))
    .withColumn("TEMPLATENAME", rpad(col("data.TEMPLATENAME"), <...>, " "))
    .withColumn("SUBSCRIBERID", rpad(col("data.SUBSCRIBERID"), <...>, " "))
    .withColumn("Dummy", rpad(col("data.Dummy"), <...>, " "))
    .select(
        col("data.COMM_SK").alias("COMM_SK"),
        col("FIRSTNAME"),
        col("LASTNAME"),
        col("MIDDLENAME"),
        col("DESTINATION"),
        col("ATTACHMENTS"),
        col("LETTERQUEUEID"),
        col("TEMPLATENAME"),
        col("SUBSCRIBERID"),
        col("Dummy")
    )
)

write_files(
    df_recipientsData,
    f"{adls_path_publish}/external/GuidingCareRightFaxRecipientData.txt",
    delimiter="$",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)