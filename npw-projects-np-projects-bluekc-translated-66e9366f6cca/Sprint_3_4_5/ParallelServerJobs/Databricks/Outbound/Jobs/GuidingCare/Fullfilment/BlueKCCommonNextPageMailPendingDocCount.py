# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2017 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : BlueKCCommonPerceptiveAPIDocStorageSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING : Job gets count for pending Letters for perceptive storage.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                               Date                         Project                                   Change Description            Development Project          Code Reviewer                   Date Reviewed       
# MAGIC ------------------------------              ------------------        ----------------------             ---------------------------------------       ------------------------------\(9)----------------------------------         -------------------------------        
# MAGIC Bharani Chalamalasetty      04/01/2020             Fullfillment                       Original Programming               OutboundDev3                    Jaideep Mankala              04/20/2020
# MAGIC 
# MAGIC Saranya A                                2021-06-29            US370660         Modified Job to process NextPage and RightFax    OutboundDev3          Jaideep Mankala                 06/30/2021
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)jobs in Parallel
# MAGIC Saranya A                                2021-10-14           US396204          Modified Logic to generate Link column    OutboundDev3    Jaideep Mankala         10/15/2021    
# MAGIC \(9)\(9)\(9)\(9)\(9)


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import rpad, col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


CommTrnsmsnOwner = get_widget_value('CommTrnsmsnOwner','')
commtrnsmsn_secret_name = get_widget_value('commtrnsmsn_secret_name','')
ParamFilePath = get_widget_value('ParamFilePath','')
ProvType = get_widget_value('ProvType','')
Vendor = get_widget_value('Vendor','')
RunID = get_widget_value('RunID','')

percep_schema = StructType([
    StructField("COMM_SK", IntegerType(), False)
])

df_Percep = (
    spark.read
    .option("delimiter", "$$")
    .option("header", False)
    .option("quote", None)
    .option("nullValue", None)
    .schema(percep_schema)
    .csv(f"{adls_path_publish}/external/GuidingCare{Vendor}MailStatusData.txt")
)

percepRecData_schema = StructType([
    StructField("COMM_SK", IntegerType(), False),
    StructField("FIRSTNAME", StringType(), False),
    StructField("LASTNAME", StringType(), False),
    StructField("MIDDLENAME", StringType(), False),
    StructField("DESTINATION", StringType(), False),
    StructField("ATTACHMENTS", StringType(), False),
    StructField("LETTERQUEUEID", StringType(), False),
    StructField("TEMPLATENAME", StringType(), False),
    StructField("SUBSCRIBERID", StringType(), False),
    StructField("INDICATOR", StringType(), False),
    StructField("CMPLNT_ID", StringType(), True),
    StructField("UM_REF_ID", StringType(), True),
    StructField("MBR_GRP_ID", StringType(), True),
    StructField("MBR_BRTH_DT", StringType(), True),
    StructField("IMG_ID", StringType(), True),
    StructField("DRAWER_ID", StringType(), True),
    StructField("DOC_TYP_ID", StringType(), True),
    StructField("MBR_SFX", StringType(), True),
    StructField("Dummy", StringType(), True)
])

df_PercepRecData = (
    spark.read
    .option("delimiter", "$")
    .option("header", False)
    .option("quote", None)
    .option("nullValue", None)
    .schema(percepRecData_schema)
    .csv(f"{adls_path_publish}/external/GuidingCareNextPagePercepMailInitialData_{RunID}.txt")
)

df_Transformer_32_data = df_PercepRecData.filter(trim(col("INDICATOR")) == 'N').select(
    col("COMM_SK").alias("COMM_SK"),
    col("FIRSTNAME").alias("FIRSTNAME"),
    col("LASTNAME").alias("LASTNAME"),
    col("MIDDLENAME").alias("MIDDLENAME"),
    col("DESTINATION").alias("DESTINATION"),
    col("ATTACHMENTS").alias("ATTACHMENTS"),
    col("LETTERQUEUEID").alias("LETTERQUEUEID"),
    col("TEMPLATENAME").alias("TEMPLATENAME"),
    col("SUBSCRIBERID").alias("SUBSCRIBERID"),
    col("Dummy").alias("Dummy")
)

df_LookUp = df_Transformer_32_data.alias("data").join(
    df_Percep.alias("lkup"),
    on=[col("data.COMM_SK") == col("lkup.COMM_SK")],
    how="left"
)

df_DSLink22 = df_LookUp.select(
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

df_lnk_xfm = df_LookUp.select(
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

df_lnk_xfm_rpad = (
    df_lnk_xfm
    .withColumn("FIRSTNAME", rpad(col("FIRSTNAME"), <...>, " "))
    .withColumn("LASTNAME", rpad(col("LASTNAME"), <...>, " "))
    .withColumn("MIDDLENAME", rpad(col("MIDDLENAME"), <...>, " "))
    .withColumn("DESTINATION", rpad(col("DESTINATION"), <...>, " "))
    .withColumn("ATTACHMENTS", rpad(col("ATTACHMENTS"), <...>, " "))
    .withColumn("LETTERQUEUEID", rpad(col("LETTERQUEUEID"), <...>, " "))
    .withColumn("TEMPLATENAME", rpad(col("TEMPLATENAME"), <...>, " "))
    .withColumn("SUBSCRIBERID", rpad(col("SUBSCRIBERID"), <...>, " "))
    .withColumn("Dummy", rpad(col("Dummy"), <...>, " "))
)

df_lnk_xfm_final = df_lnk_xfm_rpad.select(
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

write_files(
    df_lnk_xfm_final,
    f"{adls_path_publish}/external/GuidingCare{Vendor}MailRecipientData.txt",
    delimiter="$",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

df_DSLink22.show(10)