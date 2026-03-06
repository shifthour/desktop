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
# MAGIC Saranya A                                2021-10-14           US396204          Added new job parameters to pass new API link and key    OutboundDev3    Jaideep Mankala         10/15/2021   
# MAGIC 
# MAGIC 
# MAGIC Jaideep Mankala               01/02/2024            US603879            Updated code to resend faxes when file not found     OutboundDev3\(9)Harsha Ravuri\(9)01/17/2024   
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)on first attempt
# MAGIC 
# MAGIC \(9)\(9)\(9)\(9)\(9)


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, when, length, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


CommTrnsmsnOwner = get_widget_value('CommTrnsmsnOwner','')
commtrnsmsn_secret_name = get_widget_value('commtrnsmsn_secret_name','')
ParamFilePath = get_widget_value('ParamFilePath','')
ProvType = get_widget_value('ProvType','')
Vendor = get_widget_value('Vendor','')

schema_Percep = StructType([
    StructField("COMM_SK", IntegerType(), nullable=False),
    StructField("COMM_SRC_SYS_CD", StringType(), nullable=False),
    StructField("COMM_TRGT_SYS_CD", StringType(), nullable=False),
    StructField("COMM_STTUS_CD", StringType(), nullable=False),
    StructField("COMM_TYP_CD", StringType(), nullable=False),
    StructField("COMM_STTUS_CMNT_TX", StringType(), nullable=False),
    StructField("LAST_UPDT_DTM", TimestampType(), nullable=False),
    StructField("COMM_SENT_DTM", TimestampType(), nullable=False)
])

df_Percep = (
    spark.read
    .option("sep", "$$")
    .option("quote", "\"")
    .option("header", False)
    .option("nullValue", None)
    .schema(schema_Percep)
    .csv(f"{adls_path_publish}/external/GuidingCare{Vendor}PercepStatusData.txt")
)

jdbc_url, jdbc_props = get_db_config(commtrnsmsn_secret_name)

extract_query = f"""
SELECT COMM_SK, FIRSTNAME, LASTNAME, MIDDLENAME, cast(DESTINATION as varchar(20)) as DESTINATION, ATTACHMENTS,
       cast(LETTERQUEUEID as varchar(40)) as LETTERQUEUEID, TEMPLATENAME, SUBSCRIBERID , 
       CMPLNT_ID , UM_REF_ID , MBR_GRP_ID, MBR_BRTH_DT, IMG_ID , DRAWER_ID, DOC_TYP_ID
FROM 
(
  SELECT C.COMM_SK as COMM_SK, 
         ltrim(rtrim(C.MBR_FIRST_NM)) as FIRSTNAME,
         ltrim(rtrim(C.MBR_LAST_NM)) as LASTNAME,
         case when len(ltrim(rtrim(C.MBR_MID_NM)))=0 then '' else ltrim(rtrim(C.MBR_MID_NM)) end as MIDDLENAME,
         '' as DESTINATION,
         ltrim(rtrim(C.COMM_FILE_NM)) AS ATTACHMENTS, 
         ltrim(rtrim(C.IMG_ID)) AS LETTERQUEUEID,
         ltrim(rtrim(C.COMM_TMPLT_NM)) AS TEMPLATENAME, 
         ltrim(rtrim(C.MBR_ID)) as SUBSCRIBERID,
         ltrim(rtrim(C.CMPLNT_ID)) as CMPLNT_ID,
         ltrim(rtrim(C.UM_REF_ID)) as UM_REF_ID,
         ltrim(rtrim(C.MBR_GRP_ID)) as MBR_GRP_ID,
         ltrim(rtrim(C.MBR_BRTH_DT)) as MBR_BRTH_DT,
         ltrim(rtrim(C.IMG_ID)) as IMG_ID,
         ltrim(rtrim(T.DRAWER_ID)) as DRAWER_ID,
         ltrim(rtrim(T.DOC_TYP_ID)) as DOC_TYP_ID
  FROM {CommTrnsmsnOwner}.[COMM] C
  Inner Join {CommTrnsmsnOwner}.[COMM_TMPLT_RECPNT_TRNSMSN_TYP] T
    on C.COMM_TMPLT_NM = T.COMM_TMPLT_RECPNT_TRNSMSN_TYP_NM
  WHERE C.COMM_STTUS_CD='DOCRCVD' and C.COMM_TYP_CD in ('ALTRSTAFILE','AGPAHUBFILE')
    and reverse(substring(reverse(COMM_TMPLT_NM),1,2)) {ProvType} '_F'
)  A

UNION

SELECT COMM_SK, FIRSTNAME, LASTNAME, MIDDLENAME, cast(DESTINATION as varchar(20)) as DESTINATION, ATTACHMENTS,
       cast(LETTERQUEUEID as varchar(40)) as LETTERQUEUEID, TEMPLATENAME, SUBSCRIBERID , 
       CMPLNT_ID , UM_REF_ID , MBR_GRP_ID, MBR_BRTH_DT, IMG_ID , DRAWER_ID, DOC_TYP_ID
FROM 
(
  SELECT C.COMM_SK as COMM_SK, 
         ltrim(rtrim(C.MBR_FIRST_NM)) as FIRSTNAME,
         ltrim(rtrim(C.MBR_LAST_NM)) as LASTNAME,
         case when len(ltrim(rtrim(C.MBR_MID_NM)))=0 then '' else ltrim(rtrim(C.MBR_MID_NM)) end as MIDDLENAME,
         '' as DESTINATION,
         ltrim(rtrim(C.COMM_FILE_NM)) AS ATTACHMENTS, 
         ltrim(rtrim(C.IMG_ID)) AS LETTERQUEUEID,
         ltrim(rtrim(C.COMM_TMPLT_NM)) AS TEMPLATENAME, 
         ltrim(rtrim(C.MBR_ID)) as SUBSCRIBERID,
         ltrim(rtrim(C.CMPLNT_ID)) as CMPLNT_ID,
         ltrim(rtrim(C.UM_REF_ID)) as UM_REF_ID,
         ltrim(rtrim(C.MBR_GRP_ID)) as MBR_GRP_ID,
         ltrim(rtrim(C.MBR_BRTH_DT)) as MBR_BRTH_DT,
         ltrim(rtrim(C.IMG_ID)) as IMG_ID,
         ltrim(rtrim(T.DRAWER_ID)) as DRAWER_ID,
         ltrim(rtrim(T.DOC_TYP_ID)) as DOC_TYP_ID
  FROM {CommTrnsmsnOwner}.[COMM] C
  Inner Join {CommTrnsmsnOwner}.[COMM_TMPLT_RECPNT_TRNSMSN_TYP] T
    on C.COMM_TMPLT_NM = T.COMM_TMPLT_RECPNT_TRNSMSN_TYP_NM
  WHERE 
    C.COMM_STTUS_CD='FAILED' and C.COMM_TYP_CD='PRCPTVSTS' and C.COMM_STTUS_CMNT_TX = 'File Not Found'
    AND C.CRT_DTM > DATEADD(HH,-2, CURRENT_TIMESTAMP)
    and reverse(substring(reverse(COMM_TMPLT_NM),1,2)) {ProvType} '_F'
)  A
"""

df_COMM_TRNSMSN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_xfm_Required_fields = df_COMM_TRNSMSN.select(
    col("COMM_SK"),
    EReplace(col("FIRSTNAME"), '$', ' ').alias("FIRSTNAME"),
    EReplace(col("LASTNAME"), '$', ' ').alias("LASTNAME"),
    EReplace(col("MIDDLENAME"), '$', ' ').alias("MIDDLENAME"),
    EReplace(col("DESTINATION"), '$', ' ').alias("DESTINATION"),
    EReplace(col("ATTACHMENTS"), '$', ' ').alias("ATTACHMENTS"),
    EReplace(col("LETTERQUEUEID"), '$', ' ').alias("LETTERQUEUEID"),
    EReplace(col("TEMPLATENAME"), '$', ' ').alias("TEMPLATENAME"),
    EReplace(col("SUBSCRIBERID"), '$', ' ').alias("SUBSCRIBERID"),
    EReplace(col("CMPLNT_ID"), '$', ' ').alias("CMPLNT_ID"),
    EReplace(col("UM_REF_ID"), '$', ' ').alias("UM_REF_ID"),
    EReplace(col("MBR_GRP_ID"), '$', ' ').alias("MBR_GRP_ID"),
    EReplace(col("MBR_BRTH_DT"), '$', ' ').alias("MBR_BRTH_DT"),
    EReplace(col("IMG_ID"), '$', ' ').alias("IMG_ID"),
    EReplace(col("DRAWER_ID"), '$', ' ').alias("DRAWER_ID"),
    EReplace(col("DOC_TYP_ID"), '$', ' ').alias("DOC_TYP_ID"),
    (when(
        (col("CMPLNT_ID").isNotNull()) & (length(trim(col("CMPLNT_ID"))) > 0),
        EReplace(col("CMPLNT_ID"), '$', ' ')
     ).otherwise(
        EReplace(col("UM_REF_ID"), '$', ' ')
     )).alias("CASE_ID"),
    lit(" ").alias("Dummy")
)

df_LookUp = df_xfm_Required_fields.alias("data").join(
    df_Percep.alias("lkup"),
    col("data.COMM_SK") == col("lkup.COMM_SK"),
    "left"
)

df_LookUp_lkupp = df_LookUp.select(
    col("data.COMM_SK").alias("COMM_SK"),
    col("data.FIRSTNAME").alias("FIRSTNAME"),
    col("data.LASTNAME").alias("LASTNAME"),
    col("data.MIDDLENAME").alias("MIDDLENAME"),
    col("data.DESTINATION").alias("DESTINATION"),
    col("data.ATTACHMENTS").alias("ATTACHMENTS"),
    col("data.LETTERQUEUEID").alias("LETTERQUEUEID"),
    col("data.TEMPLATENAME").alias("TEMPLATENAME"),
    col("data.SUBSCRIBERID").alias("SUBSCRIBERID"),
    col("data.CMPLNT_ID").alias("CMPLNT_ID"),
    col("data.UM_REF_ID").alias("UM_REF_ID"),
    col("data.MBR_GRP_ID").alias("MBR_GRP_ID"),
    col("data.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("data.IMG_ID").alias("IMG_ID"),
    col("data.DRAWER_ID").alias("DRAWER_ID"),
    col("data.DOC_TYP_ID").alias("DOC_TYP_ID"),
    col("data.MBR_SFX").alias("MBR_SFX"),
    col("data.CASE_ID").alias("CASE_ID"),
    col("data.Dummy").alias("Dummy")
)

df_LookUp_lnk_xfm = df_LookUp.select(
    col("data.COMM_SK").alias("COMM_SK"),
    col("data.FIRSTNAME").alias("FIRSTNAME"),
    col("data.LASTNAME").alias("LASTNAME"),
    col("data.MIDDLENAME").alias("MIDDLENAME"),
    col("data.DESTINATION").alias("DESTINATION"),
    col("data.ATTACHMENTS").alias("ATTACHMENTS"),
    col("data.LETTERQUEUEID").alias("LETTERQUEUEID"),
    col("data.TEMPLATENAME").alias("TEMPLATENAME"),
    col("data.SUBSCRIBERID").alias("SUBSCRIBERID"),
    col("data.CMPLNT_ID").alias("CMPLNT_ID"),
    col("data.UM_REF_ID").alias("UM_REF_ID"),
    col("data.MBR_GRP_ID").alias("MBR_GRP_ID"),
    col("data.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("data.IMG_ID").alias("IMG_ID"),
    col("data.DRAWER_ID").alias("DRAWER_ID"),
    col("data.DOC_TYP_ID").alias("DOC_TYP_ID"),
    col("data.MBR_SFX").alias("MBR_SFX"),
    col("data.CASE_ID").alias("CASE_ID"),
    col("data.Dummy").alias("Dummy")
)

df_Percep_data = df_LookUp_lnk_xfm

df_Percep_data_final = df_Percep_data.select(
    col("COMM_SK"),
    rpad(col("FIRSTNAME"), <...>, " ").alias("FIRSTNAME"),
    rpad(col("LASTNAME"), <...>, " ").alias("LASTNAME"),
    rpad(col("MIDDLENAME"), <...>, " ").alias("MIDDLENAME"),
    rpad(col("DESTINATION"), <...>, " ").alias("DESTINATION"),
    rpad(col("ATTACHMENTS"), <...>, " ").alias("ATTACHMENTS"),
    rpad(col("LETTERQUEUEID"), <...>, " ").alias("LETTERQUEUEID"),
    rpad(col("TEMPLATENAME"), <...>, " ").alias("TEMPLATENAME"),
    rpad(col("SUBSCRIBERID"), <...>, " ").alias("SUBSCRIBERID"),
    rpad(col("CMPLNT_ID"), <...>, " ").alias("CMPLNT_ID"),
    rpad(col("UM_REF_ID"), <...>, " ").alias("UM_REF_ID"),
    rpad(col("MBR_GRP_ID"), <...>, " ").alias("MBR_GRP_ID"),
    rpad(col("MBR_BRTH_DT"), <...>, " ").alias("MBR_BRTH_DT"),
    rpad(col("IMG_ID"), <...>, " ").alias("IMG_ID"),
    rpad(col("DRAWER_ID"), <...>, " ").alias("DRAWER_ID"),
    rpad(col("DOC_TYP_ID"), <...>, " ").alias("DOC_TYP_ID"),
    rpad(col("MBR_SFX"), <...>, " ").alias("MBR_SFX"),
    rpad(col("CASE_ID"), <...>, " ").alias("CASE_ID"),
    rpad(col("Dummy"), <...>, " ").alias("Dummy")
)

write_files(
    df_Percep_data_final,
    f"{adls_path_publish}/external/GuidingCare{Vendor}PercepRecipientData.txt",
    "$",
    "overwrite",
    False,
    False,
    None,
    None
)

df_Peek_23 = df_LookUp_lkupp.limit(10)