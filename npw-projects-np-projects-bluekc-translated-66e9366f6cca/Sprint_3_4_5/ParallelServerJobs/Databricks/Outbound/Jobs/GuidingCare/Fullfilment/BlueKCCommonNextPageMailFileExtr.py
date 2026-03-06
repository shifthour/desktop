# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2017 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : BlueKCCommonNextPageMailFileSeq
# MAGIC 
# MAGIC 
# MAGIC PROCESSING : Job creates Extract file for Next Page from COMM table.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                               Date                         Project                                   Change Description           \(9)\(9)\(9) Development Project          Code Reviewer                   Date Reviewed       
# MAGIC ------------------------------              ------------------        ----------------------             ---------------------------------------      \(9)\(9)\(9) ------------------------------\(9)----------------------------------         -------------------------------        
# MAGIC Bharani Chalamalasetty      04/01/2020             Fullfillment                       Original Programming               \(9)\(9)\(9)OutboundDev3                    Jaideep Mankala              04/20/2020
# MAGIC 
# MAGIC Bharani Chalamalasetty      07/20/2020            US243985   Added lookup to COMM_TMPLT_RECPNT_TRNSMSN_TYP       OutboundDev3                   Jaideep Mankala                 07/22/2020
# MAGIC \(9)\(9)\(9)\(9)\(9)          and pulled 5 fields
# MAGIC Saranya A                                2021-06-29            US370660         Modified Job to process NextPage and RightFax                              OutboundDev3          Jaideep Mankala                 06/30/2021
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)jobs in Parallel
# MAGIC 
# MAGIC Saranya A                                2021-10-14           US396204          Added new job parameters to pass new API link and key    OutboundDev3    Jaideep Mankala         10/15/2021    
# MAGIC Saranya A                          2022-03-15               US502007            Replace DataSets with Sequential file                    OutboundDev3                    Jaideep Mankala                03/16/2022     
# MAGIC Jaideep Mankala               2022-03-29                US579105           Updated code to check for pdf file existence                        OutboundDev3                     Jeyaprasanna                               2023-03-30
# MAGIC \(9)\(9)\(9)\(9)\(9)

# MAGIC Both Perceptive and Mail processing Data
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Outbound
# COMMAND ----------


# Retrieve job parameters
CommTrnsmsnOwner = get_widget_value('CommTrnsmsnOwner','')
commtrnsmsn_secret_name = get_widget_value('commtrnsmsn_secret_name','')
ParamFilePath = get_widget_value('ParamFilePath','')
RunID = get_widget_value('RunID','')
NXTPGOutboundFile = get_widget_value('NXTPGOutboundFile','')
CurrDateTm = get_widget_value('CurrDateTm','')
ProvType = get_widget_value('ProvType','')
CurrentDateM1 = get_widget_value('CurrentDateM1','')

# --------------------------------------------------------------------------------
# Stage: PERCP_COMM_TRNSMSN (ODBCConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_PERCP_COMM_TRNSMSN, jdbc_props_PERCP_COMM_TRNSMSN = get_db_config(commtrnsmsn_secret_name)
extract_query_PERCP_COMM_TRNSMSN = """SELECT COMM_SK, FIRSTNAME, LASTNAME, MIDDLENAME, cast(DESTINATION as varchar(20)) as DESTINATION, ATTACHMENTS, cast(LETTERQUEUEID as varchar(40)) as LETTERQUEUEID, TEMPLATENAME, SUBSCRIBERID , INDICATOR, 
CMPLNT_ID , UM_REF_ID , MBR_GRP_ID, MBR_BRTH_DT, IMG_ID , DRAWER_ID, DOC_TYP_ID , CRT_DTM
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
       'P' as INDICATOR,
       ltrim(rtrim(C.CMPLNT_ID)) as CMPLNT_ID , 
       ltrim(rtrim(C.UM_REF_ID)) as UM_REF_ID , 
       ltrim(rtrim(C.MBR_GRP_ID)) as MBR_GRP_ID, 
       ltrim(rtrim(C.MBR_BRTH_DT)) as MBR_BRTH_DT, 
       ltrim(rtrim(C.IMG_ID)) as IMG_ID , 
       ltrim(rtrim(T.DRAWER_ID)) as DRAWER_ID, 
       ltrim(rtrim(T.DOC_TYP_ID)) as DOC_TYP_ID,
       C.CRT_DTM CRT_DTM
FROM """ + CommTrnsmsnOwner + """.[COMM] C
Inner Join """ + CommTrnsmsnOwner + """.[COMM_TMPLT_RECPNT_TRNSMSN_TYP] T
  on C.COMM_TMPLT_NM = T.COMM_TMPLT_RECPNT_TRNSMSN_TYP_NM
WHERE C.COMM_STTUS_CD='DOCRCVD' and C.COMM_TYP_CD in ('ALTRSTAFILE','AGPAHUBFILE') 
and reverse(substring(reverse(C.COMM_TMPLT_NM),1,2)) """ + ProvType + """ '_F'

UNION ALL

SELECT C.COMM_SK as COMM_SK, 
       ltrim(rtrim(C.MBR_FIRST_NM)) as FIRSTNAME,
       ltrim(rtrim(C.MBR_LAST_NM)) as LASTNAME,
       case when len(ltrim(rtrim(C.MBR_MID_NM)))=0 then '' else ltrim(rtrim(C.MBR_MID_NM)) end as MIDDLENAME,
       '' as DESTINATION,
       ltrim(rtrim(C.COMM_FILE_NM)) AS ATTACHMENTS, 
       ltrim(rtrim(C.IMG_ID)) AS LETTERQUEUEID,
       ltrim(rtrim(C.COMM_TMPLT_NM)) AS TEMPLATENAME, 
       ltrim(rtrim(C.MBR_ID)) as SUBSCRIBERID,
       'N' as INDICATOR,
       ltrim(rtrim(C.CMPLNT_ID)) as CMPLNT_ID , 
       ltrim(rtrim(C.UM_REF_ID)) as UM_REF_ID , 
       ltrim(rtrim(C.MBR_GRP_ID)) as MBR_GRP_ID, 
       ltrim(rtrim(C.MBR_BRTH_DT)) as MBR_BRTH_DT, 
       ltrim(rtrim(C.IMG_ID)) as IMG_ID , 
       ltrim(rtrim(T.DRAWER_ID)) as DRAWER_ID, 
       ltrim(rtrim(T.DOC_TYP_ID)) as DOC_TYP_ID,
       C.CRT_DTM CRT_DTM
FROM """ + CommTrnsmsnOwner + """.[COMM] C
Inner Join """ + CommTrnsmsnOwner + """.[COMM_TMPLT_RECPNT_TRNSMSN_TYP] T
  on C.COMM_TMPLT_NM = T.COMM_TMPLT_RECPNT_TRNSMSN_TYP_NM
WHERE C.COMM_TYP_CD in ('ALTRSTAFILE','AGPAHUBFILE') AND C.COMM_STTUS_CD='MAILRCVD'
)  A
"""
df_PERCP_COMM_TRNSMSN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_PERCP_COMM_TRNSMSN)
    .options(**jdbc_props_PERCP_COMM_TRNSMSN)
    .option("query", extract_query_PERCP_COMM_TRNSMSN)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: COMM_TRNSMSN (ODBCConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_COMM_TRNSMSN, jdbc_props_COMM_TRNSMSN = get_db_config(commtrnsmsn_secret_name)
extract_query_COMM_TRNSMSN = """SELECT [COMM_SK]
      ,[COMM_ID]
      ,[COMM_SRC_SYS_CD]
      ,[COMM_TRGT_SYS_CD]
      ,[COMM_SENT_DTM]
      ,[COMM_STTUS_CD]
      ,[COMM_TYP_CD]
      ,[COMM_STTUS_CMNT_TX]
      ,[COMM_DESC]
      ,[COMM_FILE_NM]
      ,[COMM_JOB_ID]
      ,[COMM_LANG_NM]
      ,[COMM_RECPNT_TYP_NM]
      ,[COMM_SRC_SYS_MDUL_CD]
      ,[COMM_TRGT_SYS_MDUL_CD]
      ,[COMM_TMPLT_NM]
      ,[COMM_RQST_BY_USER_ID]
      ,[COMM_RQST_BY_USER_DTM]
      ,[COMM_TRNSMSN_CT]
      ,[CMPLNT_ID]
      ,[MBR_ID]
      ,[SUB_ID]
      ,[MBR_BRTH_DT]
      ,[MBR_ELIG_STRT_DT]
      ,[MBR_ELIG_END_DT]
      ,[MBR_ETHNIC_NM]
      ,[MBR_GNDR_CD]
      ,[MBR_FIRST_NM]
      ,[MBR_LAST_NM]
      ,[MBR_MID_NM]
      ,[MBR_ADDR_LN]
      ,[MBR_ADDR_CITY_NM]
      ,[MBR_ADDR_ST_NM]
      ,[MBR_ADDR_ZIP_CD]
      ,[MBR_ADDR_CNTY_NM]
      ,[MBR_ADDR_CTRY_NM]
      ,[MBR_ADDR_TYP_NM]
      ,[PROD_NM]
      ,[PROV_ID]
      ,[PROV_NTNL_PROV_ID]
      ,[PROV_EMAIL_ADDR]
      ,[PROV_ETHNIC_NM]
      ,[PROV_FIRST_NM]
      ,[PROV_LAST_NM]
      ,[PROV_MID_NM]
      ,[PROV_NM]
      ,[PROV_ADDR_FAX_NO]
      ,[PROV_ADDR_FAX_OVRD_NO]
      ,[PROV_ADDR_LN]
      ,[PROV_ADDR_CITY_NM]
      ,[PROV_ADDR_ST_NM]
      ,[PROV_ADDR_ZIP_CD]
      ,[PROV_ADDR_CNTY_NM]
      ,[PROV_ADDR_PHN_NO]
      ,[PROV_ADDR_PHN_OVRD_NO]
      ,[PROV_ADDR_TYP_NM]
      ,[PROV_PRI_ADDR_IN]
      ,[UM_AUTH_TYP_NM]
      ,[UM_REF_ID]
      ,[CRT_DTM]
      ,[CRT_USER_ID]
      ,[LAST_UPDT_DTM]
      ,[LAST_UPDT_USER_ID]
      ,[COMM_TMPLT_RECPNT_TRNSMSN_TYP_ID]
FROM """ + CommTrnsmsnOwner + """.[COMM] where COMM_TYP_CD in ('ALTRSTAFILE','AGPAHUBFILE') AND COMM_STTUS_CD='MAILRCVD'"""
df_COMM_TRNSMSN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_COMM_TRNSMSN)
    .options(**jdbc_props_COMM_TRNSMSN)
    .option("query", extract_query_COMM_TRNSMSN)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: COMM_TMPLT_RECPNT_TRNSMSN_TYP (ODBCConnectorPX)
# --------------------------------------------------------------------------------
jdbc_url_COMM_TMPLT_RECPNT_TRNSMSN_TYP, jdbc_props_COMM_TMPLT_RECPNT_TRNSMSN_TYP = get_db_config(commtrnsmsn_secret_name)
extract_query_COMM_TMPLT_RECPNT_TRNSMSN_TYP = "SELECT * FROM " + CommTrnsmsnOwner + ".[COMM_TMPLT_RECPNT_TRNSMSN_TYP]"
df_COMM_TMPLT_RECPNT_TRNSMSN_TYP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_COMM_TMPLT_RECPNT_TRNSMSN_TYP)
    .options(**jdbc_props_COMM_TMPLT_RECPNT_TRNSMSN_TYP)
    .option("query", extract_query_COMM_TMPLT_RECPNT_TRNSMSN_TYP)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: lkp_COMM_REF (PxLookup) - Cross join because no join conditions
# --------------------------------------------------------------------------------
# We join df_COMM_TRNSMSN (primary) with df_COMM_TMPLT_RECPNT_TRNSMSN_TYP (lookup) on no condition => cross join.
df_lkp_COMM_REF_intermediate = df_COMM_TRNSMSN.alias("lnk_COMM").crossJoin(
    df_COMM_TMPLT_RECPNT_TRNSMSN_TYP.alias("lnk_ref")
)
df_lkp_COMM_REF = df_lkp_COMM_REF_intermediate.select(
    F.col("lnk_COMM.COMM_SK").alias("COMM_SK"),
    F.col("lnk_COMM.COMM_ID").alias("COMM_ID"),
    F.col("lnk_COMM.COMM_SRC_SYS_CD").alias("COMM_SRC_SYS_CD"),
    F.col("lnk_COMM.COMM_TRGT_SYS_CD").alias("COMM_TRGT_SYS_CD"),
    F.col("lnk_COMM.COMM_SENT_DTM").alias("COMM_SENT_DTM"),
    F.col("lnk_COMM.COMM_STTUS_CD").alias("COMM_STTUS_CD"),
    F.col("lnk_COMM.COMM_TYP_CD").alias("COMM_TYP_CD"),
    F.col("lnk_COMM.COMM_STTUS_CMNT_TX").alias("COMM_STTUS_CMNT_TX"),
    F.col("lnk_COMM.COMM_DESC").alias("COMM_DESC"),
    F.col("lnk_COMM.COMM_FILE_NM").alias("COMM_FILE_NM"),
    F.col("lnk_COMM.COMM_JOB_ID").alias("COMM_JOB_ID"),
    F.col("lnk_COMM.COMM_LANG_NM").alias("COMM_LANG_NM"),
    F.col("lnk_COMM.COMM_RECPNT_TYP_NM").alias("COMM_RECPNT_TYP_NM"),
    F.col("lnk_COMM.COMM_SRC_SYS_MDUL_CD").alias("COMM_SRC_SYS_MDUL_CD"),
    F.col("lnk_COMM.COMM_TRGT_SYS_MDUL_CD").alias("COMM_TRGT_SYS_MDUL_CD"),
    F.col("lnk_COMM.COMM_TMPLT_NM").alias("COMM_TMPLT_NM"),
    F.col("lnk_COMM.COMM_RQST_BY_USER_ID").alias("COMM_RQST_BY_USER_ID"),
    F.col("lnk_COMM.COMM_RQST_BY_USER_DTM").alias("COMM_RQST_BY_USER_DTM"),
    F.col("lnk_COMM.COMM_TRNSMSN_CT").alias("COMM_TRNSMSN_CT"),
    F.col("lnk_COMM.CMPLNT_ID").alias("CMPLNT_ID"),
    F.col("lnk_COMM.MBR_ID").alias("MBR_ID"),
    F.col("lnk_COMM.SUB_ID").alias("SUB_ID"),
    F.col("lnk_COMM.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("lnk_COMM.MBR_ELIG_STRT_DT").alias("MBR_ELIG_STRT_DT"),
    F.col("lnk_COMM.MBR_ELIG_END_DT").alias("MBR_ELIG_END_DT"),
    F.col("lnk_COMM.MBR_ETHNIC_NM").alias("MBR_ETHNIC_NM"),
    F.col("lnk_COMM.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("lnk_COMM.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("lnk_COMM.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("lnk_COMM.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("lnk_COMM.MBR_ADDR_LN").alias("MBR_ADDR_LN"),
    F.col("lnk_COMM.MBR_ADDR_CITY_NM").alias("MBR_ADDR_CITY_NM"),
    F.col("lnk_COMM.MBR_ADDR_ST_NM").alias("MBR_ADDR_ST_NM"),
    F.col("lnk_COMM.MBR_ADDR_ZIP_CD").alias("MBR_ADDR_ZIP_CD"),
    F.col("lnk_COMM.MBR_ADDR_CNTY_NM").alias("MBR_ADDR_CNTY_NM"),
    F.col("lnk_COMM.MBR_ADDR_CTRY_NM").alias("MBR_ADDR_CTRY_NM"),
    F.col("lnk_COMM.MBR_ADDR_TYP_NM").alias("MBR_ADDR_TYP_NM"),
    F.col("lnk_COMM.PROD_NM").alias("PROD_NM"),
    F.col("lnk_COMM.PROV_ID").alias("PROV_ID"),
    F.col("lnk_COMM.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("lnk_COMM.PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
    F.col("lnk_COMM.PROV_ETHNIC_NM").alias("PROV_ETHNIC_NM"),
    F.col("lnk_COMM.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("lnk_COMM.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("lnk_COMM.PROV_MID_NM").alias("PROV_MID_NM"),
    F.col("lnk_COMM.PROV_NM").alias("PROV_NM"),
    F.col("lnk_COMM.PROV_ADDR_FAX_NO").alias("PROV_ADDR_FAX_NO"),
    F.col("lnk_COMM.PROV_ADDR_FAX_OVRD_NO").alias("PROV_ADDR_FAX_OVRD_NO"),
    F.col("lnk_COMM.PROV_ADDR_LN").alias("PROV_ADDR_LN"),
    F.col("lnk_COMM.PROV_ADDR_CITY_NM").alias("PROV_ADDR_CITY_NM"),
    F.col("lnk_COMM.PROV_ADDR_ST_NM").alias("PROV_ADDR_ST_NM"),
    F.col("lnk_COMM.PROV_ADDR_ZIP_CD").alias("PROV_ADDR_ZIP_CD"),
    F.col("lnk_COMM.PROV_ADDR_CNTY_NM").alias("PROV_ADDR_CNTY_NM"),
    F.col("lnk_COMM.PROV_ADDR_PHN_NO").alias("PROV_ADDR_PHN_NO"),
    F.col("lnk_COMM.PROV_ADDR_PHN_OVRD_NO").alias("PROV_ADDR_PHN_OVRD_NO"),
    F.col("lnk_COMM.PROV_ADDR_TYP_NM").alias("PROV_ADDR_TYP_NM"),
    F.col("lnk_COMM.PROV_PRI_ADDR_IN").alias("PROV_PRI_ADDR_IN"),
    F.col("lnk_COMM.UM_AUTH_TYP_NM").alias("UM_AUTH_TYP_NM"),
    F.col("lnk_COMM.UM_REF_ID").alias("UM_REF_ID"),
    F.col("lnk_COMM.CRT_DTM").alias("CRT_DTM"),
    F.col("lnk_COMM.CRT_USER_ID").alias("CRT_USER_ID"),
    F.col("lnk_COMM.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("lnk_COMM.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    F.col("lnk_ref.COLOR_CD").alias("COLOR_CD"),
    F.col("lnk_ref.CC_ID").alias("CC_ID"),
    F.col("lnk_ref.ENVELOPE_TYP_ID").alias("ENVELOPE_TYP_ID"),
    F.col("lnk_ref.BUS_REPLY_ENVELOPE_TYP_ID").alias("BRE_TYP_ID"),
    F.col("lnk_ref.INSRT_TYP_ID").alias("INSRT_TYP_ID"),
)

# --------------------------------------------------------------------------------
# Stage: PDFFiles (PxSequentialFile) Input
# --------------------------------------------------------------------------------
schema_PDFFiles = StructType([
    StructField("FileName", StringType(), True)
])
df_PDFFiles = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_PDFFiles)
    .load(f"{adls_path_publish}/external/NextPagePDFCheckList.txt")
)

# --------------------------------------------------------------------------------
# Stage: Copy_32 (PxCopy)
# --------------------------------------------------------------------------------
df_DSLink31 = df_PDFFiles
df_DSLink33 = df_DSLink31.select(F.col("FileName").alias("FileName"))
df_DSLink34 = df_DSLink31.select(F.col("FileName").alias("FileName"))

# --------------------------------------------------------------------------------
# Stage: Lookup_27 (PxLookup)
# --------------------------------------------------------------------------------
# Primary: df_PERCP_COMM_TRNSMSN as lnk_Src1
# Lookup: df_DSLink34 on lnk_Src1.ATTACHMENTS = DSLink34.FileName (left join)
df_Lookup_27_intermediate = df_PERCP_COMM_TRNSMSN.alias("lnk_Src1").join(
    df_DSLink34.alias("DSLink34"),
    F.col("lnk_Src1.ATTACHMENTS") == F.col("DSLink34.FileName"),
    how="left"
)
df_Lookup_27 = df_Lookup_27_intermediate.select(
    F.col("lnk_Src1.COMM_SK").alias("COMM_SK"),
    F.col("lnk_Src1.FIRSTNAME").alias("FIRSTNAME"),
    F.col("lnk_Src1.LASTNAME").alias("LASTNAME"),
    F.col("lnk_Src1.MIDDLENAME").alias("MIDDLENAME"),
    F.col("lnk_Src1.DESTINATION").alias("DESTINATION"),
    F.col("lnk_Src1.ATTACHMENTS").alias("ATTACHMENTS"),
    F.col("lnk_Src1.LETTERQUEUEID").alias("LETTERQUEUEID"),
    F.col("lnk_Src1.TEMPLATENAME").alias("TEMPLATENAME"),
    F.col("lnk_Src1.SUBSCRIBERID").alias("SUBSCRIBERID"),
    F.col("lnk_Src1.INDICATOR").alias("INDICATOR"),
    F.col("lnk_Src1.CMPLNT_ID").alias("CMPLNT_ID"),
    F.col("lnk_Src1.UM_REF_ID").alias("UM_REF_ID"),
    F.col("lnk_Src1.MBR_GRP_ID").alias("MBR_GRP_ID"),
    F.col("lnk_Src1.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("lnk_Src1.IMG_ID").alias("IMG_ID"),
    F.col("lnk_Src1.DRAWER_ID").alias("DRAWER_ID"),
    F.col("lnk_Src1.DOC_TYP_ID").alias("DOC_TYP_ID"),
    F.col("lnk_Src1.CRT_DTM").alias("CRT_DTM"),
    F.col("DSLink34.FileName").alias("FileName")
)

# --------------------------------------------------------------------------------
# Stage: Required_fields (CTransformerStage)
# --------------------------------------------------------------------------------
# Define the filter constraint for output link lnk_xfm
# ( (FileName not null and length(trim(FileName))>=4 )
#    OR (trim(CRT_DTM)[1,10]< CurrentDateM1 and FileName is null and length(trim(FileName))<4 ) )
df_Required_fields_constraint = (
    (F.col("FileName").isNotNull() & (F.length(trim(F.col("FileName"))) >= 4))
    | (
        (F.substring(trim(F.col("CRT_DTM")), 1, 10) < F.lit(CurrentDateM1)) 
        & (F.col("FileName").isNull()) 
        & (F.length(trim(F.col("FileName"))) < 4)
    )
)
df_Required_fields_lnk_xfm = df_Lookup_27.filter(df_Required_fields_constraint).select(
    F.col("COMM_SK").alias("COMM_SK"),
    EReplace(F.col("FIRSTNAME"), F.lit('$'), F.lit(' ')).alias("FIRSTNAME"),
    EReplace(F.col("LASTNAME"), F.lit('$'), F.lit(' ')).alias("LASTNAME"),
    EReplace(F.col("MIDDLENAME"), F.lit('$'), F.lit(' ')).alias("MIDDLENAME"),
    EReplace(F.col("DESTINATION"), F.lit('$'), F.lit(' ')).alias("DESTINATION"),
    EReplace(F.col("ATTACHMENTS"), F.lit('$'), F.lit(' ')).alias("ATTACHMENTS"),
    EReplace(F.col("LETTERQUEUEID"), F.lit('$'), F.lit(' ')).alias("LETTERQUEUEID"),
    EReplace(F.col("TEMPLATENAME"), F.lit('$'), F.lit(' ')).alias("TEMPLATENAME"),
    EReplace(F.col("SUBSCRIBERID"), F.lit('$'), F.lit(' ')).alias("SUBSCRIBERID"),
    F.col("INDICATOR").alias("INDICATOR"),
    EReplace(F.col("CMPLNT_ID"), F.lit('$'), F.lit(' ')).alias("CMPLNT_ID"),
    EReplace(F.col("UM_REF_ID"), F.lit('$'), F.lit(' ')).alias("UM_REF_ID"),
    EReplace(F.col("MBR_GRP_ID"), F.lit('$'), F.lit(' ')).alias("MBR_GRP_ID"),
    EReplace(F.col("MBR_BRTH_DT"), F.lit('$'), F.lit(' ')).alias("MBR_BRTH_DT"),
    EReplace(F.col("IMG_ID"), F.lit('$'), F.lit(' ')).alias("IMG_ID"),
    EReplace(F.col("DRAWER_ID"), F.lit('$'), F.lit(' ')).alias("DRAWER_ID"),
    EReplace(F.col("DOC_TYP_ID"), F.lit('$'), F.lit(' ')).alias("DOC_TYP_ID"),
    Right(EReplace(F.col("SUBSCRIBERID"), F.lit('$'), F.lit(' ')), F.lit(2)).alias("MBR_SFX"),
    F.lit(' ').alias("Dummy")
)

# --------------------------------------------------------------------------------
# Stage: Percep_data (PxSequentialFile) - writing
# --------------------------------------------------------------------------------
# Write df_Required_fields_lnk_xfm to "GuidingCareNextPagePercepMailInitialData_#RunID#.txt" with $ delimiter
# Re-select columns in final order as in the transform
df_Percep_data = df_Required_fields_lnk_xfm.select(
    "COMM_SK",
    "FIRSTNAME",
    "LASTNAME",
    "MIDDLENAME",
    "DESTINATION",
    "ATTACHMENTS",
    "LETTERQUEUEID",
    "TEMPLATENAME",
    "SUBSCRIBERID",
    "INDICATOR",
    "CMPLNT_ID",
    "UM_REF_ID",
    "MBR_GRP_ID",
    "MBR_BRTH_DT",
    "IMG_ID",
    "DRAWER_ID",
    "DOC_TYP_ID",
    "MBR_SFX",
    "Dummy"
)
write_files(
    df_Percep_data,
    f"{adls_path_publish}/external/GuidingCareNextPagePercepMailInitialData_{RunID}.txt",
    delimiter="$",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Lookup_29 (PxLookup)
# --------------------------------------------------------------------------------
# Primary: df_lkp_COMM_REF as lnk_Src1
# Lookup: df_DSLink33 on lnk_Src1.COMM_FILE_NM = DSLink33.FileName (left join)
df_Lookup_29_intermediate = df_lkp_COMM_REF.alias("lnk_Src1").join(
    df_DSLink33.alias("DSLink33"),
    F.col("lnk_Src1.COMM_FILE_NM") == F.col("DSLink33.FileName"),
    how="left"
)
df_Lookup_29 = df_Lookup_29_intermediate.select(
    F.col("lnk_Src1.COMM_SK").alias("COMM_SK"),
    F.col("lnk_Src1.COMM_ID").alias("COMM_ID"),
    F.col("lnk_Src1.COMM_SRC_SYS_CD").alias("COMM_SRC_SYS_CD"),
    F.col("lnk_Src1.COMM_TRGT_SYS_CD").alias("COMM_TRGT_SYS_CD"),
    F.col("lnk_Src1.COMM_SENT_DTM").alias("COMM_SENT_DTM"),
    F.col("lnk_Src1.COMM_STTUS_CD").alias("COMM_STTUS_CD"),
    F.col("lnk_Src1.COMM_TYP_CD").alias("COMM_TYP_CD"),
    F.col("lnk_Src1.COMM_STTUS_CMNT_TX").alias("COMM_STTUS_CMNT_TX"),
    F.col("lnk_Src1.COMM_DESC").alias("COMM_DESC"),
    F.col("lnk_Src1.COMM_FILE_NM").alias("COMM_FILE_NM"),
    F.col("lnk_Src1.COMM_JOB_ID").alias("COMM_JOB_ID"),
    F.col("lnk_Src1.COMM_LANG_NM").alias("COMM_LANG_NM"),
    F.col("lnk_Src1.COMM_RECPNT_TYP_NM").alias("COMM_RECPNT_TYP_NM"),
    F.col("lnk_Src1.COMM_SRC_SYS_MDUL_CD").alias("COMM_SRC_SYS_MDUL_CD"),
    F.col("lnk_Src1.COMM_TRGT_SYS_MDUL_CD").alias("COMM_TRGT_SYS_MDUL_CD"),
    F.col("lnk_Src1.COMM_TMPLT_NM").alias("COMM_TMPLT_NM"),
    F.col("lnk_Src1.COMM_RQST_BY_USER_ID").alias("COMM_RQST_BY_USER_ID"),
    F.col("lnk_Src1.COMM_RQST_BY_USER_DTM").alias("COMM_RQST_BY_USER_DTM"),
    F.col("lnk_Src1.COMM_TRNSMSN_CT").alias("COMM_TRNSMSN_CT"),
    F.col("lnk_Src1.CMPLNT_ID").alias("CMPLNT_ID"),
    F.col("lnk_Src1.MBR_ID").alias("MBR_ID"),
    F.col("lnk_Src1.SUB_ID").alias("SUB_ID"),
    F.col("lnk_Src1.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("lnk_Src1.MBR_ELIG_STRT_DT").alias("MBR_ELIG_STRT_DT"),
    F.col("lnk_Src1.MBR_ELIG_END_DT").alias("MBR_ELIG_END_DT"),
    F.col("lnk_Src1.MBR_ETHNIC_NM").alias("MBR_ETHNIC_NM"),
    F.col("lnk_Src1.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("lnk_Src1.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("lnk_Src1.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("lnk_Src1.MBR_MID_NM").alias("MBR_MID_NM"),
    F.col("lnk_Src1.MBR_ADDR_LN").alias("MBR_ADDR_LN"),
    F.col("lnk_Src1.MBR_ADDR_CITY_NM").alias("MBR_ADDR_CITY_NM"),
    F.col("lnk_Src1.MBR_ADDR_ST_NM").alias("MBR_ADDR_ST_NM"),
    F.col("lnk_Src1.MBR_ADDR_ZIP_CD").alias("MBR_ADDR_ZIP_CD"),
    F.col("lnk_Src1.MBR_ADDR_CNTY_NM").alias("MBR_ADDR_CNTY_NM"),
    F.col("lnk_Src1.MBR_ADDR_CTRY_NM").alias("MBR_ADDR_CTRY_NM"),
    F.col("lnk_Src1.MBR_ADDR_TYP_NM").alias("MBR_ADDR_TYP_NM"),
    F.col("lnk_Src1.PROD_NM").alias("PROD_NM"),
    F.col("lnk_Src1.PROV_ID").alias("PROV_ID"),
    F.col("lnk_Src1.PROV_NTNL_PROV_ID").alias("PROV_NTNL_PROV_ID"),
    F.col("lnk_Src1.PROV_EMAIL_ADDR").alias("PROV_EMAIL_ADDR"),
    F.col("lnk_Src1.PROV_ETHNIC_NM").alias("PROV_ETHNIC_NM"),
    F.col("lnk_Src1.PROV_FIRST_NM").alias("PROV_FIRST_NM"),
    F.col("lnk_Src1.PROV_LAST_NM").alias("PROV_LAST_NM"),
    F.col("lnk_Src1.PROV_MID_NM").alias("PROV_MID_NM"),
    F.col("lnk_Src1.PROV_NM").alias("PROV_NM"),
    F.col("lnk_Src1.PROV_ADDR_FAX_NO").alias("PROV_ADDR_FAX_NO"),
    F.col("lnk_Src1.PROV_ADDR_FAX_OVRD_NO").alias("PROV_ADDR_FAX_OVRD_NO"),
    F.col("lnk_Src1.PROV_ADDR_LN").alias("PROV_ADDR_LN"),
    F.col("lnk_Src1.PROV_ADDR_CITY_NM").alias("PROV_ADDR_CITY_NM"),
    F.col("lnk_Src1.PROV_ADDR_ST_NM").alias("PROV_ADDR_ST_NM"),
    F.col("lnk_Src1.PROV_ADDR_ZIP_CD").alias("PROV_ADDR_ZIP_CD"),
    F.col("lnk_Src1.PROV_ADDR_CNTY_NM").alias("PROV_ADDR_CNTY_NM"),
    F.col("lnk_Src1.PROV_ADDR_PHN_NO").alias("PROV_ADDR_PHN_NO"),
    F.col("lnk_Src1.PROV_ADDR_PHN_OVRD_NO").alias("PROV_ADDR_PHN_OVRD_NO"),
    F.col("lnk_Src1.PROV_ADDR_TYP_NM").alias("PROV_ADDR_TYP_NM"),
    F.col("lnk_Src1.PROV_PRI_ADDR_IN").alias("PROV_PRI_ADDR_IN"),
    F.col("lnk_Src1.UM_AUTH_TYP_NM").alias("UM_AUTH_TYP_NM"),
    F.col("lnk_Src1.UM_REF_ID").alias("UM_REF_ID"),
    F.col("lnk_Src1.CRT_DTM").alias("CRT_DTM"),
    F.col("lnk_Src1.CRT_USER_ID").alias("CRT_USER_ID"),
    F.col("lnk_Src1.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("lnk_Src1.LAST_UPDT_USER_ID").alias("LAST_UPDT_USER_ID"),
    F.col("lnk_Src1.COLOR_CD").alias("COLOR_CD"),
    F.col("lnk_Src1.CC_ID").alias("CC_ID"),
    F.col("lnk_Src1.ENVELOPE_TYP_ID").alias("ENVELOPE_TYP_ID"),
    F.col("lnk_Src1.BRE_TYP_ID").alias("BRE_TYP_ID"),
    F.col("lnk_Src1.INSRT_TYP_ID").alias("INSRT_TYP_ID"),
    F.col("DSLink33.FileName").alias("FileName")
)

# --------------------------------------------------------------------------------
# Stage: xfm_Required_fields (CTransformerStage)
# --------------------------------------------------------------------------------
# We have three output links with different constraints.

df_xfm_Required_fields = df_Lookup_29

# 1) lnk_xfm -> Seq_MailExtract
constraint_lnk_xfm = (
    (F.col("FileName").isNotNull() & (F.length(trim(F.col("FileName"))) >= 4))
    | (
        (F.substring(trim(F.col("CRT_DTM")), 1, 10) < F.lit(CurrentDateM1))
        & (F.col("FileName").isNull())
        & (F.length(trim(F.col("FileName"))) < 4)
    )
)
df_xfm_Required_fields_lnk_xfm = df_xfm_Required_fields.filter(constraint_lnk_xfm).select(
    F.col("COMM_ID").alias("LetterQueueId"),
    F.col("COMM_FILE_NM").alias("FileName"),
    F.col("COMM_TMPLT_NM").alias("TemplateName"),
    F.col("COMM_RQST_BY_USER_ID").alias("CreatedBy"),
    F.col("COMM_RQST_BY_USER_DTM").alias("CreatedOn"),
    F.when(
        (F.col("CMPLNT_ID").isNotNull()) & (F.length(trim(F.col("CMPLNT_ID"))) > 0),
        F.col("CMPLNT_ID")
    ).otherwise(F.col("UM_REF_ID")).alias("AuthorizationUniqueIdentifier"),
    F.lit('').alias("AuthorizationNumber"),
    F.col("MBR_ID").alias("SubscriberPrimaryIdentifier"),
    F.col("SUB_ID").alias("SubscriberSecondaryIdentifier"),
    F.col("MBR_FIRST_NM").alias("SubscriberFirstName"),
    F.col("MBR_MID_NM").alias("SubscriberMiddleName"),
    TimestampToDate(F.col("MBR_BRTH_DT")).alias("SubscriberDOB"),
    F.col("MBR_LAST_NM").alias("SubscriberLastName"),
    F.col("MBR_GNDR_CD").alias("SubscriberGender"),
    F.col("MBR_ETHNIC_NM").alias("SubscriberEthnicity"),
    F.lit('').alias("SubscriberLineOfBusiness"),
    F.col("PROD_NM").alias("SubscriberBenefitPlan"),
    TimestampToDate(F.col("MBR_ELIG_STRT_DT")).alias("SubscriberEligibilityStartDate"),
    TimestampToDate(F.col("MBR_ELIG_END_DT")).alias("SubscriberEligibilityEndDate"),
    F.col("MBR_ADDR_LN").alias("SubscriberAddressLine1"),
    F.lit('').alias("SubscriberAddressLine2"),
    F.lit('').alias("SubscriberAddressLine3"),
    F.col("MBR_ADDR_CITY_NM").alias("SubscriberCity"),
    F.col("MBR_ADDR_ST_NM").alias("SubscriberState"),
    F.col("MBR_ADDR_CNTY_NM").alias("SubscriberCounty"),
    F.col("MBR_ADDR_CTRY_NM").alias("SubscriberCountry"),
    F.col("MBR_ADDR_ZIP_CD").alias("SubscriberZip"),
    F.col("MBR_ADDR_TYP_NM").alias("SubscriberAddressType"),
    F.col("PROV_ID").alias("ProviderPrimaryIdentifier"),
    F.col("PROV_NTNL_PROV_ID").alias("ProviderSecondaryIdentifier"),
    F.col("PROV_FIRST_NM").alias("ProviderFirstName"),
    F.col("PROV_LAST_NM").alias("ProviderLastName"),
    F.col("PROV_MID_NM").alias("ProviderMiddleName"),
    F.col("PROV_NM").alias("ProviderName"),
    F.col("PROV_ETHNIC_NM").alias("ProviderEthnicity"),
    F.col("PROV_ADDR_LN").alias("ProviderAddressLine1"),
    F.lit('').alias("ProviderAddressLine2"),
    F.lit('').alias("ProviderAddressLine3"),
    F.col("PROV_ADDR_CITY_NM").alias("ProviderCity"),
    F.col("PROV_ADDR_ST_NM").alias("ProviderState"),
    F.col("PROV_ADDR_ZIP_CD").alias("ProviderZip"),
    F.col("PROV_ADDR_CNTY_NM").alias("ProviderCountyDescription"),
    F.col("PROV_ADDR_PHN_NO").alias("ProviderOfficePhone"),
    F.col("PROV_ADDR_PHN_OVRD_NO").alias("ProviderOfficePhoneOveride"),
    F.col("PROV_ADDR_FAX_NO").alias("ProviderFax"),
    F.col("PROV_ADDR_FAX_OVRD_NO").alias("ProviderFaxOverride"),
    F.col("PROV_PRI_ADDR_IN").alias("ISProviderPrimaryAddress"),
    F.col("PROV_EMAIL_ADDR").alias("ProviderEmail"),
    F.col("PROV_ADDR_TYP_NM").alias("ProviderAddressType"),
    F.col("UM_AUTH_TYP_NM").alias("ProviderAuthorizationType"),
    F.col("COLOR_CD").alias("ColorCode"),
    F.col("CC_ID").alias("CostCenterID"),
    F.col("ENVELOPE_TYP_ID").alias("EnvelopeTypeID"),
    F.col("BRE_TYP_ID").alias("BRETypeID"),
    F.col("INSRT_TYP_ID").alias("InsertTypeID")
)

# 2) mail -> MailExtract
constraint_mail = constraint_lnk_xfm  # same filter
df_xfm_Required_fields_mail = df_xfm_Required_fields.filter(constraint_mail).select(
    F.col("COMM_SK").alias("COMM_SK"),
    F.col("COMM_ID").alias("COMM_ID"),
    F.lit("COMMTRNSMSN").alias("COMM_SRC_SYS_CD"),
    F.lit("NXTPG").alias("COMM_TRGT_SYS_CD"),
    F.lit("SUCCESS").alias("COMM_STTUS_CD"),
    F.lit("MAILRQST").alias("COMM_TYP_CD"),
    F.lit("Mail Request Initiated Successfully").alias("COMM_STTUS_CMNT_TX"),
    F.lit(CurrDateTm).alias("LAST_UPDT_DTM")
)

# 3) Missing -> Sequential_File_42
constraint_missing = (
    (F.substring(trim(F.col("CRT_DTM")), 1, 10) <= F.lit(CurrentDateM1))
    & (F.col("FileName").isNull())
    & (F.substring(trim(F.col("COMM_FILE_NM")), 1, 2) == F.lit("AH"))
)
df_xfm_Required_fields_missing = df_xfm_Required_fields.filter(constraint_missing).select(
    F.col("COMM_ID").alias("COMM_ID"),
    F.col("COMM_FILE_NM").alias("COMM_FILE_NM"),
    F.col("UM_REF_ID").alias("UM_REF_ID"),
    F.col("CMPLNT_ID").alias("CMPLNT_ID"),
    F.col("CRT_DTM").alias("CRT_DTM")
)

# --------------------------------------------------------------------------------
# Stage: Seq_MailExtract (PxSequentialFile) - write df_xfm_Required_fields_lnk_xfm
# --------------------------------------------------------------------------------
# Delimiter '|', header = True
df_Seq_MailExtract = df_xfm_Required_fields_lnk_xfm.select(
    "LetterQueueId",
    "FileName",
    "TemplateName",
    "CreatedBy",
    "CreatedOn",
    "AuthorizationUniqueIdentifier",
    "AuthorizationNumber",
    "SubscriberPrimaryIdentifier",
    "SubscriberSecondaryIdentifier",
    "SubscriberFirstName",
    "SubscriberMiddleName",
    "SubscriberDOB",
    "SubscriberLastName",
    "SubscriberGender",
    "SubscriberEthnicity",
    "SubscriberLineOfBusiness",
    "SubscriberBenefitPlan",
    "SubscriberEligibilityStartDate",
    "SubscriberEligibilityEndDate",
    "SubscriberAddressLine1",
    "SubscriberAddressLine2",
    "SubscriberAddressLine3",
    "SubscriberCity",
    "SubscriberState",
    "SubscriberCounty",
    "SubscriberCountry",
    "SubscriberZip",
    "SubscriberAddressType",
    "ProviderPrimaryIdentifier",
    "ProviderSecondaryIdentifier",
    "ProviderFirstName",
    "ProviderLastName",
    "ProviderMiddleName",
    "ProviderName",
    "ProviderEthnicity",
    "ProviderAddressLine1",
    "ProviderAddressLine2",
    "ProviderAddressLine3",
    "ProviderCity",
    "ProviderState",
    "ProviderZip",
    "ProviderCountyDescription",
    "ProviderOfficePhone",
    "ProviderOfficePhoneOveride",
    "ProviderFax",
    "ProviderFaxOverride",
    "ISProviderPrimaryAddress",
    "ProviderEmail",
    "ProviderAddressType",
    "ProviderAuthorizationType",
    "ColorCode",
    "CostCenterID",
    "EnvelopeTypeID",
    "BRETypeID",
    "InsertTypeID"
)
write_files(
    df_Seq_MailExtract,
    f"{adls_path_publish}/external/{NXTPGOutboundFile}_{RunID}.txt",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote=None,
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: MailExtract (PxSequentialFile) - write df_xfm_Required_fields_mail
# --------------------------------------------------------------------------------
df_MailExtract = df_xfm_Required_fields_mail.select(
    "COMM_SK",
    "COMM_ID",
    "COMM_SRC_SYS_CD",
    "COMM_TRGT_SYS_CD",
    "COMM_STTUS_CD",
    "COMM_TYP_CD",
    "COMM_STTUS_CMNT_TX",
    "LAST_UPDT_DTM"
)
write_files(
    df_MailExtract,
    f"{adls_path_publish}/external/GuidingCare_MailExtrct_Update.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Sequential_File_42 (PxSequentialFile) - write df_xfm_Required_fields_missing
# --------------------------------------------------------------------------------
df_Sequential_File_42 = df_xfm_Required_fields_missing.select(
    "COMM_ID",
    "COMM_FILE_NM",
    "UM_REF_ID",
    "CMPLNT_ID",
    "CRT_DTM"
)
write_files(
    df_Sequential_File_42,
    f"{adls_path_publish}/external/NextPageMissingPDFLetters.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)