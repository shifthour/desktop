# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2007 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Job Name:  FctsAplLvlSttusExtr
# MAGIC Called by:      FctsAplExtrSeq
# MAGIC 
# MAGIC Processing:  IDS Appeals Level Status Facets extract
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC              Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC              Previous Run Aborted:         Restart, no other steps neccessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                               Project/                                                                                                                  Development                  Code                  Date
# MAGIC Developer        Date              Altiris #           Change Description                                                                         Environment                   Reviewer            Reviewed
# MAGIC ----------------------  -------------------   -------------------   ------------------------------------------------------------------------------------                    ----------------------------------      ----------------------     -------------------   
# MAGIC Hugh Sisson    07/24/2007   3028              Original program                                                                                                                       Steph Goddard   9/6/07
# MAGIC Bhoomi Dasari 10/18/2007   3028              Added Balancing Snapshot 
# MAGIC Kalyan Neelam  2014-04-02   TFS 3974      Updated logic for field APST_USID_ROUTE                                                                          Bhoomi Dasari     4/9/2014
# MAGIC                                                                     to trim the field for Null check in BusinessRules transformer
# MAGIC 
# MAGIC Jag Yelavarthi    2015-01-19   TFS#8431    Changed "IDS_SK" to "Table_Name_SK"                                   IntegrateNewDevl            Kalyan Neelam       2015-01-27
# MAGIC 
# MAGIC Ravi Singh          2018-10- 18                    Added new shared container instead of                      MTM-5841       IntegrateDev2\(9)Abhiram Dasarathy\(9)2018-10-30 
# MAGIC                                                                   transformer stage 
# MAGIC Prabhu ES           2022-02-25    S2S           MSSQL connection parameters added                                               IntegrateDev5\(9)Ken Bradmon\(9)2022-05-19

# MAGIC Balancing Snapshot
# MAGIC Trim and Strip Carriage Return, Line Feed, and Tab.
# MAGIC Extract Facets Appeal Level Status Data
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing sequential file to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, length, upper, lit, concat, regexp_replace, rpad
from pyspark.sql.functions import trim as spark_trim
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/AplLvlSttusPkey
# COMMAND ----------

# Parameter Retrieval
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','40')
RunID = get_widget_value('RunID','200708031020')
RunDate = get_widget_value('RunDate','2007-10-29')
BeginDate = get_widget_value('BeginDate','2000-01-01')
EndDate = get_widget_value('EndDate','2007-10-29')

# STAGE: FACETS (ODBCConnector)
jdbc_url_FACETS, jdbc_props_FACETS = get_db_config(facets_secret_name)
extract_query_FACETS = f"""
SELECT 
CAS.APAP_ID,
CAS.APLV_SEQ_NO,
CAS.APST_SEQ_NO,
CAS.APST_STS,
CAS.APST_STS_DTM,
CAS.APST_MCTR_REAS,
CAS.APST_USID,
CAS.APST_USID_ROUTE
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APST_STATUS CAS,
{FacetsOwner}.CMC_APLV_APP_LEVEL CAAL
WHERE
CAAL.APAP_ID = APAP.APAP_ID
AND CAAL.APAP_ID = CAS.APAP_ID
AND CAAL.APLV_SEQ_NO = CAS.APLV_SEQ_NO
AND (
       ( APAP.APAP_LAST_UPD_DTM >= '{BeginDate}' AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}' )
    OR ( CAAL.APLV_LAST_UPD_DTM >= '{BeginDate}' AND CAAL.APLV_LAST_UPD_DTM <= '{EndDate}' )
)
"""
df_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_FACETS)
    .options(**jdbc_props_FACETS)
    .option("query", extract_query_FACETS)
    .load()
)

# STAGE: StripF (CTransformerStage)
df_StripF = df_FACETS
df_StripF = df_StripF.withColumn(
    "APAP_ID",
    when(
        col("APAP_ID").isNull() | (length(spark_trim(regexp_replace(col("APAP_ID"), "[\\r\\n\\t]+", ""))) == 0),
        ""
    ).otherwise(upper(spark_trim(regexp_replace(col("APAP_ID"), "[\\r\\n\\t]+", ""))))
)
df_StripF = df_StripF.withColumn("APLV_SEQ_NO", col("APLV_SEQ_NO"))
df_StripF = df_StripF.withColumn("APST_SEQ_NO", col("APST_SEQ_NO"))
df_StripF = df_StripF.withColumn(
    "APST_STS",
    when(
        col("APST_STS").isNull() | (length(spark_trim(regexp_replace(col("APST_STS"), "[\\r\\n\\t]+", ""))) == 0),
        ""
    ).otherwise(upper(spark_trim(regexp_replace(col("APST_STS"), "[\\r\\n\\t]+", ""))))
)
# Assume user-defined function for the FORMAT.DATE(...) call:
df_StripF = df_StripF.withColumn(
    "APST_STS_DTM",
    FORMAT_DATE(col("APST_STS_DTM"), "SYBASE", "TIMESTAMP", "DB2TIMESTAMP")  # user-defined
)
df_StripF = df_StripF.withColumn(
    "APST_MCTR_REAS",
    when(
        col("APST_MCTR_REAS").isNull() | (length(spark_trim(regexp_replace(col("APST_MCTR_REAS"), "[\\r\\n\\t]+", ""))) == 0),
        ""
    ).otherwise(upper(spark_trim(regexp_replace(col("APST_MCTR_REAS"), "[\\r\\n\\t]+", ""))))
)
df_StripF = df_StripF.withColumn(
    "APST_USID",
    when(
        col("APST_USID").isNull() | (length(spark_trim(regexp_replace(col("APST_USID"), "[\\r\\n\\t]+", ""))) == 0),
        ""
    ).otherwise(spark_trim(regexp_replace(col("APST_USID"), "[\\r\\n\\t]+", "")))
)
df_StripF = df_StripF.withColumn(
    "APST_USID_ROUTE",
    when(
        col("APST_USID_ROUTE").isNull() | (length(spark_trim(regexp_replace(col("APST_USID_ROUTE"), "[\\r\\n\\t]+", ""))) == 0),
        ""
    ).otherwise(spark_trim(regexp_replace(col("APST_USID_ROUTE"), "[\\r\\n\\t]+", "")))
)

df_StripF = df_StripF.select(
    "APAP_ID",
    "APLV_SEQ_NO",
    "APST_SEQ_NO",
    "APST_STS",
    "APST_STS_DTM",
    "APST_MCTR_REAS",
    "APST_USID",
    "APST_USID_ROUTE"
)

# STAGE: BusinessRules (CTransformerStage)
df_BusinessRules = df_StripF
df_BusinessRules = df_BusinessRules.withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
df_BusinessRules = df_BusinessRules.withColumn("INSRT_UPDT_CD", lit("I"))
df_BusinessRules = df_BusinessRules.withColumn("DISCARD_IN", lit("N"))
df_BusinessRules = df_BusinessRules.withColumn("PASS_THRU_IN", lit("Y"))
df_BusinessRules = df_BusinessRules.withColumn("FIRST_RECYC_DT", col("RunDate"))  # Will fix below
df_BusinessRules = df_BusinessRules.withColumn("ERR_CT", lit(0))
df_BusinessRules = df_BusinessRules.withColumn("RECYCLE_CT", lit(0))
df_BusinessRules = df_BusinessRules.withColumn("SRC_SYS_CD", lit("FACETS"))
df_BusinessRules = df_BusinessRules.withColumn(
    "PRI_KEY_STRING",
    concat(lit("FACETS"), lit(";"), col("APAP_ID"), lit(";"), col("APLV_SEQ_NO"), lit(";"), col("APST_SEQ_NO"))
)
df_BusinessRules = df_BusinessRules.withColumn("APL_LVL_STTUS_SK", lit(0))
df_BusinessRules = df_BusinessRules.withColumn(
    "APL_ID",
    when(
        col("APAP_ID").isNull() | (length(spark_trim(col("APAP_ID"))) == 0),
        "NA"
    ).otherwise(upper(spark_trim(col("APAP_ID"))))
)
df_BusinessRules = df_BusinessRules.withColumn("APL_LVL_SEQ_NO", col("APLV_SEQ_NO"))
df_BusinessRules = df_BusinessRules.withColumn("SEQ_NO", col("APST_SEQ_NO"))
df_BusinessRules = df_BusinessRules.withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
df_BusinessRules = df_BusinessRules.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
df_BusinessRules = df_BusinessRules.withColumn("APL_LVL_SK", lit(0))
df_BusinessRules = df_BusinessRules.withColumn(
    "APST_USID",
    when(col("APST_USID").isNotNull(), col("APST_USID")).otherwise(lit("NA"))
)
df_BusinessRules = df_BusinessRules.withColumn(
    "APST_USID_ROUTE",
    when(
        (spark_trim(col("APST_USID_ROUTE")).isNull()) | (length(spark_trim(col("APST_USID_ROUTE"))) == 0),
        "NA"
    ).otherwise(col("APST_USID_ROUTE"))
)
df_BusinessRules = df_BusinessRules.withColumn(
    "APL_LVL_STTUS_CD",
    when(
        col("APST_STS").isNull() | (length(spark_trim(col("APST_STS"))) == 0),
        "NA"
    ).otherwise(upper(spark_trim(col("APST_STS"))))
)
df_BusinessRules = df_BusinessRules.withColumn(
    "APL_LVL_STTUS_RSN_CD",
    when(
        col("APST_MCTR_REAS").isNull() | (length(spark_trim(col("APST_MCTR_REAS"))) == 0),
        "NA"
    ).otherwise(upper(spark_trim(col("APST_MCTR_REAS"))))
)
df_BusinessRules = df_BusinessRules.withColumn("STTUS_DTM", col("APST_STS_DTM"))

# Fix the "FIRST_RECYC_DT" to the job parameter RunDate
df_BusinessRules = df_BusinessRules.withColumn("FIRST_RECYC_DT", lit(RunDate))

df_BusinessRules = df_BusinessRules.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "APL_LVL_STTUS_SK",
    "APL_ID",
    "APL_LVL_SEQ_NO",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_LVL_SK",
    "APST_USID",
    "APST_USID_ROUTE",
    "APL_LVL_STTUS_CD",
    "APL_LVL_STTUS_RSN_CD",
    "STTUS_DTM"
)

# STAGE: AplLvlSttusPkey (CContainerStage)
params_container_AplLvlSttusPkey = {
    "CurrRunCycle": CurrRunCycle
}
df_IdsAplLvlSttusExtr = AplLvlSttusPkey(df_BusinessRules, params_container_AplLvlSttusPkey)

# STAGE: IdsAplLvlSttusExtr (CSeqFileStage) - write to file
# Before writing, apply rpad to char/varchar columns
df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr
df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr_final.withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr_final.withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr_final.withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr_final.withColumn("APST_USID", rpad(col("APST_USID"), 10, " "))
df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr_final.withColumn("APST_USID_ROUTE", rpad(col("APST_USID_ROUTE"), 10, " "))
df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr_final.withColumn("APL_LVL_STTUS_CD", rpad(col("APL_LVL_STTUS_CD"), 2, " "))
df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr_final.withColumn("APL_LVL_STTUS_RSN_CD", rpad(col("APL_LVL_STTUS_RSN_CD"), 4, " "))

df_IdsAplLvlSttusExtr_final = df_IdsAplLvlSttusExtr_final.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "APL_LVL_STTUS_SK",
    "APL_ID",
    "APL_LVL_SEQ_NO",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "APL_LVL_SK",
    "APST_USID",
    "APST_USID_ROUTE",
    "APL_LVL_STTUS_CD",
    "APL_LVL_STTUS_RSN_CD",
    "STTUS_DTM"
)

write_files(
    df_IdsAplLvlSttusExtr_final,
    f"{adls_path}/key/FctsAplLvlSttusExtr.AplLvlSttus.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# STAGE: SnapShot_FACETS (ODBCConnector)
jdbc_url_SNAP, jdbc_props_SNAP = get_db_config(facets_secret_name)
extract_query_SNAP = f"""
SELECT 
CAS.APAP_ID,
CAS.APLV_SEQ_NO,
CAS.APST_SEQ_NO,
CAS.APST_STS_DTM
FROM 
{FacetsOwner}.CMC_APAP_APPEALS APAP,
{FacetsOwner}.CMC_APST_STATUS CAS,
{FacetsOwner}.CMC_APLV_APP_LEVEL CAAL
WHERE
CAAL.APAP_ID = APAP.APAP_ID
AND CAAL.APAP_ID = CAS.APAP_ID
AND CAAL.APLV_SEQ_NO = CAS.APLV_SEQ_NO
AND (
       ( APAP.APAP_LAST_UPD_DTM >= '{BeginDate}' AND APAP.APAP_LAST_UPD_DTM <= '{EndDate}' )
    OR ( CAAL.APLV_LAST_UPD_DTM >= '{BeginDate}' AND CAAL.APLV_LAST_UPD_DTM <= '{EndDate}' )
)
"""
df_SnapShot_FACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_SNAP)
    .options(**jdbc_props_SNAP)
    .option("query", extract_query_SNAP)
    .load()
)

# STAGE: Rules (CTransformerStage)
df_Rules = df_SnapShot_FACETS
df_Rules = df_Rules.withColumn(
    "SRC_SYS_CD_SK",
    GetFkeyCodes(lit("IDS"), lit(1), lit("SOURCE SYSTEM"), lit("FACETS"), lit("X"))  # user-defined
)
df_Rules = df_Rules.withColumn(
    "APL_ID",
    spark_trim(regexp_replace(col("APAP_ID"), "[\\r\\n\\t]+", ""))
)
df_Rules = df_Rules.withColumn("APL_LVL_SEQ_NO", col("APLV_SEQ_NO"))
df_Rules = df_Rules.withColumn("SEQ_NO", col("APST_SEQ_NO"))
df_Rules = df_Rules.withColumn(
    "STTUS_DTM",
    FORMAT_DATE(col("APST_STS_DTM"), "SYBASE", "TIMESTAMP", "DB2TIMESTAMP")  # user-defined
)

df_Rules = df_Rules.select(
    "SRC_SYS_CD_SK",
    "APL_ID",
    "APL_LVL_SEQ_NO",
    "SEQ_NO",
    "STTUS_DTM"
)

# STAGE: B_APL_LVL_STTUS (CSeqFileStage) - write to file
# No char columns specified with length, so no rpad needed here
df_B_APL_LVL_STTUS_final = df_Rules.select(
    "SRC_SYS_CD_SK",
    "APL_ID",
    "APL_LVL_SEQ_NO",
    "SEQ_NO",
    "STTUS_DTM"
)

write_files(
    df_B_APL_LVL_STTUS_final,
    f"{adls_path}/load/B_APL_LVL_STTUS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)