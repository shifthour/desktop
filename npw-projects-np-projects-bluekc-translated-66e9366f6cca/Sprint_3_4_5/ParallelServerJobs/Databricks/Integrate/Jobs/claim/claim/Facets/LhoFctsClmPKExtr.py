# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmPKExtr
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after FctsClmDriverBuild
# MAGIC          *  UNIX file K_CLM.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Included are any adjusted to/from claims that will be foreign keyed later. 
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC              CLM_PCA
# MAGIC              CLM_EXTRNL_REF_DATA
# MAGIC              CLM_EXTRNL_MBRSH
# MAGIC              CLM_EXTRNL_PROV
# MAGIC              FCLTY_CLM
# MAGIC              ITS_CLM
# MAGIC              CLM_REMIT_HIST
# MAGIC              CLM_ALT_PAYE
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)              Development Project\(9)           Code Reviewer\(9)              Date Reviewed       
# MAGIC ============================================================================================================================================================== 
# MAGIC Brent Leland\(9)2008-07-25\(9)3567 Primary Key     Original Programming                                                                  devlIDS                            Steph Goddard                           10/03/2008
# MAGIC                                                                                                  
# MAGIC Manasa Andru         2014-02-19              TFS - 1321               Trimmed the CLCL_ID in the Trns1 andTrns4 stage               IntegrateCurDevl                   Bhoomi Dasari                             2/24/2014
# MAGIC  
# MAGIC Prabhu ES               2022-03-29               S2S                          MSSQL ODBC conn params added                                       IntegrateDev5\(9)Ken Bradmon\(9)2022-06-10

# MAGIC Get SK for primary key on input record
# MAGIC hf_clm hash file used to key tables
# MAGIC CLM
# MAGIC CLM_PCA
# MAGIC CLM_EXTRNL_REF_DATA
# MAGIC CLM_EXTRNL_MBRSH
# MAGIC CLM_EXTRNL_PROV
# MAGIC FCLTY_CLM
# MAGIC ITS_CLM
# MAGIC CLM_REMIT_HIST
# MAGIC CLM_ALT_PAYE
# MAGIC Facets Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Check is adjusted from or to claim was keyed above
# MAGIC Key adj-to and adj-from claim IDs on claims above
# MAGIC Get SK for records with out keys
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import lit, when, col, concat, rpad
from pyspark.sql.types import StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
DriverTable = get_widget_value('DriverTable','')
AdjFromTable = get_widget_value('AdjFromTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CommitPoint = get_widget_value('CommitPoint','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstgowner_secret_name = get_widget_value('lhofacetsstgowner_secret_name','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

jdbc_url_TMP_ADJ_CLM, jdbc_props_TMP_ADJ_CLM = get_db_config(lhofacetsstgowner_secret_name)
df_TMP_ADJ_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_TMP_ADJ_CLM)
    .options(**jdbc_props_TMP_ADJ_CLM)
    .option(
        "query",
        f"""
SELECT 
  CLM.CLCL_ID,
  CLM.CLCL_CUR_STS,
  CLM.CLCL_PAID_DT,
  CLM.CLCL_LAST_ACT_DTM
FROM 
  tempdb..{AdjFromTable} DRVR,
  {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLM
WHERE 
  CLM.CLCL_ID = DRVR.CLM_ID
"""
    )
    .load()
)

df_TMP_ADJ_CLM1 = df_TMP_ADJ_CLM.withColumn(
    "svIsRvrsl",
    when(
        (trim(col("CLCL_CUR_STS")) == "91") | (trim(col("CLCL_CUR_STS")) == "89"),
        True
    ).otherwise(False)
)

df_Radj = (
    df_TMP_ADJ_CLM1.filter(col("svIsRvrsl") == True)
    .withColumn("CLM_ID", concat(trim(col("CLCL_ID")), lit("R")))
    .select("CLM_ID")
)
df_Adj = (
    df_TMP_ADJ_CLM1.filter(col("svIsRvrsl") == False)
    .withColumn("CLM_ID", trim(col("CLCL_ID")))
    .select("CLM_ID")
)

df_Collector2 = df_Radj.select("CLM_ID").unionByName(df_Adj.select("CLM_ID"))
df_Collector2 = df_Collector2.withColumn("CLM_ID", rpad(col("CLM_ID"), 12, " "))

jdbc_url_TMP_IDS_CLAIM, jdbc_props_TMP_IDS_CLAIM = get_db_config(lhofacetsstgowner_secret_name)
df_TMP_IDS_CLAIM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_TMP_IDS_CLAIM)
    .options(**jdbc_props_TMP_IDS_CLAIM)
    .option("dbtable", f"tempdb..{DriverTable}")
    .load()
)

df_TMP_IDS_CLAIM1 = df_TMP_IDS_CLAIM.withColumn(
    "svIsReversal",
    when(
        (trim(col("CLM_STS")) == "91") | (trim(col("CLM_STS")) == "89"),
        True
    ).otherwise(False)
)

df_Rclaim = (
    df_TMP_IDS_CLAIM1.filter(col("svIsReversal") == True)
    .withColumn("CLM_ID", concat(trim(col("CLM_ID")), lit("R")))
    .select("CLM_ID")
)
df_Claims = (
    df_TMP_IDS_CLAIM1.filter(col("svIsReversal") == False)
    .withColumn("CLM_ID", trim(col("CLM_ID")))
    .select("CLM_ID")
)

df_Collector = df_Claims.select("CLM_ID").unionByName(df_Rclaim.select("CLM_ID"))
df_Collector = df_Collector.withColumn("CLM_ID", rpad(col("CLM_ID"), 12, " "))

df_Trans2 = (
    df_Collector
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSK))
    .withColumn("CLM_ID", col("CLM_ID"))
    .select("SRC_SYS_CD_SK", "CLM_ID")
)

params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_hf_clm_pk_lkup = ClmLoadPK(df_Trans2, params_ClmLoadPK)

jdbc_url_ids_for_hf, jdbc_props_ids_for_hf = get_db_config(ids_secret_name)
execute_dml(f"DROP TABLE IF EXISTS STAGING.LhoFctsClmPKExtr_hf_clm_pk_lkup_temp", jdbc_url_ids_for_hf, jdbc_props_ids_for_hf)
df_hf_clm_pk_lkup.select("SRC_SYS_CD","CLM_ID","CRT_RUN_CYC_EXCTN_SK","CLM_SK").write \
    .format("jdbc") \
    .option("url", jdbc_url_ids_for_hf) \
    .options(**jdbc_props_ids_for_hf) \
    .option("dbtable", "STAGING.LhoFctsClmPKExtr_hf_clm_pk_lkup_temp") \
    .mode("overwrite") \
    .save()

merge_sql_1 = f"""
MERGE IDS.dummy_hf_clm_pk_lkup AS T
USING STAGING.LhoFctsClmPKExtr_hf_clm_pk_lkup_temp AS S
ON (T.SRC_SYS_CD=S.SRC_SYS_CD AND T.CLM_ID=S.CLM_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK,
    T.CLM_SK=S.CLM_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, CLM_ID, CRT_RUN_CYC_EXCTN_SK, CLM_SK)
  VALUES (S.SRC_SYS_CD, S.CLM_ID, S.CRT_RUN_CYC_EXCTN_SK, S.CLM_SK);
"""
execute_dml(merge_sql_1, jdbc_url_ids_for_hf, jdbc_props_ids_for_hf)

df_hf_clm_pk_lkup_read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_for_hf)
    .options(**jdbc_props_ids_for_hf)
    .option("query", "SELECT SRC_SYS_CD, CLM_ID, CRT_RUN_CYC_EXCTN_SK, CLM_SK FROM IDS.dummy_hf_clm_pk_lkup")
    .load()
)

df_Collector2_with_srcsys = df_Collector2.withColumn("SrcSysCd", lit(SrcSysCd))
df_Trans4_join = df_Collector2_with_srcsys.alias("Adjust").join(
    df_hf_clm_pk_lkup_read.alias("Lkup"),
    (col("Adjust.SrcSysCd") == col("Lkup.SRC_SYS_CD")) & (col("Adjust.CLM_ID") == col("Lkup.CLM_ID")),
    "left"
)

df_NewAdjClms = (
    df_Trans4_join
    .filter(col("Lkup.CLM_SK").isNull())
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSK))
    .withColumn("CLM_ID", col("Adjust.CLM_ID"))
    .select("SRC_SYS_CD_SK", "CLM_ID")
)

params_ClmLoadPK2 = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_hf_clm_pk_lkup2 = ClmLoadPK(df_NewAdjClms, params_ClmLoadPK2)

execute_dml(f"DROP TABLE IF EXISTS STAGING.LhoFctsClmPKExtr_hf_clm_pk_lkup2_temp", jdbc_url_ids_for_hf, jdbc_props_ids_for_hf)
df_hf_clm_pk_lkup2.select("SRC_SYS_CD","CLM_ID","CRT_RUN_CYC_EXCTN_SK","CLM_SK").write \
    .format("jdbc") \
    .option("url", jdbc_url_ids_for_hf) \
    .options(**jdbc_props_ids_for_hf) \
    .option("dbtable", "STAGING.LhoFctsClmPKExtr_hf_clm_pk_lkup2_temp") \
    .mode("overwrite") \
    .save()

merge_sql_2 = f"""
MERGE IDS.dummy_hf_clm_pk_lkup AS T
USING STAGING.LhoFctsClmPKExtr_hf_clm_pk_lkup2_temp AS S
ON (T.SRC_SYS_CD=S.SRC_SYS_CD AND T.CLM_ID=S.CLM_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK,
    T.CLM_SK=S.CLM_SK
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, CLM_ID, CRT_RUN_CYC_EXCTN_SK, CLM_SK)
  VALUES (S.SRC_SYS_CD, S.CLM_ID, S.CRT_RUN_CYC_EXCTN_SK, S.CLM_SK);
"""
execute_dml(merge_sql_2, jdbc_url_ids_for_hf, jdbc_props_ids_for_hf)