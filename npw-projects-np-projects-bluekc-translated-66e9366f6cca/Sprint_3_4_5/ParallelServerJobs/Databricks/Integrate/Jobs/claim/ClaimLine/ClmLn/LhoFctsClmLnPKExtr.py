# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008, 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmLnPKExtr
# MAGIC CALLED BY:  FctsClmPrereqSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  This job must be run after FctsClmDriverBuild
# MAGIC          *  UNIX file K_CLM_LN.dat is removed in Before-job to remove previous keys incase of earlier abend.  File writting is set to append.  
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Included are any adjusted to/from claims that will be foreign keyed later. 
# MAGIC          *  The primary key hash file hf_clm_ln is the output of this job and is used by the following tables for keying
# MAGIC              CLM_LN
# MAGIC              CLM_LN_PCA
# MAGIC              CLM_LN_REMIT
# MAGIC              CLM_LN_CLNCL_EDIT
# MAGIC 
# MAGIC 2;hf_clm_ln_pk_lkup;hf_clm_pk_adj_tofrom
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                  \(9)Date               \(9)Project/Altiris #                       \(9)Change Description\(9)\(9)\(9)\(9)Development Project      \(9)Code Reviewer     \(9)Date Reviewed       
# MAGIC ==============================================================================================================================================================================
# MAGIC Manasa Andru   \(9)2020-08-10         \(9)US -  263702                        \(9)Original Programming                               \(9)\(9)IntegrateDev2                   \(9)Jaideep Mankala  \(9)10/09/2020
# MAGIC Prabhu ES      \(9)2022-03-17           \(9)S2S                                        \(9)MSSQL ODBC conn params added        \(9)\(9)IntegrateDev5\(9)\(9)Ken Bradmon\(9)2022-06-11

# MAGIC Get SK for primary key on input record
# MAGIC hf_clm_ln hash file used to key tables
# MAGIC CLM_LN
# MAGIC CLM_LN_PCA
# MAGIC CLM_LN_REMIT
# MAGIC CLM_LN_CLNCL_EDIT
# MAGIC Facets Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Dental Lines
# MAGIC Medical Lines
# MAGIC Key adj-to and adj-from claim IDs on claims above
# MAGIC Check is adjusted from or to claim was keyed above
# MAGIC Get SK for records with out keys
# MAGIC Eliminate dups from input sources
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, lit, when
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
DriverTable = get_widget_value('DriverTable','')
AdjFromTable = get_widget_value('AdjFromTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnLoadPK
# COMMAND ----------
jdbc_url, jdbc_props = get_db_config(<...>)

df_CMC_CDDL_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
RTrim(TMP.CLM_ID) AS CLM_ID,
CL.CDDL_SEQ_NO,
TMP.CLM_STS,
TMP.CLCL_PAID_DT,
TMP.CLCL_ID_ADJ_TO,
TMP.CLCL_ID_ADJ_FROM
FROM {LhoFacetsStgOwner}.CMC_CDDL_CL_LINE CL,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = CL.CLCL_ID
"""
    )
    .load()
)

df_DntlClms = df_CMC_CDDL_CL_LINE

df_RDntl_stage = df_DntlClms.filter(
    ((trim(col("CLM_STS")) == '91') | (trim(col("CLM_STS")) == '89'))
)

df_RDntl = (
    df_RDntl_stage.withColumn("CLM_ID", trim(col("CLM_ID")).cast(StringType()))
    .withColumn("CLM_ID", col("CLM_ID") + lit("R"))
    .withColumn("CLM_LN_SEQ_NO", col("CDDL_SEQ_NO"))
    .select("CLM_ID", "CLM_LN_SEQ_NO")
)

df_Dntl = (
    df_DntlClms
    .withColumn("CLM_ID", col("CLM_ID"))
    .withColumn("CLM_LN_SEQ_NO", col("CDDL_SEQ_NO"))
    .select("CLM_ID", "CLM_LN_SEQ_NO")
)

df_lnk_clm_ln_pk_dntl_hold = dedup_sort(df_Dntl, ["CLM_ID","CLM_LN_SEQ_NO"], [])
df_lnk_clm_ln_pk_rdntl_hold = dedup_sort(df_RDntl, ["CLM_ID","CLM_LN_SEQ_NO"], [])

df_CMC_CDML_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
RTrim(TMP.CLM_ID) as CLM_ID,
CL.CDML_SEQ_NO,
TMP.CLM_STS,
TMP.CLCL_PAID_DT,
TMP.CLCL_ID_ADJ_TO,
TMP.CLCL_ID_ADJ_FROM
FROM {LhoFacetsStgOwner}.CMC_CDML_CL_LINE CL,
     tempdb..{DriverTable} TMP
WHERE TMP.CLM_ID = CL.CLCL_ID
"""
    )
    .load()
)

df_MedClms = df_CMC_CDML_CL_LINE

df_RMed_stage = df_MedClms.filter(
    ((trim(col("CLM_STS")) == '91') | (trim(col("CLM_STS")) == '89'))
)

df_RMed = (
    df_RMed_stage.withColumn("CLM_ID", trim(col("CLM_ID")).cast(StringType()))
    .withColumn("CLM_ID", col("CLM_ID") + lit("R"))
    .withColumn("CLM_LN_SEQ_NO", col("CDML_SEQ_NO"))
    .select("CLM_ID", "CLM_LN_SEQ_NO")
)

df_Med = (
    df_MedClms
    .withColumn("CLM_ID", col("CLM_ID"))
    .withColumn("CLM_LN_SEQ_NO", col("CDML_SEQ_NO"))
    .select("CLM_ID", "CLM_LN_SEQ_NO")
)

df_lnk_clm_ln_pk_med_hold = dedup_sort(df_Med, ["CLM_ID","CLM_LN_SEQ_NO"], [])
df_lnk_clm_ln_pk_rmed_hold = dedup_sort(df_RMed, ["CLM_ID","CLM_LN_SEQ_NO"], [])

df_Collector = (
    df_lnk_clm_ln_pk_dntl_hold
    .unionByName(df_lnk_clm_ln_pk_rdntl_hold)
    .unionByName(df_lnk_clm_ln_pk_med_hold)
    .unionByName(df_lnk_clm_ln_pk_rmed_hold)
)

df_All_Clms = df_Collector

df_Transform = (
    df_All_Clms
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSK).cast(IntegerType()))
    .withColumn("CLM_ID", trim(col("CLM_ID")))
    .withColumn("CLM_LN_SEQ_NO", col("CLM_LN_SEQ_NO"))
    .select("SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO")
)

params_ClmLnLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_PK_Clms = ClmLnLoadPK(df_Transform, params_ClmLnLoadPK)

df_PK_Clms.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", "dummy_hf_clm_ln_pk_lkup").mode("append").save()

df_DntlAdj = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
RTrim(CLM.CLCL_ID) as CLCL_ID,
CL.CDDL_SEQ_NO,
CLM.CLCL_CUR_STS,
CLM.CLCL_PAID_DT,
CLM.CLCL_LAST_ACT_DTM
FROM
     tempdb..{AdjFromTable} DRVR,
     {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLM,
     {LhoFacetsStgOwner}.CMC_CDDL_CL_LINE CL
WHERE CLM.CLCL_ID   = DRVR.CLM_ID
  AND DRVR.CLM_ID   = CL.CLCL_ID
"""
    )
    .load()
)

df_lnk_clm_ln_pk_denadj_hold = dedup_sort(df_DntlAdj, ["CLCL_ID","CDDL_SEQ_NO","CLCL_CUR_STS","CLCL_PAID_DT","CLCL_LAST_ACT_DTM"], [])

df_MedAdj = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
RTrim(CLM.CLCL_ID) AS CLCL_ID,
CL.CDDL_SEQ_NO,
CLM.CLCL_CUR_STS,
CLM.CLCL_PAID_DT,
CLM.CLCL_LAST_ACT_DTM
FROM
     tempdb..{AdjFromTable} DRVR,
     {LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLM,
     {LhoFacetsStgOwner}.CMC_CDDL_CL_LINE CL
WHERE CLM.CLCL_ID   = DRVR.CLM_ID
  AND DRVR.CLM_ID   = CL.CLCL_ID
"""
    )
    .load()
)

df_lnk_clm_ln_pk_medadj_hold = dedup_sort(df_MedAdj, ["CLCL_ID","CDDL_SEQ_NO","CLCL_CUR_STS","CLCL_PAID_DT","CLCL_LAST_ACT_DTM"], [])

df_Collector2 = df_lnk_clm_ln_pk_denadj_hold.unionByName(df_lnk_clm_ln_pk_medadj_hold)
df_Adj = df_Collector2

df_Adj_r_stage = df_Adj.filter(
    ((trim(col("CLCL_CUR_STS")) == '91') | (trim(col("CLCL_CUR_STS")) == '89'))
)

df_Radj = (
    df_Adj_r_stage.withColumn("CLM_ID", trim(col("CLCL_ID")).cast(StringType()))
    .withColumn("CLM_ID", col("CLM_ID") + lit("R"))
    .withColumn("CLM_LN_SEQ_NO", col("CDDL_SEQ_NO"))
    .select("CLM_ID", "CLM_LN_SEQ_NO")
)

df_Oadj = (
    df_Adj
    .withColumn("CLM_ID", trim(col("CLCL_ID")).cast(StringType()))
    .withColumn("CLM_LN_SEQ_NO", col("CDDL_SEQ_NO"))
    .select("CLM_ID", "CLM_LN_SEQ_NO")
)

df_lnk_clm_ln_pk_radj_hold = dedup_sort(df_Radj, ["CLM_ID","CLM_LN_SEQ_NO"], [])
df_lnk_clm_ln_pk_oadj_hold = dedup_sort(df_Oadj, ["CLM_ID","CLM_LN_SEQ_NO"], [])

df_Collector3 = df_lnk_clm_ln_pk_radj_hold.unionByName(df_lnk_clm_ln_pk_oadj_hold)
df_Colllection = df_Collector3

df_lnk_clm_pk_adj_tofrom = dedup_sort(df_Colllection, ["CLM_ID","CLM_LN_SEQ_NO"], [])
df_Adjust = df_lnk_clm_pk_adj_tofrom

df_hf_clm_ln_pk_lkup_read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT SRC_SYS_CD, CLM_ID, CLM_LN_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, CLM_LN_SK FROM dummy_hf_clm_ln_pk_lkup"
    )
    .load()
)

df_Adjust_alias = df_Adjust.alias("Adjust")
df_Lkup_alias = df_hf_clm_ln_pk_lkup_read.alias("Lkup")

df_Trans4_join = df_Adjust_alias.join(
    df_Lkup_alias,
    (
        (col("Lkup.SRC_SYS_CD") == SrcSysCd)
        & (col("Adjust.CLM_ID") == col("Lkup.CLM_ID"))
        & (col("Adjust.CLM_LN_SEQ_NO") == col("Lkup.CLM_LN_SEQ_NO"))
    ),
    "left"
)

df_NewAdjClms = df_Trans4_join.filter(col("Lkup.CLM_LN_SK").isNull()).select(
    lit(SrcSysCdSK).cast(IntegerType()).alias("SRC_SYS_CD_SK"),
    col("Adjust.CLM_ID").alias("CLM_ID"),
    col("Adjust.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO")
)

params_ClmLnLoadPK2 = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_Not_Used = ClmLnLoadPK(df_NewAdjClms, params_ClmLnLoadPK2)

df_Not_Used.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", "dummy_hf_clm_ln_pk_lkup").mode("append").save()

df_final = df_Not_Used.select(
    rpad(col("SRC_SYS_CD"),12," ").alias("SRC_SYS_CD"),
    rpad(col("CLM_ID"),12," ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("CLM_LN_SK")
)

write_files(
    df_final,
    "hf_clm_ln_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)