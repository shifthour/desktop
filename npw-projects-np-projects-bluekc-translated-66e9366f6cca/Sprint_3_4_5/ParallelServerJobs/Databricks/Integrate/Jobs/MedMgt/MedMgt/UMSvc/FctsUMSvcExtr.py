# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     FctsUMSvcExtr
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_UMAC_ACTIVITY to a landing file for the IDS
# MAGIC INPUTS:	CMC_UMAC_ACTIVITY
# MAGIC HASH FILES:  hf_um_diag_set
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                            FORMAT.DATE
# MAGIC pROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                               Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                         ----------------------------------              ---------------------------------               -------------------------
# MAGIC  2006-02-22             Ralph Tucker           Original Programming.
# MAGIC  
# MAGIC Parik                               04/12/2007              3264                            Added Balancing process to the overall job that takes      devlIDS30                             Steph Goddard                       9/14/07
# MAGIC                                                                                                               a snapshot of the source data   
# MAGIC O. Nielsen                        07/29/2008           Facets 4.5.1                 Changed UMSV_IDCD_ID_PRID to VarChar(10)               devlIDSnew                          Steph Goddard                       08/20/2008
# MAGIC                                                                                                             throughout all transforms                                            
# MAGIC 
# MAGIC Bhoomi D                       03/19/2008             3808                             Made change to the Pkey hash file so that it represents    devlIDS                                 Steph Goddard                       03/30/2009
# MAGIC                                                                                                             correct Primary key instead of UM_REF_SK  
# MAGIC  
# MAGIC Bhoomi Dasari                04/09/2009          3808                               Added SrcSysCdSk to balancing snapshot                              devlIDS                            Steph Goddard                       04/10/2009
# MAGIC 
# MAGIC Rick Henry                     2012-05-13           4896                               Added Diag_Cd_Typ_Cd, Proc_Cd_Typ_cd                        NewDevl                            SAndrew                                 2012-05-20
# MAGIC                                                                                                            and Proc_Cd_Cat_Cd
# MAGIC Raja Gummadi                2013-01-10          TTR-1402,1413               Changed logic for UMSV_IDCD_ID_PRI,IPCD_ID to         IntegrateNewDevl                Bhoomi Dasari                       2/25/2013
# MAGIC                                                                                                         'NA' for Nulls   
# MAGIC Raja Gummadi               2013-05-23           TTR-1518                       Changed Proc Cd lookup for Snapshot                                IntegrateNewDevl               Bhoomi Dasari                        6/4/2013
# MAGIC 
# MAGIC  Akhila M                       10/21/2016             5628-                     Exclusion Criteria- P_SEL_PRCS_CRITR                                    IntegrateDev2                Kalyan Neelam                        2016-11-09
# MAGIC                                                              WorkersComp                 to remove wokers comp GRGR_ID's  
# MAGIC Prabhu ES                     2022-03-07         S2S Remediation            MSSQL ODBC conn params added                                        IntegrateDev5		Harsha Ravuri		06-14-2022
# MAGIC 
# MAGIC Harikrishnarao Yadav     2024-01-30        US 610136                   Added field UMSV_MCTR_RIND from Source                          IntegrateDev2                 Jeyaprasanna                         2024-02-09
# MAGIC                                                                                                       toTarget

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Assign primary surrogate key
# MAGIC Extract Facets Data
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, when, length, lit, substring, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

FacetsOwner = get_widget_value("FacetsOwner","")
facets_secret_name = get_widget_value("facets_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
BCBSOwner = get_widget_value("BCBSOwner","")
bcbs_secret_name = get_widget_value("bcbs_secret_name","")
TmpOutFile = get_widget_value("TmpOutFile","IdsUmSvcExtr.dat.pkey")
CurrRunCycle = get_widget_value("CurrRunCycle","")
DriverTable = get_widget_value("DriverTable","TMP_IDS_UM")
RunID = get_widget_value("RunID","")
CurrDate = get_widget_value("CurrDate","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)

# --------------------------------------------------------------------------------
# SCENARIO B: Hashed File "hf_lkup" (hf_um_svc) - treat as dummy table "dummy_hf_um_svc"
# Read from dummy table
df_hf_lkup = (
    spark.read.format("jdbc")
        .option("url", jdbc_url_ids)
        .options(**jdbc_props_ids)
        .option("query", "SELECT SRC_SYS_CD, UM_REF_ID, UM_SVC_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_SVC_SK FROM IDS.dummy_hf_um_svc")
        .load()
)

# --------------------------------------------------------------------------------
# Stage: PROC_CD (DB2Connector from IDS)
extract_query_proc_cd = f"""
SELECT PROC_CD,
       PROC_CD_TYP_CD,
       PROC_CD_CAT_CD
FROM {IDSOwner}.PROC_CD
WHERE PROC_CD_CAT_CD = 'MED'
"""
df_PROC_CD = (
    spark.read.format("jdbc")
        .option("url", jdbc_url_ids)
        .options(**jdbc_props_ids)
        .option("query", extract_query_proc_cd)
        .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_UmSvc_ProcCd (Scenario A – intermediate hashed file)
# Key columns: [PROC_CD]
# Deduplicate on PROC_CD
df_hf_UmSvc_ProcCd_raw = df_PROC_CD
df_hf_UmSvc_ProcCd = dedup_sort(
    df_hf_UmSvc_ProcCd_raw,
    partition_cols=["PROC_CD"],
    sort_cols=[("PROC_CD", "A")]
)

# --------------------------------------------------------------------------------
# Stage: CD_MPPG (DB2Connector from IDS)
extract_query_cd_mppg = f"""
SELECT SRC_CD,
       TRGT_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
AND TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
AND SRC_CLCTN_CD = 'FACETS DBO'
"""
df_CD_MPPG = (
    spark.read.format("jdbc")
        .option("url", jdbc_url_ids)
        .options(**jdbc_props_ids)
        .option("query", extract_query_cd_mppg)
        .load()
)

# --------------------------------------------------------------------------------
# Stage: hf_umsvc_cd_mppng_trgt_cd (Scenario A – intermediate hashed file)
# Key columns: [SRC_CD]
df_hf_umsvc_cd_mppng_trgt_cd_raw = df_CD_MPPG
df_hf_umsvc_cd_mppng_trgt_cd = dedup_sort(
    df_hf_umsvc_cd_mppng_trgt_cd_raw,
    partition_cols=["SRC_CD"],
    sort_cols=[("SRC_CD", "A")]
)

# --------------------------------------------------------------------------------
# Stage: CMC_UMAC_ACTIVITY (ODBCConnector)
extract_query_cmc_umac = f"""
SELECT
    SVC.UMUM_REF_ID,
    SVC.UMSV_SEQ_NO,
    SVC.MEME_CK,
    SVC.UMSV_AUTH_IND,
    SVC.UMSV_REF_IND,
    SVC.UMSV_TYPE,
    SVC.UMSV_CAT,
    SVC.UMSV_INPUT_USID,
    SVC.UMSV_INPUT_DT,
    SVC.UMSV_RECD_DT,
    SVC.UMSV_NEXT_REV_DT,
    SVC.UMSV_AUTH_DT,
    SVC.UMVT_STS,
    SVC.UMVT_SEQ_NO,
    SVC.UMSV_FROM_DT,
    SVC.UMSV_TO_DT,
    SVC.UMSV_PRPR_ID_REQ,
    SVC.UMSV_PRPR_ID_SVC,
    SVC.UMSV_PRPR_ID_FAC,
    SVC.UMSV_PRPR_ID_PCP,
    SVC.UMSV_IDCD_ID_PRI,
    SVC.UMSV_PSCD_ID_REQ,
    SVC.UMSV_PSCD_ID_AUTH,
    SVC.UMSV_PSCD_POS_AUTH,
    SVC.SESE_ID,
    SVC.SESE_RULE,
    SVC.SEGR_ID,
    SVC.IPCD_ID,
    SVC.UMSV_UNITS_REQ,
    SVC.UMSV_UNITS_AUTH,
    SVC.UMSV_DISALL_EXCD,
    SVC.UMSV_MCTR_LDNY,
    SVC.UMSV_ME_AGE,
    SVC.UMSV_AMT_ALLOW,
    SVC.UMSV_UNITS_ALLOW,
    SVC.UMSV_AMT_PAID,
    SVC.UMSV_UNITS_PAID,
    SVC.UMSV_DENY_DT,
    SVC.ATXR_SOURCE_ID,
    SVC.UMSV_MCTR_RIND,
    MGT.UMUM_ICD_IND_PROC
FROM {FacetsOwner}.CMC_UMSV_SERVICES SVC,
     tempdb..{DriverTable} AS T,
     {FacetsOwner}.CMC_UMUM_UTIL_MGT MGT
WHERE SVC.UMUM_REF_ID = T.UM_REF_ID
  AND SVC.UMUM_REF_ID = MGT.UMUM_REF_ID
  AND NOT EXISTS (
    SELECT DISTINCT 'Y'
    FROM {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
         {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
    WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
      AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
      AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
      AND CMC_GRGR.GRGR_CK=MGT.GRGR_CK
)
"""
df_CMC_UMAC_ACTIVITY = (
    spark.read.format("jdbc")
        .option("url", jdbc_url_facets)
        .options(**jdbc_props_facets)
        .option("query", extract_query_cmc_umac)
        .load()
)

# --------------------------------------------------------------------------------
# Stage: StripField (CTransformerStage)
df_strip = df_CMC_UMAC_ACTIVITY.select(
    strip_field(col("UMUM_REF_ID")).alias("UMUM_REF_ID"),
    col("UMSV_SEQ_NO").alias("UMSV_SEQ_NO"),
    col("MEME_CK").alias("MEME_CK"),
    strip_field(col("UMSV_AUTH_IND")).alias("UMSV_AUTH_IND"),
    strip_field(col("UMSV_REF_IND")).alias("UMSV_REF_IND"),
    strip_field(col("UMSV_TYPE")).alias("UMSV_TYPE"),
    strip_field(col("UMSV_CAT")).alias("UMSV_CAT"),
    strip_field(col("UMSV_INPUT_USID")).alias("UMSV_INPUT_USID"),
    col("UMSV_INPUT_DT").alias("UMSV_INPUT_DT"),
    col("UMSV_RECD_DT").alias("UMSV_RECD_DT"),
    col("UMSV_NEXT_REV_DT").alias("UMSV_NEXT_REV_DT"),
    col("UMSV_AUTH_DT").alias("UMSV_AUTH_DT"),
    strip_field(col("UMVT_STS")).alias("UMVT_STS"),
    col("UMVT_SEQ_NO").alias("UMVT_SEQ_NO"),
    col("UMSV_FROM_DT").alias("UMSV_FROM_DT"),
    col("UMSV_TO_DT").alias("UMSV_TO_DT"),
    strip_field(col("UMSV_PRPR_ID_REQ")).alias("UMSV_PRPR_ID_REQ"),
    strip_field(col("UMSV_PRPR_ID_SVC")).alias("UMSV_PRPR_ID_SVC"),
    strip_field(col("UMSV_PRPR_ID_FAC")).alias("UMSV_PRPR_ID_FAC"),
    strip_field(col("UMSV_PRPR_ID_PCP")).alias("UMSV_PRPR_ID_PCP"),
    when((length(strip_field(col("UMSV_IDCD_ID_PRI"))) == 0) | (col("UMSV_IDCD_ID_PRI").isNull()), lit("NA"))
       .otherwise(strip_field(col("UMSV_IDCD_ID_PRI"))).alias("UMSV_IDCD_ID_PRI"),
    strip_field(col("UMSV_PSCD_ID_REQ")).alias("UMSV_PSCD_ID_REQ"),
    strip_field(col("UMSV_PSCD_ID_AUTH")).alias("UMSV_PSCD_ID_AUTH"),
    strip_field(col("UMSV_PSCD_POS_AUTH")).alias("UMSV_PSCD_POS_AUTH"),
    strip_field(col("SESE_ID")).alias("SESE_ID"),
    strip_field(col("SESE_RULE")).alias("SESE_RULE"),
    strip_field(col("SEGR_ID")).alias("SEGR_ID"),
    when((length(strip_field(col("IPCD_ID"))) == 0) | (col("IPCD_ID").isNull()), lit("NA"))
       .otherwise(strip_field(col("IPCD_ID"))).alias("IPCD_ID"),
    col("UMSV_UNITS_REQ").alias("UMSV_UNITS_REQ"),
    col("UMSV_UNITS_AUTH").alias("UMSV_UNITS_AUTH"),
    strip_field(col("UMSV_DISALL_EXCD")).alias("UMSV_DISALL_EXCD"),
    strip_field(col("UMSV_MCTR_LDNY")).alias("UMSV_MCTR_LDNY"),
    col("UMSV_ME_AGE").alias("UMSV_ME_AGE"),
    col("UMSV_AMT_ALLOW").alias("UMSV_AMT_ALLOW"),
    col("UMSV_UNITS_ALLOW").alias("UMSV_UNITS_ALLOW"),
    col("UMSV_AMT_PAID").alias("UMSV_AMT_PAID"),
    col("UMSV_UNITS_PAID").alias("UMSV_UNITS_PAID"),
    col("UMSV_DENY_DT").alias("UMSV_DENY_DT"),
    col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    when(length(strip_field(col("UMUM_ICD_IND_PROC"))) == 0, lit("9"))
       .otherwise(strip_field(col("UMUM_ICD_IND_PROC"))).alias("UMUM_ICD_IND_PROC"),
    # Next column: PROC_CD_CAT_CD_DT => FORMAT.DATE(UMSV_FROM_DT, "SYBASE","TIMESTAMP","CCYY-MM-DD") => interpret as current logic not used later, skip or store
    # but we do not skip logic; we interpret it as a date conversion. The instructions do not provide a direct python function for this format
    # We cannot use an "expr" for Sybase. We keep the column as given text. We'll store as is because there's no direct function for Sybase conversion:
    # We'll represent it as a string of "YYYY-MM-DD".
    when(col("UMSV_FROM_DT").isNull(), lit("")).otherwise(col("UMSV_FROM_DT")).alias("PROC_CD_CAT_CD_DT"),
    strip_field(col("UMSV_MCTR_RIND")).alias("UMSV_MCTR_RIND")
)

# --------------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# The transform reads from df_strip (PrimaryLink)
# Also does a left join on df_hf_umsvc_cd_mppng_trgt_cd as diag_cd_typ_cd
# Join condition: Strip.UMUM_ICD_IND_PROC == diag_cd_typ_cd.SRC_CD
df_business_join = df_strip.alias("Strip").join(
    df_hf_umsvc_cd_mppng_trgt_cd.alias("diag_cd_typ_cd"),
    on=[col("Strip.UMUM_ICD_IND_PROC") == col("diag_cd_typ_cd.SRC_CD")],
    how="left"
)
df_business = df_business_join.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"),10," ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"),1," ").alias("DISCARD_IN"),
    rpad(lit("Y"),1," ").alias("PASS_THRU_IN"),
    col("CurrDate").alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    rpad(lit("FACETS"),   6, " ").alias("SRC_SYS_CD"),
    (lit("FACETS") + lit(";") + col("Strip.UMUM_REF_ID") + lit(";") + col("Strip.UMSV_SEQ_NO")).alias("PRI_KEY_STRING"),
    col("Strip.UMUM_REF_ID").alias("UM_REF_ID"),
    col("Strip.UMSV_SEQ_NO").alias("UM_SVC_SEQ_NO"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("UM_SK"),
    col("Strip.MEME_CK").alias("MEME_CK"),
    when(length(col("Strip.UMSV_AUTH_IND")) == 0, lit("N")).otherwise(col("Strip.UMSV_AUTH_IND")).alias("UMSV_AUTH_IND"),
    when(length(col("Strip.UMSV_REF_IND"))  == 0, lit("N")).otherwise(col("Strip.UMSV_REF_IND")).alias("UMSV_REF_IND"),
    when(length(col("Strip.UMSV_TYPE")) == 0, lit("NA")).otherwise(col("Strip.UMSV_TYPE")).alias("UMSV_TYPE"),
    when(length(col("Strip.UMSV_CAT"))  == 0, lit("NA")).otherwise(col("Strip.UMSV_CAT")).alias("UMSV_CAT"),
    when(length(col("Strip.UMSV_INPUT_USID")) == 0, lit("UNK")).otherwise(col("Strip.UMSV_INPUT_USID")).alias("UMSV_INPUT_USID"),
    # Next are date conversions with "FORMAT.DATE(...,'SYBASE','TIMESTAMP','CCYY-MM-DD')"
    when(col("Strip.UMSV_INPUT_DT").isNull(), lit("")).otherwise(col("Strip.UMSV_INPUT_DT")).alias("UMSV_INPUT_DT"),
    when(col("Strip.UMSV_RECD_DT").isNull(),  lit("")).otherwise(col("Strip.UMSV_RECD_DT")).alias("UMSV_RECD_DT"),
    when(col("Strip.UMSV_NEXT_REV_DT").isNull(), lit("")).otherwise(col("Strip.UMSV_NEXT_REV_DT")).alias("UMSV_NEXT_REV_DT"),
    when(col("Strip.UMSV_AUTH_DT").isNull(),  lit("")).otherwise(col("Strip.UMSV_AUTH_DT")).alias("UMSV_AUTH_DT"),
    when(length(col("Strip.UMVT_STS")) == 0, lit("NA")).otherwise(col("Strip.UMVT_STS")).alias("UMVT_STS"),
    col("Strip.UMVT_SEQ_NO").alias("UMVT_SEQ_NO"),
    when(col("Strip.UMSV_FROM_DT").isNull(), lit("")).otherwise(col("Strip.UMSV_FROM_DT")).alias("UMSV_FROM_DT"),
    when(col("Strip.UMSV_TO_DT").isNull(),   lit("")).otherwise(col("Strip.UMSV_TO_DT")).alias("UMSV_TO_DT"),
    when(length(col("Strip.UMSV_PRPR_ID_REQ")) == 0, lit("NA")).otherwise(col("Strip.UMSV_PRPR_ID_REQ")).alias("UMSV_PRPR_ID_REQ"),
    when(length(col("Strip.UMSV_PRPR_ID_SVC")) == 0, lit("NA")).otherwise(col("Strip.UMSV_PRPR_ID_SVC")).alias("UMSV_PRPR_ID_SVC"),
    when(length(col("Strip.UMSV_PRPR_ID_FAC")) == 0, lit("NA")).otherwise(col("Strip.UMSV_PRPR_ID_FAC")).alias("UMSV_PRPR_ID_FAC"),
    when(length(col("Strip.UMSV_PRPR_ID_PCP")) == 0, lit("NA")).otherwise(col("Strip.UMSV_PRPR_ID_PCP")).alias("UMSV_PRPR_ID_PCP"),
    when((length(col("Strip.UMSV_IDCD_ID_PRI")) == 0) | (col("Strip.UMSV_IDCD_ID_PRI").isNull()), lit("NA"))
      .otherwise(col("Strip.UMSV_IDCD_ID_PRI")).alias("UMSV_IDCD_ID_PRI"),
    when(length(col("Strip.UMSV_PSCD_ID_REQ")) == 0, lit("NA")).otherwise(col("Strip.UMSV_PSCD_ID_REQ")).alias("UMSV_PSCD_ID_REQ"),
    when(length(col("Strip.UMSV_PSCD_ID_AUTH"))== 0, lit("NA")).otherwise(col("Strip.UMSV_PSCD_ID_AUTH")).alias("UMSV_PSCD_ID_AUTH"),
    when(length(col("Strip.UMSV_PSCD_POS_AUTH"))==0, lit("NA")).otherwise(col("Strip.UMSV_PSCD_POS_AUTH")).alias("UMSV_PSCD_POS_AUTH"),
    when(length(col("Strip.SESE_ID")) == 0, lit("NA")).otherwise(col("Strip.SESE_ID")).alias("SESE_ID"),
    when(length(col("Strip.SESE_RULE"))==0, lit("")).otherwise(col("Strip.SESE_RULE")).alias("SESE_RULE"),
    when(length(col("Strip.SEGR_ID")) == 0, lit("NA")).otherwise(col("Strip.SEGR_ID")).alias("SEGR_ID"),
    col("Strip.IPCD_ID").alias("IPCD_ID"),
    when(length(col("Strip.IPCD_ID")) >= 7, substring(col("Strip.IPCD_ID"), 6, 2)).otherwise(lit(" ")).alias("PROC_CD_MOD"),
    col("Strip.UMSV_UNITS_REQ").alias("UMSV_UNITS_REQ"),
    col("Strip.UMSV_UNITS_AUTH").alias("UMSV_UNITS_AUTH"),
    when(length(col("Strip.UMSV_DISALL_EXCD"))==0, lit("NA")).otherwise(col("Strip.UMSV_DISALL_EXCD")).alias("UMSV_DISALL_EXCD"),
    when(length(col("Strip.UMSV_MCTR_LDNY")) == 0, lit("NA")).otherwise(col("Strip.UMSV_MCTR_LDNY")).alias("UMSV_MCTR_LDNY"),
    col("Strip.UMSV_ME_AGE").alias("UMSV_ME_AGE"),
    col("Strip.UMSV_AMT_ALLOW").alias("UMSV_AMT_ALLOW"),
    col("Strip.UMSV_UNITS_ALLOW").alias("UMSV_UNITS_ALLOW"),
    col("Strip.UMSV_AMT_PAID").alias("UMSV_AMT_PAID"),
    col("Strip.UMSV_UNITS_PAID").alias("UMSV_UNITS_PAID"),
    when(col("Strip.UMSV_DENY_DT").isNull(), lit("")).otherwise(col("Strip.UMSV_DENY_DT")).alias("UMSV_DENY_DT"),
    when(col("Strip.ATXR_SOURCE_ID").isNull(), lit("")).otherwise(col("Strip.ATXR_SOURCE_ID")).alias("ATXR_SOURCE_ID"),
    col("diag_cd_typ_cd.TRGT_CD").alias("DIAG_CD_TYP_CD"),
    when(length(col("Strip.UMSV_MCTR_RIND"))==0, lit("NA")).otherwise(col("Strip.UMSV_MCTR_RIND")).alias("UMSV_MCTR_RIND"),
    # Provide placeholders for columns used in next stage's logic
    # We'll also pass through PROC_CD_TYP_CD, PROC_CD_CAT_CD for next stage "Check_Sk"
    lit("").alias("PROC_CD_TYP_CD"),
    lit("").alias("PROC_CD_CAT_CD")
)

# --------------------------------------------------------------------------------
# Stage: Check_Sk (CTransformerStage)
# Two left joins to df_hf_UmSvc_ProcCd for "ProcCdTypCd" and "ProcCdTypCd1"
# First join condition:
#   BusinessRules.IPCD_ID = ProcCdTypCd.PROC_CD,
#   BusinessRules.PROC_CD_TYP_CD = ProcCdTypCd.PROC_CD_TYP_CD,
#   BusinessRules.PROC_CD_CAT_CD = ProcCdTypCd.PROC_CD_CAT_CD
df_check_1 = df_business.alias("Transform").join(
    df_hf_UmSvc_ProcCd.alias("ProcCdTypCd"),
    on=[
        col("Transform.IPCD_ID")       == col("ProcCdTypCd.PROC_CD"),
        col("Transform.PROC_CD_TYP_CD")== col("ProcCdTypCd.PROC_CD_TYP_CD"),
        col("Transform.PROC_CD_CAT_CD")== col("ProcCdTypCd.PROC_CD_CAT_CD")
    ],
    how="left"
)

# Second join condition, referencing the same df_hf_UmSvc_ProcCd but different alias
#   Substrings(Transform.IPCD_ID,1,5) = ProcCdTypCd1.PROC_CD
df_check = df_check_1.alias("Temp").join(
    df_hf_UmSvc_ProcCd.alias("ProcCdTypCd1"),
    on=[
        substring(col("Temp.IPCD_ID"),1,5) == col("ProcCdTypCd1.PROC_CD")
    ],
    how="left"
)

# Now define stage variables and final columns
df_check_sk = df_check.select(
    col("Temp.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("Temp.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("Temp.DISCARD_IN").alias("DISCARD_IN"),
    col("Temp.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("Temp.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("Temp.ERR_CT").alias("ERR_CT"),
    col("Temp.RECYCLE_CT").alias("RECYCLE_CT"),
    col("Temp.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Temp.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("Temp.UM_REF_ID").alias("UM_REF_ID"),
    col("Temp.UM_SVC_SEQ_NO").alias("UM_SVC_SEQ_NO"),
    col("Temp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Temp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Temp.UM_SK").alias("UM_SK"),
    col("Temp.MEME_CK").alias("MEME_CK"),
    col("Temp.UMSV_AUTH_IND").alias("UMSV_AUTH_IND"),
    col("Temp.UMSV_REF_IND").alias("UMSV_REF_IND"),
    col("Temp.UMSV_TYPE").alias("UMSV_TYPE"),
    col("Temp.UMSV_CAT").alias("UMSV_CAT"),
    col("Temp.UMSV_INPUT_USID").alias("UMSV_INPUT_USID"),
    col("Temp.UMSV_INPUT_DT").alias("UMSV_INPUT_DT"),
    col("Temp.UMSV_RECD_DT").alias("UMSV_RECD_DT"),
    col("Temp.UMSV_NEXT_REV_DT").alias("UMSV_NEXT_REV_DT"),
    col("Temp.UMSV_AUTH_DT").alias("UMSV_AUTH_DT"),
    col("Temp.UMVT_STS").alias("UMVT_STS"),
    col("Temp.UMVT_SEQ_NO").alias("UMVT_SEQ_NO"),
    col("Temp.UMSV_FROM_DT").alias("UMSV_FROM_DT"),
    col("Temp.UMSV_TO_DT").alias("UMSV_TO_DT"),
    col("Temp.UMSV_PRPR_ID_REQ").alias("UMSV_PRPR_ID_REQ"),
    col("Temp.UMSV_PRPR_ID_SVC").alias("UMSV_PRPR_ID_SVC"),
    col("Temp.UMSV_PRPR_ID_FAC").alias("UMSV_PRPR_ID_FAC"),
    col("Temp.UMSV_PRPR_ID_PCP").alias("UMSV_PRPR_ID_PCP"),
    col("Temp.UMSV_IDCD_ID_PRI").alias("UMSV_IDCD_ID_PRI"),
    col("Temp.UMSV_PSCD_ID_REQ").alias("UMSV_PSCD_ID_REQ"),
    col("Temp.UMSV_PSCD_ID_AUTH").alias("UMSV_PSCD_ID_AUTH"),
    col("Temp.UMSV_PSCD_POS_AUTH").alias("UMSV_PSCD_POS_AUTH"),
    col("Temp.SESE_ID").alias("SESE_ID"),
    col("Temp.SESE_RULE").alias("SESE_RULE"),
    col("Temp.SEGR_ID").alias("SEGR_ID"),
    # Stage variable: svProcCd => If IsNull(ProcCdTypCd.PROC_CD) = false then that else 'NA'
    # But we have columns from the join that might be null. We'll replicate logic:
    when(col("ProcCdTypCd.PROC_CD").isNotNull(), col("ProcCdTypCd.PROC_CD"))
      .otherwise(lit("NA")).alias("svProcCd"),
    # Stage variable: svProcCd1 => If svProcCd = 'NA' then check if IsNull(ProcCdTypCd1.PROC_CD) => 'NA' else that, else svProcCd
    when(
       col("ProcCdTypCd.PROC_CD").isNull(),
       when(col("ProcCdTypCd1.PROC_CD").isNull(), lit("NA"))
         .otherwise(col("ProcCdTypCd1.PROC_CD"))
    ).otherwise(col("ProcCdTypCd.PROC_CD")).alias("svProcCd1"),
    # Stage variable: svProcCdTypCd
    when(col("ProcCdTypCd.PROC_CD").isNotNull(), col("ProcCdTypCd.PROC_CD_TYP_CD"))
      .otherwise(
         when(col("ProcCdTypCd1.PROC_CD").isNotNull(), col("ProcCdTypCd1.PROC_CD_TYP_CD"))
         .otherwise(lit("NA"))
      ).alias("svProcCdTypCd"),
    # Stage variable: svProcCdCatCd
    when(col("ProcCdTypCd.PROC_CD").isNotNull(), col("ProcCdTypCd.PROC_CD_CAT_CD"))
      .otherwise(
         when(col("ProcCdTypCd1.PROC_CD").isNotNull(), col("ProcCdTypCd1.PROC_CD_CAT_CD"))
         .otherwise(lit("NA"))
      ).alias("svProcCdCatCd"),
    col("Temp.IPCD_ID").alias("IPCD_ID"),
    col("Temp.PROC_CD_MOD").alias("PROC_CD_MOD"),
    col("Temp.UMSV_UNITS_REQ").alias("UMSV_UNITS_REQ"),
    col("Temp.UMSV_UNITS_AUTH").alias("UMSV_UNITS_AUTH"),
    col("Temp.UMSV_DISALL_EXCD").alias("UMSV_DISALL_EXCD"),
    col("Temp.UMSV_MCTR_LDNY").alias("UMSV_MCTR_LDNY"),
    col("Temp.UMSV_ME_AGE").alias("UMSV_ME_AGE"),
    col("Temp.UMSV_AMT_ALLOW").alias("UMSV_AMT_ALLOW"),
    col("Temp.UMSV_UNITS_ALLOW").alias("UMSV_UNITS_ALLOW"),
    col("Temp.UMSV_AMT_PAID").alias("UMSV_AMT_PAID"),
    col("Temp.UMSV_UNITS_PAID").alias("UMSV_UNITS_PAID"),
    col("Temp.UMSV_DENY_DT").alias("UMSV_DENY_DT"),
    col("Temp.ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    col("Temp.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    # Here we define the final columns for PROC_CD_TYP_CD / PROC_CD_CAT_CD from stage variables:
    # "PROC_CD_TYP_CD" => svProcCdTypCd
    # "PROC_CD_CAT_CD" => svProcCdCatCd
    # for the second link logic:
    # If ProcCdTypCd.PROC_CD_TYP_CD = 'ICD10' Or svProcCd1 = 'NA' => ...
    # The job has expression:
    # "PROC_CD_MOD" => If ProcCdTypCd.PROC_CD_TYP_CD = 'ICD10' Or svProcCd1='NA' then Space(2) else ...
    # We already did that. We'll just keep columns as is.
    col("svProcCdTypCd").alias("PROC_CD_TYP_CD"),
    col("svProcCdCatCd").alias("PROC_CD_CAT_CD"),
    col("Temp.UMSV_MCTR_RIND").alias("UMSV_MCTR_RIND")
).withColumnRenamed("svProcCd1","svProcCd1")

final_cols_check = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "UM_REF_ID",
    "UM_SVC_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "UM_SK",
    "MEME_CK",
    "UMSV_AUTH_IND",
    "UMSV_REF_IND",
    "UMSV_TYPE",
    "UMSV_CAT",
    "UMSV_INPUT_USID",
    "UMSV_INPUT_DT",
    "UMSV_RECD_DT",
    "UMSV_NEXT_REV_DT",
    "UMSV_AUTH_DT",
    "UMVT_STS",
    "UMVT_SEQ_NO",
    "UMSV_FROM_DT",
    "UMSV_TO_DT",
    "UMSV_PRPR_ID_REQ",
    "UMSV_PRPR_ID_SVC",
    "UMSV_PRPR_ID_FAC",
    "UMSV_PRPR_ID_PCP",
    "UMSV_IDCD_ID_PRI",
    "UMSV_PSCD_ID_REQ",
    "UMSV_PSCD_ID_AUTH",
    "UMSV_PSCD_POS_AUTH",
    "SESE_ID",
    "SESE_RULE",
    "SEGR_ID",
    "IPCD_ID",
    "PROC_CD_MOD",
    "UMSV_UNITS_REQ",
    "UMSV_UNITS_AUTH",
    "UMSV_DISALL_EXCD",
    "UMSV_MCTR_LDNY",
    "UMSV_ME_AGE",
    "UMSV_AMT_ALLOW",
    "UMSV_UNITS_ALLOW",
    "UMSV_AMT_PAID",
    "UMSV_UNITS_PAID",
    "UMSV_DENY_DT",
    "ATXR_SOURCE_ID",
    "DIAG_CD_TYP_CD",
    "PROC_CD_TYP_CD",
    "PROC_CD_CAT_CD",
    "UMSV_MCTR_RIND",
    "svProcCd1"
]
df_check_sk = df_check_sk.select(*final_cols_check)

# --------------------------------------------------------------------------------
# Stage: PrimaryKey (CTransformerStage)
# Left join with df_hf_lkup on (SRC_SYS_CD, UM_REF_ID, UM_SVC_SEQ_NO)
df_pk_join = df_check_sk.alias("SK_Check").join(
    df_hf_lkup.alias("lkup"),
    on=[
        col("SK_Check.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
        col("SK_Check.UM_REF_ID")  == col("lkup.UM_REF_ID"),
        col("SK_Check.UM_SVC_SEQ_NO") == col("lkup.UM_SVC_SEQ_NO")
    ],
    how="left"
)

# Derive stage variables:
# SK = if IsNull(lkup.UM_SVC_SK) then KeyMgtGetNextValueConcurrent(...) else lkup.UM_SVC_SK
# We do not call KeyMgtGetNextValueConcurrent directly, but create UM_SVC_SK as Null if needed, then use SurrogateKeyGen
df_primary_tmp = df_pk_join.select(
    col("SK_Check.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("SK_Check.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("SK_Check.DISCARD_IN").alias("DISCARD_IN"),
    col("SK_Check.PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("SK_Check.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("SK_Check.ERR_CT").alias("ERR_CT"),
    col("SK_Check.RECYCLE_CT").alias("RECYCLE_CT"),
    col("SK_Check.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SK_Check.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    when(col("lkup.UM_SVC_SK").isNull(), lit(None)).otherwise(col("lkup.UM_SVC_SK")).alias("_TEMP_UM_SVC_SK"),
    col("SK_Check.UM_REF_ID").alias("UM_REF_ID"),
    col("SK_Check.UM_SVC_SEQ_NO").alias("SEQ_NO"),
    when(col("lkup.UM_SVC_SK").isNull(), col("SK_Check.CRT_RUN_CYC_EXCTN_SK"))
       .otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("_TEMP_CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SK_Check.UM_SK").alias("UM_SK"),
    col("SK_Check.MEME_CK").alias("MEME_CK"),
    col("SK_Check.UMSV_AUTH_IND").alias("UMSV_AUTH_IND"),
    col("SK_Check.UMSV_REF_IND").alias("UMSV_REF_IND"),
    col("SK_Check.UMSV_TYPE").alias("UMSV_TYPE"),
    col("SK_Check.UMSV_CAT").alias("UMSV_CAT"),
    col("SK_Check.UMSV_INPUT_USID").alias("UMSV_INPUT_USID"),
    col("SK_Check.UMSV_INPUT_DT").alias("UMSV_INPUT_DT"),
    col("SK_Check.UMSV_RECD_DT").alias("UMSV_RECD_DT"),
    col("SK_Check.UMSV_NEXT_REV_DT").alias("UMSV_NEXT_REV_DT"),
    col("SK_Check.UMSV_AUTH_DT").alias("UMSV_AUTH_DT"),
    col("SK_Check.UMVT_STS").alias("UMVT_STS"),
    col("SK_Check.UMVT_SEQ_NO").alias("UMVT_SEQ_NO"),
    col("SK_Check.UMSV_FROM_DT").alias("UMSV_FROM_DT"),
    col("SK_Check.UMSV_TO_DT").alias("UMSV_TO_DT"),
    col("SK_Check.UMSV_PRPR_ID_REQ").alias("UMSV_PRPR_ID_REQ"),
    col("SK_Check.UMSV_PRPR_ID_SVC").alias("UMSV_PRPR_ID_SVC"),
    col("SK_Check.UMSV_PRPR_ID_FAC").alias("UMSV_PRPR_ID_FAC"),
    col("SK_Check.UMSV_PRPR_ID_PCP").alias("UMSV_PRPR_ID_PCP"),
    col("SK_Check.UMSV_IDCD_ID_PRI").alias("UMSV_IDCD_ID_PRI"),
    col("SK_Check.UMSV_PSCD_ID_REQ").alias("UMSV_PSCD_ID_REQ"),
    col("SK_Check.UMSV_PSCD_ID_AUTH").alias("UMSV_PSCD_ID_AUTH"),
    col("SK_Check.UMSV_PSCD_POS_AUTH").alias("UMSV_PSCD_POS_AUTH"),
    col("SK_Check.SESE_ID").alias("SESE_ID"),
    col("SK_Check.SESE_RULE").alias("SESE_RULE"),
    col("SK_Check.SEGR_ID").alias("SEGR_ID"),
    col("SK_Check.IPCD_ID").alias("IPCD_ID"),
    col("SK_Check.PROC_CD_MOD").alias("PROC_CD_MOD"),
    col("SK_Check.UMSV_UNITS_REQ").alias("UMSV_UNITS_REQ"),
    col("SK_Check.UMSV_UNITS_AUTH").alias("UMSV_UNITS_AUTH"),
    col("SK_Check.UMSV_DISALL_EXCD").alias("UMSV_DISALL_EXCD"),
    col("SK_Check.UMSV_MCTR_LDNY").alias("UMSV_MCTR_LDNY"),
    col("SK_Check.UMSV_ME_AGE").alias("UMSV_ME_AGE"),
    col("SK_Check.UMSV_AMT_ALLOW").alias("UMSV_AMT_ALLOW"),
    col("SK_Check.UMSV_UNITS_ALLOW").alias("UMSV_UNITS_ALLOW"),
    col("SK_Check.UMSV_AMT_PAID").alias("UMSV_AMT_PAID"),
    col("SK_Check.UMSV_UNITS_PAID").alias("UMSV_UNITS_PAID"),
    col("SK_Check.UMSV_DENY_DT").alias("UMSV_DENY_DT"),
    col("SK_Check.ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID"),
    col("SK_Check.DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD"),
    col("SK_Check.PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD"),
    col("SK_Check.PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD"),
    col("SK_Check.UMSV_MCTR_RIND").alias("UMSV_MCTR_RIND"),
    col("SK_Check.svProcCd1").alias("svProcCd1")
)

# Now rename those temporary columns to final:
df_primary = df_primary_tmp.withColumn("UM_SVC_SK",
    when(col("_TEMP_UM_SVC_SK").isNull(), lit(None)).otherwise(col("_TEMP_UM_SVC_SK"))
).withColumn("CRT_RUN_CYC_EXCTN_SK",
    col("_TEMP_CRT_RUN_CYC_EXCTN_SK")
).drop("_TEMP_UM_SVC_SK","_TEMP_CRT_RUN_CYC_EXCTN_SK")

# SurrogateKeyGen for UM_SVC_SK
df_enriched = df_primary
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"UM_SVC_SK",<schema>,<secret_name>)

# Now we have df_enriched with UM_SVC_SK populated if needed.

# Write out to "dummy_hf_um_svc" (scenario B upsert) for new records
# We identify matched on (SRC_SYS_CD, UM_REF_ID, SEQ_NO)
# If matched => do nothing, else insert.
df_hf_write = df_enriched.filter(col("UM_SVC_SK").isNotNull())  # We have rows needing to ensure upsert

temp_table_name = f"STAGING.FctsUMSvcExtr_hf_write_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url_ids, jdbc_props_ids)

# Create the physical staging table
df_hf_write.select(
    col("SRC_SYS_CD"),
    col("UM_REF_ID"),
    col("SEQ_NO").alias("UM_SVC_SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("UM_SVC_SK")
).write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_name,
    mode="overwrite",
    properties=jdbc_props_ids
)

merge_sql_hf = f"""
MERGE IDS.dummy_hf_um_svc as TARGET
USING {temp_table_name} as SOURCE
ON TARGET.SRC_SYS_CD = SOURCE.SRC_SYS_CD
   AND TARGET.UM_REF_ID = SOURCE.UM_REF_ID
   AND TARGET.UM_SVC_SEQ_NO = SOURCE.UM_SVC_SEQ_NO
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, UM_REF_ID, UM_SVC_SEQ_NO, CRT_RUN_CYC_EXCTN_SK, UM_SVC_SK)
  VALUES (SOURCE.SRC_SYS_CD, SOURCE.UM_REF_ID, SOURCE.UM_SVC_SEQ_NO, SOURCE.CRT_RUN_CYC_EXCTN_SK, SOURCE.UM_SVC_SK);
"""
execute_dml(merge_sql_hf, jdbc_url_ids, jdbc_props_ids)

# --------------------------------------------------------------------------------
# Stage: IdsUmSvcExtr (CSeqFileStage)
# We write the file with all columns from df_enriched that match the "Key" link
df_ids_um_svc_extr_cols = [
    "JOB_EXCTN_RCRD_ERR_SK","INSRT_UPDT_CD","DISCARD_IN","PASS_THRU_IN","FIRST_RECYC_DT","ERR_CT","RECYCLE_CT",
    "SRC_SYS_CD","PRI_KEY_STRING","UM_SVC_SK","UM_REF_ID","SEQ_NO","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "UM_SK","MEME_CK","UMSV_AUTH_IND","UMSV_REF_IND","UMSV_TYPE","UMSV_CAT","UMSV_INPUT_USID","UMSV_INPUT_DT",
    "UMSV_RECD_DT","UMSV_NEXT_REV_DT","UMSV_AUTH_DT","UMVT_STS","UMVT_SEQ_NO","UMSV_FROM_DT","UMSV_TO_DT",
    "UMSV_PRPR_ID_REQ","UMSV_PRPR_ID_SVC","UMSV_PRPR_ID_FAC","UMSV_PRPR_ID_PCP","UMSV_IDCD_ID_PRI","UMSV_PSCD_ID_REQ",
    "UMSV_PSCD_ID_AUTH","UMSV_PSCD_POS_AUTH","SESE_ID","SESE_RULE","SEGR_ID","IPCD_ID","PROC_CD_MOD","UMSV_UNITS_REQ",
    "UMSV_UNITS_AUTH","UMSV_DISALL_EXCD","UMSV_MCTR_LDNY","UMSV_ME_AGE","UMSV_AMT_ALLOW","UMSV_UNITS_ALLOW",
    "UMSV_AMT_PAID","UMSV_UNITS_PAID","UMSV_DENY_DT","ATXR_SOURCE_ID","DIAG_CD_TYP_CD","PROC_CD_TYP_CD",
    "PROC_CD_CAT_CD","UMSV_MCTR_RIND"
]

# For char/varchar, apply rpad to match lengths declared in DataStage.
# We'll do a final select with rpad for those columns that had length in the metadata:
df_idsUmSvcExtr = df_enriched.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"),10," "),
    rpad(col("DISCARD_IN"),1," "),
    rpad(col("PASS_THRU_IN"),1," "),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"),6," "),
    col("PRI_KEY_STRING"),
    col("UM_SVC_SK"),
    col("UM_REF_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("UM_SK"),
    col("MEME_CK"),
    rpad(col("UMSV_AUTH_IND"),1," "),
    rpad(col("UMSV_REF_IND"),1," "),
    rpad(col("UMSV_TYPE"),1," "),
    rpad(col("UMSV_CAT"),1," "),
    rpad(col("UMSV_INPUT_USID"),10," "),
    rpad(col("UMSV_INPUT_DT"),10," "),
    rpad(col("UMSV_RECD_DT"),10," "),
    rpad(col("UMSV_NEXT_REV_DT"),10," "),
    rpad(col("UMSV_AUTH_DT"),10," "),
    rpad(col("UMVT_STS"),2," "),
    col("UMVT_SEQ_NO"),
    rpad(col("UMSV_FROM_DT"),10," "),
    rpad(col("UMSV_TO_DT"),10," "),
    rpad(col("UMSV_PRPR_ID_REQ"),12," "),
    rpad(col("UMSV_PRPR_ID_SVC"),12," "),
    rpad(col("UMSV_PRPR_ID_FAC"),12," "),
    rpad(col("UMSV_PRPR_ID_PCP"),12," "),
    col("UMSV_IDCD_ID_PRI"),
    rpad(col("UMSV_PSCD_ID_REQ"),2," "),
    rpad(col("UMSV_PSCD_ID_AUTH"),2," "),
    rpad(col("UMSV_PSCD_POS_AUTH"),1," "),
    rpad(col("SESE_ID"),4," "),
    rpad(col("SESE_RULE"),3," "),
    rpad(col("SEGR_ID"),4," "),
    rpad(col("IPCD_ID"),7," "),
    rpad(col("PROC_CD_MOD"),2," "),
    col("UMSV_UNITS_REQ"),
    col("UMSV_UNITS_AUTH"),
    rpad(col("UMSV_DISALL_EXCD"),3," "),
    rpad(col("UMSV_MCTR_LDNY"),4," "),
    col("UMSV_ME_AGE"),
    col("UMSV_AMT_ALLOW"),
    col("UMSV_UNITS_ALLOW"),
    col("UMSV_AMT_PAID"),
    col("UMSV_UNITS_PAID"),
    rpad(col("UMSV_DENY_DT"),10," "),
    col("ATXR_SOURCE_ID"),
    col("DIAG_CD_TYP_CD"),
    col("PROC_CD_TYP_CD"),
    col("PROC_CD_CAT_CD"),
    rpad(col("UMSV_MCTR_RIND"),4," ")
)

write_files(
    df_idsUmSvcExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Stage: Transform (CTransformerStage) after Check_Sk => "Snapshot" link
# The input link is from the "Check_Sk" with link name "Snapshot"
df_snapshot_in = df_check_sk.select(
    rpad(col("SRC_SYS_CD"),6," ").alias("SRC_SYS_CD"),
    col("UM_REF_ID"),
    col("UM_SVC_SEQ_NO"),
    col("DIAG_CD_TYP_CD"),
    col("IPCD_ID"),
    col("PROC_CD_TYP_CD"),
    col("PROC_CD_CAT_CD"),
    col("UMSV_CAT"),
    col("UMSV_AMT_PAID").alias("PD_AMT"),
    col("UMSV_UNITS_ALLOW").alias("ALW_UNIT_CT")
).withColumnRenamed("SRC_SYS_CD", "Snapshot_SRC_SYS_CD")

# Stage variables: 
# svPriDiagCdSk = GetFkeyDiagCd(Snapshot_SRC_SYS_CD, 1, Snapshot.UMSV_IDCD_ID_PRI, Snapshot.DIAG_CD_TYP_CD, 'X')
# but the job's pinned columns are "UMSV_IDCD_ID_PRI"? Actually we see it referencing "Snapshot.UMSV_IDCD_ID_PRI" in DataStage,
# but only columns that we see in the final are used. The job JSON for Snapshot link has "UMSV_IDCD_ID_PRI" -> We do not see it in the select.
# In the real job design, they'd pass it, but not shown. We'll just call with the same references:
df_transform_snapshot = df_snapshot_in.withColumn(
    "UMSV_IDCD_ID_PRI",
    lit("")
).withColumn(
    "svPriDiagCdSk",
    GetFkeyDiagCd(col("Snapshot_SRC_SYS_CD"), lit(1), col("UMSV_IDCD_ID_PRI"), col("DIAG_CD_TYP_CD"), lit("X"))
).withColumn(
    "svProcCdSk",
    GetFkeyProcCd(col("Snapshot_SRC_SYS_CD"), lit(1), col("IPCD_ID"), col("PROC_CD_TYP_CD"), col("PROC_CD_CAT_CD"), lit("X"))
).withColumn(
    "svUmSvcTreatCatCdSk",
    GetFkeyCodes(col("Snapshot_SRC_SYS_CD"), lit(1), lit("UTILIZATION MANAGEMENT TREATMENT"), col("UMSV_CAT"), lit("X"))
)

df_snapshot_out = df_transform_snapshot.select(
    rpad(col("Snapshot_SRC_SYS_CD"),6," ").alias("SRC_SYS_CD_SK"),
    col("UM_REF_ID"),
    col("UM_SVC_SEQ_NO"),
    col("svPriDiagCdSk").alias("PRI_DIAG_CD_SK"),
    col("svProcCdSk").alias("PROC_CD_SK"),
    col("svUmSvcTreatCatCdSk").alias("UM_SVC_TREAT_CAT_CD_SK"),
    col("PD_AMT"),
    col("ALW_UNIT_CT")
)

# --------------------------------------------------------------------------------
# Stage: Snapshot_File (CSeqFileStage)
# Write final file with no header
write_files(
    df_snapshot_out,
    f"{adls_path}/load/B_UM_SVC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# --------------------------------------------------------------------------------
# After job routine = "1" => no specific subroutine code was provided, so nothing more to do.