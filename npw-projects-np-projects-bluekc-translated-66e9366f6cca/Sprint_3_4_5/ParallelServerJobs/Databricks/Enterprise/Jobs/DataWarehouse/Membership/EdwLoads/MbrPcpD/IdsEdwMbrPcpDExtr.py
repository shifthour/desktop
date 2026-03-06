# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     
# MAGIC 	   EdwMbrPcpExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	   Pulls data from IDS and codes hash file to load into the EDW
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	   IDS:   MBR_PCP
# MAGIC                              PROV
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                    hf_cdma_codes - Load the CDMA codes to get cd and cd_nm from cd_sk
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source, lookup all code SK values and get the natural codes
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   Sequential file for database loading
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                  Hugh Sisson   08/19/2005           - Originally Program
# MAGIC                  Suzanne Saylor  04/12/2006      - changed > #BeginCycle# to >=, added RunCycle parameter, set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC                  Sharon Andrew   07/06/2006   -   Renamed from EdwMbrPCPExtr to IdsMbrPcpExtr.  
# MAGIC                                                                         Added driver table processing for extractions
# MAGIC                                                                        Seperated all inner joins in sql to each own extraction.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                                                                                   DATASTAGE               CODE                                 DATE
# MAGIC DEVELOPER                  DATE                      PROJECT                               DESCRIPTION                                                                                                              ENVIRONMENT          REVIEWER                      REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill                         06/25/2013               5114                                   Create Load File for EDW Table MBR_PCP_D                                                  EnterpriseWhseDevl
# MAGIC TSieg                                  01/17/2019                5878                                   Adding MBR_PCP_MNL_ASGMT_RSN_CD_SK to db2_MBR_PCP_in           EnterpriseDev2           Kalyan Neelam         2019-02-25
# MAGIC                                                                                                                                 stage, Adding Ref_MbrPcpMnlAsgmtRsn_Lkp stream from Cpy_Mpping_Cd                                
# MAGIC                                                                                                                                 stage, Adding 3 new PCP_MNL_ASGMT_RSN fields to lkp_Codes
# MAGIC                                                                                                                                 and seq_MBR_PCP_D_Load stages

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrPcpDExtr
# MAGIC Read from source table MBR_PCP and PROV from IDS.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC Write MBR_PCP_D Data into a Sequential file for Load Job IdsEdwMbrPcpDLoad.
# MAGIC Add Defaults and Null Handling.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_MBR_PCP_in = f"""SELECT
  MBR_PCP.MBR_PCP_SK,
  COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
  MBR_PCP.MBR_UNIQ_KEY,
  MBR_PCP.MBR_PCP_TYP_CD_SK,
  MBR_PCP.EFF_DT_SK,
  MBR_PCP.CRT_RUN_CYC_EXCTN_SK,
  MBR_PCP.LAST_UPDT_RUN_CYC_EXCTN_SK,
  MBR_PCP.MBR_SK,
  MBR_PCP.PROV_SK,
  MBR_PCP.MBR_PCP_ASG_OVRD_RSN_CD_SK,
  MBR_PCP.MBR_PCP_TERM_RSN_CD_SK,
  MBR_PCP.PCP_AUTO_ASG_IN,
  MBR_PCP.TERM_DT_SK,
  MBR_PCP.SRC_SYS_LAST_UPDT_DT_SK,
  MBR_PCP.SRC_SYS_LAST_UPDT_USER_SK,
  MBR_PCP.MBR_PCP_MNL_ASGMT_RSN_CD_SK
FROM {IDSOwner}.MBR_PCP MBR_PCP
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON MBR_PCP.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.W_MBR_DRVR DRVR
WHERE DRVR.SUBJ_NM = 'MBR_UNIQ_KEY'
  AND DRVR.KEY_VAL_INT = MBR_PCP.MBR_UNIQ_KEY
"""

df_db2_MBR_PCP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_MBR_PCP_in)
    .load()
)

extract_query_db2_IDS_PROV_Extr = f"""
SELECT
  PROV.PROV_SK,
  PROV.PROV_ID
FROM {IDSOwner}.PROV PROV
"""

df_db2_IDS_PROV_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_IDS_PROV_Extr)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = f"""
SELECT
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Ref_MbrPcpType_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_Ref_AsgOvrdRsn_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_Ref_MbrPcpTermRsn_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_Ref_MbrPcpMnlAsgmtRsn_Lkp = df_db2_CD_MPPNG_Extr.select(
    col("CD_MPPNG_SK"),
    col("TRGT_CD"),
    col("TRGT_CD_NM")
)

df_lkp_Codes = (
    df_db2_MBR_PCP_in.alias("lnk_IIdsEdwMbrPcpDExtr_InAbc")
    .join(
        df_Ref_MbrPcpType_Lkp.alias("Ref_MbrPcpType_Lkp"),
        col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_TYP_CD_SK") == col("Ref_MbrPcpType_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_AsgOvrdRsn_Lkp.alias("Ref_AsgOvrdRsn_Lkp"),
        col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_ASG_OVRD_RSN_CD_SK") == col("Ref_AsgOvrdRsn_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_Ref_MbrPcpTermRsn_Lkp.alias("Ref_MbrPcpTermRsn_Lkp"),
        col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_TERM_RSN_CD_SK") == col("Ref_MbrPcpTermRsn_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_db2_IDS_PROV_Extr.alias("lnk_IDS_PROV_Out"),
        col("lnk_IIdsEdwMbrPcpDExtr_InAbc.PROV_SK") == col("lnk_IDS_PROV_Out.PROV_SK"),
        "left"
    )
    .join(
        df_Ref_MbrPcpMnlAsgmtRsn_Lkp.alias("Ref_MbrPcpMnlAsgmtRsn_Lkp"),
        col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_MNL_ASGMT_RSN_CD_SK") == col("Ref_MbrPcpMnlAsgmtRsn_Lkp.CD_MPPNG_SK"),
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes.select(
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_SK").alias("MBR_PCP_SK"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Ref_MbrPcpType_Lkp.TRGT_CD").alias("MBR_PCP_TYP_CD"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.EFF_DT_SK").alias("MBR_PCP_EFF_DT_SK"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_SK").alias("MBR_SK"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.PROV_SK").alias("PROV_SK"),
    col("Ref_AsgOvrdRsn_Lkp.TRGT_CD").alias("MBR_PCP_ASG_OVRD_RSN_CD"),
    col("Ref_AsgOvrdRsn_Lkp.TRGT_CD_NM").alias("MBR_PCP_ASG_OVRD_RSN_NM"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.PCP_AUTO_ASG_IN").alias("PCP_AUTO_ASG_IN"),
    col("Ref_MbrPcpTermRsn_Lkp.TRGT_CD").alias("MBR_PCP_TERM_RSN_CD"),
    col("Ref_MbrPcpTermRsn_Lkp.TRGT_CD_NM").alias("MBR_PCP_TERM_RSN_NM"),
    col("Ref_MbrPcpType_Lkp.TRGT_CD_NM").alias("MBR_PCP_TYP_NM"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.TERM_DT_SK").alias("MBR_PCP_TERM_DT_SK"),
    col("lnk_IDS_PROV_Out.PROV_ID").alias("PROV_ID"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_ASG_OVRD_RSN_CD_SK").alias("MBR_PCP_ASG_OVRD_RSN_CD_SK"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_TERM_RSN_CD_SK").alias("MBR_PCP_TERM_RSN_CD_SK"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_TYP_CD_SK").alias("MBR_PCP_TYP_CD_SK"),
    col("lnk_IDS_PROV_Out.PROV_SK").alias("IDS_PROV_SK_1"),
    col("Ref_MbrPcpMnlAsgmtRsn_Lkp.TRGT_CD").alias("MBR_PCP_MNL_ASGMT_RSN_CD"),
    col("Ref_MbrPcpMnlAsgmtRsn_Lkp.TRGT_CD_NM").alias("MBR_PCP_MNL_ASGMT_RSN_NM"),
    col("lnk_IIdsEdwMbrPcpDExtr_InAbc.MBR_PCP_MNL_ASGMT_RSN_CD_SK").alias("MBR_PCP_MNL_ASGMT_RSN_CD_SK")
)

df_xfm_BusinessLogic = df_lkp_Codes.withColumn(
    "PROV_ID",
    when(col("IDS_PROV_SK_1").isNull(), lit("UNK")).otherwise(trim(col("PROV_ID")))
)

df_xfm_BusinessLogic = df_xfm_BusinessLogic.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate))
df_xfm_BusinessLogic = df_xfm_BusinessLogic.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(CurrRunCycleDate))
df_xfm_BusinessLogic = df_xfm_BusinessLogic.withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
df_xfm_BusinessLogic = df_xfm_BusinessLogic.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))

df_seq_MBR_PCP_D_Load = df_xfm_BusinessLogic.select(
    "MBR_PCP_SK",
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "MBR_PCP_TYP_CD",
    rpad(col("MBR_PCP_EFF_DT_SK"), 10, " ").alias("MBR_PCP_EFF_DT_SK"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    "MBR_SK",
    "PROV_SK",
    "MBR_PCP_ASG_OVRD_RSN_CD",
    "MBR_PCP_ASG_OVRD_RSN_NM",
    rpad(col("PCP_AUTO_ASG_IN"), 1, " ").alias("MBR_PCP_AUTO_ASG_IN"),
    "MBR_PCP_TERM_RSN_CD",
    "MBR_PCP_TERM_RSN_NM",
    "MBR_PCP_TYP_NM",
    rpad(col("MBR_PCP_TERM_DT_SK"), 10, " ").alias("MBR_PCP_TERM_DT_SK"),
    "PROV_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "MBR_PCP_ASG_OVRD_RSN_CD_SK",
    "MBR_PCP_TERM_RSN_CD_SK",
    "MBR_PCP_TYP_CD_SK",
    "MBR_PCP_MNL_ASGMT_RSN_CD",
    "MBR_PCP_MNL_ASGMT_RSN_NM",
    "MBR_PCP_MNL_ASGMT_RSN_CD_SK"
)

write_files(
    df_seq_MBR_PCP_D_Load,
    f"{adls_path}/load/MBR_PCP_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)