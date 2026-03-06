# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                   Uses amounts on driver table to determine the claims line records that need to have the amounts updated on the amount to updatePulls data from ids  and update edw.
# MAGIC 
# MAGIC                
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                   TRIM claim numbers from tables to match to clm_ln
# MAGIC                            
# MAGIC 
# MAGIC                   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                      
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                 Project/Altiris #         Change Description                                                                                                              Development Project      Code Reviewer          Date Reviewed       
# MAGIC ----------------------              --------------------     ------------------------         -----------------------------------------------------------------------                                                                       --------------------------------       -------------------------------   ----------------------------       
# MAGIC 
# MAGIC SAndrew                     2009-11-15      3833 Remit Update    new program                                                                                                                        devlIDS                             Steph Goddard         12/10/2009
# MAGIC Kalyan Neelam         2013-06-26       4963 VBB Phase III      Added 3 new columns on end - VBB_RULE_SK, VBB_EXCD_SK, CLM_LN_VBB_IN        IntegrateNewDevl           Bhoomi Dasari           6/26/2013 
# MAGIC Manasa Andru          2014-11-02        TFS-9580                    Added 2 new fields (ITS_SUPLMT_DSCNT_AMT & ITS_SRCHRG_AMT)                         IntegrateNewDevl             Kalyan Neelam           2014-11-03
# MAGIC 
# MAGIC Shanmugam A 	 2017-03-02         5321                        SQL in stage 'IDS_W_CLM_LN_REMIT_DSALW_UPDT' will be aliased to 		IntegrateDev2              Jag Yelavarthi           2017-03-07
# MAGIC 						match with stage meta data 
# MAGIC 
# MAGIC Hari Pinnaka          2017-08-07      5792                             Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,                                                     IntegrateDev1               Kalyan Neelam          2017-09-06
# MAGIC                                                                                         NDC_UNIT_CT) at the end              
# MAGIC 
# MAGIC Madhavan B           2018-02-06      5792 	       Changed the datatype of the column                                                                                               IntegrateDev1               Kalyan Neelam          2018-02-12
# MAGIC                                                       		       NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)

# MAGIC IDS Claim Line Remit Update Claim Line
# MAGIC Reads the IDS Claim Line records that need to have the fields updated and creates load file. 
# MAGIC  
# MAGIC Does not do direct updates in case the process abends in the middle of program and will double substract upon restart.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter parsing
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Stage: IDS_W_CLM_LN_REMIT_DSALW_UPDT (DB2Connector - Read)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT 
SRC_SYS_CD_SK,
CLM_ID,
CLM_LN_SEQ_NO,
CLM_LN_SK,
SUM(DIFF_REMIT_DSALW_AMT) AS DIFF_REMIT_DSALW_AMT
FROM {IDSOwner}.W_CLM_LN_REMIT_DSALW_UPDT DRVR
GROUP BY 
SRC_SYS_CD_SK,
CLM_ID,
CLM_LN_SEQ_NO,
CLM_LN_SK
"""
df_IDS_W_CLM_LN_REMIT_DSALW_UPDT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: IDS_CLM_LN_on_DRVR (DB2Connector - Read)
extract_query = f"""
SELECT 
CLM_LN.CLM_LN_SK,
CLM_LN.SRC_SYS_CD_SK,
CLM_LN.CLM_ID,
CLM_LN.CLM_LN_SEQ_NO,
CLM_LN.CRT_RUN_CYC_EXCTN_SK,
CLM_LN.LAST_UPDT_RUN_CYC_EXCTN_SK,
CLM_LN.CLM_SK,
CLM_LN.PROC_CD_SK,
CLM_LN.SVC_PROV_SK,
CLM_LN.CLM_LN_DSALW_EXCD_SK,
CLM_LN.CLM_LN_EOB_EXCD_SK,
CLM_LN.CLM_LN_FINL_DISP_CD_SK,
CLM_LN.CLM_LN_LOB_CD_SK,
CLM_LN.CLM_LN_POS_CD_SK,
CLM_LN.CLM_LN_PREAUTH_CD_SK,
CLM_LN.CLM_LN_PREAUTH_SRC_CD_SK,
CLM_LN.CLM_LN_PRICE_SRC_CD_SK,
CLM_LN.CLM_LN_RFRL_CD_SK,
CLM_LN.CLM_LN_RVNU_CD_SK,
CLM_LN.CLM_LN_ROOM_PRICE_METH_CD_SK,
CLM_LN.CLM_LN_ROOM_TYP_CD_SK,
CLM_LN.CLM_LN_TOS_CD_SK,
CLM_LN.CLM_LN_UNIT_TYP_CD_SK,
CLM_LN.CAP_LN_IN,
CLM_LN.PRI_LOB_IN,
CLM_LN.SVC_END_DT_SK,
CLM_LN.SVC_STRT_DT_SK,
CLM_LN.AGMNT_PRICE_AMT,
CLM_LN.ALW_AMT,
CLM_LN.CHRG_AMT,
CLM_LN.COINS_AMT,
CLM_LN.CNSD_CHRG_AMT,
CLM_LN.COPAY_AMT,
CLM_LN.DEDCT_AMT,
CLM_LN.DSALW_AMT,
CLM_LN.ITS_HOME_DSCNT_AMT,
CLM_LN.NO_RESP_AMT,
CLM_LN.MBR_LIAB_BSS_AMT,
CLM_LN.PATN_RESP_AMT,
CLM_LN.PAYBL_AMT,
CLM_LN.PAYBL_TO_PROV_AMT,
CLM_LN.PAYBL_TO_SUB_AMT,
CLM_LN.PROC_TBL_PRICE_AMT,
CLM_LN.PROFL_PRICE_AMT,
CLM_LN.PROV_WRT_OFF_AMT,
CLM_LN.RISK_WTHLD_AMT,
CLM_LN.SVC_PRICE_AMT,
CLM_LN.SUPLMT_DSCNT_AMT,
CLM_LN.ALW_PRICE_UNIT_CT,
CLM_LN.UNIT_CT,
CLM_LN.DEDCT_AMT_ACCUM_ID,
CLM_LN.PREAUTH_SVC_SEQ_NO,
CLM_LN.RFRL_SVC_SEQ_NO,
CLM_LN.LMT_PFX_ID,
CLM_LN.PREAUTH_ID,
CLM_LN.PROD_CMPNT_DEDCT_PFX_ID,
CLM_LN.PROD_CMPNT_SVC_PAYMT_ID,
CLM_LN.RFRL_ID_TX,
CLM_LN.SVC_ID,
CLM_LN.SVC_PRICE_RULE_ID,
CLM_LN.SVC_RULE_TYP_TX,
CLM_LN.CLM_LN_SVC_LOC_TYP_CD_SK,
CLM_LN.CLM_LN_SVC_PRICE_RULE_CD_SK,
CLM_LN.NON_PAR_SAV_AMT,
CLM_LN.VBB_RULE_SK,
CLM_LN.VBB_EXCD_SK,
CLM_LN.CLM_LN_VBB_IN,
CLM_LN.ITS_SUPLMT_DSCNT_AMT,
CLM_LN.ITS_SRCHRG_AMT,
CLM_LN.NDC_SK,
CLM_LN.NDC_DRUG_FORM_CD_SK,
CLM_LN.NDC_UNIT_CT
FROM {IDSOwner}.CLM_LN CLM_LN,
     {IDSOwner}.W_CLM_LN_REMIT_DSALW_UPDT DRVR
WHERE DRVR.SRC_SYS_CD_SK = CLM_LN.SRC_SYS_CD_SK
  AND DRVR.CLM_ID = CLM_LN.CLM_ID
  AND DRVR.CLM_LN_SEQ_NO = CLM_LN.CLM_LN_SEQ_NO
"""
df_IDS_CLM_LN_on_DRVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# Stage: hf_clmln_remit_dsalw_updt (CHashedFileStage) - Scenario A (intermediate hashed file)
# Replace with deduplicate logic on key column "CLM_LN_SK"
df_hf_clmln_remit_dsalw_updt = df_IDS_CLM_LN_on_DRVR.dropDuplicates(["CLM_LN_SK"])

# Stage: Copy_of_Transformer_401 (CTransformerStage)
df_Copy_of_Transformer_401_joined = df_IDS_W_CLM_LN_REMIT_DSALW_UPDT.alias("DRVR").join(
    df_hf_clmln_remit_dsalw_updt.alias("clm_ln_to_update"),
    F.col("DRVR.CLM_LN_SK") == F.col("clm_ln_to_update.CLM_LN_SK"),
    "left"
)

df_update_clm_ln = df_Copy_of_Transformer_401_joined.filter(
    F.col("clm_ln_to_update.CLM_LN_SK").isNotNull()
)

df_update_clm_ln_final = df_update_clm_ln.select(
    F.col("clm_ln_to_update.CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("clm_ln_to_update.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("clm_ln_to_update.CLM_ID").alias("CLM_ID"),
    F.col("clm_ln_to_update.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("clm_ln_to_update.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("clm_ln_to_update.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("clm_ln_to_update.CLM_SK").alias("CLM_SK"),
    F.col("clm_ln_to_update.PROC_CD_SK").alias("PROC_CD_SK"),
    F.col("clm_ln_to_update.SVC_PROV_SK").alias("SVC_PROV_SK"),
    F.col("clm_ln_to_update.CLM_LN_DSALW_EXCD_SK").alias("CLM_LN_DSALW_EXCD_SK"),
    F.col("clm_ln_to_update.CLM_LN_EOB_EXCD_SK").alias("CLM_LN_EOB_EXCD_SK"),
    F.col("clm_ln_to_update.CLM_LN_FINL_DISP_CD_SK").alias("CLM_LN_FINL_DISP_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_LOB_CD_SK").alias("CLM_LN_LOB_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_POS_CD_SK").alias("CLM_LN_POS_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_PREAUTH_CD_SK").alias("CLM_LN_PREAUTH_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_PREAUTH_SRC_CD_SK").alias("CLM_LN_PREAUTH_SRC_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_PRICE_SRC_CD_SK").alias("CLM_LN_PRICE_SRC_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_RFRL_CD_SK").alias("CLM_LN_RFRL_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_RVNU_CD_SK").alias("CLM_LN_RVNU_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_ROOM_PRICE_METH_CD_SK").alias("CLM_LN_ROOM_PRICE_METH_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_ROOM_TYP_CD_SK").alias("CLM_LN_ROOM_TYP_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_TOS_CD_SK").alias("CLM_LN_TOS_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_UNIT_TYP_CD_SK").alias("CLM_LN_UNIT_TYP_CD_SK"),
    F.rpad(F.col("clm_ln_to_update.CAP_LN_IN"), 1, " ").alias("CAP_LN_IN"),
    F.rpad(F.col("clm_ln_to_update.PRI_LOB_IN"), 1, " ").alias("PRI_LOB_IN"),
    F.rpad(F.col("clm_ln_to_update.SVC_END_DT_SK"), 10, " ").alias("SVC_END_DT_SK"),
    F.rpad(F.col("clm_ln_to_update.SVC_STRT_DT_SK"), 10, " ").alias("SVC_STRT_DT_SK"),
    F.col("clm_ln_to_update.AGMNT_PRICE_AMT").alias("AGMNT_PRICE_AMT"),
    F.col("clm_ln_to_update.ALW_AMT").alias("ALW_AMT"),
    F.col("clm_ln_to_update.CHRG_AMT").alias("CHRG_AMT"),
    F.col("clm_ln_to_update.COINS_AMT").alias("COINS_AMT"),
    F.col("clm_ln_to_update.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("clm_ln_to_update.COPAY_AMT").alias("COPAY_AMT"),
    F.col("clm_ln_to_update.DEDCT_AMT").alias("DEDCT_AMT"),
    (F.col("clm_ln_to_update.DSALW_AMT") + F.col("DRVR.DIFF_REMIT_DSALW_AMT")).alias("DSALW_AMT"),
    F.col("clm_ln_to_update.ITS_HOME_DSCNT_AMT").alias("ITS_HOME_DSCNT_AMT"),
    F.col("clm_ln_to_update.NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("clm_ln_to_update.MBR_LIAB_BSS_AMT").alias("MBR_LIAB_BSS_AMT"),
    F.col("clm_ln_to_update.PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("clm_ln_to_update.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("clm_ln_to_update.PAYBL_TO_PROV_AMT").alias("PAYBL_TO_PROV_AMT"),
    F.col("clm_ln_to_update.PAYBL_TO_SUB_AMT").alias("PAYBL_TO_SUB_AMT"),
    F.col("clm_ln_to_update.PROC_TBL_PRICE_AMT").alias("PROC_TBL_PRICE_AMT"),
    F.col("clm_ln_to_update.PROFL_PRICE_AMT").alias("PROFL_PRICE_AMT"),
    (F.col("clm_ln_to_update.PROV_WRT_OFF_AMT") + F.col("DRVR.DIFF_REMIT_DSALW_AMT")).alias("PROV_WRT_OFF_AMT"),
    F.col("clm_ln_to_update.RISK_WTHLD_AMT").alias("RISK_WTHLD_AMT"),
    F.col("clm_ln_to_update.SVC_PRICE_AMT").alias("SVC_PRICE_AMT"),
    F.col("clm_ln_to_update.SUPLMT_DSCNT_AMT").alias("SUPLMT_DSCNT_AMT"),
    F.col("clm_ln_to_update.ALW_PRICE_UNIT_CT").alias("ALW_PRICE_UNIT_CT"),
    F.col("clm_ln_to_update.UNIT_CT").alias("UNIT_CT"),
    F.col("clm_ln_to_update.DEDCT_AMT_ACCUM_ID").alias("DEDCT_AMT_ACCUM_ID"),
    F.col("clm_ln_to_update.PREAUTH_SVC_SEQ_NO").alias("PREAUTH_SVC_SEQ_NO"),
    F.col("clm_ln_to_update.RFRL_SVC_SEQ_NO").alias("RFRL_SVC_SEQ_NO"),
    F.col("clm_ln_to_update.LMT_PFX_ID").alias("LMT_PFX_ID"),
    F.col("clm_ln_to_update.PREAUTH_ID").alias("PREAUTH_ID"),
    F.col("clm_ln_to_update.PROD_CMPNT_DEDCT_PFX_ID").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("clm_ln_to_update.PROD_CMPNT_SVC_PAYMT_ID").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("clm_ln_to_update.RFRL_ID_TX").alias("RFRL_ID_TX"),
    F.col("clm_ln_to_update.SVC_ID").alias("SVC_ID"),
    F.rpad(F.col("clm_ln_to_update.SVC_PRICE_RULE_ID"), 4, " ").alias("SVC_PRICE_RULE_ID"),
    F.col("clm_ln_to_update.SVC_RULE_TYP_TX").alias("SVC_RULE_TYP_TX"),
    F.col("clm_ln_to_update.CLM_LN_SVC_LOC_TYP_CD_SK").alias("CLM_LN_SVC_LOC_TYP_CD_SK"),
    F.col("clm_ln_to_update.CLM_LN_SVC_PRICE_RULE_CD_SK").alias("CLM_LN_SVC_PRICE_RULE_CD_SK"),
    F.col("clm_ln_to_update.NON_PAR_SAV_AMT").alias("NON_PAR_SAV_AMT"),
    F.col("clm_ln_to_update.VBB_RULE_SK").alias("VBB_RULE_SK"),
    F.col("clm_ln_to_update.VBB_EXCD_SK").alias("VBB_EXCD_SK"),
    F.rpad(F.col("clm_ln_to_update.CLM_LN_VBB_IN"), 1, " ").alias("CLM_LN_VBB_IN"),
    F.col("clm_ln_to_update.ITS_SUPLMT_DSCNT_AMT").alias("ITS_SUPLMT_DSCNT_AMT"),
    F.col("clm_ln_to_update.ITS_SRCHRG_AMT").alias("ITS_SRCHRG_AMT"),
    F.col("clm_ln_to_update.NDC_SK").alias("NDC_SK"),
    F.col("clm_ln_to_update.NDC_DRUG_FORM_CD_SK").alias("NDC_DRUG_FORM_CD_SK"),
    F.col("clm_ln_to_update.NDC_UNIT_CT").alias("NDC_UNIT_CT")
)

# Stage: CLM_LN_IDSRemitDsalwUpdt_dat (CSeqFileStage - Write)
write_files(
    df_update_clm_ln_final,
    f"{adls_path}/load/CLM_LN.IDSRemitDsalwUpdt.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)