# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC **********************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    PSJrnlEntryIncmExtr
# MAGIC CALLED BY:  PSJrnlEntryExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from a flat file (GL_INCOME_DTL_TEMP) to common directory
# MAGIC   Currently, no need to do extract since data is already in flat file comma delimited.
# MAGIC 
# MAGIC *******NOTE:  Flat file is pulled from /landing/ directory, not /verified/.  
# MAGIC 
# MAGIC     
# MAGIC INPUTS:
# MAGIC 	GL_INCOME_DTL_TEMP
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                   FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Create file in common directory for creating jrnl_entry_trans and jrnl_entry_prcs files in IDS
# MAGIC                   Both files are fed through transform, primary, and foreign key programs together - the files are split out in the foreign key program.
# MAGIC                   The primary key for both tables is the same.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Steph Goddard    10/2004-   Originally Programmed
# MAGIC              Steph Goddard    11/11/2004   Moved PRODUCT field to FNCL_LOB_SK instead of PDBL_ACCT_CAT.  PRODUCT is not product as we know it, but the financial line of business.  It is populated differently in PRODUCT than it is in PDBL_ACCT_CAT.
# MAGIC              Steph Goddard    07/18/2005   Changes for sequencer
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                 5/9/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard             09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                               2008-08-19      3567(Primary Key)          Added primary key process to the job                       devlIDS                      Steph Goddard             08/22/2008
# MAGIC Steph Goddard               02/27/2009    prod supp                       changed format date call                                          devlIDS
# MAGIC 
# MAGIC T.Sieg                            2013-02-01           4701                          Changed date format in svACCTGDT in                   IDSDevl                      Bhoomi Dasari               3/28/2013
# MAGIC                                                                                                      BusinessRules and Transform
# MAGIC 	                                                                                     transformer to get required format for IDS tables
# MAGIC                                                                                                      Replacing Sequential file stage with ODBC stage
# MAGIC                                                                                                      to extract GL transactions from SYBASE
# MAGIC                                                                                                      Adding $BCBSFIN parameters to use in ODBC stage
# MAGIC 
# MAGIC Sudheer                            2017-09-11             5599                      Modiifed the Source Sql to pull the workday data  IntegrateDev2           Jag Yelavarthi                2017-09-12
# MAGIC 
# MAGIC Tim Sieg		      2018-02-01	   5599		       Removed the Data Elements in the job.	     IntegrateDev1
# MAGIC 
# MAGIC T.Sieg                  2022-05-15      S2S Remediation        Replaced extract stages with ODBC connector                  IntegrateDev5	Ken Bradmon	2022-06-07

# MAGIC Pulling PeopleSoft data from flat files
# MAGIC Hash file (hf_jrnl_entry_trans_allcol) cleared in calling program
# MAGIC SYBASE table with the Income GL transactions
# MAGIC This container is used in:
# MAGIC PSJrnlEntryCapExtr
# MAGIC PSJrnlEntryClmExtr
# MAGIC PSJrnlEntryComsnExtr
# MAGIC PSJrnlEntryDrugExtr
# MAGIC PSJrnlEntryIncmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Apply business logic
# MAGIC Balancing snapshot of source file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, concat_ws, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
InFile = get_widget_value('InFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/JrnlEntryTransPK
# COMMAND ----------

# GL_Incm_BCP_table
jdbc_url_GL_Incm_BCP_table, jdbc_props_GL_Incm_BCP_table = get_db_config(bcbsfin_secret_name)
extract_query_GL_Incm_BCP_table = """SELECT GL_INC_DTL_CK,
 LOBD_ID,
 PDBL_ACCT_CAT,
 SNAP_SRC_CD,
 FA_SUB_SRC_CD, 
MAP_FLD_1_CD, 
MAP_FLD_2_CD, 
MAP_FLD_3_CD, 
MAP_FLD_4_CD,
 MAP_EFF_DT, 
PAGR_ID, 
GRGR_ID, 
GL_CAT_CD, 
FA_ACCT_NO, 
TX_AMT, 
SNAP_ACT_DT, 
DR_CR_CD, 
BLAC_POSTING_DT, 
PDPD_ID, 
PDBL_ID,
 BLEI_CK, 
BLBL_DUE_DT, 
BLAC_DEBIT_AMT, 
BLAC_CREDIT_AMT, 
BLAC_YR1_IND, 
BLAC_CREATE_DTM, 
GRGR_NAME, 
SGSG_ID, 
LOBD_NAME, 
ORIG_ACTG_CAT_CD, 
RVSED_ACTG_CAT_CD, 
MAP_IN, 
BUSINESS_UNIT, 
TRANSACTION_ID, 
TRANSACTION_LINE, 
ACCOUNTING_DT, 
APPL_JRNL_ID, 
BUSINESS_UNIT_GL, 
ACCOUNT,
 DEPTID, 
OPERATING_UNIT, 
PRODUCT, 
AFFILIATE, 
CHARTFIELD1, 
CHARTFIELD2, 
CHARTFIELD3, 
PROJECT_ID, 
CURRENCY_CD, 
RT_TYPE, 
MONETARY_AMOUNT, 
FOREIGN_AMOUNT, 
JRNL_LN_REF, 
LINE_DESCR,
GL_DISTRIB_STATUS,
 WD_LOB_CD,
 WD_BANK_ACCT_NM,
 WD_RVNU_CAT_ID,
 WD_SPEND_CAT_ID,
WD_CC_ID,
WD_DNR_ID,
WD_AFFIL_ID,
WD_CUST_ID,
WD_ACCT_NO
FROM BCBSFIN..GL_INCOME_DTL_BCP
"""
df_GL_Incm_BCP_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_GL_Incm_BCP_table)
    .options(**jdbc_props_GL_Incm_BCP_table)
    .option("query", extract_query_GL_Incm_BCP_table)
    .load()
)

# BusinessRules with stage variable SnapActDt
df_BusinessRules = df_GL_Incm_BCP_table.withColumn(
    "SnapActDt",
    FormatDate(col("SNAP_ACT_DT"), "SYBASE", "TIMESTAMP", "DATE")
)

# Output pin "AllCol" (43 columns)
df_BusinessRules_AllCol = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_INC_DTL_CK").alias("SRC_TRANS_CK"),
    lit("INCM").alias("SRC_TRANS_TYP_CD"),
    rpad(col("SnapActDt"), 10, " ").alias("ACCTG_DT_SK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat_ws(";", lit(SrcSysCd), col("GL_INC_DTL_CK"), lit("INCM"), col("SnapActDt")).alias("PRI_KEY_STRING"),
    lit(0).alias("JRNL_ENTRY_TRANS_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("TRANS_TBL_SK"),
    rpad(when(length(trim(col("PRODUCT"))) == 0, lit("NA")).otherwise(col("PRODUCT")), 4, " ").alias("FNCL_LOB"),
    rpad(when(length(trim(col("DR_CR_CD"))) == 0, lit("NA")).otherwise(col("DR_CR_CD")), 2, " ").alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    rpad(col("MAP_IN"), 1, " ").alias("DIST_GL_IN"),
    col("TX_AMT").alias("JRNL_ENTRY_TRANS_AMT"),
    col("TRANSACTION_LINE").alias("TRANS_LN_NO"),
    rpad(col("WD_ACCT_NO"), 10, " ").alias("ACCT_NO"),
    rpad(when(col("WD_AFFIL_ID").isNull(), lit("    ")).otherwise(col("WD_AFFIL_ID")), 5, " ").alias("AFFILIAT_NO"),
    rpad(col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    rpad(col("BUSINESS_UNIT_GL"), 5, " ").alias("BUS_UNIT_GL_NO"),
    rpad(col("BUSINESS_UNIT"), 5, " ").alias("BUS_UNIT_NO"),
    rpad(when(col("WD_CC_ID").isNull(), lit("    ")).otherwise(col("WD_CC_ID")), 10, " ").alias("CC_ID"),
    rpad(col("LINE_DESCR"), 30, " ").alias("JRNL_LN_DESC"),
    rpad(col("JRNL_LN_REF"), 10, " ").alias("JRNL_LN_REF_NO"),
    rpad(when(col("WD_DNR_ID").isNull(), lit("    ")).otherwise(col("WD_DNR_ID")), 8, " ").alias("OPR_UNIT_NO"),
    rpad(when(col("WD_CUST_ID").isNull(), lit("    ")).otherwise(col("WD_CUST_ID")), 10, " ").alias("SUB_ACCT_NO"),
    rpad(col("TRANSACTION_ID"), 10, " ").alias("TRANS_ID"),
    rpad(col("MAP_FLD_1_CD"), 4, " ").alias("PRCS_MAP_1_TX"),
    rpad(col("MAP_FLD_2_CD"), 4, " ").alias("PRCS_MAP_2_TX"),
    rpad(col("MAP_FLD_3_CD"), 4, " ").alias("PRCS_MAP_3_TX"),
    rpad(col("MAP_FLD_4_CD"), 4, " ").alias("PRCS_MAP_4_TX"),
    rpad(col("FA_SUB_SRC_CD"), 5, " ").alias("PRCS_SUB_SRC_CD"),
    rpad(lit(" "), 12, " ").alias("CLCL_ID"),
    col("WD_LOB_CD").alias("WD_LOB_CD"),
    col("WD_BANK_ACCT_NM").alias("WD_BANK_ACCT_NM"),
    col("WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    col("WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID")
)

# Output pin "Transform" (4 columns)
df_BusinessRules_Transform = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_INC_DTL_CK").alias("SRC_TRANS_CK"),
    lit("INCM").alias("SRC_TRANS_TYP_CD"),
    rpad(col("SnapActDt"), 10, " ").alias("ACCTG_DT_SK")
)

# Container: JrnlEntryTransPK
params_JrnlEntryTransPK = {
    "IDSOwner": IDSOwner,
    "TmpOutFile": TmpOutFile,
    "InFile": InFile,
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrDate,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}
df_JrnlEntryTransPK_Key = JrnlEntryTransPK(df_BusinessRules_Transform, df_BusinessRules_AllCol, params_JrnlEntryTransPK)

# IdsJrnlEntryIncmExtr
df_IdsJrnlEntryIncmExtr = df_JrnlEntryTransPK_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("JRNL_ENTRY_TRANS_SK"),
    col("SRC_TRANS_CK"),
    col("SRC_TRANS_TYP_CD"),
    col("ACCTG_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("TRANS_TBL_SK"),
    col("FNCL_LOB"),
    col("JRNL_ENTRY_TRANS_DR_CR_CD"),
    col("DIST_GL_IN"),
    col("JRNL_ENTRY_TRANS_AMT"),
    col("TRANS_LN_NO"),
    col("ACCT_NO"),
    col("AFFILIAT_NO"),
    col("APPL_JRNL_ID"),
    col("BUS_UNIT_GL_NO"),
    col("BUS_UNIT_NO"),
    col("CC_ID"),
    col("JRNL_LN_DESC"),
    col("JRNL_LN_REF_NO"),
    col("OPR_UNIT_NO"),
    col("SUB_ACCT_NO"),
    col("TRANS_ID"),
    col("PRCS_MAP_1_TX"),
    col("PRCS_MAP_2_TX"),
    col("PRCS_MAP_3_TX"),
    col("PRCS_MAP_4_TX"),
    col("PRCS_SUB_SRC_CD"),
    col("CLCL_ID"),
    col("WD_LOB_CD"),
    col("WD_BANK_ACCT_NM"),
    col("WD_RVNU_CAT_ID"),
    col("WD_SPEND_CAT_ID")
)
write_files(
    df_IdsJrnlEntryIncmExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

# GL_Incm_BCP_table1
jdbc_url_GL_Incm_BCP_table1, jdbc_props_GL_Incm_BCP_table1 = get_db_config(bcbsfin_secret_name)
extract_query_GL_Incm_BCP_table1 = """SELECT GL_INC_DTL_CK,
 LOBD_ID,
 PDBL_ACCT_CAT,
 SNAP_SRC_CD,
 FA_SUB_SRC_CD, 
MAP_FLD_1_CD, 
MAP_FLD_2_CD, 
MAP_FLD_3_CD, 
MAP_FLD_4_CD,
 MAP_EFF_DT, 
PAGR_ID, 
GRGR_ID, 
GL_CAT_CD, 
FA_ACCT_NO, 
TX_AMT, 
SNAP_ACT_DT, 
DR_CR_CD, 
BLAC_POSTING_DT, 
PDPD_ID, 
PDBL_ID,
 BLEI_CK, 
BLBL_DUE_DT, 
BLAC_DEBIT_AMT, 
BLAC_CREDIT_AMT, 
BLAC_YR1_IND, 
BLAC_CREATE_DTM, 
GRGR_NAME, 
SGSG_ID, 
LOBD_NAME, 
ORIG_ACTG_CAT_CD, 
RVSED_ACTG_CAT_CD, 
MAP_IN, 
BUSINESS_UNIT, 
TRANSACTION_ID, 
TRANSACTION_LINE, 
ACCOUNTING_DT, 
APPL_JRNL_ID, 
BUSINESS_UNIT_GL, 
ACCOUNT,
 DEPTID, 
OPERATING_UNIT, 
PRODUCT, 
AFFILIATE, 
CHARTFIELD1, 
CHARTFIELD2, 
CHARTFIELD3, 
PROJECT_ID, 
CURRENCY_CD, 
RT_TYPE, 
MONETARY_AMOUNT, 
FOREIGN_AMOUNT, 
JRNL_LN_REF, 
LINE_DESCR,
GL_DISTRIB_STATUS
FROM BCBSFIN..GL_INCOME_DTL_BCP
"""
df_GL_Incm_BCP_table1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_GL_Incm_BCP_table1)
    .options(**jdbc_props_GL_Incm_BCP_table1)
    .option("query", extract_query_GL_Incm_BCP_table1)
    .load()
)

# Transform stage
df_Transform_StageVars = df_GL_Incm_BCP_table1.withColumn(
    "svACCTDT", FormatDate(col("SNAP_ACT_DT"), "SYBASE", "TIMESTAMP", "DATE")
).withColumn(
    "svAcctDtSk", GetFkeyDate("IDS", lit(100), col("svACCTDT"), "X")
).withColumn(
    "svSrcTransTypCdSk", GetFkeyCodes("FACETS", lit(110), "SOURCE TRANSACTION TYPE CODE", "INCM", "X")
)

df_Transform_Output = df_Transform_StageVars.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_INC_DTL_CK").alias("SRC_TRANS_CK"),
    col("svSrcTransTypCdSk").alias("SRC_TRANS_TYP_CD_SK"),
    rpad(col("svAcctDtSk"), 10, " ").alias("ACCTG_DT_SK")
)

# Snapshot_File
df_Snapshot_File = df_Transform_Output.select(
    "SRC_SYS_CD_SK",
    "SRC_TRANS_CK",
    "SRC_TRANS_TYP_CD_SK",
    "ACCTG_DT_SK"
)
write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_JRNL_ENTRY_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)