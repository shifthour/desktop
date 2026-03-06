# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *******************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    PSJrnlEntryComExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from a flat file (GL_COM_DTL_TEMP) to common directory
# MAGIC   Currently, no need to do extract since data is already in flat file 
# MAGIC 
# MAGIC *******NOTE:  Flat file is pulled from /landing/ directory, not /verified/.  
# MAGIC 
# MAGIC     
# MAGIC INPUTS:
# MAGIC 	GL_COM_DTL_TEMP
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC 
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
# MAGIC               Steph Goddard    11/11/2004   Moved PRODUCT field to FNCL_LOB_SK instead of PDBL_ACCT_CAT.  PRODUCT is not product as we know it, but the financial line of business.  It is populated differently in PRODUCT than it is in PDBL_ACCT_CAT.
# MAGIC             Steph Goddard    7/2005       Changes for sequencer
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                  5/9/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard            09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                                2008-08-19        3567(Primary Key)        Added the primary key process to the job                devlIDS                       Steph Goddard            08/21/2008
# MAGIC Steph Goddard                02/27/2009      prod supp                     changed format date call                                          devlIDS
# MAGIC 
# MAGIC T.Sieg                              2013-02-12        4701                           Changed date format in svACCTGDT in                    IDSDevl                      Bhoomi Dasari             3/28/2013    
# MAGIC                                                                                                      BusinessRules and Transform
# MAGIC                                                                                                      transformer to get required format for IDS tables
# MAGIC                                                                                                      Replacing Sequential file stage with ODBC stage
# MAGIC                                                                                                     to extract GL transactions from SYBASE
# MAGIC                                                                                                     Adding $BCBSFIN parameters to use in ODBC stage
# MAGIC 
# MAGIC Sudheer                            2017-09-11             5599                      Modiifed the Source Sql to pull the workday data  IntegrateDev2            Jag Yelavarthi               2017-09-12
# MAGIC 
# MAGIC T.Sieg                            2022-06-11         S2S Remediation    Added ODBC connetor stages for extract stages           IntegrateDev5          Jeyaprasanna               2022-06-12

# MAGIC Pulling PeopleSoft data from flat files
# MAGIC Hash file (hf_jrnl_entry_trans_allcol) cleared in calling program
# MAGIC SYBASE table with the Commission GL transactions
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
# MAGIC %run ../../../../../shared_containers/PrimaryKey/JrnlEntryTransPK
# COMMAND ----------

from pyspark.sql.functions import col, lit, when, length, concat, to_date, rpad
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
BCBSFINOwner = get_widget_value("BCBSFINOwner","")
bcbsfin_secret_name = get_widget_value("bcbsfin_secret_name","")
TmpOutFile = get_widget_value("TmpOutFile","")
InFile = get_widget_value("InFile","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
CurrDate = get_widget_value("CurrDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

jdbc_url_bcbsfin, jdbc_props_bcbsfin = get_db_config(bcbsfin_secret_name)

extract_query_1 = """SELECT GL_COM_DTL_CK,
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
 BLCT_CK, 
BLEI_BILL_LEVEL, 
BLEI_BILL_LEVEL_CK, 
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
FROM BCBSFIN..GL_COM_DTL_BCP
"""

df_GL_Comsn_BCP_table = (
  spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option("query", extract_query_1)
    .load()
)

df_BusinessRules_temp = df_GL_Comsn_BCP_table.withColumn("SnapActDt", col("SNAP_ACT_DT").cast("date"))

df_BusinessRules_AllCol = (
  df_BusinessRules_temp
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .withColumn("SRC_TRANS_CK", col("GL_COM_DTL_CK"))
    .withColumn("SRC_TRANS_TYP_CD", lit("COMSN"))
    .withColumn("ACCTG_DT_SK", col("SnapActDt"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", lit(CurrDate))
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", lit(SrcSysCd))
    .withColumn(
      "PRI_KEY_STRING",
      concat(
        lit(SrcSysCd),
        lit(";"),
        col("GL_COM_DTL_CK"),
        lit(";"),
        lit("COMSN"),
        lit(";"),
        col("SnapActDt")
      )
    )
    .withColumn("JRNL_ENTRY_TRANS_SK", lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("TRANS_TBL_SK", lit(0))
    .withColumn(
      "FNCL_LOB",
      when(length(trim(col("PRODUCT"))) == 0, lit("NA")).otherwise(col("PRODUCT"))
    )
    .withColumn(
      "JRNL_ENTRY_TRANS_DR_CR_CD",
      when(length(trim(col("DR_CR_CD"))) == 0, lit("NA")).otherwise(col("DR_CR_CD"))
    )
    .withColumn("DIST_GL_IN", col("MAP_IN"))
    .withColumn("JRNL_ENTRY_TRANS_AMT", col("TX_AMT"))
    .withColumn("TRANS_LN_NO", col("TRANSACTION_LINE"))
    .withColumn("ACCT_NO", col("WD_ACCT_NO"))
    .withColumn(
      "AFFILIAT_NO",
      when(col("WD_AFFIL_ID").isNull(), lit("    ")).otherwise(col("WD_AFFIL_ID"))
    )
    .withColumn("APPL_JRNL_ID", col("APPL_JRNL_ID"))
    .withColumn("BUS_UNIT_GL_NO", col("BUSINESS_UNIT_GL"))
    .withColumn("BUS_UNIT_NO", col("BUSINESS_UNIT"))
    .withColumn(
      "CC_ID",
      when(col("WD_CC_ID").isNull(), lit("    ")).otherwise(col("WD_CC_ID"))
    )
    .withColumn("JRNL_LN_DESC", col("LINE_DESCR"))
    .withColumn("JRNL_LN_REF_NO", col("JRNL_LN_REF"))
    .withColumn(
      "OPR_UNIT_NO",
      when(col("WD_DNR_ID").isNull(), lit("    ")).otherwise(col("WD_DNR_ID"))
    )
    .withColumn(
      "SUB_ACCT_NO",
      when(col("WD_CUST_ID").isNull(), lit("    ")).otherwise(col("WD_CUST_ID"))
    )
    .withColumn("TRANS_ID", col("TRANSACTION_ID"))
    .withColumn("PRCS_MAP_1_TX", col("MAP_FLD_1_CD"))
    .withColumn("PRCS_MAP_2_TX", col("MAP_FLD_2_CD"))
    .withColumn("PRCS_MAP_3_TX", col("MAP_FLD_3_CD"))
    .withColumn("PRCS_MAP_4_TX", col("MAP_FLD_4_CD"))
    .withColumn("PRCS_SUB_SRC_CD", col("FA_SUB_SRC_CD"))
    .withColumn("CLCL_ID", lit(" "))
    .withColumn("WD_LOB_CD", col("WD_LOB_CD"))
    .withColumn("WD_BANK_ACCT_NM", col("WD_BANK_ACCT_NM"))
    .withColumn("WD_RVNU_CAT_ID", col("WD_RVNU_CAT_ID"))
    .withColumn("WD_SPEND_CAT_ID", col("WD_SPEND_CAT_ID"))
)

df_BusinessRules_Transform = (
  df_BusinessRules_temp
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .withColumn("SRC_TRANS_CK", col("GL_COM_DTL_CK"))
    .withColumn("SRC_TRANS_TYP_CD", lit("COMSN"))
    .withColumn("ACCTG_DT_SK", col("SnapActDt"))
    .select(
      "SRC_SYS_CD_SK",
      "SRC_TRANS_CK",
      "SRC_TRANS_TYP_CD",
      "ACCTG_DT_SK"
    )
)

params_JrnlEntryTransPK = {
  "IDSOwner": IDSOwner,
  "TmpOutFile": TmpOutFile,
  "InFile": InFile,
  "CurrRunCycle": CurrRunCycle,
  "CurrDate": CurrDate,
  "SrcSysCd": SrcSysCd,
  "SrcSysCdSk": SrcSysCdSk
}

df_JrnlEntryTransPK = JrnlEntryTransPK(df_BusinessRules_Transform, df_BusinessRules_AllCol, params_JrnlEntryTransPK)

df_IdsJrnlEntryComsnExtr = df_JrnlEntryTransPK.select(
  col("JOB_EXCTN_RCRD_ERR_SK"),
  rpad(col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
  rpad(col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
  rpad(col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
  col("FIRST_RECYC_DT"),
  col("ERR_CT"),
  col("RECYCLE_CT"),
  col("SRC_SYS_CD"),
  col("PRI_KEY_STRING"),
  col("JRNL_ENTRY_TRANS_SK"),
  col("SRC_TRANS_CK"),
  col("SRC_TRANS_TYP_CD"),
  rpad(col("ACCTG_DT_SK"),10," ").alias("ACCTG_DT_SK"),
  col("CRT_RUN_CYC_EXCTN_SK"),
  col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
  col("TRANS_TBL_SK"),
  rpad(col("FNCL_LOB"),10," ").alias("FNCL_LOB"),
  rpad(col("JRNL_ENTRY_TRANS_DR_CR_CD"),2," ").alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
  rpad(col("DIST_GL_IN"),1," ").alias("DIST_GL_IN"),
  col("JRNL_ENTRY_TRANS_AMT"),
  col("TRANS_LN_NO"),
  rpad(col("ACCT_NO"),10," ").alias("ACCT_NO"),
  rpad(col("AFFILIAT_NO"),5," ").alias("AFFILIAT_NO"),
  rpad(col("APPL_JRNL_ID"),10," ").alias("APPL_JRNL_ID"),
  rpad(col("BUS_UNIT_GL_NO"),5," ").alias("BUS_UNIT_GL_NO"),
  rpad(col("BUS_UNIT_NO"),5," ").alias("BUS_UNIT_NO"),
  rpad(col("CC_ID"),10," ").alias("CC_ID"),
  rpad(col("JRNL_LN_DESC"),30," ").alias("JRNL_LN_DESC"),
  rpad(col("JRNL_LN_REF_NO"),10," ").alias("JRNL_LN_REF_NO"),
  rpad(col("OPR_UNIT_NO"),8," ").alias("OPR_UNIT_NO"),
  rpad(col("SUB_ACCT_NO"),10," ").alias("SUB_ACCT_NO"),
  rpad(col("TRANS_ID"),10," ").alias("TRANS_ID"),
  rpad(col("PRCS_MAP_1_TX"),4," ").alias("PRCS_MAP_1_TX"),
  rpad(col("PRCS_MAP_2_TX"),4," ").alias("PRCS_MAP_2_TX"),
  rpad(col("PRCS_MAP_3_TX"),4," ").alias("PRCS_MAP_3_TX"),
  rpad(col("PRCS_MAP_4_TX"),4," ").alias("PRCS_MAP_4_TX"),
  rpad(col("PRCS_SUB_SRC_CD"),5," ").alias("PRCS_SUB_SRC_CD"),
  rpad(col("CLCL_ID"),12," ").alias("CLCL_ID"),
  col("WD_LOB_CD"),
  col("WD_BANK_ACCT_NM"),
  col("WD_RVNU_CAT_ID"),
  col("WD_SPEND_CAT_ID")
)

write_files(
  df_IdsJrnlEntryComsnExtr,
  f"{adls_path}/key/{TmpOutFile}",
  ",",
  "overwrite",
  False,
  False,
  "\"",
  None
)

extract_query_2 = """SELECT GL_COM_DTL_CK,
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
 BLCT_CK, 
BLEI_BILL_LEVEL, 
BLEI_BILL_LEVEL_CK, 
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
FROM BCBSFIN..GL_COM_DTL_BCP
"""

df_GL_Comsn_BCP_table1 = (
  spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option("query", extract_query_2)
    .load()
)

df_Transform_temp = (
  df_GL_Comsn_BCP_table1
    .withColumn("svACCTDT", col("SNAP_ACT_DT").cast("date"))
    .withColumn("svAcctDtSk", GetFkeyDate("IDS", 100, col("svACCTDT"), "X"))
    .withColumn("svSrcTransTypCdSk", GetFkeyCodes("FACETS", 110, "SOURCE TRANSACTION TYPE CODE", "COMSN", "X"))
)

df_Transform_final = (
  df_Transform_temp
    .withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk))
    .withColumn("SRC_TRANS_CK", col("GL_COM_DTL_CK"))
    .withColumn("SRC_TRANS_TYP_CD_SK", col("svSrcTransTypCdSk"))
    .withColumn("ACCTG_DT_SK", col("svAcctDtSk"))
    .select(
      "SRC_SYS_CD_SK",
      "SRC_TRANS_CK",
      "SRC_TRANS_TYP_CD_SK",
      "ACCTG_DT_SK"
    )
)

df_Snapshot_File = df_Transform_final.select(
  col("SRC_SYS_CD_SK"),
  col("SRC_TRANS_CK"),
  col("SRC_TRANS_TYP_CD_SK"),
  rpad(col("ACCTG_DT_SK"),10," ").alias("ACCTG_DT_SK")
)

write_files(
  df_Snapshot_File,
  f"{adls_path}/load/B_JRNL_ENTRY_TRANS.dat",
  ",",
  "overwrite",
  False,
  False,
  "\"",
  None
)