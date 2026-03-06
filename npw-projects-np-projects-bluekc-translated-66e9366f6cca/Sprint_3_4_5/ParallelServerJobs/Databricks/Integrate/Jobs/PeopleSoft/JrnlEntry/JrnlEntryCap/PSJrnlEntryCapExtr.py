# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    PSJrnlEntryCapExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from a flat file (GL_CAP_DTL_TEMP) to common directory
# MAGIC   Currently, no need to do extract since data is already in flat file comma delimited.
# MAGIC 
# MAGIC *******NOTE:  Flat file is pulled from /landing/ directory, not /verified/.  
# MAGIC 
# MAGIC     
# MAGIC INPUTS:
# MAGIC 	GL_CAP_DTL_TEMP
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
# MAGIC               Steph Goddard    11/11/2004   Moved PRODUCT field to FNCL_LOB_SK instead of PDBL_ACCT_CAT.  PRODUCT is not product as we know it, but the financial line of business.   It is populated differently in PRODUCT than it is in PDBL_ACCT_CAT.
# MAGIC              Steph Goddard   7/13/2005    Changes for sequencer
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                 5/8/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard             09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                               2008-08-19         3567(Primary Key)        Added Primary Key process to the job                      devlIDS                     Steph Goddard              08/21/2008
# MAGIC Steph Goddard               02/27/2009       prod supp                     changed format date call                                           devlIDS
# MAGIC 
# MAGIC T. Sieg                            2013-01-18          4701                          Changed date format in svACCTGDT in                      IDSDevl                  Bhoomi Dasari               3/28/2013
# MAGIC                                                                                                      BusinessRules and Transform transformer to
# MAGIC                                                                                                      get required format for IDS tables
# MAGIC                                                                                                      Replacing Sequential file stage with ODBC stage
# MAGIC                                                                                                       to extract GL transactions from SYBASE
# MAGIC                                                                                                       Adding $BCBSFIN parameters to use in ODBC stage
# MAGIC  
# MAGIC Sudheer                            2017-09-11             5599                      Modiifed the Source Sql to pull the workday data  IntegrateDev2           Jag Yelavarthi               2017-09-12
# MAGIC 
# MAGIC T. Sieg                             2017-10-16             5599                     Correcting Business Rules for JRNL_LN_REF_NO         IntegrateDev2      Jag Yelavarthi             2017-11-29
# MAGIC                                                                                                      replacing PRODUCT with JRNL_LN_REF
# MAGIC                                                                                                       from source

# MAGIC Pulling PeopleSoft data from flat files
# MAGIC This container is used in:
# MAGIC PSJrnlEntryCapExtr
# MAGIC PSJrnlEntryClmExtr
# MAGIC PSJrnlEntryComsnExtr
# MAGIC PSJrnlEntryDrugExtr
# MAGIC PSJrnlEntryIncmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_jrnl_entry_trans_allcol) cleared in calling program
# MAGIC SYBASE table with the Capitation GL transactions
# MAGIC Apply business logic
# MAGIC Writing Sequential File to /key
# MAGIC Balancing snapshot of source file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
InFile = get_widget_value('InFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url_bcbsfin, jdbc_props_bcbsfin = get_db_config(bcbsfin_secret_name)

df_GL_Cap_BCP_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option(
        "query",
        """SELECT GL_CAP_DTL_CK,
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
 CRME_FUND_AMT,
 CRME_PYMT_METH_IND,
 GRGR_CK,
 CRME_FUND_RATE,
 CRME_PAY_FROM_DT,
 CRME_PAY_THRU_DT,
 PDPD_ID,
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
 FROM #$BCBSFINDB#..GL_CAP_DTL_BCP;"""
    )
    .load()
)

df_BusinessRules = df_GL_Cap_BCP_table.withColumn(
    "svACCTGDT",
    FORMAT_DATE(F.col("SNAP_ACT_DT"), "SYBASE", "TIMESTAMP", "DATE")
)

df_BusinessRulesAllCol = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("GL_CAP_DTL_CK").alias("SRC_TRANS_CK"),
    F.lit("CAP").alias("SRC_TRANS_TYP_CD"),
    F.col("svACCTGDT").alias("ACCTG_DT_SK"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("GL_CAP_DTL_CK"), F.lit(";"), F.lit("CAP"), F.lit(";"), F.col("svACCTGDT")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("JRNL_ENTRY_TRANS_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("TRANS_TBL_SK"),
    F.when(F.length(trim(F.col("PRODUCT"))) == 0, F.lit("NA")).otherwise(F.col("PRODUCT")).alias("FNCL_LOB"),
    F.when(F.length(trim(F.col("DR_CR_CD"))) == 0, F.lit("NA")).otherwise(F.col("DR_CR_CD")).alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    F.col("MAP_IN").alias("DIST_GL_IN"),
    F.col("TX_AMT").alias("JRNL_ENTRY_TRANS_AMT"),
    F.col("TRANSACTION_LINE").alias("TRANS_LN_NO"),
    F.col("WD_ACCT_NO").alias("ACCT_NO"),
    F.when(F.col("WD_AFFIL_ID").isNull(), F.lit("    ")).otherwise(F.col("WD_AFFIL_ID")).alias("AFFILIAT_NO"),
    F.col("APPL_JRNL_ID").alias("APPL_JRNL_ID"),
    F.col("BUSINESS_UNIT_GL").alias("BUS_UNIT_GL_NO"),
    F.col("BUSINESS_UNIT").alias("BUS_UNIT_NO"),
    F.when(F.col("WD_CC_ID").isNull(), F.lit("    ")).otherwise(F.col("WD_CC_ID")).alias("CC_ID"),
    F.col("LINE_DESCR").alias("JRNL_LN_DESC"),
    F.col("JRNL_LN_REF").alias("JRNL_LN_REF_NO"),
    F.when(F.col("WD_DNR_ID").isNull(), F.lit("    ")).otherwise(F.col("WD_DNR_ID")).alias("OPR_UNIT_NO"),
    F.when(F.col("WD_CUST_ID").isNull(), F.lit("    ")).otherwise(F.col("WD_CUST_ID")).alias("SUB_ACCT_NO"),
    F.col("TRANSACTION_ID").alias("TRANS_ID"),
    F.col("MAP_FLD_1_CD").alias("PRCS_MAP_1_TX"),
    F.col("MAP_FLD_2_CD").alias("PRCS_MAP_2_TX"),
    F.col("MAP_FLD_3_CD").alias("PRCS_MAP_3_TX"),
    F.col("MAP_FLD_4_CD").alias("PRCS_MAP_4_TX"),
    F.col("FA_SUB_SRC_CD").alias("PRCS_SUB_SRC_CD"),
    F.lit(" ").alias("CLCL_ID"),
    F.col("WD_LOB_CD").alias("WD_LOB_CD"),
    F.col("WD_BANK_ACCT_NM").alias("WD_BANK_ACCT_NM"),
    F.col("WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    F.col("WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID")
)

df_BusinessRulesTransform = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("GL_CAP_DTL_CK").alias("SRC_TRANS_CK"),
    F.lit("CAP").alias("SRC_TRANS_TYP_CD"),
    F.col("svACCTGDT").alias("ACCTG_DT_SK")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/JrnlEntryTransPK
# COMMAND ----------

params_JrnlEntryTransPK = {
    "IDSOwner": IDSOwner,
    "TmpOutFile": TmpOutFile,
    "InFile": InFile,
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrDate,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}

df_IdsJrnlEntryCapExtr = JrnlEntryTransPK(df_BusinessRulesTransform, df_BusinessRulesAllCol, params_JrnlEntryTransPK)

df_IdsJrnlEntryCapExtr_select = df_IdsJrnlEntryCapExtr.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("JRNL_ENTRY_TRANS_SK"),
    F.col("SRC_TRANS_CK"),
    F.col("SRC_TRANS_TYP_CD"),
    F.rpad(F.col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TRANS_TBL_SK"),
    F.rpad(F.col("FNCL_LOB"), 10, " ").alias("FNCL_LOB"),
    F.rpad(F.col("JRNL_ENTRY_TRANS_DR_CR_CD"), 2, " ").alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    F.rpad(F.col("DIST_GL_IN"), 1, " ").alias("DIST_GL_IN"),
    F.col("JRNL_ENTRY_TRANS_AMT"),
    F.col("TRANS_LN_NO"),
    F.rpad(F.col("ACCT_NO"), 10, " ").alias("ACCT_NO"),
    F.rpad(F.col("AFFILIAT_NO"), 5, " ").alias("AFFILIAT_NO"),
    F.rpad(F.col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    F.rpad(F.col("BUS_UNIT_GL_NO"), 5, " ").alias("BUS_UNIT_GL_NO"),
    F.rpad(F.col("BUS_UNIT_NO"), 5, " ").alias("BUS_UNIT_NO"),
    F.rpad(F.col("CC_ID"), 10, " ").alias("CC_ID"),
    F.rpad(F.col("JRNL_LN_DESC"), 30, " ").alias("JRNL_LN_DESC"),
    F.rpad(F.col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO"),
    F.rpad(F.col("OPR_UNIT_NO"), 8, " ").alias("OPR_UNIT_NO"),
    F.rpad(F.col("SUB_ACCT_NO"), 10, " ").alias("SUB_ACCT_NO"),
    F.rpad(F.col("TRANS_ID"), 10, " ").alias("TRANS_ID"),
    F.rpad(F.col("PRCS_MAP_1_TX"), 4, " ").alias("PRCS_MAP_1_TX"),
    F.rpad(F.col("PRCS_MAP_2_TX"), 4, " ").alias("PRCS_MAP_2_TX"),
    F.rpad(F.col("PRCS_MAP_3_TX"), 4, " ").alias("PRCS_MAP_3_TX"),
    F.rpad(F.col("PRCS_MAP_4_TX"), 4, " ").alias("PRCS_MAP_4_TX"),
    F.rpad(F.col("PRCS_SUB_SRC_CD"), 5, " ").alias("PRCS_SUB_SRC_CD"),
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("WD_LOB_CD"),
    F.col("WD_BANK_ACCT_NM"),
    F.col("WD_RVNU_CAT_ID"),
    F.col("WD_SPEND_CAT_ID")
)

write_files(
    df_IdsJrnlEntryCapExtr_select,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_GL_Cap_BCP_table1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option(
        "query",
        """SELECT GL_CAP_DTL_CK,
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
 CRME_FUND_AMT,
 CRME_PYMT_METH_IND,
 GRGR_CK,
 CRME_FUND_RATE,
 CRME_PAY_FROM_DT,
 CRME_PAY_THRU_DT,
 PDPD_ID,
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
 FROM #$BCBSFINDB#..GL_CAP_DTL_BCP;"""
    )
    .load()
)

df_Transform = df_GL_Cap_BCP_table1.withColumn(
    "svACCTDT",
    FORMAT_DATE(F.col("SNAP_ACT_DT"), "SYBASE", "TIMESTAMP", "DATE")
).withColumn(
    "svAcctDtSk",
    GetFkeyDate(F.lit("IDS"), F.lit(100), F.col("svACCTDT"), F.lit("X"))
).withColumn(
    "svSrcTransTypCdSk",
    GetFkeyCodes(F.lit("FACETS"), F.lit(110), F.lit("SOURCE TRANSACTION TYPE CODE"), F.lit("CAP"), F.lit("X"))
)

df_TransformOutput = df_Transform.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("GL_CAP_DTL_CK").alias("SRC_TRANS_CK"),
    F.col("svSrcTransTypCdSk").alias("SRC_TRANS_TYP_CD_SK"),
    F.col("svAcctDtSk").alias("ACCTG_DT_SK")
)

df_TransformOutput_select = df_TransformOutput.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("SRC_TRANS_CK"),
    F.col("SRC_TRANS_TYP_CD_SK"),
    F.rpad(F.col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK")
)

write_files(
    df_TransformOutput_select,
    f"{adls_path}/load/B_JRNL_ENTRY_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)