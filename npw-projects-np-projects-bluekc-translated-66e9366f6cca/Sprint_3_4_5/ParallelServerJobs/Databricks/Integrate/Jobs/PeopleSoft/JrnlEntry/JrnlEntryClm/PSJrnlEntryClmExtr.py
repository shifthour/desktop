# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *******************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    PSJrnlEntryClmExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from a flat file (GL_CLM_DTL_TEMP) to common directory
# MAGIC   Currently, no need to do extract since data is already in flat file 
# MAGIC 
# MAGIC *******NOTE:  Flat file is pulled from /landing/ directory, not /verified/.  
# MAGIC 
# MAGIC     
# MAGIC INPUTS:
# MAGIC 	GL_CLM_DTL_TEMP
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
# MAGIC             Steph Goddard   07/2005       Changes for sequencer implementation
# MAGIC  
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                 5/8/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard            09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                               2008-08-19        3567(Primary Key)         Added Primary Key process to the job                      devlIDS                     Steph Goddard            08/21/2008
# MAGIC Steph Goddard               02/26/2009       Prod Supp                    changed to get clm_sk from K_CLM table                devlIDS
# MAGIC                                                                                                        changed date call
# MAGIC 
# MAGIC T.Sieg                            2013-01-31          4701                          Changed date format in svACCTGDT in                      IDSDevl                    Bhoomi Dasari             3/28/2013
# MAGIC                                                                                                      BusinessRules and Transform transformer to get
# MAGIC                                                                                                      required format for IDS tables
# MAGIC                                                                                                     Replacing Sequential file stage with ODBC stage
# MAGIC                                                                                                     to extract GL transactions from SYBASE 
# MAGIC 	                                                                                    Adding $BCBSFIN parameters to use in ODBC stage
# MAGIC 
# MAGIC Sudheer                            2017-09-11             5599                      Modiifed the Source Sql to pull the workday data  IntegrateDev2            Jag Yelavarthi                2017-09-12
# MAGIC 
# MAGIC T.Sieg                             2017-11-06               5599                    Unchecked "Allow Multiple Instance" in job properties  IntegerateDev2        Jag Yelavarthi             2017-11-29  
# MAGIC 
# MAGIC T.Sieg                            2022-06-10         S2S Remediation    Added ODBC connetor stages for extract stages           IntegrateDev5             Jeyaprasanna              2022-06-11

# MAGIC Pulling PeopleSoft data from flat files
# MAGIC Hash file (hf_jrnl_entry_trans_allcol) cleared in calling program
# MAGIC This container is used in:
# MAGIC PSJrnlEntryCapExtr
# MAGIC PSJrnlEntryClmExtr
# MAGIC PSJrnlEntryComsnExtr
# MAGIC PSJrnlEntryDrugExtr
# MAGIC PSJrnlEntryIncmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC SYBASE table with the Claims GL transactions
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
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, trim, length, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
TmpOutFile = get_widget_value('TmpOutFile','')
InFile = get_widget_value('InFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_K_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT K_CLM.SRC_SYS_CD_SK, K_CLM.CLM_ID, K_CLM.CLM_SK FROM {IDSOwner}.K_CLM K_CLM")
    .load()
)

jdbc_url_bcbsfin, jdbc_props_bcbsfin = get_db_config(bcbsfin_secret_name)
df_GL_Clm_BCP_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option("query", f"SELECT GL_CLM_DTL_CK,\n LOBD_ID,\n PDBL_ACCT_CAT,\n SNAP_SRC_CD,\n FA_SUB_SRC_CD,\n MAP_FLD_1_CD,\n MAP_FLD_2_CD,\n MAP_FLD_3_CD,\n MAP_FLD_4_CD,\n MAP_EFF_DT,\n PAGR_ID,\n GRGR_ID,\n GL_CAT_CD,\n FA_ACCT_NO,\n TX_AMT,\n SNAP_ACT_DT,\n DR_CR_CD,\n ACPR_CREATE_DT,\n CLCL_CL_TYPE,\n CLCL_CL_SUB_TYPE,\n CLCL_ID_ADJ_TO,\n CLCL_ID,\n CLCK_PYMT_OVRD_IND,\n GRGR_CK,\n CLCL_ID_ADJ_FROM,\n PDPD_ID,\n MAP_IN,\n CLCL_LOW_SVC_DT,\n CLCK_INT_AMT,\n CLCK_NET_AMT,\n CKPY_REF_ID,\n CKPY_PAY_DT,\n BUSINESS_UNIT,\n TRANSACTION_ID,\n TRANSACTION_LINE,\n ACCOUNTING_DT,\n APPL_JRNL_ID,\n BUSINESS_UNIT_GL,\n ACCOUNT,\n DEPTID,\n OPERATING_UNIT,\n PRODUCT,\n AFFILIATE,\n CHARTFIELD1,\n CHARTFIELD2,\n CHARTFIELD3,\n PROJECT_ID,\n CURRENCY_CD,\n RT_TYPE,\n MONETARY_AMOUNT,\n FOREIGN_AMOUNT,\n JRNL_LN_REF,\n LINE_DESCR,\n GL_DISTRIB_STATUS\n FROM {BCBSFINOwner}..GL_CLAIM_DTL_BCP")
    .load()
)

df_BCPExtract = df_GL_Clm_BCP_table.alias("BCPExtract")
df_clm_lookup = df_K_CLM.alias("clm_lookup")

df_BusinessRules_Joined = (
    df_BCPExtract.join(
        df_clm_lookup,
        (
            (lit(SrcSysCdSk) == col("clm_lookup.SRC_SYS_CD_SK"))
            & (col("BCPExtract.CLCL_ID") == col("clm_lookup.CLM_ID"))
        ),
        "left"
    )
    .withColumn("SnapActDt", format_date(col("BCPExtract.SNAP_ACT_DT"), "SYBASE", "TIMESTAMP", "DATE"))
)

df_BusinessRules_AllCol = df_BusinessRules_Joined.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("BCPExtract.GL_CLM_DTL_CK").alias("SRC_TRANS_CK"),
    lit("CLM").alias("SRC_TRANS_TYP_CD"),
    col("SnapActDt").alias("ACCTG_DT_SK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(
        lit(SrcSysCd), lit(";"), col("BCPExtract.GL_CLM_DTL_CK"), lit(";"), lit("CLM"), lit(";"), col("SnapActDt")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("JRNL_ENTRY_TRANS_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("clm_lookup.CLM_SK").isNull(), lit(0)).otherwise(col("clm_lookup.CLM_SK")).alias("TRANS_TBL_SK"),
    when(length(trim(col("BCPExtract.PRODUCT"))) == 0, lit("NA")).otherwise(col("BCPExtract.PRODUCT")).alias("FNCL_LOB"),
    when(length(trim(col("BCPExtract.DR_CR_CD"))) == 0, lit("NA")).otherwise(col("BCPExtract.DR_CR_CD")).alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    col("BCPExtract.MAP_IN").alias("DIST_GL_IN"),
    col("BCPExtract.TX_AMT").alias("JRNL_ENTRY_TRANS_AMT"),
    col("BCPExtract.TRANSACTION_LINE").alias("TRANS_LN_NO"),
    col("BCPExtract.WD_ACCT_NO").alias("ACCT_NO"),
    when(col("BCPExtract.WD_AFFIL_ID").isNull(), lit("    ")).otherwise(col("BCPExtract.WD_AFFIL_ID")).alias("AFFILIAT_NO"),
    col("BCPExtract.APPL_JRNL_ID").alias("APPL_JRNL_ID"),
    col("BCPExtract.BUSINESS_UNIT_GL").alias("BUS_UNIT_GL_NO"),
    col("BCPExtract.BUSINESS_UNIT").alias("BUS_UNIT_NO"),
    when(col("BCPExtract.WD_CC_ID").isNull(), lit("    ")).otherwise(col("BCPExtract.WD_CC_ID")).alias("CC_ID"),
    col("BCPExtract.LINE_DESCR").alias("JRNL_LN_DESC"),
    col("BCPExtract.JRNL_LN_REF").alias("JRNL_LN_REF_NO"),
    when(col("BCPExtract.WD_DNR_ID").isNull(), lit("    ")).otherwise(col("BCPExtract.WD_DNR_ID")).alias("OPR_UNIT_NO"),
    when(col("BCPExtract.WD_CUST_ID").isNull(), lit("    ")).otherwise(col("BCPExtract.WD_CUST_ID")).alias("SUB_ACCT_NO"),
    col("BCPExtract.TRANSACTION_ID").alias("TRANS_ID"),
    col("BCPExtract.MAP_FLD_1_CD").alias("PRCS_MAP_1_TX"),
    col("BCPExtract.MAP_FLD_2_CD").alias("PRCS_MAP_2_TX"),
    col("BCPExtract.MAP_FLD_3_CD").alias("PRCS_MAP_3_TX"),
    col("BCPExtract.MAP_FLD_4_CD").alias("PRCS_MAP_4_TX"),
    col("BCPExtract.FA_SUB_SRC_CD").alias("PRCS_SUB_SRC_CD"),
    col("BCPExtract.CLCL_ID").alias("CLCL_ID"),
    col("BCPExtract.WD_LOB_CD").alias("WD_LOB_CD"),
    col("BCPExtract.WD_BANK_ACCT_NM").alias("WD_BANK_ACCT_NM"),
    col("BCPExtract.WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    col("BCPExtract.WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID")
)

df_BusinessRules_Transform = df_BusinessRules_Joined.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("BCPExtract.GL_CLM_DTL_CK").alias("SRC_TRANS_CK"),
    lit("CLM").alias("SRC_TRANS_TYP_CD"),
    col("SnapActDt").alias("ACCTG_DT_SK")
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
df_JrnlEntryTransPK_Key = JrnlEntryTransPK(df_BusinessRules_Transform, df_BusinessRules_AllCol, params_JrnlEntryTransPK)

df_IdsJrnlEntryClmExtr = df_JrnlEntryTransPK_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("JRNL_ENTRY_TRANS_SK"),
    col("SRC_TRANS_CK"),
    col("SRC_TRANS_TYP_CD"),
    rpad(col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("TRANS_TBL_SK"),
    rpad(col("FNCL_LOB"), 10, " ").alias("FNCL_LOB"),
    rpad(col("JRNL_ENTRY_TRANS_DR_CR_CD"), 2, " ").alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    rpad(col("DIST_GL_IN"), 1, " ").alias("DIST_GL_IN"),
    col("JRNL_ENTRY_TRANS_AMT"),
    col("TRANS_LN_NO"),
    rpad(col("ACCT_NO"), 10, " ").alias("ACCT_NO"),
    rpad(col("AFFILIAT_NO"), 5, " ").alias("AFFILIAT_NO"),
    rpad(col("APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    rpad(col("BUS_UNIT_GL_NO"), 5, " ").alias("BUS_UNIT_GL_NO"),
    rpad(col("BUS_UNIT_NO"), 5, " ").alias("BUS_UNIT_NO"),
    rpad(col("CC_ID"), 10, " ").alias("CC_ID"),
    rpad(col("JRNL_LN_DESC"), 30, " ").alias("JRNL_LN_DESC"),
    rpad(col("JRNL_LN_REF_NO"), 10, " ").alias("JRNL_LN_REF_NO"),
    rpad(col("OPR_UNIT_NO"), 8, " ").alias("OPR_UNIT_NO"),
    rpad(col("SUB_ACCT_NO"), 10, " ").alias("SUB_ACCT_NO"),
    rpad(col("TRANS_ID"), 10, " ").alias("TRANS_ID"),
    rpad(col("PRCS_MAP_1_TX"), 4, " ").alias("PRCS_MAP_1_TX"),
    rpad(col("PRCS_MAP_2_TX"), 4, " ").alias("PRCS_MAP_2_TX"),
    rpad(col("PRCS_MAP_3_TX"), 4, " ").alias("PRCS_MAP_3_TX"),
    rpad(col("PRCS_MAP_4_TX"), 4, " ").alias("PRCS_MAP_4_TX"),
    rpad(col("PRCS_SUB_SRC_CD"), 5, " ").alias("PRCS_SUB_SRC_CD"),
    rpad(col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    col("WD_LOB_CD"),
    col("WD_BANK_ACCT_NM"),
    col("WD_RVNU_CAT_ID"),
    col("WD_SPEND_CAT_ID")
)

write_files(
    df_IdsJrnlEntryClmExtr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_GL_Clm_BCP_table1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option("query", f"SELECT GL_CLM_DTL_CK,\n LOBD_ID,\n PDBL_ACCT_CAT,\n SNAP_SRC_CD,\n FA_SUB_SRC_CD,\n MAP_FLD_1_CD,\n MAP_FLD_2_CD,\n MAP_FLD_3_CD,\n MAP_FLD_4_CD,\n MAP_EFF_DT,\n PAGR_ID,\n GRGR_ID,\n GL_CAT_CD,\n FA_ACCT_NO,\n TX_AMT,\n SNAP_ACT_DT,\n DR_CR_CD,\n ACPR_CREATE_DT,\n CLCL_CL_TYPE,\n CLCL_CL_SUB_TYPE,\n CLCL_ID_ADJ_TO,\n CLCL_ID,\n CLCK_PYMT_OVRD_IND,\n GRGR_CK,\n CLCL_ID_ADJ_FROM,\n PDPD_ID,\n MAP_IN,\n CLCL_LOW_SVC_DT,\n CLCK_INT_AMT,\n CLCK_NET_AMT,\n CKPY_REF_ID,\n CKPY_PAY_DT,\n BUSINESS_UNIT,\n TRANSACTION_ID,\n TRANSACTION_LINE,\n ACCOUNTING_DT,\n APPL_JRNL_ID,\n BUSINESS_UNIT_GL,\n ACCOUNT,\n DEPTID,\n OPERATING_UNIT,\n PRODUCT,\n AFFILIATE,\n CHARTFIELD1,\n CHARTFIELD2,\n CHARTFIELD3,\n PROJECT_ID,\n CURRENCY_CD,\n RT_TYPE,\n MONETARY_AMOUNT,\n FOREIGN_AMOUNT,\n JRNL_LN_REF,\n LINE_DESCR,\n GL_DISTRIB_STATUS\n FROM {BCBSFINOwner}..GL_CLAIM_DTL_BCP")
    .load()
)

df_BCPExtract_1 = df_GL_Clm_BCP_table1.alias("BCPExtract")
df_Transform = (
    df_BCPExtract_1
    .withColumn("svACCTDT", format_date(col("BCPExtract.SNAP_ACT_DT"), "SYBASE", "TIMESTAMP", "DATE"))
    .withColumn("svAcctDtSk", GetFkeyDate(lit("IDS"), lit(100), col("svACCTDT"), lit("X")))
    .withColumn("svSrcTransTypCdSk", GetFkeyCodes(lit("FACETS"), lit(110), lit("SOURCE TRANSACTION TYPE CODE"), lit("CLM"), lit("X")))
)

df_Transform_Output = df_Transform.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("BCPExtract.GL_CLM_DTL_CK").alias("SRC_TRANS_CK"),
    col("svSrcTransTypCdSk").alias("SRC_TRANS_TYP_CD_SK"),
    col("svAcctDtSk").alias("ACCTG_DT_SK")
)

df_Snapshot_File = df_Transform_Output.select(
    rpad(col("SRC_SYS_CD_SK"), 0, " ").alias("SRC_SYS_CD_SK"),
    col("SRC_TRANS_CK"),
    col("SRC_TRANS_TYP_CD_SK"),
    rpad(col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK")
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_JRNL_ENTRY_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)