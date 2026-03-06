# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     PSCapTransExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the People Soft Capitation file created by Darrel Innes (3206) and creates a CRF row for subsequent primary and foreign keying.
# MAGIC     
# MAGIC 
# MAGIC INPUTS:
# MAGIC    #FilePath#/landing/#InFile#;
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC                   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Steph Goddard  10/12/2004-   Originally Programmed
# MAGIC               Steph Goddard    11/11/2004   Moved PRODUCT field to FNCL_LOB_SK instead of PDBL_ACCT_CAT.  PRODUCT is not product as we know it, but the financial line of business.   It is populated differently in PRODUCT than it is in PDBL_ACCT_CAT.
# MAGIC             Steph Goddard  07/19/2005       Changed for sequencer
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/4/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard             09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                               2008-08-15         3567(Primary Key)        Added the new primary key process                        devlIDS                     Steph Goddard               08/21/2008
# MAGIC Steph Goddard               02/27/2009       prod supp                     changed format date call                                         devlIDS
# MAGIC 
# MAGIC T.Sieg                             2013-01-16         4701                            Changed date format in svACCTGDT, PD_FROM_DT      IDSDevl           Bhoomi Dasari              3/22/2013
# MAGIC                                                                                                       PD_THRU_DT in BusinessRules and Transform
# MAGIC                                                                                                       transformer to get required format for IDS tables
# MAGIC                                                                                                       Replacing Sequential file stage with ODBC stage
# MAGIC                                                                                                       to extract GL transactions from SYBASE
# MAGIC                                                                                                       Adding $BCBSFIN parameters to use in ODBC stage

# MAGIC SYBASE table with the Capitation GL transactions
# MAGIC Read the PeopleSoft Cap Trans Extract file
# MAGIC Hash file (hf_cap_trans_allcol) cleared in calling program
# MAGIC This container is used in:
# MAGIC PSCapTransExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Apply business logic
# MAGIC Balancing snapshot of source file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql.functions import col, lit, when, length, concat, to_date, trim, rpad, concat_ws
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/CapTransPK
# COMMAND ----------

bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
TmpOutFile = get_widget_value('TmpOutFile','')
InFile = get_widget_value('InFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# ------------------------------------------------------------------------------
# Stage: GL_Cap_BCP_table (CODBCStage) -> BusinessRules (CTransformerStage)
# ------------------------------------------------------------------------------

jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)
extract_query = """SELECT GL_CAP_DTL_CK,
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
FROM BCBSFIN..GL_CAP_DTL_BCP
"""
df_GL_Cap_BCP_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BusinessRules_common = df_GL_Cap_BCP_table.withColumn(
    "svACCTGDT", to_date(col("SNAP_ACT_DT"), "yyyy-MM-dd")
)

# -------------------------------
# Output link "AllCol"
# -------------------------------
df_BusinessRules_all = df_BusinessRules_common.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_CAP_DTL_CK").alias("CAP_TRANS_CK"),
    col("svACCTGDT").alias("ACCTG_DT_SK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(lit(SrcSysCd), lit(";"), col("GL_CAP_DTL_CK"), lit(";"), col("svACCTGDT")).alias("PRI_KEY_STRING"),
    lit(0).alias("CAP_TRANS_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(length(trim(col("GRGR_ID"))) == 0, lit("NA")).otherwise(col("GRGR_ID")).alias("GRP"),
    when(length(trim(col("PDPD_ID"))) == 0, lit("NA")).otherwise(col("PDPD_ID")).alias("PROD"),
    when(length(trim(col("PRODUCT"))) == 0, lit("NA")).otherwise(col("PRODUCT")).alias("FNCL_LOB"),
    when(length(trim(col("LOBD_ID"))) == 0, lit("NA")).otherwise(col("LOBD_ID")).alias("CAP_TRANS_LOB_CD"),
    when(length(trim(col("CRME_PYMT_METH_IND"))) == 0, lit(" ")).otherwise(col("CRME_PYMT_METH_IND")).alias("CAP_TRANS_PAYMT_METH_CD"),
    to_date(col("CRME_PAY_FROM_DT"), "yyyy-MM-dd").alias("PD_FROM_DT"),
    to_date(col("CRME_PAY_THRU_DT"), "yyyy-MM-dd").alias("PD_THRU_DT"),
    col("CRME_FUND_AMT").alias("CALC_FUND_AMT"),
    col("CRME_FUND_RATE").alias("FUND_RATE_AMT")
)

# -------------------------------
# Output link "Transform"
# -------------------------------
df_BusinessRules_transform = df_BusinessRules_common.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_CAP_DTL_CK").alias("CAP_TRANS_CK"),
    col("svACCTGDT").alias("ACCTG_DT_SK")
)

# --------------------------------------------------------------------------------
# Stage: CapTransPK (Shared Container) -> merges two inputs from "Transform" & "AllCol"
# --------------------------------------------------------------------------------

params_CapTransPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_CapTransPK = CapTransPK(df_BusinessRules_transform, df_BusinessRules_all, params_CapTransPK)

# ------------------------------------------------------------------
# Stage: IdsCapTransExtr (CSeqFileStage) -> write final output file
# ------------------------------------------------------------------

df_IdsCapTransExtr_write = df_CapTransPK.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CAP_TRANS_SK"),
    col("CAP_TRANS_CK"),
    rpad(col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("GRP"), 8, " ").alias("GRP"),
    rpad(col("PROD"), 8, " ").alias("PROD"),
    rpad(col("FNCL_LOB"), 4, " ").alias("FNCL_LOB"),
    rpad(col("CAP_TRANS_LOB_CD"), 4, " ").alias("CAP_TRANS_LOB_CD"),
    rpad(col("CAP_TRANS_PAYMT_METH_CD"), 4, " ").alias("CAP_TRANS_PAYMT_METH_CD"),
    rpad(col("PD_FROM_DT"), 10, " ").alias("PD_FROM_DT"),
    rpad(col("PD_THRU_DT"), 10, " ").alias("PD_THRU_DT"),
    col("CALC_FUND_AMT"),
    col("FUND_RATE_AMT")
)

write_files(
    df_IdsCapTransExtr_write,
    f"{adls_path}/key/{TmpOutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

# ------------------------------------------------------------------------------
# Stage: GL_Cap_BCP_table1 (CODBCStage) -> Transform (CTransformerStage)
# ------------------------------------------------------------------------------

jdbc_url2, jdbc_props2 = get_db_config(bcbsfin_secret_name)
extract_query_2 = """SELECT GL_CAP_DTL_CK,
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
FROM BCBSFIN..GL_CAP_DTL_BCP
"""
df_GL_Cap_BCP_table1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url2)
    .options(**jdbc_props2)
    .option("query", extract_query_2)
    .load()
)

df_Transform_common = df_GL_Cap_BCP_table1.withColumn(
    "svACCTDT", to_date(col("SNAP_ACT_DT"), "yyyy-MM-dd")
).withColumn(
    "svAcctDtSk", GetFkeyDate("IDS", lit(100), col("svACCTDT"), lit("X"))
)

df_Transform_output = df_Transform_common.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_CAP_DTL_CK").alias("CAP_TRANS_CK"),
    col("svAcctDtSk").alias("ACCTG_DT_SK")
)

# -------------------------------------------------------------------
# Stage: Snapshot_File (CSeqFileStage) -> write final snapshot output
# -------------------------------------------------------------------

df_Snapshot_File_write = df_Transform_output.select(
    col("SRC_SYS_CD_SK"),
    col("CAP_TRANS_CK"),
    rpad(col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK")
)

write_files(
    df_Snapshot_File_write,
    f"{adls_path}/load/B_CAP_TRANS.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)