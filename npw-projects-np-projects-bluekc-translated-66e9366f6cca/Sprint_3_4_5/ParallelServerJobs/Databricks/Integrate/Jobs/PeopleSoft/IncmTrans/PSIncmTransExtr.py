# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     PSIncmTransExtr
# MAGIC CALLED BY:  PSJrnlEntryExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the People Soft Income file created by Darrel Innes (3206) and creates a CRF row for subsequent primary and foreign keying.
# MAGIC     
# MAGIC INPUTS:
# MAGIC    #FilePath#/landing/#InFile#;
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
# MAGIC              Steph Goddard 10/11/2004-   Originally Programmed
# MAGIC               Steph Goddard    11/11/2004   Moved PRODUCT field to FNCL_LOB_SK instead of PDBL_ACCT_CAT.  PRODUCT is not product as we know it, but the financial line of business.   It is populated differently in PRODUCT than it is in PDBL_ACCT_CAT.
# MAGIC              Steph Goddard  07/13/2005    Changes for sequencer
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                  5/8/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard             09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                                2008-08-19       3567(Primary Key)         Added Primary Key process to the job                         devlIDS                   Steph Goddard             08/21/2008
# MAGIC Steph Goddard                02/27/2009     prod supp                      changed format date call                                              devlIDS
# MAGIC 
# MAGIC T. Sieg                            2013-01-31       4701                             Changed date format in svACCTGDT, BILL_DUE_DATE      IDSDevl         Bhoomi Dasari            3/26/2013
# MAGIC                                                                                                      CRT_DT, and POSTING_DT in BusinessRules and
# MAGIC                                                                                                      svACCTGDT in Transform transformer to get required
# MAGIC                                                                                                      format for IDS tables
# MAGIC                                                                                                      Replacing Sequential file stage with ODBC stage to
# MAGIC                                                                                                      extract GL transactions from SYBASE
# MAGIC 	                                                                                    Adding $BCBSFIN parameters to use in ODBC stage
# MAGIC 
# MAGIC T.Sieg                  2022-05-15      S2S Remediation        Replaced extract stages with ODBC connector                  IntegrateDev5	Ken Bradmon	2022-06-07

# MAGIC Read the PeopleSoft Income file
# MAGIC Apply business logic
# MAGIC This container is used in:
# MAGIC PSIncmTransExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_incm_trans_allcol) cleared in calling program
# MAGIC SYBASE table with the Income GL transactions
# MAGIC Writing Sequential File to /key
# MAGIC Balancing snapshot of source file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/IncmTransPK
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

jdbc_url_bcp, jdbc_props_bcp = get_db_config(bcbsfin_secret_name)
extract_query_GL_Incm_BCP_table = """SELECT 
GL_INC_DTL_CK,
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
FROM #$BCBSFINDB#..GL_INCOME_DTL_BCP"""
df_GL_Incm_BCP_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcp)
    .options(**jdbc_props_bcp)
    .option("query", extract_query_GL_Incm_BCP_table)
    .load()
)

df_BusinessRules_svACCTGDT = df_GL_Incm_BCP_table.withColumn(
    "svACCTGDT",
    F.lit(None)  # Placeholder for FORMAT.DATE(BCPExtract.SNAP_ACT_DT,"SYBASE","TIMESTAMP","DATE"), treated as user-defined
)

df_BusinessRulesAllCol = (
    df_BusinessRules_svACCTGDT
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("INCM_TRANS_CK", F.col("GL_INC_DTL_CK"))
    .withColumn("ACCTG_DT_SK", F.col("svACCTGDT"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("GL_INC_DTL_CK"), F.lit(";"), F.col("svACCTGDT"))
    )
    .withColumn("INCM_TRANS_SK", F.lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "GRP",
        F.when(F.length(trim(F.col("GRGR_ID"))) == 0, F.lit("NA")).otherwise(F.col("GRGR_ID"))
    )
    .withColumn(
        "PROD",
        F.when(F.length(trim(F.col("PDPD_ID"))) == 0, F.lit("NA")).otherwise(F.col("PDPD_ID"))
    )
    .withColumn(
        "SUBGRP",
        F.when(F.length(trim(F.col("SGSG_ID"))) == 0, F.lit("NA")).otherwise(F.col("SGSG_ID"))
    )
    .withColumn("BILL_ENTY_CK", F.col("BLEI_CK"))
    .withColumn(
        "FNCL_LOB",
        F.when(F.length(trim(F.col("PRODUCT"))) == 0, F.lit("NA")).otherwise(F.col("PRODUCT"))
    )
    .withColumn(
        "ORIG_FNCL_LOB",
        F.when(F.length(trim(F.col("ORIG_ACTG_CAT_CD"))) == 0, F.lit("NA")).otherwise(F.col("ORIG_ACTG_CAT_CD"))
    )
    .withColumn(
        "RVSED_FNCL_LOB",
        F.when(F.length(trim(F.col("RVSED_ACTG_CAT_CD"))) == 0, F.lit("NA")).otherwise(F.col("RVSED_ACTG_CAT_CD"))
    )
    .withColumn(
        "INCM_TRANS_LOB_CD",
        F.when(F.length(trim(F.col("LOBD_ID"))) == 0, F.lit("NA")).otherwise(F.col("LOBD_ID"))
    )
    .withColumn(
        "FIRST_YR_IN",
        F.when(F.length(trim(F.col("BLAC_YR1_IND"))) == 0, F.lit(" ")).otherwise(F.col("BLAC_YR1_IND"))
    )
    .withColumn(
        "BILL_DUE_DT",
        F.lit(None)  # Placeholder for FORMAT.DATE(BCPExtract.BLBL_DUE_DT,"SYBASE","TIMESTAMP","DATE")
    )
    .withColumn(
        "CRT_DT",
        F.lit(None)  # Placeholder for FORMAT.DATE(BCPExtract.BLAC_CREATE_DTM,"SYBASE","TIMESTAMP","DATE")
    )
    .withColumn(
        "POSTING_DT",
        F.lit(None)  # Placeholder for FORMAT.DATE(BCPExtract.BLAC_POSTING_DT,"SYBASE","TIMESTAMP","DATE")
    )
    .withColumn("BILL_CMPNT_ID", F.col("PDBL_ID"))
)

df_BusinessRulesTransform = (
    df_BusinessRules_svACCTGDT
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("INCM_TRANS_CK", F.col("GL_INC_DTL_CK"))
    .withColumn("ACCTG_DT_SK", F.col("svACCTGDT"))
)

cols_allcol = [
    ("SRC_SYS_CD_SK", "char", 0),
    ("INCM_TRANS_CK", None, 0),
    ("ACCTG_DT_SK", "char", 10),
    ("JOB_EXCTN_RCRD_ERR_SK", None, 0),
    ("INSRT_UPDT_CD", "char", 10),
    ("DISCARD_IN", "char", 1),
    ("PASS_THRU_IN", "char", 1),
    ("FIRST_RECYC_DT", None, 0),
    ("ERR_CT", None, 0),
    ("RECYCLE_CT", None, 0),
    ("SRC_SYS_CD", None, 0),
    ("PRI_KEY_STRING", None, 0),
    ("INCM_TRANS_SK", None, 0),
    ("CRT_RUN_CYC_EXCTN_SK", None, 0),
    ("LAST_UPDT_RUN_CYC_EXCTN_SK", None, 0),
    ("GRP", None, 0),
    ("PROD", None, 0),
    ("SUBGRP", "char", 4),
    ("BILL_ENTY_CK", None, 0),
    ("FNCL_LOB", "char", 10),
    ("ORIG_FNCL_LOB", "char", 10),
    ("RVSED_FNCL_LOB", "char", 10),
    ("INCM_TRANS_LOB_CD", "char", 10),
    ("FIRST_YR_IN", "char", 1),
    ("BILL_DUE_DT", None, 0),
    ("CRT_DT", None, 0),
    ("POSTING_DT", None, 0),
    ("BILL_CMPNT_ID", None, 0),
]
select_expr_allcol = []
for c_name, c_type, c_len in cols_allcol:
    if c_type == "char" or c_type == "varchar":
        select_expr_allcol.append(F.rpad(F.col(c_name).cast(StringType()), c_len, " ").alias(c_name))
    else:
        select_expr_allcol.append(F.col(c_name))

df_BusinessRulesAllCol = df_BusinessRulesAllCol.select(*select_expr_allcol)

cols_transform = [
    ("SRC_SYS_CD_SK", "char", 0),
    ("INCM_TRANS_CK", None, 0),
    ("ACCTG_DT_SK", "char", 10),
]
select_expr_transform = []
for c_name, c_type, c_len in cols_transform:
    if c_type == "char" or c_type == "varchar":
        select_expr_transform.append(F.rpad(F.col(c_name).cast(StringType()), c_len, " ").alias(c_name))
    else:
        select_expr_transform.append(F.col(c_name))

df_BusinessRulesTransform = df_BusinessRulesTransform.select(*select_expr_transform)

params_incmtranspk = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_key = IncmTransPK(df_BusinessRulesTransform, df_BusinessRulesAllCol, params_incmtranspk)

cols_key = [
    ("JOB_EXCTN_RCRD_ERR_SK", None, 0),
    ("INSRT_UPDT_CD", "char", 10),
    ("DISCARD_IN", "char", 1),
    ("PASS_THRU_IN", "char", 1),
    ("FIRST_RECYC_DT", None, 0),
    ("ERR_CT", None, 0),
    ("RECYCLE_CT", None, 0),
    ("SRC_SYS_CD", None, 0),
    ("PRI_KEY_STRING", None, 0),
    ("INCM_TRANS_SK", None, 0),
    ("INCM_TRANS_CK", None, 0),
    ("ACCTG_DT_SK", "char", 10),
    ("CRT_RUN_CYC_EXCTN_SK", None, 0),
    ("LAST_UPDT_RUN_CYC_EXCTN_SK", None, 0),
    ("GRP", None, 0),
    ("PROD", None, 0),
    ("SUBGRP", "char", 4),
    ("BILL_ENTY_CK", None, 0),
    ("FNCL_LOB", "char", 10),
    ("ORIG_FNCL_LOB", "char", 10),
    ("RVSED_FNCL_LOB", "char", 10),
    ("INCM_TRANS_LOB_CD", "char", 10),
    ("FIRST_YR_IN", "char", 1),
    ("BILL_DUE_DT", None, 0),
    ("CRT_DT", None, 0),
    ("POSTING_DT", None, 0),
    ("BILL_CMPNT_ID", None, 0),
]
select_expr_key = []
for c_name, c_type, c_len in cols_key:
    if c_type == "char" or c_type == "varchar":
        select_expr_key.append(F.rpad(F.col(c_name).cast(StringType()), c_len, " ").alias(c_name))
    else:
        select_expr_key.append(F.col(c_name))
df_key = df_key.select(*select_expr_key)

write_files(
    df_key,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

jdbc_url_bcp2, jdbc_props_bcp2 = get_db_config(bcbsfin_secret_name)
extract_query_GL_Incm_BCP_table1 = """SELECT 
GL_INC_DTL_CK,
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
FROM #$BCBSFINDB#..GL_INCOME_DTL_BCP"""
df_GL_Incm_BCP_table1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcp2)
    .options(**jdbc_props_bcp2)
    .option("query", extract_query_GL_Incm_BCP_table1)
    .load()
)

df_Transform_svACCTDT = df_GL_Incm_BCP_table1.withColumn(
    "svACCTDT",
    F.lit(None)  # Placeholder for FORMAT.DATE(BCPExtract.SNAP_ACT_DT, "SYBASE","TIMESTAMP","DATE")
).withColumn(
    "svAcctDtSk",
    F.lit(None)  # Placeholder for GetFkeyDate("IDS",100,svACCTDT,"X")
)

df_Transform_out = (
    df_Transform_svACCTDT
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("INCM_TRANS_CK", F.col("GL_INC_DTL_CK"))
    .withColumn("ACCTG_DT_SK", F.col("svAcctDtSk"))
)

cols_snapshot = [
    ("SRC_SYS_CD_SK", "char", 0),
    ("INCM_TRANS_CK", None, 0),
    ("ACCTG_DT_SK", "char", 10),
]
select_expr_snapshot = []
for c_name, c_type, c_len in cols_snapshot:
    if c_type == "char" or c_type == "varchar":
        select_expr_snapshot.append(F.rpad(F.col(c_name).cast(StringType()), c_len, " ").alias(c_name))
    else:
        select_expr_snapshot.append(F.col(c_name))

df_Snapshot_File = df_Transform_out.select(*select_expr_snapshot)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_INCM_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_parqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)