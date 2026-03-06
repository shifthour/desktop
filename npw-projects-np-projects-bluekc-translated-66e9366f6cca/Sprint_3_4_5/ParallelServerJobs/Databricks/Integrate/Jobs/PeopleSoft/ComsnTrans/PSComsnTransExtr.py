# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     PSComsnTransExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the People Soft Commission file created by Darrel Innes (3206) and creates a CRF row for subsequent primary and foreign keying.
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
# MAGIC              Steph Goddard  10/05/2004-   Originally Programmed
# MAGIC               Steph Goddard    11/11/2004   Moved PRODUCT field to FNCL_LOB_SK instead of PDBL_ACCT_CAT.  PRODUCT is not product as we know it, but the financial line of business  It is populated differently in PRODUCT than it is in PDBL_ACCT_CAT.
# MAGIC              Steph Goddard  7/2005            Changed to extract - logic not changed, but combined transform and pkey for sequencer implementation
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/7/2007          3264                              Added Balancing process to the overall                 devlIDS30                  Steph Goddard            09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                                2008-08-18         3567(Primary Key)      Added primary key process to the job                       devlIDS                      Steph Goddard             08/21/2008 
# MAGIC Steph Goddard                02/27/2009        prod supp                  changed format date call                                           devl IDS
# MAGIC 
# MAGIC T.Sieg                             2013-02-12           4701                        Changed date format in svACCTGDT, BILL_DUE_DATE,    IDSDevl          Bhoomi Dasari              3/22/2013
# MAGIC                                                                                                     CRT_DT, and POSTING_DT in BusinessRules and
# MAGIC 	                                                                                    svACCTGDT in Transform transformer to get required
# MAGIC                                                                                                     format for IDS tables
# MAGIC                                                                                                     Replacing Sequential file stage with ODBC stage
# MAGIC                                                                                                     to extract GL transactions from SYBASE
# MAGIC                                                                                                     Adding $BCBSFIN parameters to use in ODBC stage
# MAGIC 
# MAGIC T.Sieg                            2022-06-11         S2S Remediation    Added ODBC connetor stages for extract stages           IntegrateDev5            Jeyaprasanna              2022-06-12

# MAGIC Read the PeopleSoft Commission Extract file
# MAGIC This container is used in:
# MAGIC PSComsnTransExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_comsn_trans_allcol) cleared in calling program
# MAGIC SYBASE table with the Commission GL transactions
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
# MAGIC %run ../../../../shared_containers/PrimaryKey/ComsnTransPK
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
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

# -------------------------------------------------------------------------
# GL_Comsn_BCP_table (ODBCConnector)
# -------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)
extract_query = (
    "SELECT GL_COM_DTL_CK,\n LOBD_ID, \nPDBL_ACCT_CAT, \nSNAP_SRC_CD,\n FA_SUB_SRC_CD,\n MAP_FLD_1_CD, \nMAP_FLD_2_CD,\n MAP_FLD_3_CD, \nMAP_FLD_4_CD, \nMAP_EFF_DT, \nPAGR_ID, \nGRGR_ID, \nGL_CAT_CD, \nFA_ACCT_NO, \nTX_AMT, \nSNAP_ACT_DT, \nDR_CR_CD, \nBLAC_POSTING_DT, \nPDPD_ID, \nPDBL_ID, \nBLEI_CK, \nBLBL_DUE_DT, \nBLAC_DEBIT_AMT, \nBLAC_CREDIT_AMT,\n BLAC_YR1_IND, \nBLAC_CREATE_DTM, \nGRGR_NAME, \nSGSG_ID, \nLOBD_NAME, \nORIG_ACTG_CAT_CD, \nRVSED_ACTG_CAT_CD,\n BLCT_CK, \nBLEI_BILL_LEVEL, \nBLEI_BILL_LEVEL_CK, \nMAP_IN, \nBUSINESS_UNIT, \nTRANSACTION_ID, \nTRANSACTION_LINE, \nACCOUNTING_DT, \nAPPL_JRNL_ID, \nBUSINESS_UNIT_GL, \nACCOUNT, \nDEPTID, \nOPERATING_UNIT, \nPRODUCT, \nAFFILIATE, \nCHARTFIELD1, \nCHARTFIELD2, \nCHARTFIELD3, \nPROJECT_ID,\n CURRENCY_CD, \nRT_TYPE, \nMONETARY_AMOUNT, \nFOREIGN_AMOUNT, \nJRNL_LN_REF, \nLINE_DESCR, \nGL_DISTRIB_STATUS\nFROM "
    f"{BCBSFINOwner}..GL_COM_DTL_BCP"
)
df_GL_Comsn_BCP_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# -------------------------------------------------------------------------
# BusinessRules (CTransformerStage)
# -------------------------------------------------------------------------
df_BusinessRules = df_GL_Comsn_BCP_table.withColumn(
    "svACCTDT",
    F.to_date(F.col("SNAP_ACT_DT").cast("string"), "yyyy-MM-dd")
)

df_BusinessRules_AllCol = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("GL_COM_DTL_CK").alias("COMSN_TRANS_CK"),
    F.col("svACCTDT").alias("ACCTG_DT_SK"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("GL_COM_DTL_CK"), F.lit(";"), F.col("svACCTDT").cast("string")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("COMSN_TRANS_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.length(trim(F.col("PRODUCT"))) == 0, "NA").otherwise(F.col("PRODUCT")).alias("FNCL_LOB"),
    F.when(F.length(trim(F.col("GRGR_ID"))) == 0, "NA").otherwise(F.col("GRGR_ID")).alias("GRP"),
    F.when(F.length(trim(F.col("ORIG_ACTG_CAT_CD"))) == 0, "NA").otherwise(F.col("ORIG_ACTG_CAT_CD")).alias("ORIG_FNCL_LOB"),
    F.when(F.length(trim(F.col("PDPD_ID"))) == 0, "NA").otherwise(F.col("PDPD_ID")).alias("PROD"),
    F.when(F.length(trim(F.col("RVSED_ACTG_CAT_CD"))) == 0, "NA").otherwise(F.col("RVSED_ACTG_CAT_CD")).alias("RVSED_FNCL_LOB"),
    F.when(F.length(trim(F.col("SGSG_ID"))) == 0, "NA").otherwise(F.col("SGSG_ID")).alias("SUBGRP"),
    F.when(F.length(trim(F.col("BLEI_BILL_LEVEL"))) == 0, "NA").otherwise(F.col("BLEI_BILL_LEVEL")).alias("COMSN_TRANS_BILL_LVL_CD"),
    F.when(F.length(trim(F.col("LOBD_ID"))) == 0, "NA").otherwise(F.col("LOBD_ID")).alias("COMSN_TRANS_LOB_CD"),
    F.when(F.length(trim(F.col("BLAC_YR1_IND"))) == 0, " ").otherwise(F.col("BLAC_YR1_IND")).alias("FIRST_YR_IN"),
    F.to_date(F.col("BLBL_DUE_DT").cast("string"), "yyyy-MM-dd").alias("BILL_DUE_DT"),
    F.to_date(F.col("BLAC_CREATE_DTM").cast("string"), "yyyy-MM-dd").alias("CRT_DT"),
    F.to_date(F.col("BLAC_POSTING_DT").cast("string"), "yyyy-MM-dd").alias("POSTING_DT"),
    F.col("BLCT_CK").alias("BILL_CMPNT_TOT_CK"),
    F.col("BLEI_CK").alias("BILL_ENTY_CK"),
    F.col("BLEI_BILL_LEVEL_CK").alias("BILL_LVL_CK"),
    F.col("PDBL_ID").alias("BILL_CMPNT_ID")
)

df_BusinessRules_Transform = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("GL_COM_DTL_CK").alias("COMSN_TRANS_CK"),
    F.col("svACCTDT").alias("ACCTG_DT_SK")
)

# -------------------------------------------------------------------------
# ComsnTransPK (CContainerStage)
# -------------------------------------------------------------------------
params_container = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_ComsnTransPK_output = ComsnTransPK(df_BusinessRules_Transform, df_BusinessRules_AllCol, params_container)

# -------------------------------------------------------------------------
# IdsComsnTransExtr (CSeqFileStage)
# -------------------------------------------------------------------------
df_IdsComsnTransExtr_write = df_ComsnTransPK_output.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("COMSN_TRANS_SK"),
    F.col("COMSN_TRANS_CK"),
    F.rpad(F.col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("FNCL_LOB"), 10, " ").alias("FNCL_LOB"),
    F.col("GRP"),
    F.rpad(F.col("ORIG_FNCL_LOB"), 10, " ").alias("ORIG_FNCL_LOB"),
    F.col("PROD"),
    F.rpad(F.col("RVSED_FNCL_LOB"), 10, " ").alias("RVSED_FNCL_LOB"),
    F.rpad(F.col("SUBGRP"), 4, " ").alias("SUBGRP"),
    F.rpad(F.col("COMSN_TRANS_BILL_LVL_CD"), 1, " ").alias("COMSN_TRANS_BILL_LVL_CD"),
    F.rpad(F.col("COMSN_TRANS_LOB_CD"), 4, " ").alias("COMSN_TRANS_LOB_CD"),
    F.rpad(F.col("FIRST_YR_IN"), 1, " ").alias("FIRST_YR_IN"),
    F.col("BILL_DUE_DT"),
    F.col("CRT_DT"),
    F.col("POSTING_DT"),
    F.col("BILL_CMPNT_TOT_CK"),
    F.col("BILL_ENTY_CK"),
    F.col("BILL_LVL_CK"),
    F.col("BILL_CMPNT_ID")
)
write_files(
    df_IdsComsnTransExtr_write,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# -------------------------------------------------------------------------
# GL_Comsn_BCP_table1 (ODBCConnector)
# -------------------------------------------------------------------------
jdbc_url_2, jdbc_props_2 = get_db_config(bcbsfin_secret_name)
extract_query_2 = (
    "SELECT GL_COM_DTL_CK,\n LOBD_ID, \nPDBL_ACCT_CAT, \nSNAP_SRC_CD,\n FA_SUB_SRC_CD,\n MAP_FLD_1_CD, \nMAP_FLD_2_CD,\n MAP_FLD_3_CD, \nMAP_FLD_4_CD, \nMAP_EFF_DT, \nPAGR_ID, \nGRGR_ID, \nGL_CAT_CD, \nFA_ACCT_NO, \nTX_AMT, \nSNAP_ACT_DT, \nDR_CR_CD, \nBLAC_POSTING_DT, \nPDPD_ID, \nPDBL_ID, \nBLEI_CK, \nBLBL_DUE_DT, \nBLAC_DEBIT_AMT, \nBLAC_CREDIT_AMT,\n BLAC_YR1_IND, \nBLAC_CREATE_DTM, \nGRGR_NAME, \nSGSG_ID, \nLOBD_NAME, \nORIG_ACTG_CAT_CD, \nRVSED_ACTG_CAT_CD,\n BLCT_CK, \nBLEI_BILL_LEVEL, \nBLEI_BILL_LEVEL_CK, \nMAP_IN, \nBUSINESS_UNIT, \nTRANSACTION_ID, \nTRANSACTION_LINE, \nACCOUNTING_DT, \nAPPL_JRNL_ID, \nBUSINESS_UNIT_GL, \nACCOUNT, \nDEPTID, \nOPERATING_UNIT, \nPRODUCT, \nAFFILIATE, \nCHARTFIELD1, \nCHARTFIELD2, \nCHARTFIELD3, \nPROJECT_ID,\n CURRENCY_CD, \nRT_TYPE, \nMONETARY_AMOUNT, \nFOREIGN_AMOUNT, \nJRNL_LN_REF, \nLINE_DESCR, \nGL_DISTRIB_STATUS\nFROM "
    f"{BCBSFINOwner}..GL_COM_DTL_BCP"
)
df_GL_Comsn_BCP_table1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_2)
    .options(**jdbc_props_2)
    .option("query", extract_query_2)
    .load()
)

# -------------------------------------------------------------------------
# Transform (CTransformerStage)
# -------------------------------------------------------------------------
df_Transform_1 = df_GL_Comsn_BCP_table1.withColumn(
    "svACCTDT",
    F.to_date(F.col("SNAP_ACT_DT").cast("string"), "yyyy-MM-dd")
).withColumn(
    "svAcctDtSk",
    GetFkeyDate("IDS", F.lit(100), F.col("svACCTDT"), F.lit("X"))
)

df_Transform_output = df_Transform_1.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("GL_COM_DTL_CK").alias("COMSN_TRANS_CK"),
    F.col("svAcctDtSk").alias("ACCTG_DT_SK")
)

# -------------------------------------------------------------------------
# Snapshot_File (CSeqFileStage)
# -------------------------------------------------------------------------
df_Snapshot_File_write = df_Transform_output.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("COMSN_TRANS_CK"),
    F.rpad(F.col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK")
)
write_files(
    df_Snapshot_File_write,
    f"{adls_path}/load/B_COMSN_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)