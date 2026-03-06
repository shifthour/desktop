# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     PSDrugTransTrns
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the People Soft Drug file created by Darrel Innes (3206) and creates a CRF row for subsequent primary and foreign keying.
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
# MAGIC              Steph Goddrad   10/05/2004-   Originally Programmed
# MAGIC               Steph Goddard    11/11/2004   Moved PRODUCT field to FNCL_LOB_SK instead of PDBL_ACCT_CAT.  PRODUCT is not product as we know it, but the financial line of business.   It is populated differently in PRODUCT than it is in PDBL_ACCT_CAT.
# MAGIC               Steph Goddard  7/13/2005        Changes for sequencer
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              5/7/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard            09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                              2008-08-18       3567(Primary Key)           Added primary key process to the job                     devlIDS                       Steph Goddard           08/21/2008
# MAGIC Steph Goddard              02/27/2009      prod supp                       changed format date call                                         devlIDS
# MAGIC 
# MAGIC 
# MAGIC sudheer champati           2013-07-25           4701                          Changed date format in svACCTGDT in                   IDSDevl                    Bhoomi Dasari            9/5/2013 
# MAGIC                                                                                                      BusinessRules and Transform
# MAGIC 	                                                                                     transformer to get required format for IDS tables
# MAGIC                                                                                                      Replacing Sequential file stage with Sybase stage
# MAGIC                                                                                                      to extract GL transactions from SYBASE
# MAGIC                                                                                                      Adding $BCBSFIN parameters to use in Sybase stage
# MAGIC 
# MAGIC Madhavan B                 2017-10-16          TFS 20094                 Changed the column SNAP_ACCT_DT                     IntegrateDev1          Jag Yelavarthi               2017-10-18
# MAGIC                                                                                                     datatype from char to timestamp
# MAGIC 
# MAGIC Madhavan B                 2017-11-07          TFS 20094                 Added new parameter CurrRunCycle and passed      IntegrateDev3          Jag Yelavarthi               2017-11-07
# MAGIC                                                                                                     to PK shared container to populate actula run cycle
# MAGIC                                                                                                     number instead of hardcode value of '1'

# MAGIC Extract the PeopleSoft Drug Extract
# MAGIC Hash file (hf_drug_trans_allcol) cleared in calling program
# MAGIC This container is used in:
# MAGIC PSDrugTransExtr
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, to_date, concat_ws, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Retrieve parameter values
BCBSFINOwner = get_widget_value('$BCBSFINOwner','')
IDSOwner = get_widget_value('$IDSOwner','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

# Acquire DB connection details for BCBS
jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)

# Read from GL_DRUG_DTL_BCP_Table (Sybase -> BCBS)
df_GL_DRUG_DTL_BCP_Table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
          GL_DRG_DTL_CK,
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
          PDPD_ID,
          PDBL_ID,
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
        FROM {BCBSFINOwner}.GL_DRUG_DTL_BCP_Table
        """
    )
    .load()
)

# BusinessRules (CTransformerStage) - Derive stage variable and 2 output pins
df_businessrules_stagevars = df_GL_DRUG_DTL_BCP_Table.withColumn(
    "svACCTGDT",
    to_date(col("SNAP_ACT_DT"), "yyyy-MM-dd")  # Replicating FORMAT.DATE(...,"DATE")
)

# First output pin: AllCol
df_allcol = df_businessrules_stagevars.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_DRG_DTL_CK").alias("DRUG_TRANS_CK"),
    rpad(col("svACCTGDT").cast("string"), 10, " ").alias("ACCTG_DT_SK"),  # char(10)
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),       # char(10)
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),           # char(1)
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),         # char(1)
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", lit(SrcSysCd), col("GL_DRG_DTL_CK"), col("svACCTGDT").cast("string")).alias("PRI_KEY_STRING"),
    lit(0).alias("DRUG_TRANS_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(
        F.expr("CASE WHEN length(trim(PRODUCT))=0 THEN 'NA' ELSE PRODUCT END"), 
        10, " "
    ).alias("FNCL_LOB"),  # char(10)
    F.expr("CASE WHEN length(trim(GRGR_ID))=0 THEN 'NA' ELSE GRGR_ID END").alias("GRP"),
    F.expr("CASE WHEN length(trim(PDPD_ID))=0 THEN 'NA' ELSE PDPD_ID END").alias("PROD"),
    rpad(
        F.expr("CASE WHEN length(trim(LOBD_ID))=0 THEN 'NA' ELSE LOBD_ID END"),
        10, " "
    ).alias("DRUG_TRANS_LOB_CD"),  # char(10)
    col("PDBL_ID").alias("BILL_CMPNT_ID")
)

# Second output pin: Transform
df_transform = df_businessrules_stagevars.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_DRG_DTL_CK").alias("DRUG_TRANS_CK"),
    rpad(col("svACCTGDT").cast("string"), 10, " ").alias("ACCTG_DT_SK")  # char(10)
)

# MAGIC %run ../../../../shared_containers/PrimaryKey/DrugTransPK
# COMMAND ----------

# Shared Container DrugTransPK has two inputs and one output
params_container = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_key = DrugTransPK(df_transform, df_allcol, params_container)

# IdsDrugTransExtr (CSeqFileStage) - final file from link "Key"
df_ids_drug_trans_extr = df_key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("DRUG_TRANS_SK"),
    col("DRUG_TRANS_CK"),
    rpad(col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("FNCL_LOB"), 10, " ").alias("FNCL_LOB"),
    col("GRP"),
    col("PROD"),
    rpad(col("DRUG_TRANS_LOB_CD"), 10, " ").alias("DRUG_TRANS_LOB_CD"),
    col("BILL_CMPNT_ID")
)

write_files(
    df_ids_drug_trans_extr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Read from PSI_Source (Sybase -> BCBS)
df_PSI_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT
          GL_DRG_DTL_CK,
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
          PDPD_ID,
          PDBL_ID,
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
        FROM {BCBSFINOwner}.PSI_Source
        """
    )
    .load()
)

# Transform (CTransformerStage)
df_transform_psi = (
    df_PSI_Source
    .withColumn("svACCTDT", to_date(col("SNAP_ACT_DT"), "yyyy-MM-dd"))
    .withColumn("svAcctDtSk", GetFkeyDate("IDS", lit(100), col("svACCTDT"), lit("X")))
)

df_output = df_transform_psi.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("GL_DRG_DTL_CK").alias("DRUG_TRANS_CK"),
    rpad(col("svAcctDtSk").cast("string"), 10, " ").alias("ACCTG_DT_SK")  # char(10)
)

# Snapshot_File (CSeqFileStage)
df_snapshot_file = df_output.select(
    col("SRC_SYS_CD_SK"),
    col("DRUG_TRANS_CK"),
    rpad(col("ACCTG_DT_SK"), 10, " ").alias("ACCTG_DT_SK")
)

write_files(
    df_snapshot_file,
    f"{adls_path}/load/B_DRUG_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)