# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:    PS820JrnlEntryExtr
# MAGIC CALLED BY:  PS820JrnlEntryCntl
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Create file in common directory for creating jrnl_entry_trans files in IDS
# MAGIC                   files are fed through transform, primary, and foreign key programs together - the files are split out in the foreign key program.
# MAGIC                
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                               		Project/                                                                                                                        						Code                  	Date
# MAGIC Developer                    	Date                   	Altiris #     		Change Description                                                                                     		Environment		Reviewer            	Reviewed
# MAGIC ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Sudheer                           06/15/2015       	5128                             	Original Programming                                         				IntegrateNewDevl          	Kalyan Neelam             2015-06-18
# MAGIC 
# MAGIC Sudheer                            2017-09-11            	5599                      	Modiifed the Source Sql to pull the workday data  			IntegrateDev2             		Jag Yelavarthi               2017-09-12
# MAGIC 
# MAGIC Tim Sieg                          2017-11-08           	5599                       	 Added PRODUCT to Source Sql                             			IntegrateDev2             		Jag Yelavarthi               2017-11-29
# MAGIC                                                                                                     		Changed the source loaded to FNCL_LOB to use
# MAGIC                                                                                                      		PRODUCT in the BusinessRules Trans 
# MAGIC Prabhu ES                       2022-03-14          	S2S                          	MSSQL ODBC conn params added                         			IntegrateDev5		Ken Bradmon	2022-06-05
# MAGIC 
# MAGIC Raj K                               2022-07-06                      S2S                        Updated ISNULL function to Len(Trim()) in BusinessRules transformer stage for
# MAGIC                                                                                                                AFFIL_ID, CC_ID, DNR_ID, CUST_ID                                                                                     IntegrateDev5                          Jeyaprasanna          2022-07-06

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql.functions import col, lit, length, when, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

bcbs_secret_name = get_widget_value('bcbs_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrYrMo = get_widget_value('CurrYrMo','')
PrevMonth = get_widget_value('PrevMonth','')

# ODBCConnector Stage: GL_ON_EXCH_FED_PYMT_DTL_table
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)
extract_query_GL_ON_EXCH_FED_PYMT_DTL_table = """SELECT
DTL.GL_ON_EXCH_FED_PYMT_DTL_CK,
DTL.SNAP_SRC_CD,
DTL.ACCOUNTING_DT,
DTL.PDBL_ACCT_CAT,
DTL.DR_CR_CD,
DTL.MAP_IN,
DTL.MONETARY_AMOUNT,
DTL.TRANSACTION_LINE,
DTL.ACCOUNT,
DTL.AFFILIATE,
DTL.APPL_JRNL_ID,
DTL.BUSINESS_UNIT_GL,
DTL.BUSINESS_UNIT,
DTL.DEPTID,
DTL.TRANSACTION_ID,
DTL.OPERATING_UNIT,
DTL.PRODUCT,
DTL.CHARTFIELD1,
DTL.WD_LOB_CD,
DTL.WD_BANK_ACCT_NM,
DTL.WD_RVNU_CAT_ID,
DTL.WD_SPEND_CAT_ID,
DTL.WD_CC_ID,
DTL.WD_DNR_ID,
DTL.WD_AFFIL_ID,
DTL.WD_CUST_ID,
DTL.WD_ACCT_NO
FROM #$BCBSFINOwner#.GL_ON_EXCH_FED_PYMT_DTL DTL
WHERE
SUBSTRING(CONVERT(Char(8),(DTL.PAYMT_SUBMT_DT),112),1,6) = '#CurrYrMo#'
"""
df_GL_ON_EXCH_FED_PYMT_DTL_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbs)
    .options(**jdbc_props_bcbs)
    .option("query", extract_query_GL_ON_EXCH_FED_PYMT_DTL_table)
    .load()
)

# DB2Connector Stage: ON_EXCH
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ON_EXCH = """SELECT
TRANS.ON_EXCH_FED_PAYMT_TRANS_CK,
TRANS.ON_EXCH_FED_PAYMT_TRANS_SK
FROM #$IDSOwner#.ON_EXCH_FED_PAYMT_TRANS TRANS
WHERE
LEFT(TRANS.ACCTG_DT_SK,7) = '#PrevMonth#'
"""
df_ON_EXCH = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ON_EXCH)
    .load()
)

# CHashedFileStage: hf_onexch_gl_dtl - Scenario A (intermediate hashed file)
# Deduplicate on key columns: ON_EXCH_FED_PAYMT_TRANS_CK
df_hf_onexch_gl_dtl = dedup_sort(
    df_ON_EXCH,
    ["ON_EXCH_FED_PAYMT_TRANS_CK"],
    [("ON_EXCH_FED_PAYMT_TRANS_CK", "A")]
)

# CTransformerStage: BusinessRules
# Left join primary link (df_GL_ON_EXCH_FED_PYMT_DTL_table) with reference link (df_hf_onexch_gl_dtl)
df_BusinessRules_join = df_GL_ON_EXCH_FED_PYMT_DTL_table.alias("ONEXExtract").join(
    df_hf_onexch_gl_dtl.alias("Key"),
    (col("ONEXExtract.GL_ON_EXCH_FED_PYMT_DTL_CK") == col("Key.ON_EXCH_FED_PAYMT_TRANS_CK")),
    how="left"
)

# Stage Variable svACCTGDT = FORMAT.DATE(ONEXExtract.ACCOUNTING_DT, "SYBASE", "TIMESTAMP", "DATE")
# (Assume FORMAT.DATE(...) is a user-defined function in the environment)
df_BusinessRules = df_BusinessRules_join.withColumn(
    "svACCTGDT",
    FORMAT_DATE("ONEXExtract.ACCOUNTING_DT", "SYBASE", "TIMESTAMP", "DATE")
)

# Output link "AllCol", constraint: ISNULL(Key.ON_EXCH_FED_PAYMT_TRANS_SK) = @FALSE => Key.ON_EXCH_FED_PAYMT_TRANS_SK is not null
df_BusinessRules_AllCol = df_BusinessRules.filter(col("Key.ON_EXCH_FED_PAYMT_TRANS_SK").isNotNull()).select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("ONEXExtract.GL_ON_EXCH_FED_PYMT_DTL_CK").alias("SRC_TRANS_CK"),
    col("ONEXExtract.SNAP_SRC_CD").alias("SRC_TRANS_TYP_CD"),
    rpad(col("svACCTGDT"), 10, " ").alias("ACCTG_DT_SK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(
        lit(SrcSysCd), lit(";"),
        col("ONEXExtract.GL_ON_EXCH_FED_PYMT_DTL_CK").cast(StringType()),
        lit(";"), lit("820"), lit(";"),
        col("svACCTGDT")
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("JRNL_ENTRY_TRANS_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Key.ON_EXCH_FED_PAYMT_TRANS_SK").alias("TRANS_TBL_SK"),
    rpad(col("ONEXExtract.PRODUCT"), 4, " ").alias("FNCL_LOB"),
    rpad(col("ONEXExtract.DR_CR_CD"), 2, " ").alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    rpad(col("ONEXExtract.MAP_IN"), 1, " ").alias("DIST_GL_IN"),
    col("ONEXExtract.MONETARY_AMOUNT").alias("JRNL_ENTRY_TRANS_AMT"),
    col("ONEXExtract.TRANSACTION_LINE").alias("TRANS_LN_NO"),
    rpad(col("ONEXExtract.WD_ACCT_NO"), 10, " ").alias("ACCT_NO"),
    rpad(
        when(length(trim(col("ONEXExtract.WD_AFFIL_ID"))) == 0, lit("    "))
        .otherwise(col("ONEXExtract.WD_AFFIL_ID")),
        5, " "
    ).alias("AFFILIAT_NO"),
    rpad(col("ONEXExtract.APPL_JRNL_ID"), 10, " ").alias("APPL_JRNL_ID"),
    rpad(col("ONEXExtract.BUSINESS_UNIT_GL"), 5, " ").alias("BUS_UNIT_GL_NO"),
    rpad(col("ONEXExtract.BUSINESS_UNIT"), 5, " ").alias("BUS_UNIT_NO"),
    rpad(
        when(length(trim(col("ONEXExtract.WD_CC_ID"))) == 0, lit("    "))
        .otherwise(col("ONEXExtract.WD_CC_ID")),
        10, " "
    ).alias("CC_ID"),
    rpad(col("ONEXExtract.TRANSACTION_ID"), 30, " ").alias("JRNL_LN_DESC"),
    rpad(
        concat(
            col("ONEXExtract.TRANSACTION_LINE"),
            lit("LOB"),
            col("ONEXExtract.PDBL_ACCT_CAT")
        ),
        10, " "
    ).alias("JRNL_LN_REF_NO"),
    rpad(
        when(length(trim(col("ONEXExtract.WD_DNR_ID"))) == 0, lit("    "))
        .otherwise(col("ONEXExtract.WD_DNR_ID")),
        8, " "
    ).alias("OPR_UNIT_NO"),
    rpad(
        when(length(trim(col("ONEXExtract.WD_CUST_ID"))) == 0, lit("    "))
        .otherwise(col("ONEXExtract.WD_CUST_ID")),
        10, " "
    ).alias("SUB_ACCT_NO"),
    rpad(col("ONEXExtract.TRANSACTION_ID"), 10, " ").alias("TRANS_ID"),
    rpad(lit(""), 4, " ").alias("PRCS_MAP_1_TX"),
    rpad(lit(""), 4, " ").alias("PRCS_MAP_2_TX"),
    rpad(lit(""), 4, " ").alias("PRCS_MAP_3_TX"),
    rpad(lit(""), 4, " ").alias("PRCS_MAP_4_TX"),
    rpad(lit(""), 5, " ").alias("PRCS_SUB_SRC_CD"),
    rpad(lit(""), 12, " ").alias("CLCL_ID"),
    col("ONEXExtract.WD_LOB_CD").alias("WD_LOB_CD"),
    col("ONEXExtract.WD_BANK_ACCT_NM").alias("WD_BANK_ACCT_NM"),
    col("ONEXExtract.WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    col("ONEXExtract.WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID")
)

# Output link "Transform" (no constraint)
df_BusinessRules_Transform = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("ONEXExtract.GL_ON_EXCH_FED_PYMT_DTL_CK").alias("SRC_TRANS_CK"),
    col("ONEXExtract.SNAP_SRC_CD").alias("SRC_TRANS_TYP_CD"),
    rpad(col("svACCTGDT"), 10, " ").alias("ACCTG_DT_SK")
)

# CContainerStage: JrnlEntryTransPK (Shared Container)
# MAGIC %run ../../../../../shared_containers/PrimaryKey/JrnlEntryTransPK
# COMMAND ----------

container_params_JrnlEntryTransPK = {
    "IDSOwner": IDSOwner,
    "TmpOutFile": TmpOutFile,
    "InFile": get_widget_value('InFile',''),
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrDate,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}
df_JrnlEntryTransPK_Key = JrnlEntryTransPK(df_BusinessRules_AllCol, df_BusinessRules_Transform, container_params_JrnlEntryTransPK)

# CSeqFileStage: IdsJrnlEntry820Extr
# Write to key/#TmpOutFile# with comma delimiter, quote='"'
df_IdsJrnlEntry820Extr = df_JrnlEntryTransPK_Key.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "JRNL_ENTRY_TRANS_SK",
    "SRC_TRANS_CK",
    "SRC_TRANS_TYP_CD",
    "ACCTG_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "TRANS_TBL_SK",
    "FNCL_LOB",
    "JRNL_ENTRY_TRANS_DR_CR_CD",
    "DIST_GL_IN",
    "JRNL_ENTRY_TRANS_AMT",
    "TRANS_LN_NO",
    "ACCT_NO",
    "AFFILIAT_NO",
    "APPL_JRNL_ID",
    "BUS_UNIT_GL_NO",
    "BUS_UNIT_NO",
    "CC_ID",
    "JRNL_LN_DESC",
    "JRNL_LN_REF_NO",
    "OPR_UNIT_NO",
    "SUB_ACCT_NO",
    "TRANS_ID",
    "PRCS_MAP_1_TX",
    "PRCS_MAP_2_TX",
    "PRCS_MAP_3_TX",
    "PRCS_MAP_4_TX",
    "PRCS_SUB_SRC_CD",
    "CLCL_ID",
    "WD_LOB_CD",
    "WD_BANK_ACCT_NM",
    "WD_RVNU_CAT_ID",
    "WD_SPEND_CAT_ID"
)

write_files(
    df_IdsJrnlEntry820Extr,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)