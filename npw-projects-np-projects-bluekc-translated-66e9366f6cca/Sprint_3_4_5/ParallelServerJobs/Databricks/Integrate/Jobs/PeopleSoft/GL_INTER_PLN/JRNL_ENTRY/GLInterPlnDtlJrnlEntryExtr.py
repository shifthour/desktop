# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    
# MAGIC 
# MAGIC 
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
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sudheer                           10/12/2015        5212                             Original Programming                                         IntegrateDEV1               Kalyan Neelam              2016-01-04
# MAGIC 
# MAGIC Sudheer                           2017-09-11             5599                      Modiifed the Source Sql to pull the workday data  IntegrateDev2              Jag Yelavarthi                2017-09-12
# MAGIC 
# MAGIC Tim Sieg                          2017-11-08           5599                        Added PRODUCT to Source Sql                             IntegrateDev2             Jag Yelavarthi                2017-11-29
# MAGIC                                                                                                     Changed the source loaded to FNCL_LOB to use
# MAGIC                                                                                                      PRODUCT in the BusinessRules Trans 
# MAGIC 
# MAGIC Raj K                               2022-07-12                      S2S                   Updated ISNULL function to Len(Trim()) in 
# MAGIC                                                                                                          BusinessRules transformer stage for
# MAGIC                                                                                                           AFFIL_ID, CC_ID, DNR_ID, CUST_ID               IntegrateDev5            Jeyaprasanna               2022-07-12

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
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, length, to_date, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
StrtDt = get_widget_value('StrtDt','')
EndDt = get_widget_value('EndDt','')
ids_secret_name = get_widget_value('ids_secret_name','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')

jdbc_url_bcbsfin, jdbc_props_bcbsfin = get_db_config(bcbsfin_secret_name)
extract_query_bcbsfin = f"""
SELECT 
CAST(BILL.GL_INTER_PLAN_BILL_DTL_CK AS INT) AS GL_INTER_PLAN_BILL_DTL_CK,
BILL.PDBL_ACCT_CAT,
BILL.SNAP_SRC_CD,
BILL.DR_CR_CD,
BILL.MAP_IN,
BILL.BUSINESS_UNIT,
BILL.TRANSACTION_ID,
BILL.TRANSACTION_LINE,
BILL.ACCOUNTING_DT,
BILL.APPL_JRNL_ID,
BILL.BUSINESS_UNIT_GL,
BILL.ACCOUNT,
BILL.DEPTID,
BILL.OPERATING_UNIT,
BILL.PRODUCT,
BILL.AFFILIATE,
BILL.CHARTFIELD1,
BILL.MONETARY_AMOUNT,
BILL.JRNL_LN_REF,
BILL.WD_LOB_CD,
BILL.WD_BANK_ACCT_NM,
BILL.WD_RVNU_CAT_ID,
BILL.WD_SPEND_CAT_ID,
BILL.WD_CC_ID,
BILL.WD_DNR_ID,
BILL.WD_AFFIL_ID,
BILL.WD_CUST_ID,
BILL.WD_ACCT_NO
FROM {BCBSFINOwner}..GL_INTER_PLAN_BILL_DTL BILL
WHERE BILL.MAP_EFF_DT = '{StrtDt}' AND BILL.SNAP_ACT_DT = '{EndDt}'
"""
df_GL_INTER_PLAN_BILL_DTL_table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_bcbsfin)
    .options(**jdbc_props_bcbsfin)
    .option("query", extract_query_bcbsfin)
    .load()
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"""
SELECT DISTINCT
TRANS.INTER_PLN_BILL_TRANS_SK,
TRANS.INTER_PLN_BILL_TRANS_CK
FROM {IDSOwner}.INTER_PLN_BILL_TRANS TRANS,
{IDSOwner}.MBR_AUDIT MBR_AUDIT
"""
df_INTER_PLN_BILL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

df_hf_onexch_gl_dtl = dedup_sort(
    df_INTER_PLN_BILL,
    partition_cols=["INTER_PLN_BILL_TRANS_CK"],
    sort_cols=[("INTER_PLN_BILL_TRANS_CK", "A")]
)

df_join = (
    df_GL_INTER_PLAN_BILL_DTL_table.alias("InterPlnBill")
    .join(
        df_hf_onexch_gl_dtl.alias("Key"),
        (col("InterPlnBill.GL_INTER_PLAN_BILL_DTL_CK") == col("Key.INTER_PLN_BILL_TRANS_CK")),
        how="left"
    )
)

df_join = df_join.withColumn("svACCTGDT", to_date(col("InterPlnBill.ACCOUNTING_DT")))

df_AllCol_matched = df_join.filter(col("Key.INTER_PLN_BILL_TRANS_SK").isNotNull())

df_AllCol = df_AllCol_matched.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("InterPlnBill.GL_INTER_PLAN_BILL_DTL_CK").alias("SRC_TRANS_CK"),
    col("InterPlnBill.SNAP_SRC_CD").alias("SRC_TRANS_TYP_CD"),
    col("svACCTGDT").alias("ACCTG_DT_SK"),
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    (lit(SrcSysCd) 
     .concat(lit(";")) 
     .concat(col("InterPlnBill.GL_INTER_PLAN_BILL_DTL_CK").cast("string")) 
     .concat(lit(";MVP;")) 
     .concat(col("svACCTGDT").cast("string"))).alias("PRI_KEY_STRING"),
    lit(0).alias("JRNL_ENTRY_TRANS_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Key.INTER_PLN_BILL_TRANS_SK").alias("TRANS_TBL_SK"),
    col("InterPlnBill.PRODUCT").alias("FNCL_LOB"),
    col("InterPlnBill.DR_CR_CD").alias("JRNL_ENTRY_TRANS_DR_CR_CD"),
    col("InterPlnBill.MAP_IN").alias("DIST_GL_IN"),
    col("InterPlnBill.MONETARY_AMOUNT").alias("JRNL_ENTRY_TRANS_AMT"),
    col("InterPlnBill.TRANSACTION_LINE").alias("TRANS_LN_NO"),
    col("InterPlnBill.WD_ACCT_NO").alias("ACCT_NO"),
    when(length(trim(col("InterPlnBill.WD_AFFIL_ID"))) == 0, lit("    "))
       .otherwise(col("InterPlnBill.WD_AFFIL_ID")).alias("AFFILIAT_NO"),
    col("InterPlnBill.APPL_JRNL_ID").alias("APPL_JRNL_ID"),
    col("InterPlnBill.BUSINESS_UNIT_GL").alias("BUS_UNIT_GL_NO"),
    col("InterPlnBill.BUSINESS_UNIT").alias("BUS_UNIT_NO"),
    when(length(trim(col("InterPlnBill.WD_CC_ID"))) == 0, lit("    "))
       .otherwise(col("InterPlnBill.WD_CC_ID")).alias("CC_ID"),
    col("InterPlnBill.TRANSACTION_ID").alias("JRNL_LN_DESC"),
    col("InterPlnBill.JRNL_LN_REF").alias("JRNL_LN_REF_NO"),
    when(length(trim(col("InterPlnBill.WD_DNR_ID"))) == 0, lit("    "))
       .otherwise(col("InterPlnBill.WD_DNR_ID")).alias("OPR_UNIT_NO"),
    when(length(trim(col("InterPlnBill.WD_CUST_ID"))) == 0, lit("    "))
       .otherwise(col("InterPlnBill.WD_CUST_ID")).alias("SUB_ACCT_NO"),
    col("InterPlnBill.TRANSACTION_ID").alias("TRANS_ID"),
    lit("").alias("PRCS_MAP_1_TX"),
    lit("").alias("PRCS_MAP_2_TX"),
    lit("").alias("PRCS_MAP_3_TX"),
    lit("").alias("PRCS_MAP_4_TX"),
    lit("").alias("PRCS_SUB_SRC_CD"),
    lit("").alias("CLCL_ID"),
    col("InterPlnBill.WD_LOB_CD").alias("WD_LOB_CD"),
    col("InterPlnBill.WD_BANK_ACCT_NM").alias("WD_BANK_ACCT_NM"),
    col("InterPlnBill.WD_RVNU_CAT_ID").alias("WD_RVNU_CAT_ID"),
    col("InterPlnBill.WD_SPEND_CAT_ID").alias("WD_SPEND_CAT_ID")
)

df_Transform = df_join.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("InterPlnBill.GL_INTER_PLAN_BILL_DTL_CK").alias("SRC_TRANS_CK"),
    col("InterPlnBill.SNAP_SRC_CD").alias("SRC_TRANS_TYP_CD"),
    col("svACCTGDT").alias("ACCTG_DT_SK")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/JrnlEntryTransPK
# COMMAND ----------

params = {
    "IDSOwner": IDSOwner,
    "TmpOutFile": TmpOutFile,
    "InFile": "<...>",
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrDate,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}

df_container_output = JrnlEntryTransPK(df_AllCol, df_Transform, params)

df_final = df_container_output.select(
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
    df_final,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)