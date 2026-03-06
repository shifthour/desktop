# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Brent Leland                   04/05/2005                                                 Changed SQL lookups to hash files for FNCL_LOB and CD_MPPNG.
# MAGIC 
# MAGIC 
# MAGIC Sudheer Champati         09/11/2017        5599                                 Modifed the job to create the output file to      IntegrateDev2                 Jag Yelavarthi              2017-09-12
# MAGIC                                                                                                           match workday integration.
# MAGIC 
# MAGIC Tim Sieg                        10/5/2017         5599                                 Adding sort stage and stage variables in             IntegrateDev2               Kalyan Neelam            2017-10-10
# MAGIC                                                                                                          trnsJrnlSplit for new JournalKey logic
# MAGIC                                                                                                          Correcting IDS parameter names,
# MAGIC                                                                                                          missing $
# MAGIC 
# MAGIC Tim Sieg                        10/11/2017       5599                                Changing rules for JournallineexternalrefID,          IntegrateDev2              Jag Yelavarthi               2017-11-29
# MAGIC                                                                                                        JournalSource and  CompanyReferenceID
# MAGIC                                                                                                         output fields in trnsJrnlSplt stage
# MAGIC                                                                                                         Replacing FNCL_LOB_CD lookup with 
# MAGIC                                                                                                        JRNL_ENTRY_TRANS_CRSWALK loolup
# MAGIC 
# MAGIC Tim Sieg                         02/13/2018      5599                               Modify SQL for last updated row in                        IntegrateDev2              Kalyan Neelam            2018-02-20
# MAGIC                                                                                                        JRNL_ENTRY_TRANS_CRSWALK       
# MAGIC                                                                                                        extract in JRNL_ENTRY_TRANS_CRSWALK stage 
# MAGIC 
# MAGIC TimSieg                          03/09/2018          5599                           Adding logic to GL_FNCL_LOB_CD field in the      IntegrateDev2	          Jaideep Mankala        03/19/2018
# MAGIC                                                                                                        TrnsJrnlMapFields stage to remove line feed 
# MAGIC                                                                                                         character in field on output file
# MAGIC 
# MAGIC TimSieg                          04/01/2018          5599                           Modify logic on GL_FNCL_LOB_CD field in the      IntegrateDev2	         Kalyan Neelam             2018-04-26
# MAGIC                                                                                                        TrnsJrnlMapFields stage to TRIM output to 
# MAGIC                                                                                                         eliminate additional rows for same GL_FNCL_LOB_CD
# MAGIC 
# MAGIC Tim Sieg                         04/26/2018           5599                          Changed logic that loads the                                  IntegrateDev2            Kalyan Neelam             2018-05-01
# MAGIC                                                                                                        Worktag_Organization_Reference_ID field in the
# MAGIC                                                                                                         trnsJrnlSplt stage to ensure that this field is populated
# MAGIC                                                                                                         on every JE line in the output file

# MAGIC Sorting JE rows for adding key ID in output file
# MAGIC Extract CRSWALK_SK for FNCL_LOB historical lookup. Using LGCY_FNCL_LOB_CD from source
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>  # (No shared containers actually used in this job, but required by instructions)
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
Acctg_Dt = get_widget_value('Acctg_Dt','')
Trans_LOB = get_widget_value('Trans_LOB','')
TmpOutFile = get_widget_value('TmpOutFile','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from JRNL_ENTRY_TRANS (DB2Connector)
extract_query_JRNL_ENTRY_TRANS = f"""
SELECT
  ACCTG_DT_SK,
  JRNL_ENTRY_TRANS_AMT,
  TRANS_LN_NO,
  ACCT_NO,
  AFFILIATE_NO,
  BUS_UNIT_GL_NO,
  BUS_UNIT_NO,
  CC_ID,
  JRNL_LN_DESC,
  OPR_UNIT_NO,
  SUB_ACCT_NO,
  FNCL_LOB_SK,
  JRNL_ENTRY_TRANS_DR_CR_CD_SK,
  DIST_GL_IN,
  APP_JRNL_ID,
  JRNL_LN_REF_NO,
  GL_FNCL_LOB_CD,
  BANK_ACCT_NM,
  RVNU_CAT_ID,
  SPEND_CAT_ID
FROM {IDSOwner}.JRNL_ENTRY_TRANS
WHERE DIST_GL_IN = 'Y'
  AND ACCTG_DT_SK = '{Acctg_Dt}'
  AND TRANS_LN_NO = {Trans_LOB}
  AND JRNL_ENTRY_TRANS_AMT <> 0.00
"""

df_JRNL_ENTRY_TRANS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_JRNL_ENTRY_TRANS)
    .load()
)

df_JRNL_ENTRY_TRANS = df_JRNL_ENTRY_TRANS.select(
    "ACCTG_DT_SK",
    "JRNL_ENTRY_TRANS_AMT",
    "TRANS_LN_NO",
    "ACCT_NO",
    "AFFILIATE_NO",
    "BUS_UNIT_GL_NO",
    "BUS_UNIT_NO",
    "CC_ID",
    "JRNL_LN_DESC",
    "OPR_UNIT_NO",
    "SUB_ACCT_NO",
    "FNCL_LOB_SK",
    "JRNL_ENTRY_TRANS_DR_CR_CD_SK",
    "DIST_GL_IN",
    "APP_JRNL_ID",
    "JRNL_LN_REF_NO",
    "GL_FNCL_LOB_CD",
    "BANK_ACCT_NM",
    "RVNU_CAT_ID",
    "SPEND_CAT_ID"
)

# Temporary column to fulfill the join condition Key.WD_LOB_CD (unknown origin).
df_JRNL_ENTRY_TRANS = df_JRNL_ENTRY_TRANS.withColumn("WD_LOB_CD", F.lit("<unresolved_WD_LOB_CD>"))

# Read from hf_etrnl_cd_mppng (CHashedFileStage) - scenario C
df_hf_etrnl_cd_mppng = spark.read.parquet("hf_etrnl_cd_mppng.parquet")
df_hf_etrnl_cd_mppng = df_hf_etrnl_cd_mppng.select(
    "CD_MPPNG_SK",
    "TRGT_CD",
    "TRGT_CD_NM",
    "SRC_CD",
    "SRC_CD_NM"
)

# Read from JRNL_ENTRY_TRANS_CRSWALK (DB2Connector)
extract_query_JRNL_ENTRY_TRANS_CRSWALK = f"""
SELECT
  TRIM(CRSWALK.LGCY_FNCL_LOB_CD) AS LGCY_FNCL_LOB_CD,
  CRSWALK.JRNL_ENTRY_TRANS_CRSWALK_SK,
  CRSWALK.GL_FNCL_LOB_CD
FROM {IDSOwner}.JRNL_ENTRY_TRANS_CRSWALK CRSWALK,
     (
       SELECT
         SRC_SYS_CD,
         LGCY_FNCL_LOB_CD,
         MAX(JRNL_ENTRY_TRANS_CRSWALK_SK) AS MXSK,
         MAX(LAST_UPDT_RUN_CYC_EXCTN_SK) AS MXUPDTSK
       FROM {IDSOwner}.JRNL_ENTRY_TRANS_CRSWALK
       GROUP BY
         SRC_SYS_CD,
         LGCY_FNCL_LOB_CD
     ) MX
WHERE CRSWALK.JRNL_ENTRY_TRANS_CRSWALK_SK = MX.MXSK
  AND CRSWALK.LAST_UPDT_RUN_CYC_EXCTN_SK = MX.MXUPDTSK
  AND CRSWALK.LGCY_FNCL_LOB_CD = MX.LGCY_FNCL_LOB_CD
"""

df_jrnl_entry_trans_crswalk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_JRNL_ENTRY_TRANS_CRSWALK)
    .load()
)

df_jrnl_entry_trans_crswalk = df_jrnl_entry_trans_crswalk.select(
    "LGCY_FNCL_LOB_CD",
    "JRNL_ENTRY_TRANS_CRSWALK_SK",
    "GL_FNCL_LOB_CD"
)

# hf_jrnl_entry_trans_crswalk (CHashedFileStage) - scenario A: remove duplicates on key column LGCY_FNCL_LOB_CD
df_jrnl_entry_trans_crswalk = df_jrnl_entry_trans_crswalk.dropDuplicates(["LGCY_FNCL_LOB_CD"])

# TrnsJrnlMapFields (CTransformerStage)
#  - Primary link: df_JRNL_ENTRY_TRANS (alias lnkJrnlEntryIn)
#  - Lookup link 1: df_hf_etrnl_cd_mppng (alias lnkCD_MAPPINGIn), join on lnkJrnlEntryIn.JRNL_ENTRY_TRANS_DR_CR_CD_SK == lnkCD_MAPPINGIn.CD_MPPNG_SK
#  - Lookup link 2: df_jrnl_entry_trans_crswalk (alias lnkJECrswlk), left join on
#      substring(trim(lnkJrnlEntryIn.JRNL_LN_REF_NO),7,4) = lnkJECrswlk.LGCY_FNCL_LOB_CD
#      and
#      lnkJrnlEntryIn.WD_LOB_CD = lnkJECrswlk.GL_FNCL_LOB_CD
df_map_1 = df_JRNL_ENTRY_TRANS.alias("lnkJrnlEntryIn").join(
    df_hf_etrnl_cd_mppng.alias("lnkCD_MAPPINGIn"),
    on=F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_DR_CR_CD_SK") == F.col("lnkCD_MAPPINGIn.CD_MPPNG_SK"),
    how="left"
)

df_map_2 = df_map_1.alias("lnkJrnlEntryIn").join(
    df_jrnl_entry_trans_crswalk.alias("lnkJECrswlk"),
    on=[
        F.substring(F.trim(F.col("lnkJrnlEntryIn.JRNL_LN_REF_NO")), 7, 4) == F.col("lnkJECrswlk.LGCY_FNCL_LOB_CD"),
        F.col("lnkJrnlEntryIn.WD_LOB_CD") == F.col("lnkJECrswlk.GL_FNCL_LOB_CD")
    ],
    how="left"
)

df_map_filtered = df_map_2.filter(F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT") != 0.00)

# Build output columns
#  MONETARY_AMOUNT = IF lnkCD_MAPPINGIn.TRGT_CD = "CR" THEN (0 - lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT) ELSE lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT
monetary_amount_expr = F.when(
    F.col("lnkCD_MAPPINGIn.TRGT_CD") == F.lit("CR"),
    -F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT")
).otherwise(F.col("lnkJrnlEntryIn.JRNL_ENTRY_TRANS_AMT"))

#  GL_FNCL_LOB_CD: remove \n \r, then trim
clean_gl_fncl_lob_cd = F.regexp_replace(F.col("lnkJrnlEntryIn.GL_FNCL_LOB_CD"), "[\\n\\r]", "")
clean_gl_fncl_lob_cd = F.trim(clean_gl_fncl_lob_cd)

#  BANK_ACCT_NM: if null then " " else original
bank_acct_nm_expr = F.when(F.col("lnkJrnlEntryIn.BANK_ACCT_NM").isNull(), F.lit(" ")).otherwise(F.col("lnkJrnlEntryIn.BANK_ACCT_NM"))

#  RVNU_CAT_ID: if null then " " else original
rvnu_cat_id_expr = F.when(F.col("lnkJrnlEntryIn.RVNU_CAT_ID").isNull(), F.lit(" ")).otherwise(F.col("lnkJrnlEntryIn.RVNU_CAT_ID"))

#  SPEND_CAT_ID: if null then " " else original
spend_cat_id_expr = F.when(F.col("lnkJrnlEntryIn.SPEND_CAT_ID").isNull(), F.lit(" ")).otherwise(F.col("lnkJrnlEntryIn.SPEND_CAT_ID"))

#  JRNL_LN_REF_NO: IF LEN(TRIM(GL_FNCL_LOB_CD))=0 THEN lnkJECrswlk.GL_FNCL_LOB_CD[5,4] ELSE lnkJrnlEntryIn.GL_FNCL_LOB_CD[5,4]
jrnl_ln_ref_no_expr = F.when(
    F.length(F.trim(clean_gl_fncl_lob_cd)) == 0,
    F.substring(F.col("lnkJECrswlk.GL_FNCL_LOB_CD"), 5, 4)
).otherwise(F.substring(clean_gl_fncl_lob_cd, 5, 4))

df_trns_map_fields = df_map_filtered.select(
    F.rpad(F.col("lnkJrnlEntryIn.BUS_UNIT_NO"), 5, " ").alias("BUSINESS_UNIT"),
    F.rpad(F.col("lnkJrnlEntryIn.ACCT_NO"), 10, " ").alias("ACCOUNT"),
    F.rpad(F.col("lnkJrnlEntryIn.CC_ID"), 10, " ").alias("DEPTID"),
    F.rpad(F.col("lnkJrnlEntryIn.AFFILIATE_NO"), 5, " ").alias("AFFILIATE"),
    F.rpad(F.col("lnkJrnlEntryIn.SUB_ACCT_NO"), 10, " ").alias("CHARTFIELD1"),
    monetary_amount_expr.alias("MONETARY_AMOUNT"),
    F.rpad(F.col("lnkJrnlEntryIn.JRNL_LN_DESC"), 30, " ").alias("LINE_DESC"),
    F.rpad(F.col("lnkJrnlEntryIn.APP_JRNL_ID"), 10, " ").alias("APP_JRNL_ID"),
    F.rpad(clean_gl_fncl_lob_cd, 20, " ").alias("GL_FNCL_LOB_CD"),
    F.rpad(bank_acct_nm_expr, 100, " ").alias("BANK_ACCT_NM"),
    F.rpad(rvnu_cat_id_expr, 20, " ").alias("RVNU_CAT_ID"),
    F.rpad(spend_cat_id_expr, 20, " ").alias("SPEND_CAT_ID"),
    jrnl_ln_ref_no_expr.alias("JRNL_LN_REF_NO")
)

# AggJrnlSum (AGGREGATOR)
# Group by = BUSINESS_UNIT, ACCOUNT, DEPTID, AFFILIATE, CHARTFIELD1, LINE_DESC, APP_JRNL_ID, BANK_ACCT_NM, RVNU_CAT_ID, SPEND_CAT_ID
# Sum = MONETARY_AMOUNT
# Pass through = GL_FNCL_LOB_CD, JRNL_LN_REF_NO
df_agg = (
    df_trns_map_fields.groupBy(
        "BUSINESS_UNIT",
        "ACCOUNT",
        "DEPTID",
        "AFFILIATE",
        "CHARTFIELD1",
        "LINE_DESC",
        "APP_JRNL_ID",
        "BANK_ACCT_NM",
        "RVNU_CAT_ID",
        "SPEND_CAT_ID"
    )
    .agg(
        F.sum("MONETARY_AMOUNT").alias("MONETARY_AMOUNT"),
        F.first("GL_FNCL_LOB_CD").alias("GL_FNCL_LOB_CD"),
        F.first("JRNL_LN_REF_NO").alias("JRNL_LN_REF_NO")
    )
)

# TrnsGetHdrFields (CTransformerStage) - filter "MONETARY_AMOUNT <> 0.00"
df_trns_hdr_fields = df_agg.filter(F.col("MONETARY_AMOUNT") != 0.00).select(
    F.rpad(F.col("APP_JRNL_ID"), 10, " ").alias("APP_JRNL_ID"),
    F.rpad(F.col("BUSINESS_UNIT"), 5, " ").alias("BUSINESS_UNIT"),
    F.rpad(F.col("ACCOUNT"), 10, " ").alias("ACCOUNT"),
    F.rpad(F.col("DEPTID"), 10, " ").alias("DEPTID"),
    F.rpad(F.col("AFFILIATE"), 5, " ").alias("AFFILIATE"),
    "MONETARY_AMOUNT",
    F.rpad(F.col("CHARTFIELD1"), 10, " ").alias("CHARTFIELD1"),
    F.rpad(F.col("LINE_DESC"), 30, " ").alias("LINE_DESC"),
    F.rpad(F.col("GL_FNCL_LOB_CD"), 20, " ").alias("GL_FNCL_LOB_CD"),
    F.rpad(F.col("BANK_ACCT_NM"), 100, " ").alias("BANK_ACCT_NM"),
    F.rpad(F.col("RVNU_CAT_ID"), 20, " ").alias("RVNU_CAT_ID"),
    F.rpad(F.col("SPEND_CAT_ID"), 20, " ").alias("SPEND_CAT_ID"),
    F.rpad(F.col("JRNL_LN_REF_NO"), 20, " ").alias("JRNL_LN_REF_NO")
)

# SrtJrnlEntry (sort)
df_srt_jrnl_entry = df_trns_hdr_fields.orderBy(
    "APP_JRNL_ID",
    "BUSINESS_UNIT",
    "ACCOUNT"
)

# trnsJrnlSplit (CTransformerStage) - stage vars implement a logic to increment a sequence
# We replicate with dense_rank over (APP_JRNL_ID). If the ID repeats, the same JournalKey; if new, increment by 1.
w = Window.orderBy("APP_JRNL_ID").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df_srt_jrnl_entry = df_srt_jrnl_entry.withColumn("JournalKey", F.dense_rank().over(Window.orderBy("APP_JRNL_ID")))

# Add a column with Acctg_Dt (AccountingDate) and handle JournalSource with if logic on Trans_LOB and APP_JRNL_ID
df_srt_jrnl_entry = df_srt_jrnl_entry.withColumn("Trans_LOB", F.lit(Trans_LOB))

journal_source_expr = (
    F.when(F.col("Trans_LOB") == "231", "Capitation")
    .when((F.col("Trans_LOB") == "230") & (F.col("APP_JRNL_ID") == F.lit("ITS")), "Claims_ITS")
    .when((F.col("Trans_LOB") == "230") & (F.col("APP_JRNL_ID") == F.lit("CLM")), "Claims")
    .when((F.col("Trans_LOB") == "270") & (F.col("APP_JRNL_ID") == F.lit("COM")), "Commission_Accrual")
    .when((F.col("Trans_LOB") == "270") & (F.col("APP_JRNL_ID") == F.lit("CPY")), "Commission_Payment")
    .when(F.col("Trans_LOB") == "210", "Premiums")
    .when(F.col("Trans_LOB") == "235", "NDBH_Settlement")
    .when(F.col("Trans_LOB") == "820", "CMS_820")
    .when(F.col("Trans_LOB") == "281", "MVP")
    .otherwise("Drug")
)

acctg_date_col = F.lit(Acctg_Dt)

# Build final columns for lnkJrnlOut
df_jrnl_out = df_srt_jrnl_entry.select(
    F.col("JournalKey").alias("JournalKey"),
    F.lit("Organization_Reference_ID").alias("CompanyReferenceIDType"),
    F.lit("10000").alias("CompanyReferenceID"),
    F.lit("USD").alias("Currency"),
    F.lit("Actuals").alias("LedgerType"),
    acctg_date_col.alias("AccountingDate"),
    journal_source_expr.alias("JournalSource"),
    F.lit("Organization_Reference_ID").alias("LineCompanyReferenceIDType"),
    F.trim(F.col("BUSINESS_UNIT")).alias("LineCompanyReferenceID"),
    F.lit("Account_Set_ID").alias("LedgerAccountReferenceID_ParentIDType"),
    F.lit("Standard").alias("LedgerAccountReferenceID_ParentID"),
    F.lit("Ledger_Account_ID").alias("LedgerAccountReferenceIDType"),
    F.trim(F.col("ACCOUNT")).alias("LedgerAccountReferenceID"),
    F.when(F.col("MONETARY_AMOUNT") > 0.00, F.col("MONETARY_AMOUNT")).otherwise(F.lit(None)).alias("DebitAmount"),
    F.when(F.col("MONETARY_AMOUNT") < 0.00, -F.col("MONETARY_AMOUNT")).otherwise(F.lit(None)).alias("CreditAmount"),
    F.lit("USD").alias("LineCurrency"),
    F.trim(F.col("LINE_DESC")).alias("LineMemo"),
    F.trim(F.col("JRNL_LN_REF_NO")).alias("JournalLineExternalReferenceID"),
    F.trim(F.col("DEPTID")).alias("Worktag_Cost_Center_Reference_ID"),
    F.trim(F.col("SPEND_CAT_ID")).alias("Worktag_Spend_Category_ID"),
    F.when(
        F.length(F.trim(F.col("GL_FNCL_LOB_CD"))) == 0,
        F.concat(F.lit("LOB_"), F.trim(F.col("JRNL_LN_REF_NO")))
    ).otherwise(F.trim(F.col("GL_FNCL_LOB_CD"))).alias("Worktag_Organization_Reference_ID"),
    F.trim(F.col("RVNU_CAT_ID")).alias("Worktag_Revenue_Category_ID"),
    F.trim(F.col("BANK_ACCT_NM")).alias("Worktag_Bank_Account_ID"),
    F.trim(F.col("CHARTFIELD1")).alias("Worktag_Customer_Reference_ID"),
    F.when(F.trim(F.col("AFFILIATE")) == F.lit("00"), F.lit(None)).otherwise(F.trim(F.col("AFFILIATE"))).alias("Worktag_Company_Reference_ID"),
    F.lit(None).alias("Worktag_Donor_ID")
)

# Dtl_WorkdayExtr (CSeqFileStage) - write to external path
output_path = f"{adls_path_publish}/external/{TmpOutFile}"
df_final = df_jrnl_out.select(
    "JournalKey",
    "CompanyReferenceIDType",
    "CompanyReferenceID",
    "Currency",
    "LedgerType",
    "AccountingDate",
    "JournalSource",
    "LineCompanyReferenceIDType",
    "LineCompanyReferenceID",
    "LedgerAccountReferenceID_ParentIDType",
    "LedgerAccountReferenceID_ParentID",
    "LedgerAccountReferenceIDType",
    "LedgerAccountReferenceID",
    "DebitAmount",
    "CreditAmount",
    "LineCurrency",
    "LineMemo",
    "JournalLineExternalReferenceID",
    "Worktag_Cost_Center_Reference_ID",
    "Worktag_Spend_Category_ID",
    "Worktag_Organization_Reference_ID",
    "Worktag_Revenue_Category_ID",
    "Worktag_Bank_Account_ID",
    "Worktag_Customer_Reference_ID",
    "Worktag_Company_Reference_ID",
    "Worktag_Donor_ID"
)

write_files(
    df_final,
    output_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)