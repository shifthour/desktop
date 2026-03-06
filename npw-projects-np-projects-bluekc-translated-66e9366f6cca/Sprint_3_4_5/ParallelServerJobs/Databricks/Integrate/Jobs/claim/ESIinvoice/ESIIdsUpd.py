# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Update IDS CLM and DRUG_CLM with paid date from Argus monthly file
# MAGIC       
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                Update IDS CLM with paid date from Argus monthly file
# MAGIC                Update IDS DRUG_CLM recon_dt_sk with current date
# MAGIC               If claim on input does not have matching claim in IDS, it is written to an 'unmatched' sequential file for error processing
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                                                                                     Project #                  Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ----------------------------------------------------------------------------                                           ----------------               ------------------------------------       ----------------------------           ----------------
# MAGIC Sharon Andrew     11/01/2008                 original programming                                                                                      3784 PBM               devlIDSnew                       Steph Goddard               11/07/2008
# MAGIC Brent Leland         11-25-2008                  Moved fields forward for unmatched exception table                                     3567 Primary Key    devlIDS
# MAGIC                                                                  Added W_DRUG_ENR output
# MAGIC Raja Gummadi      08-28-2013                  Corrected logic for MAX_ALW_CST_REDC_IN field in DRUG_CLM extract.   5115 BHI               IntegrateNewDevl             Sharon Andrew         2013-09-04
# MAGIC SAndrew               2014-01-28                 Added criteria when looking up the ClaimID on IDS.CLM.   We should only update claims that have not yet already been paid.   Therefore, lookup claims on IDS.CLM where ClaimID is on the ESI Invoice file and IDS.PD_DT_SK equals NA.

# MAGIC Used to find finacial LOB
# MAGIC Read weekly Invoice file from ESI  ESIDrugClmInvoiceLand
# MAGIC Direct update of CLM and DRUG_CLM
# MAGIC Claim IDs for missing records in IDS
# MAGIC Claims unmatched to IDS claims written to exception file.  Used in ESIInvoiceExcptTrans
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, DateType
)
from pyspark.sql.functions import (
    col, lit, rpad, row_number, when, isnull
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SourceSys = get_widget_value('SourceSys', 'ESI')
RunID = get_widget_value('RunID', '100')
RunCycle = get_widget_value('RunCycle', '100')
CurrentDate = get_widget_value('CurrentDate', '')
_IDSOwner = get_widget_value('$IDSOwner', '$PROJDEF')
ids_secret_name = get_widget_value('ids_secret_name', '')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read entire CLM table (replacing the DS query with '?'):
df_IdsClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CLM_SK, SRC_SYS_CD_SK, CLM_ID FROM {_IDSOwner}.CLM")
    .load()
)

# Read CD_MPPNG with direct condition (no '?'):
df_IDS_src_cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_CD, CD_MPPNG_SK FROM {_IDSOwner}.CD_MPPNG WHERE SRC_CD='ESI' AND TRGT_DOMAIN_NM='SOURCE SYSTEM'")
    .load()
)

# Deduplicate for hashed-file hf_esi_invoice_ids_src_cd (Scenario A):
df_IDS_src_cd = dedup_sort(
    df_IDS_src_cd,
    ["SRC_CD"],
    [("SRC_CD","A")]
)

# Read ESI_Invoice (CSeqFileStage) with explicit schema:
schema_ESI_Invoice = StructType([
    StructField("RCRD_ID", StringType(), True),
    StructField("CLAIM_ID", StringType(), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("MEM_CK_KEY", StringType(), True),
    StructField("BTCH_NO", StringType(), True),
    StructField("PDX_NO", StringType(), True),
    StructField("RX_NO", StringType(), True),
    StructField("DT_FILLED", StringType(), True),
    StructField("NDC_NO", StringType(), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_RFL_CD", StringType(), True),
    StructField("METRIC_QTY", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST", StringType(), True),
    StructField("DISPNS_FEE", StringType(), True),
    StructField("COPAY_AMT", StringType(), True),
    StructField("SLS_TAX", StringType(), True),
    StructField("AMT_BILL", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("SEX_CD", StringType(), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", StringType(), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", StringType(), True),
    StructField("PRESCRIBER_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", StringType(), True),
    StructField("RESUB_CYC_CT", StringType(), True),
    StructField("DT_RX_WRTN", StringType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", StringType(), True),
    StructField("ELIG_CLRFCTN_CD", StringType(), True),
    StructField("CMPND_CD", StringType(), True),
    StructField("NO_OF_RFLS_AUTH", StringType(), True),
    StructField("LVL_OF_SVC", StringType(), True),
    StructField("RX_ORIG_CD", StringType(), True),
    StructField("RX_DENIAL_CLRFCTN", StringType(), True),
    StructField("PRI_PRESCRIBER", StringType(), True),
    StructField("CLNC_ID_NO", StringType(), True),
    StructField("DRUG_TYP", StringType(), True),
    StructField("PRESCRIBER_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT_CLMED", StringType(), True),
    StructField("UNIT_DOSE_IN", StringType(), True),
    StructField("OTHR_PAYOR_AMT", StringType(), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", StringType(), True),
    StructField("FULL_AWP", StringType(), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("MSTR_CAR", StringType(), True),
    StructField("SUB_CAR", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("ESI_SUB_GRP", StringType(), True),
    StructField("PLN_DSGNR", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("ADMIN_FEE", StringType(), True),
    StructField("CAP_AMT", StringType(), True),
    StructField("INGR_CST_SUB", StringType(), True),
    StructField("MBR_NON_COPAY_AMT", StringType(), True),
    StructField("MBR_PAY_CD", StringType(), True),
    StructField("INCNTV_FEE", StringType(), True),
    StructField("CLM_ADJ_AMT", StringType(), True),
    StructField("CLM_ADJ_CD", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("GNRC_CLS_NO", StringType(), True),
    StructField("THRPTC_CLS_AHFS", StringType(), True),
    StructField("PDX_TYP", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG", StringType(), True),
    StructField("PD_DT", StringType(), True),
    StructField("BNF_CD", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("ORIG_MBR", StringType(), True),
    StructField("DT_OF_INJURY", StringType(), True),
    StructField("FEE_AMT", StringType(), True),
    StructField("ESI_REF_NO", StringType(), True),
    StructField("CLNT_CUST_ID", StringType(), True),
    StructField("PLN_TYP", StringType(), True),
    StructField("ESI_ADJDCT_REF_NO", StringType(), True),
    StructField("ESI_ANCLRY_AMT", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("PAID_DATE", StringType(), True),
    StructField("PRTL_FILL_STTUS_CD", StringType(), True),
    StructField("ESI_BILL_DT", StringType(), True),
    StructField("FSA_VNDR_CD", StringType(), True),
    StructField("PICA_DRUG_CD", StringType(), True),
    StructField("AMT_CLMED", StringType(), True),
    StructField("AMT_DSALW", StringType(), True),
    StructField("FED_DRUG_CLS_CD", StringType(), True),
    StructField("DEDCT_AMT", StringType(), True),
    StructField("BNF_COPAY_100", StringType(), True),
    StructField("CLM_PRCS_TYP", StringType(), True),
    StructField("INDEM_HIER_TIER_NO", StringType(), True),
    StructField("FLR", StringType(), True),
    StructField("MCARE_D_COV_DRUG", StringType(), True),
    StructField("RETRO_LICS_CD", StringType(), True),
    StructField("RETRO_LICS_AMT", StringType(), True),
    StructField("LICS_SBSDY_AMT", StringType(), True),
    StructField("MED_B_DRUG", StringType(), True),
    StructField("MED_B_CLM", StringType(), True),
    StructField("PRESCRIBER_QLFR", StringType(), True),
    StructField("PRESCRIBER_ID_NPI", StringType(), True),
    StructField("PDX_QLFR", StringType(), True),
    StructField("PDX_ID_NPI", StringType(), True),
    StructField("HRA_APLD_AMT", StringType(), True),
    StructField("ESI_THER_CLS", StringType(), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HRA_FLAG", StringType(), True),
    StructField("DOSE_CD", StringType(), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", StringType(), True),
    StructField("COPAY_BNF_OPT", StringType(), True),
    StructField("GNRC_PROD_IN_GPI", StringType(), True),
    StructField("PRESCRIBER_SPEC", StringType(), True),
    StructField("VAL_CD", StringType(), True),
    StructField("PRI_CARE_PDX", StringType(), True),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), True)
])

df_ESI_Invoice = (
    spark.read.format("csv")
    .schema(schema_ESI_Invoice)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .load(f"{adls_path}/verified/ESIDrugClmInvoicePaidUpdt.dat.{RunID}")
)

# TrnsGetSrcCd: left join using condition 'ESI' == src_cd_lkup.SRC_CD
df_PaidClm = (
    df_ESI_Invoice.alias("Input")
    .join(
        df_IDS_src_cd.alias("src_cd_lkup"),
        col("src_cd_lkup.SRC_CD") == lit("ESI"),
        "left"
    )
    .select(
        col("Input.CLAIM_ID").alias("CLAIM_ID"),
        col("src_cd_lkup.SRC_CD").alias("SRC_CD"),
        col("src_cd_lkup.CD_MPPNG_SK").alias("SRC_SYS_CD_SK"),
        col("Input.ESI_BILL_DT").alias("PAID_DATE"),
        col("Input.AMT_BILL").alias("AMT_BILL"),
        col("Input.OTHR_COV_CD").alias("OTHR_COV_CD"),
        col("Input.OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
        col("Input.ADMIN_FEE").alias("ADMIN_FEE"),
        col("Input.BILL_BSS_CD").alias("BILL_BSS_CD"),
        col("Input.GRP_ID").alias("GRP_ID"),
        col("Input.MEM_CK_KEY").alias("MEM_CK_KEY"),
        col("Input.GRP_NO").alias("GRP_NO"),
        col("Input.DT_FILLED").alias("DT_FILLED")
    )
)

# Trns1: left join with df_IdsClm on (SRC_SYS_CD_SK, CLAIM_ID). 
# Stage variables:
#   ClmSk = IF IsNull(IdsClm.CLM_ID) THEN 0 ELSE IdsClm.CLM_SK
#   PdDtSk = GetFkeyDate('IDS', ClmSk, PaidClm.PAID_DATE, 'X')
#   ReconDtSk = GetFkeyDate('IDS', ClmSk, CurrentDate, 'X')
#   ClmCOBTypeSk = IF (PaidClm.OTHR_PAYOR_AMT <> 0 AND PaidClm.OTHR_COV_CD='2') THEN GetFkeyCodes(PaidClm.SRC_CD, ClmSk, "CLAIM COB", PaidClm.OTHR_COV_CD, "X") ELSE 1
#   DrugClmBillBssCdSk = GetFkeyCodes(PaidClm.SRC_CD, ClmSk, " DRUG CLAIM BILLED BASIS", PaidClm.BILL_BSS_CD, "X")

df_Trans1_joined = (
    df_PaidClm.alias("PaidClm")
    .join(
        df_IdsClm.alias("IdsClm"),
        [
            col("PaidClm.SRC_SYS_CD_SK") == col("IdsClm.SRC_SYS_CD_SK"),
            col("PaidClm.CLAIM_ID") == col("IdsClm.CLM_ID")
        ],
        "left"
    )
)

df_Trans1 = df_Trans1_joined.select(
    col("PaidClm.*"),
    when(isnull(col("IdsClm.CLM_ID")), lit(0)).otherwise(col("IdsClm.CLM_SK")).alias("ClmSk"),
).withColumn(
    "PdDtSk",
    lit(GetFkeyDate('IDS', col("ClmSk"), col("PAID_DATE"), 'X'))  # Expression in DS
).withColumn(
    "ReconDtSk",
    lit(GetFkeyDate('IDS', col("ClmSk"), lit(CurrentDate), 'X'))
).withColumn(
    "ClmCOBTypeSk",
    when(
        (col("OTHR_PAYOR_AMT") != 0) & (col("OTHR_COV_CD") == '2'),
        lit(GetFkeyCodes(col("SRC_CD"), col("ClmSk"), "CLAIM COB", col("OTHR_COV_CD"), "X"))
    ).otherwise(lit(1))
).withColumn(
    "DrugClmBillBssCdSk",
    lit(GetFkeyCodes(col("SRC_CD"), col("ClmSk"), " DRUG CLAIM BILLED BASIS", col("BILL_BSS_CD"), "X"))
)

df_ClmUpdt = df_Trans1.filter(col("ClmSk") != 0).select(
    col("ClmSk").alias("CLM_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PdDtSk").alias("PD_DT_SK"),
    col("AMT_BILL").alias("ACTL_PD_AMT"),
    col("AMT_BILL").alias("PAYBL_AMT"),
    col("ClmCOBTypeSk").alias("CLM_COB_CD_SK")
)

df_DrugClmUpdt = df_Trans1.filter(col("ClmSk") != 0).select(
    col("ClmSk").alias("CLM_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ReconDtSk").alias("RECON_DT_SK"),
    when(trim(col("BILL_BSS_CD")) == lit("09"), lit("Y")).otherwise(lit("N")).alias("MAX_ALW_CST_REDC_IN"),
    col("ADMIN_FEE").alias("ADM_FEE_AMT"),
    col("DrugClmBillBssCdSk").alias("DRUG_CLM_BILL_BSS_CD_SK")
)

df_ClmLnUpdt = df_Trans1.filter(col("ClmSk") != 0).select(
    col("ClmSk").alias("CLM_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("AMT_BILL").alias("PAYBL_AMT")
)

df_ClmRemitHistUpdt = df_Trans1.filter(col("ClmSk") != 0).select(
    col("ClmSk").alias("CLM_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("AMT_BILL").alias("ACTL_PD_AMT"),
    col("OTHR_PAYOR_AMT").alias("COB_PD_AMT")
)

df_Unmatched = df_Trans1.filter(col("ClmSk") == 0).select(
    col("SRC_SYS_CD_SK"),
    col("CLAIM_ID"),
    col("PAID_DATE"),
    col("AMT_BILL"),
    col("GRP_ID"),
    col("MEM_CK_KEY").alias("MEM_UNIQ_KEY"),
    col("GRP_NO"),
    col("DT_FILLED")
)

# Update_IDS merges (4 input links) => use separate staging for each link.
# For char columns, apply rpad before writing.

# 1) ClmUpdt => merges into #$IDSOwner#.CLM
df_ClmUpdt_final = df_ClmUpdt.withColumn("PD_DT_SK", rpad(col("PD_DT_SK"), 10, " "))
stg_table_ClmUpdt = "STAGING.ESIIdsUpd_Update_IDS_ClmUpdt_temp"
spark.sql(f"DROP TABLE IF EXISTS {stg_table_ClmUpdt}")
df_ClmUpdt_final.write.jdbc(jdbc_url, stg_table_ClmUpdt, mode="overwrite", properties=jdbc_props)
merge_sql_ClmUpdt = f"""
MERGE INTO {_IDSOwner}.CLM AS T
USING {stg_table_ClmUpdt} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.PD_DT_SK = S.PD_DT_SK,
    T.ACTL_PD_AMT = S.ACTL_PD_AMT,
    T.PAYBL_AMT = S.PAYBL_AMT,
    T.CLM_COB_CD_SK = S.CLM_COB_CD_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, PD_DT_SK, ACTL_PD_AMT, PAYBL_AMT, CLM_COB_CD_SK)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.PD_DT_SK, S.ACTL_PD_AMT, S.PAYBL_AMT, S.CLM_COB_CD_SK);
"""
execute_dml(merge_sql_ClmUpdt, jdbc_url, jdbc_props)

# 2) DrugClmUpdt => merges into #$IDSOwner#.DRUG_CLM
df_DrugClmUpdt_final = (
    df_DrugClmUpdt
    .withColumn("RECON_DT_SK", rpad(col("RECON_DT_SK"), 10, " "))
    .withColumn("MAX_ALW_CST_REDC_IN", rpad(col("MAX_ALW_CST_REDC_IN"), 1, " "))
)
stg_table_DrugClmUpdt = "STAGING.ESIIdsUpd_Update_IDS_DrugClmUpdt_temp"
spark.sql(f"DROP TABLE IF EXISTS {stg_table_DrugClmUpdt}")
df_DrugClmUpdt_final.write.jdbc(jdbc_url, stg_table_DrugClmUpdt, mode="overwrite", properties=jdbc_props)
merge_sql_DrugClmUpdt = f"""
MERGE INTO {_IDSOwner}.DRUG_CLM AS T
USING {stg_table_DrugClmUpdt} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.RECON_DT_SK = S.RECON_DT_SK,
    T.MAX_ALW_CST_REDC_IN = S.MAX_ALW_CST_REDC_IN,
    T.ADM_FEE_AMT = S.ADM_FEE_AMT,
    T.DRUG_CLM_BILL_BSS_CD_SK = S.DRUG_CLM_BILL_BSS_CD_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, RECON_DT_SK, MAX_ALW_CST_REDC_IN, ADM_FEE_AMT, DRUG_CLM_BILL_BSS_CD_SK)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.RECON_DT_SK, S.MAX_ALW_CST_REDC_IN, S.ADM_FEE_AMT, S.DRUG_CLM_BILL_BSS_CD_SK);
"""
execute_dml(merge_sql_DrugClmUpdt, jdbc_url, jdbc_props)

# 3) ClmLnUpdt => merges into #$IDSOwner#.CLM_LN
df_ClmLnUpdt_final = df_ClmLnUpdt
stg_table_ClmLnUpdt = "STAGING.ESIIdsUpd_Update_IDS_ClmLnUpdt_temp"
spark.sql(f"DROP TABLE IF EXISTS {stg_table_ClmLnUpdt}")
df_ClmLnUpdt_final.write.jdbc(jdbc_url, stg_table_ClmLnUpdt, mode="overwrite", properties=jdbc_props)
merge_sql_ClmLnUpdt = f"""
MERGE INTO {_IDSOwner}.CLM_LN AS T
USING {stg_table_ClmLnUpdt} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.PAYBL_AMT = S.PAYBL_AMT
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, PAYBL_AMT)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.PAYBL_AMT);
"""
execute_dml(merge_sql_ClmLnUpdt, jdbc_url, jdbc_props)

# 4) ClmRemitHistUpdt => merges into #$IDSOwner#.CLM_REMIT_HIST
df_ClmRemitHistUpdt_final = df_ClmRemitHistUpdt
stg_table_ClmRemitHistUpdt = "STAGING.ESIIdsUpd_Update_IDS_ClmRemitHistUpdt_temp"
spark.sql(f"DROP TABLE IF EXISTS {stg_table_ClmRemitHistUpdt}")
df_ClmRemitHistUpdt_final.write.jdbc(jdbc_url, stg_table_ClmRemitHistUpdt, mode="overwrite", properties=jdbc_props)
merge_sql_ClmRemitHistUpdt = f"""
MERGE INTO {_IDSOwner}.CLM_REMIT_HIST AS T
USING {stg_table_ClmRemitHistUpdt} AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.ACTL_PD_AMT = S.ACTL_PD_AMT,
    T.COB_PD_AMT = S.COB_PD_AMT
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, ACTL_PD_AMT, COB_PD_AMT)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.ACTL_PD_AMT, S.COB_PD_AMT);
"""
execute_dml(merge_sql_ClmRemitHistUpdt, jdbc_url, jdbc_props)

# Trns2 (Unmatched => multiple outputs)
# 1) InvcUnmatched => ESI_Invoice_Unmatched
df_InvcUnmatched = df_Unmatched.select(
    lit(0).alias("ESI_INVC_EXCPT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLAIM_ID").alias("CLM_ID"),
    lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("UNK").alias("FNCL_LOB_CD"),
    rpad(col("GRP_ID"), 8, " ").alias("GRP_ID"),
    col("MEM_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    rpad(col("GRP_NO"), 18, " ").alias("PROD_ID"),
    rpad(col("PAID_DATE"), 10, " ").alias("PD_DT_SK"),
    rpad(lit(CurrentDate), 10, " ").alias("PRCS_DT_SK"),
    col("AMT_BILL").alias("ACTL_PD_AMT"),
    rpad(col("DT_FILLED"), 10, " ").alias("DT_FILLED")
)

write_files(
    df_InvcUnmatched.select(
        "ESI_INVC_EXCPT_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FNCL_LOB_CD",
        "GRP_ID",
        "MBR_UNIQ_KEY",
        "PROD_ID",
        "PD_DT_SK",
        "PRCS_DT_SK",
        "ACTL_PD_AMT",
        "DT_FILLED"
    ),
    f"{adls_path}/key/ESI_Invoice_Unmatched.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 2) Mbr => hf_esi_invoice_mbr_filldt (Scenario A hashed file => deduplicate on [FILL_DT_SK, MBR_UNIQ_KEY])
df_Mbr = df_Unmatched.select(
    col("DT_FILLED").alias("FILL_DT_SK"),
    col("MEM_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)
df_Mbr_dedup = dedup_sort(
    df_Mbr,
    ["FILL_DT_SK","MBR_UNIQ_KEY"],
    [("FILL_DT_SK","A"), ("MBR_UNIQ_KEY","A")]
)

# 3) EmailUnmatched => ESI_IDS_Unmatched (one column: CLAIM_ID)
df_EmailUnmatched = df_Unmatched.select(
    col("CLAIM_ID").alias("CLAIM_ID")
)

write_files(
    df_EmailUnmatched.select("CLAIM_ID"),
    f"{adls_path_publish}/external/processed/ESI_IDS_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Next: Trns3 => input is Mbr_Date => that is df_Mbr_dedup
# Output => "Load" => W_DRUG_ENR with columns: CLM_ID = @INROWNUM, FILL_DT_SK, MBR_UNIQ_KEY
window_spec = Window.orderBy(lit(1))
df_Trns3 = df_Mbr_dedup.withColumn("CLM_ID", row_number().over(window_spec))

# W_DRUG_ENR => write file
df_W_DRUG_ENR = df_Trns3.select(
    "CLM_ID",
    rpad(col("FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

write_files(
    df_W_DRUG_ENR.select("CLM_ID","FILL_DT_SK","MBR_UNIQ_KEY"),
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)