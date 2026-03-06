# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Update EDW CLM_F  with paid date from ESI weekly Invoice file
# MAGIC     
# MAGIC PROCESSING:
# MAGIC                Update EDW CLM_F with paid date from ESI weekly file
# MAGIC                 If claim on input does not have matching claim in EDW, it is written to an 'unmatched' sequential file for error processing
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Developer                Date                 Project / TTR        Change Description                                                                                        Development Project                    Code Reviewer          Date Reviewed      
# MAGIC ------------------              --------------------    -----------------------       -----------------------------------------------------------------------------------------------------                   --------------------------------                   -------------------------------     ----------------------------   
# MAGIC Sharon Andrew       11/1/08            3784 PBM Vendor  original programming                                                                                       devlIDSnew                                Steph Goddard            11/07/2008
# MAGIC Brent Leland            11-25-2008       3567 Primary Key   Removed writing to unmatched exception file.  Done in IDS updt                  devlIDS
# MAGIC Dan Long               4/19/2013         TTR-1492             Changed the Performance parameters to the new standard values                 IntegrateNewDevl                      Bhoomi Dasari              4/30/2013
# MAGIC                                                                                       of 512 for the buffer size and 300 for the timeout value.                                               
# MAGIC Raja Gummadi        2013-09-20         TFS - 2795          Changed logic for BILL_BSS_CD_SK from TRGT_CD to SRC_CD                  IntegrateNewDevl                       Kalyan Neelam             2013-09-27
# MAGIC SAndrew               2014-01-28                 Added criteria when looking up the ClaimID on IDS.CLM.   We should only update claims that have not yet already been paid.   Therefore, lookup claims on IDS.CLM where ClaimID is on the ESI Invoice file and IDS.PD_DT_SK equals NA.

# MAGIC Claim IDs for missing records in EDW
# MAGIC Read ESI Prepped / Landed Invoice file (only Record 4 types)
# MAGIC Directly updates the EDW Claim tables
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


SourceSys = get_widget_value("SourceSys", "ESI")
RunID = get_widget_value("RunID", "")
RunCycle = get_widget_value("RunCycle", "")
RunCycleDate = get_widget_value("RunCycleDate", "")
CurrentDate = get_widget_value("CurrentDate", "")
EDWOwner = get_widget_value("EDWOwner", "")
edw_secret_name = get_widget_value("edw_secret_name", "")

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

# 1) Read from EDW: CLNDR_DT_D -> df_date
extract_query_date = f"SELECT CLNDR_DT_SK, YR_MO_SK FROM {EDWOwner}.CLNDR_DT_D"
df_date = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_date)
    .load()
)

# 2) Read from EDW: CD_MPPNG -> df_lkup
extract_query_lkup = f"SELECT SRC_CD, SRC_DOMAIN_NM, TRGT_CD, TRGT_CD_NM FROM {EDWOwner}.CD_MPPNG WHERE SRC_DOMAIN_NM in ('DRUG CLAIM BILLED BASIS','CLAIM COB')"
df_lkup = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_lkup)
    .load()
)

# 3) Read from EDW: CLM_F (unfiltered) -> df_EdwClm
extract_query_clm = f"SELECT CLM_SK, SRC_SYS_CD, CLM_ID FROM {EDWOwner}.CLM_F"
df_EdwClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_clm)
    .load()
)

# Deduplicate for hf_esi_edw_invoice_date (Scenario A). Key: CLNDR_DT_SK
df_date_lkup = dedup_sort(
    df_date,
    partition_cols=["CLNDR_DT_SK"],
    sort_cols=[]
)

# Deduplicate for hf_esiinvoice_clmf2_billedbasis (Scenario A). Keys: SRC_CD, SRC_DOMAIN_NM
df_hf_esiinvoice_clmf2_billedbasis = dedup_sort(
    df_lkup,
    partition_cols=["SRC_CD", "SRC_DOMAIN_NM"],
    sort_cols=[]
)

# 4) Read ESI_Invoice file (CSeqFileStage) from "verified/ESIDrugClmInvoicePaidUpdt.dat.#RunID#"
#    Define all columns (matching the DataStage metadata). Map "numeric" to DecimalType(38,10) or similar.
esi_invoice_schema = StructType([
    StructField("RCRD_ID", DecimalType(38, 10), nullable=False),
    StructField("CLAIM_ID", StringType(), nullable=False),
    StructField("PRCSR_NO", DecimalType(38, 10), nullable=False),
    StructField("MEM_CK_KEY", IntegerType(), nullable=False),
    StructField("BTCH_NO", DecimalType(38, 10), nullable=False),
    StructField("PDX_NO", StringType(), nullable=False),
    StructField("RX_NO", DecimalType(38, 10), nullable=False),
    StructField("DT_FILLED", StringType(), nullable=False),
    StructField("NDC_NO", DecimalType(38, 10), nullable=False),
    StructField("DRUG_DESC", StringType(), nullable=False),
    StructField("NEW_RFL_CD", DecimalType(38, 10), nullable=False),
    StructField("METRIC_QTY", DecimalType(38, 10), nullable=False),
    StructField("DAYS_SUPL", DecimalType(38, 10), nullable=False),
    StructField("BSS_OF_CST_DTRM", StringType(), nullable=False),
    StructField("INGR_CST", DecimalType(38, 10), nullable=False),
    StructField("DISPNS_FEE", DecimalType(38, 10), nullable=False),
    StructField("COPAY_AMT", DecimalType(38, 10), nullable=False),
    StructField("SLS_TAX", DecimalType(38, 10), nullable=False),
    StructField("AMT_BILL", DecimalType(38, 10), nullable=False),
    StructField("PATN_FIRST_NM", StringType(), nullable=False),
    StructField("PATN_LAST_NM", StringType(), nullable=False),
    StructField("DOB", DecimalType(38, 10), nullable=False),
    StructField("SEX_CD", DecimalType(38, 10), nullable=False),
    StructField("CARDHLDR_ID_NO", StringType(), nullable=False),
    StructField("RELSHP_CD", DecimalType(38, 10), nullable=False),
    StructField("GRP_NO", StringType(), nullable=False),
    StructField("HOME_PLN", StringType(), nullable=False),
    StructField("HOST_PLN", DecimalType(38, 10), nullable=False),
    StructField("PRESCRIBER_ID", StringType(), nullable=False),
    StructField("DIAG_CD", StringType(), nullable=False),
    StructField("CARDHLDR_FIRST_NM", StringType(), nullable=False),
    StructField("CARDHLDR_LAST_NM", StringType(), nullable=False),
    StructField("PRAUTH_NO", DecimalType(38, 10), nullable=False),
    StructField("PA_MC_SC_NO", StringType(), nullable=False),
    StructField("CUST_LOC", DecimalType(38, 10), nullable=False),
    StructField("RESUB_CYC_CT", DecimalType(38, 10), nullable=False),
    StructField("DT_RX_WRTN", DecimalType(38, 10), nullable=False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), nullable=False),
    StructField("PRSN_CD", StringType(), nullable=False),
    StructField("OTHR_COV_CD", DecimalType(38, 10), nullable=False),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38, 10), nullable=False),
    StructField("CMPND_CD", DecimalType(38, 10), nullable=False),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38, 10), nullable=False),
    StructField("LVL_OF_SVC", DecimalType(38, 10), nullable=False),
    StructField("RX_ORIG_CD", DecimalType(38, 10), nullable=False),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38, 10), nullable=False),
    StructField("PRI_PRESCRIBER", StringType(), nullable=False),
    StructField("CLNC_ID_NO", DecimalType(38, 10), nullable=False),
    StructField("DRUG_TYP", DecimalType(38, 10), nullable=False),
    StructField("PRESCRIBER_LAST_NM", StringType(), nullable=False),
    StructField("POSTAGE_AMT_CLMED", DecimalType(38, 10), nullable=False),
    StructField("UNIT_DOSE_IN", DecimalType(38, 10), nullable=False),
    StructField("OTHR_PAYOR_AMT", DecimalType(38, 10), nullable=False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38, 10), nullable=False),
    StructField("FULL_AWP", DecimalType(38, 10), nullable=False),
    StructField("EXPNSN_AREA", StringType(), nullable=False),
    StructField("MSTR_CAR", StringType(), nullable=False),
    StructField("SUB_CAR", StringType(), nullable=False),
    StructField("CLM_TYP", StringType(), nullable=False),
    StructField("ESI_SUB_GRP", StringType(), nullable=False),
    StructField("PLN_DSGNR", StringType(), nullable=False),
    StructField("ADJDCT_DT", StringType(), nullable=False),
    StructField("ADMIN_FEE", DecimalType(38, 10), nullable=False),
    StructField("CAP_AMT", DecimalType(38, 10), nullable=False),
    StructField("INGR_CST_SUB", DecimalType(38, 10), nullable=False),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38, 10), nullable=False),
    StructField("MBR_PAY_CD", StringType(), nullable=False),
    StructField("INCNTV_FEE", DecimalType(38, 10), nullable=False),
    StructField("CLM_ADJ_AMT", DecimalType(38, 10), nullable=False),
    StructField("CLM_ADJ_CD", StringType(), nullable=False),
    StructField("FRMLRY_FLAG", StringType(), nullable=False),
    StructField("GNRC_CLS_NO", StringType(), nullable=False),
    StructField("THRPTC_CLS_AHFS", StringType(), nullable=False),
    StructField("PDX_TYP", StringType(), nullable=False),
    StructField("BILL_BSS_CD", StringType(), nullable=False),
    StructField("USL_AND_CUST_CHRG", DecimalType(38, 10), nullable=False),
    StructField("PD_DT", DecimalType(38, 10), nullable=False),
    StructField("BNF_CD", StringType(), nullable=False),
    StructField("DRUG_STRG", StringType(), nullable=False),
    StructField("ORIG_MBR", StringType(), nullable=False),
    StructField("DT_OF_INJURY", DecimalType(38, 10), nullable=False),
    StructField("FEE_AMT", DecimalType(38, 10), nullable=False),
    StructField("ESI_REF_NO", StringType(), nullable=False),
    StructField("CLNT_CUST_ID", StringType(), nullable=False),
    StructField("PLN_TYP", StringType(), nullable=False),
    StructField("ESI_ADJDCT_REF_NO", DecimalType(38, 10), nullable=False),
    StructField("ESI_ANCLRY_AMT", DecimalType(38, 10), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("SUBGRP_ID", StringType(), nullable=False),
    StructField("CLS_PLN_ID", StringType(), nullable=False),
    StructField("PAID_DATE", StringType(), nullable=False),       # Not in actual metadata, but shown as char(10) in the job
    StructField("PRTL_FILL_STTUS_CD", StringType(), nullable=False),
    StructField("ESI_BILL_DT", DecimalType(38, 10), nullable=False),
    StructField("FSA_VNDR_CD", StringType(), nullable=False),
    StructField("PICA_DRUG_CD", StringType(), nullable=False),
    StructField("AMT_CLMED", DecimalType(38, 10), nullable=False),
    StructField("AMT_DSALW", DecimalType(38, 10), nullable=False),
    StructField("FED_DRUG_CLS_CD", StringType(), nullable=False),
    StructField("DEDCT_AMT", DecimalType(38, 10), nullable=False),
    StructField("BNF_COPAY_100", StringType(), nullable=False),
    StructField("CLM_PRCS_TYP", StringType(), nullable=False),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38, 10), nullable=False),
    StructField("FLR", StringType(), nullable=False),
    StructField("MCARE_D_COV_DRUG", StringType(), nullable=False),
    StructField("RETRO_LICS_CD", StringType(), nullable=False),
    StructField("RETRO_LICS_AMT", DecimalType(38, 10), nullable=False),
    StructField("LICS_SBSDY_AMT", DecimalType(38, 10), nullable=False),
    StructField("MED_B_DRUG", StringType(), nullable=False),
    StructField("MED_B_CLM", StringType(), nullable=False),
    StructField("PRESCRIBER_QLFR", StringType(), nullable=False),
    StructField("PRESCRIBER_ID_NPI", StringType(), nullable=False),
    StructField("PDX_QLFR", StringType(), nullable=False),
    StructField("PDX_ID_NPI", StringType(), nullable=False),
    StructField("HRA_APLD_AMT", DecimalType(38, 10), nullable=False),
    StructField("ESI_THER_CLS", DecimalType(38, 10), nullable=False),
    StructField("HIC_NO", StringType(), nullable=False),
    StructField("HRA_FLAG", StringType(), nullable=False),
    StructField("DOSE_CD", DecimalType(38, 10), nullable=False),
    StructField("LOW_INCM", StringType(), nullable=False),
    StructField("RTE_OF_ADMIN", StringType(), nullable=False),
    StructField("DEA_SCHD", DecimalType(38, 10), nullable=False),
    StructField("COPAY_BNF_OPT", DecimalType(38, 10), nullable=False),
    StructField("GNRC_PROD_IN_GPI", DecimalType(38, 10), nullable=False),
    StructField("PRESCRIBER_SPEC", StringType(), nullable=False),
    StructField("VAL_CD", StringType(), nullable=False),
    StructField("PRI_CARE_PDX", StringType(), nullable=False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), nullable=False),
])

df_esi_invoice = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .schema(esi_invoice_schema)
    .load(f"{adls_path}/verified/ESIDrugClmInvoicePaidUpdt.dat.{RunID}")
)

# 5) TrnsGetSrcCd -> (Output: PaidClm)
df_trnsgetsrccd = df_esi_invoice.select(
    F.col("CLAIM_ID").alias("CLAIM_ID"),
    F.lit(SourceSys).alias("SRC_CD"),
    F.col("ESI_BILL_DT").alias("PAID_DATE"),
    F.col("AMT_BILL").alias("AMT_BILL"),
    F.col("OTHR_COV_CD").alias("OTHR_COV_CD"),
    F.col("OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    F.col("ADMIN_FEE").alias("ADMIN_FEE"),
    F.col("BILL_BSS_CD").alias("BILL_BSS_CD"),
)

df_paidclm = df_trnsgetsrccd

# 6) Now, in DataStage, the EDW stage had "WHERE SRC_SYS_CD=? AND CLM_ID=?". 
#    We skip that "WHERE" and do a join in "Trns1" using df_EdwClm. 
#    The primary link for "Trns1" is "PaidClm", so replicate the approach of writing "PaidClm" into a staging table (per guidelines).
execute_dml("DROP TABLE IF EXISTS STAGING.ESIEdwUpd_Trns1_temp", jdbc_url, jdbc_props)

df_paidclm.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.ESIEdwUpd_Trns1_temp") \
    .mode("overwrite") \
    .save()

# 7) Trns1 joins:
#    - EdwClm (left join on PaidClm.SRC_CD=EdwClm.SRC_SYS_CD and PaidClm.CLAIM_ID=EdwClm.CLM_ID)
#    - billedbasislkup (left join on PaidClm.BILL_BSS_CD=billedbasislkup.SRC_CD and 'DRUG CLAIM BILLED BASIS'=SRC_DOMAIN_NM)
#    Stage variable: PdDtSk = GetFkeyDate('IDS', EdwClm.CLM_SK, PaidClm.PAID_DATE, 'X')
df_trns1_joined = (
    df_paidclm.alias("PaidClm")
    .join(
        df_EdwClm.alias("EdwClm"),
        (F.col("PaidClm.SRC_CD") == F.col("EdwClm.SRC_SYS_CD"))
        & (F.col("PaidClm.CLAIM_ID") == F.col("EdwClm.CLM_ID")),
        how="left"
    )
    .join(
        df_hf_esiinvoice_clmf2_billedbasis.alias("billedbasislkup"),
        (F.col("PaidClm.BILL_BSS_CD") == F.col("billedbasislkup.SRC_CD"))
        & (F.lit("DRUG CLAIM BILLED BASIS") == F.col("billedbasislkup.SRC_DOMAIN_NM")),
        how="left"
    )
)

df_trns1_withsv = df_trns1_joined.withColumn(
    "PdDtSk",
    GetFkeyDate("IDS", F.col("EdwClm.CLM_SK"), F.col("PaidClm.PAID_DATE"), "X")
)

# Output 1: "GetPaidYrMo" -> constraint IsNull(EdwClm.CLM_SK)=False
df_getpaidyrmo = df_trns1_withsv.filter(F.col("EdwClm.CLM_SK").isNotNull()).select(
    F.col("EdwClm.CLM_SK").alias("CLM_SK"),
    F.col("EdwClm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("EdwClm.CLM_ID").alias("CLM_ID"),
    F.lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("PdDtSk").alias("CLM_PD_DT_SK"),
    F.col("PaidClm.AMT_BILL").alias("AMT_BILL"),
    F.col("PaidClm.OTHR_COV_CD").alias("OTHR_COV_CD"),
    F.col("PaidClm.OTHR_PAYOR_AMT").alias("OTHR_PAYOR_AMT"),
    F.col("PaidClm.ADMIN_FEE").alias("ADMIN_FEE"),
    F.when(
        F.col("billedbasislkup.TRGT_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("billedbasislkup.TRGT_CD")).alias("BILL_BSS_CD"),
    F.when(
        F.col("billedbasislkup.TRGT_CD_NM").isNull(),
        F.lit("UNKNOWN")
    ).otherwise(F.col("billedbasislkup.TRGT_CD_NM")).alias("BILL_BSS_NM"),
    F.when(
        F.col("billedbasislkup.TRGT_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("billedbasislkup.SRC_CD")).alias("BILL_BSS_SRC_CD")
)

# Output 2: "Unmatched" -> constraint IsNull(EdwClm.CLM_SK)=True
df_unmatched = df_trns1_withsv.filter(F.col("EdwClm.CLM_SK").isNull()).select(
    F.col("PaidClm.CLAIM_ID").alias("CLAIM_ID")
)

# Write unmatched to external/processed => ESI_EDW_Unmatched_#RunID#.txt
# Apply rpad if char/varchar. Here CLAIM_ID was "varchar" with no stated length, use an assumed length, e.g. 20.
df_unmatched_final = df_unmatched.withColumn(
    "CLAIM_ID",
    F.rpad(F.col("CLAIM_ID"), 20, " ")
)
write_files(
    df_unmatched_final.select("CLAIM_ID"),
    f"{adls_path_publish}/processed/ESI_EDW_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 8) Next stage: "PaidYrMo" with primary link = df_getpaidyrmo
execute_dml("DROP TABLE IF EXISTS STAGING.ESIEdwUpd_PaidYrMo_temp", jdbc_url, jdbc_props)

df_getpaidyrmo.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.ESIEdwUpd_PaidYrMo_temp") \
    .mode("overwrite") \
    .save()

# Perform left joins:
# - date_lkup on CLM_PD_DT_SK == CLNDR_DT_SK
# - coblookup on OTHR_COV_CD == SRC_CD AND 'CLAIM COB' == SRC_DOMAIN_NM
df_paidyrmo_joined = (
    df_getpaidyrmo.alias("GetPaidYrMo")
    .join(
        df_date_lkup.alias("Date_lkup"),
        F.col("GetPaidYrMo.CLM_PD_DT_SK") == F.col("Date_lkup.CLNDR_DT_SK"),
        how="left"
    )
    .join(
        df_hf_esiinvoice_clmf2_billedbasis.alias("coblookup"),
        (F.col("GetPaidYrMo.OTHR_COV_CD") == F.col("coblookup.SRC_CD"))
        & (F.lit("CLAIM COB") == F.col("coblookup.SRC_DOMAIN_NM")),
        how="left"
    )
)

# Stage variables in PaidYrMo:
# 1) CobType = IF OTHR_COV_CD=2 AND OTHR_PAYOR_AMT<>0 THEN OTHR_COV_CD ELSE 'NA'
# 2) ClmCobTypCdSk = GetFkeyCodes('ESI', 1, 'CLAIM COB', CobType, 'X')
# 3) ClmPdYrMoSk = IF IsNull(Date_lkup.YR_MO_SK) THEN substring(CLM_PD_DT_SK,1,4) + substring(CLM_PD_DT_SK,6,2) ELSE Date_lkup.YR_MO_SK
# 4) BllBssCdSk = GetFkeyCodes(SRC_SYS_CD, CLM_SK, ' DRUG CLAIM BILLED BASIS', BILL_BSS_SRC_CD, 'X')
df_paidyrmo_vars = (
    df_paidyrmo_joined
    .withColumn(
        "CobType",
        F.when(
            (F.col("GetPaidYrMo.OTHR_COV_CD") == F.lit(2))
            & (F.col("GetPaidYrMo.OTHR_PAYOR_AMT") != 0),
            F.col("GetPaidYrMo.OTHR_COV_CD").cast(StringType())
        ).otherwise(F.lit("NA"))
    )
    .withColumn(
        "ClmCobTypCdSk",
        GetFkeyCodes("ESI", F.lit(1), "CLAIM COB", F.col("CobType"), "X")
    )
    .withColumn(
        "ClmPdYrMoSk",
        F.when(
            F.col("Date_lkup.YR_MO_SK").isNull(),
            F.concat(F.col("GetPaidYrMo.CLM_PD_DT_SK").substr(F.lit(1), F.lit(4)),
                     F.col("GetPaidYrMo.CLM_PD_DT_SK").substr(F.lit(6), F.lit(2)))
        ).otherwise(F.col("Date_lkup.YR_MO_SK"))
    )
    .withColumn(
        "BllBssCdSk",
        GetFkeyCodes(
            F.col("GetPaidYrMo.SRC_SYS_CD"),
            F.col("GetPaidYrMo.CLM_SK"),
            " DRUG CLAIM BILLED BASIS",
            F.col("GetPaidYrMo.BILL_BSS_SRC_CD"),
            "X"
        )
    )
)

# Now produce the three outputs to Update_Clm_F:

# ClmFactUpdt
df_ClmFactUpdt = df_paidyrmo_vars.select(
    F.col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
    F.lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GetPaidYrMo.CLM_PD_DT_SK").alias("CLM_PD_DT_SK"),
    F.col("ClmPdYrMoSk").alias("CLM_PD_YR_MO_SK"),
    F.when(
        (F.col("GetPaidYrMo.OTHR_COV_CD") == 2)
        & (F.col("GetPaidYrMo.OTHR_PAYOR_AMT") != 0),
        F.when(F.col("coblookup.TRGT_CD").isNull(), F.lit("UNK")).otherwise(F.col("coblookup.TRGT_CD"))
    ).otherwise(F.lit("NA")).alias("CLM_COB_CD"),
    F.col("GetPaidYrMo.AMT_BILL").alias("CLM_ACTL_PD_AMT"),
    F.col("GetPaidYrMo.AMT_BILL").alias("CLM_PAYBL_AMT"),
    F.col("GetPaidYrMo.ADMIN_FEE").alias("DRUG_CLM_ADM_FEE_AMT"),
    F.col("GetPaidYrMo.AMT_BILL").alias("CLM_LN_TOT_PAYBL_AMT")
)

# ClmF2Updt
df_ClmF2Updt = df_paidyrmo_vars.select(
    F.col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
    F.lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BllBssCdSk").alias("DRUG_CLM_BILL_BSS_CD_SK"),
    F.col("GetPaidYrMo.BILL_BSS_CD").alias("DRUG_CLM_BILL_BSS_CD"),
    F.col("GetPaidYrMo.BILL_BSS_NM").alias("DRUG_CLM_BILL_BSS_NM"),
    F.col("ClmCobTypCdSk").alias("CLM_COB_CD_SK")
)

# ClmLnFUpdt
df_ClmLnFUpdt = df_paidyrmo_vars.select(
    F.col("GetPaidYrMo.CLM_SK").alias("CLM_SK"),
    F.lit(RunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GetPaidYrMo.AMT_BILL").alias("CLM_LN_PAYBL_AMT")
)

# For writing to DB, each link merges into a different table.

# -------------------------------------------------------------
# Merge #1: "ClmFactUpdt" -> #$EDWOwner#.CLM_F
# We rpad columns defined as CHAR(...) or VARCHAR(...).
df_ClmFactUpdt_final = (
    df_ClmFactUpdt
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("CLM_PD_DT_SK", F.rpad(F.col("CLM_PD_DT_SK"), 10, " "))
    .withColumn("CLM_PD_YR_MO_SK", F.rpad(F.col("CLM_PD_YR_MO_SK"), 6, " "))
    # CLM_COB_CD not declared as char() in the snippet, assume char(20):
    .withColumn("CLM_COB_CD", F.rpad(F.col("CLM_COB_CD"), 20, " "))
)

execute_dml("DROP TABLE IF EXISTS STAGING.ESIEdwUpd_Update_Clm_F_ClmFactUpdt_temp", jdbc_url, jdbc_props)
df_ClmFactUpdt_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.ESIEdwUpd_Update_Clm_F_ClmFactUpdt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_1 = f"""
MERGE INTO {EDWOwner}.CLM_F AS T
USING STAGING.ESIEdwUpd_Update_Clm_F_ClmFactUpdt_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    T.CLM_PD_DT_SK = S.CLM_PD_DT_SK,
    T.CLM_PD_YR_MO_SK = S.CLM_PD_YR_MO_SK,
    T.CLM_COB_CD = S.CLM_COB_CD,
    T.CLM_ACTL_PD_AMT = S.CLM_ACTL_PD_AMT,
    T.CLM_PAYBL_AMT = S.CLM_PAYBL_AMT,
    T.DRUG_CLM_ADM_FEE_AMT = S.DRUG_CLM_ADM_FEE_AMT,
    T.CLM_LN_TOT_PAYBL_AMT = S.CLM_LN_TOT_PAYBL_AMT
WHEN NOT MATCHED THEN INSERT (
  CLM_SK,
  LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  CLM_PD_DT_SK,
  CLM_PD_YR_MO_SK,
  CLM_COB_CD,
  CLM_ACTL_PD_AMT,
  CLM_PAYBL_AMT,
  DRUG_CLM_ADM_FEE_AMT,
  CLM_LN_TOT_PAYBL_AMT
)
VALUES (
  S.CLM_SK,
  S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  S.CLM_PD_DT_SK,
  S.CLM_PD_YR_MO_SK,
  S.CLM_COB_CD,
  S.CLM_ACTL_PD_AMT,
  S.CLM_PAYBL_AMT,
  S.DRUG_CLM_ADM_FEE_AMT,
  S.CLM_LN_TOT_PAYBL_AMT
);
"""
execute_dml(merge_sql_1, jdbc_url, jdbc_props)

# -------------------------------------------------------------
# Merge #2: "ClmF2Updt" -> #$EDWOwner#.CLM_F2
df_ClmF2Updt_final = (
    df_ClmF2Updt
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    # DRUG_CLM_BILL_BSS_CD was (char(20)) -> pad:
    .withColumn("DRUG_CLM_BILL_BSS_CD", F.rpad(F.col("DRUG_CLM_BILL_BSS_CD"), 20, " "))
    # DRUG_CLM_BILL_BSS_NM was (varchar(255)) -> pad to 255
    .withColumn("DRUG_CLM_BILL_BSS_NM", F.rpad(F.col("DRUG_CLM_BILL_BSS_NM"), 255, " "))
)

execute_dml("DROP TABLE IF EXISTS STAGING.ESIEdwUpd_Update_Clm_F_ClmF2Updt_temp", jdbc_url, jdbc_props)
df_ClmF2Updt_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.ESIEdwUpd_Update_Clm_F_ClmF2Updt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_2 = f"""
MERGE INTO {EDWOwner}.CLM_F2 AS T
USING STAGING.ESIEdwUpd_Update_Clm_F_ClmF2Updt_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    T.DRUG_CLM_BILL_BSS_CD_SK = S.DRUG_CLM_BILL_BSS_CD_SK,
    T.DRUG_CLM_BILL_BSS_CD = S.DRUG_CLM_BILL_BSS_CD,
    T.DRUG_CLM_BILL_BSS_NM = S.DRUG_CLM_BILL_BSS_NM,
    T.CLM_COB_CD_SK = S.CLM_COB_CD_SK
WHEN NOT MATCHED THEN INSERT (
  CLM_SK,
  LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  DRUG_CLM_BILL_BSS_CD_SK,
  DRUG_CLM_BILL_BSS_CD,
  DRUG_CLM_BILL_BSS_NM,
  CLM_COB_CD_SK
)
VALUES (
  S.CLM_SK,
  S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  S.DRUG_CLM_BILL_BSS_CD_SK,
  S.DRUG_CLM_BILL_BSS_CD,
  S.DRUG_CLM_BILL_BSS_NM,
  S.CLM_COB_CD_SK
);
"""
execute_dml(merge_sql_2, jdbc_url, jdbc_props)

# -------------------------------------------------------------
# Merge #3: "ClmLnFUpdt" -> #$EDWOwner#.CLM_LN_F
df_ClmLnFUpdt_final = (
    df_ClmLnFUpdt
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
)

execute_dml("DROP TABLE IF EXISTS STAGING.ESIEdwUpd_Update_Clm_F_ClmLnFUpdt_temp", jdbc_url, jdbc_props)
df_ClmLnFUpdt_final.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.ESIEdwUpd_Update_Clm_F_ClmLnFUpdt_temp") \
    .mode("overwrite") \
    .save()

merge_sql_3 = f"""
MERGE INTO {EDWOwner}.CLM_LN_F AS T
USING STAGING.ESIEdwUpd_Update_Clm_F_ClmLnFUpdt_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_DT_SK = S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
    T.CLM_LN_PAYBL_AMT = S.CLM_LN_PAYBL_AMT
WHEN NOT MATCHED THEN INSERT (
  CLM_SK,
  LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  CLM_LN_PAYBL_AMT
)
VALUES (
  S.CLM_SK,
  S.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
  S.CLM_LN_PAYBL_AMT
);
"""
execute_dml(merge_sql_3, jdbc_url, jdbc_props)