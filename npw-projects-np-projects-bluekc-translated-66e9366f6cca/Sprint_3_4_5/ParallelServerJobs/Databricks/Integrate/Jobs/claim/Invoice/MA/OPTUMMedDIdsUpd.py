# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Update IDS CLM and DRUG_CLM with paid date from OPTUMRX file
# MAGIC       
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                Update IDS CLM with paid date
# MAGIC                Update IDS DRUG_CLM recon_dt_sk with current date
# MAGIC               If claim on input does not have matching claim in IDS, it is written to an 'unmatched' sequential file for error processing
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                       Change Description                                                                             Project #              Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ----------------------------------------------------------------------------                               ----------------               ------------------------------------       ----------------------------           ----------------
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs                      Initial Development                   IntegrateDev2	Abhiram Dasarathy	2020-12-11

# MAGIC Used to find finacial LOB
# MAGIC Read weekly Invoice file from OPTUMRX OPTUMRXDrugClmInvoiceLand
# MAGIC Direct update of CLM and DRUG_CLM
# MAGIC Claim IDs for missing records in IDS
# MAGIC Claims unmatched to IDS claims written to exception file.  Used in OPTUMRXInvoiceExcptTrans
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SourceSys = get_widget_value("SourceSys","OPTUMRX")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","")
CurrentDate = get_widget_value("CurrentDate","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# ----------------------------------------------------------------
# Read full table (remove '?' parameter conditions and let downstream join handle it)
df_IdsClm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CLM_SK, SRC_SYS_CD_SK, CLM_ID FROM {IDSOwner}.CLM")
    .load()
)

df_src_cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT SRC_CD, CD_MPPNG_SK
        FROM {IDSOwner}.CD_MPPNG
        WHERE SRC_DRVD_LKUP_VAL = 'OPTUMRX'
          AND SRC_SYS_CD = 'IDS'
          AND SRC_CLCTN_CD = 'IDS'
          AND SRC_DOMAIN_NM = 'SOURCE SYSTEM'
          AND TRGT_CLCTN_CD = 'IDS'
          AND TRGT_DOMAIN_NM = 'SOURCE SYSTEM'
        """
    )
    .load()
)

# Hashed file hf_optum_invoice_ids_src_cd (Scenario A) => deduplicate on key SRC_CD
df_hf_optum_invoice_ids_src_cd = dedup_sort(
    df_src_cd,
    ["SRC_CD"],
    [("SRC_CD","A")]
)

schema_OPTUMRX_Invoice = StructType([
    StructField("CLAIM_ID", StringType(), False),
    StructField("INVC_DT", DecimalType(8,0), False),
    StructField("TOT_CST", DecimalType(18,2), False),
    StructField("CLM_ADM_FEE", DecimalType(18,2), False),
    StructField("BILL_CLM_CST", DecimalType(18,2), False),
    StructField("MBR_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("CLM_STTUS", StringType(), False),
    StructField("GRP_NM", StringType(), True),
    StructField("ACCT_ID", StringType(), True)
])

df_OPTUMRX_Invoice = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", False)
    .schema(schema_OPTUMRX_Invoice)
    .load(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

# Transformer TrnsGetSrcCd: left join on constant 'OPTUMRX' = SRC_CD
df_PaidClm = (
    df_OPTUMRX_Invoice.alias("Input")
    .join(
        df_hf_optum_invoice_ids_src_cd.alias("src_cd_lkup"),
        on=(F.lit("OPTUMRX") == F.col("src_cd_lkup.SRC_CD")),
        how="left"
    )
    .select(
        F.rpad(F.col("Input.CLAIM_ID"), <...>, " ").alias("CLAIM_ID"),
        F.rpad(F.col("src_cd_lkup.SRC_CD"), <...>, " ").alias("SRC_CD"),
        F.col("src_cd_lkup.CD_MPPNG_SK").alias("SRC_SYS_CD_SK"),
        F.rpad(F.col("Input.INVC_DT").cast(StringType()), 10, " ").alias("PAID_DATE"),
        F.col("Input.TOT_CST").alias("TOT_CST"),
        F.col("Input.CLM_ADM_FEE").alias("ADMIN_FEE"),
        F.col("Input.BILL_CLM_CST").alias("BILL_CLM_CST"),
        F.rpad(F.col("Input.GRP_ID"), 8, " ").alias("GRP_ID"),
        F.rpad(F.col("Input.MBR_ID"), 20, " ").alias("MEM_CK_KEY"),
        F.rpad(F.col("Input.GRP_ID"), 18, " ").alias("GRP_NO"),
        F.rpad(F.col("Input.DT_FILLED"), 10, " ").alias("DT_FILLED"),
        F.rpad(F.col("Input.ACCT_ID"), 15, " ").alias("ACCT_ID")
    )
)

# Transformer Trns1: Left-join with df_IdsClm on (SRC_SYS_CD_SK, CLAIM_ID). 
# The job's JSON also mentions PaidClm.CLM_SK = IdsClm.CLM_SK, but no CLM_SK column exists in PaidClm;
# we proceed with the columns that do exist:
df_Trns1 = (
    df_PaidClm.alias("PaidClm")
    .join(
        df_IdsClm.alias("IdsClm"),
        on=[
            F.col("PaidClm.SRC_SYS_CD_SK") == F.col("IdsClm.SRC_SYS_CD_SK"),
            F.col("PaidClm.CLAIM_ID") == F.col("IdsClm.CLM_ID")
        ],
        how="left"
    )
    .withColumn(
        "ClmSk",
        F.when(F.col("IdsClm.CLM_ID").isNull(), F.lit(0)).otherwise(F.col("IdsClm.CLM_SK"))
    )
    .withColumn(
        "PdDtSk",
        F.expr("GetFkeyDate('IDS', ClmSk, PaidClm.PAID_DATE, 'X')")
    )
    .withColumn(
        "ReconDtSk",
        F.expr("GetFkeyDate('IDS', ClmSk, CurrentDate, 'X')")
    )
)

# Output link: ClmUpdt (Constraint: ClmSk <> 0)
df_Trns1_ClmUpdt = (
    df_Trns1
    .filter(F.col("ClmSk") != 0)
    .select(
        F.col("ClmSk").alias("CLM_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.rpad(F.col("PdDtSk"), 10, " ").alias("PD_DT_SK")
    )
)

# Output link: DrugClmUpdt (Constraint: ClmSk <> 0)
df_Trns1_DrugClmUpdt = (
    df_Trns1
    .filter(F.col("ClmSk") != 0)
    .select(
        F.col("ClmSk").alias("CLM_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.rpad(F.col("ReconDtSk"), 10, " ").alias("RECON_DT_SK"),
        F.col("PaidClm.ADMIN_FEE").alias("ADM_FEE_AMT")
    )
)

# Output link: Unmatched (Constraint: IsNull(IdsClm.CLM_SK) => ClmSk=0)
df_Trns1_Unmatched = (
    df_Trns1
    .filter(F.col("ClmSk") == 0)
    .select(
        F.col("PaidClm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("PaidClm.CLAIM_ID").alias("CLAIM_ID"),
        F.col("PaidClm.PAID_DATE").alias("PAID_DATE"),
        F.col("PaidClm.BILL_CLM_CST").alias("AMT_BILL"),
        F.rpad(F.col("PaidClm.GRP_ID"), 8, " ").alias("GRP_ID"),
        F.col("PaidClm.MEM_CK_KEY").alias("MEM_UNIQ_KEY"),
        F.rpad(F.col("PaidClm.GRP_NO"), 18, " ").alias("GRP_NO"),
        F.rpad(F.col("PaidClm.DT_FILLED"), 10, " ").alias("DT_FILLED"),
        F.rpad(F.col("PaidClm.ACCT_ID"), 15, " ").alias("ACCT_ID")
    )
)

# Output link: DrugClmPriceUpdt (No constraint)
df_Trns1_DrugClmPriceUpdt = (
    df_Trns1
    .select(
        F.col("PaidClm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("PaidClm.CLAIM_ID").alias("CLM_ID"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PaidClm.BILL_CLM_CST").alias("INVC_TOT_DUE_AMT")
    )
)

# ----------------------
# DB2_Connector_156 merges for each link
# ClmUpdt
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_ClmUpdt_temp",
    jdbc_url,
    jdbc_props
)
(
    df_Trns1_ClmUpdt.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_ClmUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_ClmUpdt = """
MERGE <...> AS T
USING STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_ClmUpdt_temp AS S
ON (T.CLM_SK = S.CLM_SK)
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.PD_DT_SK = S.PD_DT_SK
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, PD_DT_SK)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.PD_DT_SK);
"""
execute_dml(merge_sql_ClmUpdt, jdbc_url, jdbc_props)

# DrugClmUpdt
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_DrugClmUpdt_temp",
    jdbc_url,
    jdbc_props
)
(
    df_Trns1_DrugClmUpdt.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_DrugClmUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_DrugClmUpdt = """
MERGE <...> AS T
USING STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_DrugClmUpdt_temp AS S
ON (T.CLM_SK = S.CLM_SK)
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.RECON_DT_SK = S.RECON_DT_SK,
    T.ADM_FEE_AMT = S.ADM_FEE_AMT
WHEN NOT MATCHED THEN
  INSERT (CLM_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, RECON_DT_SK, ADM_FEE_AMT)
  VALUES (S.CLM_SK, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.RECON_DT_SK, S.ADM_FEE_AMT);
"""
execute_dml(merge_sql_DrugClmUpdt, jdbc_url, jdbc_props)

# DrugClmPriceUpdt
execute_dml(
    f"DROP TABLE IF EXISTS STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp",
    jdbc_url,
    jdbc_props
)
(
    df_Trns1_DrugClmPriceUpdt.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_DrugClmPriceUpdt = """
MERGE <...> AS T
USING STAGING.OPTUMMedDIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp AS S
ON (T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK AND T.CLM_ID = S.CLM_ID)
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.INVC_TOT_DUE_AMT = S.INVC_TOT_DUE_AMT
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD_SK, CLM_ID, LAST_UPDT_RUN_CYC_EXCTN_SK, INVC_TOT_DUE_AMT)
  VALUES (S.SRC_SYS_CD_SK, S.CLM_ID, S.LAST_UPDT_RUN_CYC_EXCTN_SK, S.INVC_TOT_DUE_AMT);
"""
execute_dml(merge_sql_DrugClmPriceUpdt, jdbc_url, jdbc_props)

# ----------------------
# Transformer Trns2 uses df_Trns1_Unmatched as PrimaryLink
df_InvcUnmatched = (
    df_Trns1_Unmatched
    .select(
        F.lit(0).alias("ESI_INVC_EXCPT_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("CLAIM_ID").alias("CLM_ID"),
        F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit("UNK").alias("FNCL_LOB_CD"),
        F.rpad(F.col("GRP_ID"), 8, " ").alias("GRP_ID"),
        F.col("MEM_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.rpad(F.col("GRP_NO"), 18, " ").alias("PROD_ID"),
        F.rpad(F.col("PAID_DATE"), 10, " ").alias("PD_DT_SK"),
        F.rpad(F.lit(CurrentDate), 10, " ").alias("PRCS_DT_SK"),
        F.col("AMT_BILL").alias("ACTL_PD_AMT"),
        F.rpad(F.col("DT_FILLED"), 10, " ").alias("DT_FILLED"),
        F.rpad(F.col("ACCT_ID"), 15, " ").alias("ACCT_ID")
    )
)

df_Mbr = (
    df_Trns1_Unmatched
    .select(
        F.rpad(F.col("DT_FILLED"), 10, " ").alias("FILL_DT_SK"),
        F.col("MEM_UNIQ_KEY").alias("MBR_UNIQ_KEY")
    )
)

df_EmailUnmatched = (
    df_Trns1_Unmatched
    .select(
        F.col("CLAIM_ID")
    )
)

# Write OPTUMRX_Invoice_Unmatched
write_files(
    df_InvcUnmatched,
    f"{adls_path}/key/OPTUMRX_Invoice_Unmatched.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Write OPTUMRX_IDS_Unmatched
write_files(
    df_EmailUnmatched,
    f"{adls_path_publish}/external/processed/OPTUMRX_IDS_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Hashed file hf_optum_invoice_mbr_filldt (Scenario A) => deduplicate on key (FILL_DT_SK, MBR_UNIQ_KEY)
df_hf_optum_invoice_mbr_filldt = dedup_sort(
    df_Mbr,
    ["FILL_DT_SK","MBR_UNIQ_KEY"],
    [("FILL_DT_SK","A"),("MBR_UNIQ_KEY","A")]
)

# Transformer Trns3
df_Trns3 = (
    df_hf_optum_invoice_mbr_filldt.alias("Mbr_Date")
    .withColumn("CLM_ID", F.monotonically_increasing_id())
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.rpad(F.col("Mbr_Date.FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
        F.col("Mbr_Date.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
    )
)

# Write W_DRUG_ENR
write_files(
    df_Trns3,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)