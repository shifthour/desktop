# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
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
# MAGIC Developer                    Date                       Change Description                                                                             Project #                  Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ----------------------------------------------------------------------------                               ----------------               ------------------------------------       ----------------------------           ----------------
# MAGIC Deepa Bajaj           2019-10-23                 Originally created                                                                                6131- PBM Replacement     IntegrateDev2        Kalyan Neelam              2019-11-27

# MAGIC Used to find finacial LOB
# MAGIC Read weekly Invoice file from OPTUMRX OPTUMRXDrugClmInvoiceLand
# MAGIC Direct update of CLM and DRUG_CLM
# MAGIC Claim IDs for missing records in IDS
# MAGIC Claims unmatched to IDS claims written to exception file.  Used in OPTUMRXInvoiceExcptTrans
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
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType, IntegerType, 
    BooleanType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
SourceSys = get_widget_value('SourceSys','OPTUMRX')
RunID = get_widget_value('RunID','100')
RunCycle = get_widget_value('RunCycle','100')
CurrentDate = get_widget_value('CurrentDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Database connection for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read from #$IDSOwner#.CD_MPPNG (src_cd link)
df_ids_src_cd = (
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

# Read from #$IDSOwner#.CLM (IdsClm link) - Remove "WHERE ... = ?" for a full set, to be joined downstream
df_ids_IdsClmAll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
        SELECT CLM_SK, SRC_SYS_CD_SK, CLM_ID
        FROM {IDSOwner}.CLM
        """
    )
    .load()
)

# Schema for OPTUMRX_Invoice (CSeqFileStage)
schema_OPTUMRX_Invoice = StructType([
    StructField("CLAIM_ID", StringType(), nullable=False),
    StructField("INVC_DT", DecimalType(15, 0), nullable=False),
    StructField("TOT_CST", DecimalType(15, 2), nullable=False),
    StructField("CLM_ADM_FEE", DecimalType(15, 2), nullable=False),
    StructField("BILL_CLM_CST", DecimalType(15, 2), nullable=False),
    StructField("MBR_ID", StringType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("DT_FILLED", StringType(), nullable=False),
    StructField("CLM_STTUS", StringType(), nullable=False),
    StructField("GRP_NM", StringType(), nullable=True),
    StructField("ACCT_ID", StringType(), nullable=True)
])

df_OPTUMRX_Invoice = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_OPTUMRX_Invoice)
    .csv(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

# hf_optum_invoice_ids_src_cd (CHashedFileStage) - Scenario A (intermediate)
# Deduplicate on key columns "SRC_CD"
df_hf_optum_invoice_ids_src_cd = dedup_sort(df_ids_src_cd, ["SRC_CD"], [])

# TrnsGetSrcCd (CTransformerStage)
df_TrnsGetSrcCd = (
    df_OPTUMRX_Invoice.alias("Input")
    .join(
        df_hf_optum_invoice_ids_src_cd.alias("src_cd_lkup"),
        (F.lit("OPTUMRX") == F.col("src_cd_lkup.SRC_CD")),
        how="left"
    )
)

df_PaidClm = df_TrnsGetSrcCd.select(
    F.col("Input.CLAIM_ID").alias("CLAIM_ID"),
    F.col("src_cd_lkup.SRC_CD").alias("SRC_CD"),
    F.col("src_cd_lkup.CD_MPPNG_SK").alias("SRC_SYS_CD_SK"),
    F.col("Input.INVC_DT").alias("PAID_DATE"),
    F.col("Input.TOT_CST").alias("TOT_CST"),
    F.col("Input.CLM_ADM_FEE").alias("ADMIN_FEE"),
    F.col("Input.BILL_CLM_CST").alias("BILL_CLM_CST"),
    F.col("Input.GRP_ID").alias("GRP_ID"),
    F.col("Input.MBR_ID").alias("MEM_CK_KEY"),
    F.col("Input.GRP_ID").alias("GRP_NO"),
    F.col("Input.DT_FILLED").alias("DT_FILLED"),
    F.col("Input.ACCT_ID").alias("ACCT_ID")
)

# Trns1 (CTransformerStage) - Join with IdsClm (left)
df_Trns1_join = (
    df_PaidClm.alias("PaidClm")
    .join(
        df_ids_IdsClmAll.alias("IdsClm"),
        (
            (F.col("PaidClm.SRC_SYS_CD_SK") == F.col("IdsClm.SRC_SYS_CD_SK"))
            & (F.col("PaidClm.CLAIM_ID") == F.col("IdsClm.CLM_ID"))
        ),
        how="left"
    )
)

df_Trns1_interim = (
    df_Trns1_join
    .withColumn(
        "ClmSk",
        F.when(F.col("IdsClm.CLM_ID").isNull(), F.lit(0)).otherwise(F.col("IdsClm.CLM_SK"))
    )
    .withColumn(
        "PdDtSk",
        GetFkeyDate(F.lit("IDS"), F.col("ClmSk"), F.col("PaidClm.PAID_DATE"), F.lit("X"))
    )
    .withColumn(
        "ReconDtSk",
        GetFkeyDate(F.lit("IDS"), F.col("ClmSk"), F.lit(CurrentDate), F.lit("X"))
    )
)

# Trns1 outputs
# ClmUpdt
dfTrns1_ClmUpdt = (
    df_Trns1_interim
    .filter(F.col("ClmSk") != 0)
    .select(
        F.col("ClmSk").alias("CLM_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PdDtSk").alias("PD_DT_SK")
    )
)

# DrugClmUpdt
dfTrns1_DrugClmUpdt = (
    df_Trns1_interim
    .filter(F.col("ClmSk") != 0)
    .select(
        F.col("ClmSk").alias("CLM_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ReconDtSk").alias("RECON_DT_SK"),
        F.col("PaidClm.ADMIN_FEE").alias("ADM_FEE_AMT")
    )
)

# Unmatched
dfTrns1_Unmatched = (
    df_Trns1_interim
    .filter(F.col("IdsClm.CLM_SK").isNull())
    .select(
        F.col("PaidClm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("PaidClm.CLAIM_ID").alias("CLAIM_ID"),
        F.col("PaidClm.PAID_DATE").alias("PAID_DATE"),
        F.col("PaidClm.BILL_CLM_CST").alias("AMT_BILL"),
        F.col("PaidClm.GRP_ID").alias("GRP_ID"),
        F.col("PaidClm.MEM_CK_KEY").alias("MEM_UNIQ_KEY"),
        F.col("PaidClm.GRP_NO").alias("GRP_NO"),
        F.col("PaidClm.DT_FILLED").alias("DT_FILLED"),
        F.col("PaidClm.ACCT_ID").alias("ACCT_ID")
    )
)

# DrugClmPriceUpdt
dfTrns1_DrugClmPriceUpdt = (
    df_Trns1_interim
    .select(
        F.col("PaidClm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("PaidClm.CLAIM_ID").alias("CLM_ID"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PaidClm.BILL_CLM_CST").alias("INVC_TOT_DUE_AMT")
    )
)

# DB2_Connector_156 (DB2Connector) - upserts for each input pin

# 1) ClmUpdt
spark.sql(f"DROP TABLE IF EXISTS STAGING.OPTUMIdsUpd_DB2_Connector_156_ClmUpdt_temp")
(
    dfTrns1_ClmUpdt
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMIdsUpd_DB2_Connector_156_ClmUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_ClmUpdt = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING STAGING.OPTUMIdsUpd_DB2_Connector_156_ClmUpdt_temp AS S
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

# 2) DrugClmUpdt
spark.sql(f"DROP TABLE IF EXISTS STAGING.OPTUMIdsUpd_DB2_Connector_156_DrugClmUpdt_temp")
(
    dfTrns1_DrugClmUpdt
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMIdsUpd_DB2_Connector_156_DrugClmUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_DrugClmUpdt = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING STAGING.OPTUMIdsUpd_DB2_Connector_156_DrugClmUpdt_temp AS S
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

# 3) DrugClmPriceUpdt
spark.sql(f"DROP TABLE IF EXISTS STAGING.OPTUMIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp")
(
    dfTrns1_DrugClmPriceUpdt
    .write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_DrugClmPriceUpdt = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING STAGING.OPTUMIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp AS S
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

# Trns2 (CTransformerStage) - input dfTrns1_Unmatched
df_Trns2 = dfTrns1_Unmatched

dfInvcUnmatched = df_Trns2.select(
    F.lit(0).alias("ESI_INVC_EXCPT_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLAIM_ID").alias("CLM_ID"),
    F.lit(RunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("UNK").alias("FNCL_LOB_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MEM_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("GRP_NO").alias("PROD_ID"),
    F.col("PAID_DATE").alias("PD_DT_SK"),
    F.lit(CurrentDate).alias("PRCS_DT_SK"),
    F.col("AMT_BILL").alias("ACTL_PD_AMT"),
    F.col("DT_FILLED").alias("DT_FILLED"),
    F.col("ACCT_ID").alias("ACCT_ID")
)

dfMbr = df_Trns2.select(
    F.col("DT_FILLED").alias("FILL_DT_SK"),
    F.col("MEM_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

dfEmailUnmatched = df_Trns2.select(
    F.col("CLAIM_ID").alias("CLAIM_ID")
)

# OPTUMRX_Invoice_Unmatched (CSeqFileStage) - Write
# Apply rpad to char columns: GRP_ID(char(8)), PROD_ID(char(18)), PD_DT_SK(char(10)), PRCS_DT_SK(char(10)), DT_FILLED(char(10)), ACCT_ID(char(15))
dfInvcUnmatched_final = (
    dfInvcUnmatched
    .withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
    .withColumn("PROD_ID", F.rpad(F.col("PROD_ID"), 18, " "))
    .withColumn("PD_DT_SK", F.rpad(F.col("PD_DT_SK"), 10, " "))
    .withColumn("PRCS_DT_SK", F.rpad(F.col("PRCS_DT_SK"), 10, " "))
    .withColumn("DT_FILLED", F.rpad(F.col("DT_FILLED"), 10, " "))
    .withColumn("ACCT_ID", F.rpad(F.col("ACCT_ID"), 15, " "))
    .select(
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
        "DT_FILLED",
        "ACCT_ID"
    )
)

write_files(
    dfInvcUnmatched_final,
    f"{adls_path}/key/OPTUMRX_Invoice_Unmatched.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# OPTUMRX_IDS_Unmatched (CSeqFileStage) - Write
# Only column is CLAIM_ID, which was varchar with no specified length, so no rpad needed.
dfEmailUnmatched_final = (
    dfEmailUnmatched
    .select("CLAIM_ID")
)

write_files(
    dfEmailUnmatched_final,
    f"{adls_path_publish}/external/processed/OPTUMRX_IDS_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# hf_optum_invoice_mbr_filldt (CHashedFileStage) - Scenario A
df_hf_optum_invoice_mbr_filldt = dedup_sort(
    dfMbr,
    ["FILL_DT_SK", "MBR_UNIQ_KEY"],
    []
)

# Trns3 (CTransformerStage)
df_Trns3 = df_hf_optum_invoice_mbr_filldt.withColumn(
    "CLM_ID",
    F.monotonically_increasing_id() + F.lit(1)
).select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FILL_DT_SK").alias("FILL_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

# W_DRUG_ENR (CSeqFileStage) - Write
# One of the columns is FILL_DT_SK (char(10)), apply rpad
df_W_DRUG_ENR_final = (
    df_Trns3
    .withColumn("FILL_DT_SK", F.rpad(F.col("FILL_DT_SK"), 10, " "))
    .select("CLM_ID", "FILL_DT_SK", "MBR_UNIQ_KEY")
)

write_files(
    df_W_DRUG_ENR_final,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)