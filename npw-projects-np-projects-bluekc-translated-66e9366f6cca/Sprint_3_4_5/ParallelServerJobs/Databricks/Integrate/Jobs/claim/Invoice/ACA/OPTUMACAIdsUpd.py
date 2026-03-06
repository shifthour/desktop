# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Update IDS CLM and DRUG_CLM with paid date from OPTUMRX ACA  file
# MAGIC Called by OPTUMACADrugInvoiceUpdateSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                Update IDS CLM with paid date
# MAGIC                Update IDS DRUG_CLM recon_dt_sk with current date
# MAGIC               If claim on input does not have matching claim in IDS, it is written to an 'unmatched' sequential file for error processing
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Developer                      Date                 Prjoect / TTR                                                                           Change Description                                                                                     Development Project                    Code Reviewer          Date Reviewed          
# MAGIC ------------------                 --------------------      -----------------------                                                                       --------------------------------------------------------------------------------------------                             --------------------------------                   -----------------------------     ----------------------------   
# MAGIC Velmani Kondappan    2020-10-20      6264 - PBM PHASE II - Government Programs                      Initial Development                                                                                             IntegrateDev2                           Reddy Sanam            2020-12-20

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
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import DataFrame
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

ids_secret_name = get_widget_value('ids_secret_name','')
SourceSys = get_widget_value('SourceSys','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
IDSOwner = get_widget_value('$IDSOwner','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Read the reference data for src_cd (no parameter placeholders)
extract_query_src_cd = (
    f"SELECT SRC_CD,CD_MPPNG_SK "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE SRC_DRVD_LKUP_VAL = 'OPTUMRX' AND SRC_SYS_CD = 'IDS' "
    f"AND SRC_CLCTN_CD = 'IDS' AND SRC_DOMAIN_NM = 'SOURCE SYSTEM' "
    f"AND TRGT_CLCTN_CD = 'IDS' AND TRGT_DOMAIN_NM = 'SOURCE SYSTEM'"
)

df_IDS_src_cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_src_cd)
    .load()
)

# Deduplicate for hashed file hf_optum_invoice_ids_src_cd (Scenario A)
df_src_cd_dedup = dedup_sort(
    df_IDS_src_cd,
    partition_cols=["SRC_CD"],
    sort_cols=[("SRC_CD", "A")]
)

# Read the entire CLM table for IdsClm lookup (removing the parameter-based WHERE)
extract_query_ids_clm = f"SELECT CLM_SK, SRC_SYS_CD_SK, CLM_ID FROM {IDSOwner}.CLM"
df_IdsClm_full = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ids_clm)
    .load()
)

# Read OPTUMRX_Invoice (CSeqFileStage)
schema_OPTUMRX_Invoice = T.StructType([
    T.StructField("CLAIM_ID", T.StringType(), nullable=False),
    T.StructField("INVC_DT", T.DecimalType(15,2), nullable=False),
    T.StructField("TOT_CST", T.DecimalType(15,2), nullable=False),
    T.StructField("CLM_ADM_FEE", T.DecimalType(15,2), nullable=False),
    T.StructField("BILL_CLM_CST", T.DecimalType(15,2), nullable=False),
    T.StructField("MBR_ID", T.StringType(), nullable=False),
    T.StructField("GRP_ID", T.StringType(), nullable=False),
    T.StructField("DT_FILLED", T.StringType(), nullable=False),
    T.StructField("CLM_STTUS", T.StringType(), nullable=False),
    T.StructField("GRP_NM", T.StringType(), nullable=True),
    T.StructField("ACCT_ID", T.StringType(), nullable=True)
])

df_OPTUMRX_Invoice = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_OPTUMRX_Invoice)
    .load(f"{adls_path}/verified/OPTUMRXDrugClmInvoicePaidUpdt.dat.{RunID}")
)

# TrnsGetSrcCd: primary link (df_OPTUMRX_Invoice), lookup link (df_src_cd_dedup) on constant 'OPTUMRX' = src_cd_lkup.SRC_CD
df_join_trnsGetSrcCd = (
    df_OPTUMRX_Invoice.alias("Input")
    .join(
        df_src_cd_dedup.alias("src_cd_lkup"),
        F.lit("OPTUMRX") == F.col("src_cd_lkup.SRC_CD"),
        how="left"
    )
)

df_PaidClm = df_join_trnsGetSrcCd.select(
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

# Trns1: Left join with df_IdsClm_full on (PaidClm.CLM_SK=IdsClm.CLM_SK, SRC_SYS_CD_SK=IdsClm.SRC_SYS_CD_SK, CLAIM_ID=IdsClm.CLM_ID).
# The job design references PaidClm.CLM_SK, though it does not exist. We still replicate the join conditions exactly as stated.
df_join_trns1 = (
    df_PaidClm.alias("PaidClm")
    .withColumn("CLM_SK", F.lit(None).cast(T.LongType()))  # Because the job references CLM_SK from PaidClm, but it's absent
    .join(
        df_IdsClm_full.alias("IdsClm"),
        (F.col("PaidClm.CLM_SK") == F.col("IdsClm.CLM_SK")) &
        (F.col("PaidClm.SRC_SYS_CD_SK") == F.col("IdsClm.SRC_SYS_CD_SK")) &
        (F.col("PaidClm.CLAIM_ID") == F.col("IdsClm.CLM_ID")),
        how="left"
    )
)

# Stage Variables: ClmSk, PdDtSk, ReconDtSk
df_trns1_vars = (
    df_join_trns1
    .withColumn(
        "ClmSk",
        F.when(F.isnull(F.col("IdsClm.CLM_ID")), F.lit(0)).otherwise(F.col("IdsClm.CLM_SK"))
    )
    .withColumn(
        "PdDtSk",
        GetFkeyDate('IDS', F.col("ClmSk"), F.col("PaidClm.PAID_DATE"), F.lit("X"))
    )
    .withColumn(
        "ReconDtSk",
        GetFkeyDate('IDS', F.col("ClmSk"), current_date(), F.lit("X"))
    )
)

# Output link "ClmUpdt" -> constraint "ClmSk <> 0"
df_ClmUpdt = (
    df_trns1_vars
    .filter(F.col("ClmSk") != 0)
    .select(
        F.col("ClmSk").alias("CLM_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PdDtSk").alias("PD_DT_SK")
    )
)

# Output link "DrugClmUpdt" -> constraint "ClmSk <> 0"
df_DrugClmUpdt = (
    df_trns1_vars
    .filter(F.col("ClmSk") != 0)
    .select(
        F.col("ClmSk").alias("CLM_SK"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ReconDtSk").alias("RECON_DT_SK"),
        F.col("PaidClm.ADMIN_FEE").alias("ADM_FEE_AMT")
    )
)

# Output link "Unmatched" -> constraint "IsNull(IdsClm.CLM_SK) = @TRUE"
df_Unmatched = (
    df_trns1_vars
    .filter(F.isnull(F.col("IdsClm.CLM_SK")))
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

# Output link "DrugClmPriceUpdt"
df_DrugClmPriceUpdt = (
    df_trns1_vars
    .select(
        F.col("PaidClm.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("PaidClm.CLAIM_ID").alias("CLM_ID"),
        F.lit(RunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PaidClm.BILL_CLM_CST").alias("INVC_TOT_DUE_AMT")
    )
)

# Write/merge for DB2_Connector_156 (Database=IDS). We have three input links. 
# 1) ClmUpdt -> assume merges into {IDSOwner}.CLM
spark.sql(f"DROP TABLE IF EXISTS STAGING.OPTUMACAIdsUpd_DB2_Connector_156_ClmUpdt_temp")
(
    df_ClmUpdt.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMACAIdsUpd_DB2_Connector_156_ClmUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_ClmUpdt = f"""
MERGE INTO {IDSOwner}.CLM AS T
USING STAGING.OPTUMACAIdsUpd_DB2_Connector_156_ClmUpdt_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.PD_DT_SK = S.PD_DT_SK
WHEN NOT MATCHED THEN
  INSERT (
    CLM_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    PD_DT_SK
  )
  VALUES (
    S.CLM_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.PD_DT_SK
  );
"""
execute_dml(merge_sql_ClmUpdt, jdbc_url, jdbc_props)

# 2) DrugClmUpdt -> assume merges into {IDSOwner}.CLM_DRUG
spark.sql(f"DROP TABLE IF EXISTS STAGING.OPTUMACAIdsUpd_DB2_Connector_156_DrugClmUpdt_temp")
(
    df_DrugClmUpdt.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMACAIdsUpd_DB2_Connector_156_DrugClmUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_DrugClmUpdt = f"""
MERGE INTO {IDSOwner}.CLM_DRUG AS T
USING STAGING.OPTUMACAIdsUpd_DB2_Connector_156_DrugClmUpdt_temp AS S
ON T.CLM_SK = S.CLM_SK
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.RECON_DT_SK = S.RECON_DT_SK,
    T.ADM_FEE_AMT = S.ADM_FEE_AMT
WHEN NOT MATCHED THEN
  INSERT (
    CLM_SK,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    RECON_DT_SK,
    ADM_FEE_AMT
  )
  VALUES (
    S.CLM_SK,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.RECON_DT_SK,
    S.ADM_FEE_AMT
  );
"""
execute_dml(merge_sql_DrugClmUpdt, jdbc_url, jdbc_props)

# 3) DrugClmPriceUpdt -> assume merges into {IDSOwner}.CLM_PRICE
spark.sql(f"DROP TABLE IF EXISTS STAGING.OPTUMACAIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp")
(
    df_DrugClmPriceUpdt.write
    .format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", "STAGING.OPTUMACAIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp")
    .mode("overwrite")
    .save()
)
merge_sql_DrugClmPriceUpdt = f"""
MERGE INTO {IDSOwner}.CLM_PRICE AS T
USING STAGING.OPTUMACAIdsUpd_DB2_Connector_156_DrugClmPriceUpdt_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN
  UPDATE SET
    T.LAST_UPDT_RUN_CYC_EXCTN_SK = S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    T.INVC_TOT_DUE_AMT = S.INVC_TOT_DUE_AMT
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD_SK,
    CLM_ID,
    LAST_UPDT_RUN_CYC_EXCTN_SK,
    INVC_TOT_DUE_AMT
  )
  VALUES (
    S.SRC_SYS_CD_SK,
    S.CLM_ID,
    S.LAST_UPDT_RUN_CYC_EXCTN_SK,
    S.INVC_TOT_DUE_AMT
  );
"""
execute_dml(merge_sql_DrugClmPriceUpdt, jdbc_url, jdbc_props)

# Trns2: "Unmatched" => output pins => "InvcUnmatched", "Mbr", "EmailUnmatched"

# InvcUnmatched => OPTUMRX_Invoice_Unmatched
df_InvcUnmatched = df_Unmatched.select(
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

# Mbr => hf_optum_invoice_mbr_filldt (Scenario A => dedup on FILL_DT_SK, MBR_UNIQ_KEY)
df_Mbr = df_Unmatched.select(
    F.col("DT_FILLED").alias("FILL_DT_SK"),
    F.col("MEM_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)
df_Mbr_dedup = dedup_sort(
    df_Mbr,
    partition_cols=["FILL_DT_SK","MBR_UNIQ_KEY"],
    sort_cols=[("FILL_DT_SK","A"), ("MBR_UNIQ_KEY","A")]
)

# EmailUnmatched => OPTUMRX_IDS_Unmatched
df_EmailUnmatched = df_Unmatched.select(
    F.col("CLAIM_ID")
)
df_EmailUnmatched_out = df_EmailUnmatched.select(
    # If CLAIM_ID is considered varchar with unknown length, we do not have a stated length to rpad. We leave as-is.
    F.col("CLAIM_ID")
)

write_files(
    df_EmailUnmatched_out,
    f"{adls_path_publish}/processed/OPTUMRX_IDS_Unmatched_{RunID}.txt",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Trns3 => reads from hf_optum_invoice_mbr_filldt => (df_Mbr_dedup). 
# Output => CLM_ID=@INROWNUM, FILL_DT_SK, MBR_UNIQ_KEY

window_rownum = Window.orderBy(F.lit(1))
df_Trns3 = (
    df_Mbr_dedup
    .withColumn("rownum_tmp", F.row_number().over(window_rownum))
    .select(
        F.col("rownum_tmp").alias("CLM_ID"),
        F.col("FILL_DT_SK"),
        F.col("MBR_UNIQ_KEY")
    )
)

# Output link => W_DRUG_ENR => CSeqFile => columns in order
# FILL_DT_SK is char(10)
df_W_DRUG_ENR = df_Trns3.select(
    F.col("CLM_ID"),
    F.rpad(F.col("FILL_DT_SK"), 10, " ").alias("FILL_DT_SK"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY")
)

write_files(
    df_W_DRUG_ENR,
    f"{adls_path}/load/W_DRUG_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)