# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Set primary key for OPTUMRX exception table.
# MAGIC Called from OPTUMACADrugInvoiceUpdateSeq
# MAGIC 
# MAGIC MODIFICATIONS: 
# MAGIC Developer                                    Date                        Change Description                                      Project #                                                            Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------                          ----------------------------        -----------------------------------------------------------------        ----------------                                                          ------------------------------------       ----------------------------           -------------
# MAGIC Ramu Avula                       2020-10-20                 Originally developed                                        6264 - PBM PHASE II - Government Programs       IntegrateDev2                  Reddy Sanam                 12-10-2020

# MAGIC Primary Key OPTUMRX Invoice Exception Record
# MAGIC Called from OPTUMACADrugInvoiceUpdateSeq
# MAGIC Input file created in OPTUMRXIdsUpd
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType
)
from pyspark.sql.functions import col, when, isnull, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Parameters
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read OPTUMRX_Invoice_Unmatched (CSeqFileStage)
schema_OPTUMRX_Invoice_Unmatched = StructType([
    StructField("ESI_INVC_EXCPT_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("FNCL_LOB_CD", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("PROD_ID", StringType(), False),
    StructField("PD_DT_SK", StringType(), False),
    StructField("PRCS_DT_SK", StringType(), False),
    StructField("ACTL_PD_AMT", DecimalType(38, 10), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("ACCT_ID", StringType(), False)
])

df_OPTUMRX_Invoice_Unmatched = (
    spark.read
    .option("quote", "\"")
    .option("delimiter", ",")
    .option("header", "false")
    .schema(schema_OPTUMRX_Invoice_Unmatched)
    .csv(f"{adls_path}/key/OPTUMRX_Invoice_Unmatched.dat.{RunID}")
)

# Read from IDS (DB2Connector) => df_ids
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = f"""
SELECT
  MBR.MBR_UNIQ_KEY,
  DRUG.FILL_DT_SK,
  LOB.FNCL_LOB_CD,
  CMPNT.PROD_ID,
  GRP.GRP_ID
FROM
  {IDSOwner}.W_DRUG_ENR DRUG,
  {IDSOwner}.CD_MPPNG MAP1,
  {IDSOwner}.FNCL_LOB LOB,
  {IDSOwner}.MBR_ENR MBR,
  {IDSOwner}.SUBGRP SUBGRP,
  {IDSOwner}.GRP GRP,
  {IDSOwner}.PROD_CMPNT CMPNT,
  {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT
WHERE
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND DRUG.FILL_DT_SK BETWEEN MBR.EFF_DT_SK AND MBR.TERM_DT_SK
  AND CMPNT.PROD_SK = MBR.PROD_SK
  AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
  AND MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD = 'MED'
  AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ( 'MED', 'MED1' )
  AND DRUG.FILL_DT_SK BETWEEN CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
  AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
  AND BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
  AND MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  AND SUBGRP.GRP_SK=GRP.GRP_SK
"""
df_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

# Scenario A for hf_optumrx_invoice_fncl_lob => deduplicate by [MBR_UNIQ_KEY, DT_FILLED, GRP_ID]
df_hf_optumrx_invoice_fncl_lob = df_ids.withColumnRenamed("FILL_DT_SK","DT_FILLED")
df_hf_optumrx_invoice_fncl_lob = dedup_sort(
    df_hf_optumrx_invoice_fncl_lob,
    ["MBR_UNIQ_KEY", "DT_FILLED", "GRP_ID"],
    [("MBR_UNIQ_KEY","A"),("DT_FILLED","A"),("GRP_ID","A")]
)

# Read from IDS (DB2Connector) => df_OPTUMRX_INVC_ECPT
jdbc_url_ids_2, jdbc_props_ids_2 = get_db_config(ids_secret_name)
extract_query_OPTUMRX_INVC_ECPT = f"""
SELECT
  SRC_SYS_CD_SK,
  CLM_ID,
  ESI_INVC_EXCPT_SK,
  CRT_RUN_CYC_EXCTN_SK
FROM
  {IDSOwner}.ESI_INVC_EXCPT
"""
df_OPTUMRX_INVC_ECPT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids_2)
    .options(**jdbc_props_ids_2)
    .option("query", extract_query_OPTUMRX_INVC_ECPT)
    .load()
)

# Scenario A for hf_optumrx_invc_excpt_sk => deduplicate by [SRC_SYS_CD_SK, CLM_ID]
df_hf_optumrx_invc_excpt_sk = dedup_sort(
    df_OPTUMRX_INVC_ECPT,
    ["SRC_SYS_CD_SK","CLM_ID"],
    [("SRC_SYS_CD_SK","A"),("CLM_ID","A")]
)

# Trns2 (CTransformerStage) - combine primary link and 2 lookups
df_Trans2_join = (
    df_OPTUMRX_Invoice_Unmatched.alias("Input")
    .join(
        df_hf_optumrx_invoice_fncl_lob.alias("Fncl_LOB"),
        [
            col("Input.MBR_UNIQ_KEY") == col("Fncl_LOB.MBR_UNIQ_KEY"),
            col("Input.DT_FILLED") == col("Fncl_LOB.DT_FILLED"),
            col("Input.ACCT_ID") == col("Fncl_LOB.GRP_ID")
        ],
        "left"
    )
    .join(
        df_hf_optumrx_invc_excpt_sk.alias("Excpt_Sk"),
        [
            col("Input.SRC_SYS_CD_SK") == col("Excpt_Sk.SRC_SYS_CD_SK"),
            col("Input.CLM_ID") == col("Excpt_Sk.CLM_ID")
        ],
        "left"
    )
)

df_enriched = (
    df_Trans2_join
    .withColumn(
        "TEMP_ESI_INVC_EXCPT_SK",
        when(isnull(col("Excpt_Sk.ESI_INVC_EXCPT_SK")), None)
        .otherwise(col("Excpt_Sk.ESI_INVC_EXCPT_SK"))
    )
    .withColumn(
        "TEMP_CRT_RUN_CYC_EXCTN_SK",
        when(isnull(col("Excpt_Sk.ESI_INVC_EXCPT_SK")), col("RunCycle"))
        .otherwise(col("Excpt_Sk.CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("ESI_INVC_EXCPT_SK", col("TEMP_ESI_INVC_EXCPT_SK"))  # placeholder until SurrogateKeyGen
    .withColumn("SRC_SYS_CD_SK", col("Input.SRC_SYS_CD_SK"))
    .withColumn("CLM_ID", col("Input.CLM_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", col("TEMP_CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", col("Input.LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn(
        "FNCL_LOB_CD",
        when(isnull(col("Fncl_LOB.FNCL_LOB_CD")), "UNK")
        .otherwise(col("Fncl_LOB.FNCL_LOB_CD"))
    )
    .withColumn("GRP_ID", col("Input.ACCT_ID"))
    .withColumn(
        "MBR_UNIQ_KEY",
        col("Input.MBR_UNIQ_KEY")
    )
    .withColumn(
        "PROD_ID",
        when(isnull(col("Fncl_LOB.PROD_ID")), col("Input.PROD_ID"))
        .otherwise(col("Fncl_LOB.PROD_ID"))
    )
    .withColumn("PD_DT_SK", col("Input.PD_DT_SK"))
    .withColumn("PRCS_DT_SK", col("Input.PRCS_DT_SK"))
    .withColumn("ACTL_PD_AMT", col("Input.ACTL_PD_AMT"))
    .withColumn("DT_FILLED", col("Input.DT_FILLED"))
)

# SurrogateKeyGen call for ESI_INVC_EXCPT_SK
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"ESI_INVC_EXCPT_SK",<schema>,<secret_name>)

# Select final columns in correct order and apply rpad for char/varchar
df_final = (
    df_enriched
    .select(
        col("ESI_INVC_EXCPT_SK"),
        col("SRC_SYS_CD_SK"),
        rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        rpad(col("FNCL_LOB_CD"), <...>, " ").alias("FNCL_LOB_CD"),
        rpad(col("GRP_ID"), 8, " ").alias("GRP_ID"),
        col("MBR_UNIQ_KEY"),
        rpad(col("PROD_ID"), 18, " ").alias("PROD_ID"),
        rpad(col("PD_DT_SK"), 10, " ").alias("PD_DT_SK"),
        rpad(col("PRCS_DT_SK"), 10, " ").alias("PRCS_DT_SK"),
        col("ACTL_PD_AMT"),
        rpad(col("DT_FILLED"), 10, " ").alias("DT_FILLED")
    )
)

# Write final file (CSeqFileStage)
write_files(
    df_final,
    f"{adls_path}/key/OPTUMRX_Invoice_trans.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)