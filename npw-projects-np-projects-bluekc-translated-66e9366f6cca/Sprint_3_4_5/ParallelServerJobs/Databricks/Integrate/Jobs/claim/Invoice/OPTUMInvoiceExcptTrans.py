# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:   Set primary key for OPTUMRX exception table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                    Date                        Change Description                                      Project #                              Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------                          ----------------------------        -----------------------------------------------------------------        ----------------                            ------------------------------------       ----------------------------           -------------
# MAGIC Deepa Bajaj                           2019-10-28                 Originally developed                                        6131- PBM Replacement     IntegrateDev2                    Kalyan Neelam                2019-11-27
# MAGIC 
# MAGIC Arpitha V                                2021-03-26                Filter condition add in Source SQL  
# MAGIC                                                                                 to eliminate multiple rows of FNCL_LOB_SK 
# MAGIC                                                                                 and the random financial_lob-sk.                       US 362113                           IntegrateDev2                   Jeyaprasanna                 2021-03-26

# MAGIC Primary Key OPTUMRX Invoice Exception Records
# MAGIC Input file created in OPTUMRXIdsUpd
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
CurrentDate = get_widget_value('CurrentDate','')

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
    StructField("ACTL_PD_AMT", DecimalType(10, 2), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("ACCT_ID", StringType(), False)
])

df_OPTUMRX_Invoice_Unmatched = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_OPTUMRX_Invoice_Unmatched)
    .load(f"{adls_path}/key/OPTUMRX_Invoice_Unmatched.dat.{RunID}")
)

# Read from IDS (DB2Connector) for MBR_ENROLL (ids stage)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_ids = f"""
SELECT
  MBR.MBR_UNIQ_KEY,
  DRUG.FILL_DT_SK,
  LOB.FNCL_LOB_CD,
  CMPNT.PROD_ID,
  GRP.GRP_ID
FROM {IDSOwner}.W_DRUG_ENR DRUG,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CD_MPPNG CD_MPPNG1,
     {IDSOwner}.CD_MPPNG CD_MPPNG2,
     {IDSOwner}.FNCL_LOB LOB,
     {IDSOwner}.MBR_ENR MBR,
     {IDSOwner}.SUBGRP SUBGRP,
     {IDSOwner}.GRP GRP,
     {IDSOwner}.PROD_CMPNT CMPNT,
     {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
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
  AND SUBGRP.GRP_SK = GRP.GRP_SK
  AND MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD_MPPNG1.CD_MPPNG_SK
  AND CMPNT.PROD_CMPNT_TYP_CD_SK = CD_MPPNG2.CD_MPPNG_SK
  AND CD_MPPNG1.TRGT_CD = 'MED'
  AND CD_MPPNG2.TRGT_CD = 'PDBL'
"""

df_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ids)
    .load()
)

df_ids_renamed = df_ids.select(
    F.col("MBR_UNIQ_KEY"),
    F.col("FILL_DT_SK").alias("DT_FILLED"),
    F.col("FNCL_LOB_CD"),
    F.col("PROD_ID"),
    F.col("GRP_ID")
)

# hf_optumrx_invoice_fncl_lob (Scenario A dedup on Primary Keys MBR_UNIQ_KEY, DT_FILLED, GRP_ID)
df_hf_optumrx_invoice_fncl_lob = dedup_sort(
    df_ids_renamed,
    ["MBR_UNIQ_KEY", "DT_FILLED", "GRP_ID"],
    []
)

# Read from IDS (DB2Connector) for ESI_INVC_EXCPT (OPTUMRX_INVC_ECPT stage)
extract_query_optumrx_invc_ecpt = f"""
SELECT
  SRC_SYS_CD_SK,
  CLM_ID,
  ESI_INVC_EXCPT_SK,
  CRT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.ESI_INVC_EXCPT
"""

df_optumrx_invc_ecpt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_optumrx_invc_ecpt)
    .load()
)

# hf_optumrx_invc_excpt_sk (Scenario A dedup on Primary Keys SRC_SYS_CD_SK, CLM_ID)
df_hf_optumrx_invc_excpt_sk = dedup_sort(
    df_optumrx_invc_ecpt,
    ["SRC_SYS_CD_SK", "CLM_ID"],
    []
)

# Trns2 (CTransformerStage) - Main Input + Lookup Joins
df_trns2 = (
    df_OPTUMRX_Invoice_Unmatched.alias("Input")
    .join(
        df_hf_optumrx_invoice_fncl_lob.alias("Fncl_LOB"),
        (
            (F.col("Input.MBR_UNIQ_KEY") == F.col("Fncl_LOB.MBR_UNIQ_KEY")) &
            (F.col("Input.DT_FILLED") == F.col("Fncl_LOB.DT_FILLED")) &
            (F.col("Input.ACCT_ID") == F.col("Fncl_LOB.GRP_ID"))
        ),
        "left"
    )
    .join(
        df_hf_optumrx_invc_excpt_sk.alias("Excpt_Sk"),
        (
            (F.col("Input.SRC_SYS_CD_SK") == F.col("Excpt_Sk.SRC_SYS_CD_SK")) &
            (F.col("Input.CLM_ID") == F.col("Excpt_Sk.CLM_ID"))
        ),
        "left"
    )
    .select(
        F.when(F.col("Excpt_Sk.ESI_INVC_EXCPT_SK").isNull(), F.lit(None)).alias("ESI_INVC_EXCPT_SK"),
        F.col("Input.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Input.CLM_ID").alias("CLM_ID"),
        F.when(
            F.col("Excpt_Sk.ESI_INVC_EXCPT_SK").isNull(),
            F.lit(RunCycle)
        ).otherwise(
            F.col("Excpt_Sk.CRT_RUN_CYC_EXCTN_SK")
        ).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Input.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            F.col("Fncl_LOB.FNCL_LOB_CD").isNull(),
            F.lit("UNK")
        ).otherwise(
            F.col("Fncl_LOB.FNCL_LOB_CD")
        ).alias("FNCL_LOB_CD"),
        F.col("Input.ACCT_ID").alias("GRP_ID"),
        F.col("Input.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.when(
            F.col("Fncl_LOB.PROD_ID").isNull(),
            F.col("Input.PROD_ID")
        ).otherwise(
            F.col("Fncl_LOB.PROD_ID")
        ).alias("PROD_ID"),
        F.col("Input.PD_DT_SK").alias("PD_DT_SK"),
        F.col("Input.PRCS_DT_SK").alias("PRCS_DT_SK"),
        F.col("Input.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
        F.col("Input.DT_FILLED").alias("DT_FILLED")
    )
)

# SurrogateKeyGen for ESI_INVC_EXCPT_SK
df_enriched = SurrogateKeyGen(df_trns2,<DB sequence name>,"ESI_INVC_EXCPT_SK",<schema>,<secret_name>)

# Final select with column order and rpad for char columns before writing
df_final = df_enriched.select(
    F.col("ESI_INVC_EXCPT_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FNCL_LOB_CD"),
    rpad(F.col("GRP_ID"), 8, " ").alias("GRP_ID"),
    F.col("MBR_UNIQ_KEY"),
    rpad(F.col("PROD_ID"), 18, " ").alias("PROD_ID"),
    rpad(F.col("PD_DT_SK"), 10, " ").alias("PD_DT_SK"),
    rpad(F.col("PRCS_DT_SK"), 10, " ").alias("PRCS_DT_SK"),
    F.col("ACTL_PD_AMT"),
    rpad(F.col("DT_FILLED"), 10, " ").alias("DT_FILLED")
)

# Write to OPTUMRX_Invoice_trans (CSeqFileStage)
write_files(
    df_final,
    f"{adls_path}/key/OPTUMRX_Invoice_trans.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)