# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: OptumMedDDrugInvoiceCntl
# MAGIC 
# MAGIC PROCESSING:   Set primary key for OPTUMRX exception table.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                    Date                        Change Description                                      Project #                              Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------                          ----------------------------        -----------------------------------------------------------------        ----------------                            ------------------------------------       ----------------------------           -------------
# MAGIC Velmani Kondappan    2020-10-20                  Initial Development                        6264 - PBM PHASE II - Government Programs           IntegrateDev2	Abhiram Dasarathy	2020-12-11

# MAGIC Primary Key OPTUMRX Invoice Exception Records
# MAGIC Input file created in OPTUMRXIdsUpd
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd","OPTUMRX")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","")
CurrentDate = get_widget_value("CurrentDate","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

df_OPTUMRX_Invoice_Unmatched_schema = StructType([
    StructField("ESI_INVC_EXCPT_SK", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("FNCL_LOB_CD", StringType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("PROD_ID", StringType(), nullable=False),
    StructField("PD_DT_SK", StringType(), nullable=False),
    StructField("PRCS_DT_SK", StringType(), nullable=False),
    StructField("ACTL_PD_AMT", DecimalType(38, 10), nullable=False),
    StructField("DT_FILLED", StringType(), nullable=False),
    StructField("ACCT_ID", StringType(), nullable=False)
])

df_OPTUMRX_Invoice_Unmatched = (
    spark.read
    .option("quote", '"')
    .option("header", False)
    .option("delimiter", ",")
    .schema(df_OPTUMRX_Invoice_Unmatched_schema)
    .csv(f"{adls_path}/key/OPTUMRX_Invoice_Unmatched.dat.{RunID}")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_ids = """SELECT MBR.MBR_UNIQ_KEY,
       DRUG.FILL_DT_SK,
       LOB.FNCL_LOB_CD,
       CMPNT.PROD_ID,
       GRP.GRP_ID
FROM   #$IDSOwner#.W_DRUG_ENR DRUG,
       #$IDSOwner#.CD_MPPNG MAP1,
       #$IDSOwner#.FNCL_LOB LOB,
       #$IDSOwner#.MBR_ENR MBR,
       #$IDSOwner#.SUBGRP SUBGRP,
       #$IDSOwner#.GRP GRP,
       #$IDSOwner#.PROD_CMPNT CMPNT,
       #$IDSOwner#.PROD_BILL_CMPNT BILL_CMPNT
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
AND SUBGRP.GRP_SK=GRP.GRP_SK""".replace("#$IDSOwner#", f"{IDSOwner}")

df_ids = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ids)
    .load()
)

df_hf_optumrx_invoice_fncl_lob = df_ids.withColumnRenamed("FILL_DT_SK", "DT_FILLED")
df_hf_optumrx_invoice_fncl_lob = dedup_sort(
    df_hf_optumrx_invoice_fncl_lob,
    ["MBR_UNIQ_KEY", "DT_FILLED", "GRP_ID"],
    []
)

extract_query_OPTUMRX_INVC_ECPT = "SELECT SRC_SYS_CD_SK,CLM_ID,ESI_INVC_EXCPT_SK,CRT_RUN_CYC_EXCTN_SK FROM #$IDSOwner#.ESI_INVC_EXCPT".replace("#$IDSOwner#", f"{IDSOwner}")
df_OPTUMRX_INVC_ECPT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_OPTUMRX_INVC_ECPT)
    .load()
)

df_hf_optumrx_invc_excpt_sk = dedup_sort(
    df_OPTUMRX_INVC_ECPT,
    ["SRC_SYS_CD_SK", "CLM_ID"],
    []
)

df_trans2 = (
    df_OPTUMRX_Invoice_Unmatched.alias("Input")
    .join(
        df_hf_optumrx_invoice_fncl_lob.alias("Fncl_LOB"),
        (F.col("Input.MBR_UNIQ_KEY") == F.col("Fncl_LOB.MBR_UNIQ_KEY"))
        & (F.col("Input.DT_FILLED") == F.col("Fncl_LOB.DT_FILLED"))
        & (F.col("Input.ACCT_ID") == F.col("Fncl_LOB.GRP_ID")),
        how="left"
    )
    .join(
        df_hf_optumrx_invc_excpt_sk.alias("Excpt_Sk"),
        (F.col("Input.SRC_SYS_CD_SK") == F.col("Excpt_Sk.SRC_SYS_CD_SK"))
        & (F.col("Input.CLM_ID") == F.col("Excpt_Sk.CLM_ID")),
        how="left"
    )
    .select(
        F.when(F.col("Excpt_Sk.ESI_INVC_EXCPT_SK").isNull(), F.lit(None)).alias("ESI_INVC_EXCPT_SK"),
        F.col("Input.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Input.CLM_ID").alias("CLM_ID"),
        F.when(F.col("Excpt_Sk.ESI_INVC_EXCPT_SK").isNull(), F.lit(RunCycle)).otherwise(F.col("Excpt_Sk.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Input.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(F.col("Fncl_LOB.FNCL_LOB_CD").isNull(), F.lit("UNK")).otherwise(F.col("Fncl_LOB.FNCL_LOB_CD")).alias("FNCL_LOB_CD"),
        F.col("Input.ACCT_ID").alias("GRP_ID"),
        F.col("Input.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.when(F.col("Fncl_LOB.PROD_ID").isNull(), F.col("Input.PROD_ID")).otherwise(F.col("Fncl_LOB.PROD_ID")).alias("PROD_ID"),
        F.col("Input.PD_DT_SK").alias("PD_DT_SK"),
        F.col("Input.PRCS_DT_SK").alias("PRCS_DT_SK"),
        F.col("Input.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
        F.col("Input.DT_FILLED").alias("DT_FILLED")
    )
)

df_enriched = df_trans2
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"ESI_INVC_EXCPT_SK",<schema>,<secret_name>)

df_final = df_enriched.select(
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
)

df_final = df_final.withColumn("GRP_ID", F.rpad(F.col("GRP_ID"), 8, " "))
df_final = df_final.withColumn("PROD_ID", F.rpad(F.col("PROD_ID"), 18, " "))
df_final = df_final.withColumn("PD_DT_SK", F.rpad(F.col("PD_DT_SK"), 10, " "))
df_final = df_final.withColumn("PRCS_DT_SK", F.rpad(F.col("PRCS_DT_SK"), 10, " "))
df_final = df_final.withColumn("DT_FILLED", F.rpad(F.col("DT_FILLED"), 10, " "))

write_files(
    df_final,
    f"{adls_path}/key/OPTUMRX_Invoice_trans.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)