# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2009, 2016 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC Called by:
# MAGIC                     FctsAltFundLoadSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     *
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                  Project/                                                                                                                           Code                   Date
# MAGIC Developer           Date              Altiris #         Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -----------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Naren Garapaty  09/20/2007   3259            Initial program                                                                                              Steph Goddard   09/27/2007
# MAGIC Hugh Sisson       03/27/2009   118315        Use CLM K-table to look up CLM_SK, added IDS parameters, and            Steph Goddard   04/06/2009  
# MAGIC                                                                      passing in SrcSysCdSk from sequencer
# MAGIC 
# MAGIC Hugh Sisson       2016-06-07    TFS12538   Change SEQ_NO from SmallInt to Integer                                                   Jag Yelavarthi    2015-06-08

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Remove duplicate keys
# MAGIC Load working table then join to K_CLM table
# MAGIC Created in FctsAltFundInvcPaymtExtr
# MAGIC Assign all foreign keys and create default rows for UNK and NA.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
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
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import col, when, lit, coalesce, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

InFile = get_widget_value("InFile","IdsAltFundInvcPaymtPkey.AltFundInvcPaymtTmp.dat")
Logging = get_widget_value("Logging","N")
SrcSysCdSk = get_widget_value("SrcSysCdSk","0")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# ----------------------------------------------------------------------------
# Stage: IdsAltFundInvcPaymt (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_IdsAltFundInvcPaymt = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("ALT_FUND_INVC_PAYMT_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND_INVC_ID", StringType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("ALT_FUND_CNTR_PERD_NO", IntegerType(), nullable=False),
    StructField("CLS_PLN_ID", StringType(), nullable=False),
    StructField("PROD_ID", StringType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("SUB_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND_INVC", IntegerType(), nullable=False),
    StructField("CLM", IntegerType(), nullable=False),
    StructField("CLS_PLN", IntegerType(), nullable=False),
    StructField("GRP", IntegerType(), nullable=False),
    StructField("MBR", IntegerType(), nullable=False),
    StructField("PROD", IntegerType(), nullable=False),
    StructField("SUBGRP", IntegerType(), nullable=False),
    StructField("SUB", IntegerType(), nullable=False),
    StructField("FUND_FROM_DT", StringType(), nullable=False),
    StructField("FUND_THRU_DT", StringType(), nullable=False),
    StructField("PCA_AMT", DecimalType(38,10), nullable=False),
    StructField("SGSG_ID", StringType(), nullable=False),
    StructField("GRGR_ID", StringType(), nullable=False)
])

df_IdsAltFundInvcPaymt = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsAltFundInvcPaymt)
    .load(f"{adls_path}/key/{InFile}")
)

# ----------------------------------------------------------------------------
# Stage: WALT_FUND_INVC_PAYMT (CSeqFileStage)
# ----------------------------------------------------------------------------
schema_WALT_FUND_INVC_PAYMT = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False)
])

df_WALT_FUND_INVC_PAYMT = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_WALT_FUND_INVC_PAYMT)
    .load(f"{adls_path}/load/W_ALT_FUND_INVC_PAYMT.dat")
)

# ----------------------------------------------------------------------------
# Stage: hf_alt_fund_invc_paymt_keys (CHashedFileStage) - Scenario A
# Instead of writing/reading the hashed file, deduplicate on keys (SRC_SYS_CD_SK, CLM_ID)
# ----------------------------------------------------------------------------
df_hf_alt_fund_invc_paymt_keys_dedup = dedup_sort(
    df_WALT_FUND_INVC_PAYMT,
    ["SRC_SYS_CD_SK", "CLM_ID"],
    []
)

# ----------------------------------------------------------------------------
# Stage: W_ALT_FUND_INVC_PAYMT (DB2Connector) - Write / Merge into #$IDSOwner#.W_ALT_FUND_INVC_PAYMT
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Write df_hf_alt_fund_invc_paymt_keys_dedup to a staging table
execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsAltFundInvcPaymt_W_ALT_FUND_INVC_PAYMT_temp", jdbc_url, jdbc_props)

df_hf_alt_fund_invc_paymt_keys_dedup.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.IdsAltFundInvcPaymt_W_ALT_FUND_INVC_PAYMT_temp") \
    .mode("overwrite") \
    .save()

merge_sql_W_ALT_FUND_INVC_PAYMT = f"""
MERGE {IDSOwner}.W_ALT_FUND_INVC_PAYMT AS T
USING STAGING.IdsAltFundInvcPaymt_W_ALT_FUND_INVC_PAYMT_temp AS S
ON T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK
   AND T.CLM_ID = S.CLM_ID
WHEN MATCHED THEN UPDATE SET
   T.SRC_SYS_CD_SK = S.SRC_SYS_CD_SK,
   T.CLM_ID = S.CLM_ID
WHEN NOT MATCHED THEN INSERT
   (SRC_SYS_CD_SK, CLM_ID)
   VALUES (S.SRC_SYS_CD_SK, S.CLM_ID);
"""
execute_dml(merge_sql_W_ALT_FUND_INVC_PAYMT, jdbc_url, jdbc_props)

# The stage SQL also includes creation of W_PAYMT_SUM and dropping W_ALT_FUND_INVC_PAYMT
execute_dml(f"CREATE TABLE {IDSOwner}.W_PAYMT_SUM (SRC_SYS_CD_SK INT NOT NULL, PAYMT_REF_ID CHAR(16) NOT NULL, PAYMT_SUM_LOB_CD CHAR(4) NOT NULL)", jdbc_url, jdbc_props)
execute_dml(f"DROP TABLE {IDSOwner}.W_ALT_FUND_INVC_PAYMT", jdbc_url, jdbc_props)

# After SQL: runstats
execute_dml(f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.W_PAYMT_SUM on key columns with distribution on key columns and detailed indexes all allow write access')", jdbc_url, jdbc_props)

# Output pin query => "SELECT ... FROM #$IDSOwner#.W_ALT_FUND_INVC_PAYMT w, #$IDSOwner#.K_CLM k..."
df_W_ALT_FUND_INVC_PAYMT_Keys = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"""
SELECT 
  k.SRC_SYS_CD_SK,
  k.CLM_ID,
  k.CLM_SK
FROM 
  {IDSOwner}.W_ALT_FUND_INVC_PAYMT w,
  {IDSOwner}.K_CLM k
WHERE 
  w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
  AND w.CLM_ID = k.CLM_ID
""")
    .load()
)

# ----------------------------------------------------------------------------
# Stage: hf_iafip_clm (CHashedFileStage) - Scenario A deduplicate on (SRC_SYS_CD_SK, CLM_ID)
# ----------------------------------------------------------------------------
df_hf_iafip_clm_dedup = dedup_sort(
    df_W_ALT_FUND_INVC_PAYMT_Keys,
    ["SRC_SYS_CD_SK", "CLM_ID"],
    []
)

# ----------------------------------------------------------------------------
# Stage: PurgeTrn (CTransformerStage)
# Primary input => df_IdsAltFundInvcPaymt
# Lookup => df_hf_iafip_clm_dedup (left join on SRC_SYS_CD_SK & CLM_ID)
# ----------------------------------------------------------------------------
joined_df = df_IdsAltFundInvcPaymt.alias("Key").join(
    df_hf_iafip_clm_dedup.alias("ClmSkLkup"),
    (col("Key.SrcSysCdSk") == col("ClmSkLkup.SRC_SYS_CD_SK")) & (col("Key.CLM_ID") == col("ClmSkLkup.CLM_ID")),
    how="left"
)

df_with_vars = joined_df \
    .withColumn("svFundFromDtSk", GetFkeyDate('IDS', col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.FUND_FROM_DT"), Logging)) \
    .withColumn("svAltFundInvcSk", GetFkeyAltFundInvc(col("Key.SRC_SYS_CD"), col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.ALT_FUND_INVC"), Logging)) \
    .withColumn("svClsPlnSk", GetFkeyClsPln(col("Key.SRC_SYS_CD"), col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.CLS_PLN"), Logging)) \
    .withColumn("svGrpSk", GetFkeyGrp(col("Key.SRC_SYS_CD"), col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.GRGR_ID"), Logging)) \
    .withColumn("svMbrSk", GetFkeyMbr(col("Key.SRC_SYS_CD"), col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.MBR_UNIQ_KEY"), Logging)) \
    .withColumn("svProdSk", GetFkeyProd(col("Key.SRC_SYS_CD"), col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.PROD"), Logging)) \
    .withColumn("svSubGrpSk", GetFkeySubgrp(col("Key.SRC_SYS_CD"), col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.GRGR_ID"), col("Key.SGSG_ID"), Logging)) \
    .withColumn("svSubSk", GetFkeySub(col("Key.SRC_SYS_CD"), col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.SUB_UNIQ_KEY"), Logging)) \
    .withColumn("svFundThruDtSk", GetFkeyDate('IDS', col("Key.ALT_FUND_INVC_PAYMT_SK"), col("Key.FUND_THRU_DT"), Logging)) \
    .withColumn("PassThru", col("Key.PASS_THRU_IN")) \
    .withColumn("ErrCount", GetFkeyErrorCnt(col("Key.ALT_FUND_INVC_PAYMT_SK")))

df_enriched = df_with_vars \
    .withColumn("CLM_SK_joined", when(col("ClmSkLkup.CLM_SK").isNull(), lit(0)).otherwise(col("ClmSkLkup.CLM_SK")))

# ----------------------------------------------------------------------------
# Split outputs by constraints
# ----------------------------------------------------------------------------
df_fkey = df_enriched.filter((col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y")))
df_recycle = df_enriched.filter(col("ErrCount") > lit(0))

# DefaultUNK => single row
# Columns per the link definition
defaultUNK_data = [{
    "ALT_FUND_INVC_PAYMT_SK": 0,
    "SRC_SYS_CD_SK": 0,
    "ALT_FUND_INVC_ID": "UNK",
    "MBR_UNIQ_KEY": 0,
    "ALT_FUND_CNTR_PERD_NO": 0,
    "CLS_PLN_ID": "UNK",
    "PROD_ID": "UNK",
    "CLM_ID": 0,
    "SUB_UNIQ_KEY": 0,
    "SEQ_NO": 0,
    "CRT_RUN_CYC_EXCTN_SK": 0,
    "LAST_UPDT_RUN_CYC_EXCTN_SK": 0,
    "ALT_FUND_INVC_SK": 0,
    "CLM_SK_joined": 0,
    "CLS_PLN_SK": 0,
    "GRP_SK": 0,
    "MBR_SK": 0,
    "PROD_SK": 0,
    "SUBGRP_SK": 0,
    "SUB_SK": 0,
    "FUND_FROM_DT_SK": "UNK",
    "FUND_THRU_DT_SK": "UNK",
    "PCA_AMT": 0.00
}]
df_defaultUNK = spark.createDataFrame(defaultUNK_data)

# DefaultNA => single row
defaultNA_data = [{
    "ALT_FUND_INVC_PAYMT_SK": 1,
    "SRC_SYS_CD_SK": 1,
    "ALT_FUND_INVC_ID": "NA",
    "MBR_UNIQ_KEY": 1,
    "ALT_FUND_CNTR_PERD_NO": 1,
    "CLS_PLN_ID": "NA",
    "PROD_ID": "NA",
    "CLM_ID": 1,
    "SUB_UNIQ_KEY": 1,
    "SEQ_NO": 1,
    "CRT_RUN_CYC_EXCTN_SK": 1,
    "LAST_UPDT_RUN_CYC_EXCTN_SK": 1,
    "ALT_FUND_INVC_SK": 1,
    "CLM_SK_joined": 1,
    "CLS_PLN_SK": 1,
    "GRP_SK": 1,
    "MBR_SK": 1,
    "PROD_SK": 1,
    "SUBGRP_SK": 1,
    "SUB_SK": 1,
    "FUND_FROM_DT_SK": "NA",
    "FUND_THRU_DT_SK": "NA",
    "PCA_AMT": 0.00
}]
df_defaultNA = spark.createDataFrame(defaultNA_data)

# ----------------------------------------------------------------------------
# Stage: hf_recycle (CHashedFileStage) - Scenario C => write to parquet
# ----------------------------------------------------------------------------
# Columns for recycle link
df_recycle_output = df_recycle.select(
    GetRecycleKey(col("Key.ALT_FUND_INVC_PAYMT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("Key.INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(col("Key.DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(col("Key.PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("Key.RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("Key.ALT_FUND_INVC_PAYMT_SK").alias("ALT_FUND_INVC_PAYMT_SK"),
    col("Key.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    col("Key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Key.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    col("Key.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("Key.PROD_ID").alias("PROD_ID"),
    col("Key.CLM_ID").alias("CLM_ID"),
    col("Key.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("Key.SEQ_NO").alias("SEQ_NO"),
    col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Key.ALT_FUND_INVC").alias("ALT_FUND_INVC_SK"),
    col("Key.CLM").alias("CLM_SK"),
    col("Key.CLS_PLN").alias("CLS_PLN_SK"),
    col("Key.GRP").alias("GRP_SK"),
    col("Key.MBR").alias("MBR_SK"),
    col("Key.PROD").alias("PROD_SK"),
    col("Key.SUBGRP").alias("SUBGRP_SK"),
    col("Key.SUB").alias("SUB_SK"),
    rpad(col("Key.FUND_FROM_DT"),10," ").alias("FUND_FROM_DT_SK"),
    rpad(col("Key.FUND_THRU_DT"),10," ").alias("FUND_THRU_DT_SK"),
    col("Key.PCA_AMT").alias("PCA_AMT")
)

write_files(
    df_recycle_output,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: Collector (CCollector)
# Union the three inputs: df_fkey, df_defaultUNK, df_defaultNA
# ----------------------------------------------------------------------------
df_fkey_out = df_fkey.select(
    col("Key.ALT_FUND_INVC_PAYMT_SK").alias("ALT_FUND_INVC_PAYMT_SK"),
    col("Key.SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("Key.ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
    col("Key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("Key.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    col("Key.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("Key.PROD_ID").alias("PROD_ID"),
    col("Key.CLM_ID").alias("CLM_ID"),
    col("Key.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("Key.SEQ_NO").alias("SEQ_NO"),
    col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svAltFundInvcSk").alias("ALT_FUND_INVC_SK"),
    col("CLM_SK_joined").alias("CLM_SK"),
    col("svClsPlnSk").alias("CLS_PLN_SK"),
    col("svGrpSk").alias("GRP_SK"),
    col("svMbrSk").alias("MBR_SK"),
    col("svProdSk").alias("PROD_SK"),
    col("svSubGrpSk").alias("SUBGRP_SK"),
    col("svSubSk").alias("SUB_SK"),
    col("svFundFromDtSk").alias("FUND_FROM_DT_SK"),
    col("svFundThruDtSk").alias("FUND_THRU_DT_SK"),
    col("Key.PCA_AMT").alias("PCA_AMT")
)

df_defaultUNK_out = df_defaultUNK.select(
    col("ALT_FUND_INVC_PAYMT_SK"),
    col("SRC_SYS_CD_SK"),
    col("ALT_FUND_INVC_ID"),
    col("MBR_UNIQ_KEY"),
    col("ALT_FUND_CNTR_PERD_NO"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("CLM_ID"),
    col("SUB_UNIQ_KEY"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ALT_FUND_INVC_SK"),
    col("CLM_SK_joined").alias("CLM_SK"),
    col("CLS_PLN_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("SUB_SK"),
    col("FUND_FROM_DT_SK"),
    col("FUND_THRU_DT_SK"),
    col("PCA_AMT")
)

df_defaultNA_out = df_defaultNA.select(
    col("ALT_FUND_INVC_PAYMT_SK"),
    col("SRC_SYS_CD_SK"),
    col("ALT_FUND_INVC_ID"),
    col("MBR_UNIQ_KEY"),
    col("ALT_FUND_CNTR_PERD_NO"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("CLM_ID"),
    col("SUB_UNIQ_KEY"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ALT_FUND_INVC_SK"),
    col("CLM_SK_joined").alias("CLM_SK"),
    col("CLS_PLN_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("SUB_SK"),
    col("FUND_FROM_DT_SK"),
    col("FUND_THRU_DT_SK"),
    col("PCA_AMT")
)

df_collector = df_fkey_out.union(df_defaultUNK_out).union(df_defaultNA_out)

# ----------------------------------------------------------------------------
# Stage: ALT_FUND_INVC_PAYMT (CSeqFileStage) - Write final file
# ----------------------------------------------------------------------------
# According to final column order and rpad for char(10) columns
df_final = df_collector \
    .withColumn("FUND_FROM_DT_SK", rpad(col("FUND_FROM_DT_SK"), 10, " ")) \
    .withColumn("FUND_THRU_DT_SK", rpad(col("FUND_THRU_DT_SK"), 10, " "))

write_files(
    df_final.select(
        "ALT_FUND_INVC_PAYMT_SK",
        "SRC_SYS_CD_SK",
        "ALT_FUND_INVC_ID",
        "MBR_UNIQ_KEY",
        "ALT_FUND_CNTR_PERD_NO",
        "CLS_PLN_ID",
        "PROD_ID",
        "CLM_ID",
        "SUB_UNIQ_KEY",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_FUND_INVC_SK",
        "CLM_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "MBR_SK",
        "PROD_SK",
        "SUBGRP_SK",
        "SUB_SK",
        "FUND_FROM_DT_SK",
        "FUND_THRU_DT_SK",
        "PCA_AMT"
    ),
    f"{adls_path}/load/ALT_FUND_INVC_PAYMT.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# SurrogateKeyGen usage is not indicated here.
# No further statements.