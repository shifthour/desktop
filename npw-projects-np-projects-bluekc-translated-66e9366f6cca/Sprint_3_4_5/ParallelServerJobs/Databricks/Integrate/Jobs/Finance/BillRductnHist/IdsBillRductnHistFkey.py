# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 04/10/09 14:22:08 Batch  15076_51731 PROMOTE bckcetl ids20 dsadm bls for hs
# MAGIC ^1_2 04/10/09 14:18:47 Batch  15076_51530 INIT bckcett testIDS dsadm BLS FOR HS
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/07/07 14:46:06 Batch  14556_53195 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 11/07/07 14:33:10 Batch  14556_52396 INIT bckcett testIDScur dsadm bls for rt
# MAGIC ^1_1 10/17/07 13:27:15 Batch  14535_48445 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_1 10/17/07 13:17:55 Batch  14535_47880 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC Â© Copyright 2007, 2009 Blue Cross and Blue Shield of Kansas City
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
# MAGIC                                                  Project/                                                                                                                       Code                   Date
# MAGIC Developer           Date              Altiris #     Change Description                                                                                     Reviewer            Reviewed
# MAGIC -------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------
# MAGIC Naren Garapaty  09/20/2007   3259        Initial program                                                                                              Steph Goddard   09/27/2007   
# MAGIC Hugh Sisson       04/01/2009   188315    Use CLM K-table to look up CLM_SK, added IDS parameters, and            Steph Goddard   04/06/2009
# MAGIC                                                                   passing in SrcSysCdSk from sequencer                                                     
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24   358186    Changed Datatype length for field ALT_FUND_INVC                              Reddy Sanam     04/01/2021
# MAGIC                                                                   char(12) to Varchar(15)

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC Created in FctsBillRductnHistExtr
# MAGIC Load working table then join to K_CLM table
# MAGIC Remove duplicate keys
# MAGIC Assign all foreign keys and create default rows for UNK and NA.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# --------------------------------------------------------------------------------
# Retrieve job parameters
# --------------------------------------------------------------------------------
InFile = get_widget_value("InFile", "IdsBillRductnHistPkey.BillRductnHistTmp.dat")
Logging = get_widget_value("Logging", "N")
TmpOutFile = get_widget_value("TmpOutFile", "BILL_RDUCTN_HIST.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# --------------------------------------------------------------------------------
# Stages
# --------------------------------------------------------------------------------

# 1) Stage: IdsBillRductnHist (CSeqFileStage) => Read the file from adls_path/key
schema_IdsBillRductnHist = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("BILL_RDUCTN_HIST_SK", IntegerType(), nullable=False),
    StructField("MBR_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("BILL_ENTY_UNIQ_KEY", IntegerType(), nullable=False),
    StructField("ALT_FUND_CNTR_PERD_NO", IntegerType(), nullable=False),
    StructField("FUND_FROM_DT", StringType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND_INVC", StringType(), nullable=False),
    StructField("BILL_ENTY", IntegerType(), nullable=False),
    StructField("CLS_PLN", IntegerType(), nullable=False),
    StructField("GRP", IntegerType(), nullable=False),
    StructField("MBR", IntegerType(), nullable=False),
    StructField("CLM", IntegerType(), nullable=False),
    StructField("PROD", IntegerType(), nullable=False),
    StructField("SUBGRP", IntegerType(), nullable=False),
    StructField("FUND_THRU_DT", StringType(), nullable=False),
    StructField("PCA_AMT", DecimalType(38,10), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("SGSG_ID", StringType(), nullable=False),
    StructField("GRGR_ID", StringType(), nullable=False)
])

df_IdsBillRductnHist = (
    spark.read
    .option("header", "false")
    .option("quote", '"')
    .option("sep", ",")
    .schema(schema_IdsBillRductnHist)
    .csv(f"{adls_path}/key/{InFile}")
)

# 2) Stage: WBILL_RDUCTN_HIST (CSeqFileStage) => Read from adls_path/load
schema_WBILL_RDUCTN_HIST = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False)
])
df_WBILL_RDUCTN_HIST = (
    spark.read
    .option("header", "false")
    .option("quote", '"')
    .option("sep", ",")
    .schema(schema_WBILL_RDUCTN_HIST)
    .csv(f"{adls_path}/load/W_BILL_RDUCTN_HIST.dat")
)

# 3) Stage: hf_bill_rductn_hist_keys (CHashedFileStage) => Scenario A (intermediate). 
# Deduplicate on key columns [SRC_SYS_CD_SK, CLM_ID].
df_WBILL_RDUCTN_HIST_dedup = dedup_sort(
    df_WBILL_RDUCTN_HIST,
    partition_cols=["SRC_SYS_CD_SK", "CLM_ID"],
    sort_cols=[]
)

# 4) Stage: W_BILL_RDUCTN_HIST (DB2Connector => Database=IDS)
# TableAction=Truncate, plus the SQL statements:
#   INSERT INTO #$IDSOwner#.W_BILL_RDUCTN_HIST (SRC_SYS_CD_SK,CLM_ID) ...
#   CREATE TABLE #$IDSOwner#.W_PAYMT_SUM (...)
#   DROP TABLE #$IDSOwner#.W_BILL_RDUCTN_HIST
#   After_Sql => CALL SYSPROC.ADMIN_CMD('runstats ...')
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Truncate
execute_dml(f"TRUNCATE TABLE {IDSOwner}.W_BILL_RDUCTN_HIST", jdbc_url, jdbc_props)

# Insert data (in DataStage terms); here we use Spark append
df_WBILL_RDUCTN_HIST_dedup.select("SRC_SYS_CD_SK", "CLM_ID").write.jdbc(
    url=jdbc_url,
    table=f"{IDSOwner}.W_BILL_RDUCTN_HIST",
    mode="append",
    properties=jdbc_props
)

# CREATE TABLE #$IDSOwner#.W_PAYMT_SUM
execute_dml(
    f"""
    CREATE TABLE {IDSOwner}.W_PAYMT_SUM (
       SRC_SYS_CD_SK INTEGER NOT NULL,
       PAYMT_REF_ID CHAR(16) NOT NULL,
       PAYMT_SUM_LOB_CD CHAR(4) NOT NULL
    )
    """,
    jdbc_url,
    jdbc_props
)

# DROP TABLE #$IDSOwner#.W_BILL_RDUCTN_HIST
execute_dml(
    f"DROP TABLE {IDSOwner}.W_BILL_RDUCTN_HIST",
    jdbc_url,
    jdbc_props
)

# AfterSql => RUNSTATS
execute_dml(
    f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.W_PAYMT_SUM on key columns with distribution on key columns and detailed indexes all allow write access')",
    jdbc_url,
    jdbc_props
)

# 4) OutputPin => "Keys" => we do a DB read with the query
extract_query_ibrh = f"""
SELECT 
      k.SRC_SYS_CD_SK,
      k.CLM_ID,
      k.CLM_SK
FROM 
      {IDSOwner}.W_BILL_RDUCTN_HIST w,
      {IDSOwner}.K_CLM k
WHERE 
      w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
  AND w.CLM_ID = k.CLM_ID
"""

df_ibrh_clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_ibrh)
    .load()
)

# 5) Stage: hf_ibrh_clm (CHashedFileStage) => Scenario A => Deduplicate on [SRC_SYS_CD_SK, CLM_ID].
df_ibrh_clm_dedup = dedup_sort(
    df_ibrh_clm,
    partition_cols=["SRC_SYS_CD_SK", "CLM_ID"],
    sort_cols=[]
)

# 6) Stage: PurgeTrn (CTransformerStage)
# Primary link: df_IdsBillRductnHist
# Lookup link: df_ibrh_clm_dedup left join on:
#   param(SrcSysCdSk) == df_ibrh_clm_dedup["SRC_SYS_CD_SK"] AND
#   df_IdsBillRductnHist["CLM_ID"] == df_ibrh_clm_dedup["CLM_ID"]
# Then define stage variables, then constraints for multiple output links.

df_purge_joined = df_IdsBillRductnHist.alias("Key").join(
    df_ibrh_clm_dedup.alias("ClmSkLkup").filter(F.col("ClmSkLkup.SRC_SYS_CD_SK") == F.lit(SrcSysCdSk)),
    on=[
        F.col("Key.CLM_ID") == F.col("ClmSkLkup.CLM_ID")
    ],
    how="left"
)

# Add stage variables:
df_purge_vars = (
    df_purge_joined
    .withColumn("svFundFromDtSk", F.lit(None))  # GetFkeyDate(...) => user-defined
    .withColumn("svBillEntySk", F.lit(None))    # GetFkeyBillEnty(...) => user-defined
    .withColumn("svClsPlnSk", F.lit(None))      # ...
    .withColumn("svGrpSk", F.lit(None))
    .withColumn("svMbrSk", F.lit(None))
    .withColumn("svProdSk", F.lit(None))
    .withColumn("svSubGrpSk", F.lit(None))
    .withColumn("svFundThruDtSk", F.lit(None))
    .withColumn("svAltFundInvcSk", F.lit(None))
    .withColumn("PassThru", F.col("Key.PASS_THRU_IN"))
    .withColumn("ErrCount", F.lit(None))  # GetFkeyErrorCnt(...) => user-defined
)

# Because these user-defined calls are “already defined,” we just call them directly if needed:
df_purge_vars = df_purge_vars.withColumn(
    "svFundFromDtSk",
    GetFkeyDate(F.lit("IDS"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.FUND_FROM_DT"), F.lit(Logging))
).withColumn(
    "svBillEntySk",
    GetFkeyBillEnty(F.col("Key.SRC_SYS_CD"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.BILL_ENTY_UNIQ_KEY"), F.lit(Logging))
).withColumn(
    "svClsPlnSk",
    GetFkeyClsPln(F.col("Key.SRC_SYS_CD"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.CLS_PLN"), F.lit(Logging))
).withColumn(
    "svGrpSk",
    GetFkeyGrp(F.col("Key.SRC_SYS_CD"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.GRGR_ID"), F.lit(Logging))
).withColumn(
    "svMbrSk",
    GetFkeyMbr(F.col("Key.SRC_SYS_CD"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.MBR_UNIQ_KEY"), F.lit(Logging))
).withColumn(
    "svProdSk",
    GetFkeyProd(F.col("Key.SRC_SYS_CD"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.PROD"), F.lit(Logging))
).withColumn(
    "svSubGrpSk",
    GetFkeySubgrp(F.col("Key.SRC_SYS_CD"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.GRGR_ID"), F.col("Key.SGSG_ID"), F.lit(Logging))
).withColumn(
    "svFundThruDtSk",
    GetFkeyDate(F.lit("IDS"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.FUND_THRU_DT"), F.lit(Logging))
).withColumn(
    "svAltFundInvcSk",
    GetFkeyAltFundInvc(F.col("Key.SRC_SYS_CD"), F.col("Key.BILL_RDUCTN_HIST_SK"), F.col("Key.ALT_FUND_INVC"), F.lit(Logging))
).withColumn(
    "ErrCount",
    GetFkeyErrorCnt(F.col("Key.BILL_RDUCTN_HIST_SK"))
)

# Handle the "IF IsNull(ClmSkLkup.CLM_SK) THEN 0 ELSE ClmSkLkup.CLM_SK"
df_enriched = df_purge_vars.withColumn(
    "CLM_SK_resolved",
    F.when(F.col("ClmSkLkup.CLM_SK").isNull(), F.lit(0)).otherwise(F.col("ClmSkLkup.CLM_SK"))
)

# Output links:

# (A) Fkey => "ErrCount = 0 Or PassThru = 'Y'"
df_Fkey = df_enriched.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).select(
    F.col("Key.BILL_RDUCTN_HIST_SK").alias("BILL_RDUCTN_HIST_SK"),
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Key.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("Key.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("svFundFromDtSk").alias("FUND_FROM_DT_SK"),
    F.col("Key.SEQ_NO").alias("SEQ_NO"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svAltFundInvcSk").alias("ALT_FUND_INVC_SK"),
    F.col("svBillEntySk").alias("BILL_ENTY_SK"),
    F.col("svClsPlnSk").alias("CLS_PLN_SK"),
    F.col("svGrpSk").alias("GRP_SK"),
    F.col("svMbrSk").alias("MBR_SK"),
    F.col("CLM_SK_resolved").alias("CLM_SK"),
    F.col("svProdSk").alias("PROD_SK"),
    F.col("svSubGrpSk").alias("SUBGRP_SK"),
    F.col("svFundThruDtSk").alias("FUND_THRU_DT_SK"),
    F.col("Key.PCA_AMT").alias("PCA_AMT"),
    F.col("Key.CLM_ID").alias("CLM_ID")
)

# (B) lnkRecycle => "ErrCount > 0"
df_lnkRecycle = df_enriched.filter(
    F.col("ErrCount") > 0
).select(
    GetRecycleKey(F.col("Key.BILL_RDUCTN_HIST_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ErrCount").alias("ERR_CT"),
    (F.col("Key.RECYCLE_CT") + 1).alias("RECYCLE_CT"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Key.BILL_RDUCTN_HIST_SK").alias("BILL_RDUCTN_HIST_SK"),
    F.col("Key.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Key.BILL_ENTY_UNIQ_KEY").alias("BILL_ENTY_UNIQ_KEY"),
    F.col("Key.ALT_FUND_CNTR_PERD_NO").alias("ALT_FUND_CNTR_PERD_NO"),
    F.col("Key.FUND_FROM_DT").alias("FUND_FROM_DT_SK"),
    F.col("Key.SEQ_NO").alias("SEQ_NO"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Key.ALT_FUND_INVC").alias("ALT_FUND_INVC_SK"),
    F.col("Key.BILL_ENTY").alias("BILL_ENTY_SK"),
    F.col("Key.CLS_PLN").alias("CLS_PLN_SK"),
    F.col("Key.GRP").alias("GRP_SK"),
    F.col("Key.MBR").alias("MBR_SK"),
    F.col("Key.CLM").alias("CLM_SK"),
    F.col("Key.PROD").alias("PROD_SK"),
    F.col("Key.SUBGRP").alias("SUBGRP_SK"),
    F.col("Key.FUND_THRU_DT").alias("FUND_THRU_DT_SK"),
    F.col("Key.PCA_AMT").alias("PCA_AMT"),
    F.col("Key.CLM_ID").alias("CLM_ID")
)

# (C) DefaultUNK => "@INROWNUM = 1" => produce one record
df_defaultOne = df_enriched.limit(1)
df_DefaultUNK = df_defaultOne.select(
    F.lit(0).alias("BILL_RDUCTN_HIST_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(0).alias("MBR_UNIQ_KEY"),
    F.lit(0).alias("BILL_ENTY_UNIQ_KEY"),
    F.lit(0).alias("ALT_FUND_CNTR_PERD_NO"),
    F.lit("UNK").alias("FUND_FROM_DT_SK"),
    F.lit(0).alias("SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("ALT_FUND_INVC_SK"),
    F.lit(0).alias("BILL_ENTY_SK"),
    F.lit(0).alias("CLS_PLN_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit(0).alias("PROD_SK"),
    F.lit(0).alias("SUBGRP_SK"),
    F.lit("UNK").alias("FUND_THRU_DT_SK"),
    F.lit(0.00).alias("PCA_AMT"),
    F.lit("UNK").alias("CLM_ID")
)

# (D) DefaultNA => "@INROWNUM = 1" => produce one record
df_DefaultNA = df_defaultOne.select(
    F.lit(1).alias("BILL_RDUCTN_HIST_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(1).alias("MBR_UNIQ_KEY"),
    F.lit(1).alias("BILL_ENTY_UNIQ_KEY"),
    F.lit(1).alias("ALT_FUND_CNTR_PERD_NO"),
    F.lit("NA").alias("FUND_FROM_DT_SK"),
    F.lit(1).alias("SEQ_NO"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("ALT_FUND_INVC_SK"),
    F.lit(1).alias("BILL_ENTY_SK"),
    F.lit(1).alias("CLS_PLN_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit(1).alias("PROD_SK"),
    F.lit(1).alias("SUBGRP_SK"),
    F.lit("NA").alias("FUND_THRU_DT_SK"),
    F.lit(0.00).alias("PCA_AMT"),
    F.lit("NA").alias("CLM_ID")
)

# 7) Stage: hf_recycle (CHashedFileStage) => Scenario C => write to parquet
# Before writing, apply rpad where needed
# INSRT_UPDT_CD (char(10)), DISCARD_IN (char(1)), PASS_THRU_IN (char(1)), FUND_FROM_DT_SK (char(10)), FUND_THRU_DT_SK (char(10)) – from the selected columns
df_lnkRecycle_padded = (
    df_lnkRecycle
    .withColumn(
       "INSRT_UPDT_CD",
       F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")
    )
    .withColumn(
       "DISCARD_IN",
       F.rpad(F.col("DISCARD_IN"), 1, " ")
    )
    .withColumn(
       "PASS_THRU_IN",
       F.rpad(F.col("PASS_THRU_IN"), 1, " ")
    )
    .withColumn(
       "FUND_FROM_DT_SK",
       F.rpad(F.col("FUND_FROM_DT_SK"), 10, " ")
    )
    .withColumn(
       "FUND_THRU_DT_SK",
       F.rpad(F.col("FUND_THRU_DT_SK"), 10, " ")
    )
)

write_files(
    df_lnkRecycle_padded,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    '"',
    None
)

# 8) Stage: Collector (CCollector) => union of df_Fkey, df_DefaultUNK, df_DefaultNA
df_Fkey_padded = (
    df_Fkey
    .withColumn(
       "FUND_FROM_DT_SK",
       F.rpad(F.col("FUND_FROM_DT_SK"), 10, " ")
    )
    .withColumn(
       "FUND_THRU_DT_SK",
       F.rpad(F.col("FUND_THRU_DT_SK"), 10, " ")
    )
)
df_DefaultUNK_padded = (
    df_DefaultUNK
    .withColumn(
       "FUND_FROM_DT_SK",
       F.rpad(F.col("FUND_FROM_DT_SK"), 10, " ")
    )
    .withColumn(
       "FUND_THRU_DT_SK",
       F.rpad(F.col("FUND_THRU_DT_SK"), 10, " ")
    )
)
df_DefaultNA_padded = (
    df_DefaultNA
    .withColumn(
       "FUND_FROM_DT_SK",
       F.rpad(F.col("FUND_FROM_DT_SK"), 10, " ")
    )
    .withColumn(
       "FUND_THRU_DT_SK",
       F.rpad(F.col("FUND_THRU_DT_SK"), 10, " ")
    )
)

df_Collector = df_Fkey_padded.unionByName(df_DefaultUNK_padded).unionByName(df_DefaultNA_padded)

# 9) Stage: BILL_RDUCTN_HIST (CSeqFileStage) => write to adls_path/load/#TmpOutFile#
# The final columns in the correct order:
final_cols = [
    "BILL_RDUCTN_HIST_SK",
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "BILL_ENTY_UNIQ_KEY",
    "ALT_FUND_CNTR_PERD_NO",
    "FUND_FROM_DT_SK",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_FUND_INVC_SK",
    "BILL_ENTY_SK",
    "CLS_PLN_SK",
    "GRP_SK",
    "MBR_SK",
    "CLM_SK",
    "PROD_SK",
    "SUBGRP_SK",
    "FUND_THRU_DT_SK",
    "PCA_AMT",
    "CLM_ID"
]

df_final = df_Collector.select(*final_cols)

write_files(
    df_final,
    f"{adls_path}/load/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# AfterJobRoutine = "1" (not specified, no additional details given beyond above). Done.