# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 09:39:22 Batch  14638_34767 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/27/07 09:46:56 Batch  14606_35219 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/07/07 14:46:06 Batch  14556_53195 PROMOTE bckcetl ids20 dsadm bls for rt
# MAGIC ^1_1 11/07/07 14:33:10 Batch  14556_52396 INIT bckcett testIDScur dsadm bls for rt
# MAGIC ^1_1 10/17/07 13:27:15 Batch  14535_48445 PROMOTE bckcett testIDScur u06640 Ralph
# MAGIC ^1_1 10/17/07 13:17:55 Batch  14535_47880 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC 
# MAGIC Â© Copyright 2007 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  <Sequencer Name>
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                    Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ----------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Ralph Tucker              2007-08-27                                      Original Programming.                                                                  devlIDS30                       Steph Goddard          09/28/2007

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
InFile = get_widget_value('InFile','')
Logging = get_widget_value('Logging','')

# Schema for IdsAltFundInvcDscrtn (CSeqFileStage)
schema_IdsAltFundInvcDscrtn = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("ALT_FUND_INVC_DSCRTN_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND_INVC_ID", StringType(), nullable=False),
    StructField("SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ALT_FUND_INVC_SK", StringType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("SUBGRP_ID", StringType(), nullable=False),
    StructField("INVC_DSCRTN_BEG_DT_SK", StringType(), nullable=False),
    StructField("INVC_DSCRTN_END_DT_SK", StringType(), nullable=False),
    StructField("EXTRA_CNTR_AMT", DecimalType(38,10), nullable=False),
    StructField("DSCRTN_MO_QTY", IntegerType(), nullable=False),
    StructField("DSCRTN_DESC", StringType(), nullable=True),
    StructField("DSCRTN_PRSN_ID_TX", StringType(), nullable=True),
    StructField("DSCRTN_SH_DESC", StringType(), nullable=True)
])

# Read from file (IdsAltFundInvcDscrtn)
df_IdsAltFundInvcDscrtn = (
    spark.read
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsAltFundInvcDscrtn)
    .csv(f"{adls_path}/key/{InFile}")
)

# Transformer: ForeignKey
df_ForeignKey_vars = (
    df_IdsAltFundInvcDscrtn
    .withColumn("SrcSysCd", GetFkeyCodes(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.lit("SOURCE SYSTEM"), F.col("SRC_SYS_CD"), F.lit(Logging)))
    .withColumn("svGrp", GetFkeyGrp(F.col("SRC_SYS_CD"), F.col("ALT_FUND_INVC_SK"), F.col("GRP_ID"), F.lit(Logging)))
    .withColumn("svSubGrp", GetFkeySubgrp(F.col("SRC_SYS_CD"), F.col("ALT_FUND_INVC_SK"), F.col("GRP_ID"), F.col("SUBGRP_ID"), F.lit(Logging)))
    .withColumn("svBegDt", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("INVC_DSCRTN_BEG_DT_SK"), F.lit(Logging)))
    .withColumn("svEndDt", GetFkeyDate(F.lit("IDS"), F.col("ALT_FUND_INVC_SK"), F.col("INVC_DSCRTN_END_DT_SK"), F.lit(Logging)))
    .withColumn("svAltFundInvcSk", GetFkeyAltFundInvc(F.col("SRC_SYS_CD"), F.col("ALT_FUND_INVC_DSCRTN_SK"), F.col("ALT_FUND_INVC_ID"), F.lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("ALT_FUND_INVC_SK")))
)

# Output link Fkey (ErrCount = 0)
df_Fkey = (
    df_ForeignKey_vars
    .filter(F.col("ErrCount") == 0)
    .select(
        F.col("ALT_FUND_INVC_DSCRTN_SK").alias("ALT_FUND_INVC_DSCRTN_SK"),
        F.col("SrcSysCd").alias("SRC_SYS_CD_SK"),
        F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svAltFundInvcSk").alias("ALT_FUND_INVC_SK"),
        F.col("svGrp").alias("GRP_SK"),
        F.col("svSubGrp").alias("SUBGRP_SK"),
        F.col("svBegDt").alias("INVC_DSCRTN_BEG_DT_SK"),
        F.col("svEndDt").alias("INVC_DSCRTN_END_DT_SK"),
        F.col("EXTRA_CNTR_AMT").alias("EXTRA_CNTR_AMT"),
        F.col("DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY"),
        F.col("DSCRTN_DESC").alias("DSCRTN_DESC"),
        F.col("DSCRTN_PRSN_ID_TX").alias("DSCRTN_PRSN_ID_TX"),
        F.col("DSCRTN_SH_DESC").alias("DSCRTN_SH_DESC")
    )
)

# Output link DefaultUNK (@INROWNUM = 1)
df_DefaultUNK = (
    df_ForeignKey_vars
    .limit(1)
    .select(
        F.lit(0).alias("ALT_FUND_INVC_DSCRTN_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit("UNK").alias("ALT_FUND_INVC_ID"),
        F.lit(0).alias("SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("ALT_FUND_INVC_SK"),
        F.lit(0).alias("GRP_SK"),
        F.lit(0).alias("SUBGRP_SK"),
        F.lit("UNK").alias("INVC_DSCRTN_BEG_DT_SK"),
        F.lit("UNK").alias("INVC_DSCRTN_END_DT_SK"),
        F.lit(0).alias("EXTRA_CNTR_AMT"),
        F.lit(0).alias("DSCRTN_MO_QTY"),
        F.lit("UNK").alias("DSCRTN_DESC"),
        F.lit("UNK").alias("DSCRTN_PRSN_ID_TX"),
        F.lit("UNK").alias("DSCRTN_SH_DESC")
    )
)

# Output link DefaultNA (@INROWNUM = 1)
df_DefaultNA = (
    df_ForeignKey_vars
    .limit(1)
    .select(
        F.lit(1).alias("ALT_FUND_INVC_DSCRTN_SK"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit("NA").alias("ALT_FUND_INVC_ID"),
        F.lit(1).alias("SEQ_NO"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("ALT_FUND_INVC_SK"),
        F.lit(1).alias("GRP_SK"),
        F.lit(1).alias("SUBGRP_SK"),
        F.lit("NA").alias("INVC_DSCRTN_BEG_DT_SK"),
        F.lit("NA").alias("INVC_DSCRTN_END_DT_SK"),
        F.lit(0).alias("EXTRA_CNTR_AMT"),
        F.lit(0).alias("DSCRTN_MO_QTY"),
        F.lit("NA").alias("DSCRTN_DESC"),
        F.lit("NA").alias("DSCRTN_PRSN_ID_TX"),
        F.lit("NA").alias("DSCRTN_SH_DESC")
    )
)

# Output link Recycle (ErrCount > 0)
df_Recycle = (
    df_ForeignKey_vars
    .filter(F.col("ErrCount") > 0)
    .select(
        GetRecycleKey(F.col("ALT_FUND_INVC_DSCRTN_SK")).alias("ALT_FUND_INVC_DSCRTN_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("ALT_FUND_INVC_ID").alias("ALT_FUND_INVC_ID"),
        F.col("SEQ_NO").alias("SEQ_NO"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ALT_FUND_INVC_SK").alias("ALT_FUND_INVC_SK"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
        F.col("INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
        F.col("EXTRA_CNTR_AMT").alias("EXTRA_CNTR_AMT"),
        F.col("DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY"),
        F.col("DSCRTN_DESC").alias("DSCRTN_DESC"),
        F.col("DSCRTN_PRSN_ID_TX").alias("DSCRTN_PRSN_ID_TX"),
        F.col("DSCRTN_SH_DESC").alias("DSCRTN_SH_DESC")
    )
)

# Write to hashed file hf_recycle (Scenario C => parquet)
write_files(
    df_Recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Collector: union of Fkey, DefaultUNK, DefaultNA
df_Collector = df_Fkey.unionByName(df_DefaultUNK).unionByName(df_DefaultNA)

# Final rpad for char(10) columns in the collector output
df_Collector_enriched = (
    df_Collector
    .withColumn("INVC_DSCRTN_BEG_DT_SK", F.rpad(F.col("INVC_DSCRTN_BEG_DT_SK"), 10, " "))
    .withColumn("INVC_DSCRTN_END_DT_SK", F.rpad(F.col("INVC_DSCRTN_END_DT_SK"), 10, " "))
)

# Select columns in correct order before writing
df_Final = df_Collector_enriched.select(
    "ALT_FUND_INVC_DSCRTN_SK",
    "SRC_SYS_CD_SK",
    "ALT_FUND_INVC_ID",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_FUND_INVC_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "INVC_DSCRTN_BEG_DT_SK",
    "INVC_DSCRTN_END_DT_SK",
    "EXTRA_CNTR_AMT",
    "DSCRTN_MO_QTY",
    "DSCRTN_DESC",
    "DSCRTN_PRSN_ID_TX",
    "DSCRTN_SH_DESC"
)

# Write final output (AltFundInvcDscrtn -> CSeqFileStage)
write_files(
    df_Final,
    f"{adls_path}/load/ALT_FUND_INVC_DSCRTN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)