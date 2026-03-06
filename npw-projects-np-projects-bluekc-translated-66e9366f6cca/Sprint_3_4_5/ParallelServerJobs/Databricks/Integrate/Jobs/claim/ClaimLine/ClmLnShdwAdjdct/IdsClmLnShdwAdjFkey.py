# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING:   Uses the file from extract job, FKey process is done here along with Error recycle and NA,UNK rows.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Raja Gummadi         2015-09-04       5382                       Original Programming                                                                                                                   IntegrateDev2                 Kalyan Neelam           2015-09-22
# MAGIC Rekha Radhakrishna 2020-10-20    6131                       Added field PBM_CALC_CSR_SBSDY_AMT                                                                              IntegrateDev2

# MAGIC Fkey Process
# MAGIC Load file used in Load seq
# MAGIC Write Fkey errors to hash file
# MAGIC This hash file is written to by all claim foriegn key jobs.
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
    StringType,
    IntegerType,
    DecimalType,
    TimestampType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value('SrcSysCd','')
Logging = get_widget_value('Logging','')
InFile = get_widget_value('InFile','')

# Schema for ClmLnShdwAdjExtr
schema_ClmLnShdwAdjExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("CLM_LN_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("SRC_SYS_CD_SK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD", StringType(), nullable=True),
    StructField("CLM_LN_SHADOW_ADJDCT_STTUS_CD", StringType(), nullable=False),
    StructField("SHADOW_MED_UTIL_EDIT_IN", StringType(), nullable=False),
    StructField("CNSD_CHRG_AMT", DecimalType(38,10), nullable=False),
    StructField("INIT_ADJDCT_ALW_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_ALW_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_COINS_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_COPAY_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_DEDCT_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_DSALW_AMT", DecimalType(38,10), nullable=False),
    StructField("SHADOW_ADJDCT_PAYBL_AMT", DecimalType(38,10), nullable=False),
    StructField("INIT_ADJDCT_ALW_PRICE_UNIT_CT", IntegerType(), nullable=False),
    StructField("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT", IntegerType(), nullable=False),
    StructField("SHADOW_DEDCT_AMT_ACCUM_ID", StringType(), nullable=False),
    StructField("SHADOW_LMT_PFX_ID", StringType(), nullable=False),
    StructField("SHADOW_PROD_CMPNT_DEDCT_PFX_ID", StringType(), nullable=False),
    StructField("SHADOW_PROD_CMPNT_SVC_PAYMT_ID", StringType(), nullable=False),
    StructField("SHADOW_SVC_RULE_TYP_TX", StringType(), nullable=True),
    StructField("PBM_CALC_CSR_SBSDY_AMT", DecimalType(38,10), nullable=True)
])

df_ClmLnShdwAdjExtr = (
    spark.read
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_ClmLnShdwAdjExtr)
    .csv(f"{adls_path}/key/{InFile}")
)

df_Transformer_0 = (
    df_ClmLnShdwAdjExtr
    .withColumn(
        "svExcdSk",
        GetFkeyExcd(
            SrcSysCd,
            F.col("CLM_LN_SK"),
            F.col("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD"),
            Logging
        )
    )
    .withColumn(
        "svSttusCdSk",
        GetFkeySrcTrgtClctnCodes(
            SrcSysCd,
            F.col("CLM_LN_SK"),
            F.lit("CLAIM STATUS"),
            F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD"),
            F.lit("FACETS DBO"),
            F.lit("IDS"),
            Logging
        )
    )
    .withColumn("PassThru", F.col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("CLM_LN_SK")))
)

df_Transformer_0_main = df_Transformer_0.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == "Y")
).withColumn(
    "SHADOW_MED_UTIL_EDIT_IN",
    F.when(
        F.col("SHADOW_MED_UTIL_EDIT_IN").isNull() | (F.length(F.col("SHADOW_MED_UTIL_EDIT_IN")) == 0),
        F.lit(" ")
    ).otherwise(F.col("SHADOW_MED_UTIL_EDIT_IN"))
).select(
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svExcdSk").alias("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
    F.col("svSttusCdSk").alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
    F.col("SHADOW_MED_UTIL_EDIT_IN").alias("SHADOW_MED_UTIL_EDIT_IN"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("INIT_ADJDCT_ALW_AMT").alias("INIT_ADJDCT_ALW_AMT"),
    F.col("SHADOW_ADJDCT_ALW_AMT").alias("SHADOW_ADJDCT_ALW_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("SHADOW_ADJDCT_DSALW_AMT").alias("SHADOW_ADJDCT_DSALW_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("INIT_ADJDCT_ALW_PRICE_UNIT_CT").alias("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT").alias("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("SHADOW_DEDCT_AMT_ACCUM_ID").alias("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.col("SHADOW_LMT_PFX_ID").alias("SHADOW_LMT_PFX_ID"),
    F.col("SHADOW_PROD_CMPNT_DEDCT_PFX_ID").alias("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("SHADOW_PROD_CMPNT_SVC_PAYMT_ID").alias("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("SHADOW_SVC_RULE_TYP_TX").alias("SHADOW_SVC_RULE_TYP_TX"),
    F.col("PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT")
)

df_Transformer_0_na_raw = df_Transformer_0.limit(1)
df_Transformer_0_na = df_Transformer_0_na_raw.select(
    F.lit(1).alias("CLM_LN_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
    F.lit(1).alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
    F.lit("N").alias("SHADOW_MED_UTIL_EDIT_IN"),
    F.lit(0).alias("CNSD_CHRG_AMT"),
    F.lit(0).alias("INIT_ADJDCT_ALW_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_ALW_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_COINS_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_DSALW_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.lit(0).alias("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.lit(0).alias("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.lit("NA").alias("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.lit("NA").alias("SHADOW_LMT_PFX_ID"),
    F.lit("NA").alias("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.lit("NA").alias("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.lit("NA").alias("SHADOW_SVC_RULE_TYP_TX"),
    F.col("PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT")
)

df_Transformer_0_unk_raw = df_Transformer_0.limit(1)
df_Transformer_0_unk = df_Transformer_0_unk_raw.select(
    F.lit(0).alias("CLM_LN_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK"),
    F.lit(0).alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK"),
    F.lit("N").alias("SHADOW_MED_UTIL_EDIT_IN"),
    F.lit(0).alias("CNSD_CHRG_AMT"),
    F.lit(0).alias("INIT_ADJDCT_ALW_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_ALW_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_COINS_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_DSALW_AMT"),
    F.lit(0).alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.lit(0).alias("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.lit(0).alias("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.lit("UNK").alias("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.lit("UNK").alias("SHADOW_LMT_PFX_ID"),
    F.lit("UNK").alias("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.lit("UNK").alias("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.lit("UNK").alias("SHADOW_SVC_RULE_TYP_TX"),
    F.col("PBM_CALC_CSR_SBSDY_AMT").alias("PBM_CALC_CSR_SBSDY_AMT")
)

df_Transformer_0_recycle_clms = df_Transformer_0.filter(
    F.col("ErrCount") > 0
).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CLM_ID").alias("CLM_ID")
)

df_Transformer_0_recycle = df_Transformer_0.filter(
    F.col("ErrCount") > 0
).select(
    GetRecycleKey(F.col("CLM_LN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_SK").alias("CLM_LN_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD").alias("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD"),
    F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD").alias("CLM_LN_SHADOW_ADJDCT_STTUS_CD"),
    F.col("SHADOW_MED_UTIL_EDIT_IN").alias("SHADOW_MED_UTIL_EDIT_IN"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("INIT_ADJDCT_ALW_AMT").alias("INIT_ADJDCT_ALW_AMT"),
    F.col("SHADOW_ADJDCT_ALW_AMT").alias("SHADOW_ADJDCT_ALW_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT").alias("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT").alias("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT").alias("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("SHADOW_ADJDCT_DSALW_AMT").alias("SHADOW_ADJDCT_DSALW_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT").alias("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("INIT_ADJDCT_ALW_PRICE_UNIT_CT").alias("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT").alias("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("SHADOW_DEDCT_AMT_ACCUM_ID").alias("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.col("SHADOW_LMT_PFX_ID").alias("SHADOW_LMT_PFX_ID"),
    F.col("SHADOW_PROD_CMPNT_DEDCT_PFX_ID").alias("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("SHADOW_PROD_CMPNT_SVC_PAYMT_ID").alias("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("SHADOW_SVC_RULE_TYP_TX").alias("SHADOW_SVC_RULE_TYP_TX")
)

# Write to hf_claim_recycle_keys (scenario C -> parquet)
df_tmp_hf_claim_recycle_keys = df_Transformer_0_recycle_clms.select(
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID")
)
df_tmp_hf_claim_recycle_keys = df_tmp_hf_claim_recycle_keys.withColumn(
    "SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 255, " ")
).withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), 255, " ")
)
write_files(
    df_tmp_hf_claim_recycle_keys,
    "hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

# Write to hf_recycle (scenario C -> parquet)
df_tmp_hf_recycle = df_Transformer_0_recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD"),
    F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD"),
    F.col("SHADOW_MED_UTIL_EDIT_IN"),
    F.col("CNSD_CHRG_AMT"),
    F.col("INIT_ADJDCT_ALW_AMT"),
    F.col("SHADOW_ADJDCT_ALW_AMT"),
    F.col("SHADOW_ADJDCT_COINS_AMT"),
    F.col("SHADOW_ADJDCT_COPAY_AMT"),
    F.col("SHADOW_ADJDCT_DEDCT_AMT"),
    F.col("SHADOW_ADJDCT_DSALW_AMT"),
    F.col("SHADOW_ADJDCT_PAYBL_AMT"),
    F.col("INIT_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("SHADOW_ADJDCT_ALW_PRICE_UNIT_CT"),
    F.col("SHADOW_DEDCT_AMT_ACCUM_ID"),
    F.col("SHADOW_LMT_PFX_ID"),
    F.col("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("SHADOW_SVC_RULE_TYP_TX")
)
df_tmp_hf_recycle = df_tmp_hf_recycle \
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " ")) \
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " ")) \
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " ")) \
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), 255, " ")) \
    .withColumn("PRI_KEY_STRING", F.rpad(F.col("PRI_KEY_STRING"), 255, " ")) \
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 255, " ")) \
    .withColumn("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD", F.rpad(F.col("CLM_LN_SHADOW_ADJDCT_DSALW_EXCD"), 255, " ")) \
    .withColumn("CLM_LN_SHADOW_ADJDCT_STTUS_CD", F.rpad(F.col("CLM_LN_SHADOW_ADJDCT_STTUS_CD"), 255, " ")) \
    .withColumn("SHADOW_MED_UTIL_EDIT_IN", F.rpad(F.col("SHADOW_MED_UTIL_EDIT_IN"), 1, " ")) \
    .withColumn("SHADOW_DEDCT_AMT_ACCUM_ID", F.rpad(F.col("SHADOW_DEDCT_AMT_ACCUM_ID"), 255, " ")) \
    .withColumn("SHADOW_LMT_PFX_ID", F.rpad(F.col("SHADOW_LMT_PFX_ID"), 255, " ")) \
    .withColumn("SHADOW_PROD_CMPNT_DEDCT_PFX_ID", F.rpad(F.col("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"), 255, " ")) \
    .withColumn("SHADOW_PROD_CMPNT_SVC_PAYMT_ID", F.rpad(F.col("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"), 255, " ")) \
    .withColumn("SHADOW_SVC_RULE_TYP_TX", F.rpad(F.col("SHADOW_SVC_RULE_TYP_TX"), 255, " "))

write_files(
    df_tmp_hf_recycle,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Link_Collector_1_Main = df_Transformer_0_main.select(
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK",
    "CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK",
    "SHADOW_MED_UTIL_EDIT_IN",
    "CNSD_CHRG_AMT",
    "INIT_ADJDCT_ALW_AMT",
    "SHADOW_ADJDCT_ALW_AMT",
    "SHADOW_ADJDCT_COINS_AMT",
    "SHADOW_ADJDCT_COPAY_AMT",
    "SHADOW_ADJDCT_DEDCT_AMT",
    "SHADOW_ADJDCT_DSALW_AMT",
    "SHADOW_ADJDCT_PAYBL_AMT",
    "INIT_ADJDCT_ALW_PRICE_UNIT_CT",
    "SHADOW_ADJDCT_ALW_PRICE_UNIT_CT",
    "SHADOW_DEDCT_AMT_ACCUM_ID",
    "SHADOW_LMT_PFX_ID",
    "SHADOW_PROD_CMPNT_DEDCT_PFX_ID",
    "SHADOW_PROD_CMPNT_SVC_PAYMT_ID",
    "SHADOW_SVC_RULE_TYP_TX",
    "PBM_CALC_CSR_SBSDY_AMT"
)

df_Link_Collector_1_NA = df_Transformer_0_na.select(
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK",
    "CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK",
    "SHADOW_MED_UTIL_EDIT_IN",
    "CNSD_CHRG_AMT",
    "INIT_ADJDCT_ALW_AMT",
    "SHADOW_ADJDCT_ALW_AMT",
    "SHADOW_ADJDCT_COINS_AMT",
    "SHADOW_ADJDCT_COPAY_AMT",
    "SHADOW_ADJDCT_DEDCT_AMT",
    "SHADOW_ADJDCT_DSALW_AMT",
    "SHADOW_ADJDCT_PAYBL_AMT",
    "INIT_ADJDCT_ALW_PRICE_UNIT_CT",
    "SHADOW_ADJDCT_ALW_PRICE_UNIT_CT",
    "SHADOW_DEDCT_AMT_ACCUM_ID",
    "SHADOW_LMT_PFX_ID",
    "SHADOW_PROD_CMPNT_DEDCT_PFX_ID",
    "SHADOW_PROD_CMPNT_SVC_PAYMT_ID",
    "SHADOW_SVC_RULE_TYP_TX",
    "PBM_CALC_CSR_SBSDY_AMT"
)

df_Link_Collector_1_UNK = df_Transformer_0_unk.select(
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_LN_SHADOW_ADJDCT_DSALW_EXCD_SK",
    "CLM_LN_SHADOW_ADJDCT_STTUS_CD_SK",
    "SHADOW_MED_UTIL_EDIT_IN",
    "CNSD_CHRG_AMT",
    "INIT_ADJDCT_ALW_AMT",
    "SHADOW_ADJDCT_ALW_AMT",
    "SHADOW_ADJDCT_COINS_AMT",
    "SHADOW_ADJDCT_COPAY_AMT",
    "SHADOW_ADJDCT_DEDCT_AMT",
    "SHADOW_ADJDCT_DSALW_AMT",
    "SHADOW_ADJDCT_PAYBL_AMT",
    "INIT_ADJDCT_ALW_PRICE_UNIT_CT",
    "SHADOW_ADJDCT_ALW_PRICE_UNIT_CT",
    "SHADOW_DEDCT_AMT_ACCUM_ID",
    "SHADOW_LMT_PFX_ID",
    "SHADOW_PROD_CMPNT_DEDCT_PFX_ID",
    "SHADOW_PROD_CMPNT_SVC_PAYMT_ID",
    "SHADOW_SVC_RULE_TYP_TX",
    "PBM_CALC_CSR_SBSDY_AMT"
)

df_Link_Collector_1 = (
    df_Link_Collector_1_Main
    .union(df_Link_Collector_1_NA)
    .union(df_Link_Collector_1_UNK)
)

df_tmp_LoadFile = df_Link_Collector_1 \
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 255, " ")) \
    .withColumn("SHADOW_MED_UTIL_EDIT_IN", F.rpad(F.col("SHADOW_MED_UTIL_EDIT_IN"), 1, " ")) \
    .withColumn("SHADOW_DEDCT_AMT_ACCUM_ID", F.rpad(F.col("SHADOW_DEDCT_AMT_ACCUM_ID"), 255, " ")) \
    .withColumn("SHADOW_LMT_PFX_ID", F.rpad(F.col("SHADOW_LMT_PFX_ID"), 255, " ")) \
    .withColumn("SHADOW_PROD_CMPNT_DEDCT_PFX_ID", F.rpad(F.col("SHADOW_PROD_CMPNT_DEDCT_PFX_ID"), 255, " ")) \
    .withColumn("SHADOW_PROD_CMPNT_SVC_PAYMT_ID", F.rpad(F.col("SHADOW_PROD_CMPNT_SVC_PAYMT_ID"), 255, " ")) \
    .withColumn("SHADOW_SVC_RULE_TYP_TX", F.rpad(F.col("SHADOW_SVC_RULE_TYP_TX"), 255, " "))

write_files(
    df_tmp_LoadFile,
    f"{adls_path}/load/CLM_LN_SHADOW_ADJDCT.{SrcSysCd}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)