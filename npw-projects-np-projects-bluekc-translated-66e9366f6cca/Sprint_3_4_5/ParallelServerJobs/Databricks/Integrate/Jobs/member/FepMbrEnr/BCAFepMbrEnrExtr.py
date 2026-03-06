# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  EdwFepMbrEnrCntl
# MAGIC 
# MAGIC PROCESSING:  Extracts data from flat file and loads it into FEP_MBR_ENR table.
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                  Project/Altiris #      \(9)    Change Description                                            Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------      ------------------------      \(9)    ------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Santosh Bokka       2013-09-09        5056\(9)                    Original Programming\(9)\(9)               IntegrateNewDevl            Kalyan Neelam         2013-10-04
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                  Project/Altiris #      \(9)    Change Description                                            Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------      ------------------------      \(9)    ------------------------------------------------------------------         --------------------------------       -------------------------------   ----------------------------       
# MAGIC Santosh Bokka       2013-11-04        5056\(9)                    Added FEP Mbr Local Indicator Transformer       IntegrateNewDevl          Kalyan Neelam           2013-11-08

# MAGIC P_FEP_MBR_ENR Extract
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
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql.functions import col, when, length, isnull, lit
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunDate = get_widget_value('RunDate','')
SourceSk = get_widget_value('SourceSk','')
CrrrRunCycle = get_widget_value('CrrrRunCycle','')
InFile = get_widget_value('InFile','')

# Read from IDS MBR (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_mbr = (
    "SELECT "
    "MBR.MBR_SK, "
    "SUB.SUB_ID, "
    "MBR.BRTH_DT_SK AS MBR_BRTH_DT_SK, "
    "MBR.SSN AS MBR_SSN, "
    "MBR.FIRST_NM AS MBR_FIRST_NM, "
    "MBR.LAST_NM AS MBR_LAST_NM, "
    "MBR.MBR_GNDR_CD_SK AS MBR_GNDR_CD_SK, "
    "MBR.FEP_LOCAL_CNTR_IN AS FEP_LOCAL_CNTR_IN "
    "FROM " + IDSOwner + ".MBR MBR, " + IDSOwner + ".SUB SUB "
    "WHERE SUB.SUB_SK = MBR.SUB_SK"
)
df_MBR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr)
    .load()
)

# Rename columns for consistency
df_MBR = (
    df_MBR
    .withColumnRenamed("MBR_GNDR_CD_SK", "MBR_GNDR_CD")
)

# Scenario C for hf_etrnl_cd_mppng (CHashedFileStage only reading)
df_hf_etrnl_cd_mppng = spark.read.parquet(f"{adls_path}/hf_etrnl_cd_mppng.parquet")
df_hf_etrnl_cd_mppng = df_hf_etrnl_cd_mppng.select("CD_MPPNG_SK", "TRGT_CD")

# Mbr_Info (CTransformerStage) with one primary link (df_MBR) and one lookup link (df_hf_etrnl_cd_mppng) left-join
df_mbr_info_joined = df_MBR.alias("fep_mbr").join(
    df_hf_etrnl_cd_mppng.alias("cdmppg"),
    (col("fep_mbr.MBR_GNDR_CD") == col("cdmppg.CD_MPPNG_SK")),
    "left"
)

# Build outputs from Mbr_Info

# step1_lkup
df_step1_lkup = df_mbr_info_joined.select(
    col("fep_mbr.SUB_ID").alias("SUB_ID"),
    col("fep_mbr.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    col("fep_mbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("fep_mbr.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    when(isnull(col("cdmppg.CD_MPPNG_SK")) | (length(col("cdmppg.CD_MPPNG_SK")) == 0), lit("0"))
    .otherwise(col("cdmppg.TRGT_CD")).alias("MBR_GNDR_CD"),
    col("fep_mbr.MBR_SK").alias("MBR_SK"),
    col("fep_mbr.MBR_SSN").alias("MBR_SSN"),
    col("fep_mbr.FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

# step2_lkup
df_step2_lkup = df_mbr_info_joined.select(
    col("fep_mbr.SUB_ID").alias("SUB_ID"),
    col("fep_mbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("fep_mbr.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("fep_mbr.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    col("fep_mbr.MBR_SK").alias("MBR_SK"),
    when(isnull(col("cdmppg.CD_MPPNG_SK")) | (length(col("cdmppg.CD_MPPNG_SK")) == 0), lit("0"))
    .otherwise(col("cdmppg.TRGT_CD")).alias("MBR_GNDR_CD"),
    col("fep_mbr.FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

# step3_lkup
df_step3_lkup = df_mbr_info_joined.select(
    col("fep_mbr.SUB_ID").alias("SUB_ID"),
    col("fep_mbr.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
    col("fep_mbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
    when(isnull(col("cdmppg.CD_MPPNG_SK")) | (length(col("cdmppg.CD_MPPNG_SK")) == 0), lit("0"))
    .otherwise(col("cdmppg.TRGT_CD")).alias("MBR_GNDR_CD"),
    col("fep_mbr.MBR_SK").alias("MBR_SK"),
    col("fep_mbr.MBR_SSN").alias("MBR_SSN"),
    col("fep_mbr.FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

# step4_lkup
df_step4_lkup = df_mbr_info_joined.select(
    col("fep_mbr.SUB_ID").alias("SUB_ID"),
    col("fep_mbr.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("fep_mbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
    when(isnull(col("cdmppg.CD_MPPNG_SK")) | (length(col("cdmppg.CD_MPPNG_SK")) == 0), lit("0"))
    .otherwise(col("cdmppg.TRGT_CD")).alias("MBR_GNDR_CD"),
    col("fep_mbr.MBR_SK").alias("MBR_SK"),
    col("fep_mbr.FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

# Scenario A for hf_fep_mbr_enr_step1
df_step1_lkup = dedup_sort(
    df_step1_lkup,
    partition_cols=["SUB_ID","MBR_BRTH_DT_SK","MBR_LAST_NM","MBR_FIRST_NM"],
    sort_cols=[]
)

# Scenario A for hf_fep_mbr_enr_step2
df_step2_lkup = dedup_sort(
    df_step2_lkup,
    partition_cols=["SUB_ID","MBR_LAST_NM","MBR_FIRST_NM","MBR_BRTH_DT_SK"],
    sort_cols=[]
)

# Scenario A for hf_fep_mbr_enr_step3
df_step3_lkup = dedup_sort(
    df_step3_lkup,
    partition_cols=["SUB_ID","MBR_BRTH_DT_SK","MBR_LAST_NM"],
    sort_cols=[]
)

# Scenario A for hf_fep_mbr_enr_step4
df_step4_lkup = dedup_sort(
    df_step4_lkup,
    partition_cols=["SUB_ID","MBR_FIRST_NM"],
    sort_cols=[]
)

# Read BCA_File (CSeqFileStage) from landing
schema_BCA_File = StructType([
    StructField("ALLOCD_CRS_PLN_CD", StringType(), True),
    StructField("ALLOCD_SHIELD_PLN_CD", StringType(), True),
    StructField("CONSIS_MBR_ID", StringType(), True),
    StructField("FEP_CNTR_ID", StringType(), True),
    StructField("LGCY_MBR_ID", StringType(), True),
    StructField("CUR_PATN_CD", StringType(), True),
    StructField("RELSHP_CD", StringType(), True),
    StructField("ENR_OPT_CD", StringType(), True),
    StructField("LAST_NM", StringType(), True),
    StructField("FIRST_NM", StringType(), True),
    StructField("MIDINIT", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("ST_ADDR_1", StringType(), True),
    StructField("ST_ADDR_2", StringType(), True),
    StructField("ADDR_CITY", StringType(), True),
    StructField("ADDR_ST", StringType(), True),
    StructField("ADDR_ZIP", StringType(), True),
    StructField("SUB_PHN_NO", StringType(), True),
    StructField("MBR_MO_BASE_YR", IntegerType(), True),
    StructField("PROSP_RISK_SCORE", DecimalType(38,10), True),
    StructField("ACTL_AMT_AGGD_DCG_CAT", StringType(), True),
    StructField("ACTL_LOW_AMT", DecimalType(38,10), True),
    StructField("ACTL_HI_AMT", DecimalType(38,10), True),
    StructField("AGGD_DCG_CAT_YR_2", StringType(), True),
    StructField("XPCT_LOW_AMT_YR_2", DecimalType(38,10), True),
    StructField("XPCT_HI_AMT_YR_2", DecimalType(38,10), True),
    StructField("MCARE_FLAG", StringType(), True),
    StructField("HSPC_FLAG", StringType(), True),
    StructField("DROP_DT", StringType(), True),
    StructField("EXTR_STTUS_CD", StringType(), True),
    StructField("ACTV_ANNUITANT_GRP_CD", StringType(), True),
    StructField("MCARE_COV_CD", StringType(), True),
    StructField("MCARE_STTUS_CD", StringType(), True),
    StructField("OTHR_PRI_ENR_COV", StringType(), True)
])

df_BCA_File = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_BCA_File)
    .load(f"{adls_path_raw}/landing/{InFile}")
)

# Trim_Data (CTransformerStage)
df_Trim_Data = (
    df_BCA_File
    .withColumn("ALLOCD_CRS_PLN_CD", trim(strip_field(col("ALLOCD_CRS_PLN_CD"))))
    .withColumn("ALLOCD_SHIELD_PLN_CD", trim(strip_field(col("ALLOCD_SHIELD_PLN_CD"))))
    .withColumn("CONSIS_MBR_ID", trim(strip_field(col("CONSIS_MBR_ID"))))
    .withColumn("FEP_CNTR_ID", trim(strip_field(col("FEP_CNTR_ID"))))
    .withColumn("LGCY_MBR_ID", trim(strip_field(col("LGCY_MBR_ID"))))
    .withColumn("CUR_PATN_CD", trim(strip_field(col("CUR_PATN_CD"))))
    .withColumn("RELSHP_CD", trim(strip_field(col("RELSHP_CD"))))
    .withColumn("ENR_OPT_CD", trim(strip_field(col("ENR_OPT_CD"))))
    .withColumn("LAST_NM", trim(strip_field(col("LAST_NM"))))
    .withColumn("FIRST_NM", trim(strip_field(col("FIRST_NM"))))
    .withColumn("MIDINIT", trim(strip_field(col("MIDINIT"))))
    .withColumn("GNDR_CD", trim(strip_field(col("GNDR_CD"))))
    .withColumn("DOB", trim(strip_field(col("DOB"))))
    .withColumn("ST_ADDR_1", trim(strip_field(col("ST_ADDR_1"))))
    .withColumn("ST_ADDR_2", trim(strip_field(col("ST_ADDR_2"))))
    .withColumn("ADDR_CITY", trim(strip_field(col("ADDR_CITY"))))
    .withColumn("ADDR_ST", trim(strip_field(col("ADDR_ST"))))
    .withColumn("ADDR_ZIP", trim(strip_field(col("ADDR_ZIP"))))
    .withColumn("SUB_PHN_NO", trim(strip_field(col("SUB_PHN_NO"))))
    .withColumn("MBR_MO_BASE_YR", trim(strip_field(col("MBR_MO_BASE_YR"))))
    .withColumn("PROSP_RISK_SCORE", trim(strip_field(col("PROSP_RISK_SCORE"))))
    .withColumn("ACTL_AMT_AGGD_DCG_CAT", trim(strip_field(col("ACTL_AMT_AGGD_DCG_CAT"))))
    .withColumn("ACTL_LOW_AMT", trim(strip_field(col("ACTL_LOW_AMT"))))
    .withColumn("ACTL_HI_AMT", trim(strip_field(col("ACTL_HI_AMT"))))
    .withColumn("AGGD_DCG_CAT_YR_2", trim(strip_field(col("AGGD_DCG_CAT_YR_2"))))
    .withColumn("XPCT_LOW_AMT_YR_2", trim(strip_field(col("XPCT_LOW_AMT_YR_2"))))
    .withColumn("XPCT_HI_AMT_YR_2", trim(strip_field(col("XPCT_HI_AMT_YR_2"))))
    .withColumn("MCARE_FLAG", trim(strip_field(col("MCARE_FLAG"))))
    .withColumn("HSPC_FLAG", trim(strip_field(col("HSPC_FLAG"))))
    .withColumn("DROP_DT", trim(strip_field(col("DROP_DT"))))
    .withColumn("EXTR_STTUS_CD", trim(strip_field(col("EXTR_STTUS_CD"))))
    .withColumn("ACTV_ANNUITANT_GRP_CD", trim(strip_field(col("ACTV_ANNUITANT_GRP_CD"))))
    .withColumn("MCARE_COV_CD", trim(strip_field(col("MCARE_COV_CD"))))
    .withColumn("MCARE_STTUS_CD", trim(strip_field(col("MCARE_STTUS_CD"))))
    .withColumn("OTHR_PRI_ENR_COV", trim(strip_field(col("OTHR_PRI_ENR_COV"))))
)

# Pre_Process (CTransformerStage)
df_Pre_Process = df_Trim_Data.select(
    col("ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
    col("ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
    col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    when(
        length(trim(col("FEP_CNTR_ID"), lit("0"), lit("L"))) == 7,
        lit("R0") + trim(col("FEP_CNTR_ID"), lit("0"), lit("L"))
    ).when(
        length(trim(col("FEP_CNTR_ID"), lit("0"), lit("L"))) == 6,
        lit("R00") + trim(col("FEP_CNTR_ID"), lit("0"), lit("L"))
    ).when(
        length(trim(col("FEP_CNTR_ID"), lit("0"), lit("L"))) == 5,
        lit("R000") + trim(col("FEP_CNTR_ID"), lit("0"), lit("L"))
    ).otherwise(lit("R") + trim(col("FEP_CNTR_ID"), lit("0"), lit("L")))
    .alias("FEP_CNTR_ID"),
    col("LGCY_MBR_ID").substr(lit(-9), lit(9)).alias("LGCY_MBR_ID"),
    col("CUR_PATN_CD").alias("CUR_PATN_CD"),
    col("RELSHP_CD").alias("RELSHP_CD"),
    col("ENR_OPT_CD").alias("ENR_OPT_CD"),
    col("LAST_NM").alias("LAST_NM"),
    col("FIRST_NM").alias("FIRST_NM"),
    col("MIDINIT").alias("MIDINIT"),
    col("GNDR_CD").alias("GNDR_CD"),
    col("DOB").alias("DOB"),
    col("ST_ADDR_1").alias("ST_ADDR_1"),
    col("ST_ADDR_2").alias("ST_ADDR_2"),
    col("ADDR_CITY").alias("ADDR_CITY"),
    col("ADDR_ST").alias("ADDR_ST"),
    col("ADDR_ZIP").alias("ADDR_ZIP"),
    col("SUB_PHN_NO").alias("SUB_PHN_NO"),
    col("MBR_MO_BASE_YR").alias("MBR_MO_BASE_YR"),
    col("PROSP_RISK_SCORE").alias("PROSP_RISK_SCORE"),
    col("ACTL_AMT_AGGD_DCG_CAT").alias("ACTL_AMT_AGGD_DCG_CAT"),
    col("ACTL_LOW_AMT").alias("ACTL_LOW_AMT"),
    col("ACTL_HI_AMT").alias("ACTL_HI_AMT"),
    col("AGGD_DCG_CAT_YR_2").alias("AGGD_DCG_CAT_YR_2"),
    col("XPCT_LOW_AMT_YR_2").alias("XPCT_LOW_AMT_YR_2"),
    col("XPCT_HI_AMT_YR_2").alias("XPCT_HI_AMT_YR_2"),
    col("MCARE_FLAG").alias("MCARE_FLAG"),
    col("HSPC_FLAG").alias("HSPC_FLAG"),
    col("DROP_DT").alias("DROP_DT"),
    col("EXTR_STTUS_CD").alias("EXTR_STTUS_CD"),
    col("ACTV_ANNUITANT_GRP_CD").alias("ACTV_ANNUITANT_GRP_CD"),
    col("MCARE_COV_CD").alias("MCARE_COV_CD"),
    col("MCARE_STTUS_CD").alias("MCARE_STTUS_CD"),
    col("OTHR_PRI_ENR_COV").alias("OTHR_PRI_ENR_COV")
)

# Step_1 (CTransformerStage) has two inputs:
#   PrimaryLink = BCA_out (df_Pre_Process)
#   LookupLink = step1_lkup_in (df_step1_lkup)
# The constraints produce two outputs: match1, nomatch1

df_step1_join = df_Pre_Process.alias("BCA_out").join(
    df_step1_lkup.alias("step1_lkup_in"),
    (
        (col("BCA_out.FEP_CNTR_ID") == col("step1_lkup_in.SUB_ID")) &
        (col("BCA_out.DOB") == col("step1_lkup_in.MBR_BRTH_DT_SK")) &
        (col("BCA_out.LAST_NM") == col("step1_lkup_in.MBR_LAST_NM")) &
        (col("BCA_out.FIRST_NM") == col("step1_lkup_in.MBR_FIRST_NM")) &
        (col("BCA_out.GNDR_CD") == col("step1_lkup_in.MBR_GNDR_CD")) &
        (col("BCA_out.MBR_SSN") == col("step1_lkup_in.MBR_SSN"))
    ),
    "left"
)

df_step1_match1 = df_step1_join.filter(
    ~isnull(col("step1_lkup_in.SUB_ID")) &
    ~isnull(col("step1_lkup_in.MBR_BRTH_DT_SK")) &
    ~isnull(col("step1_lkup_in.MBR_FIRST_NM")) &
    ~isnull(col("step1_lkup_in.MBR_LAST_NM"))
).select(
    col("step1_lkup_in.MBR_SK").alias("MBR_SK"),
    col("BCA_out.LGCY_MBR_ID").alias("LGCY_FEP_MBR_ID"),
    lit("0").alias("SRC_SYS_CD_SK"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("step1_lkup_in.FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

df_step1_nomatch1 = df_step1_join.filter(
    isnull(col("step1_lkup_in.SUB_ID")) &
    isnull(col("step1_lkup_in.MBR_BRTH_DT_SK")) &
    isnull(col("step1_lkup_in.MBR_FIRST_NM")) &
    isnull(col("step1_lkup_in.MBR_LAST_NM"))
).select(
    col("BCA_out.ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
    col("BCA_out.ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
    col("BCA_out.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    col("BCA_out.FEP_CNTR_ID").alias("FEP_CNTR_ID"),
    col("BCA_out.LGCY_MBR_ID").alias("LGCY_MBR_ID"),
    col("BCA_out.CUR_PATN_CD").alias("CUR_PATN_CD"),
    col("BCA_out.RELSHP_CD").alias("RELSHP_CD"),
    col("BCA_out.ENR_OPT_CD").alias("ENR_OPT_CD"),
    col("BCA_out.LAST_NM").alias("LAST_NM"),
    col("BCA_out.FIRST_NM").alias("FIRST_NM"),
    col("BCA_out.MIDINIT").alias("MIDINIT"),
    col("BCA_out.GNDR_CD").alias("GNDR_CD"),
    col("BCA_out.DOB").alias("DOB"),
    col("BCA_out.ST_ADDR_1").alias("ST_ADDR_1"),
    col("BCA_out.ST_ADDR_2").alias("ST_ADDR_2"),
    col("BCA_out.ADDR_CITY").alias("ADDR_CITY"),
    col("BCA_out.ADDR_ST").alias("ADDR_ST"),
    col("BCA_out.ADDR_ZIP").alias("ADDR_ZIP"),
    col("BCA_out.SUB_PHN_NO").alias("SUB_PHN_NO"),
    col("BCA_out.MBR_MO_BASE_YR").alias("MBR_MO_BASE_YR"),
    col("BCA_out.PROSP_RISK_SCORE").alias("PROSP_RISK_SCORE"),
    col("BCA_out.ACTL_AMT_AGGD_DCG_CAT").alias("ACTL_AMT_AGGD_DCG_CAT"),
    col("BCA_out.ACTL_LOW_AMT").alias("ACTL_LOW_AMT"),
    col("BCA_out.ACTL_HI_AMT").alias("ACTL_HI_AMT"),
    col("BCA_out.AGGD_DCG_CAT_YR_2").alias("AGGD_DCG_CAT_YR_2"),
    col("BCA_out.XPCT_LOW_AMT_YR_2").alias("XPCT_LOW_AMT_YR_2"),
    col("BCA_out.XPCT_HI_AMT_YR_2").alias("XPCT_HI_AMT_YR_2"),
    col("BCA_out.MCARE_FLAG").alias("MCARE_FLAG"),
    col("BCA_out.HSPC_FLAG").alias("HSPC_FLAG"),
    col("BCA_out.DROP_DT").alias("DROP_DT"),
    col("BCA_out.EXTR_STTUS_CD").alias("EXTR_STTUS_CD"),
    col("BCA_out.ACTV_ANNUITANT_GRP_CD").alias("ACTV_ANNUITANT_GRP_CD"),
    col("BCA_out.MCARE_COV_CD").alias("MCARE_COV_CD"),
    col("BCA_out.MCARE_STTUS_CD").alias("MCARE_STTUS_CD"),
    col("BCA_out.OTHR_PRI_ENR_COV").alias("OTHR_PRI_ENR_COV")
)

# Step_2 (CTransformerStage): primaryLink = nomatch1, lookupLink = step2_lkup_in
df_step2_join = df_step1_nomatch1.alias("nomatch1").join(
    df_step2_lkup.alias("step2_lkup_in"),
    (
        (col("nomatch1.FEP_CNTR_ID") == col("step2_lkup_in.SUB_ID")) &
        (col("nomatch1.LAST_NM") == col("step2_lkup_in.MBR_LAST_NM")) &
        (col("nomatch1.FIRST_NM") == col("step2_lkup_in.MBR_FIRST_NM")) &
        (col("nomatch1.DOB") == col("step2_lkup_in.MBR_BRTH_DT_SK")) &
        (col("nomatch1.GNDR_CD") == col("step2_lkup_in.MBR_GNDR_CD"))
    ),
    "left"
)

df_step2_match2 = df_step2_join.filter(
    ~isnull(col("step2_lkup_in.SUB_ID")) &
    ~isnull(col("step2_lkup_in.MBR_FIRST_NM")) &
    ~isnull(col("step2_lkup_in.MBR_LAST_NM"))
).select(
    col("step2_lkup_in.MBR_SK").alias("MBR_SK"),
    col("nomatch1.LGCY_MBR_ID").alias("LGCY_FEP_MBR_ID"),
    lit("0").alias("SRC_SYS_CD_SK"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("step2_lkup_in.FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

df_step2_nomatch2 = df_step2_join.filter(
    isnull(col("step2_lkup_in.SUB_ID")) &
    isnull(col("step2_lkup_in.MBR_FIRST_NM")) &
    isnull(col("step2_lkup_in.MBR_LAST_NM"))
).select(
    col("nomatch1.ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
    col("nomatch1.ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
    col("nomatch1.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    col("nomatch1.FEP_CNTR_ID").alias("FEP_CNTR_ID"),
    col("nomatch1.LGCY_MBR_ID").alias("LGCY_MBR_ID"),
    col("nomatch1.CUR_PATN_CD").alias("CUR_PATN_CD"),
    col("nomatch1.RELSHP_CD").alias("RELSHP_CD"),
    col("nomatch1.ENR_OPT_CD").alias("ENR_OPT_CD"),
    col("nomatch1.LAST_NM").alias("LAST_NM"),
    col("nomatch1.FIRST_NM").alias("FIRST_NM"),
    col("nomatch1.MIDINIT").alias("MIDINIT"),
    col("nomatch1.GNDR_CD").alias("GNDR_CD"),
    col("nomatch1.DOB").alias("DOB"),
    col("nomatch1.ST_ADDR_1").alias("ST_ADDR_1"),
    col("nomatch1.ST_ADDR_2").alias("ST_ADDR_2"),
    col("nomatch1.ADDR_CITY").alias("ADDR_CITY"),
    col("nomatch1.ADDR_ST").alias("ADDR_ST"),
    col("nomatch1.ADDR_ZIP").alias("ADDR_ZIP"),
    col("nomatch1.SUB_PHN_NO").alias("SUB_PHN_NO"),
    col("nomatch1.MBR_MO_BASE_YR").alias("MBR_MO_BASE_YR"),
    col("nomatch1.PROSP_RISK_SCORE").alias("PROSP_RISK_SCORE"),
    col("nomatch1.ACTL_AMT_AGGD_DCG_CAT").alias("ACTL_AMT_AGGD_DCG_CAT"),
    col("nomatch1.ACTL_LOW_AMT").alias("ACTL_LOW_AMT"),
    col("nomatch1.ACTL_HI_AMT").alias("ACTL_HI_AMT"),
    col("nomatch1.AGGD_DCG_CAT_YR_2").alias("AGGD_DCG_CAT_YR_2"),
    col("nomatch1.XPCT_LOW_AMT_YR_2").alias("XPCT_LOW_AMT_YR_2"),
    col("nomatch1.XPCT_HI_AMT_YR_2").alias("XPCT_HI_AMT_YR_2"),
    col("nomatch1.MCARE_FLAG").alias("MCARE_FLAG"),
    col("nomatch1.HSPC_FLAG").alias("HSPC_FLAG"),
    col("nomatch1.DROP_DT").alias("DROP_DT"),
    col("nomatch1.EXTR_STTUS_CD").alias("EXTR_STTUS_CD"),
    col("nomatch1.ACTV_ANNUITANT_GRP_CD").alias("ACTV_ANNUITANT_GRP_CD"),
    col("nomatch1.MCARE_COV_CD").alias("MCARE_COV_CD"),
    col("nomatch1.MCARE_STTUS_CD").alias("MCARE_STTUS_CD"),
    col("nomatch1.OTHR_PRI_ENR_COV").alias("OTHR_PRI_ENR_COV")
)

# Step_3
df_step3_join = df_step2_nomatch2.alias("nomatch2").join(
    df_step3_lkup.alias("step3_lkup_in"),
    (
        (col("nomatch2.FEP_CNTR_ID") == col("step3_lkup_in.SUB_ID")) &
        (col("nomatch2.DOB") == col("step3_lkup_in.MBR_BRTH_DT_SK")) &
        (col("nomatch2.LAST_NM") == col("step3_lkup_in.MBR_LAST_NM")) &
        (col("nomatch2.GNDR_CD") == col("step3_lkup_in.MBR_GNDR_CD")) &
        (col("nomatch2.MBR_SSN") == col("step3_lkup_in.MBR_SSN"))
    ),
    "left"
)

df_step3_match3 = df_step3_join.filter(
    ~isnull(col("step3_lkup_in.SUB_ID")) &
    ~isnull(col("step3_lkup_in.MBR_BRTH_DT_SK")) &
    ~isnull(col("step3_lkup_in.MBR_LAST_NM"))
).select(
    col("step3_lkup_in.MBR_SK").alias("MBR_SK"),
    col("nomatch2.LGCY_MBR_ID").alias("LGCY_FEP_MBR_ID"),
    lit("0").alias("SRC_SYS_CD_SK"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("step3_lkup_in.FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

df_step3_nomatch3 = df_step3_join.filter(
    isnull(col("step3_lkup_in.SUB_ID")) &
    isnull(col("step3_lkup_in.MBR_BRTH_DT_SK")) &
    isnull(col("step3_lkup_in.MBR_LAST_NM"))
).select(
    col("nomatch2.ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
    col("nomatch2.ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
    col("nomatch2.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    col("nomatch2.FEP_CNTR_ID").alias("FEP_CNTR_ID"),
    col("nomatch2.LGCY_MBR_ID").alias("LGCY_MBR_ID"),
    col("nomatch2.CUR_PATN_CD").alias("CUR_PATN_CD"),
    col("nomatch2.RELSHP_CD").alias("RELSHP_CD"),
    col("nomatch2.ENR_OPT_CD").alias("ENR_OPT_CD"),
    col("nomatch2.LAST_NM").alias("LAST_NM"),
    col("nomatch2.FIRST_NM").alias("FIRST_NM"),
    col("nomatch2.MIDINIT").alias("MIDINIT"),
    col("nomatch2.GNDR_CD").alias("GNDR_CD"),
    col("nomatch2.DOB").alias("DOB"),
    col("nomatch2.ST_ADDR_1").alias("ST_ADDR_1"),
    col("nomatch2.ST_ADDR_2").alias("ST_ADDR_2"),
    col("nomatch2.ADDR_CITY").alias("ADDR_CITY"),
    col("nomatch2.ADDR_ST").alias("ADDR_ST"),
    col("nomatch2.ADDR_ZIP").alias("ADDR_ZIP"),
    col("nomatch2.SUB_PHN_NO").alias("SUB_PHN_NO"),
    col("nomatch2.MBR_MO_BASE_YR").alias("MBR_MO_BASE_YR"),
    col("nomatch2.PROSP_RISK_SCORE").alias("PROSP_RISK_SCORE"),
    col("nomatch2.ACTL_AMT_AGGD_DCG_CAT").alias("ACTL_AMT_AGGD_DCG_CAT"),
    col("nomatch2.ACTL_LOW_AMT").alias("ACTL_LOW_AMT"),
    col("nomatch2.ACTL_HI_AMT").alias("ACTL_HI_AMT"),
    col("nomatch2.AGGD_DCG_CAT_YR_2").alias("AGGD_DCG_CAT_YR_2"),
    col("nomatch2.XPCT_LOW_AMT_YR_2").alias("XPCT_LOW_AMT_YR_2"),
    col("nomatch2.XPCT_HI_AMT_YR_2").alias("XPCT_HI_AMT_YR_2"),
    col("nomatch2.MCARE_FLAG").alias("MCARE_FLAG"),
    col("nomatch2.HSPC_FLAG").alias("HSPC_FLAG"),
    col("nomatch2.DROP_DT").alias("DROP_DT"),
    col("nomatch2.EXTR_STTUS_CD").alias("EXTR_STTUS_CD"),
    col("nomatch2.ACTV_ANNUITANT_GRP_CD").alias("ACTV_ANNUITANT_GRP_CD"),
    col("nomatch2.MCARE_COV_CD").alias("MCARE_COV_CD"),
    col("nomatch2.MCARE_STTUS_CD").alias("MCARE_STTUS_CD"),
    col("nomatch2.OTHR_PRI_ENR_COV").alias("OTHR_PRI_ENR_COV")
)

# Step_4
df_step4_join = df_step3_nomatch3.alias("nomatch3").join(
    df_step4_lkup.alias("step4_lkup_in"),
    (
        (col("nomatch3.FEP_CNTR_ID") == col("step4_lkup_in.SUB_ID")) &
        (col("nomatch3.FIRST_NM") == col("step4_lkup_in.MBR_FIRST_NM")) &
        (col("nomatch3.LAST_NM") == col("step4_lkup_in.MBR_LAST_NM")) &
        (col("nomatch3.GNDR_CD") == col("step4_lkup_in.MBR_GNDR_CD"))
    ),
    "left"
)

df_step4_match4 = df_step4_join.filter(
    ~isnull(col("step4_lkup_in.SUB_ID")) &
    ~isnull(col("step4_lkup_in.MBR_FIRST_NM"))
).select(
    col("step4_lkup_in.MBR_SK").alias("MBR_SK"),
    col("nomatch3.LGCY_MBR_ID").alias("LGCY_FEP_MBR_ID"),
    lit("0").alias("SRC_SYS_CD_SK"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("step4_lkup_in.FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

df_step4_NotMatch = df_step4_join.filter(
    isnull(col("step4_lkup_in.SUB_ID")) &
    isnull(col("step4_lkup_in.MBR_FIRST_NM"))
).select(
    col("nomatch3.ALLOCD_CRS_PLN_CD").alias("ALLOCD_CRS_PLN_CD"),
    col("nomatch3.ALLOCD_SHIELD_PLN_CD").alias("ALLOCD_SHIELD_PLN_CD"),
    col("nomatch3.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    col("nomatch3.FEP_CNTR_ID").alias("FEP_CNTR_ID"),
    col("nomatch3.LGCY_MBR_ID").alias("LGCY_MBR_ID"),
    col("nomatch3.CUR_PATN_CD").alias("CUR_PATN_CD"),
    col("nomatch3.RELSHP_CD").alias("RELSHP_CD"),
    col("nomatch3.ENR_OPT_CD").alias("ENR_OPT_CD"),
    col("nomatch3.LAST_NM").alias("LAST_NM"),
    col("nomatch3.FIRST_NM").alias("FIRST_NM"),
    col("nomatch3.MIDINIT").alias("MIDINIT"),
    col("nomatch3.GNDR_CD").alias("GNDR_CD"),
    col("nomatch3.DOB").alias("DOB"),
    col("nomatch3.ST_ADDR_1").alias("ST_ADDR_1"),
    col("nomatch3.ST_ADDR_2").alias("ST_ADDR_2"),
    col("nomatch3.ADDR_CITY").alias("ADDR_CITY"),
    col("nomatch3.ADDR_ST").alias("ADDR_ST"),
    col("nomatch3.ADDR_ZIP").alias("ADDR_ZIP"),
    col("nomatch3.SUB_PHN_NO").alias("SUB_PHN_NO"),
    col("nomatch3.MBR_MO_BASE_YR").alias("MBR_MO_BASE_YR"),
    col("nomatch3.PROSP_RISK_SCORE").alias("PROSP_RISK_SCORE"),
    col("nomatch3.ACTL_AMT_AGGD_DCG_CAT").alias("ACTL_AMT_AGGD_DCG_CAT"),
    col("nomatch3.ACTL_LOW_AMT").alias("ACTL_LOW_AMT"),
    col("nomatch3.ACTL_HI_AMT").alias("ACTL_HI_AMT"),
    col("nomatch3.AGGD_DCG_CAT_YR_2").alias("AGGD_DCG_CAT_YR_2"),
    col("nomatch3.XPCT_LOW_AMT_YR_2").alias("XPCT_LOW_AMT_YR_2"),
    col("nomatch3.XPCT_HI_AMT_YR_2").alias("XPCT_HI_AMT_YR_2"),
    col("nomatch3.MCARE_FLAG").alias("MCARE_FLAG"),
    col("nomatch3.HSPC_FLAG").alias("HSPC_FLAG"),
    col("nomatch3.DROP_DT").alias("DROP_DT"),
    col("nomatch3.EXTR_STTUS_CD").alias("EXTR_STTUS_CD"),
    col("nomatch3.ACTV_ANNUITANT_GRP_CD").alias("ACTV_ANNUITANT_GRP_CD"),
    col("nomatch3.MCARE_COV_CD").alias("MCARE_COV_CD"),
    col("nomatch3.MCARE_STTUS_CD").alias("MCARE_STTUS_CD"),
    col("nomatch3.OTHR_PRI_ENR_COV").alias("OTHR_PRI_ENR_COV")
)

# FEP_MBR_ENR_Notmatch (CSeqFileStage)
write_files(
    df_step4_NotMatch,
    f"{adls_path_publish}/external/FepMbrEnr_NoMatch.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None
)

# Link_Collector: collect match1, match2, match3, match4 => union them
df_match_all = df_step1_match1.unionByName(df_step2_match2).unionByName(df_step3_match3).unionByName(df_step4_match4)

# FEP_MBR_INDICATOR
df_FEP_MBR_INDICATOR = df_match_all

df_FEP_MBR_IND_match = df_FEP_MBR_INDICATOR.filter(col("FEP_LOCAL_CNTR_IN") == lit("Y")).select(
    col("MBR_SK").alias("MBR_SK"),
    col("LGCY_FEP_MBR_ID").alias("LGCY_FEP_MBR_ID"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

df_FEP_MBR_IND_N = df_FEP_MBR_INDICATOR.filter(col("FEP_LOCAL_CNTR_IN") == lit("N")).select(
    col("MBR_SK").alias("MBR_SK"),
    col("LGCY_FEP_MBR_ID").alias("LGCY_FEP_MBR_ID"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("FEP_LOCAL_CNTR_IN").alias("FEP_LOCAL_CNTR_IN")
)

# FEP_MBR_IN_N (CSeqFileStage)
write_files(
    df_FEP_MBR_IND_N.select(
        "MBR_SK",
        "LGCY_FEP_MBR_ID",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FEP_LOCAL_CNTR_IN"
    ),
    f"{adls_path_publish}/external/FepMbrEnr_Indicator_N.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Scenario B for hf_bca_fep_mbr_enr:
# Read from the dummy table
jdbc_url_dummy, jdbc_props_dummy = get_db_config(<dummy_secret_name_for_scenarioB>)
df_hf_bca_fep_mbr_enr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dummy)
    .options(**jdbc_props_dummy)
    .option("query", "SELECT MBR_SK, LGCY_FEP_MBR_ID, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK FROM dummy_hf_bca_fep_mbr_enr")
    .load()
)

# RULES stage: primary link = df_FEP_MBR_IND_match, lookup link = df_hf_bca_fep_mbr_enr left join
df_RULES_join = df_FEP_MBR_IND_match.alias("match").join(
    df_hf_bca_fep_mbr_enr.alias("lkup"),
    (
        (col("match.MBR_SK") == col("lkup.MBR_SK")) &
        (col("match.LGCY_FEP_MBR_ID") == col("lkup.LGCY_FEP_MBR_ID")) &
        (col("match.SRC_SYS_CD_SK") == col("lkup.SRC_SYS_CD_SK"))
    ),
    "left"
).withColumn(
    "NewCrtRunCycExtcnSk",
    when(isnull(col("lkup.MBR_SK")), col("CrrrRunCycle")).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
)

df_RULES_dupe_in = df_RULES_join.select(
    col("match.MBR_SK").alias("MBR_SK"),
    col("match.LGCY_FEP_MBR_ID").alias("LGCY_FEP_MBR_ID"),
    lit(SourceSk).alias("SRC_SYS_CD_SK"),
    col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("CrrrRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lkup.MBR_SK").alias("__lkup_MBR_SK")
)

df_RULES_updt = df_RULES_dupe_in.filter(isnull(col("__lkup_MBR_SK"))).select(
    col("MBR_SK"),
    col("LGCY_FEP_MBR_ID"),
    col("SRC_SYS_CD_SK"),
    lit(CrrrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_RULES_dupe_in = df_RULES_dupe_in.filter(col("__lkup_MBR_SK").isNotNull() | col("__lkup_MBR_SK").isNull()).select(
    col("MBR_SK"),
    col("LGCY_FEP_MBR_ID"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Scenario B write to the dummy table from df_RULES_updt (MERGE)
# Create staging table: STAGING.BCAFepMbrEnrExtr_hf_bca_fep_mbr_enr_updt_temp
execute_dml(
    "DROP TABLE IF EXISTS STAGING.BCAFepMbrEnrExtr_hf_bca_fep_mbr_enr_updt_temp",
    jdbc_url_dummy,
    jdbc_props_dummy
)

df_RULES_updt.createOrReplaceTempView("tmp_hf_bca_fep_mbr_enr_updt_view")  # We won't physically create a table, but we must follow instructions for gathering data
# Instead, we write df_RULES_updt into that physical table via JDBC:
# We cannot use createOrReplaceTempView, but we must follow the "no function definitions" constraint.
# We'll do a direct overwrite. The instructions require a physical table. So:
df_RULES_updt.write.jdbc(
    url=jdbc_url_dummy,
    table="STAGING.BCAFepMbrEnrExtr_hf_bca_fep_mbr_enr_updt_temp",
    mode="overwrite",
    properties=jdbc_props_dummy
)

merge_sql_hf_bca = (
    "MERGE INTO dummy_hf_bca_fep_mbr_enr AS T "
    "USING STAGING.BCAFepMbrEnrExtr_hf_bca_fep_mbr_enr_updt_temp AS S "
    "ON T.MBR_SK=S.MBR_SK AND T.LGCY_FEP_MBR_ID=S.LGCY_FEP_MBR_ID AND T.SRC_SYS_CD_SK=S.SRC_SYS_CD_SK "
    "WHEN MATCHED THEN UPDATE SET T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK "
    "WHEN NOT MATCHED THEN INSERT (MBR_SK, LGCY_FEP_MBR_ID, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK) "
    "VALUES (S.MBR_SK, S.LGCY_FEP_MBR_ID, S.SRC_SYS_CD_SK, S.CRT_RUN_CYC_EXCTN_SK);"
)

execute_dml(merge_sql_hf_bca, jdbc_url_dummy, jdbc_props_dummy)

# Scenario A for hf_fep_mbr_enr_dupes => deduplicate with key = [MBR_SK]
df_hf_fep_mbr_enr_dupes_in = df_RULES_dupe_in
df_hf_fep_mbr_enr_dupes = dedup_sort(
    df_hf_fep_mbr_enr_dupes_in,
    partition_cols=["MBR_SK"],
    sort_cols=[]
)

# Then load to FEP_MBR_ENR (CSeqFileStage)
# The stage columns in the final order: MBR_SK, LGCY_FEP_MBR_ID, SRC_SYS_CD_SK, CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK
df_FEP_MBR_ENR = df_hf_fep_mbr_enr_dupes.select(
    "MBR_SK",
    "LGCY_FEP_MBR_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

write_files(
    df_FEP_MBR_ENR,
    f"{adls_path}/load/FEP_MBR_ENR.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)