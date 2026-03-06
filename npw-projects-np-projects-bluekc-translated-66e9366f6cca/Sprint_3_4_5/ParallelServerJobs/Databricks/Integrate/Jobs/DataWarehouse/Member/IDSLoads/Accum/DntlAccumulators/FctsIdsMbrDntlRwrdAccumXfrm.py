# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC         
# MAGIC JOB NAME:  FctsIdsMbrDntlRwrdAccumExtr
# MAGIC PROCESSING: Transform job for MBR_DNTL_RWRD_ACCUM table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ==============================================================================================================================================================
# MAGIC 												DATASTAGE	CODE		DATE OF
# MAGIC DEVELOPER	DATE		PROJECT		DESCRIPTION					ENVIRONMENT	REVIEWER	REVIEW
# MAGIC ==============================================================================================================================================================
# MAGIC Abhiram Dasarathy	2016-11-29	5217 - Dental	Initial Progamming - Dental Reward Accumulators		IntegrateDev2          	Kalyan Neelam      	2016-12-02 
# MAGIC Prabhu ES	2022-05-31	S2S us527968	Sybase to SQL Remediation				IntegrateDev5	Ken Bradmon	2022-06-02

# MAGIC Transformation rules are applied on the data Extracted from Facets
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, DecimalType, StringType, IntegerType
from pyspark.sql.functions import col, lit, when, length, concat, substring, expr, stack, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
RunDate = get_widget_value('RunDate','')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1581')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')

# DB2_Connector_18
extract_query = f"""
SELECT DISTINCT
  MBR.MBR_UNIQ_KEY,
  DTL.PLN_BEG_DT_MO_DAY,
  GRP.GRP_ID
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.MBR_ENR ENR,
     {IDSOwner}.CLS_PLN_DTL DTL,
     {IDSOwner}.GRP GRP
WHERE MBR.MBR_SK = ENR.MBR_SK
  AND ENR.GRP_SK = DTL.GRP_SK
  AND ENR.GRP_SK = GRP.GRP_SK
  AND ENR.CLS_SK = DTL.CLS_SK
  AND ENR.CLS_PLN_SK = DTL.CLS_PLN_SK
  AND DTL.EFF_DT_SK <= '{RunDate}'
  AND DTL.TERM_DT_SK >= '{RunDate}'
  AND ENR.ELIG_IN = 'Y'
"""
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_DB2_Connector_18 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ds_MBR_DNTL_RWRD_ACCUM_Extr (read .ds as parquet)
schema_ds_MBR_DNTL_RWRD_ACCUM_Extr = StructType([
    StructField("MEME_CK", IntegerType(), True),
    StructField("MERW_CUR_YEAR", StringType(), True),
    StructField("MERW_YR1_AMT", DecimalType(19,4), True),
    StructField("MERW_YR1_THRES_AMT", DecimalType(19,4), True),
    StructField("MERW_YR1_RWDCO_AMT", DecimalType(19,4), True),
    StructField("MERW_YR1_BONCO_AMT", DecimalType(19,4), True),
    StructField("MERW_YR1_RWD_AMT", DecimalType(19,4), True),
    StructField("MERW_YR1_BONUS_AMT", DecimalType(19,4), True),
    StructField("MERW_YR1_OON_COUNT", DecimalType(19,4), True),
    StructField("MERW_YR2_AMT", DecimalType(19,4), True),
    StructField("MERW_YR2_THRES_AMT", DecimalType(19,4), True),
    StructField("MERW_YR2_RWDCO_AMT", DecimalType(19,4), True),
    StructField("MERW_YR2_BONCO_AMT", DecimalType(19,4), True),
    StructField("MERW_YR2_RWD_AMT", DecimalType(19,4), True),
    StructField("MERW_YR2_BONUS_AMT", DecimalType(19,4), True),
    StructField("MERW_YR2_OON_COUNT", DecimalType(19,4), True),
    StructField("MERW_YR3_AMT", DecimalType(19,4), True),
    StructField("MERW_YR3_THRES_AMT", DecimalType(19,4), True),
    StructField("MERW_YR3_RWDCO_AMT", DecimalType(19,4), True),
    StructField("MERW_YR3_BONCO_AMT", DecimalType(19,4), True),
    StructField("MERW_YR3_RWD_AMT", DecimalType(19,4), True),
    StructField("MERW_YR3_BONUS_AMT", DecimalType(19,4), True),
    StructField("MERW_YR3_OON_COUNT", DecimalType(19,4), True),
    StructField("MERW_LOCK_TOKEN", StringType(), True),
    StructField("ATXR_SOURCE_ID", StringType(), True)
])
df_ds_MBR_DNTL_RWRD_ACCUM_Extr = spark.read.schema(schema_ds_MBR_DNTL_RWRD_ACCUM_Extr).parquet(
    f"{adls_path}/ds/MBR_DNTL_RWRD_ACCUM.extr.{RunID}.parquet"
)

# Transformer_30
df_Transformer_30 = (
    df_ds_MBR_DNTL_RWRD_ACCUM_Extr
    .withColumn("svCurrYr", trim(col("MERW_CUR_YEAR")).substr(1,4))
    .select(
        col("MEME_CK").alias("MEME_CK"),
        col("svCurrYr").alias("MERW_CUR_YEAR"),
        (col("svCurrYr").cast("int") - lit(1)).cast("string").alias("MERW_CUR_YEAR_1"),
        (col("svCurrYr").cast("int") - lit(2)).cast("string").alias("MERW_CUR_YEAR_2"),
        col("MERW_YR1_AMT").alias("MERW_YR1_AMT"),
        col("MERW_YR1_THRES_AMT").alias("MERW_YR1_THRES_AMT"),
        col("MERW_YR1_RWDCO_AMT").alias("MERW_YR1_RWDCO_AMT"),
        col("MERW_YR1_BONCO_AMT").alias("MERW_YR1_BONCO_AMT"),
        col("MERW_YR1_RWD_AMT").alias("MERW_YR1_RWD_AMT"),
        col("MERW_YR1_BONUS_AMT").alias("MERW_YR1_BONUS_AMT"),
        col("MERW_YR1_OON_COUNT").alias("MERW_YR1_OON_COUNT"),
        col("MERW_YR2_AMT").alias("MERW_YR2_AMT"),
        col("MERW_YR2_THRES_AMT").alias("MERW_YR2_THRES_AMT"),
        col("MERW_YR2_RWDCO_AMT").alias("MERW_YR2_RWDCO_AMT"),
        col("MERW_YR2_BONCO_AMT").alias("MERW_YR2_BONCO_AMT"),
        col("MERW_YR2_RWD_AMT").alias("MERW_YR2_RWD_AMT"),
        col("MERW_YR2_BONUS_AMT").alias("MERW_YR2_BONUS_AMT"),
        col("MERW_YR2_OON_COUNT").alias("MERW_YR2_OON_COUNT"),
        col("MERW_YR3_AMT").alias("MERW_YR3_AMT"),
        col("MERW_YR3_THRES_AMT").alias("MERW_YR3_THRES_AMT"),
        col("MERW_YR3_RWDCO_AMT").alias("MERW_YR3_RWDCO_AMT"),
        col("MERW_YR3_BONCO_AMT").alias("MERW_YR3_BONCO_AMT"),
        col("MERW_YR3_RWD_AMT").alias("MERW_YR3_RWD_AMT"),
        col("MERW_YR3_BONUS_AMT").alias("MERW_YR3_BONUS_AMT"),
        col("MERW_YR3_OON_COUNT").alias("MERW_YR3_OON_COUNT"),
        col("MERW_LOCK_TOKEN").alias("MERW_LOCK_TOKEN"),
        col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID")
    )
)

# Pivot
df_Pivot = df_Transformer_30.selectExpr(
    "MEME_CK as MBR_UNIQ_KEY",
    """stack(
      3,
      MERW_CUR_YEAR, MERW_YR1_AMT, MERW_YR1_THRES_AMT, MERW_YR1_RWDCO_AMT, MERW_YR1_BONCO_AMT, MERW_YR1_RWD_AMT, MERW_YR1_BONUS_AMT, MERW_YR1_OON_COUNT,
      MERW_CUR_YEAR_1, MERW_YR2_AMT, MERW_YR2_THRES_AMT, MERW_YR2_RWDCO_AMT, MERW_YR2_BONCO_AMT, MERW_YR2_RWD_AMT, MERW_YR2_BONUS_AMT, MERW_YR2_OON_COUNT,
      MERW_CUR_YEAR_2, MERW_YR3_AMT, MERW_YR3_THRES_AMT, MERW_YR3_RWDCO_AMT, MERW_YR3_BONCO_AMT, MERW_YR3_RWD_AMT, MERW_YR3_BONUS_AMT, MERW_YR3_OON_COUNT
    ) as (
      YR_NO,
      ANUL_PD_AMT,
      ANUL_PD_RWRD_THRSHLD_AMT,
      AVLBL_RWRD_AMT,
      AVLBL_BNS_AMT,
      USE_RWRD_AMT,
      USE_BNS_AMT,
      OUT_OF_NTWK_CLM_CT
    )"""
)

# Lookup_6 (inner join on MBR_UNIQ_KEY)
df_Lookup_6 = (
    df_Pivot.alias("DSLink5")
    .join(
        df_DB2_Connector_18.alias("DSLink13"),
        col("DSLink5.MBR_UNIQ_KEY") == col("DSLink13.MBR_UNIQ_KEY"),
        "inner"
    )
    .select(
        col("DSLink5.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("DSLink5.YR_NO").alias("YR_NO"),
        col("DSLink5.ANUL_PD_AMT").alias("ANUL_PD_AMT"),
        col("DSLink5.ANUL_PD_RWRD_THRSHLD_AMT").alias("ANUL_PD_RWRD_THRSHLD_AMT"),
        col("DSLink5.AVLBL_RWRD_AMT").alias("AVLBL_RWRD_AMT"),
        col("DSLink5.AVLBL_BNS_AMT").alias("AVLBL_BNS_AMT"),
        col("DSLink5.USE_RWRD_AMT").alias("USE_RWRD_AMT"),
        col("DSLink5.USE_BNS_AMT").alias("USE_BNS_AMT"),
        col("DSLink5.OUT_OF_NTWK_CLM_CT").alias("OUT_OF_NTWK_CLM_CT"),
        col("DSLink13.GRP_ID").alias("GRP_ID"),
        col("DSLink13.PLN_BEG_DT_MO_DAY").alias("PLN_BEG_DT_MO_DAY")
    )
)

# Transformer_21
df_Transformer_21 = (
    df_Lookup_6
    .withColumn(
        "svDt",
        when(length(col("PLN_BEG_DT_MO_DAY")) == 3, concat(lit("0"), col("PLN_BEG_DT_MO_DAY"))).otherwise(col("PLN_BEG_DT_MO_DAY"))
    )
    .withColumn("svFrmtDt", concat(col("YR_NO"), col("svDt")))
    .withColumn("svEffDt", FORMAT.DATE.EE(col("svFrmtDt"), "CHAR", "CCYYMMDD", "CCYY-MM-DD"))
)

df_Transformer_21_output = df_Transformer_21.select(
    concat(col("MBR_UNIQ_KEY"), lit(";"), col("YR_NO"), lit(";"), lit(SrcSysCd)).alias("PRI_NAT_KEY_STRING"),
    lit(RunIDTimeStamp).alias("FIRST_RECYCLE_TS"),
    lit(0).alias("MBR_DNTL_RWRD_ACCUM_SK"),
    col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("YR_NO").alias("YR_NO"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("svEffDt").alias("PLN_YR_EFF_DT"),
    FIND.DATE.EE(col("svEffDt"), lit(11), lit("M"), lit("L"), lit("CCYY-MM-DD")).alias("PLN_YR_END_DT"),
    col("ANUL_PD_AMT").alias("ANUL_PD_AMT"),
    col("ANUL_PD_RWRD_THRSHLD_AMT").alias("ANUL_PD_RWRD_THRSHLD_AMT"),
    col("AVLBL_RWRD_AMT").alias("AVLBL_RWRD_AMT"),
    col("AVLBL_BNS_AMT").alias("AVLBL_BNS_AMT"),
    col("USE_RWRD_AMT").alias("USE_RWRD_AMT"),
    col("USE_BNS_AMT").alias("USE_BNS_AMT"),
    col("OUT_OF_NTWK_CLM_CT").alias("OUT_OF_NTWK_CLM_CT")
)

# ds_MBR_DNTL_RWRD_ACCUM_Xfrm (write .ds as parquet)
df_ds_MBR_DNTL_RWRD_ACCUM_Xfrm = df_Transformer_21_output.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYCLE_TS"),
    col("MBR_DNTL_RWRD_ACCUM_SK"),
    col("MBR_UNIQ_KEY"),
    rpad(col("YR_NO"),4," ").alias("YR_NO"),
    col("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("GRP_ID"),
    col("PLN_YR_EFF_DT"),
    col("PLN_YR_END_DT"),
    col("ANUL_PD_AMT"),
    col("ANUL_PD_RWRD_THRSHLD_AMT"),
    col("AVLBL_RWRD_AMT"),
    col("AVLBL_BNS_AMT"),
    col("USE_RWRD_AMT"),
    col("USE_BNS_AMT"),
    col("OUT_OF_NTWK_CLM_CT")
)

write_files(
    df_ds_MBR_DNTL_RWRD_ACCUM_Xfrm,
    f"MBR_DNTL_RWRD_ACCUM.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)