# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY: CblTalnCchgLoadSeq
# MAGIC 
# MAGIC PROCESSING:  This job is for Fkey Lookups, Process the records from Pkey Job and look up for SKs against CD_MPPNG table. Output file is Load ready.
# MAGIC                                                                 
# MAGIC      
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                   DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                         DESCRIPTION                                                                                    ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam          2015-07-28         5460                               Initial Programming                                                                                IntegrateDev2          Bhoomi Dasari              8/30/2015
# MAGIC 
# MAGIC Manasa Andru           2017-02-04      TFS - 16054                 Corrected the field CCHG_SNGL_CAT_GRP_SK by correcting the          IntegrateDev1           Jag Yelavarthi                2017-02-08
# MAGIC                                                                                             mapping from the input stage with the right SK field in the transformer 
# MAGIC                                                                                                       stage which was previously mapped with the 
# MAGIC                                                                                                              CCHG_SNGL_CAT_GRP_CD field.
# MAGIC 
# MAGIC 
# MAGIC Goutham K               2022-07-19        US-541862                Updated Lookup SQL to Use SRC SYS CD                                                IntegrateDev2         Reddy Sanam              2022-07-20

# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

dsjobName = "IdsCchgMultCatGrpFkey"

# ----------------------------------------------------------------------------
# Stage: seqCCHG_MULT_CAT_GRP_Pkey (PxSequentialFile) - Read
# ----------------------------------------------------------------------------
df_seqCCHG_MULT_CAT_GRP_Pkey_schema = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", TimestampType(), True),
    StructField("CCHG_MULT_CAT_GRP_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_ID", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CCHG_SNGL_CAT_GRP_1", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_2", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_3", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_4", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_5", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_6", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_7", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_8", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_9", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_CD", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_DESC", StringType(), True),
    StructField("CCHG_SNGL_CAT_GRP_MPPNG_TX", StringType(), True)
])

df_seqCCHG_MULT_CAT_GRP_Pkey = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .schema(df_seqCCHG_MULT_CAT_GRP_Pkey_schema)
    .load(f"{adls_path}/key/CCHG_MULT_CAT_GRP.{SrcSysCd}.pkey.{RunID}.dat")
)

# ----------------------------------------------------------------------------
# Stage: db2_K_CCHG_SNGL_CAT_GRP (DB2ConnectorPX) - Read via JDBC
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CCHG_SNGL_CAT_GRP_CD, CCHG_SNGL_CAT_GRP_SK FROM {IDSOwner}.K_CCHG_SNGL_CAT_GRP WHERE SRC_SYS_CD = '{SrcSysCd}'"
df_db2_K_CCHG_SNGL_CAT_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Copy (PxCopy) - Replicate into multiple outputs
# ----------------------------------------------------------------------------
df_SnglCatGrp = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp1 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp2 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp3 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp4 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp5 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp6 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp7 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp8 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_SnglCatGrp9 = df_db2_K_CCHG_SNGL_CAT_GRP.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

# ----------------------------------------------------------------------------
# Stage: CdMppngLkup (PxLookup) - Multiple left joins
# ----------------------------------------------------------------------------
df_CdMppngLkup = (
    df_seqCCHG_MULT_CAT_GRP_Pkey.alias("In")
    .join(df_SnglCatGrp1.alias("SnglCatGrp1"),
          F.col("In.CCHG_SNGL_CAT_GRP_1") == F.col("SnglCatGrp1.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp2.alias("SnglCatGrp2"),
          F.col("In.CCHG_SNGL_CAT_GRP_2") == F.col("SnglCatGrp2.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp3.alias("SnglCatGrp3"),
          F.col("In.CCHG_SNGL_CAT_GRP_3") == F.col("SnglCatGrp3.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp4.alias("SnglCatGrp4"),
          F.col("In.CCHG_SNGL_CAT_GRP_4") == F.col("SnglCatGrp4.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp5.alias("SnglCatGrp5"),
          F.col("In.CCHG_SNGL_CAT_GRP_5") == F.col("SnglCatGrp5.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp6.alias("SnglCatGrp6"),
          F.col("In.CCHG_SNGL_CAT_GRP_6") == F.col("SnglCatGrp6.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp7.alias("SnglCatGrp7"),
          F.col("In.CCHG_SNGL_CAT_GRP_7") == F.col("SnglCatGrp7.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp8.alias("SnglCatGrp8"),
          F.col("In.CCHG_SNGL_CAT_GRP_8") == F.col("SnglCatGrp8.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp9.alias("SnglCatGrp9"),
          F.col("In.CCHG_SNGL_CAT_GRP_9") == F.col("SnglCatGrp9.CCHG_SNGL_CAT_GRP_CD"), "left")
    .join(df_SnglCatGrp.alias("SnglCatGrp"),
          F.col("In.CCHG_SNGL_CAT_GRP_CD") == F.col("SnglCatGrp.CCHG_SNGL_CAT_GRP_CD"), "left")
)

df_DSLink65 = df_CdMppngLkup.select(
    F.col("In.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("In.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("In.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    F.col("In.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("In.CCHG_SNGL_CAT_GRP_1").alias("CCHG_SNGL_CAT_GRP_1"),
    F.col("In.CCHG_SNGL_CAT_GRP_2").alias("CCHG_SNGL_CAT_GRP_2"),
    F.col("In.CCHG_SNGL_CAT_GRP_3").alias("CCHG_SNGL_CAT_GRP_3"),
    F.col("In.CCHG_SNGL_CAT_GRP_4").alias("CCHG_SNGL_CAT_GRP_4"),
    F.col("In.CCHG_SNGL_CAT_GRP_5").alias("CCHG_SNGL_CAT_GRP_5"),
    F.col("In.CCHG_SNGL_CAT_GRP_6").alias("CCHG_SNGL_CAT_GRP_6"),
    F.col("In.CCHG_SNGL_CAT_GRP_7").alias("CCHG_SNGL_CAT_GRP_7"),
    F.col("In.CCHG_SNGL_CAT_GRP_8").alias("CCHG_SNGL_CAT_GRP_8"),
    F.col("In.CCHG_SNGL_CAT_GRP_9").alias("CCHG_SNGL_CAT_GRP_9"),
    F.col("In.CCHG_MULT_CAT_GRP_DESC").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.col("In.CCHG_SNGL_CAT_GRP_MPPNG_TX").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX"),
    F.col("SnglCatGrp1.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_1"),
    F.col("SnglCatGrp2.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_2"),
    F.col("SnglCatGrp3.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_3"),
    F.col("SnglCatGrp4.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_4"),
    F.col("SnglCatGrp5.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_5"),
    F.col("SnglCatGrp6.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_6"),
    F.col("SnglCatGrp7.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_7"),
    F.col("SnglCatGrp8.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_8"),
    F.col("SnglCatGrp9.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK_9"),
    F.col("SnglCatGrp.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK"),
    F.col("In.CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD")
)

# ----------------------------------------------------------------------------
# Stage: xfm_CheckLkpResults (CTransformerStage)
# ----------------------------------------------------------------------------
df_xfm_CheckLkpResults_sv = (
    df_DSLink65
    .withColumn("svSnglCatGrp1FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_1").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrp2FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_2").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrp3FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_3").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrp4FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_4").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrp5FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_5").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrp6FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_6").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrp7FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_7").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrp8FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_8").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrp9FkeyFail",  F.when(F.col("CCHG_SNGL_CAT_GRP_SK_9").isNull(), 'Y').otherwise('N'))
    .withColumn("svSnglCatGrpFkeyFail",   F.when(F.col("CCHG_SNGL_CAT_GRP_SK").isNull(), 'Y').otherwise('N'))
)

# Prepare a row_number for constraints "((@INROWNUM - 1) * @NUMPARTITIONS + @PARTITIONNUM + 1) = 1"
w = Window.orderBy(F.lit(1))
df_xfm_CheckLkpResults_indexed = df_xfm_CheckLkpResults_sv.withColumn("_row_num", F.row_number().over(w))

# Lnk_Main (no constraint, all rows) + mapped columns
df_Lnk_Main = df_xfm_CheckLkpResults_indexed.select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_1") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_1"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_1") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_1").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_1")).alias("CCHG_SNGL_CAT_GRP_1_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_2") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_2"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_2") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_2").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_2")).alias("CCHG_SNGL_CAT_GRP_2_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_3") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_3"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_3") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_3").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_3")).alias("CCHG_SNGL_CAT_GRP_3_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_4") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_4"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_4") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_4").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_4")).alias("CCHG_SNGL_CAT_GRP_4_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_5") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_5"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_5") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_5").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_5")).alias("CCHG_SNGL_CAT_GRP_5_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_6") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_6"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_6") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_6").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_6")).alias("CCHG_SNGL_CAT_GRP_6_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_7") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_7"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_7") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_7").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_7")).alias("CCHG_SNGL_CAT_GRP_7_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_8") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_8"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_8") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_8").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_8")).alias("CCHG_SNGL_CAT_GRP_8_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_9") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_9"))) == 0),
        F.lit(1)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_9") == 'UNK',
        F.lit(0)
    ).when(
        F.col("CCHG_SNGL_CAT_GRP_SK_9").isNull(),
        F.lit(0)
    ).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK_9")).alias("CCHG_SNGL_CAT_GRP_9_SK"),
    F.when(
        (F.col("CCHG_SNGL_CAT_GRP_CD") == 'NA') | (F.length(trim(F.col("CCHG_SNGL_CAT_GRP_CD"))) == 0),
        F.lit(1)
    ).otherwise(
        F.when(F.col("CCHG_SNGL_CAT_GRP_CD") == 'UNK', F.lit(0))
         .otherwise(F.col("CCHG_SNGL_CAT_GRP_SK"))
    ).alias("CCHG_SNGL_CAT_GRP_SK"),
    F.col("CCHG_MULT_CAT_GRP_DESC").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.col("CCHG_SNGL_CAT_GRP_MPPNG_TX").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX")
)

# Lnk_NA (constraint: row_num == 1) + WhereExpressions
df_Lnk_NA = df_xfm_CheckLkpResults_indexed.filter(F.col("_row_num") == 1).select(
    F.lit(1).alias("CCHG_MULT_CAT_GRP_SK"),
    F.lit("NA").alias("CCHG_MULT_CAT_GRP_ID"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_1_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_2_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_3_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_4_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_5_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_6_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_7_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_8_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_9_SK"),
    F.lit(1).alias("CCHG_SNGL_CAT_GRP_SK"),
    F.lit("NA").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.lit("NA").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX")
)

# Lnk_UNK (constraint: row_num == 1) + WhereExpressions
df_Lnk_UNK = df_xfm_CheckLkpResults_indexed.filter(F.col("_row_num") == 1).select(
    F.lit(0).alias("CCHG_MULT_CAT_GRP_SK"),
    F.lit("UNK").alias("CCHG_MULT_CAT_GRP_ID"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_1_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_2_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_3_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_4_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_5_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_6_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_7_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_8_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_9_SK"),
    F.lit(0).alias("CCHG_SNGL_CAT_GRP_SK"),
    F.lit("UNK").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.lit("UNK").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX")
)

# SnglCatGrpX_LkupFail dataframes (constraints on stage variables):
df_SnglCatGrp1_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp1FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_1")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp2_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp2FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_2")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp3_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp3FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_3")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp4_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp4FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_4")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp5_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp5FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_5")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp6_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp6FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_6")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp7_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp7FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_7")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp8_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp8FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_8")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp9_LkupFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrp9FkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_9")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_SnglCatGrp_LkuPFail = df_xfm_CheckLkpResults_indexed.filter(F.col("svSnglCatGrpFkeyFail") == 'Y').select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(dsjobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_SNGL_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_SNGL_CAT_GRP_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ----------------------------------------------------------------------------
# Stage: fnl_NA_UNK_Streams (PxFunnel)
# ----------------------------------------------------------------------------
df_fnl_NA_UNK_Streams = df_Lnk_Main.select(
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CCHG_SNGL_CAT_GRP_1_SK").alias("CCHG_SNGL_CAT_GRP_1_SK"),
    F.col("CCHG_SNGL_CAT_GRP_2_SK").alias("CCHG_SNGL_CAT_GRP_2_SK"),
    F.col("CCHG_SNGL_CAT_GRP_3_SK").alias("CCHG_SNGL_CAT_GRP_3_SK"),
    F.col("CCHG_SNGL_CAT_GRP_4_SK").alias("CCHG_SNGL_CAT_GRP_4_SK"),
    F.col("CCHG_SNGL_CAT_GRP_5_SK").alias("CCHG_SNGL_CAT_GRP_5_SK"),
    F.col("CCHG_SNGL_CAT_GRP_6_SK").alias("CCHG_SNGL_CAT_GRP_6_SK"),
    F.col("CCHG_SNGL_CAT_GRP_7_SK").alias("CCHG_SNGL_CAT_GRP_7_SK"),
    F.col("CCHG_SNGL_CAT_GRP_8_SK").alias("CCHG_SNGL_CAT_GRP_8_SK"),
    F.col("CCHG_SNGL_CAT_GRP_9_SK").alias("CCHG_SNGL_CAT_GRP_9_SK"),
    F.col("CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK"),
    F.col("CCHG_MULT_CAT_GRP_DESC").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.col("CCHG_SNGL_CAT_GRP_MPPNG_TX").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX")
).unionByName(
    df_Lnk_NA.select(
        F.col("CCHG_MULT_CAT_GRP_SK"), F.col("CCHG_MULT_CAT_GRP_ID"), F.col("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK"), F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"), F.col("CCHG_SNGL_CAT_GRP_1_SK"),
        F.col("CCHG_SNGL_CAT_GRP_2_SK"), F.col("CCHG_SNGL_CAT_GRP_3_SK"), F.col("CCHG_SNGL_CAT_GRP_4_SK"),
        F.col("CCHG_SNGL_CAT_GRP_5_SK"), F.col("CCHG_SNGL_CAT_GRP_6_SK"), F.col("CCHG_SNGL_CAT_GRP_7_SK"),
        F.col("CCHG_SNGL_CAT_GRP_8_SK"), F.col("CCHG_SNGL_CAT_GRP_9_SK"), F.col("CCHG_SNGL_CAT_GRP_SK"),
        F.col("CCHG_MULT_CAT_GRP_DESC"), F.col("CCHG_SNGL_CAT_GRP_MPPNG_TX")
    )
).unionByName(
    df_Lnk_UNK.select(
        F.col("CCHG_MULT_CAT_GRP_SK"), F.col("CCHG_MULT_CAT_GRP_ID"), F.col("SRC_SYS_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK"), F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"), F.col("CCHG_SNGL_CAT_GRP_1_SK"),
        F.col("CCHG_SNGL_CAT_GRP_2_SK"), F.col("CCHG_SNGL_CAT_GRP_3_SK"), F.col("CCHG_SNGL_CAT_GRP_4_SK"),
        F.col("CCHG_SNGL_CAT_GRP_5_SK"), F.col("CCHG_SNGL_CAT_GRP_6_SK"), F.col("CCHG_SNGL_CAT_GRP_7_SK"),
        F.col("CCHG_SNGL_CAT_GRP_8_SK"), F.col("CCHG_SNGL_CAT_GRP_9_SK"), F.col("CCHG_SNGL_CAT_GRP_SK"),
        F.col("CCHG_MULT_CAT_GRP_DESC"), F.col("CCHG_SNGL_CAT_GRP_MPPNG_TX")
    )
)

# ----------------------------------------------------------------------------
# Stage: seq_CCHG_MULT_CAT_GRP_Fkey (PxSequentialFile) - Write
# ----------------------------------------------------------------------------
# rpad for varchar columns (CCHG_MULT_CAT_GRP_ID, CCHG_MULT_CAT_GRP_DESC, CCHG_SNGL_CAT_GRP_MPPNG_TX)
df_seq_CCHG_MULT_CAT_GRP_Fkey_rpad = (
    df_fnl_NA_UNK_Streams
    .withColumn("CCHG_MULT_CAT_GRP_ID", F.rpad(F.col("CCHG_MULT_CAT_GRP_ID"), <...>, ' '))
    .withColumn("CCHG_MULT_CAT_GRP_DESC", F.rpad(F.col("CCHG_MULT_CAT_GRP_DESC"), <...>, ' '))
    .withColumn("CCHG_SNGL_CAT_GRP_MPPNG_TX", F.rpad(F.col("CCHG_SNGL_CAT_GRP_MPPNG_TX"), <...>, ' '))
)

df_seq_CCHG_MULT_CAT_GRP_Fkey_final = df_seq_CCHG_MULT_CAT_GRP_Fkey_rpad.select(
    "CCHG_MULT_CAT_GRP_SK",
    "CCHG_MULT_CAT_GRP_ID",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CCHG_SNGL_CAT_GRP_1_SK",
    "CCHG_SNGL_CAT_GRP_2_SK",
    "CCHG_SNGL_CAT_GRP_3_SK",
    "CCHG_SNGL_CAT_GRP_4_SK",
    "CCHG_SNGL_CAT_GRP_5_SK",
    "CCHG_SNGL_CAT_GRP_6_SK",
    "CCHG_SNGL_CAT_GRP_7_SK",
    "CCHG_SNGL_CAT_GRP_8_SK",
    "CCHG_SNGL_CAT_GRP_9_SK",
    "CCHG_SNGL_CAT_GRP_SK",
    "CCHG_MULT_CAT_GRP_DESC",
    "CCHG_SNGL_CAT_GRP_MPPNG_TX"
)

write_files(
    df_seq_CCHG_MULT_CAT_GRP_Fkey_final,
    f"{adls_path}/load/CCHG_MULT_CAT_GRP.{SrcSysCd}.{RunID}.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)

# ----------------------------------------------------------------------------
# Stage: FkeyErrors (PxFunnel)
# ----------------------------------------------------------------------------
df_FkeyErrors = df_SnglCatGrp1_LkupFail.unionByName(df_SnglCatGrp2_LkupFail) \
    .unionByName(df_SnglCatGrp3_LkupFail) \
    .unionByName(df_SnglCatGrp4_LkupFail) \
    .unionByName(df_SnglCatGrp5_LkupFail) \
    .unionByName(df_SnglCatGrp6_LkupFail) \
    .unionByName(df_SnglCatGrp7_LkupFail) \
    .unionByName(df_SnglCatGrp8_LkupFail) \
    .unionByName(df_SnglCatGrp9_LkupFail) \
    .unionByName(df_SnglCatGrp_LkuPFail)

# ----------------------------------------------------------------------------
# Stage: seq_FkeyFailedFile_csv (PxSequentialFile) - Write
# ----------------------------------------------------------------------------
# Columns in final order:
df_seq_FkeyFailedFile_csv_rpad = (
    df_FkeyErrors
    .withColumn("PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, ' '))
    .withColumn("JOB_NM", F.rpad(F.col("JOB_NM"), <...>, ' '))
    .withColumn("ERROR_TYP", F.rpad(F.col("ERROR_TYP"), <...>, ' '))
    .withColumn("PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), <...>, ' '))
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, ' '))
)

df_seq_FkeyFailedFile_csv_final = df_seq_FkeyFailedFile_csv_rpad.select(
    "PRI_SK",
    "PRI_NAT_KEY_STRING",
    "SRC_SYS_CD_SK",
    "JOB_NM",
    "ERROR_TYP",
    "PHYSCL_FILE_NM",
    "FRGN_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "JOB_EXCTN_SK"
)

write_files(
    df_seq_FkeyFailedFile_csv_final,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{dsjobName}.dat",
    ",",
    "overwrite",
    False,
    False,
    "^",
    None
)