# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC MODIFICATIONS:
# MAGIC ========================================================================================================================================
# MAGIC 												DATASTAGE	CODE		DATE OF
# MAGIC DEVELOPER	DATE		PROJECT	DESCRIPTION					ENVIRONMENT	REVIEWER	REVIEW
# MAGIC ========================================================================================================================================
# MAGIC Abhiram Dasarathy	2016-11-29	5217 - Dental	Initial Progamming - Dental Reward Accumulators		IntegrateDev2         Kalyan Neelam        2016-12-02

# MAGIC Perform Foreign Key Lookups to populate MBR_DNTL_RWRD_ACCUM table.
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC This is a load ready file that will go into Load job
# MAGIC FKEY failures are written into this flat file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
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
    TimestampType,
    DateType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

SrcSysCd = get_widget_value('SrcSysCd','FACETS')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','100')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','FKeysFailure.FctsIdsMbrDntlRwrdAccumCntl.100')
RunCycle = get_widget_value('RunCycle','100')

# Schema for seq_MBR_DNTL_RWRD_ACCUM (PxSequentialFile)
schema_seq_MBR_DNTL_RWRD_ACCUM = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYCLE_TS", TimestampType(), True),
    StructField("MBR_DNTL_RWRD_ACCUM_SK", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("YR_NO", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("PLN_YR_EFF_DT", DateType(), True),
    StructField("PLN_YR_END_DT", DateType(), True),
    StructField("ANUL_PD_AMT", DecimalType(38,10), True),
    StructField("ANUL_PD_RWRD_THRSHLD_AMT", DecimalType(38,10), True),
    StructField("AVLBL_RWRD_AMT", DecimalType(38,10), True),
    StructField("AVLBL_BNS_AMT", DecimalType(38,10), True),
    StructField("USE_RWRD_AMT", DecimalType(38,10), True),
    StructField("USE_BNS_AMT", DecimalType(38,10), True),
    StructField("OUT_OF_NTWK_CLM_CT", IntegerType(), True)
])

df_seq_MBR_DNTL_RWRD_ACCUM = (
    spark.read
    .schema(schema_seq_MBR_DNTL_RWRD_ACCUM)
    .option("sep", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .csv(f"{adls_path}/key/MBR_DNTL_RWRD_ACCUM.{SrcSysCd}.pkey.{RunID}.dat")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_MBR_Lkp = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", f"SELECT MBR_UNIQ_KEY, MBR_SK FROM {IDSOwner}.MBR")
    .load()
)

jdbc_url_ids2, jdbc_props_ids2 = get_db_config(ids_secret_name)
df_db2_GRP_Lkp = (
    spark.read
    .format("jdbc")
    .option("url", jdbc_url_ids2)
    .options(**jdbc_props_ids2)
    .option("query", f"SELECT GRP_ID, GRP_SK FROM {IDSOwner}.GRP")
    .load()
)

df_lkp_Code_SKs = (
    df_seq_MBR_DNTL_RWRD_ACCUM.alias("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc")
    .join(
        df_db2_MBR_Lkp.alias("Ref_MBR_In"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.MBR_UNIQ_KEY") == F.col("Ref_MBR_In.MBR_UNIQ_KEY"),
        "left"
    )
    .join(
        df_db2_GRP_Lkp.alias("Ref_Grp_In"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.GRP_ID") == F.col("Ref_Grp_In.GRP_ID"),
        "left"
    )
    .select(
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.FIRST_RECYCLE_TS").alias("FIRST_RECYCLE_TS"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.MBR_DNTL_RWRD_ACCUM_SK").alias("MBR_DNTL_RWRD_ACCUM_SK"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.YR_NO").alias("YR_NO"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Ref_Grp_In.GRP_SK").alias("GRP_SK"),
        F.col("Ref_MBR_In.MBR_SK").alias("MBR_SK"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.PLN_YR_END_DT").alias("PLN_YR_END_DT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.ANUL_PD_AMT").alias("ANUL_PD_AMT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.ANUL_PD_RWRD_THRSHLD_AMT").alias("ANUL_PD_RWRD_THRSHLD_AMT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.AVLBL_RWRD_AMT").alias("AVLBL_RWRD_AMT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.AVLBL_BNS_AMT").alias("AVLBL_BNS_AMT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.USE_RWRD_AMT").alias("USE_RWRD_AMT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.USE_BNS_AMT").alias("USE_BNS_AMT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.OUT_OF_NTWK_CLM_CT").alias("OUT_OF_NTWK_CLM_CT"),
        F.col("Lnk_IdsBcbsaFepMbrEnrFkey_InAbc.GRP_ID").alias("GRP_ID")
    )
)

df_xfm_CheckLkpResults_intermediate = (
    df_lkp_Code_SKs
    .withColumn(
        "SvMbrFKeyLkpCheck",
        F.when(F.col("MBR_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvGrpFKeyLkpCheck",
        F.when((F.col("GRP_SK").isNull()) & (F.col("GRP_ID") != 'NA'), F.lit("Y")).otherwise(F.lit("N"))
    )
)

# Lnk_MbrDntlRwrdAccum_Fkey_Main
df_main = df_xfm_CheckLkpResults_intermediate.select(
    F.col("MBR_DNTL_RWRD_ACCUM_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("YR_NO"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("GRP_ID")=='NA', F.lit(1))
     .when(F.col("GRP_ID")=='UNK', F.lit(0))
     .when(F.col("GRP_SK").isNull(), F.lit(0))
     .otherwise(F.col("GRP_SK")).alias("GRP_SK"),
    F.when(F.col("MBR_UNIQ_KEY")=='NA', F.lit(1))
     .when(F.col("MBR_UNIQ_KEY")=='UNK', F.lit(0))
     .when(F.col("MBR_SK").isNull(), F.lit(0))
     .otherwise(F.col("MBR_SK")).alias("MBR_SK"),
    F.col("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT"),
    F.col("ANUL_PD_AMT"),
    F.col("ANUL_PD_RWRD_THRSHLD_AMT"),
    F.col("AVLBL_RWRD_AMT"),
    F.col("AVLBL_BNS_AMT"),
    F.col("USE_RWRD_AMT"),
    F.col("USE_BNS_AMT"),
    F.col("OUT_OF_NTWK_CLM_CT")
)

# Lnk_MbrDntlRwrdAccum_UNK (first row only, override with literals)
df_unk = (
    df_xfm_CheckLkpResults_intermediate.limit(1)
    .select(
        F.lit(0).alias("MBR_DNTL_RWRD_ACCUM_SK"),
        F.lit(0).alias("MBR_UNIQ_KEY"),
        F.lit('1753').alias("YR_NO"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("GRP_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit("1753-01-01").alias("PLN_YR_EFF_DT"),
        F.lit("1753-01-01").alias("PLN_YR_END_DT"),
        F.lit("0.00").alias("ANUL_PD_AMT"),
        F.lit("0.00").alias("ANUL_PD_RWRD_THRSHLD_AMT"),
        F.lit("0.00").alias("AVLBL_RWRD_AMT"),
        F.lit("0.00").alias("AVLBL_BNS_AMT"),
        F.lit("0.00").alias("USE_RWRD_AMT"),
        F.lit("0.00").alias("USE_BNS_AMT"),
        F.lit("0.00").alias("OUT_OF_NTWK_CLM_CT")
    )
)

# Lnk_MbrDntlRwrdAccum_NA (first row only, override with literals)
df_na = (
    df_xfm_CheckLkpResults_intermediate.limit(1)
    .select(
        F.lit(1).alias("MBR_DNTL_RWRD_ACCUM_SK"),
        F.lit(1).alias("MBR_UNIQ_KEY"),
        F.lit('1753').alias("YR_NO"),
        F.lit(1).alias("SRC_SYS_CD_SK"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("GRP_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit("1753-01-01").alias("PLN_YR_EFF_DT"),
        F.lit("1753-01-01").alias("PLN_YR_END_DT"),
        F.lit("0.00").alias("ANUL_PD_AMT"),
        F.lit("0.00").alias("ANUL_PD_RWRD_THRSHLD_AMT"),
        F.lit("0.00").alias("AVLBL_RWRD_AMT"),
        F.lit("0.00").alias("AVLBL_BNS_AMT"),
        F.lit("0.00").alias("USE_RWRD_AMT"),
        F.lit("0.00").alias("USE_BNS_AMT"),
        F.lit("0.00").alias("OUT_OF_NTWK_CLM_CT")
    )
)

# Lnk_Mbr_Fail (SvMbrFKeyLkpCheck = 'Y')
df_mbr_fail = (
    df_xfm_CheckLkpResults_intermediate
    .filter(F.col("SvMbrFKeyLkpCheck") == 'Y')
    .select(
        F.col("MBR_DNTL_RWRD_ACCUM_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsMbrDntlRwrdAccumFkey").alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("MBR").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("MBR_UNIQ_KEY")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYCLE_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Lnk_Grp_Fail (SvGrpFKeyLkpCheck = 'Y')
df_grp_fail = (
    df_xfm_CheckLkpResults_intermediate
    .filter(F.col("SvGrpFKeyLkpCheck") == 'Y')
    .select(
        F.col("MBR_DNTL_RWRD_ACCUM_SK").alias("PRI_SK"),
        F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.lit("IdsMbrDntlRwrdAccumFkey").alias("JOB_NM"),
        F.lit("FKLOOKUP").alias("ERROR_TYP"),
        F.lit("GRP").alias("PHYSCL_FILE_NM"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("GRP_ID")).alias("FRGN_NAT_KEY_STRING"),
        F.col("FIRST_RECYCLE_TS").alias("FIRST_RECYC_TS"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
    )
)

# Funnel fnl_NA_UNK_Streams (union main, unk, na)
df_fnl_NA_UNK_Streams = (
    df_main.unionByName(df_unk)
    .unionByName(df_na)
)

# Apply rpad for any char columns in the final output (YR_NO is char(4))
df_fnl_NA_UNK_Streams = df_fnl_NA_UNK_Streams.withColumn(
    "YR_NO", F.rpad(F.col("YR_NO"), 4, " ")
).select(
    "MBR_DNTL_RWRD_ACCUM_SK",
    "MBR_UNIQ_KEY",
    "YR_NO",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP_SK",
    "MBR_SK",
    "PLN_YR_EFF_DT",
    "PLN_YR_END_DT",
    "ANUL_PD_AMT",
    "ANUL_PD_RWRD_THRSHLD_AMT",
    "AVLBL_RWRD_AMT",
    "AVLBL_BNS_AMT",
    "USE_RWRD_AMT",
    "USE_BNS_AMT",
    "OUT_OF_NTWK_CLM_CT"
)

write_files(
    df_fnl_NA_UNK_Streams,
    f"{adls_path}/load/MBR_DNTL_RWRD_ACCUM.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)

# Funnel FnlFkeyFailures (union mbr_fail, grp_fail)
df_fnlFkeyFailures = (
    df_mbr_fail.unionByName(df_grp_fail)
)

# For seq_FkeyFailedFile output, rpad unknown-length varchar columns
df_fnlFkeyFailures = (
    df_fnlFkeyFailures
    .withColumn("PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " "))
    .withColumn("JOB_NM", F.rpad(F.col("JOB_NM"), <...>, " "))
    .withColumn("ERROR_TYP", F.rpad(F.col("ERROR_TYP"), <...>, " "))
    .withColumn("PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), <...>, " "))
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, " "))
    .select(
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
)

write_files(
    df_fnlFkeyFailures,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.IdsMbrDntlRwrdAccumFkey.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)