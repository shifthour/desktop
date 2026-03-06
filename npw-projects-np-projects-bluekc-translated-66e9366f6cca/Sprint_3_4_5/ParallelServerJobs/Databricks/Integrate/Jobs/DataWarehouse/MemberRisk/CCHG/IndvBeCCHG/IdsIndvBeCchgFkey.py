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
# MAGIC Kalyan Neelam          2015-07-28         5460                               Initial Programming                                                                                IntegrateDev2            Bhoomi Dasari              8/30/2015

# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

DSJobName = "IdsIndvBeCchgFkey"

#--------------------------------------------------------------------------------
# Stage: seqINDV_BE_CCHG_Pkey (PxSequentialFile) - Read
#--------------------------------------------------------------------------------
schema_seqINDV_BE_CCHG_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), True),
    StructField("FIRST_RECYC_TS", TimestampType(), True),
    StructField("INDV_BE_CCHG_SK", IntegerType(), True),
    StructField("INDV_BE_KEY", DecimalType(38,10), True),
    StructField("CCHG_STRT_YR_MO_SK", StringType(), True),
    StructField("PRCS_YR_MO_SK", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("CCHG_MULT_CAT_GRP_PRI", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_SEC", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_TRTY", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_4TH", StringType(), True),
    StructField("CCHG_MULT_CAT_GRP_5TH", StringType(), True),
    StructField("CCHG_END_YR_MO_SK", StringType(), True),
    StructField("CCHG_CT", IntegerType(), True)
])

df_seqINDV_BE_CCHG_Pkey = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .schema(schema_seqINDV_BE_CCHG_Pkey)
    .load(f"{adls_path}/key/INDV_BE_CCHG.{SrcSysCd}.pkey.{RunID}.dat")
)

#--------------------------------------------------------------------------------
# Stage: db2_K_CCHG_MULT_CAT_GRP (DB2ConnectorPX) - Read
#--------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CCHG_MULT_CAT_GRP_ID, SRC_SYS_CD, CCHG_MULT_CAT_GRP_SK FROM {IDSOwner}.K_CCHG_MULT_CAT_GRP"
df_db2_K_CCHG_MULT_CAT_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

#--------------------------------------------------------------------------------
# Stage: Copy (PxCopy)
#--------------------------------------------------------------------------------
df_MultCatGrp1 = df_db2_K_CCHG_MULT_CAT_GRP.select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

df_MultCatGrp2 = df_db2_K_CCHG_MULT_CAT_GRP.select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

df_MultCatGrp3 = df_db2_K_CCHG_MULT_CAT_GRP.select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

df_MultCatGrp4 = df_db2_K_CCHG_MULT_CAT_GRP.select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

df_MultCatGrp5 = df_db2_K_CCHG_MULT_CAT_GRP.select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

#--------------------------------------------------------------------------------
# Stage: CdMppngLkup (PxLookup)
#--------------------------------------------------------------------------------
df_CdMppngLkup = (
    df_seqINDV_BE_CCHG_Pkey.alias("In")
    .join(
        df_MultCatGrp1.alias("MultCatGrp1"),
        (F.col("In.CCHG_MULT_CAT_GRP_PRI") == F.col("MultCatGrp1.CCHG_MULT_CAT_GRP_ID")) &
        (F.col("In.SRC_SYS_CD") == F.col("MultCatGrp1.SRC_SYS_CD")),
        "left"
    )
    .join(
        df_MultCatGrp2.alias("MultCatGrp2"),
        (F.col("In.CCHG_MULT_CAT_GRP_SEC") == F.col("MultCatGrp2.CCHG_MULT_CAT_GRP_ID")) &
        (F.col("In.SRC_SYS_CD") == F.col("MultCatGrp2.SRC_SYS_CD")),
        "left"
    )
    .join(
        df_MultCatGrp3.alias("MultCatGrp3"),
        (F.col("In.CCHG_MULT_CAT_GRP_TRTY") == F.col("MultCatGrp3.CCHG_MULT_CAT_GRP_ID")) &
        (F.col("In.SRC_SYS_CD") == F.col("MultCatGrp3.SRC_SYS_CD")),
        "left"
    )
    .join(
        df_MultCatGrp4.alias("MultCatGrp4"),
        (F.col("In.CCHG_MULT_CAT_GRP_4TH") == F.col("MultCatGrp4.CCHG_MULT_CAT_GRP_ID")) &
        (F.col("In.SRC_SYS_CD") == F.col("MultCatGrp4.SRC_SYS_CD")),
        "left"
    )
    .join(
        df_MultCatGrp5.alias("MultCatGrp5"),
        (F.col("In.CCHG_MULT_CAT_GRP_5TH") == F.col("MultCatGrp5.CCHG_MULT_CAT_GRP_ID")) &
        (F.col("In.SRC_SYS_CD") == F.col("MultCatGrp5.SRC_SYS_CD")),
        "left"
    )
    .select(
        F.col("In.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("In.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("In.INDV_BE_CCHG_SK").alias("INDV_BE_CCHG_SK"),
        F.col("In.INDV_BE_KEY").alias("INDV_BE_KEY"),
        F.col("In.CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
        F.col("In.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
        F.col("In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("In.CCHG_MULT_CAT_GRP_PRI").alias("CCHG_MULT_CAT_GRP_PRI"),
        F.col("In.CCHG_MULT_CAT_GRP_SEC").alias("CCHG_MULT_CAT_GRP_SEC"),
        F.col("In.CCHG_MULT_CAT_GRP_TRTY").alias("CCHG_MULT_CAT_GRP_TRTY"),
        F.col("In.CCHG_MULT_CAT_GRP_4TH").alias("CCHG_MULT_CAT_GRP_4TH"),
        F.col("In.CCHG_MULT_CAT_GRP_5TH").alias("CCHG_MULT_CAT_GRP_5TH"),
        F.col("In.CCHG_END_YR_MO_SK").alias("CCHG_END_YR_MO_SK"),
        F.col("In.CCHG_CT").alias("CCHG_CT"),
        F.col("MultCatGrp1.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK_1"),
        F.col("MultCatGrp2.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK_2"),
        F.col("MultCatGrp3.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK_3"),
        F.col("MultCatGrp4.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK_4"),
        F.col("MultCatGrp5.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK_5")
    )
)

#--------------------------------------------------------------------------------
# Stage: xfm_CheckLkpResults (CTransformerStage)
#--------------------------------------------------------------------------------
df_xfmCheck = (
    df_CdMppngLkup
    .withColumn(
        "svMultCatGrp1FkeyFail",
        F.when(F.col("CCHG_MULT_CAT_GRP_SK_1").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "svMultCatGrp2FkeyFail",
        F.when(F.col("CCHG_MULT_CAT_GRP_SK_2").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "svMultCatGrp3FkeyFail",
        F.when(F.col("CCHG_MULT_CAT_GRP_SK_3").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "svMultCatGrp4FkeyFail",
        F.when(F.col("CCHG_MULT_CAT_GRP_SK_4").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "svMultCatGrp5FkeyFail",
        F.when(F.col("CCHG_MULT_CAT_GRP_SK_5").isNull(), F.lit("Y")).otherwise(F.lit("N"))
    )
)

df_Lnk_Main = df_xfmCheck.select(
    F.col("INDV_BE_CCHG_SK").alias("INDV_BE_CCHG_SK"),
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("CCHG_MULT_CAT_GRP_PRI") == "NA", F.lit(1))
     .when(F.col("CCHG_MULT_CAT_GRP_PRI") == "UNK", F.lit(0))
     .when(F.col("CCHG_MULT_CAT_GRP_SK_1").isNull(), F.lit(0))
     .otherwise(F.col("CCHG_MULT_CAT_GRP_SK_1")).alias("CCHG_MULT_CAT_GRP_PRI_SK"),
    F.when(F.col("CCHG_MULT_CAT_GRP_SEC") == "NA", F.lit(1))
     .when(F.col("CCHG_MULT_CAT_GRP_SEC") == "UNK", F.lit(0))
     .when(F.col("CCHG_MULT_CAT_GRP_SK_2").isNull(), F.lit(0))
     .otherwise(F.col("CCHG_MULT_CAT_GRP_SK_2")).alias("CCHG_MULT_CAT_GRP_SEC_SK"),
    F.when(F.col("CCHG_MULT_CAT_GRP_TRTY") == "NA", F.lit(1))
     .when(F.col("CCHG_MULT_CAT_GRP_TRTY") == "UNK", F.lit(0))
     .when(F.col("CCHG_MULT_CAT_GRP_SK_3").isNull(), F.lit(0))
     .otherwise(F.col("CCHG_MULT_CAT_GRP_SK_3")).alias("CCHG_MULT_CAT_GRP_TRTY_SK"),
    F.when(F.col("CCHG_MULT_CAT_GRP_4TH") == "NA", F.lit(1))
     .when(F.col("CCHG_MULT_CAT_GRP_4TH") == "UNK", F.lit(0))
     .when(F.col("CCHG_MULT_CAT_GRP_SK_4").isNull(), F.lit(0))
     .otherwise(F.col("CCHG_MULT_CAT_GRP_SK_4")).alias("CCHG_MULT_CAT_GRP_4TH_SK"),
    F.when(F.col("CCHG_MULT_CAT_GRP_5TH") == "NA", F.lit(1))
     .when(F.col("CCHG_MULT_CAT_GRP_5TH") == "UNK", F.lit(0))
     .when(F.col("CCHG_MULT_CAT_GRP_SK_5").isNull(), F.lit(0))
     .otherwise(F.col("CCHG_MULT_CAT_GRP_SK_5")).alias("CCHG_MULT_CAT_GRP_5TH_SK"),
    F.col("CCHG_END_YR_MO_SK").alias("CCHG_END_YR_MO_SK"),
    F.col("CCHG_CT").alias("CCHG_CT")
)

df_Lnk_NA = df_xfmCheck.limit(1).select(
    F.lit(1).alias("INDV_BE_CCHG_SK"),
    F.lit(1).alias("INDV_BE_KEY"),
    F.lit("175301").alias("CCHG_STRT_YR_MO_SK"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CCHG_MULT_CAT_GRP_PRI_SK"),
    F.lit(1).alias("CCHG_MULT_CAT_GRP_SEC_SK"),
    F.lit(1).alias("CCHG_MULT_CAT_GRP_TRTY_SK"),
    F.lit(1).alias("CCHG_MULT_CAT_GRP_4TH_SK"),
    F.lit(1).alias("CCHG_MULT_CAT_GRP_5TH_SK"),
    F.lit("219912").alias("CCHG_END_YR_MO_SK"),
    F.lit(0).alias("CCHG_CT")
)

df_Lnk_UNK = df_xfmCheck.limit(1).select(
    F.lit(0).alias("INDV_BE_CCHG_SK"),
    F.lit(0).alias("INDV_BE_KEY"),
    F.lit("175301").alias("CCHG_STRT_YR_MO_SK"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CCHG_MULT_CAT_GRP_PRI_SK"),
    F.lit(0).alias("CCHG_MULT_CAT_GRP_SEC_SK"),
    F.lit(0).alias("CCHG_MULT_CAT_GRP_TRTY_SK"),
    F.lit(0).alias("CCHG_MULT_CAT_GRP_4TH_SK"),
    F.lit(0).alias("CCHG_MULT_CAT_GRP_5TH_SK"),
    F.lit("219912").alias("CCHG_END_YR_MO_SK"),
    F.lit(0).alias("CCHG_CT")
)

#--------------------------------------------------------------------------------
# Stage: fnl_NA_UNK_Streams (PxFunnel)
#--------------------------------------------------------------------------------
df_fnl_NA_UNK_Streams = (
    df_Lnk_Main.unionByName(df_Lnk_NA)
    .unionByName(df_Lnk_UNK)
)

df_seq_INDV_BE_CCHG_Fkey = df_fnl_NA_UNK_Streams.select(
    F.col("INDV_BE_CCHG_SK"),
    F.col("INDV_BE_KEY"),
    F.rpad(F.col("CCHG_STRT_YR_MO_SK"), 6, " ").alias("CCHG_STRT_YR_MO_SK"),
    F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CCHG_MULT_CAT_GRP_PRI_SK"),
    F.col("CCHG_MULT_CAT_GRP_SEC_SK"),
    F.col("CCHG_MULT_CAT_GRP_TRTY_SK"),
    F.col("CCHG_MULT_CAT_GRP_4TH_SK"),
    F.col("CCHG_MULT_CAT_GRP_5TH_SK"),
    F.rpad(F.col("CCHG_END_YR_MO_SK"), 6, " ").alias("CCHG_END_YR_MO_SK"),
    F.col("CCHG_CT")
)

write_files(
    df_seq_INDV_BE_CCHG_Fkey,
    f"{adls_path}/load/INDV_BE_CCHG.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

#--------------------------------------------------------------------------------
# Stage: FkeyErrors (PxFunnel)
#--------------------------------------------------------------------------------
df_MultCatGrp1_LkupFail = df_xfmCheck.filter(
    F.col("svMultCatGrp1FkeyFail") == "Y"
).select(
    F.col("INDV_BE_CCHG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_MULT_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_MULT_CAT_GRP_PRI")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_MultCatGrp2_LkupFail = df_xfmCheck.filter(
    F.col("svMultCatGrp2FkeyFail") == "Y"
).select(
    F.col("INDV_BE_CCHG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_MULT_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_MULT_CAT_GRP_SEC")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_MultCatGrp3_LkupFail = df_xfmCheck.filter(
    F.col("svMultCatGrp3FkeyFail") == "Y"
).select(
    F.col("INDV_BE_CCHG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_MULT_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_MULT_CAT_GRP_TRTY")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_MultCatGrp4_LkupFail = df_xfmCheck.filter(
    F.col("svMultCatGrp4FkeyFail") == "Y"
).select(
    F.col("INDV_BE_CCHG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_MULT_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_MULT_CAT_GRP_4TH")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_MultCatGrp5_LkupFail = df_xfmCheck.filter(
    F.col("svMultCatGrp5FkeyFail") == "Y"
).select(
    F.col("INDV_BE_CCHG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_MULT_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SRC_SYS_CD"), F.lit(";"), F.col("CCHG_MULT_CAT_GRP_5TH")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_FkeyErrors = (
    df_MultCatGrp1_LkupFail
    .unionByName(df_MultCatGrp2_LkupFail)
    .unionByName(df_MultCatGrp3_LkupFail)
    .unionByName(df_MultCatGrp4_LkupFail)
    .unionByName(df_MultCatGrp5_LkupFail)
)

df_FkeyErrors = df_FkeyErrors.select(
    F.col("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK"),
    F.col("JOB_NM"),
    F.col("ERROR_TYP"),
    F.col("PHYSCL_FILE_NM"),
    F.col("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

write_files(
    df_FkeyErrors,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)