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
# MAGIC Raja Gummadi           2017-03-22         Prod Supp                      Changed CLM_LN_CCHG_SK from VARCHAR to INT                        IntegrateDev1            Kalyan Neelam            2017-03-22

# MAGIC CLM and CLM_LN SK lookups
# MAGIC CLM_SRC_SYS_CD lookup
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_K_CLM_LN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD_SK as CLM_SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO, CLM_LN_SK FROM {IDSOwner}.K_CLM_LN")
    .load()
)

df_db2_K_CLM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT SRC_SYS_CD_SK as CLM_SRC_SYS_CD_SK, CLM_ID, CLM_SK FROM {IDSOwner}.K_CLM")
    .load()
)

schema_seqCLM_LN_CCHG_Pkey = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("CLM_LN_CCHG_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), False),
    StructField("CLM_SRC_SYS_CD", StringType(), False),
    StructField("PRCS_YR_MO_SK", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CCHG_MULT_CAT_GRP", StringType(), False)
])

df_seqCLM_LN_CCHG_Pkey = (
    spark.read
    .format("csv")
    .schema(schema_seqCLM_LN_CCHG_Pkey)
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .load(f"{adls_path}/key/CLM_LN_CCHG.{SrcSysCd}.pkey.{RunID}.dat")
)

df_db2_K_CCHG_MULT_CAT_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CCHG_MULT_CAT_GRP_ID, SRC_SYS_CD, CCHG_MULT_CAT_GRP_SK FROM {IDSOwner}.K_CCHG_MULT_CAT_GRP")
    .load()
)

df_ds_CD_MPPNG_LkpData = spark.read.parquet(f"{adls_path}/ds/CD_MPPNG.parquet")

df_fltr_FilterData_intermediate = df_ds_CD_MPPNG_LkpData.filter(
    (F.col("SRC_SYS_CD") == "IDS") &
    (F.col("SRC_CLCTN_CD") == "IDS") &
    (F.col("TRGT_CLCTN_CD") == "IDS") &
    (F.col("SRC_DOMAIN_NM") == "SOURCE SYSTEM") &
    (F.col("TRGT_DOMAIN_NM") == "SOURCE SYSTEM")
)

df_fltr_FilterData = df_fltr_FilterData_intermediate.select(
    F.col("SRC_CD").alias("SRC_CD"),
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK")
)

# Add a column to df_seqCLM_LN_CCHG_Pkey for the unused reference:
df_seqCLM_LN_CCHG_Pkey = df_seqCLM_LN_CCHG_Pkey.withColumn(
    "lnk_IdsDmClmDmClmLnProcCdModExtr_InABC.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK",
    F.lit(None).cast(IntegerType())
)

df_CdMppngLkup_int = (
    df_seqCLM_LN_CCHG_Pkey.alias("In")
    .join(
        df_db2_K_CCHG_MULT_CAT_GRP.alias("MultCatGrpLKup"),
        (F.col("In.CCHG_MULT_CAT_GRP") == F.col("MultCatGrpLKup.CCHG_MULT_CAT_GRP_ID")) &
        (F.col("In.SRC_SYS_CD") == F.col("MultCatGrpLKup.SRC_SYS_CD")),
        "left"
    )
    .join(
        df_fltr_FilterData.alias("ClmSrcSysCd"),
        (F.col("In.CLM_SRC_SYS_CD") == F.col("ClmSrcSysCd.SRC_CD")) &
        (F.col("In.lnk_IdsDmClmDmClmLnProcCdModExtr_InABC.CLM_LN_PROC_CD_MOD_ORDNL_CD_SK") == F.col("ClmSrcSysCd.CD_MPPNG_SK")),
        "left"
    )
)

df_CdMppngLkup = df_CdMppngLkup_int.select(
    F.col("In.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("In.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("In.CLM_LN_CCHG_SK").alias("CLM_LN_CCHG_SK"),
    F.col("In.CLM_ID").alias("CLM_ID"),
    F.col("In.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("In.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("In.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("In.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("In.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmSrcSysCd.CD_MPPNG_SK").alias("CLM_SRC_SYS_CD_SK"),
    F.col("In.CCHG_MULT_CAT_GRP").alias("CCHG_MULT_CAT_GRP"),
    F.col("MultCatGrpLKup.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

df_Join_135_int = df_CdMppngLkup.alias("DSLink65").join(
    df_db2_K_CLM.alias("ClmLKup"),
    (
        (F.col("DSLink65.CLM_ID") == F.col("ClmLKup.CLM_ID")) &
        (F.col("DSLink65.CLM_SRC_SYS_CD_SK") == F.col("ClmLKup.CLM_SRC_SYS_CD_SK"))
    ),
    "left"
)

df_Join_135 = df_Join_135_int.select(
    F.col("DSLink65.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("DSLink65.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("DSLink65.CLM_LN_CCHG_SK").alias("CLM_LN_CCHG_SK"),
    F.col("DSLink65.CLM_ID").alias("CLM_ID"),
    F.col("DSLink65.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("DSLink65.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("DSLink65.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("DSLink65.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("DSLink65.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("DSLink65.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("DSLink65.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DSLink65.CLM_SRC_SYS_CD_SK").alias("CLM_SRC_SYS_CD_SK"),
    F.col("DSLink65.CCHG_MULT_CAT_GRP").alias("CCHG_MULT_CAT_GRP"),
    F.col("DSLink65.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    F.col("ClmLKup.CLM_SK").alias("CLM_SK")
)

df_Join_136_int = df_Join_135.alias("DSLink122").join(
    df_db2_K_CLM_LN.alias("ClmLnLKup"),
    (
        (F.col("DSLink122.CLM_ID") == F.col("ClmLnLKup.CLM_ID")) &
        (F.col("DSLink122.CLM_LN_SEQ_NO") == F.col("ClmLnLKup.CLM_LN_SEQ_NO")) &
        (F.col("DSLink122.CLM_SRC_SYS_CD_SK") == F.col("ClmLnLKup.CLM_SRC_SYS_CD_SK"))
    ),
    "left"
)

df_Join_136 = df_Join_136_int.select(
    F.col("DSLink122.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("DSLink122.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("DSLink122.CLM_LN_CCHG_SK").alias("CLM_LN_CCHG_SK"),
    F.col("DSLink122.CLM_ID").alias("CLM_ID"),
    F.col("DSLink122.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("DSLink122.CLM_SRC_SYS_CD").alias("CLM_SRC_SYS_CD"),
    F.col("DSLink122.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("DSLink122.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("DSLink122.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("DSLink122.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("DSLink122.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DSLink122.CLM_SRC_SYS_CD_SK").alias("CLM_SRC_SYS_CD_SK"),
    F.col("DSLink122.CCHG_MULT_CAT_GRP").alias("CCHG_MULT_CAT_GRP"),
    F.col("DSLink122.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    F.col("DSLink122.CLM_SK").alias("CLM_SK"),
    F.col("ClmLnLKup.CLM_LN_SK").alias("CLM_LN_SK")
)

df_xfm_check = df_Join_136.alias("DSLink127") \
    .withColumn("svMultCatGrpFkeyFail", F.when(F.col("DSLink127.CCHG_MULT_CAT_GRP_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))) \
    .withColumn("svClmSrcSysCdFkeyFail", F.when(F.col("DSLink127.CLM_SRC_SYS_CD_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))) \
    .withColumn("svClmFkeyFail", F.when(F.col("DSLink127.CLM_SK").isNull(), F.lit("Y")).otherwise(F.lit("N"))) \
    .withColumn("svClmLnFkeyFail", F.when(F.col("DSLink127.CLM_LN_SK").isNull(), F.lit("Y")).otherwise(F.lit("N")))

# Main link (no constraint, all rows)
df_xfm_Lnk_Main = df_xfm_check.select(
    F.col("DSLink127.CLM_LN_CCHG_SK").alias("CLM_LN_CCHG_SK"),
    F.col("DSLink127.CLM_ID").alias("CLM_ID"),
    F.col("DSLink127.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.when(F.col("DSLink127.CLM_SRC_SYS_CD") == F.lit("NA"), F.lit(1))
     .when(F.col("DSLink127.CLM_SRC_SYS_CD") == F.lit("UNK"), F.lit(0))
     .when(F.col("DSLink127.CLM_SRC_SYS_CD_SK").isNull(), F.lit(0))
     .otherwise(F.col("DSLink127.CLM_SRC_SYS_CD_SK"))
     .alias("CLM_SRC_SYS_CD_SK"),
    F.col("DSLink127.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("DSLink127.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("DSLink127.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("DSLink127.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("DSLink127.CCHG_MULT_CAT_GRP_SK").isNull(), F.lit(0))
     .otherwise(F.col("DSLink127.CCHG_MULT_CAT_GRP_SK"))
     .alias("CCHG_MULT_CAT_GRP_SK"),
    F.when(F.col("DSLink127.CLM_SK").isNull(), F.lit(0))
     .otherwise(F.col("DSLink127.CLM_SK"))
     .alias("CLM_SK"),
    F.when(F.col("DSLink127.CLM_LN_SK").isNull(), F.lit(0))
     .otherwise(F.col("DSLink127.CLM_LN_SK"))
     .alias("CLM_LN_SK")
)

# Lnk_NA constraint => only first record
w_na = Window.orderBy(F.lit(1))
df_xfm_rn_na = df_xfm_check.withColumn("rn_na", F.row_number().over(w_na))
df_xfm_Lnk_NA = df_xfm_rn_na.filter(F.col("rn_na") == 1).select(
    F.lit(1).alias("CLM_LN_CCHG_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.lit(1).alias("CLM_SRC_SYS_CD_SK"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CCHG_MULT_CAT_GRP_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit(1).alias("CLM_LN_SK")
)

# Lnk_UNK constraint => only first record
w_unk = Window.orderBy(F.lit(1))
df_xfm_rn_unk = df_xfm_check.withColumn("rn_unk", F.row_number().over(w_unk))
df_xfm_Lnk_UNK = df_xfm_rn_unk.filter(F.col("rn_unk") == 1).select(
    F.lit(0).alias("CLM_LN_CCHG_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CLM_SRC_SYS_CD_SK"),
    F.lit("175301").alias("PRCS_YR_MO_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CCHG_MULT_CAT_GRP_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit(0).alias("CLM_LN_SK")
)

# ClmLnFkey_LkupFail => svClmLnFkeyFail = 'Y'
df_xfm_ClmLnFkey_LkupFail = df_xfm_check.filter(F.col("svClmLnFkeyFail") == "Y").select(
    F.col("DSLink127.CLM_LN_CCHG_SK").alias("PRI_SK"),
    F.col("DSLink127.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("DSLink127.CLM_SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsClmLnCchgFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CLM_LN").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("DSLink127.CLM_SRC_SYS_CD"), F.lit(";"), F.col("DSLink127.CLM_ID"), F.lit(";"), F.col("DSLink127.CLM_LN_SEQ_NO")).alias("FRGN_NAT_KEY_STRING"),
    F.col("DSLink127.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("DSLink127.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# MultCatGrp_LkupFail => svMultCatGrpFkeyFail = 'Y'
df_xfm_MultCatGrp_LkupFail = df_xfm_check.filter(F.col("svMultCatGrpFkeyFail") == "Y").select(
    F.col("DSLink127.CLM_LN_CCHG_SK").alias("PRI_SK"),
    F.col("DSLink127.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("DSLink127.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsClmLnCchgFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CCHG_MULT_CAT_GRP").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("DSLink127.SRC_SYS_CD"), F.lit(";"), F.col("DSLink127.CCHG_MULT_CAT_GRP")).alias("FRGN_NAT_KEY_STRING"),
    F.col("DSLink127.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("DSLink127.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ClmSrcSysCd_LkupFail => svClmSrcSysCdFkeyFail = 'Y'
df_xfm_ClmSrcSysCd_LkupFail = df_xfm_check.filter(F.col("svClmSrcSysCdFkeyFail") == "Y").select(
    F.col("DSLink127.CLM_LN_CCHG_SK").alias("PRI_SK"),
    F.col("DSLink127.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("DSLink127.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsClmLnCchgFkey").alias("JOB_NM"),
    F.lit("CDLOOKUP").alias("ERROR_TYP"),
    F.lit("CD_MPPNG").alias("PHYSCL_FILE_NM"),
    F.concat(
        F.lit("IDS"), F.lit(";"),
        F.lit("IDS"), F.lit(";"),
        F.lit("IDS"), F.lit(";"),
        F.lit("SOURCE SYSTEM"), F.lit(";"),
        F.lit("SOURCE SYSTEM"), F.lit(";"),
        F.col("DSLink127.CLM_SRC_SYS_CD")
    ).alias("FRGN_NAT_KEY_STRING"),
    F.col("DSLink127.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("DSLink127.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

# ClmFkey_LkupFail => svClmFkeyFail = 'Y'
df_xfm_ClmFkey_LkupFail = df_xfm_check.filter(F.col("svClmFkeyFail") == "Y").select(
    F.col("DSLink127.CLM_LN_CCHG_SK").alias("PRI_SK"),
    F.col("DSLink127.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("DSLink127.CLM_SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsClmLnCchgFkey").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CLM").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("DSLink127.CLM_SRC_SYS_CD"), F.lit(";"), F.col("DSLink127.CLM_ID")).alias("FRGN_NAT_KEY_STRING"),
    F.col("DSLink127.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("DSLink127.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fnl_NA_UNK_Streams = df_xfm_Lnk_Main.unionByName(df_xfm_Lnk_NA).unionByName(df_xfm_Lnk_UNK)

# Final reorder for funnel output
df_fnl_NA_UNK_Streams_final = df_fnl_NA_UNK_Streams.select(
    "CLM_LN_CCHG_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_SRC_SYS_CD_SK",
    "PRCS_YR_MO_SK",
    "SRC_SYS_CD_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CCHG_MULT_CAT_GRP_SK",
    "CLM_SK",
    "CLM_LN_SK"
)

# Funnel to write
# Apply rpad for char(6) columns, and for varchar columns assume length 255
# (PRCS_YR_MO_SK is char(6), CLM_ID is varchar, etc.)
df_fnl_NA_UNK_Streams_rpad = df_fnl_NA_UNK_Streams_final \
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 255, " ")) \
    .withColumn("PRCS_YR_MO_SK", F.rpad(F.col("PRCS_YR_MO_SK"), 6, " "))

write_files(
    df_fnl_NA_UNK_Streams_rpad,
    f"{adls_path}/load/CLM_LN_CCHG.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

df_FkeyErrors = df_xfm_ClmLnFkey_LkupFail.unionByName(df_xfm_MultCatGrp_LkupFail) \
    .unionByName(df_xfm_ClmSrcSysCd_LkupFail) \
    .unionByName(df_xfm_ClmFkey_LkupFail)

df_FkeyErrors_final = df_FkeyErrors.select(
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

# All columns here appear to be string or unknown length so rpad to 255 for strings
df_FkeyErrors_rpad = df_FkeyErrors_final \
    .withColumn("PRI_NAT_KEY_STRING", F.rpad(F.col("PRI_NAT_KEY_STRING"), 255, " ")) \
    .withColumn("JOB_NM", F.rpad(F.col("JOB_NM"), 255, " ")) \
    .withColumn("ERROR_TYP", F.rpad(F.col("ERROR_TYP"), 255, " ")) \
    .withColumn("PHYSCL_FILE_NM", F.rpad(F.col("PHYSCL_FILE_NM"), 255, " ")) \
    .withColumn("FRGN_NAT_KEY_STRING", F.rpad(F.col("FRGN_NAT_KEY_STRING"), 255, " "))

write_files(
    df_FkeyErrors_rpad,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.IdsClmLnCchgFkey.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)