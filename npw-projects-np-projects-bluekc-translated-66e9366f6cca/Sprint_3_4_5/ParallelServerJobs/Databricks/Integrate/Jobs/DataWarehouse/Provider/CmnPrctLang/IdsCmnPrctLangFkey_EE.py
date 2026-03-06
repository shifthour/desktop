# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  IdsProvLoadJc
# MAGIC 
# MAGIC PROCESSING: Takes the file  from primary key job and does foreign key lookups.
# MAGIC 
# MAGIC OUTPUTS:  Load file for table CMN_PRCT_LANG
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Nothing to reset just rerun. 
# MAGIC              PREVIOUS RUN ABORTED:         Normally nothing has to be done before restarting.  In some cases files in the working directories may have to be removed or recreated.                                                                           
# MAGIC 
# MAGIC MODIFICATIONS
# MAGIC Steph Goddard     5/18/2005   Job created
# MAGIC Steph Goddard     6/01/2005   Sequencer changes
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                                    Project/                                                                                                                                                         Code                          Date
# MAGIC Developer                     Date                      Altiris #                    Change Description                                                                                                       Reviewer                   Reviewed
# MAGIC ----------------------       -------------------       -------------------      ------------------------------------------------------------------------------------                                                                     ----------------------     -------------------   
# MAGIC SAndrew                        2007-09-10        TTR-82                      Standardized                                                                                                                  Steph Goddard     9/12/07
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela      2014-07-12            5345                             Original Programming                                                                                                 Kalyan Neelam      2014-12-31

# MAGIC Job Name: IdsCmnPrctLangFkey_EE
# MAGIC FKEY failures are written into this flat file.
# MAGIC Code lookups operation to get the needed FKEY values
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

seq_CMN_PRCT_LANG_PKEY_schema = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType(), False),
    StructField("FIRST_RECYC_TS", TimestampType(), False),
    StructField("CMN_PRCT_LANG_SK", IntegerType(), False),
    StructField("CMN_PRCT_ID", StringType(), False),
    StructField("LANG_SEQ_NO", IntegerType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("LANG_DESC", StringType(), True),
    StructField("CMN_PRCT_LKP_SRC_SYS_CD_FACETS", StringType(), False),
    StructField("CMN_PRCT_LKP_SRC_SYS_CD_VCAC", StringType(), False)
])

df_seq_CMN_PRCT_LANG_PKEY = (
    spark.read
    .option("header", False)
    .option("sep", ",")
    .option("quote", "^")
    .option("nullValue", None)
    .schema(seq_CMN_PRCT_LANG_PKEY_schema)
    .csv(f"{adls_path}/key/CMN_PRCT_LANG.{SrcSysCd}.pkey.{RunID}.dat")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_K_CMN_PRCT_Lkp = f"SELECT CMN_PRCT_ID, SRC_SYS_CD, CMN_PRCT_SK FROM {IDSOwner}.K_CMN_PRCT"
df_db2_K_CMN_PRCT_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_CMN_PRCT_Lkp)
    .load()
)

extract_query_db2_K_CMN_PRCT_FACETS_Lkp = f"SELECT CMN_PRCT_ID, SRC_SYS_CD, CMN_PRCT_SK FROM {IDSOwner}.K_CMN_PRCT"
df_db2_K_CMN_PRCT_FACETS_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_CMN_PRCT_FACETS_Lkp)
    .load()
)

df_lkp_join_1 = df_seq_CMN_PRCT_LANG_PKEY.alias("Lnk_IdsCmnPrctLangFkey_InAbc").join(
    df_db2_K_CMN_PRCT_Lkp.alias("Ref_K_CmnPrctVcac_In"),
    (F.col("Lnk_IdsCmnPrctLangFkey_InAbc.CMN_PRCT_ID") == F.col("Ref_K_CmnPrctVcac_In.CMN_PRCT_ID")) &
    (F.col("Lnk_IdsCmnPrctLangFkey_InAbc.CMN_PRCT_LKP_SRC_SYS_CD_VCAC") == F.col("Ref_K_CmnPrctVcac_In.SRC_SYS_CD")),
    "left"
)

df_lkp_Code_SKs = df_lkp_join_1.alias("lkp_1").join(
    df_db2_K_CMN_PRCT_FACETS_Lkp.alias("Ref_K_CmnPrctFactes_In"),
    (F.col("lkp_1.Lnk_IdsCmnPrctLangFkey_InAbc.CMN_PRCT_ID") == F.col("Ref_K_CmnPrctFactes_In.CMN_PRCT_ID")) &
    (F.col("lkp_1.Lnk_IdsCmnPrctLangFkey_InAbc.CMN_PRCT_LKP_SRC_SYS_CD_FACETS") == F.col("Ref_K_CmnPrctFactes_In.SRC_SYS_CD")),
    "left"
).select(
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.CMN_PRCT_LANG_SK").alias("CMN_PRCT_LANG_SK"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.LANG_SEQ_NO").alias("LANG_SEQ_NO"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("Ref_K_CmnPrctVcac_In.CMN_PRCT_SK").alias("CMN_PRCT_VCAC_SK"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.LANG_DESC").alias("LANG_DESC"),
    F.col("Ref_K_CmnPrctFactes_In.CMN_PRCT_SK").alias("CMN_PRCT_FACETS_SK"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.CMN_PRCT_LKP_SRC_SYS_CD_FACETS").alias("CMN_PRCT_LKP_SRC_SYS_CD_FACETS"),
    F.col("Lnk_IdsCmnPrctLangFkey_InAbc.CMN_PRCT_LKP_SRC_SYS_CD_VCAC").alias("CMN_PRCT_LKP_SRC_SYS_CD_VCAC")
)

df_xfm_CheckLkpResults = df_lkp_Code_SKs.withColumn(
    "svCmnPrctSkFKeyFailCheck",
    F.when(
        (F.col("CMN_PRCT_FACETS_SK").isNull()) &
        (F.col("CMN_PRCT_VCAC_SK").isNull()) &
        (F.col("CMN_PRCT_ID") != F.lit("NA")),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

df_fail = df_xfm_CheckLkpResults.filter(
    F.col("svCmnPrctSkFKeyFailCheck") == "Y"
).select(
    F.col("CMN_PRCT_LANG_SK").alias("PRI_SK"),
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.lit("IdsCmnPrctLangFkey_EE").alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_CMN_PRCT").alias("PHYSCL_FILE_NM"),
    (F.col("CMN_PRCT_LKP_SRC_SYS_CD_VCAC") + F.lit(";") + F.col("CMN_PRCT_ID")).alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_fail_final = df_fail.select(
    F.col("PRI_SK"),
    F.rpad(F.col("PRI_NAT_KEY_STRING"), <...>, " ").alias("PRI_NAT_KEY_STRING"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("JOB_NM"), <...>, " ").alias("JOB_NM"),
    F.rpad(F.col("ERROR_TYP"), <...>, " ").alias("ERROR_TYP"),
    F.rpad(F.col("PHYSCL_FILE_NM"), <...>, " ").alias("PHYSCL_FILE_NM"),
    F.rpad(F.col("FRGN_NAT_KEY_STRING"), <...>, " ").alias("FRGN_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("JOB_EXCTN_SK")
)

df_main = df_xfm_CheckLkpResults.filter(
    F.col("svCmnPrctSkFKeyFailCheck") != "Y"
).select(
    F.col("CMN_PRCT_LANG_SK").alias("CMN_PRCT_LANG_SK"),
    F.when(F.col("SRC_SYS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
    F.col("CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    F.col("LANG_SEQ_NO").alias("LANG_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.trim(F.col("CMN_PRCT_ID")) == F.lit("UNK"), F.lit(0))
     .when(F.trim(F.col("CMN_PRCT_ID")) == F.lit("NA"), F.lit(1))
     .when(F.col("CMN_PRCT_FACETS_SK").isNull(), F.col("CMN_PRCT_VCAC_SK"))
     .when(
         (F.col("CMN_PRCT_FACETS_SK").isNull()) &
         (F.col("CMN_PRCT_VCAC_SK").isNull()),
         F.lit(0)
     )
     .otherwise(F.col("CMN_PRCT_FACETS_SK"))
     .alias("CMN_PRCT_SK"),
    F.col("LANG_DESC").alias("LANG_DESC")
)

df_UNK = spark.createDataFrame(
    [(0, 0, "UNK", 0, 100, 100, 0, None)],
    [
        "CMN_PRCT_LANG_SK",
        "SRC_SYS_CD_SK",
        "CMN_PRCT_ID",
        "LANG_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CMN_PRCT_SK",
        "LANG_DESC"
    ]
)

df_NA = spark.createDataFrame(
    [(1, 1, "NA", 0, 100, 100, 1, None)],
    [
        "CMN_PRCT_LANG_SK",
        "SRC_SYS_CD_SK",
        "CMN_PRCT_ID",
        "LANG_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CMN_PRCT_SK",
        "LANG_DESC"
    ]
)

df_fnl_NA_UNK_Streams = df_main.union(df_UNK).union(df_NA)

df_fnl_NA_UNK_Streams_final = df_fnl_NA_UNK_Streams.select(
    F.col("CMN_PRCT_LANG_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CMN_PRCT_ID"), <...>, " ").alias("CMN_PRCT_ID"),
    F.col("LANG_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CMN_PRCT_SK"),
    F.rpad(F.col("LANG_DESC"), <...>, " ").alias("LANG_DESC")
)

write_files(
    df_fail_final,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.IdsCmnPrctLangFkey_EE.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

write_files(
    df_fnl_NA_UNK_Streams_final,
    f"{adls_path}/load/CMN_PRCT_LANG.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)