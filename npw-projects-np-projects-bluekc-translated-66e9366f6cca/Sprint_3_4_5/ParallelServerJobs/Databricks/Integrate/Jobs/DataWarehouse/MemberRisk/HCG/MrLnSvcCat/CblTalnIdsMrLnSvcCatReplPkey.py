# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC 
# MAGIC Razia                                   2021-10-19       US-428909        Copied over from CblTalnClmLnHlthCstGrpCntl       IntegrateDev1        Goutham K                     11/9/2021
# MAGIC                                                                                                           to Load the new Rpcl Table

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_MR_LN_SVC_CAT.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

schema_ds_MR_LN_SVC_CAT_Xfrm = StructType([
    StructField("PRI_NAT_KEY_STRING", StringType()),
    StructField("FIRST_RECYC_TS", StringType()),
    StructField("MR_LN_SVC_CAT_SK", StringType()),
    StructField("MR_LN_ID", StringType()),
    StructField("EFF_DT_SK", StringType()),
    StructField("SRC_SYS_CD", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", StringType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", StringType()),
    StructField("SRC_SYS_CD_SK", StringType()),
    StructField("ACTURL_HLTH_CST_GRP_SUM_ID", StringType()),
    StructField("CST_MDL_UTILITY_ID", StringType()),
    StructField("MR_LN_DESC", StringType()),
    StructField("MR_LN_PFX_DESC", StringType()),
    StructField("MR_LN_BODY_DESC", StringType()),
    StructField("MR_LN_SFX_DESC", StringType()),
    StructField("TERM_DT_SK", StringType())
])

df_ds_MR_LN_SVC_CAT_Xfrm = (
    spark.read.schema(schema_ds_MR_LN_SVC_CAT_Xfrm)
    .parquet(f"{adls_path}/ds/MR_LN_SVC_CAT.{SrcSysCd}.xfrm.{RunID}.parquet")
)

df_lnk_xfm_in = df_ds_MR_LN_SVC_CAT_Xfrm

df_lnkRemDupDataIn = df_lnk_xfm_in.select(
    F.col("MR_LN_ID").alias("MR_LN_ID"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_lnk_xfm_in.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("MR_LN_ID").alias("MR_LN_ID"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("ACTURL_HLTH_CST_GRP_SUM_ID").alias("ACTURL_HLTH_CST_GRP_SUM_ID"),
    F.col("CST_MDL_UTILITY_ID").alias("CST_MDL_UTILITY_ID"),
    F.col("MR_LN_DESC").alias("MR_LN_DESC"),
    F.col("MR_LN_PFX_DESC").alias("MR_LN_PFX_DESC"),
    F.col("MR_LN_BODY_DESC").alias("MR_LN_BODY_DESC"),
    F.col("MR_LN_SFX_DESC").alias("MR_LN_SFX_DESC"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK")
)

df_lnkRemDupDataIn_dedup = dedup_sort(
    df_lnkRemDupDataIn,
    ["MR_LN_ID","EFF_DT_SK","SRC_SYS_CD"],
    [("MR_LN_ID","A"),("EFF_DT_SK","A"),("SRC_SYS_CD","A")]
)

df_lnkRemDupDataOut = df_lnkRemDupDataIn_dedup.select(
    F.col("MR_LN_ID").alias("MR_LN_ID"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_K_MR_LN_SVC_CAT_In = f"SELECT * FROM {IDSOwner}.K_MR_LN_SVC_CAT"
df_db2_K_MR_LN_SVC_CAT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_K_MR_LN_SVC_CAT_In)
    .load()
)

df_JoinOut = (
    df_lnkRemDupDataOut.alias("lnkRemDupDataOut")
    .join(
        df_db2_K_MR_LN_SVC_CAT_In.alias("Extr"),
        on=[
            F.col("lnkRemDupDataOut.MR_LN_ID") == F.col("Extr.MR_LN_ID"),
            F.col("lnkRemDupDataOut.EFF_DT_SK") == F.col("Extr.EFF_DT_SK"),
            F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD")
        ],
        how="left"
    )
    .select(
        F.col("lnkRemDupDataOut.MR_LN_ID").alias("MR_LN_ID"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkRemDupDataOut.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Extr.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK")
    )
)

df_pre = df_JoinOut.withColumn("ORIG_SK_NULL", F.col("MR_LN_SVC_CAT_SK").isNull())
df_pre = df_pre.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("MR_LN_SVC_CAT_SK").isNull(), F.lit(IDSRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

df_enriched = SurrogateKeyGen(df_pre,<DB sequence name>,"MR_LN_SVC_CAT_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(IDSRunCycle))

df_new = df_enriched.filter(F.col("ORIG_SK_NULL") == True).select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MR_LN_ID").alias("MR_LN_ID"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MR_LN_ID").alias("MR_LN_ID"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK")
)

temp_table_db2_K_MR_LN_SVC_CAT_Load = "STAGING.CblTalnIdsMrLnSvcCatReplPkey_db2_K_MR_LN_SVC_CAT_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_K_MR_LN_SVC_CAT_Load}", jdbc_url_ids, jdbc_props_ids)
(
    df_new.write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table_db2_K_MR_LN_SVC_CAT_Load)
    .mode("overwrite")
    .save()
)
merge_sql_db2_K_MR_LN_SVC_CAT_Load = f"""
MERGE INTO {IDSOwner}.K_MR_LN_SVC_CAT AS T
USING {temp_table_db2_K_MR_LN_SVC_CAT_Load} AS S
ON T.MR_LN_ID=S.MR_LN_ID AND T.EFF_DT_SK=S.EFF_DT_SK AND T.SRC_SYS_CD=S.SRC_SYS_CD
WHEN NOT MATCHED THEN
  INSERT (SRC_SYS_CD, MR_LN_ID, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, MR_LN_SVC_CAT_SK)
  VALUES (S.SRC_SYS_CD, S.MR_LN_ID, S.EFF_DT_SK, S.CRT_RUN_CYC_EXCTN_SK, S.MR_LN_SVC_CAT_SK);
"""
execute_dml(merge_sql_db2_K_MR_LN_SVC_CAT_Load, jdbc_url_ids, jdbc_props_ids)

df_jn_PKEYs = (
    df_lnkFullDataJnIn.alias("lnkFullDataJnIn")
    .join(
        df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
        on=[
            F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD"),
            F.col("lnkFullDataJnIn.MR_LN_ID") == F.col("lnkPKEYxfmOut.MR_LN_ID"),
            F.col("lnkFullDataJnIn.EFF_DT_SK") == F.col("lnkPKEYxfmOut.EFF_DT_SK")
        ],
        how="inner"
    )
    .select(
        F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
        F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
        F.col("lnkPKEYxfmOut.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnkFullDataJnIn.MR_LN_ID").alias("MR_LN_ID"),
        F.col("lnkFullDataJnIn.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnkFullDataJnIn.ACTURL_HLTH_CST_GRP_SUM_ID").alias("ACTURL_HLTH_CST_GRP_SUM_ID"),
        F.col("lnkFullDataJnIn.CST_MDL_UTILITY_ID").alias("CST_MDL_UTILITY_ID"),
        F.col("lnkFullDataJnIn.MR_LN_DESC").alias("MR_LN_DESC"),
        F.col("lnkFullDataJnIn.MR_LN_PFX_DESC").alias("MR_LN_PFX_DESC"),
        F.col("lnkFullDataJnIn.MR_LN_BODY_DESC").alias("MR_LN_BODY_DESC"),
        F.col("lnkFullDataJnIn.MR_LN_SFX_DESC").alias("MR_LN_SFX_DESC"),
        F.col("lnkFullDataJnIn.TERM_DT_SK").alias("TERM_DT_SK")
    )
)

df_seq_MR_LN_SVC_CAT_Pkey = (
    df_jn_PKEYs
    .withColumn("EFF_DT_SK", F.rpad(F.col("EFF_DT_SK"), 10, " "))
    .withColumn("TERM_DT_SK", F.rpad(F.col("TERM_DT_SK"), 10, " "))
    .select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "MR_LN_SVC_CAT_SK",
        "SRC_SYS_CD",
        "MR_LN_ID",
        "EFF_DT_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "SRC_SYS_CD_SK",
        "ACTURL_HLTH_CST_GRP_SUM_ID",
        "CST_MDL_UTILITY_ID",
        "MR_LN_DESC",
        "MR_LN_PFX_DESC",
        "MR_LN_BODY_DESC",
        "MR_LN_SFX_DESC",
        "TERM_DT_SK"
    )
)

write_files(
    df_seq_MR_LN_SVC_CAT_Pkey,
    f"{adls_path}/key/MR_LN_SVC_CAT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)