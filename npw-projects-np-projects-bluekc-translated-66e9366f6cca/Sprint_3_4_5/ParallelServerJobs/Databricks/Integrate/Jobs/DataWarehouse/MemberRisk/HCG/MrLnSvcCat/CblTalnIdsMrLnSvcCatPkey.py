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
# MAGIC Raja Gummadi                  2015-04-22      5460                                Originally Programmed                            IntegrateNewDevl        Kalyan Neelam              2015-04-24

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
from pyspark.sql.functions import col, when, isnull, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

df_ds_MR_LN_SVC_CAT_Xfrm = spark.read.parquet(f"{adls_path}/ds/MR_LN_SVC_CAT.{SrcSysCd}.xfrm.{RunID}.parquet")
df_ds_MR_LN_SVC_CAT_Xfrm = df_ds_MR_LN_SVC_CAT_Xfrm.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "MR_LN_SVC_CAT_SK",
    "MR_LN_ID",
    "EFF_DT_SK",
    "SRC_SYS_CD",
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

df_cpy_MultiStreams_Out_lnkRemDupDataIn = df_ds_MR_LN_SVC_CAT_Xfrm.select(
    col("MR_LN_ID").alias("MR_LN_ID"),
    col("EFF_DT_SK").alias("EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_Out_lnkFullDataJnIn = df_ds_MR_LN_SVC_CAT_Xfrm.select(
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("MR_LN_ID").alias("MR_LN_ID"),
    col("EFF_DT_SK").alias("EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("ACTURL_HLTH_CST_GRP_SUM_ID").alias("ACTURL_HLTH_CST_GRP_SUM_ID"),
    col("CST_MDL_UTILITY_ID").alias("CST_MDL_UTILITY_ID"),
    col("MR_LN_DESC").alias("MR_LN_DESC"),
    col("MR_LN_PFX_DESC").alias("MR_LN_PFX_DESC"),
    col("MR_LN_BODY_DESC").alias("MR_LN_BODY_DESC"),
    col("MR_LN_SFX_DESC").alias("MR_LN_SFX_DESC"),
    col("TERM_DT_SK").alias("TERM_DT_SK")
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_Out_lnkRemDupDataIn,
    ["MR_LN_ID","EFF_DT_SK","SRC_SYS_CD"],
    [("MR_LN_ID","A"),("EFF_DT_SK","A"),("SRC_SYS_CD","A")]
)
df_rdp_NaturalKeys = df_rdp_NaturalKeys.select("MR_LN_ID","EFF_DT_SK","SRC_SYS_CD")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_K_MR_LN_SVC_CAT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT MR_LN_ID, EFF_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, MR_LN_SVC_CAT_SK FROM {IDSOwner}.K_MR_LN_SVC_CAT")
    .load()
)

df_jn_MrLn = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_MR_LN_SVC_CAT_In.alias("Extr"),
    on=[
        col("lnkRemDupDataOut.MR_LN_ID") == col("Extr.MR_LN_ID"),
        col("lnkRemDupDataOut.EFF_DT_SK") == col("Extr.EFF_DT_SK"),
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("Extr.SRC_SYS_CD")
    ],
    how="left"
).select(
    col("lnkRemDupDataOut.MR_LN_ID").alias("MR_LN_ID"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkRemDupDataOut.EFF_DT_SK").alias("EFF_DT_SK"),
    col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Extr.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK")
)

df_enriched = df_jn_MrLn.select(
    col("MR_LN_ID").alias("MR_LN_ID_original"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD_original"),
    col("EFF_DT_SK").alias("EFF_DT_SK_original"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK_original"),
    col("MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK_original")
)
df_enriched = df_enriched.withColumn(
    "MR_LN_SVC_CAT_SK_enriched",
    when(isnull(col("MR_LN_SVC_CAT_SK_original")), lit(None)).otherwise(col("MR_LN_SVC_CAT_SK_original"))
)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MR_LN_SVC_CAT_SK_enriched",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK_enriched",
    when(isnull(col("MR_LN_SVC_CAT_SK_original")), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK_original"))
)

df_xfm_PKEYgen_New = df_enriched.where(isnull(col("MR_LN_SVC_CAT_SK_original"))).select(
    col("SRC_SYS_CD_original").alias("SRC_SYS_CD"),
    col("MR_LN_ID_original").alias("MR_LN_ID"),
    col("EFF_DT_SK_original").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK_enriched").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("MR_LN_SVC_CAT_SK_enriched").alias("MR_LN_SVC_CAT_SK")
)

df_xfm_PKEYgen_lnkPKEYxfmOut = df_enriched.select(
    col("SRC_SYS_CD_original").alias("SRC_SYS_CD"),
    col("MR_LN_ID_original").alias("MR_LN_ID"),
    col("EFF_DT_SK_original").alias("EFF_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK_enriched").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MR_LN_SVC_CAT_SK_enriched").alias("MR_LN_SVC_CAT_SK")
)

df_db2_K_MR_LN_SVC_CAT_Load = df_xfm_PKEYgen_New
temp_table_db2_K_MR_LN_SVC_CAT_Load = "STAGING.CblTalnIdsMrLnSvcCatPkey_db2_K_MR_LN_SVC_CAT_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_db2_K_MR_LN_SVC_CAT_Load}", jdbc_url, jdbc_props)
df_db2_K_MR_LN_SVC_CAT_Load.write.jdbc(
    url=jdbc_url,
    table=temp_table_db2_K_MR_LN_SVC_CAT_Load,
    mode="overwrite",
    properties=jdbc_props
)
merge_sql_db2_K_MR_LN_SVC_CAT_Load = f"""
MERGE INTO {IDSOwner}.K_MR_LN_SVC_CAT AS T
USING {temp_table_db2_K_MR_LN_SVC_CAT_Load} AS S
ON
   T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.MR_LN_ID = S.MR_LN_ID
   AND T.EFF_DT_SK = S.EFF_DT_SK
WHEN MATCHED THEN UPDATE SET
   T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
   T.MR_LN_SVC_CAT_SK = S.MR_LN_SVC_CAT_SK
WHEN NOT MATCHED THEN INSERT
   (SRC_SYS_CD, MR_LN_ID, EFF_DT_SK, CRT_RUN_CYC_EXCTN_SK, MR_LN_SVC_CAT_SK)
   VALUES
   (S.SRC_SYS_CD, S.MR_LN_ID, S.EFF_DT_SK, S.CRT_RUN_CYC_EXCTN_SK, S.MR_LN_SVC_CAT_SK);
"""
execute_dml(merge_sql_db2_K_MR_LN_SVC_CAT_Load, jdbc_url, jdbc_props)

df_jn_PKEYs = df_cpy_MultiStreams_Out_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"),
        col("lnkFullDataJnIn.MR_LN_ID") == col("lnkPKEYxfmOut.MR_LN_ID"),
        col("lnkFullDataJnIn.EFF_DT_SK") == col("lnkPKEYxfmOut.EFF_DT_SK")
    ],
    how="inner"
).select(
    col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnkPKEYxfmOut.MR_LN_SVC_CAT_SK").alias("MR_LN_SVC_CAT_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.MR_LN_ID").alias("MR_LN_ID"),
    col("lnkFullDataJnIn.EFF_DT_SK").alias("EFF_DT_SK"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnkFullDataJnIn.ACTURL_HLTH_CST_GRP_SUM_ID").alias("ACTURL_HLTH_CST_GRP_SUM_ID"),
    col("lnkFullDataJnIn.CST_MDL_UTILITY_ID").alias("CST_MDL_UTILITY_ID"),
    col("lnkFullDataJnIn.MR_LN_DESC").alias("MR_LN_DESC"),
    col("lnkFullDataJnIn.MR_LN_PFX_DESC").alias("MR_LN_PFX_DESC"),
    col("lnkFullDataJnIn.MR_LN_BODY_DESC").alias("MR_LN_BODY_DESC"),
    col("lnkFullDataJnIn.MR_LN_SFX_DESC").alias("MR_LN_SFX_DESC"),
    col("lnkFullDataJnIn.TERM_DT_SK").alias("TERM_DT_SK")
)

df_seq_MR_LN_SVC_CAT_Pkey = df_jn_PKEYs.withColumn(
    "EFF_DT_SK",
    rpad(col("EFF_DT_SK"), 10, " ")
).withColumn(
    "TERM_DT_SK",
    rpad(col("TERM_DT_SK"), 10, " ")
).select(
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

write_files(
    df_seq_MR_LN_SVC_CAT_Pkey,
    f"{adls_path}/key/MR_LN_SVC_CAT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)