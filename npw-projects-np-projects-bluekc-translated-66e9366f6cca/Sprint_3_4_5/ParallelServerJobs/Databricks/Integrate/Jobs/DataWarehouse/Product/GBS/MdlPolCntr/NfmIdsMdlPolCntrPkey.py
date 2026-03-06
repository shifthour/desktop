# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC JOb Name : FctsIdsEobAccumPkey
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC Dinakar Soupaty      2018-06-05               5205                 Original Programming                                                                             IntegrateWrhsDevl              Kalyan Neelam             2018-07-05

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Job Name: IdsMdlPolCntrPkey
# MAGIC Audit fields are added into this File
# MAGIC Table K_MDL_POL_CNTR              Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Audit fields are added into this File
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
from pyspark.sql.functions import col, lit, when, coalesce
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

# 1) Read the input dataset (.ds) as Parquet
df_ds_MDL_CNTR_DS_1 = spark.read.parquet(f"{adls_path}/ds/MDLPOLCNTR.{SrcSysCd}.xfrm.{RunID}.parquet")

# 2) Copy stage: produce two output links
df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_MDL_CNTR_DS_1.select(
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("CNTR_ID").alias("CNTR_ID"),
    col("CNTR_EFF_DT").alias("CNTR_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_MDL_CNTR_DS_1.select(
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("POL_NO").alias("POL_NO"),
    col("POLICY_FORM_ID").alias("POLICY_FORM_ID"),
    col("POLICY_FORM_DT").alias("POLICY_FORM_DT"),
    col("CNTR_ID").alias("CNTR_ID"),
    col("MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT"),
    col("CNTR_EFF_DT").alias("CNTR_EFF_DT"),
    col("MDL_DOC_TERM_DT").alias("MDL_DOC_TERM_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CNTR_EFF_ON_GRP_RNWL_IN").alias("CNTR_EFF_ON_GRP_RNWL_IN"),
    col("CNTR_STATUS_ID").alias("CNTR_STATUS_ID"),
    col("CNTR_TERM_DT").alias("CNTR_TERM_DT")
)

# 3) Read reference data from DB2 (IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_KMdlPolCntrExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT MDL_DOC_ID,POL_NO,CNTR_ID,CNTR_EFF_DT,SRC_SYS_CD,MDL_POL_CNTR_SK,CRT_RUN_CYC_EXCTN_SK FROM {IDSOwner}.K_MDL_POL_CNTR ")
    .load()
)

# 4) Remove duplicates
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    ["MDL_DOC_ID", "POL_NO", "CNTR_ID", "CNTR_EFF_DT", "SRC_SYS_CD"],
    []
)

# 5) Join: rdp_NaturalKeys left-joined with df_db2_KMdlPolCntrExt
df_jn_MDL_POL_CNTR = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_KMdlPolCntrExt.alias("lnkKMdllCntrExt"),
    (
        (col("lnkRemDupDataOut.MDL_DOC_ID") == col("lnkKMdllCntrExt.MDL_DOC_ID")) &
        (col("lnkRemDupDataOut.POL_NO") == col("lnkKMdllCntrExt.POL_NO")) &
        (col("lnkRemDupDataOut.CNTR_ID") == col("lnkKMdllCntrExt.CNTR_ID")) &
        (col("lnkRemDupDataOut.CNTR_EFF_DT") == col("lnkKMdllCntrExt.CNTR_EFF_DT")) &
        (col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnkKMdllCntrExt.SRC_SYS_CD"))
    ),
    "left"
)

df_jn_MDL_POL_CNTR = df_jn_MDL_POL_CNTR.select(
    col("lnkRemDupDataOut.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkRemDupDataOut.POL_NO").alias("POL_NO"),
    col("lnkRemDupDataOut.CNTR_ID").alias("CNTR_ID"),
    col("lnkRemDupDataOut.CNTR_EFF_DT").alias("CNTR_EFF_DT"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkKMdllCntrExt.MDL_POL_CNTR_SK").alias("MDL_POL_CNTR_SK"),
    col("lnkKMdllCntrExt.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

# 6) Transformer logic to generate new Surrogate Key where needed
df_xfm_PKEYgen_1 = df_jn_MDL_POL_CNTR.withColumn(
    "MDL_POL_CNTR_SK_before", coalesce(col("MDL_POL_CNTR_SK"), lit(0))
).withColumn(
    "svMdlPolCntrSK", col("MDL_POL_CNTR_SK_before")
)

df_enriched = df_xfm_PKEYgen_1
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svMdlPolCntrSK",<schema>,<secret_name>)

df_xfm_PKEYgen_2 = df_enriched.withColumn(
    "out_CRT_RUN_CYC_EXCTN_SK",
    when(col("MDL_POL_CNTR_SK_before") == lit(0), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "out_LAST_UPDT_RUN_CYC_EXCTN_SK", lit(IDSRunCycle)
)

df_xfm_PKEYgen_lnk_KMdlPolCntr_New = df_xfm_PKEYgen_2.filter(
    col("MDL_POL_CNTR_SK_before") == 0
).select(
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("CNTR_ID"),
    col("CNTR_EFF_DT"),
    col("SRC_SYS_CD"),
    col("svMdlPolCntrSK").alias("MDL_POL_CNTR_SK"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_xfm_PKEYgen_lnkPKEYxfmOut = df_xfm_PKEYgen_2.select(
    col("svMdlPolCntrSK").alias("MDL_POL_AMNDMNT_SK"),
    col("MDL_DOC_ID"),
    col("POL_NO"),
    col("CNTR_ID"),
    col("CNTR_EFF_DT"),
    col("out_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("out_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD")
)

# 7) Write new (insert-only) records to IDS.K_MDL_POL_CNTR via MERGE
execute_dml(f"DROP TABLE IF EXISTS STAGING.NfmIdsMdlPolCntrPkey_db2_K_MdlPolCntrLoad_temp", jdbc_url, jdbc_props)

df_xfm_PKEYgen_lnk_KMdlPolCntr_New.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.NfmIdsMdlPolCntrPkey_db2_K_MdlPolCntrLoad_temp") \
    .mode("overwrite") \
    .save()

merge_sql_db2_K_MdlPolCntrLoad = f"""
MERGE INTO {IDSOwner}.K_MDL_POL_CNTR AS T
USING STAGING.NfmIdsMdlPolCntrPkey_db2_K_MdlPolCntrLoad_temp AS S
ON 
    T.MDL_DOC_ID = S.MDL_DOC_ID AND
    T.POL_NO = S.POL_NO AND
    T.CNTR_ID = S.CNTR_ID AND
    T.CNTR_EFF_DT = S.CNTR_EFF_DT AND
    T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    UPDATE SET
      MDL_DOC_ID = T.MDL_DOC_ID
WHEN NOT MATCHED THEN
    INSERT (MDL_DOC_ID, POL_NO, CNTR_ID, CNTR_EFF_DT, SRC_SYS_CD, MDL_POL_CNTR_SK, CRT_RUN_CYC_EXCTN_SK)
    VALUES (S.MDL_DOC_ID, S.POL_NO, S.CNTR_ID, S.CNTR_EFF_DT, S.SRC_SYS_CD, S.MDL_POL_CNTR_SK, S.CRT_RUN_CYC_EXCTN_SK);
"""

execute_dml(merge_sql_db2_K_MdlPolCntrLoad, jdbc_url, jdbc_props)

# 8) Join for final PKEY output
df_jn_PKEYs = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    (
        (col("lnkFullDataJnIn.MDL_DOC_ID") == col("lnkPKEYxfmOut.MDL_DOC_ID")) &
        (col("lnkFullDataJnIn.POL_NO") == col("lnkPKEYxfmOut.POL_NO")) &
        (col("lnkFullDataJnIn.CNTR_ID") == col("lnkPKEYxfmOut.CNTR_ID")) &
        (col("lnkFullDataJnIn.CNTR_EFF_DT") == col("lnkPKEYxfmOut.CNTR_EFF_DT")) &
        (col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"))
    ),
    "inner"
)

df_jn_PKEYs = df_jn_PKEYs.select(
    col("lnkPKEYxfmOut.MDL_POL_AMNDMNT_SK").alias("MDL_POL_CNTR_SK"),
    col("lnkFullDataJnIn.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkFullDataJnIn.POL_NO").alias("POL_NO"),
    col("lnkFullDataJnIn.CNTR_ID").alias("CNTR_ID"),
    col("lnkFullDataJnIn.CNTR_EFF_DT").alias("CNTR_EFF_DT"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.CNTR_EFF_ON_GRP_RNWL_IN").alias("CNTR_EFF_ON_GRP_RNWL_IN"),
    col("lnkFullDataJnIn.CNTR_STATUS_ID").alias("CNTRSTTUS_ID"),
    col("lnkFullDataJnIn.CNTR_TERM_DT").alias("CNTRTERM_DT"),
    col("lnkFullDataJnIn.MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT")
)

df_final = df_jn_PKEYs.select(
    "MDL_POL_CNTR_SK",
    "MDL_DOC_ID",
    "POL_NO",
    "CNTR_ID",
    "CNTR_EFF_DT",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CNTR_EFF_ON_GRP_RNWL_IN",
    "CNTRSTTUS_ID",
    "CNTRTERM_DT",
    "MDL_DOC_EFF_DT"
)

# 9) Write the final sequential file (.dat)
write_files(
    df_final,
    f"{adls_path}/key/MDLPOLCNTR.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)