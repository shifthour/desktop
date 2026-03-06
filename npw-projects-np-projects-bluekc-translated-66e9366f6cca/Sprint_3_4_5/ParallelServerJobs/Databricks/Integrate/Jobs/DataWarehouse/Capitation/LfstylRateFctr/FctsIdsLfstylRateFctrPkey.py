# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY: FctsIdsLfstylRateFctrCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                                                              Change Description                                                                                        Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------                                         ---------------------------------------------------------                                                                           ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Manasa Andru                 2018-08-06        60037 - Attribution/Capitation Support                      Original Programming                                                                                                IntegrateDev2            Abhiram Dasarathy	2018-08-24
# MAGIC                                                                                                                              (Primary Key Process for Ids LFSTYL_RATE_FCTR table.
# MAGIC                                                                                                                        Performs a lookup and loads the IDS - K_LFSTYL_RATE_FCTR table)

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_LFSTYL_RATE_FCTR.Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Left Outer Join Here
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','100')
RunID = get_widget_value('RunID','20180806')

# Read ds_Lfstyl_Rate_Fctr (PxDataSet) as parquet
df_ds_Lfstyl_Rate_Fctr = spark.read.parquet(f"{adls_path}/ds/LFSTYL_RATE_FCTR.LfstylRateFctr.extr.parquet")
df_ds_Lfstyl_Rate_Fctr = df_ds_Lfstyl_Rate_Fctr.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("LFSTYL_RATE_FCTR_SK"),
    F.col("LFSTYL_RATE_FCTR_ID"),
    F.col("MBR_LFSTYL_BNF_CD"),
    F.col("LFSTYL_RATE_FCTR_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_LFSTYL_BNF_CD_SK"),
    F.col("LFSTYL_RATE_FCTR_TERM_DT"),
    F.col("LFSTYL_RATE_FCTR")
)

# cpy_MultiStreams (PxCopy) outputs
df_lnkRemDupDataIn = df_ds_Lfstyl_Rate_Fctr.select(
    F.col("LFSTYL_RATE_FCTR_ID"),
    F.col("MBR_LFSTYL_BNF_CD"),
    F.col("LFSTYL_RATE_FCTR_EFF_DT"),
    F.col("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_ds_Lfstyl_Rate_Fctr.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("LFSTYL_RATE_FCTR_ID"),
    F.col("MBR_LFSTYL_BNF_CD"),
    F.col("LFSTYL_RATE_FCTR_EFF_DT"),
    F.col("SRC_SYS_CD"),
    F.col("MBR_LFSTYL_BNF_CD_SK"),
    F.col("LFSTYL_RATE_FCTR_TERM_DT"),
    F.col("LFSTYL_RATE_FCTR")
)

# rdp_NaturalKeys (PxRemDup)
df_lnkRemDupDataOut = dedup_sort(
    df_lnkRemDupDataIn,
    ["LFSTYL_RATE_FCTR_ID","MBR_LFSTYL_BNF_CD","LFSTYL_RATE_FCTR_EFF_DT","SRC_SYS_CD"],
    []
).select(
    F.col("LFSTYL_RATE_FCTR_ID"),
    F.col("MBR_LFSTYL_BNF_CD"),
    F.col("LFSTYL_RATE_FCTR_EFF_DT"),
    F.col("SRC_SYS_CD")
)

# db2_K_LfstylRateFctr_In (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT LFSTYL_RATE_FCTR_ID, MBR_LFSTYL_BNF_CD, LFSTYL_RATE_FCTR_EFF_DT, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, LFSTYL_RATE_FCTR_SK FROM {IDSOwner}.K_LFSTYL_RATE_FCTR"
df_lnk_KLfstylRateFctrExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# jn_MbrWebRgstrn (PxJoin - left join)
df_jn_MbrWebRgstrn = df_lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_lnk_KLfstylRateFctrExtr.alias("lnk_KLfstylRateFctrExtr"),
    [
        F.col("lnkRemDupDataOut.LFSTYL_RATE_FCTR_ID") == F.col("lnk_KLfstylRateFctrExtr.LFSTYL_RATE_FCTR_ID"),
        F.col("lnkRemDupDataOut.MBR_LFSTYL_BNF_CD") == F.col("lnk_KLfstylRateFctrExtr.MBR_LFSTYL_BNF_CD"),
        F.col("lnkRemDupDataOut.LFSTYL_RATE_FCTR_EFF_DT") == F.col("lnk_KLfstylRateFctrExtr.LFSTYL_RATE_FCTR_EFF_DT"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("lnk_KLfstylRateFctrExtr.SRC_SYS_CD")
    ],
    how="left"
)

df_lnk_LfstylRateFctr_JoinOut = df_jn_MbrWebRgstrn.select(
    F.col("lnkRemDupDataOut.LFSTYL_RATE_FCTR_ID").alias("LFSTYL_RATE_FCTR_ID"),
    F.col("lnkRemDupDataOut.MBR_LFSTYL_BNF_CD").alias("MBR_LFSTYL_BNF_CD"),
    F.col("lnkRemDupDataOut.LFSTYL_RATE_FCTR_EFF_DT").alias("LFSTYL_RATE_FCTR_EFF_DT"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KLfstylRateFctrExtr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KLfstylRateFctrExtr.LFSTYL_RATE_FCTR_SK").alias("LFSTYL_RATE_FCTR_SK")
)

# xfm_PKEYgen (CTransformerStage)
df_enriched = df_lnk_LfstylRateFctr_JoinOut
df_enriched = df_enriched.withColumn("svLfstylRateFctrSK", F.col("LFSTYL_RATE_FCTR_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svLfstylRateFctrSK",<schema>,<secret_name>)

df_lnk_KLfstylRateFctr_new = df_enriched.filter(
    F.col("LFSTYL_RATE_FCTR_SK").isNull()
).select(
    F.col("LFSTYL_RATE_FCTR_ID").alias("LFSTYL_RATE_FCTR_ID"),
    F.col("MBR_LFSTYL_BNF_CD").alias("MBR_LFSTYL_BNF_CD"),
    F.col("LFSTYL_RATE_FCTR_EFF_DT").alias("LFSTYL_RATE_FCTR_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("svLfstylRateFctrSK").alias("LFSTYL_RATE_FCTR_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    F.col("LFSTYL_RATE_FCTR_ID").alias("LFSTYL_RATE_FCTR_ID"),
    F.col("MBR_LFSTYL_BNF_CD").alias("MBR_LFSTYL_BNF_CD"),
    F.col("LFSTYL_RATE_FCTR_EFF_DT").alias("LFSTYL_RATE_FCTR_EFF_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.when(F.col("LFSTYL_RATE_FCTR_SK").isNull(), F.lit(IDSRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svLfstylRateFctrSK").alias("LFSTYL_RATE_FCTR_SK")
)

# db2_K_LfstylRateFctr_Load (DB2ConnectorPX) - Merge (Upsert)
drop_sql = "DROP TABLE IF EXISTS STAGING.FctsIdsLfstylRateFctrPkey_db2_K_LfstylRateFctr_Load_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_lnk_KLfstylRateFctr_new.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.FctsIdsLfstylRateFctrPkey_db2_K_LfstylRateFctr_Load_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_LFSTYL_RATE_FCTR AS T
USING STAGING.FctsIdsLfstylRateFctrPkey_db2_K_LfstylRateFctr_Load_temp AS S
ON T.LFSTYL_RATE_FCTR_ID=S.LFSTYL_RATE_FCTR_ID
 AND T.MBR_LFSTYL_BNF_CD=S.MBR_LFSTYL_BNF_CD
 AND T.LFSTYL_RATE_FCTR_EFF_DT=S.LFSTYL_RATE_FCTR_EFF_DT
 AND T.SRC_SYS_CD=S.SRC_SYS_CD
WHEN MATCHED THEN UPDATE SET
 T.CRT_RUN_CYC_EXCTN_SK=S.CRT_RUN_CYC_EXCTN_SK,
 T.LFSTYL_RATE_FCTR_SK=S.LFSTYL_RATE_FCTR_SK
WHEN NOT MATCHED THEN INSERT (
  LFSTYL_RATE_FCTR_ID,
  MBR_LFSTYL_BNF_CD,
  LFSTYL_RATE_FCTR_EFF_DT,
  SRC_SYS_CD,
  CRT_RUN_CYC_EXCTN_SK,
  LFSTYL_RATE_FCTR_SK
) VALUES (
  S.LFSTYL_RATE_FCTR_ID,
  S.MBR_LFSTYL_BNF_CD,
  S.LFSTYL_RATE_FCTR_EFF_DT,
  S.SRC_SYS_CD,
  S.CRT_RUN_CYC_EXCTN_SK,
  S.LFSTYL_RATE_FCTR_SK
);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# jn_PKEYs (PxJoin)
df_jn_PKEYs = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        F.col("lnkFullDataJnIn.LFSTYL_RATE_FCTR_ID") == F.col("lnkPKEYxfmOut.LFSTYL_RATE_FCTR_ID"),
        F.col("lnkFullDataJnIn.MBR_LFSTYL_BNF_CD") == F.col("lnkPKEYxfmOut.MBR_LFSTYL_BNF_CD"),
        F.col("lnkFullDataJnIn.LFSTYL_RATE_FCTR_EFF_DT") == F.col("lnkPKEYxfmOut.LFSTYL_RATE_FCTR_EFF_DT"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
)

df_Lnk_IdsLfstylRateFctrPkey_OutAbc = df_jn_PKEYs.select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkFullDataJnIn.MBR_LFSTYL_BNF_CD_SK").alias("MBR_LFSTYL_BNF_CD_SK"),
    F.col("lnkFullDataJnIn.LFSTYL_RATE_FCTR_TERM_DT").alias("LFSTYL_RATE_FCTR_TERM_DT"),
    F.col("lnkFullDataJnIn.LFSTYL_RATE_FCTR").alias("LFSTYL_RATE_FCTR"),
    F.col("lnkFullDataJnIn.LFSTYL_RATE_FCTR_ID").alias("LFSTYL_RATE_FCTR_ID"),
    F.col("lnkFullDataJnIn.MBR_LFSTYL_BNF_CD").alias("MBR_LFSTYL_BNF_CD"),
    F.col("lnkFullDataJnIn.LFSTYL_RATE_FCTR_EFF_DT").alias("LFSTYL_RATE_FCTR_EFF_DT"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LFSTYL_RATE_FCTR_SK").alias("LFSTYL_RATE_FCTR_SK")
)

# seq_LfstylRateFctr_PKEY (PxSequentialFile) write
write_files(
    df_Lnk_IdsLfstylRateFctrPkey_OutAbc,
    f"{adls_path}/key/LFSTYLRATEFCTR.FACETS.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)