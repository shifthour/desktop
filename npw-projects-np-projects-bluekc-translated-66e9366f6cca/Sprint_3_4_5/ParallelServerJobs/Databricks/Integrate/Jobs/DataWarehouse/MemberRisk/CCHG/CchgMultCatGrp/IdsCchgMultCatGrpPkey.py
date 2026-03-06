# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING:  This job assigns primary key value using SEQ_K_CCHG_MULT_CAT_GRP
# MAGIC                                                                 
# MAGIC      
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam          2015-08-17           5460                            Original Programming                                                                             IntegrateDev2            Bhoomi Dasari              8/30/2015

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_CCHG_MULT_CAT_GRP. Insert new SKEY rows generated as part of this run. This is a database insert operation.
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
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Parameter Definitions
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')

# Read from PxDataSet (CCHG_MULT_CAT_GRP.#SrcSysCd#.xfrm.#RunID#.ds -> parquet)
df_ds_CCHG_MULT_CAT_GRP_Xfrm = (
    spark.read.format("parquet")
    .load(f"{adls_path}/ds/CCHG_MULT_CAT_GRP.{SrcSysCd}.xfrm.{RunID}.parquet")
)

# Copy Stage (cpy_MultiStreams) - Split into two output streams
df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_CCHG_MULT_CAT_GRP_Xfrm.select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_CCHG_MULT_CAT_GRP_Xfrm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CCHG_SNGL_CAT_GRP_1").alias("CCHG_SNGL_CAT_GRP_1"),
    F.col("CCHG_SNGL_CAT_GRP_2").alias("CCHG_SNGL_CAT_GRP_2"),
    F.col("CCHG_SNGL_CAT_GRP_3").alias("CCHG_SNGL_CAT_GRP_3"),
    F.col("CCHG_SNGL_CAT_GRP_4").alias("CCHG_SNGL_CAT_GRP_4"),
    F.col("CCHG_SNGL_CAT_GRP_5").alias("CCHG_SNGL_CAT_GRP_5"),
    F.col("CCHG_SNGL_CAT_GRP_6").alias("CCHG_SNGL_CAT_GRP_6"),
    F.col("CCHG_SNGL_CAT_GRP_7").alias("CCHG_SNGL_CAT_GRP_7"),
    F.col("CCHG_SNGL_CAT_GRP_8").alias("CCHG_SNGL_CAT_GRP_8"),
    F.col("CCHG_SNGL_CAT_GRP_9").alias("CCHG_SNGL_CAT_GRP_9"),
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("CCHG_MULT_CAT_GRP_DESC").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.col("CCHG_SNGL_CAT_GRP_MPPNG_TX").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX")
)

# Remove Duplicates (rdp_NaturalKeys) - Key columns: CCHG_MULT_CAT_GRP_ID, SRC_SYS_CD
df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    partition_cols=["CCHG_MULT_CAT_GRP_ID","SRC_SYS_CD"],
    sort_cols=[]
)

# DB2 Connector (db2_K_CCHG_MULT_CAT_GRP_In) - Read from IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_db2_K_CCHG_MULT_CAT_GRP_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", f"SELECT CCHG_MULT_CAT_GRP_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,CCHG_MULT_CAT_GRP_SK FROM {IDSOwner}.K_CCHG_MULT_CAT_GRP")
    .load()
)

# Join (jn_KCchgMultCatGrp) - left outer on (CCHG_MULT_CAT_GRP_ID, SRC_SYS_CD)
df_jn_KCchgMultCatGrp_pre = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_CCHG_MULT_CAT_GRP_In.alias("Extr"),
    on=[
        (F.col("lnkRemDupDataOut.CCHG_MULT_CAT_GRP_ID") == F.col("Extr.CCHG_MULT_CAT_GRP_ID")),
        (F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD"))
    ],
    how="left"
)

df_jn_KCchgMultCatGrp = df_jn_KCchgMultCatGrp_pre.select(
    F.col("lnkRemDupDataOut.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Extr.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

# Transformer (xfm_PKEYgen) 
df_xfm_PKEYgen_in = df_jn_KCchgMultCatGrp
df_enriched = df_xfm_PKEYgen_in.withColumn(
    "IS_NEW",
    F.isnull("CCHG_MULT_CAT_GRP_SK")
).withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("IS_NEW"), IDSRunCycle).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

# Generate Surrogate Keys for rows with null CCHG_MULT_CAT_GRP_SK
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CCHG_MULT_CAT_GRP_SK",<schema>,<secret_name>)

# Set LAST_UPDT_RUN_CYC_EXCTN_SK to IDSRunCycle for all
df_enriched = df_enriched.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    F.lit(IDSRunCycle)
)

# "New" Link => rows where original CCHG_MULT_CAT_GRP_SK was null
df_xfm_PKEYgen_out_New = df_enriched.filter("IS_NEW = true").select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

# "lnkPKEYxfmOut" => all rows
df_xfm_PKEYgen_out_lnkPKEYxfmOut = df_enriched.select(
    F.col("CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK")
)

# DB2 Connector (db2_K_CCHG_MULT_CAT_GRP_Load) - Insert new rows into K_CCHG_MULT_CAT_GRP (replicate with MERGE)
df_db2_K_CCHG_MULT_CAT_GRP_Load = df_xfm_PKEYgen_out_New

drop_sql = "DROP TABLE IF EXISTS STAGING.IdsCchgMultCatGrpPkey_db2_K_CCHG_MULT_CAT_GRP_Load_temp"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_db2_K_CCHG_MULT_CAT_GRP_Load.write.jdbc(
    url=jdbc_url,
    table="STAGING.IdsCchgMultCatGrpPkey_db2_K_CCHG_MULT_CAT_GRP_Load_temp",
    mode="overwrite",
    properties=jdbc_props
)

merge_sql = f"""
MERGE INTO {IDSOwner}.K_CCHG_MULT_CAT_GRP AS T
USING STAGING.IdsCchgMultCatGrpPkey_db2_K_CCHG_MULT_CAT_GRP_Load_temp AS S
ON (
  T.CCHG_MULT_CAT_GRP_ID = S.CCHG_MULT_CAT_GRP_ID AND
  T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.CCHG_MULT_CAT_GRP_SK = S.CCHG_MULT_CAT_GRP_SK
WHEN NOT MATCHED THEN
  INSERT (CCHG_MULT_CAT_GRP_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CCHG_MULT_CAT_GRP_SK)
  VALUES (S.CCHG_MULT_CAT_GRP_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.CCHG_MULT_CAT_GRP_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# Join (jn_PKEYs) - inner join on (CCHG_MULT_CAT_GRP_ID, SRC_SYS_CD)
df_jn_PKEYs_pre = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_out_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        (F.col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP_ID") == F.col("lnkPKEYxfmOut.CCHG_MULT_CAT_GRP_ID")),
        (F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD"))
    ],
    how="inner"
)

df_jn_PKEYs = df_jn_PKEYs_pre.select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkPKEYxfmOut.CCHG_MULT_CAT_GRP_SK").alias("CCHG_MULT_CAT_GRP_SK"),
    F.col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP_ID").alias("CCHG_MULT_CAT_GRP_ID"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_1").alias("CCHG_SNGL_CAT_GRP_1"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_2").alias("CCHG_SNGL_CAT_GRP_2"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_3").alias("CCHG_SNGL_CAT_GRP_3"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_4").alias("CCHG_SNGL_CAT_GRP_4"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_5").alias("CCHG_SNGL_CAT_GRP_5"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_6").alias("CCHG_SNGL_CAT_GRP_6"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_7").alias("CCHG_SNGL_CAT_GRP_7"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_8").alias("CCHG_SNGL_CAT_GRP_8"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_9").alias("CCHG_SNGL_CAT_GRP_9"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP_DESC").alias("CCHG_MULT_CAT_GRP_DESC"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_MPPNG_TX").alias("CCHG_SNGL_CAT_GRP_MPPNG_TX")
)

# Sequential File Output (seq_CCHG_MULT_CAT_GRP_Pkey)
write_files(
    df_jn_PKEYs,
    f"{adls_path}/key/CCHG_MULT_CAT_GRP.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)