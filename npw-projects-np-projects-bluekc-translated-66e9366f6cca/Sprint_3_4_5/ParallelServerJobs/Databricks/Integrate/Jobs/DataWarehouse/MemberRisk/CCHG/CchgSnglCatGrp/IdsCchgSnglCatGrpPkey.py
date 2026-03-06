# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING:  This job assigns primary key value using SEQ_K_CCHG_SNGL_CAT_GRP
# MAGIC                                                                 
# MAGIC      
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                  DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                 PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam          2015-08-17           5460                               Initial Programming                                                                              IntegrateDev2            Bhoomi Dasari              8/30/2015

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_CCHG_SNGL_CAT_GRP. Insert new SKEY rows generated as part of this run. This is a database insert operation.
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
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')

# Read from dataset (.ds) as Parquet
df_ds_CCHG_SNGL_CAT_GRP_Xfrm = spark.read.parquet(
    f"{adls_path}/ds/CCHG_SNGL_CAT_GRP.{SrcSysCd}.xfrm.{RunID}.parquet"
)

# cpy_MultiStreams (PxCopy)
df_cpy_MultiStreams_out_lnkRemDupDataIn = df_ds_CCHG_SNGL_CAT_GRP_Xfrm.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cpy_MultiStreams_out_lnkFullDataJnIn = df_ds_CCHG_SNGL_CAT_GRP_Xfrm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("ICD_VRSN_CD").alias("ICD_VRSN_CD"),
    F.col("CCHG_SNGL_CAT_GRP_DESC").alias("CCHG_SNGL_CAT_GRP_DESC"),
    F.col("ICD_DIAG_CLM_MPPNG_DESC").alias("ICD_DIAG_CLM_MPPNG_DESC")
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys_temp = dedup_sort(
    df_cpy_MultiStreams_out_lnkRemDupDataIn,
    ["CCHG_SNGL_CAT_GRP_CD", "SRC_SYS_CD"],
    []
)
df_rdp_NaturalKeys_out_lnkRemDupDataOut = df_rdp_NaturalKeys_temp.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# db2_K_CCHG_SNGL_CAT_GRP_In (DB2ConnectorPX, read from IDS)
ids_jdbc_url, ids_jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_K_CCHG_SNGL_CAT_GRP_In = f"""
SELECT 
CCHG_SNGL_CAT_GRP_CD,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
CCHG_SNGL_CAT_GRP_SK
FROM {IDSOwner}.K_CCHG_SNGL_CAT_GRP
"""
df_db2_K_CCHG_SNGL_CAT_GRP_In = (
    spark.read.format("jdbc")
    .option("url", ids_jdbc_url)
    .options(**ids_jdbc_props)
    .option("query", extract_query_db2_K_CCHG_SNGL_CAT_GRP_In)
    .load()
)

# jn_KCchgSnglCatGrp (PxJoin) - left outer join
df_jn_KCchgSnglCatGrp = df_rdp_NaturalKeys_out_lnkRemDupDataOut.alias("lnkRemDupDataOut").join(
    df_db2_K_CCHG_SNGL_CAT_GRP_In.alias("Extr"),
    on=[
        F.col("lnkRemDupDataOut.CCHG_SNGL_CAT_GRP_CD") == F.col("Extr.CCHG_SNGL_CAT_GRP_CD"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD") == F.col("Extr.SRC_SYS_CD")
    ],
    how="left"
)
df_jn_KCchgSnglCatGrp_out_JoinOut = df_jn_KCchgSnglCatGrp.select(
    F.col("lnkRemDupDataOut.CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Extr.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

# xfm_PKEYgen (CTransformerStage)
df_temp_xfm = df_jn_KCchgSnglCatGrp_out_JoinOut.withColumn(
    "isnull_SK",
    F.col("CCHG_SNGL_CAT_GRP_SK").isNull()
).withColumn(
    "svRunCyle",
    F.when(F.col("isnull_SK"), F.lit(IDSRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
).withColumn(
    "svCchgSnglCatGrpSK",
    F.lit(None).cast(LongType())
)

df_enriched = SurrogateKeyGen(
    df_temp_xfm,
    <DB sequence name>,
    "svCchgSnglCatGrpSK",
    <schema>,
    <secret_name>
)

df_xfm_PKEYgen = df_enriched.withColumn(
    "final_CCHG_SNGL_CAT_GRP_SK",
    F.when(F.col("isnull_SK"), F.col("svCchgSnglCatGrpSK")).otherwise(F.col("CCHG_SNGL_CAT_GRP_SK"))
).withColumn(
    "final_CRT_RUN_CYC_EXCTN_SK",
    F.col("svRunCyle")
).drop("isnull_SK")

df_xfm_PKEYgen_out_New = df_xfm_PKEYgen.filter(
    F.col("CCHG_SNGL_CAT_GRP_SK").isNull()
).select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("final_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("final_CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

df_xfm_PKEYgen_out_lnkPKEYxfmOut = df_xfm_PKEYgen.select(
    F.col("CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("final_CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("final_CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK")
)

# db2_K_CCHG_SNGL_CAT_GRP_Load (DB2ConnectorPX) - Insert (Append)
jobName = "IdsCchgSnglCatGrpPkey"
staging_table_db2_K_CCHG = f"STAGING.{jobName}_db2_K_CCHG_SNGL_CAT_GRP_Load_temp"

execute_dml(
    f"DROP TABLE IF EXISTS {staging_table_db2_K_CCHG}",
    ids_jdbc_url,
    ids_jdbc_props
)

df_xfm_PKEYgen_out_New.select(
    "CCHG_SNGL_CAT_GRP_CD",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "CCHG_SNGL_CAT_GRP_SK"
).write.format("jdbc") \
    .option("url", ids_jdbc_url) \
    .options(**ids_jdbc_props) \
    .option("dbtable", staging_table_db2_K_CCHG) \
    .mode("overwrite") \
    .save()

merge_sql_db2_K_CCHG_SNGL_CAT_GRP_Load = f"""
MERGE INTO {IDSOwner}.K_CCHG_SNGL_CAT_GRP AS T
USING {staging_table_db2_K_CCHG} AS S
ON T.CCHG_SNGL_CAT_GRP_CD = S.CCHG_SNGL_CAT_GRP_CD
AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN NOT MATCHED THEN
INSERT (CCHG_SNGL_CAT_GRP_CD, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CCHG_SNGL_CAT_GRP_SK)
VALUES (S.CCHG_SNGL_CAT_GRP_CD, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.CCHG_SNGL_CAT_GRP_SK);
"""
execute_dml(merge_sql_db2_K_CCHG_SNGL_CAT_GRP_Load, ids_jdbc_url, ids_jdbc_props)

# jn_PKEYs (PxJoin) - inner join
df_jn_PKEYs = df_cpy_MultiStreams_out_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_out_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_CD") == F.col("lnkPKEYxfmOut.CCHG_SNGL_CAT_GRP_CD"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD") == F.col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
)
df_jn_PKEYs_out_Pkey_Out = df_jn_PKEYs.select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkPKEYxfmOut.CCHG_SNGL_CAT_GRP_SK").alias("CCHG_SNGL_CAT_GRP_SK"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_CD").alias("CCHG_SNGL_CAT_GRP_CD"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.ICD_VRSN_CD").alias("ICD_VRSN_CD"),
    F.col("lnkFullDataJnIn.CCHG_SNGL_CAT_GRP_DESC").alias("CCHG_SNGL_CAT_GRP_DESC"),
    F.col("lnkFullDataJnIn.ICD_DIAG_CLM_MPPNG_DESC").alias("ICD_DIAG_CLM_MPPNG_DESC")
)

# seq_CCHG_SNGL_CAT_GRP_Pkey (PxSequentialFile) - write .dat
write_files(
    df_jn_PKEYs_out_Pkey_Out.select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "CCHG_SNGL_CAT_GRP_SK",
        "CCHG_SNGL_CAT_GRP_CD",
        "SRC_SYS_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ICD_VRSN_CD",
        "CCHG_SNGL_CAT_GRP_DESC",
        "ICD_DIAG_CLM_MPPNG_DESC"
    ),
    f"{adls_path}/key/CCHG_SNGL_CAT_GRP.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)