# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2016 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING: Generates Primary Key for MBR_DNTL_RWRD_ACCUM table
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ========================================================================================================================================
# MAGIC 												DATASTAGE	CODE		DATE OF
# MAGIC DEVELOPER	DATE		PROJECT	DESCRIPTION					ENVIRONMENT	REVIEWER	REVIEW
# MAGIC ========================================================================================================================================
# MAGIC Abhiram Dasarathy	2016-11-29	5217 - Dental	Initial Progamming - Dental Reward Accumulators		IntegrateDev2         Kalyan Neelam        2016-12-02

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read db2_K_MBR_DNTL_RWRD_ACCUM Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
# COMMAND ----------
# MAGIC %run ../../../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
SrcSysCd = get_widget_value("SrcSysCd","FACETS")
IDSRunCycle = get_widget_value("IDSRunCycle","100")
RunID = get_widget_value("RunID","100")

# 1) Read PxDataSet "MBR_DNTL_RWRD_ACCUM_xfrm" from parquet
df_MBR_DNTL_RWRD_ACCUM_xfrm = spark.read.parquet(f"{adls_path}/ds/MBR_DNTL_RWRD_ACCUM.xfrm.{RunID}.parquet")

# 2) Stage cp_pk splits into two outputs
df_cp_pk_lnk_Transforms_Out = df_MBR_DNTL_RWRD_ACCUM_xfrm.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_cp_pk_Lnk_cp_Out = df_MBR_DNTL_RWRD_ACCUM_xfrm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYCLE_TS").alias("FIRST_RECYCLE_TS"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("PLN_YR_END_DT").alias("PLN_YR_END_DT"),
    F.col("ANUL_PD_AMT").alias("ANUL_PD_AMT"),
    F.col("ANUL_PD_RWRD_THRSHLD_AMT").alias("ANUL_PD_RWRD_THRSHLD_AMT"),
    F.col("AVLBL_RWRD_AMT").alias("AVLBL_RWRD_AMT"),
    F.col("AVLBL_BNS_AMT").alias("AVLBL_BNS_AMT"),
    F.col("USE_RWRD_AMT").alias("USE_RWRD_AMT"),
    F.col("USE_BNS_AMT").alias("USE_BNS_AMT"),
    F.col("OUT_OF_NTWK_CLM_CT").alias("OUT_OF_NTWK_CLM_CT")
)

# 3) Stage rdup_Natural_Keys (PxRemDup)
df_rdup_Natural_Keys = dedup_sort(
    df_cp_pk_lnk_Transforms_Out,
    ["MBR_UNIQ_KEY","YR_NO","SRC_SYS_CD"],
    []
)

# 4) Stage db2_K_MBR_DNTL_RWRD_ACCUM_in (DB2ConnectorPX) reading from IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_ids = (
    f"SELECT MBR_UNIQ_KEY, YR_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, MBR_DNTL_RWRD_ACCUM_SK "
    f"FROM {IDSOwner}.K_MBR_DNTL_RWRD_ACCUM"
)
df_db2_K_MBR_DNTL_RWRD_ACCUM_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ids)
    .load()
)

# 5) Stage jn_MbrDntlRwrdAccum (leftouterjoin)
df_jn_MbrDntlRwrdAccum_tmp = df_rdup_Natural_Keys.alias("lnk_Natural_Keys_out").join(
    df_db2_K_MBR_DNTL_RWRD_ACCUM_in.alias("lnk_KMbrDntlRwrdAccumPkey_out"),
    on=[
        F.col("lnk_Natural_Keys_out.MBR_UNIQ_KEY") == F.col("lnk_KMbrDntlRwrdAccumPkey_out.MBR_UNIQ_KEY"),
        F.col("lnk_Natural_Keys_out.YR_NO") == F.col("lnk_KMbrDntlRwrdAccumPkey_out.YR_NO"),
        F.col("lnk_Natural_Keys_out.SRC_SYS_CD") == F.col("lnk_KMbrDntlRwrdAccumPkey_out.SRC_SYS_CD")
    ],
    how="left"
)
df_jn_MBrDntlRwrdAccum = df_jn_MbrDntlRwrdAccum_tmp.select(
    F.col("lnk_Natural_Keys_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("lnk_Natural_Keys_out.YR_NO").alias("YR_NO"),
    F.col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KMbrDntlRwrdAccumPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KMbrDntlRwrdAccumPkey_out.MBR_DNTL_RWRD_ACCUM_SK").alias("MBR_DNTL_RWRD_ACCUM_SK")
)

# 6) Stage xfrm_PKEYgen (CTransformerStage)
df_enriched = df_jn_MBrDntlRwrdAccum.withColumn("orig_MBR_DNTL_RWRD_ACCUM_SK", F.col("MBR_DNTL_RWRD_ACCUM_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MBR_DNTL_RWRD_ACCUM_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "svRunCyle",
    F.when(F.col("orig_MBR_DNTL_RWRD_ACCUM_SK").isNull(), F.lit(IDSRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

df_xfrm_PKEYgen_lnk_Pkey_out = df_enriched.select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_DNTL_RWRD_ACCUM_SK").alias("MBR_DNTL_RWRD_ACCUM_SK")
)

df_xfrm_PKEYgen_lnk_KMbrDntlRwrdAccum = df_enriched.filter(
    F.col("orig_MBR_DNTL_RWRD_ACCUM_SK").isNull()
).select(
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("YR_NO").alias("YR_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MBR_DNTL_RWRD_ACCUM_SK").alias("MBR_DNTL_RWRD_ACCUM_SK")
)

# 7) Stage db2_K_MBR_DNTL_RWRD_ACCUM_Load (DB2ConnectorPX) - merge logic
temp_table = "STAGING.IdsMbrDntlRwrdAccumExtrPkey_db2_K_MBR_DNTL_RWRD_ACCUM_Load_temp"
spark.sql(f"DROP TABLE IF EXISTS {temp_table}")
(
    df_xfrm_PKEYgen_lnk_KMbrDntlRwrdAccum.write
    .format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("dbtable", temp_table)
    .mode("overwrite")
    .save()
)
merge_sql = f"""
MERGE INTO {IDSOwner}.K_MBR_DNTL_RWRD_ACCUM AS T
USING {temp_table} AS S
ON
    T.MBR_UNIQ_KEY = S.MBR_UNIQ_KEY
    AND T.YR_NO = S.YR_NO
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN NOT MATCHED THEN
    INSERT (MBR_UNIQ_KEY, YR_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, MBR_DNTL_RWRD_ACCUM_SK)
    VALUES (S.MBR_UNIQ_KEY, S.YR_NO, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.MBR_DNTL_RWRD_ACCUM_SK);
"""
execute_dml(merge_sql, jdbc_url_ids, jdbc_props_ids)

# 8) Stage jn_PKey (innerjoin)
df_jn_PKey_tmp = df_cp_pk_Lnk_cp_Out.alias("Lnk_cp_Out").join(
    df_xfrm_PKEYgen_lnk_Pkey_out.alias("lnk_Pkey_out"),
    on=[
        F.col("Lnk_cp_Out.MBR_UNIQ_KEY") == F.col("lnk_Pkey_out.MBR_UNIQ_KEY"),
        F.col("Lnk_cp_Out.YR_NO") == F.col("lnk_Pkey_out.YR_NO"),
        F.col("Lnk_cp_Out.SRC_SYS_CD") == F.col("lnk_Pkey_out.SRC_SYS_CD")
    ],
    how="inner"
)
df_jn_PKey = df_jn_PKey_tmp.select(
    F.col("Lnk_cp_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("Lnk_cp_Out.FIRST_RECYCLE_TS").alias("FIRST_RECYCLE_TS"),
    F.col("lnk_Pkey_out.MBR_DNTL_RWRD_ACCUM_SK").alias("MBR_DNTL_RWRD_ACCUM_SK"),
    F.col("Lnk_cp_Out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Lnk_cp_Out.YR_NO").alias("YR_NO"),
    F.col("Lnk_cp_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_cp_Out.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_cp_Out.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_cp_Out.PLN_YR_EFF_DT").alias("PLN_YR_EFF_DT"),
    F.col("Lnk_cp_Out.PLN_YR_END_DT").alias("PLN_YR_END_DT"),
    F.col("Lnk_cp_Out.ANUL_PD_AMT").alias("ANUL_PD_AMT"),
    F.col("Lnk_cp_Out.ANUL_PD_RWRD_THRSHLD_AMT").alias("ANUL_PD_RWRD_THRSHLD_AMT"),
    F.col("Lnk_cp_Out.AVLBL_RWRD_AMT").alias("AVLBL_RWRD_AMT"),
    F.col("Lnk_cp_Out.AVLBL_BNS_AMT").alias("AVLBL_BNS_AMT"),
    F.col("Lnk_cp_Out.USE_RWRD_AMT").alias("USE_RWRD_AMT"),
    F.col("Lnk_cp_Out.USE_BNS_AMT").alias("USE_BNS_AMT"),
    F.col("Lnk_cp_Out.OUT_OF_NTWK_CLM_CT").alias("OUT_OF_NTWK_CLM_CT")
)

# 9) Stage seq_MBR_DNTL_RWRD_ACCUM (PxSequentialFile) - write out
write_files(
    df_jn_PKey.select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYCLE_TS",
        "MBR_DNTL_RWRD_ACCUM_SK",
        "MBR_UNIQ_KEY",
        "YR_NO",
        "SRC_SYS_CD",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "GRP_ID",
        "PLN_YR_EFF_DT",
        "PLN_YR_END_DT",
        "ANUL_PD_AMT",
        "ANUL_PD_RWRD_THRSHLD_AMT",
        "AVLBL_RWRD_AMT",
        "AVLBL_BNS_AMT",
        "USE_RWRD_AMT",
        "USE_BNS_AMT",
        "OUT_OF_NTWK_CLM_CT"
    ),
    f"{adls_path}/key/MBR_DNTL_RWRD_ACCUM.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)