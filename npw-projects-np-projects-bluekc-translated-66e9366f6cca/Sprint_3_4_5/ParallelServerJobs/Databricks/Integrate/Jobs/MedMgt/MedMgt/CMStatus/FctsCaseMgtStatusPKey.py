# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called By: FctsMedMgtCntl
# MAGIC 
# MAGIC 
# MAGIC Process : Case Mgt Status Pkey process which is processed and loaded to CASE_MGT_STTUS table
# MAGIC 
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                              		  Project/                                                                                                    	Code                  Date
# MAGIC Developer         	Date             	 Altiris #     	Change Description                                                   	Reviewer            Reviewed
# MAGIC -----------------------  	-------------------   	-------------   	-----------------------------------------------------------------------------   	-------------------------  -------------------
# MAGIC Jaideep Mankala         2019-07-09          US115013               Case Mgt Sttus PKey Process         			Abhiram Dasarathy	2019-07-15

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_CASE_MGT_STTS Table to pull the Natural Keys and the Skey.
# MAGIC Inner Join primary key info with table info
# MAGIC Left Join on Natural Keys
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC JobName: IdsProdQhpPkey
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from SBC
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


SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')

# Read the PxDataSet (converted to parquet)
df_ds_CASE_MGT_STS_Xfm = (
    spark.read.parquet(
        f"{adls_path}/ds/CASE_MGT_STTUS.{SrcSysCd}.extr.{RunID}.parquet"
    )
)

# Cp_Pk Stage - Two output links
df_Cp_Pk_CP_Out = df_ds_CASE_MGT_STS_Xfm.select(
    F.col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("USUS_ID").alias("USUS_ID"),
    F.col("CMST_STS").alias("CMST_STS"),
    F.col("CMST_STS_DTM").alias("CMST_STS_DTM"),
    F.col("CMST_MCTR_REAS").alias("CMST_MCTR_REAS"),
    F.col("CMST_USID_ROUTE").alias("CMST_USID_ROUTE")
)

df_Cp_Pk_lnk_Transforms_Out = df_ds_CASE_MGT_STS_Xfm.select(
    F.col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# rdup_Natural_Keys Stage (PxRemDup)
df_rdup_Natural_Keys = dedup_sort(
    df_Cp_Pk_lnk_Transforms_Out,
    ["CASE_MGT_ID", "CASE_MGT_STTUS_SEQ_NO", "SRC_SYS_CD"],
    []
)

# db2_K_CASE_MGT_STTUS_in Stage (DB2ConnectorPX, read from IDS)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    f"SELECT CASE_MGT_ID, CASE_MGT_STTUS_SEQ_NO, SRC_SYS_CD, "
    f"CRT_RUN_CYC_EXCTN_SK, CASE_MGT_STTUS_SK "
    f"FROM {IDSOwner}.K_CASE_MGT_STTUS"
)
df_db2_K_CASE_MGT_STTUS_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# jn_CMST Stage (PxJoin, left outer join)
df_jn_CMST_joined = df_rdup_Natural_Keys.alias("lnk_Natural_Keys_out").join(
    df_db2_K_CASE_MGT_STTUS_in.alias("lnk_KCmStsPkey_out"),
    on=[
        F.col("lnk_Natural_Keys_out.CASE_MGT_ID") == F.col("lnk_KCmStsPkey_out.CASE_MGT_ID"),
        F.col("lnk_Natural_Keys_out.CASE_MGT_STTUS_SEQ_NO") == F.col("lnk_KCmStsPkey_out.CASE_MGT_STTUS_SEQ_NO"),
        F.col("lnk_Natural_Keys_out.SRC_SYS_CD") == F.col("lnk_KCmStsPkey_out.SRC_SYS_CD"),
    ],
    how="left"
)

df_jn_CMST = df_jn_CMST_joined.select(
    F.col("lnk_Natural_Keys_out.CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("lnk_Natural_Keys_out.CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_KCmStsPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_KCmStsPkey_out.CASE_MGT_STTUS_SK").alias("CASE_MGT_STTUS_SK")
)

# xfrm_PKEYgen Stage (CTransformerStage)
df_jn_CMST = df_jn_CMST.withColumn("ORIG_CASE_MGT_STTUS_SK", F.col("CASE_MGT_STTUS_SK"))
df_enriched = df_jn_CMST
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CASE_MGT_STTUS_SK",<schema>,<secret_name>)
df_enriched = df_enriched.withColumn(
    "CRT_RUN_CYC_EXCTN_SK",
    F.when(F.col("ORIG_CASE_MGT_STTUS_SK").isNull(), F.lit(IDSRunCycle))
     .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)
df_enriched = df_enriched.withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(IDSRunCycle))

df_xfrm_PKEYgen_lnk_Pkey_out = df_enriched.select(
    F.col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CASE_MGT_STTUS_SK").alias("CASE_MGT_STTUS_SK")
)

df_xfrm_PKEYgen_lnk_KCmSt_New = df_enriched.filter(
    F.col("ORIG_CASE_MGT_STTUS_SK").isNull()
).select(
    F.col("CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CASE_MGT_STTUS_SK").alias("CASE_MGT_STTUS_SK")
)

# DB2_K_CASE_MGT_STTUS_Load Stage (DB2ConnectorPX, IDS, insert logic => replicate as merge with only insert clause)
temp_table_name = "STAGING.FctsCaseMgtStatusPKey_DB2_K_CASE_MGT_STTUS_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name}", jdbc_url, jdbc_props)
(
    df_xfrm_PKEYgen_lnk_KCmSt_New.write.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", temp_table_name)
    .mode("append")
    .save()
)
merge_sql = f"""
MERGE INTO {IDSOwner}.K_CASE_MGT_STTUS AS T
USING {temp_table_name} AS S
ON T.CASE_MGT_ID = S.CASE_MGT_ID
AND T.CASE_MGT_STTUS_SEQ_NO = S.CASE_MGT_STTUS_SEQ_NO
AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN NOT MATCHED THEN
  INSERT (CASE_MGT_ID, CASE_MGT_STTUS_SEQ_NO, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CASE_MGT_STTUS_SK)
  VALUES (S.CASE_MGT_ID, S.CASE_MGT_STTUS_SEQ_NO, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.CASE_MGT_STTUS_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# jn_PKey Stage (PxJoin, inner join)
df_jn_PKey_joined = df_xfrm_PKEYgen_lnk_Pkey_out.alias("lnk_Pkey_out").join(
    df_Cp_Pk_CP_Out.alias("CP_Out"),
    on=[
        F.col("lnk_Pkey_out.CASE_MGT_ID") == F.col("CP_Out.CASE_MGT_ID"),
        F.col("lnk_Pkey_out.CASE_MGT_STTUS_SEQ_NO") == F.col("CP_Out.CASE_MGT_STTUS_SEQ_NO"),
        F.col("lnk_Pkey_out.SRC_SYS_CD") == F.col("CP_Out.SRC_SYS_CD"),
    ],
    how="inner"
)

df_jn_PKey = df_jn_PKey_joined.select(
    F.col("CP_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("CP_Out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnk_Pkey_out.CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("lnk_Pkey_out.CASE_MGT_STTUS_SEQ_NO").alias("CASE_MGT_STTUS_SEQ_NO"),
    F.col("lnk_Pkey_out.SRC_SYS_CD").alias("SRC_SYS_CD_1"),
    F.col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_Pkey_out.CASE_MGT_STTUS_SK").alias("CASE_MGT_STTUS_SK"),
    F.col("CP_Out.USUS_ID").alias("USUS_ID"),
    F.col("CP_Out.CMST_STS").alias("CMST_STS"),
    F.col("CP_Out.CMST_STS_DTM").alias("CMST_STS_DTM"),
    F.col("CP_Out.CMST_MCTR_REAS").alias("CMST_MCTR_REAS"),
    F.col("CP_Out.CMST_USID_ROUTE").alias("CMST_USID_ROUTE")
)

# seq_CASE_MGT_STTUS_PKEY Stage (PxSequentialFile)
write_files(
    df_jn_PKey.select(
        "PRI_NAT_KEY_STRING",
        "FIRST_RECYC_TS",
        "CASE_MGT_ID",
        "CASE_MGT_STTUS_SEQ_NO",
        "SRC_SYS_CD_1",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CASE_MGT_STTUS_SK",
        "USUS_ID",
        "CMST_STS",
        "CMST_STS_DTM",
        "CMST_MCTR_REAS",
        "CMST_USID_ROUTE"
    ),
    f"{adls_path}/key/CASE_MGT_STTUS.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)