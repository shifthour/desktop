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
# MAGIC                                              		  Project/                                                                                                    	Code                  	Date
# MAGIC Developer         	Date             	 Altiris #     	Change Description                                                   	Reviewer            	Reviewed
# MAGIC -----------------------  	-------------------   	-------------   	-----------------------------------------------------------------------------   	------------------------- 	 -------------------
# MAGIC Sravya Gorla        2019-09-03                 US140167              Case Mgt Cost PKey Process         			Jaideep Mankala  	09/24/2019

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_CASE_MGT_COST Table to pull the Natural Keys and the Skey.
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


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

df_ds_CASE_MGT_COST_Xfm = spark.read.parquet(
    f"{adls_path}/ds/CASE_MGT_COST.{SrcSysCd}.extr.{RunID}.parquet"
)

df_cp_pk_out1 = df_ds_CASE_MGT_COST_Xfm.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "CASE_MGT_ID",
    "CST_SEQ_NO",
    "SRC_SYS_CD",
    "CST_INPT_USER_ID",
    "CST_INPT_DT",
    "FROM_DT",
    "TO_DT",
    "ACTL_CST_AMT",
    "PROJ_CST_AMT",
    "CMCS_MCTR_COST"
)

df_cp_pk_out2 = df_ds_CASE_MGT_COST_Xfm.select(
    "CASE_MGT_ID",
    "CST_SEQ_NO",
    "SRC_SYS_CD"
)

df_rdup_Natural_Keys = dedup_sort(
    df_cp_pk_out2,
    ["CASE_MGT_ID", "CST_SEQ_NO", "SRC_SYS_CD"],
    []
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = (
    "Select CASE_MGT_ID, CST_SEQ_NO, SRC_SYS_CD, "
    "CRT_RUN_CYC_EXCTN_SK, CASE_MGT_CST_LOG_SK "
    f"from {IDSOwner}.K_CASE_MGT_CST_LOG"
)
df_db2_K_CASE_MGT_COST_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_jn_CMCT = df_rdup_Natural_Keys.alias("A").join(
    df_db2_K_CASE_MGT_COST_in.alias("B"),
    (
        (F.col("A.CASE_MGT_ID") == F.col("B.CASE_MGT_ID"))
        & (F.col("A.CST_SEQ_NO") == F.col("B.CST_SEQ_NO"))
        & (F.col("A.SRC_SYS_CD") == F.col("B.SRC_SYS_CD"))
    ),
    how="left"
).select(
    F.col("A.CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("A.CST_SEQ_NO").alias("CST_SEQ_NO"),
    F.col("A.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("B.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("B.CASE_MGT_CST_LOG_SK").alias("CASE_MGT_CST_LOG_SK")
)

df_temp = df_jn_CMCT.withColumn("ORIG_CASE_MGT_CST_LOG_SK", F.col("CASE_MGT_CST_LOG_SK"))
df_enriched = SurrogateKeyGen(df_temp,<DB sequence name>,"CASE_MGT_CST_LOG_SK",<schema>,<secret_name>)

df_xfrm_PKEYgen = (
    df_enriched
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.when(F.col("ORIG_CASE_MGT_CST_LOG_SK").isNull(), F.lit(IDSRunCycle))
        .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(IDSRunCycle))
)

df_lnk_Pkey_out = df_xfrm_PKEYgen.select(
    "CASE_MGT_ID",
    "CST_SEQ_NO",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CASE_MGT_CST_LOG_SK",
    "ORIG_CASE_MGT_CST_LOG_SK"
)

df_lnk_KCmSt_New = (
    df_lnk_Pkey_out
    .filter(F.col("ORIG_CASE_MGT_CST_LOG_SK").isNull())
    .select(
        "CASE_MGT_ID",
        "CST_SEQ_NO",
        "SRC_SYS_CD",
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("CASE_MGT_CST_LOG_SK").alias("CASE_MGT_CST_LOG_SK")
    )
)

df_lnk_KCmSt_New.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", f"{IDSOwner}.K_CASE_MGT_CST_LOG") \
    .mode("append") \
    .save()

df_jn_PKey = df_lnk_Pkey_out.alias("A").join(
    df_cp_pk_out1.alias("B"),
    (
        (F.col("A.CASE_MGT_ID") == F.col("B.CASE_MGT_ID"))
        & (F.col("A.CST_SEQ_NO") == F.col("B.CST_SEQ_NO"))
        & (F.col("A.SRC_SYS_CD") == F.col("B.SRC_SYS_CD"))
    ),
    how="inner"
).select(
    F.col("B.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("B.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("A.CASE_MGT_ID").alias("CASE_MGT_ID"),
    F.col("A.SRC_SYS_CD").alias("SRC_SYS_CD_1"),
    F.col("A.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("A.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("A.CST_SEQ_NO").alias("CST_SEQ_NO"),
    F.col("A.CASE_MGT_CST_LOG_SK").alias("CASE_MGT_CST_LOG_SK"),
    F.col("B.CST_INPT_USER_ID").alias("CST_INPT_USER_ID"),
    F.col("B.CST_INPT_DT").alias("CST_INPT_DT"),
    F.col("B.FROM_DT").alias("FROM_DT"),
    F.col("B.TO_DT").alias("TO_DT"),
    F.col("B.ACTL_CST_AMT").alias("ACTL_CST_AMT"),
    F.col("B.PROJ_CST_AMT").alias("PROJ_CST_AMT"),
    F.col("B.CMCS_MCTR_COST").alias("CMCS_MCTR_COST")
)

df_seq_CASE_MGT_COST_PKEY = df_jn_PKey.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "CASE_MGT_ID",
    "SRC_SYS_CD_1",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CST_SEQ_NO",
    "CASE_MGT_CST_LOG_SK",
    "CST_INPT_USER_ID",
    "CST_INPT_DT",
    "FROM_DT",
    "TO_DT",
    "ACTL_CST_AMT",
    "PROJ_CST_AMT",
    "CMCS_MCTR_COST"
)

write_files(
    df_seq_CASE_MGT_COST_PKEY,
    f"{adls_path}/key/CASE_MGT_CST.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)