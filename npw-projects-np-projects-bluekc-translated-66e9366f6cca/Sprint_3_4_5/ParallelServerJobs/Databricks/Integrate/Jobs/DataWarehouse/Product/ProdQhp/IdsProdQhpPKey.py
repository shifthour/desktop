# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsProdQhpPKey
# MAGIC 
# MAGIC Called By: SbcIdsProdQhpExtCntl
# MAGIC 
# MAGIC                           
# MAGIC PROCESSING:
# MAGIC     Build primary key for  PROD_QHP table load data 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Jaideep Mankala              2016-07-22         5605                            Original Programming                                             IntegrateDev2               Kalyan Neelam              2016-08-10
# MAGIC Jaideep Mankala              2016-08-17         5605                           Added new field to input dataset QHP_EFF_DT    IntegrateDev2               Kalyan Neelam              2016-09-21

# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC Insert Only the Newly generated Pkeys into the K Table
# MAGIC Read K_PROD_QHP Table to pull the Natural Keys and the Skey.
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
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')
IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')

df_ds_PROD_QHP_Xfm = spark.read.parquet(f"{adls_path}/ds/PROD_QHP.{SrcSysCd}.xfrm.{RunID}.parquet")
df_ds_PROD_QHP_Xfm = df_ds_PROD_QHP_Xfm.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "PROD_QHP_SK",
    "PROD_ID",
    "EFF_DT",
    "SRC_SYS_CD_SK",
    "SRC_SYS_CD",
    "TERM_DT",
    "QHP_ID",
    "LAST_UPDT_DT",
    "QHP_EFF_DT_SK"
)

df_Cp_Pk_out_1 = df_ds_PROD_QHP_Xfm.select(
    col("PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("PROD_ID").alias("PROD_ID"),
    col("EFF_DT").alias("PROD_QHP_EFF_DT_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("TERM_DT").alias("TERM_DT"),
    col("QHP_ID").alias("QHP_ID"),
    col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    col("QHP_EFF_DT_SK").alias("QHP_EFF_DT_SK")
)

df_Cp_Pk_out_2 = df_ds_PROD_QHP_Xfm.select(
    col("PROD_ID").alias("PROD_ID"),
    col("EFF_DT").alias("PROD_QHP_EFF_DT_SK"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

df_rdup_Natural_Keys = dedup_sort(
    df_Cp_Pk_out_2,
    ["PROD_ID", "PROD_QHP_EFF_DT_SK", "SRC_SYS_CD"],
    [("PROD_ID", "A"), ("PROD_QHP_EFF_DT_SK", "A"), ("SRC_SYS_CD", "A")]
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT PROD_ID, PROD_QHP_EFF_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PROD_QHP_SK FROM {IDSOwner}.K_PROD_QHP"
df_db2_K_PROD_QHP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_jn_ProdQhp = df_rdup_Natural_Keys.alias("lnk_Natural_Keys_out").join(
    df_db2_K_PROD_QHP_in.alias("lnk_KProdQhpPkey_out"),
    [
        col("lnk_Natural_Keys_out.PROD_ID") == col("lnk_KProdQhpPkey_out.PROD_ID"),
        col("lnk_Natural_Keys_out.PROD_QHP_EFF_DT_SK") == col("lnk_KProdQhpPkey_out.PROD_QHP_EFF_DT_SK"),
        col("lnk_Natural_Keys_out.SRC_SYS_CD") == col("lnk_KProdQhpPkey_out.SRC_SYS_CD")
    ],
    how="left"
)
df_jn_ProdQhp = df_jn_ProdQhp.select(
    col("lnk_Natural_Keys_out.PROD_ID").alias("PROD_ID"),
    col("lnk_Natural_Keys_out.PROD_QHP_EFF_DT_SK").alias("PROD_QHP_EFF_DT_SK"),
    col("lnk_Natural_Keys_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_KProdQhpPkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_KProdQhpPkey_out.PROD_QHP_SK").alias("PROD_QHP_SK")
)

df_xfrm_in = df_jn_ProdQhp.withColumn("is_new", col("PROD_QHP_SK").isNull())
df_enriched = df_xfrm_in.withColumn(
    "temp_svProdQhpSK",
    when(col("is_new"), lit(None)).otherwise(col("PROD_QHP_SK"))
)
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"temp_svProdQhpSK",<schema>,<secret_name>)
df_enriched = (
    df_enriched
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        when(col("is_new"), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
    )
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(IDSRunCycle))
    .withColumn("PROD_QHP_SK", col("temp_svProdQhpSK"))
)

df_lnk_Pkey_out = df_enriched.select(
    col("PROD_ID"),
    col("PROD_QHP_EFF_DT_SK"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PROD_QHP_SK"),
    col("is_new")
)

df_lnk_KProdQhp_New = df_lnk_Pkey_out.filter(col("is_new")).select(
    col("PROD_ID"),
    col("PROD_QHP_EFF_DT_SK"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("PROD_QHP_SK")
)

execute_dml(f"DROP TABLE IF EXISTS STAGING.IdsProdQhpPKey_DB2_K_PROD_QHP_Load_temp", jdbc_url, jdbc_props)
df_lnk_KProdQhp_New.write.format("jdbc").option("url", jdbc_url).options(**jdbc_props).option("dbtable", "STAGING.IdsProdQhpPKey_DB2_K_PROD_QHP_Load_temp").mode("append").save()
merge_sql = f"""
MERGE INTO {IDSOwner}.K_PROD_QHP AS T
USING STAGING.IdsProdQhpPKey_DB2_K_PROD_QHP_Load_temp AS S
ON
    T.PROD_ID = S.PROD_ID
    AND T.PROD_QHP_EFF_DT_SK = S.PROD_QHP_EFF_DT_SK
    AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
    -- No operation for append 
WHEN NOT MATCHED THEN
    INSERT (PROD_ID, PROD_QHP_EFF_DT_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, PROD_QHP_SK)
    VALUES (S.PROD_ID, S.PROD_QHP_EFF_DT_SK, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.PROD_QHP_SK);
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

df_jn_PKey = df_lnk_Pkey_out.drop("is_new").alias("lnk_Pkey_out").join(
    df_Cp_Pk_out_1.alias("CP_Out"),
    [
        col("lnk_Pkey_out.PROD_ID") == col("CP_Out.PROD_ID"),
        col("lnk_Pkey_out.PROD_QHP_EFF_DT_SK") == col("CP_Out.PROD_QHP_EFF_DT_SK"),
        col("lnk_Pkey_out.SRC_SYS_CD") == col("CP_Out.SRC_SYS_CD")
    ],
    how="inner"
)
df_jn_PKey = df_jn_PKey.select(
    col("CP_Out.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("CP_Out.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnk_Pkey_out.PROD_ID").alias("PROD_ID"),
    col("lnk_Pkey_out.PROD_QHP_EFF_DT_SK").alias("PROD_QHP_EFF_DT_SK"),
    col("lnk_Pkey_out.SRC_SYS_CD").alias("SRC_SYS_CD_1"),
    col("lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_Pkey_out.PROD_QHP_SK").alias("PROD_QHP_SK"),
    col("CP_Out.TERM_DT").alias("TERM_DT"),
    col("CP_Out.QHP_ID").alias("QHP_ID"),
    col("CP_Out.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    col("CP_Out.QHP_EFF_DT_SK").alias("QHP_EFF_DT_SK")
)

df_jn_PKey_final = df_jn_PKey.withColumn("PROD_ID", rpad(col("PROD_ID"), 8, " ")) \
    .withColumn("PROD_QHP_EFF_DT_SK", rpad(col("PROD_QHP_EFF_DT_SK"), 10, " ")) \
    .withColumn("TERM_DT", rpad(col("TERM_DT"), 10, " ")) \
    .withColumn("QHP_EFF_DT_SK", rpad(col("QHP_EFF_DT_SK"), 10, " "))

df_jn_PKey_final = df_jn_PKey_final.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "PROD_ID",
    "PROD_QHP_EFF_DT_SK",
    "SRC_SYS_CD_1",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROD_QHP_SK",
    "TERM_DT",
    "QHP_ID",
    "LAST_UPDT_DT",
    "QHP_EFF_DT_SK"
)

write_files(
    df_jn_PKey_final,
    f"{adls_path}/key/PROD_QHP.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)