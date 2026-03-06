# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Pooja Sunkara          2014-08-15              5345                             Original Programming                                                                        IntegrateWrhsDevl     Kalyan Neelam              2015-01-07
# MAGIC 
# MAGIC Archana Palivela      2015-05-13           #5401                          CRT_RUN_CYC value that goes into K table is set to IDSRunCycle.  IntegrateWrhsDevl      Jag Yelavarthi               2015-05-14
# MAGIC                                                                                                    It was incorrectly set to incoming value from Dedup stage, which is
# MAGIC                                                                                                    causing all the new rows go into K table getting a Zero for 
# MAGIC                                                                                                    CRT_RUN_CYC
# MAGIC 
# MAGIC Kshema H K            2023-10-13             us 598277              Added  CULT_CPBLTY_NM (VarChar 50 Nullable) field                           IntegrateDevB            Jeyaprasanna              2023-10-18
# MAGIC                                                                                                                and mapped till Target

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_CMN_PRCT.Insert new SKEY rows generated as part of this run. This is a database insert operation.
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
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter retrieval
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')

# Read from dataset (.ds) as parquet
df_ds_CMN_PRCT_Xfrm = spark.read.parquet(f"{adls_path}/ds/CMN_PRCT.{SrcSysCd}.xfrm.{RunID}.parquet")

# cpy_MultiStreams Outputs
df_remDupDataIn = df_ds_CMN_PRCT_Xfrm.select(
    col("CMN_PRCT_ID"),
    col("SRC_SYS_CD")
)

df_FullDataJnIn = df_ds_CMN_PRCT_Xfrm.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("CMN_PRCT_ID"),
    col("SRC_SYS_CD"),
    col("SRC_SYS_CD_SK"),
    col("CMN_PRCT_GNDR_CD"),
    col("ACTV_IN"),
    col("BRTH_DT"),
    col("INIT_CRDTL_DT"),
    col("LAST_CRDTL_DT"),
    col("NEXT_CRDTL_DT"),
    col("FIRST_NM"),
    col("LAST_NM"),
    col("MIDINIT"),
    col("CMN_PRCT_TTL"),
    col("NTNL_PROV_ID"),
    col("SSN"),
    col("CULT_CPBLTY_NM")
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys = dedup_sort(
    df_remDupDataIn,
    partition_cols=["CMN_PRCT_ID", "SRC_SYS_CD"],
    sort_cols=[]
)

# db2_K_CMN_PRCT_In: read from IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CMN_PRCT_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,CMN_PRCT_SK FROM {IDSOwner}.K_CMN_PRCT"
df_db2_K_CMN_PRCT_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# jn_CmnPrct (left join)
df_jn_CmnPrct = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_CMN_PRCT_In.alias("lnk_KCmnPrct_Extr"),
    [
        col("lnkRemDupDataOut.CMN_PRCT_ID") == col("lnk_KCmnPrct_Extr.CMN_PRCT_ID"),
        col("lnkRemDupDataOut.SRC_SYS_CD") == col("lnk_KCmnPrct_Extr.SRC_SYS_CD")
    ],
    how="left"
).select(
    col("lnkRemDupDataOut.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_KCmnPrct_Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_KCmnPrct_Extr.CMN_PRCT_SK").alias("CMN_PRCT_SK")
)

# xfm_PKEYgen
df_xfm = df_jn_CmnPrct.withColumn(
    "original_CMN_PRCT_SK_isNull",
    F.col("CMN_PRCT_SK").isNull()
)

df_xfm = df_xfm.withColumn(
    "CRT_RUN_CYC_EXCTN_SK_x",
    when(col("original_CMN_PRCT_SK_isNull"), lit(IDSRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
)

df_xfm = df_xfm.withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_SK_x",
    lit(IDSRunCycle)
)

df_enriched = SurrogateKeyGen(df_xfm,<DB sequence name>,"CMN_PRCT_SK",<schema>,<secret_name>)

# lnk_KCmnPrct_Out (rows where CMN_PRCT_SK was originally null)
df_lnk_KCmnPrct_Out = df_enriched.filter(
    col("original_CMN_PRCT_SK_isNull")
).select(
    col("CMN_PRCT_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK_x").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("CMN_PRCT_SK")
)

# lnkPKEYxfmOut (all rows with updated columns)
df_lnkPKEYxfmOut = df_enriched.select(
    col("CMN_PRCT_ID"),
    col("SRC_SYS_CD"),
    col("CRT_RUN_CYC_EXCTN_SK_x").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK_x").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CMN_PRCT_SK")
)

# db2_K_CMN_PRCT_Load => merge into #$IDSOwner#.K_CMN_PRCT
temp_table = "STAGING.IdsCmnPrctPkey_db2_K_CMN_PRCT_Load_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table}", jdbc_url, jdbc_props)

df_lnk_KCmnPrct_Out.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_CMN_PRCT AS T
USING {temp_table} AS S
ON T.CMN_PRCT_ID = S.CMN_PRCT_ID AND T.SRC_SYS_CD = S.SRC_SYS_CD
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.CMN_PRCT_SK = S.CMN_PRCT_SK
WHEN NOT MATCHED THEN
  INSERT (CMN_PRCT_ID, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, CMN_PRCT_SK)
  VALUES (S.CMN_PRCT_ID, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK, S.CMN_PRCT_SK)
;
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# jn_PKEYs (inner join)
df_jn_PKEYs = df_FullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        col("lnkFullDataJnIn.CMN_PRCT_ID") == col("lnkPKEYxfmOut.CMN_PRCT_ID"),
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
).select(
    col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    col("lnkPKEYxfmOut.CMN_PRCT_SK").alias("CMN_PRCT_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.CMN_PRCT_ID").alias("CMN_PRCT_ID"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("lnkFullDataJnIn.CMN_PRCT_GNDR_CD").alias("CMN_PRCT_GNDR_CD"),
    col("lnkFullDataJnIn.ACTV_IN").alias("ACTV_IN"),
    col("lnkFullDataJnIn.BRTH_DT").alias("BRTH_DT"),
    col("lnkFullDataJnIn.INIT_CRDTL_DT").alias("INIT_CRDTL_DT"),
    col("lnkFullDataJnIn.LAST_CRDTL_DT").alias("LAST_CRDTL_DT"),
    col("lnkFullDataJnIn.NEXT_CRDTL_DT").alias("NEXT_CRDTL_DT"),
    col("lnkFullDataJnIn.FIRST_NM").alias("FIRST_NM"),
    col("lnkFullDataJnIn.LAST_NM").alias("LAST_NM"),
    col("lnkFullDataJnIn.MIDINIT").alias("MIDINIT"),
    col("lnkFullDataJnIn.CMN_PRCT_TTL").alias("CMN_PRCT_TTL"),
    col("lnkFullDataJnIn.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    col("lnkFullDataJnIn.SSN").alias("SSN"),
    col("lnkFullDataJnIn.CULT_CPBLTY_NM").alias("CULT_CPBLTY_NM")
)

# seq_CMN_PRCT_Pkey => write to .dat
df_final = df_jn_PKEYs.select(
    col("PRI_NAT_KEY_STRING"),
    col("FIRST_RECYC_TS"),
    col("CMN_PRCT_SK"),
    col("SRC_SYS_CD"),
    col("CMN_PRCT_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SRC_SYS_CD_SK"),
    col("CMN_PRCT_GNDR_CD"),
    rpad(col("ACTV_IN"), 1, " "),
    rpad(col("BRTH_DT"), 10, " "),
    rpad(col("INIT_CRDTL_DT"), 10, " "),
    rpad(col("LAST_CRDTL_DT"), 10, " "),
    rpad(col("NEXT_CRDTL_DT"), 10, " "),
    col("FIRST_NM"),
    col("LAST_NM"),
    rpad(col("MIDINIT"), 1, " "),
    col("CMN_PRCT_TTL"),
    col("NTNL_PROV_ID"),
    col("SSN"),
    col("CULT_CPBLTY_NM")
)

write_files(
    df_final,
    f"{adls_path}/key/CMN_PRCT.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)