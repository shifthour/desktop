# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC PROCESSING:  This job assigns primary key value using SEQ_K_INDV_BE_CCHG
# MAGIC                                                                 
# MAGIC      
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Kalyan Neelam       2015-08-17           5460                            Original Programming                                                                             IntegrateDev2            Bhoomi Dasari              8/30/2015
# MAGIC Raja Gummadi        2017-03-14           Prod Supp                   Changed Primary Keying process                                                          IntegrateDev1               Kalyan Neelam             2017-03-22
# MAGIC Goutham Kalidindi   2024-12-19          US-636523                  MAX INT value reached on INDV_BE_CCHG_SK - Modified the         IntegrateDev2                ReddySanam              2024-12-19
# MAGIC                                                                                                SQL in the MAX_K_INDV_BE_CCHG_In Stage to get
# MAGIC                                                                                                max value less than 0

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_INDV_BE_CCHG. Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Land into Seq File for the FKEY job
# MAGIC New PKEYs are generated in this transformer.
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
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
YearMo = get_widget_value('YearMo','')

# Read ds_INDV_BE_CCHG_Xfrm (DataSet -> parquet)
df_ds_INDV_BE_CCHG_Xfrm = spark.read.parquet(
    f"{adls_path}/ds/INDV_BE_CCHG.{SrcSysCd}.xfrm.{RunID}.parquet"
)

# Copy stage cpy_MultiStreams: split into two outputs
df_cpy_MultiStreams_lnkRemDupDataIn = df_ds_INDV_BE_CCHG_Xfrm.select(
    F.col("INDV_BE_KEY"),
    F.col("CCHG_STRT_YR_MO_SK"),
    F.col("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD")
)

df_cpy_MultiStreams_lnkFullDataJnIn = df_ds_INDV_BE_CCHG_Xfrm.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("INDV_BE_KEY"),
    F.col("CCHG_STRT_YR_MO_SK"),
    F.col("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CCHG_MULT_CAT_GRP_PRI"),
    F.col("CCHG_MULT_CAT_GRP_SEC"),
    F.col("CCHG_MULT_CAT_GRP_TRTY"),
    F.col("CCHG_MULT_CAT_GRP_4TH"),
    F.col("CCHG_MULT_CAT_GRP_5TH"),
    F.col("CCHG_END_YR_MO_SK"),
    F.col("CCHG_CT")
)

# Read MAX_K_INDV_BE_CCHG_In (DB2 -> Azure SQL)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT '{YearMo}' AS PRCS_YR_MO_SK, COALESCE(MAX(INDV_BE_CCHG_SK),0) AS MAX_INDV_BE_CCHG_SK FROM {IDSOwner}.K_INDV_BE_CCHG WHERE INDV_BE_CCHG_SK < 0"
df_MAX_K_INDV_BE_CCHG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# rdp_NaturalKeys (PxRemDup)
df_rdp_NaturalKeys_temp = dedup_sort(
    df_cpy_MultiStreams_lnkRemDupDataIn,
    ["INDV_BE_KEY","CCHG_STRT_YR_MO_SK","PRCS_YR_MO_SK","SRC_SYS_CD"],
    [("INDV_BE_KEY","A"),("CCHG_STRT_YR_MO_SK","A"),("PRCS_YR_MO_SK","A"),("SRC_SYS_CD","A")]
)
df_rdp_NaturalKeys = df_rdp_NaturalKeys_temp.select(
    F.col("INDV_BE_KEY"),
    F.col("CCHG_STRT_YR_MO_SK"),
    F.col("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD")
)

# Read db2_K_INDV_BE_CCHG_In (DB2 -> Azure SQL)
extract_query = f"SELECT INDV_BE_KEY, CCHG_STRT_YR_MO_SK, PRCS_YR_MO_SK, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK, INDV_BE_CCHG_SK FROM {IDSOwner}.K_INDV_BE_CCHG WHERE PRCS_YR_MO_SK = '{YearMo}'"
df_db2_K_INDV_BE_CCHG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# jn_KIndvBeCchg (left join)
df_jn_KIndvBeCchg_joined = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2_K_INDV_BE_CCHG_In.alias("Extr"),
    on=[
        F.col("lnkRemDupDataOut.INDV_BE_KEY")==F.col("Extr.INDV_BE_KEY"),
        F.col("lnkRemDupDataOut.CCHG_STRT_YR_MO_SK")==F.col("Extr.CCHG_STRT_YR_MO_SK"),
        F.col("lnkRemDupDataOut.PRCS_YR_MO_SK")==F.col("Extr.PRCS_YR_MO_SK"),
        F.col("lnkRemDupDataOut.SRC_SYS_CD")==F.col("Extr.SRC_SYS_CD")
    ],
    how="left"
)
df_jn_KIndvBeCchg = df_jn_KIndvBeCchg_joined.select(
    F.col("lnkRemDupDataOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("lnkRemDupDataOut.CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
    F.col("lnkRemDupDataOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("lnkRemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Extr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Extr.INDV_BE_CCHG_SK").alias("INDV_BE_CCHG_SK")
)

# Lookup_14 (left join)
df_Lookup_14_joined = df_jn_KIndvBeCchg.alias("JoinOut").join(
    df_MAX_K_INDV_BE_CCHG_In.alias("Extr"),
    on=[F.col("JoinOut.PRCS_YR_MO_SK")==F.col("Extr.PRCS_YR_MO_SK")],
    how="left"
)
df_Lookup_14 = df_Lookup_14_joined.select(
    F.col("JoinOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("JoinOut.CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
    F.col("JoinOut.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("JoinOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("JoinOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("JoinOut.INDV_BE_CCHG_SK").alias("INDV_BE_CCHG_SK"),
    F.col("Extr.MAX_INDV_BE_CCHG_SK").alias("MAX_INDV_BE_CCHG_SK")
)

# xfm_PKEYgen (Transformer logic)
w = Window.orderBy(F.monotonically_increasing_id())
df_xfm_PKEYgen_stage = (
    df_Lookup_14
    .withColumn("orig_isnull_INDV_BE_CCHG_SK", F.col("INDV_BE_CCHG_SK").isNull())
    .withColumn("row_num", F.row_number().over(w))
)

df_xfm_PKEYgen = df_xfm_PKEYgen_stage.withColumn(
    "svIndvBeCchgSK",
    F.when(
        F.col("orig_isnull_INDV_BE_CCHG_SK"),
        (F.col("row_num") - 1) + F.col("MAX_INDV_BE_CCHG_SK")
    ).otherwise(F.col("INDV_BE_CCHG_SK"))
).withColumn(
    "svRunCyle",
    F.when(
        F.col("orig_isnull_INDV_BE_CCHG_SK"),
        F.lit(IDSRunCycle)
    ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
)

# Output link "New" => isNull(INDV_BE_CCHG_SK) == True originally
df_xfm_PKEYgen_New = df_xfm_PKEYgen.filter(
    F.col("orig_isnull_INDV_BE_CCHG_SK") == True
).select(
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("svIndvBeCchgSK").alias("INDV_BE_CCHG_SK")
)

# Output link "lnkPKEYxfmOut" => all rows
df_xfm_PKEYgen_lnkPKEYxfmOut = df_xfm_PKEYgen.select(
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
    F.col("PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svRunCyle").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svIndvBeCchgSK").alias("INDV_BE_CCHG_SK")
)

# db2_K_INDV_BE_CCHG_Load => Merge logic (insert-only in DataStage, but we use upsert)
df_xfm_PKEYgen_New.createOrReplaceTempView("tempTable_db2_K_INDV_BE_CCHG_New")  # Internal Spark temp view
# Write to staging table
temp_table_name = "STAGING.IdsIndvBeCchgPkey_db2_K_INDV_BE_CCHG_Load_temp"
drop_sql = f"DROP TABLE IF EXISTS {temp_table_name}"
execute_dml(drop_sql, jdbc_url, jdbc_props)

df_xfm_PKEYgen_New.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", temp_table_name) \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE INTO {IDSOwner}.K_INDV_BE_CCHG AS T
USING {temp_table_name} AS S
ON 
(
  T.INDV_BE_KEY = S.INDV_BE_KEY AND
  T.CCHG_STRT_YR_MO_SK = S.CCHG_STRT_YR_MO_SK AND
  T.PRCS_YR_MO_SK = S.PRCS_YR_MO_SK AND
  T.SRC_SYS_CD = S.SRC_SYS_CD
)
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.INDV_BE_CCHG_SK = S.INDV_BE_CCHG_SK
WHEN NOT MATCHED THEN
  INSERT 
  (
    INDV_BE_KEY,
    CCHG_STRT_YR_MO_SK,
    PRCS_YR_MO_SK,
    SRC_SYS_CD,
    CRT_RUN_CYC_EXCTN_SK,
    INDV_BE_CCHG_SK
  )
  VALUES 
  (
    S.INDV_BE_KEY,
    S.CCHG_STRT_YR_MO_SK,
    S.PRCS_YR_MO_SK,
    S.SRC_SYS_CD,
    S.CRT_RUN_CYC_EXCTN_SK,
    S.INDV_BE_CCHG_SK
  );
"""
execute_dml(merge_sql, jdbc_url, jdbc_props)

# jn_PKEYs (inner join)
df_jn_PKEYs_joined = df_cpy_MultiStreams_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_xfm_PKEYgen_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    on=[
        F.col("lnkFullDataJnIn.INDV_BE_KEY")==F.col("lnkPKEYxfmOut.INDV_BE_KEY"),
        F.col("lnkFullDataJnIn.CCHG_STRT_YR_MO_SK")==F.col("lnkPKEYxfmOut.CCHG_STRT_YR_MO_SK"),
        F.col("lnkFullDataJnIn.PRCS_YR_MO_SK")==F.col("lnkPKEYxfmOut.PRCS_YR_MO_SK"),
        F.col("lnkFullDataJnIn.SRC_SYS_CD")==F.col("lnkPKEYxfmOut.SRC_SYS_CD")
    ],
    how="inner"
)
df_jn_PKEYs = df_jn_PKEYs_joined.select(
    F.col("lnkFullDataJnIn.PRI_NAT_KEY_STRING").alias("PRI_NAT_KEY_STRING"),
    F.col("lnkFullDataJnIn.FIRST_RECYC_TS").alias("FIRST_RECYC_TS"),
    F.col("lnkPKEYxfmOut.INDV_BE_CCHG_SK").alias("INDV_BE_CCHG_SK"),
    F.col("lnkFullDataJnIn.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("lnkFullDataJnIn.CCHG_STRT_YR_MO_SK").alias("CCHG_STRT_YR_MO_SK"),
    F.col("lnkFullDataJnIn.PRCS_YR_MO_SK").alias("PRCS_YR_MO_SK"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnkFullDataJnIn.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP_PRI").alias("CCHG_MULT_CAT_GRP_PRI"),
    F.col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP_SEC").alias("CCHG_MULT_CAT_GRP_SEC"),
    F.col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP_TRTY").alias("CCHG_MULT_CAT_GRP_TRTY"),
    F.col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP_4TH").alias("CCHG_MULT_CAT_GRP_4TH"),
    F.col("lnkFullDataJnIn.CCHG_MULT_CAT_GRP_5TH").alias("CCHG_MULT_CAT_GRP_5TH"),
    F.col("lnkFullDataJnIn.CCHG_END_YR_MO_SK").alias("CCHG_END_YR_MO_SK"),
    F.col("lnkFullDataJnIn.CCHG_CT").alias("CCHG_CT")
)

# seq_INDV_BE_CCHG_Pkey (write delimited file)
df_seq_INDV_BE_CCHG_Pkey = df_jn_PKEYs.select(
    F.col("PRI_NAT_KEY_STRING"),
    F.col("FIRST_RECYC_TS"),
    F.col("INDV_BE_CCHG_SK"),
    F.col("INDV_BE_KEY"),
    F.rpad(F.col("CCHG_STRT_YR_MO_SK"), 6, " ").alias("CCHG_STRT_YR_MO_SK"),
    F.rpad(F.col("PRCS_YR_MO_SK"), 6, " ").alias("PRCS_YR_MO_SK"),
    F.col("SRC_SYS_CD"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CCHG_MULT_CAT_GRP_PRI"),
    F.col("CCHG_MULT_CAT_GRP_SEC"),
    F.col("CCHG_MULT_CAT_GRP_TRTY"),
    F.col("CCHG_MULT_CAT_GRP_4TH"),
    F.col("CCHG_MULT_CAT_GRP_5TH"),
    F.rpad(F.col("CCHG_END_YR_MO_SK"), 6, " ").alias("CCHG_END_YR_MO_SK"),
    F.col("CCHG_CT")
)

write_files(
    df_seq_INDV_BE_CCHG_Pkey,
    f"{adls_path}/key/INDV_BE_CCHG.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)