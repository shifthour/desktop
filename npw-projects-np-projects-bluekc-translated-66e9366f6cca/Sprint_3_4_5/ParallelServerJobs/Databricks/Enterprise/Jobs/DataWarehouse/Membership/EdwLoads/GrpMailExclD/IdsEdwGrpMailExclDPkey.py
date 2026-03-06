# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                           DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------             -------------------------------    ------------------------------       --------------------
# MAGIC  Bhupinder Kaur                7/22/2013           5114                          generate primary key for Grp_Mail_Excl_D                                        EnterpriseWhseDevl

# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Left Outer Join Here
# MAGIC Build primary key for  GRP_MAIL_EXCL_D
# MAGIC K table will be read to see if there is an SK already available for the Natural Keys. 
# MAGIC 
# MAGIC Job Name: IdsEdwGrpMailExclDPky
# MAGIC Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
# MAGIC 
# MAGIC IMPORTANT: Make sure to change the Database SEQUENCE Name to the corresponding table name.
# MAGIC if there is no FKEY needed, just land it to Seq File for the load job follows.
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
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

df_ds_GRP_MAIL_EXCL_D_Extr = spark.read.parquet(f"{adls_path}/ds/GRP_MAIL_EXCL_D.parquet")

df_cpy_MultiStreams = df_ds_GRP_MAIL_EXCL_D_Extr

df_cpy_MultiStreams_out1 = df_cpy_MultiStreams.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MAIL_TYP_CD").alias("MAIL_TYP_CD")
)

df_cpy_MultiStreams_out2 = df_cpy_MultiStreams.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("MAIL_TYP_CD").alias("MAIL_TYP_CD"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("GRP_MAIL_EXCL_DESC").alias("GRP_MAIL_EXCL_DESC"),
    F.col("GRP_MAIL_EXCL_MAIL_TYP_NM").alias("GRP_MAIL_EXCL_MAIL_TYP_NM"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_rdp_NaturalKeys = dedup_sort(
    df_cpy_MultiStreams_out1,
    ["SRC_SYS_CD","GRP_ID","MAIL_TYP_CD"],
    []
)

jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
extract_query_db2_KGrpMailExclDRead = """SELECT 
SRC_SYS_CD,
GRP_ID,
MAIL_TYP_CD,
CRT_RUN_CYC_EXCTN_DT_SK,
GRP_MAIL_EXCL_SK,
CRT_RUN_CYC_EXCTN_SK
FROM """ + "$EDWOwner" + """.K_GRP_MAIL_EXCL_D"""

df_db2_KGrpMailExclDRead = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", extract_query_db2_KGrpMailExclDRead)
    .load()
)

df_jn_KGrpMailExclD = df_rdp_NaturalKeys.alias("lnk_RemDupDataOut").join(
    df_db2_KGrpMailExclDRead.alias("lnk_KGrpMailExclDIn"),
    on=[
        F.col("lnk_RemDupDataOut.SRC_SYS_CD") == F.col("lnk_KGrpMailExclDIn.SRC_SYS_CD"),
        F.col("lnk_RemDupDataOut.GRP_ID") == F.col("lnk_KGrpMailExclDIn.GRP_ID"),
        F.col("lnk_RemDupDataOut.MAIL_TYP_CD") == F.col("lnk_KGrpMailExclDIn.MAIL_TYP_CD")
    ],
    how="left"
)

df_jn_KGrpMailExclD_out = df_jn_KGrpMailExclD.select(
    F.col("lnk_RemDupDataOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_RemDupDataOut.GRP_ID").alias("GRP_ID"),
    F.col("lnk_RemDupDataOut.MAIL_TYP_CD").alias("MAIL_TYP_CD"),
    F.col("lnk_KGrpMailExclDIn.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_KGrpMailExclDIn.GRP_MAIL_EXCL_SK").alias("GRP_MAIL_EXCL_SK"),
    F.col("lnk_KGrpMailExclDIn.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
)

df_enriched = df_jn_KGrpMailExclD_out.withColumn("orig_GRP_MAIL_EXCL_SK", F.col("GRP_MAIL_EXCL_SK"))
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"GRP_MAIL_EXCL_SK",<schema>,<secret_name>)

df_xfrm_PKEYgen_out1 = (
    df_enriched
    .filter(F.col("orig_GRP_MAIL_EXCL_SK").isNull())
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("MAIL_TYP_CD").alias("MAIL_TYP_CD"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("GRP_MAIL_EXCL_SK").alias("GRP_MAIL_EXCL_SK"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

df_xfrm_PKEYgen_out2 = (
    df_enriched
    .select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("GRP_ID").alias("GRP_ID"),
        F.col("MAIL_TYP_CD").alias("MAIL_TYP_CD"),
        F.col("GRP_MAIL_EXCL_SK").alias("GRP_MAIL_EXCL_SK"),
        F.when(F.col("orig_GRP_MAIL_EXCL_SK").isNull(), EDWRunCycleDate)
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK")).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.when(F.col("orig_GRP_MAIL_EXCL_SK").isNull(), EDWRunCycle)
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

spark.sql("DROP TABLE IF EXISTS STAGING.IdsEdwGrpMailExclDPkey_db2_KGrpMailExclDLoad_temp")

df_xfrm_PKEYgen_out1.write \
    .format("jdbc") \
    .option("url", jdbc_url_EDW) \
    .options(**jdbc_props_EDW) \
    .option("dbtable", "STAGING.IdsEdwGrpMailExclDPkey_db2_KGrpMailExclDLoad_temp") \
    .mode("append") \
    .save()

merge_sql_db2_KGrpMailExclDLoad = """
MERGE """ + "$EDWOwner" + """.K_GRP_MAIL_EXCL_D AS T
USING STAGING.IdsEdwGrpMailExclDPkey_db2_KGrpMailExclDLoad_temp AS S
ON (
  T.SRC_SYS_CD = S.SRC_SYS_CD
  AND T.GRP_ID = S.GRP_ID
  AND T.MAIL_TYP_CD = S.MAIL_TYP_CD
)
WHEN MATCHED THEN
  UPDATE SET
    T.SRC_SYS_CD = S.SRC_SYS_CD,
    T.GRP_ID = S.GRP_ID,
    T.MAIL_TYP_CD = S.MAIL_TYP_CD,
    T.CRT_RUN_CYC_EXCTN_DT_SK = S.CRT_RUN_CYC_EXCTN_DT_SK,
    T.GRP_MAIL_EXCL_SK = S.GRP_MAIL_EXCL_SK,
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN
  INSERT (
    SRC_SYS_CD,
    GRP_ID,
    MAIL_TYP_CD,
    CRT_RUN_CYC_EXCTN_DT_SK,
    GRP_MAIL_EXCL_SK,
    CRT_RUN_CYC_EXCTN_SK
  )
  VALUES (
    S.SRC_SYS_CD,
    S.GRP_ID,
    S.MAIL_TYP_CD,
    S.CRT_RUN_CYC_EXCTN_DT_SK,
    S.GRP_MAIL_EXCL_SK,
    S.CRT_RUN_CYC_EXCTN_SK
  )
;
"""

execute_dml(merge_sql_db2_KGrpMailExclDLoad, jdbc_url_EDW, jdbc_props_EDW)

df_jn_PKEYs = df_cpy_MultiStreams_out2.alias("lnk_FullDataJnIn").join(
    df_xfrm_PKEYgen_out2.alias("lnk_PKEYxfrmOut"),
    on=[
        F.col("lnk_FullDataJnIn.SRC_SYS_CD") == F.col("lnk_PKEYxfrmOut.SRC_SYS_CD"),
        F.col("lnk_FullDataJnIn.GRP_ID") == F.col("lnk_PKEYxfrmOut.GRP_ID"),
        F.col("lnk_FullDataJnIn.MAIL_TYP_CD") == F.col("lnk_PKEYxfrmOut.MAIL_TYP_CD")
    ],
    how="left"
)

df_jn_PKEYs_out = df_jn_PKEYs.select(
    F.col("lnk_PKEYxfrmOut.GRP_MAIL_EXCL_SK").alias("GRP_MAIL_EXCL_SK"),
    F.col("lnk_FullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_FullDataJnIn.GRP_ID").alias("GRP_ID"),
    F.col("lnk_FullDataJnIn.MAIL_TYP_CD").alias("MAIL_TYP_CD"),
    F.col("lnk_PKEYxfrmOut.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_FullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_FullDataJnIn.GRP_SK").alias("GRP_SK"),
    F.col("lnk_FullDataJnIn.GRP_MAIL_EXCL_DESC").alias("GRP_MAIL_EXCL_DESC"),
    F.col("lnk_FullDataJnIn.GRP_MAIL_EXCL_MAIL_TYP_NM").alias("GRP_MAIL_EXCL_MAIL_TYP_NM"),
    F.col("lnk_PKEYxfrmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_FullDataJnIn.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_seq_GRP_MAIL_EXCL_D_csv_PKey_out = (
    df_jn_PKEYs_out
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
    )
    .select(
        "GRP_MAIL_EXCL_SK",
        "SRC_SYS_CD",
        "GRP_ID",
        "MAIL_TYP_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "GRP_SK",
        "GRP_MAIL_EXCL_DESC",
        "GRP_MAIL_EXCL_MAIL_TYP_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

write_files(
    df_seq_GRP_MAIL_EXCL_D_csv_PKey_out,
    f"{adls_path}/load/GRP_MAIL_EXCL_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)