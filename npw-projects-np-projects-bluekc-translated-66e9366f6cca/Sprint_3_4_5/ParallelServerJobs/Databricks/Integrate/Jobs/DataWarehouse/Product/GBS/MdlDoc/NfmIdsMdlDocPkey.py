# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC JOb Name : FctsIdsMdlDocPkey
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# MAGIC DinakarS              2018-05-25               5205                          Original Programming                                                                             IntegrateWrhsDevl            Kalyan Neelam          2018-07-03

# MAGIC Job Name: IdsMdlDocPkey
# MAGIC Remove Duplicates stage takes care of getting rid of duplicate rows with the same Natural key get into the PKEYing step.
# MAGIC A concious effort need to be made to evaluate which one of the duplicate set row to drop is a case by case basis decision.
# MAGIC Table K_MDL_DOC.               Insert new SKEY rows generated as part of this run. This is a database insert operation.
# MAGIC Audit fields are added into this File
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
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSDB = get_widget_value('IDSDB','')
IDSAcct = get_widget_value('IDSAcct','')
IDSPW = get_widget_value('IDSPW','')

jdbc_url_db2KMdlDocExt, jdbc_props_db2KMdlDocExt = get_db_config(ids_secret_name)
extract_query_db2KMdlDocExt = (
    "SELECT \nMDL_DOC_SK,\nMDL_DOC_ID,\nMDL_DOC_EFF_DT,\nSRC_SYS_CD,\nCRT_RUN_CYC_EXCTN_SK\nFROM "
    + IDSOwner
    + ".K_MDL_DOC"
)
df_db2KMdlDocExt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2KMdlDocExt)
    .options(**jdbc_props_db2KMdlDocExt)
    .option("query", extract_query_db2KMdlDocExt)
    .load()
)

df_ds_MDL_DOC_DS = spark.read.parquet(
    f"{adls_path}/ds/MDLDOC.{SrcSysCd}.xfrm.{RunID}.parquet"
)

df_lnkRemDupDataIn = df_ds_MDL_DOC_DS.select(
    col("MDL_DOC_ID"),
    col("MDL_DOC_EFF_DT"),
    col("SRC_SYS_CD")
)

df_lnkFullDataJnIn = df_ds_MDL_DOC_DS.select(
    col("MDL_DOC_ID"),
    col("MDL_DOC_EFF_DT"),
    col("SRC_SYS_CD"),
    col("MDL_DOC_TERM_DT"),
    col("MDL_DOC_NM")
)

df_rdp_NaturalKeys = dedup_sort(
    df_lnkRemDupDataIn,
    ["MDL_DOC_ID", "MDL_DOC_EFF_DT", "SRC_SYS_CD"],
    [("MDL_DOC_ID", "A"), ("MDL_DOC_EFF_DT", "A"), ("SRC_SYS_CD", "A")]
)

df_jn_MdlDoc = df_rdp_NaturalKeys.alias("lnkRemDupDataOut").join(
    df_db2KMdlDocExt.alias("lnkKMdlDocExt"),
    [
        df_rdp_NaturalKeys["MDL_DOC_ID"] == df_db2KMdlDocExt["MDL_DOC_ID"],
        df_rdp_NaturalKeys["MDL_DOC_EFF_DT"] == df_db2KMdlDocExt["MDL_DOC_EFF_DT"],
        df_rdp_NaturalKeys["SRC_SYS_CD"] == df_db2KMdlDocExt["SRC_SYS_CD"],
    ],
    how="left"
)

df_jn_MdlDoc_out = df_jn_MdlDoc.select(
    df_db2KMdlDocExt["MDL_DOC_SK"].alias("MDL_DOC_SK"),
    df_rdp_NaturalKeys["MDL_DOC_ID"].alias("MDL_DOC_ID"),
    df_rdp_NaturalKeys["MDL_DOC_EFF_DT"].alias("MDL_DOC_EFF_DT"),
    df_rdp_NaturalKeys["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_db2KMdlDocExt["CRT_RUN_CYC_EXCTN_SK"].alias("CRT_RUN_CYC_EXCTN_SK")
)

df_xfm_PKEYgen_in = df_jn_MdlDoc_out.withColumnRenamed("MDL_DOC_SK", "originalMDL_DOC_SK")

df_enriched = df_xfm_PKEYgen_in.withColumn(
    "MDL_DOC_SK",
    when((col("originalMDL_DOC_SK").isNull()) | (col("originalMDL_DOC_SK") == lit(0)), None)
    .otherwise(col("originalMDL_DOC_SK"))
)

df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"MDL_DOC_SK",<schema>,<secret_name>)

df_lnk_KMdlDoc_New = df_enriched.filter(
    (col("originalMDL_DOC_SK").isNull()) | (col("originalMDL_DOC_SK") == lit(0))
).select(
    col("MDL_DOC_SK").alias("MDL_DOC_SK"),
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(IDSRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
)

df_lnkPKEYxfmOut = df_enriched.select(
    col("MDL_DOC_SK").alias("MDL_DOC_SK"),
    col("MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    when(
        (col("originalMDL_DOC_SK").isNull()) | (col("originalMDL_DOC_SK") == lit(0)),
        lit(IDSRunCycle),
    )
    .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
    .alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

jdbc_url_db2KMdlDocLoad, jdbc_props_db2KMdlDocLoad = get_db_config(ids_secret_name)
execute_dml(
    "DROP TABLE IF EXISTS STAGING.NfmIdsMdlDocPkey_db2_K_MdlDocLoad_temp",
    jdbc_url_db2KMdlDocLoad,
    jdbc_props_db2KMdlDocLoad
)
df_lnk_KMdlDoc_New.write.format("jdbc").option("url", jdbc_url_db2KMdlDocLoad).options(**jdbc_props_db2KMdlDocLoad).option("dbtable", "STAGING.NfmIdsMdlDocPkey_db2_K_MdlDocLoad_temp").mode("append").save()

merge_sql_db2KMdlDocLoad = f"""
MERGE {IDSOwner}.K_MDL_DOC AS T
USING STAGING.NfmIdsMdlDocPkey_db2_K_MdlDocLoad_temp AS S
ON T.MDL_DOC_SK = S.MDL_DOC_SK
WHEN MATCHED THEN UPDATE SET
T.MDL_DOC_ID = S.MDL_DOC_ID,
T.MDL_DOC_EFF_DT = S.MDL_DOC_EFF_DT,
T.SRC_SYS_CD = S.SRC_SYS_CD,
T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK
WHEN NOT MATCHED THEN INSERT
(MDL_DOC_SK, MDL_DOC_ID, MDL_DOC_EFF_DT, SRC_SYS_CD, CRT_RUN_CYC_EXCTN_SK)
VALUES
(S.MDL_DOC_SK, S.MDL_DOC_ID, S.MDL_DOC_EFF_DT, S.SRC_SYS_CD, S.CRT_RUN_CYC_EXCTN_SK);
"""
execute_dml(merge_sql_db2KMdlDocLoad, jdbc_url_db2KMdlDocLoad, jdbc_props_db2KMdlDocLoad)

df_jn_PKEYs = df_lnkFullDataJnIn.alias("lnkFullDataJnIn").join(
    df_lnkPKEYxfmOut.alias("lnkPKEYxfmOut"),
    [
        col("lnkFullDataJnIn.MDL_DOC_ID") == col("lnkPKEYxfmOut.MDL_DOC_ID"),
        col("lnkFullDataJnIn.MDL_DOC_EFF_DT") == col("lnkPKEYxfmOut.MDL_DOC_EFF_DT"),
        col("lnkFullDataJnIn.SRC_SYS_CD") == col("lnkPKEYxfmOut.SRC_SYS_CD"),
    ],
    how="inner"
)

df_jn_PKEYs_out = df_jn_PKEYs.select(
    col("lnkPKEYxfmOut.MDL_DOC_SK").alias("MDL_DOC_SK"),
    col("lnkFullDataJnIn.MDL_DOC_ID").alias("MDL_DOC_ID"),
    col("lnkFullDataJnIn.MDL_DOC_EFF_DT").alias("MDL_DOC_EFF_DT"),
    col("lnkFullDataJnIn.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnkFullDataJnIn.MDL_DOC_TERM_DT").alias("MDL_DOC_TERM_DT"),
    col("lnkFullDataJnIn.MDL_DOC_NM").alias("MDL_DOC_NM"),
    col("lnkPKEYxfmOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lnkPKEYxfmOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

df_TXN_MDLDOC_in = df_jn_PKEYs_out

df_Lnk_Prod_Pkey_Main = df_TXN_MDLDOC_in.select(
    "MDL_DOC_SK",
    "MDL_DOC_ID",
    "MDL_DOC_EFF_DT",
    "SRC_SYS_CD",
    "MDL_DOC_TERM_DT",
    "MDL_DOC_NM",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK"
)

df_Lnk_Prod_UNK_temp = df_TXN_MDLDOC_in.limit(1)
df_Lnk_Prod_UNK = (
    df_Lnk_Prod_UNK_temp
    .withColumn("MDL_DOC_SK", lit(0))
    .withColumn("MDL_DOC_ID", lit("UNK"))
    .withColumn("MDL_DOC_EFF_DT", lit("1753-01-01"))
    .withColumn("MDL_DOC_TERM_DT", lit("1753-01-01"))
    .withColumn("MDL_DOC_NM", lit(None).cast(StringType()))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(100))
    .select(
        "MDL_DOC_SK",
        "MDL_DOC_ID",
        "MDL_DOC_EFF_DT",
        "SRC_SYS_CD",
        "MDL_DOC_TERM_DT",
        "MDL_DOC_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

df_Lnk_Prod_NA_temp = df_TXN_MDLDOC_in.limit(1)
df_Lnk_Prod_NA = (
    df_Lnk_Prod_NA_temp
    .withColumn("MDL_DOC_SK", lit(1))
    .withColumn("MDL_DOC_ID", lit("NA"))
    .withColumn("MDL_DOC_EFF_DT", lit("1753-01-01"))
    .withColumn("MDL_DOC_TERM_DT", lit("1753-01-01"))
    .withColumn("MDL_DOC_NM", lit(None).cast(StringType()))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(100))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(100))
    .select(
        "MDL_DOC_SK",
        "MDL_DOC_ID",
        "MDL_DOC_EFF_DT",
        "SRC_SYS_CD",
        "MDL_DOC_TERM_DT",
        "MDL_DOC_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

df_fnl_NA_UNK_Streams = (
    df_Lnk_Prod_Pkey_Main
    .unionByName(df_Lnk_Prod_NA)
    .unionByName(df_Lnk_Prod_UNK)
)

df_seq_MDL_DOC_PKEY_out = (
    df_fnl_NA_UNK_Streams
    .withColumn("MDL_DOC_ID", rpad("MDL_DOC_ID", <...>, " "))
    .withColumn("MDL_DOC_EFF_DT", rpad("MDL_DOC_EFF_DT", <...>, " "))
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " "))
    .withColumn("MDL_DOC_TERM_DT", rpad("MDL_DOC_TERM_DT", <...>, " "))
    .withColumn("MDL_DOC_NM", rpad("MDL_DOC_NM", <...>, " "))
    .select(
        "MDL_DOC_SK",
        "MDL_DOC_ID",
        "MDL_DOC_EFF_DT",
        "SRC_SYS_CD",
        "MDL_DOC_TERM_DT",
        "MDL_DOC_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK"
    )
)

write_files(
    df_seq_MDL_DOC_PKEY_out,
    f"{adls_path}/load/MDL_DOC.{SrcSysCd}.pkey.{RunID}.dat",
    delimiter='|',
    mode='overwrite',
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)