# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Hugh Sisson                2009-06-14          3500                           Original program                                                                                           
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela     06/14/2013        5114                              Create Load File for EDW Table PROV_SPEC_PROV_CAT_XREF_D      EnterpriseWhseDevl    Peter Marshall         8/15/2013
# MAGIC Santosh Bokka         05/15/2014         TFS 2363                       Added 2 columns CRT_RUN_CYC_EXCTN_DT_SK 
# MAGIC                                                                                                    and LAST_UPDT_RUN_CYC_EXCTN_DT_SK                                       EnterpriseNewDevl       Kalyan Neelam        2014-05-23

# MAGIC Write PROV_SPEC_PROV_CAT_XREF_D Data into a Sequential file for Load Ready Job.
# MAGIC Read data from source table PROV_SPEC_PROV_CAT_XREF
# MAGIC Job name: IdsEdwProvSpecProvCatXrefDExtr
# MAGIC 
# MAGIC Job Extract data from IDS table and apply Fkey and Code denormalization, create a load ready file.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# Parameters
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
IDSRunCycle = get_widget_value("IDSRunCycle","")
EDWRunCycleDate = get_widget_value("EDWRunCycleDate","")
EDWRunCycle = get_widget_value("EDWRunCycle","")

# DB2: db2_PROV_SPEC_PROV_CAT_XREF_Extr
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_PROV_SPEC_PROV_CAT_XREF_Extr = (
    "SELECT  XREF.PROV_SPEC_PROV_CAT_XREF_SK,       "
    "      XREF.PROV_SPEC_CD,       "
    "      XREF.PROV_CAT_CD,       "
    "      XREF.CRT_RUN_CYC_EXCTN_SK,       "
    "      XREF.LAST_UPDT_RUN_CYC_EXCTN_SK,       "
    "      XREF.PROV_CAT_SK,       "
    "      XREF.PROV_SPEC_CD_SK,       "
    "      XREF.WEB_SRCH_IN,       "
    "      XREF.USER_ID,       "
    "      XREF.LAST_UPDT_DT_SK      "
    "FROM "
    + IDSOwner
    + ".PROV_SPEC_PROV_CAT_XREF  XREF "
    "WHERE "
    "XREF.LAST_UPDT_RUN_CYC_EXCTN_SK >= "
    + IDSRunCycle
)
df_db2_PROV_SPEC_PROV_CAT_XREF_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_SPEC_PROV_CAT_XREF_Extr)
    .load()
)

# DB2: db2_PROV_CAT
extract_query_db2_PROV_CAT = (
    "SELECT\nPROV_CAT_SK,\nPROV_CAT_NM \nFROM \n "
    + IDSOwner
    + ".PROV_CAT "
)
df_db2_PROV_CAT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_CAT)
    .load()
)

# DB2: DB2_PROV_SPEC_CD
extract_query_DB2_PROV_SPEC_CD = (
    "SELECT \nPROV_SPEC_CD_SK,\n PROV_SPEC_NM\n FROM   \n"
    + IDSOwner
    + ".PROV_SPEC_CD "
)
df_DB2_PROV_SPEC_CD = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_DB2_PROV_SPEC_CD)
    .load()
)

# Lookup: lkp_Codes
df_db2_PROV_SPEC_PROV_CAT_XREF_Extr_ali = df_db2_PROV_SPEC_PROV_CAT_XREF_Extr.alias("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC")
df_DB2_PROV_SPEC_CD_ali = df_DB2_PROV_SPEC_CD.alias("Lnk_ProvSpecCd")
df_db2_PROV_CAT_ali = df_db2_PROV_CAT.alias("Lnk_ProvCat")

df_lkp_Codes_joined = (
    df_db2_PROV_SPEC_PROV_CAT_XREF_Extr_ali
    .join(
        df_DB2_PROV_SPEC_CD_ali,
        df_db2_PROV_SPEC_PROV_CAT_XREF_Extr_ali["PROV_SPEC_CD_SK"] == df_DB2_PROV_SPEC_CD_ali["PROV_SPEC_CD_SK"],
        "left"
    )
    .join(
        df_db2_PROV_CAT_ali,
        df_db2_PROV_SPEC_PROV_CAT_XREF_Extr_ali["PROV_CAT_SK"] == df_db2_PROV_CAT_ali["PROV_CAT_SK"],
        "left"
    )
)

df_lkp_Codes = df_lkp_Codes_joined.select(
    col("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC.PROV_SPEC_PROV_CAT_XREF_SK").alias("PROV_SPEC_PROV_CAT_XREF_SK"),
    col("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC.PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC.PROV_CAT_CD").alias("PROV_CAT_CD"),
    col("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC.PROV_SPEC_CD_SK").alias("PROV_SPEC_CD_SK"),
    col("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC.WEB_SRCH_IN").alias("WEB_SRCH_IN"),
    col("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC.USER_ID").alias("USER_ID"),
    col("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    col("Lnk_ProvSpecCd.PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    col("Lnk_ProvCat.PROV_CAT_NM").alias("PROV_CAT_NM"),
    col("Lnk_IdsEdwProvSpecProvCatXrefDExtr_InABC.PROV_CAT_SK").alias("PROV_CAT_SK")
)

# Transformer: xmf_businessLogic
df_xmf_businessLogic = df_lkp_Codes.select(
    col("PROV_SPEC_PROV_CAT_XREF_SK").alias("PROV_SPEC_PROV_CAT_XREF_SK"),
    col("PROV_SPEC_CD").alias("PROV_SPEC_CD"),
    col("PROV_CAT_CD").alias("PROV_CAT_CD"),
    when(trim(col("PROV_CAT_NM")) == "", "UNK").otherwise(col("PROV_CAT_NM")).alias("_TEMP_PROV_CAT_NM"),
    when(trim(col("PROV_SPEC_NM")) == "", "UNK").otherwise(col("PROV_SPEC_NM")).alias("_TEMP_PROV_SPEC_NM"),
    col("WEB_SRCH_IN").alias("WEB_SRCH_IN"),
    col("USER_ID").alias("USER_ID"),
    col("LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
    col("PROV_CAT_SK").alias("_TEMP_PROV_CAT_SK"),
    col("PROV_SPEC_CD_SK").alias("_TEMP_PROV_SPEC_SK")
)

df_xmf_businessLogic = df_xmf_businessLogic.select(
    col("PROV_SPEC_PROV_CAT_XREF_SK"),
    col("PROV_SPEC_CD"),
    col("PROV_CAT_CD"),
    # CRT_RUN_CYC_EXCTN_DT_SK
    when(col("PROV_SPEC_PROV_CAT_XREF_SK").isNotNull(), EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    # LAST_UPDT_RUN_CYC_EXCTN_DT_SK
    when(col("PROV_SPEC_PROV_CAT_XREF_SK").isNotNull(), EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("_TEMP_PROV_CAT_SK").alias("PROV_CAT_SK"),
    col("_TEMP_PROV_SPEC_SK").alias("PROV_SPEC_SK"),
    col("_TEMP_PROV_CAT_NM").alias("PROV_CAT_NM"),
    col("_TEMP_PROV_SPEC_NM").alias("PROV_SPEC_NM"),
    col("WEB_SRCH_IN"),
    col("USER_ID"),
    col("LAST_UPDT_DT_SK"),
    when(col("PROV_SPEC_PROV_CAT_XREF_SK").isNotNull(), EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    when(col("PROV_SPEC_PROV_CAT_XREF_SK").isNotNull(), EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Writing to PxSequentialFile: Seq_PROV_SPEC_PROV_CAT_XREF_D_Load
df_final = df_xmf_businessLogic.select(
    col("PROV_SPEC_PROV_CAT_XREF_SK"),
    col("PROV_SPEC_CD"),
    col("PROV_CAT_CD"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("PROV_CAT_SK"),
    col("PROV_SPEC_SK"),
    col("PROV_CAT_NM"),
    col("PROV_SPEC_NM"),
    rpad(col("WEB_SRCH_IN"), 1, " ").alias("WEB_SRCH_IN"),
    col("USER_ID"),
    rpad(col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/PROV_SPEC_PROV_CAT_XREF_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)