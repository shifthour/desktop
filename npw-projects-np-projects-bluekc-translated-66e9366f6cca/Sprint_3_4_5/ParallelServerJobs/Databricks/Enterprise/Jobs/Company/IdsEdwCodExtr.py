# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME : IdsEdwCodLoad
# MAGIC 
# MAGIC CALLED BY: IdsEdwCodCntl
# MAGIC 
# MAGIC PROCESSING: Extracts and process Company data sourced from ids.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                      Project/Altiris #                    Change Description                             Development Project              Code Reviewer          Date Reviewed
# MAGIC ----------------------------------      -------------------         -----------------------------------        -----------------------------------------------------         ----------------------------------               ----------------------------      -------------------------   
# MAGIC Raj Kommineni                 2020-04-30                  5879                                Originally Programmed                         EnterpriseDev2

# MAGIC Assign State Name from the Code mappings
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
EDWOwner = get_widget_value('EDWOwner','')
CurrDt = get_widget_value('CurrDt','')
IdsRunCycle = get_widget_value('IdsRunCycle','')
ids_secret_name = get_widget_value('ids_secret_name','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT CO_SK,CO_ID,SRC_SYS_CD,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,CO_LGL_NM,FULL_CO_MAIL_ADDR_TX,CO_MAIL_ADDR_LN_1,CO_MAIL_ADDR_LN_2,CO_MAIL_ADDR_CITY_NM,CO_MAIL_ADDR_ST_CD_SK,CO_MAIL_ADDR_ZIP_CD_5,CO_MAIL_ADDR_ZIP_CD_4,NAIC_ID,TAX_ID FROM {IDSOwner}.CO WHERE LAST_UPDT_RUN_CYC_EXCTN_SK >= {IdsRunCycle}"
df_coextr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query = f"Select CD_MPPNG_SK, TRGT_CD_NM, TRGT_CD from {EDWOwner}.CD_MPPNG where SRC_SYS_CD = 'IDS' AND SRC_DOMAIN_NM = 'STATE'"
df_cdstextr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_lkpld = (
    df_coextr.alias("coextr")
    .join(
        df_cdstextr.alias("cdstextr"),
        F.col("coextr.CO_MAIL_ADDR_ST_CD_SK") == F.col("cdstextr.CD_MPPNG_SK"),
        how="left",
    )
    .select(
        F.col("coextr.CO_SK").alias("CO_SK"),
        F.col("coextr.CO_ID").alias("CO_ID"),
        F.col("coextr.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("coextr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("coextr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("coextr.CO_LGL_NM").alias("CO_LGL_NM"),
        F.col("coextr.FULL_CO_MAIL_ADDR_TX").alias("FULL_CO_MAIL_ADDR_TX"),
        F.col("coextr.CO_MAIL_ADDR_LN_1").alias("CO_MAIL_ADDR_LN_1"),
        F.col("coextr.CO_MAIL_ADDR_LN_2").alias("CO_MAIL_ADDR_LN_2"),
        F.col("coextr.CO_MAIL_ADDR_CITY_NM").alias("CO_MAIL_ADDR_CITY_NM"),
        F.col("coextr.CO_MAIL_ADDR_ST_CD_SK").alias("CO_MAIL_ADDR_ST_CD_SK"),
        F.col("coextr.CO_MAIL_ADDR_ZIP_CD_5").alias("CO_MAIL_ADDR_ZIP_CD_5"),
        F.col("coextr.CO_MAIL_ADDR_ZIP_CD_4").alias("CO_MAIL_ADDR_ZIP_CD_4"),
        F.col("coextr.NAIC_ID").alias("NAIC_ID"),
        F.col("coextr.TAX_ID").alias("TAX_ID"),
        F.col("cdstextr.TRGT_CD").alias("TRGT_CD"),
        F.col("cdstextr.TRGT_CD_NM").alias("TRGT_CD_NM"),
    )
)

df_pkeyload = (
    df_lkpld.withColumn(
        "SRC_SYS_CD",
        F.when(F.col("SRC_SYS_CD") == F.lit(1580), F.lit("WORKDAY")).otherwise(F.lit("NOSOURCE")),
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrDt))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrDt))
    .withColumn("CO_MAIL_ADDR_ST_CD", F.col("TRGT_CD"))
    .withColumn(
        "CO_MAIL_ADDR_ST_NM",
        F.when(
            F.length(
                F.trim(
                    F.when(F.col("TRGT_CD_NM").isNotNull(), F.col("TRGT_CD_NM")).otherwise(F.lit(""))
                )
            )
            == 0,
            F.lit(" "),
        ).otherwise(F.col("TRGT_CD_NM")),
    )
    .withColumn(
        "CO_MAIL_ADDR_ZIP_CD_5",
        F.rpad(F.col("CO_MAIL_ADDR_ZIP_CD_5"), 5, " "),
    )
    .withColumn(
        "CO_MAIL_ADDR_ZIP_CD_4",
        F.rpad(F.col("CO_MAIL_ADDR_ZIP_CD_4"), 4, " "),
    )
    .withColumn("CO_SK", F.col("CO_SK"))
    .withColumn("CO_ID", F.col("CO_ID"))
    .withColumn("CO_LGL_NM", F.col("CO_LGL_NM"))
    .withColumn("FULL_CO_MAIL_ADDR_TX", F.col("FULL_CO_MAIL_ADDR_TX"))
    .withColumn("CO_MAIL_ADDR_LN_1", F.col("CO_MAIL_ADDR_LN_1"))
    .withColumn("CO_MAIL_ADDR_LN_2", F.col("CO_MAIL_ADDR_LN_2"))
    .withColumn("CO_MAIL_ADDR_CITY_NM", F.col("CO_MAIL_ADDR_CITY_NM"))
    .withColumn("NAIC_ID", F.col("NAIC_ID"))
    .withColumn("TAX_ID", F.col("TAX_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("CO_MAIL_ADDR_ST_CD_SK", F.col("CO_MAIL_ADDR_ST_CD_SK"))
    .select(
        "CO_SK",
        "CO_ID",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "CO_LGL_NM",
        "FULL_CO_MAIL_ADDR_TX",
        "CO_MAIL_ADDR_LN_1",
        "CO_MAIL_ADDR_LN_2",
        "CO_MAIL_ADDR_CITY_NM",
        "CO_MAIL_ADDR_ST_CD",
        "CO_MAIL_ADDR_ST_NM",
        "CO_MAIL_ADDR_ZIP_CD_5",
        "CO_MAIL_ADDR_ZIP_CD_4",
        "NAIC_ID",
        "TAX_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CO_MAIL_ADDR_ST_CD_SK",
    )
)

write_files(
    df_pkeyload,
    f"{adls_path}/load/EDW.CO_D.Load.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=True,
    quote="\"",
    nullValue=None,
)