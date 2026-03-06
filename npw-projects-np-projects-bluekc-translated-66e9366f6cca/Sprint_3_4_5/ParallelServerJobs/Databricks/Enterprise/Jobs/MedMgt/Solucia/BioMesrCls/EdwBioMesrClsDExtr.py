# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwBioMesrClsDExtr
# MAGIC 
# MAGIC DESCRIPTION:  Extracts from IDS BIO_MESR_CLS to flatfile BIO_MESR_CLS_D.dat
# MAGIC       
# MAGIC OUTPUTS: 
# MAGIC                     Flatfile: BIO_MESR_CLS_D.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                      Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------                               ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 12/29/2011       CDC/D4765                  Original Programming.                                                    EnterpriseCurDevl          Sharon Andrew	         2012-01-10

# MAGIC Extract IDS Data
# MAGIC Apply business logic.
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
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value("IDSOwner", "$PROJDEF")
ids_secret_name = get_widget_value("ids_secret_name", "")
CurrentDate = get_widget_value("CurrentDate", "2011-12-29")
EdwRunCycle = get_widget_value("EdwRunCycle", "100")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT BIO_MESR_CLS_SK,BIO_MESR_TYP_CD,GNDR_CD,AGE_RNG_MIN_YR_NO,AGE_RNG_MAX_YR_NO,BIO_MESR_RNG_LOW_NO,BIO_MESR_RNG_HI_NO,EFF_DT_SK,SRC_SYS_CD_SK,CRT_RUN_CYC_EXCTN_SK,LAST_UPDT_RUN_CYC_EXCTN_SK,BIO_CLS_CD_SK,BIO_MESR_TYP_CD_SK,GNDR_CD_SK,TERM_DT_SK,BIO_CLS_RANK_NO,LAST_UPDT_USER_ID,LAST_UPDT_DT_SK FROM {IDSOwner}.BIO_MESR_CLS"
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_cdma_codes = spark.read.parquet(f"{adls_path}/hf_cdma_codes.parquet")

df_busrules = (
    df_IDS.alias("Extract")
    .join(
        df_hf_cdma_codes.alias("refSrcSysCd"),
        F.col("Extract.SRC_SYS_CD_SK") == F.col("refSrcSysCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("refBioClsNm"),
        F.col("Extract.BIO_CLS_CD_SK") == F.col("refBioClsNm.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("refBioMesrTypCd"),
        F.col("Extract.BIO_MESR_TYP_CD_SK") == F.col("refBioMesrTypCd.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_hf_cdma_codes.alias("refGndrCd"),
        F.col("Extract.GNDR_CD_SK") == F.col("refGndrCd.CD_MPPNG_SK"),
        "left",
    )
)

df_enriched = (
    df_busrules
    .withColumn("BIO_MESR_CLS_SK", F.col("Extract.BIO_MESR_CLS_SK"))
    .withColumn("BIO_MESR_TYP_CD", F.col("Extract.BIO_MESR_TYP_CD"))
    .withColumn("GNDR_CD", F.col("Extract.GNDR_CD"))
    .withColumn("AGE_RNG_MIN_YR_NO", F.col("Extract.AGE_RNG_MIN_YR_NO"))
    .withColumn("AGE_RNG_MAX_YR_NO", F.col("Extract.AGE_RNG_MAX_YR_NO"))
    .withColumn("BIO_MESR_RNG_LOW_NO", F.col("Extract.BIO_MESR_RNG_LOW_NO"))
    .withColumn("BIO_MESR_RNG_HI_NO", F.col("Extract.BIO_MESR_RNG_HI_NO"))
    .withColumn("BIO_MESR_CLS_EFF_DT_SK", F.col("Extract.EFF_DT_SK"))
    .withColumn(
        "SRC_SYS_CD",
        F.when(
            F.col("refSrcSysCd.TRGT_CD").isNull()
            | (F.length(trim(F.col("refSrcSysCd.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("refSrcSysCd.TRGT_CD")),
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrentDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(CurrentDate))
    .withColumn(
        "EDW_CUR_RCRD_IN",
        F.when(
            F.col("Extract.TERM_DT_SK") == F.lit("2199-12-31"), F.lit("Y")
        ).otherwise(F.lit("N")),
    )
    .withColumn(
        "BIO_CLS_CD",
        F.when(
            F.col("refBioClsNm.TRGT_CD").isNull()
            | (F.length(trim(F.col("refBioClsNm.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("refBioClsNm.TRGT_CD")),
    )
    .withColumn(
        "BIO_CLS_NM",
        F.when(
            F.col("refBioClsNm.TRGT_CD").isNull()
            | (F.length(trim(F.col("refBioClsNm.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("refBioClsNm.TRGT_CD_NM")),
    )
    .withColumn(
        "BIO_MESR_TYP_NM",
        F.when(
            F.col("refBioMesrTypCd.TRGT_CD").isNull()
            | (F.length(trim(F.col("refBioMesrTypCd.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("refBioMesrTypCd.TRGT_CD_NM")),
    )
    .withColumn(
        "GNDR_CD_NM",
        F.when(
            F.col("refGndrCd.TRGT_CD").isNull()
            | (F.length(trim(F.col("refGndrCd.TRGT_CD"))) == 0),
            F.lit("NA"),
        ).otherwise(F.col("refGndrCd.TRGT_CD_NM")),
    )
    .withColumn("BIO_MESR_CLS_TERM_DT_SK", F.col("Extract.TERM_DT_SK"))
    .withColumn("BIO_CLS_RANK_NO", F.col("Extract.BIO_CLS_RANK_NO"))
    .withColumn("LAST_UPDT_USER_ID", F.col("Extract.LAST_UPDT_USER_ID"))
    .withColumn("LAST_UPDT_DT_SK", F.col("Extract.LAST_UPDT_DT_SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("Extract.CRT_RUN_CYC_EXCTN_SK"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EdwRunCycle))
    .withColumn("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK", F.col("Extract.LAST_UPDT_RUN_CYC_EXCTN_SK"))
    .withColumn("BIO_CLS_CD_SK", F.col("Extract.BIO_CLS_CD_SK"))
    .withColumn("BIO_MESR_TYP_CD_SK", F.col("Extract.BIO_MESR_TYP_CD_SK"))
    .withColumn("GNDR_CD_SK", F.col("Extract.GNDR_CD_SK"))
    .withColumn("BIO_MESR_CLS_EFF_DT_SK", F.rpad(F.col("BIO_MESR_CLS_EFF_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("EDW_CUR_RCRD_IN", F.rpad(F.col("EDW_CUR_RCRD_IN"), 1, " "))
    .withColumn("BIO_MESR_CLS_TERM_DT_SK", F.rpad(F.col("BIO_MESR_CLS_TERM_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_DT_SK", F.rpad(F.col("LAST_UPDT_DT_SK"), 10, " "))
    .withColumn("SRC_SYS_CD", F.rpad(F.col("SRC_SYS_CD"), <...>, " "))
    .withColumn("BIO_CLS_CD", F.rpad(F.col("BIO_CLS_CD"), <...>, " "))
    .withColumn("BIO_CLS_NM", F.rpad(F.col("BIO_CLS_NM"), <...>, " "))
    .withColumn("BIO_MESR_TYP_NM", F.rpad(F.col("BIO_MESR_TYP_NM"), <...>, " "))
    .withColumn("GNDR_CD_NM", F.rpad(F.col("GNDR_CD_NM"), <...>, " "))
)

df_output = df_enriched.select(
    "BIO_MESR_CLS_SK",
    "BIO_MESR_TYP_CD",
    "GNDR_CD",
    "AGE_RNG_MIN_YR_NO",
    "AGE_RNG_MAX_YR_NO",
    "BIO_MESR_RNG_LOW_NO",
    "BIO_MESR_RNG_HI_NO",
    "BIO_MESR_CLS_EFF_DT_SK",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "EDW_CUR_RCRD_IN",
    "BIO_CLS_CD",
    "BIO_CLS_NM",
    "BIO_MESR_TYP_NM",
    "GNDR_CD_NM",
    "BIO_MESR_CLS_TERM_DT_SK",
    "BIO_CLS_RANK_NO",
    "LAST_UPDT_USER_ID",
    "LAST_UPDT_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BIO_CLS_CD_SK",
    "BIO_MESR_TYP_CD_SK",
    "GNDR_CD_SK"
)

write_files(
    df_output,
    f"{adls_path}/load/BIO_MESR_CLS_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)