# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Oliver Nielsen  09/10/2005    Originally Programmed
# MAGIC 
# MAGIC                                                                                                                                                                                                                                            DATASTAGE              CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                                ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------                          -------------------------------         ------------------------------      -----------------------
# MAGIC Bhupinder Kaur            06/21/2013      5114                               Create Load File for EDW Table TAX_DMGRPHC_D                                                  EnterpriseWhseDevl     Peter Marshall              8/27/2013

# MAGIC Write TAX_DMGRPHC Data into a Sequential file for Load Job IdsEdwProvTaxDmgrphcDLoad.
# MAGIC Read from IDS source table:
# MAGIC TAX_DMGRPHC 
# MAGIC 
# MAGIC Run Cycle filters is applied to get just the needed rows forward
# MAGIC Job: IdsEdwTaxDmgrphcDExtr
# MAGIC Add Defaults and Null Handling.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Code SK lookups for Denormalization
# MAGIC Lookup Keys:
# MAGIC TAX_DMGRPHC_ID_TYP_CD_SK
# MAGIC TAX_DMGRPHC_ST_CD_SK
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = """
SELECT
TAX_DMGRPHC_SK,
SRC_SYS_CD_SK,
TAX_ID,
TAX_DMGRPHC_ID_TYP_CD_SK,
RPTNG_IN,
CORP_TAX_NM,
FIRST_NM,
MIDINIT,
LAST_NM,
TAX_TTL,
ADDR_LN_1,
ADDR_LN_2,
ADDR_LN_3,
CITY_NM,
TAX_DMGRPHC_ST_CD_SK,
POSTAL_CD,
CNTY_NM,
TAX_DMGRPHC_CTRY_CD_SK,
PHN_NO,
PHN_NO_EXT,
FAX_NO,
FAX_NO_EXT,
EMAIL_ADDR_TX
FROM {}.TAX_DMGRPHC
""".format(IDSOwner)
df_db2_TAX_DMGRPHC_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = """
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {}.CD_MPPNG
""".format(IDSOwner)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Ref_TaxDmgrphcStCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Ref_TaxDmgrphcIdTypCd = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_lkp_Codes_1 = df_db2_TAX_DMGRPHC_Extr.alias("lnk_IdsEdwTaxDmgrphcExtr_InABC").join(
    df_Ref_TaxDmgrphcStCd.alias("Ref_TaxDmgrphcStCd"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.TAX_DMGRPHC_ST_CD_SK") == F.col("Ref_TaxDmgrphcStCd.CD_MPPNG_SK"),
    how="left"
)

df_lkp_Codes = df_lkp_Codes_1.join(
    df_Ref_TaxDmgrphcIdTypCd.alias("Ref_TaxDmgrphcIdTypCd"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.TAX_DMGRPHC_ID_TYP_CD_SK") == F.col("Ref_TaxDmgrphcIdTypCd.CD_MPPNG_SK"),
    how="left"
)

df_lkp_CodesLkpData_out = df_lkp_Codes.select(
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.TAX_DMGRPHC_SK").alias("TAX_DMGRPHC_SK"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.TAX_ID").alias("TAX_ID"),
    F.col("Ref_TaxDmgrphcIdTypCd.TRGT_CD").alias("TAX_DMGRPHC_ID_TYP_CD"),
    F.col("Ref_TaxDmgrphcIdTypCd.TRGT_CD_NM").alias("TAX_DMGRPHC_ID_TYP_NM"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.CORP_TAX_NM").alias("TAX_DMGRPHC_CORP_TAX_NM"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.FIRST_NM").alias("TAX_DMGRPHC_FIRST_NM"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.MIDINIT").alias("TAX_DMGRPHC_MIDINIT"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.LAST_NM").alias("TAX_DMGRPHC_LAST_NM"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.TAX_TTL").alias("TAX_DMGRPHC_TAX_TTL"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.ADDR_LN_1").alias("TAX_DMGRPHC_ADDR_LN_1"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.ADDR_LN_2").alias("TAX_DMGRPHC_ADDR_LN_2"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.ADDR_LN_3").alias("TAX_DMGRPHC_ADDR_LN_3"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.CITY_NM").alias("TAX_DMGRPHC_CITY_NM"),
    F.col("Ref_TaxDmgrphcStCd.TRGT_CD").alias("TAX_DMGRPHC_ST_CD"),
    F.col("Ref_TaxDmgrphcStCd.TRGT_CD_NM").alias("TAX_DMGRPHC_ST_NM"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.POSTAL_CD").alias("POSTAL_CD"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.CNTY_NM").alias("TAX_DMGRPHC_CNTY_NM"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.PHN_NO").alias("TAX_DMGRPHC_PHN_NO"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.PHN_NO_EXT").alias("TAX_DMGRPHC_PHN_NO_EXT"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.FAX_NO").alias("TAX_DMGRPHC_FAX_NO"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.FAX_NO_EXT").alias("TAX_DMGRPHC_FAX_NO_EXT"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.EMAIL_ADDR_TX").alias("TAX_DMGRPHC_EMAIL_ADDR_TX"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.RPTNG_IN").alias("TAX_DMGRPHC_RPTNG_IN"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.TAX_DMGRPHC_ID_TYP_CD_SK").alias("TAX_DMGRPHC_ID_TYP_CD_SK"),
    F.col("lnk_IdsEdwTaxDmgrphcExtr_InABC.TAX_DMGRPHC_ST_CD_SK").alias("TAX_DMGRPHC_ST_CD_SK")
)

df_xfrm_BusinessLogic = (
    df_lkp_CodesLkpData_out
    .withColumn(
        "svPostalCd",
        F.when(
            F.col("POSTAL_CD").isNull() | (trim(F.col("POSTAL_CD")) == ""),
            F.lit("")
        ).otherwise(
            FORMAT_POSTALCD_EE(F.col("POSTAL_CD"))
        )
    )
    .withColumn(
        "svAddZipCd5",
        F.when(
            (F.length(trim(F.substring("svPostalCd", 1, 5))) == 0) | (F.col("svPostalCd") == ""),
            F.lit(None)
        ).otherwise(
            F.substring("svPostalCd", 1, 5)
        )
    )
    .withColumn(
        "svAddZipCd4",
        F.when(
            (F.length(trim(F.col("svPostalCd"))) < 6) | (F.col("svPostalCd") == ""),
            F.lit(None)
        ).otherwise(
            F.substring("svPostalCd", 6, 4)
        )
    )
    .withColumn(
        "svValidSk",
        F.when(
            (F.col("TAX_DMGRPHC_ST_CD_SK") == 1) | (F.col("TAX_DMGRPHC_ST_CD_SK") == 0),
            F.lit(False)
        ).otherwise(F.lit(True))
    )
)

df_enriched = (
    df_xfrm_BusinessLogic
    .withColumn(
        "TAX_DMGRPHC_ID_TYP_CD",
        F.when(
            F.col("TAX_DMGRPHC_ID_TYP_CD").isNull() | (trim(F.col("TAX_DMGRPHC_ID_TYP_CD")) == ""),
            F.lit("UNK")
        ).otherwise(
            F.col("TAX_DMGRPHC_ID_TYP_CD")
        )
    )
    .withColumn(
        "TAX_DMGRPHC_ST_CD",
        F.when(
            F.col("TAX_DMGRPHC_ST_CD").isNull() | (trim(F.col("TAX_DMGRPHC_ST_CD")) == ""),
            F.lit("UNK")
        ).otherwise(
            F.col("TAX_DMGRPHC_ST_CD")
        )
    )
    .withColumn(
        "TAX_DMGRPHC_ZIP_CD_5",
        F.when(
            (~F.col("svValidSk")) | (Num(F.col("svAddZipCd5")) == False),
            F.lit(None)
        ).otherwise(F.col("svAddZipCd5"))
    )
    .withColumn(
        "TAX_DMGRPHC_ZIP_CD_4",
        F.when(
            (~F.col("svValidSk")) | (Num(F.col("svAddZipCd4")) == False),
            F.lit(None)
        ).otherwise(F.col("svAddZipCd4"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.lit(EDWRunCycleDate))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(EDWRunCycle))
)

df_seq_TAX_DMGRPHC_D_csv_Load = df_enriched.select(
    "TAX_DMGRPHC_SK",
    "TAX_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "TAX_DMGRPHC_ID_TYP_CD",
    "TAX_DMGRPHC_ID_TYP_NM",
    "TAX_DMGRPHC_CORP_TAX_NM",
    "TAX_DMGRPHC_FIRST_NM",
    "TAX_DMGRPHC_MIDINIT",
    "TAX_DMGRPHC_LAST_NM",
    "TAX_DMGRPHC_TAX_TTL",
    "TAX_DMGRPHC_ADDR_LN_1",
    "TAX_DMGRPHC_ADDR_LN_2",
    "TAX_DMGRPHC_ADDR_LN_3",
    "TAX_DMGRPHC_CITY_NM",
    "TAX_DMGRPHC_ST_CD",
    "TAX_DMGRPHC_ST_NM",
    "TAX_DMGRPHC_ZIP_CD_5",
    "TAX_DMGRPHC_ZIP_CD_4",
    "TAX_DMGRPHC_CNTY_NM",
    "TAX_DMGRPHC_PHN_NO",
    "TAX_DMGRPHC_PHN_NO_EXT",
    "TAX_DMGRPHC_FAX_NO",
    "TAX_DMGRPHC_FAX_NO_EXT",
    "TAX_DMGRPHC_EMAIL_ADDR_TX",
    "TAX_DMGRPHC_RPTNG_IN",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "TAX_DMGRPHC_ID_TYP_CD_SK",
    "TAX_DMGRPHC_ST_CD_SK"
)

df_seq_TAX_DMGRPHC_D_csv_Load = (
    df_seq_TAX_DMGRPHC_D_csv_Load
    .withColumn("TAX_DMGRPHC_MIDINIT", F.rpad("TAX_DMGRPHC_MIDINIT", 1, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad("CRT_RUN_CYC_EXCTN_DT_SK", 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", 10, " "))
    .withColumn("TAX_DMGRPHC_PHN_NO_EXT", F.rpad("TAX_DMGRPHC_PHN_NO_EXT", 5, " "))
    .withColumn("TAX_DMGRPHC_FAX_NO_EXT", F.rpad("TAX_DMGRPHC_FAX_NO_EXT", 5, " "))
    .withColumn("TAX_DMGRPHC_RPTNG_IN", F.rpad("TAX_DMGRPHC_RPTNG_IN", 1, " "))
    .withColumn("TAX_DMGRPHC_ZIP_CD_5", F.rpad("TAX_DMGRPHC_ZIP_CD_5", 5, " "))
    .withColumn("TAX_DMGRPHC_ZIP_CD_4", F.rpad("TAX_DMGRPHC_ZIP_CD_4", 4, " "))
)

write_files(
    df_seq_TAX_DMGRPHC_D_csv_Load,
    f"{adls_path}/load/TAX_DMGRPHC_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)