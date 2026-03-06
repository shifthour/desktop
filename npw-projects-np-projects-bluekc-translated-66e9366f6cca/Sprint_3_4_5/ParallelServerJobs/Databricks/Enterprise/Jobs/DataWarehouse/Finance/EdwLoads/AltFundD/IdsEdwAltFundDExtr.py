# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                             Date                  Project/Altiris #                                   Change Description                                                    Development Project     Code Reviewer            Date Reviewed
# MAGIC ----------------------------------        -------------------      -----------------------------------    ---------------------------------------------------------------------------------------------             ----------------------------------    ---------------------------------    -------------------------   
# MAGIC Naren                                8/20/2007                       3259                          Originally Programmed                                                                        devlEDW10          Steph Goddard            10/07/2007
# MAGIC 
# MAGIC 
# MAGIC                                          
# MAGIC Raj Mangalampally           2013-08-14                      5114                             Original Programming                                                                 EnterpriseWrhsDevl    Peter Marshall              12/10/2013
# MAGIC                                                                                                                   (Server to Parallel Conversion)

# MAGIC This is Source extract data from an IDS table and apply Code denormalization, create a load ready file.
# MAGIC 
# MAGIC Job Name:
# MAGIC IdsEdwAltFundDExtr
# MAGIC 
# MAGIC Table:
# MAGIC ALT_FUND_D
# MAGIC Read from source table ALT_FUND
# MAGIC This is a load ready file conforms to the target table structure.
# MAGIC 
# MAGIC Write ALT_FUND_D Data into a Sequential file for Load Job.
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Add Defaults and Null Handling
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC 1) ALT_FUND_ST_CD_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_db2_ALT_FUND_D_in = (
    "SELECT \n"
    "ALT_FUND.ALT_FUND_SK,\n"
    "COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,\n"
    "ALT_FUND.SRC_SYS_CD_SK,\n"
    "ALT_FUND.ALT_FUND_UNIQ_KEY,\n"
    "ALT_FUND.CRT_RUN_CYC_EXCTN_SK,\n"
    "ALT_FUND.ALT_FUND_ID,\n"
    "ALT_FUND.ALT_FUND_NM,\n"
    "ALT_FUND.EFF_DT_SK,\n"
    "ALT_FUND.TERM_DT_SK,\n"
    "ALT_FUND.ADDR_LN_1,\n"
    "ALT_FUND.ADDR_LN_2,\n"
    "ALT_FUND.ADDR_LN_3,\n"
    "ALT_FUND.CITY_NM,\n"
    "ALT_FUND.ALT_FUND_ST_CD_SK,\n"
    "ALT_FUND.POSTAL_CD,\n"
    "ALT_FUND.EMAIL_ADDR_TX \n"
    "FROM " + IDSOwner + ".ALT_FUND ALT_FUND\n"
    "LEFT JOIN " + IDSOwner + ".CD_MPPNG CD\n"
    "ON ALT_FUND.SRC_SYS_CD_SK = CD.CD_MPPNG_SK"
)
df_db2_ALT_FUND_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_ALT_FUND_D_in)
    .load()
)

extract_query_db2_CD_MPPNG_Extr = (
    "SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from "
    + IDSOwner
    + ".CD_MPPNG"
)
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_lkp_Codes = (
    df_db2_ALT_FUND_D_in.alias("lnk_IdsEdwAltFundDExtr_InABC")
    .join(
        df_db2_CD_MPPNG_Extr.alias("lnk_RefAltFundStCdSkOut"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ALT_FUND_ST_CD_SK")
        == F.col("lnk_RefAltFundStCdSkOut.CD_MPPNG_SK"),
        "left",
    )
    .select(
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ALT_FUND_UNIQ_KEY").alias("ALT_FUND_UNIQ_KEY"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.CITY_NM").alias("CITY_NM"),
        F.col("lnk_RefAltFundStCdSkOut.TRGT_CD").alias("TRGT_CD"),
        F.col("lnk_RefAltFundStCdSkOut.TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.POSTAL_CD").alias("POSTAL_CD"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_IdsEdwAltFundDExtr_InABC.ALT_FUND_ST_CD_SK").alias("ALT_FUND_ST_CD_SK"),
    )
)

df_temp = (
    df_lkp_Codes.withColumn(
        "svPostalCd",
        F.when(
            F.col("POSTAL_CD").isNull() | (trim(F.col("POSTAL_CD")) == ""),
            F.lit(""),
        ).otherwise(FORMAT.POSTALCD.EE(F.col("POSTAL_CD"))),
    )
    .withColumn(
        "svZipCode4",
        F.when(
            (F.length(trim(F.col("svPostalCd"))) < 6) | (F.col("svPostalCd") == ""),
            F.lit(None),
        ).otherwise(F.substring(F.col("svPostalCd"), 6, 4)),
    )
    .withColumn(
        "svZipCode5",
        F.when(
            (F.length(trim(F.substring(F.col("svPostalCd"), 1, 5))) == 0)
            | (F.col("svPostalCd") == ""),
            F.lit("     "),
        ).otherwise(F.substring(F.col("svPostalCd"), 1, 5)),
    )
)

df_AltFundDMainExtr = (
    df_temp.filter((F.col("ALT_FUND_SK") != 0) & (F.col("ALT_FUND_SK") != 1))
    .select(
        F.col("ALT_FUND_SK").alias("ALT_FUND_SK"),
        F.when(
            trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")
        ).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        F.col("ALT_FUND_UNIQ_KEY").alias("ALT_FUND_UNIQ_KEY"),
        F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("ALT_FUND_ID").alias("ALT_FUND_ID"),
        F.col("ALT_FUND_NM").alias("ALT_FUND_NM"),
        F.col("ADDR_LN_1").alias("ALT_FUND_ADDR_LN_1"),
        F.col("ADDR_LN_2").alias("ALT_FUND_ADDR_LN_2"),
        F.col("ADDR_LN_3").alias("ALT_FUND_ADDR_LN_3"),
        F.col("CITY_NM").alias("ALT_FUND_CITY_NM"),
        F.col("TRGT_CD").alias("ALT_FUND_ST_CD"),
        F.col("TRGT_CD_NM").alias("ALT_FUND_ST_NM"),
        F.col("svZipCode5").alias("ALT_FUND_ZIP_CD_5"),
        F.col("svZipCode4").alias("ALT_FUND_ZIP_CD_4"),
        F.col("EMAIL_ADDR_TX").alias("ALT_FUND_EMAIL_ADDR_TX"),
        F.col("EFF_DT_SK").alias("ALT_FUND_EFF_DT_SK"),
        F.col("TERM_DT_SK").alias("ALT_FUND_TERM_DT_SK"),
        F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ALT_FUND_ST_CD_SK").alias("ALT_FUND_ST_CD_SK"),
    )
)

df_UNK = (
    df_temp.limit(1)
    .select(
        F.lit(0).alias("ALT_FUND_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit(0).alias("ALT_FUND_UNIQ_KEY"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("UNK").alias("ALT_FUND_ID"),
        F.lit("UNK").alias("ALT_FUND_NM"),
        F.lit("").alias("ALT_FUND_ADDR_LN_1"),
        F.lit("").alias("ALT_FUND_ADDR_LN_2"),
        F.lit("").alias("ALT_FUND_ADDR_LN_3"),
        F.lit("UNK").alias("ALT_FUND_CITY_NM"),
        F.lit("UNK").alias("ALT_FUND_ST_CD"),
        F.lit("UNK").alias("ALT_FUND_ST_NM"),
        F.lit("").alias("ALT_FUND_ZIP_CD_5"),
        F.lit("").alias("ALT_FUND_ZIP_CD_4"),
        F.lit("").alias("ALT_FUND_EMAIL_ADDR_TX"),
        F.lit("1753-01-01").alias("ALT_FUND_EFF_DT_SK"),
        F.lit("1753-01-01").alias("ALT_FUND_TERM_DT_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("ALT_FUND_ST_CD_SK"),
    )
)

df_NA = (
    df_temp.limit(1)
    .select(
        F.lit(1).alias("ALT_FUND_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit(1).alias("ALT_FUND_UNIQ_KEY"),
        F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("NA").alias("ALT_FUND_ID"),
        F.lit("NA").alias("ALT_FUND_NM"),
        F.lit("").alias("ALT_FUND_ADDR_LN_1"),
        F.lit("").alias("ALT_FUND_ADDR_LN_2"),
        F.lit("").alias("ALT_FUND_ADDR_LN_3"),
        F.lit("NA").alias("ALT_FUND_CITY_NM"),
        F.lit("NA").alias("ALT_FUND_ST_CD"),
        F.lit("NA").alias("ALT_FUND_ST_NM"),
        F.lit("").alias("ALT_FUND_ZIP_CD_5"),
        F.lit("").alias("ALT_FUND_ZIP_CD_4"),
        F.lit("").alias("ALT_FUND_EMAIL_ADDR_TX"),
        F.lit("1753-01-01").alias("ALT_FUND_EFF_DT_SK"),
        F.lit("1753-01-01").alias("ALT_FUND_TERM_DT_SK"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("ALT_FUND_ST_CD_SK"),
    )
)

df_fnl_UNK_NA = (
    df_AltFundDMainExtr.unionByName(df_UNK)
    .unionByName(df_NA)
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "),
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "),
    )
    .withColumn(
        "ALT_FUND_ZIP_CD_5",
        rpad(F.col("ALT_FUND_ZIP_CD_5"), 5, " "),
    )
    .withColumn(
        "ALT_FUND_ZIP_CD_4",
        rpad(F.col("ALT_FUND_ZIP_CD_4"), 4, " "),
    )
    .withColumn(
        "ALT_FUND_EFF_DT_SK",
        rpad(F.col("ALT_FUND_EFF_DT_SK"), 10, " "),
    )
    .withColumn(
        "ALT_FUND_TERM_DT_SK",
        rpad(F.col("ALT_FUND_TERM_DT_SK"), 10, " "),
    )
)

df_final = df_fnl_UNK_NA.select(
    "ALT_FUND_SK",
    "SRC_SYS_CD",
    "ALT_FUND_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "ALT_FUND_ID",
    "ALT_FUND_NM",
    "ALT_FUND_ADDR_LN_1",
    "ALT_FUND_ADDR_LN_2",
    "ALT_FUND_ADDR_LN_3",
    "ALT_FUND_CITY_NM",
    "ALT_FUND_ST_CD",
    "ALT_FUND_ST_NM",
    "ALT_FUND_ZIP_CD_5",
    "ALT_FUND_ZIP_CD_4",
    "ALT_FUND_EMAIL_ADDR_TX",
    "ALT_FUND_EFF_DT_SK",
    "ALT_FUND_TERM_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ALT_FUND_ST_CD_SK",
)

write_files(
    df_final,
    f"{adls_path}/load/ALT_FUND_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None,
)