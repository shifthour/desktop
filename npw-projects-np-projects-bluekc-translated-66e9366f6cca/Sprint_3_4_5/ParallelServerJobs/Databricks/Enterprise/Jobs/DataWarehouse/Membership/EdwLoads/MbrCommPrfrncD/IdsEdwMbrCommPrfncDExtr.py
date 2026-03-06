# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name:IdsEdwMbrCommPrfncDExtr
# MAGIC 
# MAGIC Calling job: EdwMbrExtrLoadDriverSeq
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:                                                                                                                                                                                                                        DataStage                   Code                 Date 
# MAGIC Developer             Date                  UsSer Story           Change Description                                                                                                                                 Project                         Reviewer           Reviewed
# MAGIC -------------------------    -------------------       --------------------------    ----------------------------------------------------------------------------------------------------------------------------------------------                  ----------------------------        ----------------------    ------------------------- 
# MAGIC Venkata Y            2020-08-06         US#255919          Original Programming                                                                                                                               Enterprise Dev2           Hugh Sisson      2020-08-14

# MAGIC Write MBR_COMM_PRFRNC COMM Data into a Sequential file for Load Job IdsEdwMbrComPrfrncDLoad
# MAGIC Read all the Data from IDS MBR_COMM_PRFRNC COMM Table;
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name: IdsEdwMbrCommPrfncDExtr
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


ids_secret_name = get_widget_value('ids_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')

jdbc_url_app_user, jdbc_props_app_user = get_db_config(ids_secret_name)
extract_query_app_user = f"""select distinct
USER_SK,
USER_ID,
substr (ACTV_DIR_ACCT_NM, 1,18 ) as ACTV_DIR_ACCT_NM
FROM {IDSOwner}.APP_USER"""
df_app_user = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_app_user)
    .options(**jdbc_props_app_user)
    .option("query", extract_query_app_user)
    .load()
)

jdbc_url_CD_MPPNG, jdbc_props_CD_MPPNG = get_db_config(ids_secret_name)
extract_query_CD_MPPNG = f"""SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_CD_MPPNG)
    .options(**jdbc_props_CD_MPPNG)
    .option("query", extract_query_CD_MPPNG)
    .load()
)

df_Ref_trnm = df_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_Lprfnc_nm = df_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_Lprfnc_cd = df_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_ctyp_nm = df_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_Ref_Cpnm = df_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_addr_sk = df_CD_MPPNG.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

jdbc_url_MBR_COMM_PRFRNC, jdbc_props_MBR_COMM_PRFRNC = get_db_config(ids_secret_name)
extract_query_MBR_COMM_PRFRNC = f"""select 
COMM.MBR_COMM_PRFRNC_SK,
COMM.MBR_UNIQ_KEY,
COMM.EFF_DT,
COMM.COMM_TYP_CD,
COMM.MBR_COMM_PRFRNC_CD,
COMM.SRC_SYS_CD,
COMM.CRT_RUN_CYC_EXCTN_SK,
COMM.LAST_UPDT_RUN_CYC_EXCTN_SK,
COMM.MBR_SK,
COMM.SRC_SYS_LAST_UPDT_USER_SK,
COMM.COMM_TYP_CD_SK,
COMM.MBR_COMM_PRFRNC_CD_SK,
COMM.MBR_LANG_PRFRNC_CD_SK,
COMM.SUB_ADDR_CD_SK,
COMM.TERM_RSN_CD_SK,
COMM.OPT_OUT_IN,
COMM.RCVD_DT,
COMM.TERM_DT,
COMM.SRC_SYS_LAST_UPDT_DTM,
COMM.MBR_EMAIL_ADDR,
COMM.MBR_TX_MSG_PHN_NO,
MB.SUB_SK
FROM {IDSOwner}.MBR_COMM_PRFRNC COMM,
{IDSOwner}.MBR MB
WHERE COMM.MBR_SK = MB.MBR_SK"""
df_MBR_COMM_PRFRNC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_MBR_COMM_PRFRNC)
    .options(**jdbc_props_MBR_COMM_PRFRNC)
    .option("query", extract_query_MBR_COMM_PRFRNC)
    .load()
)

jdbc_url_SUB_ADDR, jdbc_props_SUB_ADDR = get_db_config(ids_secret_name)
extract_query_SUB_ADDR = f"""select distinct
SRC_SYS_CD_SK,
SUB_ADDR_CD_SK,
SUB_SK,
ADDR_LN_1,
CITY_NM,
ZIP_CD_5,
SUB_ADDR_ST_CD_SK
FROM {IDSOwner}.SUB_ADDR"""
df_SUB_ADDR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_SUB_ADDR)
    .options(**jdbc_props_SUB_ADDR)
    .option("query", extract_query_SUB_ADDR)
    .load()
)

df_ADDR_LN = df_SUB_ADDR.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("ZIP_CD_5").alias("ZIP_CD_5"),
    F.col("SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK")
)

df_lkp_Add_cds = df_MBR_COMM_PRFRNC.alias("db_p").join(
    df_ADDR_LN.alias("ADDR_LN"),
    [
        F.col("db_p.SUB_ADDR_CD_SK") == F.col("ADDR_LN.SUB_ADDR_CD_SK"),
        F.col("db_p.SUB_SK") == F.col("ADDR_LN.SUB_SK")
    ],
    "left"
).select(
    F.col("db_p.MBR_COMM_PRFRNC_SK").alias("MBR_COMM_PRFRNC_SK"),
    F.col("db_p.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("db_p.EFF_DT").alias("EFF_DT"),
    F.col("db_p.COMM_TYP_CD").alias("COMM_TYP_CD"),
    F.col("db_p.MBR_COMM_PRFRNC_CD").alias("MBR_COMM_PRFRNC_CD"),
    F.col("db_p.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("db_p.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("db_p.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("db_p.MBR_SK").alias("MBR_SK"),
    F.col("db_p.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.col("db_p.COMM_TYP_CD_SK").alias("COMM_TYP_CD_SK"),
    F.col("db_p.MBR_COMM_PRFRNC_CD_SK").alias("MBR_COMM_PRFRNC_CD_SK"),
    F.col("db_p.MBR_LANG_PRFRNC_CD_SK").alias("MBR_LANG_PRFRNC_CD_SK"),
    F.col("db_p.SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
    F.col("db_p.TERM_RSN_CD_SK").alias("TERM_RSN_CD_SK"),
    F.col("db_p.OPT_OUT_IN").alias("OPT_OUT_IN"),
    F.col("db_p.RCVD_DT").alias("RCVD_DT"),
    F.col("db_p.TERM_DT").alias("TERM_DT"),
    F.col("db_p.SRC_SYS_LAST_UPDT_DTM").alias("SRC_SYS_LAST_UPDT_DTM"),
    F.col("db_p.MBR_EMAIL_ADDR").alias("MBR_EMAIL_ADDR"),
    F.col("db_p.MBR_TX_MSG_PHN_NO").alias("MBR_TX_MSG_PHN_NO"),
    F.col("db_p.SUB_SK").alias("SUB_SK"),
    F.col("ADDR_LN.ADDR_LN_1").alias("MBR_COMM_ADDR_LN"),
    F.col("ADDR_LN.CITY_NM").alias("MBR_COMM_CITY_NM"),
    F.col("ADDR_LN.ZIP_CD_5").alias("MBR_COMM_ZIP_CD"),
    F.col("ADDR_LN.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK")
)

df_lkp_mpng_Codes = (
    df_lkp_Add_cds.alias("db2")
    .join(df_Ref_ctyp_nm.alias("Ref_ctyp_nm"), F.col("db2.COMM_TYP_CD_SK") == F.col("Ref_ctyp_nm.CD_MPPNG_SK"), "left")
    .join(df_Ref_Cpnm.alias("Ref_Cpnm"), F.col("db2.MBR_COMM_PRFRNC_CD_SK") == F.col("Ref_Cpnm.CD_MPPNG_SK"), "left")
    .join(df_Ref_Lprfnc_cd.alias("Ref_Lprfnc_cd"), F.col("db2.MBR_LANG_PRFRNC_CD_SK") == F.col("Ref_Lprfnc_cd.CD_MPPNG_SK"), "left")
    .join(df_Ref_Lprfnc_nm.alias("Ref_Lprfnc_nm"), F.col("db2.MBR_LANG_PRFRNC_CD_SK") == F.col("Ref_Lprfnc_nm.CD_MPPNG_SK"), "left")
    .join(df_Ref_trnm.alias("Ref_trnm"), F.col("db2.TERM_RSN_CD_SK") == F.col("Ref_trnm.CD_MPPNG_SK"), "left")
    .join(df_addr_sk.alias("addr_sk"), F.col("db2.SUB_ADDR_ST_CD_SK") == F.col("addr_sk.CD_MPPNG_SK"), "left")
    .select(
        F.col("db2.MBR_COMM_PRFRNC_SK").alias("MBR_COMM_PRFRNC_SK"),
        F.col("db2.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("db2.EFF_DT").alias("EFF_DT"),
        F.col("db2.COMM_TYP_CD").alias("COMM_TYP_CD"),
        F.col("db2.MBR_COMM_PRFRNC_CD").alias("MBR_COMM_PRFRNC_CD"),
        F.col("db2.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("db2.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("db2.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("db2.MBR_SK").alias("MBR_SK"),
        F.col("db2.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("db2.COMM_TYP_CD_SK").alias("COMM_TYP_CD_SK"),
        F.col("db2.MBR_COMM_PRFRNC_CD_SK").alias("MBR_COMM_PRFRNC_CD_SK"),
        F.col("db2.MBR_LANG_PRFRNC_CD_SK").alias("MBR_LANG_PRFRNC_CD_SK"),
        F.col("db2.SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
        F.col("db2.TERM_RSN_CD_SK").alias("TERM_RSN_CD_SK"),
        F.col("db2.OPT_OUT_IN").alias("OPT_OUT_IN"),
        F.col("db2.RCVD_DT").alias("RCVD_DT"),
        F.col("db2.TERM_DT").alias("TERM_DT"),
        F.col("db2.SRC_SYS_LAST_UPDT_DTM").alias("SRC_SYS_LAST_UPDT_DTM"),
        F.col("db2.MBR_EMAIL_ADDR").alias("MBR_EMAIL_ADDR"),
        F.col("db2.MBR_TX_MSG_PHN_NO").alias("MBR_TX_MSG_PHN_NO"),
        F.col("db2.SUB_SK").alias("SUB_SK"),
        F.col("db2.MBR_COMM_ADDR_LN").alias("MBR_COMM_ADDR_LN"),
        F.col("db2.MBR_COMM_CITY_NM").alias("MBR_COMM_CITY_NM"),
        F.col("db2.MBR_COMM_ZIP_CD").alias("MBR_COMM_ZIP_CD"),
        F.col("addr_sk.TRGT_CD").alias("MBR_COMM_ST_CD"),
        F.col("Ref_ctyp_nm.TRGT_CD_NM").alias("COMM_TYP_NM"),
        F.col("Ref_Cpnm.TRGT_CD_NM").alias("MBR_COMM_PRFRNC_NM"),
        F.col("Ref_Lprfnc_cd.TRGT_CD").alias("MBR_LANG_PRFRNC_CD"),
        F.col("Ref_Lprfnc_nm.TRGT_CD_NM").alias("MBR_LANG_PRFRNC_NM"),
        F.col("Ref_trnm.TRGT_CD_NM").alias("MBR_COMM_PRFRNC_TERM_RSN_NM"),
        F.col("Ref_trnm.TRGT_CD").alias("MBR_COMM_PRFRNC_TERM_RSN_CD")
    )
)

df_lkp_dt = (
    df_lkp_mpng_Codes.alias("cds")
    .join(
        df_app_user.alias("db_user"),
        F.col("cds.SRC_SYS_LAST_UPDT_USER_SK") == F.col("db_user.USER_SK"),
        "left"
    )
    .select(
        F.col("cds.MBR_COMM_PRFRNC_SK").alias("MBR_COMM_PRFRNC_SK"),
        F.col("cds.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("cds.EFF_DT").alias("EFF_DT"),
        F.col("cds.COMM_TYP_CD").alias("COMM_TYP_CD"),
        F.col("cds.MBR_COMM_PRFRNC_CD").alias("MBR_COMM_PRFRNC_CD"),
        F.col("cds.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("cds.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("cds.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("cds.MBR_SK").alias("MBR_SK"),
        F.col("cds.COMM_TYP_NM").alias("COMM_TYP_NM"),
        F.col("cds.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("cds.MBR_COMM_PRFRNC_NM").alias("MBR_COMM_PRFRNC_NM"),
        F.col("cds.MBR_LANG_PRFRNC_CD").alias("MBR_LANG_PRFRNC_CD"),
        F.col("cds.MBR_LANG_PRFRNC_NM").alias("MBR_LANG_PRFRNC_NM"),
        F.col("cds.MBR_COMM_PRFRNC_TERM_RSN_CD").alias("MBR_COMM_PRFRNC_TERM_RSN_CD"),
        F.col("cds.MBR_COMM_PRFRNC_TERM_RSN_NM").alias("MBR_COMM_PRFRNC_TERM_RSN_NM"),
        F.col("cds.OPT_OUT_IN").alias("OPT_OUT_IN"),
        F.col("cds.RCVD_DT").alias("RCVD_DT"),
        F.col("cds.TERM_DT").alias("TERM_DT"),
        F.col("cds.SRC_SYS_LAST_UPDT_DTM").alias("SRC_SYS_LAST_UPDT_DTM"),
        F.col("cds.MBR_EMAIL_ADDR").alias("MBR_EMAIL_ADDR"),
        F.col("cds.MBR_TX_MSG_PHN_NO").alias("MBR_TX_MSG_PHN_NO"),
        F.col("cds.MBR_COMM_ADDR_LN").alias("MBR_COMM_ADDR_LN"),
        F.col("cds.MBR_COMM_CITY_NM").alias("MBR_COMM_CITY_NM"),
        F.col("cds.MBR_COMM_ST_CD").alias("MBR_COMM_ST_CD"),
        F.col("cds.MBR_COMM_ZIP_CD").alias("MBR_COMM_ZIP_CD"),
        F.col("cds.COMM_TYP_CD_SK").alias("COMM_TYP_CD_SK"),
        F.col("cds.MBR_COMM_PRFRNC_CD_SK").alias("MBR_COMM_PRFRNC_CD_SK"),
        F.col("cds.MBR_LANG_PRFRNC_CD_SK").alias("MBR_LANG_PRFRNC_CD_SK"),
        F.col("cds.SUB_ADDR_CD_SK").alias("SUB_ADDR_CD_SK"),
        F.col("cds.TERM_RSN_CD_SK").alias("TERM_RSN_CD_SK"),
        F.col("cds.SUB_SK").alias("SUB_SK"),
        F.col("db_user.USER_ID").alias("USER_ID")
    )
)

w = Window.orderBy(F.lit(1))
df_temp = df_lkp_dt.withColumn("_row_number", F.row_number().over(w))

df_orginal = (
    df_temp
    .withColumn(
        "MBR_COMM_PRFRNC_SK",
        F.col("MBR_COMM_PRFRNC_SK")
    )
    .withColumn(
        "MBR_UNIQ_KEY",
        F.col("MBR_UNIQ_KEY")
    )
    .withColumn(
        "MBR_COMM_PRFRNC_EFF_DT",
        TimestampToDate(F.col("EFF_DT"))
    )
    .withColumn(
        "COMM_TYP_CD",
        F.when(
            F.when(F.col("COMM_TYP_CD").isNotNull(), F.col("COMM_TYP_CD")).otherwise("") == "", 
            "NULL"
        ).otherwise(F.col("COMM_TYP_CD"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_CD",
        F.when(
            F.when(F.col("MBR_COMM_PRFRNC_CD").isNotNull(), F.col("MBR_COMM_PRFRNC_CD")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_COMM_PRFRNC_CD"))
    )
    .withColumn(
        "SRC_SYS_CD",
        F.col("SRC_SYS_CD")
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.lit(EDWRunCycleDate), 10, " ")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.rpad(F.lit(EDWRunCycleDate), 10, " ")
    )
    .withColumn(
        "MBR_SK",
        F.when(
            F.when(F.col("MBR_SK").isNotNull(), F.col("MBR_SK")).otherwise(F.lit(0)) == 0,
            F.lit(0000)
        ).otherwise(F.col("MBR_SK"))
    )
    .withColumn(
        "SRC_SYS_LAST_UPDT_USER_SK",
        F.when(
            F.when(F.col("SRC_SYS_LAST_UPDT_USER_SK").isNotNull(), F.col("SRC_SYS_LAST_UPDT_USER_SK")).otherwise(F.lit(0)) == 0,
            F.lit(0000)
        ).otherwise(F.col("SRC_SYS_LAST_UPDT_USER_SK"))
    )
    .withColumn(
        "COMM_TYP_NM",
        F.when(
            F.when(F.col("COMM_TYP_NM").isNotNull(), F.col("COMM_TYP_NM")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("COMM_TYP_NM"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_NM",
        F.when(
            F.when(F.col("MBR_COMM_PRFRNC_NM").isNotNull(), F.col("MBR_COMM_PRFRNC_NM")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_COMM_PRFRNC_NM"))
    )
    .withColumn(
        "MBR_LANG_PRFRNC_CD",
        F.when(
            F.when(F.col("MBR_LANG_PRFRNC_CD").isNotNull(), F.col("MBR_LANG_PRFRNC_CD")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_LANG_PRFRNC_CD"))
    )
    .withColumn(
        "MBR_LANG_PRFRNC_NM",
        F.when(
            F.when(F.col("MBR_LANG_PRFRNC_NM").isNotNull(), F.col("MBR_LANG_PRFRNC_NM")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_LANG_PRFRNC_NM"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_TERM_RSN_CD",
        F.when(
            F.when(F.col("MBR_COMM_PRFRNC_TERM_RSN_CD").isNotNull(), F.col("MBR_COMM_PRFRNC_TERM_RSN_CD")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_COMM_PRFRNC_TERM_RSN_CD"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_TERM_RSN_NM",
        F.when(
            F.when(F.col("MBR_COMM_PRFRNC_TERM_RSN_NM").isNotNull(), F.col("MBR_COMM_PRFRNC_TERM_RSN_NM")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_COMM_PRFRNC_TERM_RSN_NM"))
    )
    .withColumn(
        "OPT_OUT_IN",
        F.col("OPT_OUT_IN")
    )
    .withColumn(
        "MBR_COMM_PRFRNC_RCVD_DT",
        TimestampToDate(F.col("RCVD_DT"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_TERM_DT",
        TimestampToDate(F.col("TERM_DT"))
    )
    .withColumn(
        "SRC_SYS_LAST_UPDT_DTM",
        F.col("SRC_SYS_LAST_UPDT_DTM")
    )
    .withColumn(
        "SRC_SYS_LAST_UPDT_USER_ID",
        F.when(F.col("USER_ID").isNull(), F.lit("NA")).otherwise(F.col("USER_ID"))
    )
    .withColumn(
        "MBR_EMAIL_ADDR",
        F.when(
            F.when(F.col("MBR_EMAIL_ADDR").isNotNull(), F.col("MBR_EMAIL_ADDR")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_EMAIL_ADDR"))
    )
    .withColumn(
        "MBR_TX_MSG_PHN_NO",
        F.when(
            F.when(F.col("MBR_TX_MSG_PHN_NO").isNotNull(), F.col("MBR_TX_MSG_PHN_NO")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_TX_MSG_PHN_NO"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_ADDR_LN",
        F.when(
            F.when(F.col("MBR_COMM_ADDR_LN").isNotNull(), F.col("MBR_COMM_ADDR_LN")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_COMM_ADDR_LN"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_CITY_NM",
        F.when(
            F.when(F.col("MBR_COMM_CITY_NM").isNotNull(), F.col("MBR_COMM_CITY_NM")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_COMM_CITY_NM"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_ST_CD",
        F.when(
            F.when(F.col("MBR_COMM_ST_CD").isNotNull(), F.col("MBR_COMM_ST_CD")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_COMM_ST_CD"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_ZIP_CD",
        F.when(
            F.when(F.col("MBR_COMM_ZIP_CD").isNotNull(), F.col("MBR_COMM_ZIP_CD")).otherwise("") == "",
            "NULL"
        ).otherwise(F.col("MBR_COMM_ZIP_CD"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.col("CRT_RUN_CYC_EXCTN_SK")
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
    .withColumn(
        "COMM_TYP_CD_SK",
        F.when(
            F.when(F.col("COMM_TYP_CD_SK").isNotNull(), F.col("COMM_TYP_CD_SK")).otherwise(F.lit(0)) == 0,
            F.lit(0)
        ).otherwise(F.col("COMM_TYP_CD_SK"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_CD_SK",
        F.when(
            F.when(F.col("MBR_COMM_PRFRNC_CD_SK").isNotNull(), F.col("MBR_COMM_PRFRNC_CD_SK")).otherwise(F.lit(0)) == 0,
            F.lit(0)
        ).otherwise(F.col("MBR_COMM_PRFRNC_CD_SK"))
    )
    .withColumn(
        "MBR_LANG_PRFRNC_CD_SK",
        F.when(
            F.when(F.col("MBR_LANG_PRFRNC_CD_SK").isNotNull(), F.col("MBR_LANG_PRFRNC_CD_SK")).otherwise(F.lit(0)) == 0,
            F.lit(0)
        ).otherwise(F.col("MBR_LANG_PRFRNC_CD_SK"))
    )
    .withColumn(
        "MBR_COMM_PRFRNC_TERM_RSN_CD_SK",
        F.when(
            F.when(F.col("TERM_RSN_CD_SK").isNotNull(), F.col("TERM_RSN_CD_SK")).otherwise(F.lit(0)) == 0,
            F.lit(0)
        ).otherwise(F.col("TERM_RSN_CD_SK"))
    )
    .filter(F.col("_row_number") >= 1)  # all rows
    .select(
        "MBR_COMM_PRFRNC_SK",
        "MBR_UNIQ_KEY",
        "MBR_COMM_PRFRNC_EFF_DT",
        "COMM_TYP_CD",
        "MBR_COMM_PRFRNC_CD",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "MBR_SK",
        "SRC_SYS_LAST_UPDT_USER_SK",
        "COMM_TYP_NM",
        "MBR_COMM_PRFRNC_NM",
        "MBR_LANG_PRFRNC_CD",
        "MBR_LANG_PRFRNC_NM",
        "MBR_COMM_PRFRNC_TERM_RSN_CD",
        "MBR_COMM_PRFRNC_TERM_RSN_NM",
        "OPT_OUT_IN",
        "MBR_COMM_PRFRNC_RCVD_DT",
        "MBR_COMM_PRFRNC_TERM_DT",
        "SRC_SYS_LAST_UPDT_DTM",
        "SRC_SYS_LAST_UPDT_USER_ID",
        "MBR_EMAIL_ADDR",
        "MBR_TX_MSG_PHN_NO",
        "MBR_COMM_PRFRNC_ADDR_LN",
        "MBR_COMM_PRFRNC_CITY_NM",
        "MBR_COMM_PRFRNC_ST_CD",
        "MBR_COMM_PRFRNC_ZIP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "COMM_TYP_CD_SK",
        "MBR_COMM_PRFRNC_CD_SK",
        "MBR_LANG_PRFRNC_CD_SK",
        "MBR_COMM_PRFRNC_TERM_RSN_CD_SK"
    )
)

df_na = (
    df_temp
    .filter(F.col("_row_number") == 1)
    .select(
        F.lit(1).alias("MBR_COMM_PRFRNC_SK"),
        F.lit(1).alias("MBR_UNIQ_KEY"),
        F.lit("1753-01-01").alias("MBR_COMM_PRFRNC_EFF_DT"),
        F.lit("NA").alias("COMM_TYP_CD"),
        F.lit("NA").alias("MBR_COMM_PRFRNC_CD"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.rpad(F.lit("NA"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("MBR_SK"),
        F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.lit("NA").alias("COMM_TYP_NM"),
        F.lit("NA").alias("MBR_COMM_PRFRNC_NM"),
        F.lit("NA").alias("MBR_LANG_PRFRNC_CD"),
        F.lit("NA").alias("MBR_LANG_PRFRNC_NM"),
        F.lit("NA").alias("MBR_COMM_PRFRNC_TERM_RSN_CD"),
        F.lit("NA").alias("MBR_COMM_PRFRNC_TERM_RSN_NM"),
        F.rpad(F.lit("1"), 1, " ").alias("OPT_OUT_IN"),
        F.lit("1753-01-01").alias("MBR_COMM_PRFRNC_RCVD_DT"),
        F.lit("1753-01-01").alias("MBR_COMM_PRFRNC_TERM_DT"),
        F.lit("1753-01-01 00:00:00 ").alias("SRC_SYS_LAST_UPDT_DTM"),
        F.lit("NA").alias("SRC_SYS_LAST_UPDT_USER_ID"),
        F.lit("NA").alias("MBR_EMAIL_ADDR"),
        F.lit("NA").alias("MBR_TX_MSG_PHN_NO"),
        F.lit("NA").alias("MBR_COMM_PRFRNC_ADDR_LN"),
        F.lit("NA").alias("MBR_COMM_PRFRNC_CITY_NM"),
        F.lit("NA").alias("MBR_COMM_PRFRNC_ST_CD"),
        F.rpad(F.lit("NA"), 5, " ").alias("MBR_COMM_PRFRNC_ZIP_CD"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("COMM_TYP_CD_SK"),
        F.lit(1).alias("MBR_COMM_PRFRNC_CD_SK"),
        F.lit(1).alias("MBR_LANG_PRFRNC_CD_SK"),
        F.lit(1).alias("MBR_COMM_PRFRNC_TERM_RSN_CD_SK")
    )
)

df_unk = (
    df_temp
    .filter(F.col("_row_number") == 1)
    .select(
        F.lit(0).alias("MBR_COMM_PRFRNC_SK"),
        F.lit(0).alias("MBR_UNIQ_KEY"),
        F.lit("1753-01-01").alias("MBR_COMM_PRFRNC_EFF_DT"),
        F.lit("UNK").alias("COMM_TYP_CD"),
        F.lit("UNK").alias("MBR_COMM_PRFRNC_CD"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.rpad(F.lit("UNK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.rpad(F.lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("MBR_SK"),
        F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.lit("UNK").alias("COMM_TYP_NM"),
        F.lit("UNK").alias("MBR_COMM_PRFRNC_NM"),
        F.lit("UNK").alias("MBR_LANG_PRFRNC_CD"),
        F.lit("UNK").alias("MBR_LANG_PRFRNC_NM"),
        F.lit("UNK").alias("MBR_COMM_PRFRNC_TERM_RSN_CD"),
        F.lit("UNK").alias("MBR_COMM_PRFRNC_TERM_RSN_NM"),
        F.rpad(F.lit("0"), 1, " ").alias("OPT_OUT_IN"),
        F.lit("1753-01-01").alias("MBR_COMM_PRFRNC_RCVD_DT"),
        F.lit("1753-01-01").alias("MBR_COMM_PRFRNC_TERM_DT"),
        F.lit("1753-01-01 00:00:00 ").alias("SRC_SYS_LAST_UPDT_DTM"),
        F.lit("UNK").alias("SRC_SYS_LAST_UPDT_USER_ID"),
        F.lit("UNK").alias("MBR_EMAIL_ADDR"),
        F.lit("UNK").alias("MBR_TX_MSG_PHN_NO"),
        F.lit("UNK").alias("MBR_COMM_PRFRNC_ADDR_LN"),
        F.lit("UNK").alias("MBR_COMM_PRFRNC_CITY_NM"),
        F.lit("UNK").alias("MBR_COMM_PRFRNC_ST_CD"),
        F.rpad(F.lit("UNK"), 5, " ").alias("MBR_COMM_PRFRNC_ZIP_CD"),
        F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("COMM_TYP_CD_SK"),
        F.lit(0).alias("MBR_COMM_PRFRNC_CD_SK"),
        F.lit(0).alias("MBR_LANG_PRFRNC_CD_SK"),
        F.lit(0).alias("MBR_COMM_PRFRNC_TERM_RSN_CD_SK")
    )
)

df_funnel = df_orginal.unionByName(df_unk).unionByName(df_na)

df_final = df_funnel.select(
    F.col("MBR_COMM_PRFRNC_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("MBR_COMM_PRFRNC_EFF_DT"),
    F.col("COMM_TYP_CD"),
    F.col("MBR_COMM_PRFRNC_CD"),
    F.col("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK"),
    F.col("COMM_TYP_NM"),
    F.col("MBR_COMM_PRFRNC_NM"),
    F.col("MBR_LANG_PRFRNC_CD"),
    F.col("MBR_LANG_PRFRNC_NM"),
    F.col("MBR_COMM_PRFRNC_TERM_RSN_CD"),
    F.col("MBR_COMM_PRFRNC_TERM_RSN_NM"),
    F.col("OPT_OUT_IN"),
    F.col("MBR_COMM_PRFRNC_RCVD_DT"),
    F.col("MBR_COMM_PRFRNC_TERM_DT"),
    F.col("SRC_SYS_LAST_UPDT_DTM"),
    F.col("SRC_SYS_LAST_UPDT_USER_ID"),
    F.col("MBR_EMAIL_ADDR"),
    F.col("MBR_TX_MSG_PHN_NO"),
    F.col("MBR_COMM_PRFRNC_ADDR_LN"),
    F.col("MBR_COMM_PRFRNC_CITY_NM"),
    F.col("MBR_COMM_PRFRNC_ST_CD"),
    F.col("MBR_COMM_PRFRNC_ZIP_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("COMM_TYP_CD_SK"),
    F.col("MBR_COMM_PRFRNC_CD_SK"),
    F.col("MBR_LANG_PRFRNC_CD_SK"),
    F.col("MBR_COMM_PRFRNC_TERM_RSN_CD_SK")
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_COMM_PRFNC.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)