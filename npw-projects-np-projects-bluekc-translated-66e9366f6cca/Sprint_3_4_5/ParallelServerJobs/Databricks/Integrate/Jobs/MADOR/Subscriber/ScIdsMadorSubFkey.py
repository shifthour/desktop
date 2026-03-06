# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #               Change Description                             Development Project      Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------     ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Sethuraman Rajendran     03/30/2018      5839- MADOR                 Originally Programmed                            IntegrateDev2           Kalyan Neelam              2018-06-14
# MAGIC Abhiram Dasarathy	         2019-12-12	F-114877		Changed SUB_ID and GRP_ID to	          IntegrateDev2           Jaideep Mankala          12/13/2019
# MAGIC 						to SUB_ID_1 and GRP_ID_1 in lkup

# MAGIC Look up with CD_MPPNG table for foreign key values.
# MAGIC This is a load ready file that will go into Load job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DateType
)
from pyspark.sql import functions as F
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
PrefixFkeyFailedFileName = get_widget_value('PrefixFkeyFailedFileName','')

DSJobName = "ScIdsMadorSubFkey"

# Read from DB2ConnectorPX - SUB
jdbc_url_sub, jdbc_props_sub = get_db_config(ids_secret_name)
extract_query_sub = f"SELECT SUB_ID as SUB_ID_1, SUB_SK FROM {IDSOwner}.SUB"
df_SUB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_sub)
    .options(**jdbc_props_sub)
    .option("query", extract_query_sub)
    .load()
)

# Read from DB2ConnectorPX - GRP
jdbc_url_grp, jdbc_props_grp = get_db_config(ids_secret_name)
extract_query_grp = f"SELECT GRP_ID as GRP_ID_1, GRP_SK FROM {IDSOwner}.GRP"
df_GRP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_grp)
    .options(**jdbc_props_grp)
    .option("query", extract_query_grp)
    .load()
)

# Read from DB2ConnectorPX - CD_MPPNG
jdbc_url_cd_mppng, jdbc_props_cd_mppng = get_db_config(ids_secret_name)
extract_query_cd_mppng = (
    f"SELECT SRC_DRVD_LKUP_VAL, CD_MPPNG_SK "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE SRC_SYS_CD='IDS' AND SRC_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='STATE' "
    f"AND TRGT_CLCTN_CD='IDS' AND TRGT_DOMAIN_NM='STATE'"
)
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cd_mppng)
    .options(**jdbc_props_cd_mppng)
    .option("query", extract_query_cd_mppng)
    .load()
)

# Read from DB2ConnectorPX - CD_MPPNG2
jdbc_url_cd_mppng2, jdbc_props_cd_mppng2 = get_db_config(ids_secret_name)
extract_query_cd_mppng2 = (
    f"SELECT SRC_DRVD_LKUP_VAL, CD_MPPNG_SK AS CD_MPPNG_SK_2 "
    f"FROM {IDSOwner}.CD_MPPNG "
    f"WHERE SRC_SYS_CD='IDS' AND SRC_CLCTN_CD='IDS' AND SRC_DOMAIN_NM='STATE' "
    f"AND TRGT_CLCTN_CD='IDS' AND TRGT_DOMAIN_NM='STATE'"
)
df_CD_MPPNG2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cd_mppng2)
    .options(**jdbc_props_cd_mppng2)
    .option("query", extract_query_cd_mppng2)
    .load()
)

# Read PxSequentialFile - seq_SUB_MA_DOR_Pkey
schema_seq_SUB_MA_DOR_Pkey = StructType([
    StructField("GRP_MA_DOR_SK", IntegerType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("TAX_YR", StringType(), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("AS_OF_DTM", TimestampType(), False),
    StructField("GRP_SK", IntegerType(), False),
    StructField("SUB_SK", IntegerType(), False),
    StructField("SUB_FIRST_NM", StringType(), False),
    StructField("SUB_MIDINIT", StringType(), True),
    StructField("SUB_LAST_NM", StringType(), False),
    StructField("SUB_HOME_ADDR_LN_1", StringType(), False),
    StructField("SUB_HOME_ADDR_LN_2", StringType(), True),
    StructField("SUB_HOME_ADDR_LN_3", StringType(), True),
    StructField("SUB_HOME_ADDR_CITY_NM", StringType(), False),
    StructField("SUB_HOME_ADDR_ST_CD", StringType(), True),
    StructField("SUB_HOME_ADDR_ZIP_CD_5", StringType(), False),
    StructField("SUB_HOME_ADDR_ZIP_CD_4", StringType(), True),
    StructField("SUB_MAIL_ADDR_LN_1", StringType(), False),
    StructField("SUB_MAIL_ADDR_LN_2", StringType(), True),
    StructField("SUB_MAIL_ADDR_LN_3", StringType(), True),
    StructField("SUB_MAIL_ADDR_CITY_NM", StringType(), False),
    StructField("SUB_MAIL_ADDR_ST_CD", StringType(), True),
    StructField("SUB_MAIL_ADDR_ZIP_CD_5", StringType(), False),
    StructField("SUB_MAIL_ADDR_ZIP_CD_4", StringType(), True),
    StructField("JAN_COV_IN", StringType(), False),
    StructField("FEB_COV_IN", StringType(), False),
    StructField("MAR_COV_IN", StringType(), False),
    StructField("APR_COV_IN", StringType(), False),
    StructField("MAY_COV_IN", StringType(), False),
    StructField("JUN_COV_IN", StringType(), False),
    StructField("JUL_COV_IN", StringType(), False),
    StructField("AUG_COV_IN", StringType(), False),
    StructField("SEP_COV_IN", StringType(), False),
    StructField("OCT_COV_IN", StringType(), False),
    StructField("NOV_COV_IN", StringType(), False),
    StructField("DEC_COV_IN", StringType(), False),
    StructField("SUB_BRTH_DT", DateType(), False),
    StructField("MBR_SFX_NO", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("SUB_MA_DOR_SK", IntegerType(), False)
])
file_path_seq_SUB_MA_DOR_Pkey = f"{adls_path}/key/SUB_MA_DOR.{SrcSysCd}.pkey.{RunID}.dat"
df_seq_SUB_MA_DOR_Pkey = (
    spark.read.format("csv")
    .schema(schema_seq_SUB_MA_DOR_Pkey)
    .option("delimiter", ",")
    .option("quote", "^")
    .option("header", "false")
    .option("nullValue", None)
    .load(file_path_seq_SUB_MA_DOR_Pkey)
)

# Lookup LkupFkey: Primary link = df_seq_SUB_MA_DOR_Pkey, left link = df_CD_MPPNG, left link = df_CD_MPPNG2
df_LkupFkey_1 = df_seq_SUB_MA_DOR_Pkey.alias("p").join(
    df_CD_MPPNG.alias("lk"),
    F.col("p.SUB_HOME_ADDR_ST_CD") == F.col("lk.SRC_DRVD_LKUP_VAL"),
    how="left"
)
df_LkupFkey_2 = df_LkupFkey_1.join(
    df_CD_MPPNG2.alias("lk2"),
    F.col("p.SUB_MAIL_ADDR_ST_CD") == F.col("lk2.SRC_DRVD_LKUP_VAL"),
    how="left"
)
df_LkupFkey = df_LkupFkey_2.select(
    F.col("p.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
    F.col("p.SUB_ID").alias("SUB_ID"),
    F.col("p.GRP_ID").alias("GRP_ID"),
    F.col("p.TAX_YR").alias("TAX_YR"),
    F.col("p.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("p.AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("p.GRP_SK").alias("GRP_SK"),
    F.col("p.SUB_SK").alias("SUB_SK"),
    F.col("p.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("p.SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("p.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("p.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("p.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("p.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("p.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("p.SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    F.col("lk.CD_MPPNG_SK").alias("SUB_ST_CD_SK"),
    F.col("p.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("p.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("p.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("p.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("p.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("p.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("p.SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    F.col("lk2.CD_MPPNG_SK_2").alias("SUB_MAIL_ST_CD_SK"),
    F.col("p.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("p.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.col("p.JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("p.FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("p.MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("p.APR_COV_IN").alias("APR_COV_IN"),
    F.col("p.MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("p.JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("p.JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("p.AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("p.SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("p.OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("p.NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("p.DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("p.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("p.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("p.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("p.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("p.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK")
).alias("LkupOut")

# Lookup LkpGrp: Primary link = df_LkupFkey, left link = df_GRP
df_LkpGrp_1 = df_LkupFkey.alias("LkupOut").join(
    df_GRP.alias("g"),
    F.col("LkupOut.GRP_ID") == F.col("g.GRP_ID_1"),
    how="left"
)
df_LkpGrp = df_LkpGrp_1.select(
    F.col("LkupOut.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
    F.col("LkupOut.SUB_ID").alias("SUB_ID"),
    F.col("LkupOut.GRP_ID").alias("GRP_ID"),
    F.col("LkupOut.TAX_YR").alias("TAX_YR"),
    F.col("LkupOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LkupOut.AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("LkupOut.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("LkupOut.SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("LkupOut.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("LkupOut.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("LkupOut.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("LkupOut.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("LkupOut.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("LkupOut.SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    F.col("LkupOut.SUB_ST_CD_SK").alias("SUB_ST_CD_SK"),
    F.col("LkupOut.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("LkupOut.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("LkupOut.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("LkupOut.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("LkupOut.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("LkupOut.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("LkupOut.SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    F.col("LkupOut.SUB_MAIL_ST_CD_SK").alias("SUB_MAIL_ST_CD_SK"),
    F.col("LkupOut.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("LkupOut.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.col("LkupOut.JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("LkupOut.FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("LkupOut.MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("LkupOut.APR_COV_IN").alias("APR_COV_IN"),
    F.col("LkupOut.MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("LkupOut.JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("LkupOut.JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("LkupOut.AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("LkupOut.SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("LkupOut.OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("LkupOut.NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("LkupOut.DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("LkupOut.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("LkupOut.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("LkupOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LkupOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LkupOut.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("g.GRP_SK").alias("GRP_SK")
).alias("LkpSub")

# Lookup Lkp_Sub: Primary link = df_LkpGrp, left link = df_SUB
df_LkpSub_1 = df_LkpGrp.alias("LkpSub").join(
    df_SUB.alias("s"),
    F.col("LkpSub.SUB_ID") == F.col("s.SUB_ID_1"),
    how="left"
)
df_LkpSub = df_LkpSub_1.select(
    F.col("LkpSub.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
    F.col("LkpSub.SUB_ID").alias("SUB_ID"),
    F.col("LkpSub.GRP_ID").alias("GRP_ID"),
    F.col("LkpSub.TAX_YR").alias("TAX_YR"),
    F.col("LkpSub.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("LkpSub.AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("LkpSub.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("LkpSub.SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("LkpSub.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("LkpSub.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("LkpSub.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("LkpSub.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("LkpSub.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("LkpSub.SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    F.col("LkpSub.SUB_ST_CD_SK").alias("SUB_ST_CD_SK"),
    F.col("LkpSub.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("LkpSub.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("LkpSub.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("LkpSub.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("LkpSub.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("LkpSub.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("LkpSub.SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    F.col("LkpSub.SUB_MAIL_ST_CD_SK").alias("SUB_MAIL_ST_CD_SK"),
    F.col("LkpSub.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("LkpSub.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.col("LkpSub.JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("LkpSub.FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("LkpSub.MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("LkpSub.APR_COV_IN").alias("APR_COV_IN"),
    F.col("LkpSub.MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("LkpSub.JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("LkpSub.JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("LkpSub.AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("LkpSub.SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("LkpSub.OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("LkpSub.NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("LkpSub.DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("LkpSub.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("LkpSub.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("LkpSub.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LkpSub.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("LkpSub.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("LkpSub.GRP_SK").alias("GRP_SK"),
    F.col("s.SUB_SK").alias("SUB_SK")
).alias("LkupOut")

# xfm_CheckLkpResults
df_xfm_vars = (
    df_LkpSub
    .withColumn(
        "SvFKeyLkpCheck",
        F.when(
            F.trim(
                F.when(F.col("SUB_ST_CD_SK").isNotNull(), F.col("SUB_ST_CD_SK")).otherwise(F.lit(0)).cast("string")
            ).eqNullSafe("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckGrpSk",
        F.when(
            F.trim(
                F.when(F.col("GRP_SK").isNotNull(), F.col("GRP_SK")).otherwise(F.lit(0)).cast("string")
            ).eqNullSafe("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvFKeyLkpCheckSubSk",
        F.when(
            F.trim(
                F.when(F.col("SUB_SK").isNotNull(), F.col("SUB_SK")).otherwise(F.lit(0)).cast("string")
            ).eqNullSafe("0"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

window_global = Window.orderBy(F.lit(1))
df_xfm_check = df_xfm_vars.withColumn("row_id", F.row_number().over(window_global))

# MAIN link (no filter)
df_MAIN = df_xfm_check

# NA link (constraint => first row)
df_NA = df_xfm_check.filter("row_id=1")

# UNK link (constraint => first row)
df_UNK = df_xfm_check.filter("row_id=1")

# Lnk_K_FKey_Fail_Cd_Mppng => SvFKeyLkpCheck='Y'
df_Lnk_K_FKey_Fail_Cd_Mppng = df_xfm_check.filter(F.col("SvFKeyLkpCheck") == "Y")

# Grp_FK_Fail => SvFKeyLkpCheckGrpSk='Y'
df_Grp_FK_Fail = df_xfm_check.filter(F.col("SvFKeyLkpCheckGrpSk") == "Y")

# Sub_FK_Fail => SvFKeyLkpCheckSubSk='Y'
df_Sub_FK_Fail = df_xfm_check.filter(F.col("SvFKeyLkpCheckSubSk") == "Y")

# Funnel_65 => union of Lnk_K_FKey_Fail_Cd_Mppng, Grp_FK_Fail, Sub_FK_Fail
common_cols_65 = [
    "PRI_SK","PRI_NAT_KEY_STRING","SRC_SYS_CD_SK","JOB_NM","ERROR_TYP","PHYSCL_FILE_NM","FRGN_NAT_KEY_STRING","FIRST_RECYC_TS","JOB_EXCTN_SK"
]
df_Lnk_K_FKey_Fail_Cd_Mppng_sel = df_Lnk_K_FKey_Fail_Cd_Mppng.select(
    F.col("SUB_MA_DOR_SK").alias("PRI_SK"),
    F.concat(F.col("SUB_ID"),F.lit(' '),F.col("GRP_ID"),F.lit(' '),F.col("TAX_YR"),F.lit(' '),F.col("SRC_SYS_CD")).alias("PRI_NAT_KEY_STRING"),
    F.lit("0").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_SUB_MA_DOR").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SUB_ID"),F.lit(' '),F.col("GRP_ID"),F.lit(' '),F.col("TAX_YR"),F.lit(' '),F.col("SRC_SYS_CD"),F.lit(' '),F.col("SUB_HOME_ADDR_ST_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Grp_FK_Fail_sel = df_Grp_FK_Fail.select(
    F.col("SUB_MA_DOR_SK").alias("PRI_SK"),
    F.concat(F.col("SUB_ID"),F.lit(' '),F.col("GRP_ID"),F.lit(' '),F.col("TAX_YR"),F.lit(' '),F.col("SRC_SYS_CD")).alias("PRI_NAT_KEY_STRING"),
    F.lit("0").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_SUB_MA_DOR").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SUB_ID"),F.lit(' '),F.col("GRP_ID"),F.lit(' '),F.col("TAX_YR"),F.lit(' '),F.col("SRC_SYS_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Sub_FK_Fail_sel = df_Sub_FK_Fail.select(
    F.col("SUB_MA_DOR_SK").alias("PRI_SK"),
    F.concat(F.col("SUB_ID"),F.lit(' '),F.col("GRP_ID"),F.lit(' '),F.col("TAX_YR"),F.lit(' '),F.col("SRC_SYS_CD")).alias("PRI_NAT_KEY_STRING"),
    F.lit("0").alias("SRC_SYS_CD_SK"),
    F.lit(DSJobName).alias("JOB_NM"),
    F.lit("FKLOOKUP").alias("ERROR_TYP"),
    F.lit("K_SUB_MA_DOR").alias("PHYSCL_FILE_NM"),
    F.concat(F.col("SUB_ID"),F.lit(' '),F.col("GRP_ID"),F.lit(' '),F.col("TAX_YR"),F.lit(' '),F.col("SRC_SYS_CD")).alias("FRGN_NAT_KEY_STRING"),
    F.col("AS_OF_DTM").alias("FIRST_RECYC_TS"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("JOB_EXCTN_SK")
)

df_Funnel_65 = df_Lnk_K_FKey_Fail_Cd_Mppng_sel.unionByName(df_Grp_FK_Fail_sel).unionByName(df_Sub_FK_Fail_sel)

# seq_FkeyFailedFile => write the union to #PrefixFkeyFailedFileName#.#DSJobName#.dat
df_seq_FkeyFailedFile = df_Funnel_65.select(common_cols_65)
# For each column that is string, apply rpad (no lengths given, use 200).
df_seq_FkeyFailedFile_padded = df_seq_FkeyFailedFile.select(
    [
        F.rpad(F.col(c).cast(StringType()), 200, " ").alias(c) if c not in ["JOB_EXCTN_SK","PRI_SK","SRC_SYS_CD_SK"] 
        else F.col(c)
        for c in common_cols_65
    ]
)

write_files(
    df_seq_FkeyFailedFile_padded,
    f"{adls_path}/fkey_failures/{PrefixFkeyFailedFileName}.{DSJobName}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Funnel_70 => union of MAIN, UNK, NA
common_cols_70 = [
    "SUB_MA_DOR_SK","SUB_ID","GRP_ID","TAX_YR","SRC_SYS_CD","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AS_OF_DTM","GRP_MA_DOR_SK","GRP_SK","SUB_SK","SUB_FIRST_NM","SUB_MIDINIT","SUB_LAST_NM","SUB_HOME_ADDR_LN_1",
    "SUB_HOME_ADDR_LN_2","SUB_HOME_ADDR_LN_3","SUB_HOME_ADDR_CITY_NM","SUB_HOME_ADDR_ST_CD_SK","SUB_HOME_ADDR_ZIP_CD_5",
    "SUB_HOME_ADDR_ZIP_CD_4","SUB_MAIL_ADDR_LN_1","SUB_MAIL_ADDR_LN_2","SUB_MAIL_ADDR_LN_3","SUB_MAIL_ADDR_CITY_NM",
    "SUB_MAIL_ADDR_ST_CD_SK","SUB_MAIL_ADDR_ZIP_CD_5","SUB_MAIL_ADDR_ZIP_CD_4","JAN_COV_IN","FEB_COV_IN","MAR_COV_IN",
    "APR_COV_IN","MAY_COV_IN","JUN_COV_IN","JUL_COV_IN","AUG_COV_IN","SEP_COV_IN","OCT_COV_IN","NOV_COV_IN","DEC_COV_IN",
    "SUB_BRTH_DT","MBR_SFX_NO"
]

df_MAIN_sel = df_MAIN.select(
    F.col("SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
    F.expr("If(trim(coalesce(GRP_SK,0))='0',1,GRP_SK)").alias("GRP_SK"),
    F.expr("If(trim(coalesce(SUB_SK,0))='0',1,SUB_SK)").alias("SUB_SK"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.expr('If(trim(coalesce(SUB_ST_CD_SK,""))="",1862,SUB_ST_CD_SK)').alias("SUB_HOME_ADDR_ST_CD_SK"),
    F.col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.expr('If(trim(coalesce(SUB_MAIL_ST_CD_SK,""))="",1862,SUB_MAIL_ST_CD_SK)').alias("SUB_MAIL_ADDR_ST_CD_SK"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.col("JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("APR_COV_IN").alias("APR_COV_IN"),
    F.col("MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_UNK_sel = df_UNK.select(
    F.lit(0).alias("SUB_MA_DOR_SK"),
    F.lit("UNK").alias("SUB_ID"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("TAX_YR"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    current_timestamp().alias("AS_OF_DTM"),
    F.lit(0).alias("GRP_MA_DOR_SK"),
    F.lit(0).alias("GRP_SK"),
    F.lit(0).alias("SUB_SK"),
    F.lit("UNK").alias("SUB_FIRST_NM"),
    F.lit("X").alias("SUB_MIDINIT"),
    F.lit("UNK").alias("SUB_LAST_NM"),
    F.lit("UNK").alias("SUB_HOME_ADDR_LN_1"),
    F.lit("UNK").alias("SUB_HOME_ADDR_LN_2"),
    F.lit("UNK").alias("SUB_HOME_ADDR_LN_3"),
    F.lit("UNK").alias("SUB_HOME_ADDR_CITY_NM"),
    F.lit(0).alias("SUB_HOME_ADDR_ST_CD_SK"),
    F.lit("UNK").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.lit("UNK").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.lit("UNK").alias("SUB_MAIL_ADDR_LN_1"),
    F.lit("UNK").alias("SUB_MAIL_ADDR_LN_2"),
    F.lit("UNK").alias("SUB_MAIL_ADDR_LN_3"),
    F.lit("UNK").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.lit(0).alias("SUB_MAIL_ADDR_ST_CD_SK"),
    F.lit("UNK").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.lit("UNK").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.lit("N").alias("JAN_COV_IN"),
    F.lit("N").alias("FEB_COV_IN"),
    F.lit("N").alias("MAR_COV_IN"),
    F.lit("N").alias("APR_COV_IN"),
    F.lit("N").alias("MAY_COV_IN"),
    F.lit("N").alias("JUN_COV_IN"),
    F.lit("N").alias("JUL_COV_IN"),
    F.lit("N").alias("AUG_COV_IN"),
    F.lit("N").alias("SEP_COV_IN"),
    F.lit("N").alias("OCT_COV_IN"),
    F.lit("N").alias("NOV_COV_IN"),
    F.lit("N").alias("DEC_COV_IN"),
    current_date().alias("SUB_BRTH_DT"),
    F.lit("UNK").alias("MBR_SFX_NO")
)

df_NA_sel = df_NA.select(
    F.lit(1).alias("SUB_MA_DOR_SK"),
    F.lit("NA").alias("SUB_ID"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("TAX_YR"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    current_timestamp().alias("AS_OF_DTM"),
    F.lit(1).alias("GRP_MA_DOR_SK"),
    F.lit(1).alias("GRP_SK"),
    F.lit(1).alias("SUB_SK"),
    F.lit("NA").alias("SUB_FIRST_NM"),
    F.lit("N").alias("SUB_MIDINIT"),
    F.lit("NA").alias("SUB_LAST_NM"),
    F.lit("NA").alias("SUB_HOME_ADDR_LN_1"),
    F.lit("NA").alias("SUB_HOME_ADDR_LN_2"),
    F.lit("NA").alias("SUB_HOME_ADDR_LN_3"),
    F.lit("NA").alias("SUB_HOME_ADDR_CITY_NM"),
    F.lit(1).alias("SUB_HOME_ADDR_ST_CD_SK"),
    F.lit("NA").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.lit("NA").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.lit("NA").alias("SUB_MAIL_ADDR_LN_1"),
    F.lit("NA").alias("SUB_MAIL_ADDR_LN_2"),
    F.lit("NA").alias("SUB_MAIL_ADDR_LN_3"),
    F.lit("NA").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.lit(1).alias("SUB_MAIL_ADDR_ST_CD_SK"),
    F.lit("NA").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.lit("NA").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.lit("N").alias("JAN_COV_IN"),
    F.lit("N").alias("FEB_COV_IN"),
    F.lit("N").alias("MAR_COV_IN"),
    F.lit("N").alias("APR_COV_IN"),
    F.lit("N").alias("MAY_COV_IN"),
    F.lit("N").alias("JUN_COV_IN"),
    F.lit("N").alias("JUL_COV_IN"),
    F.lit("N").alias("AUG_COV_IN"),
    F.lit("N").alias("SEP_COV_IN"),
    F.lit("N").alias("OCT_COV_IN"),
    F.lit("N").alias("NOV_COV_IN"),
    F.lit("N").alias("DEC_COV_IN"),
    current_date().alias("SUB_BRTH_DT"),
    F.lit("NA").alias("MBR_SFX_NO")
)

df_Funnel_70 = df_MAIN_sel.unionByName(df_UNK_sel).unionByName(df_NA_sel)

df_seq_SUB_MA_DOR_Fkey = df_Funnel_70.select(common_cols_70)

# Apply rpad for char/varchar columns with known or implied lengths. Where lengths are known:
#  TAX_YR char(4), SUB_MIDINIT char(1), SUB_HOME_ADDR_ZIP_CD_5 char(5), SUB_HOME_ADDR_ZIP_CD_4 char(4),
#  SUB_MAIL_ADDR_ZIP_CD_5 char(5), SUB_MAIL_ADDR_ZIP_CD_4 char(4), JAN_COV_IN..DEC_COV_IN char(1).
#  All other string columns default to rpad(200).
def pad_col(col_name, length):
    return F.rpad(F.col(col_name), length, " ")

def default_pad(col_name):
    return F.rpad(F.col(col_name).cast(StringType()), 200, " ")

padded_exprs_70 = []
for c in common_cols_70:
    if c in ["TAX_YR"]:
        padded_exprs_70.append(pad_col(c,4).alias(c))
    elif c in ["SUB_MIDINIT"]:
        padded_exprs_70.append(pad_col(c,1).alias(c))
    elif c in ["SUB_HOME_ADDR_ZIP_CD_5","SUB_MAIL_ADDR_ZIP_CD_5"]:
        padded_exprs_70.append(pad_col(c,5).alias(c))
    elif c in ["SUB_HOME_ADDR_ZIP_CD_4","SUB_MAIL_ADDR_ZIP_CD_4"]:
        padded_exprs_70.append(pad_col(c,4).alias(c))
    elif c in ["JAN_COV_IN","FEB_COV_IN","MAR_COV_IN","APR_COV_IN","MAY_COV_IN","JUN_COV_IN","JUL_COV_IN","AUG_COV_IN","SEP_COV_IN","OCT_COV_IN","NOV_COV_IN","DEC_COV_IN"]:
        padded_exprs_70.append(pad_col(c,1).alias(c))
    else:
        # Check if column is integer, date, or timestamp => leave as is
        # Otherwise pad (string/varchar)
        # We'll guess we leave numeric or date alone.
        # So we check schema from df_seq_SUB_MA_DOR_Fkey: they might be int or date or timestamp?
        # Just do type-based check dynamically is not an option in static code, so do a quick approach:
        if c in ["SUB_MA_DOR_SK","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK","GRP_MA_DOR_SK","GRP_SK","SUB_SK"]:
            padded_exprs_70.append(F.col(c))
        elif c in ["SUB_BRTH_DT"]:
            padded_exprs_70.append(F.col(c))
        elif c in ["AS_OF_DTM"]:
            padded_exprs_70.append(F.col(c))
        else:
            padded_exprs_70.append(default_pad(c).alias(c))

df_seq_SUB_MA_DOR_Fkey_padded = df_seq_SUB_MA_DOR_Fkey.select(padded_exprs_70)

write_files(
    df_seq_SUB_MA_DOR_Fkey_padded,
    f"{adls_path}/load/SUB_MA_DOR.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)