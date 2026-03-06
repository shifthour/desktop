# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : IdsEdwMadorSeq
# MAGIC 
# MAGIC PROCESSING :Extracts subscriber data IDS and writes to the file SUB_MA_DOR_D.dat
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-04-12\(9)5839\(9)                                          Original Programming\(9)\(9)\(9)          EnterpriseDev2               
# MAGIC 
# MAGIC Ravi Ranjan               \(9) 2024-10-23\(9)US 630642\(9)                   Updated the transformation logic for the coulmns           EnterpriseDev2                         Jeyaprasanna           2024-12-18 
# MAGIC                                                                                                                                     JAN_COV_IN,FEB_COV_IN,MAR_COV_IN, 
# MAGIC                                                                                                                                    APR_COV_IN, MAY_COV_IN,JUN_COV_IN,
# MAGIC                                                                                                                                    JUL_COV_IN,AUG_COV_IN,SEP_COV_IN,
# MAGIC                                                                                                                                    OCT_COV_IN,NOV_COV_IN,DEC_COV_IN,
# MAGIC                                                                                                                                    FULL_YR_COV_IN\(9)\(9)\(9)

# MAGIC Extracts subscriber data IDS and writes to the file SUB_MA_DOR_D.dat
# MAGIC Extract Subscriber Data from IDS
# MAGIC Edit data and format into table format
# MAGIC Create the Load file SUB_MA_DOR_D.dat
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


EDWOwner = get_widget_value("EDWOwner", "")
edw_secret_name = get_widget_value("edw_secret_name", "")
IDSOwner = get_widget_value("IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")
SrcSysCd = get_widget_value("SrcSysCd", "")
RunID = get_widget_value("RunID", "")
FctsRunCycle = get_widget_value("FctsRunCycle", "")
BcbsScRunCycle = get_widget_value("BcbsScRunCycle", "")
UwsRunCycle = get_widget_value("UwsRunCycle", "")
EdwRunCycleDate = get_widget_value("EdwRunCycleDate", "")
EdwRunCycle = get_widget_value("EdwRunCycle", "")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_SUB_HIST = (
    f"Select GRP_ID, SUB_ID, TAX_YR, count(1) as Mbr_Cnt "
    f"from {IDSOwner}.sub_ma_dor_hist "
    f"group by GRP_ID, SUB_ID, TAX_YR "
    f"order by count(1) desc"
)
df_SUB_HIST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_SUB_HIST)
    .load()
)

extract_query_SUB_MA_DOR = (
    f"SELECT "
    f"SUB_MA_DOR_SK, "
    f"SUB_ID, "
    f"GRP_ID, "
    f"TAX_YR, "
    f"SRC_SYS_CD, "
    f"CRT_RUN_CYC_EXCTN_SK, "
    f"LAST_UPDT_RUN_CYC_EXCTN_SK, "
    f"AS_OF_DTM, "
    f"GRP_MA_DOR_SK, "
    f"GRP_SK, "
    f"SUB_SK, "
    f"SUB_FIRST_NM, "
    f"SUB_MIDINIT, "
    f"SUB_LAST_NM, "
    f"SUB_HOME_ADDR_LN_1, "
    f"SUB_HOME_ADDR_LN_2, "
    f"SUB_HOME_ADDR_LN_3, "
    f"SUB_HOME_ADDR_CITY_NM, "
    f"SUB_HOME_ADDR_ST_CD_SK, "
    f"SUB_HOME_ADDR_ZIP_CD_5, "
    f"SUB_HOME_ADDR_ZIP_CD_4, "
    f"SUB_MAIL_ADDR_LN_1, "
    f"SUB_MAIL_ADDR_LN_2, "
    f"SUB_MAIL_ADDR_LN_3, "
    f"SUB_MAIL_ADDR_CITY_NM, "
    f"SUB_MAIL_ADDR_ST_CD_SK, "
    f"SUB_MAIL_ADDR_ZIP_CD_5, "
    f"SUB_MAIL_ADDR_ZIP_CD_4, "
    f"JAN_COV_IN, "
    f"FEB_COV_IN, "
    f"MAR_COV_IN, "
    f"APR_COV_IN, "
    f"MAY_COV_IN, "
    f"JUN_COV_IN, "
    f"JUL_COV_IN, "
    f"AUG_COV_IN, "
    f"SEP_COV_IN, "
    f"OCT_COV_IN, "
    f"NOV_COV_IN, "
    f"DEC_COV_IN, "
    f"SUB_BRTH_DT, "
    f"MBR_SFX_NO "
    f"FROM {IDSOwner}.SUB_MA_DOR "
    f"WHERE "
    f"(SRC_SYS_CD = 'FACETS' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {FctsRunCycle} OR "
    f"SRC_SYS_CD = 'BCBSSC' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {BcbsScRunCycle} OR "
    f"SRC_SYS_CD = 'UWS' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {UwsRunCycle})"
)
df_SUB_MA_DOR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_SUB_MA_DOR)
    .load()
)

extract_query_Cd_Mppng = (
    f"SELECT CD_MPPNG_SK, SRC_DRVD_LKUP_VAL, TRGT_CD, TRGT_CD_NM "
    f"FROM {IDSOwner}.CD_MPPNG CD_MPPNG "
    f"WHERE SRC_SYS_CD = 'IDS' AND SRC_CLCTN_CD = 'IDS' "
    f"AND SRC_DOMAIN_NM = 'STATE' "
    f"AND TRGT_CLCTN_CD = 'IDS' "
    f"AND TRGT_DOMAIN_NM = 'STATE'"
)
df_Cd_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Cd_Mppng)
    .load()
)

extract_query_Cd_Mppng1 = (
    f"SELECT CD_MPPNG_SK, SRC_DRVD_LKUP_VAL AS SRC_DRVD_LKUP_VAL1, TRGT_CD AS TRGT_CD1, TRGT_CD_NM AS TRGT_CD_NM1 "
    f"FROM {IDSOwner}.CD_MPPNG CD_MPPNG "
    f"WHERE SRC_SYS_CD = 'IDS' AND SRC_CLCTN_CD = 'IDS' "
    f"AND SRC_DOMAIN_NM = 'STATE' "
    f"AND TRGT_CLCTN_CD = 'IDS' "
    f"AND TRGT_DOMAIN_NM = 'STATE'"
)
df_Cd_Mppng1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Cd_Mppng1)
    .load()
)

df_Lkp_Sub_tmp = (
    df_SUB_MA_DOR.alias("Lnk_Sub")
    .join(
        df_Cd_Mppng.alias("Lnk_Cd_Mppng"),
        F.col("Lnk_Sub.SUB_HOME_ADDR_ST_CD_SK") == F.col("Lnk_Cd_Mppng.CD_MPPNG_SK"),
        "left",
    )
    .join(
        df_Cd_Mppng1.alias("Lnk_Cd_Mppng2"),
        F.col("Lnk_Sub.SUB_MAIL_ADDR_ST_CD_SK") == F.col("Lnk_Cd_Mppng2.CD_MPPNG_SK"),
        "left",
    )
)

df_Lkp_Sub = df_Lkp_Sub_tmp.select(
    F.col("Lnk_Sub.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("Lnk_Sub.SUB_ID").alias("SUB_ID"),
    F.col("Lnk_Sub.GRP_ID").alias("GRP_ID"),
    F.col("Lnk_Sub.TAX_YR").alias("TAX_YR"),
    F.col("Lnk_Sub.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Lnk_Sub.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Sub.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Lnk_Sub.AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("Lnk_Sub.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
    F.col("Lnk_Sub.GRP_SK").alias("GRP_SK"),
    F.col("Lnk_Sub.SUB_SK").alias("SUB_SK"),
    F.col("Lnk_Sub.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Lnk_Sub.SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("Lnk_Sub.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("Lnk_Sub.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("Lnk_Sub.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("Lnk_Sub.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("Lnk_Sub.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("Lnk_Sub.SUB_HOME_ADDR_ST_CD_SK").alias("SUB_HOME_ADDR_ST_CD_SK"),
    F.col("Lnk_Sub.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("Lnk_Sub.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("Lnk_Sub.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("Lnk_Sub.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("Lnk_Sub.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("Lnk_Sub.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("Lnk_Sub.SUB_MAIL_ADDR_ST_CD_SK").alias("SUB_MAIL_ADDR_ST_CD_SK"),
    F.col("Lnk_Sub.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("Lnk_Sub.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.col("Lnk_Sub.JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("Lnk_Sub.FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("Lnk_Sub.MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("Lnk_Sub.APR_COV_IN").alias("APR_COV_IN"),
    F.col("Lnk_Sub.MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("Lnk_Sub.JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("Lnk_Sub.JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("Lnk_Sub.AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("Lnk_Sub.SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("Lnk_Sub.OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("Lnk_Sub.NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("Lnk_Sub.DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("Lnk_Sub.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("Lnk_Sub.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("Lnk_Cd_Mppng.TRGT_CD").alias("TRGT_CD"),
    F.col("Lnk_Cd_Mppng.TRGT_CD_NM").alias("TRGT_CD_NM"),
    F.col("Lnk_Cd_Mppng2.SRC_DRVD_LKUP_VAL1").alias("SRC_DRVD_LKUP_VAL1"),
    F.col("Lnk_Cd_Mppng2.TRGT_CD1").alias("TRGT_CD1"),
    F.col("Lnk_Cd_Mppng2.TRGT_CD_NM1").alias("TRGT_CD_NM1"),
)

df_Lkp_Mbr = (
    df_Lkp_Sub.alias("Lnk_Lkp")
    .join(
        df_SUB_HIST.alias("LnkCd_Mppng"),
        (
            (F.col("Lnk_Lkp.SUB_ID") == F.col("LnkCd_Mppng.SUB_ID"))
            & (F.col("Lnk_Lkp.GRP_ID") == F.col("LnkCd_Mppng.GRP_ID"))
            & (F.col("Lnk_Lkp.TAX_YR") == F.col("LnkCd_Mppng.TAX_YR"))
        ),
        "left",
    )
    .select(
        F.col("Lnk_Lkp.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
        F.col("Lnk_Lkp.SUB_ID").alias("SUB_ID"),
        F.col("Lnk_Lkp.GRP_ID").alias("GRP_ID"),
        F.col("Lnk_Lkp.TAX_YR").alias("TAX_YR"),
        F.col("Lnk_Lkp.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Lnk_Lkp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_Lkp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Lnk_Lkp.AS_OF_DTM").alias("AS_OF_DTM"),
        F.col("Lnk_Lkp.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
        F.col("Lnk_Lkp.GRP_SK").alias("GRP_SK"),
        F.col("Lnk_Lkp.SUB_SK").alias("SUB_SK"),
        F.col("Lnk_Lkp.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        F.col("Lnk_Lkp.SUB_MIDINIT").alias("SUB_MIDINIT"),
        F.col("Lnk_Lkp.SUB_LAST_NM").alias("SUB_LAST_NM"),
        F.col("Lnk_Lkp.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
        F.col("Lnk_Lkp.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
        F.col("Lnk_Lkp.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
        F.col("Lnk_Lkp.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
        F.col("Lnk_Lkp.SUB_HOME_ADDR_ST_CD_SK").alias("SUB_HOME_ADDR_ST_CD_SK"),
        F.col("Lnk_Lkp.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
        F.col("Lnk_Lkp.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
        F.col("Lnk_Lkp.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
        F.col("Lnk_Lkp.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
        F.col("Lnk_Lkp.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
        F.col("Lnk_Lkp.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
        F.col("Lnk_Lkp.SUB_MAIL_ADDR_ST_CD_SK").alias("SUB_MAIL_ADDR_ST_CD_SK"),
        F.col("Lnk_Lkp.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        F.col("Lnk_Lkp.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
        F.col("Lnk_Lkp.JAN_COV_IN").alias("JAN_COV_IN"),
        F.col("Lnk_Lkp.FEB_COV_IN").alias("FEB_COV_IN"),
        F.col("Lnk_Lkp.MAR_COV_IN").alias("MAR_COV_IN"),
        F.col("Lnk_Lkp.APR_COV_IN").alias("APR_COV_IN"),
        F.col("Lnk_Lkp.MAY_COV_IN").alias("MAY_COV_IN"),
        F.col("Lnk_Lkp.JUN_COV_IN").alias("JUN_COV_IN"),
        F.col("Lnk_Lkp.JUL_COV_IN").alias("JUL_COV_IN"),
        F.col("Lnk_Lkp.AUG_COV_IN").alias("AUG_COV_IN"),
        F.col("Lnk_Lkp.SEP_COV_IN").alias("SEP_COV_IN"),
        F.col("Lnk_Lkp.OCT_COV_IN").alias("OCT_COV_IN"),
        F.col("Lnk_Lkp.NOV_COV_IN").alias("NOV_COV_IN"),
        F.col("Lnk_Lkp.DEC_COV_IN").alias("DEC_COV_IN"),
        F.col("Lnk_Lkp.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
        F.col("Lnk_Lkp.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("Lnk_Lkp.TRGT_CD").alias("TRGT_CD"),
        F.col("Lnk_Lkp.TRGT_CD_NM").alias("TRGT_CD_NM"),
        F.col("Lnk_Lkp.SRC_DRVD_LKUP_VAL1").alias("SRC_DRVD_LKUP_VAL1"),
        F.col("Lnk_Lkp.TRGT_CD1").alias("TRGT_CD1"),
        F.col("Lnk_Lkp.TRGT_CD_NM1").alias("TRGT_CD_NM1"),
        F.col("LnkCd_Mppng.MBR_CNT").alias("MBR_CNT"),
    )
)

df_Trf_intermediate = df_Lkp_Mbr.withColumn(
    "SvFullYearCovIn",
    F.when(
        (trim(F.when(F.col("JAN_COV_IN").isNull(), F.lit("")).otherwise(F.col("JAN_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("FEB_COV_IN").isNull(), F.lit("")).otherwise(F.col("FEB_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("MAR_COV_IN").isNull(), F.lit("")).otherwise(F.col("MAR_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("APR_COV_IN").isNull(), F.lit("")).otherwise(F.col("APR_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("MAY_COV_IN").isNull(), F.lit("")).otherwise(F.col("MAY_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("JUN_COV_IN").isNull(), F.lit("")).otherwise(F.col("JUN_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("JUL_COV_IN").isNull(), F.lit("")).otherwise(F.col("JUL_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("AUG_COV_IN").isNull(), F.lit("")).otherwise(F.col("AUG_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("SEP_COV_IN").isNull(), F.lit("")).otherwise(F.col("SEP_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("OCT_COV_IN").isNull(), F.lit("")).otherwise(F.col("OCT_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("NOV_COV_IN").isNull(), F.lit("")).otherwise(F.col("NOV_COV_IN"))) == F.lit("X"))
        & (trim(F.when(F.col("DEC_COV_IN").isNull(), F.lit("")).otherwise(F.col("DEC_COV_IN"))) == F.lit("X")),
        F.lit("Y"),
    ).otherwise(F.lit("N")),
)

df_Trf = df_Trf_intermediate.select(
    F.col("SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("TAX_YR").alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(EdwRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EdwRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("SUB_SK").alias("SUB_SK"),
    F.col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    F.col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    F.col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    F.col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    F.col("TRGT_CD").alias("SUB_HOME_ADDR_ST_CD"),
    F.col("TRGT_CD_NM").alias("SUB_HOME_ADDR_ST_NM"),
    F.col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    F.col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    F.col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    F.col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    F.col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    F.col("TRGT_CD1").alias("SUB_MAIL_ADDR_ST_CD"),
    F.col("TRGT_CD_NM1").alias("SUB_MAIL_ADDR_ST_NM"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    F.col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("JAN_COV_IN") == F.lit("X")) | (F.col("JAN_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("JAN_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("FEB_COV_IN") == F.lit("X")) | (F.col("FEB_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("FEB_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("MAR_COV_IN") == F.lit("X")) | (F.col("MAR_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("MAR_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("APR_COV_IN") == F.lit("X")) | (F.col("APR_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("APR_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("MAY_COV_IN") == F.lit("X")) | (F.col("MAY_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("MAY_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("JUN_COV_IN") == F.lit("X")) | (F.col("JUN_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("JUN_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("JUL_COV_IN") == F.lit("X")) | (F.col("JUL_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("JUL_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("AUG_COV_IN") == F.lit("X")) | (F.col("AUG_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("AUG_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("SEP_COV_IN") == F.lit("X")) | (F.col("SEP_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("SEP_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("OCT_COV_IN") == F.lit("X")) | (F.col("OCT_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("OCT_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("NOV_COV_IN") == F.lit("X")) | (F.col("NOV_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("NOV_COV_IN"),
    F.when(
        F.col("SvFullYearCovIn") == F.lit("Y"),
        F.lit("")
    ).otherwise(
        F.when(
            (F.col("DEC_COV_IN") == F.lit("X")) | (F.col("DEC_COV_IN") == F.lit("Y")), 
            F.lit("X")
        ).otherwise(F.lit(""))
    ).alias("DEC_COV_IN"),
    F.col("SvFullYearCovIn").alias("FULL_YR_COV_IN"),
    F.when(
        F.coalesce(F.col("MBR_CNT"), F.lit(0)) > 1,
        F.lit("Y")
    ).otherwise(F.lit("N")).alias("CRCTN_IN"),
    F.col("SUB_BRTH_DT").alias("SUB_BRTH_DT"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.lit(EdwRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("SRC_SYS_CD") == F.lit("FACETS"), F.lit(FctsRunCycle))
    .when(F.col("SRC_SYS_CD") == F.lit("BCBSSC"), F.lit(BcbsScRunCycle))
    .when(F.col("SRC_SYS_CD") == F.lit("UWS"), F.lit(UwsRunCycle))
    .otherwise(F.lit("1"))
    .alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_HOME_ADDR_ST_CD_SK").alias("SUB_HOME_ADDR_ST_CD_SK"),
    F.col("SUB_MAIL_ADDR_ST_CD_SK").alias("SUB_MAIL_ADDR_ST_CD_SK"),
)

df_final = df_Trf.select(
    F.col("SUB_MA_DOR_SK"),
    F.col("SUB_ID"),
    F.col("GRP_ID"),
    rpad(F.col("TAX_YR"), 4, " ").alias("TAX_YR"),
    F.col("SRC_SYS_CD"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("AS_OF_DTM"),
    F.col("GRP_MA_DOR_SK"),
    F.col("GRP_SK"),
    F.col("SUB_SK"),
    F.col("SUB_FIRST_NM"),
    rpad(F.col("SUB_MIDINIT"), 1, " ").alias("SUB_MIDINIT"),
    F.col("SUB_LAST_NM"),
    F.col("SUB_HOME_ADDR_LN_1"),
    F.col("SUB_HOME_ADDR_LN_2"),
    F.col("SUB_HOME_ADDR_LN_3"),
    F.col("SUB_HOME_ADDR_CITY_NM"),
    F.col("SUB_HOME_ADDR_ST_CD"),
    F.col("SUB_HOME_ADDR_ST_NM"),
    rpad(F.col("SUB_HOME_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    rpad(F.col("SUB_HOME_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    F.col("SUB_MAIL_ADDR_LN_1"),
    F.col("SUB_MAIL_ADDR_LN_2"),
    F.col("SUB_MAIL_ADDR_LN_3"),
    F.col("SUB_MAIL_ADDR_CITY_NM"),
    F.col("SUB_MAIL_ADDR_ST_CD"),
    F.col("SUB_MAIL_ADDR_ST_NM"),
    rpad(F.col("SUB_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    rpad(F.col("SUB_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    rpad(F.col("JAN_COV_IN"), 1, " ").alias("JAN_COV_IN"),
    rpad(F.col("FEB_COV_IN"), 1, " ").alias("FEB_COV_IN"),
    rpad(F.col("MAR_COV_IN"), 1, " ").alias("MAR_COV_IN"),
    rpad(F.col("APR_COV_IN"), 1, " ").alias("APR_COV_IN"),
    rpad(F.col("MAY_COV_IN"), 1, " ").alias("MAY_COV_IN"),
    rpad(F.col("JUN_COV_IN"), 1, " ").alias("JUN_COV_IN"),
    rpad(F.col("JUL_COV_IN"), 1, " ").alias("JUL_COV_IN"),
    rpad(F.col("AUG_COV_IN"), 1, " ").alias("AUG_COV_IN"),
    rpad(F.col("SEP_COV_IN"), 1, " ").alias("SEP_COV_IN"),
    rpad(F.col("OCT_COV_IN"), 1, " ").alias("OCT_COV_IN"),
    rpad(F.col("NOV_COV_IN"), 1, " ").alias("NOV_COV_IN"),
    rpad(F.col("DEC_COV_IN"), 1, " ").alias("DEC_COV_IN"),
    rpad(F.col("FULL_YR_COV_IN"), 1, " ").alias("FULL_YR_COV_IN"),
    rpad(F.col("CRCTN_IN"), 1, " ").alias("CRCTN_IN"),
    F.col("SUB_BRTH_DT"),
    F.col("MBR_SFX_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("SUB_HOME_ADDR_ST_CD_SK"),
    F.col("SUB_MAIL_ADDR_ST_CD_SK"),
)

write_files(
    df_final,
    f"{adls_path}/load/SUB_MA_DOR_D.{SrcSysCd}.{RunID}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)