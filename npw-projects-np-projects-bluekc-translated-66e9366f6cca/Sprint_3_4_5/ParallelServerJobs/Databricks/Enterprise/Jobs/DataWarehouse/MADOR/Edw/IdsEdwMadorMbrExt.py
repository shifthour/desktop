# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : IdsEdwMadorSeq
# MAGIC 
# MAGIC PROCESSING : Extracts member data from IDS and writes to the file MBR_MA_DOR_D.dat
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-04-12\(9)5839\(9)                                          Original Programming\(9)\(9)\(9)          EnterpriseDev2                        Kalyan Neelam          2018-06-14
# MAGIC 
# MAGIC Ravi Ranjan               \(9) 2024-10-23\(9)US 630642\(9)                   Updated the transformation logic for the coulmns            EnterpriseDev2                      Jeyaprasanna             2024-12-18             
# MAGIC                                                                                                                                    JAN_COV_IN,FEB_COV_IN,MAR_COV_IN,
# MAGIC                                                                                                                                    APR_COV_IN, MAY_COV_IN,JUN_COV_IN,
# MAGIC                                                                                                                                    JUL_COV_IN,AUG_COV_IN,SEP_COV_IN,
# MAGIC                                                                                                                                    OCT_COV_IN,NOV_COV_IN,DEC_COV_IN,
# MAGIC                                                                                                                                    FULL_YR_COV_IN\(9)\(9)\(9)

# MAGIC Extracts member data from IDS and writes to the file MBR_MA_DOR_D.dat
# MAGIC Extract Member Data from IDS
# MAGIC Edit data and format into table format
# MAGIC Create the Load file MBR_MA_DOR_D.dat
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunID = get_widget_value('RunID','')
FctsRunCycle = get_widget_value('FctsRunCycle','')
SrcSysCd = get_widget_value('SrcSysCd','')
BcbsScRunCycle = get_widget_value('BcbsScRunCycle','')
UwsRunCycle = get_widget_value('UwsRunCycle','')
EdwRunCycleDate = get_widget_value('EdwRunCycleDate','')
EdwRunCycle = get_widget_value('EdwRunCycle','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"Select GRP_ID, SUB_ID, MBR_ID, TAX_YR, count(1) as Mbr_Cnt from {IDSOwner}.mbr_ma_dor_hist group by GRP_ID, SUB_ID, MBR_ID, TAX_YR Order by Count(1) desc"
df_MBR_HIST = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""
SELECT
MBR_MA_DOR_SK,
MBR_ID,
SUB_ID,
GRP_ID,
TAX_YR,
SRC_SYS_CD,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
AS_OF_DTM,
GRP_SK,
MBR_SK,
SUB_MA_DOR_SK,
SUB_SK,
MBR_RELSHP_CD_SK,
MBR_FIRST_NM,
MBR_MIDINIT,
MBR_LAST_NM,
MBR_MAIL_ADDR_LN_1,
MBR_MAIL_ADDR_LN_2,
MBR_MAIL_ADDR_LN_3,
MBR_MAIL_ADDR_CITY_NM,
MBR_MAIL_ADDR_ST_CD_SK,
MBR_MAIL_ADDR_ZIP_CD_5,
MBR_MAIL_ADDR_ZIP_CD_4,
JAN_COV_IN,
FEB_COV_IN,
MAR_COV_IN,
APR_COV_IN,
MAY_COV_IN,
JUN_COV_IN,
JUL_COV_IN,
AUG_COV_IN,
SEP_COV_IN,
OCT_COV_IN,
NOV_COV_IN,
DEC_COV_IN,
MBR_BRTH_DT,
MBR_SFX_NO
FROM {IDSOwner}.MBR_MA_DOR
WHERE
(SRC_SYS_CD = 'FACETS' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {FctsRunCycle} OR
SRC_SYS_CD = 'BCBSSC' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {BcbsScRunCycle} OR
SRC_SYS_CD = 'UWS' AND LAST_UPDT_RUN_CYC_EXCTN_SK >= {UwsRunCycle} )
"""
df_MBR_MA_DOR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""
SELECT CD_MPPNG_SK, SRC_DRVD_LKUP_VAL, TRGT_CD, TRGT_CD_NM 
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE SRC_SYS_CD = 'IDS' AND SRC_CLCTN_CD = 'IDS'
AND SRC_DOMAIN_NM = 'STATE'
AND TRGT_CLCTN_CD = 'IDS'
AND TRGT_DOMAIN_NM = 'STATE'
"""
df_Cd_Mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

extract_query = f"""
SELECT CD_MPPNG_SK, SRC_DRVD_LKUP_VAL, TRGT_CD as TRGT_CD1, TRGT_CD_NM as TRGT_CD_NM1, SRC_SYS_CD
FROM {IDSOwner}.CD_MPPNG CD_MPPNG
WHERE SRC_DOMAIN_NM = 'MEMBER RELATIONSHIP'
AND TRGT_CLCTN_CD = 'IDS'
AND TRGT_DOMAIN_NM = 'MEMBER RELATIONSHIP'
"""
df_CD_MPPNG1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Lkp_Mbr_intermediate = (
    df_MBR_MA_DOR.alias("Lnk_Lkp_Mbr")
    .join(
        df_Cd_Mppng.alias("LnkCd_Mppng"),
        (
            (col("Lnk_Lkp_Mbr.MBR_ID") == col("LnkCd_Mppng.MBR_ID"))
            & (col("Lnk_Lkp_Mbr.SUB_ID") == col("LnkCd_Mppng.SUB_ID"))
            & (col("Lnk_Lkp_Mbr.GRP_ID") == col("LnkCd_Mppng.GRP_ID"))
            & (col("Lnk_Lkp_Mbr.TAX_YR") == col("LnkCd_Mppng.TAX_YR"))
            & (col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_ST_CD_SK") == col("LnkCd_Mppng.SRC_DRVD_LKUP_VAL"))
            & (col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_ST_CD_SK") == col("LnkCd_Mppng.CD_MPPNG_SK"))
        ),
        "left",
    )
    .join(
        df_CD_MPPNG1.alias("lkup2"),
        (
            (col("Lnk_Lkp_Mbr.MBR_RELSHP_CD_SK") == col("lkup2.CD_MPPNG_SK"))
            & (col("Lnk_Lkp_Mbr.SRC_SYS_CD") == col("lkup2.SRC_SYS_CD"))
        ),
        "left",
    )
)

df_Lkp_Mbr = df_Lkp_Mbr_intermediate.select(
    col("Lnk_Lkp_Mbr.MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK"),
    col("Lnk_Lkp_Mbr.MBR_ID").alias("MBR_ID"),
    col("Lnk_Lkp_Mbr.SUB_ID").alias("SUB_ID"),
    col("Lnk_Lkp_Mbr.GRP_ID").alias("GRP_ID"),
    col("Lnk_Lkp_Mbr.TAX_YR").alias("TAX_YR"),
    col("Lnk_Lkp_Mbr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_Lkp_Mbr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Lnk_Lkp_Mbr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Lnk_Lkp_Mbr.AS_OF_DTM").alias("AS_OF_DTM"),
    col("Lnk_Lkp_Mbr.GRP_SK").alias("GRP_SK"),
    col("Lnk_Lkp_Mbr.MBR_SK").alias("MBR_SK"),
    col("Lnk_Lkp_Mbr.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    col("Lnk_Lkp_Mbr.SUB_SK").alias("SUB_SK"),
    col("Lnk_Lkp_Mbr.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    col("Lnk_Lkp_Mbr.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("Lnk_Lkp_Mbr.MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("Lnk_Lkp_Mbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_ST_CD_SK").alias("MBR_MAIL_ADDR_ST_CD_SK"),
    col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    col("Lnk_Lkp_Mbr.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    col("Lnk_Lkp_Mbr.JAN_COV_IN").alias("JAN_COV_IN"),
    col("Lnk_Lkp_Mbr.FEB_COV_IN").alias("FEB_COV_IN"),
    col("Lnk_Lkp_Mbr.MAR_COV_IN").alias("MAR_COV_IN"),
    col("Lnk_Lkp_Mbr.APR_COV_IN").alias("APR_COV_IN"),
    col("Lnk_Lkp_Mbr.MAY_COV_IN").alias("MAY_COV_IN"),
    col("Lnk_Lkp_Mbr.JUN_COV_IN").alias("JUN_COV_IN"),
    col("Lnk_Lkp_Mbr.JUL_COV_IN").alias("JUL_COV_IN"),
    col("Lnk_Lkp_Mbr.AUG_COV_IN").alias("AUG_COV_IN"),
    col("Lnk_Lkp_Mbr.SEP_COV_IN").alias("SEP_COV_IN"),
    col("Lnk_Lkp_Mbr.OCT_COV_IN").alias("OCT_COV_IN"),
    col("Lnk_Lkp_Mbr.NOV_COV_IN").alias("NOV_COV_IN"),
    col("Lnk_Lkp_Mbr.DEC_COV_IN").alias("DEC_COV_IN"),
    col("Lnk_Lkp_Mbr.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("Lnk_Lkp_Mbr.MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("LnkCd_Mppng.TRGT_CD").alias("TRGT_CD"),
    col("LnkCd_Mppng.TRGT_CD_NM").alias("TRGT_CD_NM"),
    col("lkup2.TRGT_CD1").alias("TRGT_CD1"),
    col("lkup2.TRGT_CD_NM1").alias("TRGT_CD_NM1"),
)

df_Lkp_Mbr1_intermediate = (
    df_Lkp_Mbr.alias("Lnk_Mbr")
    .join(
        df_MBR_HIST.alias("LnkCd_Mppng"),
        (
            (col("Lnk_Mbr.MBR_ID") == col("LnkCd_Mppng.MBR_ID"))
            & (col("Lnk_Mbr.SUB_ID") == col("LnkCd_Mppng.SUB_ID"))
            & (col("Lnk_Mbr.GRP_ID") == col("LnkCd_Mppng.GRP_ID"))
            & (col("Lnk_Mbr.TAX_YR") == col("LnkCd_Mppng.TAX_YR"))
            & (col("Lnk_Mbr.MBR_MAIL_ADDR_ST_CD_SK") == col("LnkCd_Mppng.SRC_DRVD_LKUP_VAL"))
            & (col("Lnk_Mbr.MBR_MAIL_ADDR_ST_CD_SK") == col("LnkCd_Mppng.CD_MPPNG_SK"))
        ),
        "left",
    )
)

df_Lkp_Mbr1 = df_Lkp_Mbr1_intermediate.select(
    col("Lnk_Mbr.MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK"),
    col("Lnk_Mbr.MBR_ID").alias("MBR_ID"),
    col("Lnk_Mbr.SUB_ID").alias("SUB_ID"),
    col("Lnk_Mbr.GRP_ID").alias("GRP_ID"),
    col("Lnk_Mbr.TAX_YR").alias("TAX_YR"),
    col("Lnk_Mbr.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("Lnk_Mbr.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("Lnk_Mbr.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("Lnk_Mbr.AS_OF_DTM").alias("AS_OF_DTM"),
    col("Lnk_Mbr.GRP_SK").alias("GRP_SK"),
    col("Lnk_Mbr.MBR_SK").alias("MBR_SK"),
    col("Lnk_Mbr.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    col("Lnk_Mbr.SUB_SK").alias("SUB_SK"),
    col("Lnk_Mbr.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    col("Lnk_Mbr.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("Lnk_Mbr.MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("Lnk_Mbr.MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("Lnk_Mbr.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    col("Lnk_Mbr.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    col("Lnk_Mbr.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    col("Lnk_Mbr.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    col("Lnk_Mbr.MBR_MAIL_ADDR_ST_CD_SK").alias("MBR_MAIL_ADDR_ST_CD_SK"),
    col("Lnk_Mbr.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    col("Lnk_Mbr.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    col("Lnk_Mbr.JAN_COV_IN").alias("JAN_COV_IN"),
    col("Lnk_Mbr.FEB_COV_IN").alias("FEB_COV_IN"),
    col("Lnk_Mbr.MAR_COV_IN").alias("MAR_COV_IN"),
    col("Lnk_Mbr.APR_COV_IN").alias("APR_COV_IN"),
    col("Lnk_Mbr.MAY_COV_IN").alias("MAY_COV_IN"),
    col("Lnk_Mbr.JUN_COV_IN").alias("JUN_COV_IN"),
    col("Lnk_Mbr.JUL_COV_IN").alias("JUL_COV_IN"),
    col("Lnk_Mbr.AUG_COV_IN").alias("AUG_COV_IN"),
    col("Lnk_Mbr.SEP_COV_IN").alias("SEP_COV_IN"),
    col("Lnk_Mbr.OCT_COV_IN").alias("OCT_COV_IN"),
    col("Lnk_Mbr.NOV_COV_IN").alias("NOV_COV_IN"),
    col("Lnk_Mbr.DEC_COV_IN").alias("DEC_COV_IN"),
    col("Lnk_Mbr.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("Lnk_Mbr.MBR_SFX_NO").alias("MBR_SFX_NO"),
    col("Lnk_Mbr.TRGT_CD").alias("TRGT_CD"),
    col("Lnk_Mbr.TRGT_CD_NM").alias("TRGT_CD_NM"),
    col("Lnk_Mbr.TRGT_CD1").alias("TRGT_CD1"),
    col("Lnk_Mbr.TRGT_CD_NM1").alias("TRGT_CD_NM1"),
    col("LnkCd_Mppng.Mbr_Cnt").alias("MBR_CNT"),
)

df_trf_sv = df_Lkp_Mbr1.withColumn(
    "SvFullYearCovIn",
    when(
        (trim(col("JAN_COV_IN")) == "X")
        & (trim(col("FEB_COV_IN")) == "X")
        & (trim(col("MAR_COV_IN")) == "X")
        & (trim(col("APR_COV_IN")) == "X")
        & (trim(col("MAY_COV_IN")) == "X")
        & (trim(col("JUN_COV_IN")) == "X")
        & (trim(col("JUL_COV_IN")) == "X")
        & (trim(col("AUG_COV_IN")) == "X")
        & (trim(col("SEP_COV_IN")) == "X")
        & (trim(col("OCT_COV_IN")) == "X")
        & (trim(col("NOV_COV_IN")) == "X")
        & (trim(col("DEC_COV_IN")) == "X"),
        "Y",
    ).otherwise("N"),
)

df_trf = df_trf_sv.select(
    col("MBR_MA_DOR_SK").alias("MBR_MA_DOR_SK"),
    col("MBR_ID").alias("MBR_ID"),
    col("SUB_ID").alias("SUB_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("TAX_YR").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(EdwRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EdwRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("AS_OF_DTM").alias("AS_OF_DTM"),
    col("GRP_SK").alias("GRP_SK"),
    col("MBR_SK").alias("MBR_SK"),
    col("SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    col("SUB_SK").alias("SUB_SK"),
    when(
        trim(col("TRGT_CD1")) == "",
        "N/A",
    ).otherwise(col("TRGT_CD1")).alias("MBR_RELSHP_CD"),
    when(
        trim(col("TRGT_CD_NM1")) == "",
        "N/A",
    ).otherwise(col("TRGT_CD_NM1")).alias("MBR_RELSHP_NM"),
    col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    col("MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    col("MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    col("MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    col("MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    col("TRGT_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    col("TRGT_CD_NM").alias("MBR_MAIL_ADDR_ST_NM"),
    col("MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    col("MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("JAN_COV_IN") == "X") | (col("JAN_COV_IN") == "Y"), "X"
    ).otherwise("").alias("JAN_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("FEB_COV_IN") == "X") | (col("FEB_COV_IN") == "Y"), "X"
    ).otherwise("").alias("FEB_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("MAR_COV_IN") == "X") | (col("MAR_COV_IN") == "Y"), "X"
    ).otherwise("").alias("MAR_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("APR_COV_IN") == "X") | (col("APR_COV_IN") == "Y"), "X"
    ).otherwise("").alias("APR_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("MAY_COV_IN") == "X") | (col("MAY_COV_IN") == "Y"), "X"
    ).otherwise("").alias("MAY_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("JUN_COV_IN") == "X") | (col("JUN_COV_IN") == "Y"), "X"
    ).otherwise("").alias("JUN_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("JUL_COV_IN") == "X") | (col("JUL_COV_IN") == "Y"), "X"
    ).otherwise("").alias("JUL_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("AUG_COV_IN") == "X") | (col("AUG_COV_IN") == "Y"), "X"
    ).otherwise("").alias("AUG_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("SEP_COV_IN") == "X") | (col("SEP_COV_IN") == "Y"), "X"
    ).otherwise("").alias("SEP_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("OCT_COV_IN") == "X") | (col("OCT_COV_IN") == "Y"), "X"
    ).otherwise("").alias("OCT_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("NOV_COV_IN") == "X") | (col("NOV_COV_IN") == "Y"), "X"
    ).otherwise("").alias("NOV_COV_IN"),
    when(col("SvFullYearCovIn") == "Y", "").when(
        (col("DEC_COV_IN") == "X") | (col("DEC_COV_IN") == "Y"), "X"
    ).otherwise("").alias("DEC_COV_IN"),
    col("SvFullYearCovIn").alias("FULL_YR_COV_IN"),
    when(
        (col("MBR_CNT").isNotNull()) & (col("MBR_CNT") > 1),
        "Y",
    ).otherwise("N").alias("CRCTN_IN"),
    col("MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    lit(EdwRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EdwRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(col("SRC_SYS_CD") == "FACETS", FctsRunCycle)
    .when(col("SRC_SYS_CD") == "BCBSSC", BcbsScRunCycle)
    .when(col("SRC_SYS_CD") == "UWS", UwsRunCycle)
    .otherwise(lit("1"))
    .alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    col("MBR_MAIL_ADDR_ST_CD_SK").alias("MBR_MAIL_ADDR_ST_CD_SK"),
)

df_final = (
    df_trf.select(
        col("MBR_MA_DOR_SK"),
        col("MBR_ID"),
        col("SUB_ID"),
        col("GRP_ID"),
        rpad(col("TAX_YR"), 4, " ").alias("TAX_YR"),
        col("SRC_SYS_CD"),
        rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("AS_OF_DTM"),
        col("GRP_SK"),
        col("MBR_SK"),
        col("SUB_MA_DOR_SK"),
        col("SUB_SK"),
        col("MBR_RELSHP_CD"),
        col("MBR_RELSHP_NM"),
        col("MBR_FIRST_NM"),
        rpad(col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
        col("MBR_LAST_NM"),
        col("MBR_MAIL_ADDR_LN_1"),
        col("MBR_MAIL_ADDR_LN_2"),
        col("MBR_MAIL_ADDR_LN_3"),
        col("MBR_MAIL_ADDR_CITY_NM"),
        col("MBR_MAIL_ADDR_ST_CD"),
        col("MBR_MAIL_ADDR_ST_NM"),
        rpad(col("MBR_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
        rpad(col("MBR_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
        rpad(col("JAN_COV_IN"), 1, " ").alias("JAN_COV_IN"),
        rpad(col("FEB_COV_IN"), 1, " ").alias("FEB_COV_IN"),
        rpad(col("MAR_COV_IN"), 1, " ").alias("MAR_COV_IN"),
        rpad(col("APR_COV_IN"), 1, " ").alias("APR_COV_IN"),
        rpad(col("MAY_COV_IN"), 1, " ").alias("MAY_COV_IN"),
        rpad(col("JUN_COV_IN"), 1, " ").alias("JUN_COV_IN"),
        rpad(col("JUL_COV_IN"), 1, " ").alias("JUL_COV_IN"),
        rpad(col("AUG_COV_IN"), 1, " ").alias("AUG_COV_IN"),
        rpad(col("SEP_COV_IN"), 1, " ").alias("SEP_COV_IN"),
        rpad(col("OCT_COV_IN"), 1, " ").alias("OCT_COV_IN"),
        rpad(col("NOV_COV_IN"), 1, " ").alias("NOV_COV_IN"),
        rpad(col("DEC_COV_IN"), 1, " ").alias("DEC_COV_IN"),
        rpad(col("FULL_YR_COV_IN"), 1, " ").alias("FULL_YR_COV_IN"),
        rpad(col("CRCTN_IN"), 1, " ").alias("CRCTN_IN"),
        col("MBR_BRTH_DT"),
        col("MBR_SFX_NO"),
        col("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("MBR_RELSHP_CD_SK"),
        col("MBR_MAIL_ADDR_ST_CD_SK"),
    )
)

write_files(
    df_final,
    f"{adls_path}/load/MBR_MA_DOR_D.{SrcSysCd}.{RunID}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)