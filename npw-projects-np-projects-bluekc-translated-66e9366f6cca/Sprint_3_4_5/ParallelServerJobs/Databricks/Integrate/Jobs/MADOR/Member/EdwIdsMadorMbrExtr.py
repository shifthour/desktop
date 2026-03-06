# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : EdwIdsMadorExtrSeq
# MAGIC 
# MAGIC PROCESSING : Extract Member data from EDW tables.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-03-30\(9)5839 - MADOR                                    Original Programming\(9)\(9)\(9)          IntegrateDev2                         Kalyan Neelam           2018-06-12
# MAGIC Abhiram Dasarathy\(9)\(9)2019-12-12\(9)F-114877\(9)\(9)Updated the join stage to use MbrsFromFACETS as driver\(9)          IntegrateDev2                         Jaideep Mankala       12/13/2019
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)\(9)Also, Added Parameters for Deductibles and OOPs

# MAGIC Select recast data from EDW where
# MAGIC RCST.SRC_SYS_CD = 'FACETS'
# MAGIC Split the records in to multiple links according the month
# MAGIC Extract Member data from EDW tables.
# MAGIC Extract Member data from EDW tables.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Tax_Year = get_widget_value("Tax_Year","")
SrcSysCd = get_widget_value("SrcSysCd","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
RunID = get_widget_value("RunID","")
EDWOwner = get_widget_value("EDWOwner","")
edw_secret_name = get_widget_value("edw_secret_name","")
Begin_Date = get_widget_value("Begin_Date","")
End_Date = get_widget_value("End_Date","")
Begin_CCYYMM = get_widget_value("Begin_CCYYMM","")
End_CCYYMM = get_widget_value("End_CCYYMM","")
AsOfDtm = get_widget_value("AsOfDtm","")

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_IDS_SUB_MA_DOR = f"Select GRP_ID, TAX_YR, SUB_ID, SUB_MA_DOR_SK From {IDSOwner}.SUB_MA_DOR Where TAX_YR = '{Tax_Year}'"
df_IDS_SUB_MA_DOR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_IDS_SUB_MA_DOR)
    .load()
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
query_EDWMassachusettsMbrs = f"""
SELECT
MBR_D.MBR_SK,
MBR_D.MBR_ID,
MBR_D.SUB_ID,
MBR_D.MBR_SFX_NO,
MBR_D.SRC_SYS_CD,
MBR_ENR_D.GRP_ID,
MBR_D.MBR_LAST_NM,
MBR_D.MBR_FIRST_NM,
MBR_D.MBR_MIDINIT,
MBR_D.MBR_HOME_ADDR_LN_1,
MBR_D.MBR_HOME_ADDR_LN_2,
MBR_D.MBR_HOME_ADDR_CITY_NM,
MBR_D.MBR_HOME_ADDR_ST_CD,
MBR_D.MBR_HOME_ADDR_ZIP_CD_5,
MBR_D.MBR_HOME_ADDR_ZIP_CD_4,
MBR_D.MBR_BRTH_DT_SK,
MBR_D.MBR_RELSHP_CD_SK
From {EDWOwner}.MBR_D as MBR_D
INNER JOIN {EDWOwner}.MBR_ENR_D as MBR_ENR_D
        ON MBR_D.MBR_SK = MBR_ENR_D.MBR_SK
Where
MBR_D.SRC_SYS_CD = '{SrcSysCd}'
and MBR_ENR_D.MBR_ENR_ELIG_IN = 'Y'
and MBR_ENR_D.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
and MBR_ENR_D.MBR_ENR_TERM_DT_SK >= '{Begin_Date}'
and MBR_ENR_D.MBR_ENR_EFF_DT_SK <= '{End_Date}'
and MBR_ENR_D.GRP_ID not in ('10000000', '10001000', '10003000', '10004000', '10022000', '10023000', '10024000')
and SUBSTR(MBR_ENR_D.GRP_ID,1,2) <> '65'
and MBR_ENR_D.PROD_SH_NM <> 'BCARE'
and MBR_D.MBR_HOME_ADDR_ST_CD = 'MA'
and MBR_D.MBR_HOME_ADDR_ZIP_CD_5 < '10000'
and MBR_D.MBR_HOME_ADDR_CITY_NM not in ('KANSAS CITY', 'LEES SUMMIT', 'ST CHARLES', 'LEES SUMMITT' )
and MBR_ENR_D.GRP_ID IS NOT NULL

UNION

SELECT
MBR_D.MBR_SK,
MBR_D.MBR_ID,
MBR_D.SUB_ID,
MBR_D.MBR_SFX_NO,
MBR_D.SRC_SYS_CD,
MBR_ENR_D.GRP_ID,
MBR_D.MBR_LAST_NM,
MBR_D.MBR_FIRST_NM,
MBR_D.MBR_MIDINIT,
MBR_D.MBR_HOME_ADDR_LN_1,
MBR_D.MBR_HOME_ADDR_LN_2,
MBR_D.MBR_HOME_ADDR_CITY_NM,
MBR_D.MBR_HOME_ADDR_ST_CD,
MBR_D.MBR_HOME_ADDR_ZIP_CD_5,
MBR_D.MBR_HOME_ADDR_ZIP_CD_4,
MBR_D.MBR_BRTH_DT_SK,
MBR_D.MBR_RELSHP_CD_SK
From {EDWOwner}.MBR_D as MBR_D
INNER JOIN {EDWOwner}.MBR_ENR_D as MBR_ENR_D
        ON MBR_D.MBR_SK = MBR_ENR_D.MBR_SK
INNER JOIN {EDWOwner}.MBR_GNRL_LOC_HIST_D as HIST_D
        ON MBR_D.MBR_SK = HIST_D.MBR_SK
Where
MBR_D.SRC_SYS_CD = '{SrcSysCd}'
and MBR_ENR_D.MBR_ENR_ELIG_IN = 'Y'
and MBR_ENR_D.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'
and MBR_ENR_D.MBR_ENR_TERM_DT_SK >= '{Begin_Date}'
and MBR_ENR_D.MBR_ENR_EFF_DT_SK <= '{End_Date}'
and MBR_ENR_D.PROD_SH_NM <> 'BCARE'
and HIST_D.MBR_HOME_ADDR_ST_CD = 'MA'
and HIST_D.MBR_HOME_ADDR_ZIP_CD_5 < '10000'
and HIST_D.EDW_RCRD_END_DT_SK >=  '{Begin_Date}'
and MBR_ENR_D.GRP_ID not in ('10000000', '10001000', '10003000', '10004000', '10022000', '10023000', '10024000')
and SUBSTR(MBR_ENR_D.GRP_ID,1,2) <> '65'
and MBR_D.MBR_HOME_ADDR_ST_CD <> 'MA'
and MBR_D.MBR_HOME_ADDR_CITY_NM <> HIST_D.MBR_HOME_ADDR_CITY_NM
and HIST_D.MBR_HOME_ADDR_CITY_NM not in ('KANSAS CITY', 'LEES SUMMIT', 'ST CHARLES','LEES SUMMITT')
and MBR_ENR_D.GRP_ID IS NOT NULL
"""
df_EDWMassachusettsMbrs = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", query_EDWMassachusettsMbrs)
    .load()
)

query_EDW_MBR_RCSTData = f"""
SELECT 
MBR_SK, 
ACTVTY_YR_MO_SK, 
MBR_CT
FROM
(
  SELECT
    RCST.MBR_SK,
    RCST.ACTVTY_YR_MO_SK,
    SUM(RCST.MBR_CT) MBR_CT
  FROM {EDWOwner}.MBR_RCST_CT_F as RCST
  WHERE 
    RCST.SRC_SYS_CD = 'FACETS'
    and RCST.PROD_BILL_CMPNT_COV_CAT_CD = 'MED'
    and RCST.ACTVTY_YR_MO_SK >= '{Begin_CCYYMM}'
    and RCST.ACTVTY_YR_MO_SK <=  '{End_CCYYMM}'
  GROUP BY RCST.MBR_SK, RCST.ACTVTY_YR_MO_SK
)
WHERE
MBR_CT >  0.47
"""
df_EDW_MBR_RCSTData = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", query_EDW_MBR_RCSTData)
    .load()
)

df_EDWRcstDataOut = df_EDW_MBR_RCSTData.alias("EDWRcstDataOut")

df_EdwRcstJan = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "01").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_JAN")
)

df_EdwRcstFeb = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "02").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_FEB")
)

df_EdwRcstMar = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "03").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_MAR")
)

df_EdwRcstApr = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "04").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_APR")
)

df_EdwRcstMay = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "05").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_MAY")
)

df_EdwRcstJun = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "05").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_JUN")
)

df_EdwRcstJul = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "07").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_JUL")
)

df_EdwRcstAug = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "08").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_AUG")
)

df_EdwRcstSep = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "09").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_SEP")
)

df_EdwRcstOct = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "10").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_OCT")
)

df_EdwRcstNov = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "11").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_NOV")
)

df_EdwRcstDec = df_EDWRcstDataOut.filter(F.substring("ACTVTY_YR_MO_SK", 5, 2) == "12").select(
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("MBR_CT").alias("MBR_CT_DEC")
)

df_MbrsFromEDW = df_EDWMassachusettsMbrs.alias("MbrsFromEDW")

df_Jn_RcstData = (
    df_MbrsFromEDW.join(df_EdwRcstJan.alias("EdwRcstJan"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstFeb.alias("EdwRcstFeb"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstMar.alias("EdwRcstMar"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstApr.alias("EdwRcstApr"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstMay.alias("EdwRcstMay"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstJun.alias("EdwRcstJun"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstJul.alias("EdwRcstJul"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstAug.alias("EdwRcstAug"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstSep.alias("EdwRcstSep"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstOct.alias("EdwRcstOct"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstNov.alias("EdwRcstNov"), on=["MBR_SK"], how="left")
    .join(df_EdwRcstDec.alias("EdwRcstEdwRcstDec"), on=["MBR_SK"], how="left")
    .select(
        F.col("MbrsFromEDW.MBR_SK").alias("MBR_SK"),
        F.col("EdwRcstJan.MBR_CT_JAN").alias("MBR_CT_JAN"),
        F.col("EdwRcstFeb.MBR_CT_FEB").alias("MBR_CT_FEB"),
        F.col("EdwRcstMar.MBR_CT_MAR").alias("MBR_CT_MAR"),
        F.col("EdwRcstApr.MBR_CT_APR").alias("MBR_CT_APR"),
        F.col("EdwRcstMay.MBR_CT_MAY").alias("MBR_CT_MAY"),
        F.col("EdwRcstJun.MBR_CT_JUN").alias("MBR_CT_JUN"),
        F.col("EdwRcstJul.MBR_CT_JUL").alias("MBR_CT_JUL"),
        F.col("EdwRcstAug.MBR_CT_AUG").alias("MBR_CT_AUG"),
        F.col("EdwRcstSep.MBR_CT_SEP").alias("MBR_CT_SEP"),
        F.col("EdwRcstOct.MBR_CT_OCT").alias("MBR_CT_OCT"),
        F.col("EdwRcstNov.MBR_CT_NOV").alias("MBR_CT_NOV"),
        F.col("EdwRcstEdwRcstDec.MBR_CT_DEC").alias("MBR_CT_DEC"),
        F.col("MbrsFromEDW.MBR_ID").alias("MBR_ID"),
        F.col("MbrsFromEDW.SUB_ID").alias("SUB_ID"),
        F.col("MbrsFromEDW.MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("MbrsFromEDW.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("MbrsFromEDW.GRP_ID").alias("GRP_ID"),
        F.col("MbrsFromEDW.MBR_LAST_NM").alias("MBR_LAST_NM"),
        F.col("MbrsFromEDW.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        F.col("MbrsFromEDW.MBR_MIDINIT").alias("MBR_MIDINIT"),
        F.col("MbrsFromEDW.MBR_HOME_ADDR_LN_1").alias("MBR_HOME_ADDR_LN_1"),
        F.col("MbrsFromEDW.MBR_HOME_ADDR_LN_2").alias("MBR_HOME_ADDR_LN_2"),
        F.col("MbrsFromEDW.MBR_HOME_ADDR_CITY_NM").alias("MBR_HOME_ADDR_CITY_NM"),
        F.col("MbrsFromEDW.MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"),
        F.col("MbrsFromEDW.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        F.col("MbrsFromEDW.MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_HOME_ADDR_ZIP_CD_4"),
        F.col("MbrsFromEDW.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("MbrsFromEDW.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK")
    )
)

df_TrFormat_Mbr_Data_pre = df_Jn_RcstData.filter(
    (
        trim(
            F.when(F.col("MBR_LAST_NM").isNotNull(), F.col("MBR_LAST_NM")).otherwise("")
        ) != ""
    )
    | (
        trim(
            F.when(F.col("MBR_FIRST_NM").isNotNull(), F.col("MBR_FIRST_NM")).otherwise("")
        ) != ""
    )
)

df_TrFormat_Mbr_Data = df_TrFormat_Mbr_Data_pre.select(
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.lit(Tax_Year).alias("TAX_YR"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.lit(AsOfDtm).alias("AS_OF_DTM"),
    F.lit("NA").alias("GRP_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.lit("NA").alias("SUB_SK"),
    F.col("MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MBR_HOME_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    F.col("MBR_HOME_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    F.lit(None).alias("MBR_MAIL_ADDR_LN_3"),
    F.col("MBR_HOME_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.col("MBR_HOME_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    F.col("MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.when(F.col("MBR_CT_JAN") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("JAN_COV_IN"),
    F.when(F.col("MBR_CT_FEB") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("FEB_COV_IN"),
    F.when(F.col("MBR_CT_MAR") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("MAR_COV_IN"),
    F.when(F.col("MBR_CT_APR") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("APR_COV_IN"),
    F.when(F.col("MBR_CT_MAY") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("MAY_COV_IN"),
    F.when(F.col("MBR_CT_JUN") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("JUN_COV_IN"),
    F.when(F.col("MBR_CT_JUL") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("JUL_COV_IN"),
    F.when(F.col("MBR_CT_AUG") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("AUG_COV_IN"),
    F.when(F.col("MBR_CT_SEP") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("SEP_COV_IN"),
    F.when(F.col("MBR_CT_OCT") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("OCT_COV_IN"),
    F.when(F.col("MBR_CT_NOV") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("NOV_COV_IN"),
    F.when(F.col("MBR_CT_DEC") > 0.47, F.lit("Y")).otherwise(F.lit("N")).alias("DEC_COV_IN"),
    F.to_date(F.col("MBR_BRTH_DT_SK"), "yyyy-MM-dd").alias("MBR_BRTH_DT"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.when(F.col("MBR_SFX_NO") == "00", F.lit("M"))
     .when(F.col("MBR_SFX_NO") == "01", F.lit("H"))
     .when(F.col("MBR_SFX_NO") == "02", F.lit("W"))
     .when(F.col("MBR_SFX_NO") == "03", F.lit("S"))
     .when(F.col("MBR_SFX_NO") == "04", F.lit("D"))
     .otherwise(F.lit("O"))
     .alias("MBR_RELSHP_TYPE_NO")
)

df_MassMbrAllOut = df_TrFormat_Mbr_Data.alias("MassMbrAllOut")
df_lk_Sub = df_IDS_SUB_MA_DOR.alias("lk_Sub")

df_Lkp_Sub_joined = (
    df_MassMbrAllOut.join(
        df_lk_Sub,
        on=[
            df_MassMbrAllOut["SUB_ID"] == df_lk_Sub["SUB_ID"],
            df_MassMbrAllOut["GRP_ID"] == df_lk_Sub["GRP_ID"],
            df_MassMbrAllOut["TAX_YR"] == df_lk_Sub["TAX_YR"],
        ],
        how="inner",
    )
)

df_final = df_Lkp_Sub_joined.select(
    F.col("MassMbrAllOut.MBR_ID").alias("MBR_ID"),
    F.col("MassMbrAllOut.SUB_ID").alias("SUB_ID"),
    F.col("MassMbrAllOut.GRP_ID").alias("GRP_ID"),
    F.col("MassMbrAllOut.TAX_YR").alias("TAX_YR"),
    F.col("MassMbrAllOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MassMbrAllOut.AS_OF_DTM").alias("AS_OF_DTM"),
    F.col("MassMbrAllOut.GRP_SK").alias("GRP_SK"),
    F.col("MassMbrAllOut.MBR_SK").alias("MBR_SK"),
    F.col("lk_Sub.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("MassMbrAllOut.SUB_SK").alias("SUB_SK"),
    F.col("MassMbrAllOut.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("MassMbrAllOut.MBR_RELSHP_TYPE_NO").alias("MBR_RELSHP_TYPE_NO"),
    F.col("MassMbrAllOut.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("MassMbrAllOut.MBR_MIDINIT").alias("MBR_MIDINIT"),
    F.col("MassMbrAllOut.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MassMbrAllOut.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    F.col("MassMbrAllOut.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    F.col("MassMbrAllOut.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    F.col("MassMbrAllOut.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.col("MassMbrAllOut.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    F.col("MassMbrAllOut.MBR_MAIL_ADDR_ZIP_CD_5").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.col("MassMbrAllOut.MBR_MAIL_ADDR_ZIP_CD_4").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.col("MassMbrAllOut.JAN_COV_IN").alias("JAN_COV_IN"),
    F.col("MassMbrAllOut.FEB_COV_IN").alias("FEB_COV_IN"),
    F.col("MassMbrAllOut.MAR_COV_IN").alias("MAR_COV_IN"),
    F.col("MassMbrAllOut.APR_COV_IN").alias("APR_COV_IN"),
    F.col("MassMbrAllOut.MAY_COV_IN").alias("MAY_COV_IN"),
    F.col("MassMbrAllOut.JUN_COV_IN").alias("JUN_COV_IN"),
    F.col("MassMbrAllOut.JUL_COV_IN").alias("JUL_COV_IN"),
    F.col("MassMbrAllOut.AUG_COV_IN").alias("AUG_COV_IN"),
    F.col("MassMbrAllOut.SEP_COV_IN").alias("SEP_COV_IN"),
    F.col("MassMbrAllOut.OCT_COV_IN").alias("OCT_COV_IN"),
    F.col("MassMbrAllOut.NOV_COV_IN").alias("NOV_COV_IN"),
    F.col("MassMbrAllOut.DEC_COV_IN").alias("DEC_COV_IN"),
    F.col("MassMbrAllOut.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("MassMbrAllOut.MBR_SFX_NO").alias("MBR_SFX_NO")
)

df_to_write = df_final
df_to_write = df_to_write.withColumn("TAX_YR", F.rpad(F.col("TAX_YR"), 4, " "))
df_to_write = df_to_write.withColumn("MBR_MIDINIT", F.rpad(F.col("MBR_MIDINIT"), 1, " "))
df_to_write = df_to_write.withColumn("MBR_MAIL_ADDR_ZIP_CD_5", F.rpad(F.col("MBR_MAIL_ADDR_ZIP_CD_5"), 5, " "))
df_to_write = df_to_write.withColumn("MBR_MAIL_ADDR_ZIP_CD_4", F.rpad(F.col("MBR_MAIL_ADDR_ZIP_CD_4"), 4, " "))
df_to_write = df_to_write.withColumn("JAN_COV_IN", F.rpad(F.col("JAN_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("FEB_COV_IN", F.rpad(F.col("FEB_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("MAR_COV_IN", F.rpad(F.col("MAR_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("APR_COV_IN", F.rpad(F.col("APR_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("MAY_COV_IN", F.rpad(F.col("MAY_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("JUN_COV_IN", F.rpad(F.col("JUN_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("JUL_COV_IN", F.rpad(F.col("JUL_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("AUG_COV_IN", F.rpad(F.col("AUG_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("SEP_COV_IN", F.rpad(F.col("SEP_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("OCT_COV_IN", F.rpad(F.col("OCT_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("NOV_COV_IN", F.rpad(F.col("NOV_COV_IN"), 1, " "))
df_to_write = df_to_write.withColumn("DEC_COV_IN", F.rpad(F.col("DEC_COV_IN"), 1, " "))

write_files(
    df_to_write,
    f"{adls_path}/ds/MBR_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    '"',
    None
)