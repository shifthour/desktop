# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : 
# MAGIC 
# MAGIC PROCESSING : The job Extract Member data from UWS member table.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-04-17\(9)5839\(9)                                          Original Programming\(9)\(9)\(9)          IntegrateDev2                          Kalyan Neelam          2018-06-12

# MAGIC Extract Member data from UWS Member table.
# MAGIC Member data extract for UWS feed
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Tax_Year = get_widget_value('Tax_Year','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
UWSOwner = get_widget_value('UWSOwner','')
IDSOwner = get_widget_value('IDSOwner','')
AsOfDtm = get_widget_value('AsOfDtm','')

uws_secret_name = get_widget_value('uws_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')

jdbc_url, jdbc_props = get_db_config(uws_secret_name)
extract_query = f"""
SELECT 
  MBR_ID,
  TAX_YR,
  SRC_SYS_CD,
  CRT_USER_ID,
  GRP_ID,
  SUB_ID,
  MBR_RELSHP_CD,
  MBR_FIRST_NM,
  MBR_MIDINIT,
  MBR_LAST_NM,
  MBR_MAIL_ADDR_LN_1,
  MBR_MAIL_ADDR_LN_2,
  MBR_MAIL_ADDR_LN_3,
  MBR_MAIL_ADDR_CITY_NM,
  MBR_MAIL_ADDR_ST_CD,
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
FROM {UWSOwner}.MBR_MA_DOR
WHERE TAX_YR = '{Tax_Year}'
"""
df_UWS_Member_Data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"""
SELECT
  SUB.GRP_ID,
  SUB.SUB_ID,
  SUB.TAX_YR,
  SUB.SRC_SYS_CD,
  SUB.SUB_MA_DOR_SK,
  SUB.AS_OF_DTM
FROM {IDSOwner}.GRP_MA_DOR grp
INNER JOIN {IDSOwner}.SUB_MA_DOR sub
  ON grp.GRP_ID = sub.GRP_ID
WHERE sub.TAX_YR = '{Tax_Year}'
"""
df_IDS_MBR_MA_DOR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Lkp_Mbr = df_UWS_Member_Data.alias("MbrsLookUpFromUWS").join(
    df_IDS_MBR_MA_DOR.alias("lnk_MBR"),
    (
        (F.col("MbrsLookUpFromUWS.GRP_ID") == F.col("lnk_MBR.GRP_ID")) &
        (F.col("MbrsLookUpFromUWS.TAX_YR") == F.col("lnk_MBR.TAX_YR")) &
        (F.col("MbrsLookUpFromUWS.SUB_ID") == F.col("lnk_MBR.SUB_ID"))
    ),
    how="inner"
).select(
    F.col("MbrsLookUpFromUWS.MBR_ID").alias("MBR_ID"),
    F.rpad(F.col("MbrsLookUpFromUWS.TAX_YR"), 4, " ").alias("TAX_YR"),
    F.col("MbrsLookUpFromUWS.CRT_USER_ID").alias("CRT_USER_ID"),
    F.col("MbrsLookUpFromUWS.GRP_ID").alias("GRP_ID"),
    F.col("MbrsLookUpFromUWS.SUB_ID").alias("SUB_ID"),
    F.col("MbrsLookUpFromUWS.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("MbrsLookUpFromUWS.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.rpad(F.col("MbrsLookUpFromUWS.MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    F.col("MbrsLookUpFromUWS.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("MbrsLookUpFromUWS.MBR_MAIL_ADDR_LN_1").alias("MBR_MAIL_ADDR_LN_1"),
    F.col("MbrsLookUpFromUWS.MBR_MAIL_ADDR_LN_2").alias("MBR_MAIL_ADDR_LN_2"),
    F.col("MbrsLookUpFromUWS.MBR_MAIL_ADDR_LN_3").alias("MBR_MAIL_ADDR_LN_3"),
    F.col("MbrsLookUpFromUWS.MBR_MAIL_ADDR_CITY_NM").alias("MBR_MAIL_ADDR_CITY_NM"),
    F.col("MbrsLookUpFromUWS.MBR_MAIL_ADDR_ST_CD").alias("MBR_MAIL_ADDR_ST_CD"),
    F.rpad(F.col("MbrsLookUpFromUWS.MBR_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.rpad(F.col("MbrsLookUpFromUWS.MBR_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.rpad(F.col("MbrsLookUpFromUWS.JAN_COV_IN"), 1, " ").alias("JAN_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.FEB_COV_IN"), 1, " ").alias("FEB_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.MAR_COV_IN"), 1, " ").alias("MAR_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.APR_COV_IN"), 1, " ").alias("APR_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.MAY_COV_IN"), 1, " ").alias("MAY_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.JUN_COV_IN"), 1, " ").alias("JUN_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.JUL_COV_IN"), 1, " ").alias("JUL_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.AUG_COV_IN"), 1, " ").alias("AUG_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.SEP_COV_IN"), 1, " ").alias("SEP_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.OCT_COV_IN"), 1, " ").alias("OCT_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.NOV_COV_IN"), 1, " ").alias("NOV_COV_IN"),
    F.rpad(F.col("MbrsLookUpFromUWS.DEC_COV_IN"), 1, " ").alias("DEC_COV_IN"),
    F.col("MbrsLookUpFromUWS.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("MbrsLookUpFromUWS.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MbrsLookUpFromUWS.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_MBR.SUB_MA_DOR_SK").alias("SUB_MA_DOR_SK"),
    F.col("lnk_MBR.AS_OF_DTM").alias("AS_OF_DTM")
)

df_Tr_Format_Mbr_Data = df_Lkp_Mbr
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn(
    "SUB_MA_DOR_SK",
    F.when(F.col("SUB_MA_DOR_SK") == 0, F.lit(1)).otherwise(F.col("SUB_MA_DOR_SK"))
)
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_ID", F.col("MBR_ID"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("SUB_ID", F.col("SUB_ID"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("GRP_ID", F.col("GRP_ID"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("TAX_YR", F.col("TAX_YR"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("SRC_SYS_CD", F.col("SRC_SYS_CD"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("AS_OF_DTM", F.lit(AsOfDtm))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("GRP_SK", F.lit("NA"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_SK", F.lit("NA"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("SUB_SK", F.lit("NA"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_RELSHP_CD_SK", F.lit(99))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn(
    "MBR_RELSHP_TYPE_NO",
    F.when((F.col("SRC_SYS_CD") == 'FACETS') & (F.col("MBR_SFX_NO") == '00'), 'M')
     .when((F.col("SRC_SYS_CD") == 'FACETS') & (F.col("MBR_SFX_NO") == '01'), 'H')
     .when((F.col("SRC_SYS_CD") == 'FACETS') & (F.col("MBR_SFX_NO") == '02'), 'W')
     .when((F.col("SRC_SYS_CD") == 'FACETS') & (F.col("MBR_SFX_NO") == '03'), 'S')
     .when((F.col("SRC_SYS_CD") == 'FACETS') & (F.col("MBR_SFX_NO") == '04'), 'D')
     .when(F.col("SRC_SYS_CD") == 'FACETS', 'O')
     .otherwise('0')
)
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_FIRST_NM", F.col("MBR_FIRST_NM"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_MIDINIT", F.col("MBR_MIDINIT"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_LAST_NM", F.col("MBR_LAST_NM"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_MAIL_ADDR_LN_1", F.col("MBR_MAIL_ADDR_LN_1"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_MAIL_ADDR_LN_2", F.col("MBR_MAIL_ADDR_LN_2"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_MAIL_ADDR_LN_3", F.col("MBR_MAIL_ADDR_LN_3"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_MAIL_ADDR_CITY_NM", F.col("MBR_MAIL_ADDR_CITY_NM"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_MAIL_ADDR_ST_CD", F.col("MBR_MAIL_ADDR_ST_CD"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_MAIL_ADDR_ZIP_CD_5", F.col("MBR_MAIL_ADDR_ZIP_CD_5"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_MAIL_ADDR_ZIP_CD_4", F.col("MBR_MAIL_ADDR_ZIP_CD_4"))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("JAN_COV_IN", UpCase(F.col("JAN_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("FEB_COV_IN", UpCase(F.col("FEB_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MAR_COV_IN", UpCase(F.col("MAR_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("APR_COV_IN", UpCase(F.col("APR_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MAY_COV_IN", UpCase(F.col("MAY_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("JUN_COV_IN", UpCase(F.col("JUN_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("JUL_COV_IN", UpCase(F.col("JUL_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("AUG_COV_IN", UpCase(F.col("AUG_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("SEP_COV_IN", UpCase(F.col("SEP_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("OCT_COV_IN", UpCase(F.col("OCT_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("NOV_COV_IN", UpCase(F.col("NOV_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("DEC_COV_IN", UpCase(F.col("DEC_COV_IN")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn("MBR_BRTH_DT", TimestampToDate(F.col("MBR_BRTH_DT")))
df_Tr_Format_Mbr_Data = df_Tr_Format_Mbr_Data.withColumn(
    "MBR_SFX_NO",
    F.when(Num(F.col("MBR_SFX_NO")) == 1, F.col("MBR_SFX_NO")).otherwise('0')
)

df_Member_DS = df_Tr_Format_Mbr_Data.select(
    F.col("SUB_MA_DOR_SK"),
    F.col("MBR_ID"),
    F.col("SUB_ID"),
    F.col("GRP_ID"),
    F.rpad(F.col("TAX_YR"), 4, " ").alias("TAX_YR"),
    F.col("SRC_SYS_CD"),
    F.col("AS_OF_DTM"),
    F.col("GRP_SK"),
    F.col("MBR_SK"),
    F.col("SUB_SK"),
    F.col("MBR_RELSHP_CD_SK"),
    F.col("MBR_RELSHP_TYPE_NO"),
    F.col("MBR_FIRST_NM"),
    F.rpad(F.col("MBR_MIDINIT"), 1, " ").alias("MBR_MIDINIT"),
    F.col("MBR_LAST_NM"),
    F.col("MBR_MAIL_ADDR_LN_1"),
    F.col("MBR_MAIL_ADDR_LN_2"),
    F.col("MBR_MAIL_ADDR_LN_3"),
    F.col("MBR_MAIL_ADDR_CITY_NM"),
    F.col("MBR_MAIL_ADDR_ST_CD"),
    F.rpad(F.col("MBR_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("MBR_MAIL_ADDR_ZIP_CD_5"),
    F.rpad(F.col("MBR_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("MBR_MAIL_ADDR_ZIP_CD_4"),
    F.rpad(F.col("JAN_COV_IN"), 1, " ").alias("JAN_COV_IN"),
    F.rpad(F.col("FEB_COV_IN"), 1, " ").alias("FEB_COV_IN"),
    F.rpad(F.col("MAR_COV_IN"), 1, " ").alias("MAR_COV_IN"),
    F.rpad(F.col("APR_COV_IN"), 1, " ").alias("APR_COV_IN"),
    F.rpad(F.col("MAY_COV_IN"), 1, " ").alias("MAY_COV_IN"),
    F.rpad(F.col("JUN_COV_IN"), 1, " ").alias("JUN_COV_IN"),
    F.rpad(F.col("JUL_COV_IN"), 1, " ").alias("JUL_COV_IN"),
    F.rpad(F.col("AUG_COV_IN"), 1, " ").alias("AUG_COV_IN"),
    F.rpad(F.col("SEP_COV_IN"), 1, " ").alias("SEP_COV_IN"),
    F.rpad(F.col("OCT_COV_IN"), 1, " ").alias("OCT_COV_IN"),
    F.rpad(F.col("NOV_COV_IN"), 1, " ").alias("NOV_COV_IN"),
    F.rpad(F.col("DEC_COV_IN"), 1, " ").alias("DEC_COV_IN"),
    F.col("MBR_BRTH_DT"),
    F.col("MBR_SFX_NO")
)

write_files(
    df_Member_DS,
    f"{adls_path}/ds/MBR_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)