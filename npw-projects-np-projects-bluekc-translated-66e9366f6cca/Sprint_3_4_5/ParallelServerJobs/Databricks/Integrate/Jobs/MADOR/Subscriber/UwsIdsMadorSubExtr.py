# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : 
# MAGIC 
# MAGIC PROCESSING : The job Extract Subscriber data from UWS subscriber table.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-04-17\(9)5839\(9)                                          Original Programming\(9)\(9)\(9)          IntegrateDev2                          Kalyan Neelam          2018-06-12

# MAGIC Extract Subscriber data from UWS subscriber table.
# MAGIC Subscriber data extract for UWS feed
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


Tax_Year = get_widget_value("Tax_Year", "2018")
RunID = get_widget_value("RunID", "100")
SrcSysCd = get_widget_value("SrcSysCd", "BCBSKC")
UWSOwner = get_widget_value("UWSOwner", "")
IDSOwner = get_widget_value("IDSOwner", "")
AsOfDtm = get_widget_value("AsOfDtm", "")
uws_secret_name = get_widget_value("uws_secret_name", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

# --------------------------------------------------------------------------------
# Stage: UWS_Sub_Data (ODBCConnectorPX)
query_UWS_Sub_Data = (
    f"SELECT SUB_ID, TAX_YR, SRC_SYS_CD, CRT_USER_ID, GRP_ID, SUB_FIRST_NM, "
    f"SUB_MIDINIT, SUB_LAST_NM, SUB_HOME_ADDR_LN_1, SUB_HOME_ADDR_LN_2, "
    f"SUB_HOME_ADDR_LN_3, SUB_HOME_ADDR_CITY_NM, SUB_HOME_ADDR_ST_CD, "
    f"SUB_HOME_ADDR_ZIP_CD_5, SUB_HOME_ADDR_ZIP_CD_4, SUB_MAIL_ADDR_LN_1, "
    f"SUB_MAIL_ADDR_LN_2, SUB_MAIL_ADDR_LN_3, SUB_MAIL_ADDR_CITY_NM, "
    f"SUB_MAIL_ADDR_ST_CD, SUB_MAIL_ADDR_ZIP_CD_5, SUB_MAIL_ADDR_ZIP_CD_4, "
    f"JAN_COV_IN, FEB_COV_IN, MAR_COV_IN, APR_COV_IN, MAY_COV_IN, JUN_COV_IN, "
    f"JUL_COV_IN, AUG_COV_IN, SEP_COV_IN, OCT_COV_IN, NOV_COV_IN, DEC_COV_IN, "
    f"SUB_BRTH_DT, MBR_SFX_NO "
    f"FROM {UWSOwner}.SUB_MA_DOR "
    f"WHERE TAX_YR = '{Tax_Year}'"
)
jdbc_url_UWS, jdbc_props_UWS = get_db_config(uws_secret_name)
df_UWS_Sub_Data = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_UWS)
    .options(**jdbc_props_UWS)
    .option("query", query_UWS_Sub_Data)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: IDS_SUB_MA_DOR (DB2ConnectorPX)
query_IDS_SUB_MA_DOR = (
    f"SELECT GRP.GRP_ID, GRP.TAX_YR, GRP.SRC_SYS_CD, GRP.GRP_MA_DOR_SK, GRP.AS_OF_DTM "
    f"FROM {IDSOwner}.GRP_MA_DOR GRP "
    f"WHERE GRP.TAX_YR = '{Tax_Year}'"
)
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
df_IDS_SUB_MA_DOR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_IDS_SUB_MA_DOR)
    .load()
)

# --------------------------------------------------------------------------------
# Stage: Lkp_Sub (PxLookup) - inner join
s = df_UWS_Sub_Data.alias("SubscriberInfoLookup")
l = df_IDS_SUB_MA_DOR.alias("lnk_SUB")
df_Lkp_Sub = (
    s.join(
        l,
        (s["GRP_ID"] == l["GRP_ID"]) & (s["TAX_YR"] == l["TAX_YR"]),
        "inner"
    )
    .select(
        l["GRP_MA_DOR_SK"].alias("GRP_MA_DOR_SK"),
        s["SUB_ID"].alias("SUB_ID"),
        s["TAX_YR"].alias("TAX_YR"),
        s["CRT_USER_ID"].alias("CRT_USER_ID"),
        l["AS_OF_DTM"].alias("AS_OF_DTM"),
        s["GRP_ID"].alias("GRP_ID"),
        s["SRC_SYS_CD"].alias("SRC_SYS_CD"),
        s["SUB_FIRST_NM"].alias("SUB_FIRST_NM"),
        s["SUB_MIDINIT"].alias("SUB_MIDINIT"),
        s["SUB_LAST_NM"].alias("SUB_LAST_NM"),
        s["SUB_HOME_ADDR_LN_1"].alias("SUB_HOME_ADDR_LN_1"),
        s["SUB_HOME_ADDR_LN_2"].alias("SUB_HOME_ADDR_LN_2"),
        s["SUB_HOME_ADDR_LN_3"].alias("SUB_HOME_ADDR_LN_3"),
        s["SUB_HOME_ADDR_CITY_NM"].alias("SUB_HOME_ADDR_CITY_NM"),
        s["SUB_HOME_ADDR_ST_CD"].alias("SUB_HOME_ADDR_ST_CD"),
        s["SUB_HOME_ADDR_ZIP_CD_5"].alias("SUB_HOME_ADDR_ZIP_CD_5"),
        s["SUB_HOME_ADDR_ZIP_CD_4"].alias("SUB_HOME_ADDR_ZIP_CD_4"),
        s["SUB_MAIL_ADDR_LN_1"].alias("SUB_MAIL_ADDR_LN_1"),
        s["SUB_MAIL_ADDR_LN_2"].alias("SUB_MAIL_ADDR_LN_2"),
        s["SUB_MAIL_ADDR_LN_3"].alias("SUB_MAIL_ADDR_LN_3"),
        s["SUB_MAIL_ADDR_CITY_NM"].alias("SUB_MAIL_ADDR_CITY_NM"),
        s["SUB_MAIL_ADDR_ST_CD"].alias("SUB_MAIL_ADDR_ST_CD"),
        s["SUB_MAIL_ADDR_ZIP_CD_5"].alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        s["SUB_MAIL_ADDR_ZIP_CD_4"].alias("SUB_MAIL_ADDR_ZIP_CD_4"),
        s["JAN_COV_IN"].alias("JAN_COV_IN"),
        s["FEB_COV_IN"].alias("FEB_COV_IN"),
        s["MAR_COV_IN"].alias("MAR_COV_IN"),
        s["APR_COV_IN"].alias("APR_COV_IN"),
        s["MAY_COV_IN"].alias("MAY_COV_IN"),
        s["JUN_COV_IN"].alias("JUN_COV_IN"),
        s["JUL_COV_IN"].alias("JUL_COV_IN"),
        s["AUG_COV_IN"].alias("AUG_COV_IN"),
        s["SEP_COV_IN"].alias("SEP_COV_IN"),
        s["OCT_COV_IN"].alias("OCT_COV_IN"),
        s["NOV_COV_IN"].alias("NOV_COV_IN"),
        s["DEC_COV_IN"].alias("DEC_COV_IN"),
        s["SUB_BRTH_DT"].alias("SUB_BRTH_DT"),
        s["MBR_SFX_NO"].alias("MBR_SFX_NO"),
    )
)

# --------------------------------------------------------------------------------
# Stage: Tr_Format_Sub_Data (CTransformerStage)
df_Tr_Format_Sub_Data = df_Lkp_Sub.select(
    when(col("GRP_MA_DOR_SK") == 0, lit(1)).otherwise(col("GRP_MA_DOR_SK")).alias("GRP_MA_DOR_SK"),
    col("SUB_ID").alias("SUB_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("TAX_YR").alias("TAX_YR"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    lit(AsOfDtm).alias("AS_OF_DTM"),
    lit("NA").alias("GRP_SK"),
    lit("NA").alias("SUB_SK"),
    col("SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    col("SUB_MIDINIT").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM").alias("SUB_LAST_NM"),
    col("SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
    col("SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
    col("SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
    col("SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
    col("SUB_HOME_ADDR_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
    col("SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    col("SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    col("SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
    col("SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
    col("SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
    col("SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
    col("SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
    col("SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    col("SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
    col("JAN_COV_IN").alias("JAN_COV_IN"),
    col("FEB_COV_IN").alias("FEB_COV_IN"),
    col("MAR_COV_IN").alias("MAR_COV_IN"),
    col("APR_COV_IN").alias("APR_COV_IN"),
    col("MAY_COV_IN").alias("MAY_COV_IN"),
    col("JUN_COV_IN").alias("JUN_COV_IN"),
    col("JUL_COV_IN").alias("JUL_COV_IN"),
    col("AUG_COV_IN").alias("AUG_COV_IN"),
    col("SEP_COV_IN").alias("SEP_COV_IN"),
    col("OCT_COV_IN").alias("OCT_COV_IN"),
    col("NOV_COV_IN").alias("NOV_COV_IN"),
    col("DEC_COV_IN").alias("DEC_COV_IN"),
    TimestampToDate(col("SUB_BRTH_DT")).alias("SUB_BRTH_DT"),
    when(col("MBR_SFX_NO").cast("double").isNotNull(), col("MBR_SFX_NO")).otherwise(lit("0")).alias("MBR_SFX_NO")
)

# --------------------------------------------------------------------------------
# Stage: Subscriber_DS (PxDataSet) => translate to parquet
df_final = df_Tr_Format_Sub_Data.select(
    col("GRP_MA_DOR_SK"),
    col("SUB_ID"),
    col("GRP_ID"),
    rpad(col("TAX_YR"), 4, " ").alias("TAX_YR"),
    col("SRC_SYS_CD"),
    col("AS_OF_DTM"),
    col("GRP_SK"),
    col("SUB_SK"),
    col("SUB_FIRST_NM"),
    rpad(col("SUB_MIDINIT"), 1, " ").alias("SUB_MIDINIT"),
    col("SUB_LAST_NM"),
    col("SUB_HOME_ADDR_LN_1"),
    col("SUB_HOME_ADDR_LN_2"),
    col("SUB_HOME_ADDR_LN_3"),
    col("SUB_HOME_ADDR_CITY_NM"),
    col("SUB_HOME_ADDR_ST_CD"),
    rpad(col("SUB_HOME_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_HOME_ADDR_ZIP_CD_5"),
    rpad(col("SUB_HOME_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_HOME_ADDR_ZIP_CD_4"),
    col("SUB_MAIL_ADDR_LN_1"),
    col("SUB_MAIL_ADDR_LN_2"),
    col("SUB_MAIL_ADDR_LN_3"),
    col("SUB_MAIL_ADDR_CITY_NM"),
    col("SUB_MAIL_ADDR_ST_CD"),
    rpad(col("SUB_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
    rpad(col("SUB_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
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
    col("SUB_BRTH_DT"),
    col("MBR_SFX_NO")
)

file_path = f"{adls_path}/ds/SUB_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet"
write_files(
    df_final,
    file_path,
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)