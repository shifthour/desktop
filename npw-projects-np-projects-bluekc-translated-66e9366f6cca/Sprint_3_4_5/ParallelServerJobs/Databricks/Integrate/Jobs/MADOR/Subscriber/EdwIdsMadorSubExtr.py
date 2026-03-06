# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY : 
# MAGIC 
# MAGIC PROCESSING : The job Extract Subscriber data from EDW tables.
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                 Date                 \(9)Project                                                Change Description                                               Development Project\(9)          Code Reviewer          Date Reviewed       
# MAGIC ------------------                               --------------------     \(9)------------------------                             -----------------------------------------------------------------------             ------------------------------\(9)         -------------------------------   ----------------------------       
# MAGIC Sethuraman Rajendran           \(9) 2018-04-17\(9)5839\(9)                                          Original Programming\(9)\(9)\(9)          IntegrateDev2                         Kalyan Neelam           2018-06-12
# MAGIC Abhiram Dasarathy\(9)\(9)2019-12-12\(9)F-114877\(9)\(9)Updated the join stage to use MbrsFromFACETS as driver\(9)          IntegrateDev2                         Jaideep Mankala       12/13/2019

# MAGIC Split the records in to multiple links according the month
# MAGIC Select recast data from EDW where
# MAGIC RCST.SRC_SYS_CD = 'FACETS'
# MAGIC Extract Subscriber data from EDW group table.
# MAGIC Subscriber data extract for EDW feed
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, when, lit, substring, to_date, rpad
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------

jdbc_url = None
jdbc_props = None

Tax_Year = get_widget_value('Tax_Year','')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
Begin_Date = get_widget_value('Begin_Date','')
End_Date = get_widget_value('End_Date','')
Begin_CCYYMM = get_widget_value('Begin_CCYYMM','')
End_CCYYMM = get_widget_value('End_CCYYMM','')
AsOfDtm = get_widget_value('AsOfDtm','')

# IDS_GRP_MA_DOR (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query_IDS_GRP_MA_DOR = f"Select GRP_ID, TAX_YR, GRP_MA_DOR_SK From {IDSOwner}.GRP_MA_DOR Where TAX_YR = '{Tax_Year}'"
df_IDS_GRP_MA_DOR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_IDS_GRP_MA_DOR)
    .load()
)

# EDWMassachusettsMbrs (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_EDWMassachusettsMbrs = (
    f"SELECT\n"
    f"MBR_D.MBR_SK,\n"
    f"MBR_D.MBR_ID,\n"
    f"MBR_D.SUB_ID,\n"
    f"MBR_D.MBR_SFX_NO,\n"
    f"MBR_D.SRC_SYS_CD,\n"
    f"MBR_ENR_D.GRP_ID,\n"
    f"MBR_D.MBR_LAST_NM,\n"
    f"MBR_D.MBR_FIRST_NM,\n"
    f"MBR_D.MBR_MIDINIT,\n"
    f"MBR_D.MBR_HOME_ADDR_LN_1,\n"
    f"MBR_D.MBR_HOME_ADDR_LN_2,\n"
    f"MBR_D.MBR_HOME_ADDR_CITY_NM,\n"
    f"MBR_D.MBR_HOME_ADDR_ST_CD,\n"
    f"MBR_D.MBR_HOME_ADDR_ZIP_CD_5,\n"
    f"MBR_D.MBR_HOME_ADDR_ZIP_CD_4,\n"
    f"MBR_D.MBR_BRTH_DT_SK,\n"
    f"MBR_D.MBR_RELSHP_CD\n"
    f"From \n"
    f"{EDWOwner}.MBR_D as MBR_D\n"
    f"INNER JOIN {EDWOwner}.MBR_ENR_D as MBR_ENR_D\n"
    f"        ON MBR_D.MBR_SK = MBR_ENR_D.MBR_SK\n"
    f"Where\n"
    f"MBR_D.SRC_SYS_CD = '{SrcSysCd}'\n"
    f"and MBR_ENR_D.MBR_ENR_ELIG_IN = 'Y'\n"
    f"and MBR_ENR_D.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'\n"
    f"and MBR_ENR_D.MBR_ENR_TERM_DT_SK >= '{Begin_Date}'\n"
    f"and MBR_ENR_D.MBR_ENR_EFF_DT_SK <= '{End_Date}'\n"
    f"and MBR_ENR_D.GRP_ID not in ('10000000', '10001000', '10003000', '10004000', '10022000', '10023000', '10024000')\n"
    f"and SUBSTR(MBR_ENR_D.GRP_ID,1,2) <> '65'\n"
    f"and MBR_ENR_D.PROD_SH_NM <> 'BCARE'\n"
    f"and MBR_D.MBR_HOME_ADDR_ST_CD = 'MA' \n"
    f"and MBR_D.MBR_HOME_ADDR_ZIP_CD_5 < '10000'\n"
    f"and MBR_D.MBR_HOME_ADDR_CITY_NM not in ('KANSAS CITY', 'LEES SUMMIT', 'ST CHARLES', 'LEES SUMMITT' )\n"
    f"and MBR_ENR_D.GRP_ID IS NOT NULL\n"
    f"UNION\n"
    f"SELECT\n"
    f"MBR_D.MBR_SK,\n"
    f"MBR_D.MBR_ID,\n"
    f"MBR_D.SUB_ID,\n"
    f"MBR_D.MBR_SFX_NO,\n"
    f"MBR_D.SRC_SYS_CD,\n"
    f"MBR_ENR_D.GRP_ID,\n"
    f"MBR_D.MBR_LAST_NM,\n"
    f"MBR_D.MBR_FIRST_NM,\n"
    f"MBR_D.MBR_MIDINIT,\n"
    f"MBR_D.MBR_HOME_ADDR_LN_1,\n"
    f"MBR_D.MBR_HOME_ADDR_LN_2,\n"
    f"MBR_D.MBR_HOME_ADDR_CITY_NM,\n"
    f"MBR_D.MBR_HOME_ADDR_ST_CD,\n"
    f"MBR_D.MBR_HOME_ADDR_ZIP_CD_5,\n"
    f"MBR_D.MBR_HOME_ADDR_ZIP_CD_4,\n"
    f"MBR_D.MBR_BRTH_DT_SK,\n"
    f"MBR_D.MBR_RELSHP_CD\n"
    f"From \n"
    f"{EDWOwner}.MBR_D as MBR_D\n"
    f"INNER JOIN {EDWOwner}.MBR_ENR_D as MBR_ENR_D\n"
    f"        ON MBR_D.MBR_SK = MBR_ENR_D.MBR_SK\n"
    f"INNER JOIN {EDWOwner}.MBR_GNRL_LOC_HIST_D as HIST_D\n"
    f"        ON MBR_D.MBR_SK = HIST_D.MBR_SK\n"
    f"Where\n"
    f"MBR_D.SRC_SYS_CD = '{SrcSysCd}'\n"
    f"and MBR_ENR_D.MBR_ENR_ELIG_IN = 'Y'\n"
    f"and MBR_ENR_D.MBR_ENR_CLS_PLN_PROD_CAT_CD = 'MED'\n"
    f"and MBR_ENR_D.MBR_ENR_TERM_DT_SK >= '{Begin_Date}'\n"
    f"and MBR_ENR_D.MBR_ENR_EFF_DT_SK <= '{End_Date}'\n"
    f"and MBR_ENR_D.PROD_SH_NM <> 'BCARE'\n"
    f"and HIST_D.MBR_HOME_ADDR_ST_CD = 'MA'\n"
    f"and HIST_D.MBR_HOME_ADDR_ZIP_CD_5 < '10000'\n"
    f"and HIST_D.EDW_RCRD_END_DT_SK >=  '{Begin_Date}'\n"
    f"and MBR_ENR_D.GRP_ID not in ('10000000', '10001000', '10003000', '10004000', '10022000', '10023000', '10024000')\n"
    f"and SUBSTR(MBR_ENR_D.GRP_ID,1,2) <> '65'\n"
    f"and MBR_D.MBR_HOME_ADDR_ST_CD <> 'MA' \n"
    f"and MBR_D.MBR_HOME_ADDR_CITY_NM <> HIST_D.MBR_HOME_ADDR_CITY_NM\n"
    f"and HIST_D.MBR_HOME_ADDR_CITY_NM not in ('KANSAS CITY', 'LEES SUMMIT', 'ST CHARLES','LEES SUMMITT')\n"
    f"and MBR_ENR_D.GRP_ID IS NOT NULL"
)
df_MbrsFromFACETS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EDWMassachusettsMbrs)
    .load()
)

# EDW_MBR_RCSTData (DB2ConnectorPX)
jdbc_url, jdbc_props = get_db_config(edw_secret_name)
extract_query_EDW_MBR_RCSTData = (
    f"SELECT \n"
    f"MBR_SK, \n"
    f"ACTVTY_YR_MO_SK, \n"
    f"MBR_CT \n"
    f"FROM\n"
    f"(SELECT\n"
    f"RCST.MBR_SK,\n"
    f"RCST. ACTVTY_YR_MO_SK,\n"
    f"SUM(RCST.MBR_CT) MBR_CT\n"
    f"FROM {EDWOwner}.MBR_RCST_CT_F as RCST\n"
    f"WHERE \n"
    f"RCST.SRC_SYS_CD = 'FACETS'\n"
    f"and RCST.PROD_BILL_CMPNT_COV_CAT_CD = 'MED'\n"
    f"and RCST.ACTVTY_YR_MO_SK >= '{Begin_CCYYMM}'\n"
    f"and RCST.ACTVTY_YR_MO_SK <=  '{End_CCYYMM}'\n"
    f"GROUP BY RCST.MBR_SK, RCST. ACTVTY_YR_MO_SK)\n"
    f"WHERE\n"
    f"MBR_CT >  0.47"
)
df_EDWRcstDataOut = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_EDW_MBR_RCSTData)
    .load()
)

# TrMapRcstData (CTransformerStage): multiple output links
df_EdwRcstJan = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '01').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_JAN")
)
df_EdwRcstFeb = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '02').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_FEB")
)
df_EdwRcstMar = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '03').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_MAR")
)
df_EdwRcstApr = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '04').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_APR")
)
df_EdwRcstMay = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '05').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_MAY")
)
df_EdwRcstJun = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '05').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_JUN")
)  # constraint repeated as per the job definition
df_EdwRcstJul = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '07').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_JUL")
)
df_EdwRcstAug = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '08').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_AUG")
)
df_EdwRcstSep = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '09').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_SEP")
)
df_EdwRcstOct = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '10').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_OCT")
)
df_EdwRcstNov = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '11').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_NOV")
)
df_EdwRcstEdwRcstDec = df_EDWRcstDataOut.filter(substring(col("ACTVTY_YR_MO_SK"), 5, 2) == '12').select(
    col("MBR_SK").alias("MBR_SK"),
    col("MBR_CT").alias("MBR_CT_DEC")
)

# Jn_RcstData (PxJoin with operator=leftouterjoin, key=MBR_SK)
df_Jn_RcstData = (
    df_MbrsFromFACETS.alias("MbrsFromFACETS")
    .join(df_EdwRcstJan.alias("EdwRcstJan"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstJan.MBR_SK"), "left")
    .join(df_EdwRcstFeb.alias("EdwRcstFeb"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstFeb.MBR_SK"), "left")
    .join(df_EdwRcstMar.alias("EdwRcstMar"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstMar.MBR_SK"), "left")
    .join(df_EdwRcstApr.alias("EdwRcstApr"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstApr.MBR_SK"), "left")
    .join(df_EdwRcstMay.alias("EdwRcstMay"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstMay.MBR_SK"), "left")
    .join(df_EdwRcstJun.alias("EdwRcstJun"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstJun.MBR_SK"), "left")
    .join(df_EdwRcstJul.alias("EdwRcstJul"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstJul.MBR_SK"), "left")
    .join(df_EdwRcstAug.alias("EdwRcstAug"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstAug.MBR_SK"), "left")
    .join(df_EdwRcstSep.alias("EdwRcstSep"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstSep.MBR_SK"), "left")
    .join(df_EdwRcstOct.alias("EdwRcstOct"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstOct.MBR_SK"), "left")
    .join(df_EdwRcstNov.alias("EdwRcstNov"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstNov.MBR_SK"), "left")
    .join(df_EdwRcstEdwRcstDec.alias("EdwRcstEdwRcstDec"), col("MbrsFromFACETS.MBR_SK") == col("EdwRcstEdwRcstDec.MBR_SK"), "left")
    .select(
        col("MbrsFromFACETS.MBR_SK").alias("MBR_SK"),
        col("EdwRcstJan.MBR_CT_JAN").alias("MBR_CT_JAN"),
        col("EdwRcstFeb.MBR_CT_FEB").alias("MBR_CT_FEB"),
        col("EdwRcstMar.MBR_CT_MAR").alias("MBR_CT_MAR"),
        col("EdwRcstApr.MBR_CT_APR").alias("MBR_CT_APR"),
        col("EdwRcstMay.MBR_CT_MAY").alias("MBR_CT_MAY"),
        col("EdwRcstJun.MBR_CT_JUN").alias("MBR_CT_JUN"),
        col("EdwRcstJul.MBR_CT_JUL").alias("MBR_CT_JUL"),
        col("EdwRcstAug.MBR_CT_AUG").alias("MBR_CT_AUG"),
        col("EdwRcstSep.MBR_CT_SEP").alias("MBR_CT_SEP"),
        col("EdwRcstOct.MBR_CT_OCT").alias("MBR_CT_OCT"),
        col("EdwRcstNov.MBR_CT_NOV").alias("MBR_CT_NOV"),
        col("EdwRcstEdwRcstDec.MBR_CT_DEC").alias("MBR_CT_DEC"),
        col("MbrsFromFACETS.MBR_ID").alias("MBR_ID"),
        col("MbrsFromFACETS.SUB_ID").alias("SUB_ID"),
        col("MbrsFromFACETS.MBR_SFX_NO").alias("MBR_SFX_NO"),
        col("MbrsFromFACETS.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("MbrsFromFACETS.GRP_ID").alias("GRP_ID"),
        col("MbrsFromFACETS.MBR_LAST_NM").alias("MBR_LAST_NM"),
        col("MbrsFromFACETS.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
        col("MbrsFromFACETS.MBR_MIDINIT").alias("MBR_MIDINIT"),
        col("MbrsFromFACETS.MBR_HOME_ADDR_LN_1").alias("MBR_HOME_ADDR_LN_1"),
        col("MbrsFromFACETS.MBR_HOME_ADDR_LN_2").alias("MBR_HOME_ADDR_LN_2"),
        col("MbrsFromFACETS.MBR_HOME_ADDR_CITY_NM").alias("MBR_HOME_ADDR_CITY_NM"),
        col("MbrsFromFACETS.MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"),
        col("MbrsFromFACETS.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        col("MbrsFromFACETS.MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_HOME_ADDR_ZIP_CD_4"),
        col("MbrsFromFACETS.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        col("MbrsFromFACETS.MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
    )
)

# Tr_Format_Grp_Data (CTransformerStage)
df_MassSubAllOut = (
    df_Jn_RcstData
    .filter(
        (col("MBR_RELSHP_CD") == 'SUB') | (col("MBR_SFX_NO") == '00')
    )
    .select(
        col("SUB_ID").alias("SUB_ID"),
        col("GRP_ID").alias("GRP_ID"),
        lit(Tax_Year).alias("TAX_YR"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        lit(AsOfDtm).alias("AS_OF_DTM"),
        lit("NA").alias("GRP_SK"),
        lit("NA").alias("SUB_SK"),
        col("MBR_FIRST_NM").alias("SUB_FIRST_NM"),
        col("MBR_MIDINIT").alias("SUB_MIDINIT"),
        col("MBR_LAST_NM").alias("SUB_LAST_NM"),
        col("MBR_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
        col("MBR_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
        lit(None).alias("SUB_HOME_ADDR_LN_3"),
        col("MBR_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
        col("MBR_HOME_ADDR_ST_CD").alias("SUB_ST_CD"),
        col("MBR_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
        col("MBR_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
        col("MBR_HOME_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
        col("MBR_HOME_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
        lit(None).alias("SUB_MAIL_ADDR_LN_3"),
        col("MBR_HOME_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
        col("MBR_HOME_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
        col("MBR_HOME_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        col("MBR_HOME_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
        when(col("MBR_CT_JAN") > 0.47, 'Y').otherwise('N').alias("JAN_COV_IN"),
        when(col("MBR_CT_FEB") > 0.47, 'Y').otherwise('N').alias("FEB_COV_IN"),
        when(col("MBR_CT_MAR") > 0.47, 'Y').otherwise('N').alias("MAR_COV_IN"),
        when(col("MBR_CT_APR") > 0.47, 'Y').otherwise('N').alias("APR_COV_IN"),
        when(col("MBR_CT_MAY") > 0.47, 'Y').otherwise('N').alias("MAY_COV_IN"),
        when(col("MBR_CT_JUN") > 0.47, 'Y').otherwise('N').alias("JUN_COV_IN"),
        when(col("MBR_CT_JUL") > 0.47, 'Y').otherwise('N').alias("JUL_COV_IN"),
        when(col("MBR_CT_AUG") > 0.47, 'Y').otherwise('N').alias("AUG_COV_IN"),
        when(col("MBR_CT_SEP") > 0.47, 'Y').otherwise('N').alias("SEP_COV_IN"),
        when(col("MBR_CT_OCT") > 0.47, 'Y').otherwise('N').alias("OCT_COV_IN"),
        when(col("MBR_CT_NOV") > 0.47, 'Y').otherwise('N').alias("NOV_COV_IN"),
        when(col("MBR_CT_DEC") > 0.47, 'Y').otherwise('N').alias("DEC_COV_IN"),
        to_date(col("MBR_BRTH_DT_SK"), "yyyy-MM-dd").alias("SUB_BRTH_DT"),
        col("MBR_SFX_NO").alias("MBR_SFX_NO")
    )
)

# Lkp_Grp (PxLookup, inner join)
df_Lkp_Grp = (
    df_MassSubAllOut.alias("MassSubAllOut")
    .join(
        df_IDS_GRP_MA_DOR.alias("lnk_GRP"),
        [
            col("MassSubAllOut.GRP_ID") == col("lnk_GRP.GRP_ID"),
            col("MassSubAllOut.TAX_YR") == col("lnk_GRP.TAX_YR")
        ],
        "inner"
    )
    .select(
        col("lnk_GRP.GRP_MA_DOR_SK").alias("GRP_MA_DOR_SK"),
        col("MassSubAllOut.SUB_ID").alias("SUB_ID"),
        col("MassSubAllOut.GRP_ID").alias("GRP_ID"),
        col("MassSubAllOut.TAX_YR").alias("TAX_YR"),
        col("MassSubAllOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("MassSubAllOut.AS_OF_DTM").alias("AS_OF_DTM"),
        col("MassSubAllOut.GRP_SK").alias("GRP_SK"),
        col("MassSubAllOut.SUB_SK").alias("SUB_SK"),
        col("MassSubAllOut.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
        col("MassSubAllOut.SUB_MIDINIT").alias("SUB_MIDINIT"),
        col("MassSubAllOut.SUB_LAST_NM").alias("SUB_LAST_NM"),
        col("MassSubAllOut.SUB_HOME_ADDR_LN_1").alias("SUB_HOME_ADDR_LN_1"),
        col("MassSubAllOut.SUB_HOME_ADDR_LN_2").alias("SUB_HOME_ADDR_LN_2"),
        col("MassSubAllOut.SUB_HOME_ADDR_LN_3").alias("SUB_HOME_ADDR_LN_3"),
        col("MassSubAllOut.SUB_HOME_ADDR_CITY_NM").alias("SUB_HOME_ADDR_CITY_NM"),
        col("MassSubAllOut.SUB_ST_CD").alias("SUB_HOME_ADDR_ST_CD"),
        col("MassSubAllOut.SUB_HOME_ADDR_ZIP_CD_5").alias("SUB_HOME_ADDR_ZIP_CD_5"),
        col("MassSubAllOut.SUB_HOME_ADDR_ZIP_CD_4").alias("SUB_HOME_ADDR_ZIP_CD_4"),
        col("MassSubAllOut.SUB_MAIL_ADDR_LN_1").alias("SUB_MAIL_ADDR_LN_1"),
        col("MassSubAllOut.SUB_MAIL_ADDR_LN_2").alias("SUB_MAIL_ADDR_LN_2"),
        col("MassSubAllOut.SUB_MAIL_ADDR_LN_3").alias("SUB_MAIL_ADDR_LN_3"),
        col("MassSubAllOut.SUB_MAIL_ADDR_CITY_NM").alias("SUB_MAIL_ADDR_CITY_NM"),
        col("MassSubAllOut.SUB_MAIL_ADDR_ST_CD").alias("SUB_MAIL_ADDR_ST_CD"),
        col("MassSubAllOut.SUB_MAIL_ADDR_ZIP_CD_5").alias("SUB_MAIL_ADDR_ZIP_CD_5"),
        col("MassSubAllOut.SUB_MAIL_ADDR_ZIP_CD_4").alias("SUB_MAIL_ADDR_ZIP_CD_4"),
        col("MassSubAllOut.JAN_COV_IN").alias("JAN_COV_IN"),
        col("MassSubAllOut.FEB_COV_IN").alias("FEB_COV_IN"),
        col("MassSubAllOut.MAR_COV_IN").alias("MAR_COV_IN"),
        col("MassSubAllOut.APR_COV_IN").alias("APR_COV_IN"),
        col("MassSubAllOut.MAY_COV_IN").alias("MAY_COV_IN"),
        col("MassSubAllOut.JUN_COV_IN").alias("JUN_COV_IN"),
        col("MassSubAllOut.JUL_COV_IN").alias("JUL_COV_IN"),
        col("MassSubAllOut.AUG_COV_IN").alias("AUG_COV_IN"),
        col("MassSubAllOut.SEP_COV_IN").alias("SEP_COV_IN"),
        col("MassSubAllOut.OCT_COV_IN").alias("OCT_COV_IN"),
        col("MassSubAllOut.NOV_COV_IN").alias("NOV_COV_IN"),
        col("MassSubAllOut.DEC_COV_IN").alias("DEC_COV_IN"),
        col("MassSubAllOut.SUB_BRTH_DT").alias("SUB_BRTH_DT"),
        col("MassSubAllOut.MBR_SFX_NO").alias("MBR_SFX_NO")
    )
)

# Subscriber_DS (PxDataSet) -> scenario C => write parquet via write_files
df_subscriber_final = df_Lkp_Grp.select(
    col("GRP_MA_DOR_SK"),
    rpad(col("SUB_ID"), 0, " ").alias("SUB_ID"),      # Not defined as char => no length known, keep as is (omitting rpad length).
    rpad(col("GRP_ID"), 0, " ").alias("GRP_ID"),      # No declared char length in the metadata.
    rpad(col("TAX_YR"), 4, " ").alias("TAX_YR"),      # char(4)
    rpad(col("SRC_SYS_CD"), 0, " ").alias("SRC_SYS_CD"),  # no declared length
    rpad(col("AS_OF_DTM"), 0, " ").alias("AS_OF_DTM"),    # no declared length
    rpad(col("GRP_SK"), 0, " ").alias("GRP_SK"),          # no declared length
    rpad(col("SUB_SK"), 0, " ").alias("SUB_SK"),          # no declared length
    rpad(col("SUB_FIRST_NM"), 0, " ").alias("SUB_FIRST_NM"),
    rpad(col("SUB_MIDINIT"), 1, " ").alias("SUB_MIDINIT"),  # char(1)
    rpad(col("SUB_LAST_NM"), 0, " ").alias("SUB_LAST_NM"),
    rpad(col("SUB_HOME_ADDR_LN_1"), 0, " ").alias("SUB_HOME_ADDR_LN_1"),
    rpad(col("SUB_HOME_ADDR_LN_2"), 0, " ").alias("SUB_HOME_ADDR_LN_2"),
    rpad(col("SUB_HOME_ADDR_LN_3"), 0, " ").alias("SUB_HOME_ADDR_LN_3"),
    rpad(col("SUB_HOME_ADDR_CITY_NM"), 0, " ").alias("SUB_HOME_ADDR_CITY_NM"),
    rpad(col("SUB_HOME_ADDR_ST_CD"), 0, " ").alias("SUB_HOME_ADDR_ST_CD"),
    rpad(col("SUB_HOME_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_HOME_ADDR_ZIP_CD_5"),  # char(5)
    rpad(col("SUB_HOME_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_HOME_ADDR_ZIP_CD_4"),  # char(4)
    rpad(col("SUB_MAIL_ADDR_LN_1"), 0, " ").alias("SUB_MAIL_ADDR_LN_1"),
    rpad(col("SUB_MAIL_ADDR_LN_2"), 0, " ").alias("SUB_MAIL_ADDR_LN_2"),
    rpad(col("SUB_MAIL_ADDR_LN_3"), 0, " ").alias("SUB_MAIL_ADDR_LN_3"),
    rpad(col("SUB_MAIL_ADDR_CITY_NM"), 0, " ").alias("SUB_MAIL_ADDR_CITY_NM"),
    rpad(col("SUB_MAIL_ADDR_ST_CD"), 0, " ").alias("SUB_MAIL_ADDR_ST_CD"),
    rpad(col("SUB_MAIL_ADDR_ZIP_CD_5"), 5, " ").alias("SUB_MAIL_ADDR_ZIP_CD_5"),  # char(5)
    rpad(col("SUB_MAIL_ADDR_ZIP_CD_4"), 4, " ").alias("SUB_MAIL_ADDR_ZIP_CD_4"),  # char(4)
    rpad(col("JAN_COV_IN"), 1, " ").alias("JAN_COV_IN"),  # char(1)
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
    rpad(col("MBR_SFX_NO"), 0, " ").alias("MBR_SFX_NO")
)

write_files(
    df_subscriber_final,
    f"{adls_path}/ds/SUB_MA_DOR_.{SrcSysCd}.extr.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_parquet=True,
    header=True,
    quote="\"",
    nullValue=None
)