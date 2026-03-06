# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Brent Leland 08/10/2005    -   Originally Programmed
# MAGIC               SAndrew       12/14/2005   -    Project 1460.   Table / outfile file name changed from MBR_GNRL_LOC_D  to   MBR_GNRL_LOC_HIST_D
# MAGIC               Suzanne Saylor  04/12/2006 - changed > #BeginCycle# to >=, added RunCycle parameter, set LAST_RUN_CYC_EXCTN_SK to RunCycle
# MAGIC               Sharon Andrew   07/06/2006    EDW Membership changed extractions to be pulled from W_MBR_DEL table.   Last Activity Run Cycle is not always indexed on all tables.  Helps expediate extractions.
# MAGIC                                                                  Seperated all inner joins to be separate extractions.
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC Balkarn Gill               07/15/2013        5114                              Create Load File for EDW Table MBR_GNRL_LOC_HIST_D              EnterpriseWhseDevl  Peter Marshall              9/19/2013
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela     01/22/2014        Added EDWRunCycle to CRT_RUN_CYC_EXCTN_SK for new record Link.                          EnterpriseWhseDevl        Jag Yelavarthi            2014-01-23  
# MAGIC 
# MAGIC Jag Yelavarthi         2014-02-28         #5345                               Added NA and UNK rows creation process as part of extract            EnterpriseWhseDevl     Bhoomi Dasari           2/28/2014

# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Job Name:IdsEdwMbrGnrlLocHistDExtr
# MAGIC Read from source table MBR from IDS.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC SUB_ADDR_ST_CD_SK
# MAGIC Write MBR_GNRL_LOC_HIST_D Data into a Sequential file for Load Job IdsEdwMbrGnrlLocHistDLoad.
# MAGIC Add EDWRunCycle  and Null Handling and ZipCd.
# MAGIC 
# MAGIC Add EDWRunCycle
# MAGIC Funnel coulmns for Update and Insert
# MAGIC Filter NA and UNK rows from Update, Notcurrent, New and Current links.
# MAGIC 
# MAGIC NA and UNK rows will be populated as their own links
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, upper, substring, length, rpad
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

query_db2_MBR_GNRL_LOC_HIST_D_Read = f"""
SELECT 
MBR_SK,
EDW_RCRD_STRT_DT_SK,
SRC_SYS_CD,
MBR_UNIQ_KEY,
CRT_RUN_CYC_EXCTN_DT_SK,
LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
EDW_CUR_RCRD_IN,
EDW_RCRD_END_DT_SK,
SUB_SK,
MBR_HOME_ADDR_CITY_NM,
MBR_HOME_ADDR_ST_CD,
MBR_HOME_ADDR_ZIP_CD_5,
MBR_HOME_ADDR_ZIP_CD_4,
MBR_HOME_ADDR_CNTY_NM,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_HOME_ADDR_ST_CD_SK
FROM {EDWOwner}.MBR_GNRL_LOC_HIST_D
WHERE EDW_CUR_RCRD_IN = 'Y' AND MBR_SK NOT IN (1,0)
"""

df_db2_MBR_GNRL_LOC_HIST_D_Read = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", query_db2_MBR_GNRL_LOC_HIST_D_Read)
    .load()
)

query_db2_CD_MPPNG_Extr = f"""
SELECT CD_MPPNG_SK, COALESCE(TRGT_CD,'UNK') TRGT_CD, COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CD_MPPNG_Extr)
    .load()
)

query_db2_MBR_in = f"""
SELECT
mbr.MBR_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
mbr.MBR_UNIQ_KEY,
mbr.SUB_SK,
mbr.CRT_RUN_CYC_EXCTN_SK,
mbr.LAST_UPDT_RUN_CYC_EXCTN_SK
FROM {IDSOwner}.MBR mbr
LEFT JOIN {IDSOwner}.CD_MPPNG CD
  ON mbr.SRC_SYS_CD_SK = CD.CD_MPPNG_SK,
{IDSOwner}.W_MBR_DRVR DRVR
WHERE DRVR.KEY_VAL_INT = mbr.MBR_UNIQ_KEY
"""

df_db2_MBR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_MBR_in)
    .load()
)

query_db2_SUB_ADDR_Extr = f"""
SELECT distinct
(saddr.SUB_ADDR_SK),
saddr.SUB_UNIQ_KEY,
saddr.SRC_SYS_CD_SK,
saddr.SUB_SK,
saddr.CITY_NM,
saddr.POSTAL_CD,
saddr.SUB_ADDR_ST_CD_SK,
saddr.CNTY_NM
FROM {IDSOwner}.W_MBR_DRVR  DRVR,
     {IDSOwner}.SUB_ADDR    saddr
WHERE DRVR.KEY_VAL_INT     = saddr.SUB_UNIQ_KEY
  and DRVR.SRC_SYS_CD_SK   = saddr.SRC_SYS_CD_SK
"""

df_db2_SUB_ADDR_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_SUB_ADDR_Extr)
    .load()
)

query_db2_MBR_ADDR_TYP_Extr = f"""
SELECT distinct
maddr.MBR_SK,
maddr.MBR_UNIQ_KEY,
maddr.SRC_SYS_CD_SK,
maddr.CRT_RUN_CYC_EXCTN_SK,
maddr.LAST_UPDT_RUN_CYC_EXCTN_SK,
maddr.SUB_ADDR_SK
FROM {IDSOwner}.W_MBR_DRVR     DRVR,
     {IDSOwner}.MBR_ADDR_TYP  maddr,
     {IDSOwner}.CD_MPPNG      map
WHERE DRVR.KEY_VAL_INT                = maddr.MBR_UNIQ_KEY
  and DRVR.SRC_SYS_CD_SK             = maddr.SRC_SYS_CD_SK
  and maddr.MBR_ADDR_ROLE_TYP_CD_SK  = map.CD_MPPNG_SK
  and map.TRGT_CD                    = 'HOME'
"""

df_db2_MBR_ADDR_TYP_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_MBR_ADDR_TYP_Extr)
    .load()
)

df_MbrHomeAddr_lkp = (
    df_db2_MBR_ADDR_TYP_Extr.alias("lnk_mbr_addr_Lkp")
    .join(
        df_db2_SUB_ADDR_Extr.alias("Ref_sub_addr_Lkp"),
        col("lnk_mbr_addr_Lkp.SUB_ADDR_SK") == col("Ref_sub_addr_Lkp.SUB_ADDR_SK"),
        how="left"
    )
    .select(
        col("lnk_mbr_addr_Lkp.MBR_SK").alias("MBR_SK"),
        col("lnk_mbr_addr_Lkp.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_mbr_addr_Lkp.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("Ref_sub_addr_Lkp.SUB_SK").alias("SUB_SK"),
        col("Ref_sub_addr_Lkp.CITY_NM").alias("CITY_NM"),
        col("Ref_sub_addr_Lkp.POSTAL_CD").alias("POSTAL_CD"),
        col("Ref_sub_addr_Lkp.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
        col("Ref_sub_addr_Lkp.CNTY_NM").alias("CNTY_NM"),
        col("Ref_sub_addr_Lkp.SUB_ADDR_SK").alias("SUB_ADDR_SK"),
        col("lnk_mbr_addr_Lkp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK")
    )
)

df_xfm_NullHandling = (
    df_MbrHomeAddr_lkp
    .withColumn("SUB_SK", when(col("SUB_ADDR_SK").isNull(), lit(0)).otherwise(col("SUB_SK")))
    .withColumn("CITY_NM", when(col("SUB_ADDR_SK").isNull(), lit("UNK")).otherwise(col("CITY_NM")))
    .withColumn("POSTAL_CD", when(col("SUB_ADDR_SK").isNull(), lit(0)).otherwise(col("POSTAL_CD")))
    .withColumn("SUB_ADDR_ST_CD_SK", when(col("SUB_ADDR_SK").isNull(), lit("UNK")).otherwise(col("SUB_ADDR_ST_CD_SK")))
    .withColumn("CNTY_NM", when(col("SUB_ADDR_SK").isNull(), lit("UNK")).otherwise(col("CNTY_NM")))
)

df_lkp_Mbr = (
    df_db2_MBR_in.alias("lnk_IdsEdwMbrGnrlLocHistDExtr_InAbc")
    .join(
        df_xfm_NullHandling.alias("Ref_MBR_SK_Lkp"),
        col("lnk_IdsEdwMbrGnrlLocHistDExtr_InAbc.MBR_SK") == col("Ref_MBR_SK_Lkp.MBR_SK"),
        how="left"
    )
    .select(
        col("lnk_IdsEdwMbrGnrlLocHistDExtr_InAbc.MBR_SK").alias("MBR_SK"),
        col("lnk_IdsEdwMbrGnrlLocHistDExtr_InAbc.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwMbrGnrlLocHistDExtr_InAbc.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_IdsEdwMbrGnrlLocHistDExtr_InAbc.SUB_SK").alias("SUB_SK"),
        col("Ref_MBR_SK_Lkp.CITY_NM").alias("CITY_NM"),
        col("Ref_MBR_SK_Lkp.POSTAL_CD").alias("POSTAL_CD"),
        col("Ref_MBR_SK_Lkp.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
        col("Ref_MBR_SK_Lkp.CNTY_NM").alias("CNTY_NM"),
        col("Ref_MBR_SK_Lkp.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Ref_MBR_SK_Lkp.SUB_SK").alias("MBR_SUB_SK_1"),
        col("Ref_MBR_SK_Lkp.MBR_SK").alias("HOME_MBR_SK_1")
    )
)

df_lkp_Codes = (
    df_lkp_Mbr.alias("lnk_MbrLkpData_out")
    .join(
        df_db2_CD_MPPNG_Extr.alias("Ref_State_Lkp"),
        col("lnk_MbrLkpData_out.SUB_ADDR_ST_CD_SK") == col("Ref_State_Lkp.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        col("lnk_MbrLkpData_out.MBR_SK").alias("MBR_SK"),
        col("lnk_MbrLkpData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_MbrLkpData_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_MbrLkpData_out.SUB_SK").alias("SUB_SK"),
        col("lnk_MbrLkpData_out.CITY_NM").alias("CITY_NM"),
        col("lnk_MbrLkpData_out.POSTAL_CD").alias("POSTAL_CD"),
        col("Ref_State_Lkp.TRGT_CD").alias("MBR_HOME_ADDR_ST_CD"),
        col("lnk_MbrLkpData_out.CNTY_NM").alias("CNTY_NM"),
        col("lnk_MbrLkpData_out.SUB_ADDR_ST_CD_SK").alias("SUB_ADDR_ST_CD_SK"),
        col("lnk_MbrLkpData_out.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_MbrLkpData_out.MBR_SUB_SK_1").alias("MBR_SUB_SK_1"),
        col("Ref_State_Lkp.CD_MPPNG_SK").alias("CD_MPPNG_SK_1"),
        col("lnk_MbrLkpData_out.HOME_MBR_SK_1").alias("HOME_MBR_SK_1")
    )
)

df_xfm1_BusinessLogic = (
    df_lkp_Codes
    .withColumn(
        "svPostalCd1",
        when(
            col("POSTAL_CD").isNull() | (trim(col("POSTAL_CD")) == ""),
            lit("")
        ).otherwise(FORMAT_POSTALCD_EE(col("POSTAL_CD")))
    )
    .withColumn(
        "MBR_SK",
        col("MBR_SK")
    )
    .withColumn(
        "S_SRC_SYS_CD",
        when(trim(col("SRC_SYS_CD")) == "", lit("UNK")).otherwise(col("SRC_SYS_CD"))
    )
    .withColumn(
        "S_MBR_UNIQ_KEY",
        col("MBR_UNIQ_KEY")
    )
    .withColumn(
        "S_SUB_SK",
        col("SUB_SK")
    )
    .withColumn(
        "S_MBR_HOME_ADDR_CITY_NM",
        when(col("HOME_MBR_SK_1").isNull(), lit("UNK"))
        .otherwise(when(col("SUB_SK") != col("MBR_SUB_SK_1"), lit("UNK")).otherwise(col("CITY_NM")))
    )
    .withColumn(
        "S_MBR_HOME_ADDR_ST_CD",
        when(col("CD_MPPNG_SK_1").isNull(), lit("UNK"))
        .otherwise(when(col("SUB_SK") != col("MBR_SUB_SK_1"), lit("UNK")).otherwise(trim(col("MBR_HOME_ADDR_ST_CD"))))
    )
    .withColumn(
        "S_MBR_HOME_ADDR_ZIP_CD_5",
        when(length(trim(col("svPostalCd1"))) >= 5, substring(trim(col("svPostalCd1")), 1, 5)).otherwise(lit("     "))
    )
    .withColumn(
        "S_MBR_HOME_ADDR_ZIP_CD_4",
        when(length(trim(col("svPostalCd1"))) > 5, substring(trim(col("svPostalCd1")), 6, 4)).otherwise(lit("    "))
    )
    .withColumn(
        "S_MBR_HOME_ADDR_CNTY_NM",
        when(col("HOME_MBR_SK_1").isNull(), lit("UNK"))
        .otherwise(when(col("SUB_SK") != col("MBR_SUB_SK_1"), lit("UNK")).otherwise(col("CNTY_NM")))
    )
    .withColumn(
        "S_MBR_HOME_ADDR_ST_CD_SK",
        when(col("HOME_MBR_SK_1").isNull(), lit(0))
        .otherwise(when(col("SUB_SK") != col("MBR_SUB_SK_1"), lit(0)).otherwise(col("SUB_ADDR_ST_CD_SK")))
    )
    .withColumn("S_CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
)

df_Jn_MbrGnrl = (
    df_xfm1_BusinessLogic.alias("lnk_xfm_Out")
    .join(
        df_db2_MBR_GNRL_LOC_HIST_D_Read.alias("lnk_MBR_GNRL_LOC_HIST_D_In"),
        col("lnk_xfm_Out.MBR_SK") == col("lnk_MBR_GNRL_LOC_HIST_D_In.MBR_SK"),
        how="left"
    )
    .select(
        col("lnk_xfm_Out.MBR_SK").alias("MBR_SK"),
        col("lnk_xfm_Out.S_SRC_SYS_CD").alias("S_SRC_SYS_CD"),
        col("lnk_xfm_Out.S_MBR_UNIQ_KEY").alias("S_MBR_UNIQ_KEY"),
        col("lnk_xfm_Out.S_SUB_SK").alias("S_SUB_SK"),
        col("lnk_xfm_Out.S_MBR_HOME_ADDR_CITY_NM").alias("S_MBR_HOME_ADDR_CITY_NM"),
        col("lnk_xfm_Out.S_MBR_HOME_ADDR_ST_CD").alias("S_MBR_HOME_ADDR_ST_CD"),
        col("lnk_xfm_Out.S_MBR_HOME_ADDR_ZIP_CD_5").alias("S_MBR_HOME_ADDR_ZIP_CD_5"),
        col("lnk_xfm_Out.S_MBR_HOME_ADDR_ZIP_CD_4").alias("S_MBR_HOME_ADDR_ZIP_CD_4"),
        col("lnk_xfm_Out.S_MBR_HOME_ADDR_CNTY_NM").alias("S_MBR_HOME_ADDR_CNTY_NM"),
        col("lnk_xfm_Out.S_MBR_HOME_ADDR_ST_CD_SK").alias("S_MBR_HOME_ADDR_ST_CD_SK"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.EDW_CUR_RCRD_IN").alias("EDW_CUR_RCRD_IN"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.EDW_RCRD_END_DT_SK").alias("EDW_RCRD_END_DT_SK"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.SUB_SK").alias("SUB_SK"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.MBR_HOME_ADDR_CITY_NM").alias("MBR_HOME_ADDR_CITY_NM"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.MBR_HOME_ADDR_ST_CD").alias("MBR_HOME_ADDR_ST_CD"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_HOME_ADDR_ZIP_CD_4"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.MBR_HOME_ADDR_CNTY_NM").alias("MBR_HOME_ADDR_CNTY_NM"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_MBR_GNRL_LOC_HIST_D_In.MBR_HOME_ADDR_ST_CD_SK").alias("MBR_HOME_ADDR_ST_CD_SK"),
        col("lnk_xfm_Out.S_CRT_RUN_CYC_EXCTN_SK").alias("S_CRT_RUN_CYC_EXCTN_SK")
    )
)

df_xfm_BusinessLogic = (
    df_Jn_MbrGnrl
    .withColumn("CityNew", trim(upper(col("S_MBR_HOME_ADDR_CITY_NM"))) != trim(upper(col("MBR_HOME_ADDR_CITY_NM"))))
    .withColumn("CountyNew", trim(upper(col("S_MBR_HOME_ADDR_CNTY_NM"))) != trim(upper(col("MBR_HOME_ADDR_CNTY_NM"))))
    .withColumn("StateNew", trim(upper(col("S_MBR_HOME_ADDR_ST_CD"))) != trim(upper(col("MBR_HOME_ADDR_ST_CD"))))
    .withColumn("Zip5New", trim(col("S_MBR_HOME_ADDR_ZIP_CD_5")) != trim(col("MBR_HOME_ADDR_ZIP_CD_5")))
    .withColumn("Zip4New", trim(col("S_MBR_HOME_ADDR_ZIP_CD_4")) != trim(col("MBR_HOME_ADDR_ZIP_CD_4")))
    .withColumn(
        "RecordNew",
        (col("CityNew") | col("CountyNew") | col("StateNew") | col("Zip5New") | col("Zip4New"))
    )
)

df_lnk_New = (
    df_xfm_BusinessLogic
    .filter(
        (col("MBR_UNIQ_KEY").isNull())
        & (col("MBR_SK") != 0)
        & (col("MBR_SK") != 1)
    )
    .select(
        col("MBR_SK").alias("MBR_SK"),
        lit(EDWRunCycleDate).alias("EDW_RCRD_STRT_DT_SK"),
        col("S_SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("S_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit("Y").alias("EDW_CUR_RCRD_IN"),
        lit("2199-12-31").alias("EDW_RCRD_END_DT_SK"),
        col("S_SUB_SK").alias("SUB_SK"),
        upper(col("S_MBR_HOME_ADDR_CITY_NM")).alias("MBR_HOME_ADDR_CITY_NM"),
        upper(col("S_MBR_HOME_ADDR_ST_CD")).alias("MBR_HOME_ADDR_ST_CD"),
        col("S_MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        col("S_MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_HOME_ADDR_ZIP_CD_4"),
        upper(col("S_MBR_HOME_ADDR_CNTY_NM")).alias("MBR_HOME_ADDR_CNTY_NM"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("MBR_SK") == 1, lit(1)).otherwise(col("S_MBR_HOME_ADDR_ST_CD_SK")).alias("MBR_HOME_ADDR_ST_CD_SK")
    )
)

df_lnk_NotCurrent = (
    df_xfm_BusinessLogic
    .filter(
        (col("MBR_UNIQ_KEY").isNotNull())
        & (col("RecordNew") == True)
        & (col("MBR_SK") != 0)
        & (col("MBR_SK") != 1)
    )
    .select(
        col("MBR_SK").alias("MBR_SK"),
        col("EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
        upper(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit("N").alias("EDW_CUR_RCRD_IN"),
        when(col("MBR_SK") == 1, lit("2199-12-31"))
        .otherwise(FIND_DATE_EE(lit(EDWRunCycleDate), lit(-1), lit("D"), lit("X"), lit("CCYY-MM-DD")))
        .alias("EDW_RCRD_END_DT_SK"),
        col("SUB_SK").alias("SUB_SK"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("MBR_HOME_ADDR_CITY_NM"))).alias("MBR_HOME_ADDR_CITY_NM"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("MBR_HOME_ADDR_ST_CD"))).alias("MBR_HOME_ADDR_ST_CD"),
        col("MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        col("MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_HOME_ADDR_ZIP_CD_4"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("MBR_HOME_ADDR_CNTY_NM"))).alias("MBR_HOME_ADDR_CNTY_NM"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("MBR_SK") == 1, lit(1)).otherwise(col("MBR_HOME_ADDR_ST_CD_SK")).alias("MBR_HOME_ADDR_ST_CD_SK")
    )
)

df_lnk_Update = (
    df_xfm_BusinessLogic
    .filter(
        (col("MBR_UNIQ_KEY").isNotNull())
        & (col("RecordNew") == False)
        & (col("MBR_SK") != 0)
        & (col("MBR_SK") != 1)
    )
    .select(
        col("MBR_SK").alias("MBR_SK"),
        col("EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK"),
        col("S_SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("S_MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        col("EDW_CUR_RCRD_IN").alias("EDW_CUR_RCRD_IN"),
        col("EDW_RCRD_END_DT_SK").alias("EDW_RCRD_END_DT_SK"),
        col("S_SUB_SK").alias("SUB_SK"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("S_MBR_HOME_ADDR_CITY_NM"))).alias("MBR_HOME_ADDR_CITY_NM"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("S_MBR_HOME_ADDR_ST_CD"))).alias("MBR_HOME_ADDR_ST_CD"),
        col("S_MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        col("S_MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_HOME_ADDR_ZIP_CD_4"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("S_MBR_HOME_ADDR_CNTY_NM"))).alias("MBR_HOME_ADDR_CNTY_NM"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("MBR_SK") == 1, lit(1)).otherwise(col("S_MBR_HOME_ADDR_ST_CD_SK")).alias("MBR_HOME_ADDR_ST_CD_SK")
    )
)

df_lnk_Current = (
    df_xfm_BusinessLogic
    .filter(
        (col("MBR_UNIQ_KEY").isNotNull())
        & (col("RecordNew") == True)
        & (col("MBR_SK") != 0)
        & (col("MBR_SK") != 1)
    )
    .select(
        col("MBR_SK").alias("MBR_SK"),
        when(col("MBR_SK") == 1,
             col("EDW_RCRD_STRT_DT_SK")
            ).otherwise(lit(EDWRunCycleDate)
        ).alias("EDW_RCRD_STRT_DT_SK"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit("Y").alias("EDW_CUR_RCRD_IN"),
        lit("2199-12-31").alias("EDW_RCRD_END_DT_SK"),
        col("S_SUB_SK").alias("SUB_SK"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("S_MBR_HOME_ADDR_CITY_NM"))).alias("MBR_HOME_ADDR_CITY_NM"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("S_MBR_HOME_ADDR_ST_CD"))).alias("MBR_HOME_ADDR_ST_CD"),
        col("S_MBR_HOME_ADDR_ZIP_CD_5").alias("MBR_HOME_ADDR_ZIP_CD_5"),
        col("S_MBR_HOME_ADDR_ZIP_CD_4").alias("MBR_HOME_ADDR_ZIP_CD_4"),
        when(col("MBR_SK") == 1, lit("NA")).otherwise(upper(col("S_MBR_HOME_ADDR_CNTY_NM"))).alias("MBR_HOME_ADDR_CNTY_NM"),
        lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        when(col("MBR_SK") == 1, lit(1)).otherwise(col("S_MBR_HOME_ADDR_ST_CD_SK")).alias("MBR_HOME_ADDR_ST_CD_SK")
    )
)

df_lnk_NA_base = df_xfm_BusinessLogic.limit(1)
df_lnk_NA = (
    df_lnk_NA_base
    .select(
        lit(1).alias("MBR_SK"),
        lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
        lit("NA").alias("SRC_SYS_CD"),
        lit(1).alias("MBR_UNIQ_KEY"),
        lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit("Y").alias("EDW_CUR_RCRD_IN"),
        lit("1753-01-01").alias("EDW_RCRD_END_DT_SK"),
        lit(1).alias("SUB_SK"),
        lit(None).alias("MBR_HOME_ADDR_CITY_NM"),
        lit("NA").alias("MBR_HOME_ADDR_ST_CD"),
        lit(None).alias("MBR_HOME_ADDR_ZIP_CD_5"),
        lit(None).alias("MBR_HOME_ADDR_ZIP_CD_4"),
        lit(None).alias("MBR_HOME_ADDR_CNTY_NM"),
        lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("MBR_HOME_ADDR_ST_CD_SK")
    )
)

df_lnk_UNK_base = df_xfm_BusinessLogic.limit(1)
df_lnk_UNK = (
    df_lnk_UNK_base
    .select(
        lit(0).alias("MBR_SK"),
        lit("1753-01-01").alias("EDW_RCRD_STRT_DT_SK"),
        lit("UNK").alias("SRC_SYS_CD"),
        lit(0).alias("MBR_UNIQ_KEY"),
        lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit("Y").alias("EDW_CUR_RCRD_IN"),
        lit("1753-01-01").alias("EDW_RCRD_END_DT_SK"),
        lit(0).alias("SUB_SK"),
        lit(None).alias("MBR_HOME_ADDR_CITY_NM"),
        lit("UNK").alias("MBR_HOME_ADDR_ST_CD"),
        lit(None).alias("MBR_HOME_ADDR_ZIP_CD_5"),
        lit(None).alias("MBR_HOME_ADDR_ZIP_CD_4"),
        lit(None).alias("MBR_HOME_ADDR_CNTY_NM"),
        lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("MBR_HOME_ADDR_ST_CD_SK")
    )
)

df_Fnl_Update_Insert = (
    df_lnk_New
    .unionByName(df_lnk_NotCurrent)
    .unionByName(df_lnk_Current)
    .unionByName(df_lnk_Update)
    .unionByName(df_lnk_NA)
    .unionByName(df_lnk_UNK)
)

df_final = (
    df_Fnl_Update_Insert
    .withColumn("EDW_RCRD_STRT_DT_SK", rpad(col("EDW_RCRD_STRT_DT_SK"), 10, " "))
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("EDW_CUR_RCRD_IN", rpad(col("EDW_CUR_RCRD_IN"), 1, " "))
    .withColumn("EDW_RCRD_END_DT_SK", rpad(col("EDW_RCRD_END_DT_SK"), 10, " "))
    .withColumn("MBR_HOME_ADDR_ZIP_CD_5", rpad(col("MBR_HOME_ADDR_ZIP_CD_5"), 5, " "))
    .withColumn("MBR_HOME_ADDR_ZIP_CD_4", rpad(col("MBR_HOME_ADDR_ZIP_CD_4"), 4, " "))
)

write_files(
    df_final.select(
        "MBR_SK",
        "EDW_RCRD_STRT_DT_SK",
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "CRT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "EDW_CUR_RCRD_IN",
        "EDW_RCRD_END_DT_SK",
        "SUB_SK",
        "MBR_HOME_ADDR_CITY_NM",
        "MBR_HOME_ADDR_ST_CD",
        "MBR_HOME_ADDR_ZIP_CD_5",
        "MBR_HOME_ADDR_ZIP_CD_4",
        "MBR_HOME_ADDR_CNTY_NM",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "MBR_HOME_ADDR_ST_CD_SK"
    ),
    f"{adls_path}/load/MBR_GNRL_LOC_HIST_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)