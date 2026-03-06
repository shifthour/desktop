# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 - 2011 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:    Pulls provider address data from IDS.
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                 Development Project     Code Reviewer             Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------          ----------------------------------    ---------------------------------    -------------------------   
# MAGIC Ralph Tucker                  09/14/2005                                           Originally Programmed
# MAGIC Parikshith Chada             2008-09-09       3784(PBM)                    Added the new field PDX_24_HR_IN      devlEDWnew                Steph Goddard             09/10/2008
# MAGIC Ralph Tucker                  2009-07-02     3500 - Web Realign       Changed GeoCd Return code                   devlEDWnew                Steph Goddard             07/15/2009
# MAGIC Brent Leland                    2011-06-13     TTR 697                        Added columns PROV_PRI_ADDR_IN     EnterpriseCurDevl         SAndrew                       2011-06-22 
# MAGIC                                                                                                      and PROV_REMIT_ADDR_IN. 
# MAGIC                                                                                                      Removed data element entries from all 
# MAGIC                                                                                                      links. 
# MAGIC                                                                 TTR 387                      Set default to NULL from
# MAGIC                                                                                                            PROV_MAIL_ADDR_FAX_NO_EXT 
# MAGIC                                                                                                            PROV_MAIL_ADDR_ADDR_LN_2 
# MAGIC                                                                                                            PROV_MAIL_ADDR_ADDR_LN_3 
# MAGIC                                                                                                            PROV_MAIL_ADDR_ZIP_CD_4
# MAGIC                                                                                                            PROV_ADDR_ZIP_CD_4
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Modifications:                
# MAGIC Developer                    Date              Project/Altiris #                     Change Description                             Code Reviewer            Date Reviewed        
# MAGIC =================================================================================================================                                                                          
# MAGIC =================================================================================================================
# MAGIC 
# MAGIC Shiva Devagiri        007/09/2013       5114                         Server to Parallel Conversion                          Peter Marshall             9/3/2013
# MAGIC                                                                                               
# MAGIC 
# MAGIC 
# MAGIC Archana Palivela      01/22/2014       5114                      Mapped EdwRunCycle to                                  Jag Yelavarthi            2014-01-23
# MAGIC 
# MAGIC 
# MAGIC Kiran Mulakalapalli   10/28/2021     US468727              URL Downstream efforts in to EDW                   Jeyaprasanna             2021-11-02
# MAGIC                                                                                          CRT_RUN_CYC_EXCTN_SK 
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi  2024-10-08          US-628654          Modified Lookup SQL's LOC1 and LOC2               
# MAGIC                                                                                       added Condition l.PROV_ADDR_EFF_DT_SK   Reddy Sanam            2024-10-10
# MAGIC                                                                                       >= '#EDWRunCycleDate#'
# MAGIC 
# MAGIC Brent Leland          2024-11-13         US 624342          Modified db2_PROV_LOC_1_in and db2_PROV_LOC_2_in                      Jeyaprasanna                   2024-11-15
# MAGIC                                                                                      updated PROV_ADDR_EFF_DT_SK to use <= instead of >=

# MAGIC Write PROV_ADDR data into a file for  the IdsEdwProvAddrLoad Job.
# MAGIC Read all the Data from IDS PROV_ADDR table
# MAGIC Job Name: IdsEdwProvAddrDExtr
# MAGIC Add Defaults and Null Handling.
# MAGIC Code SK lookups for Denormalization
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC CD_MPPNG_SK
# MAGIC TGT_CD and TGT_CD_NM
# MAGIC Reference Code Mapping Data for Code SK lookups
# MAGIC Lookup for PRI ADDR IN and REMIT ADDR IN Mapping.
# MAGIC 
# MAGIC Lookup Keys:
# MAGIC 
# MAGIC SRC_SYS_CD_SK
# MAGIC PROV_ADDR_ID
# MAGIC PROV_ADDR_TYP_CD_SK
# MAGIC PROV_ADDR_EFF_DT_SK
# MAGIC Read the Data from IDS PROV_LOC Table
# MAGIC Copy for mail address lookup
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameter parsing
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

# Get DB config for IDS
jdbc_url, jdbc_props = get_db_config(ids_secret_name)

# Stage: db2_CD_MPPNG_in
extract_query_db2_CD_MPPNG_in = f"""
SELECT 
  CD_MPPNG_SK,
  COALESCE(TRGT_CD,'UNK') TRGT_CD,
  COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_lnk_CdMppng_out = df_db2_CD_MPPNG_in

# cpy_cd_mppng creates multiple references (same data, different link names)
df_ref_MailAddrTypCd = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_MetroRuralCovCd = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_TermRsnCd = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_MailCntyClsCd = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_MailTermRsnCd = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_StCd = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_AddrTypCd = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_CntyCls = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_SrcSys = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_MailAddrStCd = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)
df_ref_MetroMailRuralCovCd_Lkp = df_lnk_CdMppng_out.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# Stage: db2_PROV_LOC_1_in
extract_query_db2_PROV_LOC_1_in = f"""
SELECT DISTINCT
  l.SRC_SYS_CD_SK,
  l.PROV_ADDR_ID,
  l.PROV_ADDR_TYP_CD_SK,
  l.PROV_ADDR_EFF_DT_SK,
  l.PRI_ADDR_IN
FROM {IDSOwner}.PROV_LOC l
WHERE l.PRI_ADDR_IN = 'Y'
  AND l.PROV_ADDR_EFF_DT_SK <= '{EDWRunCycleDate}'
"""
df_db2_PROV_LOC_1_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_LOC_1_in)
    .load()
)

# Cpy_PrimAddr
df_Ref_PrimAddr_Lkup = df_db2_PROV_LOC_1_in.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("PRI_ADDR_IN").alias("PRI_ADDR_IN")
)

# Stage: db2_PROV_LOC_2_in
extract_query_db2_PROV_LOC_2_in = f"""
SELECT DISTINCT
  l.SRC_SYS_CD_SK,
  l.PROV_ADDR_ID,
  l.PROV_ADDR_TYP_CD_SK,
  l.PROV_ADDR_EFF_DT_SK,
  l.REMIT_ADDR_IN
FROM {IDSOwner}.PROV_LOC l
WHERE l.REMIT_ADDR_IN = 'Y'
  AND l.PROV_ADDR_EFF_DT_SK <= '{EDWRunCycleDate}'
"""
df_db2_PROV_LOC_2_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_LOC_2_in)
    .load()
)

# Cpy_RemitAddr
df_Ref_RemitAddr_Lkup = df_db2_PROV_LOC_2_in.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("REMIT_ADDR_IN").alias("REMIT_ADDR_IN")
)

# Stage: db2_PROV_ADDR_in
extract_query_db2_PROV_ADDR_in = f"""
SELECT
  a.PROV_ADDR_SK,
  a.SRC_SYS_CD_SK,
  a.PROV_ADDR_ID,
  a.PROV_ADDR_TYP_CD_SK,
  a.PROV_ADDR_EFF_DT_SK,
  a.CRT_RUN_CYC_EXCTN_SK,
  a.LAST_UPDT_RUN_CYC_EXCTN_SK,
  a.MAIL_PROV_ADRESS_SK,
  a.PROV_ADDR_CNTY_CLS_CD_SK,
  a.PROV_ADDR_GEO_ACES_RTRN_CD_TX,
  a.PROV_ADDR_METRORURAL_COV_CD_SK,
  a.PROV_ADDR_TERM_RSN_CD_SK,
  a.HCAP_IN,
  a.PDX_24_HR_IN,
  a.PRCTC_LOC_IN,
  a.PROV_ADDR_DIR_IN,
  a.TERM_DT_SK,
  a.ADDR_LN_1,
  a.ADDR_LN_2,
  a.ADDR_LN_3,
  a.CITY_NM,
  a.PROV_ADDR_ST_CD_SK,
  a.POSTAL_CD,
  a.CNTY_NM,
  a.PHN_NO,
  a.PHN_NO_EXT,
  a.FAX_NO,
  a.FAX_NO_EXT,
  a.EMAIL_ADDR_TX,
  a.LAT_TX,
  a.LONG_TX,
  a.PROV_URL_WEBSITE_TX
FROM {IDSOwner}.PROV_ADDR a
"""
df_db2_PROV_ADDR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_PROV_ADDR_in)
    .load()
)

# Copy_106
# Output 1 => lnk_IdsEdwProvAddrExtr_InABC
df_lnk_IdsEdwProvAddrExtr_InABC = df_db2_PROV_ADDR_in.select(
    F.col("PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MAIL_PROV_ADRESS_SK").alias("MAIL_PROV_ADRESS_SK"),
    F.col("PROV_ADDR_CNTY_CLS_CD_SK").alias("PROV_ADDR_CNTY_CLS_CD_SK"),
    F.col("PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.col("PROV_ADDR_METRORURAL_COV_CD_SK").alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
    F.col("PROV_ADDR_TERM_RSN_CD_SK").alias("PROV_ADDR_TERM_RSN_CD_SK"),
    F.col("HCAP_IN").alias("HCAP_IN"),
    F.col("PDX_24_HR_IN").alias("PDX_24_HR_IN"),
    F.col("PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
    F.col("PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("ADDR_LN_1").alias("ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("ADDR_LN_3"),
    F.col("CITY_NM").alias("CITY_NM"),
    F.col("PROV_ADDR_ST_CD_SK").alias("PROV_ADDR_ST_CD_SK"),
    F.col("POSTAL_CD").alias("POSTAL_CD"),
    F.col("CNTY_NM").alias("CNTY_NM"),
    F.col("PHN_NO").alias("PHN_NO"),
    F.col("PHN_NO_EXT").alias("PHN_NO_EXT"),
    F.col("FAX_NO").alias("FAX_NO"),
    F.col("FAX_NO_EXT").alias("FAX_NO_EXT"),
    F.col("EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
    F.col("LAT_TX").alias("LAT_TX"),
    F.col("LONG_TX").alias("LONG_TX"),
    F.col("PROV_URL_WEBSITE_TX").alias("PROV_URL_WEBSITE_TX")
)

# Output 2 => Ref_ProvAddrMail_Lkup
df_Ref_ProvAddrMail_Lkup = df_db2_PROV_ADDR_in.select(
    F.col("PROV_ADDR_SK").alias("PROV_ADDR_SK"),
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_MAIL_ADDR_TYP_CD_SK"),
    F.col("PROV_ADDR_EFF_DT_SK").alias("PROV_MAIL_ADDR_EFF_DT_SK"),
    F.col("PROV_ADDR_CNTY_CLS_CD_SK").alias("PROV_MAIL_ADDR_CNTY_CLS_CD_SK"),
    F.col("PROV_ADDR_METRORURAL_COV_CD_SK").alias("PROV_MAIL_ADDR_METRORURAL_COV_CD_SK"),
    F.col("PROV_ADDR_TERM_RSN_CD_SK").alias("PROV_MAIL_ADDR_TERM_RSN_CD_SK"),
    F.col("TERM_DT_SK").alias("PROV_MAIL_TERM_DT_SK"),
    F.col("ADDR_LN_1").alias("PROV_MAIL_ADDR_LN_1"),
    F.col("ADDR_LN_2").alias("PROV_MAIL_ADDR_LN_2"),
    F.col("ADDR_LN_3").alias("PROV_MAIL_ADDR_LN_3"),
    F.col("CITY_NM").alias("PROV_MAIL_CITY_NM"),
    F.col("PROV_ADDR_ST_CD_SK").alias("PROV_MAIL_PROV_ADDR_ST_CD_SK"),
    F.col("POSTAL_CD").alias("PROV_MAIL_POSTAL_CD"),
    F.col("CNTY_NM").alias("PROV_MAIL_CNTY_NM"),
    F.col("PHN_NO").alias("PROV_MAIL_PHN_NO"),
    F.col("PHN_NO_EXT").alias("PROV_MAIL_PHN_NO_EXT")
)

# Lookup_ProvMailAddr
# Primary link = df_lnk_IdsEdwProvAddrExtr_InABC
# Lookup link = df_Ref_ProvAddrMail_Lkup (left join on MAIL_PROV_ADRESS_SK = PROV_ADDR_SK)
df_lnk_ProvMailAddrLkpData = (
    df_lnk_IdsEdwProvAddrExtr_InABC.alias("lnk_IdsEdwProvAddrExtr_InABC")
    .join(
        df_Ref_ProvAddrMail_Lkup.alias("Ref_ProvAddrMail_Lkup"),
        on=F.col("lnk_IdsEdwProvAddrExtr_InABC.MAIL_PROV_ADRESS_SK") == F.col("Ref_ProvAddrMail_Lkup.PROV_ADDR_SK"),
        how="left"
    )
    .select(
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.MAIL_PROV_ADRESS_SK").alias("MAIL_PROV_ADRESS_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_CNTY_CLS_CD_SK").alias("PROV_ADDR_CNTY_CLS_CD_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_METRORURAL_COV_CD_SK").alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_TERM_RSN_CD_SK").alias("PROV_ADDR_TERM_RSN_CD_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.HCAP_IN").alias("HCAP_IN"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.CITY_NM").alias("CITY_NM"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_ADDR_ST_CD_SK").alias("PROV_ADDR_ST_CD_SK"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.POSTAL_CD").alias("POSTAL_CD"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.CNTY_NM").alias("CNTY_NM"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PHN_NO").alias("PHN_NO"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.FAX_NO").alias("FAX_NO"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.FAX_NO_EXT").alias("FAX_NO_EXT"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.LAT_TX").alias("LAT_TX"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.LONG_TX").alias("LONG_TX"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_ADDR_TYP_CD_SK").alias("PROV_MAIL_ADDR_TYP_CD_SK"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_ADDR_EFF_DT_SK").alias("PROV_MAIL_ADDR_EFF_DT_SK"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_ADDR_CNTY_CLS_CD_SK").alias("PROV_MAIL_ADDR_CNTY_CLS_CD_SK"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_ADDR_METRORURAL_COV_CD_SK").alias("PROV_MAIL_ADDR_METRORURAL_COV_CD_SK"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_ADDR_TERM_RSN_CD_SK").alias("PROV_MAIL_ADDR_TERM_RSN_CD_SK"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_TERM_DT_SK").alias("PROV_MAIL_TERM_DT_SK"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_ADDR_LN_1").alias("PROV_MAIL_ADDR_LN_1"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_ADDR_LN_2").alias("PROV_MAIL_ADDR_LN_2"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_ADDR_LN_3").alias("PROV_MAIL_ADDR_LN_3"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_CITY_NM").alias("PROV_MAIL_CITY_NM"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_PROV_ADDR_ST_CD_SK").alias("PROV_MAIL_PROV_ADDR_ST_CD_SK"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_POSTAL_CD").alias("PROV_MAIL_POSTAL_CD"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_CNTY_NM").alias("PROV_MAIL_CNTY_NM"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_PHN_NO").alias("PROV_MAIL_PHN_NO"),
        F.col("Ref_ProvAddrMail_Lkup.PROV_MAIL_PHN_NO_EXT").alias("PROV_MAIL_PHN_NO_EXT"),
        F.col("lnk_IdsEdwProvAddrExtr_InABC.PROV_URL_WEBSITE_TX").alias("PROV_URL_WEBSITE_TX")
    )
)

# lkp_Address
# Primary = df_lnk_ProvMailAddrLkpData
# Lookup1: df_Ref_PrimAddr_Lkup => left join on 4 columns
# Lookup2: df_Ref_RemitAddr_Lkup => left join on 4 columns
df_lnk_ProvMailAddr_plus_Prim = (
    df_lnk_ProvMailAddrLkpData.alias("lnk_ProvMailAddrLkpData")
    .join(
        df_Ref_PrimAddr_Lkup.alias("Ref_PrimAddr_In_Lkp"),
        on=[
            F.col("lnk_ProvMailAddrLkpData.SRC_SYS_CD_SK") == F.col("Ref_PrimAddr_In_Lkp.SRC_SYS_CD_SK"),
            F.col("lnk_ProvMailAddrLkpData.PROV_ADDR_ID") == F.col("Ref_PrimAddr_In_Lkp.PROV_ADDR_ID"),
            F.col("lnk_ProvMailAddrLkpData.PROV_ADDR_TYP_CD_SK") == F.col("Ref_PrimAddr_In_Lkp.PROV_ADDR_TYP_CD_SK"),
            F.col("lnk_ProvMailAddrLkpData.PROV_ADDR_EFF_DT_SK") == F.col("Ref_PrimAddr_In_Lkp.PROV_ADDR_EFF_DT_SK")
        ],
        how="left"
    )
    .select(
        F.col("lnk_ProvMailAddrLkpData.*"),
        F.col("Ref_PrimAddr_In_Lkp.PRI_ADDR_IN").alias("PRI_ADDR_IN_tmp")
    )
)

df_lnk_ProvMailAddr_plus_Prim_Remit = (
    df_lnk_ProvMailAddr_plus_Prim.alias("tempData")
    .join(
        df_Ref_RemitAddr_Lkup.alias("Ref_RemitAddr_In_Lkp"),
        on=[
            F.col("tempData.SRC_SYS_CD_SK") == F.col("Ref_RemitAddr_In_Lkp.SRC_SYS_CD_SK"),
            F.col("tempData.PROV_ADDR_ID") == F.col("Ref_RemitAddr_In_Lkp.PROV_ADDR_ID"),
            F.col("tempData.PROV_ADDR_TYP_CD_SK") == F.col("Ref_RemitAddr_In_Lkp.PROV_ADDR_TYP_CD_SK"),
            F.col("tempData.PROV_ADDR_EFF_DT_SK") == F.col("Ref_RemitAddr_In_Lkp.PROV_ADDR_EFF_DT_SK")
        ],
        how="left"
    )
    .select(
        F.col("tempData.PROV_ADDR_SK").alias("PROV_ADDR_SK"),
        F.col("tempData.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("tempData.PROV_ADDR_ID").alias("PROV_ADDR_ID"),
        F.col("tempData.PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
        F.col("tempData.PROV_ADDR_EFF_DT_SK").alias("PROV_ADDR_EFF_DT_SK"),
        F.col("tempData.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("tempData.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("tempData.MAIL_PROV_ADRESS_SK").alias("MAIL_PROV_ADRESS_SK"),
        F.col("tempData.PROV_ADDR_CNTY_CLS_CD_SK").alias("PROV_ADDR_CNTY_CLS_CD_SK"),
        F.col("tempData.PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
        F.col("tempData.PROV_ADDR_METRORURAL_COV_CD_SK").alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
        F.col("tempData.PROV_ADDR_TERM_RSN_CD_SK").alias("PROV_ADDR_TERM_RSN_CD_SK"),
        F.col("tempData.HCAP_IN").alias("HCAP_IN"),
        F.col("tempData.PDX_24_HR_IN").alias("PDX_24_HR_IN"),
        F.col("tempData.PRCTC_LOC_IN").alias("PRCTC_LOC_IN"),
        F.col("tempData.PROV_ADDR_DIR_IN").alias("PROV_ADDR_DIR_IN"),
        F.col("tempData.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("tempData.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("tempData.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("tempData.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("tempData.CITY_NM").alias("CITY_NM"),
        F.col("tempData.PROV_ADDR_ST_CD_SK").alias("PROV_ADDR_ST_CD_SK"),
        F.col("tempData.POSTAL_CD").alias("POSTAL_CD"),
        F.col("tempData.CNTY_NM").alias("CNTY_NM"),
        F.col("tempData.PHN_NO").alias("PHN_NO"),
        F.col("tempData.PHN_NO_EXT").alias("PHN_NO_EXT"),
        F.col("tempData.FAX_NO").alias("FAX_NO"),
        F.col("tempData.FAX_NO_EXT").alias("FAX_NO_EXT"),
        F.col("tempData.EMAIL_ADDR_TX").alias("EMAIL_ADDR_TX"),
        F.col("tempData.LAT_TX").alias("LAT_TX"),
        F.col("tempData.LONG_TX").alias("LONG_TX"),
        F.col("tempData.PROV_MAIL_ADDR_TYP_CD_SK").alias("PROV_MAIL_ADDR_TYP_CD_SK"),
        F.col("tempData.PROV_MAIL_ADDR_EFF_DT_SK").alias("PROV_MAIL_ADDR_EFF_DT_SK"),
        F.col("tempData.PROV_MAIL_ADDR_CNTY_CLS_CD_SK").alias("PROV_MAIL_ADDR_CNTY_CLS_CD_SK"),
        F.col("tempData.PROV_MAIL_ADDR_METRORURAL_COV_CD_SK").alias("PROV_MAIL_ADDR_METRORURAL_COV_CD_SK"),
        F.col("tempData.PROV_MAIL_ADDR_TERM_RSN_CD_SK").alias("PROV_MAIL_ADDR_TERM_RSN_CD_SK"),
        F.col("tempData.PROV_MAIL_TERM_DT_SK").alias("PROV_MAIL_TERM_DT_SK"),
        F.col("tempData.PROV_MAIL_ADDR_LN_1").alias("PROV_MAIL_ADDR_LN_1"),
        F.col("tempData.PROV_MAIL_ADDR_LN_2").alias("PROV_MAIL_ADDR_LN_2"),
        F.col("tempData.PROV_MAIL_ADDR_LN_3").alias("PROV_MAIL_ADDR_LN_3"),
        F.col("tempData.PROV_MAIL_CITY_NM").alias("PROV_MAIL_CITY_NM"),
        F.col("tempData.PROV_MAIL_PROV_ADDR_ST_CD_SK").alias("PROV_MAIL_PROV_ADDR_ST_CD_SK"),
        F.col("tempData.PROV_MAIL_POSTAL_CD").alias("PROV_MAIL_POSTAL_CD"),
        F.col("tempData.PROV_MAIL_CNTY_NM").alias("PROV_MAIL_CNTY_NM"),
        F.col("tempData.PROV_MAIL_PHN_NO").alias("PROV_MAIL_PHN_NO"),
        F.col("tempData.PROV_MAIL_PHN_NO_EXT").alias("PROV_MAIL_PHN_NO_EXT"),
        F.col("tempData.PROV_URL_WEBSITE_TX").alias("PROV_URL_WEBSITE_TX"),
        F.col("tempData.PRI_ADDR_IN_tmp").alias("PRI_ADDR_IN_tmp"),
        F.col("Ref_RemitAddr_In_Lkp.REMIT_ADDR_IN").alias("REMIT_ADDR_IN_tmp")
    )
)

df_lnk_ProvAddrPlusLkpData_out = df_lnk_ProvMailAddr_plus_Prim_Remit

# lkp_Codes
# Primary = df_lnk_ProvAddrPlusLkpData_out
# 11 lookups on the various reference dataframes. Left joins.
df_joined_1 = df_lnk_ProvAddrPlusLkpData_out.alias("lnk_ProvAddrPlusLkpData_out").join(
    df_ref_MailAddrStCd.alias("ref_MailAddrStCd"),
    on=F.col("lnk_ProvAddrPlusLkpData_out.PROV_MAIL_PROV_ADDR_ST_CD_SK") == F.col("ref_MailAddrStCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("lnk_ProvAddrPlusLkpData_out.*"),
    F.col("ref_MailAddrStCd.TRGT_CD").alias("PROV_MAIL_ADDR_ST_CD"),
    F.col("ref_MailAddrStCd.TRGT_CD_NM").alias("PROV_MAIL_ADDR_ST_NM")
)

df_joined_2 = df_joined_1.alias("df1").join(
    df_ref_MailAddrTypCd.alias("ref_MailAddrTypCd"),
    on=F.col("df1.PROV_MAIL_ADDR_TYP_CD_SK") == F.col("ref_MailAddrTypCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df1.*"),
    F.col("ref_MailAddrTypCd.TRGT_CD").alias("PROV_MAIL_ADDR_TYP_CD"),
    F.col("ref_MailAddrTypCd.TRGT_CD_NM").alias("PROV_MAIL_ADDR_TYP_NM")
)

df_joined_3 = df_joined_2.alias("df2").join(
    df_ref_MailTermRsnCd.alias("ref_MailTermRsnCd"),
    on=F.col("df2.PROV_MAIL_ADDR_TERM_RSN_CD_SK") == F.col("ref_MailTermRsnCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df2.*"),
    F.col("ref_MailTermRsnCd.TRGT_CD").alias("PROV_MAIL_ADDR_TERM_RSN_CD"),
    F.col("ref_MailTermRsnCd.TRGT_CD_NM").alias("PROV_MAIL_ADDR_TERM_RSN_NM")
)

df_joined_4 = df_joined_3.alias("df3").join(
    df_ref_MetroMailRuralCovCd_Lkp.alias("ref_MetroMailRuralCovCd_Lkp"),
    on=F.col("df3.PROV_MAIL_ADDR_METRORURAL_COV_CD_SK") == F.col("ref_MetroMailRuralCovCd_Lkp.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df3.*"),
    F.col("ref_MetroMailRuralCovCd_Lkp.TRGT_CD").alias("PROV_MAIL_METRO_RURAL_COV_CD"),
    F.col("ref_MetroMailRuralCovCd_Lkp.TRGT_CD_NM").alias("PROV_MAIL_METRO_RURAL_COV_NM")
)

df_joined_5 = df_joined_4.alias("df4").join(
    df_ref_StCd.alias("ref_StCd"),
    on=F.col("df4.PROV_ADDR_ST_CD_SK") == F.col("ref_StCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df4.*"),
    F.col("ref_StCd.TRGT_CD").alias("PROV_ADDR_ST_CD"),
    F.col("ref_StCd.TRGT_CD_NM").alias("PROV_ADDR_ST_NM")
)

df_joined_6 = df_joined_5.alias("df5").join(
    df_ref_MailCntyClsCd.alias("ref_MailCntyClsCd"),
    on=F.col("df5.PROV_MAIL_ADDR_CNTY_CLS_CD_SK") == F.col("ref_MailCntyClsCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df5.*"),
    F.col("ref_MailCntyClsCd.TRGT_CD").alias("PROV_MAIL_ADDR_CNTY_CLS_CD"),
    F.col("ref_MailCntyClsCd.TRGT_CD_NM").alias("PROV_MAIL_ADDR_CNTY_CLS_NM")
)

df_joined_7 = df_joined_6.alias("df6").join(
    df_ref_TermRsnCd.alias("ref_TermRsnCd"),
    on=F.col("df6.PROV_ADDR_TERM_RSN_CD_SK") == F.col("ref_TermRsnCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df6.*"),
    F.col("ref_TermRsnCd.TRGT_CD").alias("PROV_ADDR_TERM_RSN_CD"),
    F.col("ref_TermRsnCd.TRGT_CD_NM").alias("PROV_ADDR_TERM_RSN_NM")
)

df_joined_8 = df_joined_7.alias("df7").join(
    df_ref_MetroRuralCovCd.alias("ref_MetroRuralCovCd"),
    on=F.col("df7.PROV_ADDR_METRORURAL_COV_CD_SK") == F.col("ref_MetroRuralCovCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df7.*"),
    F.col("ref_MetroRuralCovCd.TRGT_CD").alias("PROV_ADDR_METRO_RURAL_COV_CD"),
    F.col("ref_MetroRuralCovCd.TRGT_CD_NM").alias("PROV_ADDR_METRO_RURAL_COV_NM")
)

df_joined_9 = df_joined_8.alias("df8").join(
    df_ref_CntyCls.alias("ref_CntyCls"),
    on=F.col("df8.PROV_ADDR_CNTY_CLS_CD_SK") == F.col("ref_CntyCls.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df8.*"),
    F.col("ref_CntyCls.TRGT_CD").alias("PROV_ADDR_CNTY_CLS_CD"),
    F.col("ref_CntyCls.TRGT_CD_NM").alias("PROV_ADDR_CNTY_CLS_NM")
)

df_joined_10 = df_joined_9.alias("df9").join(
    df_ref_AddrTypCd.alias("ref_AddrTypCd"),
    on=F.col("df9.PROV_ADDR_TYP_CD_SK") == F.col("ref_AddrTypCd.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df9.*"),
    F.col("ref_AddrTypCd.TRGT_CD").alias("PROV_ADDR_TYP_CD"),
    F.col("ref_AddrTypCd.TRGT_CD_NM").alias("PROV_ADDR_TYP_NM")
)

df_joined_11 = df_joined_10.alias("df10").join(
    df_ref_SrcSys.alias("ref_SrcSys"),
    on=F.col("df10.SRC_SYS_CD_SK") == F.col("ref_SrcSys.CD_MPPNG_SK"),
    how="left"
).select(
    F.col("df10.*"),
    F.col("ref_SrcSys.TRGT_CD").alias("SRC_SYS_CD")
)

df_lnk_CodesLkpData_out = df_joined_11.select(
    F.col("PROV_ADDR_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PROV_ADDR_ID"),
    F.col("PROV_ADDR_TYP_CD_SK"),
    F.col("PROV_ADDR_EFF_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_ADDR_CNTY_CLS_CD_SK"),
    F.col("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    F.col("PROV_ADDR_METRORURAL_COV_CD_SK"),
    F.col("PROV_ADDR_TERM_RSN_CD_SK"),
    F.col("HCAP_IN"),
    F.col("PDX_24_HR_IN"),
    F.col("PRCTC_LOC_IN"),
    F.col("PROV_ADDR_DIR_IN"),
    F.col("TERM_DT_SK"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_L_2").alias("ADDR_LN_2"),  # correct the alias if any mismatch
    F.col("ADDR_L_3").alias("ADDR_LN_3"),  # correct the alias if any mismatch
    F.col("CITY_NM"),
    F.col("PROV_ADDR_ST_CD_SK"),
    F.col("POSTAL_CD"),
    F.col("CNTY_NM"),
    F.col("PHN_NO"),
    F.col("PHN_NO_EXT"),
    F.col("FAX_NO"),
    F.col("FAX_NO_EXT"),
    F.col("EMAIL_ADDR_TX"),
    F.col("LAT_TX"),
    F.col("LONG_TX"),
    F.col("PROV_MAIL_ADDR_EFF_DT_SK"),
    F.col("PROV_MAIL_ADDR_CNTY_CLS_CD_SK"),
    F.col("PROV_MAIL_ADDR_METRORURAL_COV_CD_SK"),
    F.col("PROV_MAIL_ADDR_TERM_RSN_CD_SK"),
    F.col("PROV_MAIL_TERM_DT_SK"),
    F.col("PROV_MAIL_ADDR_LN_1"),
    F.col("PROV_MAIL_ADDR_LN_2"),
    F.col("PROV_MAIL_ADDR_LN_3"),
    F.col("PROV_MAIL_CITY_NM"),
    F.col("PROV_MAIL_PROV_ADDR_ST_CD_SK"),
    F.col("PROV_MAIL_POSTAL_CD"),
    F.col("PROV_MAIL_CNTY_NM"),
    F.col("PROV_MAIL_PHN_NO"),
    F.col("PROV_MAIL_PHN_NO_EXT"),
    F.col("PRI_ADDR_IN_tmp").alias("PRI_ADDR_IN"),
    F.col("REMIT_ADDR_IN_tmp").alias("REMIT_ADDR_IN"),
    F.col("PROV_URL_WEBSITE_TX"),
    F.col("PROV_MAIL_ADDR_TYP_CD"),
    F.col("PROV_MAIL_ADDR_TYP_NM"),
    F.col("PROV_ADDR_TYP_CD"),
    F.col("PROV_ADDR_TYP_NM"),
    F.col("PROV_ADDR_CNTY_CLS_CD"),
    F.col("PROV_ADDR_CNTY_CLS_NM"),
    F.col("PROV_ADDR_METRO_RURAL_COV_CD"),
    F.col("PROV_ADDR_METRO_RURAL_COV_NM"),
    F.col("PROV_ADDR_TERM_RSN_CD"),
    F.col("PROV_ADDR_TERM_RSN_NM"),
    F.col("PROV_ADDR_ST_CD"),
    F.col("PROV_ADDR_ST_NM"),
    F.col("PROV_MAIL_ADDR_CNTY_CLS_CD"),
    F.col("PROV_MAIL_ADDR_CNTY_CLS_NM"),
    F.col("PROV_MAIL_METRO_RURAL_COV_CD"),
    F.col("PROV_MAIL_METRO_RURAL_COV_NM"),
    F.col("PROV_MAIL_ADDR_TERM_RSN_CD"),
    F.col("PROV_MAIL_ADDR_TERM_RSN_NM"),
    F.col("PROV_MAIL_ADDR_ST_CD"),
    F.col("PROV_MAIL_ADDR_ST_NM")
).withColumnRenamed("ADDR_L_2","ADDR_LN_2").withColumnRenamed("ADDR_L_3","ADDR_LN_3")  # Correction in case the above alias needed a fix

# xfrm_BusinessLogic (CTransformerStage)
# We will chain computations for stage variables in a single DataFrame, then compute final columns.

df_stage_vars = (
    df_lnk_CodesLkpData_out
    # StageVariable: svPostalCd = FORMAT.POSTALCD.EE(POSTAL_CD)
    .withColumn("svPostalCd", FORMAT.POSTALCD.EE(F.col("POSTAL_CD")))
    # StageVariable: svMailPostCd = FORMAT.POSTALCD.EE(PROV_MAIL_POSTAL_CD)
    .withColumn("svMailPostCd", FORMAT.POSTALCD.EE(F.col("PROV_MAIL_POSTAL_CD")))
    # StageVariable: svAddrZipcd5
    .withColumn(
        "svAddrZipcd5",
        F.when(
            F.length(F.trim(F.substring(F.col("svPostalCd"),1,5))) == 0,
            F.lit(None)
        ).otherwise(F.substring(F.col("svPostalCd"),1,5))
    )
    # StageVariable: svAddrZipcd4
    .withColumn(
        "svAddrZipcd4",
        F.when(
            F.length(F.trim(F.col("svPostalCd"))) < 6,
            F.lit(None)
        ).otherwise(F.substring(F.col("svPostalCd"),6,4))
    )
    # StageVariable: svMailZipcd5
    .withColumn(
        "svMailZipcd5",
        F.when(
            F.length(F.trim(F.substring(F.col("svMailPostCd"),1,5))) == 0,
            F.lit(None)
        ).otherwise(F.substring(F.col("svMailPostCd"),1,5))
    )
    # StageVariable: svMailZipcd4
    .withColumn(
        "svMailZipcd4",
        F.when(
            F.length(F.trim(F.col("svMailPostCd"))) < 6,
            F.lit(None)
        ).otherwise(F.substring(F.col("svMailPostCd"),6,4))
    )
    # StageVariable: svAddrStateValid
    .withColumn(
        "svAddrStateValid",
        F.when(
            (F.col("PROV_ADDR_ST_CD_SK") == 1) | (F.col("PROV_ADDR_ST_CD_SK") == 0),
            F.lit(False)
        ).otherwise(F.lit(True))
    )
    # StageVariable: svMailStateValid
    .withColumn(
        "svMailStateValid",
        F.when(
            (F.col("PROV_MAIL_PROV_ADDR_ST_CD_SK") == 1) | (F.col("PROV_MAIL_PROV_ADDR_ST_CD_SK") == 0),
            F.lit(False)
        ).otherwise(F.lit(True))
    )
)

# Now compute final columns in a single select. 
# Also apply rpad for columns that are char(...) in the output metadata.

df_lnk_TrnsfrmedData_in = df_stage_vars.select(
    # PROV_ADDR_SK (Primary Key, no char length specified, pass as is)
    F.col("PROV_ADDR_SK"),
    # SRC_SYS_CD => If null or length=0 => 'NA' else self, no char length
    F.when(F.col("SRC_SYS_CD").isNull() | (F.length(F.col("SRC_SYS_CD")) == 0), F.lit("NA"))
     .otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    # PROV_ADDR_ID => pass as is
    F.col("PROV_ADDR_ID").alias("PROV_ADDR_ID"),
    # PROV_ADDR_TYP_CD => if null or length=0 => 'NA'
    F.when(F.col("PROV_ADDR_TYP_CD").isNull() | (F.length(F.col("PROV_ADDR_TYP_CD")) == 0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_TYP_CD")).alias("PROV_ADDR_TYP_CD"),
    # PROV_ADDR_EFF_DT_SK => char(10)
    F.rpad(F.col("PROV_ADDR_EFF_DT_SK"),10," ").alias("PROV_ADDR_EFF_DT_SK"),
    # CRT_RUN_CYC_EXCTN_DT_SK => char(10) => from EDWRunCycleDate
    F.rpad(F.lit(EDWRunCycleDate),10," ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    # LAST_UPDT_RUN_CYC_EXCTN_DT_SK => char(10) => from EDWRunCycleDate
    F.rpad(F.lit(EDWRunCycleDate),10," ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    # PROV_ADDR_CNTY_CLS_CD => if null/len=0 => 'NA'
    F.when(F.col("PROV_ADDR_CNTY_CLS_CD").isNull() | (F.length(F.col("PROV_ADDR_CNTY_CLS_CD")) == 0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_CNTY_CLS_CD")).alias("PROV_ADDR_CNTY_CLS_CD"),
    # PROV_ADDR_CNTY_CLS_NM => if null/len=0 => 'NA'
    F.when(F.col("PROV_ADDR_CNTY_CLS_NM").isNull() | (F.length(F.col("PROV_ADDR_CNTY_CLS_NM")) == 0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_CNTY_CLS_NM")).alias("PROV_ADDR_CNTY_CLS_NM"),
    # PROV_ADDR_DIR_IN (char(1))
    F.rpad(F.col("PROV_ADDR_DIR_IN"),1," ").alias("PROV_ADDR_DIR_IN"),
    # PROV_ADDR_GEO_ACES_RTRN_CD_TX => pass as is
    F.col("PROV_ADDR_GEO_ACES_RTRN_CD_TX").alias("PROV_ADDR_GEO_ACES_RTRN_CD_TX"),
    # PROV_ADDR_HCAP_IN (char(1))
    F.rpad(F.col("HCAP_IN"),1," ").alias("PROV_ADDR_HCAP_IN"),
    # PROV_ADDR_METRO_RURAL_COV_CD => if null/len=0 => 'NA'
    F.when(F.col("PROV_ADDR_METRO_RURAL_COV_CD").isNull() | (F.length(F.col("PROV_ADDR_METRO_RURAL_COV_CD"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_METRO_RURAL_COV_CD")).alias("PROV_ADDR_METRO_RURAL_COV_CD"),
    # PROV_ADDR_METRO_RURAL_COV_NM => if null => 'NA'
    F.when(F.col("PROV_ADDR_METRO_RURAL_COV_NM").isNull() | (F.length(F.col("PROV_ADDR_METRO_RURAL_COV_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_METRO_RURAL_COV_NM")).alias("PROV_ADDR_METRO_RURAL_COV_NM"),
    # PROV_ADDR_PDX_24_HR_IN (char(1))
    F.rpad(F.col("PDX_24_HR_IN"),1," ").alias("PROV_ADDR_PDX_24_HR_IN"),
    # PROV_ADDR_PRCTC_LOC_IN (char(1))
    F.rpad(F.col("PRCTC_LOC_IN"),1," ").alias("PROV_ADDR_PRCTC_LOC_IN"),
    # PROV_ADDR_TERM_DT_SK (char(10))
    F.rpad(F.col("TERM_DT_SK"),10," ").alias("PROV_ADDR_TERM_DT_SK"),
    # PROV_ADDR_TERM_RSN_CD => if null => 'NA'
    F.when(F.col("PROV_ADDR_TERM_RSN_CD").isNull() | (F.length(F.col("PROV_ADDR_TERM_RSN_CD"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_TERM_RSN_CD")).alias("PROV_ADDR_TERM_RSN_CD"),
    # PROV_ADDR_TERM_RSN_NM => if null => 'NA'
    F.when(F.col("PROV_ADDR_TERM_RSN_NM").isNull() | (F.length(F.col("PROV_ADDR_TERM_RSN_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_TERM_RSN_NM")).alias("PROV_ADDR_TERM_RSN_NM"),
    # PROV_ADDR_LN_1 => pass as is
    F.col("ADDR_LN_1").alias("PROV_ADDR_LN_1"),
    # PROV_ADDR_LN_2 => if isNull or len=0 => SetNull
    F.when(F.col("ADDR_LN_2").isNull() | (F.length(F.trim(F.col("ADDR_LN_2"))) == 0), F.lit(None))
     .otherwise(F.col("ADDR_LN_2")).alias("PROV_ADDR_LN_2"),
    # PROV_ADDR_LN_3 => same logic
    F.when(F.col("ADDR_LN_3").isNull() | (F.length(F.trim(F.col("ADDR_LN_3"))) == 0), F.lit(None))
     .otherwise(F.col("ADDR_LN_3")).alias("PROV_ADDR_LN_3"),
    # PROV_ADDR_CITY_NM => pass as is
    F.col("CITY_NM").alias("PROV_ADDR_CITY_NM"),
    # PROV_ADDR_ST_CD => if null => 'NA'
    F.when(F.col("PROV_ADDR_ST_CD").isNull() | (F.length(F.col("PROV_ADDR_ST_CD"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_ST_CD")).alias("PROV_ADDR_ST_CD"),
    # PROV_ADDR_ST_NM => if null => 'NA'
    F.when(F.col("PROV_ADDR_ST_NM").isNull() | (F.length(F.col("PROV_ADDR_ST_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_ST_NM")).alias("PROV_ADDR_ST_NM"),
    # PROV_ADDR_ZIP_CD_5 => char(5)
    F.rpad(
        F.when(
            (F.col("svAddrStateValid") == F.lit(False)) | (F.expr("Num(svAddrZipcd5)") == F.lit(False)),
            F.lit(None)
        ).otherwise(F.col("svAddrZipcd5")),
        5," "
    ).alias("PROV_ADDR_ZIP_CD_5"),
    # PROV_ADDR_ZIP_CD_4 => char(4)
    F.rpad(
        F.when(
            (F.col("svAddrStateValid") == F.lit(False)) | (F.expr("Num(svAddrZipcd4)") == F.lit(False)),
            F.lit(None)
        ).otherwise(F.col("svAddrZipcd4")),
        4," "
    ).alias("PROV_ADDR_ZIP_CD_4"),
    # PROV_ADDR_CNTY_NM => pass as is
    F.col("CNTY_NM").alias("PROV_ADDR_CNTY_NM"),
    # PROV_ADDR_PHN_NO
    F.col("PHN_NO").alias("PROV_ADDR_PHN_NO"),
    # PROV_ADDR_PHN_NO_EXT
    F.col("PHN_NO_EXT").alias("PROV_ADDR_PHN_NO_EXT"),
    # PROV_ADDR_FAX_NO
    F.col("FAX_NO").alias("PROV_ADDR_FAX_NO"),
    # PROV_ADDR_FAX_NO_EXT => if length=0 => SetNull
    F.when((F.length(F.trim(F.col("FAX_NO_EXT"))) == 0), F.lit(None))
     .otherwise(F.col("FAX_NO_EXT")).alias("PROV_ADDR_FAX_NO_EXT"),
    # PROV_ADDR_EMAIL_ADDR_TX
    F.col("EMAIL_ADDR_TX").alias("PROV_ADDR_EMAIL_ADDR_TX"),
    # PROV_ADDR_LAT_TX
    F.col("LAT_TX").alias("PROV_ADDR_LAT_TX"),
    # PROV_ADDR_LONG_TX
    F.col("LONG_TX").alias("PROV_ADDR_LONG_TX"),
    # PROV_ADDR_TYP_NM => if null => 'NA'
    F.when(F.col("PROV_ADDR_TYP_NM").isNull() | (F.length(F.col("PROV_ADDR_TYP_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_ADDR_TYP_NM")).alias("PROV_ADDR_TYP_NM"),
    # PROV_MAIL_ADDR_CNTY_CLS_CD => if null => 'NA'
    F.when(F.col("PROV_MAIL_ADDR_CNTY_CLS_CD").isNull() | (F.length(F.col("PROV_MAIL_ADDR_CNTY_CLS_CD"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_ADDR_CNTY_CLS_CD")).alias("PROV_MAIL_ADDR_CNTY_CLS_CD"),
    # PROV_MAIL_ADDR_CNTY_CLS_NM => if null => 'NA'
    F.when(F.col("PROV_MAIL_ADDR_CNTY_CLS_NM").isNull() | (F.length(F.col("PROV_MAIL_ADDR_CNTY_CLS_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_ADDR_CNTY_CLS_NM")).alias("PROV_MAIL_ADDR_CNTY_CLS_NM"),
    # PROV_MAIL_ADDR_EFF_DT_SK => char(10): if len(...)=0 => '1753-01-01' else self
    F.rpad(
        F.when(
            F.length(F.trim(F.col("PROV_MAIL_ADDR_EFF_DT_SK"))) == 0,
            F.lit("1753-01-01")
        ).otherwise(F.col("PROV_MAIL_ADDR_EFF_DT_SK")),
        10," "
    ).alias("PROV_MAIL_ADDR_EFF_DT_SK"),
    # PROV_MAIL_ADDR_LN_1 => if null or '' => ' '
    F.when(F.col("PROV_MAIL_ADDR_LN_1").isNull() | (F.trim(F.col("PROV_MAIL_ADDR_LN_1"))==""), F.lit(" "))
     .otherwise(F.col("PROV_MAIL_ADDR_LN_1")).alias("PROV_MAIL_ADDR_LN_1"),
    # PROV_MAIL_ADDR_LN_2 => same logic
    F.when(F.col("PROV_MAIL_ADDR_LN_2").isNull() | (F.trim(F.col("PROV_MAIL_ADDR_LN_2"))==""), F.lit(" "))
     .otherwise(F.col("PROV_MAIL_ADDR_LN_2")).alias("PROV_MAIL_ADDR_LN_2"),
    # PROV_MAIL_ADDR_LN_3 => same logic
    F.when(F.col("PROV_MAIL_ADDR_LN_3").isNull() | (F.trim(F.col("PROV_MAIL_ADDR_LN_3"))==""), F.lit(" "))
     .otherwise(F.col("PROV_MAIL_ADDR_LN_3")).alias("PROV_MAIL_ADDR_LN_3"),
    # PROV_MAIL_ADDR_CITY_NM => if null or '', ' '
    F.when(F.col("PROV_MAIL_CITY_NM").isNull() | (F.trim(F.col("PROV_MAIL_CITY_NM"))==""), F.lit(" "))
     .otherwise(F.col("PROV_MAIL_CITY_NM")).alias("PROV_MAIL_ADDR_CITY_NM"),
    # PROV_MAIL_ADDR_ST_CD => if null => 'NA'
    F.when(F.col("PROV_MAIL_ADDR_ST_CD").isNull() | (F.length(F.col("PROV_MAIL_ADDR_ST_CD"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_ADDR_ST_CD")).alias("PROV_MAIL_ADDR_ST_CD"),
    # PROV_MAIL_ADDR_ST_NM => if null => 'NA'
    F.when(F.col("PROV_MAIL_ADDR_ST_NM").isNull() | (F.length(F.col("PROV_MAIL_ADDR_ST_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_ADDR_ST_NM")).alias("PROV_MAIL_ADDR_ST_NM"),
    # PROV_MAIL_ADDR_ZIP_CD_5 => char(5)
    F.rpad(
        F.when(
            (F.col("svMailStateValid") == F.lit(False)) | (F.expr("Num(svMailZipcd5)") == F.lit(False)),
            F.lit(None)
        ).otherwise(F.col("svMailZipcd5")),
        5," "
    ).alias("PROV_MAIL_ADDR_ZIP_CD_5"),
    # PROV_MAIL_ADDR_ZIP_CD_4 => char(4)
    F.rpad(
        F.when(
            (F.col("svMailStateValid") == F.lit(False)) | (F.expr("Num(svMailZipcd4)") == F.lit(False)),
            F.lit(None)
        ).otherwise(F.col("svMailZipcd4")),
        4," "
    ).alias("PROV_MAIL_ADDR_ZIP_CD_4"),
    # PROV_MAIL_ADDR_CNTY_NM => if null or '' => ' '
    F.when(F.col("PROV_MAIL_CNTY_NM").isNull() | (F.trim(F.col("PROV_MAIL_CNTY_NM"))==""), F.lit(" "))
     .otherwise(F.col("PROV_MAIL_CNTY_NM")).alias("PROV_MAIL_ADDR_CNTY_NM"),
    # PROV_MAIL_ADDR_PHN_NO
    F.col("PROV_MAIL_PHN_NO").alias("PROV_MAIL_ADDR_PHN_NO"),
    # PROV_MAIL_ADDR_PHN_NO_EXT
    F.col("PROV_MAIL_PHN_NO_EXT").alias("PROV_MAIL_ADDR_PHN_NO_EXT"),
    # PROV_MAIL_METRO_RURAL_COV_CD => if null => 'NA'
    F.when(F.col("PROV_MAIL_METRO_RURAL_COV_CD").isNull() | (F.length(F.col("PROV_MAIL_METRO_RURAL_COV_CD"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_METRO_RURAL_COV_CD")).alias("PROV_MAIL_METRO_RURAL_COV_CD"),
    # PROV_MAIL_METRO_RURAL_COV_NM => if null => 'NA'
    F.when(F.col("PROV_MAIL_METRO_RURAL_COV_NM").isNull() | (F.length(F.col("PROV_MAIL_METRO_RURAL_COV_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_METRO_RURAL_COV_NM")).alias("PROV_MAIL_METRO_RURAL_COV_NM"),
    # PROV_MAIL_ADDR_TERM_DT_SK => char(10): if len(trim(...))=0 => '9999-12-31' else self
    F.rpad(
        F.when(
            F.length(F.trim(F.col("PROV_MAIL_TERM_DT_SK"))) == 0,
            F.lit("9999-12-31")
        ).otherwise(F.col("PROV_MAIL_TERM_DT_SK")),
        10," "
    ).alias("PROV_MAIL_ADDR_TERM_DT_SK"),
    # PROV_MAIL_ADDR_TERM_RSN_CD => if null => 'NA'
    F.when(F.col("PROV_MAIL_ADDR_TERM_RSN_CD").isNull() | (F.length(F.col("PROV_MAIL_ADDR_TERM_RSN_CD"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_ADDR_TERM_RSN_CD")).alias("PROV_MAIL_ADDR_TERM_RSN_CD"),
    # PROV_MAIL_ADDR_TERM_RSN_NM => if null => 'NA'
    F.when(F.col("PROV_MAIL_ADDR_TERM_RSN_NM").isNull() | (F.length(F.col("PROV_MAIL_ADDR_TERM_RSN_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_ADDR_TERM_RSN_NM")).alias("PROV_MAIL_ADDR_TERM_RSN_NM"),
    # PROV_MAIL_ADDR_TYP_CD => if null => 'NA'
    F.when(F.col("PROV_MAIL_ADDR_TYP_CD").isNull() | (F.length(F.col("PROV_MAIL_ADDR_TYP_CD"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_ADDR_TYP_CD")).alias("PROV_MAIL_ADDR_TYP_CD"),
    # PROV_MAIL_ADDR_TYP_NM => if null => 'NA'
    F.when(F.col("PROV_MAIL_ADDR_TYP_NM").isNull() | (F.length(F.col("PROV_MAIL_ADDR_TYP_NM"))==0), F.lit("NA"))
     .otherwise(F.col("PROV_MAIL_ADDR_TYP_NM")).alias("PROV_MAIL_ADDR_TYP_NM"),
    # CRT_RUN_CYC_EXCTN_SK => from EDWRunCycle
    F.col("EDWRunCycle").alias("CRT_RUN_CYC_EXCTN_SK"),
    # LAST_UPDT_RUN_CYC_EXCTN_SK => from EDWRunCycle
    F.col("EDWRunCycle").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # PROV_ADDR_CNTY_CLS_CD_SK => pass as is
    F.col("PROV_ADDR_CNTY_CLS_CD_SK").alias("PROV_ADDR_CNTY_CLS_CD_SK"),
    # PROV_ADDR_METRORURAL_COV_CD_SK => pass as is
    F.col("PROV_ADDR_METRORURAL_COV_CD_SK").alias("PROV_ADDR_METRORURAL_COV_CD_SK"),
    # PROV_ADDR_ST_CD_SK => pass as is
    F.col("PROV_ADDR_ST_CD_SK").alias("PROV_ADDR_ST_CD_SK"),
    # PROV_ADDR_TERM_RSN_CD_SK => pass as is
    F.col("PROV_ADDR_TERM_RSN_CD_SK").alias("PROV_ADDR_TERM_RSN_CD_SK"),
    # PROV_ADDR_TYP_CD_SK => pass as is
    F.col("PROV_ADDR_TYP_CD_SK").alias("PROV_ADDR_TYP_CD_SK"),
    # PROV_MAIL_ADDR_CNTY_CLS_CD_SK => pass as is
    F.col("PROV_MAIL_ADDR_CNTY_CLS_CD_SK").alias("PROV_MAIL_ADDR_CNTY_CLS_CD_SK"),
    # PROV_MAIL_METRORURAL_CD_SK => pass as is
    F.col("PROV_MAIL_ADDR_METRORURAL_COV_CD_SK").alias("PROV_MAIL_METRORURAL_CD_SK"),
    # PROV_MAIL_ADDR_ST_CD_SK => pass as is
    F.col("PROV_MAIL_PROV_ADDR_ST_CD_SK").alias("PROV_MAIL_ADDR_ST_CD_SK"),
    # PROV_MAIL_ADDR_TERM_RSN_CD_SK => pass as is
    F.col("PROV_MAIL_ADDR_TERM_RSN_CD_SK").alias("PROV_MAIL_ADDR_TERM_RSN_CD_SK"),
    # PROV_PRI_ADDR_IN => char(1)
    F.rpad(
        F.when(F.trim(F.col("PRI_ADDR_IN")).eqNullSafe(""), F.lit("N"))
         .otherwise(F.col("PRI_ADDR_IN")),
        1," "
    ).alias("PROV_PRI_ADDR_IN"),
    # PROV_REMIT_ADDR_IN => char(1)
    F.rpad(
        F.when(F.trim(F.col("REMIT_ADDR_IN")).eqNullSafe(""), F.lit("N"))
         .otherwise(F.col("REMIT_ADDR_IN")),
        1," "
    ).alias("PROV_REMIT_ADDR_IN"),
    # PROV_URL_WEBSITE_TX
    F.col("PROV_URL_WEBSITE_TX").alias("PROV_URL_WEBSITE_TX")
)

# seq_PROV_ADDR_D_csv_load
# Write to: #$FilePath#/load/PROV_ADDR.dat => none of "landing" or "external", so use adls_path
output_file_path = f"{adls_path}/load/PROV_ADDR.dat"

# Write the final dataframe
# Using the signature: write_files(df, file_path, delimiter, mode, is_parquet, header, quote, nullValue)
write_files(
    df_lnk_TrnsfrmedData_in,
    output_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)