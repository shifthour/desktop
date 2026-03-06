# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC PROCESSING:  Extracting data from APL_ACTVTY
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                                                           Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC 
# MAGIC Bhoomi Dasari                2/08/2007           Appeals/3028              Originally Programmed                                                                         devlEDW10                  Steph Goddard              09/15/2007
# MAGIC 
# MAGIC 
# MAGIC Raj Mangalampally         11-04-2013          5114                             Server to Parallel Conv                                                                       EnterpriseWrhsDevl       Jag Yelavarthi               2014-01-17
# MAGIC 
# MAGIC Jag Yelavarthi                 2014-02-07         5345 Daptiv#1             Changed the column derivation for                                                      EnterpriseWrhsDevl      Bhoomi Dasari               2/7/2014
# MAGIC                                                                                                         APL_SUM to handle "^"
# MAGIC                                                                                                         characters
# MAGIC Ravi Singh                       12/07/2018          MTM- 5841               Added in parameter and soruce query for extract NDBH,                    EnterpriseDev2          Kalyan Neelam              2018-12-10       
# MAGIC                                                                                                        Evicore(MEDSLTNS) and Telligen Appeal process data

# MAGIC Read data from source table APL_ACTVTY
# MAGIC Lookup Keys
# MAGIC 
# MAGIC 1) APL_ACTVTY_METH_CD_SK
# MAGIC 2) APL_ACTVTY_TYP_CD_SK
# MAGIC Lookup Keys
# MAGIC 1) USER_SK
# MAGIC 2) USER_ID
# MAGIC Job Name : IdsEdwAplActvtyDExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Retrieve parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
MedSltnsExtractRunCycle = get_widget_value('MedSltnsExtractRunCycle','')
NdbhExtractRunCycle = get_widget_value('NdbhExtractRunCycle','')
TelligenExtractRunCycle = get_widget_value('TelligenExtractRunCycle','')

# Configure JDBC for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# db2_APL_ACTVTY_D_in
sql_db2_APL_ACTVTY_D_in = f"""SELECT
APL_ACTVTY.APL_ACTVTY_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
APL_ACTVTY.APL_ID,
APL_ACTVTY.SEQ_NO,
APL_ACTVTY.APL_SK,
APL_ACTVTY.APL_RVWR_SK,
APL_ACTVTY.CRT_USER_SK,
APL_ACTVTY.LAST_UPDT_USER_SK,
APL_ACTVTY.APL_ACTVTY_METH_CD_SK,
APL_ACTVTY.APL_ACTVTY_TYP_CD_SK,
APL_ACTVTY.ACTVTY_DT_SK,
APL_ACTVTY.CRT_DTM,
APL_ACTVTY.LAST_UPDT_DTM,
APL_ACTVTY.APL_LVL_SEQ_NO,
APL_ACTVTY.ACTVTY_SUM,
APL_ACTVTY.APL_RVWR_ID
FROM {IDSOwner}.APL_ACTVTY APL_ACTVTY
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON APL_ACTVTY.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE
(CD.TRGT_CD = 'FACETS' AND APL_ACTVTY.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle})
OR
(CD.TRGT_CD = 'MEDSLTNS' AND APL_ACTVTY.LAST_UPDT_RUN_CYC_EXCTN_SK >= {MedSltnsExtractRunCycle})
OR
(CD.TRGT_CD = 'NDBH' AND APL_ACTVTY.LAST_UPDT_RUN_CYC_EXCTN_SK >= {NdbhExtractRunCycle})
OR
(CD.TRGT_CD = 'TELLIGEN' AND APL_ACTVTY.LAST_UPDT_RUN_CYC_EXCTN_SK >= {TelligenExtractRunCycle})"""

df_db2_APL_ACTVTY_D_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_db2_APL_ACTVTY_D_in)
    .load()
)

# db2_CD_MPPNG_Extr
sql_db2_CD_MPPNG_Extr = f"""SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'NA') TRGT_CD,
TRGT_CD_NM 
FROM {IDSOwner}.CD_MPPNG
"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_db2_CD_MPPNG_Extr)
    .load()
)

# cpy_cd_mppng -> Ref_AplActvtyMethCdLkup
df_cpy_cd_mppng_Ref_AplActvtyMethCdLkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# cpy_cd_mppng -> Ref_AplActvtyTypCdLkup
df_cpy_cd_mppng_Ref_AplActvtyTypCdLkup = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

# db2_SUBGRP_D_In
sql_db2_SUBGRP_D_In = f"""SELECT 
APP_USER.USER_SK,
APP_USER.USER_ID
FROM {IDSOwner}.APP_USER APP_USER
"""
df_db2_SUBGRP_D_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_db2_SUBGRP_D_In)
    .load()
)

# cpy_SubGrp -> Ref_CrtUsrSk
df_cpy_SubGrp_Ref_CrtUsrSk = df_db2_SUBGRP_D_In.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)

# cpy_SubGrp -> Ref_LstUpdtSk
df_cpy_SubGrp_Ref_LstUpdtSk = df_db2_SUBGRP_D_In.select(
    F.col("USER_SK").alias("USER_SK"),
    F.col("USER_ID").alias("USER_ID")
)

# lkp_Apl_App_Usr
df_lkpAppUsr_temp1 = df_db2_APL_ACTVTY_D_in.alias("Ink_IdsEdwAplActvtyExtr_inABC").join(
    df_cpy_SubGrp_Ref_CrtUsrSk.alias("Ref_CrtUsrSk"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.CRT_USER_SK") == F.col("Ref_CrtUsrSk.USER_SK"),
    "left"
)
df_lkp_Apl_App_Usr = df_lkpAppUsr_temp1.join(
    df_cpy_SubGrp_Ref_LstUpdtSk.alias("Ref_LstUpdtSk"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.LAST_UPDT_USER_SK") == F.col("Ref_LstUpdtSk.USER_SK"),
    "left"
).select(
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.APL_ACTVTY_SK").alias("APL_ACTVTY_SK"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.APL_ID").alias("APL_ID"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.SEQ_NO").alias("SEQ_NO"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.APL_SK").alias("APL_SK"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.APL_RVWR_SK").alias("APL_RVWR_SK"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.CRT_USER_SK").alias("CRT_USER_SK"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.APL_ACTVTY_METH_CD_SK").alias("APL_ACTVTY_METH_CD_SK"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.APL_ACTVTY_TYP_CD_SK").alias("APL_ACTVTY_TYP_CD_SK"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.ACTVTY_DT_SK").alias("ACTVTY_DT_SK"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.CRT_DTM").alias("CRT_DTM"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.ACTVTY_SUM").alias("ACTVTY_SUM"),
    F.col("Ink_IdsEdwAplActvtyExtr_inABC.APL_RVWR_ID").alias("APL_RVWR_ID"),
    F.col("Ref_CrtUsrSk.USER_ID").alias("USER_ID"),
    F.col("Ref_LstUpdtSk.USER_ID").alias("USER_ID_1")
)

# lkp_CDMA_Codes
df_lkpCDMACodes_temp1 = df_lkp_Apl_App_Usr.alias("lnk_AplAppUsr_Out").join(
    df_cpy_cd_mppng_Ref_AplActvtyMethCdLkup.alias("Ref_AplActvtyMethCdLkup"),
    F.col("lnk_AplAppUsr_Out.APL_ACTVTY_METH_CD_SK") == F.col("Ref_AplActvtyMethCdLkup.CD_MPPNG_SK"),
    "left"
)
df_lkp_CDMA_Codes = df_lkpCDMACodes_temp1.join(
    df_cpy_cd_mppng_Ref_AplActvtyTypCdLkup.alias("Ref_AplActvtyTypCdLkup"),
    F.col("lnk_AplAppUsr_Out.APL_ACTVTY_TYP_CD_SK") == F.col("Ref_AplActvtyTypCdLkup.CD_MPPNG_SK"),
    "left"
).select(
    F.col("lnk_AplAppUsr_Out.APL_ACTVTY_SK").alias("APL_ACTVTY_SK"),
    F.col("lnk_AplAppUsr_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_AplAppUsr_Out.APL_ID").alias("APL_ID"),
    F.col("lnk_AplAppUsr_Out.SEQ_NO").alias("SEQ_NO"),
    F.col("lnk_AplAppUsr_Out.APL_SK").alias("APL_SK"),
    F.col("lnk_AplAppUsr_Out.APL_RVWR_SK").alias("APL_RVWR_SK"),
    F.col("lnk_AplAppUsr_Out.CRT_USER_SK").alias("CRT_USER_SK"),
    F.col("lnk_AplAppUsr_Out.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("lnk_AplAppUsr_Out.APL_ACTVTY_METH_CD_SK").alias("APL_ACTVTY_METH_CD_SK"),
    F.col("lnk_AplAppUsr_Out.APL_ACTVTY_TYP_CD_SK").alias("APL_ACTVTY_TYP_CD_SK"),
    F.col("lnk_AplAppUsr_Out.ACTVTY_DT_SK").alias("ACTVTY_DT_SK"),
    F.col("lnk_AplAppUsr_Out.CRT_DTM").alias("CRT_DTM"),
    F.col("lnk_AplAppUsr_Out.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
    F.col("lnk_AplAppUsr_Out.APL_LVL_SEQ_NO").alias("APL_LVL_SEQ_NO"),
    F.col("lnk_AplAppUsr_Out.ACTVTY_SUM").alias("ACTVTY_SUM"),
    F.col("lnk_AplAppUsr_Out.APL_RVWR_ID").alias("APL_RVWR_ID"),
    F.col("lnk_AplAppUsr_Out.USER_ID").alias("USER_ID"),
    F.col("lnk_AplAppUsr_Out.USER_ID_1").alias("USER_ID_1"),
    F.col("Ref_AplActvtyMethCdLkup.TRGT_CD").alias("APL_ACTVTY_METH_CD"),
    F.col("Ref_AplActvtyMethCdLkup.TRGT_CD_NM").alias("APL_ACTVTY_METH_NM"),
    F.col("Ref_AplActvtyTypCdLkup.TRGT_CD").alias("APL_ACTVTY_TYP_CD"),
    F.col("Ref_AplActvtyTypCdLkup.TRGT_CD_NM").alias("APL_ACTVTY_TYP_NM")
)

# Add row numbers for the two special links UNK and NA
w = Window.orderBy(F.lit(1))
df_lkp_CDMA_Codes_rn = df_lkp_CDMA_Codes.withColumn("rownum", F.row_number().over(w))

# xfm_BusinessRules: Ink_IdsEdwAplActvtyMain_Out
df_Ink_IdsEdwAplActvtyMain_Out_pre = df_lkp_CDMA_Codes.filter(
    (F.col("APL_ACTVTY_SK") != 0) & (F.col("APL_ACTVTY_SK") != 1)
)

df_Ink_IdsEdwAplActvtyMain_Out = df_Ink_IdsEdwAplActvtyMain_Out_pre.select(
    F.col("APL_ACTVTY_SK").alias("APL_ACTVTY_SK"),
    F.when(trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(trim(F.col("SRC_SYS_CD"))).alias("SRC_SYS_CD"),
    F.col("APL_ID").alias("APL_ID"),
    F.col("SEQ_NO").alias("APL_ACTVTY_SEQ_NO"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("APL_SK").alias("APL_SK"),
    F.col("APL_RVWR_SK").alias("APL_RVWR_SK"),
    F.col("CRT_USER_SK").alias("CRT_USER_SK"),
    F.col("LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
    F.col("APL_LVL_SEQ_NO").alias("APL_ACTVTY_APL_LVL_SEQ_NO"),
    F.col("APL_RVWR_ID").alias("APL_ACTVTY_APL_RVWR_ID"),
    F.substring(F.col("CRT_DTM"), 1, 10).alias("APL_ACTVTY_CRT_DT_SK"),
    F.col("ACTVTY_DT_SK").alias("APL_ACTVTY_DT_SK"),
    F.when(F.length(trim(F.col("APL_ACTVTY_METH_CD"))) == 0, "NA").otherwise(trim(F.col("APL_ACTVTY_METH_CD"))).alias("APL_ACTVTY_METH_CD"),
    F.when(F.length(trim(F.col("APL_ACTVTY_METH_NM"))) == 0, "NA").otherwise(trim(F.col("APL_ACTVTY_METH_NM"))).alias("APL_ACTVTY_METH_NM"),
    Convert('^',' ',F.col("ACTVTY_SUM")).alias("APL_ACTVTY_SUM"),
    F.when(F.length(trim(F.col("APL_ACTVTY_TYP_CD"))) == 0, "NA").otherwise(trim(F.col("APL_ACTVTY_TYP_CD"))).alias("APL_ACTVTY_TYP_CD"),
    F.when(F.length(trim(F.col("APL_ACTVTY_TYP_NM"))) == 0, "NA").otherwise(trim(F.col("APL_ACTVTY_TYP_NM"))).alias("APL_ACTVTY_TYP_NM"),
    F.when(
        F.col("USER_ID").isNull() | (trim(F.col("USER_ID")) == ""),
        "NA"
    ).otherwise(trim(F.col("USER_ID"))).alias("CRT_USER_ID"),
    F.when(
        F.col("USER_ID_1").isNull() | (trim(F.col("USER_ID_1")) == ""),
        "NA"
    ).otherwise(trim(F.col("USER_ID_1"))).alias("LAST_UPDT_USER_ID"),
    rpad(
        FORMAT_DATE_EE(F.col("LAST_UPDT_DTM"), 'DB2','TIMESTAMP','CCYY-MM-DD'),
        10, " "
    ).alias("LAST_UPDT_DT_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_ACTVTY_METH_CD_SK").alias("APL_ACTVTY_METH_CD_SK"),
    F.col("APL_ACTVTY_TYP_CD_SK").alias("APL_ACTVTY_TYP_CD_SK")
)

# xfm_BusinessRules: UNK (rownum=1)
df_UNK_pre = df_lkp_CDMA_Codes_rn.filter(F.col("rownum") == 1)
df_UNK = df_UNK_pre.select(
    F.lit(0).alias("APL_ACTVTY_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("APL_ID"),
    F.lit(0).alias("APL_ACTVTY_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("APL_SK"),
    F.lit(0).alias("APL_RVWR_SK"),
    F.lit(0).alias("CRT_USER_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_USER_SK"),
    F.lit(0).alias("APL_ACTVTY_APL_LVL_SEQ_NO"),
    F.lit("UNK").alias("APL_ACTVTY_APL_RVWR_ID"),
    F.lit("1753-01-01").alias("APL_ACTVTY_CRT_DT_SK"),
    F.lit("1753-01-01").alias("APL_ACTVTY_DT_SK"),
    F.lit("UNK").alias("APL_ACTVTY_METH_CD"),
    F.lit("UNK").alias("APL_ACTVTY_METH_NM"),
    F.lit(None).alias("APL_ACTVTY_SUM"),
    F.lit("UNK").alias("APL_ACTVTY_TYP_CD"),
    F.lit("UNK").alias("APL_ACTVTY_TYP_NM"),
    F.lit("UNK").alias("CRT_USER_ID"),
    F.lit("UNK").alias("LAST_UPDT_USER_ID"),
    F.lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("APL_ACTVTY_METH_CD_SK"),
    F.lit(0).alias("APL_ACTVTY_TYP_CD_SK")
)

# xfm_BusinessRules: NA (rownum=1)
df_NA_pre = df_lkp_CDMA_Codes_rn.filter(F.col("rownum") == 1)
df_NA = df_NA_pre.select(
    F.lit(1).alias("APL_ACTVTY_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("APL_ID"),
    F.lit(0).alias("APL_ACTVTY_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("APL_SK"),
    F.lit(1).alias("APL_RVWR_SK"),
    F.lit(1).alias("CRT_USER_SK"),
    F.lit(1).alias("LAST_UPDT_USER_SK"),
    F.lit(0).alias("APL_ACTVTY_APL_LVL_SEQ_NO"),
    F.lit("NA").alias("APL_ACTVTY_APL_RVWR_ID"),
    F.lit("1753-01-01").alias("APL_ACTVTY_CRT_DT_SK"),
    F.lit("1753-01-01").alias("APL_ACTVTY_DT_SK"),
    F.lit("NA").alias("APL_ACTVTY_METH_CD"),
    F.lit("NA").alias("APL_ACTVTY_METH_NM"),
    F.lit(None).alias("APL_ACTVTY_SUM"),
    F.lit("NA").alias("APL_ACTVTY_TYP_CD"),
    F.lit("NA").alias("APL_ACTVTY_TYP_NM"),
    F.lit("NA").alias("CRT_USER_ID"),
    F.lit("NA").alias("LAST_UPDT_USER_ID"),
    F.lit("1753-01-01").alias("LAST_UPDT_DT_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("APL_ACTVTY_METH_CD_SK"),
    F.lit(1).alias("APL_ACTVTY_TYP_CD_SK")
)

# Funnel (fnl_NA_UNK)
df_fnl_NA_UNK = df_Ink_IdsEdwAplActvtyMain_Out.unionByName(df_UNK).unionByName(df_NA)

# Final select with rpad for char(10) columns
df_fnl_NA_UNK_final = df_fnl_NA_UNK.select(
    F.col("APL_ACTVTY_SK"),
    F.col("SRC_SYS_CD"),
    F.col("APL_ID"),
    F.col("APL_ACTVTY_SEQ_NO"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("APL_SK"),
    F.col("APL_RVWR_SK"),
    F.col("CRT_USER_SK"),
    F.col("LAST_UPDT_USER_SK"),
    F.col("APL_ACTVTY_APL_LVL_SEQ_NO"),
    F.col("APL_ACTVTY_APL_RVWR_ID"),
    rpad(F.col("APL_ACTVTY_CRT_DT_SK"), 10, " ").alias("APL_ACTVTY_CRT_DT_SK"),
    rpad(F.col("APL_ACTVTY_DT_SK"), 10, " ").alias("APL_ACTVTY_DT_SK"),
    F.col("APL_ACTVTY_METH_CD"),
    F.col("APL_ACTVTY_METH_NM"),
    F.col("APL_ACTVTY_SUM"),
    F.col("APL_ACTVTY_TYP_CD"),
    F.col("APL_ACTVTY_TYP_NM"),
    F.col("CRT_USER_ID"),
    F.col("LAST_UPDT_USER_ID"),
    rpad(F.col("LAST_UPDT_DT_SK"), 10, " ").alias("LAST_UPDT_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("APL_ACTVTY_METH_CD_SK"),
    F.col("APL_ACTVTY_TYP_CD_SK")
)

# seq_APL_ACTVTY_D_csv_load
write_files(
    df_fnl_NA_UNK_final,
    f"{adls_path}/load/APL_ACTVTY_D.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)