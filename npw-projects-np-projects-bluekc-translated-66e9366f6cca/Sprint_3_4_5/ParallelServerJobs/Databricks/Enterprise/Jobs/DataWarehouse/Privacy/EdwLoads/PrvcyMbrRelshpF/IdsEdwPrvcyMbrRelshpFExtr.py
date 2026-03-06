# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:
# MAGIC 
# MAGIC PROCESSING:
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Naren Garapaty                2/6/2007           FHP                              Originally Programmed                     devlEDW10                  Steph Goddard             02/21/2007
# MAGIC Steph Goddard                4/6/10                 3556 CDC                   brought to standards                           EnterpriseCurDevl         SAndrew                   2010-04-07
# MAGIC Steph Goddard                 7/15/10             TTR-630                      changed field name                            EnterpriseNewDevl       sANDREW                2010-10-01
# MAGIC                                                                                                         PRVCY_EXTRNL_ENTY_CLS_TYP_CD_S to PRVCY_EXTL_ENTY_CLS_TYP_CD_SK
# MAGIC 
# MAGIC Bhoomi Dasari                2/14/2013         TTR-1534                     Updated address fields from                 EnterpriseNewDevl     Kalyan Neelam            2013-03-01
# MAGIC                                                                                                        length of 40 to 80                              
# MAGIC 
# MAGIC Raj Mangalampally     12/18/2013               5114                        Original Programming                              EnterpriseWrhsDevl   Peter Marshall             1/6/2013     
# MAGIC                                                                                                     (Server to Parallel Conv)
# MAGIC 
# MAGIC Manasa Andru             2015-04-02     TFS - 1309 and 1310      Added logic to filter the 'NA' record          EnterpriseNewDevl    Kalyan Neelam              2015-04-03
# MAGIC                                                                                                     in the extract SQL of all the DB2 stages.

# MAGIC Write PRVCY_MBR_RELSHP_F.dat Data into a Sequential file for Load Job IdsEdwPrvcyMbrRelshpFLoad.
# MAGIC Read all the Data from IDS PRVCY_MBR_RELSHP  ; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwPrvcyMbrRelshpFExtr
# MAGIC This Job will extract records from PRVCY_EXTRNL_MBR from IDS and applies code Denormalization
# MAGIC Lookup Keys
# MAGIC 1PRVCY_EXTRNL_MBR_RELSHP_CD_SK
# MAGIC 2)PRVCY_EXTRNL_MBR_SK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


BeginCycle = get_widget_value('BeginCycle','')
EdwRunCycleDate = get_widget_value('EdwRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

extract_query_db2_PRVCY_EXTRNL_PRSN = f"""
SELECT distinct
PRSN.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
PRSN.FIRST_NM,
PRSN.MIDINIT,
PRSN.LAST_NM
FROM {IDSOwner}.PRVCY_EXTRNL_PRSN PRSN
"""
df_db2_PRVCY_EXTRNL_PRSN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_EXTRNL_PRSN)
    .load()
)

extract_query_db2_PRVCY_EXTRNL_MBR_D = f"""
SELECT
MBR.PRVCY_EXTRNL_MBR_SK,
MBR.PRVCY_EXTRNL_MBR_ID
FROM {EDWOwner}.PRVCY_EXTRNL_MBR_D MBR
"""
df_db2_PRVCY_EXTRNL_MBR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_PRVCY_EXTRNL_MBR_D)
    .load()
)

extract_query_db2_PRVCY_EXTRNL_ORIG = f"""
SELECT distinct
ORG.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
ORG.ORG_NM
FROM {IDSOwner}.PRVCY_EXTRNL_ORG ORG
where ORG.PRVCY_EXTRNL_ENTY_SK<>0 and
ORG.PRVCY_EXTRNL_ENTY_SK<>1
"""
df_db2_PRVCY_EXTRNL_ORIG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_EXTRNL_ORIG)
    .load()
)

extract_query_db2_MBR_D = f"""
SELECT
MBR_D.MBR_SK,
MBR_D.MBR_ID
FROM {EDWOwner}.MBR_D MBR_D
"""
df_db2_MBR_D = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_MBR_D)
    .load()
)

extract_query_CD_MPPNG_Extr1 = f"""
SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNKNOWN') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
df_CD_MPPNG_Extr1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPNG_Extr1)
    .load()
)

df_cpy_cd_mppng = df_CD_MPPNG_Extr1

df_Ref_PrvcyPrsnlRepTypCd_Lkup = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)
df_Ref_PrvcyMbrSrcCd_Lkup = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)
df_Ref_PrvcyExtrnlEntyAddrCd_Lkup = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)
df_Ref_PrvcyEntyClsTypCd_Lkup = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)
df_Ref_PrvcyMbrRelShpCd_Lkup = df_cpy_cd_mppng.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
)

extract_query_db2_PRVCY_MBR_RELSHP_in = f"""
SELECT
RELSHP.PRVCY_MBR_RELSHP_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
RELSHP.PRVCY_MBR_UNIQ_KEY,
RELSHP.SEQ_NO,
RELSHP.MBR_SK,
RELSHP.PRVCY_EXTRNL_MBR_SK,
RELSHP.CRT_DT_SK,
RELSHP.EFF_DT_SK,
RELSHP.TERM_DT_SK,
RELSHP.PRVCY_MBR_RELSHP_REP_CD_SK,
RELSHP.PRVCY_MBR_SRC_CD_SK,
RELSHP.SRC_SYS_LAST_UPDT_DT_SK,
SRC_SYS_LAST_UPDT_USER_SK,
RELSHP.PRVCY_PRSNL_REP_SK,
RELSHP.SRC_SYS_CD_SK
FROM {IDSOwner}.PRVCY_MBR_RELSHP RELSHP
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON RELSHP.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
where CD.TRGT_CD = 'FACETS'
and RELSHP.PRVCY_PRSNL_REP_SK not in (0,1)
"""
df_db2_PRVCY_MBR_RELSHP_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_MBR_RELSHP_in)
    .load()
)

extract_query_db2_PRVCY_EXTRNL_MBR_PRSN_in = f"""
SELECT
RELSHP.PRVCY_MBR_RELSHP_SK,
ENTY.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
ENTY.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK,
REP.SRC_SYS_CD_SK,
REP.PRVCY_PRSNL_REP_TYP_CD_SK,
REP.PRVCY_PRSNL_REP_ID
FROM
{IDSOwner}.PRVCY_MBR_RELSHP RELSHP,
{IDSOwner}.PRVCY_PRSNL_REP REP,
{IDSOwner}.PRVCY_EXTRNL_ENTY ENTY
WHERE
RELSHP.PRVCY_PRSNL_REP_SK = REP.PRVCY_PRSNL_REP_SK AND
REP.PRVCY_PRSNL_REP_UNIQ_KEY = ENTY.PRVCY_EXTRNL_ENTY_UNIQ_KEY AND
ENTY.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK <> 1
"""
df_db2_PRVCY_EXTRNL_MBR_PRSN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_EXTRNL_MBR_PRSN_in)
    .load()
)

df_lkp_PRVCY_EXTRNL_MBR = (
    df_db2_PRVCY_MBR_RELSHP_in.alias("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC")
    .join(
        df_db2_PRVCY_EXTRNL_MBR_PRSN_in.alias("lnk_IdsEdwPrvcyExtrnl_Lkup"),
        [
            F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.PRVCY_MBR_RELSHP_SK")
            == F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.PRVCY_MBR_RELSHP_SK"),
            F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.SRC_SYS_CD_SK")
            == F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.SRC_SYS_CD_SK"),
        ],
        how="left",
    )
    .select(
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.PRVCY_MBR_RELSHP_SK").alias("PRVCY_MBR_RELSHP_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.SEQ_NO").alias("SEQ_NO"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.PRVCY_PRSNL_REP_SK").alias("PRVCY_PRSNL_REP_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.PRVCY_MBR_RELSHP_REP_CD_SK").alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("lnk_IdsEdwPrvcyMbrRelshpExtr_InABC.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.PRVCY_MBR_RELSHP_SK").alias("PRVCY_MBR_RELSHP_SK_1"),
        F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.PRVCY_PRSNL_REP_TYP_CD_SK").alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
        F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.PRVCY_PRSNL_REP_ID").alias("PRVCY_PRSNL_REP_ID"),
        F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    )
)

df_xfm_businessRules1 = (
    df_lkp_PRVCY_EXTRNL_MBR.select(
        F.col("PRVCY_MBR_RELSHP_SK"),
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_MBR_UNIQ_KEY"),
        F.col("SEQ_NO"),
        F.col("MBR_SK"),
        F.col("PRVCY_EXTRNL_MBR_SK"),
        F.col("PRVCY_PRSNL_REP_SK"),
        F.col("CRT_DT_SK"),
        F.col("EFF_DT_SK"),
        F.col("TERM_DT_SK"),
        F.col("PRVCY_MBR_RELSHP_REP_CD_SK"),
        F.col("PRVCY_MBR_SRC_CD_SK"),
        F.col("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER_SK"),
        F.when(F.col("PRVCY_MBR_RELSHP_SK_1").isNull(), F.lit(0)).otherwise(F.col("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK")).alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        F.when(F.col("PRVCY_MBR_RELSHP_SK_1").isNull(), F.lit(0)).otherwise(F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY")).alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("PRVCY_PRSNL_REP_TYP_CD_SK").alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
        F.when(F.col("PRVCY_MBR_RELSHP_SK_1").isNull(), F.lit("NA")).otherwise(F.col("PRVCY_PRSNL_REP_ID")).alias("PRVCY_PRSNL_REP_ID"),
        F.when(F.col("SRC_SYS_CD_SK").isNull(), F.lit(0)).otherwise(F.col("SRC_SYS_CD_SK")).alias("SRC_SYS_CD_SK"),
    )
)

extract_query_db2_PRVCY_EXTRNL_ENTY_ADDR_in = f"""
SELECT DISTINCT
ADDR.SRC_SYS_CD_SK,
ADDR.PRVCY_EXTRNL_ENTY_UNIQ_KEY,
ADDR.ADDR_LN_1,
ADDR.ADDR_LN_2,
ADDR.ADDR_LN_3,
ADDR.CITY_NM,
ADDR.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK,
ADDR.POSTAL_CD,
ADDR.CNTY_NM,
MPPNG.TRGT_CD
FROM
{IDSOwner}.PRVCY_EXTRNL_ENTY_ADDR ADDR ,
{IDSOwner}.CD_MPPNG MPPNG
WHERE
ADDR.PRVCY_EXTL_ENTY_ADDR_TYP_CD_SK = MPPNG.CD_MPPNG_SK
and  MPPNG.TRGT_CD not in ( 'UNK' , 'NA')
and ADDR.PRVCY_EXTRNL_ENTY_ADDR_SK <> 1
"""
df_db2_PRVCY_EXTRNL_ENTY_ADDR_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_PRVCY_EXTRNL_ENTY_ADDR_in)
    .load()
)

df_xfm_Logic = df_db2_PRVCY_EXTRNL_ENTY_ADDR_in.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    F.col("ADDR_LN_1"),
    F.col("ADDR_LN_2"),
    F.col("ADDR_LN_3"),
    F.col("CITY_NM"),
    F.col("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
    F.col("POSTAL_CD"),
    F.col("CNTY_NM"),
    F.when(
        (
            (trim(F.col("TRGT_CD")) == "HOME")
            & (trim(F.col("TRGT_CD")) == "WORK")
        ),
        "WORK",
    )
    .when(trim(F.col("TRGT_CD")) == "HOME", "HOME")
    .when(trim(F.col("TRGT_CD")) == "WORK", "WORK")
    .otherwise(F.col("TRGT_CD"))
    .alias("TRGT_CD"),
)

df_rdp_Prvcy_Extrnl_Enty_Addr = dedup_sort(
    df_xfm_Logic,
    partition_cols=["PRVCY_EXTRNL_ENTY_UNIQ_KEY"],
    sort_cols=[("PRVCY_EXTRNL_ENTY_UNIQ_KEY", "A"), ("TRGT_CD", "A")],
)

df_lkp_PRVCY_EXTRNL_ADDR = (
    df_xfm_businessRules1.alias("lnk_PrvcyMbrRelshpExrnl_Out")
    .join(
        df_rdp_Prvcy_Extrnl_Enty_Addr.alias("lnk_rdpPrvcyExtrnlEntyAddr_Lkup"),
        [
            F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_EXTRNL_ENTY_UNIQ_KEY")
            == F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
            F.col("lnk_PrvcyMbrRelshpExrnl_Out.SRC_SYS_CD_SK")
            == F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.SRC_SYS_CD_SK"),
        ],
        how="left",
    )
    .select(
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_MBR_RELSHP_SK").alias("PRVCY_MBR_RELSHP_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.SEQ_NO").alias("SEQ_NO"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.MBR_SK").alias("MBR_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_PRSNL_REP_SK").alias("PRVCY_PRSNL_REP_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.CRT_DT_SK").alias("CRT_DT_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.TERM_DT_SK").alias("TERM_DT_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_MBR_RELSHP_REP_CD_SK").alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK").alias("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_PRSNL_REP_TYP_CD_SK").alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.PRVCY_PRSNL_REP_ID").alias("PRVCY_PRSNL_REP_ID"),
        F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1"),
        F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.ADDR_LN_1").alias("ADDR_LN_1"),
        F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.ADDR_LN_2").alias("ADDR_LN_2"),
        F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.ADDR_LN_3").alias("ADDR_LN_3"),
        F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.CITY_NM").alias("CITY_NM"),
        F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK").alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.POSTAL_CD").alias("POSTAL_CD"),
        F.col("lnk_rdpPrvcyExtrnlEntyAddr_Lkup.CNTY_NM").alias("CNTY_NM"),
        F.col("lnk_PrvcyMbrRelshpExrnl_Out.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK_alias"),
    )
)

df_xfm_businessRules2 = (
    df_lkp_PRVCY_EXTRNL_ADDR.select(
        F.col("PRVCY_MBR_RELSHP_SK"),
        F.col("SRC_SYS_CD"),
        F.col("PRVCY_MBR_UNIQ_KEY"),
        F.col("SEQ_NO"),
        F.col("MBR_SK"),
        F.col("PRVCY_EXTRNL_MBR_SK"),
        F.col("PRVCY_PRSNL_REP_SK"),
        F.col("CRT_DT_SK"),
        F.col("EFF_DT_SK"),
        F.col("TERM_DT_SK"),
        F.col("PRVCY_MBR_RELSHP_REP_CD_SK"),
        F.col("PRVCY_MBR_SRC_CD_SK"),
        F.col("SRC_SYS_LAST_UPDT_DT_SK"),
        F.col("SRC_SYS_LAST_UPDT_USER_SK"),
        F.col("PRVCY_EXTL_ENTY_CLS_TYP_CD_SK"),
        F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
        F.col("PRVCY_PRSNL_REP_TYP_CD_SK"),
        F.col("PRVCY_PRSNL_REP_ID"),
        F.when(F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1").isNull(), F.lit("UNKNOWN")).otherwise(F.col("ADDR_LN_1")).alias("ADDR_LN_1"),
        F.when(F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1").isNull(), F.lit("UNKNOWN")).otherwise(F.col("ADDR_LN_2")).alias("ADDR_LN_2"),
        F.when(F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1").isNull(), F.lit("UNKNOWN")).otherwise(F.col("ADDR_LN_3")).alias("ADDR_LN_3"),
        F.when(F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1").isNull(), F.lit("UNKNOWN")).otherwise(F.col("CITY_NM")).alias("CITY_NM"),
        F.when(F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1").isNull(), F.lit(0)).otherwise(F.col("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK")).alias("PRVCY_EXTL_ENTY_ADDR_ST_CD_SK"),
        F.when(F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1").isNull(), F.lit("UNKNOWN")).otherwise(F.col("POSTAL_CD")).alias("POSTAL_CD"),
        F.when(F.col("PRVCY_EXTRNL_ENTY_UNIQ_KEY_1").isNull(), F.lit("UNKNOWN")).otherwise(F.col("CNTY_NM")).alias("CNTY_NM"),
        F.col("SRC_SYS_CD_SK_alias").alias("SRC_SYS_CD_SK"),
    )
)

df_lkp_codes_stage = df_xfm_businessRules2.alias("lnk_PrvcyExtrnlEntyAddr_Out")

df_lkp_codes_1 = df_lkp_codes_stage.join(
    df_Ref_PrvcyMbrSrcCd_Lkup.alias("Ref_PrvcyMbrSrcCd_Lkup"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_MBR_SRC_CD_SK")
    == F.col("Ref_PrvcyMbrSrcCd_Lkup.CD_MPPNG_SK"),
    how="left",
)
df_lkp_codes_2 = df_lkp_codes_1.join(
    df_Ref_PrvcyPrsnlRepTypCd_Lkup.alias("Ref_PrvcyPrsnlRepTypCd_Lkup"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_PRSNL_REP_TYP_CD_SK")
    == F.col("Ref_PrvcyPrsnlRepTypCd_Lkup.CD_MPPNG_SK"),
    how="left",
)
df_lkp_codes_3 = df_lkp_codes_2.join(
    df_db2_PRVCY_EXTRNL_PRSN.alias("lnk_IdsEdwPrvcyExtrnl_Lkup"),
    # The JSON had extra conditions referencing MBR_RELSHP_SK, but they don't exist in that table,
    # so we only join on PRVCY_EXTRNL_ENTY_UNIQ_KEY matching if possible:
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_EXTRNL_ENTY_UNIQ_KEY")
    == F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    how="left",
)
df_lkp_codes_4 = df_lkp_codes_3.join(
    df_db2_PRVCY_EXTRNL_MBR_D.alias("lnk_IdsEdwPrvcyExtrnlMbrD_Lkup"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_EXTRNL_MBR_SK")
    == F.col("lnk_IdsEdwPrvcyExtrnlMbrD_Lkup.PRVCY_EXTRNL_MBR_SK"),
    how="left",
)
df_lkp_codes_5 = df_lkp_codes_4.join(
    df_db2_PRVCY_EXTRNL_ORIG.alias("lnk_IdsEdwExtrnlOrig_Lkup"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_EXTRNL_ENTY_UNIQ_KEY")
    == F.col("lnk_IdsEdwExtrnlOrig_Lkup.PRVCY_EXTRNL_ENTY_UNIQ_KEY"),
    how="left",
)
df_lkp_codes_6 = df_lkp_codes_5.join(
    df_db2_MBR_D.alias("lnk_IdsEdwMbr_Lkup"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.MBR_SK")
    == F.col("lnk_IdsEdwMbr_Lkup.MBR_SK"),
    how="left",
)
df_lkp_codes_7 = df_lkp_codes_6.join(
    df_Ref_PrvcyExtrnlEntyAddrCd_Lkup.alias("Ref_PrvcyExtrnlEntyAddrCd_Lkup"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_EXTL_ENTY_ADDR_ST_CD_SK")
    == F.col("Ref_PrvcyExtrnlEntyAddrCd_Lkup.CD_MPPNG_SK"),
    how="left",
)
df_lkp_codes_8 = df_lkp_codes_7.join(
    df_Ref_PrvcyEntyClsTypCd_Lkup.alias("Ref_PrvcyEntyClsTypCd_Lkup"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_EXTL_ENTY_CLS_TYP_CD_SK")
    == F.col("Ref_PrvcyEntyClsTypCd_Lkup.CD_MPPNG_SK"),
    how="left",
)
df_lkp_codes_9 = df_lkp_codes_8.join(
    df_Ref_PrvcyMbrRelShpCd_Lkup.alias("Ref_PrvcyMbrRelShpCd_Lkup"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_MBR_RELSHP_REP_CD_SK")
    == F.col("Ref_PrvcyMbrRelShpCd_Lkup.CD_MPPNG_SK"),
    how="left",
)

df_lkp_codes = df_lkp_codes_9.select(
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_MBR_RELSHP_SK").alias("PRVCY_MBR_RELSHP_SK"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.SEQ_NO").alias("PRVCY_MBR_RELSHP_SEQ_NO"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.MBR_SK").alias("MBR_SK"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.ADDR_LN_1").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_1"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.ADDR_LN_2").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_2"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.ADDR_LN_3").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_3"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.CITY_NM").alias("PRVCY_EXTRNL_ENTY_CITY_NM"),
    F.col("Ref_PrvcyExtrnlEntyAddrCd_Lkup.TRGT_CD").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
    F.col("Ref_PrvcyExtrnlEntyAddrCd_Lkup.TRGT_CD_NM").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.CNTY_NM").alias("PRVCY_EXTRNL_ENTY_CNTY_NM"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.CRT_DT_SK").alias("PRVCY_MBR_RELSHP_CRT_DT_SK"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.EFF_DT_SK").alias("PRVCY_MBR_RELSHP_EFF_DT_SK"),
    F.col("Ref_PrvcyMbrRelShpCd_Lkup.TRGT_CD").alias("PRVCY_MBR_RELSHP_REP_CD"),
    F.col("Ref_PrvcyMbrRelShpCd_Lkup.TRGT_CD_NM").alias("PRVCY_MBR_RELSHP_REP_NM"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.TERM_DT_SK").alias("PRVCY_MBR_RELSHP_TERM_DT_SK"),
    F.col("lnk_IdsEdwMbr_Lkup.MBR_ID").alias("PRVCY_MBR_ID"),
    F.col("Ref_PrvcyMbrSrcCd_Lkup.TRGT_CD").alias("PRVCY_MBR_SRC_CD"),
    F.col("Ref_PrvcyMbrSrcCd_Lkup.TRGT_CD_NM").alias("PRVCY_MBR_SRC_NM"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_PRSNL_REP_ID").alias("PRVCY_PRSNL_REP_ID"),
    F.col("Ref_PrvcyPrsnlRepTypCd_Lkup.TRGT_CD").alias("PRVCY_PRSNL_REP_TYP_CD"),
    F.col("Ref_PrvcyPrsnlRepTypCd_Lkup.TRGT_CD_NM").alias("PRVCY_PRSNL_REP_TYP_NM"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_EXTRNL_ENTY_UNIQ_KEY").alias("PRVCY_PRSNL_REP_UNIQ_KEY"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_MBR_RELSHP_REP_CD_SK").alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.PRVCY_PRSNL_REP_TYP_CD_SK").alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
    F.col("Ref_PrvcyEntyClsTypCd_Lkup.TRGT_CD").alias("PRVCY_ENTY_CLS_TYP_CD"),
    F.col("Ref_PrvcyEntyClsTypCd_Lkup.TRGT_CD_NM").alias("PRVCY_ENTY_CLS_TYP_NM"),
    F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.FIRST_NM").alias("FIRST_NM"),
    F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.MIDINIT").alias("MIDINIT"),
    F.col("lnk_IdsEdwPrvcyExtrnl_Lkup.LAST_NM").alias("LAST_NM"),
    F.col("lnk_IdsEdwPrvcyExtrnlMbrD_Lkup.PRVCY_EXTRNL_MBR_ID").alias("PRVCY_EXTRNL_MBR_ID"),
    F.col("lnk_IdsEdwExtrnlOrig_Lkup.ORG_NM").alias("ORG_NM"),
    F.col("lnk_PrvcyExtrnlEntyAddr_Out.POSTAL_CD").alias("POSTAL_CD"),
)

df_xfrm_BusinessLogic = df_lkp_codes.select(
    F.col("PRVCY_MBR_RELSHP_SK").alias("PRVCY_MBR_RELSHP_SK"),
    F.when(F.trim(F.col("SRC_SYS_CD")) == "", F.lit("UNK")).otherwise(F.col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    F.col("PRVCY_MBR_UNIQ_KEY").alias("PRVCY_MBR_UNIQ_KEY"),
    F.col("PRVCY_MBR_RELSHP_SEQ_NO").alias("PRVCY_MBR_RELSHP_SEQ_NO"),
    F.lit(EdwRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EdwRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PRVCY_EXTRNL_MBR_SK").alias("PRVCY_EXTRNL_MBR_SK"),
    F.when(
        (F.col("PRVCY_ENTY_CLS_TYP_CD") == "PRSN")
        & (F.col("MIDINIT").isNotNull()),
        F.concat_ws(" ", F.col("FIRST_NM"), F.col("MIDINIT"), F.col("LAST_NM")),
    )
    .when(
        (F.col("PRVCY_ENTY_CLS_TYP_CD") == "PRSN") & (F.col("MIDINIT").isNull()),
        F.concat_ws(" ", F.col("FIRST_NM"), F.col("LAST_NM")),
    )
    .when(
        F.col("PRVCY_ENTY_CLS_TYP_CD") == "ORG",
        F.when(F.length(F.col("ORG_NM")) > 1, F.col("ORG_NM")).otherwise(F.lit("  ")),
    )
    .otherwise(F.lit(" "))
    .alias("PRVCY_EXTRNL_ENTY_FULL_NM"),
    F.when(
        (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_1")) == "")
        | (F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_1").isNull()),
        F.lit(""),
    ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_1"))
    .alias("PRVCY_EXTRNL_ENTY_ADDR_LN_1"),
    F.when(
        (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_2")) == "")
        | (F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_2").isNull()),
        F.lit(""),
    ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_2"))
    .alias("PRVCY_EXTRNL_ENTY_ADDR_LN_2"),
    F.when(
        (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_3")) == "")
        | (F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_3").isNull()),
        F.lit(""),
    ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_3"))
    .alias("PRVCY_EXTRNL_ENTY_ADDR_LN_3"),
    F.when(
        (F.trim(F.col("PRVCY_EXTRNL_ENTY_CITY_NM")) == "")
        | (F.col("PRVCY_EXTRNL_ENTY_CITY_NM").isNull()),
        F.lit(""),
    ).otherwise(F.col("PRVCY_EXTRNL_ENTY_CITY_NM"))
    .alias("PRVCY_EXTRNL_ENTY_CITY_NM"),
    F.when(
        (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD")) == "")
        | (F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD").isNull()),
        F.lit(""),
    ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"))
    .alias("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
    F.when(
        (F.trim(F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_NM")) == "")
        | (F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_NM").isNull()),
        F.lit(""),
    ).otherwise(F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"))
    .alias("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
    F.when(
        (
            (F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD") != "1")
            | (F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD") != "0")
        )
        & (F.col("POSTAL_CD").isNotNull())
        & F.expr("Num(POSTAL_CD[1,5])=true"),  # DataStage-like expression
    , F.expr("POSTAL_CD[1,5]")).otherwise(F.lit(" ")).alias("PRVCY_EXTRNL_ENTY_ZIP_CD_5"),
    F.when(
        (
            (F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD") != "1")
            | (F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD") != "0")
        )
        & (F.expr("POSTAL_CD = '...1N0N...'") )
        & (F.expr("POSTAL_CD[6,4] = '...1N0N...'"))
        ,
        F.expr("POSTAL_CD[6,4]")
    )
    .when(
        (
            (F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD") != "1")
            | (F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD") != "0")
        )
        & (F.expr("POSTAL_CD = '...1N0N...'"))
        & (F.expr("POSTAL_CD[7,3] = '...1N0N...'"))
        ,
        F.expr("POSTAL_CD[7,3]")
    )
    .otherwise(F.lit(" "))
    .alias("PRVCY_EXTRNL_ENTY_ZIP_CD_4"),
    F.when(
        (F.trim(F.col("PRVCY_EXTRNL_ENTY_CNTY_NM")) == "")
        | (F.col("PRVCY_EXTRNL_ENTY_CNTY_NM").isNull()),
        F.lit(""),
    ).otherwise(F.col("PRVCY_EXTRNL_ENTY_CNTY_NM"))
    .alias("PRVCY_EXTRNL_ENTY_CNTY_NM"),
    F.col("PRVCY_MBR_RELSHP_CRT_DT_SK").alias("PRVCY_MBR_RELSHP_CRT_DT_SK"),
    F.col("PRVCY_MBR_RELSHP_EFF_DT_SK").alias("PRVCY_MBR_RELSHP_EFF_DT_SK"),
    F.when(F.col("PRVCY_MBR_RELSHP_REP_CD").isNotNull(), F.col("PRVCY_MBR_RELSHP_REP_CD")).otherwise(F.lit("NA")).alias("PRVCY_MBR_RELSHP_REP_CD"),
    F.when(F.col("PRVCY_MBR_RELSHP_REP_NM").isNotNull(), F.col("PRVCY_MBR_RELSHP_REP_NM")).otherwise(F.lit("NA")).alias("PRVCY_MBR_RELSHP_REP_NM"),
    F.col("PRVCY_MBR_RELSHP_TERM_DT_SK").alias("PRVCY_MBR_RELSHP_TERM_DT_SK"),
    F.when(
        (F.col("PRVCY_MBR_SRC_CD") == "FACETS")
        & (F.col("PRVCY_MBR_ID").isNotNull()),
        F.col("PRVCY_MBR_ID"),
    )
    .when(
        (F.col("PRVCY_MBR_SRC_CD") != "FACETS")
        & (F.col("PRVCY_EXTRNL_MBR_ID").isNotNull()),
        F.col("PRVCY_EXTRNL_MBR_ID"),
    )
    .otherwise(F.lit("NA"))
    .alias("PRVCY_MBR_ID"),
    F.when(F.col("PRVCY_MBR_SRC_CD").isNotNull(), F.col("PRVCY_MBR_SRC_CD")).otherwise(F.lit("NA")).alias("PRVCY_MBR_SRC_CD"),
    F.when(F.col("PRVCY_MBR_SRC_NM").isNotNull(), F.col("PRVCY_MBR_SRC_NM")).otherwise(F.lit("NA")).alias("PRVCY_MBR_SRC_NM"),
    F.when(F.col("PRVCY_PRSNL_REP_ID").isNull(), F.lit("")).otherwise(F.col("PRVCY_PRSNL_REP_ID")).alias("PRVCY_PRSNL_REP_ID"),
    F.when(F.col("PRVCY_PRSNL_REP_TYP_CD").isNotNull(), F.col("PRVCY_PRSNL_REP_TYP_CD")).otherwise(F.lit("NA")).alias("PRVCY_PRSNL_REP_TYP_CD"),
    F.when(F.col("PRVCY_PRSNL_REP_TYP_NM").isNotNull(), F.col("PRVCY_PRSNL_REP_TYP_NM")).otherwise(F.lit("NA")).alias("PRVCY_PRSNL_REP_TYP_NM"),
    F.when(F.col("PRVCY_PRSNL_REP_UNIQ_KEY").isNull(), F.lit(0)).otherwise(F.col("PRVCY_PRSNL_REP_UNIQ_KEY")).alias("PRVCY_PRSNL_REP_UNIQ_KEY"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK").alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_MBR_SRC_CD_SK").alias("PRVCY_MBR_SRC_CD_SK"),
    F.col("PRVCY_MBR_RELSHP_REP_CD_SK").alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
    F.col("PRVCY_PRSNL_REP_TYP_CD_SK").alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
)

df_mainData = df_xfrm_BusinessLogic.filter(
    (F.col("PRVCY_MBR_RELSHP_SK") != 0) & (F.col("PRVCY_MBR_RELSHP_SK") != 1)
)
df_UNK = df_xfrm_BusinessLogic.filter(
    ((F.col("PRVCY_MBR_RELSHP_SK") != 0) & (F.col("PRVCY_MBR_RELSHP_SK") != 1))
    == False  # no real row pass here, but DataStage used a constraint with row number=1
).limit(0)  # emulate no real pass, then union a synthetic row
df_UNK = df_UNK.select(
    F.lit(0).alias("PRVCY_MBR_RELSHP_SK"),
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit(0).alias("PRVCY_MBR_UNIQ_KEY"),
    F.lit(0).alias("PRVCY_MBR_RELSHP_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(0).alias("MBR_SK"),
    F.lit(0).alias("PRVCY_EXTRNL_MBR_SK"),
    F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_FULL_NM"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_1"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_2"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_3"),
    F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_CITY_NM"),
    F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
    F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ZIP_CD_5"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ZIP_CD_4"),
    F.lit("UNK").alias("PRVCY_EXTRNL_ENTY_CNTY_NM"),
    F.lit("1753-01-01").alias("PRVCY_MBR_RELSHP_CRT_DT_SK"),
    F.lit("1753-01-01").alias("PRVCY_MBR_RELSHP_EFF_DT_SK"),
    F.lit("UNK").alias("PRVCY_MBR_RELSHP_REP_CD"),
    F.lit("UNK").alias("PRVCY_MBR_RELSHP_REP_NM"),
    F.lit("1753-01-01").alias("PRVCY_MBR_RELSHP_TERM_DT_SK"),
    F.lit("UNK").alias("PRVCY_MBR_ID"),
    F.lit("UNK").alias("PRVCY_MBR_SRC_CD"),
    F.lit("UNK").alias("PRVCY_MBR_SRC_NM"),
    F.lit("UNK").alias("PRVCY_PRSNL_REP_ID"),
    F.lit("UNK").alias("PRVCY_PRSNL_REP_TYP_CD"),
    F.lit("UNK").alias("PRVCY_PRSNL_REP_TYP_NM"),
    F.lit(0).alias("PRVCY_PRSNL_REP_UNIQ_KEY"),
    F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit(0).alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("PRVCY_MBR_SRC_CD_SK"),
    F.lit(0).alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
    F.lit(0).alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
)
df_NA = df_UNK.limit(0)  # then select with different default
df_NA = df_NA.select(
    F.lit(1).alias("PRVCY_MBR_RELSHP_SK"),
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit(1).alias("PRVCY_MBR_UNIQ_KEY"),
    F.lit(0).alias("PRVCY_MBR_RELSHP_SEQ_NO"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(1).alias("MBR_SK"),
    F.lit(1).alias("PRVCY_EXTRNL_MBR_SK"),
    F.lit("NA").alias("PRVCY_EXTRNL_ENTY_FULL_NM"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_1"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_2"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ADDR_LN_3"),
    F.lit("NA").alias("PRVCY_EXTRNL_ENTY_CITY_NM"),
    F.lit("NA").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
    F.lit("NA").alias("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ZIP_CD_5"),
    F.lit("").alias("PRVCY_EXTRNL_ENTY_ZIP_CD_4"),
    F.lit("NA").alias("PRVCY_EXTRNL_ENTY_CNTY_NM"),
    F.lit("1753-01-01").alias("PRVCY_MBR_RELSHP_CRT_DT_SK"),
    F.lit("1753-01-01").alias("PRVCY_MBR_RELSHP_EFF_DT_SK"),
    F.lit("NA").alias("PRVCY_MBR_RELSHP_REP_CD"),
    F.lit("NA").alias("PRVCY_MBR_RELSHP_REP_NM"),
    F.lit("1753-01-01").alias("PRVCY_MBR_RELSHP_TERM_DT_SK"),
    F.lit("NA").alias("PRVCY_MBR_ID"),
    F.lit("NA").alias("PRVCY_MBR_SRC_CD"),
    F.lit("NA").alias("PRVCY_MBR_SRC_NM"),
    F.lit("NA").alias("PRVCY_PRSNL_REP_ID"),
    F.lit("NA").alias("PRVCY_PRSNL_REP_TYP_CD"),
    F.lit("NA").alias("PRVCY_PRSNL_REP_TYP_NM"),
    F.lit(1).alias("PRVCY_PRSNL_REP_UNIQ_KEY"),
    F.lit("1753-01-01").alias("SRC_SYS_LAST_UPDT_DT_SK"),
    F.lit(1).alias("SRC_SYS_LAST_UPDT_USER_SK"),
    F.lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("PRVCY_MBR_SRC_CD_SK"),
    F.lit(1).alias("PRVCY_MBR_RELSHP_REP_CD_SK"),
    F.lit(1).alias("PRVCY_PRSNL_REP_TYP_CD_SK"),
)

df_fnl_UNK_NA = df_mainData.unionByName(df_UNK).unionByName(df_NA)

df_fnl_select = df_fnl_UNK_NA.select(
    F.col("PRVCY_MBR_RELSHP_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PRVCY_MBR_UNIQ_KEY"),
    F.col("PRVCY_MBR_RELSHP_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("MBR_SK"),
    F.col("PRVCY_EXTRNL_MBR_SK"),
    F.col("PRVCY_EXTRNL_ENTY_FULL_NM"),
    F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_1"),
    F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_2"),
    F.col("PRVCY_EXTRNL_ENTY_ADDR_LN_3"),
    F.col("PRVCY_EXTRNL_ENTY_CITY_NM"),
    F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_CD"),
    F.col("PRVCY_EXTRNL_ENTY_ADDR_ST_NM"),
    F.col("PRVCY_EXTRNL_ENTY_ZIP_CD_5"),
    F.col("PRVCY_EXTRNL_ENTY_ZIP_CD_4"),
    F.col("PRVCY_EXTRNL_ENTY_CNTY_NM"),
    F.col("PRVCY_MBR_RELSHP_CRT_DT_SK"),
    F.col("PRVCY_MBR_RELSHP_EFF_DT_SK"),
    F.col("PRVCY_MBR_RELSHP_REP_CD"),
    F.col("PRVCY_MBR_RELSHP_REP_NM"),
    F.col("PRVCY_MBR_RELSHP_TERM_DT_SK"),
    F.col("PRVCY_MBR_ID"),
    F.col("PRVCY_MBR_SRC_CD"),
    F.col("PRVCY_MBR_SRC_NM"),
    F.col("PRVCY_PRSNL_REP_ID"),
    F.col("PRVCY_PRSNL_REP_TYP_CD"),
    F.col("PRVCY_PRSNL_REP_TYP_NM"),
    F.col("PRVCY_PRSNL_REP_UNIQ_KEY"),
    F.col("SRC_SYS_LAST_UPDT_DT_SK"),
    F.col("SRC_SYS_LAST_UPDT_USER_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PRVCY_MBR_SRC_CD_SK"),
    F.col("PRVCY_MBR_RELSHP_REP_CD_SK"),
    F.col("PRVCY_PRSNL_REP_TYP_CD_SK"),
)

df_fnl = (
    df_fnl_select
    .withColumn("CRT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " "))
    .withColumn("PRVCY_MBR_RELSHP_EFF_DT_SK", F.rpad(F.col("PRVCY_MBR_RELSHP_EFF_DT_SK"), 10, " "))
    .withColumn("PRVCY_MBR_RELSHP_TERM_DT_SK", F.rpad(F.col("PRVCY_MBR_RELSHP_TERM_DT_SK"), 10, " "))
    .withColumn("PRVCY_EXTRNL_ENTY_ZIP_CD_5", F.rpad(F.col("PRVCY_EXTRNL_ENTY_ZIP_CD_5"), 5, " "))
    .withColumn("PRVCY_EXTRNL_ENTY_ZIP_CD_4", F.rpad(F.col("PRVCY_EXTRNL_ENTY_ZIP_CD_4"), 4, " "))
    .withColumn("SRC_SYS_LAST_UPDT_DT_SK", F.rpad(F.col("SRC_SYS_LAST_UPDT_DT_SK"), 10, " "))
)

write_files(
    df_fnl,
    f"{adls_path}/load/PRVCY_MBR_RELSHP_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)