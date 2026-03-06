# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                        Date               User Story/Project    Change Description                                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    ------------------------------   -------------------------------------------------------------------------------    ----------------------------------   ---------------------------------    -------------------------       
# MAGIC Aditya Raju                      08/20/2013     5114                        Original Programming(Server to Parallel)                     IntegrateWrhsDevl       Peter Marshall               10/18/2013
# MAGIC Hugh Sisson                    2022-11-14      US547705               Updated primary query to eliminate duplicate rows     IntegrateDev2              Goutham K                     2/27/2023
# MAGIC                                                                                                 in the db2_P_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN_in

# MAGIC Read all the Data fromIDS MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Job Name: IdsDmMbrshDmClsPlnCustSvcIdCardPhnExtr
# MAGIC Write MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN Data into a Sequential file for Load Job IdsDmCustSvcldCardPhnLoad.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
ExtractRunCycle = get_widget_value('ExtractRunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrDate = get_widget_value('CurrDate','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_db2_P_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
A.GRP_ID,
A.SUBGRP_ID,
A.CLS_ID,
A.CLS_PLN_ID,
A.PROD_ID,
A.CLS_PLN_DTL_PROD_CAT_CD_SK
FROM {IDSOwner}.CLS_PLN_CUST_SVC_ID_CARD_PHN A
LEFT JOIN {IDSOwner}.CD_MPPNG CD
ON A.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
WHERE 
A.CLS_PLN_CUST_SVC_IDCARD_PHN_SK NOT IN (0, 1) 
AND A.LAST_UPDT_RUN_CYC_EXCTN_SK >= {ExtractRunCycle}
AND A.EFF_DT_SK  <= '{CurrDate}'
AND A.TERM_DT_SK >= '{CurrDate}'
AND A.CLS_PLN_DTL_PROD_CAT_CD_SK <> 0
order by A.GRP_ID,
A.SUBGRP_ID,
A.CLS_ID,
A.CLS_PLN_ID,
A.PROD_ID,
A.CLS_PLN_DTL_PROD_CAT_CD_SK
"""
    )
    .load()
)

df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT 
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM
from {IDSOwner}.CD_MPPNG
"""
    )
    .load()
)

df_db2_Cust_svc_tllfr_phn_no_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
A.GRP_ID,
A.SUBGRP_ID,
A.CLS_ID,
A.CLS_PLN_ID,
A.PROD_ID,
max(A.EFF_DT_SK) EFF_DT_SK
FROM {IDSOwner}.CLS_PLN_CUST_SVC_ID_CARD_PHN A,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
A.CUST_SVC_ID_CARD_PHN_USE_CD_SK = CD_MPPNG.CD_MPPNG_SK
and CD_MPPNG.TRGT_CD = 'CUSTSVCWATTS'
and A.EFF_DT_SK <= '{CurrDate}'
and A.TERM_DT_SK >= '{CurrDate}'
GROUP BY
A.GRP_ID,
A.SUBGRP_ID,
A.CLS_ID,
A.CLS_PLN_ID,
A.PROD_ID
"""
    )
    .load()
)

df_db2_Cust_svc_local_max_dt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
A.GRP_ID,
A.SUBGRP_ID,
A.CLS_ID,
A.CLS_PLN_ID,
A.PROD_ID,
max(A.EFF_DT_SK) EFF_DT_SK
FROM {IDSOwner}.CLS_PLN_CUST_SVC_ID_CARD_PHN A,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
A.CUST_SVC_ID_CARD_PHN_USE_CD_SK = CD_MPPNG.CD_MPPNG_SK
and CD_MPPNG.TRGT_CD = 'CUSTSVCLOCAL'
and A.EFF_DT_SK <= '{CurrDate}'
and A.TERM_DT_SK >= '{CurrDate}'
GROUP BY
A.GRP_ID,
A.SUBGRP_ID,
A.CLS_ID,
A.CLS_PLN_ID,
A.PROD_ID
"""
    )
    .load()
)

df_db2_Cust_svc_tllfr_max_dt_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT
DISTINCT 
A.GRP_ID,
A.SUBGRP_ID,
A.CLS_ID,
A.CLS_PLN_ID,
A.PROD_ID,
A.EFF_DT_SK,
B.CUST_SVC_ID_CARD_PHN_SK,
A.CUST_SVC_ID_CARD_PHN_KEY,
A.CUST_SVC_ID_CARD_PHN_USE_CD,
B.CUST_SVC_ID_CARD_PHN_AREA_CD,
B.CUST_SVC_ID_CARD_PHN_EXCH_NO,
B.CUST_SVC_ID_CARD_PHN_LN_NO
FROM {IDSOwner}.CLS_PLN_CUST_SVC_ID_CARD_PHN A,
{IDSOwner}.CUST_SVC_ID_CARD_PHN B,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
A.CUST_SVC_ID_CARD_PHN_SK = B.CUST_SVC_ID_CARD_PHN_SK
and A.CUST_SVC_ID_CARD_PHN_USE_CD_SK = CD_MPPNG.CD_MPPNG_SK
and CD_MPPNG.TRGT_CD = 'CUSTSVCWATTS'
and A.EFF_DT_SK <= '{CurrDate}'
and A.TERM_DT_SK >= '{CurrDate}'
"""
    )
    .load()
)

df_rdp_Reference_Data2_deduped = dedup_sort(
    df_db2_Cust_svc_tllfr_max_dt_in,
    ["GRP_ID", "SUBGRP_ID", "CLS_ID", "CLS_PLN_ID", "PROD_ID", "EFF_DT_SK"],
    [
        ("GRP_ID", "A"),
        ("SUBGRP_ID", "A"),
        ("CLS_ID", "A"),
        ("CLS_PLN_ID", "A"),
        ("PROD_ID", "A"),
        ("EFF_DT_SK", "A"),
    ],
)

df_rdp_Reference_Data2 = df_rdp_Reference_Data2_deduped.select(
    "GRP_ID",
    "SUBGRP_ID",
    "CLS_ID",
    "CLS_PLN_ID",
    "PROD_ID",
    "EFF_DT_SK",
    "CUST_SVC_ID_CARD_PHN_SK",
    "CUST_SVC_ID_CARD_PHN_KEY",
    "CUST_SVC_ID_CARD_PHN_USE_CD",
    "CUST_SVC_ID_CARD_PHN_AREA_CD",
    "CUST_SVC_ID_CARD_PHN_EXCH_NO",
    "CUST_SVC_ID_CARD_PHN_LN_NO",
)

df_lkp_Codes2 = (
    df_db2_Cust_svc_tllfr_phn_no_in.alias("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC")
    .join(
        df_rdp_Reference_Data2.alias("lnk_DDup_OutABC2"),
        (
            (col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.GRP_ID") == col("lnk_DDup_OutABC2.GRP_ID"))
            & (col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.SUBGRP_ID") == col("lnk_DDup_OutABC2.SUBGRP_ID"))
            & (col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.CLS_ID") == col("lnk_DDup_OutABC2.CLS_ID"))
            & (col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.CLS_PLN_ID") == col("lnk_DDup_OutABC2.CLS_PLN_ID"))
            & (col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.PROD_ID") == col("lnk_DDup_OutABC2.PROD_ID"))
            & (col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.EFF_DT_SK") == col("lnk_DDup_OutABC2.EFF_DT_SK"))
        ),
        how="left",
    )
)

df_lkp_Codes2 = df_lkp_Codes2.select(
    col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.GRP_ID").alias("GRP_ID"),
    col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.SUBGRP_ID").alias("SUBGRP_ID"),
    col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.CLS_ID").alias("CLS_ID"),
    col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.PROD_ID").alias("PROD_ID"),
    col("lnk_DDup_OutABC2.CUST_SVC_ID_CARD_PHN_SK").alias("CUST_SVC_ID_CARD_PHN_SK"),
    col("lnk_DDup_OutABC2.CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    col("lnk_DDup_OutABC2.CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
    col("lnk_DDup_OutABC2.CUST_SVC_ID_CARD_PHN_AREA_CD").alias("CUST_SVC_ID_CARD_PHN_AREA_CD"),
    col("lnk_DDup_OutABC2.CUST_SVC_ID_CARD_PHN_EXCH_NO").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
    col("lnk_DDup_OutABC2.CUST_SVC_ID_CARD_PHN_LN_NO").alias("CUST_SVC_ID_CARD_PHN_LN_NO"),
    col("lnk_IdsDmCustSvcTllfrMaxDtExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
)

df_db2_Cust_svc_local_phn_no_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        f"""
SELECT DISTINCT
A.GRP_ID,
A.SUBGRP_ID,
A.CLS_ID,
A.CLS_PLN_ID,
A.PROD_ID,
A.EFF_DT_SK,
B.CUST_SVC_ID_CARD_PHN_SK,
A.CUST_SVC_ID_CARD_PHN_KEY,
A.CUST_SVC_ID_CARD_PHN_USE_CD,
B.CUST_SVC_ID_CARD_PHN_AREA_CD,
B.CUST_SVC_ID_CARD_PHN_EXCH_NO,
B.CUST_SVC_ID_CARD_PHN_LN_NO
FROM {IDSOwner}.CLS_PLN_CUST_SVC_ID_CARD_PHN A,
{IDSOwner}.CUST_SVC_ID_CARD_PHN B,
{IDSOwner}.CD_MPPNG CD_MPPNG
WHERE
A.CUST_SVC_ID_CARD_PHN_SK = B.CUST_SVC_ID_CARD_PHN_SK
and A.CUST_SVC_ID_CARD_PHN_USE_CD_SK = CD_MPPNG.CD_MPPNG_SK
and CD_MPPNG.TRGT_CD = 'CUSTSVCLOCAL'
and A.EFF_DT_SK <= '{CurrDate}'
and A.TERM_DT_SK >= '{CurrDate}'
"""
    )
    .load()
)

df_rdp_Reference_Data1_deduped = dedup_sort(
    df_db2_Cust_svc_local_phn_no_in,
    ["GRP_ID", "SUBGRP_ID", "CLS_ID", "CLS_PLN_ID", "PROD_ID", "EFF_DT_SK"],
    [
        ("GRP_ID", "A"),
        ("SUBGRP_ID", "A"),
        ("CLS_ID", "A"),
        ("CLS_PLN_ID", "A"),
        ("PROD_ID", "A"),
        ("EFF_DT_SK", "A"),
    ],
)

df_rdp_Reference_Data1 = df_rdp_Reference_Data1_deduped.select(
    "GRP_ID",
    "SUBGRP_ID",
    "CLS_ID",
    "CLS_PLN_ID",
    "PROD_ID",
    "EFF_DT_SK",
    "CUST_SVC_ID_CARD_PHN_SK",
    "CUST_SVC_ID_CARD_PHN_KEY",
    "CUST_SVC_ID_CARD_PHN_USE_CD",
    "CUST_SVC_ID_CARD_PHN_AREA_CD",
    "CUST_SVC_ID_CARD_PHN_EXCH_NO",
    "CUST_SVC_ID_CARD_PHN_LN_NO",
)

df_lkp_Codes1 = (
    df_db2_Cust_svc_local_max_dt_in.alias("lnk_IdsDmCustsvclocalmaxdtExtr_InABC")
    .join(
        df_rdp_Reference_Data1.alias("lnk_DDup_OutABC1"),
        (
            (col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.GRP_ID") == col("lnk_DDup_OutABC1.GRP_ID"))
            & (col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.SUBGRP_ID") == col("lnk_DDup_OutABC1.SUBGRP_ID"))
            & (col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.CLS_ID") == col("lnk_DDup_OutABC1.CLS_ID"))
            & (col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.CLS_PLN_ID") == col("lnk_DDup_OutABC1.CLS_PLN_ID"))
            & (col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.PROD_ID") == col("lnk_DDup_OutABC1.PROD_ID"))
            & (col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.EFF_DT_SK") == col("lnk_DDup_OutABC1.EFF_DT_SK"))
        ),
        how="left",
    )
)

df_lkp_Codes1 = df_lkp_Codes1.select(
    col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.GRP_ID").alias("GRP_ID"),
    col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.SUBGRP_ID").alias("SUBGRP_ID"),
    col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.CLS_ID").alias("CLS_ID"),
    col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.PROD_ID").alias("PROD_ID"),
    col("lnk_DDup_OutABC1.CUST_SVC_ID_CARD_PHN_SK").alias("CUST_SVC_ID_CARD_PHN_SK"),
    col("lnk_DDup_OutABC1.CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY"),
    col("lnk_DDup_OutABC1.CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD"),
    col("lnk_DDup_OutABC1.CUST_SVC_ID_CARD_PHN_AREA_CD").alias("CUST_SVC_ID_CARD_PHN_AREA_CD"),
    col("lnk_DDup_OutABC1.CUST_SVC_ID_CARD_PHN_EXCH_NO").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO"),
    col("lnk_DDup_OutABC1.CUST_SVC_ID_CARD_PHN_LN_NO").alias("CUST_SVC_ID_CARD_PHN_LN_NO"),
    col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.EFF_DT_SK").alias("EFF_DT_SK"),
)

df_lkp_Codes = df_db2_P_MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN_in.alias("lnk_Extract_InABC")

df_lkp_Codes = df_lkp_Codes.join(
    df_lkp_Codes2.alias("ref_Cust_svc_tllfr"),
    (
        (col("lnk_Extract_InABC.GRP_ID") == col("ref_Cust_svc_tllfr.GRP_ID"))
        & (col("lnk_Extract_InABC.SUBGRP_ID") == col("ref_Cust_svc_tllfr.SUBGRP_ID"))
        & (col("lnk_Extract_InABC.CLS_ID") == col("ref_Cust_svc_tllfr.CLS_ID"))
        & (col("lnk_Extract_InABC.CLS_PLN_ID") == col("ref_Cust_svc_tllfr.CLS_PLN_ID"))
        & (col("lnk_Extract_InABC.PROD_ID") == col("ref_Cust_svc_tllfr.PROD_ID"))
        & (col("lnk_IdsDmCustsvclocalmaxdtExtr_InABC.EFF_DT_SK") == col("ref_Cust_svc_tllfr.EFF_DT_SK"))
    ),
    how="left",
)

df_lkp_Codes = df_lkp_Codes.join(
    df_lkp_Codes1.alias("ref_Cust_svc_phn"),
    (
        (col("lnk_Extract_InABC.GRP_ID") == col("ref_Cust_svc_phn.GRP_ID"))
        & (col("lnk_Extract_InABC.SUBGRP_ID") == col("ref_Cust_svc_phn.SUBGRP_ID"))
        & (col("lnk_Extract_InABC.CLS_ID") == col("ref_Cust_svc_phn.CLS_ID"))
        & (col("lnk_Extract_InABC.CLS_PLN_ID") == col("ref_Cust_svc_phn.CLS_PLN_ID"))
        & (col("lnk_Extract_InABC.PROD_ID") == col("ref_Cust_svc_phn.PROD_ID"))
    ),
    how="left",
)

df_lkp_Codes = df_lkp_Codes.join(
    df_db2_CD_MPPNG_in.alias("lnk_CdMppngOut"),
    col("lnk_Extract_InABC.CLS_PLN_DTL_PROD_CAT_CD_SK") == col("lnk_CdMppngOut.CD_MPPNG_SK"),
    how="left",
)

df_lkp_Codes = df_lkp_Codes.select(
    col("lnk_Extract_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_Extract_InABC.GRP_ID").alias("GRP_ID"),
    col("lnk_Extract_InABC.SUBGRP_ID").alias("SUBGRP_ID"),
    col("lnk_Extract_InABC.CLS_ID").alias("CLS_ID"),
    col("lnk_Extract_InABC.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("lnk_Extract_InABC.PROD_ID").alias("PROD_ID"),
    col("lnk_CdMppngOut.TRGT_CD").alias("CLS_PLN_PROD_CAT_CD"),
    col("lnk_CdMppngOut.TRGT_CD_NM").alias("CLS_PLN_PROD_CAT_NM"),
    col("ref_Cust_svc_tllfr.GRP_ID").alias("GRP_ID_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.SUBGRP_ID").alias("SUBGRP_ID_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.CLS_ID").alias("CLS_ID_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.CLS_PLN_ID").alias("CLS_PLN_ID_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.PROD_ID").alias("PROD_ID_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.CUST_SVC_ID_CARD_PHN_SK").alias("CUST_SVC_ID_CARD_PHN_SK_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.CUST_SVC_ID_CARD_PHN_AREA_CD").alias("CUST_SVC_ID_CARD_PHN_AREA_CD_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.CUST_SVC_ID_CARD_PHN_EXCH_NO").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO_TLLFR_PHN_NO"),
    col("ref_Cust_svc_tllfr.CUST_SVC_ID_CARD_PHN_LN_NO").alias("CUST_SVC_ID_CARD_PHN_LN_NO_TLLFR_PHN_NO"),
    col("ref_Cust_svc_phn.GRP_ID").alias("GRP_ID_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.SUBGRP_ID").alias("SUBGRP_ID_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.CLS_ID").alias("CLS_ID_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.CLS_PLN_ID").alias("CLS_PLN_ID_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.PROD_ID").alias("PROD_ID_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.CUST_SVC_ID_CARD_PHN_SK").alias("CUST_SVC_ID_CARD_PHN_SK_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.CUST_SVC_ID_CARD_PHN_KEY").alias("CUST_SVC_ID_CARD_PHN_KEY_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.CUST_SVC_ID_CARD_PHN_USE_CD").alias("CUST_SVC_ID_CARD_PHN_USE_CD_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.CUST_SVC_ID_CARD_PHN_AREA_CD").alias("CUST_SVC_ID_CARD_PHN_AREA_CD_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.CUST_SVC_ID_CARD_PHN_EXCH_NO").alias("CUST_SVC_ID_CARD_PHN_EXCH_NO_LOCAL_PHN_NO"),
    col("ref_Cust_svc_phn.CUST_SVC_ID_CARD_PHN_LN_NO").alias("CUST_SVC_ID_CARD_PHN_LN_NO_LOCAL_PHN_NO"),
)

df_xfrm_BusinessLogic = df_lkp_Codes.select(
    when(F.trim(col("SRC_SYS_CD")) == "", lit("UNK ")).otherwise(col("SRC_SYS_CD")).alias("SRC_SYS_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("CLS_ID").alias("CLS_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID"),
    lit("1753-01-01 00:00:00.000").alias("CLS_PLN_CUST_SVC_IDCRD_EFF_DT"),
    when(
        col("CLS_PLN_PROD_CAT_CD").isNull() | (F.trim(col("CLS_PLN_PROD_CAT_CD")) == ""),
        lit("NA "),
    ).otherwise(col("CLS_PLN_PROD_CAT_CD")).alias("CLS_PLN_PROD_CAT_CD"),
    when(
        col("CLS_PLN_PROD_CAT_NM").isNull() | (F.trim(col("CLS_PLN_PROD_CAT_NM")) == ""),
        lit("NA"),
    ).otherwise(col("CLS_PLN_PROD_CAT_NM")).alias("CLS_PLN_PROD_CAT_NM"),
    lit("2199-12-31 00:00:00.000").alias("CLS_PLN_CUST_SVC_IDCRD_TERM_DT"),
    when(
        (
            col("CUST_SVC_ID_CARD_PHN_SK_LOCAL_PHN_NO").isNull()
            & col("GRP_ID_LOCAL_PHN_NO").isNull()
            & col("SUBGRP_ID_LOCAL_PHN_NO").isNull()
            & col("CLS_ID_LOCAL_PHN_NO").isNull()
            & col("CLS_PLN_ID_LOCAL_PHN_NO").isNull()
            & col("PROD_ID_LOCAL_PHN_NO").isNull()
            & col("CUST_SVC_ID_CARD_PHN_KEY_LOCAL_PHN_NO").isNull()
            & col("CUST_SVC_ID_CARD_PHN_USE_CD_LOCAL_PHN_NO").isNull()
        ),
        lit(" "),
    )
    .when(
        (
            col("CUST_SVC_ID_CARD_PHN_AREA_CD_LOCAL_PHN_NO").isNull()
            & col("CUST_SVC_ID_CARD_PHN_EXCH_NO_LOCAL_PHN_NO").isNull()
            & col("CUST_SVC_ID_CARD_PHN_LN_NO_LOCAL_PHN_NO").isNull()
        ),
        lit(" "),
    )
    .otherwise(
        concat(
            col("CUST_SVC_ID_CARD_PHN_AREA_CD_LOCAL_PHN_NO"),
            col("CUST_SVC_ID_CARD_PHN_EXCH_NO_LOCAL_PHN_NO"),
            col("CUST_SVC_ID_CARD_PHN_LN_NO_LOCAL_PHN_NO"),
        )
    ).alias("CUST_SVC_LOCAL_PHN_NO"),
    when(
        (
            col("CUST_SVC_ID_CARD_PHN_SK_TLLFR_PHN_NO").isNull()
            & col("GRP_ID_TLLFR_PHN_NO").isNull()
            & col("SUBGRP_ID_TLLFR_PHN_NO").isNull()
            & col("CLS_ID_TLLFR_PHN_NO").isNull()
            & col("CLS_PLN_ID_TLLFR_PHN_NO").isNull()
            & col("PROD_ID_TLLFR_PHN_NO").isNull()
            & col("CUST_SVC_ID_CARD_PHN_KEY_TLLFR_PHN_NO").isNull()
            & col("CUST_SVC_ID_CARD_PHN_USE_CD_TLLFR_PHN_NO").isNull()
        ),
        lit(" "),
    )
    .when(
        (
            col("CUST_SVC_ID_CARD_PHN_AREA_CD_TLLFR_PHN_NO").isNull()
            & col("CUST_SVC_ID_CARD_PHN_EXCH_NO_TLLFR_PHN_NO").isNull()
            & col("CUST_SVC_ID_CARD_PHN_LN_NO_TLLFR_PHN_NO").isNull()
        ),
        lit(" "),
    )
    .otherwise(
        concat(
            col("CUST_SVC_ID_CARD_PHN_AREA_CD_TLLFR_PHN_NO"),
            col("CUST_SVC_ID_CARD_PHN_EXCH_NO_TLLFR_PHN_NO"),
            col("CUST_SVC_ID_CARD_PHN_LN_NO_TLLFR_PHN_NO"),
        )
    ).alias("CUST_SVC_TLLFR_PHN_NO"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_NO"),
)

final_columns = [
    "SRC_SYS_CD",
    "GRP_ID",
    "SUBGRP_ID",
    "CLS_ID",
    "CLS_PLN_ID",
    "PROD_ID",
    "CLS_PLN_CUST_SVC_IDCRD_EFF_DT",
    "CLS_PLN_PROD_CAT_CD",
    "CLS_PLN_PROD_CAT_NM",
    "CLS_PLN_CUST_SVC_IDCRD_TERM_DT",
    "CUST_SVC_LOCAL_PHN_NO",
    "CUST_SVC_TLLFR_PHN_NO",
    "LAST_UPDT_RUN_CYC_NO",
]

df_final = df_xfrm_BusinessLogic.select(*final_columns)

write_files(
    df_final,
    f"{adls_path}/load/MBRSH_DM_CLS_PLN_CUST_SVC_ID_CARD_PHN.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None,
)