# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------        ------------------------------       --------------------
# MAGIC Aditya   Raju              07/08/2013               5114                              Create Load File for EDW Table PROV_AGMNT_D                         EnterpriseWrhsDevl    Peter Marshall             08/28/2013

# MAGIC Write PROV_AGMNT_D Data into a Sequential file for Load Job IdsEdwProv_Agmnt_CdDLoad.
# MAGIC Read all the Data from IDS PROV_AGMNT_D Table; Pull records based on LAST_UPDT_RUN_CYC_EXCTN_SK.
# MAGIC Add Defaults and Null Handling.
# MAGIC Job Name: IdsEdwProvAgmntDExtr
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


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

jdbc_url_db2_PROV_AGMNT_in, jdbc_props_db2_PROV_AGMNT_in = get_db_config(ids_secret_name)
extract_query_db2_PROV_AGMNT_in = f"""SELECT
PROV_AGMNT_SK,
COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,
PROV_AGMNT_ID,
STRGT_DSCNT_RSN_EXCD_SK,
PROV_AGMNT_CAT_CD_SK,
PROV_AGMNT_IP_HOSP_PRICE_CD_SK,
PROV_AGMNT_OP_HOSP_PRICE_CD_SK,
PROV_AGMNT_PAYMT_DRAG_DT_CD_SK,
PROV_AGMNT_PAY_DRAG_FREQ_CD_SK,
PROV_AGMNT_PRICE_PROFL_CD_SK,
PROV_AGMNT_STRGT_DSCNT_CD_SK,
PROV_AGMNT_STRDSCNT_PROC_CD_SK,
STRGT_DSCNT_PCT,
SUPLMT_DSCNT_PCT,
CLM_ACPTD_MO_QTY,
AUTO_ROOM_PFX_ID,
COB_RULE_PFX_ID,
DNTL_PROC_PFX_ID,
DRG_PFX_ID,
EXCL_PFX_ID,
MULT_SURG_PFX_ID,
PAYMT_DRAG_PERD1_NO,
PROC_DEFN_ID,
PROFL_ID,
SVC_DEFN_PFX_ID,
STOPLOSS_PFX_ID,
EFF_DT_SK,
TERM_DT_SK
FROM {IDSOwner}.PROV_AGMNT PROV_AGMNT
LEFT OUTER JOIN {IDSOwner}.CD_MPPNG CD ON PROV_AGMNT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK
"""

df_db2_PROV_AGMNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_PROV_AGMNT_in)
    .options(**jdbc_props_db2_PROV_AGMNT_in)
    .option("query", extract_query_db2_PROV_AGMNT_in)
    .load()
)

jdbc_url_db2_CD_MPPNG_Extr, jdbc_props_db2_CD_MPPNG_Extr = get_db_config(ids_secret_name)
extract_query_db2_CD_MPPNG_Extr = f"""SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"""
df_db2_CD_MPPNG_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_db2_CD_MPPNG_Extr)
    .options(**jdbc_props_db2_CD_MPPNG_Extr)
    .option("query", extract_query_db2_CD_MPPNG_Extr)
    .load()
)

df_Cpy_cd_mpping_refPaymtDragDtCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_cd_mpping_refPriceProfl = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_cd_mpping_refPaymtDragFreqCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_cd_mpping_refOpHospPriceCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_cd_mpping_refStrgtDscntCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_cd_mpping_refDscntProCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_cd_mpping_refIpHospPricCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_Cpy_cd_mpping_refCatCd = df_db2_CD_MPPNG_Extr.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("TRGT_CD").alias("TRGT_CD"),
    F.col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_db2_PROV_AGMNT_in_alias = df_db2_PROV_AGMNT_in.alias("lnk_ProvAgmntD_inABC")
df_Cpy_cd_mpping_refCatCd_alias = df_Cpy_cd_mpping_refCatCd.alias("refCatCd")
df_Cpy_cd_mpping_refIpHospPricCd_alias = df_Cpy_cd_mpping_refIpHospPricCd.alias("refIpHospPricCd")
df_Cpy_cd_mpping_refDscntProCd_alias = df_Cpy_cd_mpping_refDscntProCd.alias("refDscntProCd")
df_Cpy_cd_mpping_refStrgtDscntCd_alias = df_Cpy_cd_mpping_refStrgtDscntCd.alias("refStrgtDscntCd")
df_Cpy_cd_mpping_refOpHospPriceCd_alias = df_Cpy_cd_mpping_refOpHospPriceCd.alias("refOpHospPriceCd")
df_Cpy_cd_mpping_refPaymtDragFreqCd_alias = df_Cpy_cd_mpping_refPaymtDragFreqCd.alias("refPaymtDragFreqCd")
df_Cpy_cd_mpping_refPriceProfl_alias = df_Cpy_cd_mpping_refPriceProfl.alias("refPriceProfl")
df_Cpy_cd_mpping_refPaymtDragDtCd_alias = df_Cpy_cd_mpping_refPaymtDragDtCd.alias("refPaymtDragDtCd")

df_lkp_codes = (
    df_db2_PROV_AGMNT_in_alias
    .join(
        df_Cpy_cd_mpping_refCatCd_alias,
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_CAT_CD_SK"] == df_Cpy_cd_mpping_refCatCd_alias["CD_MPPNG_SK"],
        "left"
    )
    .join(
        df_Cpy_cd_mpping_refIpHospPricCd_alias,
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_IP_HOSP_PRICE_CD_SK"] == df_Cpy_cd_mpping_refIpHospPricCd_alias["CD_MPPNG_SK"],
        "left"
    )
    .join(
        df_Cpy_cd_mpping_refDscntProCd_alias,
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_STRDSCNT_PROC_CD_SK"] == df_Cpy_cd_mpping_refDscntProCd_alias["CD_MPPNG_SK"],
        "left"
    )
    .join(
        df_Cpy_cd_mpping_refStrgtDscntCd_alias,
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_STRGT_DSCNT_CD_SK"] == df_Cpy_cd_mpping_refStrgtDscntCd_alias["CD_MPPNG_SK"],
        "left"
    )
    .join(
        df_Cpy_cd_mpping_refOpHospPriceCd_alias,
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_OP_HOSP_PRICE_CD_SK"] == df_Cpy_cd_mpping_refOpHospPriceCd_alias["CD_MPPNG_SK"],
        "left"
    )
    .join(
        df_Cpy_cd_mpping_refPaymtDragFreqCd_alias,
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_PAY_DRAG_FREQ_CD_SK"] == df_Cpy_cd_mpping_refPaymtDragFreqCd_alias["CD_MPPNG_SK"],
        "left"
    )
    .join(
        df_Cpy_cd_mpping_refPriceProfl_alias,
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_PRICE_PROFL_CD_SK"] == df_Cpy_cd_mpping_refPriceProfl_alias["CD_MPPNG_SK"],
        "left"
    )
    .join(
        df_Cpy_cd_mpping_refPaymtDragDtCd_alias,
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_PAYMT_DRAG_DT_CD_SK"] == df_Cpy_cd_mpping_refPaymtDragDtCd_alias["CD_MPPNG_SK"],
        "left"
    )
    .select(
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_SK"].alias("PROV_AGMNT_SK"),
        df_db2_PROV_AGMNT_in_alias["SRC_SYS_CD"].alias("SRC_SYS_CD"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_ID"].alias("PROV_AGMNT_ID"),
        df_db2_PROV_AGMNT_in_alias["EFF_DT_SK"].alias("PROV_AGMNT_EFF_DT_SK"),
        df_db2_PROV_AGMNT_in_alias["STRGT_DSCNT_RSN_EXCD_SK"].alias("STRGT_DSCNT_RSN_EXCD_SK"),
        df_db2_PROV_AGMNT_in_alias["AUTO_ROOM_PFX_ID"].alias("PROV_AGMNT_AUTO_ROOM_PFX_ID"),
        df_Cpy_cd_mpping_refCatCd_alias["TRGT_CD"].alias("PROV_AGMNT_CAT_CD"),
        df_Cpy_cd_mpping_refCatCd_alias["TRGT_CD_NM"].alias("PROV_AGMNT_CAT_NM"),
        df_db2_PROV_AGMNT_in_alias["CLM_ACPTD_MO_QTY"].alias("PROV_AGMNT_CLM_ACPTD_MO_QTY"),
        df_db2_PROV_AGMNT_in_alias["COB_RULE_PFX_ID"].alias("PROV_AGMNT_COB_RULE_PFX_ID"),
        df_db2_PROV_AGMNT_in_alias["DNTL_PROC_PFX_ID"].alias("PROV_AGMNT_DNTL_PROC_PFX_ID"),
        df_db2_PROV_AGMNT_in_alias["DRG_PFX_ID"].alias("PROV_AGMNT_DRG_PFX_ID"),
        df_db2_PROV_AGMNT_in_alias["EXCL_PFX_ID"].alias("PROV_AGMNT_EXCL_PFX_ID"),
        df_Cpy_cd_mpping_refIpHospPricCd_alias["TRGT_CD"].alias("PROV_AGMNT_IP_HOSP_PRICE_CD"),
        df_Cpy_cd_mpping_refIpHospPricCd_alias["TRGT_CD_NM"].alias("PROV_AGMNT_IP_HOSP_PRICE_NM"),
        df_db2_PROV_AGMNT_in_alias["MULT_SURG_PFX_ID"].alias("PROV_AGMNT_MULT_SURG_PFX_ID"),
        df_Cpy_cd_mpping_refOpHospPriceCd_alias["TRGT_CD"].alias("PROV_AGMNT_OP_HOSP_PRICE_CD"),
        df_Cpy_cd_mpping_refOpHospPriceCd_alias["TRGT_CD_NM"].alias("PROV_AGMNT_OP_HOSP_PRICE_NM"),
        df_Cpy_cd_mpping_refPaymtDragDtCd_alias["TRGT_CD"].alias("PROV_AGMNT_PAYMT_DRAG_DT_CD"),
        df_Cpy_cd_mpping_refPaymtDragDtCd_alias["TRGT_CD_NM"].alias("PROV_AGMNT_PAYMT_DRAG_DT_NM"),
        df_Cpy_cd_mpping_refPaymtDragFreqCd_alias["TRGT_CD"].alias("PROV_AGMNT_PAYMT_DRAG_FREQ_CD"),
        df_Cpy_cd_mpping_refPaymtDragFreqCd_alias["TRGT_CD_NM"].alias("PROV_AGMNT_PAYMT_DRAG_FREQ_NM"),
        df_db2_PROV_AGMNT_in_alias["PAYMT_DRAG_PERD1_NO"].alias("PROV_AGMNT_PAYMT_DRAG_PERD1_NO"),
        df_Cpy_cd_mpping_refPriceProfl_alias["TRGT_CD"].alias("PROV_AGMNT_PRICE_PROFL_CD"),
        df_Cpy_cd_mpping_refPriceProfl_alias["TRGT_CD_NM"].alias("PROV_AGMNT_PRICE_PROFL_NM"),
        df_db2_PROV_AGMNT_in_alias["PROC_DEFN_ID"].alias("PROV_AGMNT_PROC_DEFN_ID"),
        df_db2_PROV_AGMNT_in_alias["PROFL_ID"].alias("PROV_AGMNT_PROFL_ID"),
        df_db2_PROV_AGMNT_in_alias["SVC_DEFN_PFX_ID"].alias("PROV_AGMNT_SVC_DEFN_PFX_ID"),
        df_Cpy_cd_mpping_refStrgtDscntCd_alias["TRGT_CD"].alias("PROV_AGMNT_STRGT_DSCNT_CD"),
        df_Cpy_cd_mpping_refStrgtDscntCd_alias["TRGT_CD_NM"].alias("PROV_AGMNT_STRGT_DSCNT_NM"),
        df_db2_PROV_AGMNT_in_alias["STRGT_DSCNT_PCT"].alias("PROV_AGMNT_STRGT_DSCNT_PCT"),
        df_Cpy_cd_mpping_refDscntProCd_alias["TRGT_CD"].alias("PROV_AGMNT_STRGT_DSCNT_PROC_CD"),
        df_Cpy_cd_mpping_refDscntProCd_alias["TRGT_CD_NM"].alias("PROV_AGMNT_STRGT_DSCNT_PROC_NM"),
        df_db2_PROV_AGMNT_in_alias["STOPLOSS_PFX_ID"].alias("PROV_AGMNT_STOPLOSS_PFX_ID"),
        df_db2_PROV_AGMNT_in_alias["SUPLMT_DSCNT_PCT"].alias("PROV_AGMNT_SUPLMT_DSCNT_PCT"),
        df_db2_PROV_AGMNT_in_alias["TERM_DT_SK"].alias("PROV_AGMNT_TERM_DT_SK"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_CAT_CD_SK"].alias("PROV_AGMNT_CAT_CD_SK"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_IP_HOSP_PRICE_CD_SK"].alias("PROV_AGMNT_IP_HOSP_PRICE_CD_SK"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_OP_HOSP_PRICE_CD_SK"].alias("PROV_AGMNT_OP_HOSP_PRICE_CD_SK"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_PAYMT_DRAG_DT_CD_SK"].alias("PROV_AGMNT_PAYMT_DRAG_DT_CD_SK"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_PAY_DRAG_FREQ_CD_SK"].alias("PROV_AGMNT_PAY_DRAG_FREQ_CD_SK"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_PRICE_PROFL_CD_SK"].alias("PROV_AGMNT_PRICE_PROFL_CD_SK"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_STRGT_DSCNT_CD_SK"].alias("PROV_AGMNT_STRGT_DSCNT_CD_SK"),
        df_db2_PROV_AGMNT_in_alias["PROV_AGMNT_STRDSCNT_PROC_CD_SK"].alias("PROV_AGMNT_STRDSCNT_PROC_CD_SK")
    )
)

df_xfrm_BusinessLogic = df_lkp_codes.select(
    F.col("PROV_AGMNT_SK").alias("PROV_AGMNT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("PROV_AGMNT_EFF_DT_SK").alias("PROV_AGMNT_EFF_DT_SK"),
    F.lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("STRGT_DSCNT_RSN_EXCD_SK").alias("STRGT_DSCNT_RSN_EXCD_SK"),
    F.col("PROV_AGMNT_AUTO_ROOM_PFX_ID").alias("PROV_AGMNT_AUTO_ROOM_PFX_ID"),
    F.col("PROV_AGMNT_CAT_CD").alias("PROV_AGMNT_CAT_CD"),
    F.col("PROV_AGMNT_CAT_NM").alias("PROV_AGMNT_CAT_NM"),
    F.col("PROV_AGMNT_CLM_ACPTD_MO_QTY").alias("PROV_AGMNT_CLM_ACPTD_MO_QTY"),
    F.col("PROV_AGMNT_COB_RULE_PFX_ID").alias("PROV_AGMNT_COB_RULE_PFX_ID"),
    F.col("PROV_AGMNT_DNTL_PROC_PFX_ID").alias("PROV_AGMNT_DNTL_PROC_PFX_ID"),
    F.col("PROV_AGMNT_DRG_PFX_ID").alias("PROV_AGMNT_DRG_PFX_ID"),
    F.col("PROV_AGMNT_EXCL_PFX_ID").alias("PROV_AGMNT_EXCL_PFX_ID"),
    F.col("PROV_AGMNT_IP_HOSP_PRICE_CD").alias("PROV_AGMNT_IP_HOSP_PRICE_CD"),
    F.col("PROV_AGMNT_IP_HOSP_PRICE_NM").alias("PROV_AGMNT_IP_HOSP_PRICE_NM"),
    F.col("PROV_AGMNT_MULT_SURG_PFX_ID").alias("PROV_AGMNT_MULT_SURG_PFX_ID"),
    F.col("PROV_AGMNT_OP_HOSP_PRICE_CD").alias("PROV_AGMNT_OP_HOSP_PRICE_CD"),
    F.col("PROV_AGMNT_OP_HOSP_PRICE_NM").alias("PROV_AGMNT_OP_HOSP_PRICE_NM"),
    F.col("PROV_AGMNT_PAYMT_DRAG_DT_CD").alias("PROV_AGMNT_PAYMT_DRAG_DT_CD"),
    F.col("PROV_AGMNT_PAYMT_DRAG_DT_NM").alias("PROV_AGMNT_PAYMT_DRAG_DT_NM"),
    F.col("PROV_AGMNT_PAYMT_DRAG_FREQ_CD").alias("PROV_AGMNT_PAYMT_DRAG_FREQ_CD"),
    F.col("PROV_AGMNT_PAYMT_DRAG_FREQ_NM").alias("PROV_AGMNT_PAYMT_DRAG_FREQ_NM"),
    F.col("PROV_AGMNT_PAYMT_DRAG_PERD1_NO").alias("PROV_AGMNT_PAYMT_DRAG_PERD1_NO"),
    F.col("PROV_AGMNT_PRICE_PROFL_CD").alias("PROV_AGMNT_PRICE_PROFL_CD"),
    F.col("PROV_AGMNT_PRICE_PROFL_NM").alias("PROV_AGMNT_PRICE_PROFL_NM"),
    F.col("PROV_AGMNT_PROC_DEFN_ID").alias("PROV_AGMNT_PROC_DEFN_ID"),
    F.col("PROV_AGMNT_PROFL_ID").alias("PROV_AGMNT_PROFL_ID"),
    F.col("PROV_AGMNT_SVC_DEFN_PFX_ID").alias("PROV_AGMNT_SVC_DEFN_PFX_ID"),
    F.col("PROV_AGMNT_STRGT_DSCNT_CD").alias("PROV_AGMNT_STRGT_DSCNT_CD"),
    F.col("PROV_AGMNT_STRGT_DSCNT_NM").alias("PROV_AGMNT_STRGT_DSCNT_NM"),
    F.col("PROV_AGMNT_STRGT_DSCNT_PCT").alias("PROV_AGMNT_STRGT_DSCNT_PCT"),
    F.col("PROV_AGMNT_STRGT_DSCNT_PROC_CD").alias("PROV_AGMNT_STRGT_DSCNT_PROC_CD"),
    F.col("PROV_AGMNT_STRGT_DSCNT_PROC_NM").alias("PROV_AGMNT_STRGT_DSCNT_PROC_NM"),
    F.col("PROV_AGMNT_STOPLOSS_PFX_ID").alias("PROV_AGMNT_STOPLOSS_PFX_ID"),
    F.col("PROV_AGMNT_SUPLMT_DSCNT_PCT").alias("PROV_AGMNT_SUPLMT_DSCNT_PCT"),
    F.col("PROV_AGMNT_TERM_DT_SK").alias("PROV_AGMNT_TERM_DT_SK"),
    F.lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_AGMNT_CAT_CD_SK").alias("PROV_AGMNT_CAT_CD_SK"),
    F.col("PROV_AGMNT_IP_HOSP_PRICE_CD_SK").alias("PROV_AGMNT_IP_HOSP_PRICE_CD_SK"),
    F.col("PROV_AGMNT_OP_HOSP_PRICE_CD_SK").alias("PROV_AGMNT_OP_HOSP_PRICE_CD_SK"),
    F.col("PROV_AGMNT_PAYMT_DRAG_DT_CD_SK").alias("PROV_AGMNT_PAYMT_DRAG_DT_CD_SK"),
    F.col("PROV_AGMNT_PAY_DRAG_FREQ_CD_SK").alias("PROV_AGMNT_PAY_DRAG_FREQ_CD_SK"),
    F.col("PROV_AGMNT_PRICE_PROFL_CD_SK").alias("PROV_AGMNT_PRICE_PROFL_CD_SK"),
    F.col("PROV_AGMNT_STRGT_DSCNT_CD_SK").alias("PROV_AGMNT_STRGT_DSCNT_CD_SK"),
    F.col("PROV_AGMNT_STRDSCNT_PROC_CD_SK").alias("PROV_AGMNT_STRDSCNT_PROC_CD_SK")
)

df_seq_ProvAgmntDExtr_csv_load = df_xfrm_BusinessLogic.select(
    F.col("PROV_AGMNT_SK"),
    F.col("SRC_SYS_CD"),
    F.col("PROV_AGMNT_ID"),
    F.rpad(F.col("PROV_AGMNT_EFF_DT_SK"), 10, " ").alias("PROV_AGMNT_EFF_DT_SK"),
    F.rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("STRGT_DSCNT_RSN_EXCD_SK"),
    F.col("PROV_AGMNT_AUTO_ROOM_PFX_ID"),
    F.col("PROV_AGMNT_CAT_CD"),
    F.col("PROV_AGMNT_CAT_NM"),
    F.col("PROV_AGMNT_CLM_ACPTD_MO_QTY"),
    F.col("PROV_AGMNT_COB_RULE_PFX_ID"),
    F.col("PROV_AGMNT_DNTL_PROC_PFX_ID"),
    F.col("PROV_AGMNT_DRG_PFX_ID"),
    F.col("PROV_AGMNT_EXCL_PFX_ID"),
    F.col("PROV_AGMNT_IP_HOSP_PRICE_CD"),
    F.col("PROV_AGMNT_IP_HOSP_PRICE_NM"),
    F.col("PROV_AGMNT_MULT_SURG_PFX_ID"),
    F.col("PROV_AGMNT_OP_HOSP_PRICE_CD"),
    F.col("PROV_AGMNT_OP_HOSP_PRICE_NM"),
    F.col("PROV_AGMNT_PAYMT_DRAG_DT_CD"),
    F.col("PROV_AGMNT_PAYMT_DRAG_DT_NM"),
    F.col("PROV_AGMNT_PAYMT_DRAG_FREQ_CD"),
    F.col("PROV_AGMNT_PAYMT_DRAG_FREQ_NM"),
    F.col("PROV_AGMNT_PAYMT_DRAG_PERD1_NO"),
    F.col("PROV_AGMNT_PRICE_PROFL_CD"),
    F.col("PROV_AGMNT_PRICE_PROFL_NM"),
    F.col("PROV_AGMNT_PROC_DEFN_ID"),
    F.col("PROV_AGMNT_PROFL_ID"),
    F.col("PROV_AGMNT_SVC_DEFN_PFX_ID"),
    F.col("PROV_AGMNT_STRGT_DSCNT_CD"),
    F.col("PROV_AGMNT_STRGT_DSCNT_NM"),
    F.col("PROV_AGMNT_STRGT_DSCNT_PCT"),
    F.col("PROV_AGMNT_STRGT_DSCNT_PROC_CD"),
    F.col("PROV_AGMNT_STRGT_DSCNT_PROC_NM"),
    F.col("PROV_AGMNT_STOPLOSS_PFX_ID"),
    F.col("PROV_AGMNT_SUPLMT_DSCNT_PCT"),
    F.rpad(F.col("PROV_AGMNT_TERM_DT_SK"), 10, " ").alias("PROV_AGMNT_TERM_DT_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROV_AGMNT_CAT_CD_SK"),
    F.col("PROV_AGMNT_IP_HOSP_PRICE_CD_SK"),
    F.col("PROV_AGMNT_OP_HOSP_PRICE_CD_SK"),
    F.col("PROV_AGMNT_PAYMT_DRAG_DT_CD_SK"),
    F.col("PROV_AGMNT_PAY_DRAG_FREQ_CD_SK"),
    F.col("PROV_AGMNT_PRICE_PROFL_CD_SK"),
    F.col("PROV_AGMNT_STRGT_DSCNT_CD_SK"),
    F.col("PROV_AGMNT_STRDSCNT_PROC_CD_SK")
)

write_files(
    df_seq_ProvAgmntDExtr_csv_load,
    f"{adls_path}/load/PROV_AGMNT_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)