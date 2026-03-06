# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 	   Pulls data from IDS INTER_PLN_BILL_DTL table and load into the EDW INTER_PLN_BILL_DTL_F
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date                   Project       Description                                                                 Environment                      Code Reviewer                      Review Date
# MAGIC ----------------------------------         --------------------------------    -------------     ------------------------------------                                           -----------------------------        ------------------------------------       -----------------------------------
# MAGIC Karthik Chintalapani                      08/17/2015     5212 PI      Originally Program                                                      EnterpriseDev1                     Kalyan Neelam                     2015-08-20
# MAGIC  
# MAGIC Karthik Chintalapani                 02/03/2016          5212 PI      Added new column POST_AS_EXP_IN                     EnterpriseDev1                    Kalyan Neelam                     2016-02-03
# MAGIC 
# MAGIC Karthik Chintalapani          2016-08-24      5212 PI                     Added the new column PRCS_CYC_DT_SK              EnterpriseDev1                Kalyan Neelam                      2016-08-25       
# MAGIC 
# MAGIC Karthik Chintalapani     2017-04-06              5587                                     Added new columns to outbound file	   EnterpriseDev1                    Kalyan Neelam                     2017-04-11
# MAGIC VAL_BASED_PGM_ID, DSPTD_RCRD_CD, HOME_PLN_DSPT_ACTN_CD, CMNT_TX, HOST_PLN_DSPT_ACTN_CD, LOCAL_PLN_CTL_NO, PRCS_SITE_CTL_NO
# MAGIC 
# MAGIC Manasa Andru                  2018-11-02      5726              Added BILL_DTL_FMT_RCRD_TYP_CD field at the end     EnterpriseDev2                 Kalyan Neelam                     2018-12-18
# MAGIC                                                                18.5 Submission

# MAGIC Job name:
# MAGIC IdsEdwInterPlnBillDtlFExtr
# MAGIC Lookup MBR_D table to rerieve  and MBR_UNIQ_KEY
# MAGIC Write INTER_PLN_BILL_DTL_F Data into a Sequential file for Load Ready Job.
# MAGIC Pull data from Pulls data from IDS INTER_PLN_BILL_DTL
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, length, when, monotonically_increasing_id, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
RunID = get_widget_value('RunID','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
query_db2_INTER_PLN_BILL_DTL_in = f"""
SELECT
INTER_PLN_BILL_DTL_SK,
INTER_PLN_BILL_DTL_ID,
BUS_OWNER_ID,
SRC_SYS_CD_SK,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
MBR_SK,
ATTRBTN_PGM_CD_SK,
CFA_RMBRMT_TYP_CD_SK,
CFA_TRANS_TYP_CD_SK,
CTL_PLN_CD_SK,
EPSD_TYP_CD_SK,
HOME_CORP_PLN_CD_SK,
HOME_PLN_CD_SK,
HOST_CORP_PLN_CD_SK,
HOST_PLN_CD_SK,
LOCAL_PLN_CD_SK,
VAL_BASED_PGM_CD_SK,
VAL_BASED_PGM_PAYMT_TYP_CD_SK,
BTCH_CRT_DT_SK,
CBF_SETL_STRT_DT_SK,
CBF_SETL_BILL_END_DT_SK,
CFA_DISP_DT_SK,
CFA_PRCS_DT_SK,
HOME_AUTH_DENIAL_DT_SK,
INCUR_PERD_STRT_DT_SK,
INCUR_PERD_END_DT_SK,
MSRMNT_PERD_STRT_DT_SK,
MSRMNT_PERD_END_DT_SK,
MBR_ELIG_STRT_DT_SK,
MBR_ELIG_END_DT_SK,
PRCS_CYC_YR_MO_SK,
SETL_DT_SK,
ACES_FEE_ADJ_AMT,
CBF_SETL_RATE_AMT,
MBR_LVL_CHRG_AMT,
ATRBD_PROV_ID,
ATRBD_PROV_NTNL_PROV_ID,
ATTRBTN_PROV_PGM_NM,
BCBSA_MMI_ID,
CONSIS_MBR_ID,
CBF_SCCF_NO,
GRP_ID,
HOME_PLN_MBR_ID,
ITS_SUB_ID,
ORIG_INTER_PLN_BILL_DTL_SRL_NO,
ORIG_ITS_CLM_SCCF_NO,
PROV_GRP_NM,
VRNC_ACCT_ID,
BTCH_STTUS_CD_TX,
STTUS_CD_TX,
ERR_CD_TX,
POST_AS_EXP_IN,
PRCS_CYC_DT_SK,
VAL_BASED_PGM_ID,
DSPTD_RCRD_CD,
HOME_PLN_DSPT_ACTN_CD,
CMNT_TX,
HOST_PLN_DSPT_ACTN_CD,
LOCAL_PLN_CTL_NO,
PRCS_SITE_CTL_NO,
BILL_DTL_FMT_RCRD_TYP_CD
FROM {IDSOwner}.INTER_PLN_BILL_DTL
WHERE
LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

df_db2_INTER_PLN_BILL_DTL_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_INTER_PLN_BILL_DTL_in)
    .load()
)

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
query_db2_MBR_D_In = f"""
SELECT
MBR_SK,
MBR_UNIQ_KEY
FROM {EDWOwner}.MBR_D
"""

df_db2_MBR_D_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", query_db2_MBR_D_In)
    .load()
)

query_db2_CD_MPPNG_In = f"""
SELECT
CD_MPPNG_SK,
COALESCE(TRGT_CD,'UNK') AS TRGT_CD,
COALESCE(TRGT_CD_NM,'UNK') AS TRGT_CD_NM
FROM {IDSOwner}.CD_MPPNG
"""

df_db2_CD_MPPNG_In = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_db2_CD_MPPNG_In)
    .load()
)

df_ref_HostPlnCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CfatrnstypCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CfaRmbtypCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_SrcSysCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_AttrbtnPgmCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_CtlplnCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_EpsdtypCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_HomecorpplnCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_HomeplnCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_HostCorpPlncd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_LocalPlnCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_ValBasedPgmCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_ref_ValBsdPgmtypCd = df_db2_CD_MPPNG_In.select(
    col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    col("TRGT_CD").alias("TRGT_CD"),
    col("TRGT_CD_NM").alias("TRGT_CD_NM")
)

df_in_ink_IdsEdwInterPlnBillDtlFExtr_inABC = df_db2_INTER_PLN_BILL_DTL_in.alias("inabc")

df_lkup_codes_temp = (
    df_in_ink_IdsEdwInterPlnBillDtlFExtr_inABC
    .join(df_db2_MBR_D_In.alias("ref_MbrSk"), col("inabc.MBR_SK") == col("ref_MbrSk.MBR_SK"), "left")
    .join(df_ref_SrcSysCd.alias("ref_SrcSysCd"), col("inabc.SRC_SYS_CD_SK") == col("ref_SrcSysCd.CD_MPPNG_SK"), "left")
    .join(df_ref_AttrbtnPgmCd.alias("ref_AttrbtnPgmCd"), col("inabc.ATTRBTN_PGM_CD_SK") == col("ref_AttrbtnPgmCd.CD_MPPNG_SK"), "left")
    .join(df_ref_CfaRmbtypCd.alias("ref_CfaRmbtypCd"), col("inabc.CFA_RMBRMT_TYP_CD_SK") == col("ref_CfaRmbtypCd.CD_MPPNG_SK"), "left")
    .join(df_ref_CfatrnstypCd.alias("ref_CfatrnstypCd"), col("inabc.CFA_TRANS_TYP_CD_SK") == col("ref_CfatrnstypCd.CD_MPPNG_SK"), "left")
    .join(df_ref_CtlplnCd.alias("ref_CtlplnCd"), col("inabc.CTL_PLN_CD_SK") == col("ref_CtlplnCd.CD_MPPNG_SK"), "left")
    .join(df_ref_EpsdtypCd.alias("ref_EpsdtypCd"), col("inabc.EPSD_TYP_CD_SK") == col("ref_EpsdtypCd.CD_MPPNG_SK"), "left")
    .join(df_ref_HomecorpplnCd.alias("ref_HomecorpplnCd"), col("inabc.HOME_CORP_PLN_CD_SK") == col("ref_HomecorpplnCd.CD_MPPNG_SK"), "left")
    .join(df_ref_HomeplnCd.alias("ref_HomeplnCd"), col("inabc.HOME_PLN_CD_SK") == col("ref_HomeplnCd.CD_MPPNG_SK"), "left")
    .join(df_ref_HostCorpPlncd.alias("ref_HostCorpPlncd"), col("inabc.HOST_CORP_PLN_CD_SK") == col("ref_HostCorpPlncd.CD_MPPNG_SK"), "left")
    .join(df_ref_HostPlnCd.alias("ref_HostPlnCd"), col("inabc.HOST_PLN_CD_SK") == col("ref_HostPlnCd.CD_MPPNG_SK"), "left")
    .join(df_ref_LocalPlnCd.alias("ref_LocalPlnCd"), col("inabc.LOCAL_PLN_CD_SK") == col("ref_LocalPlnCd.CD_MPPNG_SK"), "left")
    .join(df_ref_ValBasedPgmCd.alias("ref_ValBasedPgmCd"), col("inabc.VAL_BASED_PGM_CD_SK") == col("ref_ValBasedPgmCd.CD_MPPNG_SK"), "left")
    .join(df_ref_ValBsdPgmtypCd.alias("ref_ValBsdPgmtypCd"), col("inabc.VAL_BASED_PGM_PAYMT_TYP_CD_SK") == col("ref_ValBsdPgmtypCd.CD_MPPNG_SK"), "left")
)

df_lkup_codes = df_lkup_codes_temp.select(
    col("inabc.INTER_PLN_BILL_DTL_SK").alias("INTER_PLN_BILL_DTL_SK"),
    col("inabc.INTER_PLN_BILL_DTL_ID").alias("INTER_PLN_BILL_DTL_ID"),
    col("inabc.BUS_OWNER_ID").alias("BUS_OWNER_ID"),
    col("inabc.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("inabc.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("inabc.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("inabc.MBR_SK").alias("MBR_SK"),
    col("inabc.ATTRBTN_PGM_CD_SK").alias("ATTRBTN_PGM_CD_SK"),
    col("inabc.CFA_RMBRMT_TYP_CD_SK").alias("CFA_RMBRMT_TYP_CD_SK"),
    col("inabc.CFA_TRANS_TYP_CD_SK").alias("CFA_TRANS_TYP_CD_SK"),
    col("inabc.CTL_PLN_CD_SK").alias("CTL_PLN_CD_SK"),
    col("inabc.EPSD_TYP_CD_SK").alias("EPSD_TYP_CD_SK"),
    col("inabc.HOME_CORP_PLN_CD_SK").alias("HOME_CORP_PLN_CD_SK"),
    col("inabc.HOME_PLN_CD_SK").alias("HOME_PLN_CD_SK"),
    col("inabc.HOST_CORP_PLN_CD_SK").alias("HOST_CORP_PLN_CD_SK"),
    col("inabc.HOST_PLN_CD_SK").alias("HOST_PLN_CD_SK"),
    col("inabc.LOCAL_PLN_CD_SK").alias("LOCAL_PLN_CD_SK"),
    col("inabc.VAL_BASED_PGM_CD_SK").alias("VAL_BASED_PGM_CD_SK"),
    col("inabc.VAL_BASED_PGM_PAYMT_TYP_CD_SK").alias("VAL_BASED_PGM_PAYMT_TYP_CD_SK"),
    col("inabc.BTCH_CRT_DT_SK").alias("BTCH_CRT_DT_SK"),
    col("inabc.CBF_SETL_STRT_DT_SK").alias("CBF_SETL_STRT_DT_SK"),
    col("inabc.CBF_SETL_BILL_END_DT_SK").alias("CBF_SETL_BILL_END_DT_SK"),
    col("inabc.CFA_DISP_DT_SK").alias("CFA_DISP_DT_SK"),
    col("inabc.CFA_PRCS_DT_SK").alias("CFA_PRCS_DT_SK"),
    col("inabc.HOME_AUTH_DENIAL_DT_SK").alias("HOME_AUTH_DENIAL_DT_SK"),
    col("inabc.INCUR_PERD_STRT_DT_SK").alias("INCUR_PERD_STRT_DT_SK"),
    col("inabc.INCUR_PERD_END_DT_SK").alias("INCUR_PERD_END_DT_SK"),
    col("inabc.MSRMNT_PERD_STRT_DT_SK").alias("MSRMNT_PERD_STRT_DT_SK"),
    col("inabc.MSRMNT_PERD_END_DT_SK").alias("MSRMNT_PERD_END_DT_SK"),
    col("inabc.MBR_ELIG_STRT_DT_SK").alias("MBR_ELIG_STRT_DT_SK"),
    col("inabc.MBR_ELIG_END_DT_SK").alias("MBR_ELIG_END_DT_SK"),
    col("inabc.PRCS_CYC_YR_MO_SK").alias("PRCS_CYC_YR_MO_SK"),
    col("inabc.SETL_DT_SK").alias("SETL_DT_SK"),
    col("inabc.ACES_FEE_ADJ_AMT").alias("ACES_FEE_ADJ_AMT"),
    col("inabc.CBF_SETL_RATE_AMT").alias("CBF_SETL_RATE_AMT"),
    col("inabc.MBR_LVL_CHRG_AMT").alias("MBR_LVL_CHRG_AMT"),
    col("inabc.ATRBD_PROV_ID").alias("ATRBD_PROV_ID"),
    col("inabc.ATRBD_PROV_NTNL_PROV_ID").alias("ATRBD_PROV_NTNL_PROV_ID"),
    col("inabc.ATTRBTN_PROV_PGM_NM").alias("ATTRBTN_PROV_PGM_NM"),
    col("inabc.BCBSA_MMI_ID").alias("BCBSA_MMI_ID"),
    col("inabc.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    col("inabc.CBF_SCCF_NO").alias("CBF_SCCF_NO"),
    col("inabc.GRP_ID").alias("GRP_ID"),
    col("inabc.HOME_PLN_MBR_ID").alias("HOME_PLN_MBR_ID"),
    col("inabc.ITS_SUB_ID").alias("ITS_SUB_ID"),
    col("inabc.ORIG_INTER_PLN_BILL_DTL_SRL_NO").alias("ORIG_INTER_PLN_BILL_DTL_SRL_NO"),
    col("inabc.ORIG_ITS_CLM_SCCF_NO").alias("ORIG_ITS_CLM_SCCF_NO"),
    col("inabc.PROV_GRP_NM").alias("PROV_GRP_NM"),
    col("inabc.VRNC_ACCT_ID").alias("VRNC_ACCT_ID"),
    col("inabc.BTCH_STTUS_CD_TX").alias("BTCH_STTUS_CD_TX"),
    col("inabc.STTUS_CD_TX").alias("STTUS_CD_TX"),
    col("inabc.ERR_CD_TX").alias("ERR_CD_TX"),
    col("ref_MbrSk.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("ref_SrcSysCd.TRGT_CD").alias("TRGT_CD"),
    col("ref_SrcSysCd.TRGT_CD_NM").alias("TRGT_CD_NM"),
    col("ref_AttrbtnPgmCd.TRGT_CD").alias("TRGT_CD_1"),
    col("ref_AttrbtnPgmCd.TRGT_CD_NM").alias("TRGT_CD_NM_1"),
    col("ref_CfaRmbtypCd.TRGT_CD").alias("TRGT_CD_2"),
    col("ref_CfaRmbtypCd.TRGT_CD_NM").alias("TRGT_CD_NM_2"),
    col("ref_CfatrnstypCd.TRGT_CD").alias("TRGT_CD_3"),
    col("ref_CfatrnstypCd.TRGT_CD_NM").alias("TRGT_CD_NM_3"),
    col("ref_CtlplnCd.TRGT_CD").alias("TRGT_CD_4"),
    col("ref_CtlplnCd.TRGT_CD_NM").alias("TRGT_CD_NM_4"),
    col("ref_EpsdtypCd.TRGT_CD").alias("TRGT_CD_5"),
    col("ref_EpsdtypCd.TRGT_CD_NM").alias("TRGT_CD_NM_5"),
    col("ref_HomecorpplnCd.TRGT_CD").alias("TRGT_CD_6"),
    col("ref_HomecorpplnCd.TRGT_CD_NM").alias("TRGT_CD_NM_6"),
    col("ref_HomeplnCd.TRGT_CD").alias("TRGT_CD_7"),
    col("ref_HomeplnCd.TRGT_CD_NM").alias("TRGT_CD_NM_7"),
    col("ref_HostCorpPlncd.TRGT_CD").alias("TRGT_CD_8"),
    col("ref_HostCorpPlncd.TRGT_CD_NM").alias("TRGT_CD_NM_8"),
    col("ref_HostPlnCd.TRGT_CD").alias("TRGT_CD_9"),
    col("ref_HostPlnCd.TRGT_CD_NM").alias("TRGT_CD_NM_9"),
    col("ref_LocalPlnCd.TRGT_CD").alias("TRGT_CD_10"),
    col("ref_LocalPlnCd.TRGT_CD_NM").alias("TRGT_CD_NM_10"),
    col("ref_ValBasedPgmCd.TRGT_CD").alias("TRGT_CD_11"),
    col("ref_ValBasedPgmCd.TRGT_CD_NM").alias("TRGT_CD_NM_11"),
    col("ref_ValBsdPgmtypCd.TRGT_CD").alias("TRGT_CD_12"),
    col("ref_ValBsdPgmtypCd.TRGT_CD_NM").alias("TRGT_CD_NM_12"),
    col("inabc.POST_AS_EXP_IN").alias("POST_AS_EXP_IN"),
    col("inabc.PRCS_CYC_DT_SK").alias("PRCS_CYC_DT_SK"),
    col("inabc.VAL_BASED_PGM_ID").alias("VAL_BASED_PGM_ID"),
    col("inabc.DSPTD_RCRD_CD").alias("DSPTD_RCRD_CD"),
    col("inabc.HOME_PLN_DSPT_ACTN_CD").alias("HOME_PLN_DSPT_ACTN_CD"),
    col("inabc.CMNT_TX").alias("CMNT_TX"),
    col("inabc.HOST_PLN_DSPT_ACTN_CD").alias("HOST_PLN_DSPT_ACTN_CD"),
    col("inabc.LOCAL_PLN_CTL_NO").alias("LOCAL_PLN_CTL_NO"),
    col("inabc.PRCS_SITE_CTL_NO").alias("PRCS_SITE_CTL_NO"),
    col("inabc.BILL_DTL_FMT_RCRD_TYP_CD").alias("BILL_DTL_FMT_RCRD_TYP_CD")
)

df_xmf_input = df_lkup_codes

df_xmf_main_pre = df_xmf_input.filter(
    (col("INTER_PLN_BILL_DTL_SK") != 0) & (col("INTER_PLN_BILL_DTL_SK") != 1)
)

df_xmf_main = df_xmf_main_pre.select(
    col("INTER_PLN_BILL_DTL_SK").alias("INTER_PLN_BILL_DTL_SK"),
    (col("INTER_PLN_BILL_DTL_ID")).alias("INTER_PLN_BILL_DTL_SRL_NO"),
    col("BUS_OWNER_ID").alias("BUS_OWNER_ID"),
    when(
        (col("TRGT_CD").isNull()) | (length(trim(col("TRGT_CD"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD")).alias("SRC_SYS_CD"),
    lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(EDWRunCycleDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("MBR_SK").alias("MBR_SK"),
    when(
        (col("TRGT_CD_1").isNull()) | (length(trim(col("TRGT_CD_1"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_1")).alias("ATTRBTN_PGM_CD"),
    when(
        (col("TRGT_CD_NM_1").isNull()) | (length(trim(col("TRGT_CD_NM_1"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_1")).alias("ATTRBTN_PGM_NM"),
    when(
        (col("TRGT_CD_2").isNull()) | (length(trim(col("TRGT_CD_2"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_2")).alias("CFA_RMBRMT_TYP_CD"),
    when(
        (col("TRGT_CD_NM_2").isNull()) | (length(trim(col("TRGT_CD_NM_2"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_2")).alias("CFA_RMBRMT_TYP_NM"),
    when(
        (col("TRGT_CD_3").isNull()) | (length(trim(col("TRGT_CD_3"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_3")).alias("CFA_TRANS_TYP_CD"),
    when(
        (col("TRGT_CD_NM_3").isNull()) | (length(trim(col("TRGT_CD_NM_3"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_3")).alias("CFA_TRANS_TYP_NM"),
    when(
        (col("TRGT_CD_4").isNull()) | (length(trim(col("TRGT_CD_4"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_4")).alias("CTL_PLN_CD"),
    when(
        (col("TRGT_CD_NM_4").isNull()) | (length(trim(col("TRGT_CD_NM_4"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_4")).alias("CTL_PLN_NM"),
    when(
        (col("TRGT_CD_5").isNull()) | (length(trim(col("TRGT_CD_5"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_5")).alias("EPSD_TYP_CD"),
    when(
        (col("TRGT_CD_NM_5").isNull()) | (length(trim(col("TRGT_CD_NM_5"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_5")).alias("EPSD_TYP_NM"),
    when(
        (col("TRGT_CD_6").isNull()) | (length(trim(col("TRGT_CD_6"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_6")).alias("HOME_CORP_PLN_CD"),
    when(
        (col("TRGT_CD_NM_6").isNull()) | (length(trim(col("TRGT_CD_NM_6"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_6")).alias("HOME_CORP_PLN_NM"),
    when(
        (col("TRGT_CD_7").isNull()) | (length(trim(col("TRGT_CD_7"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_7")).alias("HOME_PLN_CD"),
    when(
        (col("TRGT_CD_NM_7").isNull()) | (length(trim(col("TRGT_CD_NM_7"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_7")).alias("HOME_PLN_NM"),
    when(
        (col("TRGT_CD_8").isNull()) | (length(trim(col("TRGT_CD_8"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_8")).alias("HOST_CORP_PLN_CD"),
    when(
        (col("TRGT_CD_NM_8").isNull()) | (length(trim(col("TRGT_CD_NM_8"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_8")).alias("HOST_CORP_PLN_NM"),
    when(
        (col("TRGT_CD_9").isNull()) | (length(trim(col("TRGT_CD_9"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_9")).alias("HOST_PLN_CD"),
    when(
        (col("TRGT_CD_NM_9").isNull()) | (length(trim(col("TRGT_CD_NM_9"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_9")).alias("HOST_PLN_NM"),
    when(
        (col("TRGT_CD_10").isNull()) | (length(trim(col("TRGT_CD_10"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_10")).alias("LOCAL_PLN_CD"),
    when(
        (col("TRGT_CD_NM_10").isNull()) | (length(trim(col("TRGT_CD_NM_10"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_10")).alias("LOCAL_PLN_NM"),
    when(
        (col("TRGT_CD_11").isNull()) | (length(trim(col("TRGT_CD_11"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_11")).alias("VAL_BASED_PGM_CD"),
    when(
        (col("TRGT_CD_NM_11").isNull()) | (length(trim(col("TRGT_CD_NM_11"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_11")).alias("VAL_BASED_PGM_NM"),
    when(
        (col("TRGT_CD_12").isNull()) | (length(trim(col("TRGT_CD_12"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_12")).alias("VAL_BASED_PGM_PAYMT_TYP_CD"),
    when(
        (col("TRGT_CD_NM_12").isNull()) | (length(trim(col("TRGT_CD_NM_12"))) == 0),
        lit("UNK")
    ).otherwise(col("TRGT_CD_NM_12")).alias("VAL_BASED_PGM_PAYMT_TYP_NM"),
    col("BTCH_CRT_DT_SK").alias("BTCH_CRT_DT_SK"),
    col("CBF_SETL_STRT_DT_SK").alias("CBF_SETL_STRT_DT_SK"),
    col("CBF_SETL_BILL_END_DT_SK").alias("CBF_SETL_BILL_END_DT_SK"),
    col("CFA_DISP_DT_SK").alias("CFA_DISP_DT_SK"),
    col("CFA_PRCS_DT_SK").alias("CFA_PRCS_DT_SK"),
    col("HOME_AUTH_DENIAL_DT_SK").alias("HOME_AUTH_DENIAL_DT_SK"),
    col("INCUR_PERD_STRT_DT_SK").alias("INCUR_PERD_STRT_DT_SK"),
    col("INCUR_PERD_END_DT_SK").alias("INCUR_PERD_END_DT_SK"),
    col("MSRMNT_PERD_STRT_DT_SK").alias("MSRMNT_PERD_STRT_DT_SK"),
    col("MSRMNT_PERD_END_DT_SK").alias("MSRMNT_PERD_END_DT_SK"),
    col("MBR_ELIG_STRT_DT_SK").alias("MBR_ELIG_STRT_DT_SK"),
    col("MBR_ELIG_END_DT_SK").alias("MBR_ELIG_END_DT_SK"),
    col("PRCS_CYC_YR_MO_SK").alias("PRCS_CYC_YR_MO_SK"),
    col("SETL_DT_SK").alias("SETL_DT_SK"),
    col("ACES_FEE_ADJ_AMT").alias("ACES_FEE_ADJ_AMT"),
    col("CBF_SETL_RATE_AMT").alias("CBF_SETL_RATE_AMT"),
    col("MBR_LVL_CHRG_AMT").alias("MBR_LVL_CHRG_AMT"),
    col("ATRBD_PROV_ID").alias("ATRBD_PROV_ID"),
    col("ATRBD_PROV_NTNL_PROV_ID").alias("ATRBD_PROV_NTNL_PROV_ID"),
    col("ATTRBTN_PROV_PGM_NM").alias("ATTRBTN_PROV_PGM_NM"),
    col("BCBSA_MMI_ID").alias("BCBSA_MMI_ID"),
    col("CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    col("CBF_SCCF_NO").alias("CBF_SCCF_NO"),
    when(
        (col("MBR_UNIQ_KEY").isNull()) | (length(trim(col("MBR_UNIQ_KEY"))) == 0),
        lit("0")
    ).otherwise(col("MBR_UNIQ_KEY")).alias("MBR_UNIQ_KEY"),
    col("GRP_ID").alias("GRP_ID"),
    col("HOME_PLN_MBR_ID").alias("HOME_PLN_MBR_ID"),
    col("ITS_SUB_ID").alias("ITS_SUB_ID"),
    col("ORIG_INTER_PLN_BILL_DTL_SRL_NO").alias("ORIG_INTER_PLN_BILL_DTL_SRL_NO"),
    col("ORIG_ITS_CLM_SCCF_NO").alias("ORIG_ITS_CLM_SCCF_NO"),
    col("PROV_GRP_NM").alias("PROV_GRP_NM"),
    col("VRNC_ACCT_ID").alias("VRNC_ACCT_ID"),
    col("BTCH_STTUS_CD_TX").alias("BTCH_STTUS_CD_TX"),
    col("STTUS_CD_TX").alias("STTUS_CD_TX"),
    col("ERR_CD_TX").alias("ERR_CD_TX"),
    lit(EDWRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(EDWRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ATTRBTN_PGM_CD_SK").alias("ATTRBTN_PGM_CD_SK"),
    col("CFA_RMBRMT_TYP_CD_SK").alias("CFA_RMBRMT_TYP_CD_SK"),
    col("CFA_TRANS_TYP_CD_SK").alias("CFA_TRANS_TYP_CD_SK"),
    col("CTL_PLN_CD_SK").alias("CTL_PLN_CD_SK"),
    col("EPSD_TYP_CD_SK").alias("EPSD_TYP_CD_SK"),
    col("HOME_CORP_PLN_CD_SK").alias("HOME_CORP_PLN_CD_SK"),
    col("HOME_PLN_CD_SK").alias("HOME_PLN_CD_SK"),
    col("HOST_CORP_PLN_CD_SK").alias("HOST_CORP_PLN_CD_SK"),
    col("HOST_PLN_CD_SK").alias("HOST_PLN_CD_SK"),
    col("LOCAL_PLN_CD_SK").alias("LOCAL_PLN_CD_SK"),
    col("VAL_BASED_PGM_CD_SK").alias("VAL_BASED_PGM_CD_SK"),
    col("VAL_BASED_PGM_PAYMT_TYP_CD_SK").alias("VAL_BASED_PGM_PAYMT_TYP_CD_SK"),
    col("POST_AS_EXP_IN").alias("POST_AS_EXP_IN"),
    col("PRCS_CYC_DT_SK").alias("PRCS_CYC_DT_SK"),
    col("VAL_BASED_PGM_ID").alias("VAL_BASED_PGM_ID"),
    col("DSPTD_RCRD_CD").alias("DSPTD_RCRD_CD"),
    col("HOME_PLN_DSPT_ACTN_CD").alias("HOME_PLN_DSPT_ACTN_CD"),
    col("CMNT_TX").alias("CMNT_TX"),
    col("HOST_PLN_DSPT_ACTN_CD").alias("HOST_PLN_DSPT_ACTN_CD"),
    col("LOCAL_PLN_CTL_NO").alias("LOCAL_PLN_CTL_NO"),
    col("PRCS_SITE_CTL_NO").alias("PRCS_SITE_CTL_NO"),
    col("BILL_DTL_FMT_RCRD_TYP_CD").alias("BILL_DTL_FMT_RCRD_TYP_CD")
)

df_xmf_input_withrow = df_xmf_input.withColumn("row_id", row_number().over(Window.orderBy(lit(1))))

df_nalink_pre = df_xmf_input_withrow.filter(col("row_id") == 1)

df_nalink = df_nalink_pre.select(
    lit("1").alias("INTER_PLN_BILL_DTL_SK"),
    lit("NA").alias("INTER_PLN_BILL_DTL_SRL_NO"),
    lit("NA").alias("BUS_OWNER_ID"),
    lit("NA").alias("SRC_SYS_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("1").alias("MBR_SK"),
    lit("NA").alias("ATTRBTN_PGM_CD"),
    lit("NA").alias("ATTRBTN_PGM_NM"),
    lit("NA").alias("CFA_RMBRMT_TYP_CD"),
    lit("NA").alias("CFA_RMBRMT_TYP_NM"),
    lit("NA").alias("CFA_TRANS_TYP_CD"),
    lit("NA").alias("CFA_TRANS_TYP_NM"),
    lit("NA").alias("CTL_PLN_CD"),
    lit("NA").alias("CTL_PLN_NM"),
    lit("NA").alias("EPSD_TYP_CD"),
    lit("NA").alias("EPSD_TYP_NM"),
    lit("NA").alias("HOME_CORP_PLN_CD"),
    lit("NA").alias("HOME_CORP_PLN_NM"),
    lit("NA").alias("HOME_PLN_CD"),
    lit("NA").alias("HOME_PLN_NM"),
    lit("NA").alias("HOST_CORP_PLN_CD"),
    lit("NA").alias("HOST_CORP_PLN_NM"),
    lit("NA").alias("HOST_PLN_CD"),
    lit("NA").alias("HOST_PLN_NM"),
    lit("NA").alias("LOCAL_PLN_CD"),
    lit("NA").alias("LOCAL_PLN_NM"),
    lit("NA").alias("VAL_BASED_PGM_CD"),
    lit("NA").alias("VAL_BASED_PGM_NM"),
    lit("NA").alias("VAL_BASED_PGM_PAYMT_TYP_CD"),
    lit("NA").alias("VAL_BASED_PGM_PAYMT_TYP_NM"),
    lit("1753-01-01").alias("BTCH_CRT_DT_SK"),
    lit("1753-01-01").alias("CBF_SETL_STRT_DT_SK"),
    lit("1753-01-01").alias("CBF_SETL_BILL_END_DT_SK"),
    lit("1753-01-01").alias("CFA_DISP_DT_SK"),
    lit("1753-01-01").alias("CFA_PRCS_DT_SK"),
    lit("1753-01-01").alias("HOME_AUTH_DENIAL_DT_SK"),
    lit("1753-01-01").alias("INCUR_PERD_STRT_DT_SK"),
    lit("1753-01-01").alias("INCUR_PERD_END_DT_SK"),
    lit("1753-01-01").alias("MSRMNT_PERD_STRT_DT_SK"),
    lit("1753-01-01").alias("MSRMNT_PERD_END_DT_SK"),
    lit("1753-01-01").alias("MBR_ELIG_STRT_DT_SK"),
    lit("1753-01-01").alias("MBR_ELIG_END_DT_SK"),
    lit("175301").alias("PRCS_CYC_YR_MO_SK"),
    lit("1753-01-01").alias("SETL_DT_SK"),
    lit("0.00").alias("ACES_FEE_ADJ_AMT"),
    lit("0.00").alias("CBF_SETL_RATE_AMT"),
    lit("0.00").alias("MBR_LVL_CHRG_AMT"),
    lit("NA").alias("ATRBD_PROV_ID"),
    lit("NA").alias("ATRBD_PROV_NTNL_PROV_ID"),
    lit("NA").alias("ATTRBTN_PROV_PGM_NM"),
    lit("NA").alias("BCBSA_MMI_ID"),
    lit("NA").alias("CONSIS_MBR_ID"),
    lit("NA").alias("CBF_SCCF_NO"),
    lit("1").alias("MBR_UNIQ_KEY"),
    lit("NA").alias("GRP_ID"),
    lit("NA").alias("HOME_PLN_MBR_ID"),
    lit("NA").alias("ITS_SUB_ID"),
    lit("NA").alias("ORIG_INTER_PLN_BILL_DTL_SRL_NO"),
    lit("NA").alias("ORIG_ITS_CLM_SCCF_NO"),
    lit("NA").alias("PROV_GRP_NM"),
    lit("NA").alias("VRNC_ACCT_ID"),
    lit("NA").alias("BTCH_STTUS_CD_TX"),
    lit("NA").alias("STTUS_CD_TX"),
    lit("NA").alias("ERR_CD_TX"),
    lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("1").alias("ATTRBTN_PGM_CD_SK"),
    lit("1").alias("CFA_RMBRMT_TYP_CD_SK"),
    lit("1").alias("CFA_TRANS_TYP_CD_SK"),
    lit("1").alias("CTL_PLN_CD_SK"),
    lit("1").alias("EPSD_TYP_CD_SK"),
    lit("1").alias("HOME_CORP_PLN_CD_SK"),
    lit("1").alias("HOME_PLN_CD_SK"),
    lit("1").alias("HOST_CORP_PLN_CD_SK"),
    lit("1").alias("HOST_PLN_CD_SK"),
    lit("1").alias("LOCAL_PLN_CD_SK"),
    lit("1").alias("VAL_BASED_PGM_CD_SK"),
    lit("1").alias("VAL_BASED_PGM_PAYMT_TYP_CD_SK"),
    lit("N").alias("POST_AS_EXP_IN"),
    lit("1753-01-01").alias("PRCS_CYC_DT_SK"),
    lit("NA").alias("VAL_BASED_PGM_ID"),
    lit("NA").alias("DSPTD_RCRD_CD"),
    lit("NA").alias("HOME_PLN_DSPT_ACTN_CD"),
    lit("NA").alias("CMNT_TX"),
    lit("NA").alias("HOST_PLN_DSPT_ACTN_CD"),
    lit("NA").alias("LOCAL_PLN_CTL_NO"),
    lit("NA").alias("PRCS_SITE_CTL_NO"),
    lit("NA").alias("BILL_DTL_FMT_RCRD_TYP_CD")
)

df_unklink_pre = df_xmf_input_withrow.filter(col("row_id") == 1)

df_unklink = df_unklink_pre.select(
    lit("0").alias("INTER_PLN_BILL_DTL_SK"),
    lit("UNK").alias("INTER_PLN_BILL_DTL_SRL_NO"),
    lit("UNK").alias("BUS_OWNER_ID"),
    lit("UNK").alias("SRC_SYS_CD"),
    lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    lit("0").alias("MBR_SK"),
    lit("UNK").alias("ATTRBTN_PGM_CD"),
    lit("UNK").alias("ATTRBTN_PGM_NM"),
    lit("UNK").alias("CFA_RMBRMT_TYP_CD"),
    lit("UNK").alias("CFA_RMBRMT_TYP_NM"),
    lit("UNK").alias("CFA_TRANS_TYP_CD"),
    lit("UNK").alias("CFA_TRANS_TYP_NM"),
    lit("UNK").alias("CTL_PLN_CD"),
    lit("UNK").alias("CTL_PLN_NM"),
    lit("UNK").alias("EPSD_TYP_CD"),
    lit("UNK").alias("EPSD_TYP_NM"),
    lit("UNK").alias("HOME_CORP_PLN_CD"),
    lit("UNK").alias("HOME_CORP_PLN_NM"),
    lit("UNK").alias("HOME_PLN_CD"),
    lit("UNK").alias("HOME_PLN_NM"),
    lit("UNK").alias("HOST_CORP_PLN_CD"),
    lit("UNK").alias("HOST_CORP_PLN_NM"),
    lit("UNK").alias("HOST_PLN_CD"),
    lit("UNK").alias("HOST_PLN_NM"),
    lit("UNK").alias("LOCAL_PLN_CD"),
    lit("UNK").alias("LOCAL_PLN_NM"),
    lit("UNK").alias("VAL_BASED_PGM_CD"),
    lit("UNK").alias("VAL_BASED_PGM_NM"),
    lit("UNK").alias("VAL_BASED_PGM_PAYMT_TYP_CD"),
    lit("UNK").alias("VAL_BASED_PGM_PAYMT_TYP_NM"),
    lit("1753-01-01").alias("BTCH_CRT_DT_SK"),
    lit("1753-01-01").alias("CBF_SETL_STRT_DT_SK"),
    lit("1753-01-01").alias("CBF_SETL_BILL_END_DT_SK"),
    lit("1753-01-01").alias("CFA_DISP_DT_SK"),
    lit("1753-01-01").alias("CFA_PRCS_DT_SK"),
    lit("1753-01-01").alias("HOME_AUTH_DENIAL_DT_SK"),
    lit("1753-01-01").alias("INCUR_PERD_STRT_DT_SK"),
    lit("1753-01-01").alias("INCUR_PERD_END_DT_SK"),
    lit("1753-01-01").alias("MSRMNT_PERD_STRT_DT_SK"),
    lit("1753-01-01").alias("MSRMNT_PERD_END_DT_SK"),
    lit("1753-01-01").alias("MBR_ELIG_STRT_DT_SK"),
    lit("1753-01-01").alias("MBR_ELIG_END_DT_SK"),
    lit("175301").alias("PRCS_CYC_YR_MO_SK"),
    lit("1753-01-01").alias("SETL_DT_SK"),
    lit("0.00").alias("ACES_FEE_ADJ_AMT"),
    lit("0.00").alias("CBF_SETL_RATE_AMT"),
    lit("0.00").alias("MBR_LVL_CHRG_AMT"),
    lit("UNK").alias("ATRBD_PROV_ID"),
    lit("UNK").alias("ATRBD_PROV_NTNL_PROV_ID"),
    lit("UNK").alias("ATTRBTN_PROV_PGM_NM"),
    lit("UNK").alias("BCBSA_MMI_ID"),
    lit("UNK").alias("CONSIS_MBR_ID"),
    lit("UNK").alias("CBF_SCCF_NO"),
    lit("0").alias("MBR_UNIQ_KEY"),
    lit("UNK").alias("GRP_ID"),
    lit("UNK").alias("HOME_PLN_MBR_ID"),
    lit("UNK").alias("ITS_SUB_ID"),
    lit("UNK").alias("ORIG_INTER_PLN_BILL_DTL_SRL_NO"),
    lit("UNK").alias("ORIG_ITS_CLM_SCCF_NO"),
    lit("UNK").alias("PROV_GRP_NM"),
    lit("UNK").alias("VRNC_ACCT_ID"),
    lit("UNK").alias("BTCH_STTUS_CD_TX"),
    lit("UNK").alias("STTUS_CD_TX"),
    lit("UNK").alias("ERR_CD_TX"),
    lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("ATTRBTN_PGM_CD_SK"),
    lit("0").alias("CFA_RMBRMT_TYP_CD_SK"),
    lit("0").alias("CFA_TRANS_TYP_CD_SK"),
    lit("0").alias("CTL_PLN_CD_SK"),
    lit("0").alias("EPSD_TYP_CD_SK"),
    lit("0").alias("HOME_CORP_PLN_CD_SK"),
    lit("0").alias("HOME_PLN_CD_SK"),
    lit("0").alias("HOST_CORP_PLN_CD_SK"),
    lit("0").alias("HOST_PLN_CD_SK"),
    lit("0").alias("LOCAL_PLN_CD_SK"),
    lit("0").alias("VAL_BASED_PGM_CD_SK"),
    lit("0").alias("VAL_BASED_PGM_PAYMT_TYP_CD_SK"),
    lit("N").alias("POST_AS_EXP_IN"),
    lit("1753-01-01").alias("PRCS_CYC_DT_SK"),
    lit("UNK").alias("VAL_BASED_PGM_ID"),
    lit("UNK").alias("DSPTD_RCRD_CD"),
    lit("UNK").alias("HOME_PLN_DSPT_ACTN_CD"),
    lit("UNK").alias("CMNT_TX"),
    lit("UNK").alias("HOST_PLN_DSPT_ACTN_CD"),
    lit("UNK").alias("LOCAL_PLN_CTL_NO"),
    lit("UNK").alias("PRCS_SITE_CTL_NO"),
    lit("UNK").alias("BILL_DTL_FMT_RCRD_TYP_CD")
)

df_fnl_all = df_xmf_main.unionByName(df_nalink).unionByName(df_unklink)

df_fnl_all_ordered = df_fnl_all.select(
    "INTER_PLN_BILL_DTL_SK",
    "INTER_PLN_BILL_DTL_SRL_NO",
    "BUS_OWNER_ID",
    "SRC_SYS_CD",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "MBR_SK",
    "ATTRBTN_PGM_CD",
    "ATTRBTN_PGM_NM",
    "CFA_RMBRMT_TYP_CD",
    "CFA_RMBRMT_TYP_NM",
    "CFA_TRANS_TYP_CD",
    "CFA_TRANS_TYP_NM",
    "CTL_PLN_CD",
    "CTL_PLN_NM",
    "EPSD_TYP_CD",
    "EPSD_TYP_NM",
    "HOME_CORP_PLN_CD",
    "HOME_CORP_PLN_NM",
    "HOME_PLN_CD",
    "HOME_PLN_NM",
    "HOST_CORP_PLN_CD",
    "HOST_CORP_PLN_NM",
    "HOST_PLN_CD",
    "HOST_PLN_NM",
    "LOCAL_PLN_CD",
    "LOCAL_PLN_NM",
    "VAL_BASED_PGM_CD",
    "VAL_BASED_PGM_NM",
    "VAL_BASED_PGM_PAYMT_TYP_CD",
    "VAL_BASED_PGM_PAYMT_TYP_NM",
    "BTCH_CRT_DT_SK",
    "CBF_SETL_STRT_DT_SK",
    "CBF_SETL_BILL_END_DT_SK",
    "CFA_DISP_DT_SK",
    "CFA_PRCS_DT_SK",
    "HOME_AUTH_DENIAL_DT_SK",
    "INCUR_PERD_STRT_DT_SK",
    "INCUR_PERD_END_DT_SK",
    "MSRMNT_PERD_STRT_DT_SK",
    "MSRMNT_PERD_END_DT_SK",
    "MBR_ELIG_STRT_DT_SK",
    "MBR_ELIG_END_DT_SK",
    "PRCS_CYC_YR_MO_SK",
    "SETL_DT_SK",
    "ACES_FEE_ADJ_AMT",
    "CBF_SETL_RATE_AMT",
    "MBR_LVL_CHRG_AMT",
    "ATRBD_PROV_ID",
    "ATRBD_PROV_NTNL_PROV_ID",
    "ATTRBTN_PROV_PGM_NM",
    "BCBSA_MMI_ID",
    "CONSIS_MBR_ID",
    "CBF_SCCF_NO",
    "MBR_UNIQ_KEY",
    "GRP_ID",
    "HOME_PLN_MBR_ID",
    "ITS_SUB_ID",
    "ORIG_INTER_PLN_BILL_DTL_SRL_NO",
    "ORIG_ITS_CLM_SCCF_NO",
    "PROV_GRP_NM",
    "VRNC_ACCT_ID",
    "BTCH_STTUS_CD_TX",
    "STTUS_CD_TX",
    "ERR_CD_TX",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "ATTRBTN_PGM_CD_SK",
    "CFA_RMBRMT_TYP_CD_SK",
    "CFA_TRANS_TYP_CD_SK",
    "CTL_PLN_CD_SK",
    "EPSD_TYP_CD_SK",
    "HOME_CORP_PLN_CD_SK",
    "HOME_PLN_CD_SK",
    "HOST_CORP_PLN_CD_SK",
    "HOST_PLN_CD_SK",
    "LOCAL_PLN_CD_SK",
    "VAL_BASED_PGM_CD_SK",
    "VAL_BASED_PGM_PAYMT_TYP_CD_SK",
    "POST_AS_EXP_IN",
    "PRCS_CYC_DT_SK",
    "VAL_BASED_PGM_ID",
    "DSPTD_RCRD_CD",
    "HOME_PLN_DSPT_ACTN_CD",
    "CMNT_TX",
    "HOST_PLN_DSPT_ACTN_CD",
    "LOCAL_PLN_CTL_NO",
    "PRCS_SITE_CTL_NO",
    "BILL_DTL_FMT_RCRD_TYP_CD"
)

df_fnl_all_rpad = df_fnl_all_ordered \
.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
.withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")) \
.withColumn("BTCH_CRT_DT_SK", rpad(col("BTCH_CRT_DT_SK"), 10, " ")) \
.withColumn("CBF_SETL_STRT_DT_SK", rpad(col("CBF_SETL_STRT_DT_SK"), 10, " ")) \
.withColumn("CBF_SETL_BILL_END_DT_SK", rpad(col("CBF_SETL_BILL_END_DT_SK"), 10, " ")) \
.withColumn("CFA_DISP_DT_SK", rpad(col("CFA_DISP_DT_SK"), 10, " ")) \
.withColumn("CFA_PRCS_DT_SK", rpad(col("CFA_PRCS_DT_SK"), 10, " ")) \
.withColumn("HOME_AUTH_DENIAL_DT_SK", rpad(col("HOME_AUTH_DENIAL_DT_SK"), 10, " ")) \
.withColumn("INCUR_PERD_STRT_DT_SK", rpad(col("INCUR_PERD_STRT_DT_SK"), 10, " ")) \
.withColumn("INCUR_PERD_END_DT_SK", rpad(col("INCUR_PERD_END_DT_SK"), 10, " ")) \
.withColumn("MSRMNT_PERD_STRT_DT_SK", rpad(col("MSRMNT_PERD_STRT_DT_SK"), 10, " ")) \
.withColumn("MSRMNT_PERD_END_DT_SK", rpad(col("MSRMNT_PERD_END_DT_SK"), 10, " ")) \
.withColumn("MBR_ELIG_STRT_DT_SK", rpad(col("MBR_ELIG_STRT_DT_SK"), 10, " ")) \
.withColumn("MBR_ELIG_END_DT_SK", rpad(col("MBR_ELIG_END_DT_SK"), 10, " ")) \
.withColumn("POST_AS_EXP_IN", rpad(col("POST_AS_EXP_IN"), 1, " ")) \
.withColumn("PRCS_CYC_DT_SK", rpad(col("PRCS_CYC_DT_SK"), 10, " ")) \
.withColumn("PRCS_CYC_YR_MO_SK", rpad(col("PRCS_CYC_YR_MO_SK"), 6, " ")) \
.withColumn("VAL_BASED_PGM_ID", rpad(col("VAL_BASED_PGM_ID"), 7, " "))

write_files(
    df_fnl_all_rpad,
    f"{adls_path}/load/INTER_PLN_BILL_DTL_F.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="^",
    nullValue=None
)