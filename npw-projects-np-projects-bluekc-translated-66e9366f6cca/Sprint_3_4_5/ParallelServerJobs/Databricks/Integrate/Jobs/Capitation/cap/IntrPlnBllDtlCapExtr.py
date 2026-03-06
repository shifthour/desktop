# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called by:
# MAGIC                    TreoIdsCapitationOnetimeCntl
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                                                              Project/                                                                                                                                          Code                  Date
# MAGIC Developer                         Date                  Altiris #               Change Description                                                                                                                       Reviewer            Reviewed
# MAGIC --------------------------------------   -------------------      ----------------------     -----------------------------------------------------------------------------------------------------------------------    -------------------------   -------------------------  -------------------
# MAGIC Karthik Chintalapani          2015-10-14       5212                   Initial Programming                                                                                             IntegrateDev1     Kalyan Neelam   2015-10-19
# MAGIC Karthik Chintalapani          2016-01-10       5212                   Modified the logic for the CAD_FUND_ID and CAP_FUND_SK fields               IntegrateDev1     Kalyan Neelam   2016-01-19
# MAGIC Karthik Chintalapani          2016-02-02       5212                   Added new filter in the extract query POST_AS_EXP_IN='N'                            IntegrateDev1     Kalyan Neelam   2016-02-02              
# MAGIC Karthik Chintalapani          2016-08-24       5212 PI               Mapped new column PRCS_CYC_DT_SK to PD_DT_SK                                IntegrateDev1     Kalyan Neelam   2016-08-25
# MAGIC Karthik Chintalapani          2016-08-30       5212 PI               Added  'MED' to the derivation for column CRFD_ACCT_CAT                          IntegrateDev1     Kalyan Neelam   2016-08-31
# MAGIC                                                                                               as per the mapping changes.                                        
# MAGIC Tejaswi Gogineni              2018-10-23      INC0473770         Changed the Datatype for  SEQ_NO across the job to match it with the           IntegrateDev1     Hugh Sisson       2018-12-03             
# MAGIC                                                                                               table datatype to get rid of warnings..                                                
# MAGIC 
# MAGIC Mohan Karnati                 2019-11-18      171618                Removed MBR_ENR.ELIG_IN = 'Y' in MBR_ENR,FNCL_LOB,                         IntegrateDev2     Kalyan Neelam   2019-11-18
# MAGIC                                                                                             PROD_SH_NM stages to pull all members irrespective of theire eligibility  
# MAGIC                                                                                             to fix BDF inbound load issue

# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DecimalType, StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/CapPK
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunID = get_widget_value('RunID','20181203010101')
RunCycYrMo = get_widget_value('RunCycYrMo','201811')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrentDate = get_widget_value('CurrentDate','2018-12-03')
CurrRunCycle = get_widget_value('CurrRunCycle','146')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1951781674')
SrcSysCd = get_widget_value('SrcSysCd','BCBSA')

# Read from IDS_INTER_PLN_BILL_DTL (DB2)
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
query_IDS_INTER_PLN_BILL_DTL = f"""
SELECT DISTINCT
INTER_PLN_BILL_DTL.INTER_PLN_BILL_DTL_SK,
INTER_PLN_BILL_DTL.INTER_PLN_BILL_DTL_ID,
INTER_PLN_BILL_DTL.BUS_OWNER_ID,
INTER_PLN_BILL_DTL.SRC_SYS_CD_SK,
INTER_PLN_BILL_DTL.CRT_RUN_CYC_EXCTN_SK,
INTER_PLN_BILL_DTL.LAST_UPDT_RUN_CYC_EXCTN_SK,
INTER_PLN_BILL_DTL.MBR_SK,
INTER_PLN_BILL_DTL.ATTRBTN_PGM_CD_SK,
INTER_PLN_BILL_DTL.CFA_RMBRMT_TYP_CD_SK,
INTER_PLN_BILL_DTL.CFA_TRANS_TYP_CD_SK,
INTER_PLN_BILL_DTL.CTL_PLN_CD_SK,
INTER_PLN_BILL_DTL.EPSD_TYP_CD_SK,
INTER_PLN_BILL_DTL.HOME_CORP_PLN_CD_SK,
INTER_PLN_BILL_DTL.HOME_PLN_CD_SK,
INTER_PLN_BILL_DTL.HOST_CORP_PLN_CD_SK,
INTER_PLN_BILL_DTL.HOST_PLN_CD_SK,
INTER_PLN_BILL_DTL.LOCAL_PLN_CD_SK,
INTER_PLN_BILL_DTL.VAL_BASED_PGM_CD_SK,
INTER_PLN_BILL_DTL.VAL_BASED_PGM_PAYMT_TYP_CD_SK,
INTER_PLN_BILL_DTL.BTCH_CRT_DT_SK,
INTER_PLN_BILL_DTL.CBF_SETL_STRT_DT_SK,
INTER_PLN_BILL_DTL.CBF_SETL_BILL_END_DT_SK,
INTER_PLN_BILL_DTL.CFA_DISP_DT_SK,
INTER_PLN_BILL_DTL.CFA_PRCS_DT_SK,
INTER_PLN_BILL_DTL.HOME_AUTH_DENIAL_DT_SK,
INTER_PLN_BILL_DTL.INCUR_PERD_STRT_DT_SK,
INTER_PLN_BILL_DTL.INCUR_PERD_END_DT_SK,
INTER_PLN_BILL_DTL.MSRMNT_PERD_STRT_DT_SK,
INTER_PLN_BILL_DTL.MSRMNT_PERD_END_DT_SK,
INTER_PLN_BILL_DTL.MBR_ELIG_STRT_DT_SK,
INTER_PLN_BILL_DTL.MBR_ELIG_END_DT_SK,
INTER_PLN_BILL_DTL.PRCS_CYC_YR_MO_SK,
INTER_PLN_BILL_DTL.SETL_DT_SK,
INTER_PLN_BILL_DTL.ACES_FEE_ADJ_AMT,
INTER_PLN_BILL_DTL.CBF_SETL_RATE_AMT,
INTER_PLN_BILL_DTL.MBR_LVL_CHRG_AMT,
INTER_PLN_BILL_DTL.ATRBD_PROV_ID,
INTER_PLN_BILL_DTL.ATRBD_PROV_NTNL_PROV_ID,
INTER_PLN_BILL_DTL.ATTRBTN_PROV_PGM_NM,
INTER_PLN_BILL_DTL.BCBSA_MMI_ID,
INTER_PLN_BILL_DTL.CONSIS_MBR_ID,
INTER_PLN_BILL_DTL.CBF_SCCF_NO,
INTER_PLN_BILL_DTL.GRP_ID,
INTER_PLN_BILL_DTL.HOME_PLN_MBR_ID,
INTER_PLN_BILL_DTL.ITS_SUB_ID,
INTER_PLN_BILL_DTL.ORIG_INTER_PLN_BILL_DTL_SRL_NO,
INTER_PLN_BILL_DTL.ORIG_ITS_CLM_SCCF_NO,
INTER_PLN_BILL_DTL.PROV_GRP_NM,
INTER_PLN_BILL_DTL.VRNC_ACCT_ID,
INTER_PLN_BILL_DTL.BTCH_STTUS_CD_TX,
INTER_PLN_BILL_DTL.STTUS_CD_TX,
INTER_PLN_BILL_DTL.ERR_CD_TX,
INTER_PLN_BILL_DTL.POST_AS_EXP_IN,
INTER_PLN_BILL_DTL.PRCS_CYC_DT_SK
FROM {IDSOwner}.INTER_PLN_BILL_DTL INTER_PLN_BILL_DTL
WHERE
INTER_PLN_BILL_DTL.PRCS_CYC_YR_MO_SK = '{RunCycYrMo}'
AND INTER_PLN_BILL_DTL.POST_AS_EXP_IN='N'
"""
df_IDS_INTER_PLN_BILL_DTL = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_IDS_INTER_PLN_BILL_DTL)
    .load()
)

# hf_etrnl_mbr_sk (scenario C: read from parquet)
df_hf_etrnl_mbr_sk = spark.read.parquet(f"{adls_path}/hf_etrnl_mbr_sk.parquet")

# CAP_FUND (DB2) -> hf_ipbd_cap_fund_id (scenario A dedup on CAP_FUND_ID)
query_CAP_FUND = f"""
SELECT
CAP_FUND_ID,
CAP_FUND_SK
FROM
{IDSOwner}.CAP_FUND
{IDSOwner}.p_cae_mbr_drvr P_CAE_MBR_DRVR, {IDSOwner}.mbr MBR, {IDSOwner}.sub SUB, {IDSOwner}.cd_mppng CD_MPPNG
"""
df_CAP_FUND = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_CAP_FUND)
    .load()
)
df_CAP_FUND_dedup = dedup_sort(
    df_CAP_FUND,
    partition_cols=["CAP_FUND_ID"],
    sort_cols=[]
)

# FNCL_LOB (DB2) -> hf_ipbd_cap_fncl_lod_cd (scenario A dedup on MBR_SK)
query_FNCL_LOB = f"""
SELECT distinct
MBR_ENR.MBR_SK,
PROD.PROD_ID,
FNCL_LOB.FNCL_LOB_CD
FROM
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.PROD PROD,
{IDSOwner}.FNCL_LOB FNCL_LOB,
{IDSOwner}.CD_MPPNG MPPNG,
{IDSOwner}.INTER_PLN_BILL_DTL IPBD
WHERE
MBR_ENR.PROD_SK=PROD.PROD_SK
AND PROD.FNCL_LOB_SK = FNCL_LOB.FNCL_LOB_SK
AND MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MPPNG.CD_MPPNG_SK
AND IPBD.MBR_SK=MBR_ENR.MBR_SK
AND MPPNG.TRGT_CD='MED'
AND MBR_ENR.EFF_DT_SK <= IPBD.INCUR_PERD_END_DT_SK
AND MBR_ENR.TERM_DT_SK >= IPBD.INCUR_PERD_STRT_DT_SK
AND PROD.PROD_EFF_DT_SK <= IPBD.INCUR_PERD_END_DT_SK
AND PROD.PROD_TERM_DT_SK >= IPBD.INCUR_PERD_STRT_DT_SK
"""
df_FNCL_LOB = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_FNCL_LOB)
    .load()
)
df_FNCL_LOB_dedup = dedup_sort(
    df_FNCL_LOB,
    partition_cols=["MBR_SK"],
    sort_cols=[]
)

# CD_MPPNG (DB2) -> hf_ipbd_cd_mppng (scenario A dedup on CD_MPPNG_SK)
query_CD_MPPNG = f"""
SELECT
CD_MPPNG_SK,
SRC_CD,
TRGT_CD
FROM
{IDSOwner}.CD_MPPNG MPPNG
{IDSOwner}.p_cae_mbr_drvr P_CAE_MBR_DRVR, {IDSOwner}.mbr MBR, {IDSOwner}.sub SUB, {IDSOwner}.cd_mppng CD_MPPNG
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_CD_MPPNG)
    .load()
)
df_CD_MPPNG_dedup = dedup_sort(
    df_CD_MPPNG,
    partition_cols=["CD_MPPNG_SK"],
    sort_cols=[]
)

# PROD_SH_NM (DB2) -> hf_ipbd_cap_prod_sh_nm (scenario A dedup on MBR_SK)
query_PROD_SH_NM = f"""
SELECT
MBR_ENR.MBR_SK,
PROD_SH_NM.PROD_SH_NM,
MBR_ENR.TERM_DT_SK
FROM
{IDSOwner}.PROD PROD,
{IDSOwner}.PROD_SH_NM PROD_SH_NM,
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.CD_MPPNG MPPNG
WHERE
MBR_ENR.PROD_SK = PROD.PROD_SK
AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
AND MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK=MPPNG.CD_MPPNG_SK
AND MPPNG.TRGT_CD='MED'
AND MBR_ENR.TERM_DT_SK =(
    Select
        MAX(MBR_ENR1.TERM_DT_SK)
    FROM
        {IDSOwner}.MBR_ENR MBR_ENR1
    WHERE
    MBR_ENR1.MBR_SK = MBR_ENR.MBR_SK
    AND MBR_ENR1.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK=MPPNG.CD_MPPNG_SK
    AND MBR_ENR1.ELIG_IN = 'Y'
)
"""
df_PROD_SH_NM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_PROD_SH_NM)
    .load()
)
df_PROD_SH_NM_dedup = dedup_sort(
    df_PROD_SH_NM,
    partition_cols=["MBR_SK"],
    sort_cols=[]
)

# MBR_ENR (DB2) -> hf_bcbsa_ipbd_mbr_enr_output (scenario A dedup on MBR_SK)
query_MBR_ENR = f"""
SELECT distinct
MBR.MBR_SK,
CLS.CLS_ID,
CLS_PLN.CLS_PLN_ID,
MBR_ENR.GRP_SK,
MBR_ENR.EFF_DT_SK,
MBR_ENR.TERM_DT_SK,
GRP.GRP_ID
FROM
{IDSOwner}.MBR MBR,
{IDSOwner}.MBR_ENR MBR_ENR,
{IDSOwner}.CLS CLS,
{IDSOwner}.CLS_PLN CLS_PLN,
{IDSOwner}.CD_MPPNG MPPNG,
{IDSOwner}.GRP GRP,
{IDSOwner}.INTER_PLN_BILL_DTL IPBD
WHERE
MBR.MBR_SK=MBR_ENR.MBR_SK
AND MBR_ENR.GRP_SK=GRP.GRP_SK
AND MBR_ENR.CLS_SK = CLS.CLS_SK
AND MBR_ENR.CLS_PLN_SK = CLS_PLN.CLS_PLN_SK
AND MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MPPNG.CD_MPPNG_SK
AND IPBD.MBR_SK=MBR_ENR.MBR_SK
AND MPPNG.TRGT_CD='MED'
AND MBR_ENR.EFF_DT_SK <= IPBD.INCUR_PERD_END_DT_SK
AND MBR_ENR.TERM_DT_SK >= IPBD.INCUR_PERD_STRT_DT_SK
"""
df_MBR_ENR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_MBR_ENR)
    .load()
)
df_MBR_ENR_dedup = dedup_sort(
    df_MBR_ENR,
    partition_cols=["MBR_SK"],
    sort_cols=[]
)

# Xfm_Mbr_Lkp logic (Transformer)
# 1) Primary link: df_IDS_INTER_PLN_BILL_DTL as "ExtrAbcOut"
df_xfm = df_IDS_INTER_PLN_BILL_DTL.alias("ExtrAbcOut")

# 2) Lookup link: hf_etrnl_mbr_sk => scenario C => read => df_hf_etrnl_mbr_sk as "MBR_REF"
#    left join on ExtrAbcOut.MBR_SK = MBR_REF.MBR_SK
df_xfm = df_xfm.join(
    df_hf_etrnl_mbr_sk.alias("MBR_REF"),
    F.col("ExtrAbcOut.MBR_SK") == F.col("MBR_REF.MBR_SK"),
    how="left"
)

# 3) Lookup link: hf_bcbsa_ipbd_mbr_enr_output => scenario A => df_MBR_ENR_dedup => alias "MBR_ENR_3b_in"
#    left join on ExtrAbcOut.MBR_SK = MBR_ENR_3b_in.MBR_SK
df_xfm = df_xfm.join(
    df_MBR_ENR_dedup.alias("MBR_ENR_3b_in"),
    F.col("ExtrAbcOut.MBR_SK") == F.col("MBR_ENR_3b_in.MBR_SK"),
    how="left"
)

# 4) Lookup link: hf_ipbd_cap_fncl_lod_cd => scenario A => df_FNCL_LOB_dedup => alias "Fncl_lob"
#    left join on ExtrAbcOut.MBR_SK = Fncl_lob.MBR_SK AND (CRG_HIST_IN_FILE.PROD_ID = Fncl_lob.PROD_ID) 
#    The second condition references a non-existent column, but we replicate the logic as best we can.
#    If "ExtrAbcOut" does not have "PROD_ID", we can only join on MBR_SK. We keep the second condition literal for completeness:
df_xfm = df_xfm.join(
    df_FNCL_LOB_dedup.alias("Fncl_lob"),
    (
        (F.col("ExtrAbcOut.MBR_SK") == F.col("Fncl_lob.MBR_SK"))
        & F.lit(True)  # This preserves the second join condition placeholder with no matching column
    ),
    how="left"
)

# 5) Lookup link: hf_ipbd_cd_mppng => scenario A => df_CD_MPPNG_dedup => alias "Cd_Mppng_lkp"
#    left join on ExtrAbcOut.VAL_BASED_PGM_PAYMT_TYP_CD_SK = Cd_Mppng_lkp.CD_MPPNG_SK
df_xfm = df_xfm.join(
    df_CD_MPPNG_dedup.alias("Cd_Mppng_lkp"),
    F.col("ExtrAbcOut.VAL_BASED_PGM_PAYMT_TYP_CD_SK") == F.col("Cd_Mppng_lkp.CD_MPPNG_SK"),
    how="left"
)

# 6) Lookup link: hf_ipbd_cap_prod_sh_nm => scenario A => df_PROD_SH_NM_dedup => alias "ref_prod_sh_nm"
#    left join on ExtrAbcOut.MBR_SK = ref_prod_sh_nm.MBR_SK
df_xfm = df_xfm.join(
    df_PROD_SH_NM_dedup.alias("ref_prod_sh_nm"),
    F.col("ExtrAbcOut.MBR_SK") == F.col("ref_prod_sh_nm.MBR_SK"),
    how="left"
)

# Apply the constraint: "MBR_REF.MBR_UNIQ_KEY <> 0"
df_xfm = df_xfm.filter(F.col("MBR_REF.MBR_UNIQ_KEY") != F.lit(0))

# Now produce the columns for "Lnk_BusinessRules" from the spec
df_lnk_BusinessRules = df_xfm.select(
    F.col("ExtrAbcOut.INTER_PLN_BILL_DTL_SK").alias("INTER_PLN_BILL_DTL_SK"),
    F.col("ExtrAbcOut.INTER_PLN_BILL_DTL_ID").alias("INTER_PLN_BILL_DTL_ID"),
    F.col("ExtrAbcOut.BUS_OWNER_ID").alias("BUS_OWNER_ID"),
    F.col("ExtrAbcOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("ExtrAbcOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("ExtrAbcOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ExtrAbcOut.MBR_SK").alias("MBR_SK"),
    F.col("ExtrAbcOut.ATTRBTN_PGM_CD_SK").alias("ATTRBTN_PGM_CD_SK"),
    F.col("ExtrAbcOut.CFA_RMBRMT_TYP_CD_SK").alias("CFA_RMBRMT_TYP_CD_SK"),
    F.col("ExtrAbcOut.CFA_TRANS_TYP_CD_SK").alias("CFA_TRANS_TYP_CD_SK"),
    F.col("ExtrAbcOut.CTL_PLN_CD_SK").alias("CTL_PLN_CD_SK"),
    F.col("ExtrAbcOut.EPSD_TYP_CD_SK").alias("EPSD_TYP_CD_SK"),
    F.col("ExtrAbcOut.HOME_CORP_PLN_CD_SK").alias("HOME_CORP_PLN_CD_SK"),
    F.col("ExtrAbcOut.HOME_PLN_CD_SK").alias("HOME_PLN_CD_SK"),
    F.col("ExtrAbcOut.HOST_CORP_PLN_CD_SK").alias("HOST_CORP_PLN_CD_SK"),
    F.col("ExtrAbcOut.HOST_PLN_CD_SK").alias("HOST_PLN_CD_SK"),
    F.col("ExtrAbcOut.LOCAL_PLN_CD_SK").alias("LOCAL_PLN_CD_SK"),
    F.col("ExtrAbcOut.VAL_BASED_PGM_CD_SK").alias("VAL_BASED_PGM_CD_SK"),
    F.col("ExtrAbcOut.VAL_BASED_PGM_PAYMT_TYP_CD_SK").alias("VAL_BASED_PGM_PAYMT_TYP_CD_SK"),
    F.col("ExtrAbcOut.BTCH_CRT_DT_SK").alias("BTCH_CRT_DT_SK"),
    F.col("ExtrAbcOut.CBF_SETL_STRT_DT_SK").alias("CBF_SETL_STRT_DT_SK"),
    F.col("ExtrAbcOut.CBF_SETL_BILL_END_DT_SK").alias("CBF_SETL_BILL_END_DT_SK"),
    F.col("ExtrAbcOut.CFA_DISP_DT_SK").alias("CFA_DISP_DT_SK"),
    F.col("ExtrAbcOut.CFA_PRCS_DT_SK").alias("CFA_PRCS_DT_SK"),
    F.col("ExtrAbcOut.HOME_AUTH_DENIAL_DT_SK").alias("HOME_AUTH_DENIAL_DT_SK"),
    F.col("ExtrAbcOut.INCUR_PERD_STRT_DT_SK").alias("INCUR_PERD_STRT_DT_SK"),
    F.col("ExtrAbcOut.INCUR_PERD_END_DT_SK").alias("INCUR_PERD_END_DT_SK"),
    F.col("ExtrAbcOut.MSRMNT_PERD_STRT_DT_SK").alias("MSRMNT_PERD_STRT_DT_SK"),
    F.col("ExtrAbcOut.MSRMNT_PERD_END_DT_SK").alias("MSRMNT_PERD_END_DT_SK"),
    F.col("ExtrAbcOut.MBR_ELIG_STRT_DT_SK").alias("MBR_ELIG_STRT_DT_SK"),
    F.col("ExtrAbcOut.MBR_ELIG_END_DT_SK").alias("MBR_ELIG_END_DT_SK"),
    F.col("ExtrAbcOut.PRCS_CYC_YR_MO_SK").alias("PRCS_CYC_YR_MO_SK"),
    F.col("ExtrAbcOut.SETL_DT_SK").alias("SETL_DT_SK"),
    F.col("ExtrAbcOut.ACES_FEE_ADJ_AMT").alias("ACES_FEE_ADJ_AMT"),
    F.col("ExtrAbcOut.CBF_SETL_RATE_AMT").alias("CBF_SETL_RATE_AMT"),
    F.col("ExtrAbcOut.MBR_LVL_CHRG_AMT").alias("MBR_LVL_CHRG_AMT"),
    F.col("ExtrAbcOut.ATRBD_PROV_ID").alias("ATRBD_PROV_ID"),
    F.col("ExtrAbcOut.ATRBD_PROV_NTNL_PROV_ID").alias("ATRBD_PROV_NTNL_PROV_ID"),
    F.col("ExtrAbcOut.ATTRBTN_PROV_PGM_NM").alias("ATTRBTN_PROV_PGM_NM"),
    F.col("ExtrAbcOut.BCBSA_MMI_ID").alias("BCBSA_MMI_ID"),
    F.col("ExtrAbcOut.CONSIS_MBR_ID").alias("CONSIS_MBR_ID"),
    F.col("ExtrAbcOut.CBF_SCCF_NO").alias("CBF_SCCF_NO"),
    F.col("MBR_ENR_3b_in.GRP_ID").alias("GRP_ID"),
    F.col("ExtrAbcOut.HOME_PLN_MBR_ID").alias("HOME_PLN_MBR_ID"),
    F.col("ExtrAbcOut.ITS_SUB_ID").alias("ITS_SUB_ID"),
    F.col("ExtrAbcOut.ORIG_INTER_PLN_BILL_DTL_SRL_NO").alias("ORIG_INTER_PLN_BILL_DTL_SRL_NO"),
    F.col("ExtrAbcOut.ORIG_ITS_CLM_SCCF_NO").alias("ORIG_ITS_CLM_SCCF_NO"),
    F.col("ExtrAbcOut.PROV_GRP_NM").alias("PROV_GRP_NM"),
    F.col("ExtrAbcOut.VRNC_ACCT_ID").alias("VRNC_ACCT_ID"),
    F.col("ExtrAbcOut.BTCH_STTUS_CD_TX").alias("BTCH_STTUS_CD_TX"),
    F.col("ExtrAbcOut.STTUS_CD_TX").alias("STTUS_CD_TX"),
    F.col("ExtrAbcOut.ERR_CD_TX").alias("ERR_CD_TX"),
    F.col("MBR_REF.MBR_ID").alias("MBR_ID"),
    F.col("MBR_REF.LOAD_RUN_CYCLE").alias("LOAD_RUN_CYCLE"),
    F.col("MBR_REF.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("MBR_REF.SUBGRP_SK").alias("SUBGRP_SK"),
    F.col("MBR_REF.SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("MBR_REF.SUB_SK").alias("SUB_SK"),
    F.col("MBR_REF.SUB_ID").alias("SUB_ID"),
    F.col("MBR_REF.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MBR_REF.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD_SK"),
    F.col("MBR_REF.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("MBR_REF.BRTH_DT_SK").alias("BRTH_DT_SK"),
    F.col("MBR_REF.DCSD_DT_SK").alias("DCSD_DT_SK"),
    F.col("MBR_REF.ORIG_EFF_DT_SK").alias("ORIG_EFF_DT_SK"),
    F.col("MBR_REF.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("MBR_REF.FIRST_NM").alias("FIRST_NM"),
    F.col("MBR_REF.MIDINIT").alias("MIDINIT"),
    F.col("MBR_REF.LAST_NM").alias("LAST_NM"),
    F.col("MBR_REF.SSN").alias("SSN"),
    F.col("MBR_REF.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("MBR_ENR_3b_in.GRP_SK").alias("GRP_SK"),
    F.col("MBR_REF.MBR_RELSHP_CD_SK").alias("MBR_RELSHP_CD_SK"),
    F.col("MBR_REF.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("MBR_REF.CLNT_ID").alias("CLNT_ID"),
    F.col("MBR_REF.CLNT_NM").alias("CLNT_NM"),
    F.col("MBR_ENR_3b_in.CLS_ID").alias("CLS_ID"),
    F.col("MBR_ENR_3b_in.CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("MBR_ENR_3b_in.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("MBR_ENR_3b_in.TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("Fncl_lob.PROD_ID").alias("PROD_ID"),
    F.col("Fncl_lob.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
    F.expr("'Z'").alias("_dummy_src_cd_expr"),  # For the expression "'Z' : Cd_Mppng_lkp.SRC_CD"
    F.col("Cd_Mppng_lkp.SRC_CD").alias("_lkp_SRC_CD"),
    F.col("ref_prod_sh_nm.PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("Cd_Mppng_lkp.TRGT_CD").alias("TRGT_CD"),
    F.col("ExtrAbcOut.PRCS_CYC_DT_SK").alias("PRCS_CYC_DT_SK")
)

# Combine "'Z' : Cd_Mppng_lkp.SRC_CD" logic:
df_lnk_BusinessRules = df_lnk_BusinessRules.withColumn(
    "SRC_CD",
    F.concat(F.col("_dummy_src_cd_expr"), F.lit(" "), F.coalesce(F.col("_lkp_SRC_CD"), F.lit("")))
)

# Drop helper columns
df_lnk_BusinessRules = df_lnk_BusinessRules.drop("_dummy_src_cd_expr", "_lkp_SRC_CD")

# BusinessRules Stage Variables (row-by-row emulation with window):
# RowPassThru = 'Y'
df_lnk_BusinessRules = df_lnk_BusinessRules.withColumn("RowPassThru", F.lit("Y"))

# SvCurrRow = SRC_SYS_CD_SK : MBR_UNIQ_KEY : INCUR_PERD_STRT_DT_SK : CFA_DISP_DT_SK : TRGT_CD : PROD_SH_NM
df_lnk_BusinessRules = df_lnk_BusinessRules.withColumn(
    "SvCurrRow",
    F.concat_ws(
        "",
        F.col("SRC_SYS_CD_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.col("INCUR_PERD_STRT_DT_SK"),
        F.col("CFA_DISP_DT_SK"),
        F.col("TRGT_CD"),
        F.col("PROD_SH_NM")
    )
)

# We'll define a window ordering by all columns in a stable way to replicate row-based mechanism
# For consistent results, choose a deterministic ordering. 
windowSpec = Window.orderBy(
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "INCUR_PERD_STRT_DT_SK",
    "CFA_DISP_DT_SK",
    "TRGT_CD",
    "PROD_SH_NM"
)

df_lnk_BusinessRules = df_lnk_BusinessRules.withColumn(
    "SvPrevRow",
    F.lag("SvCurrRow", 1).over(windowSpec)
)

df_lnk_BusinessRules = df_lnk_BusinessRules.withColumn(
    "SvCounter",
    F.when(
        F.col("SvCurrRow") != F.col("SvPrevRow"),
        F.lit(1)
    ).otherwise(
        F.lag("SvCounter", 1).over(windowSpec) + 1
    )
)

# SvCapPoolCd = If PROD_SH_NM=... => ...
df_lnk_BusinessRules = df_lnk_BusinessRules.withColumn(
    "SvCapPoolCd",
    F.when(F.col("PROD_SH_NM") == F.lit("PC"), "HMH01")
    .when(F.col("PROD_SH_NM") == F.lit("PCB"), "JMH01")
    .when(F.col("PROD_SH_NM") == F.lit("BCARE"), "CMH01")
    .when(F.col("PROD_SH_NM") == F.lit("BLUE-ACCESS"), "NMH01")
    .otherwise("NA")
)

# SvAge:
# If TRGT_CD = 'EPSDBASEDPYMT' then AGE(strip_field(BRTH_DT_SK), strip_field(MSRMNT_PERD_STRT_DT_SK)) 
# else AGE(strip_field(BRTH_DT_SK), strip_field(INCUR_PERD_STRT_DT_SK))
df_lnk_BusinessRules = df_lnk_BusinessRules.withColumn(
    "SvAge",
    F.when(
        F.col("TRGT_CD") == F.lit("EPSDBASEDPYMT"),
        AGE(
            strip_field(F.col("BRTH_DT_SK")),
            strip_field(F.col("MSRMNT_PERD_STRT_DT_SK"))
        )
    ).otherwise(
        AGE(
            strip_field(F.col("BRTH_DT_SK")),
            strip_field(F.col("INCUR_PERD_STRT_DT_SK"))
        )
    )
)

# Now we replicate the left lookup from "hf_ipbd_cap_fund_id" => scenario A => df_CAP_FUND_dedup => alias "Cap_Fund_Id"
# on: Lnk_BusinessRules.SRC_CD = Cap_Fund_Id.CAP_FUND_ID AND ExtrAbcOut.VAL_BASED_PGM_PAYMT_TYP_CD_SK = Cap_Fund_Id.CAP_FUND_SK
# However, note that "ExtrAbcOut.VAL_BASED_PGM_PAYMT_TYP_CD_SK" is in df_lnk_BusinessRules as "VAL_BASED_PGM_PAYMT_TYP_CD_SK"
# We'll do a left join. Key columns are (SRC_CD = CAP_FUND_ID) AND (VAL_BASED_PGM_PAYMT_TYP_CD_SK = CAP_FUND_SK).
df_CAP_FUND_join = df_lnk_BusinessRules.alias("Lnk_BusinessRules").join(
    df_CAP_FUND_dedup.alias("Cap_Fund_Id"),
    (
        F.col("Lnk_BusinessRules.SRC_CD") == F.col("Cap_Fund_Id.CAP_FUND_ID")
    )
    & (
        F.col("Lnk_BusinessRules.VAL_BASED_PGM_PAYMT_TYP_CD_SK") == F.col("Cap_Fund_Id.CAP_FUND_SK")
    ),
    how="left"
)

df_BusinessRules_out = df_CAP_FUND_join

# Build "AllCol" link columns
df_AllCol = df_BusinessRules_out.select(
    F.col("SRC_SYS_CD_SK").alias("SrcSysCdSk"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.lit("NA").alias("CAP_PROV_ID"),
    F.col("INCUR_PERD_STRT_DT_SK").alias("ERN_DT_SK"),
    F.col("PRCS_CYC_DT_SK").alias("PD_DT_SK"),
    F.col("Cap_Fund_Id.CAP_FUND_ID").alias("CAP_FUND_ID"),
    F.col("SvCapPoolCd").alias("CAP_POOL_CD"),
    F.col("SvCounter").alias("SEQ_NO"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.col("CurrentDate").alias("FIRST_RECYC_DT"),  # Expression from stage is CurrentDate
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),
    F.concat_ws(
        ";",
        F.col("SrcSysCd"),
        F.col("MBR_UNIQ_KEY"),
        F.col("INCUR_PERD_STRT_DT_SK"),
        F.col("CFA_DISP_DT_SK"),
        F.col("TRGT_CD"),
        F.col("SvCapPoolCd"),
        F.col("SvCounter")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("Cap_Fund_Id.CAP_FUND_SK").alias("CAP_FUND_SK"),
    F.lit(1).alias("CAP_PROV_SK"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("FNCL_LOB_CD").alias("FNCL_LOB_ID"),
    F.col("GRP_SK").alias("GRP_CK"),
    F.col("GRP_ID").alias("GRGR_ID"),
    F.col("MBR_SK").alias("MBR_CK"),
    F.lit("NA").alias("NTWK_ID"),
    F.lit("NA").alias("PD_PROV_ID"),
    F.lit("NA").alias("PCP_PROV_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.lit("NA").alias("CAP_ADJ_RSN_CD"),
    F.lit("03").alias("CAP_ADJ_STTUS_CD"),
    F.lit("NA").alias("CAP_ADJ_TYP_CD"),
    F.lit("NA").alias("CAP_CAT_CD"),
    F.lit("NA").alias("CAP_COPAY_TYP_CD"),
    F.lit("NA").alias("CAP_LOB_CD"),
    F.lit("C").alias("CAP_PERD_CD"),
    F.lit("CPCMHPCP01").alias("CAP_SCHD_CD"),
    F.lit("P").alias("CAP_TYP_CD"),
    F.lit(0.00).alias("ADJ_AMT"),
    F.col("MBR_LVL_CHRG_AMT").alias("CAP_AMT"),
    F.lit(0.00).alias("COPAY_AMT"),
    F.lit(0.00).alias("FUND_RATE_AMT"),
    F.col("SvAge").alias("MBR_AGE"),
    F.lit(1.00).alias("MBR_MO_CT"),
    F.col("SUB_UNIQ_KEY").alias("SBSB_CK"),
    F.lit("MED").alias("CRFD_ACCT_CAT")
)

# Build "Transform" link columns
df_Transform = df_BusinessRules_out.select(
    F.col("SRC_SYS_CD_SK").alias("SrcSysCdSk"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.lit("NA").alias("CAP_PROV_ID"),
    F.col("INCUR_PERD_STRT_DT_SK").alias("ERN_DT_SK"),
    F.col("PRCS_CYC_DT_SK").alias("PD_DT_SK"),
    F.col("Cap_Fund_Id.CAP_FUND_ID").alias("CAP_FUND_ID"),
    F.col("SvCapPoolCd").alias("CAP_POOL_CD"),
    F.col("SvCounter").alias("SEQ_NO")
)

# Next, call the shared container "CapPK" with 2 inputs, 1 output
params_CapPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_CapPK_out = CapPK(df_Transform, df_AllCol, params_CapPK)

# Finally, the stage "IdsCapPkey" writes a sequential file.
# We must preserve column order, and rpad any char/varchar columns with lengths as per the final stage's schema.
df_ids_cap_pkey = df_CapPK_out.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CAP_SK",
    "MBR_UNIQ_KEY",
    "CAP_PROV_ID",
    "ERN_DT_SK",
    "PD_DT_SK",
    "CAP_FUND_ID",
    "CAP_POOL_CD",
    "SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CAP_FUND_SK",
    "CAP_PROV_SK",
    "CLS_ID",
    "CLS_PLN_ID",
    "FNCL_LOB_ID",
    "GRP_CK",
    "GRGR_ID",
    "MBR_CK",
    "NTWK_ID",
    "PD_PROV_ID",
    "PCP_PROV_ID",
    "PROD_ID",
    "SUBGRP_ID",
    "SUB_ID",
    "CAP_ADJ_RSN_CD",
    "CAP_ADJ_STTUS_CD",
    "CAP_ADJ_TYP_CD",
    "CAP_CAT_CD",
    "CAP_COPAY_TYP_CD",
    "CAP_LOB_CD",
    "CAP_PERD_CD",
    "CAP_SCHD_CD",
    "CAP_TYP_CD",
    "ADJ_AMT",
    "CAP_AMT",
    "COPAY_AMT",
    "FUND_RATE_AMT",
    "MBR_AGE",
    "MBR_MO_CT",
    "SBSB_CK",
    "CRFD_ACCT_CAT"
)

# Apply rpad for columns that are char(...) or varchar(...) in final stage
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("INSRT_UPDT_CD", F.rpad("INSRT_UPDT_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("DISCARD_IN", F.rpad("DISCARD_IN", 1, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("PASS_THRU_IN", F.rpad("PASS_THRU_IN", 1, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("ERN_DT_SK", F.rpad("ERN_DT_SK", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("PD_DT_SK", F.rpad("PD_DT_SK", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CLS_ID", F.rpad("CLS_ID", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CLS_PLN_ID", F.rpad("CLS_PLN_ID", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("FNCL_LOB_ID", F.rpad("FNCL_LOB_ID", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("GRP_CK", F.rpad("GRP_CK", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("GRGR_ID", F.rpad("GRGR_ID", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("MBR_CK", F.rpad("MBR_CK", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("NTWK_ID", F.rpad("NTWK_ID", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("PD_PROV_ID", F.rpad("PD_PROV_ID", 12, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("PCP_PROV_ID", F.rpad("PCP_PROV_ID", 12, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("PROD_ID", F.rpad("PROD_ID", 12, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("SUBGRP_ID", F.rpad("SUBGRP_ID", 12, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("SUB_ID", F.rpad("SUB_ID", 20, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_ADJ_RSN_CD", F.rpad("CAP_ADJ_RSN_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_ADJ_STTUS_CD", F.rpad("CAP_ADJ_STTUS_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_ADJ_TYP_CD", F.rpad("CAP_ADJ_TYP_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_CAT_CD", F.rpad("CAP_CAT_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_COPAY_TYP_CD", F.rpad("CAP_COPAY_TYP_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_LOB_CD", F.rpad("CAP_LOB_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_PERD_CD", F.rpad("CAP_PERD_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_SCHD_CD", F.rpad("CAP_SCHD_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CAP_TYP_CD", F.rpad("CAP_TYP_CD", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("SBSB_CK", F.rpad("SBSB_CK", 10, " "))
df_ids_cap_pkey = df_ids_cap_pkey.withColumn("CRFD_ACCT_CAT", F.rpad("CRFD_ACCT_CAT", 4, " "))

# Write output to sequential file (IdsCapPkey stage)
write_files(
    df_ids_cap_pkey,
    f"{adls_path}/key/{TmpOutFile}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)