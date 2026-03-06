# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2007, 2012 Blue Cross and Blue Shield of Kansas City
# MAGIC 
# MAGIC 8;hf_drg_clm_medstat;hf_ssn_medstat;hf_medstat_cob;hf_prov_clm_medstat;hf_er_pos_medstat;hf_clm_ln_diag_medstat;hf_medstat_facilty;hf_cd_mppng_medstat
# MAGIC 
# MAGIC 
# MAGIC Called by:
# MAGIC                     IdsMedStatCntl
# MAGIC 
# MAGIC Processing:
# MAGIC                     Pulls data from Several IDS Tables for export and FTP
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    Restart, no other steps necessary
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC Modifications:                        
# MAGIC                                                     Project/                                                                                                                                      Code                    Date
# MAGIC Developer                      Date              Altiris #           Change Description                                                                                     Reviewer              Reviewed
# MAGIC -----------------------------------   -------------------   -------------------   -----------------------------------------------------------------------------------------------------------------   ---------------------------  -------------------
# MAGIC Scott Vanderlaan           05/22/2007  3279               Initial program                                                                                              Steph Goddard     5/22/07
# MAGIC Hugh Sisson                  2012-12-26    TTR1501       Modified program to use column RX_ALW_QTY from                                 Bhoomi Dasari      1/7/2013
# MAGIC                                                                                   the DRUG_CLM table.                                              
# MAGIC Ravichandra                  07/16/2013   4850/4905    Diag & Proc code field lengths changed from 5 to7. And                             SAndrew               2013-07-29
# MAGIC Yellamraju                                                                  DIAG_CD_TYP_CD field added included in extract       
# MAGIC Harpreet Singh               07/18/2013   4850/4905   Changed Field Lengths and Added 6 Filler Fields in                                      SAndrew              2013-08-18
# MAGIC Nanda                                                                       MAIN TRANS and TextFileExport stages.    
# MAGIC Ravichandra                  11/05/2013   4850/4905   Changed trailer record layout.                                                                        Kalyan Neelam    2013-11-21
# MAGIC Yellamraju
# MAGIC Karthik Chintalapani       05/09/2014    5082            Flipped the fileds CLM_ID and Filler1 and                                                     Kalyan Neelam    2014-05-22
# MAGIC                                                                                   changed the datatype and length
# MAGIC Madhavan B                  2017-07-10    5788 BHI       Updated Field PROV_ID length from                                                           Kalyan Neelam     2017-07-12
# MAGIC                                                                                   Char(12) to Varchar(20) in CLM_PROV table 
# MAGIC Bhanu Sekhar   2019-05-07   INC0517010               Update Prov_lnk query added filter DEA.CMN_PRCT_SK NOT IN (0,1)     Hugh Sisson         2019-05-07

# MAGIC Group and Subgroup in all queries are to pull all Ford but exclude Subgroup 003
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BeginDate = get_widget_value('BeginDate','2013-10-01')
EndDate = get_widget_value('EndDate','2013-10-31')
RunID = get_widget_value('RunID','100')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_IDS_DrugClm_Link = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT
    DC.CLM_SK,
    DC.CLM_ID,
    DC.NDC_SK,
    DC.MAIL_ORDER_IN,
    DC.DISPNS_FEE_AMT,
    DC.INGR_CST_ALW_AMT,
    DC.SLS_TAX_AMT,
    DC.RX_ALW_DAYS_SUPL_QTY,
    DC.RX_ORIG_DAYS_SUPL_QTY,
    DC.RFL_NO,
    DC.DRUG_CLM_DISPNS_AS_WRTN_CD_SK,
    DC.DRUG_CLM_TIER_CD_SK,
    DC.NON_FRMLRY_DRUG_IN,
    CDMA1.TRGT_CD,
    CDMA2.TRGT_CD,
    NDC.NDC,
    DEA.DEA_NO,
    PC.PROV_ID,
    DC.RX_ALW_QTY
FROM
    {IDSOwner}.GRP G,
    {IDSOwner}.SUBGRP SG,
    {IDSOwner}.DRUG_CLM DC,
    {IDSOwner}.CLM C,
    {IDSOwner}.PROV_DEA DEA,
    {IDSOwner}.CLM_PROV PC,
    {IDSOwner}.NDC NDC,
    {IDSOwner}.CD_MPPNG CDMA1,
    {IDSOwner}.CD_MPPNG CDMA2
WHERE
    C.PD_DT_SK >= '{BeginDate}'
    AND C.PD_DT_SK <= '{EndDate}'
    AND C.CLM_SK = DC.CLM_SK
    AND PC.CLM_SK = DC.CLM_SK
    AND DC.NDC_SK = NDC.NDC_SK
    AND DC.PRSCRB_PROV_DEA_SK = DEA.PROV_DEA_SK
    AND C.GRP_SK = G.GRP_SK
    AND G.GRP_ID = '10037000'
    AND G.GRP_SK = SG.GRP_SK
    AND SG.SUBGRP_ID <> '0003'
    AND DC.DRUG_CLM_DISPNS_AS_WRTN_CD_SK = CDMA1.CD_MPPNG_SK
    AND DC.DRUG_CLM_TIER_CD_SK = CDMA2.CD_MPPNG_SK
"""
    )
    .load()
)

df_IDS_Sub_SSNLink = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT
    S.SUB_SK,
    M.MBR_SFX_NO,
    M.SSN
FROM
    {IDSOwner}.MBR M,
    {IDSOwner}.SUB S,
    {IDSOwner}.GRP G,
    {IDSOwner}.SUBGRP SG
WHERE
    M.SUB_SK = S.SUB_SK
    AND S.GRP_SK = G.GRP_SK
    AND G.GRP_SK = SG.GRP_SK
    AND G.GRP_ID = '10037000'
    AND SG.SUBGRP_ID <> '0003'
    AND M.MBR_SFX_NO = '00'
"""
    )
    .load()
)

df_IDS_MainFileLink = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT
    M.MBR_GNDR_CD_SK,
    M.MBR_RELSHP_CD_SK,
    M.BRTH_DT_SK,
    M.MBR_SK,
    C.CLM_SVC_PROV_TYP_CD_SK,
    C.CLM_TYP_CD_SK,
    C.PCP_SUBMT_IN,
    C.PD_DT_SK,
    C.RFRNG_PROV_TX,
    C.SUB_ID,
    CL.CLM_ID,
    CL.CLM_LN_SEQ_NO,
    C.CLM_SK,
    CL.CLM_LN_POS_CD_SK,
    CL.CLM_LN_RVNU_CD_SK,
    CL.CAP_LN_IN,
    CL.SVC_END_DT_SK,
    CL.SVC_STRT_DT_SK,
    CL.ALW_AMT,
    CL.COINS_AMT,
    CL.CNSD_CHRG_AMT,
    CL.COPAY_AMT,
    CL.DEDCT_AMT,
    CL.PAYBL_AMT,
    CL.PAYBL_TO_PROV_AMT,
    CL.PAYBL_TO_SUB_AMT,
    CL.UNIT_CT,
    M.INDV_BE_KEY,
    M.MBR_UNIQ_KEY,
    R.RVNU_CD,
    PCM.PROC_CD_MOD_TX,
    S.SUB_UNIQ_KEY,
    PROC.PROC_CD,
    M.SSN,
    S.SUB_SK
FROM
    {IDSOwner}.SUB S,
    {IDSOwner}.GRP G,
    {IDSOwner}.SUBGRP SG,
    {IDSOwner}.CLM C,
    {IDSOwner}.MBR M,
    {IDSOwner}.CLM_LN CL
    LEFT OUTER JOIN {IDSOwner}.RVNU_CD R
        ON CL.CLM_LN_RVNU_CD_SK = R.RVNU_CD_SK
    LEFT OUTER JOIN {IDSOwner}.PROC_CD PROC
        ON CL.PROC_CD_SK = PROC.PROC_CD_SK
    LEFT OUTER JOIN {IDSOwner}.CLM_LN_PROC_CD_MOD PCM
        ON CL.CLM_LN_SK = PCM.CLM_LN_SK
WHERE
    M.SUB_SK = S.SUB_SK
    AND C.MBR_SK = M.MBR_SK
    AND CL.CLM_SK = C.CLM_SK
    AND C.GRP_SK = G.GRP_SK
    AND G.GRP_SK = SG.GRP_SK
    AND G.GRP_ID = '10037000'
    AND SG.SUBGRP_ID <> '0003'
    AND C.PD_DT_SK >= '{BeginDate}'
    AND C.PD_DT_SK <= '{EndDate}'
"""
    )
    .load()
)

df_IDS_Prov_link = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT
    PC.CLM_SK,
    P.PROV_NM,
    PADD.POSTAL_CD,
    PN.DIR_IN,
    DEA.DEA_NO,
    MAP2.TRGT_CD,
    MAP3.TRGT_CD,
    P.PROV_ID
FROM
    {IDSOwner}.CLM C,
    {IDSOwner}.GRP G,
    {IDSOwner}.SUBGRP SG,
    {IDSOwner}.CLM_PROV PC,
    {IDSOwner}.PROV P,
    {IDSOwner}.PROV_NTWK PN,
    {IDSOwner}.PROV_LOC PLOC,
    {IDSOwner}.PROV_ADDR PADD,
    {IDSOwner}.CMN_PRCT COMMON,
    {IDSOwner}.PROV_DEA DEA,
    {IDSOwner}.CD_MPPNG MAP2,
    {IDSOwner}.CD_MPPNG MAP3,
    {IDSOwner}.CD_MPPNG MAP4
WHERE
    C.PD_DT_SK >= '{BeginDate}'
    AND C.PD_DT_SK <= '{EndDate}'
    AND C.CLM_SK = PC.CLM_SK
    AND C.GRP_SK = G.GRP_SK
    AND PADD.PROV_ADDR_TYP_CD_SK = MAP4.CD_MPPNG_SK
    AND MAP4.TRGT_CD = 'P'
    AND G.GRP_ID = '10037000'
    AND G.GRP_SK = SG.GRP_SK
    AND SG.SUBGRP_ID <> '0003'
    AND PC.PROV_SK = P.PROV_SK
    AND P.PROV_SK = PN.PROV_SK
    AND P.PROV_SK = PLOC.PROV_SK
    AND PLOC.PROV_ADDR_SK = PADD.PROV_ADDR_SK
    AND P.CMN_PRCT_SK = COMMON.CMN_PRCT_SK
    AND COMMON.CMN_PRCT_SK = DEA.CMN_PRCT_SK
    AND MAP2.CD_MPPNG_SK = P.PROV_TYP_CD_SK
    AND MAP3.CD_MPPNG_SK = P.PROV_FCLTY_TYP_CD_SK
    AND DEA.CMN_PRCT_SK NOT IN (0,1)
"""
    )
    .load()
)

df_IDS_ER_POS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT
    C.CLM_SK,
    R.RVNU_CD
FROM
    {IDSOwner}.SUB S,
    {IDSOwner}.GRP G,
    {IDSOwner}.SUBGRP SG,
    {IDSOwner}.CLM C,
    {IDSOwner}.MBR M,
    {IDSOwner}.CLM_LN CL
    LEFT OUTER JOIN {IDSOwner}.RVNU_CD R
        ON CL.CLM_LN_RVNU_CD_SK = R.RVNU_CD_SK
WHERE
    M.SUB_SK = S.SUB_SK
    AND C.MBR_SK = M.MBR_SK
    AND CL.CLM_SK = C.CLM_SK
    AND C.GRP_SK = G.GRP_SK
    AND G.GRP_SK = SG.GRP_SK
    AND (R.RVNU_CD = '0450' OR R.RVNU_CD = '0451' OR R.RVNU_CD = '0452' OR R.RVNU_CD = '0459' OR R.RVNU_CD = '0453')
    AND G.GRP_ID = '10037000'
    AND SG.SUBGRP_ID <> '0003'
    AND C.PD_DT_SK >= '{BeginDate}'
    AND C.PD_DT_SK <= '{EndDate}'
"""
    )
    .load()
)

df_IDS_DiagLink = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT
    CL.CLM_ID,
    CL.CLM_LN_SEQ_NO,
    C.CLM_SK,
    DXCD.DIAG_CD,
    DXCD.DIAG_CD_TYP_CD
FROM
    {IDSOwner}.GRP G,
    {IDSOwner}.SUBGRP SG,
    {IDSOwner}.CLM C,
    {IDSOwner}.CLM_LN CL,
    {IDSOwner}.CLM_LN_DIAG CLDX,
    {IDSOwner}.DIAG_CD DXCD
WHERE
    C.PD_DT_SK >= '{BeginDate}'
    AND C.PD_DT_SK <= '{EndDate}'
    AND C.GRP_SK = G.GRP_SK
    AND G.GRP_ID = '10037000'
    AND G.GRP_SK = SG.GRP_SK
    AND SG.SUBGRP_ID <> '0003'
    AND C.CLM_SK = CL.CLM_SK
    AND CL.CLM_LN_SK = CLDX.CLM_LN_SK
    AND CLDX.DIAG_CD_SK = DXCD.DIAG_CD_SK
"""
    )
    .load()
)

df_IDS_FacLink = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT
    FC.CLM_SK,
    FC.FCLTY_CLM_BILL_TYP_CD_SK,
    FC.LOS_DAYS,
    FC.FCLTY_CLM_DSCHG_STTUS_CD_SK,
    FC.ADMS_DT_SK,
    FC.FCLTY_CLM_BILL_CLS_CD_SK,
    FC.FCLTY_CLM_BILL_FREQ_CD_SK,
    CDMA1.TRGT_CD,
    CDMA2.TRGT_CD,
    CDMA3.TRGT_CD,
    CDMA4.SRC_CD,
    FC.ADM_PHYS_PROV_ID
FROM
    {IDSOwner}.GRP G,
    {IDSOwner}.SUBGRP SG,
    {IDSOwner}.CLM C,
    {IDSOwner}.FCLTY_CLM FC,
    {IDSOwner}.CD_MPPNG CDMA1,
    {IDSOwner}.CD_MPPNG CDMA2,
    {IDSOwner}.CD_MPPNG CDMA3,
    {IDSOwner}.CD_MPPNG CDMA4
WHERE
    C.GRP_SK = G.GRP_SK
    AND G.GRP_SK = SG.GRP_SK
    AND G.GRP_ID = '10037000'
    AND SG.SUBGRP_ID <> '0003'
    AND C.PD_DT_SK >= '{BeginDate}'
    AND C.PD_DT_SK <= '{EndDate}'
    AND C.CLM_SK = FC.CLM_SK
    AND FC.FCLTY_CLM_BILL_TYP_CD_SK = CDMA1.CD_MPPNG_SK
    AND FC.FCLTY_CLM_BILL_FREQ_CD_SK = CDMA2.CD_MPPNG_SK
    AND FC.FCLTY_CLM_BILL_CLS_CD_SK = CDMA3.CD_MPPNG_SK
    AND FC.FCLTY_CLM_DSCHG_STTUS_CD_SK = CDMA4.CD_MPPNG_SK
"""
    )
    .load()
)

df_IDS_COBLinkAll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT
    CLM_COB.CLM_SK,
    CLM_COB.PD_AMT,
    C.ALW_AMT
FROM
    {IDSOwner}.CLM C,
    {IDSOwner}.CLM_COB CLM_COB,
    {IDSOwner}.GRP G,
    {IDSOwner}.SUBGRP SG
WHERE
    C.CLM_SK = CLM_COB.CLM_SK
    AND G.GRP_SK = SG.GRP_SK
    AND G.GRP_ID = '10037000'
    AND SG.SUBGRP_ID <> '0003'
    AND C.PD_DT_SK >= '{BeginDate}'
    AND C.PD_DT_SK <= '{EndDate}'
"""
    )
    .load()
)

df_CD_Mapping = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT
    CD_MPPNG_SK as CD_MPPNG_SK,
    TRGT_CD as TRGT_CD,
    TRGT_CD_NM as TRGT_CD_NM,
    SRC_CD as SRC_CD
FROM
    {IDSOwner}.CD_MPPNG
"""
    )
    .load()
)

df_hf_CD_MAPPING = df_CD_Mapping.dropDuplicates(["CD_MPPNG_SK"])

df_hf_CD_MAPPING_GenCd = df_hf_CD_MAPPING.select(
    "CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM", "SRC_CD"
)
df_hf_CD_MAPPING_ClmPOS = df_hf_CD_MAPPING.select(
    "CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM", "SRC_CD"
)
df_hf_CD_MAPPING_ClmType = df_hf_CD_MAPPING.select(
    "CD_MPPNG_SK", "TRGT_CD", "TRGT_CD_NM", "SRC_CD"
)

df_Transformer_114_COBLink = (
    df_IDS_COBLinkAll.filter(F.col("ALW_AMT") > F.col("PD_AMT"))
    .select(
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("PD_AMT").alias("PD_AMT"),
    )
)

df_hf_data_drug_clm = (
    df_IDS_DrugClm_Link.dropDuplicates(["CLM_SK"])
    .select(
        F.col("CLM_SK").cast("int").alias("CLM_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("NDC_SK").cast("int").alias("NDC_SK"),
        F.col("MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
        F.col("DISPNS_FEE_AMT").cast("decimal(18,2)").alias("DISPNS_FEE_AMT"),
        F.col("INGR_CST_ALW_AMT").cast("decimal(18,2)").alias("INGR_CST_ALW_AMT"),
        F.col("SLS_TAX_AMT").cast("decimal(18,2)").alias("SLS_TAX_AMT"),
        F.col("RX_ALW_DAYS_SUPL_QTY").cast("int").alias("RX_ALW_DAYS_SUPL_QTY"),
        F.col("RX_ORIG_DAYS_SUPL_QTY").cast("int").alias("RX_ORIG_DAYS_SUPL_QTY"),
        F.col("RFL_NO").alias("RFL_NO"),
        F.col("DRUG_CLM_DISPNS_AS_WRTN_CD_SK").cast("int").alias("DRUG_CLM_DISPNS_AS_WRTN_CD_SK"),
        F.col("DRUG_CLM_TIER_CD_SK").cast("int").alias("DRUG_CLM_TIER_CD_SK"),
        F.col("NON_FRMLRY_DRUG_IN").alias("NON_FRMLRY_DRUG_IN"),
        F.col("TRGT_CD").alias("TRGT_CD_DAW"),
        F.col("TRGT_CD").alias("TRGT_CD_TIER"),
        F.col("NDC").alias("NDC"),
        F.col("DEA_NO").alias("DEA_NO"),
        F.col("PROV_ID").alias("PROV_ID"),
        F.col("RX_ALW_QTY").cast("decimal(18,2)").alias("RX_ALW_QTY"),
    )
)

df_hf_data_IDS_sub_ssn = (
    df_IDS_Sub_SSNLink.dropDuplicates(["SUB_SK"])
    .select(
        F.col("SUB_SK").cast("int").alias("SUB_SK"),
        F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
        F.col("SSN").alias("SSN")
    )
)

df_hf_data_Cob_Clm = (
    df_Transformer_114_COBLink.dropDuplicates(["CLM_SK"])
    .select(
        F.col("CLM_SK").cast("int").alias("CLM_SK"),
        F.col("PD_AMT").cast("decimal(18,2)").alias("PD_AMT")
    )
)

df_hf_data_Prov = (
    df_IDS_Prov_link.dropDuplicates(["CLM_SK"])
    .select(
        F.col("CLM_SK").cast("int").alias("CLM_SK"),
        F.col("PROV_NM").alias("PROV_NM"),
        F.col("POSTAL_CD").alias("POSTAL_CD"),
        F.col("DIR_IN").alias("DIR_IN"),
        F.col("DEA_NO").alias("DEA_NO"),
        F.col("TRGT_CD").alias("TRGT_CD_TYPE"),
        F.col("TRGT_CD").alias("TRGT_CD_FAC_TYPE"),
        F.col("PROV_ID").alias("PROV_ID")
    )
)

df_hf_data_ER_POS_IN = (
    df_IDS_ER_POS.dropDuplicates(["CLM_SK"])
    .select(
        F.col("CLM_SK").cast("int").alias("CLM_SK"),
        F.col("RVNU_CD").alias("RVNU_CD")
    )
)

df_hf_data_clm_ln_diag = (
    df_IDS_DiagLink.dropDuplicates(["CLM_ID","CLM_LN_SEQ_NO"])
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO").cast("int").alias("CLM_LN_SEQ_NO"),
        F.col("CLM_SK").cast("int").alias("CLM_SK"),
        F.col("DIAG_CD").alias("DIAG_CD"),
        F.col("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
    )
)

df_hf_data_facility_cllm = (
    df_IDS_FacLink.dropDuplicates(["CLM_SK"])
    .select(
        F.col("CLM_SK").cast("int").alias("CLM_SK"),
        F.col("FCLTY_CLM_BILL_TYP_CD_SK").alias("FCLTY_CLM_BILL_TYP_CD"),
        F.col("LOS_DAYS").cast("int").alias("LOS_DAYS"),
        F.col("FCLTY_CLM_DSCHG_STTUS_CD_SK").alias("FCLTY_CLM_DSCHG_STTUS_CD_SK"),
        F.col("ADMS_DT_SK").alias("ADMS_DT_SK"),
        F.col("FCLTY_CLM_BILL_CLS_CD_SK").cast("int").alias("FCLTY_CLM_BILL_CLS_CD_SK"),
        F.col("FCLTY_CLM_BILL_FREQ_CD_SK").cast("int").alias("FCLTY_CLM_BILL_FREQ_CD_SK"),
        F.col("TRGT_CD").alias("TRGT_CD_BILL_TYP"),
        F.col("TRGT_CD").alias("TRGT_CD_BILL_FREQ"),
        F.col("TRGT_CD").alias("TRGT_CD_BILL_CLS"),
        F.col("SRC_CD").alias("TRGT_CD_DSCHG_STATUS"),
        F.col("ADM_PHYS_PROV_ID").alias("ADM_PHYS_PROV_ID")
    )
)

df_MAIN_TRANS = (
    df_IDS_MainFileLink.alias("MainFileLink")
    .join(df_hf_CD_MAPPING_GenCd.alias("GenCd"), F.col("MainFileLink.MBR_GNDR_CD_SK") == F.col("GenCd.CD_MPPNG_SK"), "left")
    .join(df_hf_CD_MAPPING_ClmPOS.alias("ClmPOS"), F.col("MainFileLink.CLM_LN_POS_CD_SK") == F.col("ClmPOS.CD_MPPNG_SK"), "left")
    .join(df_hf_CD_MAPPING_ClmType.alias("ClmType"), F.col("MainFileLink.CLM_TYP_CD_SK") == F.col("ClmType.CD_MPPNG_SK"), "left")
    .join(df_hf_data_IDS_sub_ssn.alias("IDS_sub_ssn"), F.col("MainFileLink.SUB_SK") == F.col("IDS_sub_ssn.SUB_SK"), "left")
    .join(df_hf_data_drug_clm.alias("drug_clm"), F.col("MainFileLink.CLM_SK") == F.col("drug_clm.CLM_SK"), "left")
    .join(
        df_hf_data_clm_ln_diag.alias("clm_ln_diag"),
        [
            F.col("MainFileLink.CLM_ID") == F.col("clm_ln_diag.CLM_ID"),
            F.col("MainFileLink.CLM_LN_SEQ_NO") == F.col("clm_ln_diag.CLM_LN_SEQ_NO"),
        ],
        "left",
    )
    .join(df_hf_data_facility_cllm.alias("facility_cllm"), F.col("MainFileLink.CLM_SK") == F.col("facility_cllm.CLM_SK"), "left")
    .join(df_hf_data_Prov.alias("Prov"), F.col("MainFileLink.CLM_SK") == F.col("Prov.CLM_SK"), "left")
    .join(df_hf_data_ER_POS_IN.alias("ER_POS_IN"), F.col("MainFileLink.CLM_SK") == F.col("ER_POS_IN.CLM_SK"), "left")
    .join(df_hf_data_Cob_Clm.alias("Cob_Clm"), F.col("MainFileLink.CLM_SK") == F.col("Cob_Clm.CLM_SK"), "left")
)

df_MAIN_TRANS_TextFile = (
    df_MAIN_TRANS.filter(F.col("MainFileLink.CLM_LN_SEQ_NO") < 100)
    .select(
        (
            F.col("MainFileLink.ALW_AMT")
        ).alias("ALW_AMT"),
        (
            F.concat(
                F.expr("RIGHT(facility_cllm.TRGT_CD_BILL_TYP, 1)"),
                F.lit(""),
                F.expr("RIGHT(facility_cllm.TRGT_CD_BILL_CLS, 1)"),
                F.lit(""),
                F.expr("RIGHT(facility_cllm.TRGT_CD_BILL_FREQ, 1)")
            )
        ).alias("BILL_TYPE"),
        F.when(F.col("MainFileLink.CAP_LN_IN") == "X", F.lit(" ")).otherwise(F.col("MainFileLink.CAP_LN_IN")).alias("CAP_LN_IN"),
        F.col("MainFileLink.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
        F.lit(" ").alias("Filler1"),
        F.expr("LEFT(ClmType.TRGT_CD, 1)").alias("CLM_TYP_CD"),
        F.col("MainFileLink.COINS_AMT").alias("COINS_AMT"),
        F.col("MainFileLink.COPAY_AMT").alias("COPAY_AMT"),
        F.col("MainFileLink.BRTH_DT_SK").alias("BRTH_DT_SK"),
        F.col("MainFileLink.SVC_STRT_DT_SK").alias("SVC_STRT_DT_SK"),
        F.col("MainFileLink.SVC_END_DT_SK").alias("SVC_END_DT_SK"),
        F.when(F.col("facility_cllm.CLM_SK").isNull(), F.lit(" ")).otherwise(F.col("facility_cllm.ADMS_DT_SK")).alias("ADMS_DT_SK"),
        F.col("MainFileLink.PD_DT_SK").alias("PD_DT_SK"),
        F.when(F.col("facility_cllm.CLM_SK").isNull(), F.lit(" ")).otherwise(F.col("facility_cllm.LOS_DAYS")).alias("LOS_DAYS"),
        F.col("MainFileLink.DEDCT_AMT").alias("DEDCT_AMT"),
        F.lit(" ").alias("Diag_Cd_UB"),
        F.when(
            F.col("clm_ln_diag.CLM_ID").isNull() | (F.length(F.col("clm_ln_diag.DIAG_CD")) > 7),
            F.lit("UNK")
        ).otherwise(F.col("clm_ln_diag.DIAG_CD")).alias("Diag_Cd"),
        F.col("facility_cllm.TRGT_CD_DSCHG_STATUS").alias("DISCH_STATUS"),
        (F.col("MainFileLink.CNSD_CHRG_AMT") - F.col("MainFileLink.ALW_AMT")).alias("DISCOUNT_AMT"),
        F.col("IDS_sub_ssn.SSN").alias("FAMILY_ID"),
        F.col("GenCd.TRGT_CD").alias("Gender_Cd"),
        F.when(F.length(F.trim(F.col("MainFileLink.CLM_LN_SEQ_NO"))) < 3,
               F.substring(F.trim(F.col("MainFileLink.CLM_LN_SEQ_NO")), 1, 2)
        ).otherwise(F.lit("XXX")).alias("CLM_LN_SEQ_NO"),
        F.col("MainFileLink.PAYBL_AMT").alias("PAYBL_AMT"),
        F.when(F.col("Prov.DIR_IN") == "Y", F.lit("Y")).otherwise(F.lit("N")).alias("NETWORK_PROV_IN"),
        F.when(F.trim(F.col("Prov.DEA_NO")) != "NA", F.col("Prov.DEA_NO"))
         .otherwise(
            F.when(F.trim(F.col("drug_clm.DEA_NO")) != "NA", F.col("drug_clm.DEA_NO"))
             .otherwise(F.lit(" "))
         ).alias("RFRNG_PROV_TX"),
        F.when(F.col("MainFileLink.PCP_SUBMT_IN") == "X", F.lit(" ")).otherwise(F.col("MainFileLink.PCP_SUBMT_IN")).alias("PCP_SUBMT_IN"),
        F.when(F.col("ClmPOS.TRGT_CD") != "NA", F.col("ClmPOS.TRGT_CD"))
         .otherwise(
            F.when(F.col("ER_POS_IN.CLM_SK") == F.col("MainFileLink.CLM_SK"), F.lit("23"))
             .otherwise(
                F.when(
                    F.concat(F.expr("RIGHT(facility_cllm.TRGT_CD_BILL_TYP,1)"), F.expr("RIGHT(facility_cllm.TRGT_CD_BILL_CLS,1)")).isin("11","12"),
                    F.lit("21")
                ).otherwise(
                    F.when(
                        F.concat(F.expr("RIGHT(facility_cllm.TRGT_CD_BILL_TYP,1)"), F.expr("RIGHT(facility_cllm.TRGT_CD_BILL_CLS,1)")).isin("13","14"),
                        F.lit("22")
                    ).otherwise(
                        F.when(
                            F.expr("RIGHT(facility_cllm.TRGT_CD_BILL_TYP,1)") == F.lit("2"),
                            F.lit("31")
                        ).otherwise(F.lit("99"))
                    )
                )
             )
         ).alias("CLM_LN_POS_CD_SK"),
        F.lit(" ").alias("Proc_Cd_Surg1"),
        F.col("MainFileLink.PROC_CD").alias("Proc_Cd"),
        F.when(F.col("Prov.PROV_ID") != "NA", F.col("Prov.PROV_ID"))
         .otherwise(
            F.when(F.col("drug_clm.PROV_ID") != "NA", F.col("drug_clm.PROV_ID"))
             .otherwise(F.lit(" "))
         ).alias("PROV_ID"),
        F.when(F.col("Prov.TRGT_CD_TYPE") == "NA", F.expr("RIGHT(Prov.TRGT_CD_FAC_TYPE, 3)"))
         .otherwise(F.expr("RIGHT(Prov.TRGT_CD_TYPE, 3)")).alias("PROV_TYPE_CD"),
        F.expr("LEFT(Prov.POSTAL_CD, 5)").alias("POSTAL_CD"),
        F.col("MainFileLink.RVNU_CD").alias("RVNU_CD"),
        F.when(
            F.col("MainFileLink.CLM_LN_SEQ_NO") != 1,
            F.lit(0)
        ).otherwise(
            F.when(F.col("Cob_Clm.PD_AMT").isNull(), F.lit(0)).otherwise(F.col("Cob_Clm.PD_AMT"))
        ).alias("Third_Prty_Amt"),
        F.col("MainFileLink.UNIT_CT").alias("UNIT_CT"),
        F.col("MainFileLink.PROC_CD_MOD_TX").alias("PROC_CD_MOD"),
        F.col("Prov.PROV_NM").substr(F.lit(1), F.lit(30)).alias("PROV_NM"),
        F.col("MainFileLink.SSN").alias("Patient_SSN"),
        F.col("drug_clm.RX_ALW_DAYS_SUPL_QTY").alias("RX_ALW_DAYS_SUPL_QTY"),
        F.col("drug_clm.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
        F.col("drug_clm.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
        F.when(
            F.length(F.trim(F.col("drug_clm.RX_ALW_QTY"))) <= 11,
            F.when(
                F.col("drug_clm.RX_ALW_QTY") >= 0,
                F.col("drug_clm.RX_ALW_QTY")
            ).otherwise(
                -1 * F.col("drug_clm.RX_ALW_QTY")
            )
        ).otherwise(F.lit(None)).alias("RX_ORIG_QTY"),
        F.col("drug_clm.NDC").alias("NDC"),
        F.col("drug_clm.TRGT_CD_DAW").alias("DAW_Cd"),
        F.when(
            F.col("drug_clm.NDC").isNull(),
            F.lit(" ")
        ).otherwise(
            F.when(F.col("drug_clm.MAIL_ORDER_IN") == "N", F.lit("R")).otherwise(F.lit("M"))
        ).alias("MAIL_ORDER_IN"),
        F.col("drug_clm.RFL_NO").alias("RFL_NO"),
        F.col("drug_clm.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
        F.lit(" ").alias("Rx_Pay_Tier"),
        F.when(F.col("drug_clm.NON_FRMLRY_DRUG_IN") == "Y", F.lit("N"))
         .otherwise(
            F.when(F.col("drug_clm.NON_FRMLRY_DRUG_IN") == "N", F.lit("Y"))
             .otherwise(F.lit(" "))
         ).alias("Rx_Form_Ind"),
        F.when(
            F.expr("RIGHT(MainFileLink.CLM_ID,1)") == F.lit("R"),
            F.lit("D")
        ).otherwise(
            F.when(
                F.col("MainFileLink.RVNU_CD") != "NA",
                F.lit("F")
            ).otherwise(
                F.when(
                    F.col("drug_clm.NDC_SK").isNotNull(),
                    F.lit("R")
                ).otherwise(F.lit("P"))
            )
        ).alias("Claim_Code"),
        F.when(
            F.col("clm_ln_diag.CLM_ID").isNull() | (F.length(F.col("clm_ln_diag.DIAG_CD")) > 7),
            F.lit("UNK")
        ).otherwise(F.col("clm_ln_diag.DIAG_CD_TYP_CD")).alias("DIAG_CD_TYP_CD"),
        F.col("MainFileLink.CLM_ID").alias("CLM_ID"),
        F.lit(" ").alias("Filler2"),
        F.lit(" ").alias("Filler3"),
        F.lit(" ").alias("Filler4"),
        F.lit(" ").alias("Filler5"),
        F.lit(" ").alias("Filler6"),
        F.lit("D").alias("Record_Type"),
    )
)

write_files(
    df_MAIN_TRANS_TextFile,
    f"{adls_path_publish}/external/MedStat.dat.{RunID}",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Agg_In_schema = StructType([
    StructField("ALW_AMT", DecimalType(38,10)),
    StructField("BILL_TYPE", StringType()),
    StructField("CAP_LN_IN", StringType()),
    StructField("CNSD_CHRG_AMT", DecimalType(38,10)),
    StructField("Filler1", StringType()),
    StructField("CLM_TYP_CD", StringType()),
    StructField("COINS_AMT", DecimalType(38,10)),
    StructField("COPAY_AMT", DecimalType(38,10)),
    StructField("BRTH_DT_SK", StringType()),
    StructField("SVC_STRT_DT_SK", StringType()),
    StructField("SVC_END_DT_SK", StringType()),
    StructField("ADMS_DT_SK", StringType()),
    StructField("PD_DT_SK", StringType()),
    StructField("LOS_DAYS", IntegerType()),
    StructField("DEDCT_AMT", DecimalType(38,10)),
    StructField("Diag_Cd_UB", StringType()),
    StructField("Diag_Cd", StringType()),
    StructField("DISCH_STATUS", StringType()),
    StructField("DISCOUNT_AMT", DecimalType(38,10)),
    StructField("FAMILY_ID", StringType()),
    StructField("Gender_Cd", StringType()),
    StructField("CLM_LN_SEQ_NO", StringType()),
    StructField("PAYBL_AMT", DecimalType(38,10)),
    StructField("NETWORK_PROV_IN", StringType()),
    StructField("RFRNG_PROV_TX", StringType()),
    StructField("PCP_SUBMT_IN", StringType()),
    StructField("CLM_LN_POS_CD_SK", StringType()),
    StructField("Proc_Cd_Surg1", StringType()),
    StructField("Proc_Cd", StringType()),
    StructField("PROV_ID", StringType()),
    StructField("PROV_TYPE_CD", StringType()),
    StructField("POSTAL_CD", StringType()),
    StructField("RVNU_CD", StringType()),
    StructField("Third_Prty_Amt", DecimalType(38,10)),
    StructField("UNIT_CT", StringType()),
    StructField("PROC_CD_MOD", StringType()),
    StructField("PROV_NM", StringType()),
    StructField("Patient_SSN", StringType()),
    StructField("RX_ALW_DAYS_SUPL_QTY", StringType()),
    StructField("DISPNS_FEE_AMT", DecimalType(38,10)),
    StructField("INGR_CST_ALW_AMT", DecimalType(38,10)),
    StructField("RX_ORIG_QTY", StringType()),
    StructField("NDC", StringType()),
    StructField("DAW_Cd", StringType()),
    StructField("MAIL_ORDER_IN", StringType()),
    StructField("RFL_NO", StringType()),
    StructField("SLS_TAX_AMT", DecimalType(38,10)),
    StructField("Rx_Pay_Tier", StringType()),
    StructField("Rx_Form_Ind", StringType()),
    StructField("Claim_Code", StringType()),
    StructField("DIAG_CD_TYP_CD", StringType()),
    StructField("CLM_ID", StringType()),
    StructField("Filler2", StringType()),
    StructField("Filler3", StringType()),
    StructField("Filler4", StringType()),
    StructField("Filler5", StringType()),
    StructField("Filler6", StringType()),
    StructField("Record_Type", StringType()),
])

df_Agg_In = spark.read.format("csv").option("header", "false").option("delimiter", "|").option("quote", "\"").schema(df_Agg_In_schema).load(f"{adls_path_publish}/external/MedStat.dat.{RunID}")

df_Tr_AggData = df_Agg_In.agg(
    F.count("CLM_ID").alias("Rec_Cnt"),
    F.sum("PAYBL_AMT").alias("NetPayments")
)

write_files(
    df_Tr_AggData,
    f"{adls_path_publish}/external/MedStatAgg.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Tr_Sum_schema = StructType([
    StructField("Rec_Cnt", StringType()),
    StructField("NetPayments", DecimalType(38,10))
])

df_Tr_Sum = spark.read.format("csv").option("header", "false").option("delimiter", ",").option("quote", "\"").schema(df_Tr_Sum_schema).load(f"{adls_path_publish}/external/MedStatAgg.dat")

df_TrailerRecord = df_Tr_Sum.select(
    F.lit(BeginDate).alias("Data_Start_Date"),
    F.lit(EndDate).alias("Data_End_Date"),
    F.col("Rec_Cnt").alias("Rec_Cnt"),
    F.col("NetPayments").alias("NetPayments"),
    F.lit(" " * 474).alias("Filler1"),
    F.lit("T").alias("Rec_Type")
)

write_files(
    df_TrailerRecord,
    f"{adls_path_publish}/external/MedStat_Trailer.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)