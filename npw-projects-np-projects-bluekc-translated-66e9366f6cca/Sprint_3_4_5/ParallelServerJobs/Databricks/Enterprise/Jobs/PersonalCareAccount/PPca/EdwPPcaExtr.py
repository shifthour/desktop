# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 08/31/07 14:39:46 Batch  14488_52789 INIT bckcetl edw10 dsadm dsadm
# MAGIC ^1_2 02/16/07 14:56:33 Batch  14292_53801 PROMOTE bckcetl edw10 dsadm Keith for Sharon
# MAGIC ^1_2 02/16/07 14:54:26 Batch  14292_53674 INIT bckcett testEDW10 dsadm Keith for Sharon
# MAGIC ^1_13 02/16/07 14:40:30 Batch  14292_52839 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_13 02/16/07 14:38:18 Batch  14292_52701 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_13 02/16/07 14:36:59 Batch  14292_52621 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_12 02/16/07 14:34:02 Batch  14292_52444 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_10 02/07/07 13:40:29 Batch  14283_49233 PROMOTE bckcett devlEDW10 u10157 sa
# MAGIC ^1_10 02/07/07 13:17:42 Batch  14283_47863 INIT bckcett testEDW10 u10157 sa
# MAGIC ^1_6 01/31/07 16:52:58 Batch  14276_60794 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_6 01/31/07 16:50:18 Batch  14276_60621 PROMOTE bckcett devlEDW10 u10157 sa
# MAGIC ^1_6 01/31/07 16:48:40 Batch  14276_60522 INIT bckcetl edw10 dcg01 sa
# MAGIC ^1_5 01/30/07 16:40:20 Batch  14275_60023 PROMOTE bckcett edw10 u10157 sa
# MAGIC ^1_5 01/30/07 16:35:06 Batch  14275_59709 INIT bckcett testEDW10 u10157 SA
# MAGIC ^1_4 01/30/07 15:21:33 Batch  14275_55295 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_4 01/30/07 15:19:35 Batch  14275_55179 INIT bckcetl edw10 dcg01 sa
# MAGIC ^1_3 01/29/07 13:06:32 Batch  14274_47199 PROMOTE bckcett edw10 u10157 sa
# MAGIC ^1_3 01/29/07 13:01:29 Batch  14274_46893 INIT bckcett testEDW10 u10157 sa
# MAGIC ^1_1 01/25/07 09:36:46 Batch  14270_34611 INIT bckcett testEDW10 dsadm bls
# MAGIC ^1_2 01/22/07 16:18:40 Batch  14267_58774 PROMOTE bckcett testEDW10 u10157 sa
# MAGIC ^1_2 01/22/07 16:13:54 Batch  14267_58435 INIT bckcett devlEDW10 u10157 sa
# MAGIC ^1_1 01/19/07 10:30:30 Batch  14264_37832 INIT bckcett devlEDW10 u10157 sa
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwPPcaExtr
# MAGIC 
# MAGIC DESCRIPTION:      Pulls claims level data from EDW tables to be loaded into P_PCA.  The working table W_PCA_RTN_SVC_CLM_LN table is used to optimize efficiency.
# MAGIC 
# MAGIC INPUTS:
# MAGIC                                 W_PCA_RTN_SVC_CLM_LN
# MAGIC                                 MBR_D
# MAGIC                                 CLM_F
# MAGIC                                 CLM_F2
# MAGIC                                 PCA_F
# MAGIC                                 DIAG_CD_D
# MAGIC                                 CLM_EXTRNL_PROV_D
# MAGIC                                 PROV_ADDR_D
# MAGIC                                 PROV_D
# MAGIC                                 SUB_ID_HIST_D
# MAGIC                                 CLM_COB_F
# MAGIC                                 CLM_LN_COB_F
# MAGIC 
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                                 hf_ppca_pca_rtn_svc
# MAGIC                                 hf_ppca_pca
# MAGIC                                 hf_ppca_diag_cd
# MAGIC                                 hf_ppca_clm_extrnl_prov
# MAGIC                                 hf_ppca_prov
# MAGIC                                 hf_ppca_prov_addr
# MAGIC                                 hf_ppca_sub_id_hist
# MAGIC                                 hf_ppca_clm_ln_pos_cd
# MAGIC                                 hf_ppca_clm_ln_tot_coins_amt
# MAGIC                                 hf_ppca_clm_ln_tot_copay_amt
# MAGIC                                 hf_ppca_clm_ln_tot_chrg_amt
# MAGIC                                 hf_ppca_clm_ln_tot_dedct_amt
# MAGIC                                 hf_ppca_cob_clm
# MAGIC                                 hf_ppca_clm_ln_tot_mbr_oblgtn_dsalw_amt
# MAGIC                                 hf_ppca_pca_calc_patn_resp_amt
# MAGIC                                 hf_ppca_clm_ln_tot_pca_hlth_rmbrmt_acct_amt
# MAGIC                                 hf_ppca_clm_ln_tot_cnsd_chrg_amt_incld
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                                FORMAT.DATE
# MAGIC                                TRIM
# MAGIC                                NullToZero
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                                Convert dates from CCYY-MM-DD to MM-DD-CCYY
# MAGIC                                Trim whitespace
# MAGIC                                Lookups to filter records
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 		P_PCA.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                 Tao Luo                  Initial Development           06/14/2006
# MAGIC If (
# MAGIC     (IsNull (Clm_Ln_Tot_Chrg_Amt_Lookup.CLM_LN_TOT_CHRG_AMT) = @True)
# MAGIC     AND
# MAGIC     (IsNull (Clm_Ln_Tot_Dedct_Amt_Lookup.CLM_LN_TOT_DEDCT_AMT) = @True)
# MAGIC     AND
# MAGIC     (IsNull (Clm_Ln_Tot_Copay_Amt_Lookup.CLM_LN_TOT_COPAY_AMT) = @True)
# MAGIC     AND
# MAGIC     (IsNull (Clm_Ln_Tot_Coins_Amt_Lookup.CLM_LN_TOT_COINS_AMT) = @True)
# MAGIC    )
# MAGIC Then
# MAGIC   "SVCEXCL"
# MAGIC Else
# MAGIC   "NA"

# MAGIC Used for CLM_LN_TOT_DEDCT_AMT.  Sum of the CLM_LN_DEDCT_AMT.
# MAGIC Used for CLM_LN_TOT_CHRG_AMT.  Sum of the CLM_LN_CHRG_AMT.
# MAGIC Driving Data Stream.  At Claim Level.
# MAGIC Used for CLM_LN_TOT_COPAY_AMT.  Sum of the CLM_LN_COPAY_AMT.
# MAGIC Used for CLM_LN_TOT_COINS_AMT.  Sum of the CLM_LN_COINS_AMT.
# MAGIC Used for PCA_POS_CD if CLM_SUBTYP_CD = \"PR\"
# MAGIC Used for SUB_ID
# MAGIC Used for the Provider fields if CLM_ITS_IN = 'N'
# MAGIC Used for the Provider fields if CLM_ITS_IN <> 'N'
# MAGIC Used for DIAG_CD_1
# MAGIC Used for PCA_EXCL_RSN_IN, PCA_HLTH_RMBRMT_ACCT_AMT and PCA_EXCL_RSN_CD to determine if the record has been previously processed.
# MAGIC Used for PCA_EXCL_RSN_IN and PCA_EXCL_RSN_CD to determine the indicator on the claim lines for each claim.
# MAGIC Used for Provider Address fields.
# MAGIC Used for PCA_HLTH_RMBRMT_ACCT_AMT.  Sum of the charges for the included services only.
# MAGIC Used for PCA_HLTH_RMBRMT_ACCT_AMT.  Sum of the charges for the included services only.
# MAGIC Used for PCA_CALC_PATN_RESP_AMT.  Sum of Considered Charges - Payable Amount + COB Paid Amount + No-Resposibility Withhold Amount + Provider Write-off Amount
# MAGIC Used for PCA_CALC_PATN_RESP_AMT.  Sum of the CLM_LN_MBR_OBLGTN_DSALW_AMT
# MAGIC Used for PCA_CALC_PATN_RESP_AMT and PCA_HLTH_RMBRMT_ACCT_AMT to determine a COB claim.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StringType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Enterprise
# COMMAND ----------


CurrRunDate = get_widget_value('CurrRunDate','2006-06-27')
EDWOwner = get_widget_value('$EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

jdbc_url, jdbc_props = get_db_config(edw_secret_name)

df_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT(CLM_F.CLM_SK),
CLM_F.SRC_SYS_CD,
CLM_F.CLM_ITS_IN,
CLM_F.CLM_ID,
CLM_F.CLM_STTUS_CD,
CLM_F.CLM_SUBTYP_CD,
CLM_F.CLM_SVC_STRT_DT_SK,
CLM_F.CLM_SVC_END_DT_SK,
CLM_F.DIAG_CD_1_SK,
CLM_F.SVC_PROV_SK,
CLM_F2.CLM_EXTRNL_PROV_SK,
MBR_D.MBR_FIRST_NM,
MBR_D.MBR_LAST_NM,
MBR_D.MBR_BRTH_DT_SK,
MBR_D.MBR_GNDR_CD,
MBR_D.SUB_FIRST_NM,
MBR_D.SUB_LAST_NM,
CLM_F.CLM_IN_NTWK_IN,
MBR_D.MBR_RELSHP_CD,
MBR_D.MBR_UNIQ_KEY,
CLM_F.GRP_ID,
CLM_F.CLM_MBR_SFX_NO,
CLM_F.CLM_ADJ_FROM_CLM_ID,
CLM_F.CLM_ADJ_TO_CLM_ID,
MBR_D.SUB_IN,
MBR_D.MBR_MAIL_ADDR_CONF_COMM_IN,
CLM_F.CLM_LN_TOT_COINS_AMT,
CLM_F.CLM_LN_TOT_COPAY_AMT,
CLM_F.CLM_LN_TOT_DEDCT_AMT
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA,
     {EDWOwner}.MBR_D MBR_D,
     {EDWOwner}.CLM_F CLM_F,
     {EDWOwner}.CLM_F2 CLM_F2
WHERE W_PCA.INCLD_IN <> 'Y'
  AND W_PCA.CLM_SK = CLM_F.CLM_SK
  AND CLM_F.MBR_SK = MBR_D.MBR_SK
  AND CLM_F.CLM_SK = CLM_F2.CLM_SK
"""
    )
    .load()
)

df_Extract_Pca = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT DISTINCT PCA_F.CLM_ID as CLM_ID FROM {EDWOwner}.PCA_F PCA_F"
    )
    .load()
)

df_Extract_Diag_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"SELECT DIAG_CD_D.DIAG_CD_SK as DIAG_CD_SK,DIAG_CD_D.DIAG_CD as DIAG_CD FROM {EDWOwner}.DIAG_CD_D DIAG_CD_D"
    )
    .load()
)

df_Extract_Clm_Extrnl_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT CLM_F2.CLM_SK as CLM_SK,
       CLM_F2.SRC_SYS_CD as SRC_SYS_CD,
       E_PROV.CLM_EXTRNL_PROV_TAX_ID as CLM_EXTRNL_PROV_TAX_ID,
       E_PROV.CLM_EXTRNL_PROV_NM as CLM_EXTRNL_PROV_NM,
       E_PROV.CLM_EXTRNL_PROV_CITY_NM as CLM_EXTRNL_PROV_CITY_NM,
       E_PROV.CLM_EXTRNL_PROV_ST_CD as CLM_EXTRNL_PROV_ST_CD,
       E_PROV.CLM_EXTRNL_PROV_ZIP_CD_5 as CLM_EXTRNL_PROV_ZIP_CD_5,
       E_PROV.CLM_EXTRNL_PROV_ZIP_CD_4 as CLM_EXTRNL_PROV_ZIP_CD_4
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA,
     {EDWOwner}.CLM_F2 CLM_F2,
     {EDWOwner}.CLM_EXTRNL_PROV_D E_PROV
WHERE W_PCA.CLM_SK = CLM_F2.CLM_SK
  AND CLM_F2.CLM_EXTRNL_PROV_SK = E_PROV.CLM_EXTRNL_PROV_SK
"""
    )
    .load()
)

df_Extract_Prov = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       PROV_D.PROV_ID as PROV_ID,
       PROV_D.PROV_TAX_ID as PROV_TAX_ID,
       PROV_D.PROV_NM as PROV_NM
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA,
     {EDWOwner}.CLM_F CLM_F,
     {EDWOwner}.PROV_D PROV_D
WHERE W_PCA.CLM_SK = CLM_F.CLM_SK
  AND CLM_F.SVC_PROV_SK = PROV_D.PROV_SK
"""
    )
    .load()
)

df_Extract_Sub_Id_Hist = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT(CLM_F.CLM_SK) as CLM_SK,
       SUB_ID_HIST_D.SUB_ID as SUB_ID
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA,
     {EDWOwner}.CLM_F CLM_F,
     {EDWOwner}.SUB_ID_HIST_D SUB_ID_HIST_D
WHERE W_PCA.INCLD_IN <> 'Y'
  AND W_PCA.CLM_SK = CLM_F.CLM_SK
  AND CLM_F.SUB_SK = SUB_ID_HIST_D.SUB_SK
  AND SUB_ID_HIST_D.SRC_SYS_CD = 'FACETS'
  AND CLM_F.CLM_PD_DT_SK BETWEEN SUB_ID_HIST_D.EDW_RCRD_STRT_DT_SK AND SUB_ID_HIST_D.EDW_RCRD_END_DT_SK
"""
    )
    .load()
)

df_Extract_Clm_Ln_Pos_Cd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT CLM_LN_F.CLM_SK as CLM_SK,
       CLM_LN_F.CLM_LN_POS_CD as CLM_LN_POS_CD
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA,
     {EDWOwner}.CLM_LN_F CLM_LN_F
WHERE W_PCA.INCLD_IN <> 'Y'
  AND CLM_LN_F.CLM_LN_SEQ_NO = 1
  AND W_PCA.CLM_LN_SK = CLM_LN_F.CLM_LN_SK
"""
    )
    .load()
)

df_ClmLn_Tot_Chrg_Amt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       SUM(W_PCA.CLM_LN_CHRG_AMT) as CLM_LN_TOT_CHRG_AMT
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA
WHERE W_PCA.INCLD_IN = 'X'
GROUP BY W_PCA.CLM_SK
"""
    )
    .load()
)

df_ClmLn_Tot_Dedct_Amt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       SUM(W_PCA.CLM_LN_DEDCT_AMT) as CLM_LN_TOT_DEDCT_AMT
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA
WHERE W_PCA.INCLD_IN = 'X'
GROUP BY W_PCA.CLM_SK
"""
    )
    .load()
)

df_Extract_Clm_Ln_Tot_Copay_Amt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       SUM(W_PCA.CLM_LN_COPAY_AMT) as CLM_LN_TOT_COPAY_AMT
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA
WHERE W_PCA.INCLD_IN = 'X'
GROUP BY W_PCA.CLM_SK
"""
    )
    .load()
)

df_Extract_Clm_Ln_Tot_Coins_Amt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       SUM(W_PCA.CLM_LN_COINS_AMT) as CLM_LN_TOT_COINS_AMT
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA
WHERE W_PCA.INCLD_IN = 'X'
GROUP BY W_PCA.CLM_SK
"""
    )
    .load()
)

df_Cob_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT DISTINCT(W_PCA.CLM_SK) as CLM_SK
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA,
     {EDWOwner}.CLM_COB_F CLM_COB_F
WHERE W_PCA.INCLD_IN = 'X'
  AND CLM_COB_F.CLM_COB_PD_AMT <> 0
  AND W_PCA.CLM_SK = CLM_COB_F.CLM_SK
"""
    )
    .load()
)

df_ClmLnTot_Mbr_Oblgtn_Dsalw_Amt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       SUM(W_PCA.CLM_LN_MBR_OBLGTN_DSALW_AMT) as CLM_LN_TOT_MBR_OBLGTN_DSALW_AMT
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA
WHERE W_PCA.INCLD_IN = 'X'
GROUP BY W_PCA.CLM_SK
"""
    )
    .load()
)

df_Pca_Calc_Patn_Resp_Amt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       SUM(W_PCA.CLM_LN_CNSD_CHRG_AMT - W_PCA.CLM_LN_PAYBL_AMT + VALUE(CLM_LN_COB_F.CLM_LN_COB_PD_AMT, 0)
           + W_PCA.CLM_LN_NO_RESP_DSALW_AMT + W_PCA.CLM_LN_PROV_WRT_OFF_AMT) as PCA_CALC_PATN_RESP_AMT
FROM {EDWOwner}.CLM_LN_COB_F CLM_LN_COB_F
RIGHT OUTER JOIN
(
  SELECT
    PCA.CLM_SK,
    PCA.CLM_LN_SK,
    PCA.CLM_LN_CNSD_CHRG_AMT,
    PCA.CLM_LN_PAYBL_AMT,
    PCA.CLM_LN_NO_RESP_DSALW_AMT,
    PCA.CLM_LN_PROV_WRT_OFF_AMT
  FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN PCA
  WHERE PCA.INCLD_IN = 'X'
) W_PCA
ON W_PCA.CLM_LN_SK = CLM_LN_COB_F.CLM_LN_SK
GROUP BY W_PCA.CLM_SK
"""
    )
    .load()
)

df_Extract_Pca_Rtn_Svc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       W_PCA.INCLD_IN as INCLD_IN
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA
WHERE W_PCA.INCLD_IN IN ('D', 'N', 'X', 'Y')
GROUP BY W_PCA.CLM_SK, W_PCA.INCLD_IN
"""
    )
    .load()
)

df_ClmLnTot_Pca_Hlth_Rmbrmt_Acct_Amt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       SUM(W_PCA.CLM_LN_DEDCT_AMT + W_PCA.CLM_LN_COINS_AMT + W_PCA.CLM_LN_COPAY_AMT) as CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA
WHERE W_PCA.INCLD_IN = 'X'
GROUP BY W_PCA.CLM_SK
"""
    )
    .load()
)

df_ClmLnTot_Cnsd_Chrg_Amt = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option(
        "query",
        f"""
SELECT W_PCA.CLM_SK as CLM_SK,
       SUM(W_PCA.CLM_LN_CNSD_CHRG_AMT) as CLM_LN_TOT_CNSD_CHRG_AMT
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA
WHERE W_PCA.INCLD_IN = 'Y'
GROUP BY W_PCA.CLM_SK
"""
    )
    .load()
)

df_Extract_Prov_Addr = (
    spark.read.format("jdbc")
    .options(**jdbc_props)
    .option("url", jdbc_url)
    .option(
        "query",
        f"""
SELECT DISTINCT(CLM_F.CLM_SK) as CLM_SK,
       PROV_ADDR_D.PROV_ADDR_LN_1 as PROV_ADDR_LN_1,
       PROV_ADDR_D.PROV_ADDR_LN_2 as PROV_ADDR_LN_2,
       PROV_ADDR_D.PROV_ADDR_CITY_NM as PROV_ADDR_CITY_NM,
       PROV_ADDR_D.PROV_ADDR_ST_CD as PROV_ADDR_ST_CD,
       PROV_ADDR_D.PROV_ADDR_ZIP_CD_5 as PROV_ADDR_ZIP_CD_5,
       PROV_ADDR_D.PROV_ADDR_ZIP_CD_4 as PROV_ADDR_ZIP_CD_4
FROM {EDWOwner}.W_PCA_RTN_SVC_CLM_LN W_PCA,
     {EDWOwner}.CLM_F CLM_F,
     {EDWOwner}.PROV_D PROV_D,
     {EDWOwner}.PROV_ADDR_D PROV_ADDR_D
WHERE W_PCA.CLM_SK = CLM_F.CLM_SK
  AND PROV_ADDR_D.PROV_ADDR_TYP_CD = 'P'
  AND CLM_F.SVC_PROV_SK = PROV_D.PROV_SK
  AND PROV_D.PROV_ADDR_ID = PROV_ADDR_D.PROV_ADDR_ID
  AND CLM_F.CLM_SVC_STRT_DT_SK BETWEEN PROV_ADDR_D.PROV_ADDR_EFF_DT_SK AND PROV_ADDR_D.PROV_ADDR_TERM_DT_SK
"""
    )
    .load()
)

df_Extract_Pca_dedup = dedup_sort(df_Extract_Pca, ["CLM_ID"], [])
df_Extract_Diag_Cd_dedup = dedup_sort(df_Extract_Diag_Cd, ["DIAG_CD_SK"], [])
df_Extract_Clm_Extrnl_Prov_dedup = dedup_sort(df_Extract_Clm_Extrnl_Prov, ["CLM_SK","SRC_SYS_CD"], [])
df_Extract_Prov_dedup = dedup_sort(df_Extract_Prov, ["CLM_SK"], [])
df_Extract_Sub_Id_Hist_dedup = dedup_sort(df_Extract_Sub_Id_Hist, ["CLM_SK"], [])
df_Extract_Clm_Ln_Pos_Cd_dedup = dedup_sort(df_Extract_Clm_Ln_Pos_Cd, ["CLM_SK"], [])
df_ClmLn_Tot_Chrg_Amt_dedup = dedup_sort(df_ClmLn_Tot_Chrg_Amt, ["CLM_SK"], [])
df_ClmLn_Tot_Dedct_Amt_dedup = dedup_sort(df_ClmLn_Tot_Dedct_Amt, ["CLM_SK"], [])
df_Extract_Clm_Ln_Tot_Copay_Amt_dedup = dedup_sort(df_Extract_Clm_Ln_Tot_Copay_Amt, ["CLM_SK"], [])
df_Extract_Clm_Ln_Tot_Coins_Amt_dedup = dedup_sort(df_Extract_Clm_Ln_Tot_Coins_Amt, ["CLM_SK"], [])
df_Cob_Clm_dedup = dedup_sort(df_Cob_Clm, ["CLM_SK"], [])
df_ClmLnTot_Mbr_Oblgtn_Dsalw_Amt_dedup = dedup_sort(df_ClmLnTot_Mbr_Oblgtn_Dsalw_Amt, ["CLM_SK"], [])
df_Pca_Calc_Patn_Resp_Amt_dedup = dedup_sort(df_Pca_Calc_Patn_Resp_Amt, ["CLM_SK"], [])
df_Extract_Pca_Rtn_Svc_dedup = dedup_sort(df_Extract_Pca_Rtn_Svc, ["CLM_SK","INCLD_IN"], [])
df_ClmLnTot_Pca_Hlth_Rmbrmt_Acct_Amt_dedup = dedup_sort(df_ClmLnTot_Pca_Hlth_Rmbrmt_Acct_Amt, ["CLM_SK"], [])
df_ClmLnTot_Cnsd_Chrg_Amt_dedup = dedup_sort(df_ClmLnTot_Cnsd_Chrg_Amt, ["CLM_SK"], [])
df_Extract_Prov_Addr_dedup = dedup_sort(df_Extract_Prov_Addr, ["CLM_SK"], [])

df_BusinessRules = (
    df_Extract.alias("Extract")
    .join(
        df_Extract_Pca_dedup.alias("Pca_Lookup"),
        F.col("Extract.CLM_ADJ_FROM_CLM_ID") == F.col("Pca_Lookup.CLM_ID"),
        "left",
    )
    .join(
        df_Extract_Clm_Ln_Pos_Cd_dedup.alias("Clm_Ln_Pos_Cd_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Clm_Ln_Pos_Cd_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_Extract_Diag_Cd_dedup.alias("Diag_Cd_Lookup"),
        F.col("Extract.DIAG_CD_1_SK") == F.col("Diag_Cd_Lookup.DIAG_CD_SK"),
        "left",
    )
    .join(
        df_Extract_Clm_Extrnl_Prov_dedup.alias("Clm_Extrnl_Prov_Lookup"),
        [
            F.col("Extract.CLM_SK") == F.col("Clm_Extrnl_Prov_Lookup.CLM_SK"),
            F.col("Extract.SRC_SYS_CD") == F.col("Clm_Extrnl_Prov_Lookup.SRC_SYS_CD"),
        ],
        "left",
    )
    .join(
        df_Extract_Prov_dedup.alias("Prov_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Prov_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_Extract_Sub_Id_Hist_dedup.alias("Sub_Id_Hist_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Sub_Id_Hist_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_ClmLn_Tot_Chrg_Amt_dedup.alias("Clm_Ln_Tot_Chrg_Amt_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Clm_Ln_Tot_Chrg_Amt_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_ClmLn_Tot_Dedct_Amt_dedup.alias("Clm_Ln_Tot_Dedct_Amt_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Clm_Ln_Tot_Dedct_Amt_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_Extract_Clm_Ln_Tot_Copay_Amt_dedup.alias("Clm_Ln_Tot_Copay_Amt_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Clm_Ln_Tot_Copay_Amt_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_Extract_Clm_Ln_Tot_Coins_Amt_dedup.alias("Clm_Ln_Tot_Coins_Amt_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Clm_Ln_Tot_Coins_Amt_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_Cob_Clm_dedup.alias("Cob_Clm_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Cob_Clm_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_ClmLnTot_Mbr_Oblgtn_Dsalw_Amt_dedup.alias("Clm_Ln_Tot_Mbr_Oblgtn_Dsalw_Amt_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Clm_Ln_Tot_Mbr_Oblgtn_Dsalw_Amt_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_Pca_Calc_Patn_Resp_Amt_dedup.alias("Pca_Calc_Patn_Resp_Amt_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Pca_Calc_Patn_Resp_Amt_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_ClmLnTot_Pca_Hlth_Rmbrmt_Acct_Amt_dedup.alias("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_ClmLnTot_Cnsd_Chrg_Amt_dedup.alias("Clm_Ln_Tot_Cnsd_Chrg_Amt_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Clm_Ln_Tot_Cnsd_Chrg_Amt_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_Extract_Pca_Rtn_Svc_dedup.alias("Pca_Rtn_Svc_Incld"),
        [
            F.col("Extract.CLM_SK") == F.col("Pca_Rtn_Svc_Incld.CLM_SK"),
            F.lit("Y") == F.col("Pca_Rtn_Svc_Incld.INCLD_IN"),
        ],
        "left",
    )
    .join(
        df_Extract_Pca_Rtn_Svc_dedup.alias("Pca_Rtn_Svc_Excld"),
        [
            F.col("Extract.CLM_SK") == F.col("Pca_Rtn_Svc_Excld.CLM_SK"),
            F.lit("N") == F.col("Pca_Rtn_Svc_Excld.INCLD_IN"),
        ],
        "left",
    )
    .join(
        df_Extract_Pca_Rtn_Svc_dedup.alias("Pca_Rtn_Svc_Dup"),
        [
            F.col("Extract.CLM_SK") == F.col("Pca_Rtn_Svc_Dup.CLM_SK"),
            F.lit("D") == F.col("Pca_Rtn_Svc_Dup.INCLD_IN"),
        ],
        "left",
    )
    .join(
        df_Extract_Prov_Addr_dedup.alias("Prov_Addr_Lookup"),
        F.col("Extract.CLM_SK") == F.col("Prov_Addr_Lookup.CLM_SK"),
        "left",
    )
    .join(
        df_Extract_Pca_Rtn_Svc_dedup.alias("Pca_Rtn_Svc_X"),
        [
            F.col("Extract.CLM_SK") == F.col("Pca_Rtn_Svc_X.CLM_SK"),
            F.lit("X") == F.col("Pca_Rtn_Svc_X.INCLD_IN"),
        ],
        "left",
    )
    .withColumn("svPcaSttusCd", trim(F.col("Extract.CLM_STTUS_CD")))
    .withColumn(
        "svPcaSubCatDesc",
        F.when(F.col("Extract.CLM_SUBTYP_CD") == "PR", F.lit("MEDICAL")).otherwise(F.lit("FACILITY")),
    )
    .withColumn(
        "svPcaPosCd",
        F.when(F.col("Extract.CLM_SUBTYP_CD") == "PR", F.col("Clm_Ln_Pos_Cd_Lookup.CLM_LN_POS_CD"))
        .otherwise(
            F.when(F.col("Extract.CLM_SUBTYP_CD") == "IP", F.lit("12"))
            .otherwise(
                F.when(F.col("Extract.CLM_SUBTYP_CD") == "OP", F.lit("13")).otherwise(F.lit("UN"))
            )
        ),
    )
    .withColumn(
        "svSvcProvTaxId",
        F.when(F.col("Extract.CLM_ITS_IN") == "N", F.col("Prov_Lookup.PROV_TAX_ID")).otherwise(F.col("Clm_Extrnl_Prov_Lookup.CLM_EXTRNL_PROV_TAX_ID")),
    )
    .withColumn(
        "svSvcProvTaxIdRmNull",
        F.when(F.col("svSvcProvTaxId").isNull(), F.lit(" ")).otherwise(F.col("svSvcProvTaxId")),
    )
    .withColumn(
        "svSvcProvNm",
        F.when(F.col("Extract.CLM_ITS_IN") == "N", F.col("Prov_Lookup.PROV_NM")).otherwise(F.col("Clm_Extrnl_Prov_Lookup.CLM_EXTRNL_PROV_NM")),
    )
    .withColumn(
        "svProvPriPrctcAddrLn1",
        F.when(F.col("Extract.CLM_ITS_IN") == "N", F.col("Prov_Addr_Lookup.PROV_ADDR_LN_1")).otherwise(F.lit("ITS")),
    )
    .withColumn(
        "svProvPriPrctcAddrLn2",
        F.when(F.col("Extract.CLM_ITS_IN") == "N", F.col("Prov_Addr_Lookup.PROV_ADDR_LN_2")).otherwise(F.lit("ITS")),
    )
    .withColumn(
        "svProvPriPrctcAddrCityNm",
        F.when(F.col("Extract.CLM_ITS_IN") == "N", F.col("Prov_Addr_Lookup.PROV_ADDR_CITY_NM"))
        .otherwise(F.col("Clm_Extrnl_Prov_Lookup.CLM_EXTRNL_PROV_CITY_NM")),
    )
    .withColumn(
        "svProvPriPrctcAddrStCd",
        F.when(F.col("Extract.CLM_ITS_IN") == "N", F.col("Prov_Addr_Lookup.PROV_ADDR_ST_CD"))
        .otherwise(F.col("Clm_Extrnl_Prov_Lookup.CLM_EXTRNL_PROV_ST_CD")),
    )
    .withColumn(
        "svProvPriPrctcAddrStCdRmNull",
        F.when(F.col("svProvPriPrctcAddrStCd").isNull(), F.lit("UNK")).otherwise(F.col("svProvPriPrctcAddrStCd")),
    )
    .withColumn(
        "svProvPriPrctcAddrZipCd5",
        F.when(F.col("Extract.CLM_ITS_IN") == "N", F.col("Prov_Addr_Lookup.PROV_ADDR_ZIP_CD_5"))
        .otherwise(F.col("Clm_Extrnl_Prov_Lookup.CLM_EXTRNL_PROV_ZIP_CD_5")),
    )
    .withColumn(
        "svProvPriPrctcAddrZipCd4",
        F.when(F.col("Extract.CLM_ITS_IN") == "N", F.col("Prov_Addr_Lookup.PROV_ADDR_ZIP_CD_4"))
        .otherwise(F.col("Clm_Extrnl_Prov_Lookup.CLM_EXTRNL_PROV_ZIP_CD_4")),
    )
    .withColumn(
        "svPcaMbrGndrCd",
        F.when(
            (F.trim(F.col("Extract.MBR_GNDR_CD")) == "UNK")
            | (F.trim(F.col("Extract.MBR_GNDR_CD")) == "NA"),
            F.lit("U"),
        ).otherwise(F.col("Extract.MBR_GNDR_CD")),
    )
    .withColumn(
        "tmpPcaCalcPatnRespAmt",
        F.when(F.col("Cob_Clm_Lookup.CLM_SK").isNull(), 
               F.coalesce(F.col("Clm_Ln_Tot_Mbr_Oblgtn_Dsalw_Amt_Lookup.CLM_LN_TOT_MBR_OBLGTN_DSALW_AMT"), F.lit(0)))
        .otherwise(
            F.when(F.col("Extract.CLM_IN_NTWK_IN") == "Y",
                   F.coalesce(F.col("Clm_Ln_Tot_Mbr_Oblgtn_Dsalw_Amt_Lookup.CLM_LN_TOT_MBR_OBLGTN_DSALW_AMT"), F.lit(0)))
            .otherwise(F.col("Pca_Calc_Patn_Resp_Amt_Lookup.PCA_CALC_PATN_RESP_AMT"))
        ),
    )
    .withColumn("svPcaCalcPatnRespAmt", F.col("tmpPcaCalcPatnRespAmt"))
    .withColumn(
        "svPcaMbrRelshpCd",
        F.when(F.col("Extract.MBR_RELSHP_CD") == "SUB", F.lit("1"))
        .otherwise(
            F.when(F.col("Extract.MBR_RELSHP_CD") == "SPOUSE", F.lit("2"))
            .otherwise(
                F.when(F.col("Extract.MBR_RELSHP_CD") == "DPNDT", F.lit("3")).otherwise(F.lit("99"))
            )
        ),
    )
    .withColumn(
        "svClmAdjFromClmId",
        F.when(F.col("Pca_Lookup.CLM_ID").isNull(), F.lit("")).otherwise(F.col("Extract.CLM_ADJ_FROM_CLM_ID")),
    )
    .withColumn(
        "tmpPcaHlth1",
        F.when(
            F.col("Cob_Clm_Lookup.CLM_SK").isNotNull(),
            F.when(
                F.col("svPcaCalcPatnRespAmt") == F.lit(0),
                F.when(
                    F.col("Pca_Lookup.CLM_ID").isNotNull(),
                    F.when(
                        (F.col("Extract.CLM_STTUS_CD") == "A02")
                        & (F.col("Extract.CLM_ADJ_FROM_CLM_ID") != F.lit("NA")),
                        F.lit(0),
                    ).otherwise(F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT")),
                ).otherwise(
                    F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT")
                ),
            ).otherwise(
                F.when(
                    F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT") == 0,
                    F.coalesce(F.col("Clm_Ln_Tot_Cnsd_Chrg_Amt_Lookup.CLM_LN_TOT_CNSD_CHRG_AMT"), F.lit(0)),
                ).otherwise(
                    F.when(
                        F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT")
                        >= F.col("svPcaCalcPatnRespAmt"),
                        F.col("svPcaCalcPatnRespAmt"),
                    ).otherwise(
                        F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT")
                        + F.coalesce(F.col("Clm_Ln_Tot_Cnsd_Chrg_Amt_Lookup.CLM_LN_TOT_CNSD_CHRG_AMT"), F.lit(0))
                    )
                ),
            ),
        ).otherwise(
            F.when(
                F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT")
                != F.col("svPcaCalcPatnRespAmt"),
                F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT")
                + F.coalesce(F.col("Clm_Ln_Tot_Cnsd_Chrg_Amt_Lookup.CLM_LN_TOT_CNSD_CHRG_AMT"), F.lit(0)),
            ).otherwise(
                F.col("Clm_Ln_Tot_Pca_Hlth_Rmbrmt_Acct_Amt_Lookup.CLM_LN_TOT_PCA_HLTH_RMBRMT_ACCT_AMT")
            ),
        ),
    )
    .withColumn("svPcaHlthRmbrmtAcctAmt", F.coalesce(F.col("tmpPcaHlth1"), F.lit(0)))
    .withColumn(
        "svNonCoveredCharges",
        F.col("Extract.CLM_LN_TOT_COINS_AMT") + F.col("Extract.CLM_LN_TOT_COPAY_AMT") + F.col("Extract.CLM_LN_TOT_DEDCT_AMT"),
    )
    .withColumn(
        "svPcaExclRsnCdRule1",
        F.when(
            (F.col("Extract.SUB_IN") != "Y") & (F.col("Extract.MBR_MAIL_ADDR_CONF_COMM_IN") == "Y"),
            F.lit("CONFCOMM"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCdRule2",
        F.when(
            (F.col("Pca_Rtn_Svc_Incld.CLM_SK").isNull())
            & (F.col("Pca_Rtn_Svc_Excld.CLM_SK").isNull())
            & (F.col("Pca_Rtn_Svc_Dup.CLM_SK").isNotNull())
            & (F.col("Pca_Rtn_Svc_X.CLM_SK").isNull()),
            F.lit("DUPCLM"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCdRule3",
        F.when(
            (F.col("Pca_Rtn_Svc_Incld.CLM_SK").isNull())
            & (F.col("Pca_Rtn_Svc_Excld.CLM_SK").isNotNull())
            & (F.col("Pca_Rtn_Svc_Dup.CLM_SK").isNull())
            & (F.col("Pca_Rtn_Svc_X.CLM_SK").isNull()),
            F.lit("SVCEXCL"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCdRule4",
        F.when(
            (F.col("Extract.CLM_ADJ_FROM_CLM_ID") == "NA")
            & (F.col("svPcaCalcPatnRespAmt") == 0)
            & (F.col("svNonCoveredCharges") == 0),
            F.lit("PATNRESPCHGZERO"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCdRule5",
        F.when(
            (F.col("Extract.CLM_ADJ_FROM_CLM_ID") != "NA")
            & (F.col("svPcaCalcPatnRespAmt") == 0)
            & (F.col("svNonCoveredCharges") == 0)
            & (F.col("Pca_Lookup.CLM_ID").isNull()),
            F.lit("PATNRESPCHGZERO"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCdRule6",
        F.when(
            (F.col("Extract.CLM_ADJ_FROM_CLM_ID") == "NA")
            & (F.col("Pca_Rtn_Svc_Incld.CLM_SK").isNull())
            & (F.col("svPcaCalcPatnRespAmt") != 0)
            & (F.col("svNonCoveredCharges") == 0),
            F.lit("CHGZERO"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCdRule9",
        F.when(
            (F.col("Extract.CLM_ADJ_FROM_CLM_ID") != "NA")
            & (F.col("Pca_Rtn_Svc_Incld.CLM_SK").isNull())
            & (F.col("svPcaCalcPatnRespAmt") != 0)
            & (F.col("svNonCoveredCharges") == 0)
            & (F.col("Pca_Lookup.CLM_ID").isNull()),
            F.lit("CHGZERO"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCdRule7",
        F.when(
            (F.col("Cob_Clm_Lookup.CLM_SK").isNotNull())
            & (F.col("Extract.CLM_ADJ_FROM_CLM_ID") == "NA")
            & (F.col("svPcaCalcPatnRespAmt") == 0)
            & (F.col("svNonCoveredCharges") != 0),
            F.lit("PATNRESPZERO"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCdRule8",
        F.when(
            (F.col("Cob_Clm_Lookup.CLM_SK").isNotNull())
            & (F.col("Extract.CLM_ADJ_FROM_CLM_ID") != "NA")
            & (F.col("svPcaCalcPatnRespAmt") == 0)
            & (F.col("svNonCoveredCharges") != 0)
            & (F.col("Pca_Lookup.CLM_ID").isNull()),
            F.lit("PATNRESPZERO"),
        ).otherwise(F.lit("NA")),
    )
    .withColumn(
        "svPcaExclRsnCd",
        F.when(F.col("svPcaExclRsnCdRule1") != "NA", F.col("svPcaExclRsnCdRule1"))
        .otherwise(
            F.when(F.col("svPcaExclRsnCdRule2") != "NA", F.col("svPcaExclRsnCdRule2"))
            .otherwise(
                F.when(F.col("svPcaExclRsnCdRule3") != "NA", F.col("svPcaExclRsnCdRule3"))
                .otherwise(
                    F.when(F.col("svPcaExclRsnCdRule4") != "NA", F.col("svPcaExclRsnCdRule4"))
                    .otherwise(
                        F.when(F.col("svPcaExclRsnCdRule5") != "NA", F.col("svPcaExclRsnCdRule5"))
                        .otherwise(
                            F.when(F.col("svPcaExclRsnCdRule6") != "NA", F.col("svPcaExclRsnCdRule6"))
                            .otherwise(
                                F.when(F.col("svPcaExclRsnCdRule9") != "NA", F.col("svPcaExclRsnCdRule9"))
                                .otherwise(
                                    F.when(F.col("svPcaExclRsnCdRule7") != "NA", F.col("svPcaExclRsnCdRule7"))
                                    .otherwise(
                                        F.when(F.col("svPcaExclRsnCdRule8") != "NA", F.col("svPcaExclRsnCdRule8"))
                                        .otherwise(F.lit("NA"))
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ),
    )
    .withColumn(
        "svPcaExclRsnIn",
        F.when(F.col("svPcaExclRsnCd") == "NA", F.lit("N")).otherwise(F.lit("Y")),
    )
    .withColumn("svFilter", F.lit(False))
)

df_Final = df_BusinessRules.select(
    F.col("Extract.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Extract.CLM_ID").alias("CLM_ID"),
    F.lit("01").alias("PCA_LN_NO"),
    F.col("svPcaSttusCd").alias("PCA_STTUS_CD"),
    F.col("CurrRunDate").alias("PCA_EXTR_DT"),
    F.lit(None).cast(StringType()).alias("PCA_EXTR_TM"),
    F.col("Extract.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("svPcaSubCatDesc").alias("PCA_SUB_CAT_DESC"),
    F.col("Extract.CLM_SVC_STRT_DT_SK").cast(StringType()).alias("CLM_SVC_STRT_DT"),
    F.col("Extract.CLM_SVC_END_DT_SK").cast(StringType()).alias("CLM_SVC_END_DT"),
    F.col("Diag_Cd_Lookup.DIAG_CD").alias("DIAG_CD_1"),
    F.col("svPcaPosCd").alias("PCA_POS_CD"),
    F.col("Extract.SVC_PROV_SK").alias("SVC_PROV_SK"),
    F.when(F.length(F.trim(F.col("Prov_Lookup.PROV_ID"))) == 0, F.lit("UNK"))
    .otherwise(F.trim(F.col("Prov_Lookup.PROV_ID")))
    .alias("SVC_PROV_ID"),
    F.col("svSvcProvTaxIdRmNull").alias("SVC_PROV_TAX_ID"),
    F.col("svSvcProvNm").alias("SVC_PROV_NM"),
    F.col("svProvPriPrctcAddrLn1").alias("PROV_PRI_PRCTC_ADDR_LN_1"),
    F.col("svProvPriPrctcAddrLn2").alias("PROV_PRI_PRCTC_ADDR_LN_2"),
    F.col("svProvPriPrctcAddrCityNm").alias("PROV_PRI_PRCTC_ADDR_CITY_NM"),
    F.col("svProvPriPrctcAddrStCdRmNull").alias("PROV_PRI_PRCTC_ADDR_ST_CD"),
    F.col("svProvPriPrctcAddrZipCd5").alias("PROV_PRI_PRCTC_ADDR_ZIP_CD_5"),
    F.col("svProvPriPrctcAddrZipCd4").alias("PROV_PRI_PRCTC_ADDR_ZIP_CD_4"),
    F.col("Extract.MBR_FIRST_NM").alias("MBR_FIRST_NM"),
    F.col("Extract.MBR_LAST_NM").alias("MBR_LAST_NM"),
    F.col("Extract.MBR_BRTH_DT_SK").cast(StringType()).alias("MBR_BRTH_DT"),
    F.col("svPcaMbrGndrCd").alias("PCA_MBR_GNDR_CD"),
    F.col("Extract.SUB_FIRST_NM").alias("SUB_FIRST_NM"),
    F.col("Extract.SUB_LAST_NM").alias("SUB_LAST_NM"),
    F.coalesce(F.col("Clm_Ln_Tot_Chrg_Amt_Lookup.CLM_LN_TOT_CHRG_AMT"), F.lit(0)).alias("CLM_LN_TOT_CHRG_AMT"),
    F.col("Extract.CLM_IN_NTWK_IN").alias("CLM_IN_NTWK_IN"),
    F.lit("").alias("PCA_REMARK_CD"),
    F.coalesce(F.col("Clm_Ln_Tot_Dedct_Amt_Lookup.CLM_LN_TOT_DEDCT_AMT"), F.lit(0)).alias("CLM_LN_TOT_DEDCT_AMT"),
    F.coalesce(F.col("Clm_Ln_Tot_Copay_Amt_Lookup.CLM_LN_TOT_COPAY_AMT"), F.lit(0)).alias("CLM_LN_TOT_COPAY_AMT"),
    F.coalesce(F.col("Clm_Ln_Tot_Coins_Amt_Lookup.CLM_LN_TOT_COINS_AMT"), F.lit(0)).alias("CLM_LN_TOT_COINS_AMT"),
    F.col("svPcaCalcPatnRespAmt").alias("PCA_CALC_PATN_RESP_AMT"),
    F.col("svPcaMbrRelshpCd").alias("PCA_MBR_RELSHP_CD"),
    F.col("Extract.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("Extract.GRP_ID").alias("GRP_ID"),
    F.coalesce(F.col("Sub_Id_Hist_Lookup.SUB_ID"), F.lit(0)).alias("SUB_ID"),
    F.col("Extract.CLM_MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("svClmAdjFromClmId").alias("CLM_ADJ_FROM_CLM_ID"),
    F.col("Extract.CLM_ADJ_TO_CLM_ID").alias("CLM_ADJ_TO_CLM_ID"),
    F.coalesce(F.col("svPcaHlthRmbrmtAcctAmt"), F.lit(0)).alias("PCA_HLTH_RMBRMT_ACCT_AMT"),
    F.col("svPcaExclRsnIn").alias("PCA_EXCL_IN"),
    F.col("svPcaExclRsnCd").alias("PCA_EXCL_RSN_CD"),
)

df_Final_padded = df_Final.select(
    F.col("SRC_SYS_CD"),
    F.col("CLM_ID"),
    F.rpad(F.col("PCA_LN_NO"), 2, " ").alias("PCA_LN_NO"),
    F.col("PCA_STTUS_CD"),
    F.rpad(F.col("PCA_EXTR_DT"), 10, " ").alias("PCA_EXTR_DT"),
    F.rpad(F.col("PCA_EXTR_TM"), 6, " ").alias("PCA_EXTR_TM"),
    F.col("CLM_SUBTYP_CD"),
    F.col("PCA_SUB_CAT_DESC"),
    F.rpad(F.col("CLM_SVC_STRT_DT"), 10, " ").alias("CLM_SVC_STRT_DT"),
    F.rpad(F.col("CLM_SVC_END_DT"), 10, " ").alias("CLM_SVC_END_DT"),
    F.col("DIAG_CD_1"),
    F.col("PCA_POS_CD"),
    F.col("SVC_PROV_SK"),
    F.col("SVC_PROV_ID"),
    F.col("SVC_PROV_TAX_ID"),
    F.col("SVC_PROV_NM"),
    F.col("PROV_PRI_PRCTC_ADDR_LN_1"),
    F.col("PROV_PRI_PRCTC_ADDR_LN_2"),
    F.col("PROV_PRI_PRCTC_ADDR_CITY_NM"),
    F.col("PROV_PRI_PRCTC_ADDR_ST_CD"),
    F.rpad(F.col("PROV_PRI_PRCTC_ADDR_ZIP_CD_5"), 5, " ").alias("PROV_PRI_PRCTC_ADDR_ZIP_CD_5"),
    F.rpad(F.col("PROV_PRI_PRCTC_ADDR_ZIP_CD_4"), 4, " ").alias("PROV_PRI_PRCTC_ADDR_ZIP_CD_4"),
    F.col("MBR_FIRST_NM"),
    F.col("MBR_LAST_NM"),
    F.rpad(F.col("MBR_BRTH_DT"), 20, " ").alias("MBR_BRTH_DT"),
    F.col("PCA_MBR_GNDR_CD"),
    F.col("SUB_FIRST_NM"),
    F.col("SUB_LAST_NM"),
    F.col("CLM_LN_TOT_CHRG_AMT"),
    F.rpad(F.col("CLM_IN_NTWK_IN"), 1, " ").alias("CLM_IN_NTWK_IN"),
    F.lit("").alias("PCA_REMARK_CD"),
    F.col("CLM_LN_TOT_DEDCT_AMT"),
    F.col("CLM_LN_TOT_COPAY_AMT"),
    F.col("CLM_LN_TOT_COINS_AMT"),
    F.col("PCA_CALC_PATN_RESP_AMT"),
    F.col("PCA_MBR_RELSHP_CD"),
    F.col("MBR_UNIQ_KEY"),
    F.col("GRP_ID"),
    F.col("SUB_ID"),
    F.col("MBR_SFX_NO"),
    F.col("CLM_ADJ_FROM_CLM_ID"),
    F.col("CLM_ADJ_TO_CLM_ID"),
    F.col("PCA_HLTH_RMBRMT_ACCT_AMT"),
    F.rpad(F.col("PCA_EXCL_IN"), 1, " ").alias("PCA_EXCL_IN"),
    F.col("PCA_EXCL_RSN_CD"),
)

write_files(
    df_Final_padded,
    f"{adls_path}/load/P_PCA.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)