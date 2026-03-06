# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  ESIDrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Reads the ESIDrugFile.dat created in  ESIClmLand  job and puts the data into the claim  provider common record format and runs through primary key using Shared container ClmProvPkey
# MAGIC     
# MAGIC 8;hf_esi_clm_mbr_pcp;hf_esi_clm_hmo_mbr_pcp;hf_esi_clm_prov_lkup;hf_esi_clm_provnpi_lkup;hf_esi_min_dea_prov;hf_esi_prov_dea_lkup;hf_esi_prscrb_prov_npi_lkup;hf_min_esi_clm_prov_prscrb
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Parikshith Chada     2008-07-08       3784                      Original Programming                                                                   devlIDSnew                    Steph Goddard           10/27/2008
# MAGIC                                                                                        Update with new primary key process.
# MAGIC Tracy Davis            2009-04-09       TTR 477                New Field CLM_TRANSMITTAL_METH                                     devlIDS                           Steph Goddard           04/14/2009
# MAGIC 
# MAGIC Dan Long               4/19/2013        TTR-1492              Changed the Performance parameters to the new                       IntegrateNewDevl          Bhoomi Dasari              4/30/2013
# MAGIC                                                                                       standard values of 512 for the buffer size and 300
# MAGIC                                                                                       for the timeout value. No code logic                  
# MAGIC                                                                                        was modified.            
# MAGIC Dan Long                2014-04-18      5082                       Changed lenghth of CLM_ID go into
# MAGIC                                                                                        contaner ClmProvPK from 18 to 20 and                                       IntegrateNewDevl     
# MAGIC                                                                                        type to VAR, Changed CLM_PROV_ROLE_TYPE_CD
# MAGIC                                                                                        length to 20.
# MAGIC                                                                                        Changed PROV_ID type to VARCHAR.
# MAGIC 
# MAGIC Manasa Andru          2016-04-18    TFS - 12505            Modified the extract SQL in the DeaNo and MinDea links by      IntegrateDev2               Kalyan Neelam            2016-04-20
# MAGIC                                                                                           removing the join against the CMN_PRCT table.
# MAGIC 
# MAGIC Madhavan B	2017-06-20   5788 - BHI Updates    Added new column                                                                    IntegrateDev2               Kalyan Neelam            2017-07-07
# MAGIC                                                                                         SVC_FCLTY_LOC_NTNL_PROV_ID
# MAGIC 
# MAGIC Abhiram Dasarathy	2017-12-01    BREAKFIX	       Added logic to exclude CMN_PRCTN_SK = 0 from the 	          IntegrateDev2
# MAGIC 					       Dea extracts
# MAGIC 
# MAGIC Ravi Abburi             2018-01-29    5781 - HEDIS            Added the NTNL_PROV_ID                                                     IntegrateDev2                Kalyan Neelam            2018-04-23
# MAGIC 
# MAGIC Shashank Akinapalli    2019-05-09    97615                 Adding CLM_LN_VBB_IN & adjusting the Filler length.            IntegrateDev2                    Kalyan Neelam            2019-05-10
# MAGIC Shashank Akinapalli   2019-07-22    137919              Adding constraint to transformer to capture missing 
# MAGIC                                                                                                  prescriber records from source file                                         IntegrateDev1          Kalyan Neelam            2019-07-24

# MAGIC Read the ESI file created from ESIClmLand
# MAGIC Driver Table data created in ESIClmDailyLand
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to /key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType
)
from pyspark.sql.functions import (
    col, when, length, lit, upper, concat, expr, rpad, trim as pyspark_trim
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
SrcSysCd = get_widget_value('SrcSysCd','ESI')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1320918365')
CurrentDateParam = get_widget_value('CurrentDate','2007-09-11')
RunID = get_widget_value('RunID','20190729104204')
RunCycle = get_widget_value('RunCycle','4688')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# 1) Read from ProvExtr (DB2Connector)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

# Pin: ProvId => hf_esi_clm_prov_lkup
extract_query_ProvId = f"""
SELECT
PROV.PROV_ID,
PROV.TAX_ID
FROM {IDSOwner}.PROV PROV
"""
df_ProvExtr_ProvId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_ProvId.strip())
    .load()
)

# Pin: NtnlProvId => hf_esi_clm_provnpi_lkup
extract_query_NtnlProvId = f"""
SELECT
PROV.NTNL_PROV_ID,
PROV.TAX_ID,
PROV.PROV_ID,
PROV.TERM_DT_SK
FROM {IDSOwner}.PROV PROV
"""
df_ProvExtr_NtnlProvId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_NtnlProvId.strip())
    .load()
)

# Pin: DeaNo => hf_esi_prov_dea_lkup
extract_query_DeaNo = f"""
SELECT
DEA.DEA_NO,
PROV.PROV_ID,
PROV.TAX_ID
FROM {IDSOwner}.PROV_DEA DEA,
     {IDSOwner}.PROV PROV
WHERE DEA.CMN_PRCT_SK = PROV.CMN_PRCT_SK
  AND PROV.CMN_PRCT_SK NOT IN (0,1)
"""
df_ProvExtr_DeaNo = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_DeaNo.strip())
    .load()
)

# Pin: PrscrbProv => hf_esi_prscrb_prov_npi_lkup
extract_query_PrscrbProv = f"""
SELECT
PROV.NTNL_PROV_ID,
PROV.TAX_ID,
PROV.PROV_ID
FROM {IDSOwner}.PROV PROV
"""
df_ProvExtr_PrscrbProv = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_PrscrbProv.strip())
    .load()
)

# Pin: MinProvId => used downstream in BusinessRules (parameterized in DS). 
# Per instructions, replace "WHERE ...=?" logic with a full read + group.
extract_query_MinProvId = f"""
SELECT
PROV.NTNL_PROV_ID,
PROV.TERM_DT_SK,
MIN(PROV.PROV_ID) AS PROV_ID
FROM {IDSOwner}.PROV PROV
GROUP BY PROV.NTNL_PROV_ID, PROV.TERM_DT_SK
"""
df_ProvExtr_MinProvId = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MinProvId.strip())
    .load()
)

# Pin: MinDea => used downstream in BusinessRules
extract_query_MinDea = f"""
SELECT
DEA.DEA_NO,
MIN(PROV.PROV_ID) AS PROV_ID
FROM {IDSOwner}.PROV_DEA DEA,
     {IDSOwner}.PROV PROV
WHERE DEA.CMN_PRCT_SK = PROV.CMN_PRCT_SK
  AND PROV.CMN_PRCT_SK NOT IN (0,1)
GROUP BY DEA.DEA_NO
"""
df_ProvExtr_MinDea = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MinDea.strip())
    .load()
)

# Pin: MinPrscrb => used downstream in BusinessRules
extract_query_MinPrscrb = f"""
SELECT
PROV.NTNL_PROV_ID,
MIN(PROV.PROV_ID) AS PROV_ID
FROM {IDSOwner}.PROV PROV
GROUP BY PROV.NTNL_PROV_ID
"""
df_ProvExtr_MinPrscrb = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MinPrscrb.strip())
    .load()
)

# Pin: NPI => hf_esiprov_providlkup
extract_query_NPI = f"""
SELECT
PROV.PROV_ID,
PROV.NTNL_PROV_ID
FROM {IDSOwner}.PROV PROV,
     {IDSOwner}.CD_MPPNG CDMP
WHERE CDMP.CD_MPPNG_SK = PROV.SRC_SYS_CD_SK
  AND SRC_CD = 'NABP'
"""
df_ProvExtr_NPI = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_NPI.strip())
    .load()
)

# Pin: DSLink156 => hf_esiprov_dea_npi_lkup
extract_query_DSLink156 = f"""
SELECT
DEA.DEA_NO,
DEA.NTNL_PROV_ID
FROM {IDSOwner}.PROV_DEA DEA
"""
df_ProvExtr_DSLink156 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_DSLink156.strip())
    .load()
)

# Deduplicate the hashed files (Scenario A) for each
# hf_esi_clm_prov_lkup => key PROV_ID
df_ProvIdLkup = dedup_sort(df_ProvExtr_ProvId, ["PROV_ID"], [])
df_ProvIdLkup = df_ProvIdLkup.select(
    "PROV_ID",
    "TAX_ID"
)

# hf_esi_clm_provnpi_lkup => key NTNL_PROV_ID
df_NtnlProvIdLkup = dedup_sort(df_ProvExtr_NtnlProvId, ["NTNL_PROV_ID"], [])
df_NtnlProvIdLkup = df_NtnlProvIdLkup.select(
    "NTNL_PROV_ID",
    "TAX_ID",
    "PROV_ID",
    "TERM_DT_SK"
)

# hf_esi_prov_dea_lkup => key DEA_NO
df_DeaNbrLkup = dedup_sort(df_ProvExtr_DeaNo, ["DEA_NO"], [])
df_DeaNbrLkup = df_DeaNbrLkup.select(
    "DEA_NO",
    "PROV_ID",
    "TAX_ID"
)

# hf_esi_prscrb_prov_npi_lkup => key NTNL_PROV_ID
df_PrscrbProvLkup = dedup_sort(df_ProvExtr_PrscrbProv, ["NTNL_PROV_ID"], [])
df_PrscrbProvLkup = df_PrscrbProvLkup.select(
    "NTNL_PROV_ID",
    "TAX_ID",
    "PROV_ID"
)

# hf_esi_min_dea_prov => key DEA_NO
df_MinDeaLkup = dedup_sort(df_ProvExtr_MinDea, ["DEA_NO"], [])
df_MinDeaLkup = df_MinDeaLkup.select(
    "DEA_NO",
    "PROV_ID"
)

# hf_min_esi_clm_prov_prscrb => key NTNL_PROV_ID
df_MinPrscrbLkup = dedup_sort(df_ProvExtr_MinPrscrb, ["NTNL_PROV_ID"], [])
df_MinPrscrbLkup = df_MinPrscrbLkup.select(
    "NTNL_PROV_ID",
    "PROV_ID"
)

# hf_esiprov_providlkup => key PROV_ID
df_NPI_lkup = dedup_sort(df_ProvExtr_NPI, ["PROV_ID"], [])
df_NPI_lkup = df_NPI_lkup.select(
    "PROV_ID",
    "NTNL_PROV_ID"
)

# hf_esiprov_dea_npi_lkup => key DEA_NO
df_DEA_NPI_lkup = dedup_sort(df_ProvExtr_DSLink156, ["DEA_NO"], [])
df_DEA_NPI_lkup = df_DEA_NPI_lkup.select(
    "DEA_NO",
    "NTNL_PROV_ID"
)

# MinProvId is an aggregated read => no hashed file in between; used in BusinessRules as a left join
df_MinProvId = df_ProvExtr_MinProvId.select(
    "NTNL_PROV_ID",
    "TERM_DT_SK",
    "PROV_ID"
)

# 2) MbrHmoExtr (DB2Connector)
extract_query_HmoExtr = f"""
SELECT
DRVR.MBR_UNIQ_KEY,
DRVR.DRUG_FILL_DT_SK,
MAX(MBR_ENR.EFF_DT_SK) AS EFF_DT_SK
FROM {IDSOwner}.W_DRUG_CLM_PCP DRVR,
     {IDSOwner}.MBR_ENR MBR_ENR,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.PROD PROD,
     {IDSOwner}.PROD_SH_NM PRODSH,
     {IDSOwner}.CD_MPPNG MAP2
WHERE DRVR.MBR_UNIQ_KEY = MBR_ENR.MBR_UNIQ_KEY
  AND MBR_ENR.SRC_SYS_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD='FACETS'
  AND DRVR.DRUG_FILL_DT_SK BETWEEN MBR_ENR.EFF_DT_SK AND MBR_ENR.TERM_DT_SK
  AND MBR_ENR.ELIG_IN='Y'
  AND MBR_ENR.PROD_SK=PROD.PROD_SK
  AND PROD.PROD_SH_NM_SK=PRODSH.PROD_SH_NM_SK
  AND PRODSH.PROD_SH_NM_DLVRY_METH_CD_SK=MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD='HMO'
GROUP BY
DRVR.MBR_UNIQ_KEY,
DRVR.DRUG_FILL_DT_SK
"""
df_MbrHmoExtr_HmoExtr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_HmoExtr.strip())
    .load()
)

extract_query_MbrPcpEffDt = f"""
SELECT
DRVR.MBR_UNIQ_KEY,
DRVR.DRUG_FILL_DT_SK,
MAX(MBR_PCP.EFF_DT_SK) AS EFF_DT_SK,
PROV.PROV_ID,
PROV.TAX_ID,
PROV.NTNL_PROV_ID,
MPPNG.TRGT_CD
FROM {IDSOwner}.W_DRUG_CLM_PCP DRVR,
     {IDSOwner}.MBR_PCP MBR_PCP,
     {IDSOwner}.CD_MPPNG MPPNG,
     {IDSOwner}.PROV PROV
WHERE DRVR.MBR_UNIQ_KEY=MBR_PCP.MBR_UNIQ_KEY
  AND DRVR.DRUG_FILL_DT_SK BETWEEN MBR_PCP.EFF_DT_SK AND MBR_PCP.TERM_DT_SK
  AND MBR_PCP.MBR_PCP_TYP_CD_SK=MPPNG.CD_MPPNG_SK
  AND MBR_PCP.PROV_SK=PROV.PROV_SK
GROUP BY
DRVR.MBR_UNIQ_KEY,
DRVR.DRUG_FILL_DT_SK,
PROV.PROV_ID,
PROV.TAX_ID,
PROV.NTNL_PROV_ID,
MPPNG.TRGT_CD
"""
df_MbrHmoExtr_MbrPcpEffDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MbrPcpEffDt.strip())
    .load()
)

# hf_esi_clm_mbr_pcp => scenario A deduplicate => key (MBR_UNIQ_KEY, ARGUS_FILL_DT_SK)
df_MbrPcpEffDt = df_MbrHmoExtr_MbrPcpEffDt.select(
    col("MBR_UNIQ_KEY"),
    col("DRUG_FILL_DT_SK").alias("ARGUS_FILL_DT_SK"),
    col("EFF_DT_SK"),
    col("PROV_ID"),
    col("TAX_ID"),
    col("NTNL_PROV_ID"),
    col("TRGT_CD")
)
df_MbrPcpLkup = dedup_sort(df_MbrPcpEffDt, ["MBR_UNIQ_KEY","ARGUS_FILL_DT_SK"], [])
df_MbrPcpLkup = df_MbrPcpLkup.select(
    "MBR_UNIQ_KEY",
    "ARGUS_FILL_DT_SK",
    "EFF_DT_SK",
    "PROV_ID",
    "TAX_ID",
    "NTNL_PROV_ID",
    "TRGT_CD"
)

# 3) ESIClmLand => CSeqFileStage => read delimited file
schema_ESIClmLand = StructType([
    StructField("RCRD_ID", IntegerType(), False),
    StructField("CLAIM_ID", StringType(), False),
    StructField("PRCSR_NO", IntegerType(), False),
    StructField("MEM_CK_KEY", IntegerType(), False),
    StructField("BTCH_NO", IntegerType(), False),
    StructField("PDX_NO", StringType(), False),
    StructField("RX_NO", IntegerType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("NDC_NO", IntegerType(), False),
    StructField("DRUG_DESC", StringType(), False),
    StructField("NEW_RFL_CD", IntegerType(), False),
    StructField("METRIC_QTY", IntegerType(), False),
    StructField("DAYS_SUPL", IntegerType(), False),
    StructField("BSS_OF_CST_DTRM", StringType(), False),
    StructField("INGR_CST", IntegerType(), False),
    StructField("DISPNS_FEE", IntegerType(), False),
    StructField("COPAY_AMT", IntegerType(), False),
    StructField("SLS_TAX", IntegerType(), False),
    StructField("AMT_BILL", IntegerType(), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("DOB", StringType(), False),
    StructField("SEX_CD", IntegerType(), False),
    StructField("CARDHLDR_ID_NO", StringType(), False),
    StructField("RELSHP_CD", IntegerType(), False),
    StructField("GRP_NO", StringType(), False),
    StructField("HOME_PLN", StringType(), False),
    StructField("HOST_PLN", IntegerType(), False),
    StructField("PRESCRIBER_ID", StringType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("CARDHLDR_FIRST_NM", StringType(), False),
    StructField("CARDHLDR_LAST_NM", StringType(), False),
    StructField("PRAUTH_NO", IntegerType(), False),
    StructField("PA_MC_SC_NO", StringType(), False),
    StructField("CUST_LOC", IntegerType(), False),
    StructField("RESUB_CYC_CT", IntegerType(), False),
    StructField("DT_RX_WRTN", StringType(), False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), False),
    StructField("PRSN_CD", StringType(), False),
    StructField("OTHR_COV_CD", IntegerType(), False),
    StructField("ELIG_CLRFCTN_CD", IntegerType(), False),
    StructField("CMPND_CD", IntegerType(), False),
    StructField("NO_OF_RFLS_AUTH", IntegerType(), False),
    StructField("LVL_OF_SVC", IntegerType(), False),
    StructField("RX_ORIG_CD", IntegerType(), False),
    StructField("RX_DENIAL_CLRFCTN", IntegerType(), False),
    StructField("PRI_PRESCRIBER", StringType(), False),
    StructField("CLNC_ID_NO", IntegerType(), False),
    StructField("DRUG_TYP", IntegerType(), False),
    StructField("PRESCRIBER_LAST_NM", StringType(), False),
    StructField("POSTAGE_AMT_CLMED", IntegerType(), False),
    StructField("UNIT_DOSE_IN", IntegerType(), False),
    StructField("OTHR_PAYOR_AMT", IntegerType(), False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", IntegerType(), False),
    StructField("FULL_AWP", IntegerType(), False),
    StructField("EXPNSN_AREA", StringType(), False),
    StructField("MSTR_CAR", StringType(), False),
    StructField("SUB_CAR", StringType(), False),
    StructField("CLM_TYP", StringType(), False),
    StructField("ESI_SUB_GRP", StringType(), False),
    StructField("PLN_DSGNR", StringType(), False),
    StructField("ADJDCT_DT", StringType(), False),
    StructField("ADMIN_FEE", IntegerType(), False),
    StructField("CAP_AMT", IntegerType(), False),
    StructField("INGR_CST_SUB", IntegerType(), False),
    StructField("MBR_NON_COPAY_AMT", IntegerType(), False),
    StructField("MBR_PAY_CD", StringType(), False),
    StructField("INCNTV_FEE", IntegerType(), False),
    StructField("CLM_ADJ_AMT", IntegerType(), False),
    StructField("CLM_ADJ_CD", StringType(), False),
    StructField("FRMLRY_FLAG", StringType(), False),
    StructField("GNRC_CLS_NO", StringType(), False),
    StructField("THRPTC_CLS_AHFS", StringType(), False),
    StructField("PDX_TYP", StringType(), False),
    StructField("BILL_BSS_CD", StringType(), False),
    StructField("USL_AND_CUST_CHRG", IntegerType(), False),
    StructField("PD_DT", StringType(), False),
    StructField("BNF_CD", StringType(), False),
    StructField("DRUG_STRG", StringType(), False),
    StructField("ORIG_MBR", StringType(), False),
    StructField("DT_OF_INJURY", StringType(), False),
    StructField("FEE_AMT", IntegerType(), False),
    StructField("ESI_REF_NO", StringType(), False),
    StructField("CLNT_CUST_ID", StringType(), False),
    StructField("PLN_TYP", StringType(), False),
    StructField("ESI_ADJDCT_REF_NO", IntegerType(), False),
    StructField("ESI_ANCLRY_AMT", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PAID_DATE", StringType(), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("ESI_BILL_DT", StringType(), False),
    StructField("FSA_VNDR_CD", StringType(), False),
    StructField("PICA_DRUG_CD", StringType(), False),
    StructField("AMT_CLMED", IntegerType(), False),
    StructField("AMT_DSALW", IntegerType(), False),
    StructField("FED_DRUG_CLS_CD", StringType(), False),
    StructField("DEDCT_AMT", IntegerType(), False),
    StructField("BNF_COPAY_100", StringType(), False),
    StructField("CLM_PRCS_TYP", StringType(), False),
    StructField("INDEM_HIER_TIER_NO", IntegerType(), False),
    StructField("FLR", StringType(), False),
    StructField("MCARE_D_COV_DRUG", StringType(), False),
    StructField("RETRO_LICS_CD", StringType(), False),
    StructField("RETRO_LICS_AMT", IntegerType(), False),
    StructField("LICS_SBSDY_AMT", IntegerType(), False),
    StructField("MED_B_DRUG", StringType(), False),
    StructField("MED_B_CLM", StringType(), False),
    StructField("PRESCRIBER_QLFR", StringType(), False),
    StructField("PRESCRIBER_ID_NPI", StringType(), False),
    StructField("PDX_QLFR", StringType(), False),
    StructField("PDX_ID_NPI", StringType(), False),
    StructField("HRA_APLD_AMT", IntegerType(), False),
    StructField("ESI_THER_CLS", IntegerType(), False),
    StructField("HIC_NO", StringType(), False),
    StructField("HRA_FLAG", StringType(), False),
    StructField("DOSE_CD", IntegerType(), False),
    StructField("LOW_INCM", StringType(), False),
    StructField("RTE_OF_ADMIN", StringType(), False),
    StructField("DEA_SCHD", IntegerType(), False),
    StructField("COPAY_BNF_OPT", IntegerType(), False),
    StructField("GNRC_PROD_IN_GPI", IntegerType(), False),
    StructField("PRESCRIBER_SPEC", StringType(), False),
    StructField("VAL_CD", StringType(), False),
    StructField("PRI_CARE_PDX", StringType(), False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), False),
    StructField("FLR3", StringType(), False),
    StructField("PSL_FMLY_MET_AMT", IntegerType(), False),
    StructField("PSL_MBR_MET_AMT", IntegerType(), False),
    StructField("PSL_FMLY_AMT", IntegerType(), False),
    StructField("DED_FMLY_MET_AMT", IntegerType(), False),
    StructField("DED_FMLY_AMT", IntegerType(), False),
    StructField("MOPS_FMLY_AMT", IntegerType(), False),
    StructField("MOPS_FMLY_MET_AMT", IntegerType(), False),
    StructField("MOPS_MBR_MET_AMT", IntegerType(), False),
    StructField("DED_MBR_MET_AMT", IntegerType(), False),
    StructField("PSL_APLD_AMT", IntegerType(), False),
    StructField("MOPS_APLD_AMT", IntegerType(), False),
    StructField("PAR_PDX_IND", StringType(), False),
    StructField("COPAY_PCT_AMT", IntegerType(), False),
    StructField("COPAY_FLAT_AMT", IntegerType(), False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), False),
    StructField("PRESCRIPTION_NBR_2", StringType(), False),
    StructField("TRANSACTION_ID", StringType(), False),
    StructField("CROSS_REF_ID", StringType(), False),
    StructField("ADJDCT_TIMESTAMP", StringType(), False),
    StructField("CLM_LN_VBB_IN", StringType(), False),
    StructField("FLR4", StringType(), False)
])

df_Extract = (
    spark.read.format("csv")
    .schema(schema_ESIClmLand)
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .load(f"{adls_path}/verified/ESIDrugClmDaily_Land.dat.{RunID}")
)

# 4) Transform stage with input:
# Primary: df_MbrHmoExtr_HmoExtr => alias HmoExtr
# Lookup: df_MbrPcpLkup => alias MbrPcpLkup  (left join on MBR_UNIQ_KEY, ARGUS_FILL_DT_SK, EFF_DT_SK)
# Output: PCP => hf_esi_clm_hmo_mbr_pcp
df_HmoExtr = df_MbrHmoExtr_HmoExtr.select(
    col("MBR_UNIQ_KEY"),
    col("DRUG_FILL_DT_SK").alias("ARGUS_FILL_DT_SK"),
    col("EFF_DT_SK")
).alias("HmoExtr")

df_MbrPcpLkup_alias = df_MbrPcpLkup.alias("MbrPcpLkup")

df_join_Transform = df_HmoExtr.join(
    df_MbrPcpLkup_alias,
    on=[
        col("HmoExtr.MBR_UNIQ_KEY")==col("MbrPcpLkup.MBR_UNIQ_KEY"),
        col("HmoExtr.ARGUS_FILL_DT_SK")==col("MbrPcpLkup.ARGUS_FILL_DT_SK"),
        col("HmoExtr.EFF_DT_SK")==col("MbrPcpLkup.EFF_DT_SK")
    ],
    how="left"
)

df_PCP = df_join_Transform.select(
    col("HmoExtr.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    col("HmoExtr.ARGUS_FILL_DT_SK"),
    when(
        (col("MbrPcpLkup.PROV_ID").isNull()) | (length(col("MbrPcpLkup.PROV_ID")) == 0),
        lit("UNK")
    ).otherwise(col("MbrPcpLkup.PROV_ID")).alias("PROV_ID"),
    when(
        (col("MbrPcpLkup.TAX_ID").isNull()) | (length(col("MbrPcpLkup.TAX_ID")) == 0),
        lit("NA")
    ).otherwise(col("MbrPcpLkup.TAX_ID")).alias("TAX_ID"),
    when(
        (col("MbrPcpLkup.PROV_ID") == lit("0")) | (col("MbrPcpLkup.PROV_ID") == lit("1")),
        col("MbrPcpLkup.PROV_ID")
    ).otherwise(
        when(
            (col("MbrPcpLkup.NTNL_PROV_ID").isNull()) | (length(col("MbrPcpLkup.NTNL_PROV_ID")) == 0)
             | (col("MbrPcpLkup.NTNL_PROV_ID") == lit("NA"))
             | (col("MbrPcpLkup.NTNL_PROV_ID") == lit("UNK")),
            lit("UNK")
        ).otherwise(col("MbrPcpLkup.NTNL_PROV_ID"))
    ).alias("NTNL_PROV_ID"),
    when(
        (col("MbrPcpLkup.TRGT_CD").isNull())
        | (length(col("MbrPcpLkup.TRGT_CD")) == 0)
        | (col("MbrPcpLkup.TRGT_CD") != lit("PRI")),
        lit("UNK")
    ).otherwise(lit("PCP")).alias("TRGT_CD")
).alias("PCP")

# Write out => hf_esi_clm_hmo_mbr_pcp => then re-read scenario A => deduplicate
df_hf_esi_clm_hmo_mbr_pcp = dedup_sort(
    df_PCP.select(
        col("MBR_UNIQ_KEY"),
        col("ARGUS_FILL_DT_SK"),
        col("PROV_ID"),
        col("TAX_ID"),
        col("NTNL_PROV_ID"),
        col("TRGT_CD")
    ),
    ["MBR_UNIQ_KEY", "ARGUS_FILL_DT_SK"],
    []
).alias("PCPLkup")

# 5) BusinessRules => big transformer with primary link = df_Extract => alias Extract
df_Extract_alias = df_Extract.alias("Extract")
df_ProvIdLkup_alias = df_ProvIdLkup.alias("ProvIdLkup")
df_NtnlProvIdLkup_alias = df_NtnlProvIdLkup.alias("NtnlProvIdLkup")
df_DeaNbrLkup_alias = df_DeaNbrLkup.alias("DeaNbrLkup")
df_PrscrbProvLkup_alias = df_PrscrbProvLkup.alias("PrscrbProvLkup")
df_MinProvId_alias = df_MinProvId.alias("MinProvId")
df_MinDeaLkup_alias = df_MinDeaLkup.alias("MinDeaLkup")
df_MinPrscrbLkup_alias = df_MinPrscrbLkup.alias("MinPrscrbLkup")
df_NPI_lkup_alias = df_NPI_lkup.alias("NPI_lkup")
df_DEA_NPI_lkup_alias = df_DEA_NPI_lkup.alias("DEA_NPI_lkup")
df_PCPLkup_alias = df_hf_esi_clm_hmo_mbr_pcp.alias("PCPLkup")

# Join them all with left joins in sequence
join_Br = df_Extract_alias \
    .join(
        df_PCPLkup_alias,
        on=[col("Extract.MEM_CK_KEY")==col("PCPLkup.MBR_UNIQ_KEY"),
            col("Extract.DT_FILLED")==col("PCPLkup.ARGUS_FILL_DT_SK")],
        how="left"
    ) \
    .join(
        df_ProvIdLkup_alias,
        on=[col("Extract.PDX_NO")==col("ProvIdLkup.PROV_ID")],
        how="left"
    ) \
    .join(
        df_NtnlProvIdLkup_alias,
        on=[col("Extract.PDX_ID_NPI")==col("NtnlProvIdLkup.NTNL_PROV_ID")],
        how="left"
    ) \
    .join(
        df_DeaNbrLkup_alias,
        on=[col("Extract.PRESCRIBER_ID")==col("DeaNbrLkup.DEA_NO")],
        how="left"
    ) \
    .join(
        df_PrscrbProvLkup_alias,
        on=[col("Extract.PRESCRIBER_ID_NPI")==col("PrscrbProvLkup.NTNL_PROV_ID")],
        how="left"
    ) \
    .join(
        df_MinProvId_alias,
        on=[
            col("Extract.PDX_ID_NPI")==col("MinProvId.NTNL_PROV_ID"),
            col("Extract.DT_FILLED")==col("MinProvId.TERM_DT_SK")
        ],
        how="left"
    ) \
    .join(
        df_MinDeaLkup_alias,
        on=[col("Extract.PRESCRIBER_ID")==col("MinDeaLkup.DEA_NO")],
        how="left"
    ) \
    .join(
        df_MinPrscrbLkup_alias,
        on=[col("Extract.PRESCRIBER_ID_NPI")==col("MinPrscrbLkup.NTNL_PROV_ID")],
        how="left"
    ) \
    .join(
        df_NPI_lkup_alias,
        on=[col("Extract.PDX_NO")==col("NPI_lkup.PROV_ID")],
        how="left"
    ) \
    .join(
        df_DEA_NPI_lkup_alias,
        on=[col("Extract.PRESCRIBER_ID")==col("DEA_NPI_lkup.DEA_NO")],
        how="left"
    )

# Stage variables in DS
# SvcProv = if IsNull(ProvIdLkup.PROV_ID) then Extract.PDX_NO else if IsNull(NtnlProvIdLkup.PROV_ID) then NtnlProvIdLkup.PROV_ID else 'UNK'
# Actually the job says:
#   " if IsNull(ProvIdLkup.PROV_ID) = @FALSE then Extract.PDX_NO
#     else if IsNull(NtnlProvIdLkup.PROV_ID) = @FALSE then NtnlProvIdLkup.PROV_ID
#     else 'UNK' "
svcprov_expr = when(
    (col("ProvIdLkup.PROV_ID").isNotNull()),
    col("Extract.PDX_NO")
).otherwise(
    when(
        col("NtnlProvIdLkup.PROV_ID").isNotNull(),
        col("NtnlProvIdLkup.PROV_ID")
    ).otherwise(lit("UNK"))
)

clmid_expr = col("Extract.CLAIM_ID")
# PkString = SrcSysCd : ";" : ClmId
# Then appended with "SVC","PCP","PRSCRB" etc. per link constraints
curdatetime_expr = current_date()  # StageVar "CurrentDate" => current_date()

# Build a single DataFrame with columns needed; then use filter for constraints
df_Br_base = join_Br.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),  # stage variable PassThru is 'Y'
    curdatetime_expr.alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    # PkString => (SrcSysCd || ";" || CLAIM_ID)
    (lit(SrcSysCd) \
     .concat(lit(";")) \
     .concat(clmid_expr)
    ).alias("BasePk"),
    lit(0).alias("CLM_PROV_SK"),
    clmid_expr.alias("CLM_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # For SVC
    svcprov_expr.alias("SVC_PROV_EX"),
    when(
        (col("ProvIdLkup.TAX_ID").isNotNull()),
        col("ProvIdLkup.TAX_ID")
    ).otherwise(
        when(
            (col("NtnlProvIdLkup.TAX_ID").isNotNull()) &
            (col("MinProvId.NTNL_PROV_ID").isNotNull()),
            col("NtnlProvIdLkup.TAX_ID")
        ).otherwise(lit("NA"))
    ).alias("SVC_TAX_EX"),
    when(
        (col("NPI_lkup.NTNL_PROV_ID").isNull()) | (col("NPI_lkup.NTNL_PROV_ID") == lit("NA")),
        when(col("Extract.PDX_ID_NPI").isNotNull(), col("Extract.PDX_ID_NPI")).otherwise(lit("NA"))
    ).otherwise(col("NPI_lkup.NTNL_PROV_ID")).alias("SVC_NTNL_EX"),
    # For PCP
    col("PCPLkup.PROV_ID").alias("PCP_PROV_EX"),
    col("PCPLkup.TAX_ID").alias("PCP_TAX_EX"),
    when(
        (pyspark_trim(col("PCPLkup.NTNL_PROV_ID"))==lit("")) | col("PCPLkup.NTNL_PROV_ID").isNull(),
        lit("UNK")
    ).otherwise(pyspark_trim(col("PCPLkup.NTNL_PROV_ID"))).alias("PCP_NTNL_EX"),
    # For PRSCRB
    when(
        (col("DeaNbrLkup.PROV_ID").isNotNull()) & (col("MinDeaLkup.PROV_ID").isNotNull()),
        col("DeaNbrLkup.PROV_ID")
    ).otherwise(
        when(
            (col("PrscrbProvLkup.PROV_ID").isNotNull()) & (col("MinPrscrbLkup.PROV_ID").isNotNull()),
            col("PrscrbProvLkup.PROV_ID")
        ).otherwise(lit("UNK"))
    ).alias("PRSCRB_PROV_EX"),
    when(
        (col("DeaNbrLkup.TAX_ID").isNotNull()) & (col("MinDeaLkup.DEA_NO").isNotNull()),
        col("DeaNbrLkup.TAX_ID")
    ).otherwise(
        when(
            (col("PrscrbProvLkup.TAX_ID").isNotNull()) & (col("MinPrscrbLkup.NTNL_PROV_ID").isNotNull()),
            col("PrscrbProvLkup.TAX_ID")
        ).otherwise(lit("UNK"))
    ).alias("PRSCRB_TAX_EX"),
    when(
        (col("DEA_NPI_lkup.NTNL_PROV_ID").isNull()) | (col("DEA_NPI_lkup.NTNL_PROV_ID")==lit("NA")),
        when(col("Extract.PRESCRIBER_ID_NPI").isNotNull(), col("Extract.PRESCRIBER_ID_NPI")).otherwise(lit("NA"))
    ).otherwise(col("DEA_NPI_lkup.NTNL_PROV_ID")).alias("PRSCRB_NTNL_EX"),
    # For missing prescriber
    col("Extract.PRESCRIBER_ID_NPI").alias("TEMP_PRESCRIBER_ID_NPI")
)

# Now produce 4 link outputs by applying DS constraints:

# 1) SVC => ( (Not(IsNull(SvcProv)) and len(SvcProv)>0 ) or (Not(IsNull(NtnlProvIdLkup.NTNL_PROV_ID)) and len(...)>0 ))
df_SVC = df_Br_base.filter(
    (
        (col("SVC_PROV_EX")!=lit("UNK")) & (col("SVC_PROV_EX").isNotNull()) & (length(col("SVC_PROV_EX"))>0)
    )
    |
    (
        (col("NtnlProvIdLkup.NTNL_PROV_ID").isNotNull()) & (length(col("NtnlProvIdLkup.NTNL_PROV_ID"))>0)
    )
).select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    (col("BasePk").concat(lit(";SVC"))).alias("PRI_KEY_STRING"),
    col("CLM_PROV_SK"),
    col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    lit("SVC").alias("CLM_PROV_ROLE_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("SVC_PROV_EX").alias("PROV_ID"),
    col("SVC_TAX_EX").alias("TAX_ID"),
    col("SVC_NTNL_EX").alias("NTNL_PROV_ID")
)

# 2) PCP => ( Not(IsNull(PCPLkup.MBR_UNIQ_KEY)) and Len(Trim(...))>0 )
df_PCP2 = df_Br_base.filter(
    (col("PCPLkup.MBR_UNIQ_KEY").isNotNull()) & (length(pyspark_trim(col("PCPLkup.MBR_UNIQ_KEY").cast(StringType())))>0)
).select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    (col("BasePk").concat(lit(";PCP"))).alias("PRI_KEY_STRING"),
    col("CLM_PROV_SK"),
    col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    lit("PCP").alias("CLM_PROV_ROLE_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PCP_PROV_EX").alias("PROV_ID"),
    col("PCP_TAX_EX").alias("TAX_ID"),
    col("PCP_NTNL_EX").alias("NTNL_PROV_ID")
)

# 3) PRSCRB => ( (DeaNbrLkup.DEA_NO) not null or (PrscrbProvLkup.NTNL_PROV_ID) not null )
df_PRSCRB = df_Br_base.filter(
    (
        (col("DeaNbrLkup.DEA_NO").isNotNull()) & (length(col("DeaNbrLkup.DEA_NO"))>0)
    )
    |
    (
        (col("PrscrbProvLkup.NTNL_PROV_ID").isNotNull()) & (length(col("PrscrbProvLkup.NTNL_PROV_ID"))>0)
    )
).select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    (col("BasePk").concat(lit(";PRSCRB"))).alias("PRI_KEY_STRING"),
    col("CLM_PROV_SK"),
    col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    lit("PRSCRB").alias("CLM_PROV_ROLE_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("PRSCRB_PROV_EX").alias("PROV_ID"),
    col("PRSCRB_TAX_EX").alias("TAX_ID"),
    col("PRSCRB_NTNL_EX").alias("NTNL_PROV_ID")
)

# 4) PRSCRB_MISSING => (both DEA_NO and NTNL_PROV_ID are null or length<=0)
df_PRSCRB_MISSING = df_Br_base.filter(
    (
        (col("DeaNbrLkup.DEA_NO").isNull()) | (length(col("DeaNbrLkup.DEA_NO"))<=0)
    )
    &
    (
        (col("PrscrbProvLkup.NTNL_PROV_ID").isNull()) | (length(col("PrscrbProvLkup.NTNL_PROV_ID"))<=0)
    )
).select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    (col("BasePk").concat(lit(";PRSCRB"))).alias("PRI_KEY_STRING"),
    col("CLM_PROV_SK"),
    col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    lit("PRSCRB").alias("CLM_PROV_ROLE_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(
        length(pyspark_trim(col("TEMP_PRESCRIBER_ID_NPI")))==0,
        lit("UNK")
    ).otherwise(col("TEMP_PRESCRIBER_ID_NPI")).alias("PROV_ID"),
    rpad(lit("UNK"), 9, " ").alias("TAX_ID"),
    when(
        length(pyspark_trim(col("TEMP_PRESCRIBER_ID_NPI")))==0,
        lit("UNK")
    ).otherwise(col("TEMP_PRESCRIBER_ID_NPI")).alias("NTNL_PROV_ID")
)

# 6) Link_Collector => union these 4 DataFrames
common_cols = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_PROV_SK",
    "CLM_ID",
    "CLM_PROV_ROLE_TYP_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROV_ID",
    "TAX_ID",
    "NTNL_PROV_ID"
]

df_SVC_sel = df_SVC.select(common_cols)
df_PCP2_sel = df_PCP2.select(common_cols)
df_PRSCRB_sel = df_PRSCRB.select(common_cols)
df_PRSCRB_MISSING_sel = df_PRSCRB_MISSING.select(common_cols)

df_Trans = df_SVC_sel.union(df_PCP2_sel).union(df_PRSCRB_sel).union(df_PRSCRB_MISSING_sel)

# 7) Snapshot => CTransformerStage
# Outputs: 
#    SnapShot => (CLM_ID char(18), CLM_PROV_ROLE_TYP_CD char(10), PROV_ID char(12))
#    AllCol => many columns
#    Transform => some columns

df_Snapshot = df_Trans.select(
    col("CLM_ID").alias("CLM_ID"),
    col("CLM_PROV_ROLE_TYP_CD").alias("CLM_PROV_ROLE_TYP_CD"),
    col("PROV_ID").alias("PROV_ID")
)

df_AllCol = df_Trans.select(
    col("SRC_SYS_CD_SK").alias("SrcSysCdSk"),  # to be introduced: since DS used param
    # The job uses "SrcSysCdSk" from parameter, so:
    # We attach it as a column:
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),   # as DS does
    col("CLM_ID"),
    col("CLM_PROV_ROLE_TYP_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_PROV_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    pyspark_trim(col("PROV_ID")).alias("PROV_ID"),
    col("TAX_ID"),
    lit("NA").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    when(col("NTNL_PROV_ID").isNull(), lit("UNK")).otherwise(col("NTNL_PROV_ID")).alias("NTNL_PROV_ID")
)

df_TransformOut = df_Trans.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_PROV_ROLE_TYP_CD")
)

# 8) Transformer => next stage with input df_Snapshot => Output => B_CLM_PROV
# Stage variable: svClmProvRole = GetFkeyCodes('ESI', 0, "CLAIM PROVIDER ROLE TYPE", SnapShot.CLM_PROV_ROLE_TYP_CD, 'X')
df_SnapShot_alias = df_Snapshot.alias("SnapShot")

df_RowCount = df_SnapShot_alias.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    rpad(col("SnapShot.CLM_ID"),18," ").alias("CLM_ID"),
    # ClmProvRole => user-defined function call:
    #   "GetFkeyCodes('ESI',0,'CLAIM PROVIDER ROLE TYPE', SnapShot.CLM_PROV_ROLE_TYP_CD, 'X')"
    # We assume it's replaced by a column expression:
    GetFkeyCodes('ESI', lit(0), lit("CLAIM PROVIDER ROLE TYPE"), col("SnapShot.CLM_PROV_ROLE_TYP_CD"), lit("X")
    ).alias("CLM_PROV_ROLE_TYP_CD_SK"),
    rpad(col("SnapShot.PROV_ID"),12," ").alias("PROV_ID")
).alias("RowCount")

# 9) B_CLM_PROV => CSeqFileStage => write to load/B_CLM_PROV.ESI.dat.#RunID#
# Final select with rpad for char or varchar
df_B_CLM_PROV = df_RowCount.select(
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),                              # char(18) => already rpad
    col("CLM_PROV_ROLE_TYP_CD_SK"),             # numeric => no rpad
    col("PROV_ID")                              # char(12) => already rpad
)
write_files(
    df_B_CLM_PROV,
    f"{adls_path}/load/B_CLM_PROV.ESI.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 10) ClmProvPK => a Shared Container => two inputs => we pass df_AllCol, df_TransformOut => one output => df_Key
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmProvPK
# COMMAND ----------
params_ClmProvPK = {
    "CurrRunCycle": RunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_Key = ClmProvPK(df_AllCol, df_TransformOut, params_ClmProvPK)

# 11) ESIClmProvExtr => CSeqFileStage => write to key/ESIClmProvExtr.DrugClmProv.dat.#RunID#
# The final columns are in the order from the JSON:
df_ESIClmProvExtr = df_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_PROV_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    rpad(col("CLM_PROV_ROLE_TYP_CD"),10," ").alias("CLM_PROV_ROLE_TYP_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("PROV_ID"),12," ").alias("PROV_ID"),
    rpad(col("TAX_ID"),9," ").alias("TAX_ID"),
    col("SVC_FCLTY_LOC_NTNL_PROV_ID"),
    when(col("NTNL_PROV_ID").isNull(), lit("UNK")).otherwise(col("NTNL_PROV_ID")).alias("NTNL_PROV_ID")
)
write_files(
    df_ESIClmProvExtr,
    f"{adls_path}/key/ESIClmProvExtr.DrugClmProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)