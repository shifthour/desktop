# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2014 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC JOB NAME:  IdsMmsPcmhMrMelcExtr
# MAGIC 
# MAGIC CALLED BY: IdsMmsPcmhMrMelcCntl
# MAGIC 
# MAGIC DESCRIPTION: Pulls the member eligible data from PCMH_PROV_MBR_RMBRMT_ELIG and MBR_PCP tables and creates keyword files  
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date                        Project/Altiris #         Change Description\(9)\(9)\(9)    Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------                --------------------            ------------------------         -----------------------------------------------------------------------         -------------------------------           -------------------------------\(9)------------------       
# MAGIC KarthikRavikindi         2014-04-14                      5209                  Original Development                                              IntegrateNewDevl               Kalyan Neelam          2014-04-16 
# MAGIC 
# MAGIC 
# MAGIC Michael Harmon        2014-11-04             INC0114552               Corrected MEPR_CREATE_GEN_GAP for             IntegrateCurDevl                 Kalyan Neelam           2014-11-19
# MAGIC                                                                                                      keyword output file in mepr_crt transform
# MAGIC 
# MAGIC                                                                                                    
# MAGIC Karthik Chintalapani   2016-06-06       5212                  Added  logic stage to limit the HOST Members                            IntegrateDev1               Kalyan Neelam           2016-06-16
# MAGIC                                                                                  using HOST_ATTRBTN_HIST and Host_Mbr_Lkp stages
# MAGIC 
# MAGIC Karthik Chintalapani   2016-10-12       5212       Converted from Inner join to left outer join in the Xfm_Host_Mbr_lkp     IntegrateDev1                Kalyan Neelam           2016-10-19   
# MAGIC                                                                            stage as per the requirement change.
# MAGIC 
# MAGIC Krishnakanth              2016-10-22       30001     Added a parameter CapRunMonth for COBALTTALON in the extract   IntegrateDev2                Jag Yelavarthi             2016-11-16
# MAGIC          Manivannan
# MAGIC 
# MAGIC Krishnakanth              2016-11-10       30001     Added a column GEN_GAP to                                                               IntegrateDev2              Kalyan Neelam            2017-11-13      
# MAGIC          Manivannan                                              the file Mms_Pcmh_Ppo_MEPR_TERM_Detail.dat 
# MAGIC                                                                           and Mms_Pcmh_Ppo_MELC_TERM_Detail.dat
# MAGIC                                                                           in the stage Xfm_BusRules2.
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi    06-06-2023      US-586282      Update datastage job to include a fixed value in the MEPR CRT      IntegrateDev1            Reddy Sanam             2023-06-06
# MAGIC                                                                                     (mepr_crt link in xfrm rules2) stage to include an override reason. 
# MAGIC                                                                                       code to add:  ,@pMEPR_MCTR_ORSN="OVRP"
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi    01/02/2024     US-596199      Added code in transformer if the SubId is less than 9 Char Lapd 0    IntegrateDev2           Reddy Sanam              2024-01-02
# MAGIC                                                                                  so that @ symbol is at the start of each line

# MAGIC The different extract files contains keyword format of MR row created and termed, MELC row created and termed, all the files are concatenated together in IdsMmsPcmhMrMelcCntl job.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
RunDate = get_widget_value('RunDate','')
FrstDayOfMonth = get_widget_value('FrstDayOfMonth','')
LstDayOfMonth = get_widget_value('LstDayOfMonth','')
CapMonth = get_widget_value('CapMonth','')
CapRiskMonth = get_widget_value('CapRiskMonth','')
CapRiskSrcSystemCd = get_widget_value('CapRiskSrcSystemCd','')

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

extract_query_Mbr_sbsb_id = f"""
SELECT DISTINCT
MBR.MBR_SK,
MBR.MBR_UNIQ_KEY,
SUB.SUB_ID,
MBR.MBR_SFX_NO,
GRP.GRP_ID
FROM {IDSOwner}.SUB SUB,
     {IDSOwner}.MBR MBR,
     {IDSOwner}.GRP GRP
WHERE
SUB.SUB_SK = MBR.SUB_SK
AND SUB.GRP_SK = GRP.GRP_SK
AND SUB.SUB_ID NOT IN ('UNK','NA')
"""

df_Mbr_sbsb_id_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Mbr_sbsb_id)
    .load()
)

df_hf_pcmh_mbr_sbsb_id = dedup_sort(
    df_Mbr_sbsb_id_raw,
    ["MBR_SK"],
    []
)

extract_query_Host_Mbr_Lkp = f"""
SELECT
MBR.MBR_SK,
MBR.HOST_MBR_IN
FROM {IDSOwner}.MBR MBR
"""

df_Host_Mbr_Lkp_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_Host_Mbr_Lkp)
    .load()
)

df_hf_pcmh_mbr_host_in = dedup_sort(
    df_Host_Mbr_Lkp_raw,
    ["MBR_SK"],
    []
)

extract_query_HOST_ATTRBTN_HIST = f"""
SELECT
b.MBR_SK,
b.MBR_UNIQ_KEY
FROM {EDWOwner}.P_HSTD_BCBSA_MVPS_CTL a,
     {EDWOwner}.P_HSTD_MBR_BCBSA_ATTRBTN_HIST b
WHERE
a.CYC_2C_ID = b.PRCS_CYC_YR_MO_SK
AND a.ATTRBTN_2C_PERD_BEG_DT_SK = b.ATTRBTN_PERD_BEG_DT
AND ATTRBTN_2C_PERD_END_DT_SK = b.ATTRBTN_PERD_END_DT
AND a.CYC_2B_CMPL_DT_SK <> '1753-01-01'
AND a.CYC_2C_CMPL_DT_SK <> '1753-01-01'
AND a.BDF_CMPL_DT_SK = '1753-01-01'
AND b.ATTRBTN_IN = 'Y'
"""

df_HOST_ATTRBTN_HIST_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_HOST_ATTRBTN_HIST)
    .load()
)

df_hf_host_attrbtn_hist = dedup_sort(
    df_HOST_ATTRBTN_HIST_raw,
    ["MBR_SK"],
    []
)

extract_query_MR_Process = f"""
select
MEPR.PCP_SK,
MEPR.PCMH_SK,
MEPR.TREO_RISK,
MEPR.TreoMHG,
MEPR.FacetsMR,
MEPR.MR_EFF_DT,
MEPR.MR_TERM_DT,
MELC.MBR_SK as FACETS_SK,
MELC.EFF_DT_SK as MELC_EFF_DT,
MELC.TERM_DT_SK as MELC_TRM_DT,
MELC.SRC_CD as FacetsLSF
FROM
(
 SELECT
  FUTU.MBR_SK as PCP_SK,
  CURR.MBR_SK as PCMH_SK,
  CURR.RISK_CAT_ID as TREO_RISK,
  CURR.PROV_ID as TreoMHG,
  FUTU.PROV_ID as FacetsMR,
  FUTU.EFF_DT_SK as MR_EFF_DT,
  FUTU.TERM_DT_SK as MR_TERM_DT
 FROM
  (
   SELECT
    MBR_PCP.MBR_SK,
    PROV.PROV_ID,
    MBR_PCP.EFF_DT_SK,
    MBR_PCP.TERM_DT_SK
   FROM
    {IDSOwner}.MBR_PCP MBR_PCP,
    {IDSOwner}.PROV PROV
   WHERE
    PROV.PROV_SK = MBR_PCP.PROV_SK
    AND MBR_PCP.MBR_PCP_TYP_CD_SK = '424807950'
    AND MBR_PCP.EFF_DT_SK <= '{RunDate}'
    AND MBR_PCP.TERM_DT_SK > '{RunDate}'
  ) FUTU
  FULL OUTER JOIN
  (
   select
    PCMH_PROV_MBR_RMBRMT_ELIG.MBR_SK,
    PCMH_PROV_MBR_RMBRMT_ELIG.ELIG_FOR_RMBRMT_IN,
    PCMH_PROV_MBR_RMBRMT_ELIG.PROV_ID,
    MBR_RISK_MESR.RISK_CAT_ID
   from
    {IDSOwner}.MBR_RISK_MESR MBR_RISK_MESR
    RIGHT OUTER JOIN
    {IDSOwner}.PCMH_PROV_MBR_RMBRMT_ELIG PCMH_PROV_MBR_RMBRMT_ELIG
    on MBR_RISK_MESR.MBR_SK = PCMH_PROV_MBR_RMBRMT_ELIG.MBR_SK
   where
    MBR_RISK_MESR.SRC_SYS_CD_SK = (
      SELECT CD_MPPNG_SK
      FROM {IDSOwner}.CD_MPPNG
      WHERE TRGT_CD='{CapRiskSrcSystemCd}'
      AND TRGT_DOMAIN_NM='SOURCE SYSTEM'
    )
    and MBR_RISK_MESR.PRCS_YR_MO_SK = '{CapRiskMonth}'
    and PCMH_PROV_MBR_RMBRMT_ELIG.AS_OF_YR_MO_SK = '{CapMonth}'
    and PCMH_PROV_MBR_RMBRMT_ELIG.ELIG_FOR_RMBRMT_IN = 'Y'
  ) CURR
  ON CURR.MBR_SK = FUTU.MBR_SK
) MEPR
FULL OUTER JOIN
(
 SELECT
  MBR_LFSTYL_BNF.MBR_SK,
  MBR_LFSTYL_BNF.EFF_DT_SK,
  MBR_LFSTYL_BNF.TERM_DT_SK,
  MBR_LFSTYL_BNF.MBR_LFSTYL_BNF_CD_SK,
  CD_MPPNG.SRC_CD
 FROM
  {IDSOwner}.MBR_LFSTYL_BNF MBR_LFSTYL_BNF,
  {IDSOwner}.CD_MPPNG CD_MPPNG
 WHERE
  CD_MPPNG.CD_MPPNG_SK = MBR_LFSTYL_BNF.MBR_LFSTYL_BNF_CD_SK
  and MBR_LFSTYL_BNF.EFF_DT_SK < '{RunDate}'
  and MBR_LFSTYL_BNF.TERM_DT_SK >= '{RunDate}'
) MELC
on MELC.MBR_SK = COALESCE(MEPR.PCP_SK, MEPR.PCMH_SK)
"""

df_MR_Process = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MR_Process)
    .load()
)

################################################################################
# Xfm_BusRules1
################################################################################

df_Xfm_BusRules1_input = df_MR_Process

df_Xfm_BusRules1_MHG_exists_NoMR = df_Xfm_BusRules1_input.filter(
    (F.col("FacetsMR").isNull()) & (F.col("TreoMHG").isNotNull())
).withColumn("PCP_SK", F.when(F.col("PCP_SK").isNull(), F.lit("Null")).otherwise(F.col("PCP_SK"))) \
 .withColumn("PCMH_SK", F.when(F.col("PCMH_SK").isNull(), F.lit("Null")).otherwise(F.col("PCMH_SK"))) \
 .withColumn("FACETS_SK", F.when(F.col("FACETS_SK").isNull(), F.lit("Null")).otherwise(F.col("FACETS_SK"))) \
 .withColumn("TreoMHG", F.col("TreoMHG")) \
 .withColumn("FacetsMR", F.lit("Null")) \
 .withColumn("KeepMR", F.lit("KeepMR =1")) \
 .withColumn("TERM_MEPR", F.lit("Null")) \
 .withColumn("CRT_MEPR", F.col("TreoMHG")) \
 .withColumn("MR_EFF_DT", F.when(F.col("MR_EFF_DT").isNull(), F.lit("Null")).otherwise(F.col("MR_EFF_DT"))) \
 .withColumn("MR_TERM_DT", F.when(F.col("MR_TERM_DT").isNull(), F.lit("Null")).otherwise(F.col("MR_TERM_DT"))) \
 .withColumn("TREO_RISK", F.when(F.col("TREO_RISK").isNull(), F.lit("Null")).otherwise(F.col("TREO_RISK"))) \
 .withColumn("FACETS_LSF", F.when(F.col("FacetsLSF").isNull(), F.lit("Null")).otherwise(F.col("FacetsLSF"))) \
 .withColumn("MELC_EFF_DT", F.when(F.col("MELC_EFF_DT").isNull(), F.lit("Null")).otherwise(F.col("MELC_EFF_DT"))) \
 .withColumn("MELC_TERM_DT", F.when(F.col("MELC_TRM_DT").isNull(), F.lit("Null")).otherwise(F.col("MELC_TRM_DT")))

df_Xfm_BusRules1_MR_exists_NoMHG = df_Xfm_BusRules1_input.filter(
    (F.col("FacetsMR").isNotNull()) & (F.col("TreoMHG").isNull())
).withColumn("PCP_SK", F.when(F.col("PCP_SK").isNull(), F.lit("Null")).otherwise(F.col("PCP_SK"))) \
 .withColumn("PCMH_SK", F.when(F.col("PCMH_SK").isNull(), F.lit("Null")).otherwise(F.col("PCMH_SK"))) \
 .withColumn("FACETS_SK", F.when(F.col("FACETS_SK").isNull(), F.lit("Null")).otherwise(F.col("FACETS_SK"))) \
 .withColumn("TreoMHG", F.lit("Null")) \
 .withColumn("FacetsMR", F.col("FacetsMR")) \
 .withColumn("KeepMR", F.lit("Null")) \
 .withColumn("TERM_MEPR", F.col("FacetsMR")) \
 .withColumn("CRT_MEPR", F.lit("Null")) \
 .withColumn("MR_EFF_DT", F.when(F.col("MR_EFF_DT").isNull(), F.lit("Null")).otherwise(F.col("MR_EFF_DT"))) \
 .withColumn("MR_TERM_DT", F.when(F.col("MR_TERM_DT").isNull(), F.lit("Null")).otherwise(F.col("MR_TERM_DT"))) \
 .withColumn("TREO_RISK", F.when(F.col("TREO_RISK").isNull(), F.lit("Null")).otherwise(F.col("TREO_RISK"))) \
 .withColumn("FACETS_LSF", F.when(F.col("FacetsLSF").isNull(), F.lit("Null")).otherwise(F.col("FacetsLSF"))) \
 .withColumn("MELC_EFF_DT", F.when(F.col("MELC_EFF_DT").isNull(), F.lit("Null")).otherwise(F.col("MELC_EFF_DT"))) \
 .withColumn("MELC_TERM_DT", F.when(F.col("MELC_TRM_DT").isNull(), F.lit("Null")).otherwise(F.col("MELC_TRM_DT")))

df_Xfm_BusRules1_MHG_MR_exists = df_Xfm_BusRules1_input.filter(
    (F.col("PCMH_SK").isNotNull()) & (F.col("PCP_SK").isNotNull())
).withColumn("PCP_SK", F.when(F.col("PCP_SK").isNull(), F.lit("Null")).otherwise(F.col("PCP_SK"))) \
 .withColumn("PCMH_SK", F.when(F.col("PCMH_SK").isNull(), F.lit("Null")).otherwise(F.col("PCMH_SK"))) \
 .withColumn("FACETS_SK", F.when(F.col("FACETS_SK").isNull(), F.lit("Null")).otherwise(F.col("FACETS_SK"))) \
 .withColumn("TreoMHG", F.col("TreoMHG")) \
 .withColumn("FacetsMR", F.col("FacetsMR")) \
 .withColumn("KeepMR", F.lit("KeepMR =1")) \
 .withColumn("TERM_MEPR", F.when(F.col("TreoMHG") != F.col("FacetsMR"), F.col("FacetsMR")).otherwise(F.lit("x"))) \
 .withColumn("CRT_MEPR", F.when(F.col("TreoMHG") != F.col("FacetsMR"), F.col("TreoMHG")).otherwise(F.lit("x"))) \
 .withColumn("MR_EFF_DT", F.when(F.col("MR_EFF_DT").isNull(), F.lit("Null")).otherwise(F.col("MR_EFF_DT"))) \
 .withColumn("MR_TERM_DT", F.when(F.col("MR_TERM_DT").isNull(), F.lit("Null")).otherwise(F.col("MR_TERM_DT"))) \
 .withColumn("TREO_RISK", F.when(F.col("TREO_RISK").isNull(), F.lit("Null")).otherwise(F.col("TREO_RISK"))) \
 .withColumn("FACETS_LSF", F.when(F.col("FacetsLSF").isNull(), F.lit("Null")).otherwise(F.col("FacetsLSF"))) \
 .withColumn("MELC_EFF_DT", F.when(F.col("MELC_EFF_DT").isNull(), F.lit("Null")).otherwise(F.col("MELC_EFF_DT"))) \
 .withColumn("MELC_TERM_DT", F.when(F.col("MELC_TRM_DT").isNull(), F.lit("Null")).otherwise(F.col("MELC_TRM_DT")))

df_Xfm_BusRules1_No_Treo_MHG = df_Xfm_BusRules1_input.filter(
    F.col("TreoMHG").isNull()
).withColumn("PCP_SK", F.when(F.col("PCP_SK").isNull(), F.lit("Null")).otherwise(F.col("PCP_SK"))) \
 .withColumn("PCMH_SK", F.when(F.col("PCMH_SK").isNull(), F.lit("Null")).otherwise(F.col("PCMH_SK"))) \
 .withColumn("FACETS_SK", F.when(F.col("FACETS_SK").isNull(), F.lit("Null")).otherwise(F.col("FACETS_SK"))) \
 .withColumn("TreoMHG", F.lit("Null")) \
 .withColumn("FacetsMR", F.when(F.col("FacetsMR").isNull(), F.lit("Null")).otherwise(F.col("FacetsMR"))) \
 .withColumn("KeepMR", F.lit("Null")) \
 .withColumn("TERM_MEPR", F.lit("Null")) \
 .withColumn("CRT_MEPR", F.lit("Null")) \
 .withColumn("MR_EFF_DT", F.when(F.col("MR_EFF_DT").isNull(), F.lit("Null")).otherwise(F.col("MR_EFF_DT"))) \
 .withColumn("MR_TERM_DT", F.when(F.col("MR_TERM_DT").isNull(), F.lit("Null")).otherwise(F.col("MR_TERM_DT"))) \
 .withColumn("TREO_RISK", F.when(F.col("TREO_RISK").isNull(), F.lit("Null")).otherwise(F.col("TREO_RISK"))) \
 .withColumn("FACETS_LSF", F.when(F.col("FacetsLSF").isNull(), F.lit("Null")).otherwise(F.col("FacetsLSF"))) \
 .withColumn("MELC_EFF_DT", F.when(F.col("MELC_EFF_DT").isNull(), F.lit("Null")).otherwise(F.col("MELC_EFF_DT"))) \
 .withColumn("MELC_TERM_DT", F.when(F.col("MELC_TRM_DT").isNull(), F.lit("Null")).otherwise(F.col("MELC_TRM_DT")))

df_hf_pcmh_mms_mhg_nomr = dedup_sort(
    df_Xfm_BusRules1_MHG_exists_NoMR.select(
        "PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"
    ),
    ["PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_hf_pcmh_mms_mr_nomhg = dedup_sort(
    df_Xfm_BusRules1_MR_exists_NoMHG.select(
        "PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"
    ),
    ["PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_hf_pcmh_mms_mhg_mr = dedup_sort(
    df_Xfm_BusRules1_MHG_MR_exists.select(
        "PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"
    ),
    ["PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_hf_pcmh_mms_notreomhg = dedup_sort(
    df_Xfm_BusRules1_No_Treo_MHG.select(
        "PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"
    ),
    ["PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_Lkn_Coll1 = df_hf_pcmh_mms_mhg_nomr.unionByName(df_hf_pcmh_mms_mr_nomhg, allowMissingColumns=True) \
    .unionByName(df_hf_pcmh_mms_mhg_mr, allowMissingColumns=True) \
    .unionByName(df_hf_pcmh_mms_notreomhg, allowMissingColumns=True)

df_Lkn_Coll1_dedup = dedup_sort(
    df_Lkn_Coll1,
    ["PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_Xfm_Trim_input = df_Lkn_Coll1_dedup

df_Xfm_Trim_output = df_Xfm_Trim_input.withColumn(
    "MBR_SK_F",
    F.when(
        (trim(F.col("PCMH_SK")) == "Null") & (trim(F.col("FACETS_SK")) == "Null") & (trim(F.col("PCP_SK")) != "Null"),
        F.col("PCP_SK")
    ).when(
        (trim(F.col("PCMH_SK")) != "Null") & (trim(F.col("FACETS_SK")) != "Null") & (trim(F.col("PCP_SK")) == "Null"),
        F.col("FACETS_SK")
    ).when(
        (trim(F.col("PCP_SK")) == "Null") & (trim(F.col("PCMH_SK")) == "Null") & (trim(F.col("FACETS_SK")) != "Null"),
        F.col("FACETS_SK")
    ).when(
        (trim(F.col("PCP_SK")) != "Null") & (trim(F.col("PCMH_SK")) != "Null") & (trim(F.col("FACETS_SK")) == "Null"),
        F.col("PCMH_SK")
    ).when(
        (trim(F.col("PCP_SK")) == "Null") & (trim(F.col("FACETS_SK")) == "Null") & (trim(F.col("PCMH_SK")) != "Null"),
        F.col("PCMH_SK")
    ).when(
        (trim(F.col("PCP_SK")) != "Null") & (trim(F.col("FACETS_SK")) != "Null") & (trim(F.col("PCMH_SK")) == "Null"),
        F.col("FACETS_SK")
    ).when(
        (trim(F.col("PCMH_SK")) != "Null") & (trim(F.col("FACETS_SK")) != "Null") & (trim(F.col("PCP_SK")) != "Null"),
        F.col("PCP_SK")
    ).otherwise(F.lit("x"))
)

df_hf_pcmh_mms_mepr_final = dedup_sort(
    df_Xfm_Trim_output.select(
       F.col("MBR_SK_F"),
       F.col("PCP_SK"),
       F.col("PCMH_SK"),
       F.col("FACETS_SK"),
       F.col("TreoMHG"),
       F.col("FacetsMR"),
       F.col("KeepMR"),
       F.col("TERM_MEPR"),
       F.col("CRT_MEPR"),
       F.col("MR_EFF_DT"),
       F.col("MR_TERM_DT"),
       F.col("TREO_RISK"),
       F.col("FACETS_LSF"),
       F.col("MELC_EFF_DT"),
       F.col("MELC_TERM_DT")
    ),
    ["MBR_SK_F","PCP_SK","PCMH_SK","FACETS_SK","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","MR_EFF_DT","MR_TERM_DT","TREO_RISK","FACETS_LSF","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

################################################################################
# Xfm_
################################################################################

df_Xfm__input = df_hf_pcmh_mms_mepr_final

df_Xfm__MeprNotNull = df_Xfm__input.filter(
    (F.col("CRT_MEPR") != "Null") & (F.col("CRT_MEPR") != "x")
).select(
    F.col("MBR_SK_F"),
    F.col("TreoMHG"),
    F.col("FacetsMR"),
    F.col("KeepMR").alias("KeepMR").cast(StringType()),
    F.col("TERM_MEPR"),
    F.col("CRT_MEPR"),
    F.col("TREO_RISK").alias("RISK_CAT_ID"),
    F.col("FACETS_LSF").alias("SRC_CD"),
    F.col("MR_EFF_DT"),
    F.col("MR_TERM_DT"),
    F.col("MELC_EFF_DT"),
    F.col("MELC_TERM_DT")
)

df_hf_pcmh_mepr_not_null = dedup_sort(
    df_Xfm__MeprNotNull,
    ["MBR_SK_F","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","RISK_CAT_ID","SRC_CD","MR_EFF_DT","MR_TERM_DT","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_Xfm__KeepMrNotNull = df_Xfm__input.filter(
    (F.col("KeepMR") == "KeepMR =1")
).select(
    F.col("MBR_SK_F"),
    F.col("TreoMHG"),
    F.col("FacetsMR"),
    F.col("KeepMR").alias("KeepMR").cast(StringType()),
    F.col("TERM_MEPR"),
    F.col("CRT_MEPR"),
    F.col("TREO_RISK").alias("RISK_CAT_ID"),
    F.col("FACETS_LSF").alias("SRC_CD"),
    F.col("MR_EFF_DT"),
    F.col("MR_TERM_DT"),
    F.col("MELC_EFF_DT"),
    F.col("MELC_TERM_DT")
)

df_hf_pcmh_keepmr_not_null = dedup_sort(
    df_Xfm__KeepMrNotNull,
    ["MBR_SK_F","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","RISK_CAT_ID","SRC_CD","MR_EFF_DT","MR_TERM_DT","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_Xfm__KeepMrNull = df_Xfm__input.filter(
    (F.col("KeepMR") != "KeepMR =1")
).select(
    F.col("MBR_SK_F"),
    F.col("TreoMHG"),
    F.col("FacetsMR"),
    F.col("KeepMR").alias("KeepMR").cast(StringType()),
    F.col("TERM_MEPR"),
    F.col("CRT_MEPR"),
    F.col("TREO_RISK").alias("RISK_CAT_ID"),
    F.col("FACETS_LSF").alias("SRC_CD"),
    F.col("MR_EFF_DT"),
    F.col("MR_TERM_DT"),
    F.col("MELC_EFF_DT"),
    F.col("MELC_TERM_DT")
)

df_hf_pcmh_keepmr_null = dedup_sort(
    df_Xfm__KeepMrNull,
    ["MBR_SK_F","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","RISK_CAT_ID","SRC_CD","MR_EFF_DT","MR_TERM_DT","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

################################################################################
# Xfm3
################################################################################

df_Xfm3_input = df_hf_pcmh_keepmr_null

df_Xfm3_keepmepr_null = df_Xfm3_input.filter(
    (F.col("KeepMR") != "KeepMR =1")
).withColumn("CRT_MELC", F.lit("Null")) \
 .withColumn("TERM_MELC", F.when(F.col("SRC_CD") != "Null", F.col("SRC_CD")).otherwise(F.lit("Null"))) \
 .withColumn("ErrorCondition", F.lit("Null"))

df_Xfm3_keepmepr_null_sel = df_Xfm3_keepmepr_null.select(
    "MBR_SK_F",
    "TreoMHG",
    "FacetsMR",
    "KeepMR",
    "TERM_MEPR",
    "CRT_MEPR",
    "RISK_CAT_ID",
    "SRC_CD",
    "CRT_MELC",
    "TERM_MELC",
    "ErrorCondition",
    "MR_EFF_DT",
    "MR_TERM_DT",
    "MELC_EFF_DT",
    "MELC_TERM_DT"
)

################################################################################
# Xfm2
################################################################################

df_Xfm2_input = df_hf_pcmh_keepmr_not_null

df_Xfm2_notnull = df_Xfm2_input.withColumn("SvErrorCondition", F.lit("ErrorCondition=1")) \
    .withColumn("SvMELC", F.col("RISK_CAT_ID")) \
    .withColumn("SvTermMELC", F.col("SRC_CD"))

df_Xfm2_keepmepr_notnull = df_Xfm2_notnull.filter(
    (F.col("KeepMR") == "KeepMR =1")
).withColumn(
    "CRT_MEPR",
    F.when(F.col("RISK_CAT_ID") == "Null", F.lit("Null")).otherwise(F.col("CRT_MEPR"))
).withColumn(
    "CRT_MELC",
    F.when(
       (F.col("RISK_CAT_ID") != "Null") & (F.col("SRC_CD") != "Null"),
       F.when(F.col("RISK_CAT_ID") != F.col("SRC_CD"), F.col("SvMELC")).otherwise(F.lit("x"))
    ).otherwise(
       F.when((F.col("RISK_CAT_ID") != "Null") & (F.col("SRC_CD") == "Null"), F.col("SvMELC")).otherwise(F.lit("x"))
    )
).withColumn(
    "TERM_MELC",
    F.when(
       (F.col("RISK_CAT_ID") != "Null") & (F.col("SRC_CD") != "Null"),
       F.when(F.col("RISK_CAT_ID") != F.col("SRC_CD"), F.col("SvTermMELC")).otherwise(F.lit("x"))
    ).otherwise(F.lit("x"))
).withColumn(
    "ErrorCondition",
    F.when(F.col("RISK_CAT_ID") == "Null", F.col("SvErrorCondition")).otherwise(F.lit("Null"))
)

df_Xfm2_keepmepr_notnull_sel = df_Xfm2_keepmepr_notnull.select(
   "MBR_SK_F",
   "TreoMHG",
   "FacetsMR",
   "KeepMR",
   "TERM_MEPR",
   "CRT_MEPR",
   "RISK_CAT_ID",
   "SRC_CD",
   "CRT_MELC",
   "TERM_MELC",
   "ErrorCondition",
   "MR_EFF_DT",
   "MR_TERM_DT",
   "MELC_EFF_DT",
   "MELC_TERM_DT"
)

################################################################################
# Xfm1
################################################################################

df_Xfm1_input = df_hf_pcmh_mepr_not_null
df_Xfm1_notnull = df_Xfm1_input.withColumn("SvErrorCondition", F.lit("ErrorCondition=1")) \
    .withColumn("SvMELC", F.col("RISK_CAT_ID")) \
    .withColumn("SvTermMELC", F.col("SRC_CD"))

df_Xfm1_mepr_notnull = df_Xfm1_notnull.select(
    F.col("MBR_SK_F"),
    F.col("TreoMHG"),
    F.col("FacetsMR"),
    F.col("KeepMR"),
    F.when(F.col("TERM_MEPR").isNull(), F.lit("Null")).otherwise(F.col("TERM_MEPR")).alias("TERM_MEPR"),
    F.when(F.col("RISK_CAT_ID") == "Null", F.lit("Null")).otherwise(F.col("CRT_MEPR")).alias("CRT_MEPR"),
    F.col("RISK_CAT_ID"),
    F.col("SRC_CD"),
    F.when(
      (F.col("RISK_CAT_ID") != "Null") & (F.col("SRC_CD") != "Null"),
      F.when(F.col("RISK_CAT_ID") != F.col("SRC_CD"), F.col("SvMELC")).otherwise(F.lit("x"))
    ).otherwise(
      F.when((F.col("RISK_CAT_ID") != "Null") & (F.col("SRC_CD") == "Null"), F.col("SvMELC")).otherwise(F.lit("x"))
    ).alias("CRT_MELC"),
    F.when(
      (F.col("RISK_CAT_ID") != "Null") & (F.col("SRC_CD") != "Null"),
      F.when(F.col("RISK_CAT_ID") != F.col("SRC_CD"), F.col("SvTermMELC")).otherwise(F.lit("x"))
    ).otherwise(F.lit("x")).alias("TERM_MELC"),
    F.when(F.col("RISK_CAT_ID") == "Null", F.col("SvErrorCondition")).otherwise(F.lit("Null")).alias("ErrorCondition"),
    F.col("MR_EFF_DT"),
    F.col("MR_TERM_DT"),
    F.col("MELC_EFF_DT"),
    F.col("MELC_TERM_DT")
)

################################################################################
# Link_Collector_732
################################################################################

df_Link_Collector_732 = df_Xfm1_mepr_notnull.unionByName(df_Xfm2_keepmepr_notnull_sel, allowMissingColumns=True) \
    .unionByName(df_Xfm3_keepmepr_null_sel, allowMissingColumns=True)

df_Link_Collector_732_dedup = dedup_sort(
    df_Link_Collector_732,
    ["MBR_SK_F","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","RISK_CAT_ID","SRC_CD","CRT_MELC","TERM_MELC","ErrorCondition","MR_EFF_DT","MR_TERM_DT","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_hf_pcmh_melc_mr_crt_trm = df_Link_Collector_732_dedup

################################################################################
# Xfm_HostMbr_Split
################################################################################

df_Xfm_HostMbr_Split_input = df_hf_pcmh_melc_mr_crt_trm

df_Xfm_HostMbr_Split_joined = df_Xfm_HostMbr_Split_input.alias("Lnk_Host_mbr_Lkp").join(
    df_hf_pcmh_mbr_host_in.alias("Lnk_Host_Mbr"),
    on=F.col("Lnk_Host_mbr_Lkp.MBR_SK_F") == F.col("Lnk_Host_Mbr.MBR_SK"),
    how="left"
)

df_Host_Mbr = df_Xfm_HostMbr_Split_joined.filter(
    F.col("Lnk_Host_Mbr.HOST_MBR_IN") == "Y"
).select(
    F.col("Lnk_Host_mbr_Lkp.*"),
    F.col("Lnk_Host_Mbr.HOST_MBR_IN")
)

df_Lnk_Home_Mbr = df_Xfm_HostMbr_Split_joined.filter(
    F.col("Lnk_Host_Mbr.HOST_MBR_IN") == "N"
).select(
    F.col("Lnk_Host_mbr_Lkp.*"),
    F.col("Lnk_Host_Mbr.HOST_MBR_IN")
)

################################################################################
# Xfm_Host_Mbr_lkp
################################################################################

df_Xfm_Host_Mbr_lkp_input = df_Host_Mbr.withColumnRenamed("HOST_MBR_IN","HOST_MBR_IN_lookup")

df_Xfm_Host_Mbr_lkp_joined = df_Xfm_Host_Mbr_lkp_input.alias("Host_Mbr").join(
    df_hf_host_attrbtn_hist.alias("Lnk_Host_aatrbt"),
    on=F.col("Host_Mbr.MBR_SK_F") == F.col("Lnk_Host_aatrbt.MBR_SK"),
    how="left"
)

df_Match_Host_Mbr = df_Xfm_Host_Mbr_lkp_joined.select("Host_Mbr.*")

################################################################################
# Lnk_Coll_Home_Host_Mbr
################################################################################

df_Lnk_Coll_Home_Host_Mbr = df_Lnk_Home_Mbr.unionByName(df_Match_Host_Mbr, allowMissingColumns=True)

df_Lnk_Coll_Home_Host_Mbr_dedup = dedup_sort(
    df_Lnk_Coll_Home_Host_Mbr,
    ["MBR_SK_F","TreoMHG","FacetsMR","KeepMR","TERM_MEPR","CRT_MEPR","RISK_CAT_ID","SRC_CD","CRT_MELC","TERM_MELC","ErrorCondition","MR_EFF_DT","MR_TERM_DT","MELC_EFF_DT","MELC_TERM_DT"],
    []
)

df_hf_pcmh_melc_mr_crt_trm_hold = df_Lnk_Coll_Home_Host_Mbr_dedup

################################################################################
# Xfm_BusRules2
################################################################################

df_Xfm_BusRules2_input = df_hf_pcmh_melc_mr_crt_trm_hold.alias("Lnk_Main_Data").join(
    df_hf_pcmh_mbr_sbsb_id.alias("Lnk_Lkp_Sub"),
    on=F.col("Lnk_Main_Data.MBR_SK_F") == F.col("Lnk_Lkp_Sub.MBR_SK"),
    how="left"
)

df_Xfm_BusRules2_stage = df_Xfm_BusRules2_input.withColumn("FrstDayOfMonth", F.lit(FrstDayOfMonth)) \
    .withColumn("LstDayOfMonth", F.lit(LstDayOfMonth))

cond_sbsb_in = (
    (F.col("Lnk_Lkp_Sub.SUB_ID").isNotNull()) &
    (
      (F.col("Lnk_Main_Data.CRT_MELC") != "Null") & (F.col("Lnk_Main_Data.CRT_MELC") != "x") |
      (F.col("Lnk_Main_Data.CRT_MEPR") != "Null") & (F.col("Lnk_Main_Data.CRT_MEPR") != "x") |
      (F.col("Lnk_Main_Data.TERM_MEPR") != "Null") & (F.col("Lnk_Main_Data.TERM_MEPR") != "x") |
      (F.col("Lnk_Main_Data.TERM_MELC") != "Null") & (F.col("Lnk_Main_Data.TERM_MELC") != "x")
    )
)

df_sbsb_in = df_Xfm_BusRules2_stage.filter(cond_sbsb_in).select(
    F.when(F.length(F.col("Lnk_Lkp_Sub.SUB_ID"))==9, F.col("Lnk_Lkp_Sub.SUB_ID")).otherwise(F.lit("0") + F.col("Lnk_Lkp_Sub.SUB_ID")).alias("SUB_ID"),
    F.lit("00").alias("MBR_SFX_NO"),
    F.lit("1").alias("KEY"),
    F.lit("@pRECTYPE=\"SBSB\"").alias("RecType"),
    F.lit("@pSBSB_FAM_UPDATE_CD=\"AP\"").alias("FMLY_UPDT_CD"),
    F.concat(F.lit("@pSBSB_ID=\""), trim(F.col("Lnk_Lkp_Sub.SUB_ID")), F.lit("\"")).alias("SBSB_ID"),
    F.concat(F.lit("@pGRGR_ID=\""), trim(F.col("Lnk_Lkp_Sub.GRP_ID")), F.lit("\"")).alias("GRGR_ID")
)

df_hf_pcmh_mms_sbsb_dtl = dedup_sort(
    df_sbsb_in,
    ["SUB_ID","MBR_SFX_NO","KEY","RecType","FMLY_UPDT_CD","SBSB_ID","GRGR_ID"],
    []
)

cond_meme_out = cond_sbsb_in

df_meme_out = df_Xfm_BusRules2_stage.filter(cond_meme_out).select(
    F.when(F.length(F.col("Lnk_Lkp_Sub.SUB_ID"))==9, F.col("Lnk_Lkp_Sub.SUB_ID")).otherwise(F.lit("0") + F.col("Lnk_Lkp_Sub.SUB_ID")).alias("SUB_ID"),
    F.col("Lnk_Lkp_Sub.MBR_SFX_NO"),
    F.lit("2").alias("KEY"),
    F.lit("@pRECTYPE=\"MEME\"").alias("RecType"),
    F.lit("@pMEME_UPDATE_CD=\"AP\"").alias("UPDT_CD"),
    F.concat(F.lit("@pMEME_SFX=\""), trim(F.col("Lnk_Lkp_Sub.MBR_SFX_NO")), F.lit("\"")).alias("MBR_SFX")
)
df_hf_pcmh_mms_meme_dtl = dedup_sort(
    df_meme_out,
    ["SUB_ID","MBR_SFX_NO","KEY","RecType","UPDT_CD","MBR_SFX"],
    []
)

cond_mepr_crt = (
    (F.col("Lnk_Lkp_Sub.SUB_ID").isNotNull()) &
    (F.col("Lnk_Main_Data.CRT_MEPR") != "Null") & (F.col("Lnk_Main_Data.CRT_MEPR") != "x")
)
df_mepr_crt = df_Xfm_BusRules2_stage.filter(cond_mepr_crt).select(
    F.when(F.length(F.col("Lnk_Lkp_Sub.SUB_ID"))==9, F.col("Lnk_Lkp_Sub.SUB_ID")).otherwise(F.lit("0") + F.col("Lnk_Lkp_Sub.SUB_ID")).alias("SUB_ID"),
    F.col("Lnk_Lkp_Sub.MBR_SFX_NO"),
    F.lit("6").alias("KEY"),
    F.lit("@pRECTYPE=\"MEPR\"").alias("RecType"),
    F.lit("@pMEPR_UPDATE_CD=\"IN\"").alias("UPDT_CD"),
    F.lit("@pMEPR_PCP_TYPE=\"MR\"").alias("PCP_TYP"),
    F.concat(F.lit("@pMEPR_EFF_DT=\""), F.expr("FORMAT.DATE(FrstDayOfMonth,'DB2','DATE','MM/DD/CCYY')"), F.lit("\"")).alias("EFF_DT"),
    F.lit(" ").alias("TERM_RSN"),
    F.concat(F.lit("@pPRPR_ID=\""), trim(F.col("Lnk_Main_Data.TreoMHG")), F.lit("\"")).alias("PRPR_ID"),
    F.lit("@pMEPR_MCTR_ORSN=\"OVRP\"").alias("OVR_RSN"),
    F.lit("@pMEPR_CREATE_GEN_GAP=\"Y\"").alias("GEN_GAP")
)
df_hf_pcmh_mms_mepr_create = dedup_sort(
    df_mepr_crt,
    ["SUB_ID","MBR_SFX_NO","KEY","RecType","UPDT_CD","PCP_TYP","EFF_DT","TERM_RSN","PRPR_ID","OVR_RSN","GEN_GAP"],
    []
)

cond_mepr_term = (
    (F.col("Lnk_Lkp_Sub.SUB_ID").isNotNull()) &
    (F.col("Lnk_Main_Data.TERM_MEPR") != "Null") & (F.col("Lnk_Main_Data.TERM_MEPR") != "x")
)
df_mepr_term = df_Xfm_BusRules2_stage.filter(cond_mepr_term).select(
    F.when(F.length(F.col("Lnk_Lkp_Sub.SUB_ID"))==9, F.col("Lnk_Lkp_Sub.SUB_ID")).otherwise(F.lit("0") + F.col("Lnk_Lkp_Sub.SUB_ID")).alias("SUB_ID"),
    F.col("Lnk_Lkp_Sub.MBR_SFX_NO"),
    F.lit("5").alias("KEY"),
    F.lit("@pRECTYPE=\"MEPR\"").alias("RecType"),
    F.lit("@pMEPR_UPDATE_CD=\"UP\"").alias("UPDT_CD"),
    F.lit("@pMEPR_PCP_TYPE=\"MR\"").alias("PCP_TYP"),
    F.concat(F.lit("@pMEPR_EFF_DT=\""), F.expr("FORMAT.DATE(Lnk_Main_Data.MR_EFF_DT,'DB2','DATE','MM/DD/CCYY')"), F.lit("\"")).alias("EFF_DT"),
    F.concat(F.lit("@pMEPR_TERM_DT=\""), F.expr("FORMAT.DATE(LstDayOfMonth,'DB2','DATE','MM/DD/CCYY')"), F.lit("\"")).alias("TERM_DT"),
    F.lit("@pMEPR_MCTR_TRSN=\"PCMT\"").alias("TERM_RSN"),
    F.concat(F.lit("@pPRPR_ID=\""), trim(F.col("Lnk_Main_Data.FacetsMR")), F.lit("\"")).alias("PRPR_ID"),
    F.lit("@pMEPR_CREATE_GEN_GAP=\"Y\"").alias("GEN_GAP")
)
df_hf_pcmh_mms_mepr_trm = dedup_sort(
    df_mepr_term,
    ["SUB_ID","MBR_SFX_NO","KEY","RecType","UPDT_CD","PCP_TYP","EFF_DT","TERM_DT","TERM_RSN","PRPR_ID","GEN_GAP"],
    []
)

cond_melc_crt = (
    (F.col("Lnk_Lkp_Sub.SUB_ID").isNotNull()) &
    (F.col("Lnk_Main_Data.CRT_MELC") != "Null") & (F.col("Lnk_Main_Data.CRT_MELC") != "x")
)
df_melc_crt = df_Xfm_BusRules2_stage.filter(cond_melc_crt).select(
    F.when(F.length(F.col("Lnk_Lkp_Sub.SUB_ID"))==9, F.col("Lnk_Lkp_Sub.SUB_ID")).otherwise(F.lit("0") + F.col("Lnk_Lkp_Sub.SUB_ID")).alias("SUB_ID"),
    F.col("Lnk_Lkp_Sub.MBR_SFX_NO"),
    F.lit("4").alias("KEY"),
    F.lit("@pRECTYPE=\"MELC\"").alias("REC_TYPE"),
    F.lit("@pMELC_UPDATE_CD=\"IN\"").alias("MELC_UPDT_CD"),
    F.concat(F.lit("@pMELC_EFF_DT=\""), F.expr("FORMAT.DATE(FrstDayOfMonth,'DB2','DATE','MM/DD/CCYY')"), F.lit("\"")).alias("MELC_EFF_DT"),
    F.concat(F.lit("@pCRFT_MCTR_LSTY=\""), trim(F.col("Lnk_Main_Data.RISK_CAT_ID")), F.lit("\"")).alias("CRFT_MCTR_LFSTYL_CD"),
    F.lit("@pMELC_CREATE_GEN_GAP=\"Y\"").alias("GEN_GAP")
)
df_hf_pcmh_mms_melc_create = dedup_sort(
    df_melc_crt,
    ["SUB_ID","MBR_SFX_NO","KEY","REC_TYPE","MELC_UPDT_CD","MELC_EFF_DT","CRFT_MCTR_LFSTYL_CD","GEN_GAP"],
    []
)

cond_melc_term = (
    (F.col("Lnk_Lkp_Sub.SUB_ID").isNotNull()) &
    (F.col("Lnk_Main_Data.TERM_MELC") != "Null") & (F.col("Lnk_Main_Data.TERM_MELC") != "x")
)
df_melc_term = df_Xfm_BusRules2_stage.filter(cond_melc_term).select(
    F.when(F.length(F.col("Lnk_Lkp_Sub.SUB_ID"))==9, F.col("Lnk_Lkp_Sub.SUB_ID")).otherwise(F.lit("0") + F.col("Lnk_Lkp_Sub.SUB_ID")).alias("SUB_ID"),
    F.col("Lnk_Lkp_Sub.MBR_SFX_NO"),
    F.lit("3").alias("KEY"),
    F.lit("@pRECTYPE=\"MELC\"").alias("REC_TYPE"),
    F.lit("@pMELC_UPDATE_CD=\"UP\"").alias("MELC_UPDT_CD"),
    F.concat(F.lit("@pMELC_EFF_DT=\""), F.expr("FORMAT.DATE(Lnk_Main_Data.MELC_EFF_DT,'DB2','DATE','MM/DD/CCYY')"), F.lit("\"")).alias("MELC_EFF_DT"),
    F.concat(F.lit("@pMELC_TERM_DT=\""), F.expr("FORMAT.DATE(LstDayOfMonth,'DB2','DATE','MM/DD/CCYY')"), F.lit("\"")).alias("MELC_TERM_DT"),
    F.concat(F.lit("@pCRFT_MCTR_LSTY=\""), trim(F.col("Lnk_Main_Data.SRC_CD")), F.lit("\"")).alias("CRFT_MCTR_LFSTYL_CD"),
    F.lit("@pMELC_CREATE_GEN_GAP=\"Y\"").alias("GEN_GAP")
)
df_hf_pcmh_mms_melc_trm = dedup_sort(
    df_melc_term,
    ["SUB_ID","MBR_SFX_NO","KEY","REC_TYPE","MELC_UPDT_CD","MELC_EFF_DT","MELC_TERM_DT","CRFT_MCTR_LFSTYL_CD","GEN_GAP"],
    []
)

cond_error_file = (
    (F.col("Lnk_Lkp_Sub.SUB_ID").isNull()) |
    (
       ((F.col("Lnk_Main_Data.ErrorCondition") != "Null") & (F.col("Lnk_Main_Data.ErrorCondition") != "x")) |
       ((F.col("Lnk_Main_Data.FacetsMR") == "Null") & (F.col("Lnk_Main_Data.TreoMHG") != "Null") & (F.col("Lnk_Main_Data.SRC_CD") != "Null") & (F.col("Lnk_Main_Data.RISK_CAT_ID") == "Null")) |
       ((F.col("Lnk_Main_Data.FacetsMR") == "Null") & (F.col("Lnk_Main_Data.TreoMHG") != "Null") & (F.col("Lnk_Main_Data.SRC_CD") == "Null") & (F.col("Lnk_Main_Data.RISK_CAT_ID") == "Null")) |
       ((F.col("Lnk_Main_Data.FacetsMR") != "Null") & (F.col("Lnk_Main_Data.TreoMHG") != "Null") & (F.col("Lnk_Main_Data.FacetsMR") == F.col("Lnk_Main_Data.TreoMHG")) & (F.col("Lnk_Main_Data.SRC_CD") != "Null") & (F.col("Lnk_Main_Data.RISK_CAT_ID") == "Null"))
    )
)
df_error_file = df_Xfm_BusRules2_stage.filter(cond_error_file).select(
    F.col("Lnk_Main_Data.MBR_SK_F").alias("MBR_ID"),
    F.when(
       (F.col("Lnk_Main_Data.TreoMHG") == "Null") & (F.col("Lnk_Main_Data.FacetsMR") != "Null"),
       F.col("Lnk_Main_Data.FacetsMR")
    ).when(
       (F.col("Lnk_Main_Data.FacetsMR") == "Null") & (F.col("Lnk_Main_Data.TreoMHG") != "Null"),
       F.col("Lnk_Main_Data.TreoMHG")
    ).when(
       (F.col("Lnk_Main_Data.TreoMHG") != "Null") & (F.col("Lnk_Main_Data.FacetsMR") != "Null"),
       F.lit("True")
    ).otherwise(F.lit("x")).alias("MBR_LOCATION"),
    F.when(F.col("Lnk_Main_Data.TERM_MELC") != "Null","Existing Melc Termed")
     .when(F.col("Lnk_Main_Data.TERM_MEPR") != "Null","Existing Mepr Termed")
     .otherwise("No Risk Factor found for Member").alias("Message")
)
df_hf_pcmh_mms_error_file = dedup_sort(
    df_error_file,
    ["MBR_ID","MBR_LOCATION","Message"],
    []
)

################################################################################
# Final file writes
################################################################################

write_files(
    df_hf_pcmh_mms_error_file.select(
        F.rpad("MBR_ID", 10, " ").alias("MBR_ID"),
        F.rpad("MBR_LOCATION", 50, " ").alias("MBR_LOCATION"),
        F.rpad("Message", 40, " ").alias("Message")
    ),
    f"{adls_path_publish}/external/Mms_Pcmh_Error_File.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_meme_dtl.select(
        F.rpad("SUB_ID", 20, " ").alias("SUB_ID"),
        F.rpad("MBR_SFX_NO", 20, " ").alias("MBR_SFX_NO"),
        F.rpad("KEY", 1, " ").alias("KEY"),
        F.rpad("RecType", 20, " ").alias("RecType"),
        F.rpad("UPDT_CD", 20, " ").alias("UPDT_CD"),
        F.rpad("MBR_SFX", 20, " ").alias("MBR_SFX")
    ),
    f"{adls_path_publish}/external/Mms_Pcmh_Ppo_MEME_Detail.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_mepr_create.select(
        F.rpad("SUB_ID", 30, " ").alias("SUB_ID"),
        F.rpad("MBR_SFX_NO", 30, " ").alias("MBR_SFX_NO"),
        F.rpad("KEY", 1, " ").alias("KEY"),
        F.rpad("RecType", 30, " ").alias("RecType"),
        F.rpad("UPDT_CD", 30, " ").alias("UPDT_CD"),
        F.rpad("PCP_TYP", 30, " ").alias("PCP_TYP"),
        F.rpad("EFF_DT", 30, " ").alias("EFF_DT"),
        F.rpad("TERM_RSN", 30, " ").alias("TERM_RSN"),
        F.rpad("PRPR_ID", 30, " ").alias("PRPR_ID"),
        F.rpad("OVR_RSN", 30, " ").alias("OVR_RSN"),
        F.rpad("GEN_GAP", 30, " ").alias("GEN_GAP")
    ),
    f"{adls_path_publish}/external/Mms_Pcmh_Ppo_MEPR_CRT_Detail.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_mepr_trm.select(
        F.rpad("SUB_ID", 20, " ").alias("SUB_ID"),
        F.rpad("MBR_SFX_NO", 20, " ").alias("MBR_SFX_NO"),
        F.rpad("KEY", 1, " ").alias("KEY"),
        F.rpad("RecType", 20, " ").alias("RecType"),
        F.rpad("UPDT_CD", 20, " ").alias("UPDT_CD"),
        F.rpad("PCP_TYP", 20, " ").alias("PCP_TYP"),
        F.rpad("EFF_DT", 20, " ").alias("EFF_DT"),
        F.rpad("TERM_DT", 20, " ").alias("TERM_DT"),
        F.rpad("TERM_RSN", 20, " ").alias("TERM_RSN"),
        F.rpad("PRPR_ID", 20, " ").alias("PRPR_ID"),
        F.rpad("GEN_GAP", 30, " ").alias("GEN_GAP")
    ),
    f"{adls_path_publish}/external/Mms_Pcmh_Ppo_MEPR_TERM_Detail.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_melc_create.select(
        F.rpad("SUB_ID", 30, " ").alias("SUB_ID"),
        F.rpad("MBR_SFX_NO", 30, " ").alias("MBR_SFX_NO"),
        F.rpad("KEY", 1, " ").alias("KEY"),
        F.rpad("REC_TYPE", 30, " ").alias("REC_TYPE"),
        F.rpad("MELC_UPDT_CD", 30, " ").alias("MELC_UPDT_CD"),
        F.rpad("MELC_EFF_DT", 30, " ").alias("MELC_EFF_DT"),
        F.rpad("CRFT_MCTR_LFSTYL_CD", 30, " ").alias("CRFT_MCTR_LFSTYL_CD"),
        F.rpad("GEN_GAP", 30, " ").alias("GEN_GAP")
    ),
    f"{adls_path_publish}/external/Mms_Pcmh_Ppo_MELC_CRT_Detail.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_melc_trm.select(
        F.rpad("SUB_ID", 30, " ").alias("SUB_ID"),
        F.rpad("MBR_SFX_NO", 30, " ").alias("MBR_SFX_NO"),
        F.rpad("KEY", 1, " ").alias("KEY"),
        F.rpad("REC_TYPE", 30, " ").alias("REC_TYPE"),
        F.rpad("MELC_UPDT_CD", 30, " ").alias("MELC_UPDT_CD"),
        F.rpad("MELC_EFF_DT", 30, " ").alias("MELC_EFF_DT"),
        F.rpad("MELC_TERM_DT", 30, " ").alias("MELC_TERM_DT"),
        F.rpad("CRFT_MCTR_LFSTYL_CD", 30, " ").alias("CRFT_MCTR_LFSTYL_CD"),
        F.rpad("GEN_GAP", 30, " ").alias("GEN_GAP")
    ),
    f"{adls_path_publish}/external/Mms_Pcmh_Ppo_MELC_TERM_Detail.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_sbsb_dtl.select(
        F.rpad("SUB_ID", 20, " ").alias("SUB_ID"),
        F.rpad("MBR_SFX_NO", 20, " ").alias("MBR_SFX_NO"),
        F.rpad("KEY", 1, " ").alias("KEY"),
        F.rpad("RecType", 20, " ").alias("RecType"),
        F.rpad("FMLY_UPDT_CD", 20, " ").alias("FMLY_UPDT_CD"),
        F.rpad("SBSB_ID", 20, " ").alias("SBSB_ID"),
        F.rpad("GRGR_ID", 20, " ").alias("GRGR_ID")
    ),
    f"{adls_path_publish}/external/Mms_Pcmh_Ppo_SBSB_Detail.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="000",
    nullValue=None
)

write_files(
    df_hf_pcmh_mbr_sbsb_id.select(
        F.col("MBR_SK"),
        F.col("MBR_UNIQ_KEY"),
        F.col("SUB_ID"),
        F.col("MBR_SFX_NO"),
        F.col("GRP_ID")
    ),
    "hf_pcmh_mbr_sbsb_id.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mbr_host_in.select(
        F.col("MBR_SK"),
        F.col("HOST_MBR_IN")
    ),
    "hf_pcmh_mbr_host_in.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_host_attrbtn_hist.select(
        F.col("MBR_SK"),
        F.col("MBR_UNIQ_KEY")
    ),
    "hf_host_attrbtn_hist.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_mr_nomhg,
    "hf_pcmh_mms_mr_nomhg.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_mhg_mr,
    "hf_pcmh_mms_mhg_mr.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_mhg_nomr,
    "hf_pcmh_mms_mhg_nomr.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_notreomhg,
    "hf_pcmh_mms_notreomhg.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_mepr_final,
    "hf_pcmh_mms_mepr_final.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_keepmr_null,
    "hf_pcmh_keepmr_null.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_keepmr_not_null,
    "hf_pcmh_keepmr_not_null.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mepr_not_null,
    "hf_pcmh_mepr_not_null.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_melc_mr_crt_trm,
    "hf_pcmh_melc_mr_crt_trm.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_melc_mr_crt_trm_hold,
    "hf_pcmh_melc_mr_crt_trm_hold.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_error_file,
    "hf_pcmh_mms_error_file.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_meme_dtl,
    "hf_pcmh_mms_meme_dtl.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_mepr_create,
    "hf_pcmh_mms_mepr_create.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_mepr_trm,
    "hf_pcmh_mms_mepr_trm.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_melc_create,
    "hf_pcmh_mms_melc_create.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_melc_trm,
    "hf_pcmh_mms_melc_trm.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

write_files(
    df_hf_pcmh_mms_sbsb_dtl,
    "hf_pcmh_mms_sbsb_dtl.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)