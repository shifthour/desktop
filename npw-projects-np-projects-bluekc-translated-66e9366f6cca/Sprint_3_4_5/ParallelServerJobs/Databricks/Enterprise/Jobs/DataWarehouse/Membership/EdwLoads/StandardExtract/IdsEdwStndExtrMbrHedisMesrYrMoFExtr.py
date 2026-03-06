# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 - 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:  IdsEdwStndExtrMbrHedisMesrYrMoFExtr
# MAGIC CALLED BY : IdsEdwStndExtrMbrHedisMesrYrMoFSeq
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                This Job extracts Data from IDS and creates a Load file for EDW Tables MBR_HEDIS_MESR_YTD_F & MBR_HEDIS_MESR_ROLL_12_MO_F  
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                              Project/Altiris #                                        Change Description                                                                Development Project                                              Code Reviewer            Date Reviewed
# MAGIC ==========================================================================================================================================================================================================
# MAGIC Karthik Chintalapani         2021-02-10                US 341503                                           Original Programming                                                                        EnterpriseDev1                                                       Jaideep Mankala          02/12/2021
# MAGIC 
# MAGIC Reddy Sanam                 2023-08-28                 US590640                Updated with the new table definition for the table-MBR_HEDIS_MESR_YTD_F
# MAGIC                                                                                                             Added Lookups to retrieve Race Cd, Race Source,Race Nm, Race Name Source,   EnterpriseDev1                                                       Goutham Kalidindi         9/5/2023
# MAGIC                                                                                                             Ethnicity cd, Ethnicity Name, Ethnicity code Source, Ethnicity Name Source
# MAGIC 
# MAGIC Reddy Sanam               2023-09-13                  US590640                  In the stage "MBR_PCP_ATTRBTN" the sql is split to another sql since it was                                                                                          Goutham Kalidindi         9/13/2023
# MAGIC                                                                                                               hanging prod. This will reduce the number of rows retrieved and will run the job
# MAGIC                                                                                                              in less time                                                                                                                 EnterpriseDev1
# MAGIC 
# MAGIC Reddy Sanam             2023-10-27                    US598689                  Updated the derivation for the stage variables svRaceCd,svCmplncCat to                EnterpriseDev2				Ken Bradmon	2023-10-27
# MAGIC                                                                                                             to Flag Y and N accordingly
# MAGIC 
# MAGIC Reddy Sanam            2024-01-25                    US606288                  Changed datatype for field UNIT_CT to Decimal(13,2)                                                EnterpriseDev2                                                         Goutham Kalidindi       1/30/2024
# MAGIC 
# MAGIC Reddy Sanam            2024-03-18                     US613396                 Changed the Mapping for field EXCL_CT to direct mapping from source                      EnterpriseDev2                                                        Goutham Kalidindi       3/19/2024
# MAGIC 
# MAGIC Reddy Sanam           2024-10-23                      US631537                 Added MBR_GNDR_CD and propagated                                                                       EnterpriseDev2                                                       Goutham Kalidindi     10/29/2024

# MAGIC When Retrieving the data from attribution table(MBR_PCP_ATTRBTN for the current attrbution month, The query is taking longer in prod, so did the split intot a lookup so that, the rows correspoding to current attribution month will be used
# MAGIC Job Name: IdsEdwMbrHedisMesrYrMoExtr
# MAGIC Add Defaults and Null Handling.
# MAGIC Read most recent Data from IDS MBR_HEDIS_MESR_YR_MO table
# MAGIC Write MBR_HEDIS_MESR_YTD_F Data into a Sequential file for Load Job IdsEdwMbrHedisMesrYTDFLoad.
# MAGIC Added this section to retrieve the translations for Race, Race_Src, Race_Src_Nm,Ethnicity, Ethnicity_src,Ethnicity_Src_Nm, and Compliance Code category code lookup. Since all of them are under single source system. Single query is used and the data is later split based on the src domain.
# MAGIC Added this lookup to translate Hedis Meause Gaps in Care Measure sk to code and name
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

# Retrieve job parameters
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
EDWOwner = get_widget_value('EDWOwner', '')
edw_secret_name = get_widget_value('edw_secret_name', '')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate', '')
EDWRunCycle = get_widget_value('EDWRunCycle', '')
IDSRunCycle = get_widget_value('IDSRunCycle', '')

# --------------------------------------------------------------------------------
# db2_MBR_HEDIS_MESR_YR_MO_in (DB2ConnectorPX) - from IDS
# --------------------------------------------------------------------------------
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_db2_MBR_HEDIS_MESR_YR_MO_in = f"""
select 
HEDIS.MBR_HEDIS_MESR_YR_MO_SK, 
HEDIS.HEDIS_RVW_SET_NM,
HEDIS.HEDIS_POP_NM,
HEDIS.HEDIS_MESR_NM,
HEDIS.HEDIS_SUB_MESR_NM,
HEDIS.MBR_UNIQ_KEY,
HEDIS.HEDIS_MBR_BUCKET_ID,
HEDIS.BASE_EVT_EPSD_DT_SK,
HEDIS.ACTVTY_YR_NO,
HEDIS.ACTVTY_MO_NO,
HEDIS.SRC_SYS_CD,
HEDIS.cRT_RUN_CYC_EXCTN_SK,
HEDIS.LAST_UPDT_RUN_CYC_EXCTN_SK,
HEDIS.MBR_SK,
HEDIS.PROD_SH_NM_SK,
HEDIS.CMPLNC_ADMIN_IN,
HEDIS.CMPLNC_MNL_DATA_IN,
HEDIS.CMPLNC_MNL_DATA_SMPL_POP_IN,
HEDIS.CMPLNC_SMPL_POP_IN,
HEDIS.CONTRAIN_IN,
HEDIS.CONTRAIN_SMPL_POP_IN,
HEDIS.EXCL_IN,
HEDIS.EXCL_SMPL_POP_IN,
HEDIS.MESR_ELIG_IN,
HEDIS.MESR_ELIG_SMPL_POP_IN,
HEDIS.EVT_CT,
HEDIS.UNIT_CT,
HEDIS.ACRDTN_CAT_ID,
HEDIS.GRP_SK,
HEDIS.HEDIS_RVW_SET_SK,
HEDIS.HEDIS_POP_SK,
HEDIS.HEDIS_MESR_SK,
HEDIS.HEDIS_RVW_SET_END_DT,
HEDIS.CST_AMT,
HEDIS.RISK_PCT,
HEDIS.ADJ_RISK_NO,
HEDIS.PLN_ALL_CAUSE_READMSN_VRNC_NO,
HEDIS.MBR_BRTH_DT_SK,
HEDIS.CNTY_AREA_ID,
HEDIS.GRP_ID,
HEDIS.HEDIS_MESR_ABBR_ID,
HEDIS.MBR_ID,
HEDIS.MBR_INDV_BE_KEY,
HEDIS.MBR_FULL_NM,
HEDIS.ON_OFF_EXCH_ID,
HEDIS.PROD_SH_NM,
HEDIS.ETHNCTY_CD_SK,
HEDIS.ETHNCTY_SRC_CD_SK,
HEDIS.HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK,
HEDIS.NUMERATOR_CMPLNC_CAT_CD_SK,
HEDIS.RACE_CD_SK,
HEDIS.RACE_SRC_CD_SK,
HEDIS.SES_STRAT_NO,
HEDIS.ADV_ILNS_AND_FRAILTY_EXCL_CT,
HEDIS.DCSD_EXCL_CT,
HEDIS.ENR_MBR_DENOMINATOR_CT,
HEDIS.EXCL_CT,
HEDIS.HSPC_EXCL_CT,
HEDIS.LTI_SNP_EXCL_CT,
HEDIS.OPTNL_EXCL_CT,
HEDIS.RQRD_EXCL_CT,
HEDIS.PROD_ID,
HEDIS.PROD_LN_TYP_DESC,
HEDIS.PROD_ROLLUP_ID,
HEDIS.NUMERATOR_EVT_1_SVC_DT,
HEDIS.LAB_TST_VAL_NO, 
HEDIS.GAPS_IN_CARE_STR,
HEDIS.MBR_GNDR_CD
from
(
SELECT 
YR_MO.MBR_HEDIS_MESR_YR_MO_SK,
YR_MO.ACTVTY_YR_NO||'-'||CASE LENGTH(CAST(YR_MO.ACTVTY_MO_NO AS VARCHAR(4)))
 WHEN 1 THEN '0'|| CAST(YR_MO.ACTVTY_MO_NO AS VARCHAR(4))
 ELSE cast(YR_MO.ACTVTY_MO_NO as varchar(4))
END ||'-'||'01' as ROW_EFF_DT_SK,
YR_MO.HEDIS_RVW_SET_NM,
YR_MO.HEDIS_POP_NM,
YR_MO.HEDIS_MESR_NM,
YR_MO.HEDIS_SUB_MESR_NM,
YR_MO.MBR_UNIQ_KEY,
YR_MO.HEDIS_MBR_BUCKET_ID,
YR_MO.BASE_EVT_EPSD_DT_SK,
YR_MO.ACTVTY_YR_NO,
YR_MO.ACTVTY_MO_NO,
YR_MO.SRC_SYS_CD,
YR_MO.cRT_RUN_CYC_EXCTN_SK,
YR_MO.LAST_UPDT_RUN_CYC_EXCTN_SK,
YR_MO.MBR_SK,
YR_MO.PROD_SH_NM_SK,
YR_MO.CMPLNC_ADMIN_IN,
YR_MO.CMPLNC_MNL_DATA_IN,
YR_MO.CMPLNC_MNL_DATA_SMPL_POP_IN,
YR_MO.CMPLNC_SMPL_POP_IN,
YR_MO.CONTRAIN_IN,
YR_MO.CONTRAIN_SMPL_POP_IN,
YR_MO.EXCL_IN,
YR_MO.EXCL_SMPL_POP_IN,
YR_MO.MESR_ELIG_IN,
YR_MO.MESR_ELIG_SMPL_POP_IN,
YR_MO.EVT_CT,
YR_MO.UNIT_CT,
YR_MO.ACRDTN_CAT_ID,
YR_MO.GRP_SK,
YR_MO.HEDIS_RVW_SET_SK,
YR_MO.HEDIS_POP_SK,
YR_MO.HEDIS_MESR_SK,
RVW_SET.HEDIS_RVW_SET_END_DT,
YR_MO.CST_AMT,
YR_MO.RISK_PCT,
YR_MO.ADJ_RISK_NO,
YR_MO.PLN_ALL_CAUSE_READMSN_VRNC_NO,
YR_MO.MBR_BRTH_DT_SK,
YR_MO.CNTY_AREA_ID,
YR_MO.GRP_ID,
YR_MO.HEDIS_MESR_ABBR_ID,
YR_MO.MBR_ID,
YR_MO.MBR_INDV_BE_KEY,
YR_MO.MBR_FULL_NM,
YR_MO.ON_OFF_EXCH_ID,
YR_MO.PROD_SH_NM,
YR_MO.ETHNCTY_CD_SK,
YR_MO.ETHNCTY_SRC_CD_SK,
YR_MO.HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK,
YR_MO.NUMERATOR_CMPLNC_CAT_CD_SK,
YR_MO.RACE_CD_SK,
YR_MO.RACE_SRC_CD_SK,
YR_MO.SES_STRAT_NO,
YR_MO.ADV_ILNS_AND_FRAILTY_EXCL_CT,
YR_MO.DCSD_EXCL_CT,
YR_MO.ENR_MBR_DENOMINATOR_CT,
YR_MO.EXCL_CT,
YR_MO.HSPC_EXCL_CT,
YR_MO.LTI_SNP_EXCL_CT,
YR_MO.OPTNL_EXCL_CT,
YR_MO.RQRD_EXCL_CT,
YR_MO.PROD_ID,
YR_MO.PROD_LN_TYP_DESC,
YR_MO.PROD_ROLLUP_ID,
YR_MO.NUMERATOR_EVT_1_SVC_DT,
YR_MO.LAB_TST_VAL_NO,
CASE 
  WHEN PROD_LN_TYP_DESC = 'Commercial' THEN 'HEDIS_GAPS_COM'
  WHEN PROD_LN_TYP_DESC = 'Medicare' THEN 'HEDIS_GAPS_MA'
  WHEN PROD_LN_TYP_DESC = 'Exchange' THEN 'HEDIS_GAPS_ACA'
  ELSE '0'
END AS GAPS_IN_CARE_STR,
YR_MO.MBR_GNDR_CD
FROM {IDSOwner}.MBR_HEDIS_MESR_YR_MO YR_MO,
{IDSOwner}.HEDIS_RVW_SET RVW_SET
where RVW_SET.HEDIS_RVW_SET_SK=YR_MO.HEDIS_RVW_SET_SK
) HEDIS
"""
df_db2_MBR_HEDIS_MESR_YR_MO_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_MBR_HEDIS_MESR_YR_MO_in)
    .load()
)

# --------------------------------------------------------------------------------
# P_SEL_PRCS_CRITR (DB2ConnectorPX) - from EDW
# --------------------------------------------------------------------------------
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
extract_query_P_SEL_PRCS_CRITR = f"""
SELECT CRITR_VAL_FROM_TX, CRITR_VAL_THRU_TX, SEL_PRCS_ITEM_ID 
FROM {EDWOwner}.P_SEL_PRCS_CRITR
WHERE SEL_PRCS_ID = 'HEDIS_GAPS'
GROUP BY CRITR_VAL_FROM_TX, CRITR_VAL_THRU_TX, SEL_PRCS_ITEM_ID
"""
df_P_SEL_PRCS_CRITR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_P_SEL_PRCS_CRITR)
    .load()
)

# --------------------------------------------------------------------------------
# MBR_PCP_ATTRBTN (DB2ConnectorPX) - from IDS
# --------------------------------------------------------------------------------
extract_query_MBR_PCP_ATTRBTN = f"""
SELECT PCP.MBR_SK as MBR_SK,
PCP.PROV_SK as PROV_SK,
PCP.MBR_PCP_ATTRBTN_SK as MBR_PCP_ATTRBTN_SK,
PCP.ROW_EFF_DT_SK
FROM {IDSOwner}.MBR_PCP_ATTRBTN PCP,
{IDSOwner}.CD_MPPNG MPPNG
WHERE MPPNG.CD_MPPNG_SK=PCP.SRC_SYS_CD_SK  
  AND MPPNG.SRC_CD='BCBSKC'
  AND year(PCP.ROW_EFF_DT_SK) = case when month(to_date('{EDWRunCycleDate}','yyyy-mm-dd')) = 12 
                                     then year(to_date('{EDWRunCycleDate}','yyyy-mm-dd'))-1 
                                     else year(to_date('{EDWRunCycleDate}','yyyy-mm-dd')) end
"""
df_MBR_PCP_ATTRBTN = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MBR_PCP_ATTRBTN)
    .load()
)

# --------------------------------------------------------------------------------
# MBR_HEDIS_MESR_YR_MO_AttrDt (DB2ConnectorPX) - from IDS
# --------------------------------------------------------------------------------
extract_query_MBR_HEDIS_MESR_YR_MO_AttrDt = f"""
select cast(Max(ACTVTY_YR_NO||'-'||CASE LENGTH(CAST(ACTVTY_MO_NO AS VARCHAR(4)))
WHEN 1 THEN '0'|| CAST(ACTVTY_MO_NO AS VARCHAR(4))
ELSE cast(ACTVTY_MO_NO as varchar(4)) END ||'-'||'01') as char(10)) as attr_dt
from {IDSOwner}.MBR_HEDIS_MESR_YR_MO
where ACTVTY_YR_NO not in (0,1)
"""
df_MBR_HEDIS_MESR_YR_MO_AttrDt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_MBR_HEDIS_MESR_YR_MO_AttrDt)
    .load()
)

# --------------------------------------------------------------------------------
# Lkp_AttrDt (PxLookup) joining df_MBR_PCP_ATTRBTN (primary) with df_MBR_HEDIS_MESR_YR_MO_AttrDt (lookup, inner)
# Condition: LKP_Attr_Dt.ROW_EFF_DT_SK = Ref_Attr_Dt.attr_dt
# Output: MBR_SK, PROV_SK, MBR_PCP_ATTRBTN_SK, ROW_EFF_DT_SK
# --------------------------------------------------------------------------------
df_Lkp_AttrDt = (
    df_MBR_PCP_ATTRBTN.alias("LKP_Attr_Dt")
    .join(
        df_MBR_HEDIS_MESR_YR_MO_AttrDt.alias("Ref_Attr_Dt"),
        F.col("LKP_Attr_Dt.ROW_EFF_DT_SK") == F.col("Ref_Attr_Dt.attr_dt"),
        "inner"
    )
    .select(
        F.col("LKP_Attr_Dt.MBR_SK").alias("MBR_SK"),
        F.col("LKP_Attr_Dt.PROV_SK").alias("PROV_SK"),
        F.col("LKP_Attr_Dt.MBR_PCP_ATTRBTN_SK").alias("MBR_PCP_ATTRBTN_SK"),
        F.col("LKP_Attr_Dt.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK")
    )
)

# --------------------------------------------------------------------------------
# CD_MPPNG (DB2ConnectorPX) - from IDS
# --------------------------------------------------------------------------------
extract_query_CD_MPPNG = f"""
select CD_MPPNG_SK,SRC_DOMAIN_NM,TRGT_CD_NM,TRGT_CD 
from {IDSOwner}.CD_MPPNG
where SRC_SYS_CD = 'COTIVITI'
and SRC_DOMAIN_NM IN ( 'HEDIS RACE SOURCE','RACE','HEDIS COMPLIANCE CATEGORY CODE')
group by CD_MPPNG_SK,SRC_DOMAIN_NM,TRGT_CD_NM,TRGT_CD
UNION ALL
select CD_MPPNG_SK,SRC_DOMAIN_NM,TRGT_CD_NM,TRGT_CD 
from {IDSOwner}.CD_MPPNG
where CD_MPPNG_SK IN (0,1)
"""
df_CD_MPPNG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MPPNG)
    .load()
)

# --------------------------------------------------------------------------------
# SplitType (CTransformerStage) => creates multiple outputs based on constraints
# --------------------------------------------------------------------------------
df_splitType = (
    df_CD_MPPNG
    .withColumn(
        "svRaceCd",
        F.when(
            F.col("SRC_DOMAIN_NM").isin("RACE", "UNK", "NA"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svRaceSrc",
        F.when(
            F.col("SRC_DOMAIN_NM").isin("HEDIS RACE SOURCE", "UNK", "NA"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "svCmplncCat",
        F.when(
            F.col("SRC_DOMAIN_NM").isin("HEDIS COMPLIANCE CATEGORY CODE", "UNK", "NA"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
)

df_Ref_Entn = (
    df_splitType
    .filter(F.col("svRaceCd") == "Y")
    .select(
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    )
)

df_Ref_EthnSrc = (
    df_splitType
    .filter(F.col("svRaceSrc") == "Y")
    .select(
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    )
)

df_Ref_Race = (
    df_splitType
    .filter(F.col("svRaceCd") == "Y")
    .select(
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    )
)

df_Ref_RaceSrc = (
    df_splitType
    .filter(F.col("svRaceSrc") == "Y")
    .select(
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    )
)

df_Ref_CmplcCat = (
    df_splitType
    .filter(F.col("svCmplncCat") == "Y")
    .select(
        F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
        F.col("TRGT_CD").alias("TRGT_CD"),
        F.col("TRGT_CD_NM").alias("TRGT_CD_NM"),
    )
)

# --------------------------------------------------------------------------------
# Lkp_FKey (PxLookup) => multiple left joins of df_db2_MBR_HEDIS_MESR_YR_MO_in with the above refs & PRCS_CRITR
# --------------------------------------------------------------------------------
df_Lkp_FKey = (
    df_db2_MBR_HEDIS_MESR_YR_MO_in.alias("M")
    .join(df_Ref_Entn.alias("Ref_Entn"), F.col("M.ETHNCTY_CD_SK") == F.col("Ref_Entn.CD_MPPNG_SK"), "left")
    .join(df_Ref_EthnSrc.alias("Ref_EthnSrc"), F.col("M.ETHNCTY_SRC_CD_SK") == F.col("Ref_EthnSrc.CD_MPPNG_SK"), "left")
    .join(df_Ref_Race.alias("Ref_Race"), F.col("M.RACE_CD_SK") == F.col("Ref_Race.CD_MPPNG_SK"), "left")
    .join(df_Ref_RaceSrc.alias("Ref_RaceSrc"), F.col("M.RACE_SRC_CD_SK") == F.col("Ref_RaceSrc.CD_MPPNG_SK"), "left")
    .join(df_Ref_CmplcCat.alias("Ref_CmplcCat"), F.col("M.NUMERATOR_CMPLNC_CAT_CD_SK") == F.col("Ref_CmplcCat.CD_MPPNG_SK"), "left")
    .join(
        df_P_SEL_PRCS_CRITR.alias("PRCS_CRITR"),
        [
            F.col("M.HEDIS_MESR_ABBR_ID") == F.col("PRCS_CRITR.CRITR_VAL_FROM_TX"),
            F.col("M.GAPS_IN_CARE_STR") == F.col("PRCS_CRITR.SEL_PRCS_ITEM_ID")
        ],
        "left"
    )
    .select(
        F.col("M.MBR_HEDIS_MESR_YR_MO_SK").alias("MBR_HEDIS_MESR_YR_MO_SK"),
        F.col("M.HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
        F.col("M.HEDIS_POP_NM").alias("HEDIS_POP_NM"),
        F.col("M.HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
        F.col("M.HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
        F.col("M.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("M.HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
        F.col("M.BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
        F.col("M.ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
        F.col("M.ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
        F.col("M.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("M.GRP_SK").alias("GRP_SK"),
        F.col("M.HEDIS_MESR_SK").alias("HEDIS_MESR_SK"),
        F.col("M.HEDIS_POP_SK").alias("HEDIS_POP_SK"),
        F.col("M.HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
        F.col("M.MBR_SK").alias("MBR_SK"),
        F.col("M.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("Ref_Entn.TRGT_CD").alias("ETHNCTY_CD"),
        F.col("Ref_Entn.TRGT_CD_NM").alias("ETHNCTY_NM"),
        F.col("Ref_EthnSrc.TRGT_CD").alias("ETHNCTY_SRC_CD"),
        F.col("Ref_EthnSrc.TRGT_CD_NM").alias("ETHNCTY_SRC_NM"),
        F.col("Ref_CmplcCat.TRGT_CD").alias("NUMERATOR_CMPLNC_CAT_CD"),
        F.col("Ref_CmplcCat.TRGT_CD_NM").alias("NUMERATOR_CMPLNC_CAT_NM"),
        F.col("Ref_Race.TRGT_CD").alias("RACE_CD"),
        F.col("Ref_Race.TRGT_CD_NM").alias("RACE_NM"),
        F.col("Ref_RaceSrc.TRGT_CD").alias("RACE_SRC_CD"),
        F.col("Ref_RaceSrc.TRGT_CD_NM").alias("RACE_SRC_NM"),
        F.col("M.CMPLNC_ADMIN_IN").alias("CMPLNC_ADMIN_IN"),
        F.col("M.CMPLNC_MNL_DATA_IN").alias("CMPLNC_MNL_DATA_IN"),
        F.col("M.CMPLNC_MNL_DATA_SMPL_POP_IN").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
        F.col("M.CMPLNC_SMPL_POP_IN").alias("CMPLNC_SMPL_POP_IN"),
        F.col("M.CONTRAIN_IN").alias("CONTRAIN_IN"),
        F.col("M.CONTRAIN_SMPL_POP_IN").alias("CONTRAIN_SMPL_POP_IN"),
        F.col("M.EXCL_IN").alias("EXCL_IN"),
        F.col("M.EXCL_SMPL_POP_IN").alias("EXCL_SMPL_POP_IN"),
        F.col("M.MESR_ELIG_IN").alias("MESR_ELIG_IN"),
        F.col("M.MESR_ELIG_SMPL_POP_IN").alias("MESR_ELIG_SMPL_POP_IN"),
        F.col("M.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("M.ADJ_RISK_NO").alias("ADJ_RISK_NO"),
        F.col("M.CST_AMT").alias("CST_AMT"),
        F.col("M.PLN_ALL_CAUSE_READMSN_VRNC_NO").alias("PLN_ALL_CAUSE_READMSN_VRNC_NO"),
        F.col("M.RISK_PCT").alias("RISK_PCT"),
        F.col("M.SES_STRAT_NO").alias("SES_STRAT_NO"),
        F.col("M.ADV_ILNS_AND_FRAILTY_EXCL_CT").alias("ADV_ILNS_AND_FRAILTY_EXCL_CT"),
        F.col("M.DCSD_EXCL_CT").alias("DCSD_EXCL_CT"),
        F.col("M.ENR_MBR_DENOMINATOR_CT").alias("ENR_MBR_DENOMINATOR_CT"),
        F.col("M.EVT_CT").alias("EVT_CT"),
        F.col("M.EXCL_CT").alias("EXCL_CT"),
        F.col("M.HSPC_EXCL_CT").alias("HSPC_EXCL_CT"),
        F.col("M.LTI_SNP_EXCL_CT").alias("LTI_SNP_EXCL_CT"),
        F.col("M.OPTNL_EXCL_CT").alias("OPTNL_EXCL_CT"),
        F.col("M.RQRD_EXCL_CT").alias("RQRD_EXCL_CT"),
        F.col("M.UNIT_CT").alias("UNIT_CT"),
        F.col("M.ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
        F.col("M.CNTY_AREA_ID").alias("CNTY_AREA_ID"),
        F.col("M.GRP_ID").alias("GRP_ID"),
        F.col("M.HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
        F.col("M.MBR_FULL_NM").alias("MBR_FULL_NM"),
        F.col("M.MBR_ID").alias("MBR_ID"),
        F.col("M.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
        F.col("M.ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
        F.col("M.PROD_ID").alias("PROD_ID"),
        F.col("M.PROD_LN_TYP_DESC").alias("PROD_LN_TYP_DESC"),
        F.col("M.PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
        F.col("M.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("M.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("M.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("M.ETHNCTY_CD_SK").alias("ETHNCTY_CD_SK"),
        F.col("M.ETHNCTY_SRC_CD_SK").alias("ETHNCTY_SRC_CD_SK"),
        F.col("M.HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK").alias("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK"),
        F.col("M.NUMERATOR_CMPLNC_CAT_CD_SK").alias("NUMERATOR_CMPLNC_CAT_CD_SK"),
        F.col("M.RACE_CD_SK").alias("RACE_CD_SK"),
        F.col("M.RACE_SRC_CD_SK").alias("RACE_SRC_CD_SK"),
        F.col("M.NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
        F.col("M.LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
        F.col("PRCS_CRITR.CRITR_VAL_THRU_TX").alias("CRITR_VAL_THRU_TX"),
        F.col("PRCS_CRITR.SEL_PRCS_ITEM_ID").alias("SEL_PRCS_ITEM_ID"),
        F.col("M.MBR_GNDR_CD").alias("MBR_GNDR_CD")
    )
)

# --------------------------------------------------------------------------------
# Att_Prov_Attr_SK (PxJoin) => leftouterjoin on MBR_SK
# --------------------------------------------------------------------------------
df_Att_Prov_Attr_SK = (
    df_Lkp_FKey.alias("lnk_IdsEdwMbrHedisMesrYrMo")
    .join(
        df_Lkp_AttrDt.alias("Ref_Prov_Sk_Attr_Sk"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_SK") == F.col("Ref_Prov_Sk_Attr_Sk.MBR_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_HEDIS_MESR_YR_MO_SK").alias("MBR_HEDIS_MESR_YR_MO_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_POP_NM").alias("HEDIS_POP_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.BASE_EVT_EPSD_DT_SK").alias("BASE_EVT_EPSD_DT_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_MESR_SK").alias("HEDIS_MESR_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_POP_SK").alias("HEDIS_POP_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
        F.col("Ref_Prov_Sk_Attr_Sk.MBR_PCP_ATTRBTN_SK").alias("MBR_PCP_ATTRBTN_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_SK").alias("MBR_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
        F.col("Ref_Prov_Sk_Attr_Sk.PROV_SK").alias("PROV_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ETHNCTY_CD").alias("ETHNCTY_CD"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ETHNCTY_NM").alias("ETHNCTY_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ETHNCTY_SRC_CD").alias("ETHNCTY_SRC_CD"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ETHNCTY_SRC_NM").alias("ETHNCTY_SRC_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.NUMERATOR_CMPLNC_CAT_CD").alias("NUMERATOR_CMPLNC_CAT_CD"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.NUMERATOR_CMPLNC_CAT_NM").alias("NUMERATOR_CMPLNC_CAT_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.RACE_CD").alias("RACE_CD"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.RACE_NM").alias("RACE_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.RACE_SRC_CD").alias("RACE_SRC_CD"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.RACE_SRC_NM").alias("RACE_SRC_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CMPLNC_ADMIN_IN").alias("CMPLNC_ADMIN_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CMPLNC_MNL_DATA_IN").alias("CMPLNC_MNL_DATA_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CMPLNC_MNL_DATA_SMPL_POP_IN").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CMPLNC_SMPL_POP_IN").alias("CMPLNC_SMPL_POP_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CONTRAIN_IN").alias("CONTRAIN_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CONTRAIN_SMPL_POP_IN").alias("CONTRAIN_SMPL_POP_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.EXCL_IN").alias("EXCL_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.EXCL_SMPL_POP_IN").alias("EXCL_SMPL_POP_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MESR_ELIG_IN").alias("MESR_ELIG_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MESR_ELIG_SMPL_POP_IN").alias("MESR_ELIG_SMPL_POP_IN"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_BRTH_DT_SK").alias("MBR_BRTH_DT_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ADJ_RISK_NO").alias("ADJ_RISK_NO"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CST_AMT").alias("CST_AMT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.PLN_ALL_CAUSE_READMSN_VRNC_NO").alias("PLN_ALL_CAUSE_READMSN_VRNC_NO"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.RISK_PCT").alias("RISK_PCT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.SES_STRAT_NO").alias("SES_STRAT_NO"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ADV_ILNS_AND_FRAILTY_EXCL_CT").alias("ADV_ILNS_AND_FRAILTY_EXCL_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.DCSD_EXCL_CT").alias("DCSD_EXCL_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ENR_MBR_DENOMINATOR_CT").alias("ENR_MBR_DENOMINATOR_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.EVT_CT").alias("EVT_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.EXCL_CT").alias("EXCL_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HSPC_EXCL_CT").alias("HSPC_EXCL_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.LTI_SNP_EXCL_CT").alias("LTI_SNP_EXCL_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.OPTNL_EXCL_CT").alias("OPTNL_EXCL_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.RQRD_EXCL_CT").alias("RQRD_EXCL_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.UNIT_CT").alias("UNIT_CT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CNTY_AREA_ID").alias("CNTY_AREA_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.GRP_ID").alias("GRP_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_FULL_NM").alias("MBR_FULL_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_ID").alias("MBR_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.PROD_ID").alias("PROD_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.PROD_LN_TYP_DESC").alias("PROD_LN_TYP_DESC"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.PROD_SH_NM").alias("PROD_SH_NM"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ETHNCTY_CD_SK").alias("ETHNCTY_CD_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.ETHNCTY_SRC_CD_SK").alias("ETHNCTY_SRC_CD_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK").alias("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.NUMERATOR_CMPLNC_CAT_CD_SK").alias("NUMERATOR_CMPLNC_CAT_CD_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.RACE_CD_SK").alias("RACE_CD_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.RACE_SRC_CD_SK").alias("RACE_SRC_CD_SK"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.CRITR_VAL_THRU_TX").alias("CRITR_VAL_THRU_TX"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.SEL_PRCS_ITEM_ID").alias("SEL_PRCS_ITEM_ID"),
        F.col("lnk_IdsEdwMbrHedisMesrYrMo.MBR_GNDR_CD").alias("MBR_GNDR_CD")
    )
)

# --------------------------------------------------------------------------------
# xfrm_BusinessLogic (CTransformerStage)
# --------------------------------------------------------------------------------
# Build stage variables:
# svCompSumInd = if any of (CMPLNC_MNL_DATA_IN, CMPLNC_ADMIN_IN, CMPLNC_MNL_DATA_SMPL_POP_IN) = 'Y' => 'Y' else 'N'
# SvRvwSetNmRoll12 not used in final output, but we define its logic anyway (no direct column needed).
df_xfrm_BusinessLogic = (
    df_Att_Prov_Attr_SK
    .withColumn(
        "svCompSumInd",
        F.when(
            (F.col("CMPLNC_MNL_DATA_IN") == "Y")
            | (F.col("CMPLNC_ADMIN_IN") == "Y")
            | (F.col("CMPLNC_MNL_DATA_SMPL_POP_IN") == "Y"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .withColumn(
        "SvRvwSetNmRoll12",
        F.when(
            (trim(F.col("HEDIS_RVW_SET_NM"))[9:12] == "CYR")
            & (
                (trim(F.col("HEDIS_POP_NM")) == "HEDIS HMO Commercial")
                | (trim(F.col("HEDIS_POP_NM")) == "HEDIS PPO Commercial")
            ),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    # Now select the output pin "Lnk_YTD" columns with expressions
    .withColumn(
        "MBR_PCP_ATTRBTN_SK",
        F.when(F.col("MBR_PCP_ATTRBTN_SK").isNull(), F.lit(0))
         .otherwise(F.col("MBR_PCP_ATTRBTN_SK"))
    )
    .withColumn(
        "PROV_SK",
        F.when(F.col("PROV_SK").isNull(), F.lit(0)).otherwise(F.col("PROV_SK"))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_DT_SK",
        F.lit(EDWRunCycleDate)
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        F.lit(EDWRunCycleDate)
    )
    .withColumn(
        "SCRCRD_RVW_SET_DT_SK",
        F.when(
            F.length(F.col("ACTVTY_MO_NO")) == 2,
            F.concat(F.col("ACTVTY_YR_NO"), F.col("ACTVTY_MO_NO"), F.lit("01"))
        ).otherwise(
            F.concat(F.col("ACTVTY_YR_NO"), F.lit("0"), F.col("ACTVTY_MO_NO"), F.lit("01"))
        )
    )
    .withColumn(
        "HEDIS_MESR_GAP_IN_CARE_TYP_CD",
        F.col("CRITR_VAL_THRU_TX")
    )
    .withColumn(
        "HEDIS_MESR_GAP_IN_CARE_TYP_NM",
        F.when(
            trim(F.col("SEL_PRCS_ITEM_ID")) == "HEDIS_GAPS_COM",
            F.lit("COMMERCIAL GAP MEASURE")
        )
        .when(
            trim(F.col("SEL_PRCS_ITEM_ID")) == "HEDIS_GAPS_MA",
            F.lit("MEDICARE ADVANTAGE GAP MEASURE")
        )
        .when(
            trim(F.col("SEL_PRCS_ITEM_ID")) == "HEDIS_GAPS_ACA",
            F.lit("EXCHANGE GAP MEASURE")
        )
        .otherwise(F.lit(None))
    )
    .withColumn(
        "CMPLNC_SUM_IN",
        F.col("svCompSumInd")
    )
    .withColumn(
        "MBR_IN_AREA_IN",
        F.when(trim(F.col("CNTY_AREA_ID")) == "32COUNTY", F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "ON_EXCH_IN",
        F.when(trim(F.col("ON_OFF_EXCH_ID")) == "ON EXCHANGE", F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "QHP_IN",
        F.when(
            (trim(F.col("ON_OFF_EXCH_ID")) == "")
            | (F.col("ON_OFF_EXCH_ID") == "0")
            | (F.col("ON_OFF_EXCH_ID") == "1")
            | (F.col("ON_OFF_EXCH_ID") == "NA"),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    .withColumn(
        "CMPLNC_ADMIN_CT",
        F.when(F.col("CMPLNC_ADMIN_IN") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "CMPLNC_MNL_DATA_CT",
        F.when(F.col("CMPLNC_MNL_DATA_IN") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "CMPLNC_SMPL_POP_CT",
        F.when(F.col("CMPLNC_SMPL_POP_IN") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "CMPLNC_SUM_CT",
        F.when(F.col("svCompSumInd") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "CONTRAIN_CT",
        F.when(F.col("CONTRAIN_IN") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "CONTRAIN_SMPL_POP_CT",
        F.when(F.col("CONTRAIN_SMPL_POP_IN") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "EXCL_SMPL_POP_CT",
        F.when(F.col("EXCL_SMPL_POP_IN") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "MESR_ELIG_CT",
        F.when(F.col("MESR_ELIG_IN") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "MESR_ELIG_SMPL_POP_CT",
        F.when(F.col("MESR_ELIG_SMPL_POP_IN") == "Y", F.lit(1)).otherwise(F.lit(0))
    )
    .withColumn(
        "CRT_RUN_CYC_EXCTN_SK",
        F.lit(EDWRunCycle)
    )
    .withColumn(
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        F.lit(EDWRunCycle)
    )
    .withColumn(
        "IDS_LAST_UPDT_RUN_CYC_EXCTN_SK",
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)

df_seq_MBR_HEDIS_MESR_YTD_F = df_xfrm_BusinessLogic.select(
    # Order must match the final link in the DataStage job
    rpad(F.col("MBR_HEDIS_MESR_YR_MO_SK"), 0, " ").alias("MBR_HEDIS_MESR_YR_MO_SK"),  # Not char? Keep as is, but no length specified => no rpad needed with length>0
    F.col("HEDIS_RVW_SET_NM").alias("HEDIS_RVW_SET_NM"),  # not marked as char => no rpad
    F.col("HEDIS_POP_NM").alias("HEDIS_POP_NM"),          # not char
    F.col("HEDIS_MESR_NM").alias("HEDIS_MESR_NM"),        # not char
    F.col("HEDIS_SUB_MESR_NM").alias("HEDIS_SUB_MESR_NM"),
    F.col("MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("HEDIS_MBR_BUCKET_ID").alias("HEDIS_MBR_BUCKET_ID"),
    rpad(F.col("BASE_EVT_EPSD_DT_SK"), 10, " ").alias("BASE_EVT_EPSD_DT_SK"),
    F.col("ACTVTY_YR_NO").alias("ACTVTY_YR_NO"),
    F.col("ACTVTY_MO_NO").alias("ACTVTY_MO_NO"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    rpad(F.col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("GRP_SK").alias("GRP_SK"),
    F.col("HEDIS_MESR_SK").alias("HEDIS_MESR_SK"),
    F.col("HEDIS_POP_SK").alias("HEDIS_POP_SK"),
    F.col("HEDIS_RVW_SET_SK").alias("HEDIS_RVW_SET_SK"),
    F.col("MBR_PCP_ATTRBTN_SK").alias("MBR_PCP_ATTRBTN_SK"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("PROD_SH_NM_SK").alias("PROD_SH_NM_SK"),
    F.col("PROV_SK").alias("PROV_SK"),
    rpad(F.col("SCRCRD_RVW_SET_DT_SK"), 10, " ").alias("SCRCRD_RVW_SET_DT_SK"),
    F.col("ETHNCTY_CD").alias("ETHNCTY_CD"),
    F.col("ETHNCTY_NM").alias("ETHNCTY_NM"),
    F.col("ETHNCTY_SRC_CD").alias("ETHNCTY_SRC_CD"),
    F.col("ETHNCTY_SRC_NM").alias("ETHNCTY_SRC_NM"),
    F.col("HEDIS_MESR_GAP_IN_CARE_TYP_CD").alias("HEDIS_MESR_GAP_IN_CARE_TYP_CD"),
    F.col("HEDIS_MESR_GAP_IN_CARE_TYP_NM").alias("HEDIS_MESR_GAP_IN_CARE_TYP_NM"),
    F.col("NUMERATOR_CMPLNC_CAT_CD").alias("NUMERATOR_CMPLNC_CAT_CD"),
    F.col("NUMERATOR_CMPLNC_CAT_NM").alias("NUMERATOR_CMPLNC_CAT_NM"),
    F.col("RACE_CD").alias("RACE_CD"),
    F.col("RACE_NM").alias("RACE_NM"),
    F.col("RACE_SRC_CD").alias("RACE_SRC_CD"),
    F.col("RACE_SRC_NM").alias("RACE_SRC_NM"),
    rpad(F.col("CMPLNC_ADMIN_IN"), 1, " ").alias("CMPLNC_ADMIN_IN"),
    rpad(F.col("CMPLNC_MNL_DATA_IN"), 1, " ").alias("CMPLNC_MNL_DATA_IN"),
    rpad(F.col("CMPLNC_MNL_DATA_SMPL_POP_IN"), 1, " ").alias("CMPLNC_MNL_DATA_SMPL_POP_IN"),
    rpad(F.col("CMPLNC_SMPL_POP_IN"), 1, " ").alias("CMPLNC_SMPL_POP_IN"),
    rpad(F.col("CMPLNC_SUM_IN"), 1, " ").alias("CMPLNC_SUM_IN"),
    rpad(F.col("CONTRAIN_IN"), 1, " ").alias("CONTRAIN_IN"),
    rpad(F.col("CONTRAIN_SMPL_POP_IN"), 1, " ").alias("CONTRAIN_SMPL_POP_IN"),
    rpad(F.col("EXCL_IN"), 1, " ").alias("EXCL_IN"),
    rpad(F.col("EXCL_SMPL_POP_IN"), 1, " ").alias("EXCL_SMPL_POP_IN"),
    rpad(F.col("MESR_ELIG_IN"), 1, " ").alias("MESR_ELIG_IN"),
    rpad(F.col("MESR_ELIG_SMPL_POP_IN"), 1, " ").alias("MESR_ELIG_SMPL_POP_IN"),
    rpad(F.col("MBR_IN_AREA_IN"), 1, " ").alias("MBR_IN_AREA_IN"),
    rpad(F.col("ON_EXCH_IN"), 1, " ").alias("ON_EXCH_IN"),
    rpad(F.col("QHP_IN"), 1, " ").alias("QHP_IN"),
    rpad(F.col("MBR_BRTH_DT_SK"), 10, " ").alias("MBR_BRTH_DT_SK"),
    F.col("ADJ_RISK_NO").alias("ADJ_RISK_NO"),
    F.col("CST_AMT").alias("CST_AMT"),
    F.col("PLN_ALL_CAUSE_READMSN_VRNC_NO").alias("PLN_ALL_CAUSE_READMSN_VRNC_NO"),
    F.col("RISK_PCT").alias("RISK_PCT"),
    F.col("SES_STRAT_NO").alias("SES_STRAT_NO"),
    F.col("ADV_ILNS_AND_FRAILTY_EXCL_CT").alias("ADV_ILNS_AND_FRAILTY_EXCL_CT"),
    F.col("CMPLNC_ADMIN_CT").alias("CMPLNC_ADMIN_CT"),
    F.col("CMPLNC_MNL_DATA_CT").alias("CMPLNC_MNL_DATA_CT"),
    F.col("CMPLNC_SMPL_POP_CT").alias("CMPLNC_SMPL_POP_CT"),
    F.col("CMPLNC_SUM_CT").alias("CMPLNC_SUM_CT"),
    F.col("CONTRAIN_CT").alias("CONTRAIN_CT"),
    F.col("CONTRAIN_SMPL_POP_CT").alias("CONTRAIN_SMPL_POP_CT"),
    F.col("DCSD_EXCL_CT").alias("DCSD_EXCL_CT"),
    F.col("ENR_MBR_DENOMINATOR_CT").alias("ENR_MBR_DENOMINATOR_CT"),
    F.col("EVT_CT").alias("EVT_CT"),
    F.col("EXCL_CT").alias("EXCL_CT"),
    F.col("EXCL_SMPL_POP_CT").alias("EXCL_SMPL_POP_CT"),
    F.col("HSPC_EXCL_CT").alias("HSPC_EXCL_CT"),
    F.col("LTI_SNP_EXCL_CT").alias("LTI_SNP_EXCL_CT"),
    F.col("MESR_ELIG_CT").alias("MESR_ELIG_CT"),
    F.col("MESR_ELIG_SMPL_POP_CT").alias("MESR_ELIG_SMPL_POP_CT"),
    F.col("OPTNL_EXCL_CT").alias("OPTNL_EXCL_CT"),
    F.col("RQRD_EXCL_CT").alias("RQRD_EXCL_CT"),
    F.col("UNIT_CT").alias("UNIT_CT"),
    F.col("ACRDTN_CAT_ID").alias("ACRDTN_CAT_ID"),
    F.col("CNTY_AREA_ID").alias("CNTY_AREA_ID"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("HEDIS_MESR_ABBR_ID").alias("HEDIS_MESR_ABBR_ID"),
    F.col("MBR_FULL_NM").alias("MBR_FULL_NM"),
    F.col("MBR_ID").alias("MBR_ID"),
    F.col("MBR_INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("ON_OFF_EXCH_ID").alias("ON_OFF_EXCH_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("PROD_LN_TYP_DESC").alias("PROD_LN_TYP_DESC"),
    F.col("PROD_ROLLUP_ID").alias("PROD_ROLLUP_ID"),
    F.col("PROD_SH_NM").alias("PROD_SH_NM"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK").alias("IDS_LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ETHNCTY_CD_SK").alias("ETHNCTY_CD_SK"),
    F.col("ETHNCTY_SRC_CD_SK").alias("ETHNCTY_SRC_CD_SK"),
    F.col("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK").alias("HEDIS_MESR_GAP_IN_CARE_TYP_CD_SK"),
    F.col("NUMERATOR_CMPLNC_CAT_CD_SK").alias("NUMERATOR_CMPLNC_CAT_CD_SK"),
    F.col("RACE_CD_SK").alias("RACE_CD_SK"),
    F.col("RACE_SRC_CD_SK").alias("RACE_SRC_CD_SK"),
    F.col("NUMERATOR_EVT_1_SVC_DT").alias("NUMERATOR_EVT_1_SVC_DT"),
    F.col("LAB_TST_VAL_NO").alias("LAB_TST_VAL_NO"),
    F.col("MBR_GNDR_CD").alias("MBR_GNDR_CD")
)

# --------------------------------------------------------------------------------
# seq_MBR_HEDIS_MESR_YTD_F (PxSequentialFile) => Write to file with delimiter "|"
# --------------------------------------------------------------------------------
write_files(
    df_seq_MBR_HEDIS_MESR_YTD_F,
    f"{adls_path}/load/MBR_HEDIS_MESR_YTD_F.dat",
    delimiter="|",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote=None,
    nullValue=None
)