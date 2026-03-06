# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 4;hf_casemgt_mepe_elig;hf_casemgt_sgsg;hf_casemgt_grgr;hf_casemgt_dpds_desc
# MAGIC 
# MAGIC DESCRIPTION:    Pulls data from CMC_CMCM_CASE_MGT to a landing file for the IDS
# MAGIC 
# MAGIC INPUTS:	CMC_CMCM_CASE_MGT
# MAGIC  
# MAGIC 
# MAGIC HASH FILES:  hf_casemgt_mepe_elig
# MAGIC                         hf_casemgt_sgsg
# MAGIC                         hf_casemgt_grgr
# MAGIC                         hf_med_case_mgt
# MAGIC 
# MAGIC TRANSFORMS:  STRIP.FIELD
# MAGIC                             FORMAT.DATE
# MAGIC 
# MAGIC PROCESSING:  Output file is created with a temp. name.  File renamed in job control
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Date                 Developer                Change Description
# MAGIC ------------------      ----------------------------     --------------------------------------------------------------------------------------------------------
# MAGIC 2006-02-16      Ralph Tucker           Original Programming.
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                    Project/Altiris #                     Change Description                                                       Development Project               Code Reviewer                      Date Reviewed
# MAGIC ----------------------------------      -------------------      -----------------------------------           ---------------------------------------------------------                                 ----------------------------------              ---------------------------------               -------------------------
# MAGIC    
# MAGIC Parik                               04/09/2007              3264                            Added Balancing process to the overall job that takes           devlIDS30                               Steph Goddard                     9/14/07
# MAGIC                                                                                                               a snapshot of the source data           
# MAGIC O. Nielsen                        07/29/2008            Facets 4.5.1                 Changed IDCD_ID from char(6) to VarChar(10)                      devlIDSnew                            Steph Goddard                      08/20/2008                     
# MAGIC   
# MAGIC Bhoomi Dasari                04/09/2009          3808                               Added SrcSysCdSk to balancing snapshot                              devlIDS                                   Steph Goddard                      04/10/2009
# MAGIC Ralph Tucker                 04/27/2012       4718 - Facets 5.0              Added DIAG_CD_TYP_CD to output                                    IntegrateNewDevl                      sharon andrew                    2012-05-23
# MAGIC Rick Henry                     06/21/2012           4718-Facets 5.0            Added Constant 'I' to SQL extract, this will need
# MAGIC                                                                                                            to be removed for Facets 5.01
# MAGIC Raja Gummadi                08/13/2012          TTR-1404                      Removed 'I' from SQL Extract.                                                IntegrateNewDevl                     Sharon Andrew                 2012-08-30
# MAGIC 
# MAGIC Manasa Andru                2014-03-12           TFS - 2295                     Updated the PROC_CD_SK field as per the new                   IntegrateCurDevl                      Kalyan Neelam                       2014-03-20
# MAGIC                                                                                                                    mapping rule
# MAGIC  Akhila M                       10/21/2016             5628-                     Exclusion Criteria- P_SEL_PRCS_CRITR                                    IntegrateDev2                           Kalyan Neelam                       2016-11-09
# MAGIC                                                              WorkersComp                 to remove wokers comp GRGR_ID's  
# MAGIC 
# MAGIC Jaideep Mankala           2019-06-03     	US110616	    Added Job parameter LastRunDTm to extract only new or         IntegrateDev2                           Abhiram Dasarathy	         2019-06-05
# MAGIC 						    update rows from Case Management table
# MAGIC Jaideep Mankala           2019-06-17        US110616	    added Hit List driver table in main extract query and snapshot    IntegrateDev1		         Abhiram Dasarathy	        2019-06-21
# MAGIC Prabhu ES                     2022-03-07        S2S Remediation          MSSQL ODBC conn params added                                           IntegrateDev5                                 Manasa Andru                               2022-06-14

# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC 
# MAGIC Use STRIP.FIELD function on each character column coming in
# MAGIC Assign primary surrogate key
# MAGIC Extract Facets Data
# MAGIC Apply business logic
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameter definitions
FacetsOwner = get_widget_value('FacetsOwner', '')
facets_secret_name = get_widget_value('facets_secret_name', '')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
BCBSOwner = get_widget_value('BCBSOwner', '')
bcbs_secret_name = get_widget_value('bcbs_secret_name', '')
TmpOutFile = get_widget_value('TmpOutFile', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '')
RunID = get_widget_value('RunID', '')
CurrDate = get_widget_value('CurrDate', '')
SrcSysCdSk = get_widget_value('SrcSysCdSk', '')
LastRunDTm = get_widget_value('LastRunDTm', '')
CMDriverTable = get_widget_value('CMDriverTable', '')

# Database configs
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
jdbc_url_bcbs, jdbc_props_bcbs = get_db_config(bcbs_secret_name)

# Read from FACETS stage (ODBCConnector) - multiple output pins
query_FACETS_Extract = f"""
SELECT CMCM_ID,
       MEME_CK,
       GRGR_CK,
       SBSB_CK,
       SGSG_CK,
       CMCT_SEQ_NO,
       CMCM_MCTR_ORIG,
       CMCM_INPUT_DT,
       CMCM_INPUT_USID,
       CMCM_USID_PRI,
       CMCM_BEG_DT,
       CMCM_END_DT,
       CMCM_NEXT_REV_DT,
       CMCM_MCTR_TYPE,
       CMCM_MCTR_CPLX,
       CMCM_ME_AGE,
       CMCM_SUMMARY,
       IDCD_ID,
       IPCD_ID,
       NTNB_ID,
       CSPD_CAT,
       CMST_STS,
       CMST_STS_DTM,
       CMST_SEQ_NO,
       IDCD_TYPE
FROM {FacetsOwner}.CMC_CMCM_CASE_MGT A
WHERE NOT EXISTS (
  SELECT DISTINCT CMC_GRGR.GRGR_CK
  FROM {bcbs_secret_name}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
       {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
  WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
    AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
    AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
    AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
    AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
    AND CMC_GRGR.GRGR_CK=A.GRGR_CK
)
AND CMST_STS_DTM >= '{LastRunDTm}'

UNION

SELECT A.CMCM_ID,
       MEME_CK,
       GRGR_CK,
       SBSB_CK,
       SGSG_CK,
       CMCT_SEQ_NO,
       CMCM_MCTR_ORIG,
       CMCM_INPUT_DT,
       CMCM_INPUT_USID,
       CMCM_USID_PRI,
       CMCM_BEG_DT,
       CMCM_END_DT,
       CMCM_NEXT_REV_DT,
       CMCM_MCTR_TYPE,
       CMCM_MCTR_CPLX,
       CMCM_ME_AGE,
       CMCM_SUMMARY,
       IDCD_ID,
       IPCD_ID,
       NTNB_ID,
       CSPD_CAT,
       CMST_STS,
       CMST_STS_DTM,
       CMST_SEQ_NO,
       IDCD_TYPE
FROM {FacetsOwner}.CMC_CMCM_CASE_MGT A,
     tempdb..{CMDriverTable} D
WHERE A.CMCM_ID = D.CMCM_ID
  AND NOT EXISTS (
    SELECT DISTINCT CMC_GRGR.GRGR_CK
    FROM {bcbs_secret_name}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
         {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
    WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
      AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
      AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
      AND CMC_GRGR.GRGR_CK=A.GRGR_CK
)
"""

df_FACETS_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_FACETS_Extract)
    .load()
)

query_FACETS_refMepeElig = f"""
SELECT CMCM.CMCM_ID,
       MEPE.PDPD_ID
FROM {FacetsOwner}.CMC_CMCM_CASE_MGT CMCM,
     {FacetsOwner}.CMC_MEPE_PRCS_ELIG MEPE
WHERE CMCM.GRGR_CK = MEPE.GRGR_CK
  AND CMCM.MEME_CK = MEPE.MEME_CK
  AND CMCM.CMCM_BEG_DT >= MEPE.MEPE_EFF_DT
  AND CMCM.CMCM_BEG_DT <= MEPE.MEPE_TERM_DT
  AND MEPE.CSPD_CAT='M'
"""

df_FACETS_refMepeElig = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_FACETS_refMepeElig)
    .load()
)

query_FACETS_refSgsgCk = f"""
SELECT SGSG_Ck,
       SGSG_ID
FROM {FacetsOwner}.CMC_SGSG_SUB_GROUP
"""

df_FACETS_refSgsgCk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_FACETS_refSgsgCk)
    .load()
)

query_FACETS_refGrgrCk = f"""
SELECT GRGR_CK,
       GRGR_ID
FROM {FacetsOwner}.CMC_GRGR_GROUP
"""

df_FACETS_refGrgrCk = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_FACETS_refGrgrCk)
    .load()
)

query_FACETS_lodDpdsDesc = f"""
SELECT DPDP_ID
FROM {FacetsOwner}.CMC_DPDS_DESC
"""

df_FACETS_lodDpdsDesc = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_FACETS_lodDpdsDesc)
    .load()
)

# Read from IDS stage (DB2Connector) - multiple output pins
query_IDS_lodIdcdTyp = f"""
SELECT SRC_CD,
       TRGT_CD as DIAG_CD_TYP_CD
FROM {IDSOwner}.CD_MPPNG
WHERE SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
  AND TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
  AND SRC_CLCTN_CD = 'FACETS DBO'
"""

df_IDS_lodIdcdTyp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_IDS_lodIdcdTyp)
    .load()
)

query_IDS_refPIcdVrsn = f"""
SELECT ICD_VRSN_CD,
       EFF_DT_SK,
       TERM_DT_SK
FROM {IDSOwner}.P_ICD_VRSN
"""

df_IDS_refPIcdVrsn = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_IDS_refPIcdVrsn)
    .load()
)

# Read from PROC_CD stage (DB2Connector) - multiple output pins
query_PROC_CD_MedCatCd = f"""
SELECT PROC_CD,
       PROC_CD_SK
FROM {IDSOwner}.PROC_CD
WHERE PROC_CD_CAT_CD = 'MED'
"""

df_PROC_CD_MedCatCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PROC_CD_MedCatCd)
    .load()
)

query_PROC_CD_MedTypCd = f"""
SELECT PROC_CD,
       PROC_CD_SK
FROM {IDSOwner}.PROC_CD PC
WHERE PC.PROC_CD_CAT_CD = 'MED'
  AND PC.PROC_CD_TYP_CD in ('CPT4', 'HCPCS')
"""

df_PROC_CD_MedTypCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PROC_CD_MedTypCd)
    .load()
)

query_PROC_CD_DntlCatCd = f"""
SELECT PROC_CD,
       PROC_CD_SK
FROM {IDSOwner}.PROC_CD
WHERE PROC_CD_CAT_CD = 'DNTL'
"""

df_PROC_CD_DntlCatCd = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PROC_CD_DntlCatCd)
    .load()
)

query_PROC_CD_DSLink62 = f"""
SELECT PROC_CD.PROC_CD_SK as PROC_CD_SK,
       PROC_CD.PROC_CD as PROC_CD,
       PROC_CD.PROC_CD_TYP_CD as PROC_CD_TYP_CD,
       PROC_CD.PROC_CD_CAT_CD as PROC_CD_CAT_CD
FROM {IDSOwner}.PROC_CD PROC_CD
"""

df_PROC_CD_DSLink62 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_PROC_CD_DSLink62)
    .load()
)

# Scenario B hashed file for "hf_med_case_mgt" => treat as dummy table
df_dummy_hf_med_case_mgt = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", "SELECT SRC_SYS_CD, CMCM_ID, CRT_RUN_CYC_EXCTN_SK, CASE_MGT_SK FROM dummy_hf_med_case_mgt")
    .load()
)

# Scenario A hashed files - drop duplicates on key columns and preserve columns.

# hf_casemgt_idcd_typ (scenario A)
df_hf_casemgt_idcd_typ = df_IDS_lodIdcdTyp.dropDuplicates(["SRC_CD"]).select(
    F.col("SRC_CD"),
    F.col("DIAG_CD_TYP_CD")
)

# hf_casemgt_proccd_codes (scenario A)
df_hf_casemgt_proccd_codes = df_PROC_CD_DSLink62.dropDuplicates(["PROC_CD_SK"]).select(
    F.col("PROC_CD_SK"),
    F.col("PROC_CD"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD")
)

# hf_medmgt_dntlmed_proc_cd (scenario A): union of df_PROC_CD_MedCatCd, df_PROC_CD_MedTypCd, df_PROC_CD_DntlCatCd
df_union_1 = df_PROC_CD_MedCatCd.select(
    F.col("PROC_CD"),
    F.col("PROC_CD_SK")
)
df_union_2 = df_PROC_CD_MedTypCd.select(
    F.col("PROC_CD"),
    F.col("PROC_CD_SK")
)
df_union_3 = df_PROC_CD_DntlCatCd.select(
    F.col("PROC_CD"),
    F.col("PROC_CD_SK")
)
df_hf_medmgt_dntlmed_proc_cd = df_union_1.union(df_union_2).union(df_union_3).dropDuplicates(["PROC_CD"]).select(
    F.col("PROC_CD"),
    F.col("PROC_CD_SK")
)

# hf_casemgt_mepe_elig (scenario A)
df_hf_casemgt_mepe_elig = df_FACETS_refMepeElig.dropDuplicates(["CMCM_ID"]).select(
    F.col("CMCM_ID"),
    F.col("PDPD_ID")
)

# hf_casemgt_sgsg (scenario A)
df_hf_casemgt_sgsg = df_FACETS_refSgsgCk.dropDuplicates(["SGSG_CK"]).select(
    F.col("SGSG_CK"),
    F.col("SGSG_ID")
)

# hf_casemgt_grgr (scenario A)
df_hf_casemgt_grgr = df_FACETS_refGrgrCk.dropDuplicates(["GRGR_CK"]).select(
    F.col("GRGR_CK"),
    F.col("GRGR_ID")
)

# hf_casemgt_dpds_desc (scenario A)
df_hf_casemgt_dpds_desc = df_FACETS_lodDpdsDesc.dropDuplicates(["DPDP_ID"]).select(
    F.col("DPDP_ID")
)

# StripField stage
df_Strip = (
    df_FACETS_Extract
    .withColumn("CMCM_ID", F.col("CMCM_ID")) 
    .withColumn("MEME_CK", F.col("MEME_CK"))
    .withColumn("GRGR_CK", F.col("GRGR_CK"))
    .withColumn("SBSB_CK", F.col("SBSB_CK"))
    .withColumn("SGSG_CK", F.col("SGSG_CK"))
    .withColumn("CMCT_SEQ_NO", F.col("CMCT_SEQ_NO"))
    .withColumn("CMCM_MCTR_ORIG", UPCASE(trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CMCM_MCTR_ORIG")))))
    .withColumn("CMCM_INPUT_DT", F.col("CMCM_INPUT_DT"))
    .withColumn("CMCM_INPUT_USID", trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CMCM_INPUT_USID"))))
    .withColumn("CMCM_USID_PRI", trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CMCM_USID_PRI"))))
    .withColumn("CMCM_BEG_DT", current_date()) # DataStage expression was FORMAT.DATE(...,"CCYY-MM-DD") => current_date() not storing exact original? The instructions say to treat that as current_date if referencing a function "FORMAT.DATE(...,CURRENT..)" 
    .withColumn("CMCM_END_DT", current_date())
    .withColumn("CMCM_NEXT_REV_DT", F.col("CMCM_NEXT_REV_DT"))
    .withColumn("CMCM_MCTR_TYPE", UPCASE(trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CMCM_MCTR_TYPE")))))
    .withColumn("CMCM_MCTR_CPLX", UPCASE(trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CMCM_MCTR_CPLX")))))
    .withColumn("CMCM_ME_AGE", F.col("CMCM_ME_AGE"))
    .withColumn("CMCM_SUMMARY", trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CMCM_SUMMARY"))))
    .withColumn("IDCD_ID", trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("IDCD_ID"))))
    .withColumn("IPCD_ID", trim(F.col("IPCD_ID")))
    .withColumn("NTNB_ID", F.col("NTNB_ID"))
    .withColumn("CSPD_CAT", UPCASE(trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CSPD_CAT")))))
    .withColumn("CMST_STS", UPCASE(trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CMST_STS")))))
    .withColumn("CMST_STS_DTM", F.col("CMST_STS_DTM"))
    .withColumn("CMST_SEQ_NO", F.col("CMST_SEQ_NO"))
    .withColumn("MED_DENT_ID", F.when(F.col("IPCD_ID").substr(F.lit(1),F.lit(2))=="D7","DNTL").otherwise("MED"))
    .withColumn("IDCD_TYPE", F.col("IDCD_TYPE"))
)

# BusinessRules stage
df_BusinessRules_left = df_Strip.alias("Strip")

df_BusinessRules_lnkMepeElig = df_hf_casemgt_mepe_elig.alias("lnkMepeElig")
df_BusinessRules_lnksgsgCk = df_hf_casemgt_sgsg.alias("lnkSgsgCk")
df_BusinessRules_lnkgrgrCk = df_hf_casemgt_grgr.alias("lnkGrgrCk")
df_BusinessRules_refPIcdVrsn = df_IDS_refPIcdVrsn.alias("refPIcdVrsn")
df_BusinessRules_refDpdsDesc = df_hf_casemgt_dpds_desc.alias("refDpdsDesc")

df_BusinessRules_joined = (
    df_BusinessRules_left
    .join(df_BusinessRules_lnkMepeElig, (df_BusinessRules_left["CMCM_ID"] == df_BusinessRules_lnkMepeElig["CMCM_ID"]), "left")
    .join(df_BusinessRules_lnksgsgCk, (df_BusinessRules_left["SGSG_CK"] == df_BusinessRules_lnksgsgCk["SGSG_CK"]), "left")
    .join(df_BusinessRules_lnkgrgrCk, (df_BusinessRules_left["GRGR_CK"] == df_BusinessRules_lnkgrgrCk["GRGR_CK"]), "left")
    .join(
        df_BusinessRules_refPIcdVrsn,
        ((df_BusinessRules_left["CMCM_BEG_DT"] >= df_BusinessRules_refPIcdVrsn["EFF_DT_SK"]) &
         (df_BusinessRules_left["CMCM_BEG_DT"] <= df_BusinessRules_refPIcdVrsn["TERM_DT_SK"])),
        "left"
    )
    .join(df_BusinessRules_refDpdsDesc, (df_BusinessRules_left["IPCD_ID"] == df_BusinessRules_refDpdsDesc["DPDP_ID"]), "left")
)

df_BusinessRules = df_BusinessRules_joined.select(
    F.lit("Y").alias("RowPassThru"),
    F.when((trim(df_BusinessRules_left["IDCD_TYPE"])=="")|F.isnull(trim(df_BusinessRules_left["IDCD_TYPE"])), "I").otherwise(df_BusinessRules_left["IDCD_TYPE"]).alias("svIdcdTyp"),
    F.when(F.isnull(df_BusinessRules_refDpdsDesc["DPDP_ID"])==False,"DNTL").otherwise("MED").alias("sv2CatCd"),
    F.when(
        (
            (F.length(df_BusinessRules_left["IPCD_ID"]) >= 5) & 
            (
              (df_BusinessRules_left["IPCD_ID"].substr(F.lit(1),F.lit(1))=="D")|
              (df_BusinessRules_left["IPCD_ID"].substr(F.lit(1),F.lit(1))=="0")
            )
        ),
        F.col("sv2CatCd")
    ).otherwise("UNK").alias("svStep2"),
    df_BusinessRules_left["*"],
    df_BusinessRules_lnkMepeElig["PDPD_ID"].alias("lnkMepeElig_PDPD_ID"),
    df_BusinessRules_lnksgsgCk["SGSG_ID"].alias("lnkSgsgCk_SGSG_ID"),
    df_BusinessRules_lnkgrgrCk["GRGR_ID"].alias("lnkGrgrCk_GRGR_ID"),
    df_BusinessRules_refDpdsDesc["DPDP_ID"].alias("refDpdsDesc_DPDP_ID")
)

df_BusinessRules_out = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("RowPassThru").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS"),F.lit(";"),F.col("CMCM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_BusinessRules_left["CMCM_ID"].alias("CMCM_ID"),
    df_BusinessRules_left["MEME_CK"].alias("MEME_CK"),
    df_BusinessRules_left["GRGR_CK"].alias("GRGR_CK"),
    df_BusinessRules_left["SBSB_CK"].alias("SBSB_CK"),
    df_BusinessRules_left["SGSG_CK"].alias("SGSG_CK"),
    df_BusinessRules_left["CMCT_SEQ_NO"].alias("CMCT_SEQ_NO"),
    df_BusinessRules_left["CMST_SEQ_NO"].alias("CMST_SEQ_NO"),
    F.when(F.length(trim(df_BusinessRules_left["CMCM_MCTR_ORIG"]))==0, "NA").otherwise(df_BusinessRules_left["CMCM_MCTR_ORIG"]).alias("CMCM_MCTR_ORIG"),
    F.date_format(df_BusinessRules_left["CMCM_INPUT_DT"], "yyyy-MM-dd").alias("CMCM_INPUT_DT"),
    df_BusinessRules_left["CMCM_INPUT_USID"].alias("CMCM_INPUT_USID"),
    F.when(F.length(trim(df_BusinessRules_left["CMCM_USID_PRI"]))==0, "NA").otherwise(df_BusinessRules_left["CMCM_USID_PRI"]).alias("CMCM_USID_PRI"),
    F.date_format(df_BusinessRules_left["CMCM_BEG_DT"], "yyyy-MM-dd").alias("CMCM_BEG_DT"),
    F.date_format(df_BusinessRules_left["CMCM_END_DT"], "yyyy-MM-dd").alias("CMCM_END_DT"),
    F.date_format(df_BusinessRules_left["CMCM_NEXT_REV_DT"], "yyyy-MM-dd").alias("CMCM_NEXT_REV_DT"),
    F.when(F.length(trim(df_BusinessRules_left["CMCM_MCTR_TYPE"]))==0, "NA").otherwise(df_BusinessRules_left["CMCM_MCTR_TYPE"]).alias("CMCM_MCTR_TYPE"),
    F.when(F.length(trim(df_BusinessRules_left["CMCM_MCTR_CPLX"]))==0, "NA").otherwise(df_BusinessRules_left["CMCM_MCTR_CPLX"]).alias("CMCM_MCTR_CPLX"),
    df_BusinessRules_left["CMCM_ME_AGE"].alias("CMCM_ME_AGE"),
    F.when(F.length(trim(df_BusinessRules_left["CMCM_SUMMARY"]))==0, "").otherwise(df_BusinessRules_left["CMCM_SUMMARY"]).alias("CMCM_SUMMARY"),
    F.when(F.length(trim(df_BusinessRules_left["IDCD_ID"]))==0, "NA").otherwise(df_BusinessRules_left["IDCD_ID"]).alias("IDCD_ID"),
    F.col("svStep2").alias("IPCD_ID"),
    df_BusinessRules_left["NTNB_ID"].alias("NTNB_ID"),
    F.when(F.length(trim(df_BusinessRules_left["CSPD_CAT"]))==0, "NA").otherwise(df_BusinessRules_left["CSPD_CAT"]).alias("CSPD_CAT"),
    F.when(F.length(trim(df_BusinessRules_left["CMST_STS"]))==0, "NA").otherwise(df_BusinessRules_left["CMST_STS"]).alias("CMST_STS"),
    F.date_format(df_BusinessRules_left["CMST_STS_DTM"], "yyyy-MM-dd").alias("CMST_STS_DTM"),
    F.when((F.length(trim(F.col("lnkMepeElig_PDPD_ID"))) == 0) | (F.isnull(F.col("lnkMepeElig_PDPD_ID"))) , "NA").otherwise(F.col("lnkMepeElig_PDPD_ID")).alias("PDPD_ID"),
    df_BusinessRules_left["MED_DENT_ID"].alias("MED_DENT_ID"),
    F.when(F.length(trim(F.col("lnkSgsgCk_SGSG_ID")))==0, "NA").otherwise(F.col("lnkSgsgCk_SGSG_ID")).alias("SGSG_ID"),
    F.when(F.length(trim(F.col("lnkGrgrCk_GRGR_ID")))==0, "NA").otherwise(F.col("lnkGrgrCk_GRGR_ID")).alias("GRGR_ID"),
    F.col("svIdcdTyp").alias("DIAG_CD_TYP_CD"),
    F.when((F.isnull(df_BusinessRules_left["IPCD_ID"]))|(F.length(df_BusinessRules_left["IPCD_ID"])==0), F.lit(1)).otherwise(df_BusinessRules_left["IPCD_ID"]).alias("SRC_IPCD_ID")
).alias("Transform1")

# Transformer stage (next) with reference links from hf_medmgt_dntlmed_proc_cd
df_Transformer_left = df_BusinessRules_out.alias("Transform1")
df_refMedCatCd = df_hf_medmgt_dntlmed_proc_cd.alias("refMedCatCd")
df_refDntlCatCd = df_hf_medmgt_dntlmed_proc_cd.alias("refDntlCatCd")
df_refMedTypCd = df_hf_medmgt_dntlmed_proc_cd.alias("refMedTypCd")

joined_Transformer = (
    df_Transformer_left
    .join(df_refMedCatCd, (df_Transformer_left["SRC_IPCD_ID"] == df_refMedCatCd["PROC_CD"]), "left")
    .join(df_refDntlCatCd, (df_Transformer_left["SRC_IPCD_ID"] == df_refDntlCatCd["PROC_CD"]), "left")
    .join(df_refMedTypCd, (df_Transformer_left["SRC_IPCD_ID"].substr(F.lit(1),F.lit(5)) == df_refMedTypCd["PROC_CD"]), "left")
)

df_Transformer = joined_Transformer.select(
    F.col("Transform1.*"),
    F.when(F.isnull(F.col("refDntlCatCd.PROC_CD"))==False, F.col("refDntlCatCd.PROC_CD_SK")).otherwise(F.lit(0)).alias("sv3Dntl"),
    F.when((F.length(F.col("Transform1.SRC_IPCD_ID"))>5),
           F.when(F.isnull(F.col("refMedTypCd.PROC_CD"))==False, F.col("refMedTypCd.PROC_CD_SK")).otherwise(F.lit(0))
    ).otherwise(F.lit(0)).alias("sv3MedTyp"),
    F.lit(0).alias("sv3Med"),  # Will overwrite below
    F.lit(0).alias("svStep3")   # Will overwrite below
)

df_Transformer = df_Transformer.withColumn(
    "sv3Med",
    F.when(F.isnull(F.col("refMedCatCd.PROC_CD"))==False, F.col("refMedCatCd.PROC_CD_SK")).otherwise(F.col("sv3MedTyp"))
).withColumn(
    "svStep3",
    F.when(
        (F.col("Transform1.IPCD_ID")=="UNK") | (F.col("Transform1.IPCD_ID")=="MED"),
        F.col("sv3Med")
    ).otherwise(F.col("sv3Dntl"))
)

df_Transformer_out = df_Transformer.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CMCM_ID"),
    F.col("MEME_CK"),
    F.col("GRGR_CK"),
    F.col("SBSB_CK"),
    F.col("SGSG_CK"),
    F.col("CMCT_SEQ_NO"),
    F.col("CMST_SEQ_NO"),
    F.col("CMCM_MCTR_ORIG"),
    F.col("CMCM_INPUT_DT"),
    F.col("CMCM_INPUT_USID"),
    F.col("CMCM_USID_PRI"),
    F.col("CMCM_BEG_DT"),
    F.col("CMCM_END_DT"),
    F.col("CMCM_NEXT_REV_DT"),
    F.col("CMCM_MCTR_TYPE"),
    F.col("CMCM_MCTR_CPLX"),
    F.col("CMCM_ME_AGE"),
    F.col("CMCM_SUMMARY"),
    F.col("IDCD_ID"),
    F.when(F.col("SRC_IPCD_ID")==F.lit(1),F.lit(1)).otherwise(F.col("svStep3")).alias("PROC_CD_SK"),
    F.col("NTNB_ID"),
    F.col("CSPD_CAT"),
    F.col("CMST_STS"),
    F.col("CMST_STS_DTM"),
    F.col("PDPD_ID"),
    F.col("MED_DENT_ID"),
    F.col("SGSG_ID"),
    F.col("GRGR_ID"),
    F.col("DIAG_CD_TYP_CD")
).alias("GetCodes")

# Transformer2 stage - join with hf_casemgt_proccd_codes
df_Transformer2_left = df_Transformer_out.alias("GetCodes")
df_Transformer2_codes = df_hf_casemgt_proccd_codes.alias("Codes")

joined_Transformer2 = df_Transformer2_left.join(
    df_Transformer2_codes,
    (df_Transformer2_left["PROC_CD_SK"] == df_Transformer2_codes["PROC_CD_SK"]),
    "left"
)

df_Transformer2_out = joined_Transformer2.select(
    F.col("GetCodes.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("GetCodes.INSRT_UPDT_CD"),
    F.col("GetCodes.DISCARD_IN"),
    F.col("GetCodes.PASS_THRU_IN"),
    F.col("GetCodes.FIRST_RECYC_DT"),
    F.col("GetCodes.ERR_CT"),
    F.col("GetCodes.RECYCLE_CT"),
    F.col("GetCodes.SRC_SYS_CD"),
    F.col("GetCodes.PRI_KEY_STRING"),
    F.col("GetCodes.CRT_RUN_CYC_EXCTN_SK"),
    F.col("GetCodes.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GetCodes.CMCM_ID"),
    F.col("GetCodes.MEME_CK"),
    F.col("GetCodes.GRGR_CK"),
    F.col("GetCodes.SBSB_CK"),
    F.col("GetCodes.SGSG_CK"),
    F.col("GetCodes.CMCT_SEQ_NO"),
    F.col("GetCodes.CMST_SEQ_NO"),
    F.col("GetCodes.CMCM_MCTR_ORIG"),
    F.col("GetCodes.CMCM_INPUT_DT"),
    F.col("GetCodes.CMCM_INPUT_USID"),
    F.col("GetCodes.CMCM_USID_PRI"),
    F.col("GetCodes.CMCM_BEG_DT"),
    F.col("GetCodes.CMCM_END_DT"),
    F.col("GetCodes.CMCM_NEXT_REV_DT"),
    F.col("GetCodes.CMCM_MCTR_TYPE"),
    F.col("GetCodes.CMCM_MCTR_CPLX"),
    F.col("GetCodes.CMCM_ME_AGE"),
    F.col("GetCodes.CMCM_SUMMARY"),
    F.col("GetCodes.IDCD_ID"),
    F.col("GetCodes.NTNB_ID"),
    F.col("GetCodes.CSPD_CAT"),
    F.col("GetCodes.CMST_STS"),
    F.col("GetCodes.CMST_STS_DTM"),
    F.col("GetCodes.PDPD_ID"),
    F.col("GetCodes.MED_DENT_ID"),
    F.col("GetCodes.SGSG_ID"),
    F.col("GetCodes.GRGR_ID"),
    F.col("GetCodes.DIAG_CD_TYP_CD"),
    df_Transformer2_codes["PROC_CD"].alias("PROC_CD"),
    df_Transformer2_codes["PROC_CD_CAT_CD"].alias("PROC_CD_CAT_CD"),
    df_Transformer2_codes["PROC_CD_TYP_CD"].alias("PROC_CD_TYP_CD")
).alias("Transform")

# PrimaryKey stage -> left joins with "hf_med_case_mgt" (scenario B dummy) and "hf_casemgt_idcd_typ"
df_PrimaryKey_left = df_Transformer2_out.alias("Transform")
df_PrimaryKey_lkup = df_dummy_hf_med_case_mgt.alias("lkup")
df_PrimaryKey_refIdcdTyp = df_hf_casemgt_idcd_typ.alias("refIdcdTyp")

joined_PrimaryKey = (
    df_PrimaryKey_left
    .join(
        df_PrimaryKey_lkup,
        [
            df_PrimaryKey_left["SRC_SYS_CD"]==df_PrimaryKey_lkup["SRC_SYS_CD"],
            df_PrimaryKey_left["CMCM_ID"]==df_PrimaryKey_lkup["CMCM_ID"]
        ],
        "left"
    )
    .join(
        df_PrimaryKey_refIdcdTyp,
        df_PrimaryKey_left["DIAG_CD_TYP_CD"]==df_PrimaryKey_refIdcdTyp["SRC_CD"],
        "left"
    )
)

df_PrimaryKey = joined_PrimaryKey.select(
    df_PrimaryKey_left["JOB_EXCTN_RCRD_ERR_SK"].alias("JOB_EXCTN_RCRD_ERR_SK"),
    df_PrimaryKey_left["INSRT_UPDT_CD"].alias("INSRT_UPDT_CD"),
    df_PrimaryKey_left["DISCARD_IN"].alias("DISCARD_IN"),
    df_PrimaryKey_left["FIRST_RECYC_DT"].alias("FIRST_RECYC_DT"),
    df_PrimaryKey_left["PASS_THRU_IN"].alias("PASS_THRU_IN"),
    df_PrimaryKey_left["ERR_CT"].alias("ERR_CT"),
    df_PrimaryKey_left["RECYCLE_CT"].alias("RECYCLE_CT"),
    df_PrimaryKey_left["SRC_SYS_CD"].alias("SRC_SYS_CD"),
    df_PrimaryKey_left["PRI_KEY_STRING"].alias("PRI_KEY_STRING"),
    F.when(F.isnull(df_PrimaryKey_lkup["CASE_MGT_SK"])==True,
           SurrogateKeyGen(  # Stage variable "SK" -> KeyMgtGetNextValueConcurrent
               F.col("Transform"),
               "<DB sequence name>",
               "CASE_MGT_SK",
               "<schema>",
               "<secret_name>"
           )
    ).otherwise(df_PrimaryKey_lkup["CASE_MGT_SK"]).alias("CASE_MGT_SK"),
    df_PrimaryKey_left["CMCM_ID"].alias("CMCM_ID"),
    F.when(F.isnull(df_PrimaryKey_lkup["CASE_MGT_SK"])==True, F.lit(CurrRunCycle)).otherwise(df_PrimaryKey_lkup["CRT_RUN_CYC_EXCTN_SK"]).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    df_PrimaryKey_left["IDCD_ID"].alias("IDCD_ID"),
    df_PrimaryKey_left["GRGR_CK"].alias("GRGR_CK"),
    df_PrimaryKey_left["CMCM_INPUT_USID"].alias("CMCM_INPUT_USID"),
    df_PrimaryKey_left["NTNB_ID"].alias("MED_MGT_NOTE_ID"),
    df_PrimaryKey_left["MEME_CK"].alias("MEME_CK"),
    df_PrimaryKey_left["CMCM_USID_PRI"].alias("CMCM_USID_PRI"),
    df_PrimaryKey_left["PROC_CD"].alias("IPCD_ID"),
    df_PrimaryKey_left["SGSG_CK"].alias("SGSG_CK"),
    df_PrimaryKey_left["SBSB_CK"].alias("SBSB_CK"),
    df_PrimaryKey_left["CSPD_CAT"].alias("CSPD_CAT"),
    df_PrimaryKey_left["CMCM_MCTR_CPLX"].alias("CMCM_MCTR_CPLX"),
    df_PrimaryKey_left["CMCM_MCTR_ORIG"].alias("CMCM_MCTR_ORIG"),
    df_PrimaryKey_left["CMST_STS"].alias("CMST_STS"),
    df_PrimaryKey_left["CMCM_MCTR_TYPE"].alias("CMCM_MCTR_TYPE"),
    df_PrimaryKey_left["CMCM_END_DT"].alias("CMCM_END_DT"),
    df_PrimaryKey_left["CMCM_INPUT_DT"].alias("CMCM_INPUT_DT"),
    df_PrimaryKey_left["CMCM_NEXT_REV_DT"].alias("CMCM_NEXT_REV_DT"),
    df_PrimaryKey_left["CMCM_BEG_DT"].alias("CMCM_BEG_DT"),
    df_PrimaryKey_left["CMST_STS_DTM"].alias("CMST_STS_DTM"),
    df_PrimaryKey_left["CMCM_ME_AGE"].alias("CMCM_ME_AGE"),
    df_PrimaryKey_left["CMCT_SEQ_NO"].alias("CMCT_SEQ_NO"),
    df_PrimaryKey_left["CMST_SEQ_NO"].alias("CMST_SEQ_NO"),
    df_PrimaryKey_left["CMCM_SUMMARY"].alias("CMCM_SUMMARY"),
    df_PrimaryKey_left["PDPD_ID"].alias("PDPD_ID"),
    df_PrimaryKey_left["MED_DENT_ID"].alias("MED_DENT_ID"),
    df_PrimaryKey_left["SGSG_ID"].alias("SGSG_ID"),
    df_PrimaryKey_left["GRGR_ID"].alias("GRGR_ID"),
    F.when(F.isnull(df_PrimaryKey_refIdcdTyp["DIAG_CD_TYP_CD"])==True, "ICD9").otherwise(df_PrimaryKey_refIdcdTyp["DIAG_CD_TYP_CD"]).alias("DIAG_CD_TYP_CD"),
    df_PrimaryKey_left["PROC_CD_CAT_CD"].alias("PROC_CD_CAT_CD"),
    df_PrimaryKey_left["PROC_CD_TYP_CD"].alias("PROC_CD_TYP_CD"),
    F.when(F.isnull(df_PrimaryKey_lkup["CASE_MGT_SK"])==True, F.lit("INSERT")).otherwise(F.lit("UPDATE")).alias("_upd_insert_indicator")
)

# Extract final to two outputs: 
# 1) "IdsCaseMgtExtr" => CSeqFileStage
#    We select the columns in the correct order, apply rpad if char/varchar.

df_IdsCaseMgtExtr_cols = [
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType()),
    StructField("INSRT_UPDT_CD", StringType()),
    StructField("DISCARD_IN", StringType()),
    StructField("FIRST_RECYC_DT", StringType()),
    StructField("PASS_THRU_IN", StringType()),
    StructField("ERR_CT", IntegerType()),
    StructField("RECYCLE_CT", IntegerType()),
    StructField("SRC_SYS_CD", StringType()),
    StructField("PRI_KEY_STRING", StringType()),
    StructField("CASE_MGT_SK", IntegerType()),
    StructField("CMCM_ID", StringType()),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType()),
    StructField("IDCD_ID", StringType()),
    StructField("GRGR_CK", IntegerType()),
    StructField("CMCM_INPUT_USID", StringType()),
    StructField("MED_MGT_NOTE_ID", StringType()),
    StructField("MEME_CK", IntegerType()),
    StructField("CMCM_USID_PRI", StringType()),
    StructField("IPCD_ID", StringType()),
    StructField("SGSG_CK", IntegerType()),
    StructField("SBSB_CK", IntegerType()),
    StructField("CSPD_CAT", StringType()),
    StructField("CMCM_MCTR_CPLX", StringType()),
    StructField("CMCM_MCTR_ORIG", StringType()),
    StructField("CMST_STS", StringType()),
    StructField("CMCM_MCTR_TYPE", StringType()),
    StructField("CMCM_END_DT", StringType()),
    StructField("CMCM_INPUT_DT", StringType()),
    StructField("CMCM_NEXT_REV_DT", StringType()),
    StructField("CMCM_BEG_DT", StringType()),
    StructField("CMST_STS_DTM", StringType()),
    StructField("CMCM_ME_AGE", IntegerType()),
    StructField("CMCT_SEQ_NO", IntegerType()),
    StructField("CMST_SEQ_NO", IntegerType()),
    StructField("CMCM_SUMMARY", StringType()),
    StructField("PDPD_ID", StringType()),
    StructField("MED_DENT_ID", StringType()),
    StructField("SGSG_ID", StringType()),
    StructField("GRGR_ID", StringType()),
    StructField("DIAG_CD_TYP_CD", StringType()),
    StructField("PROC_CD_CAT_CD", StringType()),
    StructField("PROC_CD_TYP_CD", StringType())
]

df_IdsCaseMgtExtr_select = []
for field in df_IdsCaseMgtExtr_cols:
    cname = field.name
    ctype = field.dataType
    if isinstance(ctype, StringType):
        # find length from job metadata if "char"/"varchar" length is known, else pick some default
        # The job lists many char types. We'll match for known columns:
        length_map = {
            "INSRT_UPDT_CD": 10,
            "DISCARD_IN": 1,
            "PASS_THRU_IN": 1,
            "SRC_SYS_CD": 10,  # job used char(10) in some places
            "CMCM_ID": 9,
            "IDCD_ID": 10,
            "CMCM_INPUT_USID": 10,
            "CMCM_USID_PRI": 10,
            "IPCD_ID": 7,
            "CSPD_CAT": 1,
            "CMCM_MCTR_CPLX": 4,
            "CMCM_MCTR_ORIG": 4,
            "CMST_STS": 2,
            "CMCM_MCTR_TYPE": 4,
            "PDPD_ID": 8,
            "MED_DENT_ID": 1,
            "SGSG_ID": 4,
            "GRGR_ID": 8,
            "DIAG_CD_TYP_CD": 10,
            "PROC_CD_CAT_CD": 10,
            "PROC_CD_TYP_CD": 10
        }
        pad_len = length_map.get(cname, 50)
        df_IdsCaseMgtExtr_select.append(F.rpad(F.col(cname), pad_len, " ").alias(cname))
    else:
        df_IdsCaseMgtExtr_select.append(F.col(cname))

df_IdsCaseMgtExtr = df_PrimaryKey.select(
    [c.name for c in df_IdsCaseMgtExtr_cols] + ["_upd_insert_indicator"]
)
df_IdsCaseMgtExtr = df_IdsCaseMgtExtr.select(df_IdsCaseMgtExtr_select + [F.col("_upd_insert_indicator")])

# Write IdsCaseMgtExtr to a .dat file
final_file_path = f"{adls_path}/key/{TmpOutFile}"
write_files(
    df_IdsCaseMgtExtr.drop("_upd_insert_indicator"),
    final_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Merge logic for scenario B "hf_write" -> merges into dummy_hf_med_case_mgt
df_hf_write = df_IdsCaseMgtExtr.filter(F.col("_upd_insert_indicator")=="INSERT").select(
    "SRC_SYS_CD", "CMCM_ID", "CRT_RUN_CYC_EXCTN_SK", "CASE_MGT_SK"
)
temp_table_name_b = "STAGING.FctsCaseMgtExtr_hf_med_case_mgt_temp"
execute_dml(f"DROP TABLE IF EXISTS {temp_table_name_b}", jdbc_url_ids, jdbc_props_ids)

df_hf_write.write.jdbc(
    url=jdbc_url_ids,
    table=temp_table_name_b,
    mode="overwrite",
    properties=jdbc_props_ids
)

merge_sql_b = f"""
MERGE dummy_hf_med_case_mgt AS T
USING {temp_table_name_b} AS S
ON T.SRC_SYS_CD = S.SRC_SYS_CD
   AND T.CMCM_ID = S.CMCM_ID
WHEN MATCHED THEN
  UPDATE SET
    T.CRT_RUN_CYC_EXCTN_SK = S.CRT_RUN_CYC_EXCTN_SK,
    T.CASE_MGT_SK = S.CASE_MGT_SK
WHEN NOT MATCHED THEN
  INSERT
    (SRC_SYS_CD, CMCM_ID, CRT_RUN_CYC_EXCTN_SK, CASE_MGT_SK)
  VALUES
    (S.SRC_SYS_CD, S.CMCM_ID, S.CRT_RUN_CYC_EXCTN_SK, S.CASE_MGT_SK);
"""

execute_dml(merge_sql_b, jdbc_url_ids, jdbc_props_ids)

# Facets_Source -> "Snapshot" -> Transformer -> Snapshot_File
query_Facets_Source_Snapshot = f"""
SELECT CMCM_ID
FROM {FacetsOwner}.CMC_CMCM_CASE_MGT A
WHERE NOT EXISTS (
  SELECT DISTINCT CMC_GRGR.GRGR_CK
  FROM {bcbs_secret_name}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
       {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
  WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
    AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
    AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
    AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
    AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
    AND CMC_GRGR.GRGR_CK=A.GRGR_CK
)
AND CMST_STS_DTM >= '{LastRunDTm}'

UNION

SELECT A.CMCM_ID
FROM {FacetsOwner}.CMC_CMCM_CASE_MGT A,
     tempdb..{CMDriverTable} D
WHERE A.CMCM_ID = D.CMCM_ID
  AND NOT EXISTS (
    SELECT DISTINCT CMC_GRGR.GRGR_CK
    FROM {bcbs_secret_name}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
         {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR
    WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
      AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
      AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
      AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_PRITR.CRITR_VAL_THRU_TX
      AND CMC_GRGR.GRGR_CK=A.GRGR_CK
)
"""

df_Facets_Source_Snapshot = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", query_Facets_Source_Snapshot)
    .load()
)

df_Transform_out_snap = df_Facets_Source_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    trim(Convert("CHAR(10):CHAR(13):CHAR(9)","",F.col("CMCM_ID"))).alias("CASE_MGT_ID")
)

schema_snap = [
    StructField("SRC_SYS_CD_SK", IntegerType()),
    StructField("CASE_MGT_ID", StringType())
]
df_snap_select = []
for field in schema_snap:
    cname = field.name
    ctype = field.dataType
    if isinstance(ctype, StringType):
        # Hardcode lengths if needed from job metadata
        length_map_snap = {
            "CASE_MGT_ID": 9
        }
        pad_len_snap = length_map_snap.get(cname, 50)
        df_snap_select.append(F.rpad(F.col(cname), pad_len_snap, " ").alias(cname))
    else:
        df_snap_select.append(F.col(cname))

df_Snapshot_File = df_Transform_out_snap.select(df_snap_select)

snapshot_file_path = f"{adls_path}/load/B_CASE_MGT.dat"
write_files(
    df_Snapshot_File,
    snapshot_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)