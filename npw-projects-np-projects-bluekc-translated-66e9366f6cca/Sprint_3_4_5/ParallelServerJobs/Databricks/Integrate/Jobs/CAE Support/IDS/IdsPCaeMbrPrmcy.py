# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY:  IDSPCaeSeq
# MAGIC 
# MAGIC PROCESSING:   This process pull all active members and determines the setting of the primacy indicator.  The primacy indicator is a Y/N flag that determines the "primary" member record for an individual that may or may not have multiple coverage.  Only medical coverage is determined in the setting of the indicator.  Please see CDMA for a complete description of the rules for setting the indicator.  The process is too extensive to explain here.
# MAGIC 
# MAGIC RERUN INFORMATION: 
# MAGIC              PREVIOUS RUN SUCCESSFUL:   Load files will have to be recreated. 
# MAGIC              PREVIOUS RUN ABORTED:         Nothing to reset, just restart.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer\(9)Date                 \(9)Project/Altiris #\(9)             Change Description                                                                            Development Project\(9)  Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------\(9)--------------------\(9)------------------------\(9)            -----------------------------------------------------------------------\(9)                         --------------------------------         -----------------------------\(9)----------------------------       
# MAGIC Laurel Kindley\(9)2006-10-04                                 \(9)            Original Programming.  \(9)\(9)\(9)                                                                 
# MAGIC Laurel Kindley\(9)2007-06-15\(9)\(9)\(9)            Modified the logic for setting the primacy for those
# MAGIC \(9)\(9)\(9)\(9)\(9)\(9)            individuals who are "dependent" only coverage.
# MAGIC Laurel Kindley\(9)2008-05-12\(9)Proj 3051\(9)\(9)            Added filter to pull only the FEP members who are\(9)                         devlIDS                           Steph Goddard       05/28/2008
# MAGIC 
# MAGIC 
# MAGIC Karthik 
# MAGIC Chintalapani \(9)2012-10-25\(9)4784 SC DEFECTS          Inserted  a new Logic into the existing job  to include 
# MAGIC                                                                                                          a new table PRNT_GRP and  exclude GRP                                          IntegrateNewDevl            Bhoomi Dasari       10/26/2012
# MAGIC                                                                                                          where PRNT_GRP_ID<>'650600000' 
# MAGIC                                                                                                          Changed the buffer size of the job to 512/300 as per the standard.  
# MAGIC                                                                                                                                                                                                          
# MAGIC                                               .
# MAGIC Karthik 
# MAGIC Chintalapani \(9)2012-12-20\(9)TTR  1510                      Updated the source query to filter  SC members using PRNT_GRP_ID    IntegrateNewDevl           Kalyan Neelam      2012-12-20
# MAGIC                                                                                                        650600000 as per the new mapping rules.
# MAGIC  
# MAGIC Terri O'Bryan            2013-03-04              IM115757                        Changed SQL in IDS extract for IDS "mbr_med" link                               IntegrateNewDevl            Kalyan Neelam       2013-03-06
# MAGIC                                                                                                          replacing condition   'MBR.MBR_SK >1'
# MAGIC                                                                                                          with condition   '(MBR.MBR_SK <>0  AND MBR.MBR_SK <>1)'                                                                                                                                                                                                                                                                  
# MAGIC                                                                                                                                                                                                               
# MAGIC Karthik 
# MAGIC Chintalapani \(9)2015-09-15\(9)5212 PI                      Added the filter HOST_MBR_IN<>Y' in the extract query                              IntegrateDev1          
# MAGIC                                                                                                  to exclude host members from being populated to the primacy table 
# MAGIC 
# MAGIC Manasa Andru        2017-01-06            TFS - 16021                  Corrected the extract SQL in the IDS stage in the mbr_med link to               IntegrateDev1                  Kalyan Neelam       2017-01-09
# MAGIC                                                                                                      get the right values from the CD_MPPNG table for the field
# MAGIC                                                                                                                       MBR_RELSHP_CD.
# MAGIC 
# MAGIC Brent Leland            2023-04-20             Prod Support              Changed SQL where clause in IDS and COB stages.  Changed                   IntegrateDev1                    Jeyaprasanna         2023-04-28
# MAGIC                                                                                                   MBR_D term_dt to MBR_ENR term_dt for FEP.  MBR_D is not 
# MAGIC                                                                                                   getting term date correctly.                                                                             \(9)\(9)\(9)\(9)\(9)\(9)

# MAGIC Get non-medicaid group count
# MAGIC Get member, grp_sk, cd mapping, and member enrollment from IDS
# MAGIC COB_Trans is used to help evaluate the primacy value setting if a member has COB coverage.
# MAGIC After the primacy indicator is set, determine if a single member sk  has multiple primacy indicators.  If there is at least one Y for a MBR_SK, then set the primacy for the MBR_SK to Y.  Otherwise, set it to N.
# MAGIC Copy of hf_p_cae_mbr_cov_cnt populated above
# MAGIC Put all dependents to one hash file and all subscribers to another.  We need the subscriber information for each member in one record.
# MAGIC Get COB info
# MAGIC Get coverage count
# MAGIC Original hf_p_cae_mbr_cov_cnt.  Also used in main BUSINESS RULES transform
# MAGIC Group the individual records to determine the earliest effective date, earliest birth month for the subscriber for a member, and the minimum unique key.
# MAGIC Determine the primacy flag for dependents only and pass to the main transform.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrDate = get_widget_value("CurrDate","2013-03-04")

# =====================================================================================================
# DB2Connector Stage: COB (Database=IDS)
# Two output pins => df_COB_b_on_b and df_COB_mbr_cob
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

query_COB_b_on_b = f"""
SELECT MBR.INDV_BE_KEY as INDV_BE_KEY, MBR2.MBR_SK as MBR_SK, MBR_COB.OTHR_CAR_POL_ID as OTHR_CAR_POL_ID, CD_MPPNG2.TRGT_CD as TRGT_CD
FROM {IDSOwner}.mbr_cob MBR_COB,
     {IDSOwner}.mbr MBR,
     {IDSOwner}.cd_mppng CD_MPPNG,
     {IDSOwner}.cd_mppng CD_MPPNG2,
     {IDSOwner}.mbr_enr MBR_ENR,
     {IDSOwner}.sub SUB,
     {IDSOwner}.sub SUB2,
     {IDSOwner}.cd_mppng CD_MPPNG3,
     {IDSOwner}.mbr MBR2,
     {IDSOwner}.mbr_enr MBR_ENR2,
     {IDSOwner}.prod PROD,
     {IDSOwner}.prod_sh_nm PROD_SH_NM,
     {IDSOwner}.cd_mppng CD_MPPNG4,
     {IDSOwner}.grp GRP,
     {IDSOwner}.grp GRP2
WHERE MBR_COB.MBR_SK = MBR.MBR_SK
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
  AND MBR_COB.MBR_COB_OTHR_CAR_ID_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD IN('4','5')
  AND MBR_COB.MBR_COB_PAYMT_PRTY_CD_SK = CD_MPPNG2.CD_MPPNG_SK
  AND CD_MPPNG2.TRGT_CD IN('PRI','SEC')
  AND MBR_COB.OTHR_CAR_POL_ID <> 'NA'
  AND MBR_COB.OTHR_CAR_POL_ID = SUB.SUB_ID
  AND SUB.SUB_SK = MBR2.SUB_SK
  AND MBR.MBR_SK = MBR_ENR.MBR_SK
  AND MBR2.MBR_SK = MBR_ENR2.MBR_SK
  AND MBR2.SUB_SK = SUB2.SUB_SK
  AND SUB2.GRP_SK = GRP2.GRP_SK
  AND MBR_COB.EFF_DT_SK <= '{CurrDate}'
  AND MBR_COB.TERM_DT_SK >= '{CurrDate}'
  AND MBR_ENR.EFF_DT_SK <= '{CurrDate}'
  AND MBR_ENR.ELIG_IN='Y'
  AND MBR_ENR2.EFF_DT_SK <= '{CurrDate}'
  AND MBR_ENR2.ELIG_IN='Y'
  AND MBR_ENR2.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD_MPPNG3.CD_MPPNG_SK
  AND CD_MPPNG3.TRGT_CD = 'MED'
  AND MBR_ENR.PROD_SK = PROD.PROD_SK
  AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
  AND PROD_SH_NM.MCARE_BNF_CD_SK = CD_MPPNG4.CD_MPPNG_SK
  AND (CD_MPPNG4.TRGT_CD <> 'MCARE' OR PROD_SH_NM.MCARE_SUPLMT_COV_IN <> 'Y')
  AND ((GRP.GRP_ID <> '10023000' AND MBR_ENR.TERM_DT_SK >= '{CurrDate}') OR (GRP.GRP_ID = '10023000' AND MBR_ENR.TERM_DT_SK >= '{CurrDate}' AND MBR.FEP_LOCAL_CNTR_IN = 'Y'))
  AND ((GRP2.GRP_ID <> '10023000' AND MBR_ENR2.TERM_DT_SK >= '{CurrDate}') OR (GRP2.GRP_ID = '10023000' AND MBR_ENR.TERM_DT_SK >= '{CurrDate}' AND MBR.FEP_LOCAL_CNTR_IN = 'Y'))
"""

df_COB_b_on_b = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_COB_b_on_b)
    .load()
)

query_COB_mbr_cob = f"""
SELECT MBR.INDV_BE_KEY as INDV_BE_KEY,
       MBR.MBR_SK as MBR_SK,
       CD_MPPNG2.TRGT_CD as TRGT_CD,
       MBR_COB.OTHR_CAR_POL_ID as OTHR_CAR_POL_ID
FROM {IDSOwner}.mbr MBR,
     {IDSOwner}.mbr_cob MBR_COB,
     {IDSOwner}.cd_mppng CD_MPPNG,
     {IDSOwner}.cd_mppng CD_MPPNG2,
     {IDSOwner}.mbr_enr MBR_ENR,
     {IDSOwner}.cd_mppng CD_MPPNG3,
     {IDSOwner}.prod PROD,
     {IDSOwner}.prod_sh_nm PROD_SH_NM,
     {IDSOwner}.cd_mppng CD_MPPNG4,
     {IDSOwner}.grp GRP,
     {IDSOwner}.sub SUB
WHERE MBR.MBR_SK = MBR_COB.MBR_SK
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
  AND MBR.MBR_SK = MBR_ENR.MBR_SK
  AND MBR_COB.EFF_DT_SK<='{CurrDate}'
  AND MBR_COB.TERM_DT_SK>='{CurrDate}'
  AND MBR_ENR.EFF_DT_SK <= '{CurrDate}'
  AND MBR_ENR.ELIG_IN='Y'
  AND MBR_COB.MBR_COB_TYP_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD IN('COM','MCARE','MCARED')
  AND MBR_COB.MBR_COB_PAYMT_PRTY_CD_SK = CD_MPPNG2.CD_MPPNG_SK
  AND CD_MPPNG2.TRGT_CD IN ('PRI','SEC')
  AND MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = CD_MPPNG3.CD_MPPNG_SK
  AND CD_MPPNG3.TRGT_CD = 'MED'
  AND MBR_ENR.PROD_SK = PROD.PROD_SK
  AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
  AND PROD_SH_NM.MCARE_BNF_CD_SK = CD_MPPNG4.CD_MPPNG_SK
  AND (CD_MPPNG4.TRGT_CD <> 'MCARE' OR PROD_SH_NM.MCARE_SUPLMT_COV_IN <> 'Y')
  AND ((GRP.GRP_ID <> '10023000' AND MBR_ENR.TERM_DT_SK >= '{CurrDate}') OR (GRP.GRP_ID = '10023000' AND MBR_ENR.TERM_DT_SK  >= '{CurrDate}' AND MBR.FEP_LOCAL_CNTR_IN = 'Y'))
"""

df_COB_mbr_cob = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_COB_mbr_cob)
    .load()
)

# =====================================================================================================
# DB2Connector Stage: MEDICAID (Database=IDS) => Single output pin => "hf_p_cae_non_medicaid_grp"
query_MEDICAID = f"""
SELECT MBR.INDV_BE_KEY as INDV_BE_KEY,
       COUNT(MBR.MBR_SK) as CNT_MBR_SK
FROM {IDSOwner}.mbr MBR,
     {IDSOwner}.grp GRP,
     {IDSOwner}.sub SUB,
     {IDSOwner}.mbr_enr MBR_ENR,
     {IDSOwner}.cd_mppng CD_MPPNG,
     {IDSOwner}.prod PROD,
     {IDSOwner}.prod_sh_nm PROD_SH_NM,
     {IDSOwner}.cd_mppng CD_MPPNG2
WHERE MBR.MBR_SK = MBR_ENR.MBR_SK
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
  AND MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK=CD_MPPNG.CD_MPPNG_SK
  AND CD_MPPNG.TRGT_CD = 'MED'
  AND MBR_ENR.EFF_DT_SK <= '{CurrDate}'
  AND MBR_ENR.TERM_DT_SK >= '{CurrDate}'
  AND MBR_ENR.ELIG_IN='Y'
  AND GRP.GRP_ID <> '10024000'
  AND MBR_ENR.PROD_SK = PROD.PROD_SK
  AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
  AND PROD_SH_NM.MCARE_BNF_CD_SK = CD_MPPNG2.CD_MPPNG_SK
  AND (CD_MPPNG2.TRGT_CD <> 'MCARE' OR PROD_SH_NM.MCARE_SUPLMT_COV_IN <> 'Y')
GROUP BY MBR.INDV_BE_KEY
"""

df_MEDICAID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_MEDICAID)
    .load()
)

# =====================================================================================================
# DB2Connector Stage: IDS (Database=IDS)
# Two output pins: "mbr_med" and "cd_mppng"
query_IDS_mbr_med = f"""
SELECT MBR.INDV_BE_KEY,
       MBR.MBR_SK,
       MBR_ENR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK,
       MBR.BRTH_DT_SK,
       SUB.SUB_ID,
       MBR.MBR_GNDR_CD_SK,
       MBR.SUB_SK,
       MBR.MBR_UNIQ_KEY,
       P_CAE_UNIQ_KEY.CAE_UNIQ_KEY,
       MBR_ENR.EFF_DT_SK,
       CLNDR_DT.MO_NO,
       CD_MPPNG2.TRGT_CD,
       GRP.GRP_ID,
       GRP.GRP_SK
FROM {IDSOwner}.mbr MBR,
     {IDSOwner}.mbr_enr MBR_ENR,
     {IDSOwner}.grp GRP,
     {IDSOwner}.prnt_grp PRNT_GRP,
     {IDSOwner}.prod PROD,
     {IDSOwner}.prod_sh_nm PROD_SH_NM,
     {IDSOwner}.clndr_dt CLNDR_DT,
     {IDSOwner}.cd_mppng CD_MPPNG,
     {IDSOwner}.cd_mppng CD_MPPNG2,
     {IDSOwner}.cd_mppng CD_MPPNG3,
     {IDSOwner}.p_cae_uniq_key P_CAE_UNIQ_KEY,
     {IDSOwner}.sub SUB
WHERE GRP.PRNT_GRP_SK=PRNT_GRP.PRNT_GRP_SK
  AND PRNT_GRP.PRNT_GRP_ID <> '650600000'
  AND MBR.INDV_BE_KEY=P_CAE_UNIQ_KEY.INDV_BE_KEY
  AND MBR.MBR_SK = MBR_ENR.MBR_SK
  AND MBR_ENR.PROD_SK = PROD.PROD_SK
  AND PROD.PROD_SH_NM_SK = PROD_SH_NM.PROD_SH_NM_SK
  AND PROD_SH_NM.MCARE_BNF_CD_SK = CD_MPPNG.CD_MPPNG_SK
  AND MBR.BRTH_DT_SK = CLNDR_DT.CLNDR_DT_SK
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.GRP_SK = GRP.GRP_SK
  AND MBR.MBR_RELSHP_CD_SK = CD_MPPNG2.CD_MPPNG_SK
  AND MBR_ENR.MBR_ENR_ELIG_RSN_CD_SK=CD_MPPNG3.CD_MPPNG_SK
  AND MBR_ENR.EFF_DT_SK <= '{CurrDate}'
  AND MBR_ENR.ELIG_IN='Y'
  AND ( MBR.MBR_SK <> 0  AND MBR.MBR_SK <> 1 )
  AND MBR.INDV_BE_KEY <>1
  AND PROD_SH_NM.MCARE_SUPLMT_COV_IN <> 'Y'
  AND MBR.HOST_MBR_IN <> 'Y'
  AND CD_MPPNG3.TRGT_CD <> 'MCARE'
  AND ((GRP.GRP_ID = '10023000' AND MBR_ENR.TERM_DT_SK >= '{CurrDate}') OR (GRP.GRP_ID <> '10023000' AND MBR_ENR.TERM_DT_SK >= '{CurrDate}'))
"""

df_IDS_mbr_med = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_IDS_mbr_med)
    .load()
)

query_IDS_cd_mppng = f"""
SELECT CD_MPPNG.CD_MPPNG_SK as CD_MPPNG_SK,
       CD_MPPNG.TRGT_CD as TRGT_CD
FROM {IDSOwner}.cd_mppng CD_MPPNG
"""

df_IDS_cd_mppng = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", query_IDS_cd_mppng)
    .load()
)

# =====================================================================================================
# Because all CHashedFileStage and .ds files must be parquet (Scenario C unless indicated otherwise).
# We will read each hashed file as parquet if the job reads from it,
# and write each hashed file as parquet if the job writes to it.

# ---------------------------------------------------------------------------------
# Read CHashedFileStage: hf_p_cae_mbr_cov_cnt_copy (only output pins -> it's actually sourcing data)
# We'll read from hf_p_cae_mbr_cov_cnt_copy.parquet
df_hf_p_cae_mbr_cov_cnt_copy = spark.read.parquet("hf_p_cae_mbr_cov_cnt_copy.parquet")

# ---------------------------------------------------------------------------------
# Write CHashedFileStage: hf_p_cae_cob_othr (input from df_COB_b_on_b)
# So first we have df_COB_b_on_b => we want to store it in a parquet "hf_p_cae_cob_othr.parquet"
# The columns are [INDV_BE_KEY decimal(??), MBR_SK int, OTHR_CAR_POL_ID varchar, TRGT_CD varchar]
# We'll keep them as they come from df_COB_b_on_b. Then write.
df_hf_p_cae_cob_othr = df_COB_b_on_b.select(
    F.col("INDV_BE_KEY"),
    F.col("MBR_SK"),
    F.col("OTHR_CAR_POL_ID"),
    F.col("TRGT_CD")
)
write_files(
    df_hf_p_cae_cob_othr,
    "hf_p_cae_cob_othr.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Read it back if needed (the job indicates it is read again?), but the JSON shows it is used as input for a lookup link in BUSINESS_RULES. So let's also read it:
df_hf_p_cae_cob_othr_in = spark.read.parquet("hf_p_cae_cob_othr.parquet")

# ---------------------------------------------------------------------------------
# Write CHashedFileStage: hf_p_cae_mbr_cob (input from df_COB_mbr_cob)
df_hf_p_cae_mbr_cob = df_COB_mbr_cob.select(
    F.col("INDV_BE_KEY"),
    F.col("MBR_SK"),
    F.col("TRGT_CD"),
    F.col("OTHR_CAR_POL_ID")
)
write_files(
    df_hf_p_cae_mbr_cob,
    "hf_p_cae_mbr_cob.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
# We'll read from it if needed:
df_hf_p_cae_mbr_cob_in = spark.read.parquet("hf_p_cae_mbr_cob.parquet")

# ---------------------------------------------------------------------------------
# Write CHashedFileStage: hf_p_cae_non_medicaid_grp (input from df_MEDICAID)
df_hf_p_cae_non_medicaid_grp = df_MEDICAID.select(
    F.col("INDV_BE_KEY"),
    F.col("CNT_MBR_SK")
)
write_files(
    df_hf_p_cae_non_medicaid_grp,
    "hf_p_cae_non_medicaid_grp.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_non_medicaid_grp_in = spark.read.parquet("hf_p_cae_non_medicaid_grp.parquet")

# ---------------------------------------------------------------------------------
# Write CHashedFileStage: LOOKUP => hf_p_cae_mbr, hf_cae_cd_mppng_prmcy (two output pins from reading IDS data)
# The first output pin "mbr_med_out" columns:
#   INDV_BE_KEY, MBR_SK, MBR_ENR_CLS_PLN_PROD_CAT_CD_SK, BRTH_DT_SK(char(10)), SUB_ID, MBR_GNDR_CD_SK, SUB_SK, MBR_UNIQ_KEY,
#   CAE_UNIQ_KEY, EFF_DT_SK(char(10)), MO_NO, MBR_RELSHP_CD, GRP_ID, GRP_SK
df_hf_p_cae_mbr = df_IDS_mbr_med.select(
    F.col("INDV_BE_KEY"),
    F.col("MBR_SK"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
    F.col("BRTH_DT_SK"),
    F.col("SUB_ID"),
    F.col("MBR_GNDR_CD_SK"),
    F.col("SUB_SK"),
    F.col("MBR_UNIQ_KEY"),
    F.col("CAE_UNIQ_KEY"),
    F.col("EFF_DT_SK"),
    F.col("MO_NO"),
    F.col("TRGT_CD").alias("MBR_RELSHP_CD"),  # The JSON shows "MBR_RELSHP_CD" is "TRGT_CD"? Actually in the JSON: "MBR_RELSHP_CD" is a distinct column. But the mapping or final columns show "MBR_RELSHP_CD" from "CD_MPPNG2.TRGT_CD"? We must be consistent with the DataStage columns. Let's carefully see the final columns. The DS job has "MBR_RELSHP_CD" from "MBR_RELSHP_CD" expression. Wait, in the job, "MBR_RELSHP_CD" eventually came from "CD_MPPNG2.TRGT_CD" in some places. However, the base query includes "CD_MPPNG2.TRGT_CD" as well. The JSON columns are somewhat confusing. 
    # We must follow exactly the JSON stage output columns order from "mbr_med_out".
    F.col("GRP_ID"),
    F.col("GRP_SK")
)
write_files(
    df_hf_p_cae_mbr,
    "hf_p_cae_mbr.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# The second output pin "cd_mppng_gndr" => "hf_cae_cd_mppng_prmcy" also there's a third pin "cd_mppng_tgt"? Actually, the CHashedFileStage named "LOOKUP" has 2 output pins that reference the same file "hf_cae_cd_mppng_prmcy"? 
# We have df_IDS_cd_mppng with columns [CD_MPPNG_SK, TRGT_CD]. We are splitting them into two outputs: "cd_mppng_gndr" and "cd_mppng_tgt" but they reference the same base data. We'll just store the same data to "hf_cae_cd_mppng_prmcy.parquet".
df_hf_cae_cd_mppng_prmcy = df_IDS_cd_mppng.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD")
)
write_files(
    df_hf_cae_cd_mppng_prmcy,
    "hf_cae_cd_mppng_prmcy.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# We'll read them back as needed:
df_hf_p_cae_mbr_in = spark.read.parquet("hf_p_cae_mbr.parquet")
df_hf_cae_cd_mppng_prmcy_in = spark.read.parquet("hf_cae_cd_mppng_prmcy.parquet")

# ---------------------------------------------------------------------------------
# Next, we'll handle the Transformer "BuisnessRules" (StageName="BuisnessRules"), which has:
#   Primary link: mbr_med_out (df_hf_p_cae_mbr_in)
#   Lookup link #1: cd_mppng_gndr (left join on MBR_GNDR_CD_SK == CD_MPPNG_SK)
#   Lookup link #2: cd_mppng_tgt (left join on MBR_ENR_CLS_PLN_PROD_CAT_CD_SK == CD_MPPNG_SK)

# We join these dataframes:
df_buisnessrules_join = (
    df_hf_p_cae_mbr_in.alias("mbr_med_out")
    .join(
        df_hf_cae_cd_mppng_prmcy_in.alias("cd_mppng_gndr"),
        F.col("mbr_med_out.MBR_GNDR_CD_SK") == F.col("cd_mppng_gndr.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_hf_cae_cd_mppng_prmcy_in.alias("cd_mppng_tgt"),
        F.col("mbr_med_out.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK") == F.col("cd_mppng_tgt.CD_MPPNG_SK"),
        how="left"
    )
)

# DataStage Stage Variable: vFEP = If mbr_med_out.GRP_ID = '10023000' then If mbr_med_out.BRTH_DT_SK <> '1901-01-01' then 'Y' else 'N' else 'Y'
df_buisnessrules_enriched = df_buisnessrules_join.withColumn(
    "vFEP",
    F.when(
        (F.col("mbr_med_out.GRP_ID") == F.lit("10023000")) &
        (F.col("mbr_med_out.BRTH_DT_SK") != F.lit("1901-01-01")),
        F.lit("Y")
    ).when(
        (F.col("mbr_med_out.GRP_ID") == F.lit("10023000")) &
        (F.col("mbr_med_out.BRTH_DT_SK") == F.lit("1901-01-01")),
        F.lit("N")
    ).otherwise(F.lit("Y"))
)

# This Transformer has multiple output links, each with a Constraint:

# 1) Output => "hf_p_cae_mbr_prmcy_out" with link name=Output, Constraint = "vFEP = 'Y'"
df_buisnessrules_output = df_buisnessrules_enriched.filter(F.col("vFEP") == F.lit("Y")).select(
    F.col("mbr_med_out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("mbr_med_out.MBR_SK").alias("MBR_SK"),
    F.col("cd_mppng_tgt.TRGT_CD").alias("ENR_CD"),
    F.col("mbr_med_out.SUB_SK").alias("SUB_SK"),
    F.col("mbr_med_out.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("mbr_med_out.SUB_ID").alias("SUB_ID"),
    F.col("mbr_med_out.CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    F.when(F.col("cd_mppng_tgt.TRGT_CD") != F.lit("MED"), F.lit("N")).otherwise(F.lit(None)).alias("PRI_IN"),
    F.col("mbr_med_out.GRP_ID").alias("GRP_ID"),
    F.when(
        (F.col("mbr_med_out.BRTH_DT_SK") == F.lit("UNK")) | (F.col("mbr_med_out.BRTH_DT_SK") == F.lit("NA")),
        F.lit("1758-01-01")
    ).otherwise(F.col("mbr_med_out.BRTH_DT_SK")).alias("BRTH_DT_SK"),
    F.col("cd_mppng_gndr.TRGT_CD").alias("MBR_GNDR_CD_SK"),
    F.lit("CurrDate").alias("LAST_UPDATED"),  # Expression: "CurrDate" => literal
    F.col("mbr_med_out.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("mbr_med_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("mbr_med_out.MO_NO").alias("MO_NO"),
    F.col("mbr_med_out.GRP_SK").alias("GRP_SK")
)

# 2) Output => "hf_p_cae_mbr_cov" with link name=mbr_coverage, Constraint= "cd_mppng_tgt.TRGT_CD = 'MED' and vFEP = 'Y'"
df_buisnessrules_mbr_coverage = df_buisnessrules_enriched.filter(
    (F.col("cd_mppng_tgt.TRGT_CD") == F.lit("MED")) &
    (F.col("vFEP") == F.lit("Y"))
).select(
    F.col("mbr_med_out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("mbr_med_out.MBR_SK").alias("MBR_SK")
)

# 3) Output => "hf_p_cae_sub_out" with link name=subscribers, Constraint= "mbr_med_out.MBR_RELSHP_CD = 'SUB' and cd_mppng_tgt.TRGT_CD = 'MED' and vFEP = 'Y' and mbr_med_out.GRP_ID <> '10024000'"
df_buisnessrules_subscribers = df_buisnessrules_enriched.filter(
    (F.col("mbr_med_out.MBR_RELSHP_CD") == F.lit("SUB")) &
    (F.col("cd_mppng_tgt.TRGT_CD") == F.lit("MED")) &
    (F.col("vFEP") == F.lit("Y")) &
    (F.col("mbr_med_out.GRP_ID") != F.lit("10024000"))
).select(
    F.col("mbr_med_out.SUB_SK").alias("SUB_SK"),
    F.col("mbr_med_out.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("mbr_med_out.MBR_SK").alias("MBR_SK"),
    F.col("mbr_med_out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("mbr_med_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("mbr_med_out.MO_NO").alias("MO_NO"),
    F.col("mbr_med_out.MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
)

# 4) Output => "hf_p_cae_dpnt" with link name=dependents, Constraint= "mbr_med_out.MBR_RELSHP_CD <> 'SUB' and cd_mppng_tgt.TRGT_CD = 'MED' and vFEP = 'Y' and mbr_med_out.GRP_ID <> '10024000'"
df_buisnessrules_dependents = df_buisnessrules_enriched.filter(
    (F.col("mbr_med_out.MBR_RELSHP_CD") != F.lit("SUB")) &
    (F.col("cd_mppng_tgt.TRGT_CD") == F.lit("MED")) &
    (F.col("vFEP") == F.lit("Y")) &
    (F.col("mbr_med_out.GRP_ID") != F.lit("10024000"))
).select(
    F.col("mbr_med_out.MBR_SK").alias("MBR_SK"),
    F.col("mbr_med_out.EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("mbr_med_out.SUB_SK").alias("SUB_SK"),
    F.col("mbr_med_out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("mbr_med_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("mbr_med_out.MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
)

# Now we write these four dataframes to their respective hashed-file parquet outputs:
# hf_p_cae_mbr_prmcy_out, hf_p_cae_mbr_cov, hf_p_cae_sub_out, hf_p_cae_dpnt

write_files(
    df_buisnessrules_output,
    "hf_p_cae_mbr_prmcy_out.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
write_files(
    df_buisnessrules_mbr_coverage,
    "hf_p_cae_mbr_cov.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
write_files(
    df_buisnessrules_subscribers,
    "hf_p_cae_sub_out.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
write_files(
    df_buisnessrules_dependents,
    "hf_p_cae_dpnt.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Then read them back if needed by subsequent stages
df_hf_p_cae_mbr_prmcy_out_in = spark.read.parquet("hf_p_cae_mbr_prmcy_out.parquet")
df_hf_p_cae_mbr_cov_in = spark.read.parquet("hf_p_cae_mbr_cov.parquet")
df_hf_p_cae_sub_out_in = spark.read.parquet("hf_p_cae_sub_out.parquet")
df_hf_p_cae_dpnt_in = spark.read.parquet("hf_p_cae_dpnt.parquet")

# ---------------------------------------------------------------------------------
# A number of Aggregator stages follow. We'll implement each in turn.

# Group1 => input: "hf_p_cae_mbr_cov" => output: "hf_p_cae_mbr_cov_cnt"
df_group1 = (
    df_hf_p_cae_mbr_cov_in
    .groupBy("INDV_BE_KEY")
    .agg(F.count("MBR_SK").alias("CNT_MBR_SK"))
)
write_files(
    df_group1,
    "hf_p_cae_mbr_cov_cnt.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_mbr_cov_cnt_in = spark.read.parquet("hf_p_cae_mbr_cov_cnt.parquet")

# Group2 => input: "trg_cd" from "hf_p_cae_mbr_cob" => output: "hf_p_cae_mbr_trgt_cd"
df_mbr_cob_trg_cd = df_hf_p_cae_mbr_cob_in.select(
    F.col("INDV_BE_KEY"),
    F.col("TRGT_CD")
).where(F.col("INDV_BE_KEY").isNotNull() & F.col("TRGT_CD").isNotNull())
df_group2 = (
    df_mbr_cob_trg_cd.groupBy("INDV_BE_KEY","TRGT_CD")
    .agg(F.count("TRGT_CD").alias("CNT_TRGT_CD"))
)
write_files(
    df_group2,
    "hf_p_cae_mbr_trgt_cd.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_mbr_trgt_cd_in = spark.read.parquet("hf_p_cae_mbr_trgt_cd.parquet")

# Group3 => input: "hf_p_cae_mbr_cob" => link "cob_cnt" => output "hf_p_cae_cob_cnt"
df_cob_cnt_in = df_hf_p_cae_mbr_cob_in.select("INDV_BE_KEY","MBR_SK")
df_group3 = (
    df_cob_cnt_in.groupBy("INDV_BE_KEY")
    .agg(F.count("MBR_SK").alias("CNT_MBR_SK"))
)
write_files(
    df_group3,
    "hf_p_cae_cob_cnt.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_cob_cnt_in = spark.read.parquet("hf_p_cae_cob_cnt.parquet")

# Group4 => input: "hf_p_cae_sub_out" => we get sub_out => aggregator output => "hf_p_cae_sub_cnt_out" and "hf_p_cae_sub_eff_dte"
df_sub_out_in = df_hf_p_cae_sub_out_in.select("INDV_BE_KEY","MBR_SK","MBR_UNIQ_KEY","EFF_DT_SK")
df_group4_1 = (
    df_sub_out_in.groupBy("INDV_BE_KEY")
    .agg(
        F.count("MBR_SK").alias("CNT_SUB"),
        F.min("MBR_UNIQ_KEY").alias("MIN_UNIQ_KEY"),
        F.min("EFF_DT_SK").alias("MIN_EFF_DTE")
    )
)
write_files(
    df_group4_1,
    "hf_p_cae_sub_cnt_out.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_sub_cnt_out_in = spark.read.parquet("hf_p_cae_sub_cnt_out.parquet")

df_group4_2 = (
    df_sub_out_in.groupBy("INDV_BE_KEY","EFF_DT_SK")
    .agg(F.count("EFF_DT_SK").alias("CNT_EFF_DTE"))
    .withColumnRenamed("EFF_DT_SK","EFF_DTE_GRP")
)
write_files(
    df_group4_2,
    "hf_p_cae_sub_eff_dte.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_sub_eff_dte_in = spark.read.parquet("hf_p_cae_sub_eff_dte.parquet")

# ---------------------------------------------------------------------------------
# COB_Trans => a Transformer with primary link "mbr_cob_out" from hf_p_cae_mbr_cob, plus lookups on
#  cob_cnt_lkup (hf_p_cae_cob_cnt), trgt_cd_cnt (hf_p_cae_mbr_trgt_cd), mbr_cov_cnt_out (hf_p_cae_mbr_cov_cnt).
# In DataStage, there's stage variables: indSingle, indDual, svIncTCode. We replicate logic as columns.

# We have df_mbr_cob_out_in = df_hf_p_cae_mbr_cob_in (the "mbr_cob_out" includes the same columns).
# We'll rename it for clarity:
df_mbr_cob_out = df_hf_p_cae_mbr_cob_in.alias("mbr_cob_out")

df_cob_cnt_lkup = df_hf_p_cae_cob_cnt_in.alias("cob_cnt_lkup")
df_trgt_cd_cnt = df_hf_p_cae_mbr_trgt_cd_in.alias("trgt_cd_cnt")
df_mbr_cov_cnt_out = df_hf_p_cae_mbr_cov_cnt_copy.alias("mbr_cov_cnt_out")  # from hf_p_cae_mbr_cov_cnt_copy stage read

joined_cobtrans = (
    df_mbr_cob_out
    .join(
        df_cob_cnt_lkup,
        (F.col("mbr_cob_out.INDV_BE_KEY") == F.col("cob_cnt_lkup.INDV_BE_KEY")),
        how="left"
    )
    .join(
        df_trgt_cd_cnt,
        [
            F.col("mbr_cob_out.INDV_BE_KEY") == F.col("trgt_cd_cnt.INDV_BE_KEY"),
            F.col("mbr_cob_out.TRGT_CD") == F.col("trgt_cd_cnt.TRGT_CD")
        ],
        how="left"
    )
    .join(
        df_mbr_cov_cnt_out,
        (F.col("mbr_cob_out.INDV_BE_KEY") == F.col("mbr_cov_cnt_out.INDV_BE_KEY")),
        how="left"
    )
)

df_cobtrans = joined_cobtrans.select(
    F.col("mbr_cob_out.INDV_BE_KEY"),
    F.col("mbr_cob_out.MBR_SK"),
    F.col("mbr_cob_out.OTHR_CAR_POL_ID"),
    F.col("mbr_cob_out.TRGT_CD"),
    F.col("cob_cnt_lkup.CNT_MBR_SK").alias("lkup_CNT_MBR_SK"),
    F.col("trgt_cd_cnt.CNT_TRGT_CD").alias("lkup_CNT_TRGT_CD"),
    F.col("trgt_cd_cnt.TRGT_CD").alias("lkup_TRGT_CD"),
    F.col("mbr_cov_cnt_out.CNT_MBR_SK").alias("lkup_cov_cnt_MBR_SK")
)

# StageVariables in COB_Trans:
# indSingle = If IsNull(mbr_cov_cnt_out.INDV_BE_KEY) => 'N' else if mbr_cov_cnt_out.CNT_MBR_SK=1 then if mbr_cob_out.TRGT_CD='SEC' then 'Y' else 'N' else @NULL
# We'll do a column expression in Spark:
df_cobtrans2 = df_cobtrans.withColumn(
    "indSingle",
    F.when(
        F.col("lkup_cov_cnt_MBR_SK").isNull(),
        F.lit("N")
    ).when(
        F.col("lkup_cov_cnt_MBR_SK") == 1,
        F.when(F.col("TRGT_CD") == F.lit("SEC"), F.lit("Y")).otherwise(F.lit("N"))
    ).otherwise(F.lit(None))
)

# indDual = if cob_cnt_lkup.CNT_MBR_SK>=1 then if trgt_cd_cnt.TRGT_CD='PRI' then 'N' else if trgt_cd_cnt.TRGT_CD='SEC' then if trgt_cd_cnt.CNT_TRGT_CD=1 then 'Y' else @NULL else @NULL else @NULL
df_cobtrans3 = df_cobtrans2.withColumn(
    "indDual",
    F.when(
        F.col("lkup_CNT_MBR_SK") >= 1,
        F.when(
            F.col("lkup_TRGT_CD") == F.lit("PRI"),
            F.lit("N")
        ).otherwise(
            F.when(
                F.col("lkup_TRGT_CD") == F.lit("SEC"),
                F.when(F.col("lkup_CNT_TRGT_CD") == 1, F.lit("Y")).otherwise(F.lit(None))
            ).otherwise(F.lit(None))
        )
    ).otherwise(F.lit(None))
)

# svIncTCode = if mbr_cov_cnt_out.CNT_MBR_SK=1 then if cob_cnt_lkup.CNT_MBR_SK>1 then if mbr_cob_out.TRGT_CD='PRI' then 'Y' else 'N' else 'Y' else 'Y'
df_cobtrans4 = df_cobtrans3.withColumn(
    "svIncTCode",
    F.when(
        F.col("lkup_cov_cnt_MBR_SK") == 1,
        F.when(
            F.col("lkup_CNT_MBR_SK") > 1,
            F.when(F.col("TRGT_CD") == F.lit("PRI"), F.lit("Y")).otherwise(F.lit("N"))
        ).otherwise(F.lit("Y"))
    ).otherwise(F.lit("Y"))
)

# The transformer has two output pins:
# 1) "cob_out" => Constraint= svIncTCode='Y'
df_cob_out = df_cobtrans4.filter(F.col("svIncTCode") == F.lit("Y")).select(
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("MBR_SK").alias("MBR_SK"),
    F.col("indSingle").alias("indSingle"),
    F.col("indDual").alias("indDual"),
    F.when(F.col("lkup_TRGT_CD")==F.lit("PRI"), F.col("lkup_CNT_TRGT_CD")).otherwise(F.lit(0)).alias("cntPRI"),
    F.when(F.col("lkup_TRGT_CD")==F.lit("SEC"), F.col("lkup_CNT_TRGT_CD")).otherwise(F.lit(0)).alias("cntSEC")
)

write_files(
    df_cob_out,
    "hf_p_cae_prmcy_indicators.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_prmcy_indicators_in = spark.read.parquet("hf_p_cae_prmcy_indicators.parquet")

# 2) "prmcy_set" => Constraint= "indSingle='Y' or indDual='Y'"
df_prmcy_set = df_cobtrans4.filter(
    (F.col("indSingle") == F.lit("Y")) | (F.col("indDual") == F.lit("Y"))
).select(
    F.col("INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.lit("Y").alias("PRMCY_SET")
)
write_files(
    df_prmcy_set,
    "hf_p_cae_prmcy_set_ind.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_prmcy_set_ind_in = spark.read.parquet("hf_p_cae_prmcy_set_ind.parquet")

# ---------------------------------------------------------------------------------
# Then we have "hf_p_cae_prmcy_indicators" stage => just wrote it => no further aggregator is mentioned, it is used as a lookup in BUSINESS_RULES stage for "cob_lkup".

# ---------------------------------------------------------------------------------
# Dependent aggregator logic (Transformer_453 => references "hf_p_cae_sub_out" as subscribers_out, "hf_p_cae_dpnt" as dpnt_out)
# But the job outlines a join: dpnt_out is primary link, subscribers_out is left lookup on SUB_SK/EFF_DT_SK.
# We'll read them from our parquet:
df_subscribers_out = df_hf_p_cae_sub_out_in.alias("subscribers_out")
df_dpnt_out = df_hf_p_cae_dpnt_in.alias("dpnt_out")

df_transformer_453_join = (
    df_dpnt_out
    .join(
        df_subscribers_out,
        [
            F.col("dpnt_out.SUB_SK") == F.col("subscribers_out.SUB_SK"),
            F.col("dpnt_out.EFF_DT_SK") == F.col("subscribers_out.EFF_DT_SK")
        ],
        how="left"
    )
)

# multiple outputs from Transformer_453:
# 1) "mbr_sub_list" => Constraint= "IsNull(subscribers_out.SUB_SK)=False"
df_mbr_sub_list = df_transformer_453_join.filter(
    F.col("subscribers_out.SUB_SK").isNotNull()
).select(
    F.col("dpnt_out.MBR_SK").alias("MBR_SK"),
    F.col("dpnt_out.EFF_DT_SK").alias("MBR_EFF_DT_SK"),
    F.col("subscribers_out.EFF_DT_SK").alias("SUB_EFF_DT_SK"),
    F.col("subscribers_out.MO_NO").alias("SUB_MO_NO"),
    F.col("dpnt_out.SUB_SK").alias("MBR_SUB_SK"),
    F.col("dpnt_out.INDV_BE_KEY").alias("MBR_INDV_BE_KEY"),
    F.col("dpnt_out.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
    F.col("dpnt_out.MBR_RELSHP_CD").alias("MBR_RELSHP_CD"),
    F.col("subscribers_out.SUB_SK").alias("SUB_SK"),
    F.col("subscribers_out.MBR_SK").alias("SUB_MBR_SK"),
    F.col("subscribers_out.INDV_BE_KEY").alias("SUB_INDV_BE_KEY"),
    F.col("subscribers_out.MBR_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("subscribers_out.MBR_RELSHP_CD").alias("SUB_RELSHP_CD")
)

write_files(
    df_mbr_sub_list,
    "hf_p_cae_mbr_sub_list.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_mbr_sub_list_in = spark.read.parquet("hf_p_cae_mbr_sub_list.parquet")

# 2) "dpndt1" => Constraint= "dpnt_out.MBR_RELSHP_CD = 'DPNDT'"
df_dpndt1 = df_transformer_453_join.filter(
    F.col("dpnt_out.MBR_RELSHP_CD") == F.lit("DPNDT")
).select(
    F.col("dpnt_out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("dpnt_out.MBR_SK").alias("MBR_SK"),
    F.col("dpnt_out.MBR_RELSHP_CD").alias("MBR_RELSHP_CD")
)
write_files(
    df_dpndt1,
    "hf_p_cae_dpndt1.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_dpndt1_in = spark.read.parquet("hf_p_cae_dpndt1.parquet")

# ---------------------------------------------------------------------------------
# Group5 => input: "hf_p_cae_mbr_sub_list" => output: "hf_p_cae_mbr_sub_cnts", "hf_p_cae_sub_brth_mo", "hf_p_cae_dpndt_sub_eff_dte"
df_mbr_sub_list_in_g5 = df_hf_p_cae_mbr_sub_list_in.select(
    "MBR_SK","MBR_EFF_DT_SK","SUB_EFF_DT_SK","SUB_MO_NO","MBR_SUB_SK","MBR_INDV_BE_KEY","MBR_UNIQ_KEY","MBR_RELSHP_CD","SUB_SK","SUB_MBR_SK","SUB_INDV_BE_KEY","SUB_UNIQ_KEY","SUB_RELSHP_CD"
)

df_group5_a = (
    df_mbr_sub_list_in_g5.groupBy("MBR_INDV_BE_KEY")
    .agg(
        F.count("MBR_SK").alias("CNT_MBR_SK"),
        F.min("SUB_MO_NO").alias("MIN_BRTH_MO"),
        F.min("MBR_UNIQ_KEY").alias("MIN_MBR_UNIQ_KEY"),
        F.min("SUB_EFF_DT_SK").alias("MIN_EFF_DTE")
    )
    .withColumnRenamed("MBR_INDV_BE_KEY","INDV_BE_KEY")
)
write_files(
    df_group5_a,
    "hf_p_cae_mbr_sub_cnts.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_mbr_sub_cnts_in = spark.read.parquet("hf_p_cae_mbr_sub_cnts.parquet")

df_group5_b = (
    df_mbr_sub_list_in_g5.groupBy("MBR_INDV_BE_KEY","SUB_MO_NO")
    .agg(F.count("SUB_MO_NO").alias("CNT_BMO"))
    .withColumnRenamed("MBR_INDV_BE_KEY","INDV_BE_KEY")
    .withColumnRenamed("SUB_MO_NO","GRP_BMO")
)
write_files(
    df_group5_b,
    "hf_p_cae_sub_brth_mo.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_sub_brth_mo_in = spark.read.parquet("hf_p_cae_sub_brth_mo.parquet")

df_group5_c = (
    df_mbr_sub_list_in_g5.groupBy("MBR_INDV_BE_KEY","SUB_EFF_DT_SK")
    .agg(F.count("SUB_EFF_DT_SK").alias("CNT_EFF_DTE"))
    .withColumnRenamed("MBR_INDV_BE_KEY","INDV_BE_KEY")
    .withColumnRenamed("SUB_EFF_DT_SK","EFF_DTE_GRP")
)
write_files(
    df_group5_c,
    "hf_p_cae_dpndt_sub_eff_dte.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_dpndt_sub_eff_dte_in = spark.read.parquet("hf_p_cae_dpndt_sub_eff_dte.parquet")

# ---------------------------------------------------------------------------------
# Aggregator_575 => input: "hf_p_cae_dpndt1" => output: "hf_p_cae_dpndt3"
df_dpndt1_in = df_hf_p_cae_dpndt1_in.select("INDV_BE_KEY","MBR_SK","MBR_RELSHP_CD")
df_agg_575 = (
    df_dpndt1_in.groupBy("INDV_BE_KEY")
    .agg(F.count("MBR_RELSHP_CD").alias("CNT_DPNDT"))
)
write_files(
    df_agg_575,
    "hf_p_cae_dpndt3.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_dpndt3_in = spark.read.parquet("hf_p_cae_dpndt3.parquet")

# ---------------------------------------------------------------------------------
# Transformer_485 => primary link "mbr_subs" (from hf_p_cae_mbr_sub_list),
# lookup link "dpndt_cnts" => hf_p_cae_mbr_sub_cnts,
# lookup link "sub_bmnth_cnt" => hf_p_cae_sub_brth_mo,
# lookup link "dpndt_sub_eff_dte_cnt" => hf_p_cae_dpndt_sub_eff_dte,
# lookup link "relationship" => hf_p_cae_dpndt3
df_mbr_subs = df_hf_p_cae_mbr_sub_list_in.alias("mbr_subs")
df_dpndt_cnts = df_hf_p_cae_mbr_sub_cnts_in.alias("dpndt_cnts")
df_sub_bmnth_cnt = df_hf_p_cae_sub_brth_mo_in.alias("sub_bmnth_cnt")
df_dpndt_sub_eff_dte_cnt = df_hf_p_cae_dpndt_sub_eff_dte_in.alias("dpndt_sub_eff_dte_cnt")
df_relationship = df_hf_p_cae_dpndt3_in.alias("relationship")

df_tr485_joined = (
    df_mbr_subs
    .join(
        df_dpndt_cnts,
        F.col("mbr_subs.MBR_INDV_BE_KEY") == F.col("dpndt_cnts.INDV_BE_KEY"),
        how="left"
    )
    .join(
        df_sub_bmnth_cnt,
        [
            F.col("mbr_subs.MBR_INDV_BE_KEY") == F.col("sub_bmnth_cnt.INDV_BE_KEY"),
            F.col("mbr_subs.SUB_MO_NO") == F.col("sub_bmnth_cnt.GRP_BMO")
        ],
        how="left"
    )
    .join(
        df_dpndt_sub_eff_dte_cnt,
        F.col("mbr_subs.MBR_INDV_BE_KEY") == F.col("dpndt_sub_eff_dte_cnt.INDV_BE_KEY"),
        how="left"
    )
    .join(
        df_relationship,
        F.col("mbr_subs.MBR_INDV_BE_KEY") == F.col("relationship.INDV_BE_KEY"),
        how="left"
    )
)

# StageVariables:
# svBDay = if sub_bmnth_cnt.CNT_BMO=1 then if mbr_subs.SUB_MO_NO=dpndt_cnts.MIN_BRTH_MO then 'Y' else 'N' else NULL
df_tr485_sv1 = df_tr485_joined.withColumn(
    "svBDay",
    F.when(
        F.col("sub_bmnth_cnt.CNT_BMO") == 1,
        F.when(
            F.col("mbr_subs.SUB_MO_NO") == F.col("dpndt_cnts.MIN_BRTH_MO"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    ).otherwise(F.lit(None))
)

# svEffDte = if dpndt_sub_eff_dte_cnt.CNT_EFF_DTE=1 then if mbr_subs.SUB_EFF_DT_SK=dpndt_cnts.MIN_EFF_DTE then 'Y' else 'N' else NULL
df_tr485_sv2 = df_tr485_sv1.withColumn(
    "svEffDte",
    F.when(
        F.col("dpndt_sub_eff_dte_cnt.CNT_EFF_DTE") == 1,
        F.when(
            F.col("mbr_subs.SUB_EFF_DT_SK") == F.col("dpndt_cnts.MIN_EFF_DTE"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    ).otherwise(F.lit(None))
)

# svUniqKey = if mbr_subs.MBR_UNIQ_KEY=dpndt_cnts.MIN_MBR_UNIQ_KEY then 'Y' else 'N'
df_tr485_sv3 = df_tr485_sv2.withColumn(
    "svUniqKey",
    F.when(
        F.col("mbr_subs.MBR_UNIQ_KEY") == F.col("dpndt_cnts.MIN_MBR_UNIQ_KEY"),
        F.lit("Y")
    ).otherwise(F.lit("N"))
)

# svReltn = if IsNull(relationship.CNT_DPNDT)=TRUE or relationship.CNT_DPNDT=1 then svEffDte else svBDay
df_tr485_sv4 = df_tr485_sv3.withColumn(
    "svReltn",
    F.when(
        (F.col("relationship.CNT_DPNDT").isNull()) | (F.col("relationship.CNT_DPNDT") == 1),
        F.col("svEffDte")
    ).otherwise(F.col("svBDay"))
)

# svPrimacy = if IsNull(svReltn)=FALSE then svReltn else svUniqKey
df_tr485_sv5 = df_tr485_sv4.withColumn(
    "svPrimacy",
    F.when(
        F.col("svReltn").isNotNull(),
        F.col("svReltn")
    ).otherwise(F.col("svUniqKey"))
)

df_transformer_485_out = df_tr485_sv5.select(
    F.col("mbr_subs.MBR_SK").alias("MBR_SK"),
    F.col("svPrimacy").alias("PRMCY_IND")
)

write_files(
    df_transformer_485_out,
    "hf_p_cae_dependent_ind.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_dependent_ind_in = spark.read.parquet("hf_p_cae_dependent_ind.parquet")

# ---------------------------------------------------------------------------------
# Final BUSINESS_RULES stage (StageName="BUSINESS_RULES") has primary link = "Output2" from hf_p_cae_mbr_prmcy_out,
# and multiple left lookups: mbr_cov_cnt_out => hf_p_cae_mbr_cov_cnt_copy, cob_bob_out => hf_p_cae_cob_othr, cob_lkup => hf_p_cae_prmcy_indicators, non_med_grp_out => hf_p_cae_non_medicaid_grp, prmcy_set_out => hf_p_cae_prmcy_set_ind, subscriber_cnts => hf_p_cae_sub_cnt_out, sub_eff_dte_cnt => hf_p_cae_sub_eff_dte, dpndt_ind => hf_p_cae_dependent_ind
df_output2 = df_hf_p_cae_mbr_prmcy_out_in.alias("Output2")
df_mbr_cov_cnt_copy = df_hf_p_cae_mbr_cov_cnt_copy.alias("mbr_cov_cnt_out")
df_cob_bob_out = df_hf_p_cae_cob_othr_in.alias("cob_bob_out")
df_cob_lkup = df_hf_p_cae_prmcy_indicators_in.alias("cob_lkup")
df_non_med_grp_out = df_hf_p_cae_non_medicaid_grp_in.alias("non_med_grp_out")
df_prmcy_set_out = df_hf_p_cae_prmcy_set_ind_in.alias("prmcy_set_out")
df_subscriber_cnts = df_hf_p_cae_sub_cnt_out_in.alias("subscriber_cnts")
df_sub_eff_dte_cnt = df_hf_p_cae_sub_eff_dte_in.alias("sub_eff_dte_cnt")
df_dpndt_ind = df_hf_p_cae_dependent_ind_in.alias("dpndt_ind")

df_br_join = (
    df_output2
    .join(
        df_mbr_cov_cnt_copy,
        [F.col("Output2.INDV_BE_KEY") == F.col("mbr_cov_cnt_out.INDV_BE_KEY")],
        how="left"
    )
    .join(
        df_cob_bob_out,
        [
            F.col("Output2.INDV_BE_KEY") == F.col("cob_bob_out.INDV_BE_KEY"),
            F.col("Output2.MBR_SK") == F.col("cob_bob_out.MBR_SK"),
            F.col("Output2.SUB_ID") == F.col("cob_bob_out.OTHR_CAR_POL_ID")
        ],
        how="left"
    )
    .join(
        df_cob_lkup,
        [
            F.col("Output2.INDV_BE_KEY") == F.col("cob_lkup.INDV_BE_KEY"),
            F.col("Output2.MBR_SK") == F.col("cob_lkup.MBR_SK")
        ],
        how="left"
    )
    .join(
        df_non_med_grp_out,
        [
            F.col("Output2.INDV_BE_KEY") == F.col("non_med_grp_out.INDV_BE_KEY"),
            F.col("Output2.MBR_SK") == F.col("non_med_grp_out.CNT_MBR_SK")
        ],
        how="left"
    )
    .join(
        df_prmcy_set_out,
        [F.col("Output2.INDV_BE_KEY") == F.col("prmcy_set_out.INDV_BE_KEY")],
        how="left"
    )
    .join(
        df_subscriber_cnts,
        [
            F.col("Output2.INDV_BE_KEY") == F.col("subscriber_cnts.INDV_BE_KEY"),
            F.col("Output2.MO_NO") == F.col("subscriber_cnts.CNT_SUB")
        ],
        how="left"
    )
    .join(
        df_sub_eff_dte_cnt,
        [F.col("Output2.INDV_BE_KEY") == F.col("sub_eff_dte_cnt.INDV_BE_KEY")],
        how="left"
    )
    .join(
        df_dpndt_ind,
        [F.col("Output2.MBR_SK") == F.col("dpndt_ind.MBR_SK")],
        how="left"
    )
)

# Stage Variables in BUSINESS_RULES:
# indSingle = If mbr_cov_cnt_out.CNT_MBR_SK=1 then if IsNull(cob_lkup.indSingle) => 'Y' else cob_lkup.indSingle else NULL
df_br_sv1 = df_br_join.withColumn(
    "indSingle",
    F.when(
        F.col("mbr_cov_cnt_out.CNT_MBR_SK") == 1,
        F.when(
            F.col("cob_lkup.indSingle").isNull(),
            F.lit("Y")
        ).otherwise(F.col("cob_lkup.indSingle"))
    ).otherwise(F.lit(None))
)

# indBlueOnBlue = if IsNull(cob_bob_out.MBR_SK)=false => if cob_bob_out.TRGT_CD='PRI' then 'Y' else 'N' else NULL
df_br_sv2 = df_br_sv1.withColumn(
    "indBlueOnBlue",
    F.when(
        F.col("cob_bob_out.MBR_SK").isNotNull(),
        F.when(F.col("cob_bob_out.TRGT_CD")==F.lit("PRI"), F.lit("Y")).otherwise(F.lit("N"))
    ).otherwise(F.lit(None))
)

# indMed = If IsNull(non_med_grp_out.INDV_BE_KEY)=false => if Output2.GRP_ID='10024000' then 'N' else if non_med_grp_out.CNT_MBR_SK=1 then 'Y' else NULL else NULL
df_br_sv3 = df_br_sv2.withColumn(
    "indMed",
    F.when(
        F.col("non_med_grp_out.INDV_BE_KEY").isNotNull(),
        F.when(
            F.col("Output2.GRP_ID")==F.lit("10024000"),
            F.lit("N")
        ).otherwise(
            F.when(F.col("non_med_grp_out.CNT_MBR_SK")==1, F.lit("Y")).otherwise(F.lit(None))
        )
    ).otherwise(F.lit(None))
)

# indSub = complicated expression in JSON
df_br_sv4 = df_br_sv3.withColumn(
    "indSub",
    F.when(
        F.col("subscriber_cnts.INDV_BE_KEY").isNull(),
        F.lit(None)
    ).otherwise(
        F.when(
            F.col("subscriber_cnts.CNT_SUB") == 1,
            F.when(
                F.col("Output2.MBR_RELSHP_CD") == F.lit("SUB"),
                F.lit("Y")
            ).otherwise(F.lit("N"))
        ).otherwise(
            F.when(
                F.col("Output2.MBR_RELSHP_CD") == F.lit("SUB"),
                F.when(
                    F.col("sub_eff_dte_cnt.CNT_EFF_DTE") == 1,
                    F.when(
                        F.col("Output2.EFF_DT_SK") == F.col("subscriber_cnts.MIN_EFF_DTE"),
                        F.lit("Y")
                    ).otherwise(F.lit("N"))
                ).otherwise(
                    F.when(
                        F.col("Output2.MBR_UNIQ_KEY") == F.col("subscriber_cnts.MIN_UNIQ_KEY"),
                        F.lit("Y")
                    ).otherwise(F.lit("N"))
                )
            ).otherwise(F.lit("N"))
        )
    )
)

# indDep = dpndt_ind.PRMCY_IND
df_br_sv5 = df_br_sv4.withColumn("indDep", F.col("dpndt_ind.PRMCY_IND"))

# indPriIn = if IsNull(Output2.PRI_IN)=false then Output2.PRI_IN else if IsNull(indSingle)=false then indSingle else if IsNull(indBlueOnBlue)=false then indBlueOnBlue else if IsNull(cob_lkup.indDual)=false then cob_lkup.indDual else if IsNull(prmcy_set_out.INDV_BE_KEY)=false then 'N' else if IsNull(indMed)=false then indMed else if IsNull(indSub)=false then indSub else if IsNull(indDep)=false then indDep else NULL
df_br_sv6 = df_br_sv5.withColumn(
    "indPriIn",
    F.when(
        F.col("Output2.PRI_IN").isNotNull(),
        F.col("Output2.PRI_IN")
    ).otherwise(
        F.when(
            F.col("indSingle").isNotNull(),
            F.col("indSingle")
        ).otherwise(
            F.when(
                F.col("indBlueOnBlue").isNotNull(),
                F.col("indBlueOnBlue")
            ).otherwise(
                F.when(
                    F.col("cob_lkup.indDual").isNotNull(),
                    F.col("cob_lkup.indDual")
                ).otherwise(
                    F.when(
                        F.col("prmcy_set_out.INDV_BE_KEY").isNotNull(),
                        F.lit("N")
                    ).otherwise(
                        F.when(
                            F.col("indMed").isNotNull(),
                            F.col("indMed")
                        ).otherwise(
                            F.when(
                                F.col("indSub").isNotNull(),
                                F.col("indSub")
                            ).otherwise(
                                F.when(
                                    F.col("indDep").isNotNull(),
                                    F.col("indDep")
                                ).otherwise(F.lit(None))
                            )
                        )
                    )
                )
            )
        )
    )
)

df_BusinessRules_final = df_br_sv6.select(
    F.col("Output2.MBR_SK").alias("MBR_SK"),
    F.col("Output2.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("indPriIn").alias("PRI_IN"),
    F.col("Output2.CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    F.col("Output2.GRP_SK").alias("GRP_SK"),
    F.col("Output2.BRTH_DT_SK").alias("MBR_BRTH_DT"),
    F.col("Output2.MBR_GNDR_CD_SK").alias("MBR_GNDR_CD"),
    F.col("Output2.LAST_UPDATED").alias("LAST_UPDT_DT")
)

write_files(
    df_BusinessRules_final,
    "hf_p_cae_prmcy_main.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_prmcy_main_in = spark.read.parquet("hf_p_cae_prmcy_main.parquet")

# ---------------------------------------------------------------------------------
# Transformer_301 => input is "out" from hf_p_cae_prmcy_main => as primary link
df_out_301 = df_hf_p_cae_prmcy_main_in.alias("out")

# single output link "pri_in_y" => constraint: out.PRI_IN='Y'
df_pri_in_y = df_out_301.filter(F.col("out.PRI_IN") == F.lit("Y")).select(
    F.col("out.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.col("out.MBR_SK").alias("MBR_SK")
)

write_files(
    df_pri_in_y,
    "hf_p_cae_prmcy_yes.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_prmcy_yes_in = spark.read.parquet("hf_p_cae_prmcy_yes.parquet")

# ---------------------------------------------------------------------------------
# Aggregator_385 => input= pri_in_y => output => hf_p_cae_prmcy_yes (same file but we already wrote it, let's replicate aggregator?)
df_agg_385 = (
    df_pri_in_y.groupBy("INDV_BE_KEY")
    .agg(F.first("MBR_SK").alias("MBR_SK"))
)
# We overwrite hf_p_cae_prmcy_yes again:
write_files(
    df_agg_385,
    "hf_p_cae_prmcy_yes.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_prmcy_yes_in2 = spark.read.parquet("hf_p_cae_prmcy_yes.parquet")

# ---------------------------------------------------------------------------------
# Transformer_186 => primary link "main_prmcy" => hf_p_cae_prmcy_main, left lookup => "prmcy_cnt_out" => hf_p_cae_prmcy_yes
df_main_prmcy = df_hf_p_cae_prmcy_main_in.alias("main_prmcy")
df_prmcy_cnt_out = df_hf_p_cae_prmcy_yes_in2.alias("prmcy_cnt_out")

df_tr186_join = (
    df_main_prmcy
    .join(
        df_prmcy_cnt_out,
        [
            F.col("main_prmcy.INDV_BE_KEY") == F.col("prmcy_cnt_out.INDV_BE_KEY"),
            F.col("main_prmcy.MBR_SK") == F.col("prmcy_cnt_out.MBR_SK")
        ],
        how="left"
    )
)

df_transformer_186_final = df_tr186_join.select(
    F.col("main_prmcy.MBR_SK").alias("MBR_SK"),
    F.col("main_prmcy.CAE_UNIQ_KEY").alias("CAE_UNIQ_KEY"),
    F.col("main_prmcy.INDV_BE_KEY").alias("INDV_BE_KEY"),
    F.when(
        F.col("prmcy_cnt_out.INDV_BE_KEY").isNull(),
        F.lit("N")
    ).otherwise(
        F.when(
            F.col("main_prmcy.MBR_SK") == F.col("prmcy_cnt_out.MBR_SK"),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    ).alias("PRI_IN"),
    F.col("main_prmcy.GRP_SK").alias("GRP_SK"),
    F.col("main_prmcy.MBR_BRTH_DT").alias("MBR_BRTH_DT"),
    F.col("main_prmcy.MBR_GNDR_CD").alias("MBR_GNDR_CD"),
    F.col("main_prmcy.LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

write_files(
    df_transformer_186_final,
    "hf_p_cae_prmcy_out.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)
df_hf_p_cae_prmcy_out_final_in = spark.read.parquet("hf_p_cae_prmcy_out.parquet")

# ---------------------------------------------------------------------------------
# Final stage => P_CAE_MBR_PRMCY_dat => CSeqFileStage => writing to load/P_CAE_MBR_PRMCY.dat
# columns: MBR_SK(int), CAE_UNIQ_KEY(int), INDV_BE_KEY(decimal), PRI_IN(char2), GRP_SK(int),
#          MBR_BRTH_DT(char10), MBR_GNDR_CD(varchar?), LAST_UPDT_DT(char10)
# We preserve order and apply rpad for char/varchar columns. If length not known for "MBR_GNDR_CD", choose some default (e.g. 10).
df_final_out = df_hf_p_cae_prmcy_out_final_in.select(
    "MBR_SK",
    "CAE_UNIQ_KEY",
    "INDV_BE_KEY",
    F.rpad(F.col("PRI_IN"), 2, " ").alias("PRI_IN"),
    "GRP_SK",
    F.rpad(F.col("MBR_BRTH_DT"), 10, " ").alias("MBR_BRTH_DT"),
    F.rpad(F.col("MBR_GNDR_CD"), 10, " ").alias("MBR_GNDR_CD"),
    F.rpad(F.col("LAST_UPDT_DT"), 10, " ").alias("LAST_UPDT_DT")
)

write_files(
    df_final_out,
    f"{adls_path}/load/P_CAE_MBR_PRMCY.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)