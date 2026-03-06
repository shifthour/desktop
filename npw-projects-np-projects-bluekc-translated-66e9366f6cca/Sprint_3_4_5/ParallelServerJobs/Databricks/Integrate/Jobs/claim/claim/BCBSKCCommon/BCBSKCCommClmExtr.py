# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2017 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  BCBSKCCommDrugExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:  Reads the PDX_CLM_STD_INPT_Land.dat file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                      Date                Project/Altiris #                  Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ----------------------------------    -------------------     -------------------------------------     ------------------------------------------------------------------------------------------------    --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kaushik Kapoor             2017-12-01      5828                                  Initial Programming                                                                       IntegrateDev2                Kalyan Neelam          2018-02-26 
# MAGIC Mohan Karnati               2019-06-06      ADO-73034                       Adding CLM_TXNMY_CD filed in alpha_pfx stage and                IntegrateDev1	   Kalyan Neelam          2019-07-01
# MAGIC                                                                                                         passing it till BCBSKCCommClmExtr stage
# MAGIC Srinivas Garimella          2019-10-16      6131 PBM replacement     changed business rules for fields PD_DT,ALW_AMT,                 IntegrateDev1     
# MAGIC                                                                                                         COINS_AMT,CNSD_CHRG_AMT,COPAY_AMT,CHRG_AMT,
# MAGIC Srinivas Garimella          2019-10-26      6131 PBM replacement     changed business rules for fields                                                  IntegrateDev2
# MAGIC                                                                                                         CLM_INPT_SRC_CD_SK. CLM_COB_CD
# MAGIC Srinivas Garimella          2019-10-26      6131 PBM replacement     changed business rules for field                                                   IntegrateDev2                Kalyan Neelam          2019-11-20
# MAGIC                                                                                                         ADJ_FROM_CLM_ID
# MAGIC Rekha Radhakrishna    2020-02-27      6131                                  Changed ALW_AMT calculation for OPTUMRX                         IntegrateDev2                 Hugh Sisson             2020-03-06
# MAGIC 
# MAGIC Ramu                            2020-03-13      6131 PBM replacement      Mapped 63rd char value of Client General Purpose Area  to      IntegrateDev2                 Kalyan Neelam         2020-04-09
# MAGIC                                                                                                             coulmn to BILL_PAYMT_EXCL_IN
# MAGIC 
# MAGIC Goutham Kalidindi        10/22/2020       US-283560          Added MEDIMPACT derivation for   ALLWD_AMT                                   IntegrateDev2               Kalyan Neelam         2020-11-10
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi        11/11/2020       US-283560                           Changed MEDIMPACT derivation for   ALLWD_AMT                 IntegrateDev2            Kalyan Neelam      2020-11-12    
# MAGIC                                                                                                            Added MEDIMPACT derivation for CNSD_CHRG_AMT
# MAGIC                                                                                                            CHRG_AMT  & CLM_COB_CD
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC                                                                                                                 
# MAGIC Sri Nannapaneni           2020-07-15      6131 PBM replacement      Added 7 new fields to source seq file                                          IntegrateDev2               Sravya Gorla            2020-12-09             
# MAGIC Sri Nannapaneni           08/13/2020    6131- PBM Replacement    Added LOB_IN field to source seq file                                          IntegrateDev2                Sravya Gorla            2020-12-09  
# MAGIC 
# MAGIC Goutham Kalidindi        01-20-2021       US-341803                      Column derivation change on PAYMT_REF_ID                             IntegrateDev2            Kalyan Neelam          2021-01-21
# MAGIC                                                                                                       for MedImpact claims ONLY

# MAGIC Read the Common Input file with 157 fields created from BCBSComm
# MAGIC Writing Sequential File to /key
# MAGIC This container is used in:
# MAGIC ESIClmExtr
# MAGIC FctsClmExtr
# MAGIC MCSourceClmExtr
# MAGIC MedicaidClmExtr
# MAGIC NascoClmExtr
# MAGIC PcsClmExtr
# MAGIC PCTAClmExtr
# MAGIC WellDyneClmExtr
# MAGIC MedtrakClmExtr
# MAGIC BCBSKCCommClmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Lookup subscriber, product and member information
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, Row
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

# Get widget/parameter values
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
RunCycle = get_widget_value('RunCycle','1')
RunID = get_widget_value('RunID','100')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# 1) Stage: ids (DB2Connector) - read three queries from IDS database
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

extract_query_mbr = f"""
SELECT 
  MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
  SUB.SUB_UNIQ_KEY,
  MBR.MBR_SFX_NO,
  SUB.SUB_ID
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.W_DRUG_ENR DRUG
WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK
  AND MBR.MBR_SK NOT IN (0,1)
"""

df_mbr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr)
    .load()
)

extract_query_sub_alpha_pfx = f"""
SELECT 
  SUB_UNIQ_KEY,
  ALPHA_PFX_CD
FROM 
  {IDSOwner}.MBR MBR,
  {IDSOwner}.SUB SUB,
  {IDSOwner}.ALPHA_PFX PFX,
  {IDSOwner}.W_DRUG_ENR DRUG
WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK 
  AND SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK
"""

df_sub_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_sub_alpha_pfx)
    .load()
)

extract_query_mbr_enroll = f"""
SELECT 
  DRUG.CLM_ID,
  CLS.CLS_ID,
  PLN.CLS_PLN_ID,
  SUBGRP.SUBGRP_ID,
  CAT.EXPRNC_CAT_CD,
  LOB.FNCL_LOB_CD,
  CMPNT.PROD_ID
FROM {IDSOwner}.W_DRUG_ENR DRUG,
     {IDSOwner}.MBR_ENR                  MBR, 
     {IDSOwner}.CD_MPPNG                MAP1,
     {IDSOwner}.CLS                     CLS, 
     {IDSOwner}.SUBGRP                  SUBGRP,
     {IDSOwner}.CLS_PLN                 PLN, 
     {IDSOwner}.CD_MPPNG                MAP2, 
     {IDSOwner}.PROD_CMPNT              CMPNT,
     {IDSOwner}.PROD_BILL_CMPNT         BILL_CMPNT,
     {IDSOwner}.EXPRNC_CAT              CAT,
     {IDSOwner}.FNCL_LOB                LOB
WHERE 
  DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND DRUG.FILL_DT_SK BETWEEN MBR.EFF_DT_SK AND MBR.TERM_DT_SK
  AND MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD = 'MED'
  AND MBR.CLS_SK = CLS.CLS_SK
  AND MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK
  AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD= 'PDBL'
  AND DRUG.FILL_DT_SK BETWEEN CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
  AND CMPNT.PROD_CMPNT_EFF_DT_SK= (
       SELECT MAX ( CMPNT2.PROD_CMPNT_EFF_DT_SK )
       FROM {IDSOwner}.PROD_CMPNT CMPNT2
       WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
         AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
         AND DRUG.FILL_DT_SK BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK
  )
  AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
  AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
  AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
  AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (
       SELECT MAX ( BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK )
       FROM {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT2
       WHERE BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
         AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
         AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
         AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
         AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK
  )
  AND BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
  AND BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
"""

df_mbr_enroll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_mbr_enroll)
    .load()
)

# 2) Stage: Hash (CHashedFileStage) - Scenario A (Intermediate hashed file)
#    We apply any column expressions, then deduplicate by key columns, then forward to next stage.

# mbr_lkup output (PrimaryKey=Mbr_Uniq_Key). Expression for MBR_UNIQ_KEY is "Argus.MEM_CK_KEY".
# We assume Argus.MEM_CK_KEY() is a user-defined function requiring no explicit argument as per DataStage snippet.
df_mbr_stage = df_mbr.withColumn("MBR_UNIQ_KEY", Argus.MEM_CK_KEY())

# Deduplicate by MBR_UNIQ_KEY
df_mbr_lkup = dedup_sort(df_mbr_stage, ["MBR_UNIQ_KEY"], [("MBR_UNIQ_KEY","A")])

# sub_alpha_pfx_lkup output (PrimaryKey=SUB_UNIQ_KEY).
df_sub_alpha_pfx_lkup = dedup_sort(df_sub_alpha_pfx, ["SUB_UNIQ_KEY"], [("SUB_UNIQ_KEY","A")])

# mbr_enr_lkup output (PrimaryKey=CLM_ID).
df_mbr_enr_lkup = dedup_sort(df_mbr_enroll, ["CLM_ID"], [("CLM_ID","A")])

# 3) Stage: BCBSCommClmLand (CSeqFileStage) - read a delimited file with known schema.
#    The file path is #$FilePath#/verified/PDX_CLM_STD_INPT_Land_#SrcSysCd#.dat.#RunID#, which does not contain "landing" or "external", so use adls_path.
schema_bcbscommclmland = T.StructType([
    T.StructField("SRC_SYS_CD", T.StringType(), True),
    T.StructField("FILE_RCVD_DT", T.StringType(), True),
    T.StructField("RCRD_ID", T.DoubleType(), True),
    T.StructField("PRCSR_NO", T.StringType(), True),
    T.StructField("BTCH_NO", T.DoubleType(), True),
    T.StructField("PDX_NO", T.DoubleType(), True),
    T.StructField("RX_NO", T.DoubleType(), True),
    T.StructField("FILL_DT", T.StringType(), True),
    T.StructField("NDC", T.DoubleType(), True),
    T.StructField("DRUG_DESC", T.StringType(), True),
    T.StructField("NEW_OR_RFL_CD", T.IntegerType(), True),
    T.StructField("METRIC_QTY", T.DoubleType(), True),
    T.StructField("DAYS_SUPL", T.DoubleType(), True),
    T.StructField("BSS_OF_CST_DTRM", T.StringType(), True),
    T.StructField("INGR_CST_AMT", T.DoubleType(), True),
    T.StructField("DISPNS_FEE_AMT", T.DoubleType(), True),
    T.StructField("COPAY_AMT", T.DoubleType(), True),
    T.StructField("SLS_TAX_AMT", T.DoubleType(), True),
    T.StructField("BILL_AMT", T.DoubleType(), True),
    T.StructField("PATN_FIRST_NM", T.StringType(), True),
    T.StructField("PATN_LAST_NM", T.StringType(), True),
    T.StructField("BRTH_DT", T.StringType(), True),
    T.StructField("SEX_CD", T.DoubleType(), True),
    T.StructField("CARDHLDR_ID_NO", T.StringType(), True),
    T.StructField("RELSHP_CD", T.DoubleType(), True),
    T.StructField("GRP_NO", T.StringType(), True),
    T.StructField("HOME_PLN", T.StringType(), True),
    T.StructField("HOST_PLN", T.StringType(), True),
    T.StructField("PRSCRBR_ID", T.StringType(), True),
    T.StructField("DIAG_CD", T.StringType(), True),
    T.StructField("CARDHLDR_FIRST_NM", T.StringType(), True),
    T.StructField("CARDHLDR_LAST_NM", T.StringType(), True),
    T.StructField("PRAUTH_NO", T.StringType(), True),
    T.StructField("PA_MC_SC_NO", T.StringType(), True),
    T.StructField("CUST_LOC", T.DoubleType(), True),
    T.StructField("RESUB_CYC_CT", T.DoubleType(), True),
    T.StructField("RX_DT", T.StringType(), True),
    T.StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", T.StringType(), True),
    T.StructField("PRSN_CD", T.StringType(), True),
    T.StructField("OTHR_COV_CD", T.DoubleType(), True),
    T.StructField("ELIG_CLRFCTN_CD", T.DoubleType(), True),
    T.StructField("CMPND_CD", T.DoubleType(), True),
    T.StructField("NO_OF_RFLS_AUTH", T.DoubleType(), True),
    T.StructField("LVL_OF_SVC", T.DoubleType(), True),
    T.StructField("RX_ORIG_CD", T.DoubleType(), True),
    T.StructField("RX_DENIAL_CLRFCTN", T.DoubleType(), True),
    T.StructField("PRI_PRSCRBR", T.StringType(), True),
    T.StructField("CLNC_ID_NO", T.DoubleType(), True),
    T.StructField("DRUG_TYP", T.DoubleType(), True),
    T.StructField("PRSCRBR_LAST_NM", T.StringType(), True),
    T.StructField("POSTAGE_AMT", T.DoubleType(), True),
    T.StructField("UNIT_DOSE_IN", T.DoubleType(), True),
    T.StructField("OTHR_PAYOR_AMT", T.DoubleType(), True),
    T.StructField("BSS_OF_DAYS_SUPL_DTRM", T.DoubleType(), True),
    T.StructField("FULL_AVG_WHLSL_PRICE", T.DoubleType(), True),
    T.StructField("EXPNSN_AREA", T.StringType(), True),
    T.StructField("MSTR_CAR", T.StringType(), True),
    T.StructField("SUBCAR", T.StringType(), True),
    T.StructField("CLM_TYP", T.StringType(), True),
    T.StructField("SUBGRP", T.StringType(), True),
    T.StructField("PLN_DSGNR", T.StringType(), True),
    T.StructField("ADJDCT_DT", T.StringType(), True),
    T.StructField("ADMIN_FEE_AMT", T.DoubleType(), True),
    T.StructField("CAP_AMT", T.DoubleType(), True),
    T.StructField("INGR_CST_SUB_AMT", T.DoubleType(), True),
    T.StructField("MBR_NON_COPAY_AMT", T.DoubleType(), True),
    T.StructField("MBR_PAY_CD", T.StringType(), True),
    T.StructField("INCNTV_FEE_AMT", T.DoubleType(), True),
    T.StructField("CLM_ADJ_AMT", T.DoubleType(), True),
    T.StructField("CLM_ADJ_CD", T.StringType(), True),
    T.StructField("FRMLRY_FLAG", T.StringType(), True),
    T.StructField("GNRC_CLS_NO", T.StringType(), True),
    T.StructField("THRPTC_CLS_AHFS", T.StringType(), True),
    T.StructField("PDX_TYP", T.StringType(), True),
    T.StructField("BILL_BSS_CD", T.StringType(), True),
    T.StructField("USL_AND_CUST_CHRG_AMT", T.DoubleType(), True),
    T.StructField("PD_DT", T.StringType(), True),
    T.StructField("BNF_CD", T.StringType(), True),
    T.StructField("DRUG_STRG", T.StringType(), True),
    T.StructField("ORIG_MBR", T.StringType(), True),
    T.StructField("INJRY_DT", T.StringType(), True),
    T.StructField("FEE_AMT", T.DoubleType(), True),
    T.StructField("REF_NO", T.StringType(), True),
    T.StructField("CLNT_CUST_ID", T.StringType(), True),
    T.StructField("PLN_TYP", T.StringType(), True),
    T.StructField("ADJDCT_REF_NO", T.DoubleType(), True),
    T.StructField("ANCLRY_AMT", T.DoubleType(), True),
    T.StructField("CLNT_GNRL_PRPS_AREA", T.StringType(), True),
    T.StructField("PRTL_FILL_STTUS_CD", T.StringType(), True),
    T.StructField("BILL_DT", T.StringType(), True),
    T.StructField("FSA_VNDR_CD", T.StringType(), True),
    T.StructField("PICA_DRUG_CD", T.StringType(), True),
    T.StructField("CLM_AMT", T.DoubleType(), True),
    T.StructField("DSALW_AMT", T.DoubleType(), True),
    T.StructField("FED_DRUG_CLS_CD", T.StringType(), True),
    T.StructField("DEDCT_AMT", T.DoubleType(), True),
    T.StructField("BNF_COPAY_100", T.StringType(), True),
    T.StructField("CLM_PRCS_TYP", T.StringType(), True),
    T.StructField("INDEM_HIER_TIER_NO", T.DoubleType(), True),
    T.StructField("MCARE_D_COV_DRUG", T.StringType(), True),
    T.StructField("RETRO_LICS_CD", T.StringType(), True),
    T.StructField("RETRO_LICS_AMT", T.DoubleType(), True),
    T.StructField("LICS_SBSDY_AMT", T.DoubleType(), True),
    T.StructField("MCARE_B_DRUG", T.StringType(), True),
    T.StructField("MCARE_B_CLM", T.StringType(), True),
    T.StructField("PRSCRBR_QLFR", T.StringType(), True),
    T.StructField("PRSCRBR_NTNL_PROV_ID", T.StringType(), True),
    T.StructField("PDX_QLFR", T.StringType(), True),
    T.StructField("PDX_NTNL_PROV_ID", T.StringType(), True),
    T.StructField("HLTH_RMBRMT_ARGMT_APLD_AMT", T.DoubleType(), True),
    T.StructField("THER_CLS", T.DoubleType(), True),
    T.StructField("HIC_NO", T.StringType(), True),
    T.StructField("HLTH_RMBRMT_ARGMT_FLAG", T.StringType(), True),
    T.StructField("DOSE_CD", T.DoubleType(), True),
    T.StructField("LOW_INCM", T.StringType(), True),
    T.StructField("RTE_OF_ADMIN", T.StringType(), True),
    T.StructField("DEA_SCHD", T.DoubleType(), True),
    T.StructField("COPAY_BNF_OPT", T.DoubleType(), True),
    T.StructField("GNRC_PROD_IN", T.DoubleType(), True),
    T.StructField("PRSCRBR_SPEC", T.StringType(), True),
    T.StructField("VAL_CD", T.StringType(), True),
    T.StructField("PRI_CARE_PDX", T.StringType(), True),
    T.StructField("OFC_OF_INSPECTOR_GNRL", T.StringType(), True),
    T.StructField("PATN_SSN", T.StringType(), True),
    T.StructField("CARDHLDR_SSN", T.StringType(), True),
    T.StructField("CARDHLDR_BRTH_DT", T.StringType(), True),
    T.StructField("CARDHLDR_ADDR", T.StringType(), True),
    T.StructField("CARDHLDR_CITY", T.StringType(), True),
    T.StructField("CHADHLDR_ST", T.StringType(), True),
    T.StructField("CARDHLDR_ZIP_CD", T.StringType(), True),
    T.StructField("PSL_FMLY_MET_AMT", T.DoubleType(), True),
    T.StructField("PSL_MBR_MET_AMT", T.DoubleType(), True),
    T.StructField("PSL_FMLY_AMT", T.DoubleType(), True),
    T.StructField("DEDCT_FMLY_MET_AMT", T.StringType(), True),
    T.StructField("DEDCT_FMLY_AMT", T.DoubleType(), True),
    T.StructField("MOPS_FMLY_AMT", T.DoubleType(), True),
    T.StructField("MOPS_FMLY_MET_AMT", T.DoubleType(), True),
    T.StructField("MOPS_MBR_MET_AMT", T.DoubleType(), True),
    T.StructField("DEDCT_MBR_MET_AMT", T.DoubleType(), True),
    T.StructField("PSL_APLD_AMT", T.DoubleType(), True),
    T.StructField("MOPS_APLD_AMT", T.DoubleType(), True),
    T.StructField("PAR_PDX_IN", T.StringType(), True),
    T.StructField("COPAY_PCT_AMT", T.DoubleType(), True),
    T.StructField("COPAY_FLAT_AMT", T.DoubleType(), True),
    T.StructField("CLM_TRNSMSN_METH", T.StringType(), True),
    T.StructField("RX_NO_2012", T.DoubleType(), True),
    T.StructField("CLM_ID", T.StringType(), True),
    T.StructField("CLM_STTUS_CD", T.StringType(), True),
    T.StructField("ADJ_FROM_CLM_ID", T.StringType(), True),
    T.StructField("ADJ_TO_CLM_ID", T.StringType(), True),
    T.StructField("SUBMT_PROD_ID_QLFR", T.StringType(), True),
    T.StructField("CNTNGNT_THER_FLAG", T.StringType(), True),
    T.StructField("CNTNGNT_THER_SCHD", T.StringType(), True),
    T.StructField("CLNT_PATN_PAY_ATRBD_PROD_AMT", T.DoubleType(), True),
    T.StructField("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT", T.DoubleType(), True),
    T.StructField("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT", T.DoubleType(), True),
    T.StructField("CLNT_PATN_PAY_ATRBD_NTWK_AMT", T.DoubleType(), True),
    T.StructField("LOB_IN", T.StringType(), True)
])

df_BCBSCommClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_bcbscommclmland)
    .load(f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}")
)

# 4) Stage: BusinessRules (CTransformerStage)
#    - Primary link: df_BCBSCommClmLand (alias "BCBSKCComm")
#    - Left lookup link on df_mbr_lkup => condition: BCBSKCComm.ORIG_MBR = mbr_lkup.MBR_UNIQ_KEY
#    - We create stage variables, then produce output pin "BCBSKCCommClmTrns" with many columns.

df_br_joined = df_BCBSCommClmLand.alias("BCBSKCComm").join(
    df_mbr_lkup.alias("mbr_lkup"),
    F.col("BCBSKCComm.ORIG_MBR") == F.col("mbr_lkup.MBR_UNIQ_KEY"),
    how="left"
)

# Stage variables
df_br_stagevars = df_br_joined \
    .withColumn("svReversalClaim",
        F.when(
            F.trim(F.col("BCBSKCComm.CLM_TYP").substr(F.lit(1),F.lit(1))) == F.lit("R"),
            F.lit(True)
        ).otherwise(F.lit(False))
    ) \
    .withColumn("svSubCk",
        F.when(
            F.col("mbr_lkup.SUB_UNIQ_KEY").isNull(),
            F.lit(1)
        ).otherwise(F.col("mbr_lkup.SUB_UNIQ_KEY"))
    ) \
    .withColumn("svMbrAge", AGE(F.col("BCBSKCComm.BRTH_DT"), F.col("BCBSKCComm.FILL_DT"))) \
    .withColumn("svClmCnt",
        F.when(F.col("svReversalClaim") == True, F.lit(-1)).otherwise(F.lit(1))
    ) \
    .withColumn("svAllowAmt",
        F.when(
            F.trim(F.col("BCBSKCComm.CLNT_GNRL_PRPS_AREA").substr(F.lit(61),F.lit(2))) != F.lit("01"),
            (F.col("BCBSKCComm.SLS_TAX_AMT") + F.col("BCBSKCComm.INGR_CST_AMT") + F.col("BCBSKCComm.DISPNS_FEE_AMT"))
            - F.col("BCBSKCComm.OTHR_PAYOR_AMT")
        ).otherwise(
            F.col("BCBSKCComm.INGR_CST_AMT") + F.col("BCBSKCComm.DISPNS_FEE_AMT") + F.col("BCBSKCComm.SLS_TAX_AMT")
        )
    ) \
    .withColumn("svMIAllowAmt",
        F.when(
            F.col("BCBSKCComm.OTHR_COV_CD") == F.lit(2.0),
            (F.col("BCBSKCComm.INGR_CST_AMT") + F.col("BCBSKCComm.DISPNS_FEE_AMT") + F.col("BCBSKCComm.SLS_TAX_AMT"))
            - F.col("BCBSKCComm.OTHR_PAYOR_AMT")
        ).otherwise(
            F.col("BCBSKCComm.INGR_CST_AMT") + F.col("BCBSKCComm.DISPNS_FEE_AMT") + F.col("BCBSKCComm.SLS_TAX_AMT")
        )
    )

# Now produce the output columns exactly in order ("BCBSKCCommClmTrns" pin).
# Remember to apply rpad for char/varchar columns. If length is specified, use that length.

df_BCBSKCCommClmTrns = df_br_stagevars.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),    # char(10)
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),       # char(1)
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),     # char(1)
    current_date().alias("FIRST_RECYC_DT"),               # date type
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("BCBSKCComm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.concat(F.col("BCBSKCComm.SRC_SYS_CD"), F.lit(";"), F.col("BCBSKCComm.CLM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_SK"),  # PrimaryKey
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("BCBSKCComm.CLM_ID"), 18, " ").alias("CLM_ID"),   # char(18)
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("BCBSKCComm.ADJ_FROM_CLM_ID"), 12, " ").alias("ADJ_FROM_CLM"),   # char(12)
    F.rpad(F.col("BCBSKCComm.ADJ_TO_CLM_ID"), 12, " ").alias("ADJ_TO_CLM"),       # char(12)
    F.rpad(F.lit("NA"), 3, " ").alias("ALPHA_PFX_CD"),                            # char(3)
    F.rpad(F.lit("NA"), 3, " ").alias("CLM_EOB_EXCD"),                            # char(3)
    F.rpad(F.lit("NA"), 4, " ").alias("CLS"),                                     # char(4)
    F.rpad(F.lit("NA"), 8, " ").alias("CLS_PLN"),                                 # char(8)
    F.rpad(F.lit("NA"), 4, " ").alias("EXPRNC_CAT"),                              # char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("FNCL_LOB_NO"),                             # char(4)
    F.rpad(F.col("BCBSKCComm.GRP_NO"), 8, " ").alias("GRP"),                      # char(8)
    F.col("BCBSKCComm.ORIG_MBR").alias("MBR_CK"),
    F.rpad(F.col("BCBSKCComm.ORIG_MBR"), 8, " ").alias("PROD"),                   # char(8)
    F.rpad(F.col("BCBSKCComm.ORIG_MBR"), 4, " ").alias("SUBGRP"),                 # char(4)
    F.col("svSubCk").alias("SUB_CK"),
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_ACDNT_CD"),                           # char(10)
    F.rpad(F.lit("NA"), 2, " ").alias("CLM_ACDNT_ST_CD"),                         # char(2)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),             # char(10)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_AGMNT_SRC_CD"),                       # char(10)
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_BTCH_ACTN_CD"),                        # char(1)
    F.rpad(F.lit("N"), 10, " ").alias("CLM_CAP_CD"),                              # char(10)
    F.rpad(F.lit("STD"), 10, " ").alias("CLM_CAT_CD"),                            # char(10)
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_CHK_CYC_OVRD_CD"),                     # char(1)
    F.when(
        (F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("OPTUMRX")) &
        (F.col("BCBSKCComm.OTHR_COV_CD") == 2.0) &
        (F.col("BCBSKCComm.OTHR_PAYOR_AMT") != 0),
        F.lit("2")
    ).when(
        (F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("MEDIMPACT")) &
        (F.col("BCBSKCComm.OTHR_COV_CD") == 2.0) &
        (F.col("BCBSKCComm.OTHR_PAYOR_AMT") != 0),
        F.lit("2")
    ).otherwise(F.lit("NA")).alias("CLM_COB_CD"),                                 # char(2) in DS but listed as char(1) in JSON; keep match.
    F.lit("ACPTD").alias("FINL_DISP_CD"),
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_INPT_METH_CD"),                        # char(1)
    F.rpad(
        F.when(
            (F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("OPTUMRX")) &
            (F.col("BCBSKCComm.CLM_TRNSMSN_METH").isNotNull()),
            F.col("BCBSKCComm.CLM_TRNSMSN_METH")
        ).otherwise(F.lit("NA")),
        10, " "
    ).alias("CLM_INPT_SRC_CD"),                                                  # char(10)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_IPP_CD"),                             # char(10)
    F.rpad(
        F.when(
            F.col("BCBSKCComm.PAR_PDX_IN").isNull() | (F.length(F.col("BCBSKCComm.PAR_PDX_IN")) == 0),
            F.lit("UNK")
        ).otherwise(F.col("BCBSKCComm.PAR_PDX_IN")),
        2, " "
    ).alias("CLM_NTWK_STTUS_CD"),                                                # char(2)
    F.rpad(F.lit("NA"), 4, " ").alias("CLM_NONPAR_PROV_PFX_CD"),                  # char(4)
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_OTHER_BNF_CD"),                        # char(1)
    F.rpad(F.lit("NA"), 1, " ").alias("CLM_PAYE_CD"),                            # char(1)
    F.rpad(F.lit("NA"), 4, " ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),               # char(4)
    F.rpad(F.lit("NA"), 4, " ").alias("CLM_SVC_DEFN_PFX_CD"),                    # char(4)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_SVC_PROV_SPEC_CD"),                  # char(10)
    F.rpad(F.lit("0012"), 10, " ").alias("CLM_SVC_PROV_TYP_CD"),                 # char(10)
    F.rpad(
        F.when(
            (F.col("BCBSKCComm.CLM_STTUS_CD").isNull()) |
            (F.length(F.trim(F.col("BCBSKCComm.CLM_STTUS_CD"))) == 0),
            F.lit("0")
        ).otherwise(F.trim(F.col("BCBSKCComm.CLM_STTUS_CD"))),
        2, " "
    ).alias("CLM_STTUS_CD"),                                                     # char(2)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_SUBMT_BCBS_PLN_CD"),                 # char(10)
    F.rpad(F.lit("NA"), 10, " ").alias("CLM_SUB_BCBS_PLN_CD"),                   # char(10)
    F.rpad(F.lit("RX"), 10, " ").alias("CLM_SUBTYP_CD"),                         # char(10)
    F.rpad(F.lit("MED"), 1, " ").alias("CLM_TYP_CD"),                            # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("ATCHMT_IN"),                               # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("CLM_CLNCL_EDIT_CD"),                       # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("COBRA_CLM_IN"),                            # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("FIRST_PASS_IN"),                           # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("HOST_IN"),                                 # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("LTR_IN"),                                  # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("MCARE_ASG_IN"),                            # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("NOTE_IN"),                                 # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("PCA_AUDIT_IN"),                            # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("PCP_SUBMT_IN"),                            # char(1)
    F.rpad(F.lit("N"), 1, " ").alias("PROD_OOA_IN"),                             # char(1)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("ACDNT_DT"),                      # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("INPT_DT"),                       # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("MBR_PLN_ELIG_DT"),               # char(10)
    F.rpad(
        F.when(F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("OPTUMRX"), F.lit("2199-12-31")).otherwise(F.lit("1753-01-01")),
        10, " "
    ).alias("NEXT_RVW_DT"),                                                     # char(10)
    F.rpad(
        F.when(
            (F.col("BCBSKCComm.PD_DT").isNull()) |
            (F.length(F.trim(F.col("BCBSKCComm.PD_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.trim(F.col("BCBSKCComm.PD_DT"))),
        10, " "
    ).alias("PD_DT"),                                                           # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("PAYMT_DRAG_CYC_DT"),            # char(10)
    F.rpad(
        F.when(
            (F.col("BCBSKCComm.PD_DT").isNull()) |
            (F.length(F.trim(F.col("BCBSKCComm.PD_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.trim(F.col("BCBSKCComm.PD_DT"))),
        10, " "
    ).alias("PRCS_DT"),                                                         # char(10)
    F.rpad(
        F.when(
            (F.col("BCBSKCComm.FILE_RCVD_DT").isNull()) |
            (F.length(F.trim(F.col("BCBSKCComm.FILE_RCVD_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.trim(F.col("BCBSKCComm.FILE_RCVD_DT"))),
        10, " "
    ).alias("RCVD_DT"),                                                         # char(10)
    F.rpad(
        F.when(
            (F.col("BCBSKCComm.FILL_DT").isNull()) |
            (F.length(F.trim(F.col("BCBSKCComm.FILL_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.trim(F.col("BCBSKCComm.FILL_DT"))),
        10, " "
    ).alias("SVC_STRT_DT"),                                                     # char(10)
    F.rpad(
        F.when(
            (F.col("BCBSKCComm.FILL_DT").isNull()) |
            (F.length(F.trim(F.col("BCBSKCComm.FILL_DT"))) == 0),
            F.lit("1753-01-01")
        ).otherwise(F.trim(F.col("BCBSKCComm.FILL_DT"))),
        10, " "
    ).alias("SVC_END_DT"),                                                      # char(10)
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("SMLR_ILNS_DT"),                  # char(10)
    current_date().alias("STTUS_DT"),                                           # char(10) => stored as date, but we'll keep it as is
    F.rpad(F.lit("1753-01-01"), 10, " ").alias("WORK_UNABLE_BEG_DT"),           # char(10)
    F.rpad(F.lit("2199-12-31"), 10, " ").alias("WORK_UNABLE_END_DT"),           # char(10)
    F.lit(0.00).alias("ACDNT_AMT"),
    F.col("BCBSKCComm.BILL_AMT").alias("ACTL_PD_AMT"),
    F.when(
        F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("OPTUMRX"),
        F.col("svAllowAmt")
    ).when(
        F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("MEDIMPACT"),
        F.col("svMIAllowAmt")
    ).otherwise(F.lit(0.00)).alias("ALLOW_AMT"),
    F.lit(0.00).alias("DSALW_AMT"),
    F.when(
        F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("OPTUMRX"),
        F.col("BCBSKCComm.COPAY_PCT_AMT")
    ).otherwise(F.lit(0.00)).alias("COINS_AMT"),
    F.when(
        F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("OPTUMRX"),
        F.col("BCBSKCComm.FULL_AVG_WHLSL_PRICE")
    ).when(
        F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("MEDIMPACT"),
        F.col("BCBSKCComm.CLM_AMT")
    ).otherwise(F.lit(0.00)).alias("CNSD_CHRG_AMT"),
    F.when(
        (F.col("BCBSKCComm.COPAY_AMT").isNull()) |
        (F.length(F.trim(F.col("BCBSKCComm.COPAY_AMT"))) == 0),
        F.lit("0.0")
    ).otherwise(F.trim(F.col("BCBSKCComm.COPAY_AMT"))).alias("COPAY_AMT"),
    F.when(
        F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("OPTUMRX"),
        F.col("BCBSKCComm.FULL_AVG_WHLSL_PRICE")
    ).when(
        F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("MEDIMPACT"),
        F.col("BCBSKCComm.CLM_AMT")
    ).otherwise(F.lit(0.00)).alias("CHRG_AMT"),
    F.when(
        (F.col("BCBSKCComm.DEDCT_AMT").isNull()) |
        (F.length(F.trim(F.col("BCBSKCComm.DEDCT_AMT"))) == 0),
        F.lit("0.0")
    ).otherwise(F.trim(F.col("BCBSKCComm.DEDCT_AMT"))).alias("DEDCT_AMT"),
    F.when(
        (F.col("BCBSKCComm.BILL_AMT").isNull()) |
        (F.length(F.trim(F.col("BCBSKCComm.BILL_AMT"))) == 0),
        F.lit("0.0")
    ).otherwise(F.trim(F.col("BCBSKCComm.BILL_AMT"))).alias("PAYBL_AMT"),
    F.col("svClmCnt").alias("CLM_CT"),
    F.col("svMbrAge").alias("MBR_AGE"),
    F.col("BCBSKCComm.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("BCBSKCComm.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.rpad(F.lit("NA"), 18, " ").alias("DOC_TX_ID"),               # char(18)
    F.lit(None).alias("MCAID_RESUB_NO"),                           # char(15), @NULL
    F.rpad(F.lit("NA"), 12, " ").alias("MCARE_ID"),                # char(12)
    F.rpad(
        F.when(
            F.col("mbr_lkup.MBR_UNIQ_KEY").isNotNull(),
            F.trim(F.col("mbr_lkup.MBR_SFX_NO"))
        ).otherwise(F.lit("0")),
        2, " "
    ).alias("MBR_SFX_NO"),                                         # char(2)
    F.lit(None).alias("PATN_ACCT_NO"),
    F.rpad(
        F.when(
            F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("MEDIMPACT"),
            F.col("BCBSKCComm.ADJDCT_REF_NO")
        ).otherwise(F.lit("NA")),
        16, " "
    ).alias("PAYMT_REF_ID"),                                       # char(16)
    F.rpad(F.lit("NA"), 12, " ").alias("PROV_AGMNT_ID"),            # char(12)
    F.lit(None).alias("RFRNG_PROV_TX"),
    F.rpad(
        F.when(
            F.col("mbr_lkup.SUB_ID").isNotNull(),
            F.trim(F.col("mbr_lkup.SUB_ID"))
        ).otherwise(F.lit("0")),
        14, " "
    ).alias("SUB_ID"),                                             # char(14)
    F.lit("NA").alias("PCA_TYP_CD"),                               # no length given => but in DS is char(??). We'll keep as is for consistency with pinned columns.
    F.lit("NA").alias("REL_PCA_CLM_ID"),
    F.lit("NA").alias("REL_BASE_CLM_ID"),
    F.lit(0.00).alias("REMIT_SUPRSION_AMT"),
    F.lit("NA").alias("MCAID_STTUS_ID"),
    F.lit(0.00).alias("PATN_PD_AMT"),
    F.lit("NA").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.rpad(
        F.lit("NA"),
        12, " "
    ).alias("NTWK"),                                              # char(12)
    F.when(
        F.col("BCBSKCComm.SRC_SYS_CD") == F.lit("OPTUMRX"),
        F.col("BCBSKCComm.CLNT_GNRL_PRPS_AREA").substr(F.lit(63),F.lit(1))
    ).otherwise(F.lit("N")).alias("BILL_PAYMT_EXCL_IN")            # char(1)
)

# 5) Stage: alpha_pfx (CTransformerStage)
#    - Primary link: df_BCBSKCCommClmTrns
#    - Left lookups: sub_alpha_pfx_lkup on (BCBSKCCommClmTrns.SUB_CK = sub_alpha_pfx_lkup.SUB_UNIQ_KEY)
#                    mbr_enr_lkup on (BCBSKCCommClmTrns.CLM_ID = mbr_enr_lkup.CLM_ID)

df_alpha_join1 = df_BCBSKCCommClmTrns.alias("BCBSKCCommClmTrns").join(
    df_sub_alpha_pfx_lkup.alias("sub_alpha_pfx_lkup"),
    F.col("BCBSKCCommClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
    how="left"
)
df_alpha_join2 = df_alpha_join1.join(
    df_mbr_enr_lkup.alias("mbr_enr_lkup"),
    F.col("BCBSKCCommClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID"),
    how="left"
)

# Output pin "Transform" (df_Transform)
df_Transform = df_alpha_join2.select(
    F.col("BCBSKCCommClmTrns.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("BCBSKCCommClmTrns.INSRT_UPDT_CD"),
    F.col("BCBSKCCommClmTrns.DISCARD_IN"),
    F.col("BCBSKCCommClmTrns.PASS_THRU_IN"),
    F.col("BCBSKCCommClmTrns.FIRST_RECYC_DT"),
    F.col("BCBSKCCommClmTrns.ERR_CT"),
    F.col("BCBSKCCommClmTrns.RECYCLE_CT"),
    F.col("BCBSKCCommClmTrns.SRC_SYS_CD"),
    F.col("BCBSKCCommClmTrns.PRI_KEY_STRING"),
    F.col("BCBSKCCommClmTrns.CLM_SK"),
    F.col("BCBSKCCommClmTrns.SRC_SYS_CD_SK"),
    F.col("BCBSKCCommClmTrns.CLM_ID"),
    F.col("BCBSKCCommClmTrns.CRT_RUN_CYC_EXCTN_SK"),
    F.col("BCBSKCCommClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("BCBSKCCommClmTrns.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("BCBSKCCommClmTrns.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.rpad(
        F.when(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(), F.lit("UNK"))
         .otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")),
        3, " "
    ).alias("ALPHA_PFX_CD"),  # char(3)
    F.col("BCBSKCCommClmTrns.CLM_EOB_EXCD"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.CLS_ID")),
        4, " "
    ).alias("CLS"),  # char(4)
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.CLS_PLN_ID")),
        8, " "
    ).alias("CLS_PLN"),  # char(8)
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD")),
        4, " "
    ).alias("EXPRNC_CAT"),  # char(4)
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD")),
        4, " "
    ).alias("FNCL_LOB_NO"),  # char(4)
    F.col("BCBSKCCommClmTrns.GRP"),
    F.col("BCBSKCCommClmTrns.MBR_CK"),
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.PROD_ID")),
        8, " "
    ).alias("PROD"),  # char(8)
    F.rpad(
        F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
         .otherwise(F.col("mbr_enr_lkup.SUBGRP_ID")),
        4, " "
    ).alias("SUBGRP"),  # char(4)
    F.col("BCBSKCCommClmTrns.SUB_CK"),
    F.col("BCBSKCCommClmTrns.CLM_ACDNT_CD"),
    F.col("BCBSKCCommClmTrns.CLM_ACDNT_ST_CD"),
    F.col("BCBSKCCommClmTrns.CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("BCBSKCCommClmTrns.CLM_AGMNT_SRC_CD"),
    F.col("BCBSKCCommClmTrns.CLM_BTCH_ACTN_CD"),
    F.col("BCBSKCCommClmTrns.CLM_CAP_CD"),
    F.col("BCBSKCCommClmTrns.CLM_CAT_CD"),
    F.col("BCBSKCCommClmTrns.CLM_CHK_CYC_OVRD_CD"),
    F.col("BCBSKCCommClmTrns.CLM_COB_CD"),
    F.col("BCBSKCCommClmTrns.FINL_DISP_CD"),
    F.col("BCBSKCCommClmTrns.CLM_INPT_METH_CD"),
    F.col("BCBSKCCommClmTrns.CLM_INPT_SRC_CD"),
    F.col("BCBSKCCommClmTrns.CLM_IPP_CD"),
    F.col("BCBSKCCommClmTrns.CLM_NTWK_STTUS_CD"),
    F.col("BCBSKCCommClmTrns.CLM_NONPAR_PROV_PFX_CD"),
    F.col("BCBSKCCommClmTrns.CLM_OTHER_BNF_CD"),
    F.col("BCBSKCCommClmTrns.CLM_PAYE_CD"),
    F.col("BCBSKCCommClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("BCBSKCCommClmTrns.CLM_SVC_DEFN_PFX_CD"),
    F.col("BCBSKCCommClmTrns.CLM_SVC_PROV_SPEC_CD"),
    F.col("BCBSKCCommClmTrns.CLM_SVC_PROV_TYP_CD"),
    F.col("BCBSKCCommClmTrns.CLM_STTUS_CD"),
    F.col("BCBSKCCommClmTrns.CLM_SUBMT_BCBS_PLN_CD"),
    F.col("BCBSKCCommClmTrns.CLM_SUB_BCBS_PLN_CD"),
    F.col("BCBSKCCommClmTrns.CLM_SUBTYP_CD"),
    F.col("BCBSKCCommClmTrns.CLM_TYP_CD"),
    F.col("BCBSKCCommClmTrns.ATCHMT_IN"),
    F.col("BCBSKCCommClmTrns.CLM_CLNCL_EDIT_CD"),
    F.col("BCBSKCCommClmTrns.COBRA_CLM_IN"),
    F.col("BCBSKCCommClmTrns.FIRST_PASS_IN"),
    F.col("BCBSKCCommClmTrns.HOST_IN"),
    F.col("BCBSKCCommClmTrns.LTR_IN"),
    F.col("BCBSKCCommClmTrns.MCARE_ASG_IN"),
    F.col("BCBSKCCommClmTrns.NOTE_IN"),
    F.col("BCBSKCCommClmTrns.PCA_AUDIT_IN"),
    F.col("BCBSKCCommClmTrns.PCP_SUBMT_IN"),
    F.col("BCBSKCCommClmTrns.PROD_OOA_IN"),
    F.col("BCBSKCCommClmTrns.ACDNT_DT"),
    F.col("BCBSKCCommClmTrns.INPT_DT"),
    F.col("BCBSKCCommClmTrns.MBR_PLN_ELIG_DT"),
    F.col("BCBSKCCommClmTrns.NEXT_RVW_DT"),
    F.col("BCBSKCCommClmTrns.PD_DT"),
    F.col("BCBSKCCommClmTrns.PAYMT_DRAG_CYC_DT"),
    F.col("BCBSKCCommClmTrns.PRCS_DT"),
    F.col("BCBSKCCommClmTrns.RCVD_DT"),
    F.col("BCBSKCCommClmTrns.SVC_STRT_DT"),
    F.col("BCBSKCCommClmTrns.SVC_END_DT"),
    F.col("BCBSKCCommClmTrns.SMLR_ILNS_DT"),
    F.col("BCBSKCCommClmTrns.STTUS_DT"),
    F.col("BCBSKCCommClmTrns.WORK_UNABLE_BEG_DT"),
    F.col("BCBSKCCommClmTrns.WORK_UNABLE_END_DT"),
    F.col("BCBSKCCommClmTrns.ACDNT_AMT"),
    F.col("BCBSKCCommClmTrns.ACTL_PD_AMT"),
    F.col("BCBSKCCommClmTrns.ALLOW_AMT"),
    F.col("BCBSKCCommClmTrns.DSALW_AMT"),
    F.col("BCBSKCCommClmTrns.COINS_AMT"),
    F.col("BCBSKCCommClmTrns.CNSD_CHRG_AMT"),
    F.col("BCBSKCCommClmTrns.COPAY_AMT"),
    F.col("BCBSKCCommClmTrns.CHRG_AMT"),
    F.col("BCBSKCCommClmTrns.DEDCT_AMT"),
    F.col("BCBSKCCommClmTrns.PAYBL_AMT"),
    F.col("BCBSKCCommClmTrns.CLM_CT"),
    F.col("BCBSKCCommClmTrns.MBR_AGE"),
    F.col("BCBSKCCommClmTrns.ADJ_FROM_CLM_ID"),
    F.col("BCBSKCCommClmTrns.ADJ_TO_CLM_ID"),
    F.col("BCBSKCCommClmTrns.DOC_TX_ID"),
    F.col("BCBSKCCommClmTrns.MCAID_RESUB_NO"),
    F.col("BCBSKCCommClmTrns.MCARE_ID"),
    F.col("BCBSKCCommClmTrns.MBR_SFX_NO"),
    F.col("BCBSKCCommClmTrns.PATN_ACCT_NO"),
    F.col("BCBSKCCommClmTrns.PAYMT_REF_ID"),
    F.col("BCBSKCCommClmTrns.PROV_AGMNT_ID"),
    F.col("BCBSKCCommClmTrns.RFRNG_PROV_TX"),
    F.col("BCBSKCCommClmTrns.SUB_ID"),
    F.lit(None).alias("PRPR_ENTITY"),  # char(1)
    F.col("BCBSKCCommClmTrns.PCA_TYP_CD"),
    F.col("BCBSKCCommClmTrns.REL_PCA_CLM_ID"),
    F.lit("NA").alias("CLCL_MICRO_ID"),
    F.rpad(F.lit("N"), 1, " ").alias("CLM_UPDT_SW"),  # char(1)
    F.col("BCBSKCCommClmTrns.REMIT_SUPRSION_AMT"),
    F.col("BCBSKCCommClmTrns.MCAID_STTUS_ID"),
    F.col("BCBSKCCommClmTrns.PATN_PD_AMT"),
    F.col("BCBSKCCommClmTrns.CLM_SUBMT_ICD_VRSN_CD"),
    F.rpad(F.lit(" "), 1, " ").alias("CLM_TXNMY_CD"),
    F.col("BCBSKCCommClmTrns.BILL_PAYMT_EXCL_IN")
)

# 6) Stage: Snapshot (CTransformerStage)
#    - Primary link: df_Transform
#    - Outputs: "Pkey" => "ClmPK", "Snapshot" => "Transformer"

df_snapshot = df_Transform.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT"),
    F.col("Transform.RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING"),
    F.col("Transform.CLM_SK"),
    F.col("Transform.SRC_SYS_CD_SK"),
    F.col("Transform.CLM_ID"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("Transform.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.col("Transform.ALPHA_PFX_CD"),
    F.col("Transform.CLM_EOB_EXCD"),
    F.col("Transform.CLS").alias("CLS"),
    F.col("Transform.CLS_PLN").alias("CLS_PLN"),
    F.col("Transform.EXPRNC_CAT"),
    F.col("Transform.FNCL_LOB_NO"),
    F.col("Transform.GRP"),
    F.col("Transform.MBR_CK"),
    F.col("Transform.NTWK"),
    F.col("Transform.PROD"),
    F.col("Transform.SUBGRP"),
    F.col("Transform.SUB_CK"),
    F.col("Transform.CLM_ACDNT_CD"),
    F.col("Transform.CLM_ACDNT_ST_CD"),
    F.col("Transform.CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("Transform.CLM_AGMNT_SRC_CD"),
    F.col("Transform.CLM_BTCH_ACTN_CD"),
    F.col("Transform.CLM_CAP_CD"),
    F.col("Transform.CLM_CAT_CD"),
    F.col("Transform.CLM_CHK_CYC_OVRD_CD"),
    F.col("Transform.CLM_COB_CD"),
    F.col("Transform.FINL_DISP_CD"),
    F.col("Transform.CLM_INPT_METH_CD"),
    F.col("Transform.CLM_INPT_SRC_CD"),
    F.col("Transform.CLM_IPP_CD"),
    F.col("Transform.CLM_NTWK_STTUS_CD"),
    F.col("Transform.CLM_NONPAR_PROV_PFX_CD"),
    F.col("Transform.CLM_OTHER_BNF_CD"),
    F.col("Transform.CLM_PAYE_CD"),
    F.col("Transform.CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("Transform.CLM_SVC_DEFN_PFX_CD"),
    F.col("Transform.CLM_SVC_PROV_SPEC_CD"),
    F.col("Transform.CLM_SVC_PROV_TYP_CD"),
    F.col("Transform.CLM_STTUS_CD"),
    F.col("Transform.CLM_SUBMT_BCBS_PLN_CD"),
    F.col("Transform.CLM_SUB_BCBS_PLN_CD"),
    F.col("Transform.CLM_SUBTYP_CD"),
    F.col("Transform.CLM_TYP_CD"),
    F.col("Transform.ATCHMT_IN"),
    F.col("Transform.CLM_CLNCL_EDIT_CD"),
    F.col("Transform.COBRA_CLM_IN"),
    F.col("Transform.FIRST_PASS_IN"),
    F.col("Transform.HOST_IN"),
    F.col("Transform.LTR_IN"),
    F.col("Transform.MCARE_ASG_IN"),
    F.col("Transform.NOTE_IN"),
    F.col("Transform.PCA_AUDIT_IN"),
    F.col("Transform.PCP_SUBMT_IN"),
    F.col("Transform.PROD_OOA_IN"),
    F.col("Transform.ACDNT_DT"),
    F.col("Transform.INPT_DT"),
    F.col("Transform.MBR_PLN_ELIG_DT"),
    F.col("Transform.NEXT_RVW_DT"),
    F.col("Transform.PD_DT"),
    F.col("Transform.PAYMT_DRAG_CYC_DT"),
    F.col("Transform.PRCS_DT"),
    F.col("Transform.RCVD_DT"),
    F.col("Transform.SVC_STRT_DT"),
    F.col("Transform.SVC_END_DT"),
    F.col("Transform.SMLR_ILNS_DT"),
    F.col("Transform.STTUS_DT"),
    F.col("Transform.WORK_UNABLE_BEG_DT"),
    F.col("Transform.WORK_UNABLE_END_DT"),
    F.col("Transform.ACDNT_AMT"),
    F.col("Transform.ACTL_PD_AMT"),
    F.col("Transform.ALLOW_AMT"),
    F.col("Transform.DSALW_AMT"),
    F.col("Transform.COINS_AMT"),
    F.col("Transform.CNSD_CHRG_AMT"),
    F.col("Transform.COPAY_AMT"),
    F.col("Transform.CHRG_AMT"),
    F.col("Transform.DEDCT_AMT"),
    F.col("Transform.PAYBL_AMT"),
    F.col("Transform.CLM_CT"),
    F.col("Transform.MBR_AGE"),
    F.col("Transform.ADJ_FROM_CLM_ID"),
    F.col("Transform.ADJ_TO_CLM_ID"),
    F.col("Transform.DOC_TX_ID"),
    F.col("Transform.MCAID_RESUB_NO"),
    F.col("Transform.MCARE_ID"),
    F.col("Transform.MBR_SFX_NO"),
    F.col("Transform.PATN_ACCT_NO"),
    F.col("Transform.PAYMT_REF_ID"),
    F.col("Transform.PROV_AGMNT_ID"),
    F.col("Transform.RFRNG_PROV_TX"),
    F.col("Transform.SUB_ID"),
    F.col("Transform.PRPR_ENTITY"),
    F.col("Transform.PCA_TYP_CD"),
    F.col("Transform.REL_PCA_CLM_ID"),
    F.col("Transform.CLCL_MICRO_ID"),
    F.col("Transform.CLM_UPDT_SW"),
    F.col("Transform.REMIT_SUPRSION_AMT"),
    F.col("Transform.MCAID_STTUS_ID"),
    F.col("Transform.PATN_PD_AMT"),
    F.col("Transform.CLM_SUBMT_ICD_VRSN_CD"),
    F.col("Transform.CLM_TXNMY_CD"),
    F.col("Transform.BILL_PAYMT_EXCL_IN")
).alias("Snapshot")

df_Pkey = df_snapshot.select(
    F.col("Snapshot.JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Snapshot.INSRT_UPDT_CD"),
    F.col("Snapshot.DISCARD_IN"),
    F.col("Snapshot.PASS_THRU_IN"),
    F.col("Snapshot.FIRST_RECYC_DT"),
    F.col("Snapshot.ERR_CT"),
    F.col("Snapshot.RECYCLE_CT"),
    F.col("Snapshot.SRC_SYS_CD"),
    F.col("Snapshot.PRI_KEY_STRING"),
    F.col("Snapshot.CLM_SK"),
    F.col("Snapshot.SRC_SYS_CD_SK"),
    F.col("Snapshot.CLM_ID"),
    F.col("Snapshot.CRT_RUN_CYC_EXCTN_SK"),
    F.col("Snapshot.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Snapshot.ADJ_FROM_CLM"),
    F.col("Snapshot.ADJ_TO_CLM"),
    F.col("Snapshot.ALPHA_PFX_CD"),
    F.col("Snapshot.CLM_EOB_EXCD"),
    F.col("Snapshot.CLS"),
    F.col("Snapshot.CLS_PLN"),
    F.col("Snapshot.EXPRNC_CAT"),
    F.col("Snapshot.FNCL_LOB_NO"),
    F.col("Snapshot.GRP"),
    F.col("Snapshot.MBR_CK"),
    F.col("Snapshot.NTWK"),
    F.col("Snapshot.PROD"),
    F.col("Snapshot.SUBGRP"),
    F.col("Snapshot.SUB_CK"),
    F.col("Snapshot.CLM_ACDNT_CD"),
    F.col("Snapshot.CLM_ACDNT_ST_CD"),
    F.col("Snapshot.CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("Snapshot.CLM_AGMNT_SRC_CD"),
    F.col("Snapshot.CLM_BTCH_ACTN_CD"),
    F.col("Snapshot.CLM_CAP_CD"),
    F.col("Snapshot.CLM_CAT_CD"),
    F.col("Snapshot.CLM_CHK_CYC_OVRD_CD"),
    F.col("Snapshot.CLM_COB_CD"),
    F.col("Snapshot.FINL_DISP_CD"),
    F.col("Snapshot.CLM_INPT_METH_CD"),
    F.col("Snapshot.CLM_INPT_SRC_CD"),
    F.col("Snapshot.CLM_IPP_CD"),
    F.col("Snapshot.CLM_NTWK_STTUS_CD"),
    F.col("Snapshot.CLM_NONPAR_PROV_PFX_CD"),
    F.col("Snapshot.CLM_OTHER_BNF_CD"),
    F.col("Snapshot.CLM_PAYE_CD"),
    F.col("Snapshot.CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("Snapshot.CLM_SVC_DEFN_PFX_CD"),
    F.col("Snapshot.CLM_SVC_PROV_SPEC_CD"),
    F.col("Snapshot.CLM_SVC_PROV_TYP_CD"),
    F.col("Snapshot.CLM_STTUS_CD"),
    F.col("Snapshot.CLM_SUBMT_BCBS_PLN_CD"),
    F.col("Snapshot.CLM_SUB_BCBS_PLN_CD"),
    F.col("Snapshot.CLM_SUBTYP_CD"),
    F.col("Snapshot.CLM_TYP_CD"),
    F.col("Snapshot.ATCHMT_IN"),
    F.col("Snapshot.CLM_CLNCL_EDIT_CD"),
    F.col("Snapshot.COBRA_CLM_IN"),
    F.col("Snapshot.FIRST_PASS_IN"),
    F.col("Snapshot.HOST_IN"),
    F.col("Snapshot.LTR_IN"),
    F.col("Snapshot.MCARE_ASG_IN"),
    F.col("Snapshot.NOTE_IN"),
    F.col("Snapshot.PCA_AUDIT_IN"),
    F.col("Snapshot.PCP_SUBMT_IN"),
    F.col("Snapshot.PROD_OOA_IN"),
    F.col("Snapshot.ACDNT_DT"),
    F.col("Snapshot.INPT_DT"),
    F.col("Snapshot.MBR_PLN_ELIG_DT"),
    F.col("Snapshot.NEXT_RVW_DT"),
    F.col("Snapshot.PD_DT"),
    F.col("Snapshot.PAYMT_DRAG_CYC_DT"),
    F.col("Snapshot.PRCS_DT"),
    F.col("Snapshot.RCVD_DT"),
    F.col("Snapshot.SVC_STRT_DT"),
    F.col("Snapshot.SVC_END_DT"),
    F.col("Snapshot.SMLR_ILNS_DT"),
    F.col("Snapshot.STTUS_DT"),
    F.col("Snapshot.WORK_UNABLE_BEG_DT"),
    F.col("Snapshot.WORK_UNABLE_END_DT"),
    F.col("Snapshot.ACDNT_AMT"),
    F.col("Snapshot.ACTL_PD_AMT"),
    F.col("Snapshot.ALLOW_AMT"),
    F.col("Snapshot.DSALW_AMT"),
    F.col("Snapshot.COINS_AMT"),
    F.col("Snapshot.CNSD_CHRG_AMT"),
    F.col("Snapshot.COPAY_AMT"),
    F.col("Snapshot.CHRG_AMT"),
    F.col("Snapshot.DEDCT_AMT"),
    F.col("Snapshot.PAYBL_AMT"),
    F.col("Snapshot.CLM_CT"),
    F.col("Snapshot.MBR_AGE"),
    F.col("Snapshot.ADJ_FROM_CLM_ID"),
    F.col("Snapshot.ADJ_TO_CLM_ID"),
    F.col("Snapshot.DOC_TX_ID"),
    F.col("Snapshot.MCAID_RESUB_NO"),
    F.col("Snapshot.MCARE_ID"),
    F.col("Snapshot.MBR_SFX_NO"),
    F.col("Snapshot.PATN_ACCT_NO"),
    F.col("Snapshot.PAYMT_REF_ID"),
    F.col("Snapshot.PROV_AGMNT_ID"),
    F.col("Snapshot.RFRNG_PROV_TX"),
    F.col("Snapshot.SUB_ID"),
    F.col("Snapshot.PRPR_ENTITY"),
    F.col("Snapshot.PCA_TYP_CD"),
    F.col("Snapshot.REL_PCA_CLM_ID"),
    F.col("Snapshot.CLCL_MICRO_ID"),
    F.col("Snapshot.CLM_UPDT_SW"),
    F.col("Snapshot.REMIT_SUPRSION_AMT"),
    F.col("Snapshot.MCAID_STTUS_ID"),
    F.col("Snapshot.PATN_PD_AMT"),
    F.col("Snapshot.CLM_SUBMT_ICD_VRSN_CD"),
    F.col("Snapshot.CLM_TXNMY_CD"),
    F.col("Snapshot.BILL_PAYMT_EXCL_IN")
)

df_SnapshotToTransformer = df_snapshot.select(
    F.rpad(F.col("Snapshot.CLM_ID"), 12, " ").alias("CLM_ID"),  # char(12), PK
    F.col("Snapshot.MBR_CK").alias("MBR_CK"),
    F.rpad(F.col("Snapshot.GRP"), 8, " ").alias("GRP"),
    F.col("Snapshot.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT"),
    F.rpad(F.col("Snapshot.EXPRNC_CAT"), 4, " ").alias("EXPRNC_CAT"),  # char(4)
    F.rpad(F.col("Snapshot.FNCL_LOB_NO"), 4, " ").alias("FNCL_LOB_NO"),  # char(4)
    F.col("Snapshot.CLM_CT").alias("CLM_CT"),
    F.rpad(F.col("Snapshot.PCA_TYP_CD"), 18, " ").alias("PCA_TYP_CD"),  # char(18)
    F.rpad(F.col("Snapshot.CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),  # char(2)
    F.rpad(F.col("Snapshot.CLM_CAT_CD"), 10, " ").alias("CLM_CAT_CD")      # char(10)
).alias("Snapshot")

# 7) Stage: Transformer (CTransformerStage) with stage variables calling user-defined functions
#    Input: df_SnapshotToTransformer => Output: B_CLM link
df_Transformer_vars = df_SnapshotToTransformer.withColumn(
    "ExpCatCdSk",
    GetFkeyExprncCat(F.lit("FACETS"), F.lit(0), F.col("Snapshot.EXPRNC_CAT"), F.lit("N"))
).withColumn(
    "GrpSk",
    GetFkeyGrp(F.lit("FACETS"), F.lit(0), F.col("Snapshot.GRP"), F.lit("N"))
).withColumn(
    "MbrSk",
    GetFkeyMbr(F.lit("FACETS"), F.lit(0), F.col("Snapshot.MBR_CK"), F.lit("N"))
).withColumn(
    "FnclLobSk",
    GetFkeyFnclLob(F.lit("PSI"), F.lit(0), F.col("Snapshot.FNCL_LOB_NO"), F.lit("N"))
).withColumn(
    "PcaTypCdSk",
    GetFkeyCodes(F.lit("FACETS"), F.lit(0), F.lit("PERSONAL CARE ACCOUNT PROCESSING"), F.col("Snapshot.PCA_TYP_CD"), F.lit("N"))
).withColumn(
    "ClmSttusCdSk",
    GetFkeyCodes(F.col("SrcSysCd"), F.lit(0), F.lit("CLAIM STATUS"), F.col("Snapshot.CLM_STTUS_CD"), F.lit("N"))
).withColumn(
    "ClmCatCdSk",
    GetFkeyCodes(F.col("SrcSysCd"), F.lit(0), F.lit("CLAIM CATEGORY"), F.col("Snapshot.CLM_CAT_CD"), F.lit("X"))
)

df_B_CLM = df_Transformer_vars.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),  # PK
    F.col("Snapshot.CLM_ID").alias("CLM_ID"),    # PK
    F.col("ClmSttusCdSk").alias("CLM_STTUS_CD_SK"),
    F.col("ClmCatCdSk").alias("CLM_CAT_CD_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FnclLobSk").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.rpad(F.col("Snapshot.SVC_STRT_DT"), 10, " ").alias("SVC_STRT_DT_SK"),  # char(10)
    F.col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("Snapshot.CLM_CT").alias("CLM_CT"),
    F.col("PcaTypCdSk").alias("PCA_TYP_CD_SK")
)

# 8) Stage: B_CLM (CSeqFileStage) - write the B_CLM.#SrcSysCd#.dat.#RunID# file
write_files(
    df_B_CLM,
    f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 9) Stage: ClmPK (CContainerStage)
#    We call the shared container "ClmPK". The job has a single input link "Pkey" => we pass df_Pkey + parameters.

params_ClmPK = {
    "CurrRunCycle": RunCycle
}
df_ClmPK = ClmPK(df_Pkey, params_ClmPK)

# 10) Stage: BCBSKCCommClmExtr (CSeqFileStage) - write final file
#     File path => #$FilePath#/key/BCBSKCCommClmExtr_#SrcSysCd#.DrugClm.dat.#RunID#
#     That becomes f"{adls_path}/key/BCBSKCCommClmExtr_{SrcSysCd}.DrugClm.dat.{RunID}"

write_files(
    df_ClmPK,
    f"{adls_path}/key/BCBSKCCommClmExtr_{SrcSysCd}.DrugClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)