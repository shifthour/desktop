# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  ESIDrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the ESIDrugFile.dat created in ESIClmLand  job and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #      Change Description                                                                    Development Project    Code Reviewer          Date Reviewed       
# MAGIC ----------------------------------------  --------------------     ------------------------      -----------------------------------------------------------------------------------------          ----------------------------------   -------------------------------   ----------------------------       
# MAGIC Parikshith Chada               2008-06-26       3784                      Original Programming                                                                  devlIDSnew                  Steph Goddard          10/27/2008
# MAGIC                                                                                                  Updated with new primary key process
# MAGIC Tracy Davis                       2009-04-09       TTR 477                 New Field CLM_TRANSMITTAL_METH                                  devlIDS                         Steph Goddard          04/14/2009
# MAGIC Madhav Mynampati           2009-05-05       TTR 519                 Changed SQL for the IDS extract                                              devlIDS                         Steph Goddard          05/20/2009
# MAGIC                                                                                                  and Edited logic 
# MAGIC Ralph Tucker                    2009-08-17        15 Prod Suprt         Added field: REMIT_SUPRSION_AMT                                     devlIIDS                        Steph Goddard          08/21/2009
# MAGIC Kalyan Neelam                  2010-02-09        4278                       Added two new fields - MCAID_STTUS_ID,                  
# MAGIC                                                                                                   PATN_PD_AMT                                                                         IntegrateCurDevl           Steph Goddard         03/08/2010
# MAGIC Karthik Chintalapani          2011-11-14       TTR-1063                The Fkeycode lookup for Claim Status Code SK and Claim       IntegrateNewDevl         Brent Leland             11/22/2011
# MAGIC                                                                                                   Category Code Sk use source system code of FACETS. 
# MAGIC                                                                                                   Changed these lookups to use ESI.
# MAGIC Rick Henry                       2012-05-03        4896                       Default CLM_SUBMT_ICD_VRSN_CD to 'NA'                            NewDevl                      SAndrew                   2012-05-20
# MAGIC Raja Gummadi                  2013-09-18        TFS - 2618              Changed logic for CNSD_CHRG_AMT and CHRG_AMT           IntegrateNewDevl        Kalyan Neelam          2013-09-27
# MAGIC SAndrew                           2014-04-01         ESI F14                                                                                                                     IntegrateNewDevl         
# MAGIC                                                                                                  Changed the field mapped to COPAY_AMT from 
# MAGIC                                                                                                  ESI.COPPAY_FLAT_COPAY to ESI.COPAY_AMT
# MAGIC                                                                                                  changed the defualt for the dates from NA to 1753-01-01
# MAGIC                                                                                                  INPT_DT,
# MAGIC                                                                                                  MBR_PLN_ELIG_DT
# MAGIC                                                                                                  NEXT_RVW_DT
# MAGIC                                                                                                  PD_DT
# MAGIC                                                                                                  PAYMT_DRAG_CYC_DT
# MAGIC                                                                                                  SMLR_ILNS_DT
# MAGIC                                                                                                  WORK_UNABLE_BEG_DT
# MAGIC                                                                                                  WORK_UNABLE_END_DT
# MAGIC                                                                                                  Changed Amount Disallowed from 0.00 to value sent in from ESI.
# MAGIC                                                                                                  Offset (675 - 683) 
# MAGIC                                                                                                  Changed Payable Amount from 0.00 to ESI.AmtBilled
# MAGIC Shashank Akinapalli        2019-04-10        97615                     Adding CLM_LN_VBB_IN & adjusting the Filler length.                  IntegrateDev2             Hugh Sisson              2019-05-02
# MAGIC 
# MAGIC Mohan Karnati                 2019-06-06        ADO-73034             Adding CLM_TXNMY_CD filed in alpha_pfx stage and                 IntegrateDev1	Kalyan Neelam           2019-07-01
# MAGIC                                                                                                    passing it till ESIClmExtr stage
# MAGIC 
# MAGIC 
# MAGIC Giri  Mallavaram    2020-04-06         PBM Changes       Added BILL_PAYMT_EXCL_IN to be in sync with changes to CLM_PKey Container        IntegrateDev2              Kalyan Neelam

# MAGIC Read the ESI file created from ESIClmLand
# MAGIC Writing Sequential File to /key
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
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    substring,
    rpad,
    length,
    concat
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunCycle = get_widget_value('RunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Get DB connection details for IDS
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

#----------------------------------------------------------------------------------------------------------
# 1) Stage "ids" (DB2Connector): 3 SQL queries from IDS database
#----------------------------------------------------------------------------------------------------------

extract_query_mbr = f"""
SELECT MBR.MBR_UNIQ_KEY as MBR_UNIQ_KEY,
       SUB.SUB_UNIQ_KEY as SUB_UNIQ_KEY
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.W_DRUG_ENR DRUG
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK
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
     {IDSOwner}.MBR_ENR      MBR,
     {IDSOwner}.CD_MPPNG     MAP1,
     {IDSOwner}.CLS          CLS,
     {IDSOwner}.SUBGRP       SUBGRP,
     {IDSOwner}.CLS_PLN      PLN,
     {IDSOwner}.CD_MPPNG     MAP2,
     {IDSOwner}.PROD_CMPNT   CMPNT,
     {IDSOwner}.PROD_BILL_CMPNT  BILL_CMPNT,
     {IDSOwner}.EXPRNC_CAT   CAT,
     {IDSOwner}.FNCL_LOB     LOB
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND DRUG.FILL_DT_SK BETWEEN MBR.EFF_DT_SK AND MBR.TERM_DT_SK
  AND MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
  AND MAP1.TRGT_CD = 'MED'
  AND MBR.CLS_SK = CLS.CLS_SK
  AND MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
  AND MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
  AND MBR.PROD_SK = CMPNT.PROD_SK
  AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
  AND MAP2.TRGT_CD = 'PDBL'
  AND DRUG.FILL_DT_SK BETWEEN CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
  AND CMPNT.PROD_CMPNT_EFF_DT_SK = (
      SELECT MAX(CMPNT2.PROD_CMPNT_EFF_DT_SK)
      FROM {IDSOwner}.PROD_CMPNT CMPNT2
      WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
        AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
        AND DRUG.FILL_DT_SK BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK
  )
  AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
  AND DRUG.FILL_DT_SK BETWEEN BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
  AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
  AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK = (
      SELECT MAX(BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK)
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

#----------------------------------------------------------------------------------------------------------
# 2) Stage "Hash" (CHashedFileStage) - Scenario A (intermediate hashed file)
#   We replace with deduplicate logic on PK columns, then pass along in memory
#----------------------------------------------------------------------------------------------------------

# Deduplicate function is already defined as dedup_sort(df, partition_cols, sort_cols)
# PKs from the "mbr_lkup" link => MBR_UNIQ_KEY
df_mbr_lkup = dedup_sort(df_mbr, ["MBR_UNIQ_KEY"], [])
# PK from "sub_alpha_pfx_lkup" => SUB_UNIQ_KEY
df_sub_alpha_pfx_lkup = dedup_sort(df_sub_alpha_pfx, ["SUB_UNIQ_KEY"], [])
# PK from "mbr_enr_lkup" => CLM_ID
df_mbr_enr_lkup = dedup_sort(df_mbr_enroll, ["CLM_ID"], [])

#----------------------------------------------------------------------------------------------------------
# 3) Stage "ESIClmLand" (CSeqFileStage) - reading ESIDrugClmDaily_Land.dat.#RunID# (no "landing"/"external" in path => use adls_path)
#----------------------------------------------------------------------------------------------------------

schema_esi_land = StructType([
    StructField("RCRD_ID", DecimalType(38,10), False),
    StructField("CLAIM_ID", StringType(), False),
    StructField("PRCSR_NO", DecimalType(38,10), False),
    StructField("MEM_CK_KEY", IntegerType(), False),
    StructField("BTCH_NO", DecimalType(38,10), False),
    StructField("PDX_NO", StringType(), False),
    StructField("RX_NO", DecimalType(38,10), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("NDC_NO", DecimalType(38,10), False),
    StructField("DRUG_DESC", StringType(), False),
    StructField("NEW_RFL_CD", DecimalType(38,10), False),
    StructField("METRIC_QTY", DecimalType(38,10), False),
    StructField("DAYS_SUPL", DecimalType(38,10), False),
    StructField("BSS_OF_CST_DTRM", StringType(), False),
    StructField("INGR_CST", DecimalType(38,10), False),
    StructField("DISPNS_FEE", DecimalType(38,10), False),
    StructField("COPAY_AMT", DecimalType(38,10), False),
    StructField("SLS_TAX", DecimalType(38,10), False),
    StructField("AMT_BILL", DecimalType(38,10), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("DOB", StringType(), False),
    StructField("SEX_CD", DecimalType(38,10), False),
    StructField("CARDHLDR_ID_NO", StringType(), False),
    StructField("RELSHP_CD", DecimalType(38,10), False),
    StructField("GRP_NO", StringType(), False),
    StructField("HOME_PLN", StringType(), False),
    StructField("HOST_PLN", DecimalType(38,10), False),
    StructField("PRESCRIBER_ID", StringType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("CARDHLDR_FIRST_NM", StringType(), False),
    StructField("CARDHLDR_LAST_NM", StringType(), False),
    StructField("PRAUTH_NO", DecimalType(38,10), False),
    StructField("PA_MC_SC_NO", StringType(), False),
    StructField("CUST_LOC", DecimalType(38,10), False),
    StructField("RESUB_CYC_CT", DecimalType(38,10), False),
    StructField("DT_RX_WRTN", StringType(), False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), False),
    StructField("PRSN_CD", StringType(), False),
    StructField("OTHR_COV_CD", DecimalType(38,10), False),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38,10), False),
    StructField("CMPND_CD", DecimalType(38,10), False),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38,10), False),
    StructField("LVL_OF_SVC", DecimalType(38,10), False),
    StructField("RX_ORIG_CD", DecimalType(38,10), False),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38,10), False),
    StructField("PRI_PRESCRIBER", StringType(), False),
    StructField("CLNC_ID_NO", DecimalType(38,10), False),
    StructField("DRUG_TYP", DecimalType(38,10), False),
    StructField("PRESCRIBER_LAST_NM", StringType(), False),
    StructField("POSTAGE_AMT_CLMED", DecimalType(38,10), False),
    StructField("UNIT_DOSE_IN", DecimalType(38,10), False),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38,10), False),
    StructField("FULL_AWP", DecimalType(38,10), False),
    StructField("EXPNSN_AREA", StringType(), False),
    StructField("MSTR_CAR", StringType(), False),
    StructField("SUB_CAR", StringType(), False),
    StructField("CLM_TYP", StringType(), False),
    StructField("ESI_SUB_GRP", StringType(), False),
    StructField("PLN_DSGNR", StringType(), False),
    StructField("ADJDCT_DT", StringType(), False),
    StructField("ADMIN_FEE", DecimalType(38,10), False),
    StructField("CAP_AMT", DecimalType(38,10), False),
    StructField("INGR_CST_SUB", DecimalType(38,10), False),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38,10), False),
    StructField("MBR_PAY_CD", StringType(), False),
    StructField("INCNTV_FEE", DecimalType(38,10), False),
    StructField("CLM_ADJ_AMT", DecimalType(38,10), False),
    StructField("CLM_ADJ_CD", StringType(), False),
    StructField("FRMLRY_FLAG", StringType(), False),
    StructField("GNRC_CLS_NO", StringType(), False),
    StructField("THRPTC_CLS_AHFS", StringType(), False),
    StructField("PDX_TYP", StringType(), False),
    StructField("BILL_BSS_CD", StringType(), False),
    StructField("USL_AND_CUST_CHRG", DecimalType(38,10), False),
    StructField("PD_DT", StringType(), False),
    StructField("BNF_CD", StringType(), False),
    StructField("DRUG_STRG", StringType(), False),
    StructField("ORIG_MBR", StringType(), False),
    StructField("DT_OF_INJURY", StringType(), False),
    StructField("FEE_AMT", DecimalType(38,10), False),
    StructField("ESI_REF_NO", StringType(), False),
    StructField("CLNT_CUST_ID", StringType(), False),
    StructField("PLN_TYP", StringType(), False),
    StructField("ESI_ADJDCT_REF_NO", DecimalType(38,10), False),
    StructField("ESI_ANCLRY_AMT", DecimalType(38,10), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PAID_DATE", StringType(), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("ESI_BILL_DT", StringType(), False),
    StructField("FSA_VNDR_CD", StringType(), False),
    StructField("PICA_DRUG_CD", StringType(), False),
    StructField("AMT_CLMED", DecimalType(38,10), False),
    StructField("AMT_DSALW", DecimalType(38,10), False),
    StructField("FED_DRUG_CLS_CD", StringType(), False),
    StructField("DEDCT_AMT", DecimalType(38,10), False),
    StructField("BNF_COPAY_100", StringType(), False),
    StructField("CLM_PRCS_TYP", StringType(), False),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38,10), False),
    StructField("FLR", StringType(), False),
    StructField("MCARE_D_COV_DRUG", StringType(), False),
    StructField("RETRO_LICS_CD", StringType(), False),
    StructField("RETRO_LICS_AMT", DecimalType(38,10), False),
    StructField("LICS_SBSDY_AMT", DecimalType(38,10), False),
    StructField("MED_B_DRUG", StringType(), False),
    StructField("MED_B_CLM", StringType(), False),
    StructField("PRESCRIBER_QLFR", StringType(), False),
    StructField("PRESCRIBER_ID_NPI", StringType(), False),
    StructField("PDX_QLFR", StringType(), False),
    StructField("PDX_ID_NPI", StringType(), False),
    StructField("HRA_APLD_AMT", DecimalType(38,10), False),
    StructField("ESI_THER_CLS", DecimalType(38,10), False),
    StructField("HIC_NO", StringType(), False),
    StructField("HRA_FLAG", StringType(), False),
    StructField("DOSE_CD", DecimalType(38,10), False),
    StructField("LOW_INCM", StringType(), False),
    StructField("RTE_OF_ADMIN", StringType(), False),
    StructField("DEA_SCHD", DecimalType(38,10), False),
    StructField("COPAY_BNF_OPT", DecimalType(38,10), False),
    StructField("GNRC_PROD_IN_GPI", DecimalType(38,10), False),
    StructField("PRESCRIBER_SPEC", StringType(), False),
    StructField("VAL_CD", StringType(), False),
    StructField("PRI_CARE_PDX", StringType(), False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), False),
    StructField("FLR3", StringType(), False),
    StructField("PSL_FMLY_MET_AMT", DecimalType(38,10), False),
    StructField("PSL_MBR_MET_AMT", DecimalType(38,10), False),
    StructField("PSL_FMLY_AMT", DecimalType(38,10), False),
    StructField("DED_FMLY_MET_AMT", DecimalType(38,10), False),
    StructField("DED_FMLY_AMT", DecimalType(38,10), False),
    StructField("MOPS_FMLY_AMT", DecimalType(38,10), False),
    StructField("MOPS_FMLY_MET_AMT", DecimalType(38,10), False),
    StructField("MOPS_MBR_MET_AMT", DecimalType(38,10), False),
    StructField("DED_MBR_MET_AMT", DecimalType(38,10), False),
    StructField("PSL_APLD_AMT", DecimalType(38,10), False),
    StructField("MOPS_APLD_AMT", DecimalType(38,10), False),
    StructField("PAR_PDX_IND", StringType(), False),
    StructField("COPAY_PCT_AMT", DecimalType(38,10), False),
    StructField("COPAY_FLAT_AMT", DecimalType(38,10), False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), False),
    StructField("PRESCRIPTION_NBR_2", StringType(), False),
    StructField("TRANSACTION_ID", StringType(), False),
    StructField("CROSS_REF_ID", StringType(), False),
    StructField("ADJDCT_TIMESTAMP", StringType(), False),
    StructField("CLM_LN_VBB_IN", StringType(), False),
    StructField("FLR4", StringType(), False)
])

df_esi = (
    spark.read.format("csv")
    .schema(schema_esi_land)
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .load(f"{adls_path}/verified/ESIDrugClmDaily_Land.dat.{RunID}")
)

#----------------------------------------------------------------------------------------------------------
# 4) Stage "BusinessRules" (CTransformerStage)
#    Left join: Esi (primary) and mbr_lkup (lookup) on Esi.MEM_CK_KEY = mbr_lkup.MBR_UNIQ_KEY
#    Then create output pin "EsiClmTrns" with many columns
#----------------------------------------------------------------------------------------------------------

df_br_join = df_esi.alias("Esi").join(
    df_mbr_lkup.alias("mbr_lkup"),
    on=(col("Esi.MEM_CK_KEY") == col("mbr_lkup.MBR_UNIQ_KEY")),
    how="left"
)

# Stage variables logic:

# svAdjustedClaim = (Trim(Esi.CLM_TYP[1, 1]) = 'R') => boolean
df_br_join = df_br_join.withColumn(
    "svAdjustedClaim",
    when( trim(substring(col("Esi.CLM_TYP"), 1, 1)) == lit("R"), lit(True)).otherwise(lit(False))
)

# svSubCk = If IsNull(mbr_lkup.MBR_UNIQ_KEY) => 0 else mbr_lkup.SUB_UNIQ_KEY
df_br_join = df_br_join.withColumn(
    "svSubCk",
    when(col("mbr_lkup.MBR_UNIQ_KEY").isNull(), lit(0)).otherwise(col("mbr_lkup.SUB_UNIQ_KEY"))
)

# svMbrAge = AGE(Esi.DOB, Esi.DT_FILLED)  (assume user-defined function AGE is available)
df_br_join = df_br_join.withColumn("svMbrAge", AGE(col("Esi.DOB"), col("Esi.DT_FILLED")))

# svAdjustingFromCLmID = If trim(Esi.CROSS_REF_ID) = "000000000000000000" then "NA" else Esi.CROSS_REF_ID
df_br_join = df_br_join.withColumn(
    "svAdjustingFromCLmID",
    when(trim(col("Esi.CROSS_REF_ID")) == lit("000000000000000000"), lit("NA")).otherwise(col("Esi.CROSS_REF_ID"))
)

# svAllowedAmt = Esi.INGR_CST + Esi.DISPNS_FEE + Esi.SLS_TAX
df_br_join = df_br_join.withColumn(
    "svAllowedAmt",
    col("Esi.INGR_CST") + col("Esi.DISPNS_FEE") + col("Esi.SLS_TAX")
)

# svPreChrgAmt = If IsNull(Trim(Esi.FULL_AWP)) Or Len(Trim(Esi.FULL_AWP))=0 => 0 
#                Else If Esi.FULL_AWP=0 => Esi.USL_AND_CUST_CHRG+Esi.DISPNS_FEE 
#                     Else Esi.FULL_AWP+Esi.DISPNS_FEE
df_br_join = df_br_join.withColumn(
    "svPreChrgAmt",
    when(
        (col("Esi.FULL_AWP").isNull()) | (length(trim(col("Esi.FULL_AWP"))) == lit(0)),
        lit(0)
    ).otherwise(
        when(col("Esi.FULL_AWP") == lit(0),
             col("Esi.USL_AND_CUST_CHRG") + col("Esi.DISPNS_FEE")
        ).otherwise(col("Esi.FULL_AWP") + col("Esi.DISPNS_FEE"))
    )
)

# svCnsdChrgAmt = If Esi.CLM_TYP='P' And svPreChrgAmt<0 => 0
#                 Else If Esi.CLM_TYP='R' And svPreChrgAmt>0 => svPreChrgAmt*-1
#                 Else svPreChrgAmt
df_br_join = df_br_join.withColumn(
    "svCnsdChrgAmt",
    when(
        (col("Esi.CLM_TYP") == lit("P")) & (col("svPreChrgAmt") < lit(0)),
        lit(0)
    ).when(
        (col("Esi.CLM_TYP") == lit("R")) & (col("svPreChrgAmt") > lit(0)),
        col("svPreChrgAmt") * lit(-1)
    ).otherwise(col("svPreChrgAmt"))
)

# svAmt = svCnsdChrgAmt
df_br_join = df_br_join.withColumn("svAmt", col("svCnsdChrgAmt"))

# svClmCnt = If svAdjustedClaim then -1 else 1
df_br_join = df_br_join.withColumn(
    "svClmCnt",
    when(col("svAdjustedClaim"), lit(-1)).otherwise(lit(1))
)

# Now build output "EsiClmTrns" columns in correct order
df_EsiClmTrns = df_br_join.select(
    # LinkName "EsiClmTrns" columns in the order specified:
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"),10," ").alias("INSRT_UPDT_CD"),  # char(10)
    rpad(lit("N"),1," ").alias("DISCARD_IN"),      # char(1)
    rpad(lit("Y"),1," ").alias("PASS_THRU_IN"),    # char(1)
    current_date().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    col("SrcSysCd").alias("SRC_SYS_CD"),  # expression = SrcSysCd param
    concat(lit(SrcSysCd),lit(";"),col("Esi.CLAIM_ID")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_SK"),
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    rpad(col("Esi.CLAIM_ID"),18," ").alias("CLM_ID"),  # char(18)
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("svAdjustingFromCLmID"),12," ").alias("ADJ_FROM_CLM"),  # char(12)
    rpad(lit("NA"),12," ").alias("ADJ_TO_CLM"),     # char(12)
    rpad(lit("NA"),3," ").alias("ALPHA_PFX_CD"),    # char(3)
    rpad(lit("NA"),3," ").alias("CLM_EOB_EXCD"),    # char(3)
    rpad(lit("NA"),4," ").alias("CLS"),             # char(4)
    rpad(lit("NA"),8," ").alias("CLS_PLN"),         # char(8)
    rpad(lit("NA"),4," ").alias("EXPRNC_CAT"),      # char(4)
    rpad(lit("NA"),4," ").alias("FNCL_LOB_NO"),     # char(4)
    rpad(col("Esi.GRP_ID"),8," ").alias("GRP"),     # char(8)
    col("Esi.MEM_CK_KEY").alias("MBR_CK"),
    rpad(lit("NA"),12," ").alias("NTWK"),           # char(12)
    rpad(col("Esi.GRP_NO"),8," ").alias("PROD"),    # char(8)
    rpad(lit("NA"),4," ").alias("SUBGRP"),          # char(4)
    col("svSubCk").alias("SUB_CK"),
    rpad(lit("NA"),10," ").alias("CLM_ACDNT_CD"),   # char(10)
    rpad(lit("NA"),2," ").alias("CLM_ACDNT_ST_CD"), # char(2)
    rpad(lit("NA"),10," ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),  # char(10)
    rpad(lit("NA"),10," ").alias("CLM_AGMNT_SRC_CD"),            # char(10)
    rpad(lit("NA"),1," ").alias("CLM_BTCH_ACTN_CD"),             # char(1)
    rpad(lit("N"),10," ").alias("CLM_CAP_CD"),                   # char(10) => 'N'
    rpad(lit("STD"),10," ").alias("CLM_CAT_CD"),                 # char(10) => 'STD'
    rpad(lit("NA"),1," ").alias("CLM_CHK_CYC_OVRD_CD"),          # char(1)
    rpad(lit("NA"),2," ").alias("CLM_COB_CD"),                   # char(2) but the link had length=2 or 1? The JSON says length=2. We'll keep 2.
    lit("ACPTD").alias("FINL_DISP_CD"),
    rpad(lit("NA"),1," ").alias("CLM_INPT_METH_CD"),             # char(1)
    rpad(col("Esi.CLM_TRANSMITTAL_METH"),10," ").alias("CLM_INPT_SRC_CD"),
    rpad(lit("NA"),10," ").alias("CLM_IPP_CD"),                  # char(10)
    when(
        col("Esi.PAR_PDX_IND").isNull() | (length(col("Esi.PAR_PDX_IND"))==0),
        lit("UNK")
    ).otherwise(col("Esi.PAR_PDX_IND")).alias("CLM_NTWK_STTUS_CD"),  # char(2)
    rpad(lit("NA"),4," ").alias("CLM_NONPAR_PROV_PFX_CD"),       # char(4)
    rpad(lit("NA"),1," ").alias("CLM_OTHER_BNF_CD"),             # char(1)
    rpad(lit("NA"),1," ").alias("CLM_PAYE_CD"),                  # char(1)
    rpad(lit("NA"),4," ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),     # char(4)
    rpad(lit("NA"),4," ").alias("CLM_SVC_DEFN_PFX_CD"),          # char(4)
    rpad(lit("NA"),10," ").alias("CLM_SVC_PROV_SPEC_CD"),        # char(10)
    rpad(lit("0012"),10," ").alias("CLM_SVC_PROV_TYP_CD"),       # char(10)
    substring(col("Esi.CLM_TYP"),1,1).alias("CLM_STTUS_CD"),     # char(2) in JSON, but we only have 1 char. We'll allocate a 2-length rpad if needed below.
    rpad(lit("NA"),10," ").alias("CLM_SUBMT_BCBS_PLN_CD"),       # char(10)
    rpad(lit("NA"),10," ").alias("CLM_SUB_BCBS_PLN_CD"),         # char(10)
    rpad(lit("RX"),10," ").alias("CLM_SUBTYP_CD"),               # char(10)
    rpad(lit("MED"),1," ").alias("CLM_TYP_CD"),                  # char(1)
    rpad(lit("X"),1," ").alias("ATCHMT_IN"),                     # char(1)
    rpad(lit("X"),1," ").alias("CLM_CLNCL_EDIT_CD"),             # char(1)
    rpad(lit("X"),1," ").alias("COBRA_CLM_IN"),                  # char(1)
    rpad(lit("X"),1," ").alias("FIRST_PASS_IN"),                 # char(1)
    rpad(lit("N"),1," ").alias("HOST_IN"),                       # char(1)
    rpad(lit("X"),1," ").alias("LTR_IN"),                        # char(1)
    rpad(lit("X"),1," ").alias("MCARE_ASG_IN"),                  # char(1)
    rpad(lit("X"),1," ").alias("NOTE_IN"),                       # char(1)
    rpad(lit("X"),1," ").alias("PCA_AUDIT_IN"),                  # char(1)
    rpad(lit("X"),1," ").alias("PCP_SUBMT_IN"),                  # char(1)
    rpad(lit("X"),1," ").alias("PROD_OOA_IN"),                   # char(1)
    rpad(lit("NA"),10," ").alias("ACDNT_DT"),                    # char(10)
    rpad(lit("1753-01-01"),10," ").alias("INPT_DT"),             # char(10)
    rpad(lit("1753-01-01"),10," ").alias("MBR_PLN_ELIG_DT"),     # char(10)
    rpad(lit("1753-01-01"),10," ").alias("NEXT_RVW_DT"),         # char(10)
    rpad(lit("1753-01-01"),10," ").alias("PD_DT"),               # char(10)
    rpad(lit("1753-01-01"),10," ").alias("PAYMT_DRAG_CYC_DT"),   # char(10)
    rpad(col("Esi.ADJDCT_DT"),10," ").alias("PRCS_DT"),          # char(10)
    rpad(col("Esi.ADJDCT_DT"),10," ").alias("RCVD_DT"),          # char(10)
    rpad(col("Esi.DT_FILLED"),10," ").alias("SVC_STRT_DT"),      # char(10)
    rpad(col("Esi.DT_FILLED"),10," ").alias("SVC_END_DT"),       # char(10)
    rpad(lit("1753-01-01"),10," ").alias("SMLR_ILNS_DT"),        # char(10)
    current_date().alias("STTUS_DT"),
    rpad(lit("1753-01-01"),10," ").alias("WORK_UNABLE_BEG_DT"),  # char(10)
    rpad(lit("1753-01-01"),10," ").alias("WORK_UNABLE_END_DT"),  # char(10)
    lit(0.0).alias("ACDNT_AMT"),
    lit(0.0).alias("ACTL_PD_AMT"),
    col("svAllowedAmt").alias("ALLOW_AMT"),
    lit(0.0).alias("DSALW_AMT"),
    col("Esi.COPAY_PCT_AMT").alias("COINS_AMT"),
    col("svAmt").alias("CNSD_CHRG_AMT"),
    col("Esi.COPAY_AMT").alias("COPAY_AMT"),
    col("svAmt").alias("CHRG_AMT"),
    col("Esi.DEDCT_AMT").alias("DEDCT_AMT"),
    col("Esi.AMT_BILL").alias("PAYBL_AMT"),
    col("svClmCnt").alias("CLM_CT"),
    col("svMbrAge").alias("MBR_AGE"),
    col("svAdjustingFromCLmID").alias("ADJ_FROM_CLM_ID"),
    lit("NA").alias("ADJ_TO_CLM_ID"),
    rpad(lit("NA"),18," ").alias("DOC_TX_ID"),           # char(18)
    lit(None).alias("MCAID_RESUB_NO"),                   # char(15) => @NULL
    rpad(lit("NA"),12," ").alias("MCARE_ID"),            # char(12)
    trim(col("Esi.PRSN_CD")).alias("MBR_SFX_NO"),
    lit(None).alias("PATN_ACCT_NO"),
    col("Esi.ADJDCT_TIMESTAMP").alias("PAYMT_REF_ID"),   # char(16) => but ADJDCT_TIMESTAMP is char(11). We'll keep as is in the select.
    rpad(lit("NA"),12," ").alias("PROV_AGMNT_ID"),       # char(12)
    lit(None).alias("RFRNG_PROV_TX"),
    rpad(col("Esi.CARDHLDR_ID_NO"),14," ").alias("SUB_ID"),  # char(14)
    lit(0.00).alias("REMIT_SUPRSION_AMT"),
    lit("NA").alias("MCAID_STTUS_ID"),
    lit(0.00).alias("PATN_PD_AMT"),
    lit("NA").alias("CLM_SUBMT_ICD_VRSN_CD")
)

#----------------------------------------------------------------------------------------------------------
# 5) Stage "alpha_pfx" (CTransformerStage)
#    InputPins: EsiClmTrns (primary), sub_alpha_pfx_lkup (left join on SUB_CK=SUB_UNIQ_KEY), mbr_enr_lkup (left join on CLM_ID=CLM_ID)
#----------------------------------------------------------------------------------------------------------

df_alpha_join1 = df_EsiClmTrns.alias("EsiClmTrns").join(
    df_sub_alpha_pfx_lkup.alias("sub_alpha_pfx_lkup"),
    on=(col("EsiClmTrns.SUB_CK") == col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY")),
    how="left"
)

df_alpha_join2 = df_alpha_join1.join(
    df_mbr_enr_lkup.alias("mbr_enr_lkup"),
    on=(col("EsiClmTrns.CLM_ID") == col("mbr_enr_lkup.CLM_ID")),
    how="left"
)

# Output pin "Transform" from alpha_pfx stage
df_Transform = df_alpha_join2.select(
    # columns in order:
    col("EsiClmTrns.JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("EsiClmTrns.INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(col("EsiClmTrns.DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(col("EsiClmTrns.PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    col("EsiClmTrns.FIRST_RECYC_DT"),
    col("EsiClmTrns.ERR_CT"),
    col("EsiClmTrns.RECYCLE_CT"),
    col("EsiClmTrns.SRC_SYS_CD"),
    col("EsiClmTrns.PRI_KEY_STRING"),
    col("EsiClmTrns.CLM_SK"),
    col("EsiClmTrns.SRC_SYS_CD_SK"),
    rpad(col("EsiClmTrns.CLM_ID"),18," ").alias("CLM_ID"),
    col("EsiClmTrns.CRT_RUN_CYC_EXCTN_SK"),
    col("EsiClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("EsiClmTrns.ADJ_FROM_CLM"),12," ").alias("ADJ_FROM_CLM"),
    rpad(col("EsiClmTrns.ADJ_TO_CLM"),12," ").alias("ADJ_TO_CLM"),
    rpad(
        when(col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(), lit("UNK"))
        .otherwise(col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")),3," "
    ).alias("ALPHA_PFX_CD"),
    rpad(col("EsiClmTrns.CLM_EOB_EXCD"),3," ").alias("CLM_EOB_EXCD"),
    rpad(
        when(col("mbr_enr_lkup.CLM_ID").isNull(), lit("UNK"))
        .otherwise(col("mbr_enr_lkup.CLS_ID")),4," "
    ).alias("CLS"),
    rpad(
        when(col("mbr_enr_lkup.CLM_ID").isNull(), lit("UNK"))
        .otherwise(col("mbr_enr_lkup.CLS_PLN_ID")),8," "
    ).alias("CLS_PLN"),
    rpad(
        when(col("mbr_enr_lkup.CLM_ID").isNull(), lit("UNK"))
        .otherwise(col("mbr_enr_lkup.EXPRNC_CAT_CD")),4," "
    ).alias("EXPRNC_CAT"),
    rpad(
        when(col("mbr_enr_lkup.CLM_ID").isNull(), lit("UNK"))
        .otherwise(col("mbr_enr_lkup.FNCL_LOB_CD")),4," "
    ).alias("FNCL_LOB_NO"),
    rpad(col("EsiClmTrns.GRP"),8," ").alias("GRP"),
    col("EsiClmTrns.MBR_CK"),
    rpad(col("EsiClmTrns.NTWK"),12," ").alias("NTWK"),
    rpad(
        when(
            (col("EsiClmTrns.PROD") == lit("HMS00000")),
            col("mbr_enr_lkup.PROD_ID")
        ).otherwise(col("EsiClmTrns.PROD")),8," "
    ).alias("PROD"),
    rpad(
        when(col("mbr_enr_lkup.CLM_ID").isNull(), lit("UNK"))
        .otherwise(col("mbr_enr_lkup.SUBGRP_ID")),4," "
    ).alias("SUBGRP"),
    col("EsiClmTrns.SUB_CK"),
    rpad(col("EsiClmTrns.CLM_ACDNT_CD"),10," ").alias("CLM_ACDNT_CD"),
    rpad(col("EsiClmTrns.CLM_ACDNT_ST_CD"),2," ").alias("CLM_ACDNT_ST_CD"),
    rpad(col("EsiClmTrns.CLM_ACTIVATING_BCBS_PLN_CD"),10," ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    rpad(col("EsiClmTrns.CLM_AGMNT_SRC_CD"),10," ").alias("CLM_AGMNT_SRC_CD"),
    rpad(col("EsiClmTrns.CLM_BTCH_ACTN_CD"),1," ").alias("CLM_BTCH_ACTN_CD"),
    rpad(col("EsiClmTrns.CLM_CAP_CD"),10," ").alias("CLM_CAP_CD"),
    rpad(col("EsiClmTrns.CLM_CAT_CD"),10," ").alias("CLM_CAT_CD"),
    rpad(col("EsiClmTrns.CLM_CHK_CYC_OVRD_CD"),1," ").alias("CLM_CHK_CYC_OVRD_CD"),
    rpad(col("EsiClmTrns.CLM_COB_CD"),1," ").alias("CLM_COB_CD"),  # in original job, length 2 or 1? We'll keep 1 here matching the select
    col("EsiClmTrns.FINL_DISP_CD"),
    rpad(col("EsiClmTrns.CLM_INPT_METH_CD"),1," ").alias("CLM_INPT_METH_CD"),
    rpad(col("EsiClmTrns.CLM_INPT_SRC_CD"),10," ").alias("CLM_INPT_SRC_CD"),
    rpad(col("EsiClmTrns.CLM_IPP_CD"),10," ").alias("CLM_IPP_CD"),
    rpad(col("EsiClmTrns.CLM_NTWK_STTUS_CD"),2," ").alias("CLM_NTWK_STTUS_CD"),
    rpad(col("EsiClmTrns.CLM_NONPAR_PROV_PFX_CD"),4," ").alias("CLM_NONPAR_PROV_PFX_CD"),
    rpad(col("EsiClmTrns.CLM_OTHER_BNF_CD"),1," ").alias("CLM_OTHER_BNF_CD"),
    rpad(col("EsiClmTrns.CLM_PAYE_CD"),1," ").alias("CLM_PAYE_CD"),
    rpad(col("EsiClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD"),4," ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    rpad(col("EsiClmTrns.CLM_SVC_DEFN_PFX_CD"),4," ").alias("CLM_SVC_DEFN_PFX_CD"),
    rpad(col("EsiClmTrns.CLM_SVC_PROV_SPEC_CD"),10," ").alias("CLM_SVC_PROV_SPEC_CD"),
    rpad(col("EsiClmTrns.CLM_SVC_PROV_TYP_CD"),10," ").alias("CLM_SVC_PROV_TYP_CD"),
    rpad(col("EsiClmTrns.CLM_STTUS_CD"),2," ").alias("CLM_STTUS_CD"),
    rpad(col("EsiClmTrns.CLM_SUBMT_BCBS_PLN_CD"),10," ").alias("CLM_SUBMT_BCBS_PLN_CD"),
    rpad(col("EsiClmTrns.CLM_SUB_BCBS_PLN_CD"),10," ").alias("CLM_SUB_BCBS_PLN_CD"),
    rpad(col("EsiClmTrns.CLM_SUBTYP_CD"),10," ").alias("CLM_SUBTYP_CD"),
    rpad(col("EsiClmTrns.CLM_TYP_CD"),1," ").alias("CLM_TYP_CD"),
    rpad(col("EsiClmTrns.ATCHMT_IN"),1," ").alias("ATCHMT_IN"),
    rpad(col("EsiClmTrns.CLM_CLNCL_EDIT_CD"),1," ").alias("CLM_CLNCL_EDIT_CD"),
    rpad(col("EsiClmTrns.COBRA_CLM_IN"),1," ").alias("COBRA_CLM_IN"),
    rpad(col("EsiClmTrns.FIRST_PASS_IN"),1," ").alias("FIRST_PASS_IN"),
    rpad(col("EsiClmTrns.HOST_IN"),1," ").alias("HOST_IN"),
    rpad(col("EsiClmTrns.LTR_IN"),1," ").alias("LTR_IN"),
    rpad(col("EsiClmTrns.MCARE_ASG_IN"),1," ").alias("MCARE_ASG_IN"),
    rpad(col("EsiClmTrns.NOTE_IN"),1," ").alias("NOTE_IN"),
    rpad(col("EsiClmTrns.PCA_AUDIT_IN"),1," ").alias("PCA_AUDIT_IN"),
    rpad(col("EsiClmTrns.PCP_SUBMT_IN"),1," ").alias("PCP_SUBMT_IN"),
    rpad(col("EsiClmTrns.PROD_OOA_IN"),1," ").alias("PROD_OOA_IN"),
    rpad(col("EsiClmTrns.ACDNT_DT"),10," ").alias("ACDNT_DT"),
    rpad(col("EsiClmTrns.INPT_DT"),10," ").alias("INPT_DT"),
    rpad(col("EsiClmTrns.MBR_PLN_ELIG_DT"),10," ").alias("MBR_PLN_ELIG_DT"),
    rpad(col("EsiClmTrns.NEXT_RVW_DT"),10," ").alias("NEXT_RVW_DT"),
    rpad(col("EsiClmTrns.PD_DT"),10," ").alias("PD_DT"),
    rpad(col("EsiClmTrns.PAYMT_DRAG_CYC_DT"),10," ").alias("PAYMT_DRAG_CYC_DT"),
    rpad(col("EsiClmTrns.PRCS_DT"),10," ").alias("PRCS_DT"),
    rpad(col("EsiClmTrns.RCVD_DT"),10," ").alias("RCVD_DT"),
    rpad(col("EsiClmTrns.SVC_STRT_DT"),10," ").alias("SVC_STRT_DT"),
    rpad(col("EsiClmTrns.SVC_END_DT"),10," ").alias("SVC_END_DT"),
    rpad(col("EsiClmTrns.SMLR_ILNS_DT"),10," ").alias("SMLR_ILNS_DT"),
    rpad(col("EsiClmTrns.STTUS_DT"),10," ").alias("STTUS_DT"),
    rpad(col("EsiClmTrns.WORK_UNABLE_BEG_DT"),10," ").alias("WORK_UNABLE_BEG_DT"),
    rpad(col("EsiClmTrns.WORK_UNABLE_END_DT"),10," ").alias("WORK_UNABLE_END_DT"),
    col("EsiClmTrns.ACDNT_AMT"),
    col("EsiClmTrns.ACTL_PD_AMT"),
    col("EsiClmTrns.ALLOW_AMT"),
    col("EsiClmTrns.DSALW_AMT"),
    col("EsiClmTrns.COINS_AMT"),
    col("EsiClmTrns.CNSD_CHRG_AMT"),
    col("EsiClmTrns.COPAY_AMT"),
    col("EsiClmTrns.CHRG_AMT"),
    col("EsiClmTrns.DEDCT_AMT"),
    col("EsiClmTrns.PAYBL_AMT"),
    col("EsiClmTrns.CLM_CT"),
    col("EsiClmTrns.MBR_AGE"),
    col("EsiClmTrns.ADJ_FROM_CLM_ID"),
    col("EsiClmTrns.ADJ_TO_CLM_ID"),
    rpad(col("EsiClmTrns.DOC_TX_ID"),18," ").alias("DOC_TX_ID"),
    rpad(col("EsiClmTrns.MCAID_RESUB_NO"),15," ").alias("MCAID_RESUB_NO"),
    rpad(col("EsiClmTrns.MCARE_ID"),12," ").alias("MCARE_ID"),
    rpad(col("EsiClmTrns.MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    col("EsiClmTrns.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    rpad(col("EsiClmTrns.PAYMT_REF_ID"),16," ").alias("PAYMT_REF_ID"),
    rpad(col("EsiClmTrns.PROV_AGMNT_ID"),12," ").alias("PROV_AGMNT_ID"),
    col("EsiClmTrns.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    rpad(col("EsiClmTrns.SUB_ID"),14," ").alias("SUB_ID"),
    lit(None).alias("PRPR_ENTITY"),  # char(1) => @NULL
    rpad(lit("NA"),18," ").alias("PCA_TYP_CD"),  # char(18)
    lit("NA").alias("REL_PCA_CLM_ID"),
    lit("NA").alias("CLCL_MICRO_ID"),
    rpad(lit("N"),1," ").alias("CLM_UPDT_SW"),
    col("EsiClmTrns.REMIT_SUPRSION_AMT"),
    col("EsiClmTrns.MCAID_STTUS_ID"),
    col("EsiClmTrns.PATN_PD_AMT"),
    col("EsiClmTrns.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    lit("").alias("CLM_TXNMY_CD")
)

#----------------------------------------------------------------------------------------------------------
# 6) Stage "Snapshot" (CTransformerStage)
#    Has 2 output pins: "Pkey" and "Snapshot"
#----------------------------------------------------------------------------------------------------------

df_Pkey = df_Transform.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"),10," ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"),1," ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"),1," ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"),18," ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("ADJ_FROM_CLM"),12," ").alias("ADJ_FROM_CLM"),
    rpad(col("ADJ_TO_CLM"),12," ").alias("ADJ_TO_CLM"),
    rpad(col("ALPHA_PFX_CD"),3," ").alias("ALPHA_PFX_CD"),
    rpad(col("CLM_EOB_EXCD"),3," ").alias("CLM_EOB_EXCD"),
    rpad(col("CLS"),4," ").alias("CLS"),
    rpad(col("CLS_PLN"),8," ").alias("CLS_PLN"),
    rpad(col("EXPRNC_CAT"),4," ").alias("EXPRNC_CAT"),
    rpad(col("FNCL_LOB_NO"),4," ").alias("FNCL_LOB_NO"),
    rpad(col("GRP"),8," ").alias("GRP"),
    col("MBR_CK"),
    rpad(col("NTWK"),12," ").alias("NTWK"),
    rpad(col("PROD"),8," ").alias("PROD"),
    rpad(col("SUBGRP"),4," ").alias("SUBGRP"),
    col("SUB_CK"),
    rpad(col("CLM_ACDNT_CD"),10," ").alias("CLM_ACDNT_CD"),
    rpad(col("CLM_ACDNT_ST_CD"),2," ").alias("CLM_ACDNT_ST_CD"),
    rpad(col("CLM_ACTIVATING_BCBS_PLN_CD"),10," ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    rpad(col("CLM_AGMNT_SRC_CD"),10," ").alias("CLM_AGMNT_SRC_CD"),
    rpad(col("CLM_BTCH_ACTN_CD"),1," ").alias("CLM_BTCH_ACTN_CD"),
    rpad(col("CLM_CAP_CD"),10," ").alias("CLM_CAP_CD"),
    rpad(col("CLM_CAT_CD"),10," ").alias("CLM_CAT_CD"),
    rpad(col("CLM_CHK_CYC_OVRD_CD"),1," ").alias("CLM_CHK_CYC_OVRD_CD"),
    rpad(col("CLM_COB_CD"),1," ").alias("CLM_COB_CD"),
    col("FINL_DISP_CD"),
    rpad(col("CLM_INPT_METH_CD"),1," ").alias("CLM_INPT_METH_CD"),
    rpad(col("CLM_INPT_SRC_CD"),10," ").alias("CLM_INPT_SRC_CD"),
    rpad(col("CLM_IPP_CD"),10," ").alias("CLM_IPP_CD"),
    rpad(col("CLM_NTWK_STTUS_CD"),2," ").alias("CLM_NTWK_STTUS_CD"),
    rpad(col("CLM_NONPAR_PROV_PFX_CD"),4," ").alias("CLM_NONPAR_PROV_PFX_CD"),
    rpad(col("CLM_OTHER_BNF_CD"),1," ").alias("CLM_OTHER_BNF_CD"),
    rpad(col("CLM_PAYE_CD"),1," ").alias("CLM_PAYE_CD"),
    rpad(col("CLM_PRCS_CTL_AGNT_PFX_CD"),4," ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    rpad(col("CLM_SVC_DEFN_PFX_CD"),4," ").alias("CLM_SVC_DEFN_PFX_CD"),
    rpad(col("CLM_SVC_PROV_SPEC_CD"),10," ").alias("CLM_SVC_PROV_SPEC_CD"),
    rpad(col("CLM_SVC_PROV_TYP_CD"),10," ").alias("CLM_SVC_PROV_TYP_CD"),
    rpad(col("CLM_STTUS_CD"),2," ").alias("CLM_STTUS_CD"),
    rpad(col("CLM_SUBMT_BCBS_PLN_CD"),10," ").alias("CLM_SUBMT_BCBS_PLN_CD"),
    rpad(col("CLM_SUB_BCBS_PLN_CD"),10," ").alias("CLM_SUB_BCBS_PLN_CD"),
    rpad(col("CLM_SUBTYP_CD"),10," ").alias("CLM_SUBTYP_CD"),
    rpad(col("CLM_TYP_CD"),1," ").alias("CLM_TYP_CD"),
    rpad(col("ATCHMT_IN"),1," ").alias("ATCHMT_IN"),
    rpad(col("CLM_CLNCL_EDIT_CD"),1," ").alias("CLM_CLNCL_EDIT_CD"),
    rpad(col("COBRA_CLM_IN"),1," ").alias("COBRA_CLM_IN"),
    rpad(col("FIRST_PASS_IN"),1," ").alias("FIRST_PASS_IN"),
    rpad(col("HOST_IN"),1," ").alias("HOST_IN"),
    rpad(col("LTR_IN"),1," ").alias("LTR_IN"),
    rpad(col("MCARE_ASG_IN"),1," ").alias("MCARE_ASG_IN"),
    rpad(col("NOTE_IN"),1," ").alias("NOTE_IN"),
    rpad(col("PCA_AUDIT_IN"),1," ").alias("PCA_AUDIT_IN"),
    rpad(col("PCP_SUBMT_IN"),1," ").alias("PCP_SUBMT_IN"),
    rpad(col("PROD_OOA_IN"),1," ").alias("PROD_OOA_IN"),
    rpad(col("ACDNT_DT"),10," ").alias("ACDNT_DT"),
    rpad(col("INPT_DT"),10," ").alias("INPT_DT"),
    rpad(col("MBR_PLN_ELIG_DT"),10," ").alias("MBR_PLN_ELIG_DT"),
    rpad(col("NEXT_RVW_DT"),10," ").alias("NEXT_RVW_DT"),
    rpad(col("PD_DT"),10," ").alias("PD_DT"),
    rpad(col("PAYMT_DRAG_CYC_DT"),10," ").alias("PAYMT_DRAG_CYC_DT"),
    rpad(col("PRCS_DT"),10," ").alias("PRCS_DT"),
    rpad(col("RCVD_DT"),10," ").alias("RCVD_DT"),
    rpad(col("SVC_STRT_DT"),10," ").alias("SVC_STRT_DT"),
    rpad(col("SVC_END_DT"),10," ").alias("SVC_END_DT"),
    rpad(col("SMLR_ILNS_DT"),10," ").alias("SMLR_ILNS_DT"),
    rpad(col("STTUS_DT"),10," ").alias("STTUS_DT"),
    rpad(col("WORK_UNABLE_BEG_DT"),10," ").alias("WORK_UNABLE_BEG_DT"),
    rpad(col("WORK_UNABLE_END_DT"),10," ").alias("WORK_UNABLE_END_DT"),
    col("ACDNT_AMT"),
    col("ACTL_PD_AMT"),
    col("ALLOW_AMT"),
    col("DSALW_AMT"),
    col("COINS_AMT"),
    col("CNSD_CHRG_AMT"),
    col("COPAY_AMT"),
    col("CHRG_AMT"),
    col("DEDCT_AMT"),
    col("PAYBL_AMT"),
    col("CLM_CT"),
    col("MBR_AGE"),
    col("ADJ_FROM_CLM_ID"),
    col("ADJ_TO_CLM_ID"),
    rpad(col("DOC_TX_ID"),18," ").alias("DOC_TX_ID"),
    rpad(col("MCAID_RESUB_NO"),15," ").alias("MCAID_RESUB_NO"),
    rpad(col("MCARE_ID"),12," ").alias("MCARE_ID"),
    rpad(col("MBR_SFX_NO"),2," ").alias("MBR_SFX_NO"),
    col("PATN_ACCT_NO"),
    rpad(col("PAYMT_REF_ID"),16," ").alias("PAYMT_REF_ID"),
    rpad(col("PROV_AGMNT_ID"),12," ").alias("PROV_AGMNT_ID"),
    col("RFRNG_PROV_TX"),
    rpad(col("SUB_ID"),14," ").alias("SUB_ID"),
    rpad(col("PRPR_ENTITY"),1," ").alias("PRPR_ENTITY"),
    rpad(col("PCA_TYP_CD"),18," ").alias("PCA_TYP_CD"),
    col("REL_PCA_CLM_ID"),
    col("CLCL_MICRO_ID"),
    rpad(col("CLM_UPDT_SW"),1," ").alias("CLM_UPDT_SW"),
    col("REMIT_SUPRSION_AMT"),
    col("MCAID_STTUS_ID"),
    col("PATN_PD_AMT"),
    col("CLM_SUBMT_ICD_VRSN_CD"),
    col("CLM_TXNMY_CD"),
    rpad(lit("N"),1," ").alias("BILL_PAYMT_EXCL_IN")
)

df_Snapshot = df_Transform.select(
    rpad(col("CLM_ID"),12," ").alias("CLM_ID"),
    col("MBR_CK"),
    rpad(col("GRP"),8," ").alias("GRP"),
    col("SVC_STRT_DT"),
    col("CHRG_AMT"),
    col("PAYBL_AMT"),
    rpad(col("EXPRNC_CAT"),4," ").alias("EXPRNC_CAT"),
    rpad(col("FNCL_LOB_NO"),4," ").alias("FNCL_LOB_NO"),
    col("CLM_CT"),
    rpad(col("PCA_TYP_CD"),18," ").alias("PCA_TYP_CD"),
    rpad(col("CLM_STTUS_CD"),2," ").alias("CLM_STTUS_CD"),
    rpad(col("CLM_CAT_CD"),10," ").alias("CLM_CAT_CD")
)

#----------------------------------------------------------------------------------------------------------
# 7) Stage "Transformer" (CTransformerStage)
#    Input: df_Snapshot
#    Output pin "RowCount" => "B_CLM" with new columns referencing stage variables for codes
#    The job uses user-defined function calls like GetFkeyExprncCat, etc. We'll assume they are available.
#----------------------------------------------------------------------------------------------------------

df_Transformer = df_Snapshot.withColumn(
    "ExpCatCdSk",
    GetFkeyExprncCat(lit("FACETS"), lit(0), col("EXPRNC_CAT"), lit("N"))
).withColumn(
    "GrpSk",
    GetFkeyGrp(lit("FACETS"), lit(0), col("GRP"), lit("N"))
).withColumn(
    "MbrSk",
    GetFkeyMbr(lit("FACETS"), lit(0), col("MBR_CK"), lit("N"))
).withColumn(
    "FnclLobSk",
    GetFkeyFnclLob(lit("PSI"), lit(0), col("FNCL_LOB_NO"), lit("N"))
).withColumn(
    "PcaTypCdSk",
    GetFkeyCodes(lit("FACETS"), lit(0), lit("PERSONAL CARE ACCOUNT PROCESSING"), col("PCA_TYP_CD"), lit("N"))
).withColumn(
    "ClmSttusCdSk",
    GetFkeyCodes(lit("ESI"), lit(0), lit("CLAIM STATUS"), col("CLM_STTUS_CD"), lit("N"))
).withColumn(
    "ClmCatCdSk",
    GetFkeyCodes(lit("ESI"), lit(0), lit("CLAIM CATEGORY"), col("CLM_CAT_CD"), lit("X"))
)

df_B_CLM = df_Transformer.select(
    col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("ClmSttusCdSk").alias("CLM_STTUS_CD_SK"),
    col("ClmCatCdSk").alias("CLM_CAT_CD_SK"),
    col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    col("FnclLobSk").alias("FNCL_LOB_SK"),
    col("GrpSk").alias("GRP_SK"),
    col("MbrSk").alias("MBR_SK"),
    rpad(col("SVC_STRT_DT"),10," ").alias("SVC_STRT_DT_SK"),
    col("CHRG_AMT"),
    col("PAYBL_AMT"),
    col("CLM_CT"),
    col("PcaTypCdSk").alias("PCA_TYP_CD_SK")
)

#----------------------------------------------------------------------------------------------------------
# 8) Stage "B_CLM" (CSeqFileStage) => write to B_CLM.ESI.dat.#RunID#
#    Directory "load" (not "landing" or "external"), so path => f"{adls_path}/load/B_CLM.ESI.dat.{RunID}"
#----------------------------------------------------------------------------------------------------------

write_files(
    df_B_CLM,
    f"{adls_path}/load/B_CLM.ESI.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

#----------------------------------------------------------------------------------------------------------
# 9) Stage "ClmPK" (CContainerStage) => 1 input pin "Pkey", 1 output pin "Key"
#    We call the shared container "ClmPK" as a function with (df_pkey, params)
#----------------------------------------------------------------------------------------------------------

params_clmpk = {
    "CurrRunCycle": get_widget_value('RunCycle','')
}

df_clmpk = ClmPK(df_Pkey, params_clmpk)

#----------------------------------------------------------------------------------------------------------
# 10) Stage "ESIClmExtr" (CSeqFileStage) => writes "ESIClmExtr.DrugClm.dat.#RunID#" in directory "key"
#     (not "landing" or "external"), so => f"{adls_path}/key/..."
#----------------------------------------------------------------------------------------------------------

write_files(
    df_clmpk,
    f"{adls_path}/key/ESIClmExtr.DrugClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)