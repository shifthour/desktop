# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : MedtrakDrugClmExtrLoadSeq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC    
# MAGIC  Reads the Medtrak drug file from the extract program and adds business rules and reformats to the drug claim common format. Then runs shared container DrugClmPkey
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                        Date                       Change Description                                                   Project #             Development Project          Code Reviewer               Date Reviewed  
# MAGIC =======================================================================================================================================================================
# MAGIC Kalyan Neelam                          2010-12-22                   Initial Programming                                                        4616                  IntegrateNewDevl              Steph Goddard              12/23/2010
# MAGIC 
# MAGIC Bhoomi Dasari                           2011-11-11          Removed "hf_medtrak_drug_clm_npi_dea" from                TTR-1043            IntegrateWrhsDevl             Sandrew                        2011-11-16
# MAGIC                                                                                           HASH.CLEAR
# MAGIC Karthik Chintalapani                  2012-01-22          Added a new column DRUG_NM for the DrugClmPK            4784                  IntegrateCurDevl                Brent Leland                  03-05-2012
# MAGIC                                                                                      shared container input.
# MAGIC Raja Gummadi                           2012-07-23         Changed RX_NO field size from 9 to 20 in input file               TTR 1330           IntegrateWrhsDevl              Bhoomi Dasari              08/08/2012
# MAGIC 
# MAGIC Karthik Chintalapani                   2013-01-25           Added a new column UCR_AMT                                          4963                 IntegrateNewDevl               SAndrew                       2013-02-26
# MAGIC 
# MAGIC Manasa Andru                           2013-03-22          Added ProvRunCycle to parameters                                     TTR - 1105        IntegrateNewDevl                 Kalyan Neelam             2013-04-02
# MAGIC 
# MAGIC 
# MAGIC Raja Gummadi                          2013-08-28              Added 3 new columns to the DRUG_CLM table.               5115 - BHI         IntegrateNewDevl              Sharon Andrew         2013-09-04
# MAGIC                                                                                 PRTL_FILL_STTUS_CD
# MAGIC                                                                                 PDX_TYP
# MAGIC                                                                                 INCNTV_FEE
# MAGIC Kalyan Neelam                          2015-11-06         Added 2 new columns to PROV_DEA key file -                    5403                     IntegrateDev1                   Bhoomi Dasari               11/9/2015
# MAGIC                                                                             NTNL_PROV_ID_PROV_TYP_DESC and            
# MAGIC                                                                             NTNL_PROV_ID_PROV_TYP_DESC
# MAGIC Shanmugam A 	                2017-03-02          Make the SQL in IDS stage as user defined so when it        5321                      IntegrateDev2                   Jag Yelavarthi                2017-03-07
# MAGIC 					goes through CCMigration, it will not run into SQL issues	          
# MAGIC 
# MAGIC Sudhir Bomshetty                    2018-04-13            Mapped NTNL_PROV_ID to PRESCRIBER_ID_NPI        5781 - HEDIS        IntegrateDev2                Kalyan Neelam               2018-05-10
# MAGIC 
# MAGIC Saikiran Subbagari              2019-01-15          Added the TXNMY_CD column  in DrugClmPK Container      5887                         IntegrateDev1               Kalyan Neelam               2019-02-12
# MAGIC 
# MAGIC 
# MAGIC Giri  Mallavaram    2020-04-06         6131 PBM Changes       Added SPEC_DRUG_IN to be in sync with changes to DrugClmPKey Container        IntegrateDev2              Kalyan Neelam     
# MAGIC 
# MAGIC Velmani Kondappan      2020-08-28        6264-PBM Phase II - Government Programs           Added SUBMT_PROD_ID_QLFR,
# MAGIC                                                                                                                                          CNTNGNT_THER_FLAG,CNTNGNT_THER_SCHD,                                       IntegrateDev5       Kalyan Neelam       2020-12-10
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PROD_AMT,
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT,CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT,CLNT_PATN_PAY_ATRBD_NTWK_AMT,
# MAGIC                                                                                                                                          GNRC_PROD_IN  fields

# MAGIC Primary Key container has ODBC connection to Cactus
# MAGIC Balancing
# MAGIC MEDTRAK Drug Claim Extract
# MAGIC Read the MEDTRAK file created from MedtrakClmLand
# MAGIC This container is used in:
# MAGIC PcsDrugClmExtr
# MAGIC ESIDrugClmExtr
# MAGIC WellDyneDurgClmExtr
# MAGIC MCSourceDrugClmExtr
# MAGIC Medicaid DrugClmExtr
# MAGIC MedtrakDrugClmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/DrugClmPK
# COMMAND ----------

# Parameters
CurrentDate = get_widget_value("CurrentDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
RunCycle = get_widget_value("RunCycle","")
ProvRunCycle = get_widget_value("ProvRunCycle","")
RunID = get_widget_value("RunID","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# ----------------------------------------------------------------------------
# Stage: MedtrakClmLand (CSeqFileStage) - reading the file with an explicit schema
# ----------------------------------------------------------------------------
schema_MedtrakClmLand = StructType([
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
    StructField("DOB", DecimalType(38,10), False),
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
    StructField("DT_RX_WRTN", DecimalType(38,10), False),
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
    StructField("DT_OF_INJURY", DecimalType(38,10), False),
    StructField("FEE_AMT", DecimalType(38,10), False),
    StructField("ESI_REF_NO", StringType(), False),
    StructField("CLNT_CUST_ID", StringType(), False),
    StructField("PLN_TYP", StringType(), False),
    StructField("ESI_ADJDCT_REF_NO", DecimalType(38,10), False),
    StructField("ESI_ANCLRY_AMT", DecimalType(38,10), False),
    StructField("ESI_CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PAID_DATE", StringType(), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("ESI_BILL_DT", DecimalType(38,10), False),
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
    StructField("FLR4", StringType(), False)
])

df_MedtrakClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", '"')
    .option("delimiter", ",")
    .schema(schema_MedtrakClmLand)
    .load(f"{adls_path}/verified/MedtrakDrugClm_Land.dat.{RunID}")
)

# ----------------------------------------------------------------------------
# Stage: IDS (DB2Connector) - Read from IDS database via JDBC
# ----------------------------------------------------------------------------
jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
extract_query_IDS = f"""
SELECT NDC.NDC, MAP1.SRC_CD, NDC.DRUG_MNTN_IN
FROM {IDSOwner}.NDC NDC, {IDSOwner}.CD_MPPNG MAP1
WHERE NDC_DRUG_ABUSE_CTL_CD_SK = MAP1.CD_MPPNG_SK
"""

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_IDS)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: hf_medtrak_drg_clm_ndc_lkup (CHashedFileStage) - Scenario A
# We deduplicate by key column "NDC"
# ----------------------------------------------------------------------------
df_hf_ndc = df_IDS.select(
    F.col("NDC").alias("NDC"),
    F.col("SRC_CD").alias("NDC_DRUG_ABUSE_CTL_CD"),
    F.col("DRUG_MNTN_IN").alias("DRUG_MNTN_IN")
)

df_ndc_lkup = dedup_sort(
    df_hf_ndc,
    partition_cols=["NDC"],
    sort_cols=[("NDC","A")]
)

# ----------------------------------------------------------------------------
# Stage: IDS_PRCT_DEA (DB2Connector)
# Two output pins => PRCT, DEA
# ----------------------------------------------------------------------------
extract_query_PRCT = f"""
SELECT
       PRCT.NTNL_PROV_ID,
       MPPNG.SRC_CD,
       PRCT.CMN_PRCT_SK,
       MIN(DEA.PROV_DEA_SK)
FROM {IDSOwner}.PROV_DEA DEA,
     {IDSOwner}.CMN_PRCT PRCT,
     {IDSOwner}.CD_MPPNG MPPNG
WHERE
PRCT.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK
AND PRCT.CMN_PRCT_SK = DEA.CMN_PRCT_SK
AND DEA.CMN_PRCT_SK NOT IN (0,1)
GROUP BY
PRCT.CMN_PRCT_SK,
PRCT.NTNL_PROV_ID,
MPPNG.SRC_CD
"""

df_PRCT_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_PRCT)
    .load()
)

df_PRCT = df_PRCT_raw.select(
    F.col("NTNL_PROV_ID"),
    F.col("SRC_CD"),
    F.col("CMN_PRCT_SK"),
    F.col("MIN(PROV_DEA_SK)").alias("PROV_DEA_SK")
)

extract_query_DEA = f"""
SELECT
       NTNL_PROV_ID,
       MIN(PROV_DEA_SK)
FROM {IDSOwner}.PROV_DEA
GROUP BY
NTNL_PROV_ID
"""

df_DEA_raw = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_DEA)
    .load()
)

df_DEA = df_DEA_raw.select(
    F.col("NTNL_PROV_ID"),
    F.col("MIN(PROV_DEA_SK)").alias("PROV_DEA_SK")
)

# ----------------------------------------------------------------------------
# Stage: hf_medtrak_drugclm_prct_dea (CHashedFileStage) - Scenario A
#   We produce 2 hashed files effectively:
#   1) from df_PRCT => "hf_medtrak_drugclm_prct"
#   2) from df_DEA  => "hf_medtrak_drugclm_provdea"
#   Then 3 outputs => "hf_prct_facets", "hf_prct_vcac", "hf_prov_dea"
# ----------------------------------------------------------------------------
df_prct_dedup = dedup_sort(
    df_PRCT,
    partition_cols=["NTNL_PROV_ID","SRC_CD"],
    sort_cols=[("NTNL_PROV_ID","A"),("SRC_CD","A")]
)

df_dea_dedup = dedup_sort(
    df_DEA,
    partition_cols=["NTNL_PROV_ID"],
    sort_cols=[("NTNL_PROV_ID","A")]
)

# Outputs:
df_hf_prct_facets = df_prct_dedup
df_hf_prct_vcac = df_prct_dedup
df_hf_prov_dea = df_dea_dedup

# ----------------------------------------------------------------------------
# Stage: BusinessRules (CTransformerStage)
# Primary link: MedtrakClmLand => "df_MedtrakClmLand"
# Lookup links => df_ndc_lkup, df_hf_prov_dea, df_hf_prct_facets, df_hf_prct_vcac
# ----------------------------------------------------------------------------

df_BusinessRules = df_MedtrakClmLand

# Construct join key for NDC lookup (CmpndIn=2 => '99999999999' else NDC_NO)
df_BusinessRules = df_BusinessRules.withColumn(
    "ndc_key",
    F.when(F.col("CMPND_CD") == 2, F.lit("99999999999")).otherwise(F.col("NDC_NO"))
)

# Left join with NdcLkup
df_BusinessRules = df_BusinessRules.alias("Medtrak").join(
    df_ndc_lkup.alias("NdcLkup"),
    F.col("Medtrak.ndc_key") == F.col("NdcLkup.NDC"),
    how="left"
)

# Left join with hf_prov_dea
df_BusinessRules = df_BusinessRules.alias("Medtrak").join(
    df_hf_prov_dea.alias("hf_prov_dea"),
    F.trim(F.col("Medtrak.PRESCRIBER_ID_NPI")) == F.col("hf_prov_dea.NTNL_PROV_ID"),
    how="left"
)

# Left join with hf_prct_facets (SRC_CD='FACETS')
df_BusinessRules = df_BusinessRules.alias("Medtrak").join(
    df_hf_prct_facets.filter(F.col("SRC_CD")=='FACETS').alias("hf_prct_facets"),
    [F.trim(F.col("Medtrak.PRESCRIBER_ID_NPI")) == F.col("hf_prct_facets.NTNL_PROV_ID")],
    how="left"
)

# Left join with hf_prct_vcac (SRC_CD='VCAC')
df_BusinessRules = df_BusinessRules.alias("Medtrak").join(
    df_hf_prct_vcac.filter(F.col("SRC_CD")=='VCAC').alias("hf_prct_vcac"),
    [F.trim(F.col("Medtrak.PRESCRIBER_ID_NPI")) == F.col("hf_prct_vcac.NTNL_PROV_ID")],
    how="left"
)

# Stage variables in DS:
df_BusinessRules = df_BusinessRules \
.withColumn("FrmlryIn", F.when(F.col("Medtrak.FRMLRY_FLAG") == 'F', F.lit('Y')).otherwise(F.lit('N'))) \
.withColumn("CmpndIn", F.when(F.col("Medtrak.CMPND_CD")==0, 'N')
                       .when(F.col("Medtrak.CMPND_CD")==1, 'N')
                       .when(F.col("Medtrak.CMPND_CD")==2, 'Y')
                       .otherwise('U')) \
.withColumn("ClaimTier",
    F.when(F.col("Medtrak.DRUG_TYP")==3, 'TIER1')
     .when((F.col("Medtrak.DRUG_TYP")!=3)&(F.col("Medtrak.FRMLRY_FLAG")=='F'),'TIER2')
     .when((F.col("Medtrak.DRUG_TYP")!=3)&(F.col("Medtrak.FRMLRY_FLAG")!='F'), 'TIER3')
     .otherwise('UNK')
) \
.withColumn("LegalStatus",
    F.when(F.col("NdcLkup.NDC_DRUG_ABUSE_CTL_CD").isNull(), 'UNK')
     .otherwise(F.col("NdcLkup.NDC_DRUG_ABUSE_CTL_CD"))
)

# Now produce the output pin "Transform" columns as described
df_Transform = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),  # From parameter or reference
    F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("Medtrak.CLAIM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DRUG_CLM_SK"),
    F.col("Medtrak.CLAIM_ID").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(F.col("Medtrak.CMPND_CD")==2, F.lit("99999999999")).otherwise(F.col("Medtrak.NDC_NO")).alias("NDC"),
    F.lit(0).alias("NDC_SK"),
    F.when(F.col("hf_prct_facets.PROV_DEA_SK").isNotNull(), F.col("hf_prct_facets.PROV_DEA_SK"))
     .otherwise(
         F.when(F.col("hf_prct_vcac.PROV_DEA_SK").isNotNull(), F.col("hf_prct_vcac.PROV_DEA_SK"))
         .otherwise(
             F.when(F.col("hf_prov_dea.PROV_DEA_SK").isNotNull(), F.col("hf_prov_dea.PROV_DEA_SK"))
             .otherwise(F.lit(0))
         )
     ).alias("PROV_DEA_SK"),
    F.col("Medtrak.DISPENSE_AS_WRTN_PROD_SEL_CD").alias("DRUG_CLM_DAW_CD"),
    F.col("LegalStatus").alias("DRUG_CLM_LGL_STTUS_CD"),
    F.col("ClaimTier").alias("DRUG_CLM_TIER_CD"),
    F.lit("NA").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.col("CmpndIn").alias("CMPND_IN"),
    F.col("FrmlryIn").alias("FRMLRY_IN"),
    F.when((F.col("Medtrak.DRUG_TYP")==2)|(F.col("Medtrak.DRUG_TYP")==3), F.lit('Y')).otherwise(F.lit('N')).alias("GNRC_DRUG_IN"),
    F.when(F.col("Medtrak.PDX_NO")==2623735, F.lit('Y')).otherwise(F.lit('N')).alias("MAIL_ORDER_IN"),
    F.when(F.col("NdcLkup.DRUG_MNTN_IN").isNull(), F.lit('U')).otherwise(F.col("NdcLkup.DRUG_MNTN_IN")).alias("MNTN_IN"),
    F.when(F.trim(F.col("Medtrak.BILL_BSS_CD"))=='09',F.lit("Y")).otherwise(F.lit("N")).alias("MAC_REDC_IN"),
    F.when(F.col("Medtrak.FRMLRY_FLAG")=='F',F.lit('N')).otherwise(F.lit('Y')).alias("NON_FRMLRY_DRUG_IN"),
    F.when(F.col("Medtrak.DRUG_TYP")==1, F.lit('Y')).otherwise(F.lit('N')).alias("SNGL_SRC_IN"),
    F.when(F.col("Medtrak.CLM_TYP")=='R', F.col("Medtrak.ADJDCT_DT")).otherwise(F.lit('1753-01-01')).alias("ADJ_DT"),
    F.when(F.col("Medtrak.DT_FILLED").isNull() | (F.length(F.col("Medtrak.DT_FILLED"))==0), F.lit('1753-01-01'))
     .otherwise(F.col("Medtrak.DT_FILLED")).alias("FILL_DT"),
    F.lit('1753-01-01').alias("RECON_DT"),
    F.col("Medtrak.DISPNS_FEE").alias("DISPNS_FEE_AMT"),
    F.lit(0.00).alias("HLTH_PLN_EXCL_AMT"),
    F.lit(0.00).alias("HLTH_PLN_PD_AMT"),
    F.col("Medtrak.INGR_CST").alias("INGR_CST_ALW_AMT"),
    F.col("Medtrak.INGR_CST").alias("INGR_CST_CHRGD_AMT"),
    F.lit(0.00).alias("INGR_SAV_AMT"),
    F.lit(0.00).alias("MBR_DEDCT_EXcl_AMT"),  # typed carefully
    F.lit(0.00).alias("MBR_DIFF_PD_AMT"),
    F.lit(0.00).alias("MBR_OOP_AMT"),
    F.lit(0.00).alias("MBR_OOP_EXCL_AMT"),
    F.lit(0.00).alias("OTHR_SAV_AMT"),
    F.col("Medtrak.METRIC_QTY").alias("RX_ALW_QTY"),
    F.col("Medtrak.METRIC_QTY").alias("RX_SUBMT_QTY"),
    F.col("Medtrak.SLS_TAX").alias("SLS_TAX_AMT"),
    F.col("Medtrak.DAYS_SUPL").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("Medtrak.DAYS_SUPL").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.lit("NA").alias("PDX_NTWK_ID"),
    F.col("Medtrak.RX_NO").alias("RX_NO"),
    F.col("Medtrak.NEW_RFL_CD").alias("RFL_NO"),
    F.col("Medtrak.ESI_REF_NO").alias("VNDR_CLM_NO"),
    F.when(F.trim(F.col("Medtrak.PRAUTH_NO")).isNull() | (F.length(F.trim(F.col("Medtrak.PRAUTH_NO")))==0),
           F.lit(None)).otherwise(F.col("Medtrak.PRAUTH_NO")).alias("VNDR_PREAUTH_ID"),
    F.col("Medtrak.CLM_TYP").alias("CLM_STTUS_CD"),
    F.trim(F.col("Medtrak.PDX_NO")).alias("PROV_ID"),
    F.lit("NA").alias("NDC_LABEL_NM"),
    F.col("Medtrak.PRESCRIBER_LAST_NM").alias("PRSCRB_NAME"),
    F.lit(None).cast(StringType()).alias("PHARMACY_NAME"),
    F.lit("NA").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.lit("NA").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.lit("NA").alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    F.lit("NA").alias("DRUG_CLM_PRAUTH_CD"),
    F.lit("X").alias("MNDTRY_MAIL_ORDER_IN"),
    F.lit(0.00).alias("ADM_FEE_AMT"),
    F.lit("NA").alias("DRUG_CLM_BILL_BSS_CD"),
    F.lit(0.00).alias("AVG_WHLSL_PRICE_AMT"),
    F.col("Medtrak.PRESCRIBER_ID_NPI").alias("NTNL_PROV_ID"),
    F.col("Medtrak.PRESCRIBER_ID").alias("PRESCRIBER_ID"),
    F.lit(0.00).alias("UCR_AMT"),
    F.lit(None).alias("SUBMT_PROD_ID_QLFR"),
    F.lit(None).alias("CNTNGNT_THER_FLAG"),
    F.lit("NA").alias("CNTNGNT_THER_SCHD"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.lit("NA").alias("GNRC_PROD_IN")  # Overlaps with earlier but DS pinned it again
)

# ----------------------------------------------------------------------------
# Stage: IDS_DEA (DB2Connector)
# ----------------------------------------------------------------------------
extract_query_IDS_DEA = f"""
SELECT PROV_DEA_SK as PROV_DEA_SK, DEA_NO as DEA_NO
FROM {IDSOwner}.PROV_DEA
"""

df_IDS_DEA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", extract_query_IDS_DEA)
    .load()
)

# ----------------------------------------------------------------------------
# Stage: Transformer_230 (CTransformerStage)
# Primary link => "PROV_DEA"
# Output => "dea_sk" (constraint => PROV_DEA_SK <>0 And <>1), "deano"
# ----------------------------------------------------------------------------
df_Transformer_230 = df_IDS_DEA.alias("PROV_DEA")

df_dea_sk = df_Transformer_230.filter(
    (F.col("PROV_DEA.PROV_DEA_SK") != 0) & (F.col("PROV_DEA.PROV_DEA_SK") != 1)
).select(
    F.col("PROV_DEA.PROV_DEA_SK").alias("PROV_DEA_SK"),
    F.col("PROV_DEA.DEA_NO").alias("DEA_NO")
)

df_deano = df_Transformer_230.select(
    F.col("PROV_DEA.DEA_NO").alias("DEA_NO"),
    F.col("PROV_DEA.PROV_DEA_SK").alias("PROV_DEA_SK")
)

# ----------------------------------------------------------------------------
# Stage: hf_medtrak_drug_clm_dea (CHashedFileStage) - Scenario A
# Input => "dea_sk", "deano"
# Output => "hf_dea", "dea_no"
# ----------------------------------------------------------------------------

# Deduplicate by key columns for each output
df_dea_sk_dedup = dedup_sort(
    df_dea_sk,
    partition_cols=["PROV_DEA_SK"],
    sort_cols=[("PROV_DEA_SK","A")]
)

df_deano_dedup = dedup_sort(
    df_deano,
    partition_cols=["DEA_NO"],
    sort_cols=[("DEA_NO","A")]
)

df_hf_dea = df_dea_sk_dedup  # columns => PROV_DEA_SK, DEA_NO
df_dea_no = df_deano_dedup   # columns => DEA_NO, PROV_DEA_SK

# ----------------------------------------------------------------------------
# Stage: Snapshot (CTransformerStage)
# Primary: "Transform" => df_Transform
# Lookups: "hf_dea" => df_hf_dea, "dea_no" => df_dea_no
# ----------------------------------------------------------------------------

df_Snapshot = df_Transform.alias("Transform")

# Join with hf_dea on PROV_DEA_SK
df_Snapshot = df_Snapshot.join(
    df_hf_dea.alias("hf_dea"),
    F.col("Transform.PROV_DEA_SK") == F.col("hf_dea.PROV_DEA_SK"),
    how="left"
)

# Join with dea_no on Trim(Transform.PRESCRIBER_ID) == dea_no.DEA_NO
df_Snapshot = df_Snapshot.join(
    df_dea_no.alias("dea_no"),
    F.trim(F.col("Transform.PRESCRIBER_ID")) == F.col("dea_no.DEA_NO"),
    how="left"
)

# Output pins: "Pkey" => "DrugClmPK", "Snapshot" => "B_DRUG_CLM"

df_Pkey = df_Snapshot.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Transform.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID"),
    F.col("Transform.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.NDC").alias("NDC"),
    F.col("Transform.NDC_SK").alias("NDC_SK"),
    F.when(
      (F.col("dea_no.PROV_DEA_SK").isNotNull()) & (F.length(F.trim(F.col("dea_no.PROV_DEA_SK")))>0),
      F.trim(F.col("Transform.PRESCRIBER_ID"))
    ).otherwise(
      F.when(
         F.col("hf_dea.DEA_NO").isNull(),
         F.when(
           (F.col("Transform.PRESCRIBER_ID").isNotNull()) & (F.length(F.trim(F.col("Transform.PRESCRIBER_ID")))>0),
           F.trim(F.col("Transform.PRESCRIBER_ID"))
         ).otherwise(F.lit("UNK"))
      ).otherwise(F.col("hf_dea.DEA_NO"))
    ).alias("PRSCRB_PROV_DEA"),
    F.col("Transform.DRUG_CLM_DAW_CD").alias("DRUG_CLM_DAW_CD"),
    F.col("Transform.DRUG_CLM_LGL_STTUS_CD").alias("DRUG_CLM_LGL_STTUS_CD"),
    F.col("Transform.DRUG_CLM_TIER_CD").alias("DRUG_CLM_TIER_CD"),
    F.col("Transform.DRUG_CLM_VNDR_STTUS_CD").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.col("Transform.CMPND_IN").alias("CMPND_IN"),
    F.col("Transform.FRMLRY_IN").alias("FRMLRY_IN"),
    F.col("Transform.GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
    F.col("Transform.MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("Transform.MNTN_IN").alias("MNTN_IN"),
    F.col("Transform.MAC_REDC_IN").alias("MAC_REDC_IN"),
    F.col("Transform.NON_FRMLRY_DRUG_IN").alias("NON_FRMLRY_DRUG_IN"),
    F.col("Transform.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("Transform.ADJ_DT").alias("ADJ_DT"),
    F.col("Transform.FILL_DT").alias("FILL_DT"),
    F.col("Transform.RECON_DT").alias("RECON_DT"),
    F.col("Transform.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("Transform.HLTH_PLN_EXCL_AMT").alias("HLTH_PLN_EXCL_AMT"),
    F.col("Transform.HLTH_PLN_PD_AMT").alias("HLTH_PLN_PD_AMT"),
    F.col("Transform.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("Transform.INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
    F.col("Transform.INGR_SAV_AMT").alias("INGR_SAV_AMT"),
    F.col("Transform.MBR_DEDCT_EXcl_AMT").alias("MBR_DEDCT_EXCL_AMT"),
    F.col("Transform.MBR_DIFF_PD_AMT").alias("MBR_DIFF_PD_AMT"),
    F.col("Transform.MBR_OOP_AMT").alias("MBR_OOP_AMT"),
    F.col("Transform.MBR_OOP_EXCL_AMT").alias("MBR_OOP_EXCL_AMT"),
    F.col("Transform.OTHR_SAV_AMT").alias("OTHR_SAV_AMT"),
    F.col("Transform.RX_ALW_QTY").alias("RX_ALW_QTY"),
    F.col("Transform.RX_SUBMT_QTY").alias("RX_SUBMT_QTY"),
    F.col("Transform.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
    F.col("Transform.RX_ALW_DAYS_SUPL_QTY").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("Transform.RX_ORIG_DAYS_SUPL_QTY").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.col("Transform.PDX_NTWK_ID").alias("PDX_NTWK_ID"),
    F.col("Transform.RX_NO").alias("RX_NO"),
    F.col("Transform.RFL_NO").alias("RFL_NO"),
    F.col("Transform.VNDR_CLM_NO").alias("VNDR_CLM_NO"),
    F.col("Transform.VNDR_PREAUTH_ID").alias("VNDR_PREAUTH_ID"),
    F.col("Transform.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("Transform.PROV_ID").alias("PROV_ID"),
    F.col("Transform.NDC_LABEL_NM").alias("NDC_LABEL_NM"),
    F.col("Transform.PRSCRB_NAME").alias("PRSCRB_NAME"),
    F.col("Transform.PHARMACY_NAME").alias("PHARMACY_NAME"),
    F.col("Transform.DRUG_CLM_BNF_FRMLRY_POL_CD").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.col("Transform.DRUG_CLM_BNF_RSTRCT_CD").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.col("Transform.DRUG_CLM_MCPARTD_COVDRUG_CD").alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    F.col("Transform.DRUG_CLM_PRAUTH_CD").alias("DRUG_CLM_PRAUTH_CD"),
    F.col("Transform.MNDTRY_MAIL_ORDER_IN").alias("MNDTRY_MAIL_ORDER_IN"),
    F.col("Transform.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
    F.col("Transform.DRUG_CLM_BILL_BSS_CD").alias("DRUG_CLM_BILL_BSS_CD"),
    F.col("Transform.AVG_WHLSL_PRICE_AMT").alias("AVG_WHLSL_PRICE_AMT"),
    F.col("Transform.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.lit("NA").alias("DRUG_NM"),
    F.col("Transform.UCR_AMT").alias("UCR_AMT"),
    F.lit("NA").alias("PRTL_FILL_STTUS_CD"),
    F.lit("NA").alias("PDX_TYP"),
    F.lit(0.00).alias("INCNTV_FEE"),
    F.lit("").alias("PRSCRBR_NTNL_PROV_ID"),
    F.lit(None).alias("SPEC_DRUG_IN"),
    F.col("Transform.SUBMT_PROD_ID_QLFR").alias("SUBMT_PROD_ID_QLFR"),
    F.col("Transform.CNTNGNT_THER_FLAG").alias("CNTNGNT_THER_FLAG"),
    F.col("Transform.CNTNGNT_THER_SCHD").alias("CNTNGNT_THER_SCHD"),
    F.col("Transform.CLNT_PATN_PAY_ATRBD_PROD_AMT").alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.col("Transform.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT").alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.col("Transform.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT").alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.col("Transform.CLNT_PATN_PAY_ATRBD_NTWK_AMT").alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.col("Transform.GNRC_PROD_IN").alias("GNRC_PROD_IN")
)

df_Snapshot_out = df_Snapshot.select(
    F.col("Transform.SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("Transform.CLM_ID").alias("CLM_ID")
)

# ----------------------------------------------------------------------------
# Stage: B_DRUG_CLM (CSeqFileStage)
# Input => df_Snapshot_out
# Write to file B_DRUG_CLM.#SrcSysCd#.dat.#RunID#
# ----------------------------------------------------------------------------
# Before writing, rpad any char/varchar columns if needed (unknown length => using 255 for demonstration)
df_B_DRUG_CLM_write = df_Snapshot_out.select(
    F.rpad(F.col("SRC_SYS_CD_SK").cast(StringType()), 255, " ").alias("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID").cast(StringType()), 255, " ").alias("CLM_ID")
)

write_files(
    df_B_DRUG_CLM_write,
    f"{adls_path}/load/B_DRUG_CLM.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: DrugClmPK (CContainerStage => Shared Container)
# Input => df_Pkey
# OutputPins => 5 outputs: KeyDrugClm, KeyNDC, KeyDEA, KeyProv, KeyProvLoc
# ----------------------------------------------------------------------------

params_container = {
    "FilePath": "<...>",       # Not specified in the job usage for container, placeholder
    "RunCycle": "<...>",       # Likewise
    "CactusServer": "<...>",
    "CactusOwner": "<...>",
    "CactusAcct": "<...>",
    "CactusPW": "<...>",
    "IDSDB": "<...>",
    "IDSOwner": "<...>",
    "IDSAcct": "<...>",
    "IDSPW": "<...>",
    "ProvRunCycle": "<...>"
}

df_MedtrakDrugClm, df_MedtrakNdc, df_MedtrakProvDea, df_MedtrakProv, df_MedtrakProvLoc = DrugClmPK(
    df_Pkey,
    params_container
)

# ----------------------------------------------------------------------------
# Stage: MedtrakDrugClm (CSeqFileStage)
# ----------------------------------------------------------------------------
# Before writing, rpad char/varchar columns if needed
df_MedtrakDrugClm_write = df_MedtrakDrugClm.select(
    *[
        F.rpad(F.col(c).cast(StringType()), 255, " ").alias(c)
        for c in df_MedtrakDrugClm.columns
    ]
)

write_files(
    df_MedtrakDrugClm_write,
    f"{adls_path}/key/MedtrakDrugClmExtr.DrugClmDrug.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: MedtrakNdc (CSeqFileStage)
# ----------------------------------------------------------------------------
df_MedtrakNdc_write = df_MedtrakNdc.select(
    *[
        F.rpad(F.col(c).cast(StringType()), 255, " ").alias(c)
        for c in df_MedtrakNdc.columns
    ]
)

write_files(
    df_MedtrakNdc_write,
    f"{adls_path}/key/MedtrakDrugClmExtr.DrugNDC.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: MedtrakProvDea (CSeqFileStage)
# ----------------------------------------------------------------------------
df_MedtrakProvDea_write = df_MedtrakProvDea.select(
    *[
        F.rpad(F.col(c).cast(StringType()), 255, " ").alias(c)
        for c in df_MedtrakProvDea.columns
    ]
)

write_files(
    df_MedtrakProvDea_write,
    f"{adls_path}/key/MedtrakDrugClmExtr.DrugProvDea.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: MedtrakProv (CSeqFileStage)
# ----------------------------------------------------------------------------
df_MedtrakProv_write = df_MedtrakProv.select(
    *[
        F.rpad(F.col(c).cast(StringType()), 255, " ").alias(c)
        for c in df_MedtrakProv.columns
    ]
)

write_files(
    df_MedtrakProv_write,
    f"{adls_path}/key/MedtrakDrugClmExtr.DrugProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: MedtrakProvLoc (CSeqFileStage)
# ----------------------------------------------------------------------------
df_MedtrakProvLoc_write = df_MedtrakProvLoc.select(
    *[
        F.rpad(F.col(c).cast(StringType()), 255, " ").alias(c)
        for c in df_MedtrakProvLoc.columns
    ]
)

write_files(
    df_MedtrakProvLoc_write,
    f"{adls_path}/key/MedtrakDrugClmExtr.ProvLoc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)