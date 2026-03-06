# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  BCADrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Reads the BCADrugClm_Land.dat file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #            Change Description                                                                   Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                     --------------------     ------------------------             -----------------------------------------------------------------------------------------          --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam                 2013-10-29        5056 FEP Claims       Initial Programming                                                                    IntegrateNewDevl           Bhoomi Dasari          11/30/2013
# MAGIC 
# MAGIC Manasa Andru                  2015-01-14         TFS - 9791             Changed the Source System code for ClmSttusCdSk and         IntegrateNewDevl             Kalyan Neelam          2015-01-16
# MAGIC                                                                                                    ClmCatCdSk in the GetFkeyCodes routine
# MAGIC Abhiram Dasarathy	         2016-11-07      5568 - HEDIS	   Changed the business rules based on the mapping	         IntegrateDev2	               Kalyan Neelam          2016-11-15
# MAGIC 						   changes
# MAGIC Abhiram Dasarathy	         2017-02-03      5568 - HEDIS	   Modified the PD_DT_SK field logic			         IntegrateDev1	                Kalyan Neelam          2017-02-07	 
# MAGIC 
# MAGIC Saikiran Mahenderker      2018-03-15        5781 HEDIS            Implemented Logic for ADJ_CD to map to CLM_STTUS_CD         IntegrateDev2	Jaideep Mankala       04/04/2018
# MAGIC                                                                                                    for Generating CLM_STTUS_CD_SK          
# MAGIC 
# MAGIC Mohan Karnati                 2019-06-06        ADO-73034             Adding CLM_TXNMY_CD filed in alpha_pfx stage and                 IntegrateDev1	                Kalyan Neelam          2019-07-01
# MAGIC                                                                                                    passing it till BCAClmExtr stage  
# MAGIC 
# MAGIC Karthik Chintalapani         2019-08-27        5884       Added new columns to BCAClmLand  file stage                                            IntegrateDev1	                Kalyan Neelam          2019-09-05      
# MAGIC                                                                                  CLM_STTUS_CD,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID     
# MAGIC                                                                                  for implementing reversals logic
# MAGIC Giri  Mallavaram              2020-04-06        6131  PBM Changes       Added BILL_PAYMT_EXCL_IN to be in sync with changes to CLM_PKey Container        IntegrateDev2              Kalyan Neelam

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
# MAGIC BCBSSCClmExtr
# MAGIC BCBSSCMedClmExtr
# MAGIC BCAClmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Lookup subscriber, product and member information
# MAGIC Writing Sequential File to /key
# MAGIC Read the landing file created in BCADrugClmPreprocExtr
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
CurrentDate = get_widget_value("CurrentDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
RunCycle = get_widget_value("RunCycle","1")
RunID = get_widget_value("RunID","100")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

# Read the BCAClmLand (CSeqFileStage)
schema_BCA = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("ALLOCD_CRS_PLN_CD", StringType(), True),
    StructField("ALLOCD_SHIELD_PLN_CD", StringType(), True),
    StructField("PDX_SVC_ID", DecimalType(18,2), True),
    StructField("CLM_LN_NO", DecimalType(18,2), True),
    StructField("RX_FILLED_DT", StringType(), True),
    StructField("CONSIS_MBR_ID", DecimalType(18,2), True),
    StructField("FEP_CNTR_ID", StringType(), True),
    StructField("LGCY_MBR_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("PATN_AGE", DecimalType(18,2), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("LGCY_SRC_CD", StringType(), True),
    StructField("PRSCRB_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("PRSCRB_NTWK_CD", StringType(), True),
    StructField("PRSCRB_PROV_PLN_CD", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("BRND_NM", StringType(), True),
    StructField("LABEL_NM", StringType(), True),
    StructField("THRPTC_CAT_DESC", StringType(), True),
    StructField("GNRC_NM_DRUG_IN", StringType(), True),
    StructField("METH_DRUG_ADM", StringType(), True),
    StructField("RX_CST_EQVLNT", DecimalType(18,2), True),
    StructField("METRIC_UNIT", DecimalType(18,2), True),
    StructField("NON_METRIC_UNIT", DecimalType(18,2), True),
    StructField("DAYS_SUPPLIED", DecimalType(18,2), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("CLM_LOAD_DT", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("DISPNS_DRUG_TYP", StringType(), True),
    StructField("DISPNS_AS_WRTN_STTUS_CD", StringType(), True),
    StructField("ADJ_CD", StringType(), True),
    StructField("PD_DT", StringType(), True)
])

df_BCA = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_BCA)
    .load(f"{adls_path}/verified/BCADrugClm_Land.dat.{RunID}")
)

# BusinessRules (CTransformerStage) => Output: MedtrakClmTrns
df_MedtrakClmTrns = (
    df_BCA
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.col("SrcSysCd"))  # DataStage expression => 'SrcSysCd'
    .withColumn("PRI_KEY_STRING", F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("CLM_ID")))
    .withColumn("CLM_SK", F.lit(0))
    .withColumn("SRC_SYS_CD_SK", F.col("SrcSysCdSk"))  # DataStage expression => 'SrcSysCdSk'
    .withColumn("CLM_ID", F.col("CLM_ID").cast(StringType()))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("ADJ_FROM_CLM", F.col("ADJ_FROM_CLM_ID"))
    .withColumn("ADJ_TO_CLM", F.col("ADJ_TO_CLM_ID"))
    .withColumn("ALPHA_PFX_CD", F.lit("NA"))
    .withColumn("CLM_EOB_EXCD", F.lit("NA"))
    .withColumn("CLS", F.lit("NA"))
    .withColumn("CLS_PLN", F.lit("NA"))
    .withColumn("EXPRNC_CAT", F.lit("NA"))
    .withColumn("FNCL_LOB_NO", F.lit("NA"))
    .withColumn("GRP", F.col("GRP_ID"))
    .withColumn("MBR_CK", F.col("MBR_UNIQ_KEY"))
    .withColumn("NTWK", F.lit("NA"))
    .withColumn("PROD", F.lit("NA"))
    .withColumn("SUBGRP", F.lit("NA"))
    .withColumn("SUB_CK", F.col("SUB_UNIQ_KEY"))
    .withColumn("CLM_ACDNT_CD", F.lit("NA"))
    .withColumn("CLM_ACDNT_ST_CD", F.lit("NA"))
    .withColumn("CLM_ACTIVATING_BCBS_PLN_CD", F.lit("NA"))
    .withColumn("CLM_AGMNT_SRC_CD", F.lit("NA"))
    .withColumn("CLM_BTCH_ACTN_CD", F.lit("NA"))
    .withColumn("CLM_CAP_CD", F.lit("N"))
    .withColumn("CLM_CAT_CD", F.lit("STD"))
    .withColumn("CLM_CHK_CYC_OVRD_CD", F.lit("NA"))
    .withColumn("CLM_COB_CD", F.lit("NA")[0:1])  # DataStage has length=2, but we store 'NA' then slice
    .withColumn("FINL_DISP_CD", F.lit("ACPTD"))
    .withColumn("CLM_INPT_METH_CD", F.lit("NA"))
    .withColumn("CLM_INPT_SRC_CD", F.lit("NA"))
    .withColumn("CLM_IPP_CD", F.lit("NA"))
    .withColumn("CLM_NTWK_STTUS_CD", F.lit("NA"))
    .withColumn("CLM_NONPAR_PROV_PFX_CD", F.lit("NA"))
    .withColumn("CLM_OTHER_BNF_CD", F.lit("NA"))
    .withColumn("CLM_PAYE_CD", F.lit("NA"))
    .withColumn("CLM_PRCS_CTL_AGNT_PFX_CD", F.lit("NA"))
    .withColumn("CLM_SVC_DEFN_PFX_CD", F.lit("NA"))
    .withColumn("CLM_SVC_PROV_SPEC_CD", F.lit("NA"))
    .withColumn("CLM_SVC_PROV_TYP_CD", F.lit("0012"))
    .withColumn(
        "CLM_STTUS_CD",
        F.when(F.col("ADJ_CD") == F.lit("O"), F.lit("A02"))
         .when(F.col("ADJ_CD") == F.lit("A"), F.lit("A09"))
         .when(F.col("ADJ_CD") == F.lit("V"), F.lit("A08"))
         .otherwise(F.lit("UNK"))
    )
    .withColumn("CLM_SUBMT_BCBS_PLN_CD", F.lit("NA"))
    .withColumn("CLM_SUB_BCBS_PLN_CD", F.lit("NA"))
    .withColumn("CLM_SUBTYP_CD", F.lit("RX"))
    .withColumn("CLM_TYP_CD", F.lit("MED"))
    .withColumn("ATCHMT_IN", F.lit("N"))
    .withColumn("CLM_CLNCL_EDIT_CD", F.lit("N"))
    .withColumn("COBRA_CLM_IN", F.lit("N"))
    .withColumn("FIRST_PASS_IN", F.lit("N"))
    .withColumn("HOST_IN", F.lit("N"))
    .withColumn("LTR_IN", F.lit("N"))
    .withColumn("MCARE_ASG_IN", F.lit("N"))
    .withColumn("NOTE_IN", F.lit("N"))
    .withColumn("PCA_AUDIT_IN", F.lit("N"))
    .withColumn("PCP_SUBMT_IN", F.lit("N"))
    .withColumn("PROD_OOA_IN", F.lit("N"))
    .withColumn("ACDNT_DT", F.lit("1753-01-01"))
    .withColumn("INPT_DT", F.lit("1753-01-01"))
    .withColumn("MBR_PLN_ELIG_DT", F.lit("1753-01-01"))
    .withColumn("NEXT_RVW_DT", F.lit("1753-01-01"))
    .withColumn("PD_DT", F.col("PD_DT"))
    .withColumn("PAYMT_DRAG_CYC_DT", F.lit("1753-01-01"))
    .withColumn("PRCS_DT", F.col("CLM_PD_DT"))
    .withColumn("RCVD_DT", F.col("CLM_LOAD_DT"))
    .withColumn("SVC_STRT_DT", F.col("RX_FILLED_DT"))
    .withColumn("SVC_END_DT", F.col("RX_FILLED_DT"))
    .withColumn("SMLR_ILNS_DT", F.lit("1753-01-01"))
    .withColumn("STTUS_DT", current_date())
    .withColumn("WORK_UNABLE_BEG_DT", F.lit("1753-01-01"))
    .withColumn("WORK_UNABLE_END_DT", F.lit("2199-12-31"))
    .withColumn("ACDNT_AMT", F.lit(0.00))
    .withColumn("ACTL_PD_AMT", F.lit(0.00))
    .withColumn("ALLOW_AMT", F.when(F.col("RX_CST_EQVLNT").isNull(), F.lit(0.00)).otherwise(F.col("RX_CST_EQVLNT")))
    .withColumn("DSALW_AMT", F.lit(0.00))
    .withColumn("COINS_AMT", F.lit(0.00))
    .withColumn("CNSD_CHRG_AMT", F.when(F.col("RX_CST_EQVLNT").isNull(), F.lit(0.00)).otherwise(F.col("RX_CST_EQVLNT")))
    .withColumn("COPAY_AMT", F.lit(0.00))
    .withColumn("CHRG_AMT", F.when(F.col("RX_CST_EQVLNT").isNull(), F.lit(0.00)).otherwise(F.col("RX_CST_EQVLNT")))
    .withColumn("DEDCT_AMT", F.lit(0.00))
    .withColumn("PAYBL_AMT", F.lit(0.00))
    .withColumn("CLM_CT", F.lit(1))
    .withColumn("MBR_AGE", F.col("PATN_AGE"))
    .withColumn("ADJ_FROM_CLM_ID", F.col("ADJ_FROM_CLM_ID"))
    .withColumn("ADJ_TO_CLM_ID", F.col("ADJ_TO_CLM_ID"))
    .withColumn("DOC_TX_ID", F.lit("NA"))
    .withColumn("MCAID_RESUB_NO", F.lit("NA"))
    .withColumn("MCARE_ID", F.lit("NA"))
    .withColumn("MBR_SFX_NO", F.col("MBR_SFX_NO"))
    .withColumn("PATN_ACCT_NO", F.lit("NA"))
    .withColumn("PAYMT_REF_ID", F.lit("NA"))
    .withColumn("PROV_AGMNT_ID", F.lit("NA"))
    .withColumn("RFRNG_PROV_TX", F.lit("NA"))
    .withColumn("SUB_ID", F.col("SUB_ID"))
    .withColumn("REMIT_SUPRSION_AMT", F.lit(0.00))
    .withColumn("MCAID_STTUS_ID", F.lit("NA"))
    .withColumn("PATN_PD_AMT", F.lit(0.00))
    .withColumn("CLM_SUBMT_ICD_VRSN_CD", F.lit("NA"))
)

# ids (DB2Connector) => two queries => sub_alpha_pfx and mbr_enroll
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

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
{IDSOwner}.CLS                             CLS, 
{IDSOwner}.SUBGRP                    SUBGRP,
{IDSOwner}.CLS_PLN                    PLN, 
{IDSOwner}.CD_MPPNG               MAP2, 
{IDSOwner}.PROD_CMPNT           CMPNT,
{IDSOwner}.PROD_BILL_CMPNT  BILL_CMPNT,
{IDSOwner}.EXPRNC_CAT            CAT,
{IDSOwner}.FNCL_LOB                  LOB 

WHERE 
DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
and DRUG.FILL_DT_SK between MBR.EFF_DT_SK and MBR.TERM_DT_SK
and MBR.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK = MAP1.CD_MPPNG_SK
and MAP1.TRGT_CD = 'MED'
and MBR.CLS_SK = CLS.CLS_SK
and MBR.CLS_PLN_SK = PLN.CLS_PLN_SK
and MBR.SUBGRP_SK = SUBGRP.SUBGRP_SK
AND MBR.PROD_SK = CMPNT.PROD_SK
AND CMPNT.PROD_CMPNT_TYP_CD_SK = MAP2.CD_MPPNG_SK
AND MAP2.TRGT_CD= 'PDBL'
AND DRUG.FILL_DT_SK between CMPNT.PROD_CMPNT_EFF_DT_SK AND CMPNT.PROD_CMPNT_TERM_DT_SK
AND CMPNT.PROD_CMPNT_EFF_DT_SK= (
  SELECT MAX(CMPNT2.PROD_CMPNT_EFF_DT_SK)
  FROM {IDSOwner}.PROD_CMPNT CMPNT2
  WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
  AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
  AND DRUG.FILL_DT_SK BETWEEN CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK
)
AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
AND DRUG.FILL_DT_SK between BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (
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

# Hash (CHashedFileStage) transform => Scenario A => deduplicate on key columns, no physical file write
# sub_alpha_pfx_lkup => key: SUB_UNIQ_KEY
df_sub_alpha_pfx_dedup = dedup_sort(df_sub_alpha_pfx, ["SUB_UNIQ_KEY"], [])
# mbr_enr_lkup => key: CLM_ID
df_mbr_enroll_dedup = dedup_sort(df_mbr_enroll, ["CLM_ID"], [])

# alpha_pfx (CTransformerStage) => left joins
df_alpha_pfx_joined = (
    df_MedtrakClmTrns
    .alias("MedtrakClmTrns")
    .join(
        df_sub_alpha_pfx_dedup.alias("sub_alpha_pfx_lkup"),
        F.col("MedtrakClmTrns.SUB_CK") == F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
        "left"
    )
    .join(
        df_mbr_enroll_dedup.alias("mbr_enr_lkup"),
        F.col("MedtrakClmTrns.CLM_ID") == F.col("mbr_enr_lkup.CLM_ID"),
        "left"
    )
)

df_Transform = df_alpha_pfx_joined.select(
    F.col("MedtrakClmTrns.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("MedtrakClmTrns.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("MedtrakClmTrns.DISCARD_IN").alias("DISCARD_IN"),
    F.col("MedtrakClmTrns.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("MedtrakClmTrns.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("MedtrakClmTrns.ERR_CT").alias("ERR_CT"),
    F.col("MedtrakClmTrns.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("MedtrakClmTrns.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MedtrakClmTrns.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("MedtrakClmTrns.CLM_SK").alias("CLM_SK"),
    F.col("MedtrakClmTrns.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("MedtrakClmTrns.CLM_ID").alias("CLM_ID"),
    F.col("MedtrakClmTrns.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("MedtrakClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("MedtrakClmTrns.ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("MedtrakClmTrns.ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.when(F.col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY").isNull(), F.lit("UNK"))
     .otherwise(F.col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")).alias("ALPHA_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.CLS_ID")).alias("CLS"),
    F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.CLS_PLN_ID")).alias("CLS_PLN"),
    F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.EXPRNC_CAT_CD")).alias("EXPRNC_CAT"),
    F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.FNCL_LOB_CD")).alias("FNCL_LOB_NO"),
    F.col("MedtrakClmTrns.GRP").alias("GRP"),
    F.col("MedtrakClmTrns.MBR_CK").alias("MBR_CK"),
    F.col("MedtrakClmTrns.NTWK").alias("NTWK"),
    F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
     .otherwise(trim(F.col("mbr_enr_lkup.PROD_ID"))).alias("PROD"),
    F.when(F.col("mbr_enr_lkup.CLM_ID").isNull(), F.lit("UNK"))
     .otherwise(F.col("mbr_enr_lkup.SUBGRP_ID")).alias("SUBGRP"),
    F.col("MedtrakClmTrns.SUB_CK").alias("SUB_CK"),
    F.col("MedtrakClmTrns.CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("MedtrakClmTrns.CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("MedtrakClmTrns.CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("MedtrakClmTrns.CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.col("MedtrakClmTrns.CLM_CAP_CD").alias("CLM_CAP_CD"),
    F.col("MedtrakClmTrns.CLM_CAT_CD").alias("CLM_CAT_CD"),
    F.col("MedtrakClmTrns.CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("MedtrakClmTrns.CLM_COB_CD").alias("CLM_COB_CD"),
    F.col("MedtrakClmTrns.FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("MedtrakClmTrns.CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("MedtrakClmTrns.CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("MedtrakClmTrns.CLM_IPP_CD").alias("CLM_IPP_CD"),
    F.col("MedtrakClmTrns.CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    F.col("MedtrakClmTrns.CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.col("MedtrakClmTrns.CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("MedtrakClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    F.col("MedtrakClmTrns.CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
    F.col("MedtrakClmTrns.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("MedtrakClmTrns.CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("MedtrakClmTrns.CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("MedtrakClmTrns.CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("MedtrakClmTrns.ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("MedtrakClmTrns.CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    F.col("MedtrakClmTrns.COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("MedtrakClmTrns.FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("MedtrakClmTrns.HOST_IN").alias("HOST_IN"),
    F.col("MedtrakClmTrns.LTR_IN").alias("LTR_IN"),
    F.col("MedtrakClmTrns.MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("MedtrakClmTrns.NOTE_IN").alias("NOTE_IN"),
    F.col("MedtrakClmTrns.PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("MedtrakClmTrns.PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("MedtrakClmTrns.PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("MedtrakClmTrns.ACDNT_DT").alias("ACDNT_DT"),
    F.col("MedtrakClmTrns.INPT_DT").alias("INPT_DT"),
    F.col("MedtrakClmTrns.MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("MedtrakClmTrns.NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("MedtrakClmTrns.PD_DT").alias("PD_DT"),
    F.col("MedtrakClmTrns.PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("MedtrakClmTrns.PRCS_DT").alias("PRCS_DT"),
    F.col("MedtrakClmTrns.RCVD_DT").alias("RCVD_DT"),
    F.col("MedtrakClmTrns.SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("MedtrakClmTrns.SVC_END_DT").alias("SVC_END_DT"),
    F.col("MedtrakClmTrns.SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("MedtrakClmTrns.STTUS_DT").alias("STTUS_DT"),
    F.col("MedtrakClmTrns.WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("MedtrakClmTrns.WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("MedtrakClmTrns.ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("MedtrakClmTrns.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("MedtrakClmTrns.ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("MedtrakClmTrns.DSALW_AMT").alias("DSALW_AMT"),
    F.col("MedtrakClmTrns.COINS_AMT").alias("COINS_AMT"),
    F.col("MedtrakClmTrns.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("MedtrakClmTrns.COPAY_AMT").alias("COPAY_AMT"),
    F.col("MedtrakClmTrns.CHRG_AMT").alias("CHRG_AMT"),
    F.col("MedtrakClmTrns.DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("MedtrakClmTrns.PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("MedtrakClmTrns.CLM_CT").alias("CLM_CT"),
    F.col("MedtrakClmTrns.MBR_AGE").alias("MBR_AGE"),
    F.col("MedtrakClmTrns.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("MedtrakClmTrns.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("MedtrakClmTrns.DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("MedtrakClmTrns.MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("MedtrakClmTrns.MCARE_ID").alias("MCARE_ID"),
    F.col("MedtrakClmTrns.MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("MedtrakClmTrns.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("MedtrakClmTrns.PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("MedtrakClmTrns.PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("MedtrakClmTrns.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("MedtrakClmTrns.SUB_ID").alias("SUB_ID"),
    F.lit(None).cast(StringType()).alias("PRPR_ENTITY"),
    F.lit("NA").alias("PCA_TYP_CD"),
    F.lit("NA").alias("REL_PCA_CLM_ID"),
    F.lit("NA").alias("CLCL_MICRO_ID"),
    F.lit("N").alias("CLM_UPDT_SW"),
    F.col("MedtrakClmTrns.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("MedtrakClmTrns.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("MedtrakClmTrns.PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("MedtrakClmTrns.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.lit("").alias("CLM_TXNMY_CD")
)

# Snapshot (CTransformerStage) => two outputs: Pkey (ClmPK) and Snapshot (Transformer)
df_Pkey = df_Transform.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ADJ_FROM_CLM").alias("ADJ_FROM_CLM"),
    F.col("ADJ_TO_CLM").alias("ADJ_TO_CLM"),
    F.col("ALPHA_PFX_CD").alias("ALPHA_PFX_CD"),
    F.col("CLM_EOB_EXCD").alias("CLM_EOB_EXCD"),
    F.col("CLS").alias("CLS"),
    F.col("CLS_PLN").alias("CLS_PLN"),
    F.col("EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("GRP").alias("GRP"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("NTWK").alias("NTWK"),
    F.col("PROD").alias("PROD"),
    F.col("SUBGRP").alias("SUBGRP"),
    F.col("SUB_CK").alias("SUB_CK"),
    F.col("CLM_ACDNT_CD").alias("CLM_ACDNT_CD"),
    F.col("CLM_ACDNT_ST_CD").alias("CLM_ACDNT_ST_CD"),
    F.col("CLM_ACTIVATING_BCBS_PLN_CD").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    F.col("CLM_AGMNT_SRC_CD").alias("CLM_AGMNT_SRC_CD"),
    F.col("CLM_BTCH_ACTN_CD").alias("CLM_BTCH_ACTN_CD"),
    F.col("CLM_CAP_CD").alias("CLM_CAP_CD"),
    F.col("CLM_CAT_CD").alias("CLM_CAT_CD"),
    F.col("CLM_CHK_CYC_OVRD_CD").alias("CLM_CHK_CYC_OVRD_CD"),
    F.col("CLM_COB_CD").alias("CLM_COB_CD"),
    F.col("FINL_DISP_CD").alias("FINL_DISP_CD"),
    F.col("CLM_INPT_METH_CD").alias("CLM_INPT_METH_CD"),
    F.col("CLM_INPT_SRC_CD").alias("CLM_INPT_SRC_CD"),
    F.col("CLM_IPP_CD").alias("CLM_IPP_CD"),
    F.col("CLM_NTWK_STTUS_CD").alias("CLM_NTWK_STTUS_CD"),
    F.col("CLM_NONPAR_PROV_PFX_CD").alias("CLM_NONPAR_PROV_PFX_CD"),
    F.col("CLM_OTHER_BNF_CD").alias("CLM_OTHER_BNF_CD"),
    F.col("CLM_PAYE_CD").alias("CLM_PAYE_CD"),
    F.col("CLM_PRCS_CTL_AGNT_PFX_CD").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    F.col("CLM_SVC_DEFN_PFX_CD").alias("CLM_SVC_DEFN_PFX_CD"),
    F.col("CLM_SVC_PROV_SPEC_CD").alias("CLM_SVC_PROV_SPEC_CD"),
    F.col("CLM_SVC_PROV_TYP_CD").alias("CLM_SVC_PROV_TYP_CD"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("CLM_SUBMT_BCBS_PLN_CD").alias("CLM_SUBMT_BCBS_PLN_CD"),
    F.col("CLM_SUB_BCBS_PLN_CD").alias("CLM_SUB_BCBS_PLN_CD"),
    F.col("CLM_SUBTYP_CD").alias("CLM_SUBTYP_CD"),
    F.col("CLM_TYP_CD").alias("CLM_TYP_CD"),
    F.col("ATCHMT_IN").alias("ATCHMT_IN"),
    F.col("CLM_CLNCL_EDIT_CD").alias("CLM_CLNCL_EDIT_CD"),
    F.col("COBRA_CLM_IN").alias("COBRA_CLM_IN"),
    F.col("FIRST_PASS_IN").alias("FIRST_PASS_IN"),
    F.col("HOST_IN").alias("HOST_IN"),
    F.col("LTR_IN").alias("LTR_IN"),
    F.col("MCARE_ASG_IN").alias("MCARE_ASG_IN"),
    F.col("NOTE_IN").alias("NOTE_IN"),
    F.col("PCA_AUDIT_IN").alias("PCA_AUDIT_IN"),
    F.col("PCP_SUBMT_IN").alias("PCP_SUBMT_IN"),
    F.col("PROD_OOA_IN").alias("PROD_OOA_IN"),
    F.col("ACDNT_DT").alias("ACDNT_DT"),
    F.col("INPT_DT").alias("INPT_DT"),
    F.col("MBR_PLN_ELIG_DT").alias("MBR_PLN_ELIG_DT"),
    F.col("NEXT_RVW_DT").alias("NEXT_RVW_DT"),
    F.col("PD_DT").alias("PD_DT"),
    F.col("PAYMT_DRAG_CYC_DT").alias("PAYMT_DRAG_CYC_DT"),
    F.col("PRCS_DT").alias("PRCS_DT"),
    F.col("RCVD_DT").alias("RCVD_DT"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("SVC_END_DT").alias("SVC_END_DT"),
    F.col("SMLR_ILNS_DT").alias("SMLR_ILNS_DT"),
    F.col("STTUS_DT").alias("STTUS_DT"),
    F.col("WORK_UNABLE_BEG_DT").alias("WORK_UNABLE_BEG_DT"),
    F.col("WORK_UNABLE_END_DT").alias("WORK_UNABLE_END_DT"),
    F.col("ACDNT_AMT").alias("ACDNT_AMT"),
    F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("ALLOW_AMT").alias("ALLOW_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("MBR_AGE").alias("MBR_AGE"),
    F.col("ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    F.col("ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    F.col("DOC_TX_ID").alias("DOC_TX_ID"),
    F.col("MCAID_RESUB_NO").alias("MCAID_RESUB_NO"),
    F.col("MCARE_ID").alias("MCARE_ID"),
    F.col("MBR_SFX_NO").alias("MBR_SFX_NO"),
    F.col("PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    F.col("PAYMT_REF_ID").alias("PAYMT_REF_ID"),
    F.col("PROV_AGMNT_ID").alias("PROV_AGMNT_ID"),
    F.col("RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    F.col("SUB_ID").alias("SUB_ID"),
    F.col("PRPR_ENTITY").alias("PRPR_ENTITY"),
    F.col("PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("REL_PCA_CLM_ID").alias("REL_PCA_CLM_ID"),
    F.col("CLCL_MICRO_ID").alias("CLCL_MICRO_ID"),
    F.col("CLM_UPDT_SW").alias("CLM_UPDT_SW"),
    F.col("REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    F.col("MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    F.col("PATN_PD_AMT").alias("PATN_PD_AMT"),
    F.col("CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    F.col("CLM_TXNMY_CD").alias("CLM_TXNMY_CD"),
    F.lit("N").alias("BILL_PAYMT_EXCL_IN")
)

df_Snapshot = df_Transform.select(
    F.col("CLM_ID").cast(StringType()).alias("CLM_ID"),
    F.col("MBR_CK").alias("MBR_CK"),
    F.col("GRP").alias("GRP"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("EXPRNC_CAT").alias("EXPRNC_CAT"),
    F.col("FNCL_LOB_NO").alias("FNCL_LOB_NO"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("PCA_TYP_CD").alias("PCA_TYP_CD"),
    F.col("CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("CLM_CAT_CD").alias("CLM_CAT_CD")
)

# ClmPK (CContainerStage)
params_ClmPK = {
    "CurrRunCycle": RunCycle
}
df_ClmPK = ClmPK(df_Pkey, params_ClmPK)

# BCAClmExtr (CSeqFileStage) => write df_ClmPK
write_files(
    df_ClmPK,
    f"{adls_path}/key/BCAClmExtr.DrugClm.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer => input df_Snapshot => stage variables => output => B_CLM
df_Transformer = (
    df_Snapshot
    .withColumn("ExpCatCdSk", GetFkeyExprncCat("FACETS", F.lit(0), F.col("EXPRNC_CAT"), "N"))
    .withColumn("GrpSk", GetFkeyGrp("FACETS", F.lit(0), F.col("GRP"), "N"))
    .withColumn("MbrSk", GetFkeyMbr("FACETS", F.lit(0), F.col("MBR_CK"), "N"))
    .withColumn("FnclLobSk", GetFkeyFnclLob("PSI", F.lit(0), F.col("FNCL_LOB_NO"), "N"))
    .withColumn("PcaTypCdSk", GetFkeyCodes("FACETS", F.lit(0), "PERSONAL CARE ACCOUNT PROCESSING", F.col("PCA_TYP_CD"), "N"))
    .withColumn("ClmSttusCdSk", GetFkeyCodes(F.col("SrcSysCd"), F.lit(0), "CLAIM STATUS", F.col("CLM_STTUS_CD"), "N"))
    .withColumn("ClmCatCdSk", GetFkeyCodes(F.col("SrcSysCd"), F.lit(0), "CLAIM CATEGORY", F.col("CLM_CAT_CD"), "X"))
)

df_B_CLM = df_Transformer.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("ClmSttusCdSk").alias("CLM_STTUS_CD_SK"),
    F.col("ClmCatCdSk").alias("CLM_CAT_CD_SK"),
    F.col("ExpCatCdSk").alias("EXPRNC_CAT_SK"),
    F.col("FnclLobSk").alias("FNCL_LOB_SK"),
    F.col("GrpSk").alias("GRP_SK"),
    F.col("MbrSk").alias("MBR_SK"),
    F.col("SVC_STRT_DT").alias("SVC_STRT_DT_SK"),
    F.col("CHRG_AMT").alias("CHRG_AMT"),
    F.col("PAYBL_AMT").alias("PAYBL_AMT"),
    F.col("CLM_CT").alias("CLM_CT"),
    F.col("PcaTypCdSk").alias("PCA_TYP_CD_SK")
)

# B_CLM (CSeqFileStage) => write df_B_CLM
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