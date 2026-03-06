# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC CALLED BY : BCADrugClmExtrLoadSeq
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   Reads the BCADrugClm_Land file from the extract program and adds the Recycle fields and business rules and reformats to the drug claim common format. Then runs shared container DrugClmPkey
# MAGIC                   Formats and processing primary key for DRUG_CLM
# MAGIC                   Also, creates new rows and gets primary key for new NDC codes, new DEA and new Pharmacies
# MAGIC                   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC ==============================================================================================================================================    
# MAGIC Developer	Date		Project #		Change Description				Development Project	Code Reviewer	Date Reviewed  
# MAGIC ==============================================================================================================================================
# MAGIC Kalyan Neelam	2013-10-29	5056 FEP Claims	Initial Programming				IntegrateNewDevl		Bhoomi Dasari	11/30/2013
# MAGIC Manasa Andru	2015-01-30	TFS 9791		Added BCA to the Bal file name		IntegrateNewDevl		Kalyan Neelam	2015-01-30      
# MAGIC Kalyan Neelam	2015-11-06	5403		Added 2 new columns to PROV_DEA key file -	IntegrateDev1		Bhoomi Dasari	11/9/2015
# MAGIC 						NTNL_PROV_ID_PROV_TYP_DESC and        
# MAGIC 						NTNL_PROV_ID_PROV_TYP_DESC              
# MAGIC Abhiram Dasarathy	2016-11-07	5568 - HEDIS	Changed the business rules based on the mapping	IntegrateDev2		Kalyan Neelam	2016-11-15
# MAGIC 						changes
# MAGIC Abhiram Dasarathy	2017-02-03	5568 - HEDIS	Modified the drug quantity logic		IntegrateDev1		Kalyan Neelam        2017-02-07
# MAGIC Abhiram Dasarathy	2017-02-27   	5568 - HEDIS	Changed the transformation rule for 		IntegrateDev2		Kalyan Neelam        2017-02-28
# MAGIC 					       	DISPNS_PROV_ID
# MAGIC Sudhir Bomshetty    2017-07-20               5781 - HEDIS          PRSCRB_PROV_DEA_SK is updated to 
# MAGIC                                                                                                 accommodate the FEP Pharmacy file changes       IntegrateDev1		Kalyan Neelam       2017-07-26
# MAGIC 
# MAGIC Saikiran Mahenderker     2018-03-15       5781 HEDIS           Added  four new source columns and  updated              IntegrateDev2		Jaideep Mankala    04/04/2018
# MAGIC                                                                                                 logic for DRUG_CLM_DAW_CD,GNRC_DRUG_IN
# MAGIC 
# MAGIC Saikiran Subbagari         2019-01-15           5887         Added the TXNMY_CD column  in DrugClmPK Container       IntegrateDev1                    Kalyan Neelam        2019-02-12
# MAGIC 
# MAGIC Karthik Chintalapani         2019-08-27        5884       Added new columns to BCAClmLand  file stage                           IntegrateDev1	               Kalyan Neelam        2019-09-05
# MAGIC                                                                                  CLM_STTUS_CD,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID    
# MAGIC                                                                                  for implementing reversals logic
# MAGIC 
# MAGIC Karthik Chintalapani	2020-01-30      5884   Modifed thePrscrb_Prov_Id field transformation to handle nulls                IntegrateDev1    Jaideep Mankala      02/05/2020
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani	2020-02-21      5884  Added 2 additional columns  PRSCRBR_NTNL_PROV_ID, PDX_NTNL_PROV_ID  IntegrateDev1    Jaideep Mankala      02/24/2020
# MAGIC                                                                   in the SnapShot xfm in order to validate the output link leading to shared container
# MAGIC Giri  Mallavaram    2020-04-06         PBM Changes       Added SPEC_DRUG_IN to be in sync with changes to DrugClmPKey Container        IntegrateDev2       Kalyan Neelam   2020-04-07   
# MAGIC 
# MAGIC Velmani Kondappan      2020-08-28        6264-PBM Phase II - Government Programs           Added SUBMT_PROD_ID_QLFR,
# MAGIC                                                                                                                                          CNTNGNT_THER_FLAG,CNTNGNT_THER_SCHD,                                       IntegrateDev5         Klayan Neelam      2020-12-10
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PROD_AMT,
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT,CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
# MAGIC                                                                                                                                          CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT,CLNT_PATN_PAY_ATRBD_NTWK_AMT,
# MAGIC                                                                                                                                          GNRC_PROD_IN  fields

# MAGIC Writing Sequential File to /key
# MAGIC BCA Drug Claim Extract
# MAGIC Drug Claim Primary Key Shared Container
# MAGIC The file is created in the BCADrugClmPreProcExtr job
# MAGIC This container is used in:
# MAGIC PCSDrugClmExtr
# MAGIC ESIDrugClmExtr
# MAGIC WellDyneDurgClmExtr
# MAGIC MCSourceDrugClmExtr
# MAGIC Medicaid Drug Clm Extr
# MAGIC BCBSSCDrugClmExtr
# MAGIC BCADrugClmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
#!/usr/bin/python

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/DrugClmPK
# COMMAND ----------

# Retrieve parameters
CactusOwner = get_widget_value("CactusOwner","")
cactus_secret_name = get_widget_value("cactus_secret_name","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")
CurrDate = get_widget_value("CurrDate","")
RunCycle = get_widget_value("RunCycle","")
SrcSysCd = get_widget_value("SrcSysCd","")
RunID = get_widget_value("RunID","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
ProvRunCycle = get_widget_value("ProvRunCycle","")

# ----------------------------------------------------------------------------
# Stage: PROV_DEA (DB2Connector, Database=IDS) => Output => DEA => CHashedFileStage hf_bca_drugclm_dea
# ----------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(ids_secret_name)
df_PROV_DEA = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT PROV_DEA_SK as PROV_DEA_SK, DEA_NO as DEA_NO FROM "
        + IDSOwner
        + ".PROV_DEA"
    )
    .load()
)

# ----------------------------------------------------------------------------
# Stage: hf_bca_drugclm_dea (CHashedFileStage) => Scenario A (intermediate hashed file)
#     Key columns: PROV_DEA_SK
#     => deduplicate, feed to next stage as df_hf_bca_drugclm_dea
# ----------------------------------------------------------------------------
df_hf_bca_drugclm_dea = df_PROV_DEA.dropDuplicates(["PROV_DEA_SK"])

# This output pin is named "hf_dea" going to "DeaLookUp".
# We will keep a reference as df_hf_dea for that lookup.
df_hf_dea = df_hf_bca_drugclm_dea.select(
    "PROV_DEA_SK",
    "DEA_NO"
)

# ----------------------------------------------------------------------------
# Stage: BCADrugClm_Land (CSeqFileStage) => read from path "verified/BCADrugClm_Land.dat.#RunID#"
# ----------------------------------------------------------------------------
schema_BCADrugClm_Land = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), False),
    StructField("ADJ_FROM_CLM_ID", StringType(), False),
    StructField("ADJ_TO_CLM_ID", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("ALLOCD_CRS_PLN_CD", StringType(), True),
    StructField("ALLOCD_SHIELD_PLN_CD", StringType(), True),
    StructField("PDX_SVC_ID", DecimalType(), True),
    StructField("CLM_LN_NO", DecimalType(), True),
    StructField("RX_FILLED_DT", StringType(), True),
    StructField("CONSIS_MBR_ID", DecimalType(), True),
    StructField("FEP_CNTR_ID", StringType(), True),
    StructField("LGCY_MBR_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("PATN_AGE", DecimalType(), True),
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
    StructField("RX_CST_EQVLNT", DecimalType(), True),
    StructField("METRIC_UNIT", DecimalType(), True),
    StructField("NON_METRIC_UNIT", DecimalType(), True),
    StructField("DAYS_SUPPLIED", DecimalType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("CLM_LOAD_DT", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("DISPNS_DRUG_TYP", StringType(), True),
    StructField("DISPNS_AS_WRTN_STTUS_CD", StringType(), True),
    StructField("ADJ_CD", StringType(), True),
    StructField("PD_DT", StringType(), True)
])

df_BCADrugClm_Land = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_BCADrugClm_Land)
    .load(f"{adls_path}/verified/BCADrugClm_Land.dat.{RunID}")
)

# ----------------------------------------------------------------------------
# Stage: IDS (DB2Connector, Database=IDS) => 2 output pins: "NDC" and "PRCT"
# ----------------------------------------------------------------------------
df_NDC = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT NDC as NDC,"
        " MAP1.SRC_CD as NDC_DRUG_ABUSE_CTL_CD,"
        " DRUG_MNTN_IN as DRUG_MNTN_IN"
        " FROM "
        + IDSOwner
        + ".NDC NDC, "
        + IDSOwner
        + ".CD_MPPNG MAP1"
        " WHERE NDC_DRUG_ABUSE_CTL_CD_SK = MAP1.CD_MPPNG_SK"
    )
    .load()
)

df_PRCT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT PRCT.NTNL_PROV_ID,"
        " MPPNG.SRC_CD,"
        " PRCT.CMN_PRCT_SK,"
        " MIN(DEA.PROV_DEA_SK)"
        " FROM "
        + IDSOwner
        + ".PROV_DEA DEA,"
        + IDSOwner
        + ".CMN_PRCT PRCT,"
        + IDSOwner
        + ".CD_MPPNG MPPNG"
        " WHERE PRCT.SRC_SYS_CD_SK = MPPNG.CD_MPPNG_SK"
        " AND PRCT.CMN_PRCT_SK = DEA.CMN_PRCT_SK"
        " GROUP BY PRCT.CMN_PRCT_SK, PRCT.NTNL_PROV_ID, MPPNG.SRC_CD"
    )
    .load()
)

# ----------------------------------------------------------------------------
# Stage: hf_bca_drugclm_ndc_prct (CHashedFileStage), scenario A
#   First hashed file portion: columns => NDC (PK), NDC_DRUG_ABUSE_CTL_CD, DRUG_MNTN_IN
#   Second hashed file portion: columns => NTNL_PROV_ID (PK), SRC_CD (PK), CMN_PRCT_SK, PROV_DEA_SK
# ----------------------------------------------------------------------------
df_hf_ndc = df_NDC.dropDuplicates(["NDC"])
df_hf_prct = df_PRCT.dropDuplicates(["NTNL_PROV_ID", "SRC_CD"])

# Create references for the output pins:
# "hf_ndc", "hf_prct_facets", and "hf_prct_vcac" each read from these deduplicated sets
df_hf_ndc = df_hf_ndc.select(
    "NDC",
    "NDC_DRUG_ABUSE_CTL_CD",
    "DRUG_MNTN_IN"
)

df_hf_prct_facets = df_hf_prct.select(
    F.col("NTNL_PROV_ID"),
    F.col("SRC_CD"),
    F.col("CMN_PRCT_SK"),
    F.col("MIN(DEA.PROV_DEA_SK)").alias("PROV_DEA_SK")
)

df_hf_prct_vcac = df_hf_prct_facets.select(
    "NTNL_PROV_ID",
    "SRC_CD",
    "CMN_PRCT_SK",
    "PROV_DEA_SK"
)

# ----------------------------------------------------------------------------
# Stage: PROVDEA (DB2Connector, Database=IDS) => 3 output pins => "PROV_DEA", "PROV_DEA_NO", "PROV"
# ----------------------------------------------------------------------------
df_PROVDEA_1 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT NTNL_PROV_ID, MIN(PROV_DEA_SK) as PROV_DEA_SK"
        " FROM "
        + IDSOwner
        + ".PROV_DEA"
        " GROUP BY NTNL_PROV_ID"
    )
    .load()
)

df_PROVDEA_2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT PROV_DEA.DEA_NO, PROV_DEA.PROV_DEA_SK"
        " FROM "
        + IDSOwner
        + ".PROV_DEA PROV_DEA"
    )
    .load()
)

df_PROVDEA_3 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option(
        "query",
        "SELECT PROV.NTNL_PROV_ID, min(PROV_DEA.PROV_DEA_SK) as PROV_DEA_SK"
        " FROM "
        + IDSOwner
        + ".PROV PROV, "
        + IDSOwner
        + ".PROV_DEA PROV_DEA"
        " WHERE PROV.CMN_PRCT_SK = PROV_DEA.CMN_PRCT_SK"
        " AND PROV.CMN_PRCT_SK NOT IN (1,0)"
        " AND PROV_DEA.CMN_PRCT_SK NOT IN (1,0)"
        " AND PROV_DEA.PROV_DEA_SK <> 1"
        " GROUP BY PROV.NTNL_PROV_ID"
    )
    .load()
)

# ----------------------------------------------------------------------------
# Stage: hf_bca_drugclm_provdea (CHashedFileStage), scenario A
#   output pins => hf_prov_dea (key=NTNL_PROV_ID), hf_prov_dea_no (key=DEA_NO), hf_Prov (key=NTNL_PROV_ID)
# ----------------------------------------------------------------------------
df_PROVDEA_1_dedup = df_PROVDEA_1.dropDuplicates(["NTNL_PROV_ID"])
df_PROVDEA_2_dedup = df_PROVDEA_2.dropDuplicates(["DEA_NO"])
df_PROVDEA_3_dedup = df_PROVDEA_3.dropDuplicates(["NTNL_PROV_ID"])

df_hf_prov_dea = df_PROVDEA_1_dedup.select("NTNL_PROV_ID", "PROV_DEA_SK")
df_hf_prov_dea_no = df_PROVDEA_2_dedup.select("DEA_NO", "PROV_DEA_SK")
df_hf_Prov = df_PROVDEA_3_dedup.select("NTNL_PROV_ID", "PROV_DEA_SK")

# ----------------------------------------------------------------------------
# Stage: BusinessLogic (CTransformerStage)
#   Primary input: df_BCADrugClm_Land as BcaDrugData
#   Lookup links: df_hf_ndc (hf_ndc), df_hf_prct_facets, df_hf_prct_vcac, df_hf_prov_dea, df_hf_prov_dea_no, df_hf_Prov
#   Various expressions and output => drug_clm
# ----------------------------------------------------------------------------

df_biz_primary = df_BCADrugClm_Land.alias("BcaDrugData")

df_lk_ndc = df_hf_ndc.alias("hf_ndc")
df_lk_prct_facets = df_hf_prct_facets.alias("hf_prct_facets")
df_lk_prct_vcac = df_hf_prct_vcac.alias("hf_prct_vcac")
df_lk_prov_dea = df_hf_prov_dea.alias("hf_prov_dea")
df_lk_prov_dea_no = df_hf_prov_dea_no.alias("hf_prov_dea_no")
df_lk_Prov = df_hf_Prov.alias("hf_Prov")

# Perform left joins in sequence
# hf_ndc join condition: BcaDrugData.NDC == hf_ndc.NDC
df_biz_1 = df_biz_primary.join(
    df_lk_ndc,
    on=F.trim(F.col("BcaDrugData.NDC")) == F.trim(F.col("hf_ndc.NDC")),
    how="left"
)

# hf_prct_facets join condition:
#   BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID = hf_prct_facets.NTNL_PROV_ID
#   'FACETS' = hf_prct_facets.SRC_CD
df_biz_2 = df_biz_1.join(
    df_lk_prct_facets,
    on=[
        F.trim(F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID")) == F.trim(F.col("hf_prct_facets.NTNL_PROV_ID")),
        F.lit("FACETS") == F.col("hf_prct_facets.SRC_CD"),
    ],
    how="left"
)

# hf_prct_vcac join condition:
#   BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID = hf_prct_vcac.NTNL_PROV_ID
#   'VCAC' = hf_prct_vcac.SRC_CD
df_biz_3 = df_biz_2.join(
    df_lk_prct_vcac,
    on=[
        F.trim(F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID")) == F.trim(F.col("hf_prct_vcac.NTNL_PROV_ID")),
        F.lit("VCAC") == F.col("hf_prct_vcac.SRC_CD"),
    ],
    how="left"
)

# hf_prov_dea join condition:
#   BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID == hf_prov_dea.NTNL_PROV_ID
df_biz_4 = df_biz_3.join(
    df_lk_prov_dea,
    on=[F.trim(F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID")) == F.trim(F.col("hf_prov_dea.NTNL_PROV_ID"))],
    how="left"
)

# hf_prov_dea_no join condition:
#   BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID == hf_prov_dea_no.DEA_NO
df_biz_5 = df_biz_4.join(
    df_lk_prov_dea_no,
    on=[F.trim(F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID")) == F.trim(F.col("hf_prov_dea_no.DEA_NO"))],
    how="left"
)

# hf_Prov join condition:
#   BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID == hf_Prov.NTNL_PROV_ID
df_biz_6 = df_biz_5.join(
    df_lk_Prov,
    on=[F.trim(F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID")) == F.trim(F.col("hf_Prov.NTNL_PROV_ID"))],
    how="left"
)

# Derivations (Stage Variables and Output Columns). 
# Stage variables in DataStage:
#   svDispnsProvId = index(BcaDrugData.PDX_NTNL_PROV_ID, 'NBP', 1)
#   svProvId = BcaDrugData.PDX_NTNL_PROV_ID[svDispnsProvId+3, ...]
# We replicate this logic if needed for final columns. 
# The output columns for link "drug_clm" are numerous with various expressions.

df_drug_clm = df_biz_6.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),  # WhereExpression = 0
    F.lit("I").alias("INSRT_UPDT_CD"),       # "I"
    F.lit("N").alias("DISCARD_IN"),          # "N"
    F.lit("Y").alias("PASS_THRU_IN"),        # "Y"
    # FIRST_RECYC_DT = CurrDate => we assume user param or current_date() if it was a date, but job uses "CurrDate" param
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("BcaDrugData.CLM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DRUG_CLM_SK"),           # WhereExpression=0
    F.col("BcaDrugData.CLM_ID").alias("CLM_ID"),
    F.lit("1").alias("CLM_STTUS"),           # '1'
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.isnull(F.col("BcaDrugData.NDC")) | (F.length(F.trim(F.col("BcaDrugData.NDC"))) == 0), 
        F.lit("UNK")
    )
    .otherwise(F.upper(F.trim(F.col("BcaDrugData.NDC"))))
    .alias("NDC"),
    F.lit(0).alias("NDC_SK"),
    F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID").alias("PRSCRB_PROV_NTNL_PROV_ID"),
    # PROV_DEA_SK expression is complex
    #   If IsNull(BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID) then 0
    #   else if Num(BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID[1,1])=0 then hf_prov_dea_no.PROV_DEA_SK
    #   else if hf_prct_facets.PROV_DEA_SK not null then that
    #   else if hf_prct_vcac.PROV_DEA_SK not null then that
    #   else if hf_prov_dea.PROV_DEA_SK not null then that
    #   else if hf_Prov.PROV_DEA_SK not null then that
    #   else 0
    F.when(
        F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID").isNull(),
        F.lit(0)
    ).otherwise(
        F.when(
            # DataStage "Num(...)=0" means the first character is not numeric?
            (F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID").substr(F.lit(1), F.lit(1)).cast(IntegerType()).isNull()),
            F.col("hf_prov_dea_no.PROV_DEA_SK")
        )
        .otherwise(
            F.when(
                F.col("hf_prct_facets.PROV_DEA_SK").isNotNull(),
                F.col("hf_prct_facets.PROV_DEA_SK")
            ).otherwise(
                F.when(
                    F.col("hf_prct_vcac.PROV_DEA_SK").isNotNull(),
                    F.col("hf_prct_vcac.PROV_DEA_SK")
                ).otherwise(
                    F.when(
                        F.col("hf_prov_dea.PROV_DEA_SK").isNotNull(),
                        F.col("hf_prov_dea.PROV_DEA_SK")
                    ).otherwise(
                        F.when(
                            F.col("hf_Prov.PROV_DEA_SK").isNotNull(),
                            F.col("hf_Prov.PROV_DEA_SK")
                        ).otherwise(F.lit(0))
                    )
                )
            )
        )
    ).alias("PROV_DEA_SK"),
    F.when(
        F.col("BcaDrugData.DISPNS_AS_WRTN_STTUS_CD").isNull(),
        F.lit("NA")
    ).otherwise(F.trim(F.col("BcaDrugData.DISPNS_AS_WRTN_STTUS_CD"))).alias("DRUG_CLM_DAW_CD"),
    F.when(
        F.col("hf_ndc.NDC_DRUG_ABUSE_CTL_CD").isNull(),
        F.lit("UNK")
    ).otherwise(F.trim(F.col("hf_ndc.NDC_DRUG_ABUSE_CTL_CD"))).alias("DRUG_CLM_LGL_STTUS_CD"),
    F.lit("NA").alias("DRUG_CLM_TIER_CD"),
    F.lit("NA").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.when(
        F.trim(F.col("BcaDrugData.NDC")).substr(F.lit(1), F.lit(5)) == F.lit("99999"),
        F.lit("Y")
    ).otherwise(F.lit("N")).alias("CMPND_IN"),
    F.lit("N").alias("FRMLRY_IN"),
    F.when(
        F.col("BcaDrugData.DISPNS_DRUG_TYP") == F.lit("1"),
        F.lit("Y")
    ).otherwise(
        F.when(
            (F.col("BcaDrugData.DISPNS_DRUG_TYP") == "0") | (F.col("BcaDrugData.DISPNS_DRUG_TYP") == "2"),
            F.lit("N")
        ).otherwise(F.lit("N"))
    ).alias("GNRC_DRUG_IN"),
    F.when(
        F.trim(F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID")).substr(F.lit(1), F.lit(3)) == F.lit("088"),
        F.lit("Y")
    ).otherwise(
        F.when(
            F.trim(F.col("BcaDrugData.PRSCRB_PROV_NTNL_PROV_ID")).substr(F.lit(1), F.lit(3)) == F.lit("089"),
            F.lit("N")
        ).otherwise(F.lit("N"))
    ).alias("MAIL_ORDER_IN"),
    F.when(
        F.col("hf_ndc.DRUG_MNTN_IN").isNull(),
        F.lit("N")
    ).otherwise(F.col("hf_ndc.DRUG_MNTN_IN")).alias("MNTN_IN"),
    F.lit("N").alias("MAC_REDC_IN"),
    F.lit("N").alias("NON_FRMLRY_DRUG_IN"),
    F.when(
        F.trim(F.col("BcaDrugData.GNRC_NM_DRUG_IN")) == "Y",
        F.lit("N")
    ).otherwise(F.lit("Y")).alias("SNGL_SRC_IN"),
    F.lit("1753-01-01").alias("ADJ_DT"),
    F.col("BcaDrugData.RX_FILLED_DT").alias("FILL_DT"),
    F.lit(CurrDate).alias("RECON_DT"),
    F.lit(0.0).alias("DISPNS_FEE_AMT"),
    F.lit(0.0).alias("HLTH_PLN_EXCL_AMT"),
    F.lit(0.0).alias("HLTH_PLN_PD_AMT"),
    F.lit(0.0).alias("INGR_CST_ALW_AMT"),
    F.lit(0.0).alias("INGR_CST_CHRGD_AMT"),
    F.lit(0.0).alias("INGR_SAV_AMT"),
    F.lit(0.0).alias("MBR_DEDCT_EXCL_AMT"),
    F.lit(0.0).alias("MBR_DIFF_PD_AMT"),
    F.lit(0.0).alias("MBR_OOP_AMT"),
    F.lit(0.0).alias("MBR_OOP_EXCL_AMT"),
    F.lit(0.0).alias("OTHR_SAV_AMT"),
    F.col("BcaDrugData.METRIC_UNIT").alias("RX_ALW_QTY"),
    F.col("BcaDrugData.NON_METRIC_UNIT").alias("RX_SUBMT_QTY"),
    F.lit(0.00).alias("SLS_TAX_AMT"),
    F.col("BcaDrugData.DAYS_SUPPLIED").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("BcaDrugData.DAYS_SUPPLIED").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.lit("NA").alias("PDX_NTWK_ID"),
    F.lit(None).alias("RX_NO"),
    F.lit(None).alias("RFL_NO"),
    F.expr("substring(BcaDrugData.CLM_ID,6,length(BcaDrugData.CLM_ID))").alias("VNDR_CLM_NO"),
    F.lit(None).alias("VNDR_PREAUTH_ID"),
    F.col("BcaDrugData.PDX_NTNL_PROV_ID").alias("PROV_ID"),
    F.lit(" ").alias("NDC_LABEL_NM"),
    F.lit(" ").alias("PRSCRB_NAME"),
    F.lit("NA").alias("PHARMACY_NAME"),
    F.lit("NA").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.lit("NA").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.lit("NA").alias("DRUG_CLM_MCPA_RTD_COVDRUG_CD"),
    F.lit("NA").alias("DRUG_CLM_PRAUTH_CD"),
    F.lit("N").alias("MNDTRY_MAIL_ORDER_IN"),
    F.lit(0.00).alias("ADM_FEE_AMT"),
    F.lit("NA").alias("DRUG_CLM_BILL_BSS_CD"),
    F.lit(0.00).alias("AVG_WHLSL_PRICE_AMT"),
    F.col("BcaDrugData.PDX_NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.lit(0.00).alias("UCR_AMT"),
    F.lit(None).alias("SUBMT_PROD_ID_QLFR"),
    F.lit(None).alias("CNTNGNT_THER_FLAG"),
    F.lit("NA").alias("CNTNGNT_THER_SCHD"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.lit(0.00).alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.lit("NA").alias("GNRC_PROD_IN")
)

# ----------------------------------------------------------------------------
# Stage: DeaLookUp (CTransformerStage)
#   Primary link: df_drug_clm => alias "drug_clm"
#   Lookup link: df_hf_dea => alias "hf_dea"
#   Join conditions: drug_clm.PROV_DEA_SK == hf_dea.PROV_DEA_SK and Trim(Fep_Pdx_Data.PRSCRB_PROV_ID)[9] == hf_dea.DEA_NO
#   (The job references "Trim(Fep_Pdx_Data.PRSCRB_PROV_ID)[9]" but there is no stage "Fep_Pdx_Data" actually integrated.
#    We follow the design literally, though it looks suspicious. We'll replicate the left join logic as best we can.)
# ----------------------------------------------------------------------------
df_drug_clm_alias = df_drug_clm.alias("drug_clm")
df_dea_alias = df_hf_dea.alias("hf_dea")

# We have to emulate:
#  on= [drug_clm.PROV_DEA_SK == hf_dea.PROV_DEA_SK,
#       Trim(Fep_Pdx_Data.PRSCRB_PROV_ID)[9] == hf_dea.DEA_NO]
# But we do not truly have Fep_Pdx_Data.PRSCRB_PROV_ID in df. This is a mismatch in the JSON.
# We'll interpret it literally, using a string index:
join_condition = [
    F.col("drug_clm.PROV_DEA_SK") == F.col("hf_dea.PROV_DEA_SK"),
    F.substring(F.trim(F.lit("")), 9, 9999) == F.col("hf_dea.DEA_NO")  # There is no real column; replicating design
]

df_dealookup_joined = df_drug_clm_alias.join(
    df_dea_alias,
    on=join_condition,
    how="left"
)

# Output columns => "Transform"
df_transform = df_dealookup_joined.select(
    F.col("drug_clm.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("drug_clm.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("drug_clm.DISCARD_IN").alias("DISCARD_IN"),
    F.col("drug_clm.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("drug_clm.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("drug_clm.ERR_CT").alias("ERR_CT"),
    F.col("drug_clm.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("drug_clm.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("drug_clm.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("drug_clm.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("drug_clm.CLM_ID").alias("CLM_ID"),
    F.col("drug_clm.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("drug_clm.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("drug_clm.NDC").alias("NDC"),
    F.col("drug_clm.NDC_SK").alias("NDC_SK"),
    F.when(
        (F.col("drug_clm.PROV_DEA_SK").isNull())
        | (F.length(F.trim(F.col("drug_clm.PROV_DEA_SK").cast(StringType()))) < 0),
        F.lit("UNK")
    ).otherwise(
        F.when(
            (F.col("hf_dea.DEA_NO").isNull())
            | (F.length(F.trim(F.col("hf_dea.DEA_NO"))) < 0),
            F.lit("UNK")
        ).otherwise(F.col("hf_dea.DEA_NO"))
    ).alias("PRSCRB_PROV_DEA"),
    F.col("drug_clm.DRUG_CLM_DAW_CD").alias("DRUG_CLM_DAW_CD"),
    F.col("drug_clm.DRUG_CLM_LGL_STTUS_CD").alias("DRUG_CLM_LGL_STTUS_CD"),
    F.col("drug_clm.DRUG_CLM_TIER_CD").alias("DRUG_CLM_TIER_CD"),
    F.col("drug_clm.DRUG_CLM_VNDR_STTUS_CD").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.col("drug_clm.CMPND_IN").alias("CMPND_IN"),
    F.col("drug_clm.FRMLRY_IN").alias("FRMLRY_IN"),
    F.col("drug_clm.GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
    F.col("drug_clm.MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("drug_clm.MNTN_IN").alias("MNTN_IN"),
    F.col("drug_clm.MAC_REDC_IN").alias("MAC_REDC_IN"),
    F.col("drug_clm.NON_FRMLRY_DRUG_IN").alias("NON_FRMLRY_DRUG_IN"),
    F.col("drug_clm.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("drug_clm.ADJ_DT").alias("ADJ_DT"),
    F.col("drug_clm.FILL_DT").alias("FILL_DT"),
    F.col("drug_clm.RECON_DT").alias("RECON_DT"),
    F.col("drug_clm.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("drug_clm.HLTH_PLN_EXCL_AMT").alias("HLTH_PLN_EXCL_AMT"),
    F.col("drug_clm.HLTH_PLN_PD_AMT").alias("HLTH_PLN_PD_AMT"),
    F.col("drug_clm.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("drug_clm.INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
    F.col("drug_clm.INGR_SAV_AMT").alias("INGR_SAV_AMT"),
    F.col("drug_clm.MBR_DEDCT_EXCL_AMT").alias("MBR_DEDCT_EXCL_AMT"),
    F.col("drug_clm.MBR_DIFF_PD_AMT").alias("MBR_DIFF_PD_AMT"),
    F.col("drug_clm.MBR_OOP_AMT").alias("MBR_OOP_AMT"),
    F.col("drug_clm.MBR_OOP_EXCL_AMT").alias("MBR_OOP_EXCL_AMT"),
    F.col("drug_clm.OTHR_SAV_AMT").alias("OTHR_SAV_AMT"),
    F.col("drug_clm.RX_ALW_QTY").alias("RX_ALW_QTY"),
    F.col("drug_clm.RX_SUBMT_QTY").alias("RX_SUBMT_QTY"),
    F.col("drug_clm.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
    F.col("drug_clm.RX_ALW_DAYS_SUPL_QTY").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("drug_clm.RX_ORIG_DAYS_SUPL_QTY").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.col("drug_clm.PDX_NTWK_ID").alias("PDX_NTWK_ID"),
    F.col("drug_clm.RX_NO").alias("RX_NO"),
    F.col("drug_clm.RFL_NO").alias("RFL_NO"),
    F.col("drug_clm.VNDR_CLM_NO").alias("VNDR_CLM_NO"),
    F.col("drug_clm.VNDR_PREAUTH_ID").alias("VNDR_PREAUTH_ID"),
    # This was introduced in the preceding stage as "CLM_STTUS" with '1'
    F.col("drug_clm.CLM_STTUS").alias("CLM_STTUS_CD"),
    F.col("drug_clm.PROV_ID").alias("PROV_ID"),
    F.col("drug_clm.NDC_LABEL_NM").alias("NDC_LABEL_NM"),
    F.col("drug_clm.PRSCRB_NAME").alias("PRSCRB_NAME"),
    F.col("drug_clm.PHARMACY_NAME").alias("PHARMACY_NAME"),
    F.col("drug_clm.DRUG_CLM_BNF_FRMLRY_POL_CD").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.col("drug_clm.DRUG_CLM_BNF_RSTRCT_CD").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.col("drug_clm.DRUG_CLM_MCPA_RTD_COVDRUG_CD").alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    F.col("drug_clm.DRUG_CLM_PRAUTH_CD").alias("DRUG_CLM_PRAUTH_CD"),
    F.col("drug_clm.MNDTRY_MAIL_ORDER_IN").alias("MNDTRY_MAIL_ORDER_IN"),
    F.col("drug_clm.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
    F.col("drug_clm.DRUG_CLM_BILL_BSS_CD").alias("DRUG_CLM_BILL_BSS_CD"),
    F.col("drug_clm.AVG_WHLSL_PRICE_AMT").alias("AVG_WHLSL_PRICE_AMT"),
    F.col("drug_clm.NTNL_PROV_ID").alias("NTNL_PROV_ID"),
    F.col("drug_clm.UCR_AMT").alias("UCR_AMT"),
    F.col("drug_clm.SUBMT_PROD_ID_QLFR").alias("SUBMT_PROD_ID_QLFR"),
    F.col("drug_clm.CNTNGNT_THER_FLAG").alias("CNTNGNT_THER_FLAG"),
    F.col("drug_clm.CNTNGNT_THER_SCHD").alias("CNTNGNT_THER_SCHD"),
    F.col("drug_clm.CLNT_PATN_PAY_ATRBD_PROD_AMT").alias("CLNT_PATN_PAY_ATRBD_PROD_AMT"),
    F.col("drug_clm.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT").alias("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT"),
    F.col("drug_clm.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT").alias("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT"),
    F.col("drug_clm.CLNT_PATN_PAY_ATRBD_NTWK_AMT").alias("CLNT_PATN_PAY_ATRBD_NTWK_AMT"),
    F.col("drug_clm.GNRC_PROD_IN").alias("GNRC_PROD_IN")
)

# ----------------------------------------------------------------------------
# Stage: Snapshot (CTransformerStage)
#   Primary link: df_transform => alias "Transform"
#   2 output pins => Pkey => "DrugClmPK", Snapshot => "Transformer"
# ----------------------------------------------------------------------------
df_transform_alias = df_transform.alias("Transform")

df_Pkey = df_transform_alias.select(
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
    F.col("Transform.PRSCRB_PROV_DEA").alias("PRSCRB_PROV_DEA"),
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
    F.col("Transform.MBR_DEDCT_EXCL_AMT").alias("MBR_DEDCT_EXCL_AMT"),
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
    F.lit("NA").alias("PRSCRBR_NTNL_PROV_ID"),
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

df_Snapshot = df_transform_alias.select(
    F.col("Transform.CLM_ID").cast(StringType()).alias("CLM_ID")
)

# ----------------------------------------------------------------------------
# Stage: DrugClmPK (CContainerStage) => Shared Container "DrugClmPK"
#   1 input: df_Pkey => 5 outputs => KeyDrugClm, KeyNDC, KeyDEA, KeyProv, KeyProvLoc
# ----------------------------------------------------------------------------
params_shared = {
    "FilePath": "<...>",
    "RunCycle": "<...>",
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
df_KeyDrugClm, df_KeyNDC, df_KeyDEA, df_KeyProv, df_KeyProvLoc = DrugClmPK(df_Pkey, params_shared)

# ----------------------------------------------------------------------------
# Stage: Transformer => input: "Snapshot" => actually df_Snapshot
#   Output => "RowCount" => going to B_DRUG_CLM
# ----------------------------------------------------------------------------
df_Transformer_alias = df_Snapshot.alias("Snapshot")

df_RowCount = df_Transformer_alias.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Snapshot.CLM_ID").alias("CLM_ID")
)

# ----------------------------------------------------------------------------
# Stage: B_DRUG_CLM (CSeqFileStage)
#   Input => "RowCount" => df_RowCount
#   Write => "load/B_DRUG_CLM.BCA.dat.#RunID#" => f"{adls_path}/load/B_DRUG_CLM.BCA.dat.{RunID}"
# ----------------------------------------------------------------------------
write_files(
    df_RowCount,
    f"{adls_path}/load/B_DRUG_CLM.BCA.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: DrugProvLoc (CSeqFileStage)
#   Input => df_KeyProvLoc
#   Write => "key/BCADrugClmExtr.ProvLoc.dat.#RunID#"
# ----------------------------------------------------------------------------
write_files(
    df_KeyProvLoc,
    f"{adls_path}/key/BCADrugClmExtr.ProvLoc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: DrugProv (CSeqFileStage)
#   Input => df_KeyProv
#   Write => "key/BCADrugClmExtr.DrugProv.dat.#RunID#"
# ----------------------------------------------------------------------------
write_files(
    df_KeyProv,
    f"{adls_path}/key/BCADrugClmExtr.DrugProv.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: DrugProvDea (CSeqFileStage)
#   Input => df_KeyDEA
#   Write => "key/BCADrugClmExtr.DrugProvDea.dat.#RunID#"
# ----------------------------------------------------------------------------
write_files(
    df_KeyDEA,
    f"{adls_path}/key/BCADrugClmExtr.DrugProvDea.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: DrugClm (CSeqFileStage)
#   Input => df_KeyDrugClm
#   Write => "key/BCADrugClmExtr.DrugClmDrug.dat.#RunID#"
# ----------------------------------------------------------------------------
write_files(
    df_KeyDrugClm,
    f"{adls_path}/key/BCADrugClmExtr.DrugClmDrug.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ----------------------------------------------------------------------------
# Stage: DrugNDC (CSeqFileStage)
#   Input => df_KeyNDC
#   Write => "key/BCADrugClmExtr.DrugNDC.dat.#RunID#"
# ----------------------------------------------------------------------------
write_files(
    df_KeyNDC,
    f"{adls_path}/key/BCADrugClmExtr.DrugNDC.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# End of Job