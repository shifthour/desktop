# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.    
# MAGIC       
# MAGIC PROCESSING: get foreign key as stated above and create file to load to DRUG_CLM, create a file for adjusted claims
# MAGIC                          NDC sk is determined in IdsDrugClmPkey, and just passed to this job
# MAGIC                          Pull Argus claims only with fill dt => FillDtMin for Claim Mart
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             BJ Luce        8/2004          -   Originally Programmed - phase 2.0
# MAGIC             Brent Leland 09/10/2004  -  Added Default rows for UNK and NA
# MAGIC                                                       -  Added link partitioner
# MAGIC             BJ Luce      09/24/2004   -   turn off logging for DAW, Legal Status and 3 tier
# MAGIC             BJ Luce      03/2005             change logging to X for DAW, legal status and 3 tier
# MAGIC                                                           name adjustment file to IdsDrugClmAdj.dat
# MAGIC             BJ Luce     03/29/2005        for reversals, change check for A08 for argus claims
# MAGIC             BJ Luce     08/2005             RX deductibles file format
# MAGIC              BJ Luce     08/01/2005        add recon date
# MAGIC             BJ Luce      08/31/2005        add output file for claim mart P_CLM_DM_EXTR.datDrug#RunID#
# MAGIC             BJ Luce      12//2005          add new input and output fields for Medicare Part D staff tracker 1736
# MAGIC             Brent Leland  04/18/2006   Changed table name P_DRUG_CLM to W_DRUG_CLM and change date field to char.
# MAGIC                                                          Hard coded file names
# MAGIC                                                          Changed parameters to environment values.
# MAGIC                                                          Removed Fill date parameter and logic.  Input no longer has old records that need to be excluded from the Claim Mart.
# MAGIC             Brent Leland 05/08/2006     Changed claim mart output column format and file name.
# MAGIC 
# MAGIC 
# MAGIC Developer                    Date                                         Change Description                                                                       Project #          Development Project          Code Reviewer               Date Reviewed  
# MAGIC ------------------           ----------------------------        ----------------------------------------------------------------------------                                                ----------------         ------------------------------------       ----------------------------               ----------------
# MAGIC Parik                    2008-09-12                Added two new fields to the job                                                                                    #3784(PBM)       devlIDSnew
# MAGIC SAndrew 	            2008-10-10               Created stage var SrcSysCdToUseForCodes                                                                 #3784(PBM)       devlIDSnew               Steph Goddard             10/27/2008         
# MAGIC                                                              Changed rule for stage var LglSttusCd.  
# MAGIC                                                              Change rule for stage var AgjustedClaim 
# MAGIC                                                              Changed stage var McpartdCovdrugCd and PrauthCd to use SrcSysCdToUseForCodes 
# MAGIC                                                              Reduced number of link Collector lines out of IdsDrugClmDrugExt from 3 to one .
# MAGIC                                                              parik added ADM_FEE_AMT and DRUG_CLM_BILL_BSS_CD_SK
# MAGIC SAndrew            2009-02-20                 Added new criteria for WellDyne to test if claim is an adjustment                            3648 Labor Accnt     devlIDS                      Steph Goddard                02/21/2009
# MAGIC SAndrew            2009-02-20                 Added Welldyne in stage variable LglSttusCd to do a code lookup for domain        3648 Labor Accnt    devlIDS                      Steph Goddard                03/30/2009
# MAGIC                                                             "NDC DRUG ABUSE CONTROL"                                                                                 
# MAGIC Tracy Davis       2009-04-14                New field - AVG_WHLSL_PRICE_AMT                                                                  #3784(PBM)               devlIDS                      Steph Goddard                04/14/2009
# MAGIC Kalyan Neelam  2009-10-07                Added a new option MCSOURCE for DRUG_CLM_LGL_STTUS_CD_SK and        4098                         devlIDS                      Steph Goddard                10/15/2009
# MAGIC                                                             ADJ_DT_SK
# MAGIC Kalyan Neelam  2009-12-29                Added a new option MCAID for DRUG_CLM_LGL_STTUS_CD_SK and                         4110                        IntegrateCurDevl
# MAGIC                                                             DRUG_CLM_PRAUTH_CD_SK
# MAGIC Kalyan Neelam  2010-12-22                Added a new option MEDTRAK for DRUG_CLM_LGL_STTUS_CD_SK and                   4616              IntegrateNewDevl         Steph Goddard                12/23/2010
# MAGIC                                                             DRUG_CLM_PRAUTH_CD_SK
# MAGIC Kalyan Neelam  2011-01-17               Added hashed file lookup for Medtrak VndrClmNo - hf_medtrak_drgclm_rvrsl_hf                4616              IntegrateNewDevl         Steph Goddard                 01/18/2011
# MAGIC 
# MAGIC Karthik Chintalapani  2013-01-25                  Added a new column UCR_AMT                                                                            4963               IntegrateNewDevl         SAndrew                         2013-02-26
# MAGIC 
# MAGIC Raja Gummadi    2013-08-28              Added 3 new columns to the table.                                                                           5115- BHI                IntegrateNewDevl           SAndrew                        2013-09-04
# MAGIC                                                              DRUG_CLM_PRTL_FILL_CD_SK
# MAGIC                                                              DRUG_CLM_PDX_TYP_CD_SK
# MAGIC                                                              INCNTV_FEE_AMT
# MAGIC Kalyan Neelam   2013-11-12               Added a new option BCA for DRUG_CLM_LGL_STTUS_CD_SK                      5056 FEP Claims      IntegrateNewDevl              Bhoomi Dasari                11/30/2013
# MAGIC Kaushik Kapoor  2017-02-01               Added the source system SAVRX for LegalStatusCd Stage Variable                   5828                        IntegrateDev2                   Kalyan Neelam                    2018-02-27
# MAGIC 
# MAGIC Kaushik Kapoor  2018-03-19               Added the source system LDI for LegalStatusCd and MCPARTD_COVDRUG_CD 
# MAGIC                                                             stage variable and SAVRX for MCPARTD_COVDRUG_CD only                               5828                  IntegrateDev2                   Jaideep Mankala           03/21/2018
# MAGIC 
# MAGIC Kaushik Kapoor  2018-09-20               Added the source system CVS for LegalStatusCd and MCPARTD_COVDRUG_CD 
# MAGIC                                                             stage variable                                                                                                            5828                  IntegrateDev2                   Kalyan Neelam                    2018-10-01 
# MAGIC Narender Kamatam  2019-10-21          Added "OPTUMRX" Value to the below stage variablesin Transformer Derivation.
# MAGIC                                                                          LglSttusCd
# MAGIC                                                                         AdjustedClaim
# MAGIC                                                                         McpartDCovdrugCd
# MAGIC                                                                         PrauthCd
# MAGIC                                                                         svDrugClmPrtlFillCdSk
# MAGIC                                                                         svDrugClmPdxTypCdSk
# MAGIC                                                                                                                                                                                                    6131- 
# MAGIC                                                                                                                                                                                               PBM Replacement      IntegrateDev1                    
# MAGIC 
# MAGIC 
# MAGIC Bhargava Rampilla   2019-10-24           Modified feilds as per the                                                                   6131-PBM REPLACEMENT          IntegrateDEV1          Kalyan Neelam                    2019-11-20                              
# MAGIC                                                                           OptumRX transformation rules:   
# MAGIC                                                                            DRUG_CLM_LGL_ STTUS_CD_SK
# MAGIC                                                                           DRUG_CLM_PRTL _FILL_CD_SK
# MAGIC                                                                           DRUG_CLM_PDX_ TYP_CD_SK
# MAGIC                                                                           DRUG_CLM_MCPARTD_COVDRUG_CD_SK
# MAGIC                                                                           DRUG_CLM_PRAUTH_CD_SK
# MAGIC                                                                           DRUG_CLM_SK
# MAGIC Kalyan Neelam        2020-01-13            Removed parameter $IDSDB which was not needed in the job                                                             IntegrateDev2
# MAGIC 
# MAGIC Rekha                     2020-03-27            Changed the DRUG_CLM_PDX_NTWK_CD_SK tranformation      6131-PBM REPLACEMENT          IntegrateDev2 
# MAGIC Radhakrishna                                        to perform lookup on Substring[4,4] of PDX_NTWK_ID  
# MAGIC                                                                           
# MAGIC Sagar Sayam           2020-03-31           Added the Speciality indicator filed                                                    6131-PBM REPLACEMENT          IntegrateDev2 
# MAGIC 
# MAGIC Sri Nannapaneni           2020-07-15      6131 PBM replacement      Added 6 new fields to source seq file and drug_clm target file                      IntegrateDev2                 
# MAGIC 
# MAGIC Sri Nannapaneni           2020-08-17      6131 PBM replacement      Added GNRC_PROD_IN to end seq file for drug_clm                                  IntegrateDev2                 
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi         2020-11-12       US-283560                      Added to the MEDIMPACT to StageVariable LglStsCdSk                              IntegrateDev2               Reddy Sanam 2020-11-13
# MAGIC 
# MAGIC Goutham Kalidindi        2020-11-19        US-317229                     Added MEDIMPACT to stageVariable McPartdMedovcodeSk                     IntegrateDev2             Kalyan Neelam                    2020-11-19

# MAGIC Used in IdsDrugReversalBuild Job
# MAGIC The Hashed file is created in MedtrakDrugClmRvrslsHashedFileCrtExtr. The file contains the reversal claim ids and the ESI_GNRL_PRPS_AREA field which contains the ESI_REF_NO value of the original claim. The file is cleared in this job.
# MAGIC Read common record format file.
# MAGIC Writing Sequential File used in \"other\" job
# MAGIC Assign foreign keys and create default rows for UNK and NA
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType,
    NumericType,
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/Logging
# COMMAND ----------

# Retrieve job parameters
Source = get_widget_value('Source','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunCycle = get_widget_value('RunCycle','')
Logging = get_widget_value('Logging','')
RunID = get_widget_value('RunID','')

# Define schema for the sequential file "IdsDrugClmDrugExtr"
schema_IdsDrugClmDrugExtr = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("DRUG_CLM_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("NDC", StringType(), False),
    StructField("NDC_SK", IntegerType(), False),
    StructField("PRSCRB_PROV_DEA", StringType(), False),
    StructField("DRUG_CLM_DAW_CD", StringType(), False),
    StructField("DRUG_CLM_LGL_STTUS_CD", StringType(), False),
    StructField("DRUG_CLM_TIER_CD", StringType(), False),
    StructField("DRUG_CLM_VNDR_STTUS_CD", StringType(), False),
    StructField("CMPND_IN", StringType(), False),
    StructField("FRMLRY_IN", StringType(), False),
    StructField("GNRC_DRUG_IN", StringType(), False),
    StructField("MAIL_ORDER_IN", StringType(), False),
    StructField("MNTN_IN", StringType(), False),
    StructField("MAC_REDC_IN", StringType(), False),
    StructField("NON_FRMLRY_DRUG_IN", StringType(), False),
    StructField("SNGL_SRC_IN", StringType(), False),
    StructField("ADJ_DT", StringType(), False),
    StructField("FILL_DT", StringType(), False),
    StructField("RECON_DT", StringType(), False),
    StructField("DISPNS_FEE_AMT", DecimalType(38,10), False),
    StructField("HLTH_PLN_EXCL_AMT", DecimalType(38,10), False),
    StructField("HLTH_PLN_PD_AMT", DecimalType(38,10), False),
    StructField("INGR_CST_ALW_AMT", DecimalType(38,10), False),
    StructField("INGR_CST_CHRGD_AMT", DecimalType(38,10), False),
    StructField("INGR_SAV_AMT", DecimalType(38,10), False),
    StructField("MBR_DEDCT_EXCL_AMT", DecimalType(38,10), False),
    StructField("MBR_DIFF_PD_AMT", DecimalType(38,10), False),
    StructField("MBR_OOP_AMT", DecimalType(38,10), False),
    StructField("MBR_OOP_EXCL_AMT", DecimalType(38,10), False),
    StructField("OTHR_SAV_AMT", DecimalType(38,10), False),
    StructField("RX_ALW_QTY", DecimalType(38,10), False),
    StructField("RX_SUBMT_QTY", DecimalType(38,10), False),
    StructField("SLS_TAX_AMT", DecimalType(38,10), False),
    StructField("RX_ALW_DAYS_SUPL_QTY", IntegerType(), False),
    StructField("RX_ORIG_DAYS_SUPL_QTY", IntegerType(), False),
    StructField("PDX_NTWK_ID", StringType(), True),
    StructField("RX_NO", StringType(), True),
    StructField("RFL_NO", StringType(), True),
    StructField("VNDR_CLM_NO", StringType(), True),
    StructField("VNDR_PREAUTH_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), False),
    StructField("PROV_ID", StringType(), False),
    StructField("NDC_LABEL_NM", StringType(), False),
    StructField("DRUG_CLM_BNF_FRMLRY_POL_CD", StringType(), False),
    StructField("DRUG_CLM_BNF_RSTRCT_CD", StringType(), False),
    StructField("DRUG_CLM_MCPARTD_COVDRUG_CD", StringType(), False),
    StructField("DRUG_CLM_PRAUTH_CD", StringType(), False),
    StructField("MNDTRY_MAIL_ORDER_IN", StringType(), False),
    StructField("ADM_FEE_AMT", DecimalType(38,10), False),
    StructField("DRUG_CLM_BILL_BSS_CD", StringType(), False),
    StructField("AVG_WHLSL_PRICE_AMT", DecimalType(38,10), False),
    StructField("UCR_AMT", DecimalType(38,10), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("PDX_TYP", StringType(), False),
    StructField("INCNTV_FEE", DecimalType(38,10), False),
    StructField("SPEC_DRUG_IN", StringType(), True),
    StructField("SUBMT_PROD_ID_QLFR", StringType(), True),
    StructField("CNTNGNT_THER_FLAG", StringType(), True),
    StructField("CNTNGNT_THER_SCHD", StringType(), True),
    StructField("CLNT_PATN_PAY_ATRBD_PROD_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_NTWK_AMT", DecimalType(38,10), True),
    StructField("GNRC_PROD_IN", StringType(), True),
])

# Read the CSeqFileStage input
df_IdsDrugClmDrugExtr = (
    spark.read
    .format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsDrugClmDrugExtr)
    .load(f"{adls_path}/key/IdsDrugClmDrugExtr.DrugClmDrug.uniq")
)

# Read the hashed file "hf_medtrak_drgclm_rvrsl_hf" as parquet (Scenario C)
df_hf_medtrak_drgclm_rvrsl = spark.read.parquet(f"{adls_path}/hf_medtrak_drgclm_rvrsl.parquet")

# Alias and join (left join) for the Transformer Stage
df_join = df_IdsDrugClmDrugExtr.alias("Key").join(
    df_hf_medtrak_drgclm_rvrsl.alias("rvrsl_lkup"),
    F.col("Key.CLM_ID") == F.col("rvrsl_lkup.CLAIM_ID"),
    "left"
)

# Add job parameter as column for SRC_SYS_CD_SK usage in the Transformer
df_withparam = df_join.withColumn("SrcSysCdSk", F.lit(SrcSysCdSk))

# Build all Transformer Stage Variables on df_withparam
# Using user-defined helper functions directly
df_transformed = (
    df_withparam
    .withColumn("NDCSk", GetFkeyNDC(F.col("Key.DRUG_CLM_SK"), F.col("Key.NDC"), F.lit("X")))
    .withColumn("ClmSk", GetFkeyClm(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.col("Key.CLM_ID"), F.col("Logging")))
    .withColumn("DawCd", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("DRUG CLAIM DAW"), F.col("Key.DRUG_CLM_DAW_CD"), F.col("Logging")))
    .withColumn(
        "LglSttusCd",
        F.when(
            trim(F.col("Key.SRC_SYS_CD")).isin(
                "ESI","MEDIMPACT","WELLDYNERX","MCSOURCE","MCAID","MEDTRAK",
                "BCBSSC","BCA","BCBSA","SAVRX","LDI","CVS","OPTUMRX"
            ),
            GetFkeyCodes(
                F.lit("FDB"),
                F.col("Key.DRUG_CLM_SK"),
                F.lit("NDC DRUG ABUSE CONTROL"),
                trim(F.col("Key.DRUG_CLM_LGL_STTUS_CD")),
                F.col("Logging")
            )
        ).otherwise(
            GetFkeyCodes(
                F.col("Key.SRC_SYS_CD"),
                F.col("Key.DRUG_CLM_SK"),
                F.lit("DRUG CLAIM LEGAL STATUS"),
                F.col("Key.DRUG_CLM_LGL_STTUS_CD"),
                F.col("Logging")
            )
        )
    )
    .withColumn("TierCd", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("DRUG CLAIM TIER"), F.col("Key.DRUG_CLM_TIER_CD"), F.col("Logging")))
    .withColumn("VndrSttusCd", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("DRUG CLAIM VENDOR STATUS"), F.col("Key.DRUG_CLM_VNDR_STTUS_CD"), F.col("Logging")))
    .withColumn(
        "AdjustedClaim",
        F.when(
            (
                ((F.col("Key.CLM_STTUS_CD") == F.lit("11")) & (F.col("Key.SRC_SYS_CD") == F.lit("ARGUS"))) |
                ((F.col("Key.CLM_STTUS_CD") == F.lit("70")) & (F.col("Key.SRC_SYS_CD") == F.lit("ARGUS"))) |
                ((F.col("Key.CLM_STTUS_CD") == F.lit("R")) & (F.col("Key.SRC_SYS_CD") == F.lit("ESI")))   |
                ((F.col("Key.CLM_STTUS_CD") == F.lit("X")) & (F.col("Key.SRC_SYS_CD") == F.lit("OPTUMRX"))) |
                ((F.col("Key.CLM_STTUS_CD") == F.lit("X")) & (F.col("Key.SRC_SYS_CD") == F.lit("WELLDYNERX"))) |
                ((F.col("Key.CLM_STTUS_CD") == F.lit("R")) & (F.col("Key.SRC_SYS_CD") == F.lit("MEDTRAK")))   |
                (((F.col("Key.CLM_STTUS_CD") == F.lit("3")) | (F.col("Key.CLM_STTUS_CD") == F.lit("4"))) & (F.col("Key.SRC_SYS_CD") == F.lit("BCBSSC")))
            ),
            F.lit(True)
        ).otherwise(F.lit(False))
    )
    .withColumn(
        "AdjDtSk",
        F.when(
            trim(F.col("Key.SRC_SYS_CD")) == F.lit("MCSOURCE"),
            F.lit("NA")
        ).otherwise(
            GetFkeyDate(F.lit("IDS"), F.col("Key.DRUG_CLM_SK"), F.col("Key.ADJ_DT"), F.col("Logging"))
        )
    )
    .withColumn("DEASk", GetFkeyProvDEA(F.col("Key.DRUG_CLM_SK"), F.col("Key.PRSCRB_PROV_DEA"), F.lit("X")))
    .withColumn("ProvSk", GetFkeyProv(F.lit("NABP"), F.col("Key.DRUG_CLM_SK"), F.col("Key.PROV_ID"), F.lit("X")))
    .withColumn("FillDtSk", GetFkeyDate(F.lit("IDS"), F.col("Key.DRUG_CLM_SK"), F.col("Key.FILL_DT"), F.col("Logging")))
    .withColumn("BnfFrmlryPolCd", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("BENEFIT FORMULARY POLICY"), F.col("Key.DRUG_CLM_BNF_FRMLRY_POL_CD"), F.col("Logging")))
    .withColumn("BnfRstrctCd", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("BENEFIT RESTRICTION"), F.col("Key.DRUG_CLM_BNF_RSTRCT_CD"), F.col("Logging")))
    .withColumn(
        "McpartdCovdrugCd",
        F.when(
            F.col("Key.SRC_SYS_CD").isin("ESI","OPTUMRX","MEDIMPACT","ARGUS","MEDTRAK","BCBSSC","SAVRX","LDI","CVS"),
            GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("MEDICARE PART D COVERED DRUG"), F.col("Key.DRUG_CLM_MCPARTD_COVDRUG_CD"), F.col("Logging"))
        ).otherwise(F.lit(1))
    )
    .withColumn(
        "PdsNtwkCd",
        F.when(
            F.col("Key.SRC_SYS_CD") == F.lit("OPTUMRX"),
            F.when(
                (F.col("Key.PDX_NTWK_ID") == F.lit("NA")) | (F.col("Key.PDX_NTWK_ID") == F.lit("UNK")),
                GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("PHARMACY NETWORK"), F.col("Key.PDX_NTWK_ID"), F.col("Logging"))
            ).otherwise(
                GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("PHARMACY NETWORK"), F.expr("substring(Key.PDX_NTWK_ID, 4, 4)"), F.col("Logging"))
            )
        ).otherwise(
            GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("PHARMACY NETWORK"), F.col("Key.PDX_NTWK_ID"), F.col("Logging"))
        )
    )
    .withColumn(
        "PrauthCd",
        F.when(
            F.col("Key.SRC_SYS_CD").isin("OPTUMRX","ESI","ARGUS","MCAID","BCBSSC"),
            GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("PHARMACY PRIOR AUTH"), F.col("Key.DRUG_CLM_PRAUTH_CD"), F.col("Logging"))
        ).otherwise(F.lit(1))
    )
    .withColumn("ReconDtSk", GetFkeyDate(F.lit("IDS"), F.col("Key.DRUG_CLM_SK"), F.col("Key.RECON_DT"), F.col("Logging")))
    .withColumn("DrugClmBillBssCdSk", GetFkeyCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit(" DRUG CLAIM BILLED BASIS"), F.col("Key.DRUG_CLM_BILL_BSS_CD"), F.col("Logging")))
    .withColumn(
        "svDrugClmPrtlFillCdSk",
        F.when(
            F.col("Key.SRC_SYS_CD") == F.lit("OPTUMRX"),
            GetFkeyClctnDomainCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("DRUG PARTIAL FILL"), F.lit("OPTUMRX"), F.lit("DRUG PARTIAL FILL"), F.lit("IDS"), F.col("Key.PRTL_FILL_STTUS_CD"), F.col("Logging"))
        ).otherwise(
            GetFkeyClctnDomainCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("DRUG PARTIAL FILL"), F.lit("ESI"), F.lit("DRUG PARTIAL FILL"), F.lit("IDS"), F.col("Key.PRTL_FILL_STTUS_CD"), F.col("Logging"))
        )
    )
    .withColumn(
        "svDrugClmPdxTypCdSk",
        F.when(
            F.col("Key.SRC_SYS_CD") == F.lit("OPTUMRX"),
            GetFkeyClctnDomainCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("PHARMACY TYPE"), F.lit("OPTUMRX"), F.lit("PHARMACY TYPE"), F.lit("IDS"), F.col("Key.PDX_TYP"), F.col("Logging"))
        ).otherwise(
            GetFkeyClctnDomainCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("PHARMACY TYPE"), F.lit("ESI"), F.lit("PHARMACY TYPE"), F.lit("IDS"), F.col("Key.PDX_TYP"), F.col("Logging"))
        )
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(F.col("Key.DRUG_CLM_SK")))
    .withColumn(
        "svCntngntTherCdSk",
        F.when(
            F.col("Key.SRC_SYS_CD") == F.lit("OPTUMRX"),
            GetFkeyClctnDomainCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("CONTINGENT THERAPY"), F.lit("OPTUMRX"), F.lit("CONTINGENT THERAPY"), F.lit("IDS"), F.col("Key.CNTNGNT_THER_FLAG"), F.col("Logging"))
        ).otherwise(F.lit(0))
    )
    .withColumn(
        "svSubmtProdIdQlfrCdSk",
        F.when(
            F.col("Key.SRC_SYS_CD") == F.lit("OPTUMRX"),
            GetFkeyClctnDomainCodes(F.col("Key.SRC_SYS_CD"), F.col("Key.DRUG_CLM_SK"), F.lit("SUBMITTED PRODUCT IDENTIFIER QUALIFIER"), F.lit("OPTUMRX"), F.lit("SUBMITTED PRODUCT IDENTIFIER QUALIFIER"), F.lit("IDS"), F.col("Key.SUBMT_PROD_ID_QLFR"), F.col("Logging"))
        ).otherwise(F.lit(0))
    )
)

# Build multiple outputs from the transformer constraints

# 1) Output1 constraint: (Key.PASS_THRU_IN = "Y" or ErrCount = 0)
df_output1_pre = df_transformed.filter(
    (F.col("Key.PASS_THRU_IN") == F.lit("Y")) | (F.col("ErrCount") == 0)
)

# 2) DefaultUNK constraint: @INROWNUM = 1 but yields one row with fixed expressions
#    We simulate by taking limit(1) from df_transformed and then overriding all columns.
df_default_unk_pre = df_transformed.limit(1)
df_default_unk = df_default_unk_pre.select(
    F.lit(0).alias("DRUG_CLM_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.lit("UNK").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit(0).alias("NDC_SK"),
    F.lit(0).alias("PRSCRB_PROV_DEA_SK"),
    F.lit(0).alias("DRUG_CLM_DISPNS_AS_WRTN_CD_SK"),
    F.lit(0).alias("DRUG_CLM_LGL_STTUS_CD_SK"),
    F.lit(0).alias("DRUG_CLM_TIER_CD_SK"),
    F.lit(0).alias("DRUG_CLM_VNDR_STTUS_CD_SK"),
    F.lit("U").alias("CMPND_IN"),
    F.lit("U").alias("FRMLRY_IN"),
    F.lit("U").alias("GNRC_DRUG_IN"),
    F.lit("U").alias("MAIL_ORDER_IN"),
    F.lit("U").alias("MNTN_IN"),
    F.lit("U").alias("MAX_ALW_CST_REDC_IN"),
    F.lit("U").alias("NON_FRMLRY_DRUG_IN"),
    F.lit("U").alias("SNGL_SRC_IN"),
    F.lit("NA").alias("ADJ_DT_SK"),
    F.lit("NA").alias("FILL_DT_SK"),
    F.lit("NA").alias("RECON_DT_SK"),
    F.lit(0).alias("DISPNS_FEE_AMT"),
    F.lit(0).alias("HLTH_PLN_EXCL_AMT"),
    F.lit(0).alias("HLTH_PLN_PD_AMT"),
    F.lit(0).alias("INGR_CST_ALW_AMT"),
    F.lit(0).alias("INGR_CST_CHRGD_AMT"),
    F.lit(0).alias("INGR_SAV_AMT"),
    F.lit(0).alias("MBR_DEDCT_EXCL_AMT"),
    F.lit(0).alias("MBR_DIFF_PD_AMT"),
    F.lit(0).alias("MBR_OOP_AMT"),
    F.lit(0).alias("MBR_OOP_EXCL_AMT"),
    F.lit(0).alias("OTHR_SAV_AMT"),
    F.lit(0).alias("RX_ALW_QTY"),
    F.lit(0).alias("RX_SUBMT_QTY"),
    F.lit(0).alias("SLS_TAX_AMT"),
    F.lit(0).alias("RX_ALW_DAYS_SUPL_QTY"),
    F.lit(0).alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.lit("UNK").alias("PDX_NTWK_ID"),
    F.lit("UNK").alias("RX_NO"),
    F.lit("UNK").alias("RFL_NO"),
    F.lit("UNK").alias("VNDR_CLM_NO"),
    F.lit("UNK").alias("VNDR_PREAUTH_ID"),
    F.lit(0).alias("DRUG_CLM_BNF_FRMLRY_POL_CD_SK"),
    F.lit(0).alias("DRUG_CLM_BNF_RSTRCT_CD_SK"),
    F.lit(0).alias("DRUG_CLM_MCPARTD_COVDRUG_CD_SK"),
    F.lit(0).alias("DRUG_CLM_PDX_NTWK_CD_SK"),
    F.lit(0).alias("DRUG_CLM_PRAUTH_CD_SK"),
    F.lit("U").alias("MNDTRY_MAIL_ORDER_IN"),
    F.lit(0).alias("ADM_FEE_AMT"),
    F.lit(0).alias("DRUG_CLM_BILL_BSS_CD_SK"),
    F.lit(0).alias("AVG_WHLSL_PRICE_AMT"),
    F.lit(0).alias("UCR_AMT"),
    F.lit(0).alias("DRUG_CLM_PRTL_FILL_CD_SK"),
    F.lit(0).alias("DRUG_CLM_PDX_TYP_CD_SK"),
    F.lit(0).alias("INCNTV_FEE_AMT"),
    F.lit("U").alias("SPEC_DRUG_IN"),
    F.lit(1).alias("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.lit(1).alias("CNTNGNT_THER_CD_SK"),
    F.lit("NA").alias("CNTNGNT_THER_SCHD_ID"),
    F.lit(0).alias("MBR_NTWK_DIFF_PD_AMT"),
    F.lit(0).alias("MBR_SLS_TAX_AMT"),
    F.lit(0).alias("MBR_PRCS_FEE_AMT"),
    F.lit("UNK").alias("GNRC_PROD_IN")
)

# 3) DefaultNA constraint: @INROWNUM = 1 but yields one row with "NA"/"X"/"1"/"0" etc
df_default_na_pre = df_transformed.limit(1)
df_default_na = df_default_na_pre.select(
    F.lit(1).alias("DRUG_CLM_SK"),
    F.lit(1).alias("SRC_SYS_CD_SK"),
    F.lit("NA").alias("CLM_ID"),
    F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(1).alias("CLM_SK"),
    F.lit(1).alias("NDC_SK"),
    F.lit(1).alias("PRSCRB_PROV_DEA_SK"),
    F.lit(1).alias("DRUG_CLM_DISPNS_AS_WRTN_CD_SK"),
    F.lit(1).alias("DRUG_CLM_LGL_STTUS_CD_SK"),
    F.lit(1).alias("DRUG_CLM_TIER_CD_SK"),
    F.lit(1).alias("DRUG_CLM_VNDR_STTUS_CD_SK"),
    F.lit("X").alias("CMPND_IN"),
    F.lit("X").alias("FRMLRY_IN"),
    F.lit("X").alias("GNRC_DRUG_IN"),
    F.lit("X").alias("MAIL_ORDER_IN"),
    F.lit("X").alias("MNTN_IN"),
    F.lit("X").alias("MAX_ALW_CST_REDC_IN"),
    F.lit("X").alias("NON_FRMLRY_DRUG_IN"),
    F.lit("X").alias("SNGL_SRC_IN"),
    F.lit("NA").alias("ADJ_DT_SK"),
    F.lit("NA").alias("FILL_DT_SK"),
    F.lit("NA").alias("RECON_DT_SK"),
    F.lit(0).alias("DISPNS_FEE_AMT"),
    F.lit(0).alias("HLTH_PLN_EXCL_AMT"),
    F.lit(0).alias("HLTH_PLN_PD_AMT"),
    F.lit(0).alias("INGR_CST_ALW_AMT"),
    F.lit(0).alias("INGR_CST_CHRGD_AMT"),
    F.lit(0).alias("INGR_SAV_AMT"),
    F.lit(0).alias("MBR_DEDCT_EXCL_AMT"),
    F.lit(0).alias("MBR_DIFF_PD_AMT"),
    F.lit(0).alias("MBR_OOP_AMT"),
    F.lit(0).alias("MBR_OOP_EXCL_AMT"),
    F.lit(0).alias("OTHR_SAV_AMT"),
    F.lit(0).alias("RX_ALW_QTY"),
    F.lit(0).alias("RX_SUBMT_QTY"),
    F.lit(0).alias("SLS_TAX_AMT"),
    F.lit(0).alias("RX_ALW_DAYS_SUPL_QTY"),
    F.lit(0).alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.lit("NA").alias("PDX_NTWK_ID"),
    F.lit("NA").alias("RX_NO"),
    F.lit("NA").alias("RFL_NO"),
    F.lit("NA").alias("VNDR_CLM_NO"),
    F.lit("NA").alias("VNDR_PREAUTH_ID"),
    F.lit(1).alias("DRUG_CLM_BNF_FRMLRY_POL_CD_SK"),
    F.lit(1).alias("DRUG_CLM_BNF_RSTRCT_CD_SK"),
    F.lit(1).alias("DRUG_CLM_MCPARTD_COVDRUG_CD_SK"),
    F.lit(1).alias("DRUG_CLM_PDX_NTWK_CD_SK"),
    F.lit(1).alias("DRUG_CLM_PRAUTH_CD_SK"),
    F.lit("X").alias("MNDTRY_MAIL_ORDER_IN"),
    F.lit(0).alias("ADM_FEE_AMT"),
    F.lit(1).alias("DRUG_CLM_BILL_BSS_CD_SK"),
    F.lit(0).alias("AVG_WHLSL_PRICE_AMT"),
    F.lit(0).alias("UCR_AMT"),
    F.lit(1).alias("DRUG_CLM_PRTL_FILL_CD_SK"),
    F.lit(1).alias("DRUG_CLM_PDX_TYP_CD_SK"),
    F.lit(0).alias("INCNTV_FEE_AMT"),
    F.lit("X").alias("SPEC_DRUG_IN"),
    F.lit(1).alias("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.lit(1).alias("CNTNGNT_THER_CD_SK"),
    F.lit("NA").alias("CNTNGNT_THER_SCHD_ID"),
    F.lit(0).alias("MBR_NTWK_DIFF_PD_AMT"),
    F.lit(0).alias("MBR_SLS_TAX_AMT"),
    F.lit(0).alias("MBR_PRCS_FEE_AMT"),
    F.lit("NA").alias("GNRC_PROD_IN")
)

# Union for Collector1 stage (Output1, DefaultUNK, DefaultNA)
# They must have the same columns in the same order. We base the final schema on Output1's columns.
# We first build df_output1 with a final select for the same column structure.

df_output1 = df_output1_pre.select(
    F.when(
        F.col("Key.SRC_SYS_CD") == "OPTUMRX",
        F.col("ClmSk")
    ).otherwise(F.col("Key.DRUG_CLM_SK")).alias("DRUG_CLM_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("Key.CLM_ID").alias("CLM_ID"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ClmSk").alias("CLM_SK"),
    F.col("Key.NDC_SK").alias("NDC_SK"),
    F.col("DEASk").alias("PRSCRB_PROV_DEA_SK"),
    F.col("DawCd").alias("DRUG_CLM_DISPNS_AS_WRTN_CD_SK"),
    F.col("LglSttusCd").alias("DRUG_CLM_LGL_STTUS_CD_SK"),
    F.col("TierCd").alias("DRUG_CLM_TIER_CD_SK"),
    F.col("VndrSttusCd").alias("DRUG_CLM_VNDR_STTUS_CD_SK"),
    F.col("Key.CMPND_IN").alias("CMPND_IN"),
    F.col("Key.FRMLRY_IN").alias("FRMLRY_IN"),
    F.col("Key.GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
    F.col("Key.MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("Key.MNTN_IN").alias("MNTN_IN"),
    F.col("Key.MAC_REDC_IN").alias("MAX_ALW_CST_REDC_IN"),
    F.col("Key.NON_FRMLRY_DRUG_IN").alias("NON_FRMLRY_DRUG_IN"),
    F.col("Key.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("AdjDtSk").alias("ADJ_DT_SK"),
    F.col("FillDtSk").alias("FILL_DT_SK"),
    F.col("ReconDtSk").alias("RECON_DT_SK"),
    F.col("Key.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("Key.HLTH_PLN_EXCL_AMT").alias("HLTH_PLN_EXCL_AMT"),
    F.col("Key.HLTH_PLN_PD_AMT").alias("HLTH_PLN_PD_AMT"),
    F.col("Key.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("Key.INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
    F.col("Key.INGR_SAV_AMT").alias("INGR_SAV_AMT"),
    F.col("Key.MBR_DEDCT_EXCL_AMT").alias("MBR_DEDCT_EXCL_AMT"),
    F.col("Key.MBR_DIFF_PD_AMT").alias("MBR_DIFF_PD_AMT"),
    F.col("Key.MBR_OOP_AMT").alias("MBR_OOP_AMT"),
    F.col("Key.MBR_OOP_EXCL_AMT").alias("MBR_OOP_EXCL_AMT"),
    F.col("Key.OTHR_SAV_AMT").alias("OTHR_SAV_AMT"),
    F.col("Key.RX_ALW_QTY").alias("RX_ALW_QTY"),
    F.col("Key.RX_SUBMT_QTY").alias("RX_SUBMT_QTY"),
    F.col("Key.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
    F.col("Key.RX_ALW_DAYS_SUPL_QTY").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("Key.RX_ORIG_DAYS_SUPL_QTY").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.col("Key.PDX_NTWK_ID").alias("PDX_NTWK_ID"),
    F.col("Key.RX_NO").alias("RX_NO"),
    F.col("Key.RFL_NO").alias("RFL_NO"),
    F.col("Key.VNDR_CLM_NO").alias("VNDR_CLM_NO"),
    F.col("Key.VNDR_PREAUTH_ID").alias("VNDR_PREAUTH_ID"),
    F.col("BnfFrmlryPolCd").alias("DRUG_CLM_BNF_FRMLRY_POL_CD_SK"),
    F.col("BnfRstrctCd").alias("DRUG_CLM_BNF_RSTRCT_CD_SK"),
    F.col("McpartdCovdrugCd").alias("DRUG_CLM_MCPARTD_COVDRUG_CD_SK"),
    F.col("PdsNtwkCd").alias("DRUG_CLM_PDX_NTWK_CD_SK"),
    F.col("PrauthCd").alias("DRUG_CLM_PRAUTH_CD_SK"),
    F.col("Key.MNDTRY_MAIL_ORDER_IN").alias("MNDTRY_MAIL_ORDER_IN"),
    F.col("Key.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
    F.col("DrugClmBillBssCdSk").alias("DRUG_CLM_BILL_BSS_CD_SK"),
    F.col("Key.AVG_WHLSL_PRICE_AMT").alias("AVG_WHLSL_PRICE_AMT"),
    F.col("Key.UCR_AMT").alias("UCR_AMT"),
    F.col("svDrugClmPrtlFillCdSk").alias("DRUG_CLM_PRTL_FILL_CD_SK"),
    F.col("svDrugClmPdxTypCdSk").alias("DRUG_CLM_PDX_TYP_CD_SK"),
    F.col("Key.INCNTV_FEE").alias("INCNTV_FEE_AMT"),
    F.col("Key.SPEC_DRUG_IN").alias("SPEC_DRUG_IN"),
    F.col("svSubmtProdIdQlfrCdSk").alias("SUBMT_PROD_ID_QLFR_CD_SK"),
    F.col("svCntngntTherCdSk").alias("CNTNGNT_THER_CD_SK"),
    F.col("Key.CNTNGNT_THER_SCHD").alias("CNTNGNT_THER_SCHD_ID"),
    F.col("Key.CLNT_PATN_PAY_ATRBD_NTWK_AMT").alias("MBR_NTWK_DIFF_PD_AMT"),
    F.col("Key.CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT").alias("MBR_SLS_TAX_AMT"),
    F.col("Key.CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT").alias("MBR_PRCS_FEE_AMT"),
    F.col("Key.GNRC_PROD_IN").alias("GNRC_PROD_IN")
)

# The default_unk and default_na must match the same column names and order
df_default_unk = df_default_unk.select(df_output1.columns)
df_default_na = df_default_na.select(df_output1.columns)

# Union them together to simulate the collector
df_collector = df_output1.unionByName(df_default_unk).unionByName(df_default_na)

# 4) Recycle1 constraint: ErrCount > 0 -> "hf_recycle"
df_recycle1_pre = df_transformed.filter(F.col("ErrCount") > 0)
df_recycle1 = df_recycle1_pre.select(
    GetRecycleKey(F.col("Key.DRUG_CLM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Key.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Key.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Key.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Key.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Key.ERR_CT").alias("ERR_CT"),
    (F.col("Key.RECYCLE_CT")+F.lit(1)).alias("RECYCLE_CT"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("Key.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("Key.CLM_ID").alias("CLM_ID"),
    F.col("Key.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Key.NDC").alias("NDC"),
    F.col("Key.NDC_SK").alias("NDC_SK"),
    F.col("Key.PRSCRB_PROV_DEA").alias("PRSCRB_PROV_DEA"),
    F.col("Key.DRUG_CLM_DAW_CD").alias("DRUG_CLM_DAW_CD"),
    F.col("Key.DRUG_CLM_LGL_STTUS_CD").alias("DRUG_CLM_LGL_STTUS_CD"),
    F.col("Key.DRUG_CLM_TIER_CD").alias("DRUG_CLM_TIER_CD"),
    F.col("Key.DRUG_CLM_VNDR_STTUS_CD").alias("DRUG_CLM_VNDR_STTUS_CD"),
    F.col("Key.CMPND_IN").alias("CMPND_IN"),
    F.col("Key.FRMLRY_IN").alias("FRMLRY_IN"),
    F.col("Key.GNRC_DRUG_IN").alias("GNRC_DRUG_IN"),
    F.col("Key.MAIL_ORDER_IN").alias("MAIL_ORDER_IN"),
    F.col("Key.MNTN_IN").alias("MNTN_IN"),
    F.col("Key.MAC_REDC_IN").alias("MAC_REDC_IN"),
    F.col("Key.NON_FRMLRY_DRUG_IN").alias("NON_FRMLRY_DRUG_IN"),
    F.col("Key.SNGL_SRC_IN").alias("SNGL_SRC_IN"),
    F.col("Key.ADJ_DT").alias("ADJ_DT"),
    F.col("Key.FILL_DT").alias("FILL_DT"),
    F.col("Key.RECON_DT").alias("RECON_DT"),
    F.col("Key.DISPNS_FEE_AMT").alias("DISPNS_FEE_AMT"),
    F.col("Key.HLTH_PLN_EXCL_AMT").alias("HLTH_PLN_EXCL_AMT"),
    F.col("Key.HLTH_PLN_PD_AMT").alias("HLTH_PLN_PD_AMT"),
    F.col("Key.INGR_CST_ALW_AMT").alias("INGR_CST_ALW_AMT"),
    F.col("Key.INGR_CST_CHRGD_AMT").alias("INGR_CST_CHRGD_AMT"),
    F.col("Key.INGR_SAV_AMT").alias("INGR_SAV_AMT"),
    F.col("Key.MBR_DEDCT_EXCL_AMT").alias("MBR_DEDCT_EXCL_AMT"),
    F.col("Key.MBR_DIFF_PD_AMT").alias("MBR_DIFF_PD_AMT"),
    F.col("Key.MBR_OOP_AMT").alias("MBR_OOP_AMT"),
    F.col("Key.MBR_OOP_EXCL_AMT").alias("MBR_OOP_EXCL_AMT"),
    F.col("Key.OTHR_SAV_AMT").alias("OTHR_SAV_AMT"),
    F.col("Key.SLS_TAX_AMT").alias("SLS_TAX_AMT"),
    F.col("Key.RX_ALW_QTY").alias("RX_ALW_QTY"),
    F.col("Key.RX_SUBMT_QTY").alias("RX_SUBMT_QTY"),
    F.col("Key.RX_ALW_DAYS_SUPL_QTY").alias("RX_ALW_DAYS_SUPL_QTY"),
    F.col("Key.RX_ORIG_DAYS_SUPL_QTY").alias("RX_ORIG_DAYS_SUPL_QTY"),
    F.col("Key.PDX_NTWK_ID").alias("PDX_NTWK_ID"),
    F.col("Key.RX_NO").alias("RX_NO"),
    F.col("Key.RFL_NO").alias("RFL_NO"),
    F.col("Key.VNDR_CLM_NO").alias("VNDR_CLM_NO"),
    F.col("Key.VNDR_PREAUTH_ID").alias("VNDR_PREAUTH_ID"),
    F.col("Key.CLM_STTUS_CD").alias("CLM_STTUS_CD"),
    F.col("Key.PROV_ID").alias("PROV_ID"),
    F.col("Key.NDC_LABEL_NM").alias("NDC_LABEL_NM"),
    F.col("Key.DRUG_CLM_BNF_FRMLRY_POL_CD").alias("DRUG_CLM_BNF_FRMLRY_POL_CD"),
    F.col("Key.DRUG_CLM_BNF_RSTRCT_CD").alias("DRUG_CLM_BNF_RSTRCT_CD"),
    F.col("Key.DRUG_CLM_MCPARTD_COVDRUG_CD").alias("DRUG_CLM_MCPARTD_COVDRUG_CD"),
    F.col("Key.DRUG_CLM_PRAUTH_CD").alias("DRUG_CLM_PRAUTH_CD"),
    F.col("Key.MNDTRY_MAIL_ORDER_IN").alias("MNDTRY_MAIL_ORDER_IN"),
    F.col("Key.ADM_FEE_AMT").alias("ADM_FEE_AMT"),
    F.col("Key.DRUG_CLM_BILL_BSS_CD").alias("DRUG_CLM_BILL_BSS_CD"),
    F.col("Key.AVG_WHLSL_PRICE_AMT").alias("AVG_WHLSL_PRICE_AMT"),
    F.col("Key.UCR_AMT").alias("UCR_AMT")
)

# 5) adj_claims1 constraint: AdjustedClaim -> "W_DRUG_CLM"
df_adj_claims1_pre = df_transformed.filter(F.col("AdjustedClaim") == True)
df_adj_claims1 = df_adj_claims1_pre.select(
    F.col("Key.DRUG_CLM_SK").alias("DRUG_CLM_SK"),
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("ClmSk").alias("CLM_SK"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.RX_NO").alias("RX_NO"),
    F.col("FillDtSk").alias("FILL_DT_SK"),  # note the stage used Key.FILL_DT as char(10) but we store the same in FillDtSk
    F.col("ProvSk").alias("PROV_SK"),
    F.col("Key.CLM_ID").alias("CLM_ID"),
    F.when(
        (F.col("Key.SRC_SYS_CD") == F.lit("MEDTRAK")) &
        (F.col("rvrsl_lkup.ESI_CLNT_GNRL_PRPS_AREA").isNull()),
        F.lit("UNK")
    ).otherwise(
        F.when(
            (F.col("Key.SRC_SYS_CD") == F.lit("MEDTRAK")),
            F.col("rvrsl_lkup.ESI_CLNT_GNRL_PRPS_AREA")
        ).otherwise(
            F.col("Key.VNDR_CLM_NO")
        )
    ).alias("VNDR_CLM_NO")
)

# 6) TempFile constraint: Key.SRC_SYS_CD in ('ARGUS','ESI','OPTUMRX')
df_tempfile_pre = df_transformed.filter(
    F.col("Key.SRC_SYS_CD").isin("ARGUS","ESI","OPTUMRX")
)
df_tempfile = df_tempfile_pre.select(
    F.col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
    F.col("Key.CLM_ID").alias("CLM_ID"),
    F.col("Key.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Key.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# Now handle final writes, applying rpad for char/varchar columns in each final output.

# 6a) Collector1 => DrugClmEdit => writes to DRUG_CLM.#Source#.dat
# We apply rpad only where length is specified as char(...) or we see a length in the job definitions.
# The Collector final has these columns with known lengths (from the stage definition):
#  - CMPND_IN char(1), FRMLRY_IN char(1), GNRC_DRUG_IN char(1), MAIL_ORDER_IN char(1), MNTN_IN char(1)
#  - MAX_ALW_CST_REDC_IN char(1), NON_FRMLRY_DRUG_IN char(1), SNGL_SRC_IN char(1)
#  - ADJ_DT_SK char(10), FILL_DT_SK char(10), RECON_DT_SK char(10)
#  - MNDTRY_MAIL_ORDER_IN char(1), SPEC_DRUG_IN char(1)
# Others are undefined or various numeric or char with no length specified.

df_collector_rpad = (
    df_collector
    .withColumn("CMPND_IN", F.rpad(F.col("CMPND_IN"), 1, " "))
    .withColumn("FRMLRY_IN", F.rpad(F.col("FRMLRY_IN"), 1, " "))
    .withColumn("GNRC_DRUG_IN", F.rpad(F.col("GNRC_DRUG_IN"), 1, " "))
    .withColumn("MAIL_ORDER_IN", F.rpad(F.col("MAIL_ORDER_IN"), 1, " "))
    .withColumn("MNTN_IN", F.rpad(F.col("MNTN_IN"), 1, " "))
    .withColumn("MAX_ALW_CST_REDC_IN", F.rpad(F.col("MAX_ALW_CST_REDC_IN"), 1, " "))
    .withColumn("NON_FRMLRY_DRUG_IN", F.rpad(F.col("NON_FRMLRY_DRUG_IN"), 1, " "))
    .withColumn("SNGL_SRC_IN", F.rpad(F.col("SNGL_SRC_IN"), 1, " "))
    .withColumn("ADJ_DT_SK", F.rpad(F.col("ADJ_DT_SK"), 10, " "))
    .withColumn("FILL_DT_SK", F.rpad(F.col("FILL_DT_SK"), 10, " "))
    .withColumn("RECON_DT_SK", F.rpad(F.col("RECON_DT_SK"), 10, " "))
    .withColumn("MNDTRY_MAIL_ORDER_IN", F.rpad(F.col("MNDTRY_MAIL_ORDER_IN"), 1, " "))
    .withColumn("SPEC_DRUG_IN", F.rpad(F.col("SPEC_DRUG_IN"), 1, " "))
)

# Write to DRUG_CLM.#Source#.dat (no header, quote = ")
write_files(
    df_collector_rpad.select(df_collector.columns),
    f"{adls_path}/load/DRUG_CLM.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 6b) Recycle => "hf_recycle" => scenario C => we write parquet => rpad char columns if any
# Looking at the Recycle columns, we see possible char(1), char(10), char(12) from the job definition:
#  INSRT_UPDT_CD char(10), DISCARD_IN char(1), PASS_THRU_IN char(1), ADJ_DT char(10), FILL_DT char(10), RECON_DT char(10),
#  CMPND_IN char(1), FRMLRY_IN char(1), GNRC_DRUG_IN char(1), MAIL_ORDER_IN char(1), MNTN_IN char(1), MAC_REDC_IN char(1),
#  NON_FRMLRY_DRUG_IN char(1), SNGL_SRC_IN char(1), PROV_ID char(12), DRUG_CLM_BNF_FRMLRY_POL_CD char(1), DRUG_CLM_BNF_RSTRCT_CD char(1),
#  DRUG_CLM_MCPARTD_COVDRUG_CD char(1), DRUG_CLM_PRAUTH_CD char(1), MNDTRY_MAIL_ORDER_IN char(1)
df_recycle1_rpad = (
    df_recycle1
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("ADJ_DT", F.rpad(F.col("ADJ_DT"), 10, " "))
    .withColumn("FILL_DT", F.rpad(F.col("FILL_DT"), 10, " "))
    .withColumn("RECON_DT", F.rpad(F.col("RECON_DT"), 10, " "))
    .withColumn("CMPND_IN", F.rpad(F.col("CMPND_IN"), 1, " "))
    .withColumn("FRMLRY_IN", F.rpad(F.col("FRMLRY_IN"), 1, " "))
    .withColumn("GNRC_DRUG_IN", F.rpad(F.col("GNRC_DRUG_IN"), 1, " "))
    .withColumn("MAIL_ORDER_IN", F.rpad(F.col("MAIL_ORDER_IN"), 1, " "))
    .withColumn("MNTN_IN", F.rpad(F.col("MNTN_IN"), 1, " "))
    .withColumn("MAC_REDC_IN", F.rpad(F.col("MAC_REDC_IN"), 1, " "))
    .withColumn("NON_FRMLRY_DRUG_IN", F.rpad(F.col("NON_FRMLRY_DRUG_IN"), 1, " "))
    .withColumn("SNGL_SRC_IN", F.rpad(F.col("SNGL_SRC_IN"), 1, " "))
    .withColumn("PROV_ID", F.rpad(F.col("PROV_ID"), 12, " "))
    .withColumn("DRUG_CLM_BNF_FRMLRY_POL_CD", F.rpad(F.col("DRUG_CLM_BNF_FRMLRY_POL_CD"), 1, " "))
    .withColumn("DRUG_CLM_BNF_RSTRCT_CD", F.rpad(F.col("DRUG_CLM_BNF_RSTRCT_CD"), 1, " "))
    .withColumn("DRUG_CLM_MCPARTD_COVDRUG_CD", F.rpad(F.col("DRUG_CLM_MCPARTD_COVDRUG_CD"), 1, " "))
    .withColumn("DRUG_CLM_PRAUTH_CD", F.rpad(F.col("DRUG_CLM_PRAUTH_CD"), 1, " "))
    .withColumn("MNDTRY_MAIL_ORDER_IN", F.rpad(F.col("MNDTRY_MAIL_ORDER_IN"), 1, " "))
)

# Write "hf_recycle.parquet" (Scenario C)
write_files(
    df_recycle1_rpad,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_parqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# 6c) adj_claims1 => W_DRUG_CLM => "W_DRUG_CLM.dat" => CSeqFileStage
# The stage has columns:
#  DRUG_CLM_SK, SRC_SYS_CD_SK, CLM_SK, SRC_SYS_CD, RX_NO, FILL_DT_SK char(10), PROV_SK, CLM_ID, VNDR_CLM_NO
#  We see FILL_DT_SK is char(10).
df_adj_claims1_rpad = (
    df_adj_claims1
    .withColumn("FILL_DT_SK", F.rpad(F.col("FILL_DT_SK"), 10, " "))
)

write_files(
    df_adj_claims1_rpad,
    f"{adls_path}/load/W_DRUG_CLM.dat",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# 6d) TempFile => W_WEBDM_ETL_DRVR => "W_WEBDM_ETL_DRVR.dat.Drug.#RunID#"
# Columns: SRC_SYS_CD_SK (pk), CLM_ID (pk), SRC_SYS_CD, LAST_UPDT_RUN_CYC_EXCTN_SK
# No explicit char length info given, so no rpad needed except we do not see a mention of char(...) lengths here.

write_files(
    df_tempfile,
    f"{adls_path}/load/W_WEBDM_ETL_DRVR.dat.Drug.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)