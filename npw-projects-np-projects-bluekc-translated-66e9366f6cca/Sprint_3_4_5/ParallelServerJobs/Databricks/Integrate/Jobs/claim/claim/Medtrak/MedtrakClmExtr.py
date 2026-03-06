# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  MedtrakDrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  Reads the MedtrakDrugClm_Land.dat file and puts the data into the claim common record format and takes it through primary key process using Shared Container ClmPkey
# MAGIC     
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                        --------------------     ------------------------      -----------------------------------------------------------------------------------------          --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam                   2010-12-22        4616                     Initial Programming                                                                       IntegrateNewDevl         Steph Goddard           12/23/2010
# MAGIC Raja Gummadi                   2012-05-08         4896                     Added new Field - CLM_SUBMT_ICD_VRSN_CD                      IntegrateNewDevl          SAndrew                      2012-05-16
# MAGIC Raja Gummadi                   2012-07-23         TTR 1330            Changed RX_NO field size from 9 to 20 in input file                      IntegrateWrhsDevl        Bhoomi Dasari           08/08/2012 
# MAGIC 
# MAGIC Manasa Andru                   2015-01-14         TFS - 9791          Changed the Source System code for ClmSttusCdSk and            IntegrateNewDevl           Kalyan Neelam          2015-01-16
# MAGIC                                                                                                       ClmCatCdSk in the GetFkeyCodes routine     
# MAGIC Jaideep Mankala              2018-03-28          5828                   Changed defaults Indicator field values from 'X' to 'N' in stage        IntegrateDev2               Kalyan Neelam          2018-03-29
# MAGIC 						BusinessRules
# MAGIC 
# MAGIC Mohan Karnati                 2019-06-06        ADO-73034             Adding CLM_TXNMY_CD filed in alpha_pfx stage and                 IntegrateDev1	
# MAGIC                                                                                                    passing it till MedtrakClmExtr stage
# MAGIC 
# MAGIC Giri  Mallavaram    2020-04-06         PBM Changes       Added BILL_PAYMT_EXCL_IN to be in sync with changes to CLM_PKey Container        IntegrateDev2         Kalyan Neelam    2020-04-07

# MAGIC Read the MEDTRAK file created from MedtrakClmLand
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
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import (
    col, lit, when, isnull, concat, substring, rpad, length, udf
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrentDate = get_widget_value('CurrentDate','2020-04-08')
SrcSysCd = get_widget_value('SrcSysCd','MEDTRAK')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1')
RunCycle = get_widget_value('RunCycle','1')
RunID = get_widget_value('RunID','20180213')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmPK
# COMMAND ----------

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

query_mbr = f"""
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
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_mbr)
    .load()
)

df_mbr_lkup = df_mbr.dropDuplicates(["MBR_UNIQ_KEY"])

query_sub_alpha_pfx = f"""
SELECT SUB_UNIQ_KEY,
       ALPHA_PFX_CD
FROM {IDSOwner}.MBR MBR,
     {IDSOwner}.SUB SUB,
     {IDSOwner}.ALPHA_PFX PFX,
     {IDSOwner}.W_DRUG_ENR DRUG
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
  AND MBR.SUB_SK = SUB.SUB_SK
  AND SUB.ALPHA_PFX_SK = PFX.ALPHA_PFX_SK
"""

df_sub_alpha_pfx = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_sub_alpha_pfx)
    .load()
)

df_sub_alpha_pfx_lkup = df_sub_alpha_pfx.dropDuplicates(["SUB_UNIQ_KEY"])

query_mbr_enroll = f"""
SELECT DRUG.CLM_ID,
       CLS.CLS_ID,
       PLN.CLS_PLN_ID,
       SUBGRP.SUBGRP_ID,
       CAT.EXPRNC_CAT_CD,
       LOB.FNCL_LOB_CD,
       CMPNT.PROD_ID
FROM {IDSOwner}.W_DRUG_ENR DRUG,
     {IDSOwner}.MBR_ENR MBR,
     {IDSOwner}.CD_MPPNG MAP1,
     {IDSOwner}.CLS CLS,
     {IDSOwner}.SUBGRP SUBGRP,
     {IDSOwner}.CLS_PLN PLN,
     {IDSOwner}.CD_MPPNG MAP2,
     {IDSOwner}.PROD_CMPNT CMPNT,
     {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT,
     {IDSOwner}.EXPRNC_CAT CAT,
     {IDSOwner}.FNCL_LOB LOB
WHERE DRUG.MBR_UNIQ_KEY = MBR.MBR_UNIQ_KEY
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
       SELECT MAX ( CMPNT2.PROD_CMPNT_EFF_DT_SK )
       FROM {IDSOwner}.PROD_CMPNT CMPNT2
       WHERE CMPNT.PROD_SK = CMPNT2.PROD_SK
         AND CMPNT.PROD_CMPNT_TYP_CD_SK = CMPNT2.PROD_CMPNT_TYP_CD_SK
         AND DRUG.FILL_DT_SK between CMPNT2.PROD_CMPNT_EFF_DT_SK AND CMPNT2.PROD_CMPNT_TERM_DT_SK
  )
  AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT.PROD_CMPNT_PFX_ID
  AND DRUG.FILL_DT_SK between BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT.PROD_BILL_CMPNT_TERM_DT_SK
  AND BILL_CMPNT.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
  AND BILL_CMPNT.PROD_BILL_CMPNT_EFF_DT_SK= (
       SELECT MAX ( BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK )
       FROM {IDSOwner}.PROD_BILL_CMPNT BILL_CMPNT2
       WHERE BILL_CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
         AND CMPNT.PROD_CMPNT_PFX_ID = BILL_CMPNT2.PROD_CMPNT_PFX_ID
         AND BILL_CMPNT.PROD_BILL_CMPNT_ID = BILL_CMPNT2.PROD_BILL_CMPNT_ID
         AND BILL_CMPNT2.PROD_BILL_CMPNT_ID IN ('MED', 'MED1')
         AND DRUG.FILL_DT_SK between BILL_CMPNT2.PROD_BILL_CMPNT_EFF_DT_SK AND BILL_CMPNT2.PROD_BILL_CMPNT_TERM_DT_SK
  )
  AND BILL_CMPNT.EXPRNC_CAT_SK = CAT.EXPRNC_CAT_SK
  AND BILL_CMPNT.FNCL_LOB_SK = LOB.FNCL_LOB_SK
"""

df_mbr_enroll = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", query_mbr_enroll)
    .load()
)

df_mbr_enr_lkup = df_mbr_enroll.dropDuplicates(["CLM_ID"])

schema_MedtrakClmLand = StructType([
    StructField("RCRD_ID", DoubleType(), nullable=False),
    StructField("CLAIM_ID", StringType(), nullable=False),
    StructField("PRCSR_NO", DoubleType(), nullable=False),
    StructField("MEM_CK_KEY", IntegerType(), nullable=False),
    StructField("BTCH_NO", DoubleType(), nullable=False),
    StructField("PDX_NO", StringType(), nullable=False),
    StructField("RX_NO", DoubleType(), nullable=False),
    StructField("DT_FILLED", StringType(), nullable=False),
    StructField("NDC_NO", DoubleType(), nullable=False),
    StructField("DRUG_DESC", StringType(), nullable=False),
    StructField("NEW_RFL_CD", DoubleType(), nullable=False),
    StructField("METRIC_QTY", DoubleType(), nullable=False),
    StructField("DAYS_SUPL", DoubleType(), nullable=False),
    StructField("BSS_OF_CST_DTRM", StringType(), nullable=False),
    StructField("INGR_CST", DoubleType(), nullable=False),
    StructField("DISPNS_FEE", DoubleType(), nullable=False),
    StructField("COPAY_AMT", DoubleType(), nullable=False),
    StructField("SLS_TAX", DoubleType(), nullable=False),
    StructField("AMT_BILL", DoubleType(), nullable=False),
    StructField("PATN_FIRST_NM", StringType(), nullable=False),
    StructField("PATN_LAST_NM", StringType(), nullable=False),
    StructField("DOB", DoubleType(), nullable=False),
    StructField("SEX_CD", DoubleType(), nullable=False),
    StructField("CARDHLDR_ID_NO", StringType(), nullable=False),
    StructField("RELSHP_CD", DoubleType(), nullable=False),
    StructField("GRP_NO", StringType(), nullable=False),
    StructField("HOME_PLN", StringType(), nullable=False),
    StructField("HOST_PLN", DoubleType(), nullable=False),
    StructField("PRESCRIBER_ID", StringType(), nullable=False),
    StructField("DIAG_CD", StringType(), nullable=False),
    StructField("CARDHLDR_FIRST_NM", StringType(), nullable=False),
    StructField("CARDHLDR_LAST_NM", StringType(), nullable=False),
    StructField("PRAUTH_NO", DoubleType(), nullable=False),
    StructField("PA_MC_SC_NO", StringType(), nullable=False),
    StructField("CUST_LOC", DoubleType(), nullable=False),
    StructField("RESUB_CYC_CT", DoubleType(), nullable=False),
    StructField("DT_RX_WRTN", DoubleType(), nullable=False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), nullable=False),
    StructField("PRSN_CD", StringType(), nullable=False),
    StructField("OTHR_COV_CD", DoubleType(), nullable=False),
    StructField("ELIG_CLRFCTN_CD", DoubleType(), nullable=False),
    StructField("CMPND_CD", DoubleType(), nullable=False),
    StructField("NO_OF_RFLS_AUTH", DoubleType(), nullable=False),
    StructField("LVL_OF_SVC", DoubleType(), nullable=False),
    StructField("RX_ORIG_CD", DoubleType(), nullable=False),
    StructField("RX_DENIAL_CLRFCTN", DoubleType(), nullable=False),
    StructField("PRI_PRESCRIBER", StringType(), nullable=False),
    StructField("CLNC_ID_NO", DoubleType(), nullable=False),
    StructField("DRUG_TYP", DoubleType(), nullable=False),
    StructField("PRESCRIBER_LAST_NM", StringType(), nullable=False),
    StructField("POSTAGE_AMT_CLMED", DoubleType(), nullable=False),
    StructField("UNIT_DOSE_IN", DoubleType(), nullable=False),
    StructField("OTHR_PAYOR_AMT", DoubleType(), nullable=False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DoubleType(), nullable=False),
    StructField("FULL_AWP", DoubleType(), nullable=False),
    StructField("EXPNSN_AREA", StringType(), nullable=False),
    StructField("MSTR_CAR", StringType(), nullable=False),
    StructField("SUB_CAR", StringType(), nullable=False),
    StructField("CLM_TYP", StringType(), nullable=False),
    StructField("ESI_SUB_GRP", StringType(), nullable=False),
    StructField("PLN_DSGNR", StringType(), nullable=False),
    StructField("ADJDCT_DT", StringType(), nullable=False),
    StructField("ADMIN_FEE", DoubleType(), nullable=False),
    StructField("CAP_AMT", DoubleType(), nullable=False),
    StructField("INGR_CST_SUB", DoubleType(), nullable=False),
    StructField("MBR_NON_COPAY_AMT", DoubleType(), nullable=False),
    StructField("MBR_PAY_CD", StringType(), nullable=False),
    StructField("INCNTV_FEE", DoubleType(), nullable=False),
    StructField("CLM_ADJ_AMT", DoubleType(), nullable=False),
    StructField("CLM_ADJ_CD", StringType(), nullable=False),
    StructField("FRMLRY_FLAG", StringType(), nullable=False),
    StructField("GNRC_CLS_NO", StringType(), nullable=False),
    StructField("THRPTC_CLS_AHFS", StringType(), nullable=False),
    StructField("PDX_TYP", StringType(), nullable=False),
    StructField("BILL_BSS_CD", StringType(), nullable=False),
    StructField("USL_AND_CUST_CHRG", DoubleType(), nullable=False),
    StructField("PD_DT", StringType(), nullable=False),
    StructField("BNF_CD", StringType(), nullable=False),
    StructField("DRUG_STRG", StringType(), nullable=False),
    StructField("ORIG_MBR", StringType(), nullable=False),
    StructField("DT_OF_INJURY", DoubleType(), nullable=False),
    StructField("FEE_AMT", DoubleType(), nullable=False),
    StructField("ESI_REF_NO", StringType(), nullable=False),
    StructField("CLNT_CUST_ID", StringType(), nullable=False),
    StructField("PLN_TYP", StringType(), nullable=False),
    StructField("ESI_ADJDCT_REF_NO", DoubleType(), nullable=False),
    StructField("ESI_ANCLRY_AMT", DoubleType(), nullable=False),
    StructField("ESI_CLNT_GNRL_PRPS_AREA", StringType(), nullable=True),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("SUBGRP_ID", StringType(), nullable=False),
    StructField("CLS_PLN_ID", StringType(), nullable=False),
    StructField("PAID_DATE", StringType(), nullable=False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), nullable=False),
    StructField("ESI_BILL_DT", DoubleType(), nullable=False),
    StructField("FSA_VNDR_CD", StringType(), nullable=False),
    StructField("PICA_DRUG_CD", StringType(), nullable=False),
    StructField("AMT_CLMED", DoubleType(), nullable=False),
    StructField("AMT_DSALW", DoubleType(), nullable=False),
    StructField("FED_DRUG_CLS_CD", StringType(), nullable=False),
    StructField("DEDCT_AMT", DoubleType(), nullable=False),
    StructField("BNF_COPAY_100", StringType(), nullable=False),
    StructField("CLM_PRCS_TYP", StringType(), nullable=False),
    StructField("INDEM_HIER_TIER_NO", DoubleType(), nullable=False),
    StructField("FLR", StringType(), nullable=False),
    StructField("MCARE_D_COV_DRUG", StringType(), nullable=False),
    StructField("RETRO_LICS_CD", StringType(), nullable=False),
    StructField("RETRO_LICS_AMT", DoubleType(), nullable=False),
    StructField("LICS_SBSDY_AMT", DoubleType(), nullable=False),
    StructField("MED_B_DRUG", StringType(), nullable=False),
    StructField("MED_B_CLM", StringType(), nullable=False),
    StructField("PRESCRIBER_QLFR", StringType(), nullable=False),
    StructField("PRESCRIBER_ID_NPI", StringType(), nullable=False),
    StructField("PDX_QLFR", StringType(), nullable=False),
    StructField("PDX_ID_NPI", StringType(), nullable=False),
    StructField("HRA_APLD_AMT", DoubleType(), nullable=False),
    StructField("ESI_THER_CLS", DoubleType(), nullable=False),
    StructField("HIC_NO", StringType(), nullable=False),
    StructField("HRA_FLAG", StringType(), nullable=False),
    StructField("DOSE_CD", DoubleType(), nullable=False),
    StructField("LOW_INCM", StringType(), nullable=False),
    StructField("RTE_OF_ADMIN", StringType(), nullable=False),
    StructField("DEA_SCHD", DoubleType(), nullable=False),
    StructField("COPAY_BNF_OPT", DoubleType(), nullable=False),
    StructField("GNRC_PROD_IN_GPI", DoubleType(), nullable=False),
    StructField("PRESCRIBER_SPEC", StringType(), nullable=False),
    StructField("VAL_CD", StringType(), nullable=False),
    StructField("PRI_CARE_PDX", StringType(), nullable=False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), nullable=False),
    StructField("FLR3", StringType(), nullable=False),
    StructField("PSL_FMLY_MET_AMT", DoubleType(), nullable=False),
    StructField("PSL_MBR_MET_AMT", DoubleType(), nullable=False),
    StructField("PSL_FMLY_AMT", DoubleType(), nullable=False),
    StructField("DED_FMLY_MET_AMT", DoubleType(), nullable=False),
    StructField("DED_FMLY_AMT", DoubleType(), nullable=False),
    StructField("MOPS_FMLY_AMT", DoubleType(), nullable=False),
    StructField("MOPS_FMLY_MET_AMT", DoubleType(), nullable=False),
    StructField("MOPS_MBR_MET_AMT", DoubleType(), nullable=False),
    StructField("DED_MBR_MET_AMT", DoubleType(), nullable=False),
    StructField("PSL_APLD_AMT", DoubleType(), nullable=False),
    StructField("MOPS_APLD_AMT", DoubleType(), nullable=False),
    StructField("PAR_PDX_IND", StringType(), nullable=False),
    StructField("COPAY_PCT_AMT", DoubleType(), nullable=False),
    StructField("COPAY_FLAT_AMT", DoubleType(), nullable=False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), nullable=False),
    StructField("FLR4", StringType(), nullable=False)
])

df_Medtrak = (
    spark.read.format("csv")
    .option("sep", ",")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_MedtrakClmLand)
    .load(f"{adls_path}/verified/MedtrakDrugClm_Land.dat.{RunID}")
)

df_BusinessRules_pre = (
    df_Medtrak.alias("Medtrak")
    .join(
        df_mbr_lkup.alias("mbr_lkup"),
        col("Medtrak.MEM_CK_KEY") == col("mbr_lkup.MBR_UNIQ_KEY"),
        "left"
    )
    .withColumn(
        "svAdjustedClaim",
        when(
            trim(col("Medtrak.CLM_TYP")).substr(0, 1) == lit("R"),
            True
        ).otherwise(False)
    )
    .withColumn(
        "svSubCk",
        when(
            isnull(col("mbr_lkup.MBR_UNIQ_KEY")),
            lit(0)
        ).otherwise(col("mbr_lkup.SUB_UNIQ_KEY"))
    )
    .withColumn(
        "svMbrAge",
        AGE(col("Medtrak.DOB"), col("Medtrak.DT_FILLED"))
    )
    .withColumn(
        "svJulianDt",
        FORMAT_DATE(col("Medtrak.DT_FILLED"), "DATE", "DATE", "JULIAN")
    )
    .withColumn(
        "svAdjustingFromCLmID",
        when(
            col("svAdjustedClaim"),
            concat(
                substring(col("svJulianDt"), 3, 1000),
                trim(col("Medtrak.ESI_CLNT_GNRL_PRPS_AREA"))
            )
        ).otherwise(lit("NA"))
    )
    .withColumn(
        "svAllowedAmt",
        col("Medtrak.INGR_CST") + col("Medtrak.DISPNS_FEE") + col("Medtrak.SLS_TAX")
    )
    .withColumn(
        "svCnsdChrgAmt",
        col("svAllowedAmt")
    )
    .withColumn(
        "svChgeAmt",
        col("svAllowedAmt")
    )
    .withColumn(
        "svClmCnt",
        when(col("svAdjustedClaim"), lit(-1)).otherwise(lit(1))
    )
)

df_MedtrakClmTrns = df_BusinessRules_pre.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
    rpad(lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(lit(SrcSysCd), lit(";"), col("Medtrak.CLAIM_ID")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    rpad(col("Medtrak.CLAIM_ID"), 18, " ").alias("CLM_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("svAdjustingFromCLmID"), 12, " ").alias("ADJ_FROM_CLM"),
    rpad(lit("NA"), 12, " ").alias("ADJ_TO_CLM"),
    rpad(lit("NA"), 3, " ").alias("ALPHA_PFX_CD"),
    rpad(lit("NA"), 3, " ").alias("CLM_EOB_EXCD"),
    rpad(lit("NA"), 4, " ").alias("CLS"),
    rpad(lit("NA"), 8, " ").alias("CLS_PLN"),
    rpad(lit("NA"), 4, " ").alias("EXPRNC_CAT"),
    rpad(lit("NA"), 4, " ").alias("FNCL_LOB_NO"),
    rpad(col("Medtrak.GRP_ID"), 8, " ").alias("GRP"),
    col("Medtrak.MEM_CK_KEY").alias("MBR_CK"),
    rpad(lit("NA"), 12, " ").alias("NTWK"),
    rpad(lit("NA"), 8, " ").alias("PROD"),
    rpad(lit("NA"), 4, " ").alias("SUBGRP"),
    col("svSubCk").alias("SUB_CK"),
    rpad(lit("NA"), 10, " ").alias("CLM_ACDNT_CD"),
    rpad(lit("NA"), 2, " ").alias("CLM_ACDNT_ST_CD"),
    rpad(lit("NA"), 10, " ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    rpad(lit("NA"), 10, " ").alias("CLM_AGMNT_SRC_CD"),
    rpad(lit("NA"), 1, " ").alias("CLM_BTCH_ACTN_CD"),
    rpad(lit("N"), 10, " ").alias("CLM_CAP_CD"),
    rpad(lit("STD"), 10, " ").alias("CLM_CAT_CD"),
    rpad(lit("NA"), 1, " ").alias("CLM_CHK_CYC_OVRD_CD"),
    rpad(lit("NA"), 2, " ").alias("CLM_COB_CD"),
    lit("ACPTD").alias("FINL_DISP_CD"),
    rpad(lit("NA"), 1, " ").alias("CLM_INPT_METH_CD"),
    rpad(
        when(
            isnull(trim(col("Medtrak.CLM_TRANSMITTAL_METH"))) |
            (length(trim(col("Medtrak.CLM_TRANSMITTAL_METH"))) == 0),
            lit("NA")
        ).otherwise(col("Medtrak.CLM_TRANSMITTAL_METH")),
        10, " "
    ).alias("CLM_INPT_SRC_CD"),
    rpad(lit("NA"), 10, " ").alias("CLM_IPP_CD"),
    rpad(
        when(
            isnull(col("Medtrak.PAR_PDX_IND")) | (length(col("Medtrak.PAR_PDX_IND")) == 0),
            lit("UNK")
        ).otherwise(col("Medtrak.PAR_PDX_IND")),
        2, " "
    ).alias("CLM_NTWK_STTUS_CD"),
    rpad(lit("NA"), 4, " ").alias("CLM_NONPAR_PROV_PFX_CD"),
    rpad(lit("NA"), 1, " ").alias("CLM_OTHER_BNF_CD"),
    rpad(lit("NA"), 1, " ").alias("CLM_PAYE_CD"),
    rpad(lit("NA"), 4, " ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    rpad(lit("NA"), 4, " ").alias("CLM_SVC_DEFN_PFX_CD"),
    rpad(lit("NA"), 10, " ").alias("CLM_SVC_PROV_SPEC_CD"),
    rpad(lit("0012"), 10, " ").alias("CLM_SVC_PROV_TYP_CD"),
    rpad(trim(col("Medtrak.CLM_TYP")), 2, " ").alias("CLM_STTUS_CD"),
    rpad(lit("NA"), 10, " ").alias("CLM_SUBMT_BCBS_PLN_CD"),
    rpad(lit("NA"), 10, " ").alias("CLM_SUB_BCBS_PLN_CD"),
    rpad(lit("RX"), 10, " ").alias("CLM_SUBTYP_CD"),
    rpad(lit("MED"), 1, " ").alias("CLM_TYP_CD"),
    rpad(lit("N"), 1, " ").alias("ATCHMT_IN"),
    rpad(lit("N"), 1, " ").alias("CLM_CLNCL_EDIT_CD"),
    rpad(lit("N"), 1, " ").alias("COBRA_CLM_IN"),
    rpad(lit("N"), 1, " ").alias("FIRST_PASS_IN"),
    rpad(lit("N"), 1, " ").alias("HOST_IN"),
    rpad(lit("N"), 1, " ").alias("LTR_IN"),
    rpad(lit("N"), 1, " ").alias("MCARE_ASG_IN"),
    rpad(lit("N"), 1, " ").alias("NOTE_IN"),
    rpad(lit("N"), 1, " ").alias("PCA_AUDIT_IN"),
    rpad(lit("N"), 1, " ").alias("PCP_SUBMT_IN"),
    rpad(lit("N"), 1, " ").alias("PROD_OOA_IN"),
    rpad(lit("1753-01-01"), 10, " ").alias("ACDNT_DT"),
    rpad(lit("1753-01-01"), 10, " ").alias("INPT_DT"),
    rpad(lit("1753-01-01"), 10, " ").alias("MBR_PLN_ELIG_DT"),
    rpad(lit("1753-01-01"), 10, " ").alias("NEXT_RVW_DT"),
    rpad(col("Medtrak.PD_DT"), 10, " ").alias("PD_DT"),
    rpad(lit("1753-01-01"), 10, " ").alias("PAYMT_DRAG_CYC_DT"),
    rpad(col("Medtrak.ADJDCT_DT"), 10, " ").alias("PRCS_DT"),
    rpad(col("Medtrak.ADJDCT_DT"), 10, " ").alias("RCVD_DT"),
    rpad(col("Medtrak.DT_FILLED"), 10, " ").alias("SVC_STRT_DT"),
    rpad(col("Medtrak.DT_FILLED"), 10, " ").alias("SVC_END_DT"),
    rpad(lit("1753-01-01"), 10, " ").alias("SMLR_ILNS_DT"),
    rpad(current_date(), 10, " ").alias("STTUS_DT"),
    rpad(lit("1753-01-01"), 10, " ").alias("WORK_UNABLE_BEG_DT"),
    rpad(lit("2199-12-31"), 10, " ").alias("WORK_UNABLE_END_DT"),
    lit(0.00).alias("ACDNT_AMT"),
    col("Medtrak.AMT_BILL").alias("ACTL_PD_AMT"),
    col("svAllowedAmt").alias("ALLOW_AMT"),
    lit(0.00).alias("DSALW_AMT"),
    lit(0.00).alias("COINS_AMT"),
    col("svCnsdChrgAmt").alias("CNSD_CHRG_AMT"),
    col("Medtrak.COPAY_AMT").alias("COPAY_AMT"),
    col("svChgeAmt").alias("CHRG_AMT"),
    col("Medtrak.DEDCT_AMT").alias("DEDCT_AMT"),
    col("Medtrak.AMT_BILL").alias("PAYBL_AMT"),
    col("svClmCnt").alias("CLM_CT"),
    col("svMbrAge").alias("MBR_AGE"),
    col("svAdjustingFromCLmID").alias("ADJ_FROM_CLM_ID"),
    rpad(lit("NA"), 2, " ").alias("ADJ_TO_CLM_ID"),
    rpad(lit("NA"), 18, " ").alias("DOC_TX_ID"),
    lit(None).alias("MCAID_RESUB_NO"),
    rpad(lit("NA"), 12, " ").alias("MCARE_ID"),
    rpad(trim(col("Medtrak.PRSN_CD")), 2, " ").alias("MBR_SFX_NO"),
    lit(None).alias("PATN_ACCT_NO"),
    rpad(lit("NA"), 16, " ").alias("PAYMT_REF_ID"),
    rpad(lit("NA"), 12, " ").alias("PROV_AGMNT_ID"),
    lit(None).alias("RFRNG_PROV_TX"),
    rpad(col("Medtrak.CARDHLDR_ID_NO"), 14, " ").alias("SUB_ID"),
    lit(0.00).alias("REMIT_SUPRSION_AMT"),
    rpad(lit("NA"), 2, " ").alias("MCAID_STTUS_ID"),
    lit(0.00).alias("PATN_PD_AMT"),
    rpad(lit("NA"), 2, " ").alias("CLM_SUBMT_ICD_VRSN_CD")
)

df_alpha_pfx_pre1 = (
    df_MedtrakClmTrns.alias("MedtrakClmTrns")
    .join(
        df_sub_alpha_pfx_lkup.alias("sub_alpha_pfx_lkup"),
        col("MedtrakClmTrns.SUB_CK") == col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY"),
        "left"
    )
    .join(
        df_mbr_enr_lkup.alias("mbr_enr_lkup"),
        col("MedtrakClmTrns.CLM_ID") == col("mbr_enr_lkup.CLM_ID"),
        "left"
    )
)

df_Transform = df_alpha_pfx_pre1.select(
    col("MedtrakClmTrns.JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("MedtrakClmTrns.INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("MedtrakClmTrns.DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("MedtrakClmTrns.PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("MedtrakClmTrns.FIRST_RECYC_DT"),
    col("MedtrakClmTrns.ERR_CT"),
    col("MedtrakClmTrns.RECYCLE_CT"),
    col("MedtrakClmTrns.SRC_SYS_CD"),
    col("MedtrakClmTrns.PRI_KEY_STRING"),
    col("MedtrakClmTrns.CLM_SK"),
    col("MedtrakClmTrns.SRC_SYS_CD_SK"),
    rpad(col("MedtrakClmTrns.CLM_ID"), 18, " ").alias("CLM_ID"),
    col("MedtrakClmTrns.CRT_RUN_CYC_EXCTN_SK"),
    col("MedtrakClmTrns.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("MedtrakClmTrns.ADJ_FROM_CLM"), 12, " ").alias("ADJ_FROM_CLM"),
    rpad(col("MedtrakClmTrns.ADJ_TO_CLM"), 12, " ").alias("ADJ_TO_CLM"),
    rpad(
        when(
            isnull(col("sub_alpha_pfx_lkup.SUB_UNIQ_KEY")),
            lit("UNK")
        ).otherwise(col("sub_alpha_pfx_lkup.ALPHA_PFX_CD")),
        3, " "
    ).alias("ALPHA_PFX_CD"),
    rpad(col("MedtrakClmTrns.CLM_EOB_EXCD"), 3, " ").alias("CLM_EOB_EXCD"),
    rpad(
        when(
            isnull(col("mbr_enr_lkup.CLM_ID")),
            lit("UNK")
        ).otherwise(col("mbr_enr_lkup.CLS_ID")),
        4, " "
    ).alias("CLS"),
    rpad(
        when(
            isnull(col("mbr_enr_lkup.CLM_ID")),
            lit("UNK")
        ).otherwise(col("mbr_enr_lkup.CLS_PLN_ID")),
        8, " "
    ).alias("CLS_PLN"),
    rpad(
        when(
            isnull(col("mbr_enr_lkup.CLM_ID")),
            lit("UNK")
        ).otherwise(col("mbr_enr_lkup.EXPRNC_CAT_CD")),
        4, " "
    ).alias("EXPRNC_CAT"),
    rpad(
        when(
            isnull(col("mbr_enr_lkup.CLM_ID")),
            lit("UNK")
        ).otherwise(col("mbr_enr_lkup.FNCL_LOB_CD")),
        4, " "
    ).alias("FNCL_LOB_NO"),
    rpad(col("MedtrakClmTrns.GRP"), 8, " ").alias("GRP"),
    col("MedtrakClmTrns.MBR_CK").alias("MBR_CK"),
    rpad(col("MedtrakClmTrns.NTWK"), 12, " ").alias("NTWK"),
    rpad(
        when(
            isnull(col("mbr_enr_lkup.CLM_ID")),
            lit("UNK")
        ).otherwise(trim(col("mbr_enr_lkup.PROD_ID"))),
        8, " "
    ).alias("PROD"),
    rpad(
        when(
            isnull(col("mbr_enr_lkup.CLM_ID")),
            lit("UNK")
        ).otherwise(col("mbr_enr_lkup.SUBGRP_ID")),
        4, " "
    ).alias("SUBGRP"),
    col("MedtrakClmTrns.SUB_CK").alias("SUB_CK"),
    rpad(col("MedtrakClmTrns.CLM_ACDNT_CD"), 10, " ").alias("CLM_ACDNT_CD"),
    rpad(col("MedtrakClmTrns.CLM_ACDNT_ST_CD"), 2, " ").alias("CLM_ACDNT_ST_CD"),
    rpad(col("MedtrakClmTrns.CLM_ACTIVATING_BCBS_PLN_CD"), 10, " ").alias("CLM_ACTIVATING_BCBS_PLN_CD"),
    rpad(col("MedtrakClmTrns.CLM_AGMNT_SRC_CD"), 10, " ").alias("CLM_AGMNT_SRC_CD"),
    rpad(col("MedtrakClmTrns.CLM_BTCH_ACTN_CD"), 1, " ").alias("CLM_BTCH_ACTN_CD"),
    rpad(col("MedtrakClmTrns.CLM_CAP_CD"), 10, " ").alias("CLM_CAP_CD"),
    rpad(col("MedtrakClmTrns.CLM_CAT_CD"), 10, " ").alias("CLM_CAT_CD"),
    rpad(col("MedtrakClmTrns.CLM_CHK_CYC_OVRD_CD"), 1, " ").alias("CLM_CHK_CYC_OVRD_CD"),
    rpad(col("MedtrakClmTrns.CLM_COB_CD"), 1, " ").alias("CLM_COB_CD"),
    col("MedtrakClmTrns.FINL_DISP_CD").alias("FINL_DISP_CD"),
    rpad(col("MedtrakClmTrns.CLM_INPT_METH_CD"), 1, " ").alias("CLM_INPT_METH_CD"),
    rpad(col("MedtrakClmTrns.CLM_INPT_SRC_CD"), 10, " ").alias("CLM_INPT_SRC_CD"),
    rpad(col("MedtrakClmTrns.CLM_IPP_CD"), 10, " ").alias("CLM_IPP_CD"),
    rpad(col("MedtrakClmTrns.CLM_NTWK_STTUS_CD"), 2, " ").alias("CLM_NTWK_STTUS_CD"),
    rpad(col("MedtrakClmTrns.CLM_NONPAR_PROV_PFX_CD"), 4, " ").alias("CLM_NONPAR_PROV_PFX_CD"),
    rpad(col("MedtrakClmTrns.CLM_OTHER_BNF_CD"), 1, " ").alias("CLM_OTHER_BNF_CD"),
    rpad(col("MedtrakClmTrns.CLM_PAYE_CD"), 1, " ").alias("CLM_PAYE_CD"),
    rpad(col("MedtrakClmTrns.CLM_PRCS_CTL_AGNT_PFX_CD"), 4, " ").alias("CLM_PRCS_CTL_AGNT_PFX_CD"),
    rpad(col("MedtrakClmTrns.CLM_SVC_DEFN_PFX_CD"), 4, " ").alias("CLM_SVC_DEFN_PFX_CD"),
    rpad(col("MedtrakClmTrns.CLM_SVC_PROV_SPEC_CD"), 10, " ").alias("CLM_SVC_PROV_SPEC_CD"),
    rpad(col("MedtrakClmTrns.CLM_SVC_PROV_TYP_CD"), 10, " ").alias("CLM_SVC_PROV_TYP_CD"),
    rpad(col("MedtrakClmTrns.CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),
    rpad(col("MedtrakClmTrns.CLM_SUBMT_BCBS_PLN_CD"), 10, " ").alias("CLM_SUBMT_BCBS_PLN_CD"),
    rpad(col("MedtrakClmTrns.CLM_SUB_BCBS_PLN_CD"), 10, " ").alias("CLM_SUB_BCBS_PLN_CD"),
    rpad(col("MedtrakClmTrns.CLM_SUBTYP_CD"), 10, " ").alias("CLM_SUBTYP_CD"),
    rpad(col("MedtrakClmTrns.CLM_TYP_CD"), 1, " ").alias("CLM_TYP_CD"),
    rpad(col("MedtrakClmTrns.ATCHMT_IN"), 1, " ").alias("ATCHMT_IN"),
    rpad(col("MedtrakClmTrns.CLM_CLNCL_EDIT_CD"), 1, " ").alias("CLM_CLNCL_EDIT_CD"),
    rpad(col("MedtrakClmTrns.COBRA_CLM_IN"), 1, " ").alias("COBRA_CLM_IN"),
    rpad(col("MedtrakClmTrns.FIRST_PASS_IN"), 1, " ").alias("FIRST_PASS_IN"),
    rpad(col("MedtrakClmTrns.HOST_IN"), 1, " ").alias("HOST_IN"),
    rpad(col("MedtrakClmTrns.LTR_IN"), 1, " ").alias("LTR_IN"),
    rpad(col("MedtrakClmTrns.MCARE_ASG_IN"), 1, " ").alias("MCARE_ASG_IN"),
    rpad(col("MedtrakClmTrns.NOTE_IN"), 1, " ").alias("NOTE_IN"),
    rpad(col("MedtrakClmTrns.PCA_AUDIT_IN"), 1, " ").alias("PCA_AUDIT_IN"),
    rpad(col("MedtrakClmTrns.PCP_SUBMT_IN"), 1, " ").alias("PCP_SUBMT_IN"),
    rpad(col("MedtrakClmTrns.PROD_OOA_IN"), 1, " ").alias("PROD_OOA_IN"),
    rpad(col("MedtrakClmTrns.ACDNT_DT"), 10, " ").alias("ACDNT_DT"),
    rpad(col("MedtrakClmTrns.INPT_DT"), 10, " ").alias("INPT_DT"),
    rpad(col("MedtrakClmTrns.MBR_PLN_ELIG_DT"), 10, " ").alias("MBR_PLN_ELIG_DT"),
    rpad(col("MedtrakClmTrns.NEXT_RVW_DT"), 10, " ").alias("NEXT_RVW_DT"),
    rpad(col("MedtrakClmTrns.PD_DT"), 10, " ").alias("PD_DT"),
    rpad(col("MedtrakClmTrns.PAYMT_DRAG_CYC_DT"), 10, " ").alias("PAYMT_DRAG_CYC_DT"),
    rpad(col("MedtrakClmTrns.PRCS_DT"), 10, " ").alias("PRCS_DT"),
    rpad(col("MedtrakClmTrns.RCVD_DT"), 10, " ").alias("RCVD_DT"),
    rpad(col("MedtrakClmTrns.SVC_STRT_DT"), 10, " ").alias("SVC_STRT_DT"),
    rpad(col("MedtrakClmTrns.SVC_END_DT"), 10, " ").alias("SVC_END_DT"),
    rpad(col("MedtrakClmTrns.SMLR_ILNS_DT"), 10, " ").alias("SMLR_ILNS_DT"),
    rpad(col("MedtrakClmTrns.STTUS_DT"), 10, " ").alias("STTUS_DT"),
    rpad(col("MedtrakClmTrns.WORK_UNABLE_BEG_DT"), 10, " ").alias("WORK_UNABLE_BEG_DT"),
    rpad(col("MedtrakClmTrns.WORK_UNABLE_END_DT"), 10, " ").alias("WORK_UNABLE_END_DT"),
    col("MedtrakClmTrns.ACDNT_AMT").alias("ACDNT_AMT"),
    col("MedtrakClmTrns.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    col("MedtrakClmTrns.ALLOW_AMT").alias("ALLOW_AMT"),
    col("MedtrakClmTrns.DSALW_AMT").alias("DSALW_AMT"),
    col("MedtrakClmTrns.COINS_AMT").alias("COINS_AMT"),
    col("MedtrakClmTrns.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("MedtrakClmTrns.COPAY_AMT").alias("COPAY_AMT"),
    col("MedtrakClmTrns.CHRG_AMT").alias("CHRG_AMT"),
    col("MedtrakClmTrns.DEDCT_AMT").alias("DEDCT_AMT"),
    col("MedtrakClmTrns.PAYBL_AMT").alias("PAYBL_AMT"),
    col("MedtrakClmTrns.CLM_CT").alias("CLM_CT"),
    col("MedtrakClmTrns.MBR_AGE").alias("MBR_AGE"),
    col("MedtrakClmTrns.ADJ_FROM_CLM_ID").alias("ADJ_FROM_CLM_ID"),
    col("MedtrakClmTrns.ADJ_TO_CLM_ID").alias("ADJ_TO_CLM_ID"),
    rpad(col("MedtrakClmTrns.DOC_TX_ID"), 18, " ").alias("DOC_TX_ID"),
    rpad(col("MedtrakClmTrns.MCAID_RESUB_NO"), 15, " ").alias("MCAID_RESUB_NO"),
    rpad(col("MedtrakClmTrns.MCARE_ID"), 12, " ").alias("MCARE_ID"),
    rpad(col("MedtrakClmTrns.MBR_SFX_NO"), 2, " ").alias("MBR_SFX_NO"),
    col("MedtrakClmTrns.PATN_ACCT_NO").alias("PATN_ACCT_NO"),
    rpad(col("MedtrakClmTrns.PAYMT_REF_ID"), 16, " ").alias("PAYMT_REF_ID"),
    rpad(col("MedtrakClmTrns.PROV_AGMNT_ID"), 12, " ").alias("PROV_AGMNT_ID"),
    col("MedtrakClmTrns.RFRNG_PROV_TX").alias("RFRNG_PROV_TX"),
    rpad(col("MedtrakClmTrns.SUB_ID"), 14, " ").alias("SUB_ID"),
    lit(None).alias("PRPR_ENTITY"),
    rpad(lit("NA"), 18, " ").alias("PCA_TYP_CD"),
    rpad(lit("NA"), 2, " ").alias("REL_PCA_CLM_ID"),
    rpad(lit("NA"), 2, " ").alias("CLCL_MICRO_ID"),
    rpad(lit("N"), 1, " ").alias("CLM_UPDT_SW"),
    col("MedtrakClmTrns.REMIT_SUPRSION_AMT").alias("REMIT_SUPRSION_AMT"),
    col("MedtrakClmTrns.MCAID_STTUS_ID").alias("MCAID_STTUS_ID"),
    col("MedtrakClmTrns.PATN_PD_AMT").alias("PATN_PD_AMT"),
    col("MedtrakClmTrns.CLM_SUBMT_ICD_VRSN_CD").alias("CLM_SUBMT_ICD_VRSN_CD"),
    rpad(lit(""), 0, " ").alias("CLM_TXNMY_CD")
)

df_Snapshot_pre = df_Transform.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ADJ_FROM_CLM"),
    col("ADJ_TO_CLM"),
    col("ALPHA_PFX_CD"),
    col("CLM_EOB_EXCD"),
    col("CLS"),
    col("CLS_PLN"),
    col("EXPRNC_CAT"),
    col("FNCL_LOB_NO"),
    col("GRP"),
    col("MBR_CK"),
    col("NTWK"),
    col("PROD"),
    col("SUBGRP"),
    col("SUB_CK"),
    col("CLM_ACDNT_CD"),
    col("CLM_ACDNT_ST_CD"),
    col("CLM_ACTIVATING_BCBS_PLN_CD"),
    col("CLM_AGMNT_SRC_CD"),
    col("CLM_BTCH_ACTN_CD"),
    col("CLM_CAP_CD"),
    col("CLM_CAT_CD"),
    col("CLM_CHK_CYC_OVRD_CD"),
    col("CLM_COB_CD"),
    col("FINL_DISP_CD"),
    col("CLM_INPT_METH_CD"),
    col("CLM_INPT_SRC_CD"),
    col("CLM_IPP_CD"),
    col("CLM_NTWK_STTUS_CD"),
    col("CLM_NONPAR_PROV_PFX_CD"),
    col("CLM_OTHER_BNF_CD"),
    col("CLM_PAYE_CD"),
    col("CLM_PRCS_CTL_AGNT_PFX_CD"),
    col("CLM_SVC_DEFN_PFX_CD"),
    col("CLM_SVC_PROV_SPEC_CD"),
    col("CLM_SVC_PROV_TYP_CD"),
    col("CLM_STTUS_CD"),
    col("CLM_SUBMT_BCBS_PLN_CD"),
    col("CLM_SUB_BCBS_PLN_CD"),
    col("CLM_SUBTYP_CD"),
    col("CLM_TYP_CD"),
    col("ATCHMT_IN"),
    col("CLM_CLNCL_EDIT_CD"),
    col("COBRA_CLM_IN"),
    col("FIRST_PASS_IN"),
    col("HOST_IN"),
    col("LTR_IN"),
    col("MCARE_ASG_IN"),
    col("NOTE_IN"),
    col("PCA_AUDIT_IN"),
    col("PCP_SUBMT_IN"),
    col("PROD_OOA_IN"),
    col("ACDNT_DT"),
    col("INPT_DT"),
    col("MBR_PLN_ELIG_DT"),
    col("NEXT_RVW_DT"),
    col("PD_DT"),
    col("PAYMT_DRAG_CYC_DT"),
    col("PRCS_DT"),
    col("RCVD_DT"),
    col("SVC_STRT_DT"),
    col("SVC_END_DT"),
    col("SMLR_ILNS_DT"),
    col("STTUS_DT"),
    col("WORK_UNABLE_BEG_DT"),
    col("WORK_UNABLE_END_DT"),
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
    col("DOC_TX_ID"),
    col("MCAID_RESUB_NO"),
    col("MCARE_ID"),
    col("MBR_SFX_NO"),
    col("PATN_ACCT_NO"),
    col("PAYMT_REF_ID"),
    col("PROV_AGMNT_ID"),
    col("RFRNG_PROV_TX"),
    col("SUB_ID"),
    col("PRPR_ENTITY"),
    col("PCA_TYP_CD"),
    col("REL_PCA_CLM_ID"),
    col("CLCL_MICRO_ID"),
    col("CLM_UPDT_SW"),
    col("REMIT_SUPRSION_AMT"),
    col("MCAID_STTUS_ID"),
    col("PATN_PD_AMT"),
    col("CLM_SUBMT_ICD_VRSN_CD"),
    col("CLM_TXNMY_CD")
)

df_Pkey = df_Snapshot_pre.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ADJ_FROM_CLM"),
    col("ADJ_TO_CLM"),
    col("ALPHA_PFX_CD"),
    col("CLM_EOB_EXCD"),
    col("CLS"),
    col("CLS_PLN"),
    col("EXPRNC_CAT"),
    col("FNCL_LOB_NO"),
    col("GRP"),
    col("MBR_CK"),
    col("NTWK"),
    col("PROD"),
    col("SUBGRP"),
    col("SUB_CK"),
    col("CLM_ACDNT_CD"),
    col("CLM_ACDNT_ST_CD"),
    col("CLM_ACTIVATING_BCBS_PLN_CD"),
    col("CLM_AGMNT_SRC_CD"),
    col("CLM_BTCH_ACTN_CD"),
    col("CLM_CAP_CD"),
    col("CLM_CAT_CD"),
    col("CLM_CHK_CYC_OVRD_CD"),
    col("CLM_COB_CD"),
    col("FINL_DISP_CD"),
    col("CLM_INPT_METH_CD"),
    col("CLM_INPT_SRC_CD"),
    col("CLM_IPP_CD"),
    col("CLM_NTWK_STTUS_CD"),
    col("CLM_NONPAR_PROV_PFX_CD"),
    col("CLM_OTHER_BNF_CD"),
    col("CLM_PAYE_CD"),
    col("CLM_PRCS_CTL_AGNT_PFX_CD"),
    col("CLM_SVC_DEFN_PFX_CD"),
    col("CLM_SVC_PROV_SPEC_CD"),
    col("CLM_SVC_PROV_TYP_CD"),
    col("CLM_STTUS_CD"),
    col("CLM_SUBMT_BCBS_PLN_CD"),
    col("CLM_SUB_BCBS_PLN_CD"),
    col("CLM_SUBTYP_CD"),
    col("CLM_TYP_CD"),
    col("ATCHMT_IN"),
    col("CLM_CLNCL_EDIT_CD"),
    col("COBRA_CLM_IN"),
    col("FIRST_PASS_IN"),
    col("HOST_IN"),
    col("LTR_IN"),
    col("MCARE_ASG_IN"),
    col("NOTE_IN"),
    col("PCA_AUDIT_IN"),
    col("PCP_SUBMT_IN"),
    col("PROD_OOA_IN"),
    col("ACDNT_DT"),
    col("INPT_DT"),
    col("MBR_PLN_ELIG_DT"),
    col("NEXT_RVW_DT"),
    col("PD_DT"),
    col("PAYMT_DRAG_CYC_DT"),
    col("PRCS_DT"),
    col("RCVD_DT"),
    col("SVC_STRT_DT"),
    col("SVC_END_DT"),
    col("SMLR_ILNS_DT"),
    col("STTUS_DT"),
    col("WORK_UNABLE_BEG_DT"),
    col("WORK_UNABLE_END_DT"),
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
    col("DOC_TX_ID"),
    col("MCAID_RESUB_NO"),
    col("MCARE_ID"),
    col("MBR_SFX_NO"),
    col("PATN_ACCT_NO"),
    col("PAYMT_REF_ID"),
    col("PROV_AGMNT_ID"),
    col("RFRNG_PROV_TX"),
    col("SUB_ID"),
    col("PRPR_ENTITY"),
    col("PCA_TYP_CD"),
    col("REL_PCA_CLM_ID"),
    col("CLCL_MICRO_ID"),
    col("CLM_UPDT_SW"),
    col("REMIT_SUPRSION_AMT"),
    col("MCAID_STTUS_ID"),
    col("PATN_PD_AMT"),
    col("CLM_SUBMT_ICD_VRSN_CD"),
    col("CLM_TXNMY_CD"),
    rpad(lit("N"), 1, " ").alias("BILL_PAYMT_EXCL_IN")
)

df_Snapshot_forTransformer = df_Snapshot_pre.select(
    rpad(col("CLM_ID"), 12, " ").alias("CLM_ID"),
    col("MBR_CK"),
    rpad(col("GRP"), 8, " ").alias("GRP"),
    col("SVC_STRT_DT"),
    col("CHRG_AMT"),
    col("PAYBL_AMT"),
    rpad(col("EXPRNC_CAT"), 4, " ").alias("EXPRNC_CAT"),
    rpad(col("FNCL_LOB_NO"), 4, " ").alias("FNCL_LOB_NO"),
    col("CLM_CT"),
    rpad(col("PCA_TYP_CD"), 18, " ").alias("PCA_TYP_CD"),
    rpad(col("CLM_STTUS_CD"), 2, " ").alias("CLM_STTUS_CD"),
    rpad(col("CLM_CAT_CD"), 10, " ").alias("CLM_CAT_CD")
)

df_Transformer_pre = df_Snapshot_forTransformer

df_Transformer = df_Transformer_pre.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    # The next columns come from lookups of stage variables:
    # ExpCatCdSk = GetFkeyExprncCat(...)
    # GrpSk = GetFkeyGrp(...)
    # MbrSk = GetFkeyMbr(...)
    # FnclLobSk = GetFkeyFnclLob(...)
    # PcaTypCdSk = GetFkeyCodes(...)
    # ClmSttusCdSk = GetFkeyCodes(...)
    # ClmCatCdSk = GetFkeyCodes(...)
    # We assume these user-defined functions are called directly:
    GetFkeyCodes(col("CLM_STTUS_CD"), lit(0), lit("CLAIM STATUS"), col("CLM_STTUS_CD"), lit("N")).alias("CLM_STTUS_CD_SK"),
    GetFkeyCodes(col("CLM_CAT_CD"), lit(0), lit("CLAIM CATEGORY"), col("CLM_CAT_CD"), lit("X")).alias("CLM_CAT_CD_SK"),
    GetFkeyExprncCat(lit("FACETS"), lit(0), col("EXPRNC_CAT"), lit("N")).alias("EXPRNC_CAT_SK"),
    GetFkeyFnclLob(lit("PSI"), lit(0), col("FNCL_LOB_NO"), lit("N")).alias("FNCL_LOB_SK"),
    GetFkeyGrp(lit("FACETS"), lit(0), col("GRP"), lit("N")).alias("GRP_SK"),
    GetFkeyMbr(lit("FACETS"), lit(0), col("MBR_CK"), lit("N")).alias("MBR_SK"),
    GetFkeyCodes(lit("FACETS"), lit(0), lit("PERSONAL CARE ACCOUNT PROCESSING"), col("PCA_TYP_CD"), lit("N")).alias("PCA_TYP_CD_SK"),
    rpad(col("SVC_STRT_DT"), 10, " ").alias("SVC_STRT_DT_SK"),
    col("CHRG_AMT"),
    col("PAYBL_AMT"),
    col("CLM_CT")
)

df_B_CLM = df_Transformer.select(
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_STTUS_CD_SK"),
    col("CLM_CAT_CD_SK"),
    col("EXPRNC_CAT_SK"),
    col("FNCL_LOB_SK"),
    col("GRP_SK"),
    col("MBR_SK"),
    col("SVC_STRT_DT_SK"),
    col("CHRG_AMT"),
    col("PAYBL_AMT"),
    col("CLM_CT"),
    col("PCA_TYP_CD_SK")
)

write_files(
    df_B_CLM,
    f"{adls_path}/load/B_CLM.{SrcSysCd}.dat.{RunID}",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)

params = {
    "CurrRunCycle": RunCycle
}
df_ClmPK_output = ClmPK(df_Pkey, params)

df_MedtrakClmExtr = df_ClmPK_output

write_files(
    df_MedtrakClmExtr,
    f"{adls_path}/key/MedtrakClmExtr.DrugClm.dat.{RunID}",
    ",",
    "overwrite",
    False,
    True,
    "\"",
    None
)