# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2008, 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  ESIDrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the ESIDrugFile.dat created in  ESIClmLand  job and runs through primary key using shared container ClmLnPke
# MAGIC     
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                  Date               Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------------------    --------------------   ------------------------      -------------------------------------------------------------------------------------------------   --------------------------------       -------------------------------   ----------------------------       
# MAGIC Parikshith Chada       2008-07-03     3784(PBM)             Original Programming                                                                    devlIDSnew                   Brent Leland              11-18-2008
# MAGIC                                                                                        Updated with new primary key process
# MAGIC Tracy Davis               2009-04-03     TTR 477                New field CLM_TRANSMITTAL_METH                                      devlIDS                          Steph Goddard           04/14/2009
# MAGIC Rick Henry                2012-05-04     4896                       Default PROC_CD_TYP_CD and                                                NewDevl                        SAdnrew                  2012-05-18
# MAGIC                                                                                         PROC_CD_CAT_CD to "NA"
# MAGIC Kalyan Neelam          2013-02-19     4963 VBB Phase III  Added 3 new columns on end                                                   IntegrareNewDevl          Bhoomi Dasari            3/14/2013
# MAGIC                                                                                         VBB_RULE_ID, VBB_EXCD_ID, CLM_LN_VBB_IN
# MAGIC Raja Gummadi           2013-09-23     TFS -2618              Changed logic for CHRG_AMT and CNSD_CHRG_AMT            IntegrateNewDevl          Kalyan Neelam            2013-09-27
# MAGIC SAndrew                    2014-03-01     5082 ESI                                                                                                                    IntegrateNewDevl          Bhoomi Dasari              4/15/2014  
# MAGIC                                                                                         1.  Changed the field mapped to COPAY_AMT from 
# MAGIC                                                                                              ESI.COPPAY_FLAT_COPAY to ESI.COPAY_AMT
# MAGIC                                                                                         2.  Change the input file format to account for the four new fields - 
# MAGIC                                                                                              In ESIDrugClmLanding  declared 4 fields within the Filler4 
# MAGIC                                                                                              char (82) field that was at the end of the record.    
# MAGIC                                                                                         now after CLM_TRANSMITTAL_METH are fields 
# MAGIC                                                                                         PRESCRIPTION_NBR2, TRANSMISSION_ID, 
# MAGIC                                                                                         CROSS_REFERENCE_ID, ADJUSTMNT_DTM
# MAGIC 	                                                                        Length 12     PRESCRIPTION_NBR2 is not used                            
# MAGIC 	                                                                        Length 18  TRANSMISSION_ID is used to derive the Claim ID    
# MAGIC 	                                                                        length 18   CROSS_REFERENCE_ID is used to determine 
# MAGIC                                                                                         the original claim.  If it is 000000000000000000 then it is the
# MAGIC                                                                                         original claim.   If it <> 000000000000000000 then this is an 
# MAGIC                                                                                         adjusting claim or a reversal
# MAGIC                                                                                         length 11   AdjustmntDtTm   Provides the time the adjustment 
# MAGIC                                                                                         happened in case of need to chronologically order the 
# MAGIC                                                                                         adjustments in event sequence.
# MAGIC                                                                                         length 27  FILLER4  - not used
# MAGIC Manasa Andru           2014-10-17     TFS - 9580             Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and                IntegrateCurDevl             Kalyan Neelam             2014-10-22
# MAGIC                                                                                         ITS_SRCHRG_AMT) at the end.  
# MAGIC Madhavan B              2016-11-21     5619                       Validated Metadata of "ClmLnPK" Shared Container                  IntegrateDev2
# MAGIC Hari Pinnaka              2017-08-07     5792                       Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,    IntegrateDev1                 Kalyan Neelam             2017-09-05
# MAGIC                                                                                         NDC_UNIT_CT) at the end
# MAGIC Jaideep Mankala       2017-11-20     5828 	        Added new field to identify MED / PDX claim                             IntegrateDev2                  Kalyan Neelam            2017-11-20
# MAGIC                                                                                         when passing to Fkey job
# MAGIC Madhavan B              2018-02-06     5792 	        Changed the datatype of the column                                          IntegrateDev1                  Kalyan Neelam             2018-02-08
# MAGIC                                                                                         NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC Shashank Akinapalli   2019-04-10    97615                     Adding CLM_LN_VBB_IN & adjusting the Filler length.                IntegrateDev2                  Hugh Sisson                2019-05-02
# MAGIC 
# MAGIC Velmani K              2020-08-28        6264-PBM Phase II - Government Programs      Added APC_ID, APC_STTUS_ID     IntegrateDev5                  Kalyan Neelam             2020-12-10
# MAGIC 
# MAGIC Deepika C               2023-08-01      US 589700          Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a    IntegrateDevB	Harsha Ravuri		2023-08-31
# MAGIC                                                                                     default value in BusinessRules stage and mapped it till target

# MAGIC Read the ESI file created from ESIClmLand
# MAGIC Writing Sequential File to /key
# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job which indicates whether the claim is Medical or Pharmacy Indicator used :
# MAGIC 
# MAGIC MED - Medical
# MAGIC PDX - Pharmacy
# MAGIC This container is used in:
# MAGIC ArgusClmLnExtr
# MAGIC ESIClmLnExtr
# MAGIC PCSClmLnExtr
# MAGIC PseudoClmLnPkey
# MAGIC FctsClmLnDntlTrns
# MAGIC FctsClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnPK
# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


CurrentDate = get_widget_value("CurrentDate","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
RunID = get_widget_value("RunID","")
RunCycle = get_widget_value("RunCycle","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

schema_ESIClmLand = StructType([
    StructField("RCRD_ID", DoubleType(), False),
    StructField("CLAIM_ID", StringType(), False),
    StructField("PRCSR_NO", DoubleType(), False),
    StructField("MEM_CK_KEY", IntegerType(), False),
    StructField("BTCH_NO", DoubleType(), False),
    StructField("PDX_NO", StringType(), False),
    StructField("RX_NO", DoubleType(), False),
    StructField("DT_FILLED", StringType(), False),
    StructField("NDC_NO", DoubleType(), False),
    StructField("DRUG_DESC", StringType(), False),
    StructField("NEW_RFL_CD", DoubleType(), False),
    StructField("METRIC_QTY", DoubleType(), False),
    StructField("DAYS_SUPL", DoubleType(), False),
    StructField("BSS_OF_CST_DTRM", StringType(), False),
    StructField("INGR_CST", DoubleType(), False),
    StructField("DISPNS_FEE", DoubleType(), False),
    StructField("COPAY_AMT", DoubleType(), False),
    StructField("SLS_TAX", DoubleType(), False),
    StructField("AMT_BILL", DoubleType(), False),
    StructField("PATN_FIRST_NM", StringType(), False),
    StructField("PATN_LAST_NM", StringType(), False),
    StructField("DOB", StringType(), False),
    StructField("SEX_CD", DoubleType(), False),
    StructField("CARDHLDR_ID_NO", StringType(), False),
    StructField("RELSHP_CD", DoubleType(), False),
    StructField("GRP_NO", StringType(), False),
    StructField("HOME_PLN", StringType(), False),
    StructField("HOST_PLN", DoubleType(), False),
    StructField("PRESCRIBER_ID", StringType(), False),
    StructField("DIAG_CD", StringType(), False),
    StructField("CARDHLDR_FIRST_NM", StringType(), False),
    StructField("CARDHLDR_LAST_NM", StringType(), False),
    StructField("PRAUTH_NO", DoubleType(), False),
    StructField("PA_MC_SC_NO", StringType(), False),
    StructField("CUST_LOC", DoubleType(), False),
    StructField("RESUB_CYC_CT", DoubleType(), False),
    StructField("DT_RX_WRTN", StringType(), False),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), False),
    StructField("PRSN_CD", StringType(), False),
    StructField("OTHR_COV_CD", DoubleType(), False),
    StructField("ELIG_CLRFCTN_CD", DoubleType(), False),
    StructField("CMPND_CD", DoubleType(), False),
    StructField("NO_OF_RFLS_AUTH", DoubleType(), False),
    StructField("LVL_OF_SVC", DoubleType(), False),
    StructField("RX_ORIG_CD", DoubleType(), False),
    StructField("RX_DENIAL_CLRFCTN", DoubleType(), False),
    StructField("PRI_PRESCRIBER", StringType(), False),
    StructField("CLNC_ID_NO", DoubleType(), False),
    StructField("DRUG_TYP", DoubleType(), False),
    StructField("PRESCRIBER_LAST_NM", StringType(), False),
    StructField("POSTAGE_AMT_CLMED", DoubleType(), False),
    StructField("UNIT_DOSE_IN", DoubleType(), False),
    StructField("OTHR_PAYOR_AMT", DoubleType(), False),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DoubleType(), False),
    StructField("FULL_AWP", DoubleType(), False),
    StructField("EXPNSN_AREA", StringType(), False),
    StructField("MSTR_CAR", StringType(), False),
    StructField("SUB_CAR", StringType(), False),
    StructField("CLM_TYP", StringType(), False),
    StructField("ESI_SUB_GRP", StringType(), False),
    StructField("PLN_DSGNR", StringType(), False),
    StructField("ADJDCT_DT", StringType(), False),
    StructField("ADMIN_FEE", DoubleType(), False),
    StructField("CAP_AMT", DoubleType(), False),
    StructField("INGR_CST_SUB", DoubleType(), False),
    StructField("MBR_NON_COPAY_AMT", DoubleType(), False),
    StructField("MBR_PAY_CD", StringType(), False),
    StructField("INCNTV_FEE", DoubleType(), False),
    StructField("CLM_ADJ_AMT", DoubleType(), False),
    StructField("CLM_ADJ_CD", StringType(), False),
    StructField("FRMLRY_FLAG", StringType(), False),
    StructField("GNRC_CLS_NO", StringType(), False),
    StructField("THRPTC_CLS_AHFS", StringType(), False),
    StructField("PDX_TYP", StringType(), False),
    StructField("BILL_BSS_CD", StringType(), False),
    StructField("USL_AND_CUST_CHRG", DoubleType(), False),
    StructField("PD_DT", StringType(), False),
    StructField("BNF_CD", StringType(), False),
    StructField("DRUG_STRG", StringType(), False),
    StructField("ORIG_MBR", StringType(), False),
    StructField("DT_OF_INJURY", StringType(), False),
    StructField("FEE_AMT", DoubleType(), False),
    StructField("ESI_REF_NO", StringType(), False),
    StructField("CLNT_CUST_ID", StringType(), False),
    StructField("PLN_TYP", StringType(), False),
    StructField("ESI_ADJDCT_REF_NO", DoubleType(), False),
    StructField("ESI_ANCLRY_AMT", DoubleType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUBGRP_ID", StringType(), False),
    StructField("CLS_PLN_ID", StringType(), False),
    StructField("PAID_DATE", StringType(), False),
    StructField("PRTL_FILL_STTUS_CD", StringType(), False),
    StructField("ESI_BILL_DT", StringType(), False),
    StructField("FSA_VNDR_CD", StringType(), False),
    StructField("PICA_DRUG_CD", StringType(), False),
    StructField("AMT_CLMED", DoubleType(), False),
    StructField("AMT_DSALW", DoubleType(), False),
    StructField("FED_DRUG_CLS_CD", StringType(), False),
    StructField("DEDCT_AMT", DoubleType(), False),
    StructField("BNF_COPAY_100", StringType(), False),
    StructField("CLM_PRCS_TYP", StringType(), False),
    StructField("INDEM_HIER_TIER_NO", DoubleType(), False),
    StructField("FLR", StringType(), False),
    StructField("MCARE_D_COV_DRUG", StringType(), False),
    StructField("RETRO_LICS_CD", StringType(), False),
    StructField("RETRO_LICS_AMT", DoubleType(), False),
    StructField("LICS_SBSDY_AMT", DoubleType(), False),
    StructField("MED_B_DRUG", StringType(), False),
    StructField("MED_B_CLM", StringType(), False),
    StructField("PRESCRIBER_QLFR", StringType(), False),
    StructField("PRESCRIBER_ID_NPI", StringType(), False),
    StructField("PDX_QLFR", StringType(), False),
    StructField("PDX_ID_NPI", StringType(), False),
    StructField("HRA_APLD_AMT", DoubleType(), False),
    StructField("ESI_THER_CLS", DoubleType(), False),
    StructField("HIC_NO", StringType(), False),
    StructField("HRA_FLAG", StringType(), False),
    StructField("DOSE_CD", DoubleType(), False),
    StructField("LOW_INCM", StringType(), False),
    StructField("RTE_OF_ADMIN", StringType(), False),
    StructField("DEA_SCHD", DoubleType(), False),
    StructField("COPAY_BNF_OPT", DoubleType(), False),
    StructField("GNRC_PROD_IN_GPI", DoubleType(), False),
    StructField("PRESCRIBER_SPEC", StringType(), False),
    StructField("VAL_CD", StringType(), False),
    StructField("PRI_CARE_PDX", StringType(), False),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), False),
    StructField("FLR3", StringType(), False),
    StructField("PSL_FMLY_MET_AMT", DoubleType(), False),
    StructField("PSL_MBR_MET_AMT", DoubleType(), False),
    StructField("PSL_FMLY_AMT", DoubleType(), False),
    StructField("DED_FMLY_MET_AMT", DoubleType(), False),
    StructField("DED_FMLY_AMT", DoubleType(), False),
    StructField("MOPS_FMLY_AMT", DoubleType(), False),
    StructField("MOPS_FMLY_MET_AMT", DoubleType(), False),
    StructField("MOPS_MBR_MET_AMT", DoubleType(), False),
    StructField("DED_MBR_MET_AMT", DoubleType(), False),
    StructField("PSL_APLD_AMT", DoubleType(), False),
    StructField("MOPS_APLD_AMT", DoubleType(), False),
    StructField("PAR_PDX_IND", StringType(), False),
    StructField("COPAY_PCT_AMT", DoubleType(), False),
    StructField("COPAY_FLAT_AMT", DoubleType(), False),
    StructField("CLM_TRANSMITTAL_METH", StringType(), False),
    StructField("PRESCRIPTION_NBR_2", StringType(), False),
    StructField("TRANSACTION_ID", StringType(), False),
    StructField("CROSS_REF_ID", StringType(), False),
    StructField("ADJDCT_TIMESTAMP", StringType(), False),
    StructField("CLM_LN_VBB_IN", StringType(), False),
    StructField("FLR4", StringType(), False),
])

df_ESIClmLand = (
    spark.read.format("csv")
    .option("quote", "\"")
    .option("header", "false")
    .option("delimiter", ",")
    .schema(schema_ESIClmLand)
    .load(f"{adls_path}/verified/ESIDrugClmDaily_Land.dat.{RunID}")
)

extract_query = f"SELECT PROV.NTNL_PROV_ID, PROV.PROV_ID FROM {IDSOwner}.PROV PROV"
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_IDS_dedup = dedup_sort(df_IDS, ["NTNL_PROV_ID"], [("NTNL_PROV_ID","A")])

df_hf_esi_drg_clm_ln_prov = df_IDS_dedup

df_BusinessRules_joined = (
    df_ESIClmLand.alias("Extract")
    .join(
        df_hf_esi_drg_clm_ln_prov.alias("natln_prov"),
        F.col("Extract.PDX_ID_NPI") == F.col("natln_prov.NTNL_PROV_ID"),
        "left"
    )
)

df_BusinessRules_vars = (
    df_BusinessRules_joined
    .withColumn(
        "svServicingProvID",
        F.when(F.col("Extract.PDX_NO").isNotNull(), F.col("Extract.PDX_NO"))
         .when(F.col("natln_prov.NTNL_PROV_ID").isNotNull(), F.col("natln_prov.PROV_ID"))
         .otherwise(F.lit("UNK"))
    )
    .withColumn(
        "svAllowedAmt",
        F.col("Extract.INGR_CST") + F.col("Extract.DISPNS_FEE") + F.col("Extract.SLS_TAX")
    )
    .withColumn(
        "svChrgAmt",
        F.when(
            F.col("Extract.FULL_AWP").isNull() | (F.length(trim(F.col("Extract.FULL_AWP"))) == 0),
            F.lit(0)
        )
        .when(
            F.col("Extract.FULL_AWP") == 0,
            F.col("Extract.USL_AND_CUST_CHRG") + F.col("Extract.DISPNS_FEE")
        )
        .otherwise(F.col("Extract.FULL_AWP") + F.col("Extract.DISPNS_FEE"))
    )
    .withColumn(
        "svCnsdChrgAmt",
        F.when(
            (F.col("Extract.CLM_TYP") == F.lit("P")) & (F.col("svChrgAmt") < 0),
            F.lit(0)
        )
        .when(
            (F.col("Extract.CLM_TYP") == F.lit("R")) & (F.col("svChrgAmt") > 0),
            F.col("svChrgAmt") * -1
        )
        .otherwise(F.col("svChrgAmt"))
    )
)

df_BusinessRules_Transform = df_BusinessRules_vars.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("Extract.CLAIM_ID"), F.lit(";"), F.lit("1")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_SK"),
    F.col("Extract.CLAIM_ID").alias("CLM_ID"),
    F.lit(1).alias("CLM_LN_SEQ_NO"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("NA").alias("PROC_CD"),
    F.col("svServicingProvID").alias("SVC_PROV_ID"),
    F.lit("NA").alias("CLM_LN_DSALW_EXCD"),
    F.lit("NA").alias("CLM_LN_EOB_EXCD"),
    F.lit("ACPTD").alias("CLM_LN_FINL_DISP_CD"),
    F.lit("NA").alias("CLM_LN_LOB_CD"),
    F.lit("NA").alias("CLM_LN_POS_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_CD"),
    F.lit("NA").alias("CLM_LN_PREAUTH_SRC_CD"),
    F.lit("NA").alias("CLM_LN_PRICE_SRC_CD"),
    F.lit("NA").alias("CLM_LN_RFRL_CD"),
    F.lit("NA").alias("CLM_LN_RVNU_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.lit("NA").alias("CLM_LN_ROOM_TYP_CD"),
    F.lit("NA").alias("CLM_LN_TOS_CD"),
    F.lit("NA").alias("CLM_LN_UNIT_TYP_CD"),
    F.lit("N").alias("CAP_LN_IN"),
    F.lit("X").alias("PRI_LOB_IN"),
    F.col("Extract.DT_FILLED").alias("SVC_END_DT"),
    F.col("Extract.DT_FILLED").alias("SVC_STRT_DT"),
    F.lit("0.00").alias("AGMNT_PRICE_AMT"),
    F.col("svAllowedAmt").alias("ALW_AMT"),
    F.col("svCnsdChrgAmt").alias("CHRG_AMT"),
    F.col("Extract.COPAY_PCT_AMT").alias("COINS_AMT"),
    F.col("svCnsdChrgAmt").alias("CNSD_CHRG_AMT"),
    F.col("Extract.COPAY_AMT").alias("COPAY_AMT"),
    F.col("Extract.DEDCT_AMT").alias("DEDCT_AMT"),
    F.lit("0.00").alias("DSALW_AMT"),
    F.lit("0.00").alias("ITS_HOME_DSCNT_AMT"),
    F.lit("0.00").alias("NO_RESP_AMT"),
    F.lit("0.00").alias("MBR_LIAB_BSS_AMT"),
    F.col("Extract.COPAY_AMT").alias("PATN_RESP_AMT"),
    F.lit("0.00").alias("PAYBL_AMT"),
    F.lit("0.00").alias("PAYBL_TO_PROV_AMT"),
    F.lit("0.00").alias("PAYBL_TO_SUB_AMT"),
    F.lit("0.00").alias("PROC_TBL_PRICE_AMT"),
    F.lit("0.00").alias("PROFL_PRICE_AMT"),
    F.lit("0.00").alias("PROV_WRT_OFF_AMT"),
    F.lit("0.00").alias("RISK_WTHLD_AMT"),
    F.lit("0.00").alias("SVC_PRICE_AMT"),
    F.lit("0.00").alias("SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ALW_PRICE_UNIT_CT"),
    F.lit(0).alias("UNIT_CT"),
    F.lit("NA").alias("DEDCT_AMT_ACCUM_ID"),
    F.lit("NA").alias("PREAUTH_SVC_SEQ_NO"),
    F.lit("NA").alias("RFRL_SVC_SEQ_NO"),
    F.lit("NA").alias("LMT_PFX_ID"),
    F.lit("NA").alias("PREAUTH_ID"),
    F.lit("NA").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.lit("NA").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.lit("NA").alias("RFRL_ID_TX"),
    F.lit("NA").alias("SVC_ID"),
    F.lit("NA").alias("SVC_PRICE_RULE_ID"),
    F.lit("NA").alias("SVC_RULE_TYP_TX"),
    F.lit("NA").alias("SVC_LOC_TYP_CD"),
    F.lit("0.00").alias("NON_PAR_SAV_AMT"),
    F.lit("NA").alias("PROC_CD_TYP_CD"),
    F.lit("NA").alias("PROC_CD_CAT_CD"),
    F.col("Extract.CLM_LN_VBB_IN").alias("CLM_LN_VBB_IN"),
    F.lit(None).alias("APC_ID"),
    F.lit(None).alias("APC_STTUS_ID"),
    F.lit("NA").alias("SNOMED_CT_CD"),
    F.lit("NA").alias("CVX_VCCN_CD")
)

df_Snapshot = df_BusinessRules_Transform

df_Snapshot_PKey = df_Snapshot.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD"),
    F.col("SVC_PROV_ID"),
    F.col("CLM_LN_DSALW_EXCD"),
    F.col("CLM_LN_EOB_EXCD"),
    F.col("CLM_LN_FINL_DISP_CD"),
    F.col("CLM_LN_LOB_CD"),
    F.col("CLM_LN_POS_CD"),
    F.col("CLM_LN_PREAUTH_CD"),
    F.col("CLM_LN_PREAUTH_SRC_CD"),
    F.col("CLM_LN_PRICE_SRC_CD"),
    F.col("CLM_LN_RFRL_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.col("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD"),
    F.col("CAP_LN_IN"),
    F.col("PRI_LOB_IN"),
    F.col("SVC_END_DT"),
    F.col("SVC_STRT_DT"),
    F.col("AGMNT_PRICE_AMT"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("ITS_HOME_DSCNT_AMT"),
    F.col("NO_RESP_AMT"),
    F.col("MBR_LIAB_BSS_AMT"),
    F.col("PATN_RESP_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PAYBL_TO_PROV_AMT"),
    F.col("PAYBL_TO_SUB_AMT"),
    F.col("PROC_TBL_PRICE_AMT"),
    F.col("PROFL_PRICE_AMT"),
    F.col("PROV_WRT_OFF_AMT"),
    F.col("RISK_WTHLD_AMT"),
    F.col("SVC_PRICE_AMT"),
    F.col("SUPLMT_DSCNT_AMT"),
    F.col("ALW_PRICE_UNIT_CT"),
    F.col("UNIT_CT"),
    F.col("DEDCT_AMT_ACCUM_ID"),
    F.col("PREAUTH_SVC_SEQ_NO"),
    F.col("RFRL_SVC_SEQ_NO"),
    F.col("LMT_PFX_ID"),
    F.col("PREAUTH_ID"),
    F.col("PROD_CMPNT_DEDCT_PFX_ID"),
    F.col("PROD_CMPNT_SVC_PAYMT_ID"),
    F.col("RFRL_ID_TX"),
    F.col("SVC_ID"),
    F.col("SVC_PRICE_RULE_ID"),
    F.col("SVC_RULE_TYP_TX"),
    F.col("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD"),
    F.lit("NA").alias("VBB_RULE_ID"),
    F.lit("NA").alias("VBB_EXCD_ID"),
    F.col("CLM_LN_VBB_IN"),
    F.lit(0).alias("ITS_SUPLMT_DSCNT_AMT"),
    F.lit(0).alias("ITS_SRCHRG_AMT"),
    F.lit("NA").alias("NDC"),
    F.lit("NA").alias("NDC_DRUG_FORM_CD"),
    F.lit(None).alias("NDC_UNIT_CT"),
    F.lit("PDX").alias("MED_PDX_IND"),
    F.col("APC_ID"),
    F.col("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD")
)

df_Snapshot_Snapshot = df_Snapshot.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("PROC_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD")
)

df_Transformer_vars = (
    df_Snapshot_Snapshot
    .withColumn(
        "ProcCdSk",
        GetFkeyProcCd(
            F.lit("FACETS"),
            F.lit(0),
            F.col("PROC_CD").substr(F.lit(1),F.lit(5)),
            F.col("PROC_CD_TYP_CD"),
            F.col("PROC_CD_CAT_CD"),
            F.lit("N")
        )
    )
    .withColumn(
        "ClmLnRvnuCdSk",
        GetFkeyRvnu(
            F.lit("FACETS"),
            F.lit(0),
            F.col("CLM_LN_RVNU_CD"),
            F.lit("N")
        )
    )
)

df_Transformer = df_Transformer_vars.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("ProcCdSk").alias("PROC_CD_SK"),
    F.col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT")
)

# Write out B_CLM_LN.ESI.dat.#RunID#
# Apply rpad for any "char"/"varchar" (if lengths known). None are explicitly specified here, so just select as is:
write_files(
    df_Transformer,
    f"{adls_path}/load/B_CLM_LN.ESI.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmLnPK = {
    "CurrRunCycle": RunCycle
}
df_ClmLnPKOut = ClmLnPK(df_Snapshot_PKey, params_ClmLnPK)

# Final output ESIClmLnExtr.DrugClmLn.dat.#RunID#
# Apply rpad on char/varchar in the final set if lengths are known from the container's columns:
final_cols = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_LN_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "PROC_CD",
    "SVC_PROV_ID",
    "CLM_LN_DSALW_EXCD",
    "CLM_LN_EOB_EXCD",
    "CLM_LN_FINL_DISP_CD",
    "CLM_LN_LOB_CD",
    "CLM_LN_POS_CD",
    "CLM_LN_PREAUTH_CD",
    "CLM_LN_PREAUTH_SRC_CD",
    "CLM_LN_PRICE_SRC_CD",
    "CLM_LN_RFRL_CD",
    "CLM_LN_RVNU_CD",
    "CLM_LN_ROOM_PRICE_METH_CD",
    "CLM_LN_ROOM_TYP_CD",
    "CLM_LN_TOS_CD",
    "CLM_LN_UNIT_TYP_CD",
    "CAP_LN_IN",
    "PRI_LOB_IN",
    "SVC_END_DT",
    "SVC_STRT_DT",
    "AGMNT_PRICE_AMT",
    "ALW_AMT",
    "CHRG_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ITS_HOME_DSCNT_AMT",
    "NO_RESP_AMT",
    "MBR_LIAB_BSS_AMT",
    "PATN_RESP_AMT",
    "PAYBL_AMT",
    "PAYBL_TO_PROV_AMT",
    "PAYBL_TO_SUB_AMT",
    "PROC_TBL_PRICE_AMT",
    "PROFL_PRICE_AMT",
    "PROV_WRT_OFF_AMT",
    "RISK_WTHLD_AMT",
    "SVC_PRICE_AMT",
    "SUPLMT_DSCNT_AMT",
    "ALW_PRICE_UNIT_CT",
    "UNIT_CT",
    "DEDCT_AMT_ACCUM_ID",
    "PREAUTH_SVC_SEQ_NO",
    "RFRL_SVC_SEQ_NO",
    "LMT_PFX_ID",
    "PREAUTH_ID",
    "PROD_CMPNT_DEDCT_PFX_ID",
    "PROD_CMPNT_SVC_PAYMT_ID",
    "RFRL_ID_TX",
    "SVC_ID",
    "SVC_PRICE_RULE_ID",
    "SVC_RULE_TYP_TX",
    "SVC_LOC_TYP_CD",
    "NON_PAR_SAV_AMT",
    "PROC_CD_TYP_CD",
    "PROC_CD_CAT_CD",
    "VBB_RULE_ID",
    "VBB_EXCD_ID",
    "CLM_LN_VBB_IN",
    "ITS_SUPLMT_DSCNT_AMT",
    "ITS_SRCHRG_AMT",
    "NDC",
    "NDC_DRUG_FORM_CD",
    "NDC_UNIT_CT",
    "MED_PDX_IND",
    "APC_ID",
    "APC_STTUS_ID",
    "SNOMED_CT_CD",
    "CVX_VCCN_CD"
]

# Identify which of these columns were "char"/"varchar" with known length from the original flow:
# The job's JSON for this final stage does not explicitly provide lengths for all columns, 
# so rpad can only be reliably applied where length is known from the original definitions.
# For example: INSRT_UPDT_CD => char(10), DISCARD_IN => char(1), PASS_THRU_IN => char(1), ...
# We'll apply rpad where we have it. Others remain as is.

df_ESIClmLnExtr_pre = df_ClmLnPKOut.select(
    *[
        F.rpad(F.col(c), 10, " ").alias(c) if c == "INSRT_UPDT_CD" else
        F.rpad(F.col(c), 1, " ").alias(c) if c in ["DISCARD_IN","PASS_THRU_IN","CLM_LN_VBB_IN","CAP_LN_IN","PRI_LOB_IN"] else
        F.rpad(F.col(c), 2, " ").alias(c) if c == "CLM_LN_ROOM_PRICE_METH_CD" else
        F.rpad(F.col(c), 4, " ").alias(c) if c in [
            "DEDCT_AMT_ACCUM_ID","PREAUTH_SVC_SEQ_NO","RFRL_SVC_SEQ_NO","LMT_PFX_ID","PROD_CMPNT_DEDCT_PFX_ID",
            "PROD_CMPNT_SVC_PAYMT_ID","SVC_ID","SVC_PRICE_RULE_ID"
        ] else
        F.rpad(F.col(c), 9, " ").alias(c) if c in ["PREAUTH_ID","RFRL_ID_TX"] else
        F.rpad(F.col(c), 3, " ").alias(c) if c == "SVC_RULE_TYP_TX" else
        F.rpad(F.col(c), 20, " ").alias(c) if c == "SVC_LOC_TYP_CD" else
        F.rpad(F.col(c), 3, " ").alias(c) if c == "MED_PDX_IND" else
        F.col(c).alias(c)
        for c in final_cols
    ]
)

write_files(
    df_ESIClmLnExtr_pre,
    f"{adls_path}/key/ESIClmLnExtr.DrugClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)