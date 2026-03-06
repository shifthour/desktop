# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  MedtrakDrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the MedtrakDruClm_Land.dat file and runs through primary key using shared container ClmLnPke
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2010-12-22       4616                      Initial Programming                                                                       IntegrateNewDevl          Steph Goddard          12/23/2010
# MAGIC 
# MAGIC Raja Gummadi          2012-05-08       4896                     Added PROC_CD_TYP_CD, PROC_CD_CAT_CD and               IntegrateNewDevl          SAndrew                   2012-05-16
# MAGIC                                                                                        Defaulted to NA
# MAGIC Raja Gummadi         2012-07-23       TTR 1330             Changed RX_NO field size from 9 to 20 in input file                       IntegrateWrhsDevl         Bhoomi Dasari           08/08/2012
# MAGIC Kalyan Neelam        2013-02-19     4963 VBB Phase III        Added 3 new columns on end                                                IntegrareNewDevl          Bhoomi Dasari           3/14/2013
# MAGIC                                                                                            VBB_RULE_ID, VBB_EXCD_ID, CLM_LN_VBB_IN
# MAGIC 
# MAGIC Manasa Andru         2014-10-17        TFS - 9580           Added 2 new fields(ITS_SUPLMT_DSCNT_AMT and                  IntegrateCurDevl             Kalyan Neelam           2014-10-22
# MAGIC                                                                                          ITS_SRCHRG_AMT) at the end.
# MAGIC 
# MAGIC Madhavan B            2016-11-21        5619                     Validated Metadata of "ClmLnPK" Shared Container                    IntegrateDev2                Kalyan Neelam            2016-11-23
# MAGIC 
# MAGIC Hari Pinnaka          2017-08-07      5792                    Added 3 new fields (NDC_SK ,NDC_DRUG_FORM_CD_SK,            IntegrateDev1               Kalyan Neelam            2017-09-05
# MAGIC                                                                                     NDC_UNIT_CT) at the end
# MAGIC Jaideep Mankala      2017-11-20      5828 	   Added new field to identify MED / PDX claim                                     IntegrateDev2              Kalyan Neelam            2017-11-20
# MAGIC                                                       		when passing to Fkey job
# MAGIC 
# MAGIC Madhavan B            2018-02-06      5792 	     Changed the datatype of the column                                                IntegrateDev1              Kalyan Neelam            2018-02-08
# MAGIC                                                       		     NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)
# MAGIC 
# MAGIC Velmani K                2020-08-28        6264-PBM Phase II - Government Programs      Added APC_ID, APC_STTUS_ID   IntegrateDev5                Kalyan Neelam            2020-12-10
# MAGIC 
# MAGIC Deepika C               2023-08-01      US 589700          Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a    IntegrateDevB	Harsha Ravuri	2023-08-29
# MAGIC                                                                                    default value in BusinessRules stage and mapped it till target

# MAGIC Read the MEDTRAK file created from MedtrakClmLand
# MAGIC Writing Sequential File to /key
# MAGIC Added new field MED_PDX_IND to the common layout file that is sent to IdsClmLnFkey job
# MAGIC which indicates whether the claim is Medical or Pharmacy
# MAGIC Indicator used :
# MAGIC MED - medical
# MAGIC PDX - Pharmacy
# MAGIC This container is used in:
# MAGIC ESIClmLnExtr
# MAGIC FctsClmLnDntlTrns
# MAGIC FctsClmLnMedTrns
# MAGIC MCSourceClmLnExtr
# MAGIC MedicaidClmLnExtr
# MAGIC NascoClmLnExtr
# MAGIC PcsClmLnExtr
# MAGIC PCTAClmLnExtr
# MAGIC WellDyneClmLnExtr
# MAGIC MedtrakClmLnExtr
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
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
import pyspark.sql.functions as F
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnPK

CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
RunID = get_widget_value('RunID','')
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

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
    .option("delimiter", "|")
    .option("quote", "\"")
    .schema(schema_MedtrakClmLand)
    .load(f"{adls_path}/verified/MedtrakDrugClm_Land.dat.{RunID}")
)

jdbc_url, jdbc_props = get_db_config(ids_secret_name)
extract_query = f"SELECT PROV.NTNL_PROV_ID, PROV.PROV_ID FROM {IDSOwner}.PROV PROV"
df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_hf_medtrak_drg_clm_ln_prov = df_IDS.dropDuplicates(["NTNL_PROV_ID"])

df_BusinessRules = (
    df_MedtrakClmLand.alias("Extract")
    .join(
        df_hf_medtrak_drg_clm_ln_prov.alias("natln_prov"),
        F.col("Extract.PDX_ID_NPI") == F.col("natln_prov.NTNL_PROV_ID"),
        "left"
    )
    .withColumn(
        "svServicingProvID",
        F.when(F.col("Extract.PDX_NO").isNotNull(), F.col("Extract.PDX_NO"))
        .otherwise(
            F.when(F.col("natln_prov.NTNL_PROV_ID").isNotNull(), F.col("natln_prov.PROV_ID"))
            .otherwise(F.lit("UNK"))
        )
    )
    .withColumn(
        "svAllowedAmt",
        F.col("Extract.INGR_CST") + F.col("Extract.DISPNS_FEE") + F.col("Extract.SLS_TAX")
    )
    .withColumn("svChrgAmt", F.col("svAllowedAmt"))
    .withColumn("svCnsdChrgAmt", F.col("svAllowedAmt"))
)

df_BusinessRules_out = df_BusinessRules.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", F.lit(SrcSysCd), F.col("Extract.CLAIM_ID"), F.lit("1")).alias("PRI_KEY_STRING"),
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
    F.lit("N").alias("PRI_LOB_IN"),
    F.col("Extract.DT_FILLED").alias("SVC_END_DT"),
    F.col("Extract.DT_FILLED").alias("SVC_STRT_DT"),
    F.lit("0.00").alias("AGMNT_PRICE_AMT"),
    F.col("svAllowedAmt").alias("ALW_AMT"),
    F.col("svChrgAmt").alias("CHRG_AMT"),
    F.lit("0.00").alias("COINS_AMT"),
    F.col("svCnsdChrgAmt").alias("CNSD_CHRG_AMT"),
    F.col("Extract.COPAY_AMT").alias("COPAY_AMT"),
    F.col("Extract.DEDCT_AMT").alias("DEDCT_AMT"),
    F.lit("0.00").alias("DSALW_AMT"),
    F.lit("0.00").alias("ITS_HOME_DSCNT_AMT"),
    F.lit("0.00").alias("NO_RESP_AMT"),
    F.lit("0.00").alias("MBR_LIAB_BSS_AMT"),
    F.col("Extract.COPAY_AMT").alias("PATN_RESP_AMT"),
    F.col("Extract.AMT_BILL").alias("PAYBL_AMT"),
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
    F.lit(None).alias("RFRL_ID_TX"),
    F.lit("NA").alias("SVC_ID"),
    F.lit("NA").alias("SVC_PRICE_RULE_ID"),
    F.lit(None).alias("SVC_RULE_TYP_TX"),
    F.lit("M").alias("CLM_TYPE"),
    F.lit("NA").alias("SVC_LOC_TYP_CD"),
    F.lit("0.00").alias("NON_PAR_SAV_AMT"),
    F.lit("NA").alias("PROC_CD_TYP_CD"),
    F.lit("NA").alias("PROC_CD_CAT_CD"),
    F.lit(None).alias("APC_ID"),
    F.lit(None).alias("APC_STTUS_ID"),
    F.lit("NA").alias("SNOMED_CT_CD"),
    F.lit("NA").alias("CVX_VCCN_CD")
)

df_Snapshot_PKey = df_BusinessRules_out.select(
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
    F.lit("N").alias("CLM_LN_VBB_IN"),
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

df_Snapshot_Snapshot = df_BusinessRules_out.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("PROC_CD"),
    F.col("CLM_LN_RVNU_CD"),
    F.col("ALW_AMT"),
    F.col("CHRG_AMT"),
    F.col("PAYBL_AMT"),
    F.col("CLM_TYPE"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD")
)

df_Transformer = df_Snapshot_Snapshot.alias("Snapshot").withColumn(
    "ProcCdSk",
    GetFkeyProcCd(
        F.lit("FACETS"),
        F.lit(0),
        F.substring(F.col("Snapshot.PROC_CD"), 1, 5),
        F.col("Snapshot.PROC_CD_TYP_CD"),
        F.col("Snapshot.PROC_CD_CAT_CD"),
        F.lit("N")
    )
).withColumn(
    "ClmLnRvnuCdSk",
    GetFkeyRvnu(
        F.lit("FACETS"),
        F.lit(0),
        F.col("Snapshot.CLM_LN_RVNU_CD"),
        F.lit("N")
    )
)

df_B_CLM_LN = df_Transformer.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("Snapshot.CLCL_ID").alias("CLM_ID"),
    F.col("Snapshot.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("ProcCdSk").alias("PROC_CD_SK"),
    F.col("ClmLnRvnuCdSk").alias("CLM_LN_RVNU_CD_SK"),
    F.col("Snapshot.ALW_AMT").alias("ALW_AMT"),
    F.col("Snapshot.CHRG_AMT").alias("CHRG_AMT"),
    F.col("Snapshot.PAYBL_AMT").alias("PAYBL_AMT")
)

write_files(
    df_B_CLM_LN.select(
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "PROC_CD_SK",
        "CLM_LN_RVNU_CD_SK",
        "ALW_AMT",
        "CHRG_AMT",
        "PAYBL_AMT"
    ),
    f"{adls_path}/load/B_CLM_LN.{SrcSysCd}.dat.{RunID}",
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
df_ClmLnPK = ClmLnPK(df_Snapshot_PKey, params_ClmLnPK)

df_MedtrakClmLnExtr = df_ClmLnPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD").cast(StringType()),10," ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN").cast(StringType()),1," ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN").cast(StringType()),1," ").alias("PASS_THRU_IN"),
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
    F.rpad(F.col("CLM_LN_ROOM_PRICE_METH_CD").cast(StringType()),2," ").alias("CLM_LN_ROOM_PRICE_METH_CD"),
    F.col("CLM_LN_ROOM_TYP_CD"),
    F.col("CLM_LN_TOS_CD"),
    F.col("CLM_LN_UNIT_TYP_CD"),
    F.rpad(F.col("CAP_LN_IN").cast(StringType()),1," ").alias("CAP_LN_IN"),
    F.rpad(F.col("PRI_LOB_IN").cast(StringType()),1," ").alias("PRI_LOB_IN"),
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
    F.rpad(F.col("DEDCT_AMT_ACCUM_ID").cast(StringType()),4," ").alias("DEDCT_AMT_ACCUM_ID"),
    F.rpad(F.col("PREAUTH_SVC_SEQ_NO").cast(StringType()),4," ").alias("PREAUTH_SVC_SEQ_NO"),
    F.rpad(F.col("RFRL_SVC_SEQ_NO").cast(StringType()),4," ").alias("RFRL_SVC_SEQ_NO"),
    F.rpad(F.col("LMT_PFX_ID").cast(StringType()),4," ").alias("LMT_PFX_ID"),
    F.rpad(F.col("PREAUTH_ID").cast(StringType()),9," ").alias("PREAUTH_ID"),
    F.rpad(F.col("PROD_CMPNT_DEDCT_PFX_ID").cast(StringType()),4," ").alias("PROD_CMPNT_DEDCT_PFX_ID"),
    F.rpad(F.col("PROD_CMPNT_SVC_PAYMT_ID").cast(StringType()),4," ").alias("PROD_CMPNT_SVC_PAYMT_ID"),
    F.rpad(F.col("RFRL_ID_TX").cast(StringType()),9," ").alias("RFRL_ID_TX"),
    F.rpad(F.col("SVC_ID").cast(StringType()),4," ").alias("SVC_ID"),
    F.rpad(F.col("SVC_PRICE_RULE_ID").cast(StringType()),4," ").alias("SVC_PRICE_RULE_ID"),
    F.rpad(F.col("SVC_RULE_TYP_TX").cast(StringType()),3," ").alias("SVC_RULE_TYP_TX"),
    F.rpad(F.col("SVC_LOC_TYP_CD").cast(StringType()),20," ").alias("SVC_LOC_TYP_CD"),
    F.col("NON_PAR_SAV_AMT"),
    F.col("PROC_CD_TYP_CD"),
    F.col("PROC_CD_CAT_CD"),
    F.col("VBB_RULE_ID"),
    F.col("VBB_EXCD_ID"),
    F.rpad(F.col("CLM_LN_VBB_IN").cast(StringType()),1," ").alias("CLM_LN_VBB_IN"),
    F.col("ITS_SUPLMT_DSCNT_AMT"),
    F.col("ITS_SRCHRG_AMT"),
    F.col("NDC"),
    F.col("NDC_DRUG_FORM_CD"),
    F.col("NDC_UNIT_CT"),
    F.rpad(F.col("MED_PDX_IND").cast(StringType()),3," ").alias("MED_PDX_IND"),
    F.col("APC_ID"),
    F.col("APC_STTUS_ID"),
    F.col("SNOMED_CT_CD"),
    F.col("CVX_VCCN_CD")
)

write_files(
    df_MedtrakClmLnExtr,
    f"{adls_path}/key/MedtrakClmLnExtr.DrugClmLn.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)