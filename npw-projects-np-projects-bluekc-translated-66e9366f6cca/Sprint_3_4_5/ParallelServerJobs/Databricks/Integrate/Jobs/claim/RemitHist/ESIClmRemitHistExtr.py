# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2008, 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:          ESIDrugExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:     Reads the ESIDrugFile.dat created in  ESIClmLand  job and puts the data into the claim  remit history common record format and runs through primary key using shared container ClmRemiHistPkey
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date              Project/Altiris #   Change Description                                                                                      Development Project   Code Reviewer          Date Reviewed       
# MAGIC ---------------------------------   ------------------    ------------------------   ------------------------------------------------------------------------------------------------------------------   ---------------------------------   -------------------------------   ----------------------------       
# MAGIC Parikshith Chada         2008-07-09    3784 (PBM)         Original Programming                                                                                    devlIDSnew                Steph Goddard          10/27/2008                                 
# MAGIC                                                                                      Updated with new primary key process
# MAGIC Tracy Davis                 2009-04-09    TTR 477             New Field CLM_TRANSMITTAL_METH                                                     devlIDS                       Steph Goddard          04/14/2009
# MAGIC SAndrew                      2009-06-23    #3833                 Added two new fields to end of file and to shared container                        devlIDSnew                Steph Goddard          07/01/2009
# MAGIC                                                            Remit Alt Chrg    ALT_CHRG_IN and ALT_CHRG_PROV_WRTOFF_AMT
# MAGIC SAndrew                      2014-04-01    5082 ESI F14     Change the input file format to account for the four new fields                     IntegrateNewDevl       Bhoomi Dasari            4/15/2014
# MAGIC Shashank Akinapalli    2019-04-10    97615                 Adding CLM_LN_VBB_IN & adjusting the Filler length.                                 IntegrateDev2             Hugh Sisson               2019-05-02

# MAGIC Read ESI file from Landing job
# MAGIC This container is used in:
# MAGIC ArgusClmRemitHistExtr
# MAGIC PCSClmRemitHistExtr
# MAGIC BCBSClmRemitHistExtr
# MAGIC NascoClmRemitHistTrns
# MAGIC ESIClmRemitHistExtr
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmRemitHistPK
# COMMAND ----------

CurrentDate = get_widget_value('CurrentDate','')
RunCycle = get_widget_value('RunCycle','100')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','ESI')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1320918365')

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

df_esi_clm_land = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_ESIClmLand)
    .load(f"{adls_path}/verified/ESIDrugClmDaily_Land.dat.{RunID}")
)

df_business_rules = (
    df_esi_clm_land
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("PRI_KEY_STRING", F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLAIM_ID")))
    .withColumn("CLM_REMIT_HIST_SK", F.lit(0))
    .withColumn("SRC_SYS_CD_SK", F.lit(0))
    .withColumn("CLM_ID", F.col("CLAIM_ID"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("CLM_SK", F.lit(0))
    .withColumn("CALC_ACTL_PD_AMT_IN", F.lit("N"))
    .withColumn("SUPRESS_EOB_IN", F.lit("N"))
    .withColumn("SUPRESS_REMIT_IN", F.lit("N"))
    .withColumn("ACTL_PD_AMT", F.lit(0.00))
    .withColumn("COB_PD_AMT", F.col("OTHR_PAYOR_AMT"))
    .withColumn("COINS_AMT", F.col("COPAY_PCT_AMT"))
    .withColumn("CNSD_CHRG_AMT", (F.col("INGR_CST") + F.col("DISPNS_FEE") + F.col("SLS_TAX")))
    .withColumn("COPAY_AMT", F.col("COPAY_FLAT_AMT"))
    .withColumn("DEDCT_AMT", F.col("DEDCT_AMT"))
    .withColumn("DSALW_AMT", F.lit(0.00))
    .withColumn("ER_COPAY_AMT", F.lit(0.00))
    .withColumn("INTRST_AMT", F.lit(0.00))
    .withColumn("NO_RESP_AMT", F.lit(0.00))
    .withColumn("PATN_RESP_AMT", F.col("COPAY_AMT"))
    .withColumn("WRTOFF_AMT", F.lit(0.00))
    .withColumn("PCA_PD_AMT", F.lit(0.00))
    .withColumn("ALT_CHRG_IN", F.lit("N"))
    .withColumn("ALT_CHRG_PROV_WRTOFF_AMT", F.lit(0.00))
)

df_business_rules = df_business_rules.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_REMIT_HIST_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "CALC_ACTL_PD_AMT_IN",
    "SUPRESS_EOB_IN",
    "SUPRESS_REMIT_IN",
    "ACTL_PD_AMT",
    "COB_PD_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ER_COPAY_AMT",
    "INTRST_AMT",
    "NO_RESP_AMT",
    "PATN_RESP_AMT",
    "WRTOFF_AMT",
    "PCA_PD_AMT",
    "ALT_CHRG_IN",
    "ALT_CHRG_PROV_WRTOFF_AMT"
)

df_snapshot = df_business_rules

df_snapshot_pkey = df_snapshot.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_REMIT_HIST_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "CALC_ACTL_PD_AMT_IN",
    "SUPRESS_EOB_IN",
    "SUPRESS_REMIT_IN",
    "ACTL_PD_AMT",
    "COB_PD_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ER_COPAY_AMT",
    "INTRST_AMT",
    "NO_RESP_AMT",
    "PATN_RESP_AMT",
    "WRTOFF_AMT",
    "PCA_PD_AMT",
    "ALT_CHRG_IN",
    "ALT_CHRG_PROV_WRTOFF_AMT"
)

df_snapshot_rowcount = df_snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

write_files(
    df_snapshot_rowcount,
    f"{adls_path}/load/B_CLM_REMIT_HIST.ESI.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

params = {
    "CurrRunCycle": RunCycle
}
df_clmremithist_pk = ClmRemitHistPK(df_snapshot_pkey, params)

df_clmremithist_pk = (
    df_clmremithist_pk
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CALC_ACTL_PD_AMT_IN", F.rpad(F.col("CALC_ACTL_PD_AMT_IN"), 1, " "))
    .withColumn("SUPRESS_EOB_IN", F.rpad(F.col("SUPRESS_EOB_IN"), 1, " "))
    .withColumn("SUPRESS_REMIT_IN", F.rpad(F.col("SUPRESS_REMIT_IN"), 1, " "))
    .withColumn("ALT_CHRG_IN", F.rpad(F.col("ALT_CHRG_IN"), 1, " "))
)

df_clmremithist_pk = df_clmremithist_pk.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_REMIT_HIST_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "CALC_ACTL_PD_AMT_IN",
    "SUPRESS_EOB_IN",
    "SUPRESS_REMIT_IN",
    "ACTL_PD_AMT",
    "COB_PD_AMT",
    "COINS_AMT",
    "CNSD_CHRG_AMT",
    "COPAY_AMT",
    "DEDCT_AMT",
    "DSALW_AMT",
    "ER_COPAY_AMT",
    "INTRST_AMT",
    "NO_RESP_AMT",
    "PATN_RESP_AMT",
    "WRTOFF_AMT",
    "PCA_PD_AMT",
    "ALT_CHRG_IN",
    "ALT_CHRG_PROV_WRTOFF_AMT"
)

write_files(
    df_clmremithist_pk,
    f"{adls_path}/key/ESIClmRemitHistExtr.DrugClmRemitHist.dat.{RunID}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)