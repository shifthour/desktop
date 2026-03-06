# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2008, 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:          OptumDrugExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:     Reads the Optum landing file created in  OptumClmLand  job and puts the data into the claim  remit history common record format and runs through primary key using shared container ClmRemiHistPkey
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                    Date              Project/Altiris #   Change Description                                                                                      Development Project   Code Reviewer          Date Reviewed       
# MAGIC ---------------------------------   ------------------    ------------------------   ------------------------------------------------------------------------------------------------------------------   ---------------------------------   -------------------------------   ----------------------------       
# MAGIC Srinivas Garimella      2019-10-15   #6131                         Original Programming                                                                                IntegrateDev2              Kalyan Neelam          2019-11-20
# MAGIC Sri Nannapaneni       2020-07-15      6131 PBM                Added 7 new fields to source seq file                                                       IntegrateDev2                 
# MAGIC Sri Nannapaneni        08/13/2020    6131- PBM Replacement    Added LOB_IN field to source seq file                                          IntegrateDev2                Sravya Gorla             2020-12-09

# MAGIC Read OPTUM file from BCBSKCCommon Landing job
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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType,
    IntegerType
)
from pyspark.sql.functions import (
    lit,
    when,
    concat,
    col,
    rpad
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmRemitHistPK
# COMMAND ----------

# Retrieve job parameters
CurrentDate = get_widget_value("CurrentDate","")
RunCycle = get_widget_value("RunCycle","1")
RunID = get_widget_value("RunID","")
SrcSysCd = get_widget_value("SrcSysCd","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")

# Define schema for the incoming file (BCBSCommClmLand)
schema_BCBSCommClmLand = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("FILE_RCVD_DT", StringType(), True),
    StructField("RCRD_ID", DecimalType(38,10), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("BTCH_NO", DecimalType(38,10), True),
    StructField("PDX_NO", DecimalType(38,10), True),
    StructField("RX_NO", DecimalType(38,10), True),
    StructField("FILL_DT", StringType(), True),
    StructField("NDC", DecimalType(38,10), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_OR_RFL_CD", IntegerType(), True),
    StructField("METRIC_QTY", DecimalType(38,10), True),
    StructField("DAYS_SUPL", DecimalType(38,10), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST_AMT", DecimalType(38,10), True),
    StructField("DISPNS_FEE_AMT", DecimalType(38,10), True),
    StructField("COPAY_AMT", DecimalType(38,10), True),
    StructField("SLS_TAX_AMT", DecimalType(38,10), True),
    StructField("BILL_AMT", DecimalType(38,10), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("BRTH_DT", StringType(), True),
    StructField("SEX_CD", DecimalType(38,10), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", DecimalType(38,10), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", DecimalType(38,10), True),
    StructField("RESUB_CYC_CT", DecimalType(38,10), True),
    StructField("RX_DT", StringType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", DecimalType(38,10), True),
    StructField("ELIG_CLRFCTN_CD", DecimalType(38,10), True),
    StructField("CMPND_CD", DecimalType(38,10), True),
    StructField("NO_OF_RFLS_AUTH", DecimalType(38,10), True),
    StructField("LVL_OF_SVC", DecimalType(38,10), True),
    StructField("RX_ORIG_CD", DecimalType(38,10), True),
    StructField("RX_DENIAL_CLRFCTN", DecimalType(38,10), True),
    StructField("PRI_PRSCRBR", StringType(), True),
    StructField("CLNC_ID_NO", DecimalType(38,10), True),
    StructField("DRUG_TYP", DecimalType(38,10), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT", DecimalType(38,10), True),
    StructField("UNIT_DOSE_IN", DecimalType(38,10), True),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DecimalType(38,10), True),
    StructField("FULL_AVG_WHLSL_PRICE", DecimalType(38,10), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("MSTR_CAR", StringType(), True),
    StructField("SUBCAR", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("SUBGRP", StringType(), True),
    StructField("PLN_DSGNR", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("ADMIN_FEE_AMT", DecimalType(38,10), True),
    StructField("CAP_AMT", DecimalType(38,10), True),
    StructField("INGR_CST_SUB_AMT", DecimalType(38,10), True),
    StructField("MBR_NON_COPAY_AMT", DecimalType(38,10), True),
    StructField("MBR_PAY_CD", StringType(), True),
    StructField("INCNTV_FEE_AMT", DecimalType(38,10), True),
    StructField("CLM_ADJ_AMT", DecimalType(38,10), True),
    StructField("CLM_ADJ_CD", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("GNRC_CLS_NO", StringType(), True),
    StructField("THRPTC_CLS_AHFS", StringType(), True),
    StructField("PDX_TYP", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG_AMT", DecimalType(38,10), True),
    StructField("PD_DT", StringType(), True),
    StructField("BNF_CD", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("ORIG_MBR", StringType(), True),
    StructField("INJRY_DT", StringType(), True),
    StructField("FEE_AMT", DecimalType(38,10), True),
    StructField("REF_NO", StringType(), True),
    StructField("CLNT_CUST_ID", StringType(), True),
    StructField("PLN_TYP", StringType(), True),
    StructField("ADJDCT_REF_NO", DecimalType(38,10), True),
    StructField("ANCLRY_AMT", DecimalType(38,10), True),
    StructField("CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("PRTL_FILL_STTUS_CD", StringType(), True),
    StructField("BILL_DT", StringType(), True),
    StructField("FSA_VNDR_CD", StringType(), True),
    StructField("PICA_DRUG_CD", StringType(), True),
    StructField("CLM_AMT", DecimalType(38,10), True),
    StructField("DSALW_AMT", DecimalType(38,10), True),
    StructField("FED_DRUG_CLS_CD", StringType(), True),
    StructField("DEDCT_AMT", DecimalType(38,10), True),
    StructField("BNF_COPAY_100", StringType(), True),
    StructField("CLM_PRCS_TYP", StringType(), True),
    StructField("INDEM_HIER_TIER_NO", DecimalType(38,10), True),
    StructField("MCARE_D_COV_DRUG", StringType(), True),
    StructField("RETRO_LICS_CD", StringType(), True),
    StructField("RETRO_LICS_AMT", DecimalType(38,10), True),
    StructField("LICS_SBSDY_AMT", DecimalType(38,10), True),
    StructField("MCARE_B_DRUG", StringType(), True),
    StructField("MCARE_B_CLM", StringType(), True),
    StructField("PRSCRBR_QLFR", StringType(), True),
    StructField("PRSCRBR_NTNL_PROV_ID", StringType(), True),
    StructField("PDX_QLFR", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_APLD_AMT", DecimalType(38,10), True),
    StructField("THER_CLS", DecimalType(38,10), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_FLAG", StringType(), True),
    StructField("DOSE_CD", DecimalType(38,10), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", DecimalType(38,10), True),
    StructField("COPAY_BNF_OPT", DecimalType(38,10), True),
    StructField("GNRC_PROD_IN", DecimalType(38,10), True),
    StructField("PRSCRBR_SPEC", StringType(), True),
    StructField("VAL_CD", StringType(), True),
    StructField("PRI_CARE_PDX", StringType(), True),
    StructField("OFC_OF_INSPECTOR_GNRL", StringType(), True),
    StructField("PATN_SSN", StringType(), True),
    StructField("CARDHLDR_SSN", StringType(), True),
    StructField("CARDHLDR_BRTH_DT", StringType(), True),
    StructField("CARDHLDR_ADDR", StringType(), True),
    StructField("CARDHLDR_CITY", StringType(), True),
    StructField("CHADHLDR_ST", StringType(), True),
    StructField("CARDHLDR_ZIP_CD", StringType(), True),
    StructField("PSL_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_FMLY_AMT", DecimalType(38,10), True),
    StructField("DEDCT_FMLY_MET_AMT", StringType(), True),
    StructField("DEDCT_FMLY_AMT", DecimalType(38,10), True),
    StructField("MOPS_FMLY_AMT", DecimalType(38,10), True),
    StructField("MOPS_FMLY_MET_AMT", DecimalType(38,10), True),
    StructField("MOPS_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("DEDCT_MBR_MET_AMT", DecimalType(38,10), True),
    StructField("PSL_APLD_AMT", DecimalType(38,10), True),
    StructField("MOPS_APLD_AMT", DecimalType(38,10), True),
    StructField("PAR_PDX_IN", StringType(), True),
    StructField("COPAY_PCT_AMT", DecimalType(38,10), True),
    StructField("COPAY_FLAT_AMT", DecimalType(38,10), True),
    StructField("CLM_TRNSMSN_METH", StringType(), True),
    StructField("RX_NO_2012", DecimalType(38,10), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("SUBMT_PROD_ID_QLFR", StringType(), True),
    StructField("CNTNGNT_THER_FLAG", StringType(), True),
    StructField("CNTNGNT_THER_SCHD", StringType(), True),
    StructField("CLNT_PATN_PAY_ATRBD_PROD_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT", DecimalType(38,10), True),
    StructField("CLNT_PATN_PAY_ATRBD_NTWK_AMT", DecimalType(38,10), True),
    StructField("LOB_IN", StringType(), True)
])

# Read the file for stage "BCBSCommClmLand"
df_BCBSCommClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")
    .option("quote", "\"")
    .schema(schema_BCBSCommClmLand)
    .load(f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}")
)

# Apply the BusinessRules transformation
df_BusinessRules = (
    df_BCBSCommClmLand
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", lit(0))
    .withColumn("INSRT_UPDT_CD", lit("I"))
    .withColumn("DISCARD_IN", lit("N"))
    .withColumn("PASS_THRU_IN", lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", lit(0))
    .withColumn("RECYCLE_CT", lit(0))
    .withColumn("SRC_SYS_CD", col("SRC_SYS_CD"))
    .withColumn("CLM_ID", col("CLM_ID"))
    .withColumn("PRI_KEY_STRING", concat(col("SRC_SYS_CD"), lit(";"), col("CLM_ID")))
    .withColumn("CLM_REMIT_HIST_SK", lit(0))
    .withColumn("SRC_SYS_CD_SK", lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(0))
    .withColumn("CLM_SK", lit(0))
    .withColumn("CALC_ACTL_PD_AMT_IN", lit("N"))
    .withColumn("SUPRESS_EOB_IN", lit("N"))
    .withColumn("SUPRESS_REMIT_IN", lit("N"))
    .withColumn("ACTL_PD_AMT", col("BILL_AMT"))
    .withColumn("COB_PD_AMT", col("OTHR_PAYOR_AMT"))
    .withColumn("COINS_AMT", col("COPAY_PCT_AMT"))
    .withColumn("CNSD_CHRG_AMT", col("FULL_AVG_WHLSL_PRICE"))
    .withColumn("COPAY_AMT", col("COPAY_AMT"))
    .withColumn("DEDCT_AMT", col("DEDCT_AMT"))
    .withColumn("DSALW_AMT", lit(0.00))
    .withColumn("ER_COPAY_AMT", lit(0.00))
    .withColumn("INTRST_AMT", lit(0.00))
    .withColumn("NO_RESP_AMT", lit(0.00))
    .withColumn(
        "PATN_RESP_AMT",
        when(col("SRC_SYS_CD") == lit("OPTUMRX"), trim(col("CLM_AMT"))).otherwise(col("COPAY_AMT"))
    )
    .withColumn("WRTOFF_AMT", lit(0.00))
    .withColumn("PCA_PD_AMT", lit(0.00))
    .withColumn("ALT_CHRG_IN", lit("N"))
    .withColumn("ALT_CHRG_PROV_WRTOFF_AMT", lit(0.00))
)

# The "Snapshot" stage (pass-through in this job flow)
df_Snapshot = df_BusinessRules

# Split the Snapshot output into two links: "Pkey" and "RowCount"
# Pkey columns (pass everything out as specified)
df_Pkey = df_Snapshot.select(
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("CLM_REMIT_HIST_SK").alias("CLM_REMIT_HIST_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK").alias("CLM_SK"),
    col("CALC_ACTL_PD_AMT_IN").alias("CALC_ACTL_PD_AMT_IN"),
    col("SUPRESS_EOB_IN").alias("SUPRESS_EOB_IN"),
    col("SUPRESS_REMIT_IN").alias("SUPRESS_REMIT_IN"),
    col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    col("COB_PD_AMT").alias("COB_PD_AMT"),
    col("COINS_AMT").alias("COINS_AMT"),
    col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    col("COPAY_AMT").alias("COPAY_AMT"),
    col("DEDCT_AMT").alias("DEDCT_AMT"),
    col("DSALW_AMT").alias("DSALW_AMT"),
    col("ER_COPAY_AMT").alias("ER_COPAY_AMT"),
    col("INTRST_AMT").alias("INTRST_AMT"),
    col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    col("WRTOFF_AMT").alias("WRTOFF_AMT"),
    col("PCA_PD_AMT").alias("PCA_PD_AMT"),
    col("ALT_CHRG_IN").alias("ALT_CHRG_IN"),
    col("ALT_CHRG_PROV_WRTOFF_AMT").alias("ALT_CHRG_PROV_WRTOFF_AMT")
)

# RowCount link columns
df_RowCount = df_Snapshot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID")
)

# Write "RowCount" link to file ("B_CLM_REMIT_HIST")
df_BClmRemitHist = df_RowCount.select(
    col("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID")
)

write_files(
    df_BClmRemitHist,
    f"{adls_path}/load/B_CLM_REMIT_HIST.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Call the shared container "ClmRemitHistPK"
params = {
    "CurrRunCycle": RunCycle
}
df_ClmRemitHistPK = ClmRemitHistPK(df_Pkey, params)

# Final output "BCBSKCCommClmRemitHistExtr"
df_final = df_ClmRemitHistPK.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    col("CLM_REMIT_HIST_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    rpad(col("CALC_ACTL_PD_AMT_IN"), 1, " ").alias("CALC_ACTL_PD_AMT_IN"),
    rpad(col("SUPRESS_EOB_IN"), 1, " ").alias("SUPRESS_EOB_IN"),
    rpad(col("SUPRESS_REMIT_IN"), 1, " ").alias("SUPRESS_REMIT_IN"),
    col("ACTL_PD_AMT"),
    col("COB_PD_AMT"),
    col("COINS_AMT"),
    col("CNSD_CHRG_AMT"),
    col("COPAY_AMT"),
    col("DEDCT_AMT"),
    col("DSALW_AMT"),
    col("ER_COPAY_AMT"),
    col("INTRST_AMT"),
    col("NO_RESP_AMT"),
    col("PATN_RESP_AMT"),
    col("WRTOFF_AMT"),
    col("PCA_PD_AMT"),
    rpad(col("ALT_CHRG_IN"), 1, " ").alias("ALT_CHRG_IN"),
    col("ALT_CHRG_PROV_WRTOFF_AMT")
)

write_files(
    df_final,
    f"{adls_path}/key/BCBSKCCommClmRemitHistExtr_{SrcSysCd}.DrugClmRemitHist.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)