# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************************
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY:  ESIDrugExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:     Reads the ESIDrugFile.dat created in  ESIClmLand  job and puts the data into the claim cob history common record format and runs through primary key using shared container ClmCobPkey. Only populate for claims where third party amount <> 0
# MAGIC     
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Parikshith Chada     2008-07-09       3784                      Original Programming                                                                   devlIDS
# MAGIC 
# MAGIC Parik                       2008-09-25       3784 (PBM)            Made small changes to the logic in the job                                  devlIDSnew                    Steph Goddard          11/07/2008
# MAGIC Brent Leland           2008-11-20       3567 Primary Key   Changed to use new primary key process                                    devlIDS                          Steph Goddard          11/24/2008
# MAGIC 
# MAGIC Manasa Andru        2015-05-08         TFS - 1042            Changed the metadata for CLM_COB_TYP_CD to                      IntegrateNewDevl           Kalyan Neelam          2015-05-11 
# MAGIC                                                                                        Varchar 20 in the Transform output link.
# MAGIC 
# MAGIC Manasa Andru        2016-10-14         TFS - 13128         Changed the datatype of the field CLM_ID from Char18               IntegrateDev1                 Kalyan Neelam          2016-10-21
# MAGIC                                                                                       to Varchar20 in the Snapshot stage and Transform link.

# MAGIC Read weekly Invoice file from ESI  ESIDrugClmInvoiceLand
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
    IntegerType,
    DoubleType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmCobPK
# COMMAND ----------

# Retrieve all required parameter values
RunCycle = get_widget_value('RunCycle','1')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
_IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Define schema for ESI_Invoice (CSeqFileStage)
schema_esi_invoice = StructType([
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
    StructField("DOB", DoubleType(), False),
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
    StructField("DT_RX_WRTN", DoubleType(), False),
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
    StructField("PD_DT", DoubleType(), False),
    StructField("BNF_CD", StringType(), False),
    StructField("DRUG_STRG", StringType(), False),
    StructField("ORIG_MBR", StringType(), False),
    StructField("DT_OF_INJURY", DoubleType(), False),
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
    StructField("ESI_BILL_DT", DoubleType(), False),
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
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), False)
])

# Read file for ESI_Invoice
df_esi_invoice = (
    spark.read.format("csv")
    .option("header", "false")
    .option("inferSchema", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_esi_invoice)
    .load(f"{adls_path}/verified/ESIDrugClmInvoicePaidUpdt.dat.{RunID}")
)

# Filter and produce output (BusinessRules transform)
df_esi_invoice_filtered = df_esi_invoice.filter(
    (F.col("OTHR_COV_CD") == 2) & (F.col("OTHR_PAYOR_AMT") != 0)
)

df_BusinessRules = (
    df_esi_invoice_filtered
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLAIM_ID")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_COB_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLAIM_ID").alias("CLM_ID"),
        F.col("OTHR_COV_CD").alias("CLM_COB_TYP_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.lit("NA").alias("CLM_COB_LIAB_TYP_CD"),
        F.lit(0.00).alias("ALW_AMT"),
        F.lit(0.00).alias("COPAY_AMT"),
        F.lit(0.00).alias("DEDCT_AMT"),
        F.lit(0.00).alias("DSALW_AMT"),
        F.lit(0.00).alias("MED_COINS_AMT"),
        F.lit(0.00).alias("MNTL_HLTH_COINS_AMT"),
        F.col("OTHR_PAYOR_AMT").alias("PD_AMT"),
        F.lit(0.00).alias("SANC_AMT"),
        F.lit(" ").alias("COB_CAR_RSN_CD_TX"),
        F.lit(" ").alias("COB_CAR_RSN_TX")
    )
)

# Snapshot stage has three output links: RowCount, AllCol, Transform
df_Snapshot_RowCount = df_BusinessRules.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

df_Snapshot_AllCol = df_BusinessRules.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_COB_SK").alias("CLM_COB_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CLM_COB_LIAB_TYP_CD").alias("CLM_COB_LIAB_TYP_CD"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("MED_COINS_AMT").alias("MED_COINS_AMT"),
    F.col("MNTL_HLTH_COINS_AMT").alias("MNTL_HLTH_COINS_AMT"),
    F.col("PD_AMT").alias("PD_AMT"),
    F.col("SANC_AMT").alias("SANC_AMT"),
    F.col("COB_CAR_RSN_CD_TX").alias("COB_CAR_RSN_CD_TX"),
    F.col("COB_CAR_RSN_TX").alias("COB_CAR_RSN_TX")
)

df_Snapshot_Transform = df_BusinessRules.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_COB_TYP_CD").alias("CLM_COB_TYP_CD")
)

# Next Transformer stage takes RowCount => Produce final "Load"
df_TransformerStageVariables = (
    df_Snapshot_RowCount
    .withColumn(
        "ITSHost",
        F.when(F.col("CLM_COB_TYP_CD").substr(F.lit(1), F.lit(1)) == "*", F.lit("Y")).otherwise(F.lit("N"))
    )
    .withColumn(
        "ITSCOBType",
        F.when(F.col("ITSHost") == "Y", F.col("CLM_COB_TYP_CD").substr(F.lit(2), F.lit(1))).otherwise(F.lit("NA"))
    )
    .withColumn(
        "svClmCobTypCdSk",
        F.when(
            F.col("ITSHost") == "N",
            GetFkeyCodes("FACETS", 0, "CLAIM COB", F.col("CLM_COB_TYP_CD"), "X")
        ).otherwise(
            GetFkeyCodes("FIT", 0, "CLAIM COB", F.col("ITSCOBType"), "X")
        )
    )
)

df_Transformer = df_TransformerStageVariables.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("svClmCobTypCdSk").alias("CLM_COB_TYP_CD_SK"),
    F.col("ALW_AMT").alias("ALW_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

# Write B_CLM_COB (CSeqFileStage)
df_final_B_CLM_COB = df_Transformer.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_COB_TYP_CD_SK",
    "ALW_AMT",
    "PD_AMT"
)

write_files(
    df_final_B_CLM_COB,
    f"{adls_path}/load/B_CLM_COB.ESI.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Call Shared Container ClmCobPK with two inputs (AllCol, Transform)
params_ClmCobPK = {
    "CurrRunCycle": RunCycle,
    "RunID": RunID,
    "SrcSysCdSk": SrcSysCdSk,
    "SrcSysCd": SrcSysCd,
    "CurrentDate": CurrentDate,
    "$IDSOwner": _IDSOwner
}
output_df_ClmCobPK = ClmCobPK(df_Snapshot_AllCol, df_Snapshot_Transform, params_ClmCobPK)

# ESIClmCobKey (CSeqFileStage) => final output
df_final_ESIClmCobKey = (
    output_df_ClmCobPK
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_COB_LIAB_TYP_CD", F.rpad(F.col("CLM_COB_LIAB_TYP_CD"), 20, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_COB_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_COB_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CLM_COB_LIAB_TYP_CD",
        "ALW_AMT",
        "COPAY_AMT",
        "DEDCT_AMT",
        "DSALW_AMT",
        "MED_COINS_AMT",
        "MNTL_HLTH_COINS_AMT",
        "PD_AMT",
        "SANC_AMT",
        "COB_CAR_RSN_CD_TX",
        "COB_CAR_RSN_TX"
    )
)

write_files(
    df_final_ESIClmCobKey,
    f"{adls_path}/key/ESIClmCOBExtr.DrugClmCOB.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)