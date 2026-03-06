# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC Â© Copyright 2020 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC CALLED BY :  OptumACADrugCntl/OptumDrugCntl/OptumMedDDrugCntl
# MAGIC  
# MAGIC DESCRIPTION:  This Job Create the Load File from Sequential File created in Extract Job.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #              Change Description                                 Development Project      Code Reviewer          Date Reviewed
# MAGIC ------------------                      --------------------      ----------------------               ------------------------------------                             --------------------------------    -------------------------------      ----------------------------              
# MAGIC Revathi BoojiReddy        10-11-2022            US 559701                  Original Programming                              IntegrateDevB               Jeyaprasanna              2023-01-14


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, substring, length, when, rpad
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
IDSRunCycle = get_widget_value('IDSRunCycle','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
LOB = get_widget_value('LOB','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

df_db2_ClmSrcDtl = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT \n\nCLM_ID,\nCRT_RUN_CYC_EXCTN_SK,\nLAST_UPDT_RUN_CYC_EXCTN_SK\n\n \n\n FROM #$IDSOwner#.CLM_SRC_DTL")
    .load()
)

df_db2_Clm = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", "SELECT  \n\nCLM_SK,\nCLM_ID\n \nFROM #$IDSOwner#.CLM\n\nwhere \nLAST_UPDT_RUN_CYC_EXCTN_SK='#IDSRunCycle#'  AND\nSRC_SYS_CD_SK=#SrcSysCdSK#")
    .load()
)

schema_seq_PDX_CLM_STD_INPT_Land = StructType([
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("FILE_RCVD_DT", StringType(), True),
    StructField("RCRD_ID", IntegerType(), True),
    StructField("PRCSR_NO", StringType(), True),
    StructField("BTCH_NO", IntegerType(), True),
    StructField("PDX_NO", StringType(), True),
    StructField("RX_NO", StringType(), True),
    StructField("FILL_DT", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_OR_RFL_CD", IntegerType(), True),
    StructField("METRIC_QTY", DecimalType(38,10), True),
    StructField("DAYS_SUPL", IntegerType(), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST_AMT", DecimalType(38,10), True),
    StructField("DISPNS_FEE_AMT", DecimalType(38,10), True),
    StructField("COPAY_AMT", DecimalType(38,10), True),
    StructField("SLS_TAX_AMT", DecimalType(38,10), True),
    StructField("BILL_AMT", DecimalType(38,10), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("BRTH_DT", StringType(), True),
    StructField("SEX_CD", IntegerType(), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", IntegerType(), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", StringType(), True),
    StructField("PRSCRBR_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", StringType(), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", IntegerType(), True),
    StructField("RESUB_CYC_CT", IntegerType(), True),
    StructField("RX_DT", StringType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", IntegerType(), True),
    StructField("ELIG_CLRFCTN_CD", IntegerType(), True),
    StructField("CMPND_CD", IntegerType(), True),
    StructField("NO_OF_RFLS_AUTH", IntegerType(), True),
    StructField("LVL_OF_SVC", IntegerType(), True),
    StructField("RX_ORIG_CD", IntegerType(), True),
    StructField("RX_DENIAL_CLRFCTN", IntegerType(), True),
    StructField("PRI_PRSCRBR", StringType(), True),
    StructField("CLNC_ID_NO", IntegerType(), True),
    StructField("DRUG_TYP", IntegerType(), True),
    StructField("PRSCRBR_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT", DecimalType(38,10), True),
    StructField("UNIT_DOSE_IN", IntegerType(), True),
    StructField("OTHR_PAYOR_AMT", DecimalType(38,10), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", IntegerType(), True),
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
    StructField("ADJDCT_REF_NO", IntegerType(), True),
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
    StructField("INDEM_HIER_TIER_NO", StringType(), True),
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
    StructField("THER_CLS", StringType(), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HLTH_RMBRMT_ARGMT_FLAG", StringType(), True),
    StructField("DOSE_CD", StringType(), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", StringType(), True),
    StructField("COPAY_BNF_OPT", StringType(), True),
    StructField("GNRC_PROD_IN", StringType(), True),
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
    StructField("DEDCT_FMLY_MET_AMT", DecimalType(38,10), True),
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
    StructField("RX_NO_2012", StringType(), True),
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

df_seq_PDX_CLM_STD_INPT_Land = (
    spark.read
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .option("inferSchema", "false")
    .schema(schema_seq_PDX_CLM_STD_INPT_Land)
    .csv(f"{adls_path}/verified/PDX_CLM_STD_INPT_Land_{SrcSysCd}.dat.{RunID}")
)

df_outRegular = df_seq_PDX_CLM_STD_INPT_Land.select(
    col("CLM_ID")
)

df_outAdjustments = df_seq_PDX_CLM_STD_INPT_Land.filter(
    (substring(col("CLM_TYP"), 1, 1) == "X") | (substring(col("CLM_TYP"), 1, 1) == "R")
).select(
    substring(col("CLM_ID"), 1, (length(col("CLM_ID")) - 1)).alias("CLM_ID")
)

df_Fnl = df_outRegular.unionByName(df_outAdjustments)

df_Lkp_Clm = (
    df_Fnl.alias("Lnk_seq")
    .join(df_db2_Clm.alias("Ref_db2_Clm"), col("Lnk_seq.CLM_ID") == col("Ref_db2_Clm.CLM_ID"), how="left")
    .join(df_db2_ClmSrcDtl.alias("Ref_db2_ClmSrcDtl"), col("Lnk_seq.CLM_ID") == col("Ref_db2_ClmSrcDtl.CLM_ID"), how="left")
    .select(
        col("Ref_db2_Clm.CLM_SK").alias("CLM_SK"),
        col("Ref_db2_ClmSrcDtl.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("Lnk_seq.CLM_ID").alias("CLM_ID")
    )
)

df_XfmLoad = (
    df_Lkp_Clm
    .filter(col("CLM_SK").isNotNull())
    .select(
        col("CLM_SK").alias("CLM_SK"),
        lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
        col("CLM_ID").alias("CLM_ID"),
        when(col("CRT_RUN_CYC_EXCTN_SK").isNull(), IDSRunCycle).otherwise(col("CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(IDSRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(LOB).alias("SRC_FILE_ID")
    )
)

df_final = df_XfmLoad.select(
    col("CLM_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), 254, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("SRC_FILE_ID"), 254, " ").alias("SRC_FILE_ID")
)

write_files(
    df_final,
    f"{adls_path}/load/CLM_SRC_DTL_{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)