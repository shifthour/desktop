# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC 
# MAGIC     Reads the BCAFEPMedClm_ClaimsLanding file created in BCAFEPClmPrepProcExtr job and  transforms it into the common record format file.
# MAGIC    
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #                     Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------                     -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ---------------------------- 
# MAGIC Ravi Abburi             2017-10-04        5781 - FEP Claims                Initial Programming                                                                      IntegrateDev2                   Kalyan Neelam         2017-10-18
# MAGIC 
# MAGIC Saikiran Mahenderker    2018-03-28   5781 HEDIS                        Added new Columns                                                                  IntegrateDev2                  Jaideep Mankala      04/02/2018
# MAGIC                                                                                                    PERFORMING_NTNL_PROV_ID,PD_DAYS
# MAGIC 
# MAGIC Karthik Chintalapani   2020-03-23   5781 HEDIS   Changed the logic for ICD9 and ICD10 codes determinitation.          IntegrateDev1                       Jaideep Mankala       03/29/2020

# MAGIC Proc Cods file is used in 
# MAGIC BCAFEPClmDRGExtr job
# MAGIC Read the BCAFEP Extract file created from BCAFEPMedClm_ClaimsLanding
# MAGIC This container is used in:
# MAGIC FctsClmFcltyProcExtr
# MAGIC NascoClmFcltyProcExtr
# MAGIC BCBSSCClmFcltyProcExtr
# MAGIC BCBSAClmFcltyProcExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to ../key
# MAGIC Hash file (hf_fclty_proc_allcol) cleared from the shared container FcltyClmProcPK
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")
CurrDate = get_widget_value("CurrDate","")
IDSOwner = get_widget_value("IDSOwner","")
ids_secret_name = get_widget_value("ids_secret_name","")

# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmProcPK
# COMMAND ----------

schema_BCAFEPClmLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", IntegerType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("SRC_SYS", StringType(), True),
    StructField("RCRD_ID", StringType(), True),
    StructField("ADJ_NO", StringType(), True),
    StructField("PERFORMING_PROV_ID", StringType(), True),
    StructField("FEP_PROD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("BILL_TYP__CD", StringType(), True),
    StructField("FEP_SVC_LOC_CD", StringType(), True),
    StructField("IP_CLM_TYP_IN", StringType(), True),
    StructField("DRG_VRSN_IN", StringType(), True),
    StructField("DRG_CD", StringType(), True),
    StructField("PATN_STTUS_CD", StringType(), True),
    StructField("CLM_CLS_IN", StringType(), True),
    StructField("CLM_DENIED_FLAG", StringType(), True),
    StructField("ILNS_DT", StringType(), True),
    StructField("IP_CLM_BEG_DT", StringType(), True),
    StructField("IP_CLM_DSCHG_DT", StringType(), True),
    StructField("CLM_SVC_DT_BEG", StringType(), True),
    StructField("CLM_SVC_DT_END", StringType(), True),
    StructField("FCLTY_CLM_STMNT_BEG_DT", StringType(), True),
    StructField("FCLTY_CLM_STMNT_END_DT", StringType(), True),
    StructField("CLM_PRCS_DT", StringType(), True),
    StructField("IP_ADMS_CT", StringType(), True),
    StructField("NO_COV_DAYS", IntegerType(), True),
    StructField("DIAG_CDNG_TYP", StringType(), True),
    StructField("PRI_DIAG_CD", StringType(), True),
    StructField("PRI_DIAG_POA_IN", StringType(), True),
    StructField("ADM_DIAG_CD", StringType(), True),
    StructField("ADM_DIAG_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_1", StringType(), True),
    StructField("OTHR_DIAG_CD_1_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_2", StringType(), True),
    StructField("OTHR_DIAG_CD_2_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_3", StringType(), True),
    StructField("OTHR_DIAG_CD_3_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_4", StringType(), True),
    StructField("OTHR_DIAG_CD_4_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_5", StringType(), True),
    StructField("OTHR_DIAG_CD_5_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_6", StringType(), True),
    StructField("OTHR_DIAG_CD_6_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_7", StringType(), True),
    StructField("OTHR_DIAG_CD_7_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_8", StringType(), True),
    StructField("OTHR_DIAG_CD_8_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_9", StringType(), True),
    StructField("OTHR_DIAG_CD_9_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_10", StringType(), True),
    StructField("OTHR_DIAG_CD_10_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_11", StringType(), True),
    StructField("OTHR_DIAG_CD_11_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_12", StringType(), True),
    StructField("OTHR_DIAG_CD_12_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_13", StringType(), True),
    StructField("OTHR_DIAG_CD_13_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_14", StringType(), True),
    StructField("OTHR_DIAG_CD_14_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_15", StringType(), True),
    StructField("OTHR_DIAG_CD_15_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_16", StringType(), True),
    StructField("OTHR_DIAG_CD_16_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_17", StringType(), True),
    StructField("OTHR_DIAG_CD_17_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_18", StringType(), True),
    StructField("OTHR_DIAG_CD_18_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_19", StringType(), True),
    StructField("OTHR_DIAG_CD_19_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_20", StringType(), True),
    StructField("OTHR_DIAG_CD_20_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_21", StringType(), True),
    StructField("OTHR_DIAG_CD_21_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_22", StringType(), True),
    StructField("OTHR_DIAG_CD_22_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_23", StringType(), True),
    StructField("OTHR_DIAG_CD_23_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_24", StringType(), True),
    StructField("OTHR_DIAG_CD_24_POA_IN", StringType(), True),
    StructField("PROC_CDNG_TYP", StringType(), True),
    StructField("PRINCIPLE_PROC_CD", StringType(), True),
    StructField("PRINCIPLE_PROC_CD_DT", StringType(), True),
    StructField("OTHR_PROC_CD_1", StringType(), True),
    StructField("OTHR_PROC_CD_1_DT", StringType(), True),
    StructField("OTHR_PROC_CD_2", StringType(), True),
    StructField("OTHR_PROC_CD_2_DT", StringType(), True),
    StructField("OTHR_PROC_CD_3", StringType(), True),
    StructField("OTHR_PROC_CD_3_DT", StringType(), True),
    StructField("OTHR_PROC_CD_4", StringType(), True),
    StructField("OTHR_PROC_CD_4_DT", StringType(), True),
    StructField("OTHR_PROC_CD_5", StringType(), True),
    StructField("OTHR_PROC_CD_5_DT", StringType(), True),
    StructField("OTHR_PROC_CD_6", StringType(), True),
    StructField("OTHR_PROC_CD_6_DT", StringType(), True),
    StructField("OTHR_PROC_CD_7", StringType(), True),
    StructField("OTHR_PROC_CD_7_DT", StringType(), True),
    StructField("OTHR_PROC_CD_8", StringType(), True),
    StructField("OTHR_PROC_CD_8_DT", StringType(), True),
    StructField("OTHR_PROC_CD_9", StringType(), True),
    StructField("OTHR_PROC_CD_9_DT", StringType(), True),
    StructField("OTHR_PROC_CD_10", StringType(), True),
    StructField("OTHR_PROC_CD_10_DT", StringType(), True),
    StructField("OTHR_PROC_CD_11", StringType(), True),
    StructField("OTHR_PROC_CD_11_DT", StringType(), True),
    StructField("OTHR_PROC_CD_12", StringType(), True),
    StructField("OTHR_PROC_CD_12_DT", StringType(), True),
    StructField("OTHR_PROC_CD_13", StringType(), True),
    StructField("OTHR_PROC_CD_13_DT", StringType(), True),
    StructField("OTHR_PROC_CD_14", StringType(), True),
    StructField("OTHR_PROC_CD_14_DT", StringType(), True),
    StructField("OTHR_PROC_CD_15", StringType(), True),
    StructField("OTHR_PROC_CD_15_DT", StringType(), True),
    StructField("OTHR_PROC_CD_16", StringType(), True),
    StructField("OTHR_PROC_CD_16_DT", StringType(), True),
    StructField("OTHR_PROC_CD_17", StringType(), True),
    StructField("OTHR_PROC_CD_17_DT", StringType(), True),
    StructField("OTHR_PROC_CD_18", StringType(), True),
    StructField("OTHR_PROC_CD_18_DT", StringType(), True),
    StructField("OTHR_PROC_CD_19", StringType(), True),
    StructField("OTHR_PROC_CD_19_DT", StringType(), True),
    StructField("OTHR_PROC_CD_20", StringType(), True),
    StructField("OTHR_PROC_CD_20_DT", StringType(), True),
    StructField("OTHR_PROC_CD_21", StringType(), True),
    StructField("OTHR_PROC_CD_21_DT", StringType(), True),
    StructField("OTHR_PROC_CD_22", StringType(), True),
    StructField("OTHR_PROC_CD_22_DT", StringType(), True),
    StructField("OTHR_PROC_CD_23", StringType(), True),
    StructField("OTHR_PROC_CD_23_DT", StringType(), True),
    StructField("OTHR_PROC_CD_24", StringType(), True),
    StructField("OTHR_PROC_CD_24_DT", StringType(), True),
    StructField("RVNU_CD", StringType(), True),
    StructField("PROC_CD_NON_ICD", StringType(), True),
    StructField("PROC_CD_MOD_1", StringType(), True),
    StructField("PROC_CD_MOD_2", StringType(), True),
    StructField("PROC_CD_MOD_3", StringType(), True),
    StructField("PROC_CD_MOD_4", StringType(), True),
    StructField("CLM_UNIT", DecimalType(38,10), True),
    StructField("CLM_LN_TOT_ALL_SVC_CHRG_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_PD_AMT", DecimalType(38,10), True),
    StructField("CLM_ALT_AMT_PD_1", DecimalType(38,10), True),
    StructField("CLM_ALT_AMT_PD_2", DecimalType(38,10), True),
    StructField("LOINC_CD", StringType(), True),
    StructField("CLM_TST_RSLT", StringType(), True),
    StructField("ALT_CLM_TST_RSLT", StringType(), True),
    StructField("DRUG_CD", StringType(), True),
    StructField("DRUG_CLM_INCUR_DT", StringType(), True),
    StructField("CLM_TREAT_DURATN", StringType(), True),
    StructField("DATA_SRC", StringType(), True),
    StructField("SUPLMT_DATA_SRC_TYP", StringType(), True),
    StructField("ADTR_APRV_STTUS", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("PD_DAYS", IntegerType(), True),
    StructField("PERFORMING_NTNL_PROV_ID", StringType(), True)
])

df_BCAFEPClmLanding = (
    spark.read
    .option("header", False)
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_BCAFEPClmLanding)
    .csv(f"{adls_path}/verified/BCAFEPCMedClm_ClmLanding.dat.{RunID}")
)

df_Xfm_Clm_Ln_Extract_input = df_BCAFEPClmLanding

df_Xfm_Clm_Ln_Extract_filtered = df_Xfm_Clm_Ln_Extract_input.filter(
    F.col("CLM_CLS_IN") == "F"
)

df_Xfm_Clm_Ln_Extract_BCAFEP = df_Xfm_Clm_Ln_Extract_filtered.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("PRINCIPLE_PROC_CD").alias("PRINCIPLE_PROC_CD"),
    F.col("OTHR_PROC_CD_1").alias("OTHR_PROC_CD_1"),
    F.col("OTHR_PROC_CD_2").alias("OTHR_PROC_CD_2"),
    F.col("OTHR_PROC_CD_3").alias("OTHR_PROC_CD_3"),
    F.col("OTHR_PROC_CD_4").alias("OTHR_PROC_CD_4"),
    F.col("OTHR_PROC_CD_5").alias("OTHR_PROC_CD_5"),
    F.col("OTHR_PROC_CD_6").alias("OTHR_PROC_CD_6"),
    F.col("OTHR_PROC_CD_7").alias("OTHR_PROC_CD_7"),
    F.col("OTHR_PROC_CD_8").alias("OTHR_PROC_CD_8"),
    F.col("OTHR_PROC_CD_9").alias("OTHR_PROC_CD_9"),
    F.col("OTHR_PROC_CD_10").alias("OTHR_PROC_CD_10"),
    F.col("OTHR_PROC_CD_11").alias("OTHR_PROC_CD_11"),
    F.col("OTHR_PROC_CD_12").alias("OTHR_PROC_CD_12"),
    F.col("OTHR_PROC_CD_13").alias("OTHR_PROC_CD_13"),
    F.col("OTHR_PROC_CD_14").alias("OTHR_PROC_CD_14"),
    F.col("OTHR_PROC_CD_15").alias("OTHR_PROC_CD_15"),
    F.col("OTHR_PROC_CD_16").alias("OTHR_PROC_CD_16"),
    F.col("OTHR_PROC_CD_17").alias("OTHR_PROC_CD_17"),
    F.col("OTHR_PROC_CD_18").alias("OTHR_PROC_CD_18"),
    F.col("OTHR_PROC_CD_19").alias("OTHR_PROC_CD_19"),
    F.col("OTHR_PROC_CD_20").alias("OTHR_PROC_CD_20"),
    F.col("OTHR_PROC_CD_21").alias("OTHR_PROC_CD_21"),
    F.col("OTHR_PROC_CD_22").alias("OTHR_PROC_CD_22"),
    F.col("OTHR_PROC_CD_23").alias("OTHR_PROC_CD_23"),
    F.col("OTHR_PROC_CD_24").alias("OTHR_PROC_CD_24"),
    F.col("PRINCIPLE_PROC_CD_DT").alias("PRINCIPLE_PROC_CD_DT"),
    F.col("OTHR_PROC_CD_1_DT").alias("OTHR_PROC_CD_1_DT"),
    F.col("OTHR_PROC_CD_2_DT").alias("OTHR_PROC_CD_2_DT"),
    F.col("OTHR_PROC_CD_3_DT").alias("OTHR_PROC_CD_3_DT"),
    F.col("OTHR_PROC_CD_4_DT").alias("OTHR_PROC_CD_4_DT"),
    F.col("OTHR_PROC_CD_5_DT").alias("OTHR_PROC_CD_5_DT"),
    F.col("OTHR_PROC_CD_6_DT").alias("OTHR_PROC_CD_6_DT"),
    F.col("OTHR_PROC_CD_7_DT").alias("OTHR_PROC_CD_7_DT"),
    F.col("OTHR_PROC_CD_8_DT").alias("OTHR_PROC_CD_8_DT"),
    F.col("OTHR_PROC_CD_9_DT").alias("OTHR_PROC_CD_9_DT"),
    F.col("OTHR_PROC_CD_10_DT").alias("OTHR_PROC_CD_10_DT"),
    F.col("OTHR_PROC_CD_11_DT").alias("OTHR_PROC_CD_11_DT"),
    F.col("OTHR_PROC_CD_12_DT").alias("OTHR_PROC_CD_12_DT"),
    F.col("OTHR_PROC_CD_13_DT").alias("OTHR_PROC_CD_13_DT"),
    F.col("OTHR_PROC_CD_14_DT").alias("OTHR_PROC_CD_14_DT"),
    F.col("OTHR_PROC_CD_15_DT").alias("OTHR_PROC_CD_15_DT"),
    F.col("OTHR_PROC_CD_16_DT").alias("OTHR_PROC_CD_16_DT"),
    F.col("OTHR_PROC_CD_17_DT").alias("OTHR_PROC_CD_17_DT"),
    F.col("OTHR_PROC_CD_18_DT").alias("OTHR_PROC_CD_18_DT"),
    F.col("OTHR_PROC_CD_19_DT").alias("OTHR_PROC_CD_19_DT"),
    F.col("OTHR_PROC_CD_20_DT").alias("OTHR_PROC_CD_20_DT"),
    F.col("OTHR_PROC_CD_21_DT").alias("OTHR_PROC_CD_21_DT"),
    F.col("OTHR_PROC_CD_22_DT").alias("OTHR_PROC_CD_22_DT"),
    F.col("OTHR_PROC_CD_23_DT").alias("OTHR_PROC_CD_23_DT"),
    F.col("OTHR_PROC_CD_24_DT").alias("OTHR_PROC_CD_24_DT"),
    F.lit("1").alias("PROCNO1"),
    F.lit("2").alias("PROCNO2"),
    F.lit("3").alias("PROCNO3"),
    F.lit("4").alias("PROCNO4"),
    F.lit("5").alias("PROCNO5"),
    F.lit("6").alias("PROCNO6"),
    F.lit("7").alias("PROCNO7"),
    F.lit("8").alias("PROCNO8"),
    F.lit("9").alias("PROCNO9"),
    F.lit("10").alias("PROCNO10"),
    F.lit("11").alias("PROCNO11"),
    F.lit("12").alias("PROCNO12"),
    F.lit("13").alias("PROCNO13"),
    F.lit("14").alias("PROCNO14"),
    F.lit("15").alias("PROCNO15"),
    F.lit("16").alias("PROCNO16"),
    F.lit("17").alias("PROCNO17"),
    F.lit("18").alias("PROCNO18"),
    F.lit("19").alias("PROCNO19"),
    F.lit("20").alias("PROCNO20"),
    F.lit("21").alias("PROCNO21"),
    F.lit("22").alias("PROCNO22"),
    F.lit("23").alias("PROCNO23"),
    F.lit("24").alias("PROCNO24"),
    F.lit("25").alias("PROCNO25"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD1"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD2"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD3"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD4"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD5"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD6"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD7"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD8"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD9"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD10"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD11"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD10_CD12"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD1"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD2"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD3"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD4"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD5"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD6"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD7"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD8"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD9"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD10"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD11"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD12"),
    F.when(F.col("PROC_CDNG_TYP").isin("9","I9"), "ICD9")
     .when(F.col("PROC_CDNG_TYP").isin("0","I10"), "ICD10")
     .otherwise(F.lit(None)).alias("ICD9_CD13")
)

array_ProcCdDt = F.array(
    "PRINCIPLE_PROC_CD_DT","OTHR_PROC_CD_1_DT","OTHR_PROC_CD_2_DT","OTHR_PROC_CD_3_DT","OTHR_PROC_CD_4_DT","OTHR_PROC_CD_5_DT",
    "OTHR_PROC_CD_6_DT","OTHR_PROC_CD_7_DT","OTHR_PROC_CD_8_DT","OTHR_PROC_CD_9_DT","OTHR_PROC_CD_10_DT","OTHR_PROC_CD_11_DT",
    "OTHR_PROC_CD_12_DT","OTHR_PROC_CD_13_DT","OTHR_PROC_CD_14_DT","OTHR_PROC_CD_15_DT","OTHR_PROC_CD_16_DT","OTHR_PROC_CD_17_DT",
    "OTHR_PROC_CD_18_DT","OTHR_PROC_CD_19_DT","OTHR_PROC_CD_20_DT","OTHR_PROC_CD_21_DT","OTHR_PROC_CD_22_DT","OTHR_PROC_CD_23_DT",
    "OTHR_PROC_CD_24_DT"
)
array_ProcCd = F.array(
    "PRINCIPLE_PROC_CD","OTHR_PROC_CD_1","OTHR_PROC_CD_2","OTHR_PROC_CD_3","OTHR_PROC_CD_4","OTHR_PROC_CD_5",
    "OTHR_PROC_CD_6","OTHR_PROC_CD_7","OTHR_PROC_CD_8","OTHR_PROC_CD_9","OTHR_PROC_CD_10","OTHR_PROC_CD_11",
    "OTHR_PROC_CD_12","OTHR_PROC_CD_13","OTHR_PROC_CD_14","OTHR_PROC_CD_15","OTHR_PROC_CD_16","OTHR_PROC_CD_17",
    "OTHR_PROC_CD_18","OTHR_PROC_CD_19","OTHR_PROC_CD_20","OTHR_PROC_CD_21","OTHR_PROC_CD_22","OTHR_PROC_CD_23",
    "OTHR_PROC_CD_24"
)
array_ProcNo = F.array(
    "PROCNO1","PROCNO2","PROCNO3","PROCNO4","PROCNO5","PROCNO6","PROCNO7","PROCNO8","PROCNO9","PROCNO10",
    "PROCNO11","PROCNO12","PROCNO13","PROCNO14","PROCNO15","PROCNO16","PROCNO17","PROCNO18","PROCNO19","PROCNO20",
    "PROCNO21","PROCNO22","PROCNO23","PROCNO24","PROCNO25"
)
array_ProcCdTypCd = F.array(
    "ICD10_CD1","ICD10_CD2","ICD10_CD3","ICD10_CD4","ICD10_CD5","ICD10_CD6","ICD10_CD7","ICD10_CD8","ICD10_CD9","ICD10_CD10",
    "ICD10_CD11","ICD10_CD12","ICD9_CD1","ICD9_CD2","ICD9_CD3","ICD9_CD4","ICD9_CD5","ICD9_CD6","ICD9_CD7","ICD9_CD8",
    "ICD9_CD9","ICD9_CD10","ICD9_CD11","ICD9_CD12","ICD9_CD13"
)

df_afterPivot = (
    df_Xfm_Clm_Ln_Extract_BCAFEP
    .select(
        "CLM_ID",
        F.arrays_zip(array_ProcCdDt, array_ProcCd, array_ProcNo, array_ProcCdTypCd).alias("zipped")
    )
    .select("CLM_ID", F.explode("zipped").alias("pivoted"))
    .select(
        F.col("CLM_ID"),
        F.col("pivoted.array_ProcCdDt").alias("PROC_CD_DT"),
        F.col("pivoted.array_ProcCd").alias("PROC_CD"),
        F.col("pivoted.array_ProcNo").alias("PROC_NO"),
        F.col("pivoted.array_ProcCdTypCd").alias("PROC_CD_TYP_CD")
    )
)

df_Xfm_ProcCd_Chk_input = df_afterPivot

df_srt_1 = df_Xfm_ProcCd_Chk_input.filter(
    (F.col("PROC_CD").isNotNull()) & (F.length(trim("PROC_CD")) > 0)
).select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.regexp_replace(F.col("PROC_CD"), "\\.", "").alias("PROC_CD"),
    F.col("PROC_NO").alias("PROC_NO"),
    F.col("PROC_CD_DT").alias("PROC_CD_DT"),
    F.col("PROC_CD_TYP_CD").alias("PROC_CD_TYP_CD")
)

df_srt_asc1 = df_srt_1.sort(F.col("CLM_ID").asc(), F.col("PROC_NO").desc())

df_rm_dup = df_srt_asc1.select(
    "CLM_ID","PROC_CD","PROC_NO","PROC_CD_DT","PROC_CD_TYP_CD"
)

# Replace the hashed file with dedup logic on key columns: CLM_ID, PROC_CD
df_hf_bcafep_fcltyclmproc_dedupe = df_rm_dup.dropDuplicates(["CLM_ID","PROC_CD"])

df_srt_asc2 = df_hf_bcafep_fcltyclmproc_dedupe.sort(F.col("CLM_ID").asc(), F.col("PROC_NO").asc())

df_BusinessLogic_input = df_srt_asc2

windowSpec = Window.partitionBy("CLM_ID").orderBy("PROC_NO")
df_BusinessLogic = (
    df_BusinessLogic_input
    .withColumn("svOrdnlNo", F.row_number().over(windowSpec))
)

df_Transform1 = df_BusinessLogic.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID"), F.lit(";"), F.col("svOrdnlNo")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_PROC_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("svOrdnlNo").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.when(F.col("PROC_CD_DT").isNull(), F.lit("1753-01-01")).otherwise(F.col("PROC_CD_DT")).alias("PROC_DT"),
    F.col("PROC_CD_TYP_CD").alias("PROC_TYPE_CD"),
    F.lit(" ").alias("PROC_CD_MOD_TX"),
    F.lit("MED").alias("PROC_CD_CAT_CD")
)

df_ProcCds = df_BusinessLogic.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("svOrdnlNo").alias("PROC_ORDNL_CD"),
    F.col("PROC_CD").alias("PROC_CD")
)

write_files(
    df_ProcCds.select(
        F.col("CLM_ID"),
        F.rpad(F.col("PROC_ORDNL_CD"), 2, " ").alias("PROC_ORDNL_CD"),
        F.rpad(F.col("PROC_CD"), 7, " ").alias("PROC_CD")
    ),
    f"{adls_path}/verified/BCAFEPClmFcltyProcExtr.BCAFEPClmFcltyProcCds.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_Xfm_Snapshot_input = df_Transform1

df_AllCol = df_Xfm_Snapshot_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_PROC_SK").alias("CLM_PROC_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("PROC_CD").alias("PROC_CD"),
    F.col("PROC_DT").alias("PROC_DT"),
    F.col("PROC_TYPE_CD").alias("PROC_TYPE_CD"),
    F.col("PROC_CD_MOD_TX").alias("PROC_CD_MOD_TX"),
    F.col("PROC_CD_CAT_CD").alias("PROC_CD_CAT_CD")
)

df_SnapShot = df_Xfm_Snapshot_input.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD")
)

df_Transform = df_Xfm_Snapshot_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_PROC_ORDNL_CD").alias("FCLTY_CLM_PROC_ORDNL_CD")
)

df_Xfm_RowCnt_input = df_SnapShot

df_Xfm_RowCnt = df_Xfm_RowCnt_input.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    GetFkeyCodes(SrcSysCd, 0, "PROCEDURE ORDINAL", F.col("FCLTY_CLM_PROC_ORDNL_CD"), "X").alias("FCLTY_CLM_PROC_ORDNL_CD_SK")
)

write_files(
    df_Xfm_RowCnt.select("SRC_SYS_CD_SK","CLM_ID","FCLTY_CLM_PROC_ORDNL_CD_SK"),
    f"{adls_path}/load/B_FCLTY_CLM_PROC.BCAFEP.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_Key = FcltyClmProcPK(df_AllCol, df_Transform, params)

write_files(
    df_Key.select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CLM_PROC_SK"),
        F.col("CLM_ID"),
        F.rpad(F.col("FCLTY_CLM_PROC_ORDNL_CD"), 2, " ").alias("FCLTY_CLM_PROC_ORDNL_CD"),
        F.col("CRT_RUN_CYC_EXTCN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
        F.col("PROC_CD"),
        F.rpad(F.col("PROC_DT"), 10, " ").alias("PROC_DT"),
        F.col("PROC_TYPE_CD"),
        F.rpad(F.col("PROC_CD_MOD_TX"), 2, " ").alias("PROC_CD_MOD_TX"),
        F.col("PROC_CD_CAT_CD")
    ),
    f"{adls_path}/key/BCAFEPClmFcltyProcExtr.BCAFEPClmFcltyProc.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)