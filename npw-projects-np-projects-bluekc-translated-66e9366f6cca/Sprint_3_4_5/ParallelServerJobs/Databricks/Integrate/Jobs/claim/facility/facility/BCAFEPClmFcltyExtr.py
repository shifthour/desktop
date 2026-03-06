# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Called by:
# MAGIC                     BCAFEPFcltyClmDRGSeq
# MAGIC 
# MAGIC Processing:
# MAGIC                     Transform extracted Facility Claim data from BCAFEP into a common format 
# MAGIC 	    to be potentially merged with facility claim records from other systems  
# MAGIC 	    for the primary keying process and ultimate load into IDS
# MAGIC 
# MAGIC Control Job Rerun Information: 
# MAGIC                     Previous Run Successful:    What needs to happen before a special run can occur?
# MAGIC                     Previous Run Aborted:         Restart, no other steps necessary
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date               Project/Altiris #            Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------            -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ---------------------------- 
# MAGIC Ravi Abburi                     2017-09-29       5781 - FEP Claims        Initial Programming                                                                           IntegrateDev2                 Kalyan Neelam          2017-10-18
# MAGIC 
# MAGIC Saikiran Mahenderker     2018-03-28       5781 HEDIS                Added new Column PD_DAYS                                                        IntegrateDev2                Jaideep Mankala       04/02/2018
# MAGIC                                                                                                      mapped to HOSP_COV_DAYS
# MAGIC 
# MAGIC Sudhir Bomshetty            2018-04-18       5781 HEDIS                Added nulll value check for  HOSP_COV_DAYS                             IntegrateDev2                 Kalyan Neelam          2018-04-19

# MAGIC Grouper Input files created in BCAFEPClmDRGExtr job
# MAGIC Read the BCAFEP file created in BCAFEPClmDRGExtr
# MAGIC This container is used in:
# MAGIC FctsClmFcltyTrns
# MAGIC NascoClmFcltyExtr
# MAGIC BCBSSCClmFcltyExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
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
    DecimalType
)
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmPK
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSycCdSk = get_widget_value('SrcSycCdSk','-2022470479')
SrcSysCd = get_widget_value('SrcSysCd','BCA')
CurrDate = get_widget_value('CurrDate','2018-04-18')

# ---------------------------------------------------------------------
# Read "BCAFEPClmFcltyHold" (CSeqFileStage)
# ---------------------------------------------------------------------
schema_bcafep = StructType([
    StructField("RowNum", StringType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_LN_NO", DecimalType(38,10), False),
    StructField("ADJ_FROM_CLM_ID", StringType(), False),
    StructField("ADJ_TO_CLM_ID", StringType(), False),
    StructField("CLM_STTUS_CD", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("SRC_SYS", StringType(), False),
    StructField("RCRD_ID", StringType(), False),
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
    StructField("PD_DAYS", IntegerType(), True)
])

df_bcafep = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")
    .option("quote", "\"")
    .schema(schema_bcafep)
    .load(f"{adls_path}/verified/BCAFEPClmFcltyHold.dat.{RunID}")
)

# ---------------------------------------------------------------------
# Read "GeneratedPOA" (CSeqFileStage)
# ---------------------------------------------------------------------
schema_generatedpoa = StructType([
    StructField("HE_CLM_ID", StringType(), False),
    StructField("DIAG_CD_1", StringType(), False),
    StructField("DIAG_CD_2", StringType(), False),
    StructField("DIAG_CD_3", StringType(), False),
    StructField("DIAG_CD_4", StringType(), False),
    StructField("DIAG_CD_5", StringType(), False),
    StructField("DIAG_CD_6", StringType(), False),
    StructField("DIAG_CD_7", StringType(), False),
    StructField("DIAG_CD_8", StringType(), False),
    StructField("DIAG_CD_9", StringType(), False),
    StructField("DIAG_CD_10", StringType(), False),
    StructField("DIAG_CD_11", StringType(), False),
    StructField("DIAG_CD_12", StringType(), False),
    StructField("DIAG_CD_13", StringType(), False),
    StructField("DIAG_CD_14", StringType(), False),
    StructField("DIAG_CD_15", StringType(), False),
    StructField("DIAG_CD_16", StringType(), False),
    StructField("DIAG_CD_17", StringType(), False),
    StructField("DIAG_CD_18", StringType(), False),
    StructField("DIAG_CD_19", StringType(), False),
    StructField("DIAG_CD_20", StringType(), False),
    StructField("DIAG_CD_21", StringType(), False),
    StructField("DIAG_CD_22", StringType(), False),
    StructField("DIAG_CD_23", StringType(), False),
    StructField("DIAG_CD_24", StringType(), False),
    StructField("DIAG_CD_25", StringType(), False),
    StructField("DX_TYPE_1", StringType(), False),
    StructField("DX_TYPE_2", StringType(), False),
    StructField("DX_TYPE_3", StringType(), False),
    StructField("DX_TYPE_4", StringType(), False),
    StructField("DX_TYPE_5", StringType(), False),
    StructField("DX_TYPE_6", StringType(), False),
    StructField("DX_TYPE_7", StringType(), False),
    StructField("DX_TYPE_8", StringType(), False),
    StructField("DX_TYPE_9", StringType(), False),
    StructField("DX_TYPE_10", StringType(), False),
    StructField("DX_TYPE_11", StringType(), False),
    StructField("DX_TYPE_12", StringType(), False),
    StructField("DX_TYPE_13", StringType(), False),
    StructField("DX_TYPE_14", StringType(), False),
    StructField("DX_TYPE_15", StringType(), False),
    StructField("DX_TYPE_16", StringType(), False),
    StructField("DX_TYPE_17", StringType(), False),
    StructField("DX_TYPE_18", StringType(), False),
    StructField("DX_TYPE_19", StringType(), False),
    StructField("DX_TYPE_20", StringType(), False),
    StructField("DX_TYPE_21", StringType(), False),
    StructField("DX_TYPE_22", StringType(), False),
    StructField("DX_TYPE_23", StringType(), False),
    StructField("DX_TYPE_24", StringType(), False),
    StructField("DX_TYPE_25", StringType(), False),
    StructField("PROC_CD_1", StringType(), False),
    StructField("PROC_CD_2", StringType(), False),
    StructField("PROC_CD_3", StringType(), False),
    StructField("PROC_CD_4", StringType(), False),
    StructField("PROC_CD_5", StringType(), False),
    StructField("PROC_CD_6", StringType(), False),
    StructField("PROC_CD_7", StringType(), False),
    StructField("PROC_CD_8", StringType(), False),
    StructField("PROC_CD_9", StringType(), False),
    StructField("PROC_CD_10", StringType(), False),
    StructField("PROC_CD_11", StringType(), False),
    StructField("PROC_CD_12", StringType(), False),
    StructField("PROC_CD_13", StringType(), False),
    StructField("PROC_CD_14", StringType(), False),
    StructField("PROC_CD_15", StringType(), False),
    StructField("PROC_CD_16", StringType(), False),
    StructField("PROC_CD_17", StringType(), False),
    StructField("PROC_CD_18", StringType(), False),
    StructField("PROC_CD_19", StringType(), False),
    StructField("PROC_CD_20", StringType(), False),
    StructField("PROC_CD_21", StringType(), False),
    StructField("PROC_CD_22", StringType(), False),
    StructField("PROC_CD_23", StringType(), False),
    StructField("PROC_CD_24", StringType(), False),
    StructField("PROC_CD_25", StringType(), False),
    StructField("PX_TYPE_1", StringType(), False),
    StructField("PX_TYPE_2", StringType(), False),
    StructField("PX_TYPE_3", StringType(), False),
    StructField("PX_TYPE_4", StringType(), False),
    StructField("PX_TYPE_5", StringType(), False),
    StructField("PX_TYPE_6", StringType(), False),
    StructField("PX_TYPE_7", StringType(), False),
    StructField("PX_TYPE_8", StringType(), False),
    StructField("PX_TYPE_9", StringType(), False),
    StructField("PX_TYPE_10", StringType(), False),
    StructField("PX_TYPE_11", StringType(), False),
    StructField("PX_TYPE_12", StringType(), False),
    StructField("PX_TYPE_13", StringType(), False),
    StructField("PX_TYPE_14", StringType(), False),
    StructField("PX_TYPE_15", StringType(), False),
    StructField("PX_TYPE_16", StringType(), False),
    StructField("PX_TYPE_17", StringType(), False),
    StructField("PX_TYPE_18", StringType(), False),
    StructField("PX_TYPE_19", StringType(), False),
    StructField("PX_TYPE_20", StringType(), False),
    StructField("PX_TYPE_21", StringType(), False),
    StructField("PX_TYPE_22", StringType(), False),
    StructField("PX_TYPE_23", StringType(), False),
    StructField("PX_TYPE_24", StringType(), False),
    StructField("PX_TYPE_25", StringType(), False),
    StructField("AGE", StringType(), False),
    StructField("SEX", StringType(), False),
    StructField("DISP", StringType(), False),
    StructField("DRG", StringType(), False),
    StructField("MDC", StringType(), False),
    StructField("VERS", StringType(), False),
    StructField("FILL1", StringType(), False),
    StructField("OP", StringType(), False),
    StructField("RTN_CODE", StringType(), False),
    StructField("NO_DX", StringType(), False),
    StructField("NO_OP", StringType(), False),
    StructField("ADAYS_A", StringType(), False),
    StructField("ADAYS_D", StringType(), False),
    StructField("BWGT", StringType(), False),
    StructField("GTYPE", StringType(), False),
    StructField("CLMD_POA_IND_1", StringType(), False),
    StructField("CLMD_POA_IND_2", StringType(), False),
    StructField("CLMD_POA_IND_3", StringType(), False),
    StructField("CLMD_POA_IND_4", StringType(), False),
    StructField("CLMD_POA_IND_5", StringType(), False),
    StructField("CLMD_POA_IND_6", StringType(), False),
    StructField("CLMD_POA_IND_7", StringType(), False),
    StructField("CLMD_POA_IND_8", StringType(), False),
    StructField("CLMD_POA_IND_9", StringType(), False),
    StructField("CLMD_POA_IND_10", StringType(), False),
    StructField("CLMD_POA_IND_11", StringType(), False),
    StructField("CLMD_POA_IND_12", StringType(), False),
    StructField("CLMD_POA_IND_13", StringType(), False),
    StructField("CLMD_POA_IND_14", StringType(), False),
    StructField("CLMD_POA_IND_15", StringType(), False),
    StructField("CLMD_POA_IND_16", StringType(), False),
    StructField("CLMD_POA_IND_17", StringType(), False),
    StructField("CLMD_POA_IND_18", StringType(), False),
    StructField("CLMD_POA_IND_19", StringType(), False),
    StructField("CLMD_POA_IND_20", StringType(), False),
    StructField("CLMD_POA_IND_21", StringType(), False),
    StructField("CLMD_POA_IND_22", StringType(), False),
    StructField("CLMD_POA_IND_23", StringType(), False),
    StructField("CLMD_POA_IND_24", StringType(), False),
    StructField("CLMD_POA_IND_25", StringType(), False),
    StructField("CODE_CLASS_IND", StringType(), False),
    StructField("PTNT_TYP", StringType(), False),
    StructField("CLS_TYP", StringType(), False),
])

df_generatedpoa = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")
    .option("quote", "\"")
    .schema(schema_generatedpoa)
    .load(f"{adls_path}/verified/BCAFEPFcltyClmDRGGen_{RunID}_results.dat")
)

# Deduplicate on "HE_CLM_ID" (Scenario A Hashed File Replacement)
df_grouper_out = df_generatedpoa.dropDuplicates(["HE_CLM_ID"])

# ---------------------------------------------------------------------
# Read "NormativePOA" (CSeqFileStage)
# ---------------------------------------------------------------------
schema_normativepoa = StructType([
    StructField("HE_CLM_ID", StringType(), False),
    StructField("DIAG_CD_1", StringType(), False),
    StructField("DIAG_CD_2", StringType(), False),
    StructField("DIAG_CD_3", StringType(), False),
    StructField("DIAG_CD_4", StringType(), False),
    StructField("DIAG_CD_5", StringType(), False),
    StructField("DIAG_CD_6", StringType(), False),
    StructField("DIAG_CD_7", StringType(), False),
    StructField("DIAG_CD_8", StringType(), False),
    StructField("DIAG_CD_9", StringType(), False),
    StructField("DIAG_CD_10", StringType(), False),
    StructField("DIAG_CD_11", StringType(), False),
    StructField("DIAG_CD_12", StringType(), False),
    StructField("DIAG_CD_13", StringType(), False),
    StructField("DIAG_CD_14", StringType(), False),
    StructField("DIAG_CD_15", StringType(), False),
    StructField("DIAG_CD_16", StringType(), False),
    StructField("DIAG_CD_17", StringType(), False),
    StructField("DIAG_CD_18", StringType(), False),
    StructField("DIAG_CD_19", StringType(), False),
    StructField("DIAG_CD_20", StringType(), False),
    StructField("DIAG_CD_21", StringType(), False),
    StructField("DIAG_CD_22", StringType(), False),
    StructField("DIAG_CD_23", StringType(), False),
    StructField("DIAG_CD_24", StringType(), False),
    StructField("DIAG_CD_25", StringType(), False),
    StructField("DX_TYPE_1", StringType(), False),
    StructField("DX_TYPE_2", StringType(), False),
    StructField("DX_TYPE_3", StringType(), False),
    StructField("DX_TYPE_4", StringType(), False),
    StructField("DX_TYPE_5", StringType(), False),
    StructField("DX_TYPE_6", StringType(), False),
    StructField("DX_TYPE_7", StringType(), False),
    StructField("DX_TYPE_8", StringType(), False),
    StructField("DX_TYPE_9", StringType(), False),
    StructField("DX_TYPE_10", StringType(), False),
    StructField("DX_TYPE_11", StringType(), False),
    StructField("DX_TYPE_12", StringType(), False),
    StructField("DX_TYPE_13", StringType(), False),
    StructField("DX_TYPE_14", StringType(), False),
    StructField("DX_TYPE_15", StringType(), False),
    StructField("DX_TYPE_16", StringType(), False),
    StructField("DX_TYPE_17", StringType(), False),
    StructField("DX_TYPE_18", StringType(), False),
    StructField("DX_TYPE_19", StringType(), False),
    StructField("DX_TYPE_20", StringType(), False),
    StructField("DX_TYPE_21", StringType(), False),
    StructField("DX_TYPE_22", StringType(), False),
    StructField("DX_TYPE_23", StringType(), False),
    StructField("DX_TYPE_24", StringType(), False),
    StructField("DX_TYPE_25", StringType(), False),
    StructField("PROC_CD_1", StringType(), False),
    StructField("PROC_CD_2", StringType(), False),
    StructField("PROC_CD_3", StringType(), False),
    StructField("PROC_CD_4", StringType(), False),
    StructField("PROC_CD_5", StringType(), False),
    StructField("PROC_CD_6", StringType(), False),
    StructField("PROC_CD_7", StringType(), False),
    StructField("PROC_CD_8", StringType(), False),
    StructField("PROC_CD_9", StringType(), False),
    StructField("PROC_CD_10", StringType(), False),
    StructField("PROC_CD_11", StringType(), False),
    StructField("PROC_CD_12", StringType(), False),
    StructField("PROC_CD_13", StringType(), False),
    StructField("PROC_CD_14", StringType(), False),
    StructField("PROC_CD_15", StringType(), False),
    StructField("PROC_CD_16", StringType(), False),
    StructField("PROC_CD_17", StringType(), False),
    StructField("PROC_CD_18", StringType(), False),
    StructField("PROC_CD_19", StringType(), False),
    StructField("PROC_CD_20", StringType(), False),
    StructField("PROC_CD_21", StringType(), False),
    StructField("PROC_CD_22", StringType(), False),
    StructField("PROC_CD_23", StringType(), False),
    StructField("PROC_CD_24", StringType(), False),
    StructField("PROC_CD_25", StringType(), False),
    StructField("PX_TYPE_1", StringType(), False),
    StructField("PX_TYPE_2", StringType(), False),
    StructField("PX_TYPE_3", StringType(), False),
    StructField("PX_TYPE_4", StringType(), False),
    StructField("PX_TYPE_5", StringType(), False),
    StructField("PX_TYPE_6", StringType(), False),
    StructField("PX_TYPE_7", StringType(), False),
    StructField("PX_TYPE_8", StringType(), False),
    StructField("PX_TYPE_9", StringType(), False),
    StructField("PX_TYPE_10", StringType(), False),
    StructField("PX_TYPE_11", StringType(), False),
    StructField("PX_TYPE_12", StringType(), False),
    StructField("PX_TYPE_13", StringType(), False),
    StructField("PX_TYPE_14", StringType(), False),
    StructField("PX_TYPE_15", StringType(), False),
    StructField("PX_TYPE_16", StringType(), False),
    StructField("PX_TYPE_17", StringType(), False),
    StructField("PX_TYPE_18", StringType(), False),
    StructField("PX_TYPE_19", StringType(), False),
    StructField("PX_TYPE_20", StringType(), False),
    StructField("PX_TYPE_21", StringType(), False),
    StructField("PX_TYPE_22", StringType(), False),
    StructField("PX_TYPE_23", StringType(), False),
    StructField("PX_TYPE_24", StringType(), False),
    StructField("PX_TYPE_25", StringType(), False),
    StructField("AGE", StringType(), False),
    StructField("SEX", StringType(), False),
    StructField("DISP", StringType(), False),
    StructField("DRG", StringType(), False),
    StructField("MDC", StringType(), False),
    StructField("VERS", StringType(), False),
    StructField("FILL1", StringType(), False),
    StructField("OP", StringType(), False),
    StructField("RTN_CODE", StringType(), False),
    StructField("NO_DX", StringType(), False),
    StructField("NO_OP", StringType(), False),
    StructField("ADAYS_A", StringType(), False),
    StructField("ADAYS_D", StringType(), False),
    StructField("BWGT", StringType(), False),
    StructField("GTYPE", StringType(), False),
    StructField("CLMD_POA_IND_1", StringType(), False),
    StructField("CLMD_POA_IND_2", StringType(), False),
    StructField("CLMD_POA_IND_3", StringType(), False),
    StructField("CLMD_POA_IND_4", StringType(), False),
    StructField("CLMD_POA_IND_5", StringType(), False),
    StructField("CLMD_POA_IND_6", StringType(), False),
    StructField("CLMD_POA_IND_7", StringType(), False),
    StructField("CLMD_POA_IND_8", StringType(), False),
    StructField("CLMD_POA_IND_9", StringType(), False),
    StructField("CLMD_POA_IND_10", StringType(), False),
    StructField("CLMD_POA_IND_11", StringType(), False),
    StructField("CLMD_POA_IND_12", StringType(), False),
    StructField("CLMD_POA_IND_13", StringType(), False),
    StructField("CLMD_POA_IND_14", StringType(), False),
    StructField("CLMD_POA_IND_15", StringType(), False),
    StructField("CLMD_POA_IND_16", StringType(), False),
    StructField("CLMD_POA_IND_17", StringType(), False),
    StructField("CLMD_POA_IND_18", StringType(), False),
    StructField("CLMD_POA_IND_19", StringType(), False),
    StructField("CLMD_POA_IND_20", StringType(), False),
    StructField("CLMD_POA_IND_21", StringType(), False),
    StructField("CLMD_POA_IND_22", StringType(), False),
    StructField("CLMD_POA_IND_23", StringType(), False),
    StructField("CLMD_POA_IND_24", StringType(), False),
    StructField("CLMD_POA_IND_25", StringType(), False),
    StructField("CODE_CLASS_IND", StringType(), False),
    StructField("PTNT_TYP", StringType(), False),
    StructField("CLS_TYP", StringType(), False),
])

df_normativepoa = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", "|")
    .option("quote", "\"")
    .schema(schema_normativepoa)
    .load(f"{adls_path}/verified/BCAFEPFcltyClmDRGNorm_{RunID}_results.dat")
)

# Deduplicate on "HE_CLM_ID" (Scenario A Hashed File Replacement)
df_normative_drg = df_normativepoa.dropDuplicates(["HE_CLM_ID"])

# ---------------------------------------------------------------------
# tMergeDrg (CTransformerStage): join df_bcafep, df_grouper_out, df_normative_drg
# ---------------------------------------------------------------------
df_join = df_bcafep.alias("BCAFEP") \
    .join(df_grouper_out.alias("grouper_out"),
          F.col("BCAFEP.RowNum") == F.col("grouper_out.HE_CLM_ID"),
          "left") \
    .join(df_normative_drg.alias("normative_drg"),
          F.col("BCAFEP.RowNum") == F.col("normative_drg.HE_CLM_ID"),
          "left")

df_enriched = df_join

# Define stage variables as columns
# svDrgMthdCd
df_enriched = df_enriched.withColumn(
    "svDrgMthdCd",
    F.when(F.col("BCAFEP.FCLTY_CLM_STMNT_END_DT") < F.lit("2007-10-01"), F.lit("CMS"))
     .when((F.col("BCAFEP.FCLTY_CLM_STMNT_END_DT") > F.lit("2007-10-01")) | 
           (F.col("BCAFEP.FCLTY_CLM_STMNT_END_DT") == F.lit("2007-10-01")), F.lit("MS"))
     .otherwise(F.lit(None))
)

# svGnrtDrg = Right("0" : trim(grouper_out.DRG), 4)
df_enriched = df_enriched.withColumn(
    "svGnrtDrg_tmp",
    F.concat(F.lit("0"), trim("grouper_out.DRG"))
).withColumn(
    "svGnrtDrg",
    F.substring(F.col("svGnrtDrg_tmp"), F.length(F.col("svGnrtDrg_tmp")) - 3, 4)
).drop("svGnrtDrg_tmp")

# svNrmtvDrg = Right("0" : trim(normative_drg.DRG), 4)
df_enriched = df_enriched.withColumn(
    "svNrmtvDrg_tmp",
    F.concat(F.lit("0"), trim("normative_drg.DRG"))
).withColumn(
    "svNrmtvDrg",
    F.substring(F.col("svNrmtvDrg_tmp"), F.length(F.col("svNrmtvDrg_tmp")) - 3, 4)
).drop("svNrmtvDrg_tmp")

# svFcltyClmSubTyp
df_enriched = df_enriched.withColumn(
    "svFcltyClmSubTyp",
    F.when(F.col("BCAFEP.IP_CLM_TYP_IN") == F.lit("I"), F.lit("11"))
     .otherwise(
       F.when(
         (F.col("BCAFEP.PATN_STTUS_CD").isNull()) & (F.col("BCAFEP.CLM_CLS_IN") == F.lit("F")),
         F.lit("12")
       ).otherwise(F.lit("UNK"))
     )
)

# svLosDays
# If both IP_CLM_BEG_DT and IP_CLM_DSCHG_DT are null => 0
# If both are '1753-01-01' => 0
# else difference in days
df_enriched = df_enriched.withColumn(
    "svLosDays",
    F.when(
        (F.col("BCAFEP.IP_CLM_BEG_DT").isNull()) & (F.col("BCAFEP.IP_CLM_DSCHG_DT").isNull()),
        F.lit(0)
    ).when(
        (F.col("BCAFEP.IP_CLM_BEG_DT") == F.lit("1753-01-01")) &
        (F.col("BCAFEP.IP_CLM_DSCHG_DT") == F.lit("1753-01-01")),
        F.lit(0)
    ).otherwise(
        F.datediff(
            F.to_date("BCAFEP.IP_CLM_DSCHG_DT","yyyy-MM-dd"),
            F.to_date("BCAFEP.IP_CLM_BEG_DT","yyyy-MM-dd")
        )
    )
)

# Filter constraint: BCAFEP.CLM_CLS_IN = 'F'
df_enriched_filtered = df_enriched.filter(F.col("BCAFEP.CLM_CLS_IN") == F.lit("F"))

# Create the final columns for the "Out" link of tMergeDrg -> "Snapshot"
df_out = df_enriched_filtered.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd),F.lit(";"),F.col("BCAFEP.CLM_ID")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    F.lit(SrcSycCdSk).alias("SRC_SYS_CD_SK"),
    F.col("BCAFEP.CLM_ID").alias("CLM_ID"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit("NA").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.lit("NA").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.when(
        (F.col("BCAFEP.BILL_TYP__CD").isNull()) | (F.length(trim("BCAFEP.BILL_TYP__CD")) == 0),
        F.lit("NA")
    ).otherwise(
        F.when(F.length(trim("BCAFEP.BILL_TYP__CD"))==4,
           F.when(
               (F.col("BCAFEP.BILL_TYP__CD").substr(F.lit(2),F.lit(1)).isin("1","2","3","4","5","6","9")),
               F.concat(F.lit("0"),F.col("BCAFEP.BILL_TYP__CD").substr(F.lit(3),F.lit(1)))
           ).otherwise(
               F.when(F.col("BCAFEP.BILL_TYP__CD").substr(F.lit(2),F.lit(1))==F.lit("7"),
                      F.concat(F.lit("7"),F.col("BCAFEP.BILL_TYP__CD").substr(F.lit(3),F.lit(1)))
               ).otherwise(
                   F.when(F.col("BCAFEP.BILL_TYP__CD").substr(F.lit(2),F.lit(1))==F.lit("8"),
                          F.concat(F.lit("8"),F.col("BCAFEP.BILL_TYP__CD").substr(F.lit(3),F.lit(1)))
                   ).otherwise(F.lit("UNK"))
               )
           )
        ).otherwise(F.lit("UNK"))
    ).alias("FCLTY_CLM_BILL_CLS_CD"),
    F.when(
        (F.col("BCAFEP.BILL_TYP__CD").isNull()) | (F.length(trim("BCAFEP.BILL_TYP__CD")) == 0),
        F.lit("NA")
    ).otherwise(
        F.when(F.length(trim("BCAFEP.BILL_TYP__CD"))==4,
               F.col("BCAFEP.BILL_TYP__CD").substr(F.lit(4),F.lit(1))
        ).otherwise(F.lit("UNK"))
    ).alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.when(
        (F.col("BCAFEP.PATN_STTUS_CD").isNull()) | (F.length(trim("BCAFEP.PATN_STTUS_CD"))==0),
        F.lit("NA")
    ).otherwise(
        F.col("BCAFEP.PATN_STTUS_CD")
    ).alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.col("svFcltyClmSubTyp").alias("FCLTY_CLM_SUBTYP_CD"),
    F.when(
       (F.col("BCAFEP.PROC_CD_NON_ICD").isNull()) | (F.length(trim("BCAFEP.PROC_CD_NON_ICD"))==0),
       F.when((F.col("BCAFEP.PROC_CDNG_TYP").isNull()) | (F.length(trim("BCAFEP.PROC_CDNG_TYP"))==0),
              F.lit("NA")
       ).otherwise(
         F.when(F.col("BCAFEP.PROC_CDNG_TYP")==F.lit("9"),F.lit("ICD9"))
         .when(F.col("BCAFEP.PROC_CDNG_TYP")==F.lit("0"),F.lit("ICD10"))
         .otherwise(F.lit("NA"))
       )
    ).otherwise(
       F.when(
         F.col("BCAFEP.PROC_CD_NON_ICD").substr(F.lit(1),F.lit(1)).rlike("^[1]"),
         F.lit("CPT4")
       ).otherwise(F.lit("HCPCS"))
    ).alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.col("BCAFEP.BILL_TYP__CD").substr(F.lit(2),F.lit(1)).alias("FCLTY_TYP_CD"),
    F.when(F.col("svFcltyClmSubTyp")==F.lit("12"),F.lit("N"))
     .otherwise(
       F.when(F.col("svFcltyClmSubTyp")==F.lit("11"),F.lit("Y"))
       .otherwise(F.lit("N"))
     ).alias("IDS_GNRT_DRG_IN"),
    F.when(F.col("BCAFEP.IP_CLM_BEG_DT").isNull(),F.lit("1753-01-01"))
     .otherwise(F.col("BCAFEP.IP_CLM_BEG_DT")).alias("ADMS_DT"),
    F.when(F.col("BCAFEP.FCLTY_CLM_STMNT_BEG_DT").isNull(),F.lit("1753-01-01"))
     .otherwise(F.col("BCAFEP.FCLTY_CLM_STMNT_BEG_DT")).alias("BILL_STMNT_BEG_DT"),
    F.when(F.col("BCAFEP.FCLTY_CLM_STMNT_END_DT").isNull(),F.lit("2199-12-31"))
     .otherwise(F.col("BCAFEP.FCLTY_CLM_STMNT_END_DT")).alias("BILL_STMNT_END_DT"),
    F.when(F.col("BCAFEP.IP_CLM_DSCHG_DT").isNull(),F.lit("2199-12-31"))
     .otherwise(F.col("BCAFEP.IP_CLM_DSCHG_DT")).alias("DSCHG_DT"),
    F.when(F.col("BCAFEP.IP_CLM_BEG_DT").isNull(),F.lit("1753-01-01"))
     .otherwise(F.col("BCAFEP.IP_CLM_BEG_DT")).alias("HOSP_BEG_DT"),
    F.when(F.col("BCAFEP.IP_CLM_DSCHG_DT").isNull(),F.lit("2199-12-31"))
     .otherwise(F.col("BCAFEP.IP_CLM_DSCHG_DT")).alias("HOSP_END_DT"),
    F.lit(0).alias("ADMS_HR"),
    F.lit(0).alias("DSCHG_HR"),
    F.when(
        (F.col("BCAFEP.PD_DAYS").isNull()) | (F.length(trim("BCAFEP.PD_DAYS"))==0),
        F.lit(0)
    ).otherwise(F.col("BCAFEP.PD_DAYS")).alias("HOSP_COV_DAYS"),
    F.col("svLosDays").alias("LOS_DAYS"),
    F.lit("NA").alias("ADM_PHYS_PROV_ID"),
    F.when(F.col("BCAFEP.BILL_TYP__CD").isNull(),F.lit("UNK"))
     .otherwise(
       F.expr("left(trim(BCAFEP.BILL_TYP__CD, '0', 'L'), 3)")
     ).alias("FCLTY_BILL_TYP_TX"),
    F.when(F.col("svFcltyClmSubTyp")==F.lit("12"),F.lit("NA"))
     .otherwise(F.col("svGnrtDrg")).alias("GNRT_DRG_CD"),
    F.lit("UNK").alias("MED_RCRD_NO"),
    F.lit("UNK").alias("OTHER_PROV_ID_1"),
    F.lit("UNK").alias("OTHER_PROV_ID_2"),
    F.col("BCAFEP.DRG_CD").alias("SUBMT_DRG_CD"),
    F.when(F.col("svFcltyClmSubTyp")==F.lit("12"),F.lit("NA"))
     .otherwise(F.col("svNrmtvDrg")).alias("NRMTV_DRG_CD"),
    F.col("svDrgMthdCd").alias("DRG_METHOD_CD")
)

# ---------------------------------------------------------------------
# Snapshot (CTransformerStage) has two output links: "Transform"->FcltyClmPK, "SnapShot"->Transformer
# ---------------------------------------------------------------------
df_snapshot = df_out

# Output pin "Transform" -> pass through
df_snapshot_transform = df_snapshot.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("FCLTY_CLM_ADMS_SRC_CD").alias("FCLTY_CLM_ADMS_SRC_CD"),
    F.col("FCLTY_CLM_ADMS_TYP_CD").alias("FCLTY_CLM_ADMS_TYP_CD"),
    F.col("FCLTY_CLM_BILL_CLS_CD").alias("FCLTY_CLM_BILL_CLS_CD"),
    F.col("FCLTY_CLM_BILL_FREQ_CD").alias("FCLTY_CLM_BILL_FREQ_CD"),
    F.col("FCLTY_CLM_DSCHG_STTUS_CD").alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    F.col("FCLTY_CLM_SUBTYP_CD").alias("FCLTY_CLM_SUBTYP_CD"),
    F.col("FCLTY_CLM_PROC_BILL_METH_CD").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    F.col("FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
    F.col("IDS_GNRT_DRG_IN").alias("IDS_GNRT_DRG_IN"),
    F.col("ADMS_DT").alias("ADMS_DT"),
    F.col("BILL_STMNT_BEG_DT").alias("BILL_STMNT_BEG_DT"),
    F.col("BILL_STMNT_END_DT").alias("BILL_STMNT_END_DT"),
    F.col("DSCHG_DT").alias("DSCHG_DT"),
    F.col("HOSP_BEG_DT").alias("HOSP_BEG_DT"),
    F.col("HOSP_END_DT").alias("HOSP_END_DT"),
    F.col("ADMS_HR").alias("ADMS_HR"),
    F.col("DSCHG_HR").alias("DSCHG_HR"),
    F.col("HOSP_COV_DAYS").alias("HOSP_COV_DAYS"),
    F.col("LOS_DAYS").alias("LOS_DAYS"),
    F.col("ADM_PHYS_PROV_ID").alias("ADM_PHYS_PROV_ID"),
    F.col("FCLTY_BILL_TYP_TX").alias("FCLTY_BILL_TYP_TX"),
    F.col("GNRT_DRG_CD").alias("GNRT_DRG_CD"),
    F.col("MED_RCRD_NO").alias("MED_RCRD_NO"),
    F.col("OTHER_PROV_ID_1").alias("OTHER_PROV_ID_1"),
    F.col("OTHER_PROV_ID_2").alias("OTHER_PROV_ID_2"),
    F.col("SUBMT_DRG_CD").alias("SUBMT_DRG_CD"),
    F.col("NRMTV_DRG_CD").alias("NRMTV_DRG_CD"),
    F.col("DRG_METHOD_CD").alias("DRG_METHOD_CD")
)

# Output pin "SnapShot" -> columns: CLM_ID
df_snapshot_snap = df_snapshot.select(
    F.col("CLM_ID").alias("CLM_ID")
)

# ---------------------------------------------------------------------
# FcltyClmPK (CContainerStage) - call shared container
# ---------------------------------------------------------------------
params_fclty = {
    "CurrRunCycle": CurrRunCycle
}
df_key = FcltyClmPK(df_snapshot_transform, params_fclty)

# ---------------------------------------------------------------------
# BcafepFcltyClmExtr (CSeqFileStage writing)
# ---------------------------------------------------------------------
# Must rpad char/varchar columns to their lengths from stage definition.
# Per the stage JSON, these columns with lengths:
# INSRT_UPDT_CD (10), DISCARD_IN (1), PASS_THRU_IN (1), CLM_ID (18), etc.
# We'll apply rpad to each char/varchar column if length is known.
df_key_rpad = df_key \
.withColumn("INSRT_UPDT_CD", F.rpad("INSRT_UPDT_CD", 10, " ")) \
.withColumn("DISCARD_IN", F.rpad("DISCARD_IN", 1, " ")) \
.withColumn("PASS_THRU_IN", F.rpad("PASS_THRU_IN", 1, " ")) \
.withColumn("CLM_ID", F.rpad("CLM_ID", 18, " ")) \
.withColumn("FCLTY_CLM_ADMS_SRC_CD", F.rpad("FCLTY_CLM_ADMS_SRC_CD", 2, " ")) \
.withColumn("FCLTY_CLM_ADMS_TYP_CD", F.rpad("FCLTY_CLM_ADMS_TYP_CD", 10, " ")) \
.withColumn("FCLTY_CLM_BILL_CLS_CD", F.rpad("FCLTY_CLM_BILL_CLS_CD", 10, " ")) \
.withColumn("FCLTY_CLM_BILL_FREQ_CD", F.rpad("FCLTY_CLM_BILL_FREQ_CD", 10, " ")) \
.withColumn("FCLTY_CLM_DSCHG_STTUS_CD", F.rpad("FCLTY_CLM_DSCHG_STTUS_CD", 10, " ")) \
.withColumn("FCLTY_CLM_SUBTYP_CD", F.rpad("FCLTY_CLM_SUBTYP_CD", 10, " ")) \
.withColumn("FCLTY_CLM_PROC_BILL_METH_CD", F.rpad("FCLTY_CLM_PROC_BILL_METH_CD", 10, " ")) \
.withColumn("FCLTY_TYP_CD", F.rpad("FCLTY_TYP_CD", 10, " ")) \
.withColumn("IDS_GNRT_DRG_IN", F.rpad("IDS_GNRT_DRG_IN", 1, " ")) \
.withColumn("ADMS_DT", F.rpad("ADMS_DT", 10, " ")) \
.withColumn("BILL_STMNT_BEG_DT", F.rpad("BILL_STMNT_BEG_DT", 10, " ")) \
.withColumn("BILL_STMNT_END_DT", F.rpad("BILL_STMNT_END_DT", 10, " ")) \
.withColumn("DSCHG_DT", F.rpad("DSCHG_DT", 10, " ")) \
.withColumn("HOSP_BEG_DT", F.rpad("HOSP_BEG_DT", 10, " ")) \
.withColumn("HOSP_END_DT", F.rpad("HOSP_END_DT", 10, " ")) \
.withColumn("FCLTY_BILL_TYP_TX", F.rpad("FCLTY_BILL_TYP_TX", 3, " ")) \
.withColumn("GNRT_DRG_CD", F.rpad("GNRT_DRG_CD", 4, " ")) \
.withColumn("SUBMT_DRG_CD", F.rpad("SUBMT_DRG_CD", 4, " ")) \
.withColumn("NRMTV_DRG_CD", F.rpad("NRMTV_DRG_CD", 4, " "))

df_bcafep_fclty_clm_extr = df_key_rpad.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "FCLTY_CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "FCLTY_CLM_ADMS_SRC_CD",
    "FCLTY_CLM_ADMS_TYP_CD",
    "FCLTY_CLM_BILL_CLS_CD",
    "FCLTY_CLM_BILL_FREQ_CD",
    "FCLTY_CLM_DSCHG_STTUS_CD",
    "FCLTY_CLM_SUBTYP_CD",
    "FCLTY_CLM_PROC_BILL_METH_CD",
    "FCLTY_TYP_CD",
    "IDS_GNRT_DRG_IN",
    "ADMS_DT",
    "BILL_STMNT_BEG_DT",
    "BILL_STMNT_END_DT",
    "DSCHG_DT",
    "HOSP_BEG_DT",
    "HOSP_END_DT",
    "ADMS_HR",
    "DSCHG_HR",
    "HOSP_COV_DAYS",
    "LOS_DAYS",
    "ADM_PHYS_PROV_ID",
    "FCLTY_BILL_TYP_TX",
    "GNRT_DRG_CD",
    "MED_RCRD_NO",
    "OTHER_PROV_ID_1",
    "OTHER_PROV_ID_2",
    "SUBMT_DRG_CD",
    "NRMTV_DRG_CD",
    "DRG_METHOD_CD"
)

write_files(
    df_bcafep_fclty_clm_extr,
    f"{adls_path}/key/BCAFEPClmFcltyExtr.BCAFEPClmFclty.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ---------------------------------------------------------------------
# Transformer after Snapshot -> B_FCLTY_CLM
# ---------------------------------------------------------------------
df_transformer = df_snapshot_snap.select(
    F.lit(SrcSycCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

# Must rpad if CLM_ID is char(18)
df_transformer_rpad = df_transformer.withColumn(
    "CLM_ID", F.rpad("CLM_ID", 18, " ")
)

df_b_fclty_clm = df_transformer_rpad.select("SRC_SYS_CD_SK","CLM_ID")

write_files(
    df_b_fclty_clm,
    f"{adls_path}/load/B_FCLTY_CLM.BCAFEP.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)