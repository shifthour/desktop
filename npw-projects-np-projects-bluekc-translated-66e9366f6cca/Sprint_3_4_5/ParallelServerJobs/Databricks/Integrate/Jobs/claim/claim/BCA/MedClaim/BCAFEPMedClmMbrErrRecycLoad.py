# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Â© Copyright 2012 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY : BCAFEPMedClmLandSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION :  BCA FEP Med Claims Member Matching Error File Load
# MAGIC                                
# MAGIC 
# MAGIC MODIFICATIONS :
# MAGIC 
# MAGIC Developer                                  Date                 Project/Altiris #          Change Description                                                           Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------         ------------------------          -----------------------------------------------------------------------                   --------------------------------         -------------------------------   ----------------------------      
# MAGIC Sudhir Bomshetty                  2017-10-03              5781                          Original Programming                                                       IntegrateDev2                   Kalyan Neelam          2017-10-20


# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
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
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
SrcFileNm = get_widget_value('SrcFileNm','')
TgtFileNm = get_widget_value('TgtFileNm','')
SrcSysCd = get_widget_value('SrcSysCd','')

# Define schema for Seq_Err_File (CSeqFileStage)
schema_Seq_Err_File = StructType([
    StructField("SRC_SYS", StringType(), True),
    StructField("RCRD_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("CLM_ID", StringType(), True),
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
    StructField("RUN_DT", StringType(), True)
])

# Read the external file (Seq_Err_File)
df_seq_err_file = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_Seq_Err_File)
    .load(f"{adls_path_publish}/external/{SrcFileNm}")
)

# Replace intermediate hashed file (hf_bcafep_errclm_rm_dupe) with deduplication
df_hf_bcafep_errclm_rm_dupe = df_seq_err_file.dropDuplicates(["SRC_SYS", "RCRD_ID", "CLM_LN_NO"])

# Transformer stage (Trn_Clm_Sk_Gen)
df_trn_clm_sk_gen = (
    df_hf_bcafep_errclm_rm_dupe
    .withColumn("CLM_SK", GetFkeyClm(SrcSysCd, 1, trim(F.col("CLM_ID")), 'N'))
    .withColumn("CLM_ID", F.col("CLM_ID"))
    .withColumn("SRC_SYS_CD", F.lit("BCAMEDFEP"))
    .withColumn("CLM_TYP_CD", F.lit("MED"))
    .withColumn(
        "CLM_SUBTYP_CD",
        F.when(F.col("PATN_STTUS_CD").isNotNull() & (F.col("CLM_CLS_IN") == 'F'), 'IP')
         .when(F.col("PATN_STTUS_CD").isNull() & (F.col("CLM_CLS_IN") == 'F'), 'OP')
         .when(F.col("PATN_STTUS_CD").isNull() & (F.col("CLM_CLS_IN") == 'M'), 'PR')
         .otherwise('UNK')
    )
    .withColumn("CLM_SVC_STRT_DT_SK", FormatDate(F.col("CLM_SVC_DT_BEG"), 'DATE','MM/DD/CCYY','CCYY-MM-DD'))
    .withColumn("SRC_SYS_GRP_PFX", F.lit("NA"))
    .withColumn("SRC_SYS_GRP_ID", F.lit("NA"))
    .withColumn("SRC_SYS_GRP_SFX", F.lit("NA"))
    .withColumn("SUB_SSN", F.lit("NA"))
    .withColumn("PATN_LAST_NM", F.lit("NA"))
    .withColumn("PATN_FIRST_NM", F.lit("NA"))
    .withColumn("PATN_GNDR_CD", F.lit("NA"))
    .withColumn("PATN_BRTH_DT_SK", F.lit("NA"))
    .withColumn("ERR_CD", F.lit("MBRNOTFOUND"))
    .withColumn("ERR_DESC", F.lit("Member Not Found"))
    .withColumn("FEP_MBR_ID", F.col("MBR_ID"))
    .withColumn("SUB_FIRST_NM", F.lit("NA"))
    .withColumn("SUB_LAST_NM", F.lit("NA"))
    .withColumn("SRC_SYS_SUB_ID", F.lit("NA"))
    .withColumn("SRC_SYS_MBR_SFX_NO", F.lit("NA"))
    .withColumn("GRP_ID", F.lit("NA"))
    .withColumn("FILE_DT_SK", F.lit("NA"))
)

# Apply constraint: IsNull(Lnk_Clm_Sk.CLM_ID) = @FALSE  =>  Keep only rows where CLM_ID is not null
df_ln_err_load = df_trn_clm_sk_gen.filter(F.col("CLM_ID").isNotNull())

# Final select with column order and rpad for char/varchar
df_final = df_ln_err_load.select(
    F.rpad(F.col("CLM_SK"), <...>, " ").alias("CLM_SK"),
    F.rpad(F.col("CLM_ID"), <...>, " ").alias("CLM_ID"),
    F.rpad(F.col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_TYP_CD"), <...>, " ").alias("CLM_TYP_CD"),
    F.rpad(F.col("CLM_SUBTYP_CD"), <...>, " ").alias("CLM_SUBTYP_CD"),
    F.rpad(F.col("CLM_SVC_STRT_DT_SK"), 10, " ").alias("CLM_SVC_STRT_DT_SK"),
    F.rpad(F.col("SRC_SYS_GRP_PFX"), <...>, " ").alias("SRC_SYS_GRP_PFX"),
    F.rpad(F.col("SRC_SYS_GRP_ID"), <...>, " ").alias("SRC_SYS_GRP_ID"),
    F.rpad(F.col("SRC_SYS_GRP_SFX"), <...>, " ").alias("SRC_SYS_GRP_SFX"),
    F.rpad(F.col("SUB_SSN"), <...>, " ").alias("SUB_SSN"),
    F.rpad(F.col("PATN_LAST_NM"), <...>, " ").alias("PATN_LAST_NM"),
    F.rpad(F.col("PATN_FIRST_NM"), <...>, " ").alias("PATN_FIRST_NM"),
    F.rpad(F.col("PATN_GNDR_CD"), <...>, " ").alias("PATN_GNDR_CD"),
    F.rpad(F.col("PATN_BRTH_DT_SK"), 10, " ").alias("PATN_BRTH_DT_SK"),
    F.rpad(F.col("ERR_CD"), <...>, " ").alias("ERR_CD"),
    F.rpad(F.col("ERR_DESC"), <...>, " ").alias("ERR_DESC"),
    F.rpad(F.col("FEP_MBR_ID"), <...>, " ").alias("FEP_MBR_ID"),
    F.rpad(F.col("SUB_FIRST_NM"), <...>, " ").alias("SUB_FIRST_NM"),
    F.rpad(F.col("SUB_LAST_NM"), <...>, " ").alias("SUB_LAST_NM"),
    F.rpad(F.col("SRC_SYS_SUB_ID"), <...>, " ").alias("SRC_SYS_SUB_ID"),
    F.rpad(F.col("SRC_SYS_MBR_SFX_NO"), 3, " ").alias("SRC_SYS_MBR_SFX_NO"),
    F.rpad(F.col("GRP_ID"), <...>, " ").alias("GRP_ID"),
    F.rpad(F.col("FILE_DT_SK"), 10, " ").alias("FILE_DT_SK")
)

# Write to the target file (Seq_Err_Load)
write_files(
    df_final,
    f"{adls_path}/load/{TgtFileNm}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)