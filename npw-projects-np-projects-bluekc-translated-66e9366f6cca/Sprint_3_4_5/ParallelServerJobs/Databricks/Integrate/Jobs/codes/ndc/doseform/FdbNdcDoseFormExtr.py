# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 01/28/08 08:28:47 Batch  14638_30532 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/26/07 11:16:35 Batch  14605_40601 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/21/07 14:41:36 Batch  14570_52900 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 10/10/07 13:50:33 Batch  14528_49851 PROMOTE bckcetl ids20 dsadm bls for bl
# MAGIC ^1_2 10/10/07 13:38:24 Batch  14528_49120 INIT bckcett testIDS30 dsadm bls for bl
# MAGIC ^1_1 10/08/07 17:37:28 Batch  14526_63461 PROMOTE bckcett testIDS30 u08717 Brent
# MAGIC ^1_1 10/08/07 17:30:51 Batch  14526_63055 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 05/23/06 11:51:34 Batch  14023_42698 INIT bckcett devlIDS30 u05779 bj
# MAGIC ^1_2 12/13/05 13:55:16 Batch  13862_50122 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_1 12/05/05 10:59:01 Batch  13854_39546 INIT bckcett devlIDS30 dsadm Gina Parr
# MAGIC ^1_1 11/30/05 16:50:19 Batch  13849_60624 INIT bckcett devlIDS30 u05779 bj
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC            INPUTS:
# MAGIC            The file named RHIC3D3_HIC_THERAP_CLASS_DESC is received from First Data Bank and staged to #FilePath#/landing/ahfs.dat and staged to #FilePath#/landing/dose.dat
# MAGIC   
# MAGIC HASH FILES:  none
# MAGIC 
# MAGIC CONTAINERS:  None
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   no description fields so no STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                    Output file is created with a temp. name.                 
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                    Sequential file name is created in the job control ( TmpOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                  BJ Luce                   10/31/2005 - Original programming
# MAGIC                  BJ Luce                    4/2006  - use environment parameters, hard code output to #$FilePath#/key/FdbNdcDoseFormExtr.dat
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parikshith Chada              4/16/2007          3264                              Added Balancing process to the overall                 devlIDS30               Steph Goddard             09/27/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data                 
# MAGIC 
# MAGIC Hari Krishna Raoi Yadav  05/18/2021     US- 382322                  updated the derivaion logic in Stage BusinessRules  IntegrateSITF           Jeyaprasanna               05/23/2021
# MAGIC                                                                                                       for Column DOSE_FORM_CD_DESC

# MAGIC Transforming Doseage Form  codes in RHIC3D3_HIC_THERAP_CLASS_DESC to CRF
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Balancing snapshot of source table
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')

df_hf_dose_form_lkup = spark.read.parquet(f"{adls_path}/hf_dose_form.parquet")
df_hf_dose_form_lkup = df_hf_dose_form_lkup.select(
    F.col("DOSE_FORM_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("DOSE_FORM_SK")
)

schema_pull_dose = StructType([
    StructField("GCDF", StringType(), False),
    StructField("DOSE", StringType(), False),
    StructField("GCDF_DESC", StringType(), False)
])
df_pull_dose = (
    spark.read.schema(schema_pull_dose)
    .option("header", "false")
    .option("quote", "\"")
    .csv(f"{adls_path_raw}/landing/dose.dat")
)

df_business_rules = df_pull_dose.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FDB").alias("SRC_SYS_CD"),
    (F.lit("FDB") + F.lit(";") + F.col("GCDF")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("DOSE_FORM_SK"),
    trim(F.col("GCDF")).alias("DOSE_FORM_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    trim(F.col("DOSE")).alias("DOSE_FORM_DESC"),
    F.regexp_replace(trim(F.col("GCDF_DESC")), "[\r\n\t]", "").alias("DOSE_FORM_CD_DESC")
)

df_primarykey_join = df_business_rules.alias("Transform").join(
    df_hf_dose_form_lkup.alias("lkup"),
    F.col("Transform.DOSE_FORM_CD") == F.col("lkup.DOSE_FORM_CD"),
    "left"
)

df_primarykey_raw = df_primarykey_join.select(
    F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
    F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("Transform.ERR_CT").alias("ERR_CT"),
    F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.when(F.col("lkup.DOSE_FORM_SK").isNull(), F.lit(None)).otherwise(F.col("lkup.DOSE_FORM_SK")).alias("SK"),
    F.col("Transform.DOSE_FORM_CD").alias("DOSE_FORM_CD"),
    F.when(F.col("lkup.DOSE_FORM_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")).alias("NewCrtRunCcyExctnSK"),
    F.col("Transform.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("Transform.DOSE_FORM_DESC").alias("DOSE_FORM_DESC"),
    F.col("Transform.DOSE_FORM_CD_DESC").alias("DOSE_FORM_CD_DESC"),
    F.when(F.col("lkup.DOSE_FORM_SK").isNull(), F.lit(1)).otherwise(F.lit(0)).alias("found_ind")
)

df_primarykey = df_primarykey_raw.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("SK").alias("DOSE_FORM_SK"),
    F.col("DOSE_FORM_CD"),
    F.col("NewCrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DOSE_FORM_DESC"),
    F.col("DOSE_FORM_CD_DESC"),
    F.col("found_ind")
)

df_enriched = df_primarykey
df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"DOSE_FORM_SK",<schema>,<secret_name>)

df_updt = df_enriched.filter(F.col("found_ind") == 1).select(
    F.col("DOSE_FORM_CD").alias("DOSE_FORM_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("DOSE_FORM_SK").alias("DOSE_FORM_SK")
)
write_files(
    df_updt,
    f"{adls_path}/hf_dose_form.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_fdbndcdoseformextr = df_enriched.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("DOSE_FORM_SK"),
    F.rpad(F.col("DOSE_FORM_CD"), 3, " ").alias("DOSE_FORM_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DOSE_FORM_DESC"),
    F.col("DOSE_FORM_CD_DESC")
)
write_files(
    df_fdbndcdoseformextr,
    f"{adls_path}/key/FdbNdcDoseFormExtr.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

schema_fdb_source = StructType([
    StructField("GCDF", StringType(), False)
])
df_fdb_source = (
    spark.read.schema(schema_fdb_source)
    .option("header", "false")
    .option("quote", "\"")
    .csv(f"{adls_path_raw}/landing/dose.dat")
)

df_transform = df_fdb_source.select(
    F.regexp_replace(trim(F.col("GCDF")), "[\r\n\t]", "").alias("DOSE_FORM_CD")
)
write_files(
    df_transform.select("DOSE_FORM_CD"),
    f"{adls_path}/load/B_DOSE_FORM.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)

params = {
    "EnvProjectPath": f"dap/<...>/Integrate",
    "File_Path": "key",
    "File_Name": "FdbNdcDoseFormExtr.dat"
}
dbutils.notebook.run("../../../../../sequencer_routines/Move_File", 3600, arguments=params)