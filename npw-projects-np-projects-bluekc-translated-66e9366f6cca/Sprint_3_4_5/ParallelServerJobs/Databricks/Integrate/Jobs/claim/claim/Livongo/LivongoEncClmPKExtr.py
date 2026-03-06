# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC            
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date                             Project/Altiris #\(9)         Change Description\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------                                   --------------------     \(9)              ------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC SravyaSree Yarlagadda               2020-12-09                            311337                                     Initial Programming                                             IntegrateDev2        Jaideep Mankala       03/18/2021             \(9)

# MAGIC Livongo Encounters Claim Primary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC Output required by container but not used here
# MAGIC Get SK for records with out keys
# MAGIC This container is used in:
# MAGIC ESIClmInvoicePKExtr
# MAGIC ESIClmPKExtr
# MAGIC FctsClmPKExtr
# MAGIC FctsPcaClmPKExtr
# MAGIC MCSourceClmPKExtr
# MAGIC MedicaidClmPKExtr
# MAGIC NascoClmExtr
# MAGIC NascoClmPKExtr
# MAGIC PcsClmPKExtr
# MAGIC WellDyneClmPKExtr
# MAGIC MedtrakClmPKExtr
# MAGIC BCBSSCClmPKExtr
# MAGIC EYEMEDClmPKExtr
# MAGIC BCBSSCMedClmPKExtr
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

SrcSysCdSK = get_widget_value('SrcSysCdSK', '')
CurrRunCycle = get_widget_value('CurrRunCycle', '1')
RunID = get_widget_value('RunID', '100')
IDSOwner = get_widget_value('IDSOwner', '')
ids_secret_name = get_widget_value('ids_secret_name', '')
InFile_F = get_widget_value('InFile_F', '')
SrcSysCd = get_widget_value('SrcSysCd', '')

schema_LivongoClmLanding = StructType([
    StructField("livongo_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_date", DateType(), True),
    StructField("client_name", StringType(), True),
    StructField("insurance_member_id", StringType(), True),
    StructField("claim_code", StringType(), True),
    StructField("service_date", DateType(), True),
    StructField("quantity", StringType(), True)
])

df_LivongoClmLanding = (
    spark.read
    .option("sep", ",")
    .option("header", False)
    .option("quote", '"')
    .schema(schema_LivongoClmLanding)
    .csv(f"{adls_path}/verified/{InFile_F}")
)

df_Trans1 = (
    df_LivongoClmLanding
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSK))
    .withColumn("CLM_ID", F.col("livongo_id"))
    .select("SRC_SYS_CD_SK", "CLM_ID")
)

# Deduplicate in place of hf_livongo_encclm_clmpk (Scenario A)
df_clm_pk_in = dedup_sort(
    df_Trans1,
    partition_cols=["SRC_SYS_CD_SK", "CLM_ID"],
    sort_cols=[("SRC_SYS_CD_SK", "A"), ("CLM_ID", "A")]
)

params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "$IDSOwner": IDSOwner
}
df_clm_loadpk_out = ClmLoadPK(df_clm_pk_in, params_ClmLoadPK)

# Write to hf_clm_pk_lkup as parquet (Scenario C)
df_clm_loadpk_out = df_clm_loadpk_out.withColumn(
    "SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " ")
).withColumn(
    "CLM_ID", rpad("CLM_ID", <...>, " ")
).select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_SK"
)

write_files(
    df_clm_loadpk_out,
    "hf_clm_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)