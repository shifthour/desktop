# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2018 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  LivongoEncClmLandSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run. 
# MAGIC          *  The primary key hash file hf_clm_ln  is the output of this job and is used by the following tables for keying
# MAGIC              CLM_LN
# MAGIC              
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date                             Project/Altiris #\(9)         Change Description\(9)\(9)\(9)Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------                                   --------------------     \(9)              ------------------------\(9)-----------------------------------------------------------------------\(9)--------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Mrudula Kodali                           2020-03-10                    311337                                Initial Programming                                                     IntegrateDev2                Jaideep Mankala          03/18/2021     \(9)

# MAGIC Get SK for primary key on input record
# MAGIC LivongoEncounterClaim LinePrimary Key Process
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code claim ID and seq_no
# MAGIC Output required by container but not used here
# MAGIC This container is used in:
# MAGIC ESIClmLnPKExtr
# MAGIC FctsClmLnPKExtr
# MAGIC FctsClmLnRemitPKExtr
# MAGIC MCSourceClmLnPKExtr
# MAGIC MedicaidClmLnPKExtr
# MAGIC NascoClmLnPKExtr
# MAGIC PcsClmLnPKExtr
# MAGIC WellDyneClmLnPKExtr
# MAGIC MedtrakClmLnPKExtr
# MAGIC BCBSSCClmLnPKExtr
# MAGIC BCBSSCMedClmLnPKExtr
# MAGIC EYEMEDMedClmLnPKExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Integrate
# COMMAND ----------


# Obtain parameter values
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSK = get_widget_value('SrcSysCdSK','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
InFile_F = get_widget_value('InFile_F','')

# Read from LivongoEncClmLanding (CSeqFileStage)
schema_LivongoEncClmLanding = StructType([
    StructField("livongo_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("insurance_member_id", StringType(), True),
    StructField("claim_code", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("quantity", StringType(), True)
])
df_LivongoEncClmLanding = spark.read.format("csv") \
    .option("header", "false") \
    .option("quote", "\"") \
    .option("delimiter", ",") \
    .schema(schema_LivongoEncClmLanding) \
    .load(f"{adls_path}/verified/{InFile_F}")

# Sort_liv (sort)
df_Sort_liv = df_LivongoEncClmLanding.sort("livongo_id")

# Trans1 (CTransformerStage) - implement row-by-row logic via window
w = Window.partitionBy(F.trim(F.col("livongo_id"))).orderBy(
    "livongo_id",
    "first_name",
    "last_name",
    "birth_date",
    "client_name",
    "insurance_member_id",
    "claim_code",
    "service_date",
    "quantity"
)
df_Trans1_enriched = (
    df_Sort_liv
    .withColumn("SvOldClaimID", F.row_number().over(w))
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSK).cast("int"))
    .withColumn("CLM_ID", F.col("livongo_id"))
    .withColumn("CLM_LN_SEQ_NO", F.col("SvOldClaimID"))
)

df_Trans1_out = df_Trans1_enriched.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO"
)

# hf_livenc_clmlnpk_dedupe (CHashedFileStage) - Scenario A, deduplicate on key columns
df_hf_livenc_clmlnpk_dedupe = dedup_sort(
    df_Trans1_out,
    ["SRC_SYS_CD_SK", "CLM_ID", "CLM_LN_SEQ_NO"],
    []
)

# MAGIC %run ../../../../../../shared_containers/PrimaryKey/ClmLnLoadPK
# COMMAND ----------

params_ClmLnLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner
}
df_hf_clm_ln_pk_lkup = ClmLnLoadPK(df_hf_livenc_clmlnpk_dedupe, params_ClmLnLoadPK)

# hf_clm_ln_pk_lkup (CHashedFileStage) - Scenario C, final write to parquet
df_hf_clm_ln_pk_lkup = df_hf_clm_ln_pk_lkup \
    .withColumn("SRC_SYS_CD", rpad("SRC_SYS_CD", <...>, " ")) \
    .withColumn("CLM_ID", rpad("CLM_ID", <...>, " ")) \
    .select("SRC_SYS_CD", "CLM_ID", "CLM_LN_SEQ_NO", "CRT_RUN_CYC_EXCTN_SK", "CLM_LN_SK")

write_files(
    df_hf_clm_ln_pk_lkup,
    f"{adls_path}/hf_clm_ln_pk_lkup.parquet",
    ',',
    'overwrite',
    True,
    True,
    '\"',
    None
)