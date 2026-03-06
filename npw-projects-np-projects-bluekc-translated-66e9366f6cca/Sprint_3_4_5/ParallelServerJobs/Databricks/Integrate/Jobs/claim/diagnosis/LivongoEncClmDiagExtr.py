# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: LivongoEncClmDiagExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC  *Reads the  Livongo_Monthly_Billing_Claims.*.dat file.
# MAGIC * Livongo Encounter claims data information from provider groups to the IDS CLM_DIAG table.
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC Developer                               Date                 Project/Altiris #                 Change Description                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                          --------------------     ------------------------      -----------------------------------------------------------------------                           --------------------------------       -------------------------------   ----------------------------       
# MAGIC SravyaSree Yarlagadda       2020-12-28            311337                                 Initial Programming                                                        IntegrateDev2        Manasa Andru           2021-03-17

# MAGIC Read the LivongoClm_DiagCd_landing
# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
# MAGIC This container is used in:
# MAGIC FctsClmDiagExtr
# MAGIC NascoClmDiagExtr
# MAGIC BCBSSCClmDiagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# Parameters
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
InFile_F = get_widget_value('InFile_F','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmDiagPK
# COMMAND ----------

# Read from LivongoClmLanding (CSeqFileStage)
schema_LivongoClmLanding = StructType([
    StructField("col0", StringType(), True),
    StructField("col1", StringType(), True),
    StructField("col2", StringType(), True),
    StructField("col3", StringType(), True),
    StructField("col4", StringType(), True),
    StructField("col5", StringType(), True),
    StructField("col6", StringType(), True),
    StructField("col7", StringType(), True),
    StructField("col8", StringType(), True)
])
df_LivongoClmLanding_raw = (
    spark.read
    .option("quote", '"')
    .option("header", False)
    .schema(schema_LivongoClmLanding)
    .csv(f"{adls_path}/verified/{InFile_F}")
)
df_LivongoClmLanding = df_LivongoClmLanding_raw.select(
    F.col("col0").alias("livongo_id"),
    F.col("col1").alias("first_name"),
    F.col("col2").alias("last_name"),
    F.col("col3").alias("birth_date"),
    F.col("col4").alias("client_name"),
    F.col("col5").alias("member_id"),
    F.col("col6").alias("claim_code"),
    F.col("col7").alias("service_date"),
    F.col("col8").alias("quantity")
)

# CHashedFileStage hf_livid_dedup - Scenario A (intermediate hashed file) => deduplicate by key column [livongo_id]
df_hf_livid_dedup = dedup_sort(
    df_LivongoClmLanding,
    ["livongo_id"],
    []
)

# BusinessRules (CTransformerStage)
df_BusinessRules = df_hf_livid_dedup.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("livongo_id"), F.lit(";"), F.lit("1")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_DIAG_SK"),
    F.col("livongo_id").alias("CLM_ID"),
    F.lit("1").alias("CLM_DIAG_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.lit("Z863").alias("DIAG_CD"),
    F.lit("NA").alias("CLM_DIAG_POA_CD"),
    F.lit("ICD10").alias("DIAG_CD_TYP_CD")
)

# Snapshot (CTransformerStage) - single input, three outputs

# 1) Output pin "AllCol"
df_Snapshot_allcol = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_DIAG_SK").alias("CLM_DIAG_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("CLM_DIAG_POA_CD").alias("CLM_DIAG_POA_CD"),
    F.col("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
)

# 2) Output pin "Snapshot"
df_Snapshot_snapshot = df_BusinessRules.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD")
)

# 3) Output pin "Transform"
df_Snapshot_transform = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit("1").alias("CLM_DIAG_ORDNL_CD")
)

# Transformer (CTransformerStage) with input "Snapshot"
df_Transformer = df_Snapshot_snapshot.withColumn(
    "CLM_DIAG_ORDNL_CD",
    GetFkeyCodes('NPS', 0, "DIAGNOSIS ORDINAL", F.col("CLM_DIAG_ORDNL_CD"), 'X')
).select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD")
)

# B_CLM_DIAG (CSeqFileStage) - write out
# Before writing, apply rpad only if type is char or varchar with known length:
df_B_CLM_DIAG_write = df_Transformer.select(
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 255, " ").alias("CLM_ID"),  # No length given, using a placeholder 255
    F.rpad(F.col("CLM_DIAG_ORDNL_CD"), 2, " ").alias("CLM_DIAG_ORDNL_CD")
)
write_files(
    df_B_CLM_DIAG_write,
    f"{adls_path}/load/B_CLM_DIAG.Livongo.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# ClmDiagPK (CContainerStage)
params_ClmDiagPK = {
    "CurrRunCycle": f"{CurrRunCycle}",
    "SrcSysCd": f"{SrcSysCd}",
    "$IDSOwner": f"{IDSOwner}"
}
df_ClmDiagPK_out = ClmDiagPK(df_Snapshot_allcol, df_Snapshot_transform, params_ClmDiagPK)

# LivongoEncClmDiag (CSeqFileStage) - final write
# The output columns (18 total) in the specified order:
df_LivongoEncClmDiag = df_ClmDiagPK_out.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_DIAG_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.rpad(F.col("CLM_DIAG_ORDNL_CD"), 2, " ").alias("CLM_DIAG_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("DIAG_CD"),
    F.rpad(F.col("CLM_DIAG_POA_CD"), 2, " ").alias("CLM_DIAG_POA_CD"),
    F.col("DIAG_CD_TYP_CD")
)
write_files(
    df_LivongoEncClmDiag,
    f"{adls_path}/key/LivongoEncClmDiagExtr.LivongoEncClmDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)