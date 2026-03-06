# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  EmrProcedureClmLnProcCdModExtr
# MAGIC CALLED BY:  IdsEmrProcedureClmLnLoadSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC Reads the EmrProcedure Clm Ln file, and runs through primary key using shared container ClmLnPrcCdModPkey
# MAGIC     
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer		Date		Project/Altiris #		Change Description						Development Project		Code Reviewer	Date Reviewed       
# MAGIC ==================================================================================================================================================================================================
# MAGIC Ken Bradmon		2023-02-27	us542805			Original Programming					IntegrateDev2			Harsha Ravuri	2023-06-14

# MAGIC Job Name:  EmrProcedureClmLnProcCdModExtr
# MAGIC Purpose:  Reads the EmrProcedure Clm Ln file, and runs through primary key using shared container ClmLnPrcCdModPkey
# MAGIC /ids/prod/verified/#InFile_F#
# MAGIC Sort, then Pivot the Proc_CD_Mod values
# MAGIC Hash file (hf_clm_ln_proc_cd_mod_allcol) cleared in calling program
# MAGIC Drop the records where the MOD_CD is NULL
# MAGIC Trim all values
# MAGIC This container is used in:
# MAGIC FctsClmLnProcCdModExtr
# MAGIC NascoClmLnProcCdModExtr
# MAGIC BCBSSCClmLnProcCdModExtr
# MAGIC BCBSAClmLnProcCdModExtr
# MAGIC EyeMedClmLnProcCdModExtr
# MAGIC EyeMedMAClmLnProcCdModExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Assign values for the CLM_LN_PROC_CD_MOD_ORDNL_CD.
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnProcCdModPK
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile_F = get_widget_value('InFile_F','')

# EmrProcedureClmFormatFile (CSeqFileStage) → Read the delimited file
schema_EmrProcedureClmFormatFile = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("POL_NO", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_MID_NM", StringType(), True),
    StructField("MBI", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR", StringType(), True),
    StructField("PROC_TYPE", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("RSLT_VAL", StringType(), True),
    StructField("RNDR_NTNL_PROV_ID", StringType(), True),
    StructField("RNDR_PROV_TYP", StringType(), True),
    StructField("PROC_CD_CPT", StringType(), True),
    StructField("PROC_CD_CPT_MOD_1", StringType(), True),
    StructField("PROC_CD_CPT_MOD_2", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_1", StringType(), True),
    StructField("PROC_CD_CPTII_MOD_2", StringType(), True),
    StructField("SNOMED", StringType(), True),
    StructField("CVX", StringType(), True),
    StructField("SOURCE_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", StringType(), True),
    StructField("CLM_LN_SEQ", StringType(), True)
])

df_EmrProcedureClmFormatFile = (
    spark.read
    .option("header", "false")
    .option("quote", "000")
    .schema(schema_EmrProcedureClmFormatFile)
    .csv(f"{adls_path}/verified/{InFile_F}")
)

# Copy (CTransformerStage)
df_Copy = df_EmrProcedureClmFormatFile.select(
    trim(F.col("CLM_ID")).alias("CLM_ID"),
    trim(F.col("CLM_LN_SEQ")).alias("CLM_LN_NO"),
    trim(F.col("PROC_CD_CPT_MOD_1")).alias("MOD_CD_1"),
    trim(F.col("PROC_CD_CPT_MOD_2")).alias("MOD_CD_2"),
    trim(F.col("PROC_CD_CPTII_MOD_1")).alias("MOD_CD_3"),
    trim(F.col("PROC_CD_CPTII_MOD_2")).alias("MOD_CD_4")
)

# Sort_For_Pivot (sort)
df_Sort_For_Pivot = df_Copy.sort(F.col("CLM_ID").asc(), F.col("CLM_LN_NO").asc())

# ProcCd_Pivot (Pivot) - pivot columns MOD_CD_1, MOD_CD_2, MOD_CD_3, MOD_CD_4 into one MOD_CD column
df_ProcCd_Pivot = df_Sort_For_Pivot.select(
    "CLM_ID",
    "CLM_LN_NO",
    F.explode(F.array("MOD_CD_1","MOD_CD_2","MOD_CD_3","MOD_CD_4")).alias("MOD_CD")
)

# Trf_FilterNull (CTransformerStage) - filter out rows where MOD_CD is null or empty
df_Trf_FilterNull = (
    df_ProcCd_Pivot
    .filter((F.col("MOD_CD").isNotNull()) & (F.col("MOD_CD") != ''))
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("MOD_CD").alias("MOD_CD")
    )
)

# hf_clm_ln_EmrProcedure_prc_cd (CHashedFileStage) => Scenario A (intermediate hashed file).
# Deduplicate on key columns [CLM_ID, CLM_LN_NO, MOD_CD].
df_hf_clm_ln_EmrProcedure_prc_cd = df_Trf_FilterNull.dropDuplicates(["CLM_ID","CLM_LN_NO","MOD_CD"])

# Sort_For_Ordl_Cd (sort)
df_Sort_For_Ordl_Cd = df_hf_clm_ln_EmrProcedure_prc_cd.sort(F.col("CLM_ID").asc(), F.col("CLM_LN_NO").asc(), F.col("MOD_CD").asc())

# Trf_AssignOrdlCd (CTransformerStage) - use row_number over partition to assign ordinal
windowSpec = Window.partitionBy("CLM_ID","CLM_LN_NO").orderBy("MOD_CD")
df_Trf_AssignOrdlCd = (
    df_Sort_For_Ordl_Cd
    .filter((F.col("MOD_CD").isNotNull()) & (F.col("MOD_CD") != ''))
    .withColumn("CLM_LN_PROC_CD_MOD_ORDNL_CD", F.row_number().over(windowSpec))
    .withColumn("CLM_LN_SEQ_NO", F.col("CLM_LN_NO"))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(
            "",
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("CLM_ID"),
            F.lit(";"),
            F.col("CLM_LN_NO"),
            F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD")
        )
    )
    .withColumn("CLM_LN_PROC_CD_MOD_SK", F.lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("PROC_CD_MOD_CD", F.col("MOD_CD"))
)

# Snapshot (CTransformerStage) - produces three output links from df_Trf_AssignOrdlCd
# 1) AllCol => 17 columns
df_Snapshot_AllCol = df_Trf_AssignOrdlCd.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_LN_PROC_CD_MOD_SK").alias("CLM_LN_PROC_CD_MOD_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD_MOD_CD").alias("PROC_CD_MOD_CD")
)

# 2) Snapshot => 4 columns
df_Snapshot_Snapshot = df_Trf_AssignOrdlCd.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

# 3) Transform => 4 columns
df_Snapshot_Transform = df_Trf_AssignOrdlCd.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

# Transformer (CTransformerStage) - input from df_Snapshot_Snapshot, calculates CLM_LN_PROC_CD_MOD_ORDNL_CD_SK via GetFkeyCodes
df_Transformer = df_Snapshot_Snapshot.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
).withColumn(
    "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK",
    GetFkeyCodes(SrcSysCd, F.lit(0), "PROCEDURE ORDINAL", F.trim(F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD")), F.lit("X"))
)

# B_CLM_LN_PROC_CD_MOD (CSeqFileStage) - write out
df_B_CLM_LN_PROC_CD_MOD = df_Transformer.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_LN_SEQ_NO",
    "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK"
).withColumn(
    "CLM_ID",
    rpad(F.col("CLM_ID"), 20, " ")
).withColumn(
    "CLM_LN_SEQ_NO",
    rpad(F.col("CLM_LN_SEQ_NO"), 2, " ")
)

write_files(
    df_B_CLM_LN_PROC_CD_MOD,
    f"{adls_path}/load/B_CLM_LN_PROC_CD_MOD.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ClmLnProcCdModPK (CContainerStage) - two inputs and one output "Key"
params_ClmLnProcCdModPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_ClmLnProcCdModPK_Key = ClmLnProcCdModPK(df_Snapshot_Transform, df_Snapshot_AllCol, params_ClmLnProcCdModPK)

# EmrProcedureClmLnProcCdModExtr (CSeqFileStage) - final write
df_final_EmrProcedureClmLnProcCdModExtr = (
    df_ClmLnProcCdModPK_Key
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), 20, " "))
    .withColumn("CLM_LN_SEQ_NO", rpad(F.col("CLM_LN_SEQ_NO"), 2, " "))
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
        "CLM_LN_PROC_CD_MOD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CLM_LN_PROC_CD_MOD_ORDNL_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "PROC_CD_MOD_CD"
    )
)

write_files(
    df_final_EmrProcedureClmLnProcCdModExtr,
    f"{adls_path}/key/EmrProcedureClmLnProcCdModExtr.EmrProcedureClmLnProcCdMod.uniq.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)