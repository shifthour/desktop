# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: LivongoEncClmLnExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC  *Reads the Livongo_Monthly_Billing_Claims.*.dat file.
# MAGIC * Livongo Encounter claims data information from provider groups to the IDS CLM_LN_DIAG table.
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC Developer                                     Date            Project/Altiris #                  Change Description                                                       Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                            --------------------     ------------------------      -----------------------------------------------------------------------                           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Mrudula Kodali                     2020-12-16            311337                                   Initial Programming                                                        IntegrateDev2                Manasa Andru          2021-03-17

# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Extract Livongo EncClaim Line Detail Data
# MAGIC This container is used in:
# MAGIC FctsClmLnDiagExtr
# MAGIC NascoClmLnDiagExtr
# MAGIC BCBSSCClmLnDiagExtr
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
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, lit, concat, row_number, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDiagPK
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
InFile_F = get_widget_value('InFile_F','')

schema_LivongoClmLanding = StructType([
    StructField("livongo_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_date", StringType(), True),
    StructField("client_name", StringType(), True),
    StructField("member_id", StringType(), True),
    StructField("claim_code", StringType(), True),
    StructField("service_date", StringType(), True),
    StructField("quantity", StringType(), True)
])

df_LivongoClmLanding = (
    spark.read
    .schema(schema_LivongoClmLanding)
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .csv(f"{adls_path}/verified/{InFile_F}")
)

df_Sort_liv = df_LivongoClmLanding.sort(col("livongo_id").asc()).select(
    "livongo_id",
    "first_name",
    "last_name",
    "birth_date",
    "client_name",
    "member_id",
    "claim_code",
    "service_date",
    "quantity"
)

window_spec = Window.partitionBy(col("livongo_id")).orderBy(col("livongo_id"))
df_BusinessRules_intermediate = df_Sort_liv.withColumn("SvlvgID", col("livongo_id")).withColumn(
    "SvOldClaimID",
    row_number().over(window_spec)
)

df_BusinessRules = df_BusinessRules_intermediate.select(
    col("livongo_id").alias("CLM_ID"),
    col("SvOldClaimID").alias("CLM_LN_SEQ_NO"),
    lit("Z863").alias("DIAG_CODE"),
    lit("1").alias("CLM_LN_DIAG_ORDNL_CD"),
    lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit("0").alias("ERR_CT"),
    lit("0").alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(
        lit(SrcSysCd), lit(";"),
        col("CLM_ID"), lit(";"),
        col("CLM_LN_SEQ_NO"), lit(";"),
        lit("1")
    ).alias("PRI_KEY_STRING"),
    lit("0").alias("CLM_LN_DIAG_SK"),
    lit("0").alias("CRT_RUN_CYC_EXCTN_SK"),
    lit("0").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit("ICD10").alias("DIAG_CD_TYP_CD")
)

df_Snapshot_AllCol = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD"),
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    lit(CurrentDate).alias("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_DIAG_SK"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DIAG_CODE").alias("DIAG_CD"),
    col("DIAG_CD_TYP_CD")
)

df_Snapshot_Snapshot = df_BusinessRules.select(
    col("CLM_ID").alias("CLCL_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD")
)

df_Snapshot_Transform = df_BusinessRules.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD")
)

df_Transformer_in = df_Snapshot_Snapshot
df_Transformer = df_Transformer_in.withColumn(
    "svDiagOrdSk",
    GetFkeyCodes(
        lit("FACETS"),
        lit(0),
        lit("DIAGNOSIS ORDINAL"),
        col("CLM_LN_DIAG_ORDNL_CD"),
        lit("X")
    )
)

df_B_CLM_LN_DIAG = df_Transformer.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLCL_ID").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("svDiagOrdSk").alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

write_files(
    df_B_CLM_LN_DIAG.select("SRC_SYS_CD_SK","CLM_ID","CLM_LN_SEQ_NO","CLM_LN_DIAG_ORDNL_CD_SK"),
    f"{adls_path}/load/B_CLM_LN_DIAG.Livongo.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmLnDiagPK = {
    "DriverTable": "NA",
    "RunID": RunID,
    "CurrRunCycle": CurrRunCycle,
    "CurrDateTime": CurrentDate,
    "FacetsDB": "NA",
    "FacetsOwner": FacetsOwner,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk,
    "IDSOwner": IDSOwner
}

df_ClmLnDiagPK, = ClmLnDiagPK(df_Snapshot_AllCol, df_Snapshot_Transform, params_ClmLnDiagPK)

df_final = df_ClmLnDiagPK.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_LN_DIAG_SK"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
    col("CLM_LN_SEQ_NO"),
    col("CLM_LN_DIAG_ORDNL_CD"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("DIAG_CD"),
    col("DIAG_CD_TYP_CD")
)

write_files(
    df_final,
    f"{adls_path}/key/LivongoEncClmLnDiagExtr.LivongoEncClmLnDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)