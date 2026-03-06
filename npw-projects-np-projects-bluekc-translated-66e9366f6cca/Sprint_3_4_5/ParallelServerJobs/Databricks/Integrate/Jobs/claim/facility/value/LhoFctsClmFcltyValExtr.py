# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020, 2022 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:     LhoFctsClmFcltyValExtr
# MAGIC Calling Job:     LhoFctsClmOnDmdExtr1Seq
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                          Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                  --------------------------------       -------------------------------   ----------------------------       
# MAGIC Reddy Sanam         2020-08-12       263447                    Copied from Facets job                                                                    IntegrateDev2
# MAGIC                                                                                         Changed the source query to reflect LhoFctsStg parameters
# MAGIC                                                                                         Changed Target file name to include the substring LhoFcts
# MAGIC                                                                                         Replaced "FACETS" with SrcSysCd in BusinessRules Tranformer
# MAGIC Prabhu ES               2022-03-29       S2S                         MSSQL ODBC conn params added                                                 IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Writing Sequential File to /verified
# MAGIC Hash file (hf_fclty_val_allcol) cleared from the shared container FcltyClmValPK
# MAGIC Strip un-printable chars
# MAGIC Read from FACETS
# MAGIC This container is used in:
# MAGIC FctsClmFcltyValExtr
# MAGIC NascoClmFcltyValTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC hash file built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmValPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = (
    f"SELECT \n"
    f"CLCL_ID,\n"
    f"CLVC_NUMBER,\n"
    f"CLVC_LETTER,MEME_CK,\n"
    f"CLVC_CODE,CLVC_AMT,\n"
    f"CLVC_VALUE,\n"
    f"CLVC_LOCK_TOKEN,\n"
    f"ATXR_SOURCE_ID \n"
    f"FROM {LhoFacetsStgOwner}.CMC_CLVC_VAL_CODE A\n"
    f"     INNER JOIN tempdb..{DriverTable}  B ON A.CLCL_ID = B.CLM_ID \n"
    f"ORDER BY B.CLM_ID,\n"
    f"A.CLVC_NUMBER,\n"
    f"A.CLVC_LETTER"
)
df_CMC_CLVC_VAL_CODE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_strip = df_CMC_CLVC_VAL_CODE.select(
    strip_field("CLCL_ID").alias("CLCL_ID"),
    strip_field("CLVC_NUMBER").alias("CLVC_NUMBER"),
    strip_field("CLVC_LETTER").alias("CLVC_LETTER"),
    F.col("MEME_CK").alias("MEME_CK"),
    strip_field("CLVC_CODE").alias("CLVC_CODE"),
    F.col("CLVC_AMT").alias("CLVC_AMT"),
    F.col("CLVC_VALUE").alias("CLVC_VALUE"),
    F.col("CLVC_LOCK_TOKEN").alias("CLVC_LOCK_TOKEN"),
    F.col("ATXR_SOURCE_ID").alias("ATXR_SOURCE_ID")
)

df_businessrules = (
    df_strip.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left",
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left",
    )
)

df_businessrules_vars = df_businessrules.withColumn(
    "ClmId",
    F.when(
        F.col("Strip.CLCL_ID").isNull()
        | (F.length(trim(F.col("Strip.CLCL_ID"))) == 0),
        F.lit("UNK"),
    ).otherwise(trim(F.col("Strip.CLCL_ID")))
).withColumn(
    "PassThru",
    F.lit("Y")
)

windowSpec = Window.partitionBy("ClmId").orderBy("ClmId")
df_businessrules_vars = df_businessrules_vars.withColumn(
    "OrdNum",
    (F.row_number().over(windowSpec) - 1),
)

df_FcltyValOut = df_businessrules_vars.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("OrdNum")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("FCLTY_CLM_VAL_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.col("OrdNum").alias("FCLTY_CLM_VAL_ORD"),
    F.when(
        F.col("Strip.CLVC_NUMBER").isNull()
        | (F.length(trim(F.col("Strip.CLVC_NUMBER"))) == 0),
        "UNK",
    ).otherwise(trim(F.col("Strip.CLVC_NUMBER"))).alias("CLVC_NUMBER"),
    F.when(
        F.col("Strip.CLVC_LETTER").isNull()
        | (F.length(trim(F.col("Strip.CLVC_LETTER"))) == 0),
        "UNK",
    ).otherwise(trim(F.col("Strip.CLVC_LETTER"))).alias("CLVC_LETTER"),
    F.when(
        F.col("Strip.CLVC_CODE").isNull()
        | (F.length(trim(F.col("Strip.CLVC_CODE"))) == 0),
        "NA",
    ).otherwise(UpCase(trim(F.col("Strip.CLVC_CODE")))).alias("FCLTY_CLM_VAL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    F.when(
        F.col("Strip.CLVC_AMT").isNull()
        | (F.length(trim(F.col("Strip.CLVC_AMT"))) == 0)
        | (~(F.col("Strip.CLVC_AMT").cast("double").isNotNull())),
        0,
    ).otherwise(F.col("Strip.CLVC_AMT").cast("double")).alias("VAL_AMT"),
    F.when(
        F.col("Strip.CLVC_VALUE").isNull()
        | (F.length(trim(F.col("Strip.CLVC_VALUE"))) == 0)
        | (~(F.col("Strip.CLVC_VALUE").cast("double").isNotNull())),
        0,
    ).otherwise(F.col("Strip.CLVC_VALUE").cast("double")).alias("VAL_UNIT_CT"),
)

df_reversals = df_businessrules_vars.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    & (F.col("fcts_reversals.CLCL_CUR_STS").isin(["89", "91", "99"]))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("OrdNum")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("FCLTY_CLM_VAL_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
    F.col("OrdNum").alias("FCLTY_CLM_VAL_ORD"),
    F.when(
        F.col("Strip.CLVC_NUMBER").isNull()
        | (F.length(trim(F.col("Strip.CLVC_NUMBER"))) == 0),
        "UNK",
    ).otherwise(trim(F.col("Strip.CLVC_NUMBER"))).alias("CLVC_NUMBER"),
    F.when(
        F.col("Strip.CLVC_LETTER").isNull()
        | (F.length(trim(F.col("Strip.CLVC_LETTER"))) == 0),
        "UNK",
    ).otherwise(trim(F.col("Strip.CLVC_LETTER"))).alias("CLVC_LETTER"),
    F.when(
        F.col("Strip.CLVC_CODE").isNull()
        | (F.length(trim(F.col("Strip.CLVC_CODE"))) == 0),
        "NA",
    ).otherwise(UpCase(trim(F.col("Strip.CLVC_CODE")))).alias("FCLTY_CLM_VAL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    F.when(
        F.col("Strip.CLVC_AMT").isNull()
        | (F.length(trim(F.col("Strip.CLVC_AMT"))) == 0)
        | (~(F.col("Strip.CLVC_AMT").cast("double").isNotNull())),
        0,
    ).otherwise(-F.col("Strip.CLVC_AMT").cast("double")).alias("VAL_AMT"),
    F.when(
        F.col("Strip.CLVC_VALUE").isNull()
        | (F.length(trim(F.col("Strip.CLVC_VALUE"))) == 0)
        | (~(F.col("Strip.CLVC_VALUE").cast("double").isNotNull())),
        0,
    ).otherwise(F.col("Strip.CLVC_VALUE").cast("double")).alias("VAL_UNIT_CT"),
)

df_collector = df_reversals.unionByName(df_FcltyValOut)

df_AllCol = df_collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_ORD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("FCLTY_CLM_VAL_SK"),
    F.col("CLVC_NUMBER"),
    F.col("CLVC_LETTER"),
    F.col("FCLTY_CLM_VAL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FCLTY_CLM_SK"),
    F.col("VAL_AMT"),
    F.col("VAL_UNIT_CT")
)

df_SnapShot = df_collector.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_ORD")
)

df_Transform = df_collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_SEQ_NO")
)

df_Transformer = df_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_SEQ_NO")
)

write_files(
    df_Transformer,
    f"{adls_path}/load/B_FCLTY_CLM_VAL.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
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
df_FcltyClmValPK = FcltyClmValPK(df_AllCol, df_Transform, params)

df_FcltyClmValExtr = df_FcltyClmValPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("FCLTY_CLM_VAL_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("FCLTY_CLM_VAL_ORD"),
    F.rpad(F.col("CLVC_NUMBER"), 2, " ").alias("CLVC_NUMBER"),
    F.rpad(F.col("CLVC_LETTER"), 1, " ").alias("CLVC_LETTER"),
    F.col("FCLTY_CLM_VAL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FCLTY_CLM_SK"),
    F.col("VAL_AMT"),
    F.col("VAL_UNIT_CT")
)

write_files(
    df_FcltyClmValExtr,
    f"{adls_path}/key/LhoFctsClmFcltyValExtr.LhoFctsClmFcltyVal.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)