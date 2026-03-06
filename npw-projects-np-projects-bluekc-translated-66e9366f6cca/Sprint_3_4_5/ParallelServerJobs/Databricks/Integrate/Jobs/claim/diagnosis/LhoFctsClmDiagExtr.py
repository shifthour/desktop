# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmDiagExtr
# MAGIC Calling Job: LhoFctsClmExtr1Seq
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLMD_DIAG to a landing file for the IDS
# MAGIC 
# MAGIC       
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                        --------------------------------       -------------------------------   ----------------------------       
# MAGIC             
# MAGIC                                                                                      Copied from Facets source and made these changes below
# MAGIC Reddy Sanam      2020-08-7                                         As part of Lumeris historical load these below changes are done                           IntegrateDev2
# MAGIC                                                                                      Source query params changed
# MAGIC                                                                                       FACETS is replaced with SrcSysCd in the output mapping
# MAGIC                                                                                       in the transformer "BusinessRules"
# MAGIC                                                                                       In the target sequential file name Fcts string is replaced with
# MAGIC                                                                                       LhoFcts
# MAGIC                                                                                       In the Transformer stage change the value "FACETS" to
# MAGIC                                                                                       SrcSysCd 
# MAGIC                                                                                       In this stage -B_CLM_DIAG file name part is changed to "SrcSysCd"
# MAGIC                                                                                       hash file stage "RmDup_NaturalKey" added to remove duplicates 
# MAGIC                                                                                       Resolved Schema reconciliation warning in the shared container
# MAGIC                                                                                       "ClmDiagPK" for ordinal code
# MAGIC Venkatesh Babu 2020-10-12                                         Removed Data Elements Populated in the job
# MAGIC Prabhu ES          2022-03-29             S2S                      MSSQL ODBC conn params added                                                                       IntegrateDev5	Ken Bradmon	2022-06-08

# MAGIC Remove duplicates based on Claim ID, Ordinal Code and Source System Code.
# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
# MAGIC This container is used in:
# MAGIC 
# MAGIC LhoFctsClmDiagExtr
# MAGIC BCAFEPClmDiagExtr
# MAGIC BCBSAClmDiagExtr
# MAGIC BCBSSCClmDiagExtr
# MAGIC EyeMedClmDiagExtr
# MAGIC FctsClmDiagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to ../key
# MAGIC Extract Facets Claim Diagnosis Data
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Apply business logic
# MAGIC Hash files built in FctsClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDateTime = get_widget_value('CurrDateTime','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LhoFacetsStgOwner = get_widget_value('$LhoFacetsStgOwner','')
LhoFacetsStgPW = get_widget_value('$LhoFacetsStgPW','')
LhoFacetsStgAcct = get_widget_value('$LhoFacetsStgAcct','')
LhoFacetsStgDSN = get_widget_value('$LhoFacetsStgDSN','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmDiagPK
# COMMAND ----------

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet").select(
    "CLCL_ID",
    "CLCL_CUR_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM"
)

df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet").select(
    "CLM_ID"
)

jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
extract_query_CD_MAPPING = """SELECT	CD_MPPNG.SRC_CD,
	CD_MPPNG.TRGT_CD
FROM	#$IDSOwner#.CD_MPPNG CD_MPPNG
WHERE	CD_MPPNG.SRC_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
AND	CD_MPPNG.TRGT_DOMAIN_NM = 'DIAGNOSIS CODE TYPE'
AND	SRC_CLCTN_CD = 'FACETS DBO'"""
df_CD_MAPPING = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_CD_MAPPING)
    .load()
)

df_hf_ClmDiagExtr_DiagCdTypCd = df_CD_MAPPING.dropDuplicates(["SRC_CD"])

jdbc_url_stg, jdbc_props_stg = get_db_config(<...>)
extract_query_CMC_CLMD_DIAG = f"""
SELECT
    A.CLCL_ID,
    A.CLMD_TYPE,
    A.MEME_CK,
    A.IDCD_ID,
    A.CLMD_LOCK_TOKEN,
    A.CLMD_POA_IND,
    B.CLMF_ICD_IND_PROC,
    C.CLCL_HIGH_SVC_DT
FROM    tempdb..{DriverTable}  TMP,
        #${LhoFacetsStgOwner}#.CMC_CLCL_CLAIM   C,
        #${LhoFacetsStgOwner}#.CMC_CLMD_DIAG    A
        LEFT OUTER JOIN #${LhoFacetsStgOwner}#.CMC_CLMF_MULT_FUNC  B
          ON  A.CLCL_ID = B.CLCL_ID
WHERE   A.CLCL_ID  =  TMP.CLM_ID
  AND   C.CLCL_ID  =  A.CLCL_ID
"""
df_CMC_CLMD_DIAG = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_stg)
    .options(**jdbc_props_stg)
    .option("query", extract_query_CMC_CLMD_DIAG)
    .load()
)

df_TrnsStripField = df_CMC_CLMD_DIAG.withColumn("CLCL_ID", strip_field(F.col("CLCL_ID"))) \
    .withColumn("CLMD_TYPE", strip_field(F.col("CLMD_TYPE"))) \
    .withColumn("EXT_TIMESTAMP", F.lit(CurrDateTime)) \
    .withColumn("MEME_CK", F.col("MEME_CK")) \
    .withColumn("IDCD_ID", strip_field(F.col("IDCD_ID"))) \
    .withColumn("CLMD_LOCK_TOKEN", F.col("CLMD_LOCK_TOKEN")) \
    .withColumn("CLMD_POA_IND", strip_field(F.col("CLMD_POA_IND"))) \
    .withColumn("CLMF_ICD_IND_PROC", trim(strip_field(F.col("CLMF_ICD_IND_PROC")))) \
    .withColumn("CLCL_HIGH_SVC_DT", F.col("CLCL_HIGH_SVC_DT"))

df_join_1 = df_TrnsStripField.alias("Strip").join(
    df_hf_clm_fcts_reversals.alias("fcts_reversals"),
    F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
    "left"
)

df_join_2 = df_join_1.join(
    df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
    F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
    "left"
)

df_join_3 = df_join_2.join(
    df_hf_ClmDiagExtr_DiagCdTypCd.alias("TRGT_CD"),
    F.col("Strip.IDCD_ID") == F.col("TRGT_CD.SRC_CD"),  # Inferred condition to avoid cross join
    "left"
)

df_BusinessRules = df_join_3.withColumn(
    "ClmId", trim(F.col("Strip.CLCL_ID"))
).withColumn(
    "svOrdnlCd",
    F.when(F.col("Strip.CLMD_TYPE").substr(F.lit(1), F.lit(1)) == F.lit("0"),
           F.col("Strip.CLMD_TYPE").substr(F.lit(2), F.lit(1)))
     .when(F.col("Strip.CLMD_TYPE") == F.lit("AD"), F.lit("A"))
     .when(F.col("Strip.CLMD_TYPE") == F.lit("E1"), F.lit("E"))
     .otherwise(trim(F.col("Strip.CLMD_TYPE")))
).withColumn(
    "svClmDiagPOA",
    F.when(F.length(trim(F.col("Strip.CLMD_POA_IND"))) == 0, F.lit("NA"))
     .otherwise(trim(F.upper(F.col("Strip.CLMD_POA_IND"))))
).withColumn(
    "svDiagCdTypCd",
    F.when(F.col("TRGT_CD.TRGT_CD").isNull(), F.lit(" ")).otherwise(F.col("TRGT_CD.TRGT_CD"))
)

df_FctsClmDiagCd = df_BusinessRules.filter(
    F.col("nasco_dup_lkup.CLM_ID").isNull()
).select(
    F.col("ClmId").alias("CLM_ID"),
    F.col("svOrdnlCd").alias("CLM_DIAG_ORDNL_CD"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.concat(
        F.lit(SrcSysCd), F.lit(";"),
        strip_field(F.col("ClmId")), F.lit(";"),
        F.col("svOrdnlCd")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.regexp_replace(trim(F.col("Strip.IDCD_ID")), "\\.", "").alias("DIAG_CD"),
    F.col("svClmDiagPOA").alias("CLM_DIAG_POA_CD"),
    F.col("svDiagCdTypCd").alias("DIAG_CD_TYP_CD")
)

df_reversals = df_BusinessRules.filter(
    F.col("fcts_reversals.CLCL_ID").isNotNull()
).select(
    F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
    F.col("svOrdnlCd").alias("CLM_DIAG_ORDNL_CD"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.concat(
        F.lit(SrcSysCd), F.lit(";"),
        strip_field(F.col("ClmId")), F.lit("R;"),
        F.col("svOrdnlCd")
    ).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_DIAG_SK"),
    F.lit(0).alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.regexp_replace(trim(F.col("Strip.IDCD_ID")), "\\.", "").alias("DIAG_CD"),
    F.col("svClmDiagPOA").alias("CLM_DIAG_POA_CD"),
    F.col("svDiagCdTypCd").alias("DIAG_CD_TYP_CD")
)

df_Collector = df_reversals.unionByName(df_FctsClmDiagCd).select(
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD",
    "SRC_SYS_CD",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "PRI_KEY_STRING",
    "CLM_DIAG_SK",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "DIAG_CD",
    "CLM_DIAG_POA_CD",
    "DIAG_CD_TYP_CD"
)

df_RmDup_NaturalKey = df_Collector.dropDuplicates(["CLM_ID","CLM_DIAG_ORDNL_CD","SRC_SYS_CD"])

df_Snapshot = df_RmDup_NaturalKey.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_DIAG_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("DIAG_CD"),
    F.col("CLM_DIAG_POA_CD"),
    F.col("DIAG_CD_TYP_CD")
)

df_AllCol = df_Snapshot.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_DIAG_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("DIAG_CD"),
    F.col("CLM_DIAG_POA_CD"),
    F.col("DIAG_CD_TYP_CD")
)

df_Load = df_Snapshot.select(
    F.col("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD")
)

df_Transform = df_Snapshot.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD")
)

df_Transformer = df_Load.withColumn(
    "svDiagOrdnlCd",
    GetFkeyCodes(
        F.lit("FACETS"), 
        F.lit(0), 
        F.lit("DIAGNOSIS ORDINAL"), 
        F.col("CLM_DIAG_ORDNL_CD"), 
        F.lit("X")
    )
).select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("svDiagOrdnlCd").alias("CLM_DIAG_ORDNL_CD_SK")
)

df_B_CLM_DIAG = df_Transformer.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD_SK"
)

write_files(
    df_B_CLM_DIAG,
    f"{adls_path}/load/B_CLM_DIAG.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmDiagPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "$IDSOwner": IDSOwner
}
df_ClmDiagPK = ClmDiagPK(df_Transform, df_AllCol, params_ClmDiagPK)

df_IdsClmDiag = df_ClmDiagPK.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_DIAG_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    rpad(F.col("CLM_DIAG_ORDNL_CD"), 2, " ").alias("CLM_DIAG_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("DIAG_CD"),
    rpad(F.col("CLM_DIAG_POA_CD"), 2, " ").alias("CLM_DIAG_POA_CD"),
    F.col("DIAG_CD_TYP_CD")
)

write_files(
    df_IdsClmDiag,
    f"{adls_path}/key/LhoFctsClmDiagExtr.LhoFctsClmDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)