# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC  
# MAGIC DESCRIPTION: Runs ITS Claim message extract,  transform, primary key jobs.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer             Date                         Change Description                                                     Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------           ----------------------------     ----------------------------------------------------------------------------       ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari    03/28/2008                Initial program                                                              3255                devlIDSCur                          Steph Goddard          03/31/2008    
# MAGIC SAndrew             04/17/2008                changed keys on hash file hf_cmc_itcd to include     3255                devlIDSCur                          Steph Goddard          04/21/2008
# MAGIC                                                                ITCD_CATEGORY.   Fixed reversal logix to match orig
# MAGIC                                                                Removed IDS parms
# MAGIC 
# MAGIC Parik                  2008-08-13                  Added the new primary key process to the job            3567               devlIDS                                 Steph Goddrad         08/14/2008
# MAGIC Prabhu ES         2022-02-28                  MSSQL connection parameters added                       S2S                 IntegrateDev5                        Kalyan Neelam         2022-06-10

# MAGIC Balancing
# MAGIC This container is used in:               FctsITSClmMsgExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_its_clm_msg_allcol) cleared in calling program
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','20080331')
CurrDate = get_widget_value('CurrDate','')
IDSOwner = get_widget_value('$IDSOwner','')
FacetsOwner = get_widget_value('$FacetsOwner','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
ids_secret_name = get_widget_value('ids_secret_name','')
facets_secret_name = get_widget_value('facets_secret_name','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_itcd_code = f"SELECT ITCD_ITS_CODE,ITCD_CATEGORY,ITCD_DESC FROM {FacetsOwner}.CMC_ITCD_CODE_DESC"
df_itcd_code = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_itcd_code)
    .load()
)

extract_query_extract = (
    f"SELECT A.CLCL_ID,A.CLIM_TYP,A.CLIM_ITS_MSG_CD "
    f"FROM {FacetsOwner}.CMC_CLIM_ITS_MSG A, tempdb..{DriverTable} DRVR "
    f"WHERE A.CLCL_ID=DRVR.CLM_ID "
    f"AND A.CLIM_TYP IN ('S','D')"
)
df_extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_extract)
    .load()
)

df_StripField = df_extract.select(
    strip_field(F.trim(F.col("CLCL_ID"))).alias("CLCL_ID"),
    strip_field(F.trim(F.col("CLIM_TYP"))).alias("CLIM_TYP"),
    strip_field(F.trim(F.col("CLIM_ITS_MSG_CD"))).alias("CLIM_ITS_MSG_CD")
)

df_itcd_code_transform = df_itcd_code.select(
    strip_field(F.trim(F.col("ITCD_ITS_CODE"))).alias("ITCD_ITS_CODE"),
    strip_field(F.col("ITCD_CATEGORY")).alias("ITCD_CATEGORY"),
    strip_field(F.trim(F.col("ITCD_DESC"))).alias("ITCD_DESC")
)

write_files(
    df_itcd_code_transform,
    f"{adls_path}/hf_cmc_itcd_code_desc.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_cmc_itcd_code_desc = spark.read.parquet(f"{adls_path}/hf_cmc_itcd_code_desc.parquet")

df_Strip = df_StripField

df_joined_business = (
    df_Strip.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .join(
        df_hf_cmc_itcd_code_desc.alias("its_desc"),
        [
            F.col("Strip.CLIM_ITS_MSG_CD") == F.col("its_desc.ITCD_ITS_CODE"),
            F.when(F.col("Strip.CLIM_TYP") == F.lit("S"), F.lit("MS")).otherwise(F.lit("MD")) == F.col("its_desc.ITCD_CATEGORY")
        ],
        "left"
    )
)

df_Trans = (
    df_joined_business.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.lit(get_widget_value("CurrDate","")).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("Strip.CLCL_ID"), F.lit(";"), F.col("Strip.CLIM_TYP"), F.lit(";"), F.col("Strip.CLIM_ITS_MSG_CD")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("ITS_CLM_MSG_SK"),
        F.col("Strip.CLCL_ID").alias("CLM_ID"),
        F.col("Strip.CLIM_TYP").alias("CLIM_TYP"),
        F.lit(0).alias("ITS_CLM_MSG_FMT_CD_SK"),
        F.col("Strip.CLIM_ITS_MSG_CD").alias("ITS_CLM_MSG_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            (F.col("its_desc.ITCD_DESC").isNull()) | (F.length(F.upper(F.col("its_desc.ITCD_DESC"))) == 0),
            F.lit(None)
        ).otherwise(F.upper(F.col("its_desc.ITCD_DESC"))).alias("ITS_CLM_MSG_DESC")
    )
)

df_Reversals = (
    df_joined_business.filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("89"))
            | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91"))
            | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("99"))
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.lit(get_widget_value("CurrDate","")).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit("FACETS").alias("SRC_SYS_CD"),
        F.concat(F.lit("FACETS"), F.lit(";"), F.col("Strip.CLCL_ID"), F.lit("R;"), F.col("Strip.CLIM_TYP"), F.lit(";"), F.col("Strip.CLIM_ITS_MSG_CD")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("ITS_CLM_MSG_SK"),
        F.concat(F.col("Strip.CLCL_ID"), F.lit("R")).alias("CLM_ID"),
        F.col("Strip.CLIM_TYP").alias("CLIM_TYP"),
        F.lit(0).alias("ITS_CLM_MSG_FMT_CD_SK"),
        F.col("Strip.CLIM_ITS_MSG_CD").alias("ITS_CLM_MSG_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.when(
            (F.col("its_desc.ITCD_DESC").isNull()) | (F.length(F.upper(F.col("its_desc.ITCD_DESC"))) == 0),
            F.lit(None)
        ).otherwise(F.upper(F.col("its_desc.ITCD_DESC"))).alias("ITS_CLM_MSG_DESC")
    )
)

dfCollector = df_Reversals.unionByName(df_Trans)

dfSnapShotInput = dfCollector
dfSnapShotAllCol = dfSnapShotInput.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLIM_TYP").alias("ITS_CLM_MSG_FMT_CD"),
    F.col("ITS_CLM_MSG_ID").alias("ITS_CLM_MSG_ID"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("ITS_CLM_MSG_SK").alias("ITS_CLM_MSG_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ITS_CLM_MSG_DESC").alias("ITS_CLM_MSG_DESC")
)

dfSnapShotSnapshot = dfSnapShotInput.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLIM_TYP").alias("CLIM_TYP"),
    F.col("ITS_CLM_MSG_ID").alias("ITS_CLM_MSG_ID")
)

dfSnapShotTransform = dfSnapShotInput.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLIM_TYP").alias("ITS_CLM_MSG_FMT_CD"),
    F.col("ITS_CLM_MSG_ID").alias("ITS_CLM_MSG_ID")
)

dfSnapShotAllColOut = dfSnapShotAllCol
dfSnapShotSnapshotOut = dfSnapShotSnapshot.alias("Snapshot")

dfTransformer2 = dfSnapShotSnapshotOut.withColumn(
    "ITS_CLM_MSG_FMT_CD_SK",
    GetFkeyCodes("FACETS", 135, "ITS CLAIM MESSAGE FORMAT", F.col("CLIM_TYP"), 'X')
)

dfTransformer2RowCount = dfTransformer2.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("ITS_CLM_MSG_FMT_CD_SK"),
    F.col("ITS_CLM_MSG_ID")
)

dfTransformer2RowCount_padded = dfTransformer2RowCount
dfTransformer2RowCount_padded = dfTransformer2RowCount_padded.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))
dfTransformer2RowCount_padded = dfTransformer2RowCount_padded.withColumn("ITS_CLM_MSG_ID", F.rpad(F.col("ITS_CLM_MSG_ID"), 4, " "))

write_files(
    dfTransformer2RowCount_padded.select("SRC_SYS_CD_SK","CLM_ID","ITS_CLM_MSG_FMT_CD_SK","ITS_CLM_MSG_ID"),
    f"{adls_path}/load/B_ITS_CLM_MSG.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ITSClmMsgPK
# COMMAND ----------

params_ITSClmMsgPK = {
    "CurrRunCycle": CurrRunCycle,
    "$IDSOwner": IDSOwner,
    "SrcSysCd": SrcSysCd
}
dfITSClmMsgPK = ITSClmMsgPK(dfSnapShotTransform, dfSnapShotAllColOut, params_ITSClmMsgPK)

dfITSClmMsgPK_padded = dfITSClmMsgPK
dfITSClmMsgPK_padded = dfITSClmMsgPK_padded.withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
dfITSClmMsgPK_padded = dfITSClmMsgPK_padded.withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
dfITSClmMsgPK_padded = dfITSClmMsgPK_padded.withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
dfITSClmMsgPK_padded = dfITSClmMsgPK_padded.withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))
dfITSClmMsgPK_padded = dfITSClmMsgPK_padded.withColumn("ITS_CLM_MSG_FMT_CD", F.rpad(F.col("ITS_CLM_MSG_FMT_CD"), 1, " "))

write_files(
    dfITSClmMsgPK_padded.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "ITS_CLM_MSG_SK",
        "CLM_ID",
        "ITS_CLM_MSG_FMT_CD",
        "ITS_CLM_MSG_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ITS_CLM_MSG_DESC"
    ),
    f"{adls_path}/key/FctsITSClmMsgExtr.FctsITSClmMsg.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)