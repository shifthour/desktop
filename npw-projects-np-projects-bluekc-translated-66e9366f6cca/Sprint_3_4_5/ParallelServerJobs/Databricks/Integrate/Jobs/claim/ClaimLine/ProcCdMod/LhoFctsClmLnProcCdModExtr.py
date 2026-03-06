# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     LhoFctsClmLnProcCdModExtr
# MAGIC 
# MAGIC Called By: LhoFctsClmExtr1Seq 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CDML_CL_LINE and CMC_CDOR_LI_OVR to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CDML_CL_LINE and CMC_CDOR_LI_OVR
# MAGIC                 Joined with 
# MAGIC                 tempdb..TMP_IDS_CLAIM  to get claims updated within time period
# MAGIC   
# MAGIC HASH FILES:  hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------    
# MAGIC Venkatesh babu Munnangi    2020-10-01        US262820            Created new version to load Lho historical claims                         IntegrateDev2
# MAGIC 
# MAGIC Prabhu ES               2022-03-29        S2S                       MSSQL ODBC conn params added                                            IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Added Hash stage to remove duplicates based on kye columns
# MAGIC Hash file (hf_clm_ln_proc_cd_mod_allcol) cleared in calling program
# MAGIC Apply business logic
# MAGIC This container is used in:
# MAGIC FctsClmLnProcCdModExtr
# MAGIC NascoClmLnProcCdModExtr
# MAGIC LhoFctsClmLnProcCdModExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Pulling Facets Claim Procedure Code Modifier
# MAGIC Data for a specific Time Period
# MAGIC hash file built in ClmDriverBuild
# MAGIC 
# MAGIC bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
# MAGIC Writing Sequential File to ../key
# MAGIC Assign primary surrogate key
# MAGIC Strip Carriage Return, Line Feed, and  Tab
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, NumericType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = f"SELECT CL_LINE.CLCL_ID,CL_LINE.CDML_SEQ_NO,CL_LINE.IPCD_ID,CL_LINE.CDML_IPCD_MOD2,CL_LINE.CDML_IPCD_MOD3,CL_LINE.CDML_IPCD_MOD4 FROM {LhoFacetsStgOwner}.CMC_CDML_CL_LINE CL_LINE INNER JOIN tempdb..{DriverTable} TMP ON TMP.CLM_ID = CL_LINE.CLCL_ID"
df_CMC_CDML_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_strip = df_CMC_CDML_CL_LINE.select(
    strip_field(F.col("CLCL_ID")).alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    current_date().alias("EXT_TIMESTAMP"),
    strip_field(F.col("IPCD_ID")).alias("IPCD_ID"),
    strip_field(F.col("CDML_IPCD_MOD2")).alias("CDML_IPCD_MOD2"),
    strip_field(F.col("CDML_IPCD_MOD3")).alias("CDML_IPCD_MOD3"),
    strip_field(F.col("CDML_IPCD_MOD4")).alias("CDML_IPCD_MOD4")
)

df_businessrules = (
    df_strip.alias("Strip")
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
)

df_enriched = (
    df_businessrules
    .withColumn(
        "svMod1",
        F.when(
            F.col("Strip.IPCD_ID").isNull()
            | (F.length(trim(F.col("Strip.IPCD_ID"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("Strip.IPCD_ID"))))
    )
    .withColumn(
        "svMod2",
        F.when(
            F.col("Strip.CDML_IPCD_MOD2").isNull()
            | (F.length(trim(F.col("Strip.CDML_IPCD_MOD2"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("Strip.CDML_IPCD_MOD2"))).substr(2, 1))
    )
    .withColumn(
        "svMod3",
        F.when(
            F.col("Strip.CDML_IPCD_MOD3").isNull()
            | (F.length(trim(F.col("Strip.CDML_IPCD_MOD3"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("Strip.CDML_IPCD_MOD3"))).substr(2, 1))
    )
    .withColumn(
        "svMod4",
        F.when(
            F.col("Strip.CDML_IPCD_MOD4").isNull()
            | (F.length(trim(F.col("Strip.CDML_IPCD_MOD4"))) == 0),
            F.lit("")
        ).otherwise(F.upper(trim(F.col("Strip.CDML_IPCD_MOD4"))).substr(2, 1))
    )
    .withColumn(
        "svOrdnl1",
        F.when(F.length(F.col("svMod1")) == 7, F.lit("1")).otherwise(F.lit(""))
    )
    .withColumn(
        "svOrdnl2",
        F.when(F.col("svMod2") != "", F.lit("2")).otherwise(F.lit(""))
    )
    .withColumn(
        "svOrdnl3",
        F.when(F.col("svMod3") != "", F.lit("3")).otherwise(F.lit(""))
    )
    .withColumn(
        "svOrdnl4",
        F.when(F.col("svMod4") != "", F.lit("4")).otherwise(F.lit(""))
    )
    .withColumn("ClmId", trim(F.col("Strip.CLCL_ID")))
)

df_procCd1 = (
    df_enriched
    .filter(
        (F.length(F.col("svMod1")) == 7)
        & (F.col("svMod1") != "")
        & (F.col("nasco_dup_lkup.CLM_ID").isNull())
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("ClmId"),
            F.lit(";"),
            F.col("CDML_SEQ_NO"),
            F.lit(";"),
            F.col("svOrdnl1")
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svOrdnl1").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMod1").substr(2, 1).alias("PROC_CD_MOD_CD")
    )
)

df_procCd2 = (
    df_enriched
    .filter(
        (F.col("svMod2") != "")
        & (F.col("svMod2") != "")
        & (F.col("nasco_dup_lkup.CLM_ID").isNull())
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("ClmId"),
            F.lit(";"),
            F.col("CDML_SEQ_NO"),
            F.lit(";"),
            F.col("svOrdnl2")
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svOrdnl2").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMod2").alias("PROC_CD_MOD_CD")
    )
)

df_procCd3 = (
    df_enriched
    .filter(
        (F.col("svMod3") != "")
        & (F.col("svMod3") != "")
        & (F.col("nasco_dup_lkup.CLM_ID").isNull())
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("ClmId"),
            F.lit(";"),
            F.col("CDML_SEQ_NO"),
            F.lit(";"),
            F.col("svOrdnl3")
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svOrdnl3").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMod3").alias("PROC_CD_MOD_CD")
    )
)

df_procCd4 = (
    df_enriched
    .filter(
        (F.col("svMod4") != "")
        & (F.col("svMod4") != "")
        & (F.col("nasco_dup_lkup.CLM_ID").isNull())
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("ClmId"),
            F.lit(";"),
            F.col("CDML_SEQ_NO"),
            F.lit(";"),
            F.col("svOrdnl4")
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svOrdnl4").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMod4").alias("PROC_CD_MOD_CD")
    )
)

df_reversals_ProcCd1 = (
    df_enriched
    .filter(
        (F.length(F.col("svMod1")) == 7)
        & (F.col("svMod1") != "")
        & (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("ClmId"),
            F.lit("R;"),
            F.col("CDML_SEQ_NO"),
            F.lit(";"),
            F.col("svOrdnl1")
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        rpad(F.concat(F.col("ClmId"), F.lit("R")), 18, " ").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svOrdnl1").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMod1").substr(2, 1).alias("PROC_CD_MOD_CD")
    )
)

df_reversals_ProcCd2 = (
    df_enriched
    .filter(
        (F.col("svMod2") != "")
        & (F.col("svMod2") != "")
        & (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("ClmId"),
            F.lit("R;"),
            F.col("CDML_SEQ_NO"),
            F.lit(";"),
            F.col("svOrdnl2")
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        rpad(F.concat(F.col("ClmId"), F.lit("R")), 18, " ").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svOrdnl2").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMod2").alias("PROC_CD_MOD_CD")
    )
)

df_reversals_ProcCd3 = (
    df_enriched
    .filter(
        (F.col("svMod3") != "")
        & (F.col("svMod3") != "")
        & (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("ClmId"),
            F.lit("R;"),
            F.col("CDML_SEQ_NO"),
            F.lit(";"),
            F.col("svOrdnl3")
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        rpad(F.concat(F.col("ClmId"), F.lit("R")), 18, " ").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svOrdnl3").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMod3").alias("PROC_CD_MOD_CD")
    )
)

df_reversals_ProcCd4 = (
    df_enriched
    .filter(
        (F.col("svMod4") != "")
        & (F.col("svMod4") != "")
        & (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
        F.col("EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd),
            F.lit(";"),
            F.col("ClmId"),
            F.lit("R;"),
            F.col("CDML_SEQ_NO"),
            F.lit(";"),
            F.col("svOrdnl4")
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
        rpad(F.concat(F.col("ClmId"), F.lit("R")), 18, " ").alias("CLM_ID"),
        F.col("CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("svOrdnl4").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("svMod4").alias("PROC_CD_MOD_CD")
    )
)

df_collector = (
    df_procCd1
    .unionByName(df_procCd2)
    .unionByName(df_procCd3)
    .unionByName(df_procCd4)
    .unionByName(df_reversals_ProcCd1)
    .unionByName(df_reversals_ProcCd2)
    .unionByName(df_reversals_ProcCd3)
    .unionByName(df_reversals_ProcCd4)
)

df_rmdp = dedup_sort(
    df_collector,
    ["CLM_ID", "CLM_LN_SEQ_NO", "CLM_LN_PROC_CD_MOD_ORDNL_CD"],
    []
)

df_snap_in = df_rmdp

df_allcol = df_snap_in.select(
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
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD_MOD_CD").alias("PROC_CD_MOD_CD")
)

df_snapshot = df_snap_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

df_transform = df_snap_in.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnProcCdModPK
# COMMAND ----------

params_ClmLnProcCdModPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}

df_ClmLnProcCdModPK = ClmLnProcCdModPK(df_transform, df_allcol, params_ClmLnProcCdModPK)

schema_lho = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), True),
    StructField("INSRT_UPDT_CD", StringType(), True),
    StructField("DISCARD_IN", StringType(), True),
    StructField("PASS_THRU_IN", StringType(), True),
    StructField("FIRST_RECYC_DT", TimestampType(), True),
    StructField("ERR_CT", IntegerType(), True),
    StructField("RECYCLE_CT", IntegerType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("PRI_KEY_STRING", StringType(), True),
    StructField("CLM_LN_PROC_CD_MOD_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("CLM_LN_PROC_CD_MOD_ORDNL_CD", StringType(), True),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("PROC_CD_MOD_CD", StringType(), True)
])

df_final_lho = df_ClmLnProcCdModPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_PROC_CD_MOD_SK"),
    rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD_MOD_CD")
)

lho_file_path = f"{adls_path}/key/LhoFctsClmLnProcCdModExtr.LhoFctsClmLnProcCdMod.dat.{RunID}"
write_files(
    df_final_lho,
    lho_file_path,
    delimiter=",",
    mode="overwrite",
    is_parqet=False,
    header=False,
    quote="\"",
    nullValue=None
)

df_snapshot_in = df_snapshot

df_transformer = df_snapshot_in.withColumn(
    "svOrdnlCdSk",
    GetFkeyCodes(
        F.col("SRC_SYS_CD_SK"),
        F.lit(0),
        F.lit("PROCEDURE ORDINAL"),
        F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
        F.lit("X")
    )
)

df_rowCount = df_transformer.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnlCdSk").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
)

schema_b_clm = StructType([
    StructField("SRC_SYS_CD_SK", IntegerType(), True),
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_SEQ_NO", IntegerType(), True),
    StructField("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK", IntegerType(), True)
])

b_file_path = f"{adls_path}/load/B_CLM_LN_PROC_CD_MOD.{SrcSysCd}.dat.{RunID}"
write_files(
    df_rowCount,
    b_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)