# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2008 Blue Cross/Blue Shield of Kansas City
# MAGIC  
# MAGIC DESCRIPTION: Runs ITS Claim message extract,  transform, primary key jobs.
# MAGIC 
# MAGIC JOB NAME:  LhoFctsITSClmMsgExtr
# MAGIC Calling Job:LhoFctsClmExtr1Seq
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
# MAGIC Parik                   2008-08-13                  Added the new primary key process to the job            3567               devlIDS                                 Steph Goddrad         08/14/2008
# MAGIC 
# MAGIC Reddy Sanam      2020-11-13            Mapped 'LUMERIS' based on source system code param
# MAGIC                                                           in the BusinessRules transformer                                                           IntegrateDev2                                   Manasa Andru         2020/11/13
# MAGIC Prabhu ES           2022-03-29                MSSQL ODBC conn params added                               S2S             IntegrateDev5\(9)Ken Bradmon\(9)2022-06-10

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
from pyspark.sql.functions import col, lit, upper, length, when, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ITSClmMsgPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDate = get_widget_value('CurrDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT CLCL_ID, CLIM_TYP, CLIM_ITS_MSG_CD FROM {FacetsOwner}.CMC_CLIM_ITS_MSG A, tempdb..{DriverTable} DRVR WHERE A.CLCL_ID = DRVR.CLM_ID AND A.CLIM_TYP IN ('S','D')"
df_CMC_CLIM_ITS_MSG_Extract = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
itcd_code_query = f"SELECT ITCD_ITS_CODE, ITCD_CATEGORY, ITCD_DESC FROM {FacetsOwner}.CMC_ITCD_CODE_DESC"
df_CMC_CLIM_ITS_MSG_itcd_code = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", itcd_code_query)
    .load()
)

df_StripField = df_CMC_CLIM_ITS_MSG_Extract.select(
    strip_field(trim(col("CLCL_ID"))).alias("CLCL_ID"),
    strip_field(trim(col("CLIM_TYP"))).alias("CLIM_TYP"),
    strip_field(trim(col("CLIM_ITS_MSG_CD"))).alias("CLIM_ITS_MSG_CD")
)

df_Strip = df_CMC_CLIM_ITS_MSG_itcd_code.select(
    strip_field(trim(col("ITCD_ITS_CODE"))).alias("ITCD_ITS_CODE"),
    strip_field(col("ITCD_CATEGORY")).alias("ITCD_CATEGORY"),
    strip_field(trim(col("ITCD_DESC"))).alias("ITCD_DESC")
)

df_its_desc = dedup_sort(
    df_Strip,
    partition_cols=["ITCD_ITS_CODE", "ITCD_CATEGORY"],
    sort_cols=[]
)

svSrcSysCd = "LUMERIS" if SrcSysCd == "LUMERIS" else "FACETS"

df_br_enriched = (
    df_StripField.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        col("Strip.CLCL_ID") == col("fcts_reversals.CLCL_ID"),
        "left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        col("Strip.CLCL_ID") == col("nasco_dup_lkup.CLM_ID"),
        "left"
    )
    .join(
        df_its_desc.alias("its_desc"),
        [
            col("Strip.CLIM_ITS_MSG_CD") == col("its_desc.ITCD_ITS_CODE"),
            when(col("Strip.CLIM_TYP")=='S','MS').otherwise('MD') == col("its_desc.ITCD_CATEGORY")
        ],
        "left"
    )
)

df_Trans = df_br_enriched.filter(col("nasco_dup_lkup.CLM_ID").isNull()).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(svSrcSysCd).alias("SRC_SYS_CD"),
    (
        lit(svSrcSysCd)
        .concat(lit(";"))
        .concat(col("Strip.CLCL_ID"))
        .concat(lit(";"))
        .concat(col("Strip.CLIM_TYP"))
        .concat(lit(";"))
        .concat(col("Strip.CLIM_ITS_MSG_CD"))
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("ITS_CLM_MSG_SK"),
    col("Strip.CLCL_ID").alias("CLM_ID"),
    col("Strip.CLIM_TYP").alias("CLIM_TYP"),
    lit(0).alias("ITS_CLM_MSG_FMT_CD_SK"),
    col("Strip.CLIM_ITS_MSG_CD").alias("ITS_CLM_MSG_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(
        col("its_desc.ITCD_DESC").isNull()
        | (length(upper(col("its_desc.ITCD_DESC"))) == 0),
        None
    ).otherwise(upper(col("its_desc.ITCD_DESC"))).alias("ITS_CLM_MSG_DESC")
)

df_reversals = df_br_enriched.filter(
    col("fcts_reversals.CLCL_ID").isNotNull()
    & col("fcts_reversals.CLCL_CUR_STS").isin("89", "91", "99")
).select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    lit("I").alias("INSRT_UPDT_CD"),
    lit("N").alias("DISCARD_IN"),
    lit("Y").alias("PASS_THRU_IN"),
    lit(CurrDate).alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(svSrcSysCd).alias("SRC_SYS_CD"),
    (
        lit(svSrcSysCd)
        .concat(lit(";"))
        .concat(col("Strip.CLCL_ID"))
        .concat(lit("R;"))
        .concat(col("Strip.CLIM_TYP"))
        .concat(lit(";"))
        .concat(col("Strip.CLIM_ITS_MSG_CD"))
    ).alias("PRI_KEY_STRING"),
    lit(0).alias("ITS_CLM_MSG_SK"),
    col("Strip.CLCL_ID").concat(lit("R")).alias("CLM_ID"),
    col("Strip.CLIM_TYP").alias("CLIM_TYP"),
    lit(0).alias("ITS_CLM_MSG_FMT_CD_SK"),
    col("Strip.CLIM_ITS_MSG_CD").alias("ITS_CLM_MSG_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    when(
        col("its_desc.ITCD_DESC").isNull()
        | (length(upper(col("its_desc.ITCD_DESC"))) == 0),
        None
    ).otherwise(upper(col("its_desc.ITCD_DESC"))).alias("ITS_CLM_MSG_DESC")
)

df_collector = df_Trans.unionByName(df_reversals)

df_SnapShotAllCol = df_collector.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLIM_TYP").alias("ITS_CLM_MSG_FMT_CD"),
    col("ITS_CLM_MSG_ID").alias("ITS_CLM_MSG_ID"),
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("ITS_CLM_MSG_SK").alias("ITS_CLM_MSG_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ITS_CLM_MSG_DESC").alias("ITS_CLM_MSG_DESC")
)

df_SnapShotTransform = df_collector.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CLIM_TYP").alias("ITS_CLM_MSG_FMT_CD"),
    col("ITS_CLM_MSG_ID").alias("ITS_CLM_MSG_ID")
)

params = {
    "CurrRunCycle": CurrRunCycle,
    "IDSOwner": IDSOwner,
    "SrcSysCd": SrcSysCd
}
df_Key = ITSClmMsgPK(df_SnapShotTransform, df_SnapShotAllCol, params)

df_final = df_Key.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("ITS_CLM_MSG_SK"),
    rpad(col("CLM_ID"), 12, " ").alias("CLM_ID"),
    rpad(col("ITS_CLM_MSG_FMT_CD"), 1, " ").alias("ITS_CLM_MSG_FMT_CD"),
    rpad(col("ITS_CLM_MSG_ID"), 4, " ").alias("ITS_CLM_MSG_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("ITS_CLM_MSG_DESC"), 70, " ").alias("ITS_CLM_MSG_DESC")
)

out_file_path = f"{adls_path}/key/LhoFctsITSClmMsgExtr.LhoFctsITSClmMsg.dat.{RunID}"
write_files(
    df_final,
    out_file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)