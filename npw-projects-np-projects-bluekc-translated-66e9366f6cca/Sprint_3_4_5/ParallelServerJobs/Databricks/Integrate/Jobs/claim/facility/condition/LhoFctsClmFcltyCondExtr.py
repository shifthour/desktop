# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     LhoFctsClmFcltyCondExtr
# MAGIC       
# MAGIC CALLING JOB : LhoFctsClmOnDmdExtr1Seq
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLHC_COND_CODE to a landing file for the IDS
# MAGIC 
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLHC_COND_CODE
# MAGIC                 Joined to
# MAGIC                 IDS_TMP_CLAIM to get specific records by date
# MAGIC   
# MAGIC HASH FILES: hf_clm_nasco_dup_bypass
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   STRIP.FIELD
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Strip non-printables from input stream
# MAGIC   
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( TmpOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC                    Tom Harrocks     08/15/2004-    Originally Programmed
# MAGIC                    Steph Goddard   02/13/2006    Combined extract, transform, primary key for sequencer
# MAGIC                     BJ Luce             03/20/2006    add hf_clm_nasco_dup_bypass built in ClmDriverBuild, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC                    Sanderw            12/08/2006     Project 1756  - Reversal logix added for new status codes 89 and  and 99
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                                                      Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                                                               --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          08/15/2007                                    Added Snapshot extract for balancing                                                                                  devlIDS30                     Steph Goddard           8/30/07
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-25      3567(Primary Key)    Changed primary key process from hash file to DB2 table                                                    devlIDS                         Steph Goddard           07/29/2008 
# MAGIC 
# MAGIC Reddy Sanam         2020-08-11       US263443               New Job is created from Facets job and made these changes below.                            IntegrateDev2                    Jeyaprasanna           2020-10-22
# MAGIC                                                                                          Removed Facets Env variables and addred LhoFacets Env
# MAGIC                                                                                          variables
# MAGIC                                                                                          Changed source query to reflect LhoFacets Parameters
# MAGIC                                                                                          Changed hard coding of "FACETS" to  SrcSysCd in
# MAGIC                                                                                          "Business Rules" Transformer
# MAGIC                                                                                            changed the substring "FACETS" to parameter SrcSynCd
# MAGIC                                                                                             in the file name of the sequential  file stage-B_FCLTY_CLM_COND
# MAGIC                                                                                            Changed target file name and stage name to include the
# MAGIC                                                                                            substring "LhoFcts"
# MAGIC 
# MAGIC Venkatesh Babu     2020-10-12                                        Brought up to standards
# MAGIC Prabhu ES              2022-03-29        S2S                          MSSQL ODBC conn params added                                                                               IntegrateDev5		Ken Bradmon	2022-06-11

# MAGIC Read from FACETS
# MAGIC Writing Sequential File to ../key
# MAGIC Hash file (hf_fclty_cond_allcol) cleared from the shared container FcltyClmCondPK
# MAGIC Strip un-printable chars
# MAGIC Apply the defined business rules
# MAGIC This container is used in:
# MAGIC FctsClmFcltyCondExtr
# MAGIC NascoClmFcltyCondTrns
# MAGIC LhoFctsClmFcltyCondExtr
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
from pyspark.sql.functions import col, lit, when, length, upper, concat, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
LhoFacetsStgOwner = get_widget_value('$LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgAcct = get_widget_value('$LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('$LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('$LhoFacetsStgDSN','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = (
    f"SELECT CLCL_ID, CLHC_SEQ_NO, MEME_CK, CLHC_COND_CD, CLHC_LOCK_TOKEN "
    f"FROM {LhoFacetsStgOwner}.CMC_CLHC_COND_CODE A "
    f"INNER JOIN tempdb..{DriverTable} B ON A.CLCL_ID = B.CLM_ID"
)
df_CMC_CLHC_COND_CODE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripField = df_CMC_CLHC_COND_CODE.withColumn("CLCL_ID", strip_field(col("CLCL_ID"))) \
    .withColumn("CLHC_COND_CD", strip_field(col("CLHC_COND_CD"))) \
    .select("CLCL_ID", "CLHC_SEQ_NO", "MEME_CK", "CLHC_COND_CD", "CLHC_LOCK_TOKEN")

df_BusinessRules = (
    df_StripField.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"), col("Strip.CLCL_ID") == col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"), col("Strip.CLCL_ID") == col("nasco_dup_lkup.CLM_ID"), "left")
    .withColumn("RowPassThru", lit("Y"))
    .withColumn("ClmId", trim(col("Strip.CLCL_ID")))
)

df_ClmFcltyCond = (
    df_BusinessRules
    .filter(col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),
        lit(CurrentDate).alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        concat(lit(SrcSysCd), lit(";"), col("ClmId"), lit(";"), col("Strip.CLHC_SEQ_NO")).alias("PRI_KEY_STRING"),
        lit(0).alias("FCLTY_CLM_COND_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        rpad(col("ClmId"), 12, " ").alias("CLM_ID"),
        col("Strip.CLHC_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("FCLTY_CLM_SK"),
        when(
            col("Strip.CLHC_COND_CD").isNull() | (length(trim(col("Strip.CLHC_COND_CD"))) == 0),
            lit("NA")
        ).otherwise(upper(trim(col("Strip.CLHC_COND_CD")))).alias("FCLTY_CLM_COND_CD")
    )
)

df_reversals = (
    df_BusinessRules
    .filter(
        col("fcts_reversals.CLCL_ID").isNotNull()
        & col("fcts_reversals.CLCL_CUR_STS").isin("89", "91", "99")
    )
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        rpad(lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        rpad(lit("N"), 1, " ").alias("DISCARD_IN"),
        rpad(col("RowPassThru"), 1, " ").alias("PASS_THRU_IN"),
        lit(CurrentDate).alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        concat(lit(SrcSysCd), lit(";"), col("ClmId"), lit("R;"), col("Strip.CLHC_SEQ_NO")).alias("PRI_KEY_STRING"),
        lit(0).alias("FCLTY_CLM_COND_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        rpad(concat(col("ClmId"), lit("R")), 12, " ").alias("CLM_ID"),
        trim(col("Strip.CLHC_SEQ_NO")).alias("FCLTY_CLM_COND_SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("FCLTY_CLM_SK"),
        when(
            col("Strip.CLHC_COND_CD").isNull() | (length(trim(col("Strip.CLHC_COND_CD"))) == 0),
            lit("NA")
        ).otherwise(upper(trim(col("Strip.CLHC_COND_CD")))).alias("FCLTY_CLM_COND_CD")
    )
)

df_Collector = df_ClmFcltyCond.unionByName(df_reversals)

df_CollectorFinal = df_Collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "FCLTY_CLM_COND_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "FCLTY_CLM_COND_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FCLTY_CLM_SK",
    "FCLTY_CLM_COND_CD"
)

df_SnapshotAllCol = df_CollectorFinal.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "FCLTY_CLM_COND_SEQ_NO",
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "FCLTY_CLM_COND_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FCLTY_CLM_SK",
    "FCLTY_CLM_COND_CD"
)

df_SnapshotSnapShot = df_CollectorFinal.select(
    col("CLM_ID").alias("CLM_ID"),
    col("FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO")
)

df_SnapshotTransform = df_CollectorFinal.withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk)).select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "FCLTY_CLM_COND_SEQ_NO"
)

df_Transformer = df_SnapshotSnapShot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO")
)

write_files(
    df_Transformer.select("SRC_SYS_CD_SK", "CLM_ID", "FCLTY_CLM_COND_SEQ_NO"),
    f"{adls_path}/load/B_FCLTY_CLM_COND.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmCondPK
params_ = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "$IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_FcltyClmCondPK = FcltyClmCondPK(df_SnapshotAllCol, df_SnapshotTransform, params_)

df_LhoFctsClmFcltyCondExtr = df_FcltyClmCondPK.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "FCLTY_CLM_COND_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "FCLTY_CLM_COND_SEQ_NO",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FCLTY_CLM_SK",
    "FCLTY_CLM_COND_CD"
).withColumn(
    "INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " ")
).withColumn(
    "DISCARD_IN", rpad(col("DISCARD_IN"), 1, " ")
).withColumn(
    "PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " ")
).withColumn(
    "CLM_ID", rpad(col("CLM_ID"), 12, " ")
).withColumn(
    "FCLTY_CLM_COND_CD", rpad(col("FCLTY_CLM_COND_CD"), 2, " ")
)

write_files(
    df_LhoFctsClmFcltyCondExtr,
    f"{adls_path}/key/LhoFctsClmFcltyCondExtr.LhoFctsClmFcltyCond.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)