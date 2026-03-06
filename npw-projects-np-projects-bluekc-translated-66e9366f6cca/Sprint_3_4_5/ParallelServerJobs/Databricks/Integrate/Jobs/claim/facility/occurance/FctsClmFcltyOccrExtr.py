# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmFcltyOccrExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLHO_OCC_CODE to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLHO_OCC_CODE
# MAGIC                 Joined to
# MAGIC                 IDS_TMP_CLAIM to get specific records by date
# MAGIC   
# MAGIC HASH FILES:   hf_clm_nasco_dup_bypass
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
# MAGIC                    Tom Harrocks 08/15/2004-   Originally Programmed
# MAGIC                    Steph Goddard 02/14/2006  Added transform, primary key for sequencer
# MAGIC          BJ Luce            03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC                    Sanderw  12/08/2006   Project 1756  - Reversal logix added for new status codes 89 and  and 99       
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          08/15/2007      Balancing               Added Snapshot extract for balancing                                        devlIDS30                       Steph Goddard           8/30/07
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-28      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                         Steph Goddard          07/30/2008
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5                Manasa Andru            2022-06-10

# MAGIC Read from FACETS
# MAGIC Writing Sequential File to /verified
# MAGIC Hash file (hf_fclty_occur_allcol) cleared from the shared container FcltyClmOccurPK
# MAGIC Strip un-printable chars
# MAGIC Apply the defined business rules
# MAGIC This container is used in:
# MAGIC FctsClmFcltyOccurExtr
# MAGIC NascoClmFcltyOccurTrns
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC **hash file built in FctsClmDriverBuild.
# MAGIC **Bypass claims on hf_clm_nasco_dup_bypass, do not build a regular claim row.
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
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmOccurPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RunID = get_widget_value('RunID','')
CurrentDateParam = get_widget_value('CurrentDate','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = f"""
SELECT 
CLCL_ID,
CLHO_SEQ_NO,
MEME_CK,
CLHO_OCC_CODE,
CLHO_OCC_FROM_DT,
CLHO_OCC_TO_DT,
CLHO_LOCK_TOKEN
FROM {FacetsOwner}.CMC_CLHO_OCC_CODE A
    INNER JOIN tempdb..{DriverTable} B ON A.CLCL_ID = B.CLM_ID
"""
df_CMC_CLHO_OCC_CODE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripField = df_CMC_CLHO_OCC_CODE.select(
    strip_field(F.col("CLCL_ID")).alias("CLCL_ID"),
    F.col("CLHO_SEQ_NO").alias("CLHO_SEQ_NO"),
    F.col("MEME_CK").alias("MEME_CK"),
    strip_field(F.col("CLHO_OCC_CODE")).alias("CLHO_OCC_CODE"),
    strip_field(F.col("CLHO_OCC_FROM_DT")).alias("CLHO_OCC_FROM_DT"),
    strip_field(F.col("CLHO_OCC_TO_DT")).alias("CLHO_OCC_TO_DT"),
    F.col("CLHO_LOCK_TOKEN").alias("CLHO_LOCK_TOKEN")
)

df_joined = (
    df_StripField.alias("Strip")
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

df_with_vars = (
    df_joined
    .withColumn(
        "ClmId",
        F.when(
            F.col("Strip.CLCL_ID").isNull() | (F.length(trim(F.col("Strip.CLCL_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.CLCL_ID")))
    )
    .withColumn(
        "SeqNum",
        F.when(
            F.col("Strip.CLHO_SEQ_NO").isNull()
            | (F.length(trim(F.col("Strip.CLHO_SEQ_NO"))) == 0)
            | (F.col("Strip.CLHO_SEQ_NO").rlike("^[0-9]+$") == F.lit(False)),
            F.lit(0)
        ).otherwise(trim(F.col("Strip.CLHO_SEQ_NO")))
    )
    .withColumn("PassThru", F.lit("Y"))
)

df_FcltyOccrOut = (
    df_with_vars
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.col("PassThru").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("SeqNum")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("FCLTY_CLM_OCCUR_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.col("ClmId").alias("CLM_ID"),
        F.col("SeqNum").alias("FCLTY_CLM_OCCUR_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("FCLTY_CLM_SK"),
        F.when(
            F.col("Strip.CLHO_OCC_CODE").isNull() | (F.length(trim(F.col("Strip.CLHO_OCC_CODE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("Strip.CLHO_OCC_CODE")))).alias("FCLTY_CLM_OCCUR_CD"),
        F.date_format(F.col("Strip.CLHO_OCC_FROM_DT"), "yyyy-MM-dd").alias("FROM_DT_SK"),
        F.date_format(F.col("Strip.CLHO_OCC_TO_DT"), "yyyy-MM-dd").alias("TO_DT_SK"),
    )
)

df_reversals = (
    df_with_vars
    .filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("89"))
            | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("91"))
            | (F.col("fcts_reversals.CLCL_CUR_STS") == F.lit("99"))
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
        F.col("PassThru").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("SeqNum")).alias("PRI_KEY_STRING"),
        F.lit(0).alias("FCLTY_CLM_OCCUR_SK"),
        F.lit(0).alias("SRC_SYS_CD_SK"),
        F.concat(trim(F.col("ClmId")), F.lit("R")).alias("CLM_ID"),
        F.col("SeqNum").alias("FCLTY_CLM_OCCUR_SEQ_NO"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("FCLTY_CLM_SK"),
        F.when(
            F.col("Strip.CLHO_OCC_CODE").isNull() | (F.length(trim(F.col("Strip.CLHO_OCC_CODE"))) == 0),
            F.lit("NA")
        ).otherwise(F.upper(trim(F.col("Strip.CLHO_OCC_CODE")))).alias("FCLTY_CLM_OCCUR_CD"),
        F.date_format(F.col("Strip.CLHO_OCC_FROM_DT"), "yyyy-MM-dd").alias("FROM_DT_SK"),
        F.date_format(F.col("Strip.CLHO_OCC_TO_DT"), "yyyy-MM-dd").alias("TO_DT_SK"),
    )
)

df_Collector = df_reversals.unionByName(df_FcltyOccrOut)

df_SnapShot = df_Collector

df_AllCol = df_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("FCLTY_CLM_OCCUR_SK").alias("FCLTY_CLM_OCCUR_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
    F.col("FCLTY_CLM_OCCUR_CD").alias("FCLTY_CLM_OCCUR_CD"),
    F.col("FROM_DT_SK").alias("FROM_DT_SK"),
    F.col("TO_DT_SK").alias("TO_DT_SK")
)

df_SnapShotOut = df_SnapShot.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO")
)

df_TransformOut = df_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO")
)

df_TransformerIn = df_SnapShotOut
df_TransformerOut = df_TransformerIn.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO")
)

df_B_FCLTY_CLM_OCCUR = df_TransformerOut
write_files(
    df_B_FCLTY_CLM_OCCUR,
    f"{adls_path}/load/B_FCLTY_CLM_OCCUR.FACETS.dat.{RunID}",
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
dfKey = FcltyClmOccurPK(df_AllCol, df_TransformOut, params)

dfKeyFinal = dfKey.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("FCLTY_CLM_OCCUR_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("FCLTY_CLM_OCCUR_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("FCLTY_CLM_SK"),
    F.col("FCLTY_CLM_OCCUR_CD"),
    F.rpad(F.col("FROM_DT_SK"), 10, " ").alias("FROM_DT_SK"),
    F.rpad(F.col("TO_DT_SK"), 10, " ").alias("TO_DT_SK")
)

write_files(
    dfKeyFinal,
    f"{adls_path}/key/FctsClmFcltyOccrExtr.FctsClmFcltyOccr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)