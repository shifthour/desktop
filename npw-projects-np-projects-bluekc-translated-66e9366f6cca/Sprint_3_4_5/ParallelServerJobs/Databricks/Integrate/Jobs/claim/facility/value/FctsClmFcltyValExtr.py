# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmHospExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from CMC_CLHP_HOSP to a landing file for the IDS
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	CMC_CLHP_HOSP
# MAGIC                 Joined to
# MAGIC                 CMC_CLCL_CLAIM to get specific records by date
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
# MAGIC 
# MAGIC                    Oliver Nielsen        05/07/2004-   Originally Programmed
# MAGIC                    Oliver Nielsen        07/15/2004-  1.1 Implementation - DRG Logic, Grouper Logic, Temp Tables now based on timestamp.          
# MAGIC                    Steph Goddard      02/14/2006  Added transform and pkey process for sequencer
# MAGIC                      BJ Luce                03/20/2006     add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC                    sharon andrew      12/08/2006   Project 1756  - Reversal logix added for new status codes 89 and  and 99
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          08/15/2007       Balancing              Added Snapshot extract for balancing                                        devlIDS30                      Steph Goddard          8/30/07
# MAGIC 
# MAGIC Bhoomi Dasari         2008-08-01      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                        Steph Goddrad          08/07/2008
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5                Manasa Andru        2022-06-10

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
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmValPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','TMP_IDS_CLAIM')
FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','100')
RunID = get_widget_value('RunID','20080731')
CurrentDate = get_widget_value('CurrentDate','2008-07-31')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','105859')
SrcSysCd = get_widget_value('SrcSysCd','FACETS')

# Read hashed file "hf_clm_fcts_reversals" (Scenario C) -> parquet
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_hf_clm_fcts_reversals = df_hf_clm_fcts_reversals.select(
    "CLCL_ID",
    "CLCL_CUR_STS",
    "CLCL_PAID_DT",
    "CLCL_ID_ADJ_TO",
    "CLCL_ID_ADJ_FROM"
)

# Read hashed file "clm_nasco_dup_bypass" (Scenario C) -> parquet
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
df_clm_nasco_dup_bypass = df_clm_nasco_dup_bypass.select(
    "CLM_ID"
)

# ODBCConnector stage: CMC_CLVC_VAL_CODE
extract_query = (
    f"SELECT CLCL_ID,CLVC_NUMBER,CLVC_LETTER,MEME_CK,CLVC_CODE,CLVC_AMT,CLVC_VALUE,CLVC_LOCK_TOKEN,ATXR_SOURCE_ID "
    f"FROM {FacetsOwner}.CMC_CLVC_VAL_CODE A "
    f"INNER JOIN tempdb..{DriverTable} B ON A.CLCL_ID = B.CLM_ID "
    f"ORDER BY B.CLM_ID,A.CLVC_NUMBER,A.CLVC_LETTER"
)
jdbc_url, jdbc_props = get_db_config(facets_secret_name)
df_CMC_CLVC_VAL_CODE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)
df_CMC_CLVC_VAL_CODE = df_CMC_CLVC_VAL_CODE.select(
    "CLCL_ID",
    "CLVC_NUMBER",
    "CLVC_LETTER",
    "MEME_CK",
    "CLVC_CODE",
    "CLVC_AMT",
    "CLVC_VALUE",
    "CLVC_LOCK_TOKEN",
    "ATXR_SOURCE_ID"
)

# Transformer stage: StripField
df_Strip = (
    df_CMC_CLVC_VAL_CODE
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CLVC_NUMBER", strip_field(F.col("CLVC_NUMBER")))
    .withColumn("CLVC_LETTER", strip_field(F.col("CLVC_LETTER")))
    .withColumn("CLVC_CODE", strip_field(F.col("CLVC_CODE")))
    .select(
        "CLCL_ID",
        "CLVC_NUMBER",
        "CLVC_LETTER",
        "MEME_CK",
        "CLVC_CODE",
        "CLVC_AMT",
        "CLVC_VALUE",
        "CLVC_LOCK_TOKEN",
        "ATXR_SOURCE_ID"
    )
)

# Transformer stage: BusinessRules
df_BusinessRules_tmp = (
    df_Strip.alias("Strip")
    .join(
        df_hf_clm_fcts_reversals.alias("fcts_reversals"),
        F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
        how="left"
    )
    .join(
        df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
        F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
        how="left"
    )
)

df_BusinessRules_tmp = (
    df_BusinessRules_tmp
    .withColumn(
        "ClmId",
        F.when(
            F.col("Strip.CLCL_ID").isNull() | (F.length(trim(F.col("Strip.CLCL_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(trim(F.col("Strip.CLCL_ID")))
    )
    .withColumn("PassThru", F.lit("Y"))
)

w = Window.partitionBy("ClmId").orderBy(F.col("Strip.CLVC_NUMBER"), F.col("Strip.CLVC_LETTER"))
df_BusinessRules_tmp = df_BusinessRules_tmp.withColumn("OrdNum", F.row_number().over(w) - 1)

df_FcltyValOut = df_BusinessRules_tmp.filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
df_FcltyValOut = df_FcltyValOut.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.col("ClmId"), F.lit(";"), F.col("OrdNum")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("FCLTY_CLM_VAL_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("ClmId").alias("CLM_ID"),
    F.col("OrdNum").alias("FCLTY_CLM_VAL_ORD"),
    F.when(
        F.col("Strip.CLVC_NUMBER").isNull() | (F.length(trim(F.col("Strip.CLVC_NUMBER"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("Strip.CLVC_NUMBER"))).alias("CLVC_NUMBER"),
    F.when(
        F.col("Strip.CLVC_LETTER").isNull() | (F.length(trim(F.col("Strip.CLVC_LETTER"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("Strip.CLVC_LETTER"))).alias("CLVC_LETTER"),
    F.when(
        F.col("Strip.CLVC_CODE").isNull() | (F.length(trim(F.col("Strip.CLVC_CODE"))) == 0),
        F.lit("NA")
    ).otherwise(F.upper(trim(F.col("Strip.CLVC_CODE")))).alias("FCLTY_CLM_VAL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    F.when(
        F.col("Strip.CLVC_AMT").isNull()
        | (F.length(trim(F.col("Strip.CLVC_AMT"))) == 0)
        | (~F.col("Strip.CLVC_AMT").rlike("^-?\\d+(\\.\\d+)?$")),
        F.lit(0)
    ).otherwise(F.col("Strip.CLVC_AMT").cast("decimal(38,10)")).alias("VAL_AMT"),
    F.when(
        F.col("Strip.CLVC_VALUE").isNull()
        | (F.length(trim(F.col("Strip.CLVC_VALUE"))) == 0)
        | (~F.col("Strip.CLVC_VALUE").rlike("^-?\\d+(\\.\\d+)?$")),
        F.lit(0)
    ).otherwise(F.col("Strip.CLVC_VALUE").cast("decimal(38,10)")).alias("VAL_UNIT_CT")
)

df_reversals = df_BusinessRules_tmp.filter(
    (F.col("fcts_reversals.CLCL_ID").isNotNull())
    & (
        (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
        | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
        | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
)
df_reversals = df_reversals.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.col("PassThru").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.col("ClmId"), F.lit("R;"), F.col("OrdNum")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("FCLTY_CLM_VAL_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.concat(F.col("ClmId"), F.lit("R")).alias("CLM_ID"),
    F.col("OrdNum").alias("FCLTY_CLM_VAL_ORD"),
    F.when(
        F.col("Strip.CLVC_NUMBER").isNull() | (F.length(trim(F.col("Strip.CLVC_NUMBER"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("Strip.CLVC_NUMBER"))).alias("CLVC_NUMBER"),
    F.when(
        F.col("Strip.CLVC_LETTER").isNull() | (F.length(trim(F.col("Strip.CLVC_LETTER"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("Strip.CLVC_LETTER"))).alias("CLVC_LETTER"),
    F.when(
        F.col("Strip.CLVC_CODE").isNull() | (F.length(trim(F.col("Strip.CLVC_CODE"))) == 0),
        F.lit("NA")
    ).otherwise(F.upper(trim(F.col("Strip.CLVC_CODE")))).alias("FCLTY_CLM_VAL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("FCLTY_CLM_SK"),
    (
        -1
        * F.when(
            F.col("Strip.CLVC_AMT").isNull()
            | (F.length(trim(F.col("Strip.CLVC_AMT"))) == 0)
            | (~F.col("Strip.CLVC_AMT").rlike("^-?\\d+(\\.\\d+)?$")),
            F.lit(0)
        ).otherwise(F.col("Strip.CLVC_AMT").cast("decimal(38,10)"))
    ).alias("VAL_AMT"),
    F.when(
        F.col("Strip.CLVC_VALUE").isNull()
        | (F.length(trim(F.col("Strip.CLVC_VALUE"))) == 0)
        | (~F.col("Strip.CLVC_VALUE").rlike("^-?\\d+(\\.\\d+)?$")),
        F.lit(0)
    ).otherwise(F.col("Strip.CLVC_VALUE").cast("decimal(38,10)")).alias("VAL_UNIT_CT")
)

# Collector (Round-Robin)
df_Collector = df_reversals.unionByName(df_FcltyValOut)

# Snapshot stage
df_AllCol = (
    df_Collector
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
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
        "FCLTY_CLM_VAL_SK",
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        "CLM_ID",
        "FCLTY_CLM_VAL_ORD",
        "CLVC_NUMBER",
        "CLVC_LETTER",
        "FCLTY_CLM_VAL_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_CLM_SK",
        "VAL_AMT",
        "VAL_UNIT_CT"
    )
)
df_SnapShot = (
    df_Collector
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .select(
        F.col("CLM_ID"),
        F.col("FCLTY_CLM_VAL_ORD")
    )
)
df_Transform = (
    df_Collector
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .select(
        F.col("SRC_SYS_CD_SK"),
        F.col("CLM_ID"),
        F.col("FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_SEQ_NO")
    )
)

# Transformer stage
df_RowCount = df_SnapShot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("FCLTY_CLM_VAL_ORD").alias("FCLTY_CLM_VAL_SEQ_NO")
)

# B_FCLTY_CLM_VAL: write the file
# Before writing, apply rpad to char columns if needed
df_RowCountToWrite = df_RowCount.withColumn(
    "CLM_ID", F.rpad(F.col("CLM_ID"), 12, " ")
)
write_files(
    df_RowCountToWrite.select("SRC_SYS_CD_SK", "CLM_ID", "FCLTY_CLM_VAL_SEQ_NO"),
    f"{adls_path}/load/B_FCLTY_CLM_VAL.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# FcltyClmValPK container
params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_Key = FcltyClmValPK(df_AllCol, df_Transform, params)

# Before writing final result, apply rpad to char/varchar columns
df_Key_rpad = (
    df_Key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))
    .withColumn("CLVC_NUMBER", F.rpad(F.col("CLVC_NUMBER"), 2, " "))
    .withColumn("CLVC_LETTER", F.rpad(F.col("CLVC_LETTER"), 1, " "))
)

df_Key_rpad = df_Key_rpad.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "FCLTY_CLM_VAL_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "FCLTY_CLM_VAL_ORD",
    "CLVC_NUMBER",
    "CLVC_LETTER",
    "FCLTY_CLM_VAL_CD",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "FCLTY_CLM_SK",
    "VAL_AMT",
    "VAL_UNIT_CT"
)

write_files(
    df_Key_rpad,
    f"{adls_path}/key/FctsClmFcltyValExtr.FctsClmFcltyVal.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)