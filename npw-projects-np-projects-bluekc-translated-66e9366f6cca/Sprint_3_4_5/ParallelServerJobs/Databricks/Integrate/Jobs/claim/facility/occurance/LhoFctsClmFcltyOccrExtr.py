# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     LhoFctsClmFcltyOccrExtr
# MAGIC 
# MAGIC Called By:        LhoFctsClmOnDmdExtr1Seq
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
# MAGIC 
# MAGIC Reddy Sanam         2020-08-12    263445                       Copied from Facets job for LhoFacets Source                           IntegrateDev2                 Jeyaprasanna            10-22-2020
# MAGIC                                                                                          Chanaged the source query to extract from LhoFacetsStg
# MAGIC                                                                                          database
# MAGIC                                                                                          Changed target sequential file name to include LhoFcts
# MAGIC                                                                                          in the name
# MAGIC Prabhu ES               2022-03-29       S2S                         MSSQL ODBC conn params added                                          IntegrateDev5	Ken Bradmon	2022-06-11

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
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDate = get_widget_value('CurrentDate','')
IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
LhoFacetsStgOwner = get_widget_value('$LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgAcct = get_widget_value('$LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('$LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('$LhoFacetsStgDSN','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet").select(
    F.col("CLCL_ID"),
    F.col("CLCL_CUR_STS"),
    F.col("CLCL_PAID_DT"),
    F.col("CLCL_ID_ADJ_TO"),
    F.col("CLCL_ID_ADJ_FROM")
).alias("fcts_reversals")

df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet").select(
    F.col("CLM_ID")
).alias("nasco_dup_lkup")

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = (
    "SELECT \n"
    "CLCL_ID,CLHO_SEQ_NO,\n"
    "MEME_CK,\n"
    "CLHO_OCC_CODE,\n"
    "CLHO_OCC_FROM_DT,\n"
    "CLHO_OCC_TO_DT,\n"
    "CLHO_LOCK_TOKEN \n"
    f"FROM #${LhoFacetsStgOwner}.CMC_CLHO_OCC_CODE A\n"
    f"    INNER JOIN tempdb..#{DriverTable}  B ON  A.CLCL_ID = B.CLM_ID"
)
df_CMC_CLHO_OCC_CODE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_Strip = df_CMC_CLHO_OCC_CODE.select(
    strip_field("CLCL_ID").alias("CLCL_ID"),
    F.col("CLHO_SEQ_NO").alias("CLHO_SEQ_NO"),
    F.col("MEME_CK").alias("MEME_CK"),
    strip_field("CLHO_OCC_CODE").alias("CLHO_OCC_CODE"),
    strip_field("CLHO_OCC_FROM_DT").alias("CLHO_OCC_FROM_DT"),
    strip_field("CLHO_OCC_TO_DT").alias("CLHO_OCC_TO_DT"),
    F.col("CLHO_LOCK_TOKEN").alias("CLHO_LOCK_TOKEN")
).alias("Strip")

df_BusinessRules_input = (
    df_Strip
    .join(df_hf_clm_fcts_reversals, F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass, F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"), "left")
)

df_BusinessRules = (
    df_BusinessRules_input
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
            (F.col("Strip.CLHO_SEQ_NO").isNull()) | (F.length(trim(F.col("Strip.CLHO_SEQ_NO"))) == 0),
            F.lit(0)
        )
        .when(~F.col("Strip.CLHO_SEQ_NO").rlike("^[0-9]+$"), F.lit(0))
        .otherwise(trim(F.col("Strip.CLHO_SEQ_NO")))
    )
    .withColumn("PassThru", F.lit("Y"))
)

df_FcltyOccrOut = (
    df_BusinessRules
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.col("PassThru").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat_ws(";", F.lit(SrcSysCd), F.col("ClmId"), F.col("SeqNum")).alias("PRI_KEY_STRING"),
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
        F.date_format(F.col("Strip.CLHO_OCC_TO_DT"), "yyyy-MM-dd").alias("TO_DT_SK")
    )
)

df_reversals = (
    df_BusinessRules
    .filter(
        (F.col("fcts_reversals.CLCL_ID").isNotNull())
        & (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "91")
            | (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.col("PassThru").alias("PASS_THRU_IN"),
        F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat_ws(";", F.lit(SrcSysCd), F.col("ClmId"), F.lit("R"), F.col("SeqNum")).alias("PRI_KEY_STRING"),
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
        F.date_format(F.col("Strip.CLHO_OCC_TO_DT"), "yyyy-MM-dd").alias("TO_DT_SK")
    )
)

df_Collector = (
    df_reversals.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "FCLTY_CLM_OCCUR_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "FCLTY_CLM_OCCUR_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_CLM_SK",
        "FCLTY_CLM_OCCUR_CD",
        "FROM_DT_SK",
        "TO_DT_SK"
    ).unionByName(
        df_FcltyOccrOut.select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            "FCLTY_CLM_OCCUR_SK",
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "FCLTY_CLM_OCCUR_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "FCLTY_CLM_SK",
            "FCLTY_CLM_OCCUR_CD",
            "FROM_DT_SK",
            "TO_DT_SK"
        )
    )
)

df_SnapShot_in = df_Collector

df_AllCol = df_SnapShot_in.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
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

df_SnapShot = df_SnapShot_in.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO")
)

df_Transform = df_SnapShot_in.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO")
)

df_Transformer_in = df_SnapShot
df_RowCount = df_Transformer_in.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("FCLTY_CLM_OCCUR_SEQ_NO").alias("FCLTY_CLM_OCCUR_SEQ_NO")
).withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))  # Aligning with stage output

write_files(
    df_RowCount.select("SRC_SYS_CD_SK","CLM_ID","FCLTY_CLM_OCCUR_SEQ_NO"),
    f"{adls_path}/load/B_FCLTY_CLM_OCCUR.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmOccurPK
# COMMAND ----------

params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_Key = FcltyClmOccurPK(df_AllCol, df_Transform, params)

df_Key_final = df_Key.select(
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
    df_Key_final,
    f"{adls_path}/key/LhoFctsClmFcltyOccrExtr.LhoFctsClmFcltyOccr.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)