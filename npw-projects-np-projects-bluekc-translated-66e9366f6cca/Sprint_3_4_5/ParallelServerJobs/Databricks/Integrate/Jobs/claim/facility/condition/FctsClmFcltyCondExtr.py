# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_6 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_5 02/02/09 09:52:49 Batch  15009_35578 PROMOTE bckcetl ids20 dsadm bls for brent
# MAGIC ^1_5 01/26/09 13:03:15 Batch  15002_46999 INIT bckcett testIDS dsadm bls for bl
# MAGIC ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
# MAGIC ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_4 10/30/07 12:32:42 Batch  14548_45167 INIT bckcetl ids20 dsadm bls for sa
# MAGIC ^1_3 10/02/07 14:11:36 Batch  14520_51104 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_3 10/02/07 13:36:29 Batch  14520_48996 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 09/30/07 15:50:06 Batch  14518_57011 PROMOTE bckcett testIDS30 u03651 staeffy
# MAGIC ^1_2 09/30/07 15:29:43 Batch  14518_55788 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 09/29/07 17:49:00 Batch  14517_64144 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 09/25/07 12:58:00 Batch  14513_46683 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 03/07/07 15:56:27 Batch  14311_57391 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 03/07/07 15:24:05 Batch  14311_55447 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_8 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006
# MAGIC ^1_8 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006
# MAGIC ^1_1 12/11/06 16:27:40 Batch  14225_59263 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_1 12/11/06 16:16:52 Batch  14225_58615 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 08/07/06 07:51:34 Batch  14099_28298 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_3 04/13/06 07:36:31 Batch  13983_27397 INIT bckcett devlIDS30 u08717 Brent
# MAGIC ^1_4 04/06/06 15:22:37 Batch  13976_55361 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 04/02/06 15:43:48 Batch  13972_56632 INIT bckcett devlIDS30 dcg01 steffy
# MAGIC ^1_4 03/29/06 15:32:39 Batch  13968_55966 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_3 03/29/06 14:54:04 Batch  13968_53650 INIT bckcett devlIDS30 dsadm Brent
# MAGIC ^1_2 03/20/06 13:57:04 Batch  13959_50244 INIT bckcett devlIDS30 dsadm Brent
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmFcltyCondExtr
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
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Oliver Nielsen          08/15/2007                                    Added Snapshot extract for balancing                                          devlIDS30                     Steph Goddard           8/30/07
# MAGIC 
# MAGIC Bhoomi Dasari         2008-07-25      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                         Steph Goddard           07/29/2008 
# MAGIC Prabhu ES               2022-02-26     S2S Remediation     MSSQL connection parameters added                                         IntegrateDev5

# MAGIC Read from FACETS
# MAGIC Writing Sequential File to ../key
# MAGIC Hash file (hf_fclty_cond_allcol) cleared from the shared container FcltyClmCondPK
# MAGIC Strip un-printable chars
# MAGIC Apply the defined business rules
# MAGIC This container is used in:
# MAGIC FctsClmFcltyCondExtr
# MAGIC NascoClmFcltyCondTrns
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
from pyspark.sql.functions import col, lit, when, length, concat, upper, rpad
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/FcltyClmCondPK
# COMMAND ----------

DriverTable = get_widget_value('DriverTable','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrentDateParam = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = (
    f"SELECT CLCL_ID, CLHC_SEQ_NO, MEME_CK, CLHC_COND_CD, CLHC_LOCK_TOKEN "
    f"FROM {FacetsOwner}.CMC_CLHC_COND_CODE A "
    f"INNER JOIN tempdb..{DriverTable} B ON A.CLCL_ID = B.CLM_ID"
)
df_CMC_CLHC_COND_CODE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_StripField = (
    df_CMC_CLHC_COND_CODE
    .withColumn("CLCL_ID", strip_field(col("CLCL_ID")))
    .withColumn("CLHC_COND_CD", strip_field(col("CLHC_COND_CD")))
)

df_joined = (
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
)

df_crunch = (
    df_joined
    .withColumn("RowPassThru", lit("Y"))
    .withColumn("ClmId", trim(col("Strip.CLCL_ID")))
)

df_ClmFcltyCond = (
    df_crunch
    .filter(col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        lit("I").alias("INSRT_UPDT_CD"),
        lit("N").alias("DISCARD_IN"),
        col("RowPassThru").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit("FACETS").alias("SRC_SYS_CD"),
        concat(lit("FACETS"), lit(";"), col("ClmId"), lit(";"), col("Strip.CLHC_SEQ_NO")).alias("PRI_KEY_STRING"),
        lit(0).alias("FCLTY_CLM_COND_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        col("ClmId").alias("CLM_ID"),
        col("Strip.CLHC_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("FCLTY_CLM_SK"),
        when(
            col("Strip.CLHC_COND_CD").isNull() | (length(trim(col("Strip.CLHC_COND_CD"))) == 0),
            "NA"
        )
        .otherwise(upper(trim(col("Strip.CLHC_COND_CD"))))
        .alias("FCLTY_CLM_COND_CD")
    )
)

df_reversals = (
    df_crunch
    .filter(
        col("fcts_reversals.CLCL_ID").isNotNull() &
        col("fcts_reversals.CLCL_CUR_STS").isin("89", "91", "99")
    )
    .select(
        lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        lit("I").alias("INSRT_UPDT_CD"),
        lit("N").alias("DISCARD_IN"),
        col("RowPassThru").alias("PASS_THRU_IN"),
        current_date().alias("FIRST_RECYC_DT"),
        lit(0).alias("ERR_CT"),
        lit(0).alias("RECYCLE_CT"),
        lit("FACETS").alias("SRC_SYS_CD"),
        concat(lit("FACETS"), lit(";"), col("ClmId"), lit("R;"), col("Strip.CLHC_SEQ_NO")).alias("PRI_KEY_STRING"),
        lit(0).alias("FCLTY_CLM_COND_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        concat(col("ClmId"), lit("R")).alias("CLM_ID"),
        trim(col("Strip.CLHC_SEQ_NO")).alias("FCLTY_CLM_COND_SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("FCLTY_CLM_SK"),
        when(
            col("Strip.CLHC_COND_CD").isNull() | (length(trim(col("Strip.CLHC_COND_CD"))) == 0),
            "NA"
        )
        .otherwise(upper(trim(col("Strip.CLHC_COND_CD"))))
        .alias("FCLTY_CLM_COND_CD")
    )
)

df_collector = df_reversals.unionByName(df_ClmFcltyCond).select(
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

df_AllCol = df_collector.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO"),
    col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("FCLTY_CLM_COND_SK").alias("FCLTY_CLM_COND_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
    col("FCLTY_CLM_COND_CD").alias("FCLTY_CLM_COND_CD")
)

df_SnapShot = df_collector.select(
    col("CLM_ID").alias("CLM_ID"),
    col("FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO")
)

df_Transform = df_collector.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO")
)

df_Transformer = df_SnapShot.select(
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO")
)

write_files(
    df_Transformer,
    f"{adls_path}/load/B_FCLTY_CLM_COND.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)

container_params = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "RunID": RunID,
    "IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_Key = FcltyClmCondPK(df_AllCol, df_Transform, container_params)

df_Key_padded = (
    df_Key
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", rpad(col("CLM_ID"), 12, " "))
    .withColumn("FCLTY_CLM_COND_CD", rpad(col("FCLTY_CLM_COND_CD"), 2, " "))
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
        "FCLTY_CLM_COND_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "FCLTY_CLM_COND_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "FCLTY_CLM_SK",
        "FCLTY_CLM_COND_CD"
    )
)

write_files(
    df_Key_padded,
    f"{adls_path}/key/FctsClmFcltyCondExtr.FctsClmFcltyCond.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote="\"",
    nullValue=None
)