# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     FctsClmLnProcCdModExtr
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
# MAGIC Steph Goddard        04/01/2004-                                  Originally Programmed
# MAGIC Brent Leland            09/17/2004  -                                Removed temp. file between transform and collector.
# MAGIC Sharon Andrew       07/20/05                                        Corrected the building of the primary key for records. 
# MAGIC                                07/24/2005                                    Upper Case the ProcCodeModTx field
# MAGIC Kevin Soderlund      10/18/04                                       Added fields to extract for Facets 4.11
# MAGIC Suzanne Saylor        03/01/2006                                  Changed to combine extract, transform, pkey for sequencer 
# MAGIC BJ Luce                   03/20/2006                                   add hf_clm_nasco_dup_bypass, identifies claims that are nasco dups. If the claim is on the file, a row is not generated for it in IDS. However, an R row will be build for it if the status if '91'
# MAGIC 
# MAGIC Sanderw                 12/08/2006   Project 1576  -          Reversal logix added for new status codes 89 and  and 99
# MAGIC Oliver Nielsen           08/20/2007      Balancing             Added Snapshot File to Balancing                                                 devlIDS30                     Steph Goddard         8/30/07
# MAGIC      
# MAGIC Parik                        2008-08-08    3567(Primary Key)    Added Primary key process to the job                                            devlIDS                          Steph Goddard         08/13/2008
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5            Manasa Andru           2022-06-10

# MAGIC Pulling Facets Claim Procedure Code Modifier
# MAGIC Data for a specific Time Period
# MAGIC Hash file (hf_clm_ln_proc_cd_mod_allcol) cleared in calling program
# MAGIC This container is used in:
# MAGIC FctsClmLnProcCdModExtr
# MAGIC NascoClmLnProcCdModExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Strip Carriage Return, Line Feed, and  Tab
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC hash file built in ClmDriverBuild
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


# Widgets (parameters)
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
RunID = get_widget_value('RunID','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentDate = get_widget_value('CurrentDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnProcCdModPK
# COMMAND ----------

# Read hashed file "hf_clm_fcts_reversals" as parquet (Scenario C)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read hashed file "hf_clm_nasco_dup_bypass" as parquet (Scenario C)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ODBCConnector stage: CMC_CDML_CL_LINE
facets_jdbc_url, facets_jdbc_props = get_db_config(facets_secret_name)
extract_query = f"SELECT CL_LINE.CLCL_ID,CL_LINE.CDML_SEQ_NO,CL_LINE.IPCD_ID,CL_LINE.CDML_IPCD_MOD2,CL_LINE.CDML_IPCD_MOD3,CL_LINE.CDML_IPCD_MOD4 FROM {FacetsOwner}.CMC_CDML_CL_LINE CL_LINE INNER JOIN tempdb..{DriverTable} TMP ON TMP.CLM_ID = CL_LINE.CLCL_ID"
df_CMC_CDML_CL_LINE = (
    spark.read.format("jdbc")
    .option("url", facets_jdbc_url)
    .options(**facets_jdbc_props)
    .option("query", extract_query)
    .load()
)

# TrnsStripField transformer
df_TrnsStripField = (
    df_CMC_CDML_CL_LINE
    .withColumn("CLCL_ID", F.regexp_replace(F.col("CLCL_ID"), "[\r\n\t]", ""))
    .withColumn("CLCL_ID", trim(F.col("CLCL_ID")))
    .withColumn("CDML_SEQ_NO", F.col("CDML_SEQ_NO"))
    .withColumn("EXT_TIMESTAMP", current_date())
    .withColumn("IPCD_ID", F.regexp_replace(F.col("IPCD_ID"), "[\r\n\t]", ""))
    .withColumn("IPCD_ID", trim(F.col("IPCD_ID")))
    .withColumn("CDML_IPCD_MOD2", F.regexp_replace(F.col("CDML_IPCD_MOD2"), "[\r\n\t]", ""))
    .withColumn("CDML_IPCD_MOD2", trim(F.col("CDML_IPCD_MOD2")))
    .withColumn("CDML_IPCD_MOD3", F.regexp_replace(F.col("CDML_IPCD_MOD3"), "[\r\n\t]", ""))
    .withColumn("CDML_IPCD_MOD3", trim(F.col("CDML_IPCD_MOD3")))
    .withColumn("CDML_IPCD_MOD4", F.regexp_replace(F.col("CDML_IPCD_MOD4"), "[\r\n\t]", ""))
    .withColumn("CDML_IPCD_MOD4", trim(F.col("CDML_IPCD_MOD4")))
)

# BusinessRules transformer: join with hashed-file lookups
df_BusinessRules_pre = (
    df_TrnsStripField.alias("Strip")
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"),
          F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"),
          how="left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"),
          F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"),
          how="left")
)

# Add stage variables
df_BusinessRules = (
    df_BusinessRules_pre
    # svMod1
    .withColumn(
        "svMod1",
        F.when(
            F.col("Strip.IPCD_ID").isNull() | (F.length(trim(F.col("Strip.IPCD_ID"))) == 0),
            ""
        ).otherwise(F.upper(trim(F.col("Strip.IPCD_ID"))))
    )
    # svMod2
    .withColumn(
        "svMod2",
        F.when(
            F.col("Strip.CDML_IPCD_MOD2").isNull() | (F.length(trim(F.col("Strip.CDML_IPCD_MOD2"))) == 0),
            ""
        ).otherwise(F.upper(trim(F.col("Strip.CDML_IPCD_MOD2"))).substr(F.lit(2), F.lit(1)))
    )
    # svMod3
    .withColumn(
        "svMod3",
        F.when(
            F.col("Strip.CDML_IPCD_MOD3").isNull() | (F.length(trim(F.col("Strip.CDML_IPCD_MOD3"))) == 0),
            ""
        ).otherwise(F.upper(trim(F.col("Strip.CDML_IPCD_MOD3"))).substr(F.lit(2), F.lit(1)))
    )
    # svMod4
    .withColumn(
        "svMod4",
        F.when(
            F.col("Strip.CDML_IPCD_MOD4").isNull() | (F.length(trim(F.col("Strip.CDML_IPCD_MOD4"))) == 0),
            ""
        ).otherwise(F.upper(trim(F.col("Strip.CDML_IPCD_MOD4"))).substr(F.lit(2), F.lit(1)))
    )
    # svOrdnl1
    .withColumn(
        "svOrdnl1",
        F.when(F.length(F.col("svMod1")) == 7, "1").otherwise("")
    )
    # svOrdnl2
    .withColumn(
        "svOrdnl2",
        F.when(F.col("svMod2") != "", "2").otherwise("")
    )
    # svOrdnl3
    .withColumn(
        "svOrdnl3",
        F.when(F.col("svMod3") != "", "3").otherwise("")
    )
    # svOrdnl4
    .withColumn(
        "svOrdnl4",
        F.when(F.col("svMod4") != "", "4").otherwise("")
    )
    # ClmId
    .withColumn(
        "ClmId",
        trim(F.col("Strip.CLCL_ID"))
    )
)

# Build the eight output links from BusinessRules, then union them in Collector

# 1) ProcCd1
df_ProcCd1 = df_BusinessRules.filter(
    (F.length(F.col("svMod1")) == 7) &
    (F.col("svMod1") != "") &
    (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("svOrdnl1")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnl1").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMod1").substr(F.lit(2), F.lit(1)).alias("PROC_CD_MOD_CD")
)

# 2) ProcCd2
df_ProcCd2 = df_BusinessRules.filter(
    (F.col("svMod2") != "") &
    (F.col("svMod2") != "") &
    (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("svOrdnl2")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnl2").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMod2").alias("PROC_CD_MOD_CD")
)

# 3) ProcCd3
df_ProcCd3 = df_BusinessRules.filter(
    (F.col("svMod3") != "") &
    (F.col("svMod3") != "") &
    (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("svOrdnl3")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnl3").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMod3").alias("PROC_CD_MOD_CD")
)

# 4) ProcCd4
df_ProcCd4 = df_BusinessRules.filter(
    (F.col("svMod4") != "") &
    (F.col("svMod4") != "") &
    (F.col("nasco_dup_lkup.CLM_ID").isNull())
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit(";"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("svOrdnl4")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.col("ClmId"), 18, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnl4").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMod4").alias("PROC_CD_MOD_CD")
)

# 5) reversals_ProcCd1
df_ReversalProcCd1 = df_BusinessRules.filter(
    (F.length(F.col("svMod1")) == 7) &
    (F.col("svMod1") != "") &
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
       (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
       (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
       (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("svOrdnl1")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.concat(F.col("ClmId"), F.lit("R")), 18, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnl1").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMod1").substr(F.lit(2), F.lit(1)).alias("PROC_CD_MOD_CD")
)

# 6) reversals_ProcCd2
df_ReversalProcCd2 = df_BusinessRules.filter(
    (F.col("svMod2") != "") &
    (F.col("svMod2") != "") &
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
       (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
       (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
       (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("svOrdnl2")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.concat(F.col("ClmId"), F.lit("R")), 18, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnl2").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMod2").alias("PROC_CD_MOD_CD")
)

# 7) reversals_ProcCd3
df_ReversalProcCd3 = df_BusinessRules.filter(
    (F.col("svMod3") != "") &
    (F.col("svMod3") != "") &
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
       (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
       (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
       (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("svOrdnl3")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.concat(F.col("ClmId"), F.lit("R")), 18, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnl3").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMod3").alias("PROC_CD_MOD_CD")
)

# 8) reversals_ProcCd4
df_ReversalProcCd4 = df_BusinessRules.filter(
    (F.col("svMod4") != "") &
    (F.col("svMod4") != "") &
    (F.col("fcts_reversals.CLCL_ID").isNotNull()) &
    (
       (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
       (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
       (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
    )
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),
    F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("ClmId"), F.lit("R;"), F.col("Strip.CDML_SEQ_NO"), F.lit(";"), F.col("svOrdnl4")).alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_LN_PROC_CD_MOD_SK"),
    F.rpad(F.concat(F.col("ClmId"), F.lit("R")), 18, " ").alias("CLM_ID"),
    F.col("Strip.CDML_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("svOrdnl4").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("svMod4").alias("PROC_CD_MOD_CD")
)

# Collector (union all 8)
df_Collector = (
    df_ProcCd1.unionByName(df_ProcCd2)
    .unionByName(df_ProcCd3)
    .unionByName(df_ProcCd4)
    .unionByName(df_ReversalProcCd1)
    .unionByName(df_ReversalProcCd2)
    .unionByName(df_ReversalProcCd3)
    .unionByName(df_ReversalProcCd4)
)

# SnapShot transformer => 3 outputs: AllCol, Snapshot, Transform
df_SnapShot_AllCol = df_Collector.select(
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

df_SnapShot_Snapshot = df_Collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

df_SnapShot_Transform = df_Collector.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD").alias("CLM_LN_PROC_CD_MOD_ORDNL_CD")
)

# Next Transformer stage: input = Snapshot => define "svOrdnlCdSk" with GetFkeyCodes
# (Assume GetFkeyCodes is a predefined function in Python)
df_Transformer = (
    df_SnapShot_Snapshot
    .withColumn(
        "CLM_LN_PROC_CD_MOD_ORDNL_CD_SK",
        GetFkeyCodes(
            SrcSysCd,
            F.lit(0),
            F.lit("PROCEDURE ORDINAL"),
            F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
            F.lit("X")
        )
    )
)

# Output => RowCount => "B_CLM_LN_PROC_CD_MOD"
df_RowCount = df_Transformer.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD_SK")
)

# Write B_CLM_LN_PROC_CD_MOD.FACETS.dat.#RunID# as .dat
write_files(
    df_RowCount,
    f"{adls_path}/load/B_CLM_LN_PROC_CD_MOD.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Shared Container: ClmLnProcCdModPK
params_ClmLnProcCdModPK = {
    "IDSOwner": IDSOwner,
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd
}
df_ClmLnProcCdModPK_Key = ClmLnProcCdModPK(df_SnapShot_Transform, df_SnapShot_AllCol, params_ClmLnProcCdModPK)

# Final output stage FctsClmLnProcModExtr => write .dat
df_FctsClmLnProcModExtr = df_ClmLnProcCdModPK_Key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_PROC_CD_MOD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_PROC_CD_MOD_ORDNL_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("PROC_CD_MOD_CD")
)

# Re-pad final char columns as needed
df_FctsClmLnProcModExtr = (
    df_FctsClmLnProcModExtr
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 18, " "))
)

write_files(
    df_FctsClmLnProcModExtr,
    f"{adls_path}/key/FctsClmLnProcCdModExtr.FctsClmLnProcCdMod.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)