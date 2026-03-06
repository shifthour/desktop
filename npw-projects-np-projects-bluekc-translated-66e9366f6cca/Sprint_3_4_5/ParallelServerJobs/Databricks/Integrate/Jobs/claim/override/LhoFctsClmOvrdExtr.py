# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC JOB NAME:  LhoFctsClmOvrdExtr
# MAGIC CALLED BY:  LhoFctsStgClmExtr1Seq
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Output file is created with a temp. name.  After-Job Subroutine renames file after successful run.
# MAGIC   
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Steph Goddard        06/01/2004-                                   Originally Programmed
# MAGIC Steph Goddard        07/22/2004                                    added NullOptCode to user id field to force NA if spaces
# MAGIC SAndrew                  07/23/2004                                   added NullOptCodeNoCase to user id field to force NA if spaces
# MAGIC Steph Goddard        02/16/2006                                   Combined extract, transform, primary key for sequencer
# MAGIC BJ Luce                   03/20/2006                                   add hf_clm_nasco_dup_bypass, identifies claims that are nasco
# MAGIC                                                                                       dups. If the claim is on the file, a row is not generated for it in 
# MAGIC                                                                                        IDS. However, an R row will be build for it if the status if '91'
# MAGIC Sanderw                  12/08/2006   Project 1756            Reversal logix added for new status codes 89 and  and 99
# MAGIC Brent Leland            05/02/2007    IAD Prod. Supp.     Added current timestamp parameter to eliminate FORMAT.DATE
# MAGIC                                                                                       routine call to improve effeciency.
# MAGIC O. Nielsen                07/11/2008                                   New Primary Keying logic                                                             devlIDS                          Steph Goddard            07/17/2008
# MAGIC Christen Marshall      08/06/2020                                 Duplicated to load LHO facets stage data and updated to            IntegrateDev2
# MAGIC                                                                                       current development standards
# MAGIC Prabhu ES               2022-03-29     S2S                         MSSQL ODBC conn params added                                             IntegrateDev5	Ken Bradmon	2022-06-11

# MAGIC Facets Claim Override
# MAGIC hash file built in LhoFctsClmDriverBuild
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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmOvrdPK
# COMMAND ----------

# Retrieve parameters
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
CurrDateTime = get_widget_value('CurrDateTime','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

# Read hashed file "hf_clm_fcts_reversals" as parquet (Scenario C)
df_hf_clm_fcts_reversals = spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")

# Read hashed file "hf_clm_nasco_dup_bypass" as parquet (Scenario C)
df_clm_nasco_dup_bypass = spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")

# ODBCConnector (CMC_CLOR_CL_OVR)
jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)
extract_query = (
    f"SELECT CLCL_ID,CLOR_OR_ID,MEME_CK,CLOR_OR_AMOUNT,CLOR_OR_VALUE,CLOR_OR_DT,EXCD_ID,CLOR_OR_USID,"
    f"CLOR_AUTO_GEN,CLOR_LOCK_TOKEN FROM {LhoFacetsStgOwner}.CMC_CLOR_CL_OVR A, tempdb..{DriverTable}  TMP "
    f"WHERE TMP.CLM_ID = A.CLCL_ID and A.CLOR_OR_ID <> 'XP'"
)
df_CMC_CLOR_CL_OVR = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# StripField Transformer
df_stripfield = (
    df_CMC_CLOR_CL_OVR
    .withColumn("CLCL_ID", strip_field(F.col("CLCL_ID")))
    .withColumn("CLOR_OR_ID", strip_field(F.col("CLOR_OR_ID")))
    .withColumn("EXT_TIMESTAMP", F.lit(CurrDateTime))
    .withColumn("MEME_CK", F.col("MEME_CK"))
    .withColumn("CLOR_OR_AMOUNT", F.col("CLOR_OR_AMOUNT"))
    .withColumn("CLOR_OR_VALUE", strip_field(F.col("CLOR_OR_VALUE")))
    .withColumn("CLOR_OR_DT", F.col("CLOR_OR_DT"))
    .withColumn("EXCD_ID", strip_field(F.col("EXCD_ID")))
    .withColumn("CLOR_OR_USID", strip_field(F.col("CLOR_OR_USID")))
    .withColumn("CLOR_AUTO_GEN", strip_field(F.col("CLOR_AUTO_GEN")))
    .withColumn("CLOR_LOCK_TOKEN", F.col("CLOR_LOCK_TOKEN"))
)

# BusinessRules Transformer: primary link = df_stripfield, lookups = df_hf_clm_fcts_reversals (left), df_clm_nasco_dup_bypass (left)
df_businessrules = (
    df_stripfield.alias("Strip")
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

# Output link "ClmOvrd" constraint: nasco_dup_lkup.CLM_ID is null
df_ClmOvrd = (
    df_businessrules
    .filter(F.col("nasco_dup_lkup.CLM_ID").isNull())
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd), F.lit(";"),
            F.trim(F.col("Strip.CLCL_ID")), F.lit(";"),
            F.trim(F.col("Strip.CLOR_OR_ID"))
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_OVRD_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.trim(F.col("Strip.CLCL_ID")).alias("CLM_ID"),
        F.trim(F.col("Strip.CLOR_OR_ID")).alias("CLM_OVRD_ID"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.when(
            F.col("Strip.CLOR_OR_USID").isNull() | (F.length(F.trim(F.col("Strip.CLOR_OR_USID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.CLOR_OR_USID"))).alias("USER_ID"),
        F.when(
            (F.length(F.trim(F.col("Strip.EXCD_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.EXCD_ID"))).alias("CLM_OVRD_EXCD"),
        F.trim(F.substring(F.col("Strip.CLOR_OR_DT"), 1, 10)).alias("OVERRIDE_DT"),
        F.col("Strip.CLOR_OR_AMOUNT").alias("OVERRIDE_AMT"),
        F.col("Strip.CLOR_OR_VALUE").alias("OVERRIDE_VAL_DESC")
    )
)

# Output link "reversals" constraint:
# IsNull(fcts_reversals.CLCL_ID) = false and fcts_reversals.CLCL_CUR_STS in ('89','91','99')
df_reversals = (
    df_businessrules
    .filter(
        F.col("fcts_reversals.CLCL_ID").isNotNull() &
        (
            (F.col("fcts_reversals.CLCL_CUR_STS") == "89") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "91") |
            (F.col("fcts_reversals.CLCL_CUR_STS") == "99")
        )
    )
    .select(
        F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.lit("I").alias("INSRT_UPDT_CD"),
        F.lit("N").alias("DISCARD_IN"),
        F.lit("Y").alias("PASS_THRU_IN"),
        F.col("Strip.EXT_TIMESTAMP").alias("FIRST_RECYC_DT"),
        F.lit(0).alias("ERR_CT"),
        F.lit(0).alias("RECYCLE_CT"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.concat(
            F.lit(SrcSysCd), F.lit(";"),
            F.trim(F.col("Strip.CLCL_ID")), F.lit("R;"),
            F.trim(F.col("Strip.CLOR_OR_ID"))
        ).alias("PRI_KEY_STRING"),
        F.lit(0).alias("CLM_OVRD_SK"),
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.concat(F.trim(F.col("Strip.CLCL_ID")), F.lit("R")).alias("CLM_ID"),
        F.trim(F.col("Strip.CLOR_OR_ID")).alias("CLM_OVRD_ID"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("CLM_SK"),
        F.when(
            F.col("Strip.CLOR_OR_USID").isNull() | (F.length(F.trim(F.col("Strip.CLOR_OR_USID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.CLOR_OR_USID"))).alias("USER_ID"),
        F.when(
            (F.length(F.trim(F.col("Strip.EXCD_ID"))) == 0),
            F.lit("NA")
        ).otherwise(F.trim(F.col("Strip.EXCD_ID"))).alias("CLM_OVRD_EXCD"),
        F.trim(F.substring(F.col("Strip.CLOR_OR_DT"), 1, 10)).alias("OVERRIDE_DT"),
        Neg(F.col("Strip.CLOR_OR_AMOUNT")).alias("OVERRIDE_AMT"),
        F.col("Strip.CLOR_OR_VALUE").alias("OVERRIDE_VAL_DESC")
    )
)

# Collector stage: union of df_ClmOvrd and df_reversals
df_collector = df_ClmOvrd.unionByName(df_reversals)

# Snapshot stage has three output links. All come from df_collector.
# 1) AllColl
df_SnapshotAllColl = df_collector.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_OVRD_ID").alias("CLM_OVRD_ID"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("USER_ID").alias("USER_ID"),
    F.col("CLM_OVRD_EXCD").alias("CLM_OVRD_EXCD"),
    F.col("OVERRIDE_DT").alias("OVERRIDE_DT"),
    F.col("OVERRIDE_AMT").alias("OVERRIDE_AMT"),
    F.col("OVERRIDE_VAL_DESC").alias("OVERRIDE_VAL_DESC")
)

# 2) Transform
df_SnapshotTransform = df_collector.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_OVRD_ID").alias("CLM_OVRD_ID")
)

# 3) RowCount
df_SnapshotRowCount = df_collector.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_OVRD_ID").alias("CLM_OVRD_ID")
)

# B_CLM_OVRD: write df_SnapshotRowCount to a .dat file
# Columns could be char for CLM_ID(18) and CLM_OVRD_ID(2) based on job definitions
df_b_clm_ovrd = (
    df_SnapshotRowCount
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("CLM_OVRD_ID", rpad(F.col("CLM_OVRD_ID"), 2, " "))
    .select("SRC_SYS_CD_SK", "CLM_ID", "CLM_OVRD_ID")
)
write_files(
    df_b_clm_ovrd,
    f"{adls_path}/load/B_CLM_OVRD.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Call the shared container ClmOvrdPK
params = {
    "DriverTable": DriverTable,
    "CurrRunCycle": CurrRunCycle,
    "RunID": RunID,
    "CurrDateTime": CurrDateTime,
    "FacetsDB": get_widget_value('FacetsDB',''),
    "FacetsOwner": get_widget_value('FacetsOwner',''),
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner,
    "SrcSysCdSk": SrcSysCdSk
}
df_ClmOvrdPK_Key = ClmOvrdPK(df_SnapshotAllColl, df_SnapshotTransform, params)

# Final stage: LhoFctsClmOvrdExtr => write the output
df_final_ClmOvrdPK_Key = (
    df_ClmOvrdPK_Key
    .withColumn("INSRT_UPDT_CD", rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLM_ID", rpad(F.col("CLM_ID"), 18, " "))
    .withColumn("CLM_OVRD_EXCD", rpad(F.col("CLM_OVRD_EXCD"), 10, " "))
    .withColumn("OVERRIDE_DT", rpad(F.col("OVERRIDE_DT"), 10, " "))
    .withColumn("OVERRIDE_VAL_DESC", rpad(F.col("OVERRIDE_VAL_DESC"), 10, " "))
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
        "CLM_OVRD_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_OVRD_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "USER_ID",
        "CLM_OVRD_EXCD",
        "OVERRIDE_DT",
        "OVERRIDE_AMT",
        "OVERRIDE_VAL_DESC"
    )
)
write_files(
    df_final_ClmOvrdPK_Key,
    f"{adls_path}/key/LhoFctsClmOvrdExtr.LhoFctsClmOvrd.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)