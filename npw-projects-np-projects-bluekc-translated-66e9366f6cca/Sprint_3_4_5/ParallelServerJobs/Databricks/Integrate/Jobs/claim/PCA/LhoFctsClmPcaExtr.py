# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:  LhoFctsClmPcaExtr
# MAGIC CALLED BY:  LhoFctsClmOnDmdExtr1Seq
# MAGIC 
# MAGIC SELECT clm.CLCL_ID,
# MAGIC clm.CLHS_TOT_CONSIDER,
# MAGIC clm.CLHS_TOT_PAID,
# MAGIC clm.CLHS_SB_ALLOC_AMT,
# MAGIC clm.SBHS_PAID_AMT,
# MAGIC clcl.CLCL_LOW_SVC_DT,
# MAGIC clcl.PDPD_ID
# MAGIC FROM 
# MAGIC #$FacetsOwner#.CMC_CLHS_HSA_CLAIM clm,
# MAGIC #$FacetsOwner#.CMC_CLCL_CLAIM clcl,
# MAGIC tempdb..#DriverTable#    DRVR
# MAGIC WHERE
# MAGIC clm.CLCL_ID = DRVR.CLM_ID
# MAGIC AND
# MAGIC clm.CLCL_ID=clcl.CLCL_ID
# MAGIC ;
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Bhoomi Dasari         2008-08-05      3567(Primary Key)    Changed primary key process from hash file to DB2 table            devlIDS                         Steph Goddard          08/14/2008
# MAGIC                                                                                         Added SQL join to driver table
# MAGIC 
# MAGIC Matt Newman          2020-08-10      MA                          Copied from original and changed to use LHO sources/names 
# MAGIC 
# MAGIC 
# MAGIC Harikanth Reddy    10/12/2020                                     Brought up to standards                                      IntegrateDev2
# MAGIC Kotha Venkat                                                                  
# MAGIC Prabhu ES               2022-03-29      S2S                        MSSQL ODBC conn params added                                              IntegrateDev5	Ken Bradmon	2022-06-10

# MAGIC Read from Facets
# MAGIC Strip fields
# MAGIC Business Logic
# MAGIC Reversal
# MAGIC IDS Claim service greater 1/1/2007 extract from Facets
# MAGIC If primary key found,assign surrogate key otherwise update hash file
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Data collection
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
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmPcaPK
# COMMAND ----------

LhoFacetsStgOwner = get_widget_value("LhoFacetsStgOwner","")
lhofacetsstg_secret_name = get_widget_value("lhofacetsstg_secret_name","")
DriverTable = get_widget_value("DriverTable","")
CurrRunCycle = get_widget_value("CurrRunCycle","")
RunID = get_widget_value("RunID","")
LhoFacetsStgPW = get_widget_value("LhoFacetsStgPW","")
LhoFacetsStgDSN = get_widget_value("LhoFacetsStgDSN","")
LhoFacetsStgAcct = get_widget_value("LhoFacetsStgAcct","")
Logging = get_widget_value("Logging","")
Runcycle = get_widget_value("Runcycle","")
TmpTblRunID = get_widget_value("TmpTblRunID","")
CurrentDateParam = get_widget_value("CurrentDate","")
SrcSysCdSk = get_widget_value("SrcSysCdSk","")
SrcSysCd = get_widget_value("SrcSysCd","")

jdbc_url, jdbc_props = get_db_config(lhofacetsstg_secret_name)

# Read hashed file "hf_clm_fcts_reversals" as parquet (Scenario C)
df_hf_clm_fcts_reversals = (
    spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
    .select("CLCL_ID", "CLCL_CUR_STS", "CLCL_PAID_DT", "CLCL_ID_ADJ_TO", "CLCL_ID_ADJ_FROM")
)

# Read hashed file "clm_nasco_dup_bypass" as parquet (Scenario C)
df_clm_nasco_dup_bypass = (
    spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
    .select("CLM_ID")
)

# ODBCConnector stage: CMC_CDHL_HSA_LINE
extract_query = f"""SELECT
clm.CLCL_ID,
clm.CLHS_TOT_CONSIDER,
clm.CLHS_TOT_PAID,
clm.CLHS_SB_ALLOC_AMT,
clm.SBHS_PAID_AMT,
clcl.CLCL_LOW_SVC_DT,
clcl.PDPD_ID
FROM {LhoFacetsStgOwner}.CMC_CLHS_HSA_CLAIM clm,
     {LhoFacetsStgOwner}.CMC_CLCL_CLAIM clcl,
     tempdb..{DriverTable} drvr
WHERE clm.CLCL_ID = clcl.CLCL_ID
  AND clm.CLCL_ID = drvr.CLM_ID
"""
df_CMC_CDHL_HSA_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

# StripFields transformer
df_Strip = (
    df_CMC_CDHL_HSA_LINE
    .withColumn("CLCL_ID", F.regexp_replace(F.col("CLCL_ID"), "[\r\n\t]", ""))
    .withColumn("PDPD_ID", F.regexp_replace(F.col("PDPD_ID"), "[\r\n\t]", ""))
    .select(
        F.col("CLCL_ID"),
        F.col("CLHS_TOT_CONSIDER"),
        F.col("CLHS_TOT_PAID"),
        F.col("CLHS_SB_ALLOC_AMT"),
        F.col("SBHS_PAID_AMT"),
        F.col("CLCL_LOW_SVC_DT"),
        F.col("PDPD_ID")
    )
)

# BusinessLogic transformer: join primary link (Strip) with lookup links (fcts_reversals, nasco_dup_lkup)
df_joined = (
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
)

# Constraint: Regular (IsNull(nasco_dup_lkup.CLM_ID) = @TRUE)
df_Regular = df_joined.filter(F.isnull(F.col("nasco_dup_lkup.CLM_ID")))

df_Regular_enriched = (
    df_Regular
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("ROW_PASS_THRU", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.trim(F.col("Strip.CLCL_ID")))
    )
    .withColumn("CLM_SK", F.col("Strip.CLCL_ID"))
    .withColumn(
        "CLM_ID",
        F.when(
            F.col("Strip.CLCL_ID").isNull()
            | (F.length(F.trim(F.col("Strip.CLCL_ID"))) == 0),
            F.lit("UNK")
        ).otherwise(F.trim(F.col("Strip.CLCL_ID")))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "TOT_CNSD_AMT",
        F.when(
            F.col("Strip.CLHS_TOT_CONSIDER").isNull()
            | (F.length(F.trim(F.col("Strip.CLHS_TOT_CONSIDER"))) == 0),
            F.lit("1")
        ).otherwise(F.trim(F.col("Strip.CLHS_TOT_CONSIDER")))
    )
    .withColumn(
        "TOT_PD_AMT",
        F.when(
            F.col("Strip.CLHS_TOT_PAID").isNull()
            | (F.length(F.trim(F.col("Strip.CLHS_TOT_PAID"))) == 0),
            F.lit("1")
        ).otherwise(F.trim(F.col("Strip.CLHS_TOT_PAID")))
    )
    .withColumn(
        "SUB_TOT_PCA_AVLBL_TO_DT_AMT",
        F.when(
            F.col("Strip.CLHS_SB_ALLOC_AMT").isNull()
            | (F.length(F.trim(F.col("Strip.CLHS_SB_ALLOC_AMT"))) == 0),
            F.lit("1")
        ).otherwise(F.trim(F.col("Strip.CLHS_SB_ALLOC_AMT")))
    )
    .withColumn(
        "SUB_TOT_PCA_PD_TO_DT_AMT",
        F.when(
            F.col("Strip.SBHS_PAID_AMT").isNull()
            | (F.length(F.trim(F.col("Strip.SBHS_PAID_AMT"))) == 0),
            F.lit("1")
        ).otherwise(F.trim(F.col("Strip.SBHS_PAID_AMT")))
    )
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("ROW_PASS_THRU"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CLM_SK"),
        F.col("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("TOT_CNSD_AMT"),
        F.col("TOT_PD_AMT"),
        F.col("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
        F.col("SUB_TOT_PCA_PD_TO_DT_AMT")
    )
)

# Constraint: Reversal ((IsNull(fcts_reversals.CLCL_ID) = @FALSE))
df_Reversal = df_joined.filter(~F.col("fcts_reversals.CLCL_ID").isNull())

df_Reversal_enriched = (
    df_Reversal
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("ROW_PASS_THRU", F.lit("Y"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("FIRST_RECYC_DT", current_date())
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("Strip.CLCL_ID"), F.lit("R"))
    )
    .withColumn("CLM_SK", F.col("Strip.CLCL_ID"))
    .withColumn(
        "CLM_ID",
        F.concat(F.trim(F.col("Strip.CLCL_ID")), F.lit("R"))
    )
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn(
        "TOT_CNSD_AMT",
        -1 * F.when(
            F.col("Strip.CLHS_TOT_CONSIDER").isNull()
            | (F.length(F.trim(F.col("Strip.CLHS_TOT_CONSIDER"))) == 0)
            | (F.length(F.regexp_extract(F.col("Strip.CLHS_TOT_CONSIDER"), r"^-?\d+(\.\d+)?$", 0)) == 0),
            F.lit(0.0)
        ).otherwise(F.col("Strip.CLHS_TOT_CONSIDER").cast("double"))
    )
    .withColumn(
        "TOT_PD_AMT",
        -1 * F.when(
            F.col("Strip.CLHS_TOT_PAID").isNull()
            | (F.length(F.trim(F.col("Strip.CLHS_TOT_PAID"))) == 0)
            | (F.length(F.regexp_extract(F.col("Strip.CLHS_TOT_PAID"), r"^-?\d+(\.\d+)?$", 0)) == 0),
            F.lit(0.0)
        ).otherwise(F.col("Strip.CLHS_TOT_PAID").cast("double"))
    )
    .withColumn(
        "SUB_TOT_PCA_AVLBL_TO_DT_AMT",
        -1 * F.when(
            F.col("Strip.CLHS_SB_ALLOC_AMT").isNull()
            | (F.length(F.trim(F.col("Strip.CLHS_SB_ALLOC_AMT"))) == 0)
            | (F.length(F.regexp_extract(F.col("Strip.CLHS_SB_ALLOC_AMT"), r"^-?\d+(\.\d+)?$", 0)) == 0),
            F.lit(0.0)
        ).otherwise(F.col("Strip.CLHS_SB_ALLOC_AMT").cast("double"))
    )
    .withColumn(
        "SUB_TOT_PCA_PD_TO_DT_AMT",
        -1 * F.when(
            F.col("Strip.SBHS_PAID_AMT").isNull()
            | (F.length(F.trim(F.col("Strip.SBHS_PAID_AMT"))) == 0)
            | (F.length(F.regexp_extract(F.col("Strip.SBHS_PAID_AMT"), r"^-?\d+(\.\d+)?$", 0)) == 0),
            F.lit(0.0)
        ).otherwise(F.col("Strip.SBHS_PAID_AMT").cast("double"))
    )
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("DISCARD_IN"),
        F.col("ROW_PASS_THRU"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CLM_SK"),
        F.col("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("TOT_CNSD_AMT"),
        F.col("TOT_PD_AMT"),
        F.col("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
        F.col("SUB_TOT_PCA_PD_TO_DT_AMT")
    )
)

# Collector stage
df_Collector = df_Regular_enriched.unionByName(df_Reversal_enriched)

# Snapshot transformer with two output links: Pkey, Snapshot
df_Snapshot = df_Collector.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_SK"),
    F.col("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TOT_CNSD_AMT"),
    F.col("TOT_PD_AMT"),
    F.col("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
    F.col("SUB_TOT_PCA_PD_TO_DT_AMT")
)

# Output pin: "Pkey" -> "ClmPcaPK"
df_Snapshot_Pkey = (
    df_Snapshot
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("CLM_SK", F.lit("SK"))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit("NewCrtRunCycExtcnSk"))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit("CurrRunCycle"))
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("DISCARD_IN").alias("DISCARD_IN"),
        F.col("ROW_PASS_THRU").alias("ROW_PASS_THRU"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ERR_CT").alias("ERR_CT"),
        F.col("RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_SK").alias("CLM_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("TOT_CNSD_AMT").alias("TOT_CNSD_AMT"),
        F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
        F.col("SUB_TOT_PCA_AVLBL_TO_DT_AMT").alias("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
        F.col("SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT")
    )
)

# Output pin: "Snapshot" -> "B_CLM_PCA"
df_Snapshot_BCLMPCA = (
    df_Snapshot
    .select(
        F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
        F.col("SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT"),
        F.col("TOT_CNSD_AMT").alias("TOT_CNSD_AMT")
    )
)

# Apply rpad for char/varchar columns in the final data for B_CLM_PCA (length unknown for "CLM_ID", assume 12 since prior usage)
df_Snapshot_BCLMPCA = (
    df_Snapshot_BCLMPCA
    .withColumn("CLM_ID", F.rpad(F.col("CLM_ID"), 12, " "))
)

# Write to "B_CLM_PCA.#SrcSysCd#.dat.#RunID#"
b_clm_pca_path = f"{adls_path}/load/B_CLM_PCA.{SrcSysCd}.dat.{RunID}"
write_files(
    df_Snapshot_BCLMPCA,
    b_clm_pca_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Now call the shared container "ClmPcaPK" on df_Snapshot_Pkey
params_ClmPcaPK = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmPcaPK = ClmPcaPK(df_Snapshot_Pkey, params_ClmPcaPK)

# Finally, stage "FctsClmExtr" -> writing to "key/LhoFctsClmPcaExtr.LhoClmPca.dat#RunID#"
df_FctsClmExtr = df_ClmPcaPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("ROW_PASS_THRU"), 1, " ").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_SK"),
    F.rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TOT_CNSD_AMT"),
    F.col("TOT_PD_AMT"),
    F.col("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
    F.col("SUB_TOT_PCA_PD_TO_DT_AMT")
)

fcts_clm_extr_path = f"{adls_path}/key/LhoFctsClmPcaExtr.LhoClmPca.dat{RunID}"
write_files(
    df_FctsClmExtr,
    fcts_clm_extr_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)