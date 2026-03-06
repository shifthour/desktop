# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
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
# MAGIC Prabhu ES               2022-02-28      S2S Remediation     MSSQL connection parameters added                                        IntegrateDev5                Kalyan Neelam           2022-06-10

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
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')
DriverTable = get_widget_value('DriverTable','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
Logging = get_widget_value('Logging','')
Runcycle = get_widget_value('Runcycle','')
TmpTblRunID = get_widget_value('TmpTblRunID','')
CurrentDateParam = get_widget_value('CurrentDate','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmPcaPK
# COMMAND ----------

# Read hashed file "hf_clm_fcts_reversals" as scenario C (parquet)
df_hf_clm_fcts_reversals = (
    spark.read.parquet(f"{adls_path}/hf_clm_fcts_reversals.parquet")
)

# Read hashed file "clm_nasco_dup_bypass" as scenario C (parquet)
df_clm_nasco_dup_bypass = (
    spark.read.parquet(f"{adls_path}/hf_clm_nasco_dup_bypass.parquet")
)

jdbc_url, jdbc_props = get_db_config(facets_secret_name)
extract_query = (
    f"SELECT clm.CLCL_ID, clm.CLHS_TOT_CONSIDER, clm.CLHS_TOT_PAID, clm.CLHS_SB_ALLOC_AMT, "
    f"clm.SBHS_PAID_AMT, clcl.CLCL_LOW_SVC_DT, clcl.PDPD_ID "
    f"FROM {FacetsOwner}.CMC_CLHS_HSA_CLAIM clm, {FacetsOwner}.CMC_CLCL_CLAIM clcl, tempdb..{DriverTable} drvr "
    f"WHERE clm.CLCL_ID = clcl.CLCL_ID "
    f"  AND clm.CLCL_ID = drvr.CLM_ID"
)

df_CMC_CDHL_HSA_LINE = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_strip = df_CMC_CDHL_HSA_LINE.select(
    strip_field(F.col("CLCL_ID")).alias("CLCL_ID"),
    F.col("CLHS_TOT_CONSIDER").alias("CLHS_TOT_CONSIDER"),
    F.col("CLHS_TOT_PAID").alias("CLHS_TOT_PAID"),
    F.col("CLHS_SB_ALLOC_AMT").alias("CLHS_SB_ALLOC_AMT"),
    F.col("SBHS_PAID_AMT").alias("SBHS_PAID_AMT"),
    F.col("CLCL_LOW_SVC_DT").alias("CLCL_LOW_SVC_DT"),
    strip_field(F.col("PDPD_ID")).alias("PDPD_ID")
).alias("Strip")

df_bl = (
    df_strip
    .join(df_hf_clm_fcts_reversals.alias("fcts_reversals"), F.col("Strip.CLCL_ID") == F.col("fcts_reversals.CLCL_ID"), "left")
    .join(df_clm_nasco_dup_bypass.alias("nasco_dup_lkup"), F.col("Strip.CLCL_ID") == F.col("nasco_dup_lkup.CLM_ID"), "left")
)

df_regular = df_bl.filter("nasco_dup_lkup.CLM_ID IS NULL")
df_reversal = df_bl.filter("fcts_reversals.CLCL_ID IS NOT NULL")

df_regular_enriched = df_regular.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), trim(F.col("Strip.CLCL_ID"))).alias("PRI_KEY_STRING"),
    F.col("Strip.CLCL_ID").alias("CLM_SK"),
    F.when(
        F.col("Strip.CLCL_ID").isNull() | (F.length(trim(F.col("Strip.CLCL_ID"))) == 0),
        F.lit("UNK")
    ).otherwise(trim(F.col("Strip.CLCL_ID"))).alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("Strip.CLHS_TOT_CONSIDER").isNull() | (F.length(trim(F.col("Strip.CLHS_TOT_CONSIDER"))) == 0),
        F.lit("1")
    ).otherwise(trim(F.col("Strip.CLHS_TOT_CONSIDER"))).alias("TOT_CNSD_AMT"),
    F.when(
        F.col("Strip.CLHS_TOT_PAID").isNull() | (F.length(trim(F.col("Strip.CLHS_TOT_PAID"))) == 0),
        F.lit("1")
    ).otherwise(trim(F.col("Strip.CLHS_TOT_PAID"))).alias("TOT_PD_AMT"),
    F.when(
        F.col("Strip.CLHS_SB_ALLOC_AMT").isNull() | (F.length(trim(F.col("Strip.CLHS_SB_ALLOC_AMT"))) == 0),
        F.lit("1")
    ).otherwise(trim(F.col("Strip.CLHS_SB_ALLOC_AMT"))).alias("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
    F.when(
        F.col("Strip.SBHS_PAID_AMT").isNull() | (F.length(trim(F.col("Strip.SBHS_PAID_AMT"))) == 0),
        F.lit("1")
    ).otherwise(trim(F.col("Strip.SBHS_PAID_AMT"))).alias("SUB_TOT_PCA_PD_TO_DT_AMT")
)

df_reversal_enriched = df_reversal.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("ROW_PASS_THRU"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit("FACETS").alias("SRC_SYS_CD"),
    F.concat(F.lit("FACETS;"), F.col("Strip.CLCL_ID"), F.lit("R")).alias("PRI_KEY_STRING"),
    F.col("Strip.CLCL_ID").alias("CLM_SK"),
    F.concat(trim(F.col("Strip.CLCL_ID")), F.lit("R")).alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("Strip.CLHS_TOT_CONSIDER").isNull()
        | (F.length(trim(F.col("Strip.CLHS_TOT_CONSIDER"))) == 0)
        | (F.lit(False) == Num(trim(F.col("Strip.CLHS_TOT_CONSIDER")))),
        Neg(F.lit(0))
    ).otherwise(Neg(trim(F.col("Strip.CLHS_TOT_CONSIDER")))).alias("TOT_CNSD_AMT"),
    F.when(
        F.col("Strip.CLHS_TOT_PAID").isNull()
        | (F.length(trim(F.col("Strip.CLHS_TOT_PAID"))) == 0)
        | (F.lit(False) == Num(trim(F.col("Strip.CLHS_TOT_PAID")))),
        Neg(F.lit(0))
    ).otherwise(Neg(trim(F.col("Strip.CLHS_TOT_PAID")))).alias("TOT_PD_AMT"),
    F.when(
        F.col("Strip.CLHS_SB_ALLOC_AMT").isNull()
        | (F.length(trim(F.col("Strip.CLHS_SB_ALLOC_AMT"))) == 0)
        | (F.lit(False) == Num(trim(F.col("Strip.CLHS_SB_ALLOC_AMT")))),
        Neg(F.lit(0))
    ).otherwise(Neg(trim(F.col("Strip.CLHS_SB_ALLOC_AMT")))).alias("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
    F.when(
        F.col("Strip.SBHS_PAID_AMT").isNull()
        | (F.length(trim(F.col("Strip.SBHS_PAID_AMT"))) == 0)
        | (F.lit(False) == Num(trim(F.col("Strip.SBHS_PAID_AMT")))),
        Neg(F.lit(0))
    ).otherwise(Neg(trim(F.col("Strip.SBHS_PAID_AMT")))).alias("SUB_TOT_PCA_PD_TO_DT_AMT")
)

df_collector = df_regular_enriched.unionByName(df_reversal_enriched)

df_collector_ordered = df_collector.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "ROW_PASS_THRU",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "TOT_CNSD_AMT",
    "TOT_PD_AMT",
    "SUB_TOT_PCA_AVLBL_TO_DT_AMT",
    "SUB_TOT_PCA_PD_TO_DT_AMT"
)

df_snapshot_out = df_collector_ordered.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("TOT_PD_AMT").alias("TOT_PD_AMT"),
    F.col("SUB_TOT_PCA_PD_TO_DT_AMT").alias("SUB_TOT_PCA_PD_TO_DT_AMT"),
    F.col("TOT_CNSD_AMT").alias("TOT_CNSD_AMT")
)

# For writing B_CLM_PCA with the correct column order and rpad for CLM_ID if needed
df_snapshot_out_final = df_snapshot_out.select(
    F.col("SRC_SYS_CD_SK"),
    rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("TOT_PD_AMT"),
    F.col("SUB_TOT_PCA_PD_TO_DT_AMT"),
    F.col("TOT_CNSD_AMT")
)

write_files(
    df_snapshot_out_final,
    f"{adls_path}/load/B_CLM_PCA.FACETS.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)

df_pkey = df_collector_ordered.select(
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

container_params = {
    "CurrRunCycle": CurrRunCycle
}
df_ClmPcaPK = ClmPcaPK(df_pkey, container_params)

df_FctsClmExtr_final = df_ClmPcaPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("ROW_PASS_THRU"), 1, " ").alias("ROW_PASS_THRU"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_SK"),
    rpad(F.col("CLM_ID"), 12, " ").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("TOT_CNSD_AMT"),
    F.col("TOT_PD_AMT"),
    F.col("SUB_TOT_PCA_AVLBL_TO_DT_AMT"),
    F.col("SUB_TOT_PCA_PD_TO_DT_AMT")
)

write_files(
    df_FctsClmExtr_final,
    f"{adls_path}/key/FctsClmPcaExtr.ClmPca.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_parquet=False,
    header=False,
    quote='"',
    nullValue=None
)