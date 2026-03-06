# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC MODIFICATIONS:
# MAGIC               
# MAGIC Developer                          Date               Project/Altiris #               Change Description                                    Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------             ---------------------------------   ---------------------------------    -------------------------   
# MAGIC Kalyan Neelam                 2012-06-28         4784                            Original Programming                                    EnterpriseWrhsDevl    Brent Leland                 07-10-2012
# MAGIC Kalyan Neelam                 2012-08-06         4784                            Added                                                           EnterpriseWrhsDevl    Sharon Andrew             2012-08-07
# MAGIC                                                                                                        CLM_F2.CLM_ID <> DSLink523.PRI_REL_CLM_ID_1 
# MAGIC                                                                                                        constraint in the ClmGrp_lkup transformer
# MAGIC Kalyan Neelam                 2012-08-13         4784                           Added CLM_F join for new ITS_CLM_IN       EnterpriseWrhsDevl     Bhoomi Dasari             08/14/2012
# MAGIC                                                                                                        constraint in CLM_F2 link in CLM_F2_lkup ODBC.
# MAGIC                                                                                                        Removed FACETS srcsyscd condition in CLM_F2 ODBC
# MAGIC Kalyan Neelam                 2012-11-02         4784                           Added new ExistingClm_lkup to the               EnterpriseWrhsDevl    Bhoomi Dasari             11/6/2012
# MAGIC                                                                                                        ClmGrp_Lkup Transformer to not extract the claims 
# MAGIC                                                                                                        which are already being processed in the below route
# MAGIC 
# MAGIC Rama Kamjula                 2013-11-04            5114                        Rewritten from Server to Parallel version           EntepriseWrhsDevl      Jag Yelavarthi           2013-12-22

# MAGIC JobName:IdsClmF2ITSRelClmsUpDt
# MAGIC The dataset is created in   IdsClmF2ITSRelClms
# MAGIC This dataset is generated in   IdsClmF2ITSRelClms
# MAGIC Extract the all the Claims  associated to the Primary claim being updated and updated all of their REL_MBR_SK
# MAGIC Removes the duplicates for CLM_ID
# MAGIC Create load file for CLM_F to update the RelClm fields
# MAGIC Create load file for CLM_F2  to update the RelClm fields
# MAGIC The dataset is created in   IdsClmF2ITSRelClms
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, TimestampType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Define parameters
EDWRunDtCycle = get_widget_value('EDWRunDtCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')

# Get EDW DB config
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)

# Read from db2_CLM_F (DB2ConnectorPX)
extract_query_db2_CLM_F = f"""
SELECT 
CLM_F.CLM_SK,
CLM_F.MBR_SK
FROM {EDWOwner}.CLM_F CLM_F
WHERE CLM_F.SRC_SYS_CD = 'BCBSSC'
"""
df_db2_CLM_F = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_CLM_F)
    .load()
)

# Read from lnk_ExistingClms (PxDataSet) -> RelClmUpdt.ds => read from RelClmUpdt.parquet
df_lnk_ExistingClms = (
    spark.read.parquet(f"{adls_path}/ds/RelClmUpdt.parquet")
    .select(
        F.col("CLM_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("REL_ITS_CLM_IN"),
        F.col("REL_ITS_CLM_ID"),
        F.col("REL_ITS_CLM_SRC_SYS_CD"),
        F.col("REL_ITS_CLM_SK"),
        F.col("REL_ITS_CLM_MBR_SK"),
        F.col("REL_ITS_CLM_SK_SEC")
    )
)

# Read from db2_CLM_F2 (DB2ConnectorPX)
extract_query_db2_CLM_F2 = f"""
SELECT 
CLM_F2.CLM_SK,
SUBSTR(CLM_F2.CLM_ID,1,10) CLM_ID,
CLM_F2.REL_ITS_CLM_IN,
CLM_F2.REL_ITS_CLM_SK,
CLM_F2.REL_ITS_CLM_SRC_SYS_CD,
CLM_F2.REL_ITS_CLM_ID,
CLM_F2.REL_ITS_CLM_MBR_SK
FROM {EDWOwner}.CLM_F2 CLM_F2,
     {EDWOwner}.CLM_F CLM_F
WHERE
CLM_F2.SRC_SYS_CD = 'FACETS'
AND CLM_F2.CLM_SK = CLM_F.CLM_SK
AND CLM_F.CLM_ITS_IN = 'Y'
AND CLM_F.CLM_HOST_IN = 'Y'
"""
df_db2_CLM_F2 = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", extract_query_db2_CLM_F2)
    .load()
)

# Read from ds_ClmGrp (PxDataSet) -> ClmGrp.ds => read from ClmGrp.parquet
df_ds_ClmGrp = (
    spark.read.parquet(f"{adls_path}/ds/ClmGrp.parquet")
    .select(
        F.col("PRI_REL_CLM_ID"),
        F.col("CLM_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("REL_ITS_CLM_IN"),
        F.col("REL_ITS_CLM_SK"),
        F.col("REL_ITS_CLM_SRC_SYS_CD"),
        F.col("REL_ITS_CLM_ID"),
        F.col("REL_ITS_CLM_MBR_SK"),
        F.col("PRI_REL_CLM_ID_1"),
        F.col("ClmGrp_Ind")
    )
)

# Remove_Duplicates_381 (PxRemDup)
df_Remove_Duplicates_381 = dedup_sort(
    df_ds_ClmGrp,
    ["PRI_REL_CLM_ID"],
    []
)

# Read from ds_RelClm (PxDataSet) -> RelClmUpdt.ds => read from RelClmUpdt.parquet
df_ds_RelClm = (
    spark.read.parquet(f"{adls_path}/ds/RelClmUpdt.parquet")
    .select(
        F.col("CLM_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("REL_ITS_CLM_IN"),
        F.col("REL_ITS_CLM_ID"),
        F.col("REL_ITS_CLM_SRC_SYS_CD"),
        F.col("REL_ITS_CLM_SK"),
        F.col("REL_ITS_CLM_MBR_SK"),
        F.col("REL_ITS_CLM_SK_SEC")
    )
)

# Column_Generator_380 (PxColumnGenerator)
df_Column_Generator_380 = df_ds_RelClm.withColumn("ExistingClms_Ind", Generate(<...>))

# lkp_Codes (PxLookup) - primary link df_db2_CLM_F2, lookup links df_Column_Generator_380 & df_Remove_Duplicates_381
df_lkp_Codes_temp = (
    df_db2_CLM_F2.alias("lnk_ClmF2")
    .join(
        df_Column_Generator_380.alias("DSLink369"),
        F.col("lnk_ClmF2.CLM_SK") == F.col("DSLink369.CLM_SK"),
        "left"
    )
    .join(
        df_Remove_Duplicates_381.alias("lnk_ClmGrp"),
        F.col("lnk_ClmF2.CLM_ID") == F.col("lnk_ClmGrp.PRI_REL_CLM_ID"),
        "left"
    )
)
df_lkp_Codes = df_lkp_Codes_temp.select(
    F.col("lnk_ClmF2.CLM_SK").alias("CLM_SK_ClmF2"),
    F.col("lnk_ClmF2.REL_ITS_CLM_IN").alias("REL_ITS_CLM_IN"),
    F.col("lnk_ClmF2.REL_ITS_CLM_SK").alias("REL_ITS_CLM_SK_ClmF2"),
    F.col("lnk_ClmF2.REL_ITS_CLM_SRC_SYS_CD").alias("REL_ITS_CLM_SRC_SYS_CD"),
    F.col("lnk_ClmF2.REL_ITS_CLM_ID").alias("REL_ITS_CLM_ID"),
    F.col("lnk_ClmF2.REL_ITS_CLM_MBR_SK").alias("REL_ITS_CLM_MBR_SK"),
    F.col("lnk_ClmGrp.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("lnk_ClmGrp.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("lnk_ClmGrp.REL_ITS_CLM_SK").alias("REL_ITS_CLM_SK_ClmGrp"),
    F.col("lnk_ClmF2.CLM_ID").alias("CLM_ID_ClmF2"),
    F.col("lnk_ClmGrp.ClmGrp_Ind").alias("ClmGrp_Ind"),
    F.col("DSLink369.ExistingClms_Ind").alias("ExistingClms_Ind"),
    F.col("lnk_ClmGrp.PRI_REL_CLM_ID_1").alias("PRI_REL_CLM_ID_1_ClmGrp")
)

# Transformer_344
df_Transformer_344 = df_lkp_Codes.filter(
    (
        (F.col("ClmGrp_Ind").isNotNull() | (F.col("ClmGrp_Ind") != '')) &
        (F.col("CLM_ID_ClmF2") != F.col("PRI_REL_CLM_ID_1_ClmGrp")) &
        (F.col("ExistingClms_Ind").isNull() | (F.col("ExistingClms_Ind") == ''))
    )
)
df_Transformer_344_out = df_Transformer_344.select(
    F.col("CLM_SK_ClmF2").alias("CLM_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("REL_ITS_CLM_IN").alias("REL_ITS_CLM_IN"),
    F.col("REL_ITS_CLM_SK_ClmF2").alias("REL_ITS_CLM_SK"),
    F.col("REL_ITS_CLM_SRC_SYS_CD").alias("REL_ITS_CLM_SRC_SYS_CD"),
    F.col("REL_ITS_CLM_ID").alias("REL_ITS_CLM_ID"),
    F.col("REL_ITS_CLM_MBR_SK").alias("REL_ITS_CLM_MBR_SK"),
    F.when(
        F.col("REL_ITS_CLM_SK_ClmGrp").isNull(),
        F.lit("NA")
    ).otherwise(F.col("REL_ITS_CLM_SK_ClmGrp")).alias("REL_ITS_CLM_SK_SEC")
)

# Funnel_349 (PxFunnel)
df_Funnel_349 = (
    df_Transformer_344_out.select(
        "CLM_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "REL_ITS_CLM_IN",
        "REL_ITS_CLM_ID",
        "REL_ITS_CLM_SRC_SYS_CD",
        "REL_ITS_CLM_SK",
        "REL_ITS_CLM_MBR_SK",
        "REL_ITS_CLM_SK_SEC"
    )
    .unionByName(
        df_lnk_ExistingClms.select(
            "CLM_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
            "LAST_UPDT_RUN_CYC_EXCTN_SK",
            "REL_ITS_CLM_IN",
            "REL_ITS_CLM_ID",
            "REL_ITS_CLM_SRC_SYS_CD",
            "REL_ITS_CLM_SK",
            "REL_ITS_CLM_MBR_SK",
            "REL_ITS_CLM_SK_SEC"
        )
    )
)

# Lookup_352 (PxLookup) - primary link df_Funnel_349, lookup link df_db2_CLM_F
df_Lookup_352_temp = df_Funnel_349.alias("DSLink353").join(
    df_db2_CLM_F.alias("DSLink355"),
    F.col("DSLink353.REL_ITS_CLM_SK_SEC") == F.col("DSLink355.CLM_SK"),
    "left"
)
df_Lookup_352 = df_Lookup_352_temp.select(
    F.col("DSLink353.CLM_SK").alias("CLM_SK"),
    F.col("DSLink353.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("DSLink353.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DSLink353.REL_ITS_CLM_IN").alias("REL_ITS_CLM_IN"),
    F.col("DSLink353.REL_ITS_CLM_ID").alias("REL_ITS_CLM_ID"),
    F.col("DSLink353.REL_ITS_CLM_SRC_SYS_CD").alias("REL_ITS_CLM_SRC_SYS_CD"),
    F.col("DSLink353.REL_ITS_CLM_SK").alias("REL_ITS_CLM_SK"),
    F.col("DSLink353.REL_ITS_CLM_MBR_SK").alias("REL_ITS_CLM_MBR_SK"),
    F.col("DSLink353.REL_ITS_CLM_SK_SEC").alias("REL_ITS_CLM_SK_SEC"),
    F.col("DSLink355.MBR_SK").alias("MBR_SK")
)

# Transformer_356
df_Transformer_356 = df_Lookup_352

# lnk_CLM_F_Updt
df_lnk_CLM_F_Updt = df_Transformer_356.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK")
)

# lnk_CLM_F2_Updt
df_lnk_CLM_F2_Updt = df_Transformer_356.select(
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("REL_ITS_CLM_IN").alias("REL_ITS_CLM_IN"),
    F.col("REL_ITS_CLM_SK").alias("REL_ITS_CLM_SK"),
    F.col("REL_ITS_CLM_SRC_SYS_CD").alias("REL_ITS_CLM_SRC_SYS_CD"),
    F.col("REL_ITS_CLM_ID").alias("REL_ITS_CLM_ID"),
    F.when(
        F.col("MBR_SK").isNull(),
        F.col("REL_ITS_CLM_MBR_SK")
    ).otherwise(F.col("MBR_SK")).alias("REL_ITS_CLM_MBR_SK")
)

# seq_CLM_F_Updt (PxSequentialFile) - write to CLM_F_Updt.dat
# Ensure final select for correct column order and apply rpad on char columns
df_seq_CLM_F_Updt_final = (
    df_lnk_CLM_F_Updt
    .select(
        F.col("CLM_SK"),
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK")
    )
)
write_files(
    df_seq_CLM_F_Updt_final,
    f"{adls_path}/load/CLM_F_Updt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# seq_CLM_F2_Updt (PxSequentialFile) - write to CLM_F2_Updt.dat
# Ensure final select for correct column order and apply rpad on char columns
df_seq_CLM_F2_Updt_final = (
    df_lnk_CLM_F2_Updt
    .select(
        F.col("CLM_SK"),
        F.rpad(F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.rpad(F.col("REL_ITS_CLM_IN"), 1, " ").alias("REL_ITS_CLM_IN"),
        F.col("REL_ITS_CLM_SK"),
        F.col("REL_ITS_CLM_SRC_SYS_CD"),
        F.col("REL_ITS_CLM_ID"),
        F.col("REL_ITS_CLM_MBR_SK")
    )
)
write_files(
    df_seq_CLM_F2_Updt_final,
    f"{adls_path}/load/CLM_F2_Updt.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)