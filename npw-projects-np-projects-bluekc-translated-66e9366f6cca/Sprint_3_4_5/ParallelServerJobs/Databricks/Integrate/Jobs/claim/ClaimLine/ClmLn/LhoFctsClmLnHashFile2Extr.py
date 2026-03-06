# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC ********************************************************************************
# MAGIC 
# MAGIC Â© Copyright 2005 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC JOB NAME: FctsClmLnHashFile2Extr
# MAGIC CALLED BY LhoFctsClmOnDmdPrereqSeq
# MAGIC 
# MAGIC DESCRIPTION:  One of three hash file extracts that must run in order (1,2,3)
# MAGIC 
# MAGIC \(9)Extract Claim Line Disallow Data extract from Facets, and load to hash files to be used in FctsClmLnTrns, FctsClmLnDntlTrns and FctsClmLnDsalwExtr
# MAGIC                 Extract rows from the disallow tables for W_CLM_LN_DSALW - pull all rows for claims on TMP_DRIVER
# MAGIC 
# MAGIC  SOURCE:
# MAGIC 
# MAGIC \(9)CMC_CDML_CL_LINE\(9) 
# MAGIC                 CMC_CDDL_CL_LINE
# MAGIC                 CMC_CDMD_LI_DISALL
# MAGIC                 CMC_CDDD_DNLI_DIS
# MAGIC                 CMC_CDOR_LI_OVR
# MAGIC                 CMC_CDDO_DNLI_OVR
# MAGIC                 CMC_CLCL_CLAIM
# MAGIC                 CMC_CLMI_MISC
# MAGIC                  CMC_CDCB_LI_COB
# MAGIC                 TMP_DRIVER
# MAGIC                  IDS CD_MPPNG
# MAGIC                  IDS DSALW_EXCD
# MAGIC 
# MAGIC 
# MAGIC  Hash Files
# MAGIC               used in FctsClmLnTrns, FctsClmLnDntlTrns, FctsClmLnDsalwExtr
# MAGIC 
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC OUTPUTS:
# MAGIC 
# MAGIC \(9)W_CLM_LN_DSALW.dat to be loaded in IDS, must go through the delete process
# MAGIC 
# MAGIC  
# MAGIC                 IDS parameters
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC =============================================================================================================================================================
# MAGIC Developer                    Date                 \(9)Project                                                     Change Description                                         Development Project             Code Reviewer          Date Reviewed    
# MAGIC =============================================================================================================================================================
# MAGIC Manasa Andru          2020-08-10                        US -  263702                                         Original Programming                                              IntegrateDev2                    Jaideep Mankala      10/09/2020
# MAGIC Venkatesh Munnangi 2020-10-12                                                                             Removed Data Elements Populated in the job\(9)
# MAGIC Prabhu ES                 2022-03-17                        S2S                                                MSSQL ODBC conn params added                              IntegrateDev5      \(9)Ken Bradmon\(9)2022-06-11

# MAGIC Facets Claim Line Hash File 2 Extract
# MAGIC These hashfiles are loaded in FctsClmLnHashFile1Extr
# MAGIC 
# MAGIC Also used in FctsClmLnHashFile3Extr
# MAGIC These hash files are used in FctsClmLnHashFile3Extr
# MAGIC Build hash files for dsalw rows to use in FctsClmLnTrns, FctsClmLnDntlTrns, and FctsClmLnDsalwExtr
# MAGIC Do not clear
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC Build hash files for dsalw rows to use in FctsClmLnTrns, FctsClmLnDntlTrns, and FctsClmLnDsalwExtr
# MAGIC Do not clear
# MAGIC                    hf_clm_ln_dsalw_tu
# MAGIC                    hf_clm_ln_dsalw_a
# MAGIC                    hf_clm_ln_dsalw_x
# MAGIC                    hf_clm_ln_dsalw_cdmd
# MAGIC                    hf_clm_ln_dsalw_cddd
# MAGIC                    hf_clm_ln_dsalw_cdml
# MAGIC                    hf_clm_ln_dsalw_cddl
# MAGIC                    hf_clm_ln_dsalw_cap
# MAGIC Used in:
# MAGIC FctsClmLnHashFile3Extr
# MAGIC Hash Files created in FctsClmLnExtr
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ./Manual_Shared_Containers_Path/ <shared_container_name_if_any>
# COMMAND ----------

Source = get_widget_value('Source','')
DriverTable = get_widget_value('DriverTable','')
RunCycle = get_widget_value('RunCycle','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BCBSOwner = get_widget_value('BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')
LhoFacetsStgAcct = get_widget_value('LhoFacetsStgAcct','')
LhoFacetsStgOwner = get_widget_value('LhoFacetsStgOwner','')
lhofacetsstg_secret_name = get_widget_value('lhofacetsstg_secret_name','')
LhoFacetsStgPW = get_widget_value('LhoFacetsStgPW','')
LhoFacetsStgDSN = get_widget_value('LhoFacetsStgDSN','')

# Read from hashed file "hash_x_tmp" (Scenario C: read only)
df_hash_x_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_x_tmp.parquet")

# Read from hashed file "hash_a_tmp" (Scenario C: read only)
df_hash_a_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_a_tmp.parquet")

# Read from hashed file "hash_tu_tmp" (Scenario C: read only)
df_hash_tu_tmp = spark.read.parquet(f"{adls_path}/hf_clm_ln_dsalw_tu_tmp.parquet")

# ODBCConnector "cddd_cdmd_overrides"
jdbc_url_cddd_cdmd_overrides, jdbc_props_cddd_cdmd_overrides = get_db_config(lhofacetsstg_secret_name)
extract_query_cdmd_extr = (
    "SELECT DIS.CLCL_ID, DIS.CDML_SEQ_NO, DIS.CDMD_TYPE, DIS.CDMD_DISALL_AMT, DIS.EXCD_ID, "
    "CLAIM.CLCL_PAID_DT, LINE.CDML_DISALL_AMT "
    f"FROM {LhoFacetsStgOwner}.CMC_CDMD_LI_DISALL DIS, "
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM, "
    f"tempdb..{DriverTable} TMP, "
    f"{LhoFacetsStgOwner}.CMC_CDML_CL_LINE LINE "
    "WHERE DIS.CLCL_ID = TMP.CLM_ID "
    "and DIS.CLCL_ID = CLAIM.CLCL_ID "
    "and DIS.CLCL_ID = LINE.CLCL_ID "
    "and DIS.CDML_SEQ_NO = LINE.CDML_SEQ_NO"
)
df_cdmd_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cddd_cdmd_overrides)
    .options(**jdbc_props_cddd_cdmd_overrides)
    .option("query", extract_query_cdmd_extr)
    .load()
)

extract_query_cddd_extr = (
    "SELECT DIS.CLCL_ID, DIS.CDDL_SEQ_NO CDML_SEQ_NO, DIS.CDDD_TYPE CDMD_TYPE, DIS.CDDD_DISALL_AMT CDMD_DISALL_AMT, "
    "DIS.EXCD_ID, CLAIM.CLCL_PAID_DT, LINE.CDDL_DISALL_AMT CDML_DISALL_AMT "
    f"FROM {LhoFacetsStgOwner}.CMC_CDDD_DNLI_DIS DIS, "
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLAIM, "
    f"tempdb..{DriverTable} TMP, "
    f"{LhoFacetsStgOwner}.CMC_CDDL_CL_LINE LINE "
    "WHERE DIS.CLCL_ID = TMP.CLM_ID "
    "and DIS.CLCL_ID = CLAIM.CLCL_ID "
    "and DIS.CLCL_ID = LINE.CLCL_ID "
    "and DIS.CDDL_SEQ_NO = LINE.CDDL_SEQ_NO"
)
df_cddd_extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_cddd_cdmd_overrides)
    .options(**jdbc_props_cddd_cdmd_overrides)
    .option("query", extract_query_cddd_extr)
    .load()
)

# ODBCConnector "DsalwExcdCdml"
jdbc_url_dsalwexcdcdml, jdbc_props_dsalwexcdcdml = get_db_config(lhofacetsstg_secret_name)
extract_query_dsalwexcdcdml = (
    "SELECT CL.CLCL_ID, CDML_SEQ_NO, CL.CDML_DISALL_EXCD, CLM.CLCL_PAID_DT "
    f"FROM {LhoFacetsStgOwner}.CMC_CDML_CL_LINE CL, "
    f"tempdb..{DriverTable} TMP, "
    f"{LhoFacetsStgOwner}.CMC_CLCL_CLAIM CLM "
    "WHERE TMP.CLM_ID = CL.CLCL_ID "
    "and CLM.CLCL_ID = TMP.CLM_ID"
)
df_DsalwExcdCdml = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dsalwexcdcdml)
    .options(**jdbc_props_dsalwexcdcdml)
    .option("query", extract_query_dsalwexcdcdml)
    .load()
)

# ODBCConnector "DsalwExcdCddl" --> special logic (WHERE ... = ORCHESTRATE.*)
# We read the entire table or filtered by STTUS_CD='A' so we can later do a left join in the Transformer.
jdbc_url_dsalwexcdcddl, jdbc_props_dsalwexcdcddl = get_db_config(bcbs_secret_name)
extract_query_dsalwexcdcddl_base = (
    f"SELECT dalw.CDML_DISALL_EXCD, dalw.EFF_DT, dalw.TERM_DT, dalw.EXCD_RSPNSB_IN, dalw.EXCD_BYPS_IN, dalw.STTUS_CD "
    f"FROM {BCBSOwner}.CLM_DISALL_RSPNSB dalw"
)
df_dsalwexcdcddl_base = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_dsalwexcdcddl)
    .options(**jdbc_props_dsalwexcdcddl)
    .option("query", extract_query_dsalwexcdcddl_base)
    .load()
)
df_DsalwExcdCddl = df_dsalwexcdcddl_base.filter(F.col("STTUS_CD") == 'A')

# Transformer "Trans" has two inputs: primary "dsalw_cdml_cddl" (df_DsalwExcdCdml) and lookup "RefCddl" (df_DsalwExcdCddl).
# The job's constraint: IsNull(RefCddl.CDML_DISALL_EXCD) = @FALSE
# Because in DataStage, there's a matching condition on EFF_DT / TERM_DT, these columns do not appear in df_DsalwExcdCdml,
# but we will perform a left join on "CDML_DISALL_EXCD" and attempt a placeholder for date conditions.
df_Trans_join = (
    df_DsalwExcdCdml.alias("dsalw_cdml_cddl")
    .join(
        df_DsalwExcdCddl.alias("RefCddl"),
        (F.col("dsalw_cdml_cddl.CDML_DISALL_EXCD") == F.col("RefCddl.CDML_DISALL_EXCD"))
        & (F.col("RefCddl.EFF_DT") <= F.lit(None))  # placeholder for dt compare, no matching col in dsalw_cdml_cddl
        & (F.col("RefCddl.TERM_DT") >= F.lit(None)),  # likewise placeholder
        "left"
    )
)
# Now apply the constraint IsNull(RefCddl.CDML_DISALL_EXCD) = @FALSE => means keep rows where RefCddl.CDML_DISALL_EXCD is not null
df_Trans_filtered = df_Trans_join.filter(F.col("RefCddl.CDML_DISALL_EXCD").isNotNull())

# Columns for output link "dsalw_cdml_cddlhf"
df_Trans = df_Trans_filtered.select(
    F.col("dsalw_cdml_cddl.CLCL_ID").alias("CLCL_ID"),
    F.col("dsalw_cdml_cddl.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("RefCddl.EXCD_RSPNSB_IN").alias("EXCD_RSPNSB_IN"),
    F.col("RefCddl.EXCD_BYPS_IN").alias("EXCD_BYPS_IN"),
)

# Scenario A: "hf_dsalw_excd_cdml_cddl" is an intermediate hashed file: Trans -> hf_dsalw_excd_cdml_cddl -> cddd_dsalw_extr & cdmd_dsalw_extr
# We'll drop duplicates on key columns [CLCL_ID, CDML_SEQ_NO]
df_hf_dsalw_excd_cdml_cddl = df_Trans.dropDuplicates(["CLCL_ID","CDML_SEQ_NO"])

# "hf_dsalw_excd_cdml_cddl" then is used by cddd_dsalw_extr (lookup name "DsalwExcdCddd") and cdmd_dsalw_extr (lookup name "DsalwExcdCdmd").
# We do not physically write or read this hashed file; we keep df_hf_dsalw_excd_cdml_cddl in memory.

# Transformer "cdmd_dsalw_extr": primary link = "cdmd_extr" (df_cdmd_extr)
# Additional lookups: tu_lkup (df_hash_tu_tmp), x_lkup (df_hash_x_tmp), aa_lkup (df_hash_a_tmp), DsalwExcdCdmd (df_hf_dsalw_excd_cdml_cddl)
df_cdmd_dsalw_extr_1 = (
    df_cdmd_extr.alias("cdmd_extr")
    .join(
        df_hash_tu_tmp.alias("tu_lkup"),
        (
            (F.col("cdmd_extr.CLCL_ID") == F.col("tu_lkup.CLCL_ID")) &
            (F.col("cdmd_extr.CDML_SEQ_NO") == F.col("tu_lkup.CDML_SEQ_NO")) &
            (F.lit("ATAU") == F.col("tu_lkup.DSALW_TYP"))
        ),
        "left"
    )
)
df_cdmd_dsalw_extr_2 = (
    df_cdmd_dsalw_extr_1.alias("cdmd_extr_2")
    .join(
        df_hash_x_tmp.alias("x_lkup"),
        (
            (F.col("cdmd_extr_2.cdmd_extr.CLCL_ID") == F.col("x_lkup.CLCL_ID")) &
            (F.col("cdmd_extr_2.cdmd_extr.CDML_SEQ_NO") == F.col("x_lkup.CDML_SEQ_NO")) &
            (F.lit("AX") == F.col("x_lkup.DSALW_TYP"))
        ),
        "left"
    )
)
df_cdmd_dsalw_extr_3 = (
    df_cdmd_dsalw_extr_2.alias("cdmd_extr_3")
    .join(
        df_hash_a_tmp.alias("aa_lkup"),
        (
            (F.col("cdmd_extr_3.cdmd_extr.CLCL_ID") == F.col("aa_lkup.CLCL_ID")) &
            (F.col("cdmd_extr_3.cdmd_extr.CDML_SEQ_NO") == F.col("aa_lkup.CDML_SEQ_NO")) &
            (F.lit("AA") == F.col("aa_lkup.DSALW_TYP"))
        ),
        "left"
    )
)
df_cdmd_dsalw_extr_4 = (
    df_cdmd_dsalw_extr_3.alias("cdmd_extr_4")
    .join(
        df_hf_dsalw_excd_cdml_cddl.alias("DsalwExcdCdmd"),
        (
            (F.col("cdmd_extr_4.cdmd_extr.CLCL_ID") == F.col("DsalwExcdCdmd.CLCL_ID")) &
            (F.col("cdmd_extr_4.cdmd_extr.CDML_SEQ_NO") == F.col("DsalwExcdCdmd.CDML_SEQ_NO"))
        ),
        "left"
    )
)

# Implement stage variables logic used to define a final "constraint" and output columns
# For brevity in direct PySpark, we replicate the final constraint:
# If cdmd_extr.CDMD_DISALL_AMT = 0 then 'Y'
# else if certain conditions -> 'Y' else 'N'
# We'll do it directly in a withColumn. Then filter rows where that = 'Y'.
df_cdmd_dsalw_extr_vars = (
    df_cdmd_dsalw_extr_4
    .withColumn("svAAMatch",
        F.when(F.col("aa_lkup.CDML_SEQ_NO").isNotNull(),"Y").otherwise("N")
    )
    .withColumn("svATAUMatch",
        F.when(F.col("tu_lkup.CDML_SEQ_NO").isNotNull(),"Y").otherwise("N")
    )
    .withColumn("svAXMatch",
        F.when(F.col("x_lkup.CDML_SEQ_NO").isNotNull(),"Y").otherwise("N")
    )
    .withColumn("svAANotEqual",
        F.when(
            (F.col("svAAMatch")=="Y") &
            (F.col("aa_lkup.DSALW_AMT")!=F.col("aa_lkup.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svATAUNotEqual",
        F.when(
            (F.col("svATAUMatch")=="Y") &
            (F.col("tu_lkup.DSALW_AMT")!=F.col("tu_lkup.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svAXNotEqual",
        F.when(
            (F.col("svAXMatch")=="Y") &
            (F.col("x_lkup.DSALW_AMT")!=F.col("x_lkup.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svAABuild",
        F.when(
            (F.col("svAANotEqual")=="Y") &
            (F.col("aa_lkup.DSALW_AMT")!=F.col("cdmd_extr_4.cdmd_extr.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svATAUBuild",
        F.when(
            (F.col("svATAUNotEqual")=="Y") &
            (F.col("tu_lkup.DSALW_AMT")!=F.col("cdmd_extr_4.cdmd_extr.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svAXBuild",
        F.when(
            (F.col("svAXNotEqual")=="Y") &
            (F.col("x_lkup.DSALW_AMT")!=F.col("cdmd_extr_4.cdmd_extr.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svBuildCdmd",
        F.when(
            F.col("cdmd_extr_4.cdmd_extr.CDMD_DISALL_AMT") == F.lit(0),
            "Y"
        ).otherwise(
            F.when(
                (F.col("svAABuild")=="Y") |
                (F.col("svATAUBuild")=="Y") |
                (F.col("svAXBuild")=="Y") |
                (
                    (F.col("svAXMatch")=="N") &
                    (F.col("svATAUMatch")=="N") &
                    (F.col("svAAMatch")=="N")
                ),
                "Y"
            ).otherwise("N")
        )
    )
)

df_cdmd_dsalw_extr_filtered = df_cdmd_dsalw_extr_vars.filter(F.col("svBuildCdmd")=="Y")

# Output columns => link "dsalw_cdmd" => goes to "cdmd_hash"
df_cdmd_dsalw_extr_out = df_cdmd_dsalw_extr_filtered.select(
    F.col("cdmd_extr_4.cdmd_extr.CLCL_ID").alias("CLCL_ID"),
    F.col("cdmd_extr_4.cdmd_extr.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("cdmd_extr_4.cdmd_extr.CDMD_TYPE").alias("DSALW_TYP"),
    F.col("cdmd_extr_4.cdmd_extr.CDMD_DISALL_AMT").alias("DSALW_AMT"),
    F.col("cdmd_extr_4.cdmd_extr.EXCD_ID").alias("DSALW_EXCD"),
    F.when(F.col("DsalwExcdCdmd.EXCD_RSPNSB_IN").isNull(),"NA").otherwise(F.col("DsalwExcdCdmd.EXCD_RSPNSB_IN")).alias("EXCD_RESP_CD"),
    F.col("cdmd_extr_4.cdmd_extr.CDMD_DISALL_AMT").alias("CDML_DISALL_AMT"),
    F.when(F.col("DsalwExcdCdmd.EXCD_BYPS_IN").isNull(),"N").otherwise(F.col("DsalwExcdCdmd.EXCD_BYPS_IN")).alias("BYPS_IN")
)

# Scenario A: "cdmd_hash" is intermediate: cdmd_dsalw_extr -> cdmd_hash -> sum_all_cdmd
# Key columns: CLCL_ID, CDML_SEQ_NO, DSALW_TYP
df_cdmd_hash = df_cdmd_dsalw_extr_out.dropDuplicates(["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"])

# Pass to aggregator "sum_all_cdmd"
df_sum_all_cdmd = (
    df_cdmd_hash
    .groupBy("CLCL_ID","CDML_SEQ_NO")
    .agg(F.sum("DSALW_AMT").alias("DSALW_AMT"))
)

# Scenario C for "cdmd_sum_hash" (final hashed file). We write out to parquet.
# Maintain column order: CLCL_ID (char 12), CDML_SEQ_NO (int), DSALW_AMT (decimal).
df_cdmd_sum_hash = df_sum_all_cdmd.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("DSALW_AMT").alias("DSALW_AMT")
)
write_files(
    df_cdmd_sum_hash,
    f"hf_clm_ln_dsalw_cdmd_sum.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Transformer "cddd_dsalw_extr": primary link = cddd_extr (df_cddd_extr)
# Additional lookups: tu_lkup_d (df_hash_tu_tmp), x_lkup_d (df_hash_x_tmp), da_lkup (df_hash_a_tmp), DsalwExcdCddd (df_hf_dsalw_excd_cdml_cddl)
df_cddd_dsalw_extr_1 = (
    df_cddd_extr.alias("cddd_extr")
    .join(
        df_hash_tu_tmp.alias("tu_lkup_d"),
        (
            (F.col("cddd_extr.CLCL_ID") == F.col("tu_lkup_d.CLCL_ID")) &
            (F.col("cddd_extr.CDML_SEQ_NO") == F.col("tu_lkup_d.CDML_SEQ_NO")) &
            (F.lit("DTDU") == F.col("tu_lkup_d.DSALW_TYP"))
        ),
        "left"
    )
)
df_cddd_dsalw_extr_2 = (
    df_cddd_dsalw_extr_1.alias("cddd_extr_2")
    .join(
        df_hash_x_tmp.alias("x_lkup_d"),
        (
            (F.col("cddd_extr_2.cddd_extr.CLCL_ID") == F.col("x_lkup_d.CLCL_ID")) &
            (F.col("cddd_extr_2.cddd_extr.CDML_SEQ_NO") == F.col("x_lkup_d.CDML_SEQ_NO")) &
            (F.lit("DX") == F.col("x_lkup_d.DSALW_TYP"))
        ),
        "left"
    )
)
df_cddd_dsalw_extr_3 = (
    df_cddd_dsalw_extr_2.alias("cddd_extr_3")
    .join(
        df_hash_a_tmp.alias("da_lkup"),
        (
            (F.col("cddd_extr_3.cddd_extr.CLCL_ID") == F.col("da_lkup.CLCL_ID")) &
            (F.col("cddd_extr_3.cddd_extr.CDML_SEQ_NO") == F.col("da_lkup.CDML_SEQ_NO")) &
            (F.lit("DA") == F.col("da_lkup.DSALW_TYP"))
        ),
        "left"
    )
)
df_cddd_dsalw_extr_4 = (
    df_cddd_dsalw_extr_3.alias("cddd_extr_4")
    .join(
        df_hf_dsalw_excd_cdml_cddl.alias("DsalwExcdCddd"),
        (
            (F.col("cddd_extr_4.cddd_extr.CLCL_ID") == F.col("DsalwExcdCddd.CLCL_ID")) &
            (F.col("cddd_extr_4.cddd_extr.CDML_SEQ_NO") == F.col("DsalwExcdCddd.CDML_SEQ_NO"))
        ),
        "left"
    )
)

# Implement stage variables logic, final constraint = 'svBuildCdmd' = 'Y'
df_cddd_dsalw_extr_vars = (
    df_cddd_dsalw_extr_4
    .withColumn("svAAMatch",
        F.when(F.col("da_lkup.CDML_SEQ_NO").isNotNull(),"Y").otherwise("N")
    )
    .withColumn("svATAUMatch",
        F.when(F.col("tu_lkup_d.CDML_SEQ_NO").isNotNull(),"Y").otherwise("N")
    )
    .withColumn("svAXMatch",
        F.when(F.col("x_lkup_d.CDML_SEQ_NO").isNotNull(),"Y").otherwise("N")
    )
    .withColumn("svAANotEqual",
        F.when(
            (F.col("svAAMatch")=="Y") &
            (F.col("da_lkup.DSALW_AMT")!=F.col("da_lkup.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svATAUNotEqual",
        F.when(
            (F.col("svATAUMatch")=="Y") &
            (F.col("tu_lkup_d.DSALW_AMT")!=F.col("tu_lkup_d.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svAXNotEqual",
        F.when(
            (F.col("svAXMatch")=="Y") &
            (F.col("x_lkup_d.DSALW_AMT")!=F.col("x_lkup_d.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svAABuild",
        F.when(
            (F.col("svAANotEqual")=="Y") &
            (F.col("da_lkup.DSALW_AMT")!=F.col("cddd_extr_4.cddd_extr.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svATAUBuild",
        F.when(
            (F.col("svATAUNotEqual")=="Y") &
            (F.col("tu_lkup_d.DSALW_AMT")!=F.col("cddd_extr_4.cddd_extr.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svAXBuild",
        F.when(
            (F.col("svAXNotEqual")=="Y") &
            (F.col("x_lkup_d.DSALW_AMT")!=F.col("cddd_extr_4.cddd_extr.CDML_DISALL_AMT")),
            "Y"
        ).otherwise("N")
    )
    .withColumn("svBuildCdmd",
        F.when(
            F.col("cddd_extr_4.cddd_extr.CDMD_DISALL_AMT") == F.lit(0),
            "Y"
        ).otherwise(
            F.when(
                (F.col("svAABuild")=="Y") |
                (F.col("svATAUBuild")=="Y") |
                (F.col("svAXBuild")=="Y") |
                (
                    (F.col("svAXMatch")=="N") &
                    (F.col("svATAUMatch")=="N") &
                    (F.col("svAAMatch")=="N")
                ),
                "Y"
            ).otherwise("N")
        )
    )
)

df_cddd_dsalw_extr_filtered = df_cddd_dsalw_extr_vars.filter(F.col("svBuildCdmd")=="Y")

df_cddd_dsalw_extr_out = df_cddd_dsalw_extr_filtered.select(
    F.col("cddd_extr_4.cddd_extr.CLCL_ID").alias("CLCL_ID"),
    F.col("cddd_extr_4.cddd_extr.CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("cddd_extr_4.cddd_extr.CDMD_TYPE").alias("DSALW_TYP"),
    F.col("cddd_extr_4.cddd_extr.CDMD_DISALL_AMT").alias("DSALW_AMT"),
    F.col("cddd_extr_4.cddd_extr.EXCD_ID").alias("DSALW_EXCD"),
    F.when(F.col("DsalwExcdCddd.EXCD_RSPNSB_IN").isNull(),"NA").otherwise(F.col("DsalwExcdCddd.EXCD_RSPNSB_IN")).alias("EXCD_RESP_CD"),
    F.col("cddd_extr_4.cddd_extr.CDMD_DISALL_AMT").alias("CDML_DISALL_AMT"),
    F.when(F.col("DsalwExcdCddd.EXCD_BYPS_IN").isNull(),"N").otherwise(F.col("DsalwExcdCddd.EXCD_BYPS_IN")).alias("BYPS_IN")
)

# Scenario A: "cddd_hash" is intermediate: cddd_dsalw_extr -> cddd_hash -> sum_all_cddd
df_cddd_hash = df_cddd_dsalw_extr_out.dropDuplicates(["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"])

# Aggregator "sum_all_cddd"
df_sum_all_cddd = (
    df_cddd_hash
    .groupBy("CLCL_ID","CDML_SEQ_NO")
    .agg(F.sum("DSALW_AMT").alias("DSALW_AMT"))
)

# Scenario C: "sum_cddd_hash" is final hashed file
df_sum_cddd_hash = df_sum_all_cddd.select(
    F.rpad(F.col("CLCL_ID"), 12, " ").alias("CLCL_ID"),
    F.col("CDML_SEQ_NO").alias("CDML_SEQ_NO"),
    F.col("DSALW_AMT").alias("DSALW_AMT")
)
write_files(
    df_sum_cddd_hash,
    f"hf_clm_ln_dsalw_cddd_sum.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# AfterJobRoutine = "1" (DB2StatsSubroutine or other). If the job truly needs it, the snippet example would be:
# (No direct info about the final file path or name for the routine, so not invoked here.)
# End of notebook.