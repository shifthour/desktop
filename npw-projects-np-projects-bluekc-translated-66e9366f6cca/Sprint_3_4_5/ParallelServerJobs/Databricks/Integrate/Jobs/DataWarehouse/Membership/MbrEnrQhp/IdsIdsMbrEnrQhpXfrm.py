# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : IdsIdsMbrEnrQhpXfrm
# MAGIC 
# MAGIC Called By: IdsIdsMbrEnrQhpExtCntl
# MAGIC 
# MAGIC                    
# MAGIC PROCESSING:
# MAGIC 
# MAGIC Applies Strip Field and Transformation rules for QHP_ENR
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Jaideep Mankala              2016-07-25        5605                            Original Programming                                            IntegrateDev2               Kalyan Neelam             2016-08-10
# MAGIC Jaideep Mankala              2016-08-24        5605                           Added 2 new columns to dataset		    IntegrateDev2 
# MAGIC 						     (prod_qhp_eff and term dates) and dedupe

# MAGIC Remove Duplicates stage used to remove duplicates giving priority to SRC_CD - FACETS first and SBC next
# MAGIC Job needs to pick most recent record if a member have 2 records for Prod ID having different QHP's with OVERLAPPING dates
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC JobName: IdsIdsMbrEnrQhpXfrm
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from IDS
# MAGIC Strip Field function is applied here on all the TEXT fields
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
SrcSysCd = get_widget_value('SrcSysCd','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')

# --------------------------------------------------------------------------------
# Step 1: Read ds_MBR_ENR_QHP_Extr (PxDataSet) as Parquet
# --------------------------------------------------------------------------------
schema_ds_MBR_ENR_QHP_Extr = StructType([
    StructField("MBR_ENR_SK", StringType(), True),
    StructField("SRC_SYS_CD_SK", StringType(), True),
    StructField("MBR_UNIQ_KEY", StringType(), True),
    StructField("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK", StringType(), True),
    StructField("MBR_EFF_DT_SK", StringType(), True),
    StructField("MBR_TERM_DT_SK", StringType(), True),
    StructField("QHP_SK", StringType(), True),
    StructField("QHP_EFF_DT_SK", StringType(), True),
    StructField("QHP_TERM_DT_SK", StringType(), True),
    StructField("QHP_ID", StringType(), True),
    StructField("PROD_QHP_EFF_DT_SK", StringType(), True),
    StructField("PROD_QHP_TERM_DT_SK", StringType(), True),
    StructField("SRC_CD", StringType(), True),
    StructField("DP_IN", StringType(), True)
])

df_ds_MBR_ENR_QHP_Extr = (
    spark.read.schema(schema_ds_MBR_ENR_QHP_Extr)
    .parquet(f"MBR_ENR_QHP.{SrcSysCd}.extr.{RunID}.parquet")  # .ds replaced with .parquet
)

# --------------------------------------------------------------------------------
# Step 2: Read ds_CD_MPPNG_LkpData (PxDataSet) as Parquet
# --------------------------------------------------------------------------------
schema_ds_CD_MPPNG_LkpData = StructType([
    StructField("CD_MPPNG_SK", StringType(), True),
    StructField("SRC_CD", StringType(), True),
    StructField("SRC_CD_NM", StringType(), True),
    StructField("SRC_CLCTN_CD", StringType(), True),
    StructField("SRC_DRVD_LKUP_VAL", StringType(), True),
    StructField("SRC_DOMAIN_NM", StringType(), True),
    StructField("SRC_SYS_CD", StringType(), True),
    StructField("TRGT_CD", StringType(), True),
    StructField("TRGT_CD_NM", StringType(), True),
    StructField("TRGT_CLCTN_CD", StringType(), True),
    StructField("TRGT_DOMAIN_NM", StringType(), True)
])

df_ds_CD_MPPNG_LkpData = (
    spark.read.schema(schema_ds_CD_MPPNG_LkpData)
    .parquet("CD_MPPNG.parquet")  # .ds replaced with .parquet
)

# --------------------------------------------------------------------------------
# Step 3: Copy stage split into two outputs: src_cd and cls_pln_prd_cat
# --------------------------------------------------------------------------------
df_Copy_src_cd = df_ds_CD_MPPNG_LkpData.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)

df_Copy_cls_pln_prd_cat = df_ds_CD_MPPNG_LkpData.select(
    F.col("CD_MPPNG_SK").alias("CD_MPPNG_SK"),
    F.col("SRC_CD").alias("SRC_CD")
)

# --------------------------------------------------------------------------------
# Step 4: SRC_CLSPLN_LKUP (PxLookup) - Left Joins
# --------------------------------------------------------------------------------
df_SRC_CLSPLN_LKUP = (
    df_ds_MBR_ENR_QHP_Extr.alias("lnk_IdsMbrEnrQhpXfrm_lkup_in")
    .join(
        df_Copy_src_cd.alias("src_cd"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.SRC_SYS_CD_SK") == F.col("src_cd.CD_MPPNG_SK"),
        how="left"
    )
    .join(
        df_Copy_cls_pln_prd_cat.alias("cls_pln_prd_cat"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK") == F.col("cls_pln_prd_cat.CD_MPPNG_SK"),
        how="left"
    )
    .select(
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.MBR_ENR_SK").alias("MBR_ENR_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.MBR_ENR_CLS_PLN_PROD_CAT_CD_SK").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.MBR_EFF_DT_SK").alias("MBR_EFF_DT_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.MBR_TERM_DT_SK").alias("MBR_TERM_DT_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.QHP_SK").alias("QHP_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.QHP_EFF_DT_SK").alias("QHP_EFF_DT_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.QHP_TERM_DT_SK").alias("QHP_TERM_DT_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.QHP_ID").alias("QHP_ID"),
        F.col("src_cd.SRC_CD").alias("SRC_SYS_CD"),
        F.col("cls_pln_prd_cat.SRC_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.PROD_QHP_EFF_DT_SK").alias("PROD_QHP_EFF_DT_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.PROD_QHP_TERM_DT_SK").alias("PROD_QHP_TERM_DT_SK"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.SRC_CD").alias("SRC_CD"),
        F.col("lnk_IdsMbrEnrQhpXfrm_lkup_in.DP_IN").alias("DP_IN")
    )
)

# --------------------------------------------------------------------------------
# Step 5: RECNT_RCRD (CTransformerStage) row-by-row logic.
# Implements the stage variables with prior-row references.
# --------------------------------------------------------------------------------
window_RECNT = (
    Window.partitionBy(
        "SRC_CD",
        "MBR_ENR_CLS_PLN_PROD_CAT_CD",
        "MBR_UNIQ_KEY",
        "MBR_EFF_DT_SK"
    ).orderBy(F.col("PROD_QHP_EFF_DT_SK").desc())
)

df_RECNT_RCRD_vars = (
    df_SRC_CLSPLN_LKUP
    .withColumn("svSrcCd", trim(F.col("SRC_CD")))
    .withColumn("svMbrUnqKey", trim(F.col("MBR_UNIQ_KEY")))
    .withColumn("svMbrEnrClsPlnCd", trim(F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD_SK")))
    .withColumn("svMbrEffDt", F.col("MBR_EFF_DT_SK"))
    .withColumn("svMbrTermDt", F.col("MBR_TERM_DT_SK"))
    .withColumn("svProdQhpEffDt", F.col("PROD_QHP_EFF_DT_SK"))
    .withColumn("svProdQhpTermDt", F.col("PROD_QHP_TERM_DT_SK"))
    .withColumn("svPrevSrcCd", F.lag("svSrcCd", 1).over(window_RECNT))
    .withColumn("svPrevMbrUnqKey", F.lag("svMbrUnqKey", 1).over(window_RECNT))
    .withColumn("svPrevMbrEnrClsPlnCd", F.lag("svMbrEnrClsPlnCd", 1).over(window_RECNT))
    .withColumn("svPrevMbrEffDt", F.lag("svMbrEffDt", 1).over(window_RECNT))
    .withColumn("svPrevMbrTermDt", F.lag("svMbrTermDt", 1).over(window_RECNT))
    .withColumn("svPrevProdQhpEffDt", F.lag("svProdQhpEffDt", 1).over(window_RECNT))
    .withColumn("svPrevProdQhpTermDt", F.lag("svProdQhpTermDt", 1).over(window_RECNT))
)

df_RECNT_RCRD_calc = df_RECNT_RCRD_vars.withColumn(
    "svCalc",
    F.when(
        (F.col("svSrcCd") == F.col("svPrevSrcCd")) &
        (F.col("svMbrEnrClsPlnCd") == F.col("svPrevMbrEnrClsPlnCd")) &
        (F.col("svMbrUnqKey") == F.col("svPrevMbrUnqKey")) &
        (F.col("svMbrEffDt") == F.col("svPrevMbrEffDt")) &
        (F.col("svPrevMbrEffDt") >= F.col("svPrevProdQhpEffDt")) &
        (F.col("svPrevMbrEffDt") <= F.col("svPrevProdQhpTermDt")) &
        (F.col("svProdQhpTermDt") > F.col("svPrevProdQhpEffDt")) &
        (F.col("svMbrEffDt") >= F.col("svProdQhpEffDt")) &
        (F.col("svMbrEffDt") <= F.col("svProdQhpTermDt")),
        F.lit("N")
    ).otherwise("Y")
)

# Filter where trim(svCalc) = 'Y'
df_RECNT_RCRD_filtered = df_RECNT_RCRD_calc.filter(F.trim(F.col("svCalc")) == "Y")

# Select final output columns for RECNT_RCRD → lnk_IdsMbrEnrQhpXfrm_in
df_RECNT_RCRD_out = df_RECNT_RCRD_filtered.select(
    "MBR_ENR_SK",
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD_SK",
    "MBR_EFF_DT_SK",
    "MBR_TERM_DT_SK",
    "QHP_SK",
    "QHP_EFF_DT_SK",
    "QHP_TERM_DT_SK",
    "QHP_ID",
    "SRC_SYS_CD",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD",
    "PROD_QHP_EFF_DT_SK",
    "PROD_QHP_TERM_DT_SK",
    "SRC_CD",
    "DP_IN"
)

# --------------------------------------------------------------------------------
# Step 6: StripField (CTransformerStage)
# --------------------------------------------------------------------------------
# Two conditional expressions for PROD_QHP_EFF_DT_SK and PROD_QHP_TERM_DT_SK
df_StripField = (
    df_RECNT_RCRD_out
    .withColumn(
        "PROD_QHP_EFF_DT_SK",
        F.when(
            F.col("PROD_QHP_EFF_DT_SK") > F.col("MBR_EFF_DT_SK"),
            F.col("PROD_QHP_EFF_DT_SK")
        ).otherwise(F.col("MBR_EFF_DT_SK"))
    )
    .withColumn(
        "PROD_QHP_TERM_DT_SK",
        F.when(
            F.col("PROD_QHP_TERM_DT_SK") < F.col("MBR_TERM_DT_SK"),
            F.col("PROD_QHP_TERM_DT_SK")
        ).otherwise(F.col("MBR_TERM_DT_SK"))
    )
)

# The output pins do NOT carry DP_IN forward. Final columns for lnk_STrip_Out:
df_StripField_out = df_StripField.select(
    "MBR_ENR_SK",
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD_SK",
    "MBR_EFF_DT_SK",
    "MBR_TERM_DT_SK",
    "QHP_SK",
    "QHP_EFF_DT_SK",
    "QHP_TERM_DT_SK",
    "QHP_ID",
    "SRC_SYS_CD",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD",
    "PROD_QHP_EFF_DT_SK",
    "PROD_QHP_TERM_DT_SK",
    "SRC_CD"
    # DP_IN is dropped as per the stage output definition
)

# --------------------------------------------------------------------------------
# Step 7: BusinessRules (CTransformerStage)
# --------------------------------------------------------------------------------
# We partition by SRC_SYS_CD, MBR_UNIQ_KEY, MBR_ENR_CLS_PLN_PROD_CAT_CD, MBR_EFF_DT_SK
# and then sort by SRC_CD ascending. The logic references previous row's key and check.
w_Biz = (
    Window.partitionBy(
        "SRC_SYS_CD",
        "MBR_UNIQ_KEY",
        "MBR_ENR_CLS_PLN_PROD_CAT_CD",
        "MBR_EFF_DT_SK"
    ).orderBy(F.col("SRC_CD").asc())
)

# First, define columns for svKey, svSoruce
df_Biz_vars = (
    df_StripField_out
    .withColumn(
        "svKey",
        F.concat(
            F.trim(F.col("SRC_SYS_CD")), F.lit(":"),
            F.col("MBR_UNIQ_KEY"), F.lit(":"),
            F.trim(F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD")), F.lit(":"),
            F.trim(F.col("MBR_EFF_DT_SK"))
        )
    )
    .withColumn("svSoruce", trim(F.col("SRC_CD")))
)

# Define the previous row's stage variables
df_Biz_vars2 = (
    df_Biz_vars
    .withColumn("svPrevKey", F.lag("svKey", 1).over(w_Biz))
    .withColumn("svPrevSource", F.lag("svSoruce", 1).over(w_Biz))
    .withColumn("svPrevCheck", F.lag("svCheck", 1).over(w_Biz))  # we will define svCheck next
)

# Now define svCheck in one more pass. Because it references svPrevCheck:
df_Biz_vars3 = df_Biz_vars2.withColumn(
    "svCheck",
    F.when(
        (F.col("svKey") == F.col("svPrevKey")) &
        (F.col("svPrevSource") == F.lit("FACETS")) &
        (F.trim(F.col("SRC_CD")) == F.lit("SBC")),
        F.lit("N")
    )
    .otherwise(
        F.when(
            (F.col("svKey") == F.col("svPrevKey")) &
            (F.col("svPrevCheck") == F.lit("N")) &
            (F.trim(F.col("SRC_CD")) == F.lit("SBC")),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
)

# Re-define svPrevCheck now that svCheck is known (lag it again):
df_Biz_final = (
    df_Biz_vars3
    .withColumn("tempCheck", F.lag("svCheck", 1).over(w_Biz))
    .drop("svPrevCheck")
    .withColumnRenamed("tempCheck", "svPrevCheck")
)

# Filter using the constraint: trim(svPrevCheck) = 'Y'
df_BusinessRules_filtered = df_Biz_final.filter(F.trim(F.col("svPrevCheck")) == "Y")

# Finally select columns for remove_dupes
df_BusinessRules_out = df_BusinessRules_filtered.select(
    F.concat(
        F.trim(F.col("SRC_SYS_CD")), F.lit(";"),
        F.col("MBR_UNIQ_KEY"), F.lit(";"),
        F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD"), F.lit(";"),
        F.col("MBR_EFF_DT_SK"), F.lit(";"),
        F.col("PROD_QHP_EFF_DT_SK")
    ).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD_SK",
    F.col("MBR_EFF_DT_SK").alias("MBR_EFF_DT_SK"),
    F.col("QHP_EFF_DT_SK").alias("QHP_EFF_DT_SK"),
    F.col("MBR_TERM_DT_SK").alias("MBR_TERM_DT_SK"),
    F.col("QHP_SK").alias("QHP_SK"),
    F.col("QHP_TERM_DT_SK").alias("QHP_TERM_DT_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("MBR_ENR_CLS_PLN_PROD_CAT_CD").alias("MBR_ENR_CLS_PLN_PROD_CAT_CD"),
    "MBR_ENR_SK",
    "QHP_ID",
    "PROD_QHP_EFF_DT_SK",
    "PROD_QHP_TERM_DT_SK",
    F.col("SRC_CD").alias("SRC_CD")
)

# --------------------------------------------------------------------------------
# Step 8: DEDUPE (PxRemDup)
# --------------------------------------------------------------------------------
# Retain first record, define key columns for duplicates:
partition_cols_dedupe = [
    "SRC_SYS_CD",
    "MBR_UNIQ_KEY",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD",
    "MBR_EFF_DT_SK",
    "PROD_QHP_EFF_DT_SK"
]
sort_cols_dedupe = []  # Not explicitly sorted further for first record

df_DEDUPE_out = dedup_sort(
    df_BusinessRules_out,
    partition_cols_dedupe,
    sort_cols_dedupe
)

# --------------------------------------------------------------------------------
# Step 9: Write ds_MBR_ENR_QHP_Xfrm (PxDataSet) as Parquet
# --------------------------------------------------------------------------------
# Final select with correct column order from "lnk_IdsMbrEnrQhpXfrm_Out"
df_final = df_DEDUPE_out.select(
    "PRI_NAT_KEY_STRING",
    "FIRST_RECYC_TS",
    "SRC_SYS_CD_SK",
    "MBR_UNIQ_KEY",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD_SK",
    "MBR_EFF_DT_SK",
    "QHP_EFF_DT_SK",
    "MBR_TERM_DT_SK",
    "QHP_SK",
    "QHP_TERM_DT_SK",
    "SRC_SYS_CD",
    "MBR_ENR_CLS_PLN_PROD_CAT_CD",
    "MBR_ENR_SK",
    "QHP_ID",
    "PROD_QHP_EFF_DT_SK",
    "PROD_QHP_TERM_DT_SK"
)

# Apply rpad for char-type columns in the final dataframe:
df_final_padded = (
    df_final
    .withColumn("MBR_EFF_DT_SK", F.rpad(F.col("MBR_EFF_DT_SK"), 10, " "))
    .withColumn("QHP_EFF_DT_SK", F.rpad(F.col("QHP_EFF_DT_SK"), 10, " "))
    .withColumn("MBR_TERM_DT_SK", F.rpad(F.col("MBR_TERM_DT_SK"), 10, " "))
    .withColumn("QHP_TERM_DT_SK", F.rpad(F.col("QHP_TERM_DT_SK"), 10, " "))
    .withColumn("PROD_QHP_EFF_DT_SK", F.rpad(F.col("PROD_QHP_EFF_DT_SK"), 10, " "))
    .withColumn("PROD_QHP_TERM_DT_SK", F.rpad(F.col("PROD_QHP_TERM_DT_SK"), 10, " "))
)

write_files(
    df_final_padded,
    f"MBR_ENR_QHP.{SrcSysCd}.xfrm.{RunID}.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)