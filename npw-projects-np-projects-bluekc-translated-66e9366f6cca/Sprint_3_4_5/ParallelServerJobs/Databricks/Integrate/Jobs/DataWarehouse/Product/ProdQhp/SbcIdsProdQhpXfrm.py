# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005, 2006, 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC Job Name : SbcIdsProdQhpXfrm
# MAGIC 
# MAGIC Called By: SbcIdsProdQhpExtCntl
# MAGIC                          
# MAGIC PROCESSING:
# MAGIC 
# MAGIC Applies Strip Field and Transformation rules for QHP_ENR
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Jaideep Mankala              2016-07-25        5605                            Original Programming                                           IntegrateDev2                Kalyan Neelam            2016-08-10
# MAGIC Jaideep Mankala              2016-08-24        5605                            Added db2_K_Qhp_Lkp stage for lookup                                                  Kalyan Neelam            2016-09-21
# MAGIC 						     and message broker data source
# MAGIC Jaideep Mankala              2016-10-06        5605                          Change in svTernDt Stage Variable logic in          IntegrateDev2                 Kalyan Neelam            2016-10-06
# MAGIC 						    Overlap Check transformer
# MAGIC Jaideep Mankala              2016-10-12        5605                         Modified Lookup stage to not allow nulls                IntegrateDev2                Kalyan Neelam            2016-10-12
# MAGIC Jaideep Mankala              2016-10-18        5605                         Modified Lookup stage to allow multiple records     IntegrateDev2                Kalyan Neelam            2016-10-18

# MAGIC Strip Field function is applied here on all the TEXT fields
# MAGIC JobName: SbcIdsProdQhpXfrm
# MAGIC 
# MAGIC Strip field function and Tranforamtion rules are applied on the data Extracted from SBC and Facets
# MAGIC Strip Field function is applied here on all the TEXT fields
# MAGIC Transformed Data will land into a Dataset for Primary Keying job.
# MAGIC 
# MAGIC Data is partitioned on Natural Key Columns.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../shared_containers/PrimaryKey/<shared_container_name>
# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCd1 = get_widget_value('SrcSysCd1','')
RunID = get_widget_value('RunID','')
RunIDTimeStamp = get_widget_value('RunIDTimeStamp','')

from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query_db2_K_Qhp_Lkp = f"""
SELECT DISTINCT
QHP_ID,
EFF_DT_SK,
TERM_DT_SK,
QHP_SK,
EFF_DT_SK QHP_EFF_DT_SK
FROM {IDSOwner}.QHP
WHERE 
EFF_DT_SK <> TERM_DT_SK
AND QHP_ID NOT IN ('NA','UNK', '', ' ')
AND QHP_ID IS NOT NULL
"""

df_db2_K_Qhp_Lkp = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query_db2_K_Qhp_Lkp)
    .load()
)

# --------------------------------------------------------------------------------
# COPY stage: split into two outputs with identical columns
# --------------------------------------------------------------------------------
df_COPY_Ref_K_Qhp_In = df_db2_K_Qhp_Lkp.select(
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("QHP_SK").alias("QHP_SK"),
    F.col("QHP_EFF_DT_SK").alias("QHP_EFF_DT_SK")
)

df_COPY_Ref_K_Qhp_Fcts = df_db2_K_Qhp_Lkp.select(
    F.col("QHP_ID").alias("QHP_ID"),
    F.col("EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("TERM_DT_SK").alias("TERM_DT_SK"),
    F.col("QHP_SK").alias("QHP_SK"),
    F.col("QHP_EFF_DT_SK").alias("QHP_EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# ds_PROD_QHP_Extr: reading from DataSet -> becomes reading parquet
# --------------------------------------------------------------------------------
df_ds_PROD_QHP_Extr = spark.read.parquet(
    f"{adls_path}/ds/PROD_QHP.{SrcSysCd}.extr.{RunID}.parquet"
)

# Columns in ds_PROD_QHP_Extr (per metadata):
# [
#   PROD_ID, EFF_DT, TERM_DT, PCKG_ID, BNF_OPT_ID, CHLD_HLTH_SVC_IN, DMSTC_PRTNR_IN,
#   ELCTV_ABRTN_IN, EXMPT_WMN_PREVNTV_IN, GRFTHRED_IN, HYBRID_IN, NON_ERISA_IN,
#   NOT_CUR_IN, PRI_AGE_SEX_IN, SPCH_AND_HRNG_IN, RNWL_MO_NO, PLN_ID, ST_CD,
#   HSA_OPT_TX, PBDL_TX, NTWK_SET_TX, CMNT_TX, PROD_TX, LAST_UPDT_DT, LAST_UPDT_USER_ID, QHP_ID
# ]

# --------------------------------------------------------------------------------
# StripField stage (StripField -> sbc_Overlap_chk)
# --------------------------------------------------------------------------------
df_StripField_sbc_Overlap_chk = df_ds_PROD_QHP_Extr.select(
    # QLFD_HLTH_PLN_ID
    STRIP.FIELD.EE(F.col("QHP_ID")).alias("QLFD_HLTH_PLN_ID"),
    # PROD_ID
    STRIP.FIELD.EE(F.col("PROD_ID")).alias("PROD_ID"),
    # ENR_EFF_DT
    TimestampToString(
        FORMAT.DATE.EE(F.col("EFF_DT"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")),
        F.lit("%yyyy-%mm-%dd")
    ).alias("ENR_EFF_DT"),
    # ENR_TERM_DT
    TimestampToString(
        FORMAT.DATE.EE(F.col("TERM_DT"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")),
        F.lit("%yyyy-%mm-%dd")
    ).alias("ENR_TERM_DT"),
    # LAST_UPDT_DT
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# --------------------------------------------------------------------------------
# sbc_OverLap_Chck
#   This stage has complex row-by-row logic with stage variables.
#   Below we replicate the DataStage stage-variable behavior using an iterative approach.
# --------------------------------------------------------------------------------

w_sbc_OverLap_Chck = (
    Window.partitionBy("PROD_ID","QLFD_HLTH_PLN_ID")
    .orderBy(F.col("ENR_EFF_DT").asc(), F.col("ENR_TERM_DT").asc())
)

# Step 1: add base columns for the logic
df_sbc_OverLap_Chck_stage1 = (
    df_StripField_sbc_Overlap_chk
    .withColumn(
        "svProdQhp",
        trim(F.col("PROD_ID")) + trim(F.col("QLFD_HLTH_PLN_ID"))
    )
    .withColumn(
        "svEffDateMinOne",
        FIND.DATE.EE(F.col("ENR_EFF_DT"), F.lit("-1"), F.lit("D"), F.lit("X"), F.lit("CCYY-MM-DD"))
    )
)

# We need multiple passes to handle the forward references of newly computed variables.
# Pass 2: define "svPrevProdQhp", "svPrevEffDate", "svPrevTermDate" as lag from the future columns
# We will temporarily define placeholders for "svEffDate" and "svTermDate" so we can reference them in the end.
df_sbc_OverLap_Chck_stage2 = (
    df_sbc_OverLap_Chck_stage1
    .withColumn("dummy_svEffDate", F.lit(None).cast("string"))
    .withColumn("dummy_svTermDate", F.lit(None).cast("string"))
    .withColumn("svPrevProdQhp", F.lag("svProdQhp", 1).over(w_sbc_OverLap_Chck))
    .withColumn("svPrevEffDateTemp", F.lag("dummy_svEffDate", 1).over(w_sbc_OverLap_Chck))
    .withColumn("svPrevTermDateTemp", F.lag("dummy_svTermDate", 1).over(w_sbc_OverLap_Chck))
)

# Pass 3: define svEffDate and svTermDate based on the DataStage logic, using a single pass of when() statements
# Because DataStage changes these row by row, we emulate the final outcome in a single expression referencing
#   current row columns and the "temp" columns from previous row. This is approximate:
df_sbc_OverLap_Chck_stage3 = (
    df_sbc_OverLap_Chck_stage2
    .withColumn(
        "svEffDate",
        F.when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") == F.col("svPrevTermDateTemp")),
            F.col("svPrevEffDateTemp")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("svEffDateMinOne") == F.col("svPrevTermDateTemp")),
            F.col("svPrevEffDateTemp")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("svEffDateMinOne") > F.col("svPrevTermDateTemp")),
            F.col("ENR_EFF_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("ENR_EFF_DT") <= F.col("svPrevTermDateTemp")),
            F.col("svPrevEffDateTemp")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") <= F.col("svPrevTermDateTemp")),
            F.col("svPrevEffDateTemp")
        )
        .otherwise(F.col("ENR_EFF_DT"))
    )
    .withColumn(
        "svTermDate",
        F.when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") == F.col("svPrevTermDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("svEffDateMinOne") > F.col("svPrevTermDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") == F.col("svPrevEffDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevTermDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("ENR_EFF_DT") < F.col("svPrevTermDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") <= F.col("svPrevTermDateTemp")),
            F.col("svPrevTermDateTemp")
        )
        .otherwise(F.col("ENR_TERM_DT"))
    )
)

# Pass 4: now define final columns that we actually output for that link (sbc_Dedupe).
df_sbc_OverLap_Chck = df_sbc_OverLap_Chck_stage3.select(
    F.trim(F.col("QLFD_HLTH_PLN_ID")).alias("QLFD_HLTH_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("svEffDate").alias("ENR_EFF_DT"),
    F.col("svTermDate").alias("ENR_TERM_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD")
)

# --------------------------------------------------------------------------------
# sbc_DEDUPE (PxRemDup: Keep last record by Key: PROD_ID, QLFD_HLTH_PLN_ID, ENR_EFF_DT)
# --------------------------------------------------------------------------------
df_sbc_DEDUPE = dedup_sort(
    df_sbc_OverLap_Chck,
    ["PROD_ID", "QLFD_HLTH_PLN_ID", "ENR_EFF_DT"],
    [("ENR_EFF_DT","A")]  # or any stable sort columns needed
)

# Output columns for "lnk_qhp_lkup_Out":
df_sbc_Dedupe_lnk_qhp_lkup_Out = df_sbc_DEDUPE.select(
    F.col("QLFD_HLTH_PLN_ID").alias("QHP_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("ENR_EFF_DT").alias("ENR_EFF_DT"),
    F.col("ENR_TERM_DT").alias("ENR_TERM_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# --------------------------------------------------------------------------------
# QHP_lkup (PxLookup)
#   Primary link: df_sbc_Dedupe_lnk_qhp_lkup_Out (alias lnk_qhp_lkup_Out)
#   Lookup link: df_COPY_Ref_K_Qhp_In (Ref_K_Qhp_In) left join on QHP_ID
# --------------------------------------------------------------------------------
df_QHP_lkup_joined = (
    df_sbc_Dedupe_lnk_qhp_lkup_Out.alias("lnk_qhp_lkup_Out")
    .join(
        df_COPY_Ref_K_Qhp_In.alias("Ref_K_Qhp_In"),
        F.col("lnk_qhp_lkup_Out.QHP_ID") == F.col("Ref_K_Qhp_In.QHP_ID"),
        how="left"
    )
)

# Output columns for QHP_lkup -> STrip_Out
df_QHP_lkup_STrip_Out = df_QHP_lkup_joined.select(
    F.col("lnk_qhp_lkup_Out.PROD_ID").alias("PROD_ID"),
    F.col("lnk_qhp_lkup_Out.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("lnk_qhp_lkup_Out.ENR_EFF_DT").alias("ENR_EFF_DT"),
    F.col("lnk_qhp_lkup_Out.ENR_TERM_DT").alias("ENR_TERM_DT"),
    F.col("lnk_qhp_lkup_Out.QHP_ID").alias("QHP_ID"),
    F.col("Ref_K_Qhp_In.QHP_EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_qhp_lkup_Out.SRC_SYS_CD").alias("SRC_SYS_CD")
)

# --------------------------------------------------------------------------------
# SBC_BusinessRules
# --------------------------------------------------------------------------------
df_SBC_BusinessRules_stage1 = df_QHP_lkup_STrip_Out.select(
    (F.trim(F.col("PROD_ID")).substr(F.lit(1),F.lit(8)) + F.lit(";") + F.col("ENR_EFF_DT") + F.lit(";") + F.trim(F.col("SRC_SYS_CD"))).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit("0").alias("PROD_QHP_SK"),
    F.trim(F.col("PROD_ID")).substr(F.lit(1),F.lit(8)).alias("PROD_ID"),
    F.col("ENR_EFF_DT").alias("EFF_DT"),
    F.lit("0").alias("SRC_SYS_CD_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("ENR_TERM_DT").alias("TERM_DT"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.when(
        F.isnull(F.col("LAST_UPDT_DT")) | (F.length(F.trim(F.col("LAST_UPDT_DT"))) == 0),
        F.lit(RunIDTimeStamp)
    ).otherwise(F.col("LAST_UPDT_DT")).alias("LAST_UPDT_DT"),
    F.col("EFF_DT_SK").alias("QHP_EFF_DT_SK")
)

df_SBC_BusinessRules_lnk_SbcIdsProdQhpXfrm_Out = df_SBC_BusinessRules_stage1

# --------------------------------------------------------------------------------
# facets_PROD_QHP_Extr -> read from dataset => becomes parquet
# --------------------------------------------------------------------------------
df_facets_PROD_QHP_Extr = spark.read.parquet(
    f"{adls_path}/ds/PROD_QHP.{SrcSysCd1}.extr.{RunID}.parquet"
)

# Facets_Stripfield
df_Facets_Stripfield_Overlap_chk = df_facets_PROD_QHP_Extr.select(
    STRIP.FIELD.EE(F.col("QLFD_HLTH_PLN_ID")).alias("QLFD_HLTH_PLN_ID"),
    STRIP.FIELD.EE(F.col("PROD_ID")).alias("PROD_ID"),
    TimestampToString(
        FORMAT.DATE.EE(F.col("ENR_EFF_DT"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")),
        F.lit("%yyyy-%mm-%dd")
    ).alias("ENR_EFF_DT"),
    TimestampToString(
        FORMAT.DATE.EE(F.col("ENR_TERM_DT"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")),
        F.lit("%yyyy-%mm-%dd")
    ).alias("ENR_TERM_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT")
)

# OverLap_Chck: replicate similar stage-variable logic
w_OverLap_Chck = (
    Window.partitionBy("PROD_ID","QLFD_HLTH_PLN_ID")
    .orderBy(F.col("ENR_EFF_DT").asc(), F.col("ENR_TERM_DT").asc())
)

df_OverLap_Chck_stage1 = (
    df_Facets_Stripfield_Overlap_chk
    .withColumn(
        "svProdQhp",
        trim(F.col("PROD_ID")) + trim(F.col("QLFD_HLTH_PLN_ID"))
    )
    .withColumn(
        "svEffDateMinOne",
        FIND.DATE.EE(F.col("ENR_EFF_DT"), F.lit("-1"), F.lit("D"), F.lit("X"), F.lit("CCYY-MM-DD"))
    )
)

df_OverLap_Chck_stage2 = (
    df_OverLap_Chck_stage1
    .withColumn("dummy_svEffDate", F.lit(None).cast("string"))
    .withColumn("dummy_svTermDate", F.lit(None).cast("string"))
    .withColumn("svPrevProdQhp", F.lag("svProdQhp", 1).over(w_OverLap_Chck))
    .withColumn("svPrevEffDateTemp", F.lag("dummy_svEffDate", 1).over(w_OverLap_Chck))
    .withColumn("svPrevTermDateTemp", F.lag("dummy_svTermDate", 1).over(w_OverLap_Chck))
)

df_OverLap_Chck_stage3 = (
    df_OverLap_Chck_stage2
    .withColumn(
        "svEffDate",
        F.when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") == F.col("svPrevTermDateTemp")),
            F.col("svPrevEffDateTemp")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("svEffDateMinOne") == F.col("svPrevTermDateTemp")),
            F.col("svPrevEffDateTemp")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("svEffDateMinOne") > F.col("svPrevTermDateTemp")),
            F.col("ENR_EFF_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("ENR_EFF_DT") <= F.col("svPrevTermDateTemp")),
            F.col("svPrevEffDateTemp")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") <= F.col("svPrevTermDateTemp")),
            F.col("svPrevEffDateTemp")
        )
        .otherwise(F.col("ENR_EFF_DT"))
    )
    .withColumn(
        "svTermDate",
        F.when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") == F.col("svPrevTermDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("svEffDateMinOne") > F.col("svPrevTermDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") == F.col("svPrevEffDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevTermDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") > F.col("svPrevTermDateTemp")) &
            (F.col("ENR_EFF_DT") > F.col("svPrevEffDateTemp")) &
            (F.col("ENR_EFF_DT") < F.col("svPrevTermDateTemp")),
            F.col("ENR_TERM_DT")
        )
        .when(
            (F.col("svProdQhp") == F.col("svPrevProdQhp")) &
            (F.col("ENR_TERM_DT") <= F.col("svPrevTermDateTemp")),
            F.col("svPrevTermDateTemp")
        )
        .otherwise(F.col("ENR_TERM_DT"))
    )
)

df_OverLap_Chck = df_OverLap_Chck_stage3.select(
    F.trim(F.col("QLFD_HLTH_PLN_ID")).alias("QLFD_HLTH_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("svEffDate").alias("ENR_EFF_DT"),
    F.col("svTermDate").alias("ENR_TERM_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.lit(SrcSysCd1).alias("SRC_SYS_CD")
)

# DEDUPE (PxRemDup)
df_DEDUPE = dedup_sort(
    df_OverLap_Chck,
    ["PROD_ID", "QLFD_HLTH_PLN_ID", "ENR_EFF_DT"],
    [("ENR_EFF_DT","A")]
)

df_DEDUPE_lnk_qhp_lkup = df_DEDUPE.select(
    F.col("QLFD_HLTH_PLN_ID").alias("QLFD_HLTH_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("ENR_EFF_DT").alias("ENR_EFF_DT"),
    F.col("ENR_TERM_DT").alias("ENR_TERM_DT"),
    F.col("LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD")
)

# --------------------------------------------------------------------------------
# Facets_QHP_lkup
#   Primary link: df_DEDUPE_lnk_qhp_lkup (alias lnk_qhp_lkup)
#   Lookup link: df_COPY_Ref_K_Qhp_Fcts (alias Ref_K_Qhp_Fcts) left join on QHP_ID
# --------------------------------------------------------------------------------
df_Facets_QHP_lkup_joined = (
    df_DEDUPE_lnk_qhp_lkup.alias("lnk_qhp_lkup")
    .join(
        df_COPY_Ref_K_Qhp_Fcts.alias("Ref_K_Qhp_Fcts"),
        F.col("lnk_qhp_lkup.QLFD_HLTH_PLN_ID") == F.col("Ref_K_Qhp_Fcts.QHP_ID"),
        how="left"
    )
)

df_Facets_QHP_lkup_STrip_Out_Fcts = df_Facets_QHP_lkup_joined.select(
    F.col("lnk_qhp_lkup.PROD_ID").alias("PROD_ID"),
    F.col("lnk_qhp_lkup.ENR_EFF_DT").alias("EFF_DT"),
    F.col("lnk_qhp_lkup.ENR_TERM_DT").alias("TERM_DT"),
    F.col("lnk_qhp_lkup.LAST_UPDT_DT").alias("LAST_UPDT_DT"),
    F.col("lnk_qhp_lkup.QLFD_HLTH_PLN_ID").alias("QHP_ID"),
    F.col("Ref_K_Qhp_Fcts.QHP_EFF_DT_SK").alias("EFF_DT_SK"),
    F.col("lnk_qhp_lkup.SRC_SYS_CD").alias("SRC_SYS_CD")
)

# --------------------------------------------------------------------------------
# FACETS_BusinessRules
# --------------------------------------------------------------------------------
df_FACETS_BusinessRules_stage1 = (
    df_Facets_QHP_lkup_STrip_Out_Fcts
    .withColumn(
        "svProdQHPTermDt",
        F.col("TERM_DT")
    )
    .withColumn(
        "svProdQHPEffDt",
        F.col("EFF_DT")
    )
    .withColumn(
        "svLastUpDtm",
        TimestampToString(
            FORMAT.DATE.EE(F.col("LAST_UPDT_DT"), F.lit("SYBASE"), F.lit("TIMESTAMP"), F.lit("DB2TIMESTAMP")),
            F.lit("%yyyy-%mm-%dd")
        )
    )
)

df_FACETS_BusinessRules_lnk_FctsIdsProdQhpXfrm_Out = df_FACETS_BusinessRules_stage1.select(
    (F.trim(F.col("PROD_ID")).substr(F.lit(1),F.lit(8)) + F.lit(";") + F.col("svProdQHPEffDt") + F.lit(";") + F.trim(F.col("SRC_SYS_CD"))).alias("PRI_NAT_KEY_STRING"),
    F.lit(RunIDTimeStamp).alias("FIRST_RECYC_TS"),
    F.lit("0").alias("PROD_QHP_SK"),
    F.trim(F.col("PROD_ID")).substr(F.lit(1),F.lit(8)).alias("PROD_ID"),
    F.col("svProdQHPEffDt").alias("EFF_DT"),
    F.lit("0").alias("SRC_SYS_CD_SK"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("svProdQHPTermDt").alias("TERM_DT"),
    F.col("QHP_ID").alias("QHP_ID"),
    F.when(
        F.isnull(F.col("LAST_UPDT_DT")) | (F.length(F.trim(F.col("LAST_UPDT_DT"))) == 0),
        F.lit(RunIDTimeStamp)
    ).otherwise(F.col("LAST_UPDT_DT")).alias("LAST_UPDT_DT"),
    F.col("EFF_DT_SK").alias("QHP_EFF_DT_SK")
)

# --------------------------------------------------------------------------------
# Funnel_140
#   Input 1: df_SBC_BusinessRules_lnk_SbcIdsProdQhpXfrm_Out (lnk_SbcIdsProdQhpXfrm_Out)
#   Input 2: df_FACETS_BusinessRules_lnk_FctsIdsProdQhpXfrm_Out (lnk_FctsIdsProdQhpXfrm_Out)
#   We union them (Funnel).
# --------------------------------------------------------------------------------
df_Funnel_140_Sbc_Fcts_Prod_Qhp = (
    df_SBC_BusinessRules_lnk_SbcIdsProdQhpXfrm_Out
    .unionByName(df_FACETS_BusinessRules_lnk_FctsIdsProdQhpXfrm_Out, allowMissingColumns=True)
)

# The Funnel stage passes these columns forward in order:
#  PRI_NAT_KEY_STRING
#  FIRST_RECYC_TS
#  PROD_QHP_SK
#  PROD_ID (char(8), PK)
#  EFF_DT (char(10), PK)
#  SRC_SYS_CD_SK
#  SRC_SYS_CD (PK)
#  TERM_DT (char(10))
#  QHP_ID
#  LAST_UPDT_DT
#  QHP_EFF_DT_SK (char(10))

df_Prod_Qhp_Xfrm = df_Funnel_140_Sbc_Fcts_Prod_Qhp

# --------------------------------------------------------------------------------
# PROD_QHP_Xfrm (PxDataSet) -> write to .ds => becomes .parquet
#   Path: ds/PROD_QHP.#SrcSysCd#.xfrm.#RunID#.ds => must remove ".ds" and add ".parquet"
# --------------------------------------------------------------------------------

# Before final write, apply rpad on char/varchar columns in the final select (to maintain length).
final_cols_in_order = [
    # column name, length if char
    ("PRI_NAT_KEY_STRING", None),
    ("FIRST_RECYC_TS", None),
    ("PROD_QHP_SK", None),
    ("PROD_ID", 8),
    ("EFF_DT", 10),
    ("SRC_SYS_CD_SK", None),
    ("SRC_SYS_CD", None),
    ("TERM_DT", 10),
    ("QHP_ID", None),
    ("LAST_UPDT_DT", None),
    ("QHP_EFF_DT_SK", 10),
]

select_exprs = []
for (col_name, char_len) in final_cols_in_order:
    if char_len is not None:
        # use rpad for char columns
        select_exprs.append(
            F.rpad(F.col(col_name), char_len, " ").alias(col_name)
        )
    else:
        select_exprs.append(F.col(col_name))

df_final_Prod_Qhp_Xfrm = df_Prod_Qhp_Xfrm.select(*select_exprs)

write_files(
    df_final_Prod_Qhp_Xfrm,
    f"{adls_path}/ds/PROD_QHP.{SrcSysCd}.xfrm.{RunID}.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)