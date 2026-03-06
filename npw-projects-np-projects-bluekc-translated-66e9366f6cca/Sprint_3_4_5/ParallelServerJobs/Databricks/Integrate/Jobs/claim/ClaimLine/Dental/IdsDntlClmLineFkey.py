# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2020 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.  Output is final table format for DNTL_CLM_LN
# MAGIC 
# MAGIC INPUTS:   .../key/FctsDntlClmLnExtr.FctsDntlClmLn.uniq
# MAGIC HASH FILES:     hf_recycle - used to capture records with an error assigning surrogate key
# MAGIC 
# MAGIC PROCESSING:  assign foreign keys (surrogate keys) to claim line dental records
# MAGIC 
# MAGIC OUTPUTS:  .../load/DNTL_CLM_LN.#Source#.dat
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Steph Goddard  06/2004  -   Originally Programmed
# MAGIC              SAndrew           08/09/2004-   IDS 2.0 Changed DNTL_CAT_CD_SK to DNTL_CLM_LN_DNTL_CAT_CD_SK
# MAGIC              SAndrew           08/09/2004-   IDS 2.0 Changed DNTL_CAT_RULE_CD_SK to DNTL_CLM_LN_DNTL_CAT_RULE_CD_SK
# MAGIC              Kevin Soderlund 10/18/2004  Changed length of TOOT_SRFC_TX
# MAGIC            Steph Goddard   03/01/2006    sequencer changes
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------                             --------------------------------       -------------------------------   ----------------------------       
# MAGIC Parik                      2008-08-12       3567(Primary Key)     Added Source System code SK as part of the parameter           devlIDS                          Steph Goddard          08/14/2008
# MAGIC 
# MAGIC Rick Henry            2012-05-11         4386                       Added default of "DNTL" to proc_cd_typ_cd and                       NewDevl                        Sharon Andrew          2012-5-18
# MAGIC                                                                                         proc_cd_cat_cd in getfkeyproccd
# MAGIC 
# MAGIC Kalyan Neelam       2014-12-17           5212                  Added If SRC_SYS_CD = 'BCBSA' Then 'BCA' Else SRC_SYS_CD    IntegrateCurDevl       Bhoomi Dasari             02/04/2015
# MAGIC                                                                                      in the stage variables and pass it to GetFkeyCodes because code sets are created under BCA for BCBSA
# MAGIC 
# MAGIC Reddy Sanam       2020-10-10                                    Added If SRC_SYS_CD = 'LUMERIS' Then 'FACETS' Else SRC_SYS_CD    IntegrateDev2     
# MAGIC                                                                                     in the stage variables and pass it to GetFkeyCodes because no new codesets
# MAGIC                                                                                     are created for LUMERIS
# MAGIC 
# MAGIC Emran.Mohammad               2020-10-12                                            brought up to standards

# MAGIC Writing Sequential File to /load
# MAGIC check for foreign keys - write out record to recycle file if errors
# MAGIC Merge source data with default rows
# MAGIC Set all foreign surragote keys
# MAGIC Read common record format file from extract job.
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value('Logging','Y')
Source = get_widget_value('Source','FACETS')
InFile = get_widget_value('InFile','FctsDntlClmLnExtr.FctsDntlClmLn.dat.20120425')
SrcSysCdSk = get_widget_value('SrcSysCdSk','1')

schema_DntlClmLnCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("DNTL_CLM_LN_SK", IntegerType(), nullable=False),
    StructField("CLM_ID", StringType(), nullable=False),
    StructField("CLM_LN_SEQ_NO", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("ALT_PROC_CD", StringType(), nullable=False),
    StructField("DNTL_CLM_LN_DNTL_CAT_CD", StringType(), nullable=False),
    StructField("DNTL_CLM_LN_DNTL_CAT_RULE_CD", StringType(), nullable=False),
    StructField("DNTL_CLM_LN_ALT_PROC_EXCD", StringType(), nullable=False),
    StructField("DNTL_CLM_LN_BNF_TYP_CD", StringType(), nullable=False),
    StructField("DNTL_CLM_LN_TOOTH_SRFC_CD", StringType(), nullable=False),
    StructField("DNTL_CLM_LN_UTIL_EDIT_CD", StringType(), nullable=False),
    StructField("CAT_PAYMT_PFX_ID", StringType(), nullable=False),
    StructField("DNTL_PROC_PAYMT_PFX_ID", StringType(), nullable=False),
    StructField("DNTL_PROC_PRICE_ID", StringType(), nullable=False),
    StructField("TOOTH_BEG_NO", StringType(), nullable=True),
    StructField("TOOTH_END_NO", StringType(), nullable=False),
    StructField("TOOTH_NO", StringType(), nullable=True),
])

df_DntlClmLnCrf = (
    spark.read
        .option("header", "false")
        .option("sep", ",")
        .option("quote", "\"")
        .schema(schema_DntlClmLnCrf)
        .csv(f"{adls_path}/key/{InFile}")
)

df_foreignkey_vars = df_DntlClmLnCrf.select(
    "*",
    F.when(F.col("SRC_SYS_CD") == 'BCBSA', F.lit('BCA'))
     .when(F.col("SRC_SYS_CD") == 'LUMERIS', F.lit('FACETS'))
     .otherwise(F.col("SRC_SYS_CD")).alias("svCdMpngSrcSysCd"),
    F.when(F.col("SRC_SYS_CD") == 'BCBSSC',
           GetFkeyProcCd(F.lit("FACETS"), F.col("DNTL_CLM_LN_SK"), F.col("ALT_PROC_CD"), F.lit("DNTL"), F.lit("DNTL"), F.lit(Logging))
       )
     .otherwise(
           GetFkeyProcCd(F.col("SRC_SYS_CD"), F.col("DNTL_CLM_LN_SK"), F.col("ALT_PROC_CD"), F.lit("DNTL"), F.lit("DNTL"), F.lit(Logging))
       ).alias("AltProcCdSk"),
    GetFkeyClmLn(F.col("SRC_SYS_CD"), F.col("DNTL_CLM_LN_SK"), F.col("CLM_ID"), F.col("CLM_LN_SEQ_NO"), F.lit(Logging)).alias("ClmLnSk"),
    GetFkeyCodes(F.col("svCdMpngSrcSysCd"), F.col("DNTL_CLM_LN_SK"), F.lit("DENTAL CATEGORY"), F.col("DNTL_CLM_LN_DNTL_CAT_CD"), F.lit(Logging)).alias("DntlCatCdSk"),
    GetFkeyCodes(F.col("svCdMpngSrcSysCd"), F.col("DNTL_CLM_LN_SK"), F.lit("DENTAL CATEGORY RULE"), F.col("DNTL_CLM_LN_DNTL_CAT_RULE_CD"), F.lit(Logging)).alias("DntlCatRuleCdSk"),
    GetFkeyExcd(F.col("SRC_SYS_CD"), F.col("DNTL_CLM_LN_SK"), F.col("DNTL_CLM_LN_ALT_PROC_EXCD"), F.lit(Logging)).alias("DntlClmLnAltProcExcdSk"),
    GetFkeyCodes(F.col("svCdMpngSrcSysCd"), F.col("DNTL_CLM_LN_SK"), F.lit("DENTAL CLAIM LINE BENEFIT TYPE"), F.col("DNTL_CLM_LN_BNF_TYP_CD"), F.lit(Logging)).alias("DntlClmLnBnfTypCdSk"),
    GetFkeyCodes(F.col("svCdMpngSrcSysCd"), F.col("DNTL_CLM_LN_SK"), F.lit("DENTAL CLAIM LINE UTILIZATION EDIT"), F.col("DNTL_CLM_LN_UTIL_EDIT_CD"), F.lit(Logging)).alias("DntlClmLnUtilEditCdSk"),
    F.col("PASS_THRU_IN").alias("PassThru"),
    GetFkeyErrorCnt(F.col("DNTL_CLM_LN_SK")).alias("ErrCount")
)

df_foreignkey_fkey = (
    df_foreignkey_vars
        .filter((F.col("ErrCount") == 0) | (F.col("PassThru") == 'Y'))
        .select(
            F.col("DNTL_CLM_LN_SK").alias("DNTL_CLM_LN_SK"),
            F.lit(SrcSysCdSk).cast("int").alias("SRC_SYS_CD_SK"),
            F.col("CLM_ID").alias("CLM_ID"),
            F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
            F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AltProcCdSk").alias("ALT_PROC_CD_SK"),
            F.col("ClmLnSk").alias("CLM_LN_SK"),
            F.col("DntlClmLnAltProcExcdSk").alias("DNTL_CLM_LN_ALT_PROC_EXCD_SK"),
            F.col("DntlClmLnBnfTypCdSk").alias("DNTL_CLM_LN_BNF_TYP_CD_SK"),
            F.col("DntlCatCdSk").alias("DNTL_CLM_LN_DNTL_CAT_CD_SK"),
            F.col("DntlCatRuleCdSk").alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD_SK"),
            F.col("DntlClmLnUtilEditCdSk").alias("DNTL_CLM_LN_UTIL_EDIT_CD_SK"),
            F.col("CAT_PAYMT_PFX_ID").alias("CAT_PAYMT_PFX_ID"),
            F.col("DNTL_PROC_PAYMT_PFX_ID").alias("DNTL_PROC_PAYMT_PFX_ID"),
            F.col("DNTL_PROC_PRICE_ID").alias("DNTL_PROC_PRICE_ID"),
            F.col("TOOTH_BEG_NO").alias("TOOTH_BEG_NO"),
            F.col("TOOTH_END_NO").alias("TOOTH_END_NO"),
            F.col("TOOTH_NO").alias("TOOTH_NO"),
            F.col("DNTL_CLM_LN_TOOTH_SRFC_CD").alias("TOOTH_SRFC_TX")
        )
        .withColumnRenamed("DNTL_CLM_LN_DNTL_CAT_RULE_CD_SK", "DNTL_CLM_LN_DNTLCAT_RULE_CD_SK")
)

df_foreignkey_recycle = (
    df_foreignkey_vars
        .filter(F.col("ErrCount") > 0)
        .select(
            GetRecycleKey(F.col("DNTL_CLM_LN_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("DISCARD_IN").alias("DISCARD_IN"),
            F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("ErrCount").alias("ERR_CT"),
            (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
            F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("DNTL_CLM_LN_SK").alias("DNTL_CLM_LN_SK"),
            F.col("CLM_ID").alias("CLM_ID"),
            F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
            F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("ALT_PROC_CD").alias("ALT_PROC_CD"),
            F.col("DNTL_CLM_LN_DNTL_CAT_CD").alias("DNTL_CLM_LN_DNTL_CAT_CD"),
            F.col("DNTL_CLM_LN_DNTL_CAT_RULE_CD").alias("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
            F.col("DNTL_CLM_LN_ALT_PROC_EXCD").alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
            F.col("DNTL_CLM_LN_BNF_TYP_CD").alias("DNTL_CLM_LN_BNF_TYP_CD"),
            F.col("DNTL_CLM_LN_TOOTH_SRFC_CD").alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
            F.col("DNTL_CLM_LN_UTIL_EDIT_CD").alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
            F.col("CAT_PAYMT_PFX_ID").alias("CAT_PAYMT_PFX_ID"),
            F.col("DNTL_PROC_PAYMT_PFX_ID").alias("DNTL_PROC_PAYMT_PFX_ID"),
            F.col("DNTL_PROC_PRICE_ID").alias("DNTL_PROC_PRICE_ID"),
            F.col("TOOTH_BEG_NO").alias("TOOTH_BEG_NO"),
            F.col("TOOTH_END_NO").alias("TOOTH_END_NO"),
            F.col("TOOTH_NO").alias("TOOTH_NO")
        )
)

df_foreignkey_defaultunk = spark.createDataFrame(
    [
        (
            0, 
            0, 
            "UNK",
            0, 
            0, 
            0, 
            0, 
            0, 
            0, 
            0,
            0,
            0,
            0,
            "UNK",
            "UNK",
            "UNK",
            "UN",
            "UN",
            "UN",
            "UNK"
        )
    ],
    [
        "DNTL_CLM_LN_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_PROC_CD_SK",
        "CLM_LN_SK",
        "DNTL_CLM_LN_DNTL_CAT_CD_SK",
        "DNTL_CLM_LN_DNTL_CAT_RULE_CD_SK",
        "DNTL_CLM_LN_ALT_PROC_EXCD_SK",
        "DNTL_CLM_LN_BNF_TYP_CD_SK",
        "DNTL_CLM_LN_UTIL_EDIT_CD_SK",
        "CAT_PAYMT_PFX_ID",
        "DNTL_PROC_PAYMT_PFX_ID",
        "DNTL_PROC_PRICE_ID",
        "TOOTH_BEG_NO",
        "TOOTH_END_NO",
        "TOOTH_NO",
        "TOOTH_SRFC_TX"
    ]
).withColumnRenamed("DNTL_CLM_LN_DNTL_CAT_RULE_CD_SK", "DNTL_CLM_LN_DNTLCAT_RULE_CD_SK")

df_foreignkey_defaultna = spark.createDataFrame(
    [
        (
            1,
            1,
            "NA",
            0,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            1,
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA",
            "NA"
        )
    ],
    [
        "DNTL_CLM_LN_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_LN_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "ALT_PROC_CD_SK",
        "CLM_LN_SK",
        "DNTL_CLM_LN_DNTL_CAT_CD_SK",
        "DNTL_CLM_LN_DNTL_CAT_RULE_CD_SK",
        "DNTL_CLM_LN_ALT_PROC_EXCD_SK",
        "DNTL_CLM_LN_BNF_TYP_CD_SK",
        "DNTL_CLM_LN_UTIL_EDIT_CD_SK",
        "CAT_PAYMT_PFX_ID",
        "DNTL_PROC_PAYMT_PFX_ID",
        "DNTL_PROC_PRICE_ID",
        "TOOTH_BEG_NO",
        "TOOTH_END_NO",
        "TOOTH_NO",
        "TOOTH_SRFC_TX"
    ]
).withColumnRenamed("DNTL_CLM_LN_DNTL_CAT_RULE_CD_SK", "DNTL_CLM_LN_DNTLCAT_RULE_CD_SK")

df_foreignkey_recycle_clms = (
    df_foreignkey_vars
        .filter(F.col("ErrCount") > 0)
        .select(
            F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("CLM_ID").alias("CLM_ID")
        )
)

df_hf_recycle = df_foreignkey_recycle.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("DNTL_CLM_LN_SK"),
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.rpad(F.col("ALT_PROC_CD"), 5, " ").alias("ALT_PROC_CD"),
    F.col("DNTL_CLM_LN_DNTL_CAT_CD"),
    F.col("DNTL_CLM_LN_DNTL_CAT_RULE_CD"),
    F.rpad(F.col("DNTL_CLM_LN_ALT_PROC_EXCD"), 3, " ").alias("DNTL_CLM_LN_ALT_PROC_EXCD"),
    F.rpad(F.col("DNTL_CLM_LN_BNF_TYP_CD"), 2, " ").alias("DNTL_CLM_LN_BNF_TYP_CD"),
    F.rpad(F.col("DNTL_CLM_LN_TOOTH_SRFC_CD"), 10, " ").alias("DNTL_CLM_LN_TOOTH_SRFC_CD"),
    F.rpad(F.col("DNTL_CLM_LN_UTIL_EDIT_CD"), 3, " ").alias("DNTL_CLM_LN_UTIL_EDIT_CD"),
    F.rpad(F.col("CAT_PAYMT_PFX_ID"), 4, " ").alias("CAT_PAYMT_PFX_ID"),
    F.rpad(F.col("DNTL_PROC_PAYMT_PFX_ID"), 4, " ").alias("DNTL_PROC_PAYMT_PFX_ID"),
    F.rpad(F.col("DNTL_PROC_PRICE_ID"), 4, " ").alias("DNTL_PROC_PRICE_ID"),
    F.rpad(F.col("TOOTH_BEG_NO"), 2, " ").alias("TOOTH_BEG_NO"),
    F.rpad(F.col("TOOTH_END_NO"), 2, " ").alias("TOOTH_END_NO"),
    F.rpad(F.col("TOOTH_NO"), 2, " ").alias("TOOTH_NO")
)

write_files(
    df_hf_recycle,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_hf_claim_recycle_keys = df_foreignkey_recycle_clms.select(
    F.rpad(F.col("SRC_SYS_CD"), 1, " ").alias("SRC_SYS_CD"),
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID")
)

write_files(
    df_hf_claim_recycle_keys,
    "hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

df_collector = df_foreignkey_defaultunk.unionByName(df_foreignkey_defaultna).unionByName(df_foreignkey_fkey)

df_final = df_collector.select(
    F.col("DNTL_CLM_LN_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("ALT_PROC_CD_SK"),
    F.col("CLM_LN_SK"),
    F.col("DNTL_CLM_LN_ALT_PROC_EXCD_SK"),
    F.col("DNTL_CLM_LN_BNF_TYP_CD_SK"),
    F.col("DNTL_CLM_LN_DNTL_CAT_CD_SK"),
    F.col("DNTL_CLM_LN_DNTLCAT_RULE_CD_SK"),
    F.col("DNTL_CLM_LN_UTIL_EDIT_CD_SK"),
    F.rpad(F.col("CAT_PAYMT_PFX_ID"), 4, " ").alias("CAT_PAYMT_PFX_ID"),
    F.rpad(F.col("DNTL_PROC_PAYMT_PFX_ID"), 4, " ").alias("DNTL_PROC_PAYMT_PFX_ID"),
    F.rpad(F.col("DNTL_PROC_PRICE_ID"), 4, " ").alias("DNTL_PROC_PRICE_ID"),
    F.rpad(F.col("TOOTH_BEG_NO"), 2, " ").alias("TOOTH_BEG_NO"),
    F.rpad(F.col("TOOTH_END_NO"), 2, " ").alias("TOOTH_END_NO"),
    F.rpad(F.col("TOOTH_NO"), 2, " ").alias("TOOTH_NO"),
    F.rpad(F.col("TOOTH_SRFC_TX"), 10, " ").alias("TOOTH_SRFC_TX")
)

write_files(
    df_final,
    f"DNTL_CLM_LN.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)