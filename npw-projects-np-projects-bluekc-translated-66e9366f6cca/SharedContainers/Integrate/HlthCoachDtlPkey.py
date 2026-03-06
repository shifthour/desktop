
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
HlthCoachDtlPkey  –  Shared Container
DESCRIPTION:
    Takes the sorted file and applies the primary key. Preps the file for the foreign-key lookup process that follows.
INPUTS:
    Transformer link  :  Transform
    Dummy table       :  dummy_hf_hlth_coach_dtl   (replaces hashed file hf_hlth_coach_dtl)
OUTPUTS:
    Link              :  Load
MODIFICATIONS:
    Steph Goddard 03/22/2004  -   Originally Programmed.
    Steph Goddard 02/07/2007  -   Changed documentation, added PCTAClmDiagExtr
    Kalyan Neelam  2010-07-01 -   Put HLTH_COACH_DTL_SK as PK SK and assigned CurrRunCycle to CRT_RUN_CYC_EXCTN_SK
ANNOTATIONS:
    This container is used in:
        NewDirCoachDtlExtr
        ……Extr
    These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_HlthCoachDtlPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack required runtime parameters
    CurrRunCycle            = params["CurrRunCycle"]
    jdbc_url                = params["jdbc_url"]
    jdbc_props              = params["jdbc_props"]
    # ------------------------------------------------------------------
    # Read dummy table that replaces hashed file hf_hlth_coach_dtl
    lkup_query = """
        SELECT
            SRC_SYS_CD                 AS SRC_SYS_CD_lkp,
            MBR_UNIQ_KEY               AS MBR_UNIQ_KEY_lkp,
            HLTH_COACH_ID              AS HLTH_COACH_ID_lkp,
            CNTCT_DT_SK                AS CNTCT_DT_SK_lkp,
            CNTCT_TM                   AS CNTCT_TM_lkp,
            INTERACTN_TYP             AS INTERACTN_TYP_lkp,
            CRT_RUN_CYC_EXCTN_SK       AS CRT_RUN_CYC_EXCTN_SK_lkp,
            HLTH_COACH_DTL_SK          AS HLTH_COACH_DTL_SK_lkp
        FROM dummy_hf_hlth_coach_dtl
    """
    df_lkup = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("query", lkup_query)
            .load()
    )
    # ------------------------------------------------------------------
    # Join input with lookup
    join_cond = (
        (F.col("df_Transform.SRC_SYS_CD")     == F.col("SRC_SYS_CD_lkp"))     &
        (F.col("df_Transform.MBR_UNIQ_KEY")   == F.col("MBR_UNIQ_KEY_lkp"))   &
        (F.col("df_Transform.HLTH_COACH_ID")  == F.col("HLTH_COACH_ID_lkp"))  &
        (F.col("df_Transform.CNTCT_DT")       == F.col("CNTCT_DT_SK_lkp"))    &
        (F.col("df_Transform.CNTCT_TM")       == F.col("CNTCT_TM_lkp"))       &
        (F.col("df_Transform.INTERACTN_TYP")  == F.col("INTERACTN_TYP_lkp"))
    )
    df_enriched = (
        df_Transform.alias("df_Transform")
        .join(df_lkup.alias("lkup"), join_cond, "left")
    )
    # ------------------------------------------------------------------
    # Lookup status (NOTFOUND)
    df_enriched = df_enriched.withColumn(
        "lkup_NOTFOUND",
        F.col("HLTH_COACH_DTL_SK_lkp").isNull()
    )
    # ------------------------------------------------------------------
    # Primary-key logic
    df_enriched = df_enriched.withColumn(
        "SK",
        F.col("HLTH_COACH_DTL_SK_lkp")
    )
    # Populate surrogate keys where null
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)
    # ------------------------------------------------------------------
    # CRT_RUN_CYC_EXCTN_SK determination
    df_enriched = df_enriched.withColumn(
        "NewCrtRunCycExtcnSk",
        F.when(F.col("lkup_NOTFOUND"), F.lit(CurrRunCycle)) \
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_lkp"))
    )
    # ------------------------------------------------------------------
    # Prepare Load output stream
    df_Load = (
        df_enriched.select(
            F.col("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("INSRT_UPDT_CD"),
            F.col("DISCARD_IN"),
            F.col("PASS_THRU_IN"),
            F.col("FIRST_RECYC_DT"),
            F.col("ERR_CT"),
            F.col("RECYCLE_CT"),
            F.col("SRC_SYS_CD"),
            F.col("PRI_KEY_STRING"),
            F.col("SK").alias("HLTH_COACH_DTL_SK"),
            F.col("MBR_UNIQ_KEY"),
            F.col("HLTH_COACH_ID"),
            F.col("CNTCT_DT").alias("CNTCT_DT_SK"),
            F.col("CNTCT_TM"),
            F.col("INTERACTN_TYP"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("CNTCT_RSLT_ACTION"),
            F.col("CNTCT_RSLT"),
            F.col("GRP_ID"),
            F.col("CNTCT_MIN_NO")
        )
    )
    # ------------------------------------------------------------------
    # Prepare updt stream (rows not found in lookup) and write to dummy table
    df_updt = (
        df_enriched.filter(F.col("lkup_NOTFOUND"))
        .select(
            F.col("SRC_SYS_CD"),
            F.col("MBR_UNIQ_KEY"),
            F.col("HLTH_COACH_ID"),
            F.col("CNTCT_DT").alias("CNTCT_DT_SK"),
            F.col("CNTCT_TM"),
            F.col("INTERACTN_TYP"),
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("HLTH_COACH_SK")
        )
    )
    (
        df_updt.write.mode("append")
        .format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", "dummy_hf_hlth_coach_dtl")
        .save()
    )
    # ------------------------------------------------------------------
    return df_Load
