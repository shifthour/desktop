# Databricks notebook cell
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F

def run_HlthCoachPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Shared Container: HlthCoachPkey
    DESCRIPTION:
        HlthCoachSumPkey – Used in NewDirHlthCoachSumExtr
    MODIFICATIONS:
        Bhoomi Dasari – 12/07/2007 – Initial program
        Kalyan Neelam – 2010-07-01 – Added HLTH_COACH_SK logic
    """
    # --------------------------------------------------
    # Unpack parameters
    CurrRunCycle       = params["CurrRunCycle"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    # --------------------------------------------------
    # Read dummy table replacing hashed file hf_hlth_coach
    extract_query = """
        SELECT
            SRC_SYS_CD             as SRC_SYS_CD_lkp,
            MBR_UNIQ_KEY           as MBR_UNIQ_KEY_lkp,
            HLTH_COACH_ID          as HLTH_COACH_ID_lkp,
            HLTH_COACH_STTUS_CD    as HLTH_COACH_STTUS_CD_lkp,
            CRT_RUN_CYC_EXCTN_SK   as CRT_RUN_CYC_EXCTN_SK_lkp,
            HLTH_COACH_SK          as HLTH_COACH_SK_lkp
        FROM dummy_hf_hlth_coach
    """
    df_hf_hlth_coach = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    # --------------------------------------------------
    # Join lookup with incoming stream
    join_expr = (
        (F.col("transform.SRC_SYS_CD")          == F.col("SRC_SYS_CD_lkp")) &
        (F.col("transform.MBR_UNIQ_KEY")        == F.col("MBR_UNIQ_KEY_lkp")) &
        (F.col("transform.HLTH_COACH_ID")       == F.col("HLTH_COACH_ID_lkp")) &
        (F.col("transform.HLTH_COACH_STTUS_CD") == F.col("HLTH_COACH_STTUS_CD_lkp"))
    )
    df_join = (
        df_Transform.alias("transform")
        .join(df_hf_hlth_coach.alias("lkup"), join_expr, "left")
    )
    # --------------------------------------------------
    # Enrich with derived columns
    df_enriched = (
        df_join
        .withColumn("NewCrtRunCycExtcnSk",
            F.when(F.col("HLTH_COACH_SK_lkp").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn("SK",
            F.when(F.col("HLTH_COACH_SK_lkp").isNull(), F.lit(None))
             .otherwise(F.col("HLTH_COACH_SK_lkp"))
        )
    )
    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)
    # --------------------------------------------------
    # Prepare update rows for dummy table
    df_updt = (
        df_enriched
        .filter(F.col("HLTH_COACH_SK_lkp").isNull())
        .select(
            F.trim(F.col("transform.SRC_SYS_CD")).alias("SRC_SYS_CD"),
            F.col("transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            F.trim(F.col("transform.HLTH_COACH_ID")).alias("HLTH_COACH_ID"),
            F.trim(F.col("transform.HLTH_COACH_STTUS_CD")).alias("HLTH_COACH_STTUS_CD"),
            F.lit(CurrRunCycle).cast("long").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("HLTH_COACH_SK")
        )
    )
    # Write updates back to dummy table
    (
        df_updt.write.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", "dummy_hf_hlth_coach")
            .mode("append")
            .save()
    )
    # --------------------------------------------------
    # Build output link 'Key'
    df_key = (
        df_enriched.select(
            F.col("transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("transform.DISCARD_IN").alias("DISCARD_IN"),
            F.col("transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("transform.ERR_CT").alias("ERR_CT"),
            F.col("transform.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("SK").alias("HLTH_COACH_SK"),
            F.col("transform.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
            F.col("transform.HLTH_COACH_ID").alias("HLTH_COACH_ID"),
            F.col("transform.HLTH_COACH_STTUS_CD").alias("HLTH_COACH_STTUS_CD"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).cast("long").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("transform.GRP_ID").alias("GRP_ID"),
            F.col("transform.MBR_CK").alias("MBR_CK"),
            F.col("transform.HLTH_COACH_ABILITY_CHG_CD").alias("HLTH_COACH_ABILITY_CHG_CD"),
            F.col("transform.HLTH_COACH_ABILITY_COPE_CD").alias("HLTH_COACH_ABILITY_COPE_CD"),
            F.col("transform.HLTH_COACH_ABSENT_CD").alias("HLTH_COACH_ABSENT_CD"),
            F.col("transform.HLTH_COACH_BP_CD").alias("HLTH_COACH_BP_CD"),
            F.col("transform.HLTH_COACH_BMI_CD").alias("HLTH_COACH_BMI_CD"),
            F.col("transform.HLTH_COACH_CHLSTRL_CD").alias("HLTH_COACH_CHLSTRL_CD"),
            F.col("transform.HLTH_COACH_CLSD_RSN_CD").alias("HLTH_COACH_CLSD_RSN_CD"),
            F.col("transform.HLTH_COACH_XPCT_FTR_HLTH_CD").alias("HLTH_COACH_XPCT_FTR_HLTH_CD"),
            F.col("transform.HLTH_COACH_HLTH_ENRGC_HM_CD").alias("HLTH_COACH_HLTH_ENRGC_HM_CD"),
            F.col("transform.HLTH_COACH_HLTH_ENRGC_WK_CD").alias("HLTH_COACH_HLTH_ENRGC_WK_CD"),
            F.col("transform.HLTH_COACH_HDL_CD").alias("HLTH_COACH_HDL_CD"),
            F.col("transform.HLTH_COACH_LIFE_CD").alias("HLTH_COACH_LIFE_CD"),
            F.col("transform.HLTH_COACH_PHYSCL_ACTVTY_CD").alias("HLTH_COACH_PHYSCL_ACTVTY_CD"),
            F.col("transform.HLTH_COACH_PHYSCL_HLTH_CD").alias("HLTH_COACH_PHYSCL_HLTH_CD"),
            F.col("transform.HLTH_COACH_RFRL_CD").alias("HLTH_COACH_RFRL_CD"),
            F.col("transform.HLTH_COACH_SMOKE_CD").alias("HLTH_COACH_SMOKE_CD"),
            F.col("transform.HLTH_COACH_STRESS_CD").alias("HLTH_COACH_STRESS_CD"),
            F.col("transform.HLTH_COACH_SBSTNC_ABUSE_CD").alias("HLTH_COACH_SBSTNC_ABUSE_CD"),
            F.col("transform.INIT_CNTCT_DT_SK").alias("INIT_CNTCT_DT_SK"),
            F.col("transform.INTIAL_CNTCT_TM").alias("INTIAL_CNTCT_TM"),
            F.col("transform.INIT_ENR_DT_SK").alias("INIT_ENR_DT_SK"),
            F.col("transform.LAST_CNTCT_DT_SK").alias("LAST_CNTCT_DT_SK"),
            F.col("transform.LAST_CNTCT_TM").alias("LAST_CNTCT_TM"),
            F.col("transform.CNTCT_SESS_CT").alias("CNTCT_SESS_CT")
        )
    )
    return df_key