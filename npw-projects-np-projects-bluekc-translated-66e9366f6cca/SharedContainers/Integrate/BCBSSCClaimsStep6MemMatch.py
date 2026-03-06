
# Databricks notebook cell
# MAGIC %run ./Utility
# COMMAND ----------
# Databricks notebook cell
# MAGIC %run ./Routine_Functions
# COMMAND ----------
# Databricks notebook cell
"""
Job Name      : BCBSSCClaimsStep6MemMatch
Job Type      : Server Job
Category      : DS_Integrate
Folder Path   : Shared Containers

Description
-----------
Copyright 2010 Blue Cross/Blue Shield of Kansas City

CALLED BY : BCBSSCClaimsMemberMatch

DESCRIPTION:  BCBSSC Claim pre-processing. Handles the Baby Boy/Girl scenario for South Carolina.

MODIFICATIONS:
Developer        Date        Project/Altiris #    Change Description
---------------  ----------  -------------------  ----------------------------------------------
Kaushik Kapoor   2018-05-09  5828                 Original Programming to handle BabyBoy scenario
"""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F

# Databricks notebook cell
def run_BCBSSCClaimsStep6MemMatch(
    df_Step6MemMatch: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-container logic translated from IBM DataStage job 'BCBSSCClaimsStep6MemMatch'.

    Parameters
    ----------
    df_Step6MemMatch : DataFrame
        Container input link 'Step6MemMatch'.
    params : dict
        Runtime parameters dictionary.

    Returns
    -------
    DataFrame
        Container output link 'Step6Final'.
    """

    # --- Preparations --------------------------------------------------------
    # (no external parameters are referenced by this container)

    # ------------------------------------------------------------------------
    # Stage: Prep_Data  (Transformer)
    # ------------------------------------------------------------------------
    # Add a row number equivalent to @OUTROWNUM to mimic DataStage generator
    w_global = Window.orderBy(F.lit(1))
    df_all_row = (
        df_Step6MemMatch
        .withColumn("ROWNUM", F.row_number().over(w_global))
        .select(
            "ROWNUM",
            "SUB_SK",
            "MBR_BRTH_DT_SK",
            "GNDR_CD",
            "SUB_ID",
            "PATN_SSN",
            "MBR_FIRST_NM",
            "MBR_LAST_NM",
            "MBR_UNIQ_KEY",
            "GRP_ID",
            "MBR_SFX_NO",
            "SUB_SSN"
        )
    )

    df_agg_src = (
        df_Step6MemMatch
        .withColumn("ROWNUM", F.row_number().over(w_global))
        .select(
            "ROWNUM",
            "SUB_SK",
            "MBR_BRTH_DT_SK",
            "GNDR_CD"
        )
    )

    # ------------------------------------------------------------------------
    # Stage: Agg_Count_Rows (Aggregator)
    # ------------------------------------------------------------------------
    df_cnt = (
        df_agg_src
        .groupBy("SUB_SK", "MBR_BRTH_DT_SK", "GNDR_CD")
        .agg(F.count("*").alias("CNT"))
    )

    # ------------------------------------------------------------------------
    # Hash-file: hf_BCBSSC_clm_preproc_Step6AllRows  (scenario a)
    # ------------------------------------------------------------------------
    df_all_row_dedup = dedup_sort(
        df_all_row,
        ["ROWNUM", "SUB_SK", "MBR_BRTH_DT_SK", "GNDR_CD"],
        [("ROWNUM", "A")]
    )

    # ------------------------------------------------------------------------
    # Hash-file: hf_BCBSSC_clm_preproc_Step6CntRows  (scenario a)
    # ------------------------------------------------------------------------
    df_cnt_dedup = dedup_sort(
        df_cnt,
        ["SUB_SK", "MBR_BRTH_DT_SK", "GNDR_CD"],
        [("CNT", "D")]
    )

    # ------------------------------------------------------------------------
    # Stage: Lkp (Transformer) – join AllRow with Cnt
    # ------------------------------------------------------------------------
    df_join = (
        df_all_row_dedup.alias("AllRow")
        .join(
            df_cnt_dedup.alias("Cnt"),
            on=["SUB_SK", "MBR_BRTH_DT_SK", "GNDR_CD"],
            how="left"
        )
    )

    df_final = df_join.select(
        "SUB_SK",
        "MBR_BRTH_DT_SK",
        "GNDR_CD",
        "SUB_ID",
        "PATN_SSN",
        "MBR_FIRST_NM",
        "MBR_LAST_NM",
        "MBR_UNIQ_KEY",
        "GRP_ID",
        "MBR_SFX_NO",
        "SUB_SSN",
        "CNT"
    )

    # ------------------------------------------------------------------------
    # Hash-file: hf_BCBSSC_clm_preproc_step6_FinalOP (scenario a)
    # ------------------------------------------------------------------------
    df_Step6Final = dedup_sort(
        df_final,
        ["SUB_SK", "MBR_BRTH_DT_SK", "GNDR_CD"],
        [("CNT", "D")]
    )

    return df_Step6Final
# COMMAND ----------
