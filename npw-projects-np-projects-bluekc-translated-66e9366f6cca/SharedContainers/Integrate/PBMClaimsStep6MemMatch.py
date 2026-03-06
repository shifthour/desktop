# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
PBMClaimsStep6MemMatch
Copyright 2010 Blue Cross/Blue Shield of Kansas City

CALLED BY : SavRxClmPreProcExtr, MedtrakDrgClmPreProcExtr and LDIDrugClmPreProcExtr

DESCRIPTION:
    Member Match Step for handling Baby Twins scenario for all PBMs i.e., SAVRX, LDI and Medtrak

MODIFICATIONS:
Developer         Date         Project/Altiris #   Change Description
----------------  -----------  ------------------   -------------------------------------------------
Kaushik Kapoor    2018-05-08   5828                Original Programming to handle BabyBoy scenarios
"""

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as F


def run_PBMClaimsStep6MemMatch(
    df_Step6MemMatch: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack required parameters once
    # ------------------------------------------------------------------
    adls_path = params["adls_path"]

    # ------------------------------------------------------------------
    # Stage: Prep_Data (Transformer)
    # ------------------------------------------------------------------
    w_rownum = Window.orderBy(F.monotonically_increasing_id())
    df_prep = (
        df_Step6MemMatch
        .withColumn("ROWNUM", F.row_number().over(w_rownum))
    )

    df_allRow1 = (
        df_prep.select(
            "ROWNUM",
            "SUB_SSN",
            "GRP_ID",
            "MBR_BRTH_DT_SK",
            "GNDR_CD",
            "MBR_FIRST_NM",
            "MBR_LAST_NM",
            "SUB_FIRST_NM",
            "SUB_LAST_NM",
            "MBR_UNIQ_KEY",
            "MBR_SSN",
            "SUB_BRTH_DT_SK",
            "SUB_ID",
            "MBR_SFX_NO",
            "PBM_GRP_ID"
        )
    )

    df_agg_input = df_prep.select(
        "ROWNUM",
        "SUB_SSN",
        "GRP_ID",
        "MBR_BRTH_DT_SK",
        "GNDR_CD"
    )

    # ------------------------------------------------------------------
    # Stage: Agg_Count_Rows (Aggregator)
    # ------------------------------------------------------------------
    df_rowcnt = (
        df_agg_input
        .groupBy(
            "SUB_SSN",
            "GRP_ID",
            "MBR_BRTH_DT_SK",
            "GNDR_CD"
        )
        .agg(F.count("*").alias("CNT"))
    )

    # ------------------------------------------------------------------
    # Replace intermediate hashed file hf_PBM_clm_preproc_Step6AllRows
    # ------------------------------------------------------------------
    df_allRow_dedup = dedup_sort(
        df_allRow1,
        ["SUB_SSN", "GRP_ID", "MBR_BRTH_DT_SK", "GNDR_CD", "ROWNUM"],
        [("ROWNUM", "A")]
    )

    # ------------------------------------------------------------------
    # Replace intermediate hashed file hf_PBM_clm_preproc_Step6CntRows
    # ------------------------------------------------------------------
    df_rowcnt_dedup = dedup_sort(
        df_rowcnt,
        ["SUB_SSN", "GRP_ID", "MBR_BRTH_DT_SK", "GNDR_CD"],
        []
    )

    # ------------------------------------------------------------------
    # Stage: Lkp (Transformer / Lookup)
    # ------------------------------------------------------------------
    df_final = (
        df_allRow_dedup.alias("AllRow")
        .join(
            df_rowcnt_dedup.alias("Cnt"),
            on=[
                "SUB_SSN",
                "GRP_ID",
                "MBR_BRTH_DT_SK",
                "GNDR_CD"
            ],
            how="left"
        )
        .select(
            F.col("AllRow.SUB_SSN"),
            F.col("AllRow.MBR_BRTH_DT_SK"),
            F.col("AllRow.GNDR_CD"),
            F.col("AllRow.MBR_FIRST_NM"),
            F.col("AllRow.MBR_LAST_NM"),
            F.col("AllRow.SUB_FIRST_NM"),
            F.col("AllRow.SUB_LAST_NM"),
            F.col("AllRow.MBR_UNIQ_KEY"),
            F.col("AllRow.MBR_SSN"),
            F.col("AllRow.SUB_BRTH_DT_SK"),
            F.col("AllRow.GRP_ID"),
            F.col("AllRow.SUB_ID"),
            F.col("AllRow.MBR_SFX_NO"),
            F.col("AllRow.PBM_GRP_ID"),
            F.col("Cnt.CNT")
        )
    )

    # ------------------------------------------------------------------
    # Stage: hf_PBM_clm_preproc_step6_FinalOP (Parquet implementation)
    # ------------------------------------------------------------------
    write_files(
        df_final,
        f"{adls_path}/PBMClaimsStep6MemMatch_Step6Final.parquet",
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )

    # ------------------------------------------------------------------
    # Container Output: Step6Final
    # ------------------------------------------------------------------
    return df_final