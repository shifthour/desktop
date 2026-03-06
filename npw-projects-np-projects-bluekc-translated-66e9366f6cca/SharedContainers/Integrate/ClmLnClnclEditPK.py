# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ---------- 

"""
Shared Container: ClmLnClnclEditPK
Description:
    Takes the sorted file and applies the primary key. Preps the file for the foreign key lookup process that follows.

    Inputs:
        Sequential file in common record format created in transform job (link: Transform)

    Hash Files:
        hf_clm_ln – used as primary-key lookup only (read-only)

    Outputs:
        Data stream on link Key in common record format.

Annotations:
    This container is used in:
        FctsClmLnClnclEditExtr
        NascoClmLnClnclEditTrns

    This job uses hf_clm_ln as the primary key hash file since it has the same natural key as the CLM_LN table

    Primary key hash file created and loaded in FctsClmLnPKExtr, NascoClmLnPKExtr, PCSClmLnPKExtr and ArgusClmLnPKExtr processes
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def run_ClmLnClnclEditPK(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Parameters
    ----------
    df_Transform : DataFrame
        Incoming DataFrame mapped to the 'Transform' input link of the container.
    params : dict
        Runtime parameters dictionary.

    Returns
    -------
    DataFrame
        DataFrame produced on the 'Key' output link.
    """

    # ------------------------------------------------------------------
    # Unpack required parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    adls_path    = params["adls_path"]

    # ------------------------------------------------------------------
    # Read the hash-file lookup as Parquet (Scenario c)
    # ------------------------------------------------------------------
    df_lnkRef = (
        spark.read
             .parquet(f"{adls_path}/ClmLnClnclEditPK_lnkRef.parquet")
    )

    # ------------------------------------------------------------------
    # Perform the lookup join
    # ------------------------------------------------------------------
    join_cond = (
        F.trim(df_Transform.SRC_SYS_CD) == F.trim(df_lnkRef.SRC_SYS_CD)
    ) & (
        df_Transform.CLM_ID == df_lnkRef.CLM_ID
    ) & (
        df_Transform.CLM_LN_SEQ_NO == df_lnkRef.CLM_LN_SEQ_NO
    )

    df_joined = (
        df_Transform.alias("t")
        .join(df_lnkRef.alias("l"), join_cond, "left")
    )

    # ------------------------------------------------------------------
    # Derive transformation columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn(
            "SK",
            F.when(F.col("l.CLM_LN_SK").isNull(), F.lit(0))
             .otherwise(F.col("l.CLM_LN_SK"))
        )
        .withColumn(
            "NewCrtRunCcyExctnSK",
            F.when(F.col("l.CLM_LN_SK").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("l.CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # ------------------------------------------------------------------
    # Select and rename columns for the output link 'Key'
    # ------------------------------------------------------------------
    df_key = (
        df_enriched.select(
            F.col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("t.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("t.DISCARD_IN").alias("DISCARD_IN"),
            F.col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("t.ERR_CT").alias("ERR_CT"),
            F.col("t.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("SK").alias("CLM_LN_CLNCL_EDIT_SK"),
            F.col("t.CLM_ID").alias("CLM_ID"),
            F.col("t.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
            F.col("NewCrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("t.CLM_LN_CLNCL_EDIT_ACTN_CD").alias("CLM_LN_CLNCL_EDIT_ACTN_CD"),
            F.col("t.CLM_LN_CLNCL_EDIT_FMT_CHG_CD").alias("CLM_LN_CLNCL_EDIT_FMT_CHG_CD"),
            F.col("t.CLM_LN_CLNCL_EDIT_TYP_CD").alias("CLM_LN_CLNCL_EDIT_TYP_CD"),
            F.col("t.COMBND_CHRG_IN").alias("COMBND_CHRG_IN"),
            F.col("t.REF_CLM_ID").alias("REF_CLM_ID"),
            F.col("t.REF_CLM_LN_SEQ_NO").alias("REF_CLM_LN_SEQ_NO"),
            F.col("t.CLM_LN_CLNCL_EDIT_EXCD_TYP_CD").alias("CLM_LN_CLNCL_EDIT_EXCD_TYP_CD"),
        )
    )

    return df_key