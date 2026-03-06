# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: ClmLnRemitPK
Description:
    * VC LOGS *
    ^1_2 09/22/09 16:18:16 Batch  15241_58707 PROMOTE bckcett:31540 ids20 u10157 sa cuase this is third time trying
    ^1_2 09/22/09 16:15:40 Batch  15241_58542 PROMOTE bckcett:31540 testIDSnew u10157 sa
    ^1_2 09/22/09 16:12:55 Batch  15241_58378 INIT bckcett:31540 devlIDSnew u10157 sa
    ^1_1 09/21/09 12:16:59 Batch  15240_44229 INIT bckcett:31540 devlIDSnew u10157 sa for 2009-09-21 prod deployment
    ^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
    ^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
    ^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
    ^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
    ^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
    ^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy
    ^1_1 08/22/08 13:20:37 Batch  14845_48040 PROMOTE bckcetl ids20 dsadm bls for sa
    ^1_1 08/22/08 13:18:29 Batch  14845_47911 INIT bckcett testIDS dsadm bls for sa
    ^1_1 08/19/08 10:51:21 Batch  14842_39088 PROMOTE bckcett testIDS u03651 steph for Sharon 3057
    ^1_1 08/19/08 10:50:43 Batch  14842_39046 INIT bckcett devlIDSnew u03651 steffy

    ***************************************************************************************************************************************************************
    COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

    DESCRIPTION:   Shared container used for Primary Keying of Claim Line Remit job

    CALLED BY : BcbsClmLnRemitExtr

    PROCESSING:

    MODIFICATIONS:
    Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed
    -----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------
    Parik                 2008-08-06               Initial program                                                                             3057(Web Claim)               devlIDSnew     Steph Goddard        08/11/2008
Annotations:
    Primary Key for Claim Line Remit
    This container is used in:

    BcbsClmLnRemitExtr

    These programs need to be re-compiled when logic changes
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit


def run_ClmLnRemitPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Executes the logic for the shared container 'ClmLnRemitPK'.

    Parameters
    ----------
    df_Transform : DataFrame
        Incoming records that need Claim Line Remit primary-key handling.
    params : dict
        Runtime parameters and environmental settings.

    Returns
    -------
    DataFrame
        DataFrame corresponding to the container output link 'Fkey'.
    """

    # ------------------------------------------------------------------
    # Unpack required runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # ------------------------------------------------------------------
    # Read lookup parquet corresponding to hashed file 'hf_clm_ln'
    # Scenario: c  (translated to parquet lookup)
    # ------------------------------------------------------------------
    df_lkup_raw = (
        spark.read.parquet(f"{adls_path}/ClmLnRemitPK_lkup.parquet")
    )

    # Deduplicate keyed lookup data to mimic hashed-file uniqueness
    df_lkup = dedup_sort(
        df_lkup_raw,
        partition_cols=["SRC_SYS_CD", "CLM_ID", "CLM_LN_SEQ_NO"],
        sort_cols=[("CRT_RUN_CYC_EXCTN_SK", "D")]
    )

    # ------------------------------------------------------------------
    # Join incoming records with lookup
    # ------------------------------------------------------------------
    df_enriched = (
        df_Transform.alias("Transform")
        .join(
            df_lkup.alias("lkup"),
            on=[
                col("Transform.SRC_SYS_CD") == col("lkup.SRC_SYS_CD"),
                col("Transform.CLM_ID") == col("lkup.CLM_ID"),
                col("Transform.CLM_LN_SEQ_NO") == col("lkup.CLM_LN_SEQ_NO")
            ],
            how="left"
        )
    )

    # ------------------------------------------------------------------
    # Derive transformation columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_enriched
        .withColumn(
            "SK",
            when(col("lkup.CLM_LN_SK").isNotNull(), col("lkup.CLM_LN_SK")).otherwise(lit(0))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("lkup.CLM_LN_SK").isNull(), lit(CurrRunCycle)).otherwise(col("lkup.CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # ------------------------------------------------------------------
    # Build output DataFrame for link 'Fkey'
    # ------------------------------------------------------------------
    df_Fkey = (
        df_enriched.select(
            col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("Transform.DISCARD_IN").alias("DISCARD_IN"),
            col("Transform.ROW_PASS_THRU").alias("ROW_PASS_THRU"),
            col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("Transform.ERR_CT").alias("ERR_CT"),
            col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
            col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("SK").alias("CLM_LN_SK"),
            col("Transform.CLM_ID").alias("CLM_ID"),
            col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("Transform.REMIT_PATN_RESP_AMT").alias("REMIT_PATN_RESP_AMT"),
            col("Transform.REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT"),
            col("Transform.REMIT_MBR_OTHR_LIAB_AMT").alias("REMIT_MBR_OTHR_LIAB_AMT"),
            col("Transform.REMIT_NO_RESP_AMT").alias("REMIT_NO_RESP_AMT")
        )
    )

    # ------------------------------------------------------------------
    # Return container output(s)
    # ------------------------------------------------------------------
    return df_Fkey