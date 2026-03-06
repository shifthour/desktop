# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: ClmLnAltChrgRemitPK
Primary Key for Claim Line Alternate Charge Remit

* VC LOGS *
^1_2 09/22/09 16:18:16 Batch  15241_58707 PROMOTE bckcett:31540 ids20 u10157 sa cuase this is third time trying
^1_2 09/22/09 16:15:40 Batch  15241_58542 PROMOTE bckcett:31540 testIDSnew u10157 sa
^1_2 09/22/09 16:12:55 Batch  15241_58378 INIT bckcett:31540 devlIDSnew u10157 sa

**************************************************************************************************************************************************************
COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of Claim Line Alternate charge Remit job

CALLED BY : BcbsClmLnAltChrgRemitExtr

MODIFICATIONS:
Developer           Date                 Project/Altiris #      Change Description                                                                     Development Project      Code Reviewer          Date Reviewed       
------------------  -------------------- ---------------------  -----------------------------------------------------------------------                  ----------------------- -------------------- 
SAndrew             2009-06-10           3833 Remit Alternate   new program                                                                              devlIDSnew              Steph Goddard         07/02/2009                                
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_ClmLnAltChrgRemitPK(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the primary-key logic for Claim Line Alternate Charge Remit.

    Parameters
    ----------
    df_Transform : DataFrame
        Input DataFrame corresponding to the “Transform” link.
    params : dict
        Runtime parameters supplied by the calling notebook or workflow.

    Returns
    -------
    DataFrame
        Output DataFrame corresponding to the “Fkey” link.
    """
    # ------------------------------------------------------------------
    # Unpack runtime parameters (each unpacked exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # ------------------------------------------------------------------
    # Read lookup hash-file (translated to parquet – scenario c)
    # ------------------------------------------------------------------
    lkup_path = f"{adls_path}/ClmLnAltChrgRemitPK_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_path)

    # ------------------------------------------------------------------
    # Join input with lookup
    # ------------------------------------------------------------------
    df_joined = (
        df_Transform.alias("Transform")
        .join(
            df_lkup.alias("lkup"),
            (
                (F.col("Transform.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD"))
                & (F.col("Transform.CLM_ID") == F.col("lkup.CLM_ID"))
                & (F.col("Transform.CLM_LN_SEQ_NO") == F.col("lkup.CLM_LN_SEQ_NO"))
            ),
            "left",
        )
    )

    # ------------------------------------------------------------------
    # Derive transformation columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_joined.withColumn(
            "SK",
            F.when(F.col("lkup.CLM_LN_SK").isNotNull(), F.col("lkup.CLM_LN_SK")).otherwise(
                F.lit(0)
            ),
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(
                F.col("lkup.CLM_LN_SK").isNull(), F.lit(CurrRunCycle)
            ).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK")),
        )
    )

    # ------------------------------------------------------------------
    # Select & rename output columns for “Fkey” link
    # ------------------------------------------------------------------
    df_Fkey = df_enriched.select(
        F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Transform.ROW_PASS_THRU").alias("ROW_PASS_THRU"),
        F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("Transform.ERR_CT").alias("ERR_CT"),
        F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("SK").alias("CLM_LN_SK"),
        F.col("Transform.CLM_ID").alias("CLM_ID"),
        F.col("Transform.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
        F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Transform.REMIT_PROV_WRT_OFF_AMT").alias("REMIT_PROV_WRT_OFF_AMT"),
    )

    # ------------------------------------------------------------------
    # Return the single output link
    # ------------------------------------------------------------------
    return df_Fkey