# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared container used for Primary Keying of Capitation job

* VC LOGS *
^1_4 09/22/09 16:18:16 Batch  15241_58707 PROMOTE bckcett:31540 ids20 u10157 sa cuase this is third time trying
^1_4 09/22/09 16:15:40 Batch  15241_58542 PROMOTE bckcett:31540 testIDSnew u10157 sa
^1_4 09/22/09 16:12:55 Batch  15241_58378 INIT bckcett:31540 devlIDSnew u10157 sa
^1_3 09/21/09 15:26:38 Batch  15240_55605 INIT bckcett:31540 devlIDSnew u10157 sa
^1_2 09/21/09 12:16:59 Batch  15240_44229 INIT bckcett:31540 devlIDSnew u10157 sa for 2009-09-21 prod deployment
^1_1 06/29/09 16:52:24 Batch  15156_60752 INIT bckcett:31540 devlIDSnew u150906 Maddy

COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION: Shared container used for Primary Keying of Capitation job

CALLED BY : BcbsClmLnRemitDsalwExtr

PROCESSING: Load temporary table and with SQL join to key table finding existing surrogate key assignments.
Keys not found are generated and written to load file to update key table.

MODIFICATIONS:
Developer           Date                         Change Description
Brent Leland        2008-11-25                  Initial programming
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple


def run_ClmLnRemitDsalwPK(
    df_Trans: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the ClmLnRemitDsalwPK shared container.

    Parameters
    ----------
    df_Trans : DataFrame
        Incoming DataFrame representing the 'Trans' link.
    params : dict
        Dictionary containing run-time parameters.

    Returns
    -------
    DataFrame
        DataFrame corresponding to the 'Key' output link.
    """

    # --------------------------------------------------
    # Unpack parameters (each exactly once)
    # --------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    # --------------------------------------------------
    # Read hashed-file lookup as parquet (scenario c)
    # --------------------------------------------------
    lkup_path = f"{adls_path}/ClmLnRemitDsalwPK_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_path)

    # --------------------------------------------------
    # Perform left join lookup
    # --------------------------------------------------
    join_cond = (
        (F.col("trans.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD")) &
        (F.col("trans.CLM_ID") == F.col("lkup.CLM_ID")) &
        (F.col("trans.CLM_LN_SEQ_NO") == F.col("lkup.CLM_LN_SEQ_NO")) &
        (F.col("trans.CLM_LN_DSALW_TYP_CD") == F.col("lkup.CLM_LN_DSALW_TYP_CD")) &
        (F.col("trans.BYPS_IN") == F.col("lkup.BYPS_IN"))
    )

    df_joined = (
        df_Trans.alias("trans")
        .join(df_lkup.alias("lkup"), join_cond, "left")
    )

    # --------------------------------------------------
    # Derive calculated fields
    # --------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn(
            "CLM_LN_REMIT_DSALW_SK",
            F.when(F.col("lkup.CLM_LN_REMIT_DSALW_SK").isNull(), F.lit(0))
             .otherwise(F.col("lkup.CLM_LN_REMIT_DSALW_SK"))
        )
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("lkup.CLM_LN_REMIT_DSALW_SK").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # --------------------------------------------------
    # Select and order output columns
    # --------------------------------------------------
    df_output = (
        df_enriched.select(
            F.col("trans.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("trans.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("trans.DISCARD_IN").alias("DISCARD_IN"),
            F.col("trans.ROW_PASS_THRU").alias("ROW_PASS_THRU"),
            F.col("trans.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("trans.ERR_CT").alias("ERR_CT"),
            F.col("trans.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("trans.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("trans.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("CLM_LN_REMIT_DSALW_SK"),
            F.col("trans.CLM_ID").alias("CLM_ID"),
            F.col("trans.CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
            F.col("trans.CLM_LN_DSALW_TYP_CD").alias("CLM_LN_DSALW_TYP_CD"),
            F.col("trans.BYPS_IN").alias("BYPS_IN"),
            F.col("CRT_RUN_CYC_EXCTN_SK"),
            F.col("trans.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("trans.CLM_LN_SK").alias("CLM_LN_SK"),
            F.col("trans.CLM_LN_REMIT_DSALW_EXCD").alias("CLM_LN_REMIT_DSALW_EXCD"),
            F.col("trans.CLM_LN_RMT_DSW_EXCD_RESP_CD").alias("CLM_LN_RMT_DSW_EXCD_RESP_CD"),
            F.col("trans.CLM_LN_RMT_DSALW_TYP_CAT_CD").alias("CLM_LN_RMT_DSALW_TYP_CAT_CD"),
            F.col("trans.REMIT_DSALW_AMT").alias("REMIT_DSALW_AMT")
        )
    )

    return df_output