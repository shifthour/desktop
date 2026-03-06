# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: AuditLoad
Original Folder Path: Shared Containers/JobSequencer/Audit

* VC LOGS *
^1_2 02/01/08 08:45:39 Batch  14642_31548 INIT bckcetl edw10 dsadm dsadm
^1_2 12/26/07 14:11:10 Batch  14605_51077 INIT bckcetl edw10 dsadm dsadm
^1_1 08/31/07 13:46:04 Batch  14488_49571 INIT bckcetl edw10 dsadm dsadm
^1_2 04/10/07 07:40:03 Batch  14345_27609 INIT bckcetl edw10 dsadm dsadm
^1_1 04/02/07 09:53:23 Batch  14337_35608 INIT bckcetl edw10 dsadm dsadm
^1_1 01/30/06 14:41:13 Batch  13910_52880 INIT bckcetl edw10 dsadm Gina
^1_4 01/24/06 15:07:54 Batch  13904_54501 INIT bckcetl edw10 dsadm Gina Parr
^1_3 10/20/05 14:46:47 Batch  13808_53210 PROMOTE bckcetl edw10 dcg01 sa
^1_3 10/20/05 14:44:33 Batch  13808_53075 INIT bckcett devlEDW10 u10157 sa
^1_9 09/12/05 16:43:10 Batch  13770_60193 INIT bckcett devlEDW10 u10157 sa
^1_8 09/09/05 15:12:54 Batch  13767_54778 INIT bckcett devlEDW10 u10157 sa
^1_7 09/08/05 10:37:38 Batch  13766_38260 INIT bckcett devlEDW10 u10157 sa
^1_6 09/06/05 14:43:12 Batch  13764_52994 INIT bckcett devlEDW10 u10157 sa
^1_5 09/02/05 16:08:59 Batch  13760_58141 INIT bckcett devlEDW10 u10157 sa
^1_4 09/02/05 13:47:16 Batch  13760_49642 INIT bckcett devlEDW10 u10157 sa
^1_3 08/30/05 15:00:37 Batch  13757_54049 INIT bckcett devlEDW10 u10157 Initial move to test
^1_1 07/19/05 12:40:07 Batch  13715_45613 INIT bckcett devlEDW10 dsadm GIna Parr
^1_1 06/20/05 09:33:39 Batch  13686_34424 INIT bckcett devlIDS30 u08717 Brent
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def run_AuditLoad(
    df_sc_in: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the AuditLoad shared container logic.

    Parameters
    ----------
    df_sc_in : DataFrame
        Input DataFrame corresponding to the container link 'sc_in'.
    params : dict
        Runtime parameters; must include at least:
            adls_path : str

    Returns
    -------
    DataFrame
        DataFrame written to Parquet (represents link 'load').
    """
    # --------------------------------------------------
    # Unpack required parameters exactly once
    adls_path = params["adls_path"]
    # --------------------------------------------------
    # Stage: X_LOAD (Transformer)
    df_load = (
        df_sc_in.select(
            col("RunID").alias("RunID"),
            col("GroupName").alias("GroupName"),
            col("AuditComponent").alias("AuditComponent"),
            col("AuditCode").alias("AuditCode"),
            col("AuditUoM").alias("AuditUoM"),
            col("AuditValue").alias("AuditValue"),
            col("AuditTimestamp").alias("AuditTimestamp")
        )
    )

    # Stage: AUDIT_COLLECTION (CHashedFileStage translated to Parquet)
    write_files(
        df_load,
        f"{adls_path}/AuditLoad_load.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    return df_load