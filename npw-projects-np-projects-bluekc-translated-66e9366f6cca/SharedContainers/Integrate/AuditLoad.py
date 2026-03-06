# Databricks notebook source
# MAGIC %run ./Utility

# COMMAND ----------

# MAGIC %run ./Routine_Functions

# COMMAND ----------

"""
AuditLoad – Shared Container converted from IBM DataStage
Folder Path : Shared Containers/JobSequencer/Audit
Job Type    : Server Job
Job Category: DS_Integrate

Original Description:
* VC LOGS *
^1_1 01/28/08 07:49:06 Batch  14638_28152 INIT bckcetl ids20 dsadm dsadm
^1_1 01/10/08 08:38:00 Batch  14620_31084 INIT bckcetl ids20 dsadm dsadm
^1_1 12/26/07 08:56:40 Batch  14605_32206 INIT bckcetl ids20 dsadm dsadm
^1_1 11/21/07 14:15:25 Batch  14570_51332 INIT bckcetl ids20 dsadm dsadm
^1_2 03/21/07 14:59:13 Batch  14325_53956 INIT bckcetl ids20 dsadm dsadm
^1_1 03/07/07 15:57:28 Batch  14311_57454 INIT bckcetl ids20 dsadm dsadm
^1_2 07/29/05 14:39:17 Batch  13725_52764 INIT bckcetl ids20 dsadm Brent
^1_1 07/29/05 12:44:12 Batch  13725_45860 INIT bckcetl ids20 dsadm Brent
^1_1 07/06/05 08:44:13 Batch  13702_31459 PROMOTE bckcetl ids20 dsadm Gina Parr
^1_1 07/06/05 08:41:57 Batch  13702_31322 INIT bckcett testIDS30 dsadm Gina Parr
^1_1 06/20/05 09:33:39 Batch  13686_34424 INIT bckcett devlIDS30 u08717 Brent
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------

def run_AuditLoad(df_sc_in: DataFrame, params: dict) -> DataFrame:
    adls_path = params["adls_path"]

    df_load = (
        df_sc_in.select(
            "RunID",
            "AuditGroup",
            "AuditComponent",
            "AuditCode",
            "AuditUoM",
            "AuditValue",
            "AuditTimestamp",
        )
    )

    write_files(
        df_load,
        f"{adls_path}/AuditLoad_load.parquet",
        ",",
        "overwrite",
        True,
        True,
        '"',
        None,
    )

    return df_load

# COMMAND ----------