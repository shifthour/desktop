
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Job Name:        PseudoClmPK
Folder Path:     Shared Containers/PrimaryKey
Job Type:        Server Job
Job Category:    DS_Integrate

DESCRIPTION:
All the work is really done in the extract job. This job is just a pass-thru
to have the Job Control loop correctly.

VC LOGS:
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
...

HASH FILES:
hf_clm – this is both read from and written to.

ANNOTATIONS:
This container is used in:
PCTAPseudoClmExtr
These programs need to be re-compiled when logic changes
This job uses hf_clm as the primary key hash file since it has the same natural
key as the CLM table
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_PseudoClmPK(df_clm: DataFrame, params: dict) -> DataFrame:
    """
    Shared-container logic reproduced in PySpark.

    Parameters
    ----------
    df_clm : DataFrame
        Incoming claim-detail records requiring primary-key assignment.
    params : dict
        Runtime parameters and JDBC configuration entries.

    Returns
    -------
    DataFrame
        DataFrame containing the primary-key–enriched claim records.
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle            = params["CurrRunCycle"]

    IDSOwner                = params["IDSOwner"]
    ids_secret_name         = params["ids_secret_name"]
    ids_jdbc_url            = params["ids_jdbc_url"]
    ids_jdbc_props          = params["ids_jdbc_props"]

    # ------------------------------------------------------------------
    # Read the dummy table that mirrors the hash file (scenario b)
    # ------------------------------------------------------------------
    df_hf_clm = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", "SELECT * FROM <dummy_hf_clm>")      # exact column list substituted by SELECT *
            .load()
    )

    # ------------------------------------------------------------------
    # Deduplicate incoming data on natural key columns (placeholder keys)
    # ------------------------------------------------------------------
    partition_cols = ["<CLM_NBR>", "<CLM_EXTN>"]
    sort_cols      = [("<UPDT_TS>", "D")]

    df_clm_dedup = dedup_sort(df_clm, partition_cols, sort_cols)

    # ------------------------------------------------------------------
    # Identify brand-new claims (not yet in hf_clm)
    # ------------------------------------------------------------------
    join_expr = (
        (df_clm_dedup["<CLM_NBR>"] == df_hf_clm["<CLM_NBR>"]) &
        (df_clm_dedup["<CLM_EXTN>"] == df_hf_clm["<CLM_EXTN>"])
    )

    df_new = (
        df_clm_dedup.alias("in")
        .join(df_hf_clm.alias("hf"), join_expr, "left_anti")
    )

    # ------------------------------------------------------------------
    # Generate surrogate keys for the new claims
    # ------------------------------------------------------------------
    df_enriched = df_new
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "CLM_ID", <schema>, <secret_name>)

    # ------------------------------------------------------------------
    # Append the newly-keyed rows back into the dummy table
    # ------------------------------------------------------------------
    (
        df_enriched
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", "<dummy_hf_clm>")
        .mode("append")
        .save()
    )

    # ------------------------------------------------------------------
    # Return the enriched DataFrame to the calling job
    # ------------------------------------------------------------------
    return df_enriched
