
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container  : ProductPkey
Job Type          : Server Job
Category          : DS_Integrate
Folder Path       : Shared Containers

Description
-----------
Primary Key logic for IDS Product Table.

Modifications
-------------
S. Goddard    06/2005  Created; changed CurrRunCycle in updt to be consistent
Kalyan Neelam 2014-01-15 (Project 5235 ACA) Added columns QHP_ID and EFF_DT

Annotations
-----------
If primary key found, assign surrogate key, otherwise get next key and update hash file.
This container is used in:
    FctsProdProductExtr
    NascoProdProductExtr
These programs need to be re-compiled when logic changes.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_ProductPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Executes the ProductPkey shared-container logic.

    Parameters
    ----------
    df_Transform : DataFrame
        Input stream coming into the shared container on link "Transform".
    params : dict
        Runtime parameters, JDBC configs, and secrets.

    Returns
    -------
    DataFrame
        Output stream corresponding to container link "Key".
    """

    # --------------------------------------------------
    # Unpack required parameters exactly once
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    # --------------------------------------------------
    # Read dummy table replacing hashed-file hf_prod
    # --------------------------------------------------
    extract_query = """
        SELECT
            SRC_SYS_CD               AS SRC_SYS_CD_lkp,
            PROD_ID                  AS PROD_ID_lkp,
            CRT_RUN_CYC_EXCTN_SK     AS CRT_RUN_CYC_EXCTN_SK_lkp,
            PROD_SK                  AS PROD_SK_lkp
        FROM dummy_hf_prod
    """

    df_hf_prod_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # --------------------------------------------------
    # Join input with lookup
    # --------------------------------------------------
    df_join = (
        df_Transform.alias("t")
        .join(
            df_hf_prod_lkup.alias("lk"),
            (F.col("t.SRC_SYS_CD") == F.col("lk.SRC_SYS_CD_lkp")) &
            (F.col("t.PROD_ID")    == F.col("lk.PROD_ID_lkp")),
            "left"
        )
    )

    # --------------------------------------------------
    # Derive columns
    # --------------------------------------------------
    df_enriched = (
        df_join
        .withColumn("SK", F.col("lk.PROD_SK_lkp"))
        .withColumn(
            "CrtRunCycExctnSk",
            F.when(F.col("lk.PROD_SK_lkp").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("lk.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # --------------------------------------------------
    # SurrogateKeyGen – populate missing SK values
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

    # --------------------------------------------------
    # Prepare output link "Key"
    # --------------------------------------------------
    df_Key = (
        df_enriched
        .select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            F.col("SK").alias("PROD_SK"),
            "PROD_ID",
            F.col("CrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "EXPRNC_CAT",
            "PROD_SH_NM",
            "DNTL_PROD_PFX_CD",
            "PROD_LOB_TYP_CD",
            "PROD_PCKG_CD",
            "PROD_RATE_TYP_CD",
            "PROD_ST_CD",
            "SUBPROD_CD",
            "DNTL_LATE_WAIT_CD",
            "DRUG_COV_IN",
            "HLTH_COV_IN",
            "MNL_PRCS_IN",
            "MNTL_HLTH_COV_IN",
            "VSN_COV_IN",
            "PROD_EFF_DT_SK",
            "PROD_TERM_DT_SK",
            "FCTS_CONV_RATE_PFX",
            "SNAP_LOB_NO",
            "LOB_NO",
            "PROD_ABBR",
            "PROD_DESC",
            "PROD_ACCUM_SFX_CD",
            "MO_HLTH_INSUR_POOL_IN",
            "RQRD_PCP_IN",
            "QHP_ID",
            "QHP_EFF_DT_SK"
        )
    )

    # --------------------------------------------------
    # Prepare data for dummy table update (previously hashed-file write)
    # --------------------------------------------------
    df_updt = (
        df_enriched
        .filter(F.col("PROD_ID_lkp").isNull())
        .select(
            "SRC_SYS_CD",
            "PROD_ID",
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("PROD_SK")
        )
    )

    # --------------------------------------------------
    # Write updates back to dummy table
    # --------------------------------------------------
    (
        df_updt.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", "dummy_hf_prod")
        .mode("append")
        .save()
    )

    # --------------------------------------------------
    # Return the container output stream
    # --------------------------------------------------
    return df_Key
