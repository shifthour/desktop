# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
ProvPkey – Shared-container translated from IBM DataStage Server Job.

Original DataStage description (truncated):
* VC LOGS *
^1_1 04/01/09 11:15:41 Batch  15067_40549 INIT bckcetl ids20 dsadm dsadm
…
MAINTENANCE LOG
Steph Goddard 06/13/2005  changed update to RUN_CYC in hash file for consistency

MODIFICATIONS:
Developer            Date          Project/Altiris #      Change Description
Sharon Andrew        3/20/2008     eproj #3255 ITS Home   Added new IDS PROV field PROV_STTUS_CD
Steph Goddard        05/25/2010    Facets 4.7.1           removed PROV_MSG_CD field, changed to use PROV_SK instead of IDS_SK
Ralph Tucker         2012-02-07    TTR-1256               Removed Display and Data Element values
"""

# Stage/Job annotations
# This container is used in:
# NABPProvExtr
# FctsProvExtr
# ESIProvExtr
#
# These programs need to be re-compiled when logic changes

from pyspark.sql import DataFrame, functions as F


def run_ProvPkey(
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Executes the ProvPkey shared-container logic.

    Parameters
    ----------
    df_Transform : DataFrame
        Input stream corresponding to DataStage link “Transform”.
    params : dict
        Runtime parameters and JDBC configuration.

    Returns
    -------
    DataFrame
        Output stream corresponding to DataStage link “Key”.
    """

    # --------------------------------------------------
    # Unpack required parameters exactly once
    # --------------------------------------------------
    CurrRunCycle: str = params["CurrRunCycle"]
    ids_jdbc_url: str = params["ids_jdbc_url"]
    ids_jdbc_props: dict = params["ids_jdbc_props"]
    ids_secret_name: str = params["ids_secret_name"]

    # --------------------------------------------------
    # Scenario-b hashed-file replacement (dummy table)
    # --------------------------------------------------
    extract_query = """
        SELECT
              SRC_SYS_CD              AS SRC_SYS_CD_lkp
            , PROV_ID                 AS PROV_ID_lkp
            , CRT_RUN_CYC_EXCTN_SK    AS CRT_RUN_CYC_EXCTN_SK_lkp
            , PROV_SK                 AS PROV_SK_lkp
        FROM  dummy_hf_prov
    """

    df_hf_prov = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # --------------------------------------------------
    # Perform left-join lookup
    # --------------------------------------------------
    join_cond = (
        F.trim(df_Transform["SRC_SYS_CD"]) == F.trim(df_hf_prov["SRC_SYS_CD_lkp"])
    ) & (
        df_Transform["PROV_ID"] == df_hf_prov["PROV_ID_lkp"]
    )

    df_joined = (
        df_Transform.alias("t")
        .join(df_hf_prov.alias("lk"), join_cond, "left")
    )

    # --------------------------------------------------
    # Derive columns prior to surrogate-key generation
    # --------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn("SK", F.col("lk.PROV_SK_lkp"))
        .withColumn(
            "NewCrtRunCcyExctnSK",
            F.when(
                F.col("lk.PROV_SK_lkp").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("lk.CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # --------------------------------------------------
    # Surrogate-key assignment (replaces KeyMgtGetNextValueConcurrent)
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "SK",
        <schema>,
        <secret_name>
    )

    # --------------------------------------------------
    # Build “Key” output stream
    # --------------------------------------------------
    df_key = (
        df_enriched.select(
            "JOB_EXCTN_RCRD_ERR_SK",
            "INSRT_UPDT_CD",
            "DISCARD_IN",
            "PASS_THRU_IN",
            "FIRST_RECYC_DT",
            "ERR_CT",
            "RECYCLE_CT",
            "SRC_SYS_CD",
            "PRI_KEY_STRING",
            F.col("SK").alias("PROV_SK"),
            "PROV_ID",
            F.col("NewCrtRunCcyExctnSK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "CMN_PRCT",
            "PROV_BILL_SVC_SK",
            "REL_GRP_PROV",
            "REL_IPA_PROV",
            "PROV_CAP_PAYMT_EFT_METH_CD",
            "PROV_CLM_PAYMT_EFT_METH_CD",
            "PROV_CLM_PAYMT_METH_CD",
            "PROV_ENTY_CD",
            "PROV_FCLTY_TYP_CD",
            "PROV_PRCTC_TYP_CD",
            "PROV_SVC_CAT_CD",
            "PROV_SPEC_CD",
            "PROV_STTUS_CD",
            "PROV_TERM_RSN_CD",
            "PROV_TYP_CD",
            "TERM_DT",
            "PAYMT_HOLD_DT",
            "CLRNGHOUSE_ID",
            "EDI_DEST_ID",
            "EDI_DEST_QUAL",
            "NTNL_PROV_ID",
            "PROV_ADDR_ID",
            "PROV_NM",
            "TAX_ID",
            F.lit("UNK").alias("TXNMY_CD")
        )
    )

    # --------------------------------------------------
    # Prepare rows that need to be inserted to dummy table
    # --------------------------------------------------
    df_updt = (
        df_enriched
        .filter(F.col("lk.PROV_SK_lkp").isNull())
        .select(
            F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("PROV_ID").alias("PROV_ID"),
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("PROV_SK")
        )
    )

    # --------------------------------------------------
    # Write insert rows back to dummy table
    # --------------------------------------------------
    (
        df_updt.write
            .mode("append")
            .format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", "dummy_hf_prov")
            .save()
    )

    # --------------------------------------------------
    # Return container output
    # --------------------------------------------------
    return df_key