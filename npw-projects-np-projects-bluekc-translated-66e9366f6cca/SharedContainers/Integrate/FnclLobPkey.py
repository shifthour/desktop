# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName      : FnclLobPkey
JobType      : Server Job
JobCategory  : DS_Integrate
FolderPath   : Shared Containers

Developer                          Date                 Project/Altiris #                Change Description
---------------------------------- -------------------  -------------------------------- -------------------------
Tim Sieg                            9/4/2019            US136099                         Making Data Element column empty in the PrimaryKey transformer stage
Amritha A J                        2023-07-18           US 588188                        Updated FNCL_LOB_DESC column length to VARCHAR(255).
Ediga Maruthi                      2024-09-17           US628653                         Added LOB_NO column from Source to the Target.

Annotations:
Assign primary surrogate key
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_FnclLobPkey(
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-container translation of FnclLobPkey.
    Parameters
    ----------
    df_Transform : DataFrame
        Input stream corresponding to the 'Transform' link.
    params : dict
        Runtime parameters and JDBC configuration.
    Returns
    -------
    DataFrame
        Output stream corresponding to the 'Key' link.
    """

    # ------------------------------------------------------------------
    # unpack parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle          = params["CurrRunCycle"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]

    # ------------------------------------------------------------------
    # read dummy table replacing hashed-file 'hf_fncl_lob'
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT
            SRC_SYS_CD                  AS SRC_SYS_CD_lkp,
            FNCL_LOB_CD                 AS FNCL_LOB_CD_lkp,
            CRT_RUN_CYC_EXCTN_SK        AS CRT_RUN_CYC_EXCTN_SK_lkp,
            FNCL_LOB_SK                 AS FNCL_LOB_SK_lkp
        FROM {IDSOwner}.dummy_hf_fncl_lob
    """
    df_hf_fncl_lob_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ------------------------------------------------------------------
    # join with lookup
    # ------------------------------------------------------------------
    join_cond = (
        (F.col("Transform.SRC_SYS_CD") == F.col("SRC_SYS_CD_lkp")) &
        (F.trim(F.col("Transform.FNCL_LOB_CD")) == F.col("FNCL_LOB_CD_lkp"))
    )

    df_enriched = (
        df_Transform.alias("Transform")
        .join(df_hf_fncl_lob_lkup.alias("lkup"), join_cond, "left")
    )

    # ------------------------------------------------------------------
    # derive columns
    # ------------------------------------------------------------------
    df_enriched = (
        df_enriched
        .withColumn("SK", F.col("FNCL_LOB_SK_lkp"))
    )

    # surrogate-key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"SK",<schema>,<secret_name>)

    df_enriched = (
        df_enriched
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("FNCL_LOB_SK_lkp").isNull(), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "CrtRunCycExtcnSk",
            F.when(F.col("FNCL_LOB_SK_lkp").isNull(), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
        .withColumn("FNCL_FUND_TYP_NM", F.trim(F.col("Transform.FNCL_FUND_TYP_NM")))
    )

    # ------------------------------------------------------------------
    # build 'Key' output stream
    # ------------------------------------------------------------------
    df_key = df_enriched.select(
        F.col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("INSRT_UPDT_CD"),
        F.col("Transform.DISCARD_IN").alias("DISCARD_IN"),
        F.col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("Transform.ERR_CT").alias("ERR_CT"),
        F.col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("SK").alias("FNCL_LOB_SK"),
        F.col("Transform.FNCL_LOB_CD").alias("FNCL_LOB_CD"),
        F.col("CrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("Transform.FNCL_LOB_STTUS_CD_SK").alias("FNCL_LOB_STTUS_CD_SK"),
        F.col("Transform.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("Transform.FNCL_LOB_DESC").alias("FNCL_LOB_DESC"),
        F.col("Transform.SPIRA_BNF_ID").alias("SPIRA_BNF_ID"),
        F.col("Transform.FNCL_LOB_STTUS_CD").alias("FNCL_LOB_STTUS_CD"),
        F.col("Transform.LAST_UPDT_DT_SK").alias("LAST_UPDT_DT_SK"),
        F.col("Transform.LOB_NM").alias("LOB_NM"),
        F.col("Transform.referenceID").alias("referenceID"),
        F.col("Transform.REL_INT_LOB_NM").alias("REL_INT_LOB_NM"),
        F.col("Transform.FNCL_MKT_SEG_NM").alias("FNCL_MKT_SEG_NM"),
        F.col("Transform.FNCL_RPTNG_SEG_NM").alias("FNCL_RPTNG_SEG_NM"),
        F.col("Transform.QFR_GRP_NM").alias("QFR_GRP_NM"),
        F.col("Transform.STATUTORY_GRP_NM").alias("STATUTORY_GRP_NM"),
        F.col("Transform.ALLOC_ST_NM").alias("ALLOC_ST_NM"),
        F.col("Transform.FCTS_STTUS_NM").alias("FCTS_STTUS_NM"),
        F.col("Transform.ACA_STTUS_NM").alias("ACA_STTUS_NM"),
        F.col("Transform.MCARE_ADVNTG_PROD_TYP_NM").alias("MCARE_ADVNTG_PROD_TYP_NM"),
        F.col("Transform.CO_SK").alias("CO_SK"),
        F.col("Transform.BCBSA_NTNL_ACCT_IN").alias("BCBSA_NTNL_ACCT_IN"),
        F.col("Transform.FNCL_GRP_SIZE_CAT_NM").alias("FNCL_GRP_SIZE_CAT_NM"),
        F.col("FNCL_FUND_TYP_NM"),
        F.col("Transform.LOB_NO").alias("LOB_NO")
    )

    # ------------------------------------------------------------------
    # build 'updt' stream (rows not found in lookup) and write back
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .filter(F.col("FNCL_LOB_CD_lkp").isNull())
        .select(
            F.col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.trim(F.col("Transform.FNCL_LOB_CD")).alias("FNCL_LOB_CD"),
            F.col("CrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("SK").alias("FNCL_LOB_SK")
        )
    )

    if df_updt.head(1):
        (
            df_updt.write.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("dbtable", f"{IDSOwner}.dummy_hf_fncl_lob")
            .mode("append")
            .save()
        )

    # ------------------------------------------------------------------
    # return container output(s)
    # ------------------------------------------------------------------
    return df_key