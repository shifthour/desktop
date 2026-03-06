# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit


def run_ClmRelClmPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    """
    Shared Container: ClmRelClmPK

    MODIFICATIONS:
    Developer               Date                Change Description                           Project/Altius #   Development Project   Code Reviewer   Date Reviewed
    ----------------------- -------------------- ------------------------------------------- ------------------ --------------------- --------------- --------------
    Karthik Chintalapani    2012-03-05          Originally Programmed                        4784               IntegratewrhsDevl     Bhoomi Dasari   07/19/2012

    If primary key found, assign surrogate key, otherwise get next key and update hash file.
    This container is used in:
    BCBSSCClmRelClmExtr

    These programs need to be re-compiled when logic changes.
    """

    # --------------------------------------------------
    # Parameter unpacking
    # --------------------------------------------------
    CurrRunCycle  = params["CurrRunCycle"]
    jdbc_url      = params["jdbc_url"]
    jdbc_props    = params["jdbc_props"]

    # --------------------------------------------------
    # Dummy-table (hashed-file replacement) read
    # --------------------------------------------------
    extract_query = """
        SELECT
            PRI_REL_CLM_ID             AS PRI_REL_CLM_ID_lkp,
            PRI_REL_CLM_SRC_SYS_CD     AS PRI_REL_CLM_SRC_SYS_CD_lkp,
            CLM_RELSHP_TYP_CD          AS CLM_RELSHP_TYP_CD_lkp,
            SEC_REL_CLM_ID             AS SEC_REL_CLM_ID_lkp,
            SEC_REL_CLM_SRC_SYS_CD_SK  AS SEC_REL_CLM_SRC_SYS_CD_SK_lkp,
            CRT_RUN_CYC_EXCTN_SK       AS CRT_RUN_CYC_EXCTN_SK_lkp,
            CLM_REL_CLM_SK             AS CLM_REL_CLM_SK_lkp
        FROM dummy_hf_clm_rel_clm
    """

    df_lkup = (
        spark.read.format("jdbc")
            .option("url", jdbc_url)
            .options(**jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # --------------------------------------------------
    # Join source records with lookup
    # --------------------------------------------------
    join_cond = (
        (col("Transform.PRI_REL_CLM_ID")            == col("lkup.PRI_REL_CLM_ID_lkp")) &
        (col("Transform.PRI_REL_CLM_SRC_SYS_CD")    == col("lkup.PRI_REL_CLM_SRC_SYS_CD_lkp")) &
        (col("Transform.CLM_RELSHP_TYP_CD")         == col("lkup.CLM_RELSHP_TYP_CD_lkp")) &
        (col("Transform.SEC_REL_CLM_ID")            == col("lkup.SEC_REL_CLM_ID_lkp")) &
        (col("Transform.SEC_REL_CLM_SRC_SYS_CD_SK") == col("lkup.SEC_REL_CLM_SRC_SYS_CD_SK_lkp"))
    )

    df_enriched = (
        df_Transform.alias("Transform")
        .join(df_lkup.alias("lkup"), join_cond, "left")
        .withColumn(
            "CLM_REL_CLM_SK",
            when(col("CLM_REL_CLM_SK_lkp").isNull(), lit(None)).otherwise(col("CLM_REL_CLM_SK_lkp"))
        )
        .withColumn(
            "NewCrtRunCycExtcnSk",
            when(col("CLM_REL_CLM_SK_lkp").isNull(), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK_lkp"))
        )
    )

    # --------------------------------------------------
    # Surrogate-key generation
    # --------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)

    # --------------------------------------------------
    # Prepare updates for dummy table (new keys only)
    # --------------------------------------------------
    df_updt = (
        df_enriched
        .filter(col("CLM_REL_CLM_SK_lkp").isNull())
        .select(
            col("PRI_REL_CLM_ID"),
            col("PRI_REL_CLM_SRC_SYS_CD"),
            col("CLM_RELSHP_TYP_CD"),
            col("SEC_REL_CLM_ID"),
            col("SEC_REL_CLM_SRC_SYS_CD_SK"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("CLM_REL_CLM_SK")
        )
    )

    (
        df_updt.write
        .format("jdbc")
        .option("url", jdbc_url)
        .options(**jdbc_props)
        .option("dbtable", "dummy_hf_clm_rel_clm")
        .mode("append")
        .save()
    )

    # --------------------------------------------------
    # Build output link “Key”
    # --------------------------------------------------
    df_key = (
        df_enriched
        .select(
            col("JOB_EXCTN_RCRD_ERR_SK"),
            col("INSRT_UPDT_CD"),
            col("DISCARD_IN"),
            col("PASS_THRU_IN"),
            col("FIRST_RECYC_DT"),
            col("ERR_CT"),
            col("RECYCLE_CT"),
            col("SRC_SYS_CD"),
            col("PRI_KEY_STRING"),
            col("CLM_REL_CLM_SK"),
            col("PRI_REL_CLM_ID"),
            col("PRI_REL_CLM_SRC_SYS_CD"),
            col("CLM_RELSHP_TYP_CD"),
            col("SEC_REL_CLM_ID"),
            col("SEC_REL_CLM_SRC_SYS_CD_SK"),
            col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("PRI_REL_CLM"),
            col("SEC_REL_CLM"),
            col("CLM_REL_TYP_CD")
        )
    )

    return df_key