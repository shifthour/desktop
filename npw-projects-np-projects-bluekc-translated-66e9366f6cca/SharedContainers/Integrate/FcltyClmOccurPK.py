# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, trim

def run_FcltyClmOccurPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared Container: FcltyClmOccurPK
    DESCRIPTION:   Shared container used for Primary Keying of Facility Claim Occur
    CALLED BY: FctsClmFcltyOccurExtr, NascoClmFcltyOccurExtr
    PROCESSING STEPS:
        1. Prepare AllCol stream (dedup).
        2. Join incoming claim occurrence keys with existing key table.
        3. Generate surrogate keys for new records.
        4. Produce three internal streams (updt, NewKeys, Keys).
        5. Merge Keys with AllCol to create final Key output.
        6. Persist NewKeys sequential file and updt parquet file.
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters
    CurrRunCycle         = params["CurrRunCycle"]
    SrcSysCd             = params["SrcSysCd"]
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Stage: hf_fclty_occur_allcol  (Scenario a – intermediate hash file)
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "FCLTY_CLM_OCCUR_SEQ_NO"],
        [("FCLTY_CLM_OCCUR_SEQ_NO", "A")]
    )

    # ------------------------------------------------------------------
    # Stage: K_FCLTY_CLM_OCCUR_TEMP / W_Extract (simulate via DataFrame)
    # ------------------------------------------------------------------
    df_keytable = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option(
                "query",
                f"""
                SELECT
                    FCLTY_CLM_OCCUR_SK,
                    SRC_SYS_CD_SK,
                    CLM_ID,
                    FCLTY_CLM_OCCUR_SEQ_NO,
                    CRT_RUN_CYC_EXCTN_SK
                FROM {IDSOwner}.K_FCLTY_CLM_OCCUR
                """
            )
            .load()
    )

    join_expr = [
        col("w.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK"),
        col("w.CLM_ID") == col("k.CLM_ID"),
        col("w.FCLTY_CLM_OCCUR_SEQ_NO") == col("k.FCLTY_CLM_OCCUR_SEQ_NO")
    ]

    df_w_extract = (
        df_Transform.alias("w")
        .join(df_keytable.alias("k"), join_expr, "left")
        .select(
            when(col("k.FCLTY_CLM_OCCUR_SK").isNull(), lit(-1))
                .otherwise(col("k.FCLTY_CLM_OCCUR_SK")).alias("FCLTY_CLM_OCCUR_SK"),
            col("w.SRC_SYS_CD_SK"),
            col("w.CLM_ID"),
            col("w.FCLTY_CLM_OCCUR_SEQ_NO"),
            when(col("k.CRT_RUN_CYC_EXCTN_SK").isNull(), lit(CurrRunCycle))
                .otherwise(col("k.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey (Transformer)
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn("svInstUpdt",
                    when(col("FCLTY_CLM_OCCUR_SK") == -1, lit("I"))
                    .otherwise(lit("U")))
        .withColumn("svSK", col("FCLTY_CLM_OCCUR_SK"))
        .withColumn("svCrtRunCycExctnSk",
                    when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle))
                    .otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svClmId", trim(col("CLM_ID")))
    )

    # Surrogate Key Generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # Prepare internal links
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .select(
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            col("svClmId").alias("CLM_ID"),
            col("FCLTY_CLM_OCCUR_SEQ_NO"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("FCLTY_CLM_OCCUR_SK")
        )
    )

    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svClmId").alias("CLM_ID"),
            col("FCLTY_CLM_OCCUR_SEQ_NO"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("FCLTY_CLM_OCCUR_SK")
        )
    )

    df_keys = (
        df_enriched
        .select(
            col("svSK").alias("FCLTY_CLM_OCCUR_SK"),
            col("SRC_SYS_CD_SK"),
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            col("svClmId").alias("CLM_ID"),
            col("FCLTY_CLM_OCCUR_SEQ_NO"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # Persist sequential file for new keys
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_FCLTY_CLM_OCCUR.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Persist parquet file for updt link (hf_fclty_occur)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/FcltyClmOccurPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: Merge
    # ------------------------------------------------------------------
    merge_join_expr = [
        col("all.SRC_SYS_CD_SK") == col("k.SRC_SYS_CD_SK"),
        col("all.CLM_ID") == col("k.CLM_ID"),
        col("all.FCLTY_CLM_OCCUR_SEQ_NO") == col("k.FCLTY_CLM_OCCUR_SEQ_NO")
    ]

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), merge_join_expr, "left")
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD"),
            col("all.DISCARD_IN"),
            col("all.PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT"),
            col("all.ERR_CT"),
            col("all.RECYCLE_CT"),
            col("k.SRC_SYS_CD"),
            col("all.PRI_KEY_STRING"),
            col("k.FCLTY_CLM_OCCUR_SK"),
            col("k.SRC_SYS_CD_SK"),
            col("all.CLM_ID"),
            col("all.FCLTY_CLM_OCCUR_SEQ_NO"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.FCLTY_CLM_SK"),
            col("all.FCLTY_CLM_OCCUR_CD"),
            col("all.FROM_DT_SK"),
            col("all.TO_DT_SK")
        )
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key