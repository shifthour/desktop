
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container: ClmChkPK
Folder Path     : Shared Containers/PrimaryKey
Job Category    : DS_Integrate
Job Type        : Server Job

DESCRIPTION:
Shared container used for Primary Keying of Claim Chk

CALLED BY:
  • FctsClmChkExtr
  • NascoClmChkExtr

PROCESSING NOTES (converted from DataStage annotations):
  • primary key hash file only contains current run keys and is cleared before writing
  • update primary key table (K_CLM_CHK) with new keys created today
  • assign primary surrogate key
  • hash file hf_clm_chk_allcol cleared in the job FctsClmChkExtr
  • writing sequential file to /key
  • join primary key info with table info
  • load IDS temp. table
  • temp table is truncated before load and run-stats executed after load
  • SQL joins temp table with key table to assign known keys
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when, lit

def run_ClmChkPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the ClmChkPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Corresponds to input link “AllCol” from stage ClmCobPK.
    df_Transform : DataFrame
        Corresponds to input link “Transform” from stage ClmCobPK.
    params : dict
        Runtime parameters and environmental configuration.

    Returns
    -------
    DataFrame
        Output link “Key” back to the calling job.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (each exactly once, following the required rules)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ---------------------------------------------
    # (1) hf_clm_chk_allcol  – scenario a hash file
    #     Replace with dedup on key columns
    # ---------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "CLM_CHK_PAYE_TYP_CD", "CLM_CHK_LOB_CD"],
        []
    )

    # ---------------------------------------------------------------
    # (2) Read existing keys from K_CLM_CHK (database via JDBC)
    # ---------------------------------------------------------------
    extract_query = f"""
    SELECT
        CLM_CHK_SK,
        SRC_SYS_CD_SK,
        CLM_ID,
        CLM_CHK_PAYE_TYP_CD,
        CLM_CHK_LOB_CD,
        CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_CHK
    """
    df_k_clm_chk = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ---------------------------------------------------------------
    # (3) Replicate K_CLM_CHK_TEMP logic entirely in Spark
    # ---------------------------------------------------------------
    join_cond = (
        (df_k_clm_chk["SRC_SYS_CD_SK"] == df_Transform["SRC_SYS_CD_SK"]) &
        (df_k_clm_chk["CLM_ID"] == trim(df_Transform["CLM_ID"])) &
        (df_k_clm_chk["CLM_CHK_PAYE_TYP_CD"] == df_Transform["CLM_CHK_PAYE_TYP_CD"]) &
        (df_k_clm_chk["CLM_CHK_LOB_CD"] == df_Transform["CLM_CHK_LOB_CD"])
    )

    df_join = df_Transform.alias("w").join(
        df_k_clm_chk.alias("k"),
        join_cond,
        "left"
    )

    df_W_Extract = df_join.select(
        when(col("k.CLM_CHK_SK").isNull(), lit(-1)).otherwise(col("k.CLM_CHK_SK")).alias("CLM_CHK_SK"),
        col("w.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("w.CLM_ID").alias("CLM_ID"),
        col("w.CLM_CHK_PAYE_TYP_CD").alias("CLM_CHK_PAYE_TYP_CD"),
        col("w.CLM_CHK_LOB_CD").alias("CLM_CHK_LOB_CD"),
        when(col("k.CLM_CHK_SK").isNull(), lit(CurrRunCycle)).otherwise(col("k.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ---------------------------------------------------------------
    # (4) PrimaryKey Transformer logic
    # ---------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt", when(col("CLM_CHK_SK") == -1, lit("I")).otherwise(lit("U")))
        .withColumn("svCrtRunCycExctnSk",
                    when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle))
                    .otherwise(col("CRT_RUN_CYC_EXCTN_SK")))
        .withColumn("svClmId", trim(col("CLM_ID")))
        .withColumn("SrcSysCd", lit(SrcSysCd))
    )

    # Call SurrogateKeyGen exactly per specification
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)
    # After surrogate key generation, capture the surrogate key for downstream usage
    df_enriched = df_enriched.withColumn("svSK", col("CLM_CHK_SK"))

    # updt link ------------------------------------------------------
    df_updt = df_enriched.select(
        col("SrcSysCd").alias("SRC_SYS_CD"),
        col("svClmId").alias("CLM_ID"),
        col("CLM_CHK_PAYE_TYP_CD"),
        col("CLM_CHK_LOB_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("CLM_CHK_SK")
    )

    # NewKeys link ---------------------------------------------------
    df_NewKeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svClmId").alias("CLM_ID"),
            col("CLM_CHK_PAYE_TYP_CD"),
            col("CLM_CHK_LOB_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("CLM_CHK_SK")
        )
    )

    # Keys link ------------------------------------------------------
    df_Keys = df_enriched.select(
        col("svSK").alias("CLM_CHK_SK"),
        col("SRC_SYS_CD_SK"),
        col("SrcSysCd").alias("SRC_SYS_CD"),
        col("svClmId").alias("CLM_ID"),
        col("CLM_CHK_PAYE_TYP_CD"),
        col("CLM_CHK_LOB_CD"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ---------------------------------------------------------------
    # (5) Merge Transformer logic – join AllCol & Keys
    # ---------------------------------------------------------------
    merge_cond = (
        (df_AllCol_dedup["SRC_SYS_CD_SK"] == df_Keys["SRC_SYS_CD_SK"]) &
        (trim(df_AllCol_dedup["CLM_ID"]) == df_Keys["CLM_ID"]) &
        (df_AllCol_dedup["CLM_CHK_PAYE_TYP_CD"] == df_Keys["CLM_CHK_PAYE_TYP_CD"]) &
        (df_AllCol_dedup["CLM_CHK_LOB_CD"] == df_Keys["CLM_CHK_LOB_CD"])
    )

    df_merge = df_AllCol_dedup.alias("all").join(
        df_Keys.alias("keys"),
        merge_cond,
        "left"
    )

    df_Key = df_merge.select(
        col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("all.DISCARD_IN").alias("DISCARD_IN"),
        col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("all.ERR_CT").alias("ERR_CT"),
        col("all.RECYCLE_CT").alias("RECYCLE_CT"),
        col("keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("keys.CLM_CHK_SK").alias("CLM_CHK_SK"),
        col("all.CLM_ID").alias("CLM_ID"),
        col("all.CLM_CHK_PAYE_TYP_CD").alias("CLM_CHK_PAYE_TYP_CD"),
        col("all.CLM_CHK_LOB_CD").alias("CLM_CHK_LOB_CD"),
        col("keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("all.CLM_ID_1").alias("CLM_ID_1"),
        col("all.CLM_CHK_PAYMT_METH_CD").alias("CLM_CHK_PAYMT_METH_CD"),
        col("all.CLM_REMIT_HIST_PAYMTOVRD_CD").alias("CLM_REMIT_HIST_PAYMTOVRD_CD"),
        col("all.EXTRNL_CHK_IN").alias("EXTRNL_CHK_IN"),
        col("all.PCA_CHK_IN").alias("PCA_CHK_IN"),
        col("all.CHK_PD_DT").alias("CHK_PD_DT"),
        col("all.CHK_NET_PAYMT_AMT").alias("CHK_NET_PAYMT_AMT"),
        col("all.CHK_ORIG_AMT").alias("CHK_ORIG_AMT"),
        col("all.CHK_NO").alias("CHK_NO"),
        col("all.CHK_SEQ_NO").alias("CHK_SEQ_NO"),
        col("all.CHK_PAYE_NM").alias("CHK_PAYE_NM"),
        col("all.CHK_PAYMT_REF_ID").alias("CHK_PAYMT_REF_ID")
    )

    # ---------------------------------------------------------------
    # (6) Egress actions – write sequential & parquet outputs
    # ---------------------------------------------------------------
    #   – Sequential file  (K_CLM_CHK.dat)
    seq_path = f"{adls_path}/load/K_CLM_CHK.dat"
    write_files(
        df_NewKeys,
        seq_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    #   – Parquet file (hf_clm_chk)  – scenario c hash file
    parquet_path = f"{adls_path}/ClmChkPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ---------------------------------------------------------------
    # Return the single output stream back to the calling job
    # ---------------------------------------------------------------
    return df_Key
