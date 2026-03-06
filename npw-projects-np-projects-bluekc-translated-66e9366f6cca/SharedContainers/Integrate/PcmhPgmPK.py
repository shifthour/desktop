# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: PcmhPgmPK
Copyright 2011 Blue Cross and Blue Shield of Kansas City

Called by:
    MddatacorPcmhPgmExtr

Processing:
    * Primary-key assignment for PCMH_PGM

Modifications:
Developer        Date        Altiris #   Change Description
---------------  ----------  ----------  -------------------------------------------------------------
Ralph Tucker     2011-04-11  4663        Original program

Annotations from DataStage:
1) IDS Primary Key Container for PCMH PGM
2) Used by ????????????????????
3) SQL joins temp table with key table to assign known keys
4) Temp table is truncated before load and runstat done after load
5) Load IDS temp. table
6) join primary key info with table info
7) Hashfile cleared everytime the job runs
8) update primary key table (K_PCMH_PGM) with new keys created today
9) primary key hash file only contains current run keys
10) Assign primary surrogate key
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when


def run_PcmhPgmPK(
    df_AllColl: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the PcmhPgmPK shared container.

    Parameters
    ----------
    df_AllColl   : DataFrame
        Container input link "AllColl" – all columns from source.
    df_Transform : DataFrame
        Container input link "Transform" – contains SRC_SYS_CD_SK and PCMH_PGM_ID.
    params       : dict
        Runtime parameters and already-established JDBC configs.

    Returns
    -------
    DataFrame
        Container output link "Key".
    """

    # ------------------------------------------------------------------
    # Unpack required parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    RunID               = params["RunID"]
    CurrDate            = params["CurrDate"]
    SrcSysCdSk          = params["SrcSysCdSk"]
    SrcSysCd            = params["SrcSysCd"]

    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params.get("ids_secret_name")
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Stage: hf_pcmh_pgm_allcol  (Scenario a – intermediate hash file)
    # ------------------------------------------------------------------
    # Deduplicate on key columns [SRC_SYS_CD_SK, PCMH_PGM_ID]
    df_allcoll_dedup = dedup_sort(
        df_AllColl,
        ["SRC_SYS_CD_SK", "PCMH_PGM_ID"],
        []
    )

    # ------------------------------------------------------------------
    # Stage: K_PCMH_PGM  (database extract used in join logic)
    # ------------------------------------------------------------------
    extract_query_k_pcmh_pgm = f"""
        SELECT
            PCMH_PGM_SK,
            SRC_SYS_CD_SK,
            PCMH_PGM_ID,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_PCMH_PGM
    """
    df_k_pcmh_pgm = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query_k_pcmh_pgm)
            .load()
    )

    # ------------------------------------------------------------------
    # Stage logic equivalent to K_PCMH_PGM_TEMP output (df_w_extract)
    # ------------------------------------------------------------------
    # Inner join rows that already have keys
    df_have_key = (
        df_Transform.alias("t")
        .join(
            df_k_pcmh_pgm.alias("k"),
            on=["SRC_SYS_CD_SK", "PCMH_PGM_ID"],
            how="inner"
        )
        .select(
            col("k.PCMH_PGM_SK"),
            col("t.SRC_SYS_CD_SK"),
            col("t.PCMH_PGM_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # Left-anti rows that need new keys
    df_need_key = (
        df_Transform.alias("t")
        .join(
            df_k_pcmh_pgm.alias("k"),
            on=["SRC_SYS_CD_SK", "PCMH_PGM_ID"],
            how="left_anti"
        )
        .select(
            lit(-1).cast("long").alias("PCMH_PGM_SK"),
            col("t.SRC_SYS_CD_SK"),
            col("t.PCMH_PGM_ID"),
            lit(CurrRunCycle).cast("long").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    df_w_extract = df_have_key.unionByName(df_need_key)

    # ------------------------------------------------------------------
    # Stage: PrimaryKey Transformer
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            when(col("PCMH_PGM_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn("svSrcSysCd", lit(SrcSysCd))
        .withColumn(
            "svSK",
            when(col("svInstUpdt") == lit("U"), col("PCMH_PGM_SK")).otherwise(F.lit(None).cast("long"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate key generation (required replacement for KeyMgtGetNextValueConcurrent)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # Output link: updt  (for hf_pcmh_pgm)
    df_updt = df_enriched.select(
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("PCMH_PGM_ID"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("PCMH_PGM_SK")
    )

    # Output link: NewKeys  (constraint svInstUpdt='I')
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            col("PCMH_PGM_ID"),
            col("SRC_SYS_CD_SK"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("PCMH_PGM_SK")
        )
    )

    # Output link: Keys  (to Merge)
    df_keys = df_enriched.select(
        col("svSK").alias("PCMH_PGM_SK"),
        col("SRC_SYS_CD_SK"),
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("PCMH_PGM_ID"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Stage: hf_pcmh_pgm  (Scenario c – write as parquet)
    # ------------------------------------------------------------------
    parquet_path_updt = f"{adls_path}/PcmhPgmPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: K_PCMH_PGM sequential file (write .dat)
    # ------------------------------------------------------------------
    seq_path_newkeys = f"{adls_path}/load/K_PCMH_PGM.dat"
    write_files(
        df_newkeys,
        seq_path_newkeys,
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Stage: Merge Transformer – produce container output "Key"
    # ------------------------------------------------------------------
    df_merge = (
        df_allcoll_dedup.alias("all")
        .join(
            df_keys.alias("keys"),
            on=["SRC_SYS_CD_SK", "PCMH_PGM_ID"],
            how="left"
        )
    )

    df_key = df_merge.select(
        col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("all.DISCARD_IN").alias("DISCARD_IN"),
        col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("all.ERR_CT").alias("ERR_CT"),
        col("all.RECYCLE_CT").alias("RECYCLE_CT"),
        col("keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("keys.PCMH_PGM_SK").alias("PCMH_PGM_SK"),
        col("keys.PCMH_PGM_ID").alias("PCMH_PGM_ID"),
        col("all.PCMH_PGM_NM").alias("PCMH_PGM_NM"),
        col("all.PCMH_PGM_TYP_CD").alias("PCMH_PGM_TYP_CD")
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_key