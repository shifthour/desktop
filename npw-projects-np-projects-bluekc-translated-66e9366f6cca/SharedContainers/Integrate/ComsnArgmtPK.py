# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
ComsnArgmtPK – IDS Primary Key Container for Commission Argmt
================================================================
* VC LOGS *
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy

DESCRIPTION:
    Shared container used for Primary Keying of Comm Argmt job.
    Hash file (hf_comsn_argmt_allcol) cleared in calling job.
    SQL joins temp table with key table to assign known keys.
    Temp table is truncated before load and runstats done after load.
    Load IDS temp. table, join primary key info with table info,
    update primary key table (K_COMSN_ARGMT) with new keys created today,
    primary key hash file only contains current run keys and is cleared before writing,
    assign primary surrogate key.

CALLED BY:
    FctsComsnArgmtExtr
"""

from pyspark.sql import functions as F
from pyspark.sql import DataFrame


def run_ComsnArgmtPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the ComsnArgmtPK shared-container logic.

    Parameters
    ----------
    df_AllCol   : DataFrame – input link “AllCol”
    df_Transform: DataFrame – input link “Transform”
    params      : dict      – runtime parameters

    Returns
    -------
    DataFrame – output link “Key”
    """

    # ------------------------------------------------------------------
    # Unpack required parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Replace intermediate hash file “hf_comsn_argmt_allcol”
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "COMSN_ARGMT_ID"],
        [("SRC_SYS_CD_SK", "A")]
    )

    # ------------------------------------------------------------------
    # Persist df_Transform into IDS temp table K_COMSN_ARGMT_TEMP
    # ------------------------------------------------------------------
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_COMSN_ARGMT_TEMP")
        .mode("overwrite")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_COMSN_ARGMT_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # Extract W_Extract link from IDS
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.COMSN_ARGMT_SK,
                w.SRC_SYS_CD_SK,
                w.COMSN_ARGMT_ID,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_COMSN_ARGMT_TEMP w,
             {IDSOwner}.K_COMSN_ARGMT k
        WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
          AND w.COMSN_ARGMT_ID = k.COMSN_ARGMT_ID
        UNION
        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.COMSN_ARGMT_ID,
               {CurrRunCycle}
        FROM {IDSOwner}.K_COMSN_ARGMT_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_COMSN_ARGMT k2
            WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
              AND w2.COMSN_ARGMT_ID = k2.COMSN_ARGMT_ID
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("COMSN_ARGMT_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "COMSN_ARGMT_SK",
        <schema>,
        <secret_name>
    )

    df_enriched = (
        df_enriched
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(
                F.col("INSRT_UPDT_CD") == "I",
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("COMSN_ARGMT_ID_TRIM", F.trim(F.col("COMSN_ARGMT_ID")))
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    )

    # ------------------------------------------------------------------
    # Prepare output links from PrimaryKey stage
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        "SRC_SYS_CD",
        F.col("COMSN_ARGMT_ID_TRIM").alias("COMSN_ARGMT_ID"),
        "CRT_RUN_CYC_EXCTN_SK",
        "COMSN_ARGMT_SK"
    )

    df_newkeys = (
        df_enriched.filter(F.col("INSRT_UPDT_CD") == "I")
        .select(
            "SRC_SYS_CD_SK",
            F.col("COMSN_ARGMT_ID_TRIM").alias("COMSN_ARGMT_ID"),
            "CRT_RUN_CYC_EXCTN_SK",
            "COMSN_ARGMT_SK"
        )
    )

    df_keys = df_enriched.select(
        "COMSN_ARGMT_SK",
        "SRC_SYS_CD_SK",
        F.col("COMSN_ARGMT_ID_TRIM").alias("COMSN_ARGMT_ID"),
        "SRC_SYS_CD",
        F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        "CRT_RUN_CYC_EXCTN_SK"
    )

    # ------------------------------------------------------------------
    # Write to parquet (hf_comsn_argmt) and sequential file (K_COMSN_ARGMT.dat)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ComsnArgmtPK_updt.parquet",
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_COMSN_ARGMT.dat",
        ",",
        "overwrite",
        False,
        False,
        "\"",
        None
    )

    # ------------------------------------------------------------------
    # Merge transformer logic
    # ------------------------------------------------------------------
    df_Key = (
        df_allcol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
            (F.col("all.COMSN_ARGMT_ID") == F.col("k.COMSN_ARGMT_ID")),
            "left"
        )
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.COMSN_ARGMT_SK"),
            F.col("k.COMSN_ARGMT_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
            F.col("all.BEG_DT"),
            F.col("all.ARGMT_DESC")
        )
    )

    # ------------------------------------------------------------------
    # Return the single output link
    # ------------------------------------------------------------------
    return df_Key