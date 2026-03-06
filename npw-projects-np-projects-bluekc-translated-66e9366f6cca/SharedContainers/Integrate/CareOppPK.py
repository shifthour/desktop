# Databricks utility notebooks
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
CareOppPK – IBM DataStage Shared Container converted to PySpark

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
    Shared container used for Primary Keying of CareOpp job
    * VC LOGS *
    ^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
    ^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
    ^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
    ^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
    ^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

CALLED BY : ImpProCareOppExtr

ANNOTATIONS:
    Used by ImpProCareOppExtr
    IDS Primary Key Container for Care Opp
    Hash file (hf_care_opp_allcol) cleared in calling job
    SQL joins temp table with key table to assign known keys
    Temp table is tuncated before load and runstat done after load
    Load IDS temp. table
    join primary key info with table info
    update primary key table (K_CARE_OPP) with new keys created today
    primary key hash file only contains current run keys and is cleared before writing
    Assign primary surrogate key
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_CareOppPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Executes the CareOppPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream corresponding to link “AllCol”.
    df_Transform : DataFrame
        Input stream corresponding to link “Transform”.
    params : dict
        Run-time parameters and JDBC configurations already supplied by the caller.

    Returns
    -------
    DataFrame
        Output stream corresponding to link “Key”.
    """

    # ------------------------------------------------------------------
    # Unpack parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    SrcSysCd            = params["SrcSysCd"]
    # ------------------------------------------------------------------
    # 1. Hash-file (hf_care_opp_allcol) – scenario a
    #    Deduplicate the incoming AllCol stream on primary keys
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CARE_OPP_ID"],
        []  # no specific sort columns available
    )

    # ------------------------------------------------------------------
    # 2. Load to K_CARE_OPP_TEMP (truncate-and-insert) and runstats
    # ------------------------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_CARE_OPP_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CARE_OPP_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CARE_OPP_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # ------------------------------------------------------------------
    # 3. Extract W_Extract resultset
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT  k.CARE_OPP_SK,
            w.SRC_SYS_CD_SK,
            w.CARE_OPP_ID,
            k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CARE_OPP_TEMP w
    JOIN {IDSOwner}.K_CARE_OPP k
         ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
        AND w.CARE_OPP_ID   = k.CARE_OPP_ID
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CARE_OPP_ID,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CARE_OPP_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_CARE_OPP k2
        WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
          AND w2.CARE_OPP_ID   = k2.CARE_OPP_ID
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
    # 4. PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CARE_OPP_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None).cast("long")).otherwise(F.col("CARE_OPP_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svCrOppId", F.trim(F.col("CARE_OPP_ID")))
    )

    # Surrogate key generation (placeholder values per specification)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # 5. Build downstream link DataFrames
    # ------------------------------------------------------------------
    # updt
    df_updt = (
        df_enriched.select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svCrOppId").alias("CARE_OPP_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CARE_OPP_SK")
        )
    )

    # NewKeys (only inserts)
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svCrOppId").alias("CARE_OPP_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CARE_OPP_SK")
        )
    )

    # Keys
    df_Keys = (
        df_enriched.select(
            F.col("svSK").alias("CARE_OPP_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svCrOppId").alias("CARE_OPP_ID"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # 6. Persist hashed-file writes and sequential file output
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/CareOppPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CARE_OPP.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # 7. Merge transformer logic – produce final “Key” output
    # ------------------------------------------------------------------
    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
            (F.col("all.CARE_OPP_ID")   == F.col("k.CARE_OPP_ID")),
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
            F.col("k.CARE_OPP_SK"),
            F.col("all.CARE_OPP_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.BCBSKC_CLNCL_PGM_TYP_CD"),
            F.col("all.PRIMDIS_ID"),
            F.col("all.SH_DESC")
        )
    )

    return df_Key