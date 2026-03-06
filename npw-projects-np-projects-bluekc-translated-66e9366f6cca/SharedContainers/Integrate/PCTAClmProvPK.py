# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName:          PCTAClmProvPK
FolderPath:       Shared Containers/PrimaryKey
Description:      Shared container used for Primary Keying of PCTA Claim Provider
Annotations:
  - join primary key info with table info
  - update primary key table (K_CLM_PROV) with new keys created today
  - Assign primary surrogate key
  - primary key hash file only contains current run keys and is cleared before writing
  - Temp table is truncated before load and runstat done after load
  - SQL joins temp table with key table to assign known keys
  - Load IDS temp. table
  - PCTA claim provider uses a separate container from the rest of the claim providers because it lookups source system codes within the container itself - It cannot be passed in
  - This container is used in:
            PCTAClmProvExtr
    These programs need to be re-compiled when logic changes
  - Hash file (hf_clm_prov_allcol) cleared in the calling program
  - Hashfile cleared in calling program
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_PCTAClmProvPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the PCTAClmProvPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Incoming stream that feeds the original hf_clm_prov_allcol hash file.
    df_Transform : DataFrame
        Input stream that will be loaded into IDSOwner.K_CLM_PROV_TEMP.
    params : dict
        Runtime parameters, JDBC configs, and all path variables required.

    Returns
    -------
    DataFrame
        The container output link "Key".
    """

    # ------------------------------------------------------------------
    # Unpack parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # STEP-1 : Replace intermediate hash file (hf_clm_prov_allcol)
    #          with a deduplicated DataFrame
    # ------------------------------------------------------------------
    partition_cols = ["SRC_SYS_CD_SK", "CLM_ID", "CLM_PROV_ROLE_TYP_CD"]
    df_AllCol_dedup = dedup_sort(df_AllCol, partition_cols, [])
    df_SrcSysCdLkup = df_AllCol_dedup

    # ------------------------------------------------------------------
    # STEP-2 : Load IDSOwner.K_CLM_PROV_TEMP
    # ------------------------------------------------------------------
    truncate_query = f"TRUNCATE TABLE {IDSOwner}.K_CLM_PROV_TEMP"
    execute_dml(truncate_query, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.write.mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_CLM_PROV_TEMP")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_CLM_PROV_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props,
    )

    # ------------------------------------------------------------------
    # STEP-3 : Extract W_Extract DataFrame (stage: K_CLM_PROV_TEMP – output)
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT k.CLM_PROV_SK,
           w.SRC_SYS_CD_SK,
           w.CLM_ID,
           w.CLM_PROV_ROLE_TYP_CD,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CLM_PROV_TEMP w,
         {IDSOwner}.K_CLM_PROV k
    WHERE w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
      AND w.CLM_ID              = k.CLM_ID
      AND w.CLM_PROV_ROLE_TYP_CD = k.CLM_PROV_ROLE_TYP_CD
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.CLM_ID,
           w2.CLM_PROV_ROLE_TYP_CD,
           {CurrRunCycle}
    FROM {IDSOwner}.K_CLM_PROV_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.CLM_PROV_SK
        FROM {IDSOwner}.K_CLM_PROV k2
        WHERE w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
          AND w2.CLM_ID              = k2.CLM_ID
          AND w2.CLM_PROV_ROLE_TYP_CD = k2.CLM_PROV_ROLE_TYP_CD
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
    # STEP-4 : Transformer logic (stage: PrimaryKey)
    # ------------------------------------------------------------------
    df_join = (
        df_W_Extract.alias("w")
        .join(
            df_SrcSysCdLkup.alias("s"),
            ["SRC_SYS_CD_SK", "CLM_ID", "CLM_PROV_ROLE_TYP_CD"],
            "left",
        )
    )

    df_enriched = (
        df_join.withColumn(
            "svInstUpdt",
            F.when(F.col("w.CLM_PROV_SK") == F.lit(-1), F.lit("I")).otherwise(
                F.lit("U")
            ),
        )
        .withColumn("svSrcSysCd", F.col("s.SRC_SYS_CD"))
        .withColumn(
            "svSK",
            F.when(F.col("w.CLM_PROV_SK") == F.lit(-1), F.lit(None)).otherwise(
                F.col("w.CLM_PROV_SK")
            ),
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("w.CLM_PROV_SK") == F.lit(-1), F.lit(CurrRunCycle)).otherwise(
                F.col("w.CRT_RUN_CYC_EXCTN_SK")
            ),
        )
    )

    # SurrogateKeyGen handles missing surrogate keys
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "svSK", <schema>, <secret_name>)

    # ------------------------------------------------------------------
    # STEP-5 : Prepare down-stream DataFrames from transformer outputs
    # ------------------------------------------------------------------
    # updt link (to hf_clm_prov)
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("CLM_PROV_SK"),
    )

    # NewKeys link (to sequential file) – only inserts
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "CLM_PROV_ROLE_TYP_CD",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("CLM_PROV_SK"),
        )
    )

    # Keys link (to Merge transformer)
    df_Keys = df_enriched.select(
        F.col("svSK").alias("CLM_PROV_SK"),
        "SRC_SYS_CD_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    )

    # ------------------------------------------------------------------
    # STEP-6 : Write outputs of updt and NewKeys links
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/PCTAClmProvPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_CLM_PROV.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote='"',
        nullValue=None,
    )

    # ------------------------------------------------------------------
    # STEP-7 : Merge transformer logic
    # ------------------------------------------------------------------
    df_merge = (
        df_AllCol_dedup.alias("a")
        .join(df_Keys.alias("k"), ["SRC_SYS_CD_SK", "CLM_ID", "CLM_PROV_ROLE_TYP_CD"], "left")
    )

    df_Key = df_merge.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_PROV_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CLM_PROV_ROLE_TYP_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        "PROV_ID",
        "TAX_ID",
    )

    # ------------------------------------------------------------------
    # Return the container output link(s)
    # ------------------------------------------------------------------
    return df_Key