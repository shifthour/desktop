# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
    Shared container used for Primary Keying of Subscriber PCA job

CALLED BY :
    FctsSubPcaExtr

PROCESSING:
    update primary key table (K_SUB_PCA) with new keys created today
    join primary key info with table info
    Hash file (hf_sub_pca_allcol) cleared in calling program
    Load IDS temp. table
    Temp table is truncated before load and runstat done after load
    SQL joins temp table with key table to assign known keys
    primary key hash file only contains current run keys and is cleared before writing

MODIFICATIONS:
Developer               Date            Change Description                                   Project/Altius #     Code Reviewer      Date Reviewed
---------------------   -------------   ---------------------------------------------------   ------------------   ---------------   -------------
Parik                   2008-08-28      Initial program                                      3567(Primary Key)     Steph Goddard     2008-09-02
Hugh Sisson             2009-03-12      Corrected output column order in hf_sub_pca_alloc    TTR-482
Akhila M                2016-10-14      Removed Dataelement                                  5628                 Jag Yelavarthi    2016-10-14
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when

# COMMAND ----------
def run_SubPcaPK(
    df_AllCol: DataFrame,
    df_Trans: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the SubPcaPK shared container.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input link 'AllCol'
    df_Trans : DataFrame
        Input link 'Trans'
    params : dict
        Runtime parameters and JDBC configurations

    Returns
    -------
    DataFrame
        Output link 'Key'
    """

    # --------------------------------------------------
    # Unpack parameters (each exactly once)
    # --------------------------------------------------
    CurrRunCycle      = params["CurrRunCycle"]
    IDSOwner          = params["IDSOwner"]
    ids_secret_name   = params["ids_secret_name"]
    SrcSysCd          = params["SrcSysCd"]

    ids_jdbc_url      = params["ids_jdbc_url"]
    ids_jdbc_props    = params["ids_jdbc_props"]

    adls_path         = params["adls_path"]
    adls_path_raw     = params.get("adls_path_raw", adls_path)
    adls_path_publish = params.get("adls_path_publish", adls_path)

    # --------------------------------------------------
    # 1. Replace intermediate hash file hf_sub_pca_allcol (scenario a)
    #    Deduplicate on key columns
    # --------------------------------------------------
    partition_cols_hf = [
        "SRC_SYS_CD_SK",
        "SUB_UNIQ_KEY",
        "SUB_PCA_ACCUM_PFX_ID",
        "PLN_YR_BEG_DT",
        "EFF_DT"
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols_hf,
        [('<…>', 'D')]
    )
    df_AllColOut = df_AllCol_dedup

    # --------------------------------------------------
    # 2. Transform stage
    # --------------------------------------------------
    df_Transform = (
        df_Trans
        .select(
            col("SRC_SYS_CD_SK"),
            col("SUB_UNIQ_KEY"),
            col("SUB_PCA_ACCUM_PFX_ID"),
            col("PLN_YR_BEG_DT").alias("PLN_YR_BEG_DT_SK"),
            col("EFF_DT").alias("EFF_DT_SK")
        )
    )

    # --------------------------------------------------
    # 3. Load to {IDSOwner}.K_SUB_PCA_TEMP
    # --------------------------------------------------
    table_temp = f"{IDSOwner}.K_SUB_PCA_TEMP"
    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", table_temp)
        .mode("overwrite")
        .save()
    )

    # After-SQL: runstats
    query_runstats = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_SUB_PCA_TEMP "
        f"on key columns with distribution on key columns "
        f"and detailed indexes all allow write access')"
    )
    execute_dml(
        query_runstats,
        ids_jdbc_url,
        ids_jdbc_props
    )

    # --------------------------------------------------
    # 4. Extract (W_Extract) – union query
    # --------------------------------------------------
    extract_query = f"""
    SELECT k.SUB_PCA_SK,
           w.SRC_SYS_CD_SK,
           w.SUB_UNIQ_KEY,
           w.SUB_PCA_ACCUM_PFX_ID,
           w.PLN_YR_BEG_DT_SK,
           w.EFF_DT_SK,
           k.CRT_RUN_CYC_EXCTN_SK
      FROM {IDSOwner}.K_SUB_PCA_TEMP w
      JOIN {IDSOwner}.K_SUB_PCA k
        ON w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
       AND w.SUB_UNIQ_KEY        = k.SUB_UNIQ_KEY
       AND w.SUB_PCA_ACCUM_PFX_ID= k.SUB_PCA_ACCUM_PFX_ID
       AND w.PLN_YR_BEG_DT_SK    = k.PLN_YR_BEG_DT_SK
       AND w.EFF_DT_SK           = k.EFF_DT_SK
    UNION
    SELECT -1,
           w2.SRC_SYS_CD_SK,
           w2.SUB_UNIQ_KEY,
           w2.SUB_PCA_ACCUM_PFX_ID,
           w2.PLN_YR_BEG_DT_SK,
           w2.EFF_DT_SK,
           {CurrRunCycle}
      FROM {IDSOwner}.K_SUB_PCA_TEMP w2
     WHERE NOT EXISTS (
           SELECT 1
             FROM {IDSOwner}.K_SUB_PCA k2
            WHERE w2.SRC_SYS_CD_SK        = k2.SRC_SYS_CD_SK
              AND w2.SUB_UNIQ_KEY         = k2.SUB_UNIQ_KEY
              AND w2.SUB_PCA_ACCUM_PFX_ID = k2.SUB_PCA_ACCUM_PFX_ID
              AND w2.PLN_YR_BEG_DT_SK     = k2.PLN_YR_BEG_DT_SK
              AND w2.EFF_DT_SK            = k2.EFF_DT_SK
     )
    """
    df_WExtract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # Rename *_SK columns for downstream compatibility
    df_WExtract = (
        df_WExtract
        .withColumnRenamed("PLN_YR_BEG_DT_SK", "PLN_YR_BEG_DT")
        .withColumnRenamed("EFF_DT_SK", "EFF_DT")
    )

    # --------------------------------------------------
    # 5. PrimaryKey Transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_WExtract
        .withColumn(
            "INSRT_UPDT_CD",
            when(col("SUB_PCA_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn("SRC_SYS_CD", lit(SrcSysCd))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            when(col("INSRT_UPDT_CD") == lit("I"), lit(CurrRunCycle))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # SurrogateKeyGen replaces KeyMgtGetNextValueConcurrent
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "SUB_PCA_SK",
        <schema>,
        <secret_name>
    )

    # Alias for clarity
    df_Primary = df_enriched

    # updt link
    df_updt = df_Primary.select(
        "SRC_SYS_CD",
        "SUB_UNIQ_KEY",
        "SUB_PCA_ACCUM_PFX_ID",
        "PLN_YR_BEG_DT",
        "EFF_DT",
        "CRT_RUN_CYC_EXCTN_SK",
        "SUB_PCA_SK"
    )

    # NewKeys link (constraint: INSRT_UPDT_CD = 'I')
    df_NewKeys = (
        df_Primary
        .filter(col("INSRT_UPDT_CD") == lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "SUB_UNIQ_KEY",
            "SUB_PCA_ACCUM_PFX_ID",
            "PLN_YR_BEG_DT",
            "EFF_DT",
            "CRT_RUN_CYC_EXCTN_SK",
            "SUB_PCA_SK"
        )
    )

    # Keys link
    df_Keys = df_Primary.select(
        "SUB_PCA_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "SUB_UNIQ_KEY",
        "SUB_PCA_ACCUM_PFX_ID",
        "PLN_YR_BEG_DT",
        "EFF_DT",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )

    # --------------------------------------------------
    # 6. Merge Transformer
    # --------------------------------------------------
    join_expr = (
        (col("all.SUB_UNIQ_KEY") == col("k.SUB_UNIQ_KEY")) &
        (col("all.SUB_PCA_ACCUM_PFX_ID") == col("k.SUB_PCA_ACCUM_PFX_ID")) &
        (col("all.PLN_YR_BEG_DT") == col("k.PLN_YR_BEG_DT")) &
        (col("all.EFF_DT") == col("k.EFF_DT"))
    )

    df_Key = (
        df_AllColOut.alias("all")
        .join(df_Keys.alias("k"), join_expr, "left")
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
            col("k.SUB_PCA_SK"),
            col("all.SUB_UNIQ_KEY"),
            col("all.SUB_PCA_ACCUM_PFX_ID"),
            col("all.PLN_YR_BEG_DT"),
            col("all.EFF_DT"),
            col("k.CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.GRP"),
            col("all.SUB"),
            col("all.SUB_PCA_CAROVR_CALC_RULE_CD"),
            col("all.TERM_DT"),
            col("all.ALLOC_AMT"),
            col("all.BAL_AMT"),
            col("all.CAROVR_AMT"),
            col("all.MAX_CAROVR_AMT"),
            col("all.PD_AMT")
        )
    )

    # --------------------------------------------------
    # 7. Write outputs
    # --------------------------------------------------
    # Sequential file for NewKeys
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_SUB_PCA.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # Parquet replacement for hf_sub_pca (updt link)
    write_files(
        df_updt,
        f"{adls_path}/SubPcaPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # 8. Return container output
    # --------------------------------------------------
    return df_Key
# COMMAND ----------