
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared-Container: PcmhMbrTraitPK
Folder Path      : Shared Containers/PrimaryKey
Job Type         : Server Job
Job Category     : DS_Integrate

Copyright 2011 Blue Cross and Blue Shield of Kansas City

Called by:
    MddatacorPcmhMbrPgmExtr 

Processing:
    *

Control Job Rerun Information: 
    Previous Run Successful:    What needs to happen before a special run can occur?
    Previous Run Aborted:       Restart, no other steps necessary

Modifications:
Developer        Date         Altiris #   Change Description
---------------  -----------  ---------   ------------------------------------------------------------
Steph Goddard    2011-05-03   4663        Original program

Annotations
-----------
Hashfile cleared in calling program
SQL joins temp table with key table to assign known keys
Assign primary surrogate key
Temp table is truncated before load and runstat done after load
Load IDS temp. table
join primary key info with table info
update primary key table (K_PCMH_MBR_TRAIT) with new keys created today
primary key hash file only contains current run keys and is cleared before writing
This container is used in MddatacorPcmhMbrTraitExtr – need to recompile if logic changes
"""

from pyspark.sql import DataFrame, functions as F

# ------------------------------------------------------------------------------------------------------------------
def run_PcmhMbrTraitPK(
    df_Allcol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the PcmhMbrTraitPK shared container.

    Parameters
    ----------
    df_Allcol   : DataFrame
        Container-input stream "Allcol".
    df_Transform: DataFrame
        Container-input stream "Transform".
    params      : dict
        Runtime parameters and JDBC/ADLS configuration items.

    Returns
    -------
    DataFrame
        Container-output stream "key".
    """

    # ----------------------------- Runtime-parameters --------------------------------------------------------------
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    # ---------------------------------------------------------------------------------------------------------------

    spark = df_Allcol.sparkSession  # get running SparkSession

    # =========================================
    # 1. Hashed-file hf_pcmh_mbr_trait_allcol
    #    Scenario-a: intermediate hash file – use dedup_sort
    # =========================================
    df_AllColOut = dedup_sort(
        df_Allcol,
        [
            "MBR_UNIQ_KEY",
            "PROV_GRP_PROV_ID",
            "VST_DT_SK",
            "PCMH_TRAIT_ID",
            "SRC_SYS_CD_SK"
        ],
        [("<…>", "A")]
    )

    # =========================================
    # 2. DB2 Temp-table K_PCMH_MBR_TRAIT_TEMP
    #    (truncate + insert)
    # =========================================
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_PCMH_MBR_TRAIT_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform
        .select(
            "MBR_UNIQ_KEY",
            "PROV_GRP_PROV_ID",
            "VST_DT_SK",
            "PCMH_TRAIT_ID",
            "SRC_SYS_CD_SK"
        )
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_PCMH_MBR_TRAIT_TEMP")
        .mode("append")
        .save()
    )

    # =========================================
    # 3. Extract W_Extract
    # =========================================
    extract_query = f"""
        SELECT  k.PCMH_MBR_TRAIT_SK,
                w.MBR_UNIQ_KEY,
                w.PROV_GRP_PROV_ID,
                w.VST_DT_SK,
                w.PCMH_TRAIT_ID,
                w.SRC_SYS_CD_SK,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM   {IDSOwner}.K_PCMH_MBR_TRAIT_TEMP w,
               {IDSOwner}.K_PCMH_MBR_TRAIT       k
        WHERE  w.SRC_SYS_CD_SK     = k.SRC_SYS_CD_SK
          AND  w.MBR_UNIQ_KEY       = k.MBR_UNIQ_KEY
          AND  w.PROV_GRP_PROV_ID   = k.PROV_GRP_PROV_ID
          AND  w.VST_DT_SK          = k.VST_DT_SK
          AND  w.PCMH_TRAIT_ID      = k.PCMH_TRAIT_ID
          AND  w.SRC_SYS_CD_SK      = k.SRC_SYS_CD_SK
        UNION
        SELECT -1,
               w2.MBR_UNIQ_KEY,
               w2.PROV_GRP_PROV_ID,
               w2.VST_DT_SK,
               w2.PCMH_TRAIT_ID,
               w2.SRC_SYS_CD_SK,
               {CurrRunCycle}
        FROM {IDSOwner}.K_PCMH_MBR_TRAIT_TEMP w2
        WHERE NOT EXISTS (
                SELECT k2.PCMH_MBR_TRAIT_SK
                FROM   {IDSOwner}.K_PCMH_MBR_TRAIT k2
                WHERE  w2.MBR_UNIQ_KEY     = k2.MBR_UNIQ_KEY
                  AND  w2.PROV_GRP_PROV_ID = k2.PROV_GRP_PROV_ID
                  AND  w2.VST_DT_SK        = k2.VST_DT_SK
                  AND  w2.PCMH_TRAIT_ID    = k2.PCMH_TRAIT_ID
                  AND  w2.SRC_SYS_CD_SK    = k2.SRC_SYS_CD_SK
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
             .option("url",   ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # =========================================
    # 4. Transformer: PrimaryKey
    # =========================================
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("PCMH_MBR_TRAIT_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,key_col,<schema>,<secret_name>)

    df_enriched = (
        df_enriched
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(
                F.col("svInstUpdt") == F.lit("I"),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # ----------------- Outputs from PrimaryKey ----------------
    df_updt = (
        df_enriched
        .select(
            "MBR_UNIQ_KEY",
            "PROV_GRP_PROV_ID",
            "VST_DT_SK",
            "PCMH_TRAIT_ID",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("PCMH_MBR_TRAIT_SK")
        )
    )

    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "MBR_UNIQ_KEY",
            "PROV_GRP_PROV_ID",
            "VST_DT_SK",
            "PCMH_TRAIT_ID",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "PCMH_MBR_TRAIT_SK"
        )
    )

    df_keys = (
        df_enriched
        .select(
            F.col("PCMH_MBR_TRAIT_SK"),
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            "MBR_UNIQ_KEY",
            "PROV_GRP_PROV_ID",
            "VST_DT_SK",
            "PCMH_TRAIT_ID",
            F.col("svInstUpdt").alias("ISRT_UPDT_CD"),
            "CRT_RUN_CYC_EXCTN_SK"
        )
    )

    # =========================================
    # 5. Merge Transformer (AllColOut + Keys)
    # =========================================
    join_expr = (
        (F.col("all.MBR_UNIQ_KEY")     == F.col("k.MBR_UNIQ_KEY")) &
        (F.col("all.PROV_GRP_PROV_ID") == F.col("k.PROV_GRP_PROV_ID")) &
        (F.col("all.VST_DT_SK")        == F.col("k.VST_DT_SK")) &
        (F.col("all.PCMH_TRAIT_ID")    == F.col("k.PCMH_TRAIT_ID")) &
        (F.col("all.SRC_SYS_CD_SK")    == F.col("k.SRC_SYS_CD_SK"))
    )

    df_key = (
        df_AllColOut.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            "all.JOB_EXCTN_RCRD_ERR_SK",
            "all.INSRT_UPDT_CD",
            "all.DISCARD_IN",
            "all.PASS_THRU_IN",
            "all.FIRST_RECYC_DT",
            "all.ERR_CT",
            "all.RECYCLE_CT",
            "all.SRC_SYS_CD",
            "all.PRI_KEY_STRING",
            "k.PCMH_MBR_TRAIT_SK",
            "k.MBR_UNIQ_KEY",
            "k.PROV_GRP_PROV_ID",
            "k.VST_DT_SK",
            "k.PCMH_TRAIT_ID",
            "k.SRC_SYS_CD_SK",
            "k.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "all.ALT_SRC_PROV_SK",
            "all.MBR_SK",
            "all.PCMH_TRAIT_SK",
            "all.PROV_GRP_PROV_SK",
            "all.PCMH_TRAIT_VAL_SRC_CD_SK",
            "all.ALT_SRC_TX",
            "all.PCMH_TRAIT_VAL_1_TX",
            "all.PCMH_TRAIT_VAL_2_TX",
            "all.TRAIT_VAL_SRC_KEY"
        )
    )

    # =========================================
    # 6. Sequential-file output K_PCMH_MBR_TRAIT
    # =========================================
    seq_file_path = f"{adls_path}/load/K_PCMH_MBR_TRAIT.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # =========================================
    # 7. Parquet-file output hf_pcmh_mbr_trait
    # =========================================
    parquet_path = f"{adls_path}/PcmhMbrTraitPK_updt.parquet"
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

    # -----------------------------------------
    return df_key
