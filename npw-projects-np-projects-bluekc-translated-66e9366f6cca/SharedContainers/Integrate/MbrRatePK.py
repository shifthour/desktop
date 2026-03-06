# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Job Name:           MbrRatePK
Job Type:           Server Job
Job Category:       DS_Integrate
Folder Path:        Shared Containers/PrimaryKey

DESCRIPTION:
COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:   Shared container used for Primary Keying of Mbr Rate job

CALLED BY : FctsMbrRateExtr

PROCESSING:    


MODIFICATIONS:
Developer           Date               Change Description                                                                         Project/Altius #     Code Reviewer        Date Reviewed  
-----------------------    ------------------     ------------------------------------------------------------------------------------------------------  ---------------------------   -------------------------------  -------------------------
Kalyan Neelam             2014-03-24             Initial Programming                                                               5235 ACA              Bhoomi Dasari        3/27/2014


Annotations:
1) update primary key table (K_MBR_RATE) with new keys created today
2) join primary key info with table info
3) Hash file (hf_mbrrate_pkey_allcol) cleared in calling program
4) This container is used in:
   FctsMbrRateExtr
   These programs need to be re-compiled when logic changes
5) Load IDS temp. table
6) Temp table is tuncated before load and runstat done after load
7) SQL joins temp table with key table to assign known keys
8) primary key hash file only contains current run keys
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_MbrRatePK(
    df_SubPcaPK: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the MbrRatePK shared container.

    Parameters
    ----------
    df_SubPcaPK : DataFrame
        Up-stream DataFrame representing the SubPcaPK link data.
    params : dict
        Dictionary of runtime parameters and configuration items.

    Returns
    -------
    DataFrame
        Output DataFrame corresponding to the “Key” link of the container.
    """

    # -------------------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # -------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    IDSOwner            = params["IDSOwner"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    ids_secret_name     = params["ids_secret_name"]
    SrcSysCd            = params["SrcSysCd"]

    # -------------------------------------------------------------
    # 1. Replace intermediate hash-file hf_mbrrate_pkey_allcol
    #    Scenario-a (intermediate) – apply duplicate removal
    # -------------------------------------------------------------
    df_allcol = dedup_sort(
        df_SubPcaPK,
        ["MBR_UNIQ_KEY", "EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    # -------------------------------------------------------------
    # 2. Replace intermediate hash-file hf_mbrrate_pkey_dedupe
    #    Scenario-a (intermediate) – apply duplicate removal
    # -------------------------------------------------------------
    df_dedupe = dedup_sort(
        df_SubPcaPK,
        ["MBR_UNIQ_KEY", "EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    # -------------------------------------------------------------
    # 3. Load temp table IDSOwner.K_MBR_RATE_TEMP (overwrite)
    # -------------------------------------------------------------
    (
        df_dedupe.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MBR_RATE_TEMP")
        .mode("overwrite")
        .save()
    )

    # -------------------------------------------------------------
    # 4. Extract data for W_Extract link
    # -------------------------------------------------------------
    extract_query = f"""
        SELECT
               w.MBR_UNIQ_KEY,
               w.EFF_DT_SK,
               w.SRC_SYS_CD_SK,
               k.CRT_RUN_CYC_EXCTN_SK,
               k.MBR_RATE_SK
        FROM {IDSOwner}.K_MBR_RATE_TEMP w,
             {IDSOwner}.K_MBR_RATE k
        WHERE w.MBR_UNIQ_KEY  = k.MBR_UNIQ_KEY
          AND w.EFF_DT_SK     = k.EFF_DT_SK
          AND w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK

        UNION

        SELECT
               w2.MBR_UNIQ_KEY,
               w2.EFF_DT_SK,
               w2.SRC_SYS_CD_SK,
               {CurrRunCycle},
               -1
        FROM {IDSOwner}.K_MBR_RATE_TEMP w2
        WHERE NOT EXISTS (
              SELECT 1
              FROM {IDSOwner}.K_MBR_RATE k2
              WHERE w2.MBR_UNIQ_KEY  = k2.MBR_UNIQ_KEY
                AND w2.EFF_DT_SK     = k2.EFF_DT_SK
                AND w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # -------------------------------------------------------------
    # 5. PrimaryKey Transformer logic
    # -------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("MBR_RATE_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSrcSysCd",
            F.lit(SrcSysCd)
        )
        .withColumn(
            "MBR_RATE_SK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None).cast("long"))
             .otherwise(F.col("MBR_RATE_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle).cast("long"))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # -------------------------------------------------------------
    # 6. Surrogate Key Generation (replaces KeyMgtGetNextValueConcurrent)
    # -------------------------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "MBR_RATE_SK",
        <schema>,
        <secret_name>
    )

    # -------------------------------------------------------------
    # 7. Build Transformer output links
    # -------------------------------------------------------------
    # updt link (to hf_mbr_rate)
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "MBR_UNIQ_KEY",
        "EFF_DT_SK",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("MBR_RATE_SK").alias("SUB_SBSDY_SK")
    )

    # NewKeys link (constraint svInstUpdt == 'I')
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "MBR_UNIQ_KEY",
            "EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("MBR_RATE_SK")
        )
    )

    # Keys link (for merge)
    df_keys = df_enriched.select(
        "MBR_RATE_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "MBR_UNIQ_KEY",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # -------------------------------------------------------------
    # 8. Write hashed-file hf_mbr_rate as Parquet (scenario-c)
    # -------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/MbrRatePK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # -------------------------------------------------------------
    # 9. Write sequential file K_MBR_RATE.dat
    # -------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_MBR_RATE.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # -------------------------------------------------------------
    # 10. Merge Transformer logic
    # -------------------------------------------------------------
    join_expr = (
        (F.col("all.MBR_UNIQ_KEY") == F.col("k.MBR_UNIQ_KEY")) &
        (F.col("all.EFF_DT_SK")    == F.col("k.EFF_DT_SK")) &
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"))
    )

    df_merge = (
        df_allcol.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
    )

    df_Key = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN").alias("DISCARD_IN"),
        F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("all.ERR_CT").alias("ERR_CT"),
        F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("k.MBR_RATE_SK").alias("MBR_RATE_SK"),
        F.col("all.MBR_UNIQ_KEY").alias("MBR_UNIQ_KEY"),
        F.col("all.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("all.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.SMOKER_IN").alias("SMOKER_IN"),
        F.col("all.TERM_DT_SK").alias("TERM_DT_SK")
    )

    # -------------------------------------------------------------
    # 11. Return container output
    # -------------------------------------------------------------
    return df_Key