
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

"""
COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of MBR_VBB_PLN_ENR

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------

Raja Gummadi    2013-05-22               Initial Programming                                                      4963 VBB Phase III  IntegrateNewDevl                Bhoomi Dasari            7/8/2013

Annotations:
- join primary key info with table info
- update primary key table (K_VBB_PLN) with new keys created today
- Assign primary surrogate key
- primary key hash file only contains current run keys and is cleared before writing
- Temp table is tuncated before load and runstat done after load
- SQL joins temp table with key table to assign known keys
- Load IDS temp. table
- Hash file (hf_vbb_cmpnt_allcol) cleared in the calling program - IdsMbrVbbPlnEnrExtr
- Hashfile cleared in calling program
- This container is used in:
IdsMbrVbbPlnEnrExtr
These programs need to be re-compiled when logic changes
"""

def run_MbrVbbPlnEnrPK(
        df_AllCol: DataFrame,
        df_Transform: DataFrame,
        params: dict
    ) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack parameters (each only once)
    # ------------------------------------------------------------------
    CurrRunCycle           = params["CurrRunCycle"]
    SrcSysCd               = params["SrcSysCd"]
    IDSOwner               = params["IDSOwner"]
    ids_jdbc_url           = params["ids_jdbc_url"]
    ids_jdbc_props         = params["ids_jdbc_props"]
    adls_path              = params["adls_path"]
    adls_path_raw          = params["adls_path_raw"]
    adls_path_publish      = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # Hashed-file replacement – scenario a (deduplication)
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["MBR_UNIQ_KEY", "VBB_PLN_UNIQ_KEY", "SRC_SYS_CD_SK"],
        [("MBR_UNIQ_KEY", "A")]
    )

    df_transform_dedup = dedup_sort(
        df_Transform,
        ["MBR_UNIQ_KEY", "VBB_PLN_UNIQ_KEY", "SRC_SYS_CD_SK"],
        [("MBR_UNIQ_KEY", "A")]
    )
    # ------------------------------------------------------------------
    # Load temp table {IDSOwner}.K_MBR_VBB_PLN_ENR_TEMP
    # ------------------------------------------------------------------
    (
        df_transform_dedup.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MBR_VBB_PLN_ENR_TEMP")
        .mode("overwrite")
        .save()
    )

    # Optional runstats
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MBR_VBB_PLN_ENR_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props
    )
    # ------------------------------------------------------------------
    # Extract W_Extract data from DB
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT k.MBR_VBB_PLN_ENR_SK,
           w.MBR_UNIQ_KEY,
           w.VBB_PLN_UNIQ_KEY,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_MBR_VBB_PLN_ENR_TEMP w,
         {IDSOwner}.K_MBR_VBB_PLN_ENR k
    WHERE w.SRC_SYS_CD_SK   = k.SRC_SYS_CD_SK
      AND w.VBB_PLN_UNIQ_KEY = k.VBB_PLN_UNIQ_KEY
      AND w.MBR_UNIQ_KEY     = k.MBR_UNIQ_KEY
    UNION
    SELECT -1,
           w2.MBR_UNIQ_KEY,
           w2.VBB_PLN_UNIQ_KEY,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle}
    FROM {IDSOwner}.K_MBR_VBB_PLN_ENR_TEMP w2
    WHERE NOT EXISTS (
        SELECT k2.MBR_VBB_PLN_ENR_SK
        FROM {IDSOwner}.K_MBR_VBB_PLN_ENR k2
        WHERE w2.SRC_SYS_CD_SK   = k2.SRC_SYS_CD_SK
          AND w2.VBB_PLN_UNIQ_KEY = k2.VBB_PLN_UNIQ_KEY
          AND w2.MBR_UNIQ_KEY     = k2.MBR_UNIQ_KEY
    )
    """
    df_w_extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )
    # ------------------------------------------------------------------
    # PrimaryKey transformation
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("MBR_VBB_PLN_ENR_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(
                F.col("MBR_VBB_PLN_ENR_SK") == -1,
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'MBR_VBB_PLN_ENR_SK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # updt link – write parquet (hashed-file scenario c)
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        "MBR_UNIQ_KEY",
        "VBB_PLN_UNIQ_KEY",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "MBR_VBB_PLN_ENR_SK"
    )
    write_files(
        df_updt,
        f"{adls_path}/MbrVbbPlnEnrPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True
    )
    # ------------------------------------------------------------------
    # NewKeys link – write sequential file
    # ------------------------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(F.col("INSRT_UPDT_CD") == "I")
        .select(
            "MBR_UNIQ_KEY",
            "VBB_PLN_UNIQ_KEY",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "MBR_VBB_PLN_ENR_SK"
        )
    )
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_MBR_VBB_PLN_ENR.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )
    # ------------------------------------------------------------------
    # Keys link – for merge
    # ------------------------------------------------------------------
    df_keys = df_enriched.select(
        "MBR_VBB_PLN_ENR_SK",
        "MBR_UNIQ_KEY",
        "VBB_PLN_UNIQ_KEY",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )
    # ------------------------------------------------------------------
    # Merge transformer logic
    # ------------------------------------------------------------------
    df_key_out = (
        df_allcol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            [
                F.col("all.MBR_UNIQ_KEY") == F.col("k.MBR_UNIQ_KEY"),
                F.col("all.VBB_PLN_UNIQ_KEY") == F.col("k.VBB_PLN_UNIQ_KEY"),
                F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")
            ],
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
            F.col("k.MBR_VBB_PLN_ENR_SK"),
            F.col("all.MBR_UNIQ_KEY"),
            F.col("all.VBB_PLN_UNIQ_KEY"),
            F.col("all.SRC_SYS_CD_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.HIPL_ID"),
            F.col("all.HIEL_LOG_LEVEL1"),
            F.col("all.HIEL_LOG_LEVEL2"),
            F.col("all.HIEL_LOG_LEVEL3"),
            F.col("all.MEHP_ENROLLED_BY"),
            F.col("all.MEHP_STATUS"),
            F.col("all.MEHP_TERMED_BY"),
            F.col("all.MEHP_TERM_REASON"),
            F.col("all.MBR_VBB_PLN_CMPLTN_DT_SK"),
            F.col("all.MBR_VBB_PLN_ENR_DT_SK"),
            F.col("all.MBR_VBB_PLN_TERM_DT_SK"),
            F.col("all.SRC_SYS_CRT_DTM"),
            F.col("all.SRC_SYS_UPDT_DTM"),
            F.col("all.TRZ_MBR_UNVRS_ID")
        )
    )
    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_key_out
