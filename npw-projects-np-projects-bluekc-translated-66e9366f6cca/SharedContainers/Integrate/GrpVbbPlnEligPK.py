# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

PROCESSING: Shared container used for Primary Keying of GR_VBB_PLN_ELIG

MODIFICATIONS:
Developer           Date            Change Description                     Project/Altius #     Development Project     Code Reviewer     Date Reviewed
------------------- ---------------- ------------------------------------- -------------------- ----------------------- ------------------ ----------------
Kalyan Neelam       2013-06-11      Initial Programming                    4963 VBB Phase III   IntegrateNewDevl        Bhoomi Dasari     7/11/2013
"""
# join primary key info with table info
# update primary key table (K_VBB_PLN) with new keys created today
# Assign primary surrogate key
# primary key hash file NOT cleared before writing. Used in Fkey routine GetFkeyVbbPln
# Temp table is tuncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys
# Load IDS temp. table
# Hash file (hf_vbb_pln_allcol) cleared in the calling program - IdsVbbPlnExtr
# Hashfile cleared in calling program
# This container is used in:
# IdsVbbPlnExtr
# These programs need to be re-compiled when logic changes

# COMMAND ----------
from typing import Tuple
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# COMMAND ----------
def run_GrpVbbPlnEligPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic of the DataStage shared container 'GrpVbbPlnEligPK'.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input link 'AllCol' from upstream stage 'ClmProvPK'.
    df_Transform : DataFrame
        Input link 'Transform' from upstream stage 'ClmProvPK'.
    params : dict
        Dictionary of runtime parameters.

    Returns
    -------
    DataFrame
        Output link 'Key' (to stage 'ClmProvPK').
    """

    # --------------------------------------------------
    # Unpack required parameters exactly once
    # --------------------------------------------------
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # --------------------------------------------------
    # 1. Deduplicate intermediate hashed-file datasets
    #    (scenario a – intermediate hashed files)
    # --------------------------------------------------
    key_cols = [
        "VBB_PLN_UNIQ_KEY",
        "GRP_ID",
        "CLS_ID",
        "CLS_PLN_ID",
        "SRC_SYS_CD_SK"
    ]

    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols=key_cols,
        sort_cols=[]
    )

    df_DSLink56 = dedup_sort(
        df_Transform,
        partition_cols=key_cols,
        sort_cols=[]
    )

    # --------------------------------------------------
    # 2. Load temp-table K_GRP_VBB_PLN_ELIG_TEMP
    # --------------------------------------------------
    temp_table_fullname = f"{IDSOwner}.K_GRP_VBB_PLN_ELIG_TEMP"

    (
        df_DSLink56
        .write
        .format("jdbc")
        .mode("overwrite")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", temp_table_fullname)
        .save()
    )

    # --------------------------------------------------
    # 3. Runstats after load
    # --------------------------------------------------
    execute_dml(
        f"""CALL SYSPROC.ADMIN_CMD('runstats on table {temp_table_fullname} on key columns with distribution on key columns and detailed indexes all allow write access')""",
        ids_jdbc_url,
        ids_jdbc_props
    )

    # --------------------------------------------------
    # 4. Extract data (W_Extract)
    # --------------------------------------------------
    extract_query = f"""
    SELECT
        k.VBB_PLN_ELIG_SK,
        w.VBB_PLN_UNIQ_KEY,
        w.GRP_ID,
        w.CLS_ID,
        w.CLS_PLN_ID,
        w.SRC_SYS_CD_SK,
        k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_GRP_VBB_PLN_ELIG_TEMP w
    JOIN {IDSOwner}.K_GRP_VBB_PLN_ELIG k
          ON w.SRC_SYS_CD_SK   = k.SRC_SYS_CD_SK
         AND w.VBB_PLN_UNIQ_KEY = k.VBB_PLN_UNIQ_KEY
         AND w.GRP_ID           = k.GRP_ID
         AND w.CLS_ID           = k.CLS_ID
         AND w.CLS_PLN_ID       = k.CLS_PLN_ID

    UNION

    SELECT
        -1,
        w2.VBB_PLN_UNIQ_KEY,
        w2.GRP_ID,
        w2.CLS_ID,
        w2.CLS_PLN_ID,
        w2.SRC_SYS_CD_SK,
        {CurrRunCycle}
    FROM {IDSOwner}.K_GRP_VBB_PLN_ELIG_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_GRP_VBB_PLN_ELIG k2
        WHERE w2.SRC_SYS_CD_SK   = k2.SRC_SYS_CD_SK
          AND w2.VBB_PLN_UNIQ_KEY = k2.VBB_PLN_UNIQ_KEY
          AND w2.GRP_ID           = k2.GRP_ID
          AND w2.CLS_ID           = k2.CLS_ID
          AND w2.CLS_PLN_ID       = k2.CLS_PLN_ID
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query)
             .load()
    )

    # --------------------------------------------------
    # 5. PrimaryKey Transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("VBB_PLN_ELIG_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("VBB_PLN_ELIG_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # --------------------------------------------------
    # 6. Build outputs from PrimaryKey transformer
    # --------------------------------------------------
    df_updt = (
        df_enriched.select(
            "VBB_PLN_UNIQ_KEY",
            "GRP_ID",
            "CLS_ID",
            "CLS_PLN_ID",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            F.col("svSK").alias("VBB_PLN_ELIG_SK")
        )
    )

    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "VBB_PLN_UNIQ_KEY",
            "GRP_ID",
            "CLS_ID",
            "CLS_PLN_ID",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            F.col("svSK").alias("VBB_PLN_ELIG_SK")
        )
    )

    df_Keys = (
        df_enriched.select(
            F.col("svSK").alias("VBB_PLN_ELIG_SK"),
            "VBB_PLN_UNIQ_KEY",
            "GRP_ID",
            "CLS_ID",
            "CLS_PLN_ID",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --------------------------------------------------
    # 7. Persist outputs (hashed-file & sequential file)
    # --------------------------------------------------
    # a. Parquet for hf_grp_vbb_pln_elig (scenario c – parquet)
    parquet_path = f"{adls_path}/GrpVbbPlnEligPK_updt.parquet"
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

    # b. Sequential file for K_GRP_VBB_PLN_ELIG.dat
    seq_file_path = f"{adls_path}/load/K_GRP_VBB_PLN_ELIG.dat"
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

    # --------------------------------------------------
    # 8. Merge transformer logic to produce final 'Key' output
    # --------------------------------------------------
    df_key_output = (
        df_AllColOut.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.VBB_PLN_UNIQ_KEY") == F.col("k.VBB_PLN_UNIQ_KEY")) &
            (F.col("all.GRP_ID")          == F.col("k.GRP_ID")) &
            (F.col("all.CLS_ID")          == F.col("k.CLS_ID")) &
            (F.col("all.CLS_PLN_ID")      == F.col("k.CLS_PLN_ID")) &
            (F.col("all.SRC_SYS_CD_SK")   == F.col("k.SRC_SYS_CD_SK")),
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
            F.col("k.VBB_PLN_ELIG_SK"),
            F.col("all.VBB_PLN_UNIQ_KEY"),
            F.col("all.GRP_ID"),
            F.col("all.CLS_ID"),
            F.col("all.CLS_PLN_ID"),
            F.col("all.SRC_SYS_CD_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.VBB_PLN_ELIG_STRT_DT"),
            F.col("all.VBB_PLN_ELIG_END_DT")
        )
    )

    # --------------------------------------------------
    # 9. Return final output link
    # --------------------------------------------------
    return df_key_output

# COMMAND ----------