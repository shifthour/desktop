
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
IndvBePopHlthPocGoalTransPK
Copyright 2010 Blue Cross/Blue Shield of Kansas City

PROCESSING: Primary Key Shared Container for INDV_BE_POP_HLTH_POC_GOAL_TRANS.

MODIFICATIONS:
Developer            Date         Project/Altiris #   Change Description               Development Project   Code Reviewer   Date Reviewed
------------------   -----------  ------------------  ------------------------------    -------------------   -------------   -------------
Kalyan Neelam        2010-07-19   4297               Original Programming             RebuildIntNewDevl     Steph Goddard   07/21/2010
"""

# join primary key info with table info
# Assign primary surrogate key
# Load IDS temp. table
# SQL joins temp table with key table to assign known keys
# primary key hash file only contains current run keys and is cleared before writing
# update primary key table (K_INDV_BE_POP_HLTH_POC_PROB_TRANS) with new keys created today
# This container is used in:
# AlineoIndvBePopHlthPocGoalTransExtr
# Alineo INDV_BE_POP_HLTH_POC_GOAL_TRANS Primary Key Shared Container

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_IndvBePopHlthPocGoalTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the IndvBePopHlthPocGoalTransPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Input stream “AllCol”.
    df_Transform : DataFrame
        Input stream “Transform”.
    params : dict
        Runtime parameters.

    Returns
    -------
    DataFrame
        Output stream “Key”.
    """

    # --------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # --------------------------------------------------
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    # --------------------------------------------------
    # 1. Remove intermediate hash-files (scenario a)
    # --------------------------------------------------
    df_AllCol_out = dedup_sort(
        df_AllCol,
        ["PGM_ENROLLMENT_ID", "PROB_ID", "GOAL_ID", "ROW_EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    df_Transform_out = dedup_sort(
        df_Transform,
        ["POP_HLTH_PGM_ENR_ID", "POC_PROB_ID", "POC_GOAL_ID", "ROW_EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    # --------------------------------------------------
    # 2. Pull current primary-key table from IDS
    # --------------------------------------------------
    extract_query_k = f"""
        SELECT
            INDV_BE_POP_HLTH_POC_GOAL_T_SK,
            POP_HLTH_PGM_ENR_ID,
            POC_PROB_ID,
            POC_GOAL_ID,
            ROW_EFF_DT_SK,
            SRC_SYS_CD_SK,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_POC_GOAL_TRANS
    """

    df_k = (
        spark.read.format("jdbc")
             .option("url", ids_jdbc_url)
             .options(**ids_jdbc_props)
             .option("query", extract_query_k)
             .load()
    )

    # --------------------------------------------------
    # 3. Emulate W_Extract (existing & new rows)
    # --------------------------------------------------
    join_cond = (
        (df_Transform_out["POP_HLTH_PGM_ENR_ID"] == df_k["POP_HLTH_PGM_ENR_ID"]) &
        (df_Transform_out["POC_PROB_ID"]        == df_k["POC_PROB_ID"]) &
        (df_Transform_out["POC_GOAL_ID"]        == df_k["POC_GOAL_ID"]) &
        (df_Transform_out["ROW_EFF_DT_SK"]      == df_k["ROW_EFF_DT_SK"]) &
        (df_Transform_out["SRC_SYS_CD_SK"]      == df_k["SRC_SYS_CD_SK"])
    )

    df_existing = (
        df_Transform_out.alias("w")
            .join(df_k.alias("k"), join_cond, "inner")
            .select(
                F.col("k.INDV_BE_POP_HLTH_POC_GOAL_T_SK"),
                F.col("w.POP_HLTH_PGM_ENR_ID"),
                F.col("w.POC_PROB_ID"),
                F.col("w.POC_GOAL_ID"),
                F.col("w.ROW_EFF_DT_SK"),
                F.col("w.SRC_SYS_CD_SK"),
                F.col("k.CRT_RUN_CYC_EXCTN_SK")
            )
    )

    df_new = (
        df_Transform_out.alias("w2")
            .join(df_k.alias("k2"), join_cond, "left_anti")
            .select(
                F.lit(-1).alias("INDV_BE_POP_HLTH_POC_GOAL_T_SK"),
                F.col("w2.POP_HLTH_PGM_ENR_ID"),
                F.col("w2.POC_PROB_ID"),
                F.col("w2.POC_GOAL_ID"),
                F.col("w2.ROW_EFF_DT_SK"),
                F.col("w2.SRC_SYS_CD_SK"),
                F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK")
            )
    )

    df_W_Extract = df_existing.unionByName(df_new)

    # --------------------------------------------------
    # 4. Primary-Key transformer logic
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
            .withColumn(
                "INSRT_UPDT_CD",
                F.when(F.col("INDV_BE_POP_HLTH_POC_GOAL_T_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
            )
            .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
            .withColumn(
                "CRT_RUN_CYC_EXCTN_SK",
                F.when(F.col("INSRT_UPDT_CD") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
            )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"INDV_BE_POP_HLTH_POC_GOAL_T_SK",<schema>,<secret_name>)

    # --------------------------------------------------
    # 5. Split into Keys / Updt / NewKeys links
    # --------------------------------------------------
    df_keys = df_enriched.select(
        "INDV_BE_POP_HLTH_POC_GOAL_T_SK",
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "POC_GOAL_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK"
    )

    df_updt = df_enriched.select(
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "POC_GOAL_ID",
        "ROW_EFF_DT_SK",
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        "CRT_RUN_CYC_EXCTN_SK",
        "INDV_BE_POP_HLTH_POC_GOAL_T_SK"
    )

    df_newkeys = (
        df_enriched.filter(F.col("INSRT_UPDT_CD") == "I")
                   .select(
                       "POP_HLTH_PGM_ENR_ID",
                       "POC_PROB_ID",
                       "POC_GOAL_ID",
                       "ROW_EFF_DT_SK",
                       "SRC_SYS_CD_SK",
                       "CRT_RUN_CYC_EXCTN_SK",
                       "INDV_BE_POP_HLTH_POC_GOAL_T_SK"
                   )
    )

    # --------------------------------------------------
    # 6. Persist hash-file replacement & sequential file
    # --------------------------------------------------
    parquet_updt_path = f"{adls_path}/IndvBePopHlthPocGoalTransPK_Updt.parquet"
    write_files(df_updt, parquet_updt_path, ',', 'overwrite', True, True, '"')

    seq_path = f"{adls_path}/load/K_INDV_BE_POP_HLTH_POC_GOAL_TRANS.dat"
    write_files(df_newkeys, seq_path, ',', 'overwrite', False, True, '"')

    # --------------------------------------------------
    # 7. Merge transformer (AllColOut + Keys) -> Key output
    # --------------------------------------------------
    merge_cond = (
        (df_AllCol_out["PGM_ENROLLMENT_ID"] == df_keys["POP_HLTH_PGM_ENR_ID"]) &
        (df_AllCol_out["PROB_ID"]           == df_keys["POC_PROB_ID"]) &
        (df_AllCol_out["GOAL_ID"]           == df_keys["POC_GOAL_ID"]) &
        (df_AllCol_out["ROW_EFF_DT_SK"]     == df_keys["ROW_EFF_DT_SK"])
    )

    df_output = (
        df_AllCol_out.alias("all")
            .join(df_keys.alias("k"), merge_cond, "left")
            .select(
                F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
                F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
                F.col("all.DISCARD_IN").alias("DISCARD_IN"),
                F.col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
                F.col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
                F.col("all.ERR_CT").alias("ERR_CT"),
                F.col("all.RECYCLE_CT").alias("RECYCLE_CT"),
                F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
                F.col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
                F.col("k.INDV_BE_POP_HLTH_POC_GOAL_T_SK").alias("INDV_BE_POP_HLTH_POC_GOAL_T_SK"),
                F.col("all.PGM_ENROLLMENT_ID").alias("PGM_ENROLLMENT_ID"),
                F.col("all.PROB_ID").alias("PROB_ID"),
                F.col("all.GOAL_ID").alias("GOAL_ID"),
                F.col("all.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
                F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
                F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
                F.col("all.GOAL_CODE").alias("GOAL_CODE"),
                F.col("all.GOAL_OUTCOME_CTGY_CODE").alias("GOAL_OUTCOME_CTGY_CODE"),
                F.col("all.GOAL_OUTCOME_CODE").alias("GOAL_OUTCOME_CODE"),
                F.col("all.GOAL_TYPE_CODE").alias("GOAL_TYPE_CODE"),
                F.col("all.GOAL_ACTUAL_COMPLTN_DATE").alias("GOAL_ACTUAL_COMPLTN_DATE"),
                F.col("all.GOAL_START_DATE").alias("GOAL_START_DATE"),
                F.col("all.GOAL_TGT_COMPLTN_DATE").alias("GOAL_TGT_COMPLTN_DATE"),
                F.col("all.GOAL_CREATED").alias("GOAL_CREATED"),
                F.col("all.ROW_TERM_DT_SK").alias("ROW_TERM_DT_SK"),
                F.col("all.INDV_BE_KEY").alias("INDV_BE_KEY"),
                F.col("all.GOAL_MODIFIED").alias("GOAL_MODIFIED")
            )
    )

    return df_output
