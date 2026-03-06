
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# -----------------------------------------------
# IndvBePopHlthPocFocusTransPK – Primary Key Shared Container
#
# Description:
# Copyright 2010 Blue Cross/Blue Shield of Kansas City
#
# PROCESSING: Primary Key Shared Container for INDV_BE_POP_HLTH_POC_FOCUS_TRANS.
#
# MODIFICATIONS:
# Developer                       Date               Project/Altiris #               Change Description
# ----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------
# Abhiram Dasarathy          2012-04-17     4735 - Alineo 3.2            Original Programming
#                                                                   Upgrade
#
# Annotations:
# - SQL joins temp table with key table to assign known keys
# - Load IDS temp. table
# - join primary key info with table info
# - Assign primary surrogate key
# - This container is used in:
#   AlineoIndvBePopHlthPocFocusTransExtr
#   These programs need to be re-compiled when logic changes
# - Alineo INDV_BE_POP_HLTH_POC_FOCUS_TRANS Primary Key Shared Container
# -----------------------------------------------

def run_IndvBePopHlthPocFocusTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    PySpark translation of DataStage shared container: IndvBePopHlthPocFocusTransPK
    """

    # ------------------------------------------------
    # Parameter Unpacking
    # ------------------------------------------------
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    SrcSysCd             = params["SrcSysCd"]
    CurrRunCycle         = params["CurrRunCycle"]

    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]

    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]

    # ------------------------------------------------
    # 1. Remove duplicates for hashed-file (scenario a) stages
    # ------------------------------------------------
    dedup_cols_allcol = [
        "PGM_ENROLLMENT_ID",
        "PROB_ID",
        "GOAL_ID",
        "GOAL_FOCUS_CODE",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK"
    ]
    df_AllCol_dedup = dedup_sort(df_AllCol, dedup_cols_allcol, [])

    dedup_cols_transform = [
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "POC_GOAL_ID",
        "GOAL_FOCUS_CD",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK"
    ]
    df_Transform_dedup = dedup_sort(df_Transform, dedup_cols_transform, [])

    # ------------------------------------------------
    # 2. Load temp table K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS_TEMP
    # ------------------------------------------------
    before_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS_TEMP')"
    execute_dml(before_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform_dedup.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS_TEMP")
        .mode("append")
        .save()
    )

    after_sql = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------
    # 3. Extract from temp table with union logic
    # ------------------------------------------------
    extract_query = f"""
        SELECT 
                 k.INDV_BE_POP_HLTH_POC_FOCS_T_SK,
                 w.POP_HLTH_PGM_ENR_ID,
                 w.POC_PROB_ID,
                 w.POC_GOAL_ID,
                 w.GOAL_FOCUS_CD,
                 w.ROW_EFF_DT_SK,
                 w.SRC_SYS_CD_SK,
                 k.CRT_RUN_CYC_EXCTN_SK
        FROM 
                 {IDSOwner}.K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS_TEMP w,
                 {IDSOwner}.K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS k
        WHERE
                 w.POP_HLTH_PGM_ENR_ID = k.POP_HLTH_PGM_ENR_ID AND
                 w.POC_PROB_ID         = k.POC_PROB_ID AND
                 w.POC_GOAL_ID         = k.POC_GOAL_ID AND
                 w.GOAL_FOCUS_CD       = k.GOAL_FOCUS_CD AND
                 w.ROW_EFF_DT_SK       = k.ROW_EFF_DT_SK AND
                 w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
        UNION
        SELECT
                 -1,
                 w2.POP_HLTH_PGM_ENR_ID,
                 w2.POC_PROB_ID,
                 w2.POC_GOAL_ID,
                 w2.GOAL_FOCUS_CD,
                 w2.ROW_EFF_DT_SK,
                 w2.SRC_SYS_CD_SK,
                 {CurrRunCycle}
        FROM
                 {IDSOwner}.K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS_TEMP w2
        WHERE NOT EXISTS (
                 SELECT k2.INDV_BE_POP_HLTH_POC_FOCS_T_SK
                 FROM {IDSOwner}.K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS k2
                 WHERE w2.POP_HLTH_PGM_ENR_ID = k2.POP_HLTH_PGM_ENR_ID AND
                       w2.POC_PROB_ID         = k2.POC_PROB_ID AND
                       w2.POC_GOAL_ID         = k2.POC_GOAL_ID AND
                       w2.GOAL_FOCUS_CD       = k2.GOAL_FOCUS_CD AND
                       w2.ROW_EFF_DT_SK       = k2.ROW_EFF_DT_SK AND
                       w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
        )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------
    # 4. Primary Key Transformation
    # ------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svInstUpdt",
                    F.when(F.col("INDV_BE_POP_HLTH_POC_FOCS_T_SK") == -1, F.lit("I"))
                     .otherwise(F.lit("U")))
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
    )

    # Surrogate key generation – mandatory placeholder call
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"INDV_BE_POP_HLTH_POC_FOCS_T_SK",<schema>,<secret_name>)

    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnSk",
        F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle))
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )

    # Keys link
    df_keys = (
        df_enriched.select(
            "INDV_BE_POP_HLTH_POC_FOCS_T_SK",
            F.col("POP_HLTH_PGM_ENR_ID"),
            F.col("POC_PROB_ID"),
            F.col("POC_GOAL_ID"),
            F.col("GOAL_FOCUS_CD").alias("POC_GOAL_FOCUS_CODE"),
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # Updt link
    df_updt = (
        df_enriched.select(
            F.col("POP_HLTH_PGM_ENR_ID"),
            F.col("POC_PROB_ID"),
            F.col("POC_GOAL_ID"),
            F.col("GOAL_FOCUS_CD").alias("POC_GOAL_FOCUS_CODE"),
            "ROW_EFF_DT_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("INDV_BE_POP_HLTH_POC_FOCS_T_SK").alias("INDV_BE_POP_HLTH_POC_BARR_T_SK")
        )
    )

    # NewKeys link (insert only)
    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I").select(
            F.col("POP_HLTH_PGM_ENR_ID"),
            F.col("POC_PROB_ID"),
            F.col("POC_GOAL_ID"),
            F.col("GOAL_FOCUS_CD").alias("POC_GOAL_FOCUS_CODE"),
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("INDV_BE_POP_HLTH_POC_FOCS_T_SK").alias("INDV_BE_POP_HLTH_POC_BARR_T_SK")
        )
    )

    # ------------------------------------------------
    # 5. Write sequential and parquet outputs
    # ------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_INDV_BE_POP_HLTH_POC_FOCUS_TRANS.dat",
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )

    write_files(
        df_updt,
        f"{adls_path}/IndvBePopHlthPocFocusTransPK_Updt.parquet",
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # ------------------------------------------------
    # 6. Merge Transformer Logic – combine Keys with AllColOut
    # ------------------------------------------------
    merge_join_expr = (
        (df_AllCol_dedup["PGM_ENROLLMENT_ID"] == df_keys["POP_HLTH_PGM_ENR_ID"]) &
        (df_AllCol_dedup["PROB_ID"]           == df_keys["POC_PROB_ID"]) &
        (df_AllCol_dedup["GOAL_ID"]           == df_keys["POC_GOAL_ID"]) &
        (df_AllCol_dedup["GOAL_FOCUS_CODE"]   == df_keys["POC_GOAL_FOCUS_CODE"]) &
        (df_AllCol_dedup["ROW_EFF_DT_SK"]     == df_keys["ROW_EFF_DT_SK"])
    )

    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), merge_join_expr, "left")
    )

    df_Key = (
        df_merge.select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("k.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.INDV_BE_POP_HLTH_POC_FOCS_T_SK"),
            F.col("all.PGM_ENROLLMENT_ID"),
            F.col("all.PROB_ID"),
            F.col("all.GOAL_ID"),
            F.col("all.GOAL_FOCUS_CODE"),
            F.col("all.ROW_EFF_DT_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.GOAL_FOCUS_DEL_FLAG"),
            F.col("all.GOAL_FOCUS_CREATED"),
            F.col("all.ROW_TERM_DT_SK"),
            F.col("all.INDV_BE_KEY"),
            F.col("all.GOAL_FOCUS_MODIFIED")
        )
    )

    # ------------------------------------------------
    # Return container output(s)
    # ------------------------------------------------
    return df_Key
