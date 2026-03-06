
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# --------------------------------------------------------------------------------
# COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY
#
# JOB NAME:         IndvBePopHlthPgmCstmFldTransPK
#
# DESCRIPTION:      Creates Primary key for INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS
#                   table.  Used in jobs IdsIndvBePopHlthPgmCstmFldTransExtr
#
# PROCESSING NOTES:
#   * SQL joins temp table with key table to assign known keys
#   * Temp table is truncated before load and runstats executed after load
#   * Primary-key hash file only contains current-run keys and is cleared
#     before writing
#   * Updates primary-key table (K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS) with
#     new keys created today
#   * Assigns surrogate keys and applies business logic
#   * Loads IDS temp table
#
# MODIFICATIONS:
#   Developer                 Date        Project / CR     Description
#   -----------------------   ----------  ---------------  ----------------------------------
#   Abhiram Dasarathy         04/18/2012  4735 Alineo 3.2  Original Programming / Upgrade
#   Karthik Chintalapani      01/11/2013  4863 - FEP       Added PGM_CSTM_FLD_SEQ_NO column
# --------------------------------------------------------------------------------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# --------------------------------------------------------------------------------
# main shared-container function
# --------------------------------------------------------------------------------
def run_IndvBePopHlthPgmCstmFldTransPK(
    df_AlineoIndvBePopHlthCstmDataOut2: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-container logic converted from DataStage job
    IndvBePopHlthPgmCstmFldTransPK.
    """

    # --------------------------------------------------
    # unpack parameters (exactly once)
    # --------------------------------------------------
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]

    CurrRunCycle        = params["CurrRunCycle"]
    CurrDate            = params["CurrDate"]
    RunID               = params["RunID"]
    SourceSK            = params["SourceSK"]

    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]

    # --------------------------------------------------
    # BusinessRules stage
    # --------------------------------------------------
    df_allcol = (
        df_AlineoIndvBePopHlthCstmDataOut2
        .withColumn("POP_HLTH_PGM_ENR_ID", F.col("POP_HLTH_PGM_ENR_ID"))
        .withColumn("MEMBER_ID",            F.col("MEMBER_ID"))
        .withColumn("PGM_CSTM_FLD_ID",      F.col("CSTM_FLD_ID"))
        .withColumn("EFF_DT_SK",            F.col("ROW_EFF_DT"))
        .withColumn("JOB_EXCTN_RCRD_ERR_SK",F.lit(0))
        .withColumn("INSRT_UPDT_CD",        F.lit("I"))
        .withColumn("DISCARD_IN",           F.lit("N"))
        .withColumn("PASS_THRU_IN",         F.lit("Y"))
        .withColumn("FIRST_RECYC_DT",       F.lit(CurrDate))
        .withColumn("ERR_CT",               F.lit(0))
        .withColumn("RECYCLE_CT",           F.lit(0))
        .withColumn("SRC_SYS_CD",           F.lit("ALINEO"))
        .withColumn(
            "PRI_KEY_STRING",
            F.concat_ws(
                ";",
                F.col("POP_HLTH_PGM_ENR_ID"),
                F.col("CSTM_FLD_ID"),
                F.col("ROW_EFF_DT"),
                F.lit("ALINEO")
            )
        )
        .withColumn("VAL",                  F.col("VAL"))
        .withColumn("CSTM_DTL_CREATED",     F.col("CSTM_DTL_CREATED"))
        .withColumn("PGM_CSTM_FLD_SEQ_NO",  F.col("SEQ_NO"))
        .withColumn("ROW_TERM_DT_SK",       F.col("ROW_TERM_DT"))
    )

    # duplicate-removal replacing intermediate hashed file
    df_allcol_dedup = dedup_sort(
        df_allcol,
        ["POP_HLTH_PGM_ENR_ID", "PGM_CSTM_FLD_ID", "EFF_DT_SK", "PGM_CSTM_FLD_SEQ_NO"],
        []
    )

    # DataFrame for temp-table load
    df_transform = (
        df_AlineoIndvBePopHlthCstmDataOut2
        .withColumn("POP_HLTH_PGM_ENR_ID",   F.col("POP_HLTH_PGM_ENR_ID"))
        .withColumn("PGM_CSTM_FLD_ID",       F.col("CSTM_FLD_ID"))
        .withColumn("ROW_EFF_DT_SK",         F.col("ROW_EFF_DT"))
        .withColumn("PGM_CSTM_FLD_SEQ_NO",   F.col("SEQ_NO"))
        .withColumn("SRC_SYS_CD_SK",         F.lit(SourceSK))
        .select(
            "POP_HLTH_PGM_ENR_ID",
            "PGM_CSTM_FLD_ID",
            "ROW_EFF_DT_SK",
            "PGM_CSTM_FLD_SEQ_NO",
            "SRC_SYS_CD_SK"
        )
    )

    # --------------------------------------------------
    # load temp table (truncate ‑> insert ‑> runstats)
    # --------------------------------------------------
    query_truncate = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS_TEMP')"
    execute_dml(query_truncate, ids_jdbc_url, ids_jdbc_props)

    (
        df_transform
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS_TEMP")
        .mode("append")
        .save()
    )

    query_runstats = (
        f"CALL SYSPROC.ADMIN_CMD("
        f"'runstats on table {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(query_runstats, ids_jdbc_url, ids_jdbc_props)

    # --------------------------------------------------
    # extract W_Extract set
    # --------------------------------------------------
    extract_query = f"""
        SELECT
            T.POP_HLTH_PGM_ENR_ID,
            T.PGM_CSTM_FLD_ID,
            T.ROW_EFF_DT_SK,
            T.PGM_CSTM_FLD_SEQ_NO,
            T.SRC_SYS_CD_SK,
            K.CRT_RUN_CYC_EXCTN_SK,
            K.INDV_BE_POP_HLTH_PGM_CFLD_T_SK
        FROM
            {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS         K,
            {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS_TEMP    T
        WHERE
            T.POP_HLTH_PGM_ENR_ID = K.POP_HLTH_PGM_ENR_ID AND
            T.PGM_CSTM_FLD_ID     = K.PGM_CSTM_FLD_ID     AND
            T.ROW_EFF_DT_SK       = K.ROW_EFF_DT_SK       AND
            T.PGM_CSTM_FLD_SEQ_NO = K.PGM_CSTM_FLD_SEQ_NO AND
            T.SRC_SYS_CD_SK       = K.SRC_SYS_CD_SK
        UNION
        SELECT
            T2.POP_HLTH_PGM_ENR_ID,
            T2.PGM_CSTM_FLD_ID,
            T2.ROW_EFF_DT_SK,
            T2.PGM_CSTM_FLD_SEQ_NO,
            T2.SRC_SYS_CD_SK,
            {CurrRunCycle},
            -1
        FROM
            {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS_TEMP T2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS K2
            WHERE
                T2.POP_HLTH_PGM_ENR_ID = K2.POP_HLTH_PGM_ENR_ID AND
                T2.PGM_CSTM_FLD_ID     = K2.PGM_CSTM_FLD_ID     AND
                T2.ROW_EFF_DT_SK       = K2.ROW_EFF_DT_SK       AND
                T2.PGM_CSTM_FLD_SEQ_NO = K2.PGM_CSTM_FLD_SEQ_NO AND
                T2.SRC_SYS_CD_SK       = K2.SRC_SYS_CD_SK
        )
    """

    df_w_extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # PrimaryKey transformer
    # --------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("INDV_BE_POP_HLTH_PGM_CFLD_T_SK") == -1, F.lit("I"))
             .otherwise(F.lit("U"))
        )
        .withColumn("INDV_BE_POP_HLTH_PGM_CFLD_T_SK",
                    F.when(F.col("svInstUpdt") == "I", F.lit(None).cast("long"))
                     .otherwise(F.col("INDV_BE_POP_HLTH_PGM_CFLD_T_SK"))
        )
    )

    # surrogate-key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'INDV_BE_POP_HLTH_PGM_CFLD_T_SK',
        <schema>,
        <secret_name>
    )

    df_enriched = (
        df_enriched
        .withColumn("svSrcSysCd", F.lit("ALINEO"))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svSK", F.col("INDV_BE_POP_HLTH_PGM_CFLD_T_SK"))
    )

    # --------------------------------------------------
    # build three outgoing links from PrimaryKey
    # --------------------------------------------------
    select_cols_pk = [
        "POP_HLTH_PGM_ENR_ID",
        "PGM_CSTM_FLD_ID",
        "ROW_EFF_DT_SK",
        "PGM_CSTM_FLD_SEQ_NO",
        "SRC_SYS_CD_SK",
        "svCrtRunCycExctnSk",
        "svSK"
    ]

    df_updt = df_enriched.select(
        *select_cols_pk
    )

    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == "I")
        .select(*select_cols_pk)
    )

    df_keys = df_enriched.select(
        "POP_HLTH_PGM_ENR_ID",
        "PGM_CSTM_FLD_ID",
        "ROW_EFF_DT_SK",
        "PGM_CSTM_FLD_SEQ_NO",
        "SRC_SYS_CD_SK",
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("INDV_BE_POP_HLTH_PGM_CFLD_T_SK")
    )

    # --------------------------------------------------
    # Merge transformer
    # --------------------------------------------------
    df_merge = (
        df_allcol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            [
                F.col("all.POP_HLTH_PGM_ENR_ID") == F.col("k.POP_HLTH_PGM_ENR_ID"),
                F.col("all.PGM_CSTM_FLD_ID")     == F.col("k.PGM_CSTM_FLD_ID"),
                F.col("all.EFF_DT_SK")           == F.col("k.ROW_EFF_DT_SK"),
                F.col("all.PGM_CSTM_FLD_SEQ_NO") == F.col("k.PGM_CSTM_FLD_SEQ_NO")
            ],
            "left"
        )
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("all.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("all.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("k.INDV_BE_POP_HLTH_PGM_CFLD_T_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.MEMBER_ID").alias("INDV_BE_KEY"),
            F.col("all.PGM_CSTM_FLD_ID"),
            F.col("all.EFF_DT_SK").alias("ROW_EFF_DT_SK"),
            F.col("all.PGM_CSTM_FLD_SEQ_NO"),
            F.col("all.CSTM_DTL_CREATED").alias("PGM_CSTM_FLD_CRT_DT_SK"),
            F.col("all.ROW_TERM_DT_SK"),
            F.col("all.VAL").alias("PGM_CSTM_FLD_VAL_TX"),
            F.col("all.POP_HLTH_PGM_ENR_ID")
        )
    )

    # --------------------------------------------------
    # write parquet for hf_indv_be_pop_hlth_pgm_cstm_fld_trans (scenario c)
    # --------------------------------------------------
    file_path_updt = f"{adls_path}/IndvBePopHlthPgmCstmFldTransPK_updt.parquet"
    write_files(
        df_updt,
        file_path_updt,
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )

    # --------------------------------------------------
    # write sequential file for new keys
    # --------------------------------------------------
    file_path_seq = f"{adls_path}/load/K_INDV_BE_POP_HLTH_PGM_CSTM_FLD_TRANS.dat"
    write_files(
        df_newkeys,
        file_path_seq,
        ",",
        "overwrite",
        False,
        True,
        "\"",
        None
    )

    # --------------------------------------------------
    # final container output
    # --------------------------------------------------
    return df_merge
