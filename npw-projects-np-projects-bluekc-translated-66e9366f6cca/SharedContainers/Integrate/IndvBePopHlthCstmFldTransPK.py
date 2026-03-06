# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JOB NAME:  IndvBePopHlthCstmFldTransPK

DESCRIPTION:
    Creates Primary key for INDV_BE_POP_HLTH_CSTM_FLD_TRANS table.
    Used in jobs IdsIndvBePopHlthCstmFldTransExtr.

PROCESSING NOTES (from DataStage Annotations):
    • SQL joins temp table with key table to assign known keys
    • Temp table is truncated before load and runstats done after load
    • Primary key hash file only contains current run keys and is cleared before writing
    • Update primary key table (K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS) with new keys created today
    • Join primary key info with table info
    • Assign primary surrogate key
    • Apply business logic
    • Load IDS temp. table
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --------------------------------------------------------------------------------
def run_IndvBePopHlthCstmFldTransPK(
    df_AlineoIndvBePopHlthCstmDataOut2: DataFrame,
    params: dict
) -> DataFrame:
    """
    Translated shared-container logic for IndvBePopHlthCstmFldTransPK.

    Parameters
    ----------
    df_AlineoIndvBePopHlthCstmDataOut2 : DataFrame
        Container input stream "AlineoIndvBePopHlthCstmDataOut2"
    params : dict
        Runtime parameters & secrets already provided by the caller.

    Returns
    -------
    DataFrame
        Container output stream "Key"
    """

    # ----------------------------------------------------
    # 1. Unpack runtime parameters (exactly once each)
    # ----------------------------------------------------
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    CurrRunCycle          = params["CurrRunCycle"]
    CurrDate              = params["CurrDate"]
    SourceSK              = params["SourceSK"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]

    # ----------------------------------------------------
    # 2. BusinessRules stage – derivations
    # ----------------------------------------------------
    df_br = (
        df_AlineoIndvBePopHlthCstmDataOut2
        .withColumn("RowPassThru", F.lit("Y"))
        .withColumn(
            "CreateDt",
            F.expr("format_date(CSTM_DTL_CREATED, 'ACCESS', 'Timestamp', 'CCYY-MM-DD')")
        )
        .withColumn(
            "ModifiedDt",
            F.when(
                F.length(F.trim(F.col("CSTM_DTL_VAL_MODIFIED"))) == 0,
                F.lit(CurrDate)
            ).otherwise(
                F.expr("format_date(CSTM_DTL_VAL_MODIFIED, 'ACCESS', 'Timestamp', 'CCYY-MM-DD')")
            )
        )
        .withColumn(
            "EffectiveDt",
            F.when(F.col("ModifiedDt") == F.lit(CurrDate), F.col("CreateDt")).otherwise(F.col("ModifiedDt"))
        )
    )

    # -------------------------------
    # Output link “AllCol”
    # -------------------------------
    df_allcol = (
        df_br
        .select(
            F.col("MEMBER_ID"),
            F.col("CSTM_FLD_ID"),
            F.col("EffectiveDt").alias("EFF_DT_SK"),
            F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.lit("I").alias("INSRT_UPDT_CD"),
            F.lit("N").alias("DISCARD_IN"),
            F.col("RowPassThru").alias("PASS_THRU_IN"),
            F.lit(CurrDate).alias("FIRST_RECYC_DT"),
            F.lit(0).alias("ERR_CT"),
            F.lit(0).alias("RECYCLE_CT"),
            F.lit("ALINEO").alias("SRC_SYS_CD"),
            F.concat_ws(";", F.lit("ALINEO"), F.col("MEMBER_ID"), F.col("CSTM_FLD_ID"), F.col("EffectiveDt")).alias("PRI_KEY_STRING"),
            F.col("VAL"),
            F.col("CreateDt").alias("CSTM_DTL_CREATED"),
            F.col("ModifiedDt").alias("CSTM_DTL_VAL_MODIFIED")
        )
    )

    # Deduplicate – hash-file scenario a
    df_allcol = dedup_sort(
        df_allcol,
        partition_cols=["MEMBER_ID", "CSTM_FLD_ID", "EFF_DT_SK"],
        sort_cols=[]
    )

    # -------------------------------
    # Output link “Transform” for temp table load
    # -------------------------------
    df_transform = (
        df_br
        .select(
            F.col("MEMBER_ID").alias("INDV_BE_KEY"),
            F.col("CSTM_FLD_ID").alias("MBR_CSTM_FLD_ID"),
            F.col("EffectiveDt").alias("EFF_DT_SK"),
            F.lit(SourceSK).alias("SRC_SYS_CD_SK")
        )
    )

    # ----------------------------------------------------
    # 3. Persist temp rows to DB (truncate & load)
    # ----------------------------------------------------
    truncate_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS_TEMP')"
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_transform
        .write
        .mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .option("dbtable", f"{IDSOwner}.K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS_TEMP")
        .options(**ids_jdbc_props)
        .save()
    )

    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    # ----------------------------------------------------
    # 4. Extract W_Extract (join current keys with temp)
    # ----------------------------------------------------
    extract_query = f"""
    SELECT
        T.INDV_BE_KEY,
        T.MBR_CSTM_FLD_ID,
        T.EFF_DT_SK,
        T.SRC_SYS_CD_SK,
        K.CRT_RUN_CYC_EXCTN_SK,
        K.INDV_BE_POP_HLTH_CSTM_FLD_T_SK
    FROM
        {IDSOwner}.K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS K,
        {IDSOwner}.K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS_TEMP T
    WHERE
        T.INDV_BE_KEY      = K.INDV_BE_KEY AND
        T.MBR_CSTM_FLD_ID  = K.MBR_CSTM_FLD_ID AND
        T.EFF_DT_SK        = K.EFF_DT_SK AND
        T.SRC_SYS_CD_SK    = K.SRC_SYS_CD_SK

    UNION

    SELECT
        T2.INDV_BE_KEY,
        T2.MBR_CSTM_FLD_ID,
        T2.EFF_DT_SK,
        T2.SRC_SYS_CD_SK,
        {CurrRunCycle},
        -1
    FROM
        {IDSOwner}.K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS_TEMP T2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS K2
        WHERE
            T2.INDV_BE_KEY      = K2.INDV_BE_KEY AND
            T2.MBR_CSTM_FLD_ID  = K2.MBR_CSTM_FLD_ID AND
            T2.EFF_DT_SK        = K2.EFF_DT_SK AND
            T2.SRC_SYS_CD_SK    = K2.SRC_SYS_CD_SK
    )
    """

    df_w_extract = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ----------------------------------------------------
    # 5. PrimaryKey transformer logic
    # ----------------------------------------------------
    df_primary = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("INDV_BE_POP_HLTH_CSTM_FLD_T_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit("ALINEO"))
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("INDV_BE_POP_HLTH_CSTM_FLD_T_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Standard name for surrogate-key enrichment
    df_enriched = df_primary
    df_enriched = SurrogateKeyGen(df_enriched, <DB sequence name>, "svSK", <schema>, <secret_name>)

    # -------------------------------
    # Output link “updt” (to hash file – scenario c)
    # -------------------------------
    df_updt = (
        df_enriched
        .select(
            "INDV_BE_KEY",
            "MBR_CSTM_FLD_ID",
            "EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("INDV_BE_POP_HLTH_CSTM_FLD_T_SK")
        )
    )

    parquet_path_updt = f"{adls_path}/IndvBePopHlthCstmFldTransPK_updt.parquet"
    write_files(
        df_updt,
        parquet_path_updt,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # -------------------------------
    # Output link “NewKeys” – only inserts
    # -------------------------------
    df_newkeys = df_updt.filter(F.col("INDV_BE_POP_HLTH_CSTM_FLD_T_SK").isNull())

    seq_file_path = f"{adls_path}/load/K_INDV_BE_POP_HLTH_CSTM_FLD_TRANS.dat"
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

    # -------------------------------
    # Output link “Keys” – to Merge
    # -------------------------------
    df_keys = (
        df_enriched
        .select(
            "INDV_BE_KEY",
            "MBR_CSTM_FLD_ID",
            "EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("INDV_BE_POP_HLTH_CSTM_FLD_T_SK")
        )
    )

    # ----------------------------------------------------
    # 6. Merge stage – join keys & all columns
    # ----------------------------------------------------
    join_expr = [
        df_allcol["MEMBER_ID"]      == df_keys["INDV_BE_KEY"],
        df_allcol["CSTM_FLD_ID"]    == df_keys["MBR_CSTM_FLD_ID"],
        df_allcol["EFF_DT_SK"]      == df_keys["EFF_DT_SK"]
    ]

    df_merge = (
        df_allcol.alias("all")
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
            "k.INDV_BE_POP_HLTH_CSTM_FLD_T_SK".alias("INDV_BE_POP_HLTH_CSTM_FLD_SK"),
            "k.CRT_RUN_CYC_EXCTN_SK",
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            "k.INDV_BE_KEY",
            "all.CSTM_FLD_ID".alias("MBR_CSTM_FLD_ID"),
            "all.EFF_DT_SK",
            "all.CSTM_DTL_CREATED".alias("CRT_DT_SK"),
            F.lit("2199-12-31").alias("TERM_DT_SK"),
            "all.VAL".alias("MBR_CSTM_FLD_VAL_TX"),
            "all.CSTM_DTL_VAL_MODIFIED".alias("MODIFIED_DATE")
        )
    )

    # ----------------------------------------------------
    # 7. Return container output
    # ----------------------------------------------------
    return df_merge