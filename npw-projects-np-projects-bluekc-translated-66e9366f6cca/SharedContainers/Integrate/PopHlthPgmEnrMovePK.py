# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
PopHlthPgmEnrMovePK

Copyright 2012 Blue Cross/Blue Shield of Kansas City

CALLED BY : AlineoPopHlthPgmEnrMoveExtr

PROCESSING: Primary Key Shared Container for POP_HLTH_PGM_ENR_MOVE

MODIFICATIONS:
Developer        Date         Project/Altiris #   Change Description           Development Project   Code Reviewer   Date Reviewed
Manasa Andru     2012-10-30   4426                Initial Programming           IntegrateNewDevl      Kalyan Neelam    2012-10-30

Annotations:
- join primary key info with table info
- Assign primary surrogate key
- update primary key table (K_POP_HLTH_PGM_ENR_MOVE) with new keys created today
- primary key hash file only contains current run keys and is cleared before writing
- SQL joins temp table with key table to assign known keys
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_PopHlthPgmEnrMovePK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    IDSOwner = params["IDSOwner"]
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    ids_secret_name = params["ids_secret_name"]

    df_allcol_dedup = dedup_sort(
        df_AllCol,
        ["POP_HLTH_PGM_ENR_ID", "PGM_ENR_MOVED_DTM", "SRC_SYS_CD"],
        [],
    )

    execute_dml(
        f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_POP_HLTH_PGM_ENR_MOVE_TEMP')",
        ids_jdbc_url,
        ids_jdbc_props,
    )

    (
        df_Transform.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_POP_HLTH_PGM_ENR_MOVE_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_POP_HLTH_PGM_ENR_MOVE_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props,
    )

    extract_query = f"""
    SELECT k.POP_HLTH_PGM_ENR_MOVE_SK,
           w.POP_HLTH_PGM_ENR_ID,
           w.PGM_ENR_MOVED_DTM,
           w.SRC_SYS_CD_SK,
           k.CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_POP_HLTH_PGM_ENR_MOVE_TEMP w,
         {IDSOwner}.K_POP_HLTH_PGM_ENR_MOVE k
    WHERE w.POP_HLTH_PGM_ENR_ID = k.POP_HLTH_PGM_ENR_ID
      AND w.PGM_ENR_MOVED_DTM   = k.PGM_ENR_MOVED_DTM
      AND w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK
    UNION
    SELECT -1,
           w2.POP_HLTH_PGM_ENR_ID,
           w2.PGM_ENR_MOVED_DTM,
           w2.SRC_SYS_CD_SK,
           {CurrRunCycle}
    FROM {IDSOwner}.K_POP_HLTH_PGM_ENR_MOVE_TEMP w2
    WHERE NOT EXISTS (
        SELECT 1
        FROM {IDSOwner}.K_POP_HLTH_PGM_ENR_MOVE k2
        WHERE w2.POP_HLTH_PGM_ENR_ID = k2.POP_HLTH_PGM_ENR_ID
          AND w2.PGM_ENR_MOVED_DTM   = k2.PGM_ENR_MOVED_DTM
          AND w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
    )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    df_enriched = (
        df_W_Extract.withColumn(
            "svInstUpdt",
            F.when(
                F.col("POP_HLTH_PGM_ENR_MOVE_SK") == F.lit(-1), F.lit("I")
            ).otherwise(F.lit("U")),
        )
        .withColumn("SrcSysCd", F.lit("ALINEO"))
        .withColumn("svSK", F.col("POP_HLTH_PGM_ENR_MOVE_SK"))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(
                F.col("POP_HLTH_PGM_ENR_MOVE_SK") == F.lit(-1),
                F.lit(CurrRunCycle),
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")),
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    df_updt = df_enriched.select(
        "POP_HLTH_PGM_ENR_ID",
        "PGM_ENR_MOVED_DTM",
        F.col("SrcSysCd").alias("SRC_SYS_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("POP_HLTH_PGM_ENR_MOVE_SK"),
    )

    write_files(
        df_updt,
        f"{adls_path}/PopHlthPgmEnrMovePK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
    )

    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "POP_HLTH_PGM_ENR_ID",
            "PGM_ENR_MOVED_DTM",
            "SRC_SYS_CD_SK",
            F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("POP_HLTH_PGM_ENR_MOVE_SK"),
        )
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_POP_HLTH_PGM_ENR_MOVE.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
    )

    allcol_alias = df_allcol_dedup.alias("AllColOut")
    keys_alias = df_enriched.alias("Keys")

    join_cond = (
        (F.col("AllColOut.POP_HLTH_PGM_ENR_ID") == F.col("Keys.POP_HLTH_PGM_ENR_ID"))
        & (F.col("AllColOut.PGM_ENR_MOVED_DTM") == F.col("Keys.PGM_ENR_MOVED_DTM"))
        & (F.col("AllColOut.SRC_SYS_CD") == F.col("Keys.SrcSysCd"))
    )

    df_keys_output = (
        allcol_alias.join(keys_alias, join_cond, "left")
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("AllColOut.INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT"),
            F.col("AllColOut.RECYCLE_CT"),
            F.col("AllColOut.SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING"),
            F.col("Keys.svSK").alias("POP_HLTH_PGM_ENR_MOVE_SK"),
            F.col("AllColOut.POP_HLTH_PGM_ENR_ID"),
            F.col("AllColOut.PGM_ENR_MOVED_DTM"),
            F.col("Keys.svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.PGM_ENR_MOVED_DT_SK"),
            F.col("AllColOut.FROM_INDV_BE_KEY"),
            F.col("AllColOut.TO_INDV_BE_KEY"),
            F.col("AllColOut.PGM_ENR_MOVED_BY_USERID"),
        )
    )

    return df_keys_output