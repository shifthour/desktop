# Databricks utility includes
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared Container : MrkrPK
Folder Path      : Shared Containers/PrimaryKey
Job Type         : Server Job
Job Category     : DS_Integrate

DESCRIPTION:
Shared container used for Primary Keying of Mrkr job.
VC LOGS:
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

PROCESSING NOTES:
  • Hash file (hf_mrkr_allcol) cleared in calling job – replaced by in-memory de-duplication.
  • Temp table K_MRKR_TEMP truncated, loaded, runstats executed.
  • Keys resolved/created, updates written to parquet (hf_mrkr_updt) and new keys to flat-file K_MRKR.dat.
  • Final merge produces “Key” output stream.

CALLED BY:
  • ImpProMrkrExtr
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, trim, when
from pyspark.sql.types import IntegerType


def run_MrkrPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Executes the MrkrPK shared-container logic.

    Parameters
    ----------
    df_AllCol   : DataFrame  – input link “AllCol”
    df_Transform: DataFrame  – input link “Transform”
    params      : dict       – runtime parameters & configs

    Returns
    -------
    DataFrame   – output link “Key”
    """

    #
    # ------------------------------------------------------------------
    # Un-pack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    #
    # ------------------------------------------------------------------
    # 1. Replace intermediate hash-file “hf_mrkr_allcol”  (Scenario-a)
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "MRKR_ID"],
        sort_cols=[]
    )

    #
    # ------------------------------------------------------------------
    # 2. Load K_MRKR_TEMP and extract W_Extract
    # ------------------------------------------------------------------
    # 2.1 Truncate temp table
    execute_dml(
        query=f"TRUNCATE TABLE {IDSOwner}.K_MRKR_TEMP",
        jdbc_url=ids_jdbc_url,
        jdbc_props=ids_jdbc_props
    )

    # 2.2 Load data
    (
        df_Transform
        .select("SRC_SYS_CD_SK", "MRKR_ID")
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_MRKR_TEMP")
        .mode("append")
        .save()
    )

    # 2.3 Runstats
    execute_dml(
        query=(
            f"CALL SYSPROC.ADMIN_CMD("
            f"'runstats on table {IDSOwner}.K_MRKR_TEMP "
            f"on key columns with distribution on key columns "
            f"and detailed indexes all allow write access')"
        ),
        jdbc_url=ids_jdbc_url,
        jdbc_props=ids_jdbc_props
    )

    # 2.4 Extract W_Extract
    extract_query = f"""
        SELECT  k.MRKR_SK,
                w.SRC_SYS_CD_SK,
                w.MRKR_ID,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_MRKR_TEMP w
        JOIN {IDSOwner}.K_MRKR k
          ON w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
         AND w.MRKR_ID       = k.MRKR_ID
        UNION
        SELECT  -1,
                w2.SRC_SYS_CD_SK,
                w2.MRKR_ID,
                {CurrRunCycle}
        FROM {IDSOwner}.K_MRKR_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
              FROM {IDSOwner}.K_MRKR k2
             WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
               AND w2.MRKR_ID       = k2.MRKR_ID
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    #
    # ------------------------------------------------------------------
    # 3. Transformer “PrimaryKey”
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("MRKR_SK") == lit(-1), lit("I")).otherwise(lit("U"))
        )
        .withColumn(
            "svSK",
            when(col("svInstUpdt") == lit("I"), lit(None).cast(IntegerType()))
            .otherwise(col("MRKR_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("svInstUpdt") == lit("I"), lit(CurrRunCycle).cast(IntegerType()))
            .otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svMrkrId",
            trim(col("MRKR_ID"))
        )
    )

    # Surrogate key generation
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "svSK",
        <schema>,
        <secret_name>
    )

    #
    # 3.1 Output link “updt”  -> hf_mrkr (parquet)
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        col("svMrkrId").alias("MRKR_ID"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("MRKR_SK")
    )

    parquet_path_updt = f"{adls_path}/MrkrPK_updt.parquet"
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

    #
    # 3.2 Output link “NewKeys” -> sequential file K_MRKR.dat
    # ------------------------------------------------------------------
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            col("SRC_SYS_CD_SK"),
            col("svMrkrId").alias("MRKR_ID"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("MRKR_SK")
        )
    )

    seq_file_path = f"{adls_path}/load/K_MRKR.dat"
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

    #
    # 3.3 Output link “Keys”  – feed to downstream merge
    # ------------------------------------------------------------------
    df_keys = df_enriched.select(
        col("svSK").alias("MRKR_SK"),
        col("SRC_SYS_CD_SK"),
        lit(SrcSysCd).alias("SRC_SYS_CD"),
        col("svMrkrId").alias("MRKR_ID"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    #
    # ------------------------------------------------------------------
    # 4. Transformer “Merge” – produce container output “Key”
    # ------------------------------------------------------------------
    join_expr = [
        df_AllCol_dedup["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"],
        df_AllCol_dedup["MRKR_ID"]      == df_keys["MRKR_ID"]
    ]

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
        .select(
            col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("all.DISCARD_IN").alias("DISCARD_IN"),
            col("all.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("all.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("all.ERR_CT").alias("ERR_CT"),
            col("all.RECYCLE_CT").alias("RECYCLE_CT"),
            col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("all.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("k.MRKR_SK").alias("MRKR_SK"),
            col("all.MRKR_ID").alias("MRKR_ID"),
            col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("all.MRKR_CAT").alias("MRKR_CAT"),
            col("all.MRKR_TYP_CD").alias("MRKR_TYP_CD"),
            col("all.MRKR_DESC").alias("MRKR_DESC"),
            col("all.MRKR_LABEL").alias("MRKR_LABEL")
        )
    )

    #
    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key