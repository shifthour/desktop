# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
ComsnSchdPK – Shared container used for Primary Keying of Commission Schedule job.

VC LOGS
^1_1 02/20/09 11:01:04 Batch  15027_39667 PROMOTE bckcetl ids20 dsadm bls for sa
^1_1 02/20/09 10:38:04 Batch  15027_38287 INIT bckcett testIDS dsadm bls for sa
^1_2 02/19/09 15:36:15 Batch  15026_56186 PROMOTE bckcett testIDS u03651 steph for Sharon - primary key containers
^1_2 02/19/09 15:33:19 Batch  15026_56002 INIT bckcett devlIDS u03651 steffy
^1_1 01/13/09 08:41:23 Batch  14989_31287 INIT bckcett devlIDS u03651 steffy
^1_1 07/24/08 13:21:35 Batch  14816_48098 INIT bckcett devlIDS u08717 Brent

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
Shared container used for Primary Keying of Comm Schd job
CALLED BY : FctsComsnSchdExtr

ANNOTATIONS:
- IDS Primary Key Container for Commission Schd
- Used by FctsComsnSchdExtr
- Hash file (hf_comsn_schd_allcol) cleared in calling job
- SQL joins temp table with key table to assign known keys
- Temp table is truncated before load and runstat done after load
- Load IDS temp. table
- Join primary key info with table info
- Update primary key table (K_COMSN_SCHD) with new keys created today
- Primary key hash file only contains current run keys and is cleared before writing
- Assign primary surrogate key
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Tuple


def run_ComsnSchdPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Executes the ComsnSchdPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Up-stream DataFrame equivalent to link “AllCol”.
    df_Transform : DataFrame
        Up-stream DataFrame equivalent to link “Transform”.
    params : dict
        Dictionary of runtime parameters already supplied by the calling notebook.

    Returns
    -------
    DataFrame
        DataFrame corresponding to output link “Key”.
    """

    # ------------------------------------------------------------------
    # Unpack runtime parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # Stage: hf_comsn_schd_allcol  (Scenario-a hashed file removal)
    # ------------------------------------------------------------------
    # Deduplicate on primary-key columns before feeding to Merge
    df_AllColOut = dedup_sort(
        df_AllCol,
        partition_cols=["SRC_SYS_CD_SK", "COMSN_SCHD_ID"],
        sort_cols=[("SRC_SYS_CD_SK", "A"), ("COMSN_SCHD_ID", "A")],
    )

    # ------------------------------------------------------------------
    # Stage: K_COMSN_SCHD  (Key table read via JDBC)
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT
            COMSN_SCHD_SK,
            SRC_SYS_CD_SK,
            COMSN_SCHD_ID,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_COMSN_SCHD
    """
    df_k_comsn_schd = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # Stage: Simulated K_COMSN_SCHD_TEMP   (Spark-side representation)
    # ------------------------------------------------------------------
    df_temp = df_Transform.select("SRC_SYS_CD_SK", "COMSN_SCHD_ID")

    df_W_Extract = (
        df_temp.alias("w")
        .join(
            df_k_comsn_schd.alias("k"),
            ["SRC_SYS_CD_SK", "COMSN_SCHD_ID"],
            "left",
        )
        .select(
            F.when(F.col("k.COMSN_SCHD_SK").isNull(), F.lit(-1))
            .otherwise(F.col("k.COMSN_SCHD_SK"))
            .alias("COMSN_SCHD_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.COMSN_SCHD_ID"),
            F.when(F.col("k.CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(CurrRunCycle))
            .otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK"))
            .alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    # ------------------------------------------------------------------
    # Stage: PrimaryKey (Transformer)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract.withColumn(
            "svInstUpdt",
            F.when(F.col("COMSN_SCHD_SK") == -1, F.lit("I")).otherwise(F.lit("U")),
        )
        .withColumn("svSrcSysCd", F.lit("FACETS"))
        .withColumn("svComsnSchdId", F.trim(F.col("COMSN_SCHD_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(
                F.col("CRT_RUN_CYC_EXCTN_SK")
            ),
        )
        .withColumn("svSK", F.col("COMSN_SCHD_SK"))
    )

    # Sequence generation – must always follow the special-case call pattern
    df_enriched = SurrogateKeyGen(
        df_enriched, <DB sequence name>, "svSK", <schema>, <secret_name>
    )

    # ------------------------------------------------------------------
    # Stage: hf_comsn_schd  (Scenario-c hashed file → parquet)
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svComsnSchdId").alias("COMSN_SCHD_ID"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("COMSN_SCHD_SK"),
    )

    write_files(
        df_updt,
        file_path=f"{adls_path}/ComsnSchdPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )

    # ------------------------------------------------------------------
    # Stage: K_COMSN_SCHD  (Sequential file write)
    # ------------------------------------------------------------------
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            "SRC_SYS_CD_SK",
            F.col("svComsnSchdId").alias("COMSN_SCHD_ID"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("COMSN_SCHD_SK"),
        )
    )

    write_files(
        df_NewKeys,
        file_path=f"{adls_path}/load/K_COMSN_SCHD.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None,
    )

    # ------------------------------------------------------------------
    # Stage: Keys output for Merge
    # ------------------------------------------------------------------
    df_Keys = df_enriched.select(
        F.col("svSK").alias("COMSN_SCHD_SK"),
        "SRC_SYS_CD_SK",
        F.col("svComsnSchdId").alias("COMSN_SCHD_ID"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
    )

    # ------------------------------------------------------------------
    # Stage: Merge (Transformer)
    # ------------------------------------------------------------------
    df_Key = (
        df_AllColOut.alias("all")
        .join(df_Keys.alias("k"), ["SRC_SYS_CD_SK", "COMSN_SCHD_ID"], "left")
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
            F.col("k.COMSN_SCHD_SK").alias("COMSN_SCHD_SK"),
            F.col("all.COMSN_SCHD_ID").alias("COMSN_SCHD_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.COMSN_SCHD_CALC_METH_CD_SK").alias(
                "COMSN_SCHD_CALC_METH_CD_SK"
            ),
            F.col("all.SCHD_DESC").alias("SCHD_DESC"),
            F.col("all.COMSN_MTHDLGY_TYP_CD").alias("COMSN_MTHDLGY_TYP_CD"),
        )
    )

    # ------------------------------------------------------------------
    # Return the container’s single output link
    # ------------------------------------------------------------------
    return df_Key