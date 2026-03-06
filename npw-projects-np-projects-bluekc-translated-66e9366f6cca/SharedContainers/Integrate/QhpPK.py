# Databricks utility includes
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
Shared container used for Primary Keying of Qhp job  
Called By: FctsBCBSQhpExtr  
Process Description: Extract Data from Facets BCBS(QLFD_HLTH_PLN) and Load it to IDS (QHP)

Modifications:
Developer                 Date        Altiris #   Change Description                     Reviewer          Reviewed
-----------------------   ----------  ----------  -------------------------------------  -----------------  ----------
Karthik Cnintalapani      2014-01-03     5235     Originally Programmed                  Kalyan Neelam      2014-01-10
"""

# IDS Primary Key Container for QHP_ENR
# Hash file (hf_qhp_allcol) cleared in calling job
# SQL joins temp table with key table to assign known keys
# Temp table is tuncated before load and runstat done after load
# Load IDS temp. table
# join primary key info with table info
# update primary key table (QHP) with new keys created today
# primary key hash file only contains current run keys and is cleared before writing
# Assign primary surrogate key

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_QhpPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    PySpark implementation of the QhpPK shared container.
    Parameters
    ----------
    df_AllCol : DataFrame
        Input link corresponding to 'AllCol'.
    df_Transform : DataFrame
        Input link corresponding to 'Transform'.
    params : dict
        Runtime parameters required by the container.
    Returns
    -------
    DataFrame
        Output link corresponding to 'Key'.
    """

    # --------------------------------------------------
    # Un-pack runtime parameters
    # --------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # --------------------------------------------------
    # Stage: hf_qhp_allcol  (Scenario-a hash file)
    # --------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["QHP_ID", "EFF_DT_SK"],
        []
    )

    # --------------------------------------------------
    # Stage: hf_qhp_pk_dedupe  (Scenario-a hash file)
    # --------------------------------------------------
    df_DSLink69 = dedup_sort(
        df_Transform,
        ["QHP_ID", "EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    # --------------------------------------------------
    # Stage: K_QHP_TEMP  (write temp table, then extract)
    # --------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {IDSOwner}.K_QHP_TEMP",
        ids_jdbc_url,
        ids_jdbc_props
    )

    (
        df_DSLink69.write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_QHP_TEMP")
        .mode("append")
        .save()
    )

    extract_query = f"""
        SELECT
            w.QHP_ID,
            w.EFF_DT_SK,
            w.SRC_SYS_CD_SK,
            k.QHP_SK,
            k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_QHP_TEMP w,
             {IDSOwner}.K_QHP k
        WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
          AND w.QHP_ID        = k.QHP_ID
          AND w.EFF_DT_SK     = k.EFF_DT_SK
        UNION
        SELECT
            w2.QHP_ID,
            w2.EFF_DT_SK,
            w2.SRC_SYS_CD_SK,
            -1,
            {CurrRunCycle}
        FROM {IDSOwner}.K_QHP_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_QHP k2
            WHERE w2.SRC_SYS_CD_SK = k2.SRC_SYS_CD_SK
              AND w2.QHP_ID        = k2.QHP_ID
              AND w2.EFF_DT_SK     = k2.EFF_DT_SK
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
    # Stage: PrimaryKey (transformer)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract.alias("W_Extract")
        .withColumn(
            "svInstUpdt",
            F.when(F.col("QHP_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("QHP_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # Output link: updt
    df_updt = df_enriched.select(
        "QHP_ID",
        "EFF_DT_SK",
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("QHP_SK")
    )

    # Output link: NewKeys  (constraint svInstUpdt = 'I')
    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "QHP_ID",
            "EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("QHP_SK")
        )
    )

    # Output link: Keys
    df_keys = df_enriched.select(
        F.col("svSK").alias("QHP_SK"),
        "SRC_SYS_CD_SK",
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "QHP_ID",
        "EFF_DT_SK",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # --------------------------------------------------
    # Stage: hf_qhp  (scenario-c hash file -> parquet)
    # --------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/QhpPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: K_QHP (sequential file)
    # --------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_QHP.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # --------------------------------------------------
    # Stage: Merge (transformer)
    # --------------------------------------------------
    df_Key = (
        df_AllColOut.alias("AllColOut")
        .join(
            df_keys.alias("Keys"),
            (F.col("AllColOut.QHP_ID") == F.col("Keys.QHP_ID")) &
            (F.col("AllColOut.EFF_DT_SK") == F.col("Keys.EFF_DT_SK")) &
            (F.col("AllColOut.SRC_SYS_CD_SK") == F.col("Keys.SRC_SYS_CD_SK")),
            "left"
        )
        .select(
            F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
            F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("AllColOut.ERR_CT").alias("ERR_CT"),
            F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("Keys.QHP_SK").alias("QHP_SK"),
            F.col("AllColOut.QHP_ID").alias("QHP_ID"),
            F.col("AllColOut.EFF_DT_SK").alias("EFF_DT_SK"),
            F.col("AllColOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("AllColOut.LOB_CD").alias("LOB_CD"),
            F.col("AllColOut.QHP_CSR_VRNT_CD").alias("QHP_CSR_VRNT_CD"),
            F.col("AllColOut.ENR_TYP_CD").alias("ENR_TYP_CD"),
            F.col("AllColOut.ENR_CHAN_ID").alias("ENR_CHAN_ID"),
            F.col("AllColOut.SUBMTR_ID").alias("SUBMTR_ID"),
            F.col("AllColOut.METAL_LVL_CD").alias("METAL_LVL_CD"),
            F.col("AllColOut.QHP_ST_CD").alias("QHP_ST_CD"),
            F.col("AllColOut.TERM_DT_SK").alias("TERM_DT_SK")
        )
    )

    return df_Key