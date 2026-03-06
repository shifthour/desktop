
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName       : ActurlValPK
JobType       : Server Job
JobCategory   : DS_Integrate
FolderPath    : Shared Containers/PrimaryKey

COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION: Shared container used for Primary Keying of ACTURL_VAL table.
Called By: UwsActurlValExtr
Process Description: Extract Data from UWS ACTURL_VAL table and Load it to IDS ACTURL_VAL.

Modifications:                                                                                                                      
Developer                  Date          Altiris #  Change Description                                   Reviewer        Reviewed
-----------------------    ----------    ---------  ---------------------------------------------------   --------------  ----------
Karthik Chintalapani       2014-01-21    5125       Original Program ‑ IntegrateNewDevl                   Kalyan Neelam   2014-01-24

Annotations from DataStage:
• IDS Primary Key Container for ACTURL_VAL
• Hash file (hf_acturl_val_allcol) cleared in calling job
• SQL joins temp table with key table to assign known keys
• Temp table is truncated before load and runstat done after load
• Load IDS temp. table
• Join primary key info with table info
• Update primary key table (ACTURL_VAL) with new keys created today
• Primary key hash file only contains current run keys and is cleared before writing
• Assign primary surrogate key
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_ActurlValPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-container logic translated from DataStage job “ActurlValPK”.
    
    Parameters
    ----------
    df_AllCol     : DataFrame
        Input link “AllCol”.
    df_Transform  : DataFrame
        Input link “Transform”.
    params        : dict
        Runtime parameters and JDBC configurations already supplied by the caller.
    
    Returns
    -------
    DataFrame
        Output link “Key”.
    """

    # ------------------------------------------------------------------
    # 1. Unpack parameters (each exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle           = params["CurrRunCycle"]
    SrcSysCd               = params["SrcSysCd"]
    RunID                  = params["RunID"]
    CurrentDate            = params["CurrentDate"]

    IDSOwner               = params["IDSOwner"]
    ids_secret_name        = params["ids_secret_name"]

    ids_jdbc_url           = params["ids_jdbc_url"]
    ids_jdbc_props         = params["ids_jdbc_props"]

    adls_path              = params["adls_path"]
    adls_path_raw          = params["adls_path_raw"]
    adls_path_publish      = params["adls_path_publish"]
    # ------------------------------------------------------------------
    # 2. Replace intermediate hashed files (scenario a) with de-dup logic
    # ------------------------------------------------------------------
    df_AllColOut = dedup_sort(
        df_AllCol,
        ["QHP_ID", "EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    df_DSLink69 = dedup_sort(
        df_Transform,
        ["QHP_ID", "EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    # ------------------------------------------------------------------
    # 3. JDBC read of K_ACTURL_VAL (lookup for existing surrogate keys)
    # ------------------------------------------------------------------
    extract_query_k_acturl_val = f"""
        SELECT
            ACTURL_VAL_SK,
            SRC_SYS_CD_SK,
            QHP_ID,
            EFF_DT_SK,
            CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_ACTURL_VAL
    """

    df_k_acturl_val = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query_k_acturl_val)
        .load()
    )

    # ------------------------------------------------------------------
    # 4. Re-create DataStage SQL logic for W_Extract
    # ------------------------------------------------------------------
    df_join = (
        df_DSLink69.alias("w")
        .join(
            df_k_acturl_val.alias("k"),
            [
                F.col("w.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"),
                F.col("w.QHP_ID")        == F.col("k.QHP_ID"),
                F.col("w.EFF_DT_SK")     == F.col("k.EFF_DT_SK")
            ],
            "left"
        )
    )

    df_W_Extract = df_join.select(
        F.col("w.QHP_ID").alias("QHP_ID"),
        F.col("w.EFF_DT_SK").alias("EFF_DT_SK"),
        F.col("w.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.when(F.col("k.ACTURL_VAL_SK").isNull(), F.lit(-1))
         .otherwise(F.col("k.ACTURL_VAL_SK")).alias("ACTURL_VAL_SK"),
        F.when(F.col("k.CRT_RUN_CYC_EXCTN_SK").isNull(), F.lit(CurrRunCycle))
         .otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # 5. PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("ACTURL_VAL_SK") == F.lit(-1), F.lit("I"))
             .otherwise(F.lit("U"))
        )
    )

    # ------------------------------------------------------------------
    # 5a. SurrogateKey generation (special placeholder call)
    # ------------------------------------------------------------------
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "ACTURL_VAL_SK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # 5b. Remaining derivations
    # ------------------------------------------------------------------
    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnSk",
        F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle))
         .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )

    # ------------------------------------------------------------------
    # 6. Split transformer outputs
    # ------------------------------------------------------------------
    # 6a. updt  ---------------------------------------------------------
    df_updt = df_enriched.select(
        "QHP_ID",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("ACTURL_VAL_SK")
    )

    # 6b. NewKeys  ------------------------------------------------------
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "QHP_ID",
            "EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("ACTURL_VAL_SK")
        )
    )

    # 6c. Keys  ---------------------------------------------------------
    df_Keys = df_enriched.select(
        F.col("ACTURL_VAL_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        F.col("QHP_ID"),
        F.col("EFF_DT_SK"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # 7. Persist hashed-file target (hf_acturl_val) as Parquet
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/ActurlValPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # 8. Write sequential file K_ACTURL_VAL.dat
    # ------------------------------------------------------------------
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_ACTURL_VAL.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # 9. Merge transformer logic
    # ------------------------------------------------------------------
    df_merge = (
        df_AllColOut.alias("all")
        .join(
            df_Keys.alias("k"),
            [
                F.col("all.QHP_ID")     == F.col("k.QHP_ID"),
                F.col("all.EFF_DT_SK")  == F.col("k.EFF_DT_SK"),
                F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")
            ],
            "left"
        )
    )

    df_Key = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN"),
        F.col("all.PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT"),
        F.col("all.ERR_CT"),
        F.col("all.RECYCLE_CT"),
        F.col("k.SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING"),
        F.col("k.ACTURL_VAL_SK"),
        F.col("all.QHP_ID"),
        F.col("all.EFF_DT_SK"),
        F.col("all.SRC_SYS_CD_SK"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.TERM_DT_SK"),
        F.col("all.ACTURL_VAL_NO"),
        F.col("all.USER_ID"),
        F.col("all.LAST_UPDT_DT_SK")
    )

    # ------------------------------------------------------------------
    # 10. Return container output link(s)
    # ------------------------------------------------------------------
    return df_Key
