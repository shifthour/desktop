# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

"""
COPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION: Shared container used for Primary Keying of SubGvrnmtEstPrm job

CALLED BY: FctsSubSbsdyExtr

PROCESSING:

MODIFICATIONS:
Developer           Date        Change Description                                   Project/Altius #   Code Reviewer   Date Reviewed
------------------  ----------  ----------------------------------------------------  -----------------  --------------  -------------
Kalyan Neelam       2014-03-24  Initial Programming                                  5235 ACA            Bhoomi Dasari   3/27/2014
"""

# update primary key table (K_SUB_GVRNMT_EST_PRM) with new keys created today
# join primary key info with table info
# Hash file (hf_subgvrnmtestprm_pkey_allcol) cleared in calling program
# This container is used in:
# FctsSubSbsdyExtr
# These programs need to be re-compiled when logic changes
# Load IDS temp. table
# Temp table is truncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys
# primary key hash file only contains current run keys


def run_SubGvrnmtEstPrmPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ----------------------------------------------------
    # Unpack parameters (each exactly once)
    # ----------------------------------------------------
    CurrRunCycle         = params["CurrRunCycle"]
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]
    SrcSysCd             = params["SrcSysCd"]
    # ----------------------------------------------------
    # Step 1: Deduplicate incoming hash-file replacement streams
    # ----------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SUB_UNIQ_KEY", "CLS_PLN_PROD_CAT_CD", "EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    df_Transform_dedup = dedup_sort(
        df_Transform,
        ["SUB_UNIQ_KEY", "CLS_PLN_PROD_CAT_CD", "EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    # ----------------------------------------------------
    # Step 2: Read existing key table from IDS
    # ----------------------------------------------------
    extract_query = f"""
    SELECT
        SUB_UNIQ_KEY,
        CLS_PLN_PROD_CAT_CD,
        EFF_DT_SK,
        SRC_SYS_CD_SK,
        CRT_RUN_CYC_EXCTN_SK,
        SUB_GVRNMT_EST_PRM_SK
    FROM {IDSOwner}.K_SUB_GVRNMT_EST_PRM
    """
    df_existing_keys = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ----------------------------------------------------
    # Step 3: Build W_Extract logic (simulate K_SUB_GVRNMT_EST_PRM_TEMP processing)
    # ----------------------------------------------------
    join_cond = (
        (F.col("w.SUB_UNIQ_KEY") == F.col("k.SUB_UNIQ_KEY")) &
        (F.col("w.CLS_PLN_PROD_CAT_CD") == F.col("k.CLS_PLN_PROD_CAT_CD")) &
        (F.col("w.EFF_DT_SK") == F.col("k.EFF_DT_SK")) &
        (F.col("w.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"))
    )

    df_w_extract = (
        df_Transform_dedup.alias("w")
        .join(df_existing_keys.alias("k"), join_cond, "left")
        .select(
            F.col("w.SUB_UNIQ_KEY"),
            F.col("w.CLS_PLN_PROD_CAT_CD"),
            F.col("w.EFF_DT_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.when(
                F.col("k.CRT_RUN_CYC_EXCTN_SK").isNull(),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("k.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
            F.when(
                F.col("k.SUB_GVRNMT_EST_PRM_SK").isNull(),
                F.lit(-1)
            ).otherwise(F.col("k.SUB_GVRNMT_EST_PRM_SK")).alias("SUB_GVRNMT_EST_PRM_SK")
        )
    )

    # ----------------------------------------------------
    # Step 4: Apply PrimaryKey transformer logic
    # ----------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("SUB_GVRNMT_EST_PRM_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn("svSK", F.col("SUB_GVRNMT_EST_PRM_SK"))
    )

    # ----------------------------------------------------
    # Surrogate Key Generation
    # ----------------------------------------------------
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"svSK",<schema>,<secret_name>)

    # ----------------------------------------------------
    # Produce Keys, NewKeys, and Updt data sets
    # ----------------------------------------------------
    df_Keys = df_enriched.select(
        F.col("svSK").alias("SUB_GVRNMT_EST_PRM_SK"),
        F.col("svSrcSysCd").alias("SRC_SYS_CD"),
        "SUB_UNIQ_KEY",
        "CLS_PLN_PROD_CAT_CD",
        "EFF_DT_SK",
        "SRC_SYS_CD_SK",
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SUB_UNIQ_KEY",
            "CLS_PLN_PROD_CAT_CD",
            "EFF_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("SUB_GVRNMT_EST_PRM_SK")
        )
    )

    df_updt = df_enriched.select(
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "SUB_UNIQ_KEY",
        "CLS_PLN_PROD_CAT_CD",
        "EFF_DT_SK",
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("SUB_GVRNMT_EST_PRM_SK")
    )

    # ----------------------------------------------------
    # Step 5: Persist side-effect outputs
    # ----------------------------------------------------
    file_path_newkeys = f"{adls_path}/load/K_SUB_GVRNMT_EST_PRM.dat"
    write_files(
        df_NewKeys,
        file_path_newkeys,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    parquet_path_updt = f"{adls_path}/SubGvrnmtEstPrmPK_updt.parquet"
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

    # ----------------------------------------------------
    # Step 6: Merge Keys with AllCol data
    # ----------------------------------------------------
    merge_cond = (
        (F.col("all.SUB_UNIQ_KEY")        == F.col("Keys.SUB_UNIQ_KEY")) &
        (F.col("all.CLS_PLN_PROD_CAT_CD") == F.col("Keys.CLS_PLN_PROD_CAT_CD")) &
        (F.col("all.EFF_DT_SK")           == F.col("Keys.EFF_DT_SK")) &
        (F.col("all.SRC_SYS_CD_SK")       == F.col("Keys.SRC_SYS_CD_SK"))
    )

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_Keys.alias("Keys"), merge_cond, "left")
        .select(
            F.col("all.JOB_EXCTN_RCRD_ERR_SK"),
            F.col("Keys.INSRT_UPDT_CD"),
            F.col("all.DISCARD_IN"),
            F.col("all.PASS_THRU_IN"),
            F.col("all.FIRST_RECYC_DT"),
            F.col("all.ERR_CT"),
            F.col("all.RECYCLE_CT"),
            F.col("Keys.SRC_SYS_CD"),
            F.col("all.PRI_KEY_STRING"),
            F.col("Keys.SUB_GVRNMT_EST_PRM_SK"),
            F.col("all.SUB_UNIQ_KEY"),
            F.col("all.CLS_PLN_PROD_CAT_CD"),
            F.col("all.EFF_DT_SK"),
            F.col("all.SRC_SYS_CD_SK"),
            F.col("Keys.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.TERM_DT_SK"),
            F.col("all.EST_PRM_AMT"),
            F.col("all.CSPD_CAT")
        )
    )

    # ----------------------------------------------------
    # Return the container output stream
    # ----------------------------------------------------
    return df_Key