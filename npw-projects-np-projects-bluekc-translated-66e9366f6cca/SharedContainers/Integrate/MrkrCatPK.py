# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared container used for Primary Keying of MrkrCat job

* VC LOGS *
^1_1 02/05/09 11:41:41 Batch  15012_42105 PROMOTE bckcetl ids20 dsadm bls for sg
^1_1 02/05/09 10:22:42 Batch  15012_37366 INIT bckcett testIDS dsadm bls for sg
^1_2 01/29/09 10:10:07 Batch  15005_36616 PROMOTE bckcett testIDS u03651 steph - primary key/balancing
^1_2 01/29/09 09:38:37 Batch  15005_34720 INIT bckcett devlIDS u03651 steffy
^1_1 01/26/09 14:40:15 Batch  15002_52819 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

DESCRIPTION:   Shared container used for Primary Keying of MrkrCat job

CALLED BY : ImpProMrkrCatExtr

PROCESSING:    

MODIFICATIONS:
Developer           Date                         Change Description                                                     Project/Altius #          Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------        ---------------------------     ------------------------------------       ----------------------------      -------------------------
Bhoomi Dasari    2008-08-25               Initial program                                                               3567 Primary Key    devlIDS                                Steph Goddard           09/02/2008
"""
# Used by  ImpProMrkrCatExtr
# IDS Primary Key Container for Mrkr Cat
# primary key hash file only contains current run keys and is cleared before writing
# update primary key table (K_MRKR_CAT) with new keys created today
# Assign primary surrogate key
# Hash file (hf_mrkr_cat_allcol) cleared in calling job
# join primary key info with table info
# Load IDS temp. table
# Temp table is tuncated before load and runstat done after load
# SQL joins temp table with key table to assign known keys
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_MrkrCatPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # Unpack parameters
    CurrRunCycle       = params["CurrRunCycle"]
    SrcSysCd           = params["SrcSysCd"]
    IDSOwner           = params["IDSOwner"]
    ids_secret_name    = params["ids_secret_name"]
    ids_jdbc_url       = params["ids_jdbc_url"]
    ids_jdbc_props     = params["ids_jdbc_props"]
    adls_path          = params["adls_path"]
    adls_path_raw      = params["adls_path_raw"]
    adls_path_publish  = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # hf_mrkr_cat_allcol replacement – deduplicate intermediate DataFrame
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "MRKR_CAT_CD"],
        []
    )

    # ------------------------------------------------------------------
    # Load K_MRKR_CAT_TEMP table
    # ------------------------------------------------------------------
    df_Transform.select(
        "SRC_SYS_CD_SK",
        "MRKR_CAT_CD"
    ).write.format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("dbtable", f"{IDSOwner}.K_MRKR_CAT_TEMP") \
        .mode("overwrite") \
        .save()

    after_sql = f"""CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_MRKR_CAT_TEMP on key columns with distribution on key columns and detailed indexes all allow write access')"""
    execute_dml(after_sql, ids_jdbc_url, ids_jdbc_props)

    # ------------------------------------------------------------------
    # Extract data from K_MRKR_CAT_TEMP to build W_Extract
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.MRKR_CAT_SK,
                w.SRC_SYS_CD_SK,
                w.MRKR_CAT_CD,                                     
                k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_MRKR_CAT_TEMP w,
             {IDSOwner}.K_MRKR_CAT k
        WHERE w.SRC_SYS_CD_SK = k.SRC_SYS_CD_SK
              AND w.MRKR_CAT_CD    = k.MRKR_CAT_CD

        UNION

        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.MRKR_CAT_CD,
               {CurrRunCycle}
        FROM {IDSOwner}.K_MRKR_CAT_TEMP w2
        WHERE NOT EXISTS (
            SELECT k2.MRKR_CAT_SK
            FROM {IDSOwner}.K_MRKR_CAT k2
            WHERE w2.SRC_SYS_CD_SK     = k2.SRC_SYS_CD_SK
                  AND w2.MRKR_CAT_CD   = k2.MRKR_CAT_CD
        )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ------------------------------------------------------------------
    # PrimaryKey transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("MRKR_CAT_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(
                F.col("svInstUpdt") == F.lit("I"),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svMrkrCatCd",
            F.trim(F.col("MRKR_CAT_CD"))
        )
        .withColumn(
            "SRC_SYS_CD",
            F.lit(SrcSysCd)
        )
        .withColumn(
            "svSK",
            F.col("MRKR_CAT_SK")
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # Output: updt link
    df_updt = df_enriched.select(
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("svMrkrCatCd").alias("MRKR_CAT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("svSK").alias("MRKR_CAT_SK")
    )

    # Output: NewKeys link
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("svMrkrCatCd").alias("MRKR_CAT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("MRKR_CAT_SK")
        )
    )

    # Output: Keys link
    df_Keys = df_enriched.select(
        F.col("svSK").alias("MRKR_CAT_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("SRC_SYS_CD"),
        F.col("svMrkrCatCd").alias("MRKR_CAT_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
    )

    # ------------------------------------------------------------------
    # Merge transformer logic
    # ------------------------------------------------------------------
    join_condition = [
        df_AllCol_dedup["SRC_SYS_CD_SK"] == df_Keys["SRC_SYS_CD_SK"],
        df_AllCol_dedup["MRKR_CAT_CD"] == df_Keys["MRKR_CAT_CD"]
    ]
    df_merge = (
        df_AllCol_dedup.alias("AllColOut")
        .join(df_Keys.alias("Keys"), join_condition, "left")
    )
    df_Key = df_merge.select(
        F.col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("Keys.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
        F.col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("AllColOut.ERR_CT").alias("ERR_CT"),
        F.col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("Keys.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("Keys.MRKR_CAT_SK").alias("MRKR_CAT_SK"),
        F.col("AllColOut.MRKR_CAT_CD").alias("MRKR_CAT_CD"),
        F.col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AllColOut.MAJ_PRCTC_CAT_CD").alias("MAJ_PRCTC_CAT_CD"),
        F.col("AllColOut.MRKR_CAT_DESC").alias("MRKR_CAT_DESC")
    )

    # ------------------------------------------------------------------
    # Sequential file output for K_MRKR_CAT
    # ------------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_MRKR_CAT.dat"
    write_files(
        df_NewKeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Parquet output for hf_mrkr_cat
    # ------------------------------------------------------------------
    parquet_file_path = f"{adls_path}/MrkrCatPK_updt.parquet"
    write_files(
        df_updt,
        parquet_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # Return container output
    # ------------------------------------------------------------------
    return df_Key