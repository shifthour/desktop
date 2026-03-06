# Databricks utility notebooks
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Shared-Container :  FcltyClmCondPK
Description      :  Shared container used for Primary Keying of Facility Claim Cond

VC LOGS
^1_2 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
^1_2 02/10/09 13:07:43 Batch  15017_47268 INIT bckcetl ids20 dsadm dsadm
^1_1 01/26/09 11:49:42 Batch  15002_42592 PROMOTE bckcetl ids20 dsadm bls for bl
^1_1 01/26/09 11:23:51 Batch  15002_41034 INIT bckcett testIDS dsadm BLS FOR BL
^1_1 12/16/08 10:00:18 Batch  14961_36031 PROMOTE bckcett testIDS u03651 steph for Brent - claim primary keying
^1_1 12/16/08 09:15:37 Batch  14961_33343 INIT bckcett devlIDS u03651 steffy

***************************************************************************************************************************************************************
COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

CALLED BY: 
    - FctsClmFcltyCondExtr
    - NascoClmFcltyCondExtr

PROCESSING NOTES / ANNOTATIONS ( DataStage ):
    - IDS Primary Key Container for Facility Claim Cond
    - Hash file (hf_fclty_cond_allcol) cleared in the job FctsClmFcltyCondExtr and NascoClmFcltyCondExtr
    - SQL joins temp table with key table to assign known keys
    - Temp table is truncated before load and runstat done after load
    - Load IDS temp. table
    - join primary key info with table info
    - update primary key table (K_FCLTY_CLM_COND) with new keys created today
    - primary key hash file only contains current run keys and is cleared before writing
    - Assign primary surrogate key
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def run_FcltyClmCondPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    PySpark implementation of the DataStage shared container `FcltyClmCondPK`.

    Parameters
    ----------
    df_AllCol   : DataFrame
        Input link  `AllCol`  – all columns for the Facility Claim Condition record.
    df_Transform: DataFrame
        Input link  `Transform` – columns required to build the temp-table.
    params      : dict
        Runtime parameters and pre-resolved configuration values.

    Returns
    -------
    DataFrame
        Output link `Key` – enriched Facility Claim Condition records including 
        primary/surrogate keys and audit columns.
    """

    # ------------------------------------------------------------------
    #  1. Un-pack runtime parameters  (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle          = params["CurrRunCycle"]
    SrcSysCd              = params["SrcSysCd"]
    SrcSysCdSk            = params["SrcSysCdSk"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    adls_path             = params["adls_path"]
    adls_path_raw         = params["adls_path_raw"]
    adls_path_publish     = params["adls_path_publish"]
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    #  2. Replace intermediate hash-file (scenario a) with de-dup logic
    # ------------------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "CLM_ID", "FCLTY_CLM_COND_SEQ_NO"],
        []
    )

    # ------------------------------------------------------------------
    #  3. Build / refresh the temp table  K_FCLTY_CLM_COND_TEMP
    # ------------------------------------------------------------------
    tbl_temp = f"{IDSOwner}.K_FCLTY_CLM_COND_TEMP"

    # drop & recreate
    execute_dml(f"DROP TABLE IF EXISTS {tbl_temp}", ids_jdbc_url, ids_jdbc_props)
    execute_dml(
        f"""
        CREATE TABLE {tbl_temp} (
            SRC_SYS_CD_SK           INTEGER      NOT NULL,
            CLM_ID                  VARCHAR(20)  NOT NULL,
            FCLTY_CLM_COND_SEQ_NO   SMALLINT     NOT NULL,
            PRIMARY KEY (SRC_SYS_CD_SK, CLM_ID, FCLTY_CLM_COND_SEQ_NO)
        )
        """,
        ids_jdbc_url,
        ids_jdbc_props,
    )

    # insert incoming rows
    (
        df_Transform
        .select(
            "SRC_SYS_CD_SK",
            "CLM_ID",
            "FCLTY_CLM_COND_SEQ_NO"
        )
        .write
        .mode("append")
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", tbl_temp)
        .save()
    )

    # runstats / statistics
    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {tbl_temp} on key columns with distribution on key columns and detailed indexes all allow write access')",
        ids_jdbc_url,
        ids_jdbc_props,
    )

    # ------------------------------------------------------------------
    #  4. Extract W_Extract  
    # ------------------------------------------------------------------
    extract_query = f"""
        SELECT  k.FCLTY_CLM_COND_SK,
                w.SRC_SYS_CD_SK,
                w.CLM_ID,
                w.FCLTY_CLM_COND_SEQ_NO,
                k.CRT_RUN_CYC_EXCTN_SK
        FROM   {IDSOwner}.K_FCLTY_CLM_COND_TEMP w
        JOIN   {IDSOwner}.K_FCLTY_CLM_COND      k
          ON   w.SRC_SYS_CD_SK         = k.SRC_SYS_CD_SK
         AND   w.CLM_ID                = k.CLM_ID
         AND   w.FCLTY_CLM_COND_SEQ_NO = k.FCLTY_CLM_COND_SEQ_NO

        UNION

        SELECT -1,
               w2.SRC_SYS_CD_SK,
               w2.CLM_ID,
               w2.FCLTY_CLM_COND_SEQ_NO,
               {CurrRunCycle}
        FROM   {IDSOwner}.K_FCLTY_CLM_COND_TEMP w2
        WHERE  NOT EXISTS (
                SELECT 1
                FROM {IDSOwner}.K_FCLTY_CLM_COND k2
                WHERE w2.SRC_SYS_CD_SK         = k2.SRC_SYS_CD_SK
                  AND w2.CLM_ID                = k2.CLM_ID
                  AND w2.FCLTY_CLM_COND_SEQ_NO = k2.FCLTY_CLM_COND_SEQ_NO
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
    #  5. Transformer  PrimaryKey  –  column derivations
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("FCLTY_CLM_COND_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svClmId", F.trim(F.col("CLM_ID")))
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("FCLTY_CLM_COND_SK") == F.lit(-1), F.lit(CurrRunCycle))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # surrogate-key generation  (KeyMgtGetNextValueConcurrent replacement)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'FCLTY_CLM_COND_SK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    #  6. updt – write hash-file (now parquet)
    # ------------------------------------------------------------------
    df_updt = (
        df_enriched
        .select(
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svClmId").alias("CLM_ID"),
            "FCLTY_CLM_COND_SEQ_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "FCLTY_CLM_COND_SK",
        )
    )

    write_files(
        df_updt,
        f"{adls_path}/FcltyClmCondPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    #  7. NewKeys – CSV extract (constraint svInstUpdt = 'I')
    # ------------------------------------------------------------------
    df_NewKeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            F.col("svClmId").alias("CLM_ID"),
            "FCLTY_CLM_COND_SEQ_NO",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            "FCLTY_CLM_COND_SK",
        )
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_FCLTY_CLM_COND.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    #  8. Keys – DataFrame needed for merge
    # ------------------------------------------------------------------
    df_keys = (
        df_enriched
        .select(
            "FCLTY_CLM_COND_SK",
            "SRC_SYS_CD_SK",
            F.lit(SrcSysCd).alias("SRC_SYS_CD"),
            F.col("svClmId").alias("CLM_ID"),
            "FCLTY_CLM_COND_SEQ_NO",
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    # ------------------------------------------------------------------
    #  9. Merge stage
    # ------------------------------------------------------------------
    join_expr = (
        (df_AllCol_dedup["SRC_SYS_CD_SK"] == df_keys["SRC_SYS_CD_SK"])
        & (df_AllCol_dedup["CLM_ID"] == df_keys["CLM_ID"])
        & (df_AllCol_dedup["FCLTY_CLM_COND_SEQ_NO"] == df_keys["FCLTY_CLM_COND_SEQ_NO"])
    )

    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), join_expr, "left")
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
            F.col("k.FCLTY_CLM_COND_SK").alias("FCLTY_CLM_COND_SK"),
            F.col("k.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("all.CLM_ID").alias("CLM_ID"),
            F.col("all.FCLTY_CLM_COND_SEQ_NO").alias("FCLTY_CLM_COND_SEQ_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
            F.col("all.FCLTY_CLM_COND_CD").alias("FCLTY_CLM_COND_CD"),
        )
    )

    # ------------------------------------------------------------------
    # 10. Return container-output link `Key`
    # ------------------------------------------------------------------
    return df_Key