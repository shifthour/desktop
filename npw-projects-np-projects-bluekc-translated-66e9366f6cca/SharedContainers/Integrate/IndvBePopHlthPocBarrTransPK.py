# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Job Name           : IndvBePopHlthPocBarrTransPK
Job Type           : Server Job
Job Category       : DS_Integrate
Folder Path        : Shared Containers/PrimaryKey
Description        : Copyright 2010 Blue Cross/Blue Shield of Kansas City

PROCESSING: Primary Key Shared Container for INDV_BE_POP_HLTH_POC_PROB_TRANS.

MODIFICATIONS:
Developer                       Date               Project/Altiris #               Change Description                             Development Project    Code Reviewer            Date Reviewed
----------------------------------      -------------------   -----------------------------------    ---------------------------------------------------------   ----------------------------------   ---------------------------------    -------------------------   
Kalyan Neelam                2010-07-19       4297                             Original Programming                           RebuildIntNewDevl      Steph Goddard            07/21/2010

Annotations:
- SQL joins temp table with key table to assign known keys
- Load IDS temp. table
- join primary key info with table info
- Assign primary surrogate key
- This container is used in:
  AlineoIndvBePopHlthPocBarrTransExtr
  These programs need to be re-compiled when logic changes
- Alineo INDV_BE_POP_HLTH_POC_BARR_TRANS Primary Key Shared Container
"""
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# COMMAND ----------
def run_IndvBePopHlthPocBarrTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the shared-container logic for IndvBePopHlthPocBarrTransPK.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input link "AllCol".
    df_Transform : DataFrame
        Container input link "Transform".
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Container output link "Key".
    """

    # --------------------------------------------------
    # Unpack parameters (each exactly once)
    # --------------------------------------------------
    IDSOwner        = params["IDSOwner"]
    SrcSysCd        = params["SrcSysCd"]
    CurrRunCycle    = params["CurrRunCycle"]
    ids_jdbc_url    = params["ids_jdbc_url"]
    ids_jdbc_props  = params["ids_jdbc_props"]
    adls_path       = params["adls_path"]
    adls_path_raw   = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]
    # --------------------------------------------------
    # Stage: hf_indvbepophlthpocbarrtrans_allcol  (scenario a – intermediate hash file)
    # --------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["PGM_ENROLLMENT_ID", "PROB_ID", "GOAL_ID", "BARRIER_ID", "ROW_EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    # --------------------------------------------------
    # Stage: hf_indvbepophlthpocbarrtrans_transform (scenario a – intermediate hash file)
    # --------------------------------------------------
    df_transform_dedup = dedup_sort(
        df_Transform,
        ["POP_HLTH_PGM_ENR_ID", "POC_PROB_ID", "POC_GOAL_ID", "POC_BARR_ID", "ROW_EFF_DT_SK", "SRC_SYS_CD_SK"],
        []
    )

    # --------------------------------------------------
    # Stage: K_INDV_BE_POP_HLTH_POC_BARR_TRANS_TEMP (DB2 connector)
    # --------------------------------------------------
    drop_tmp_tbl = f"DROP TABLE IF EXISTS {IDSOwner}.K_INDV_BE_POP_HLTH_POC_BARR_TRANS_TEMP"
    execute_dml(drop_tmp_tbl, ids_jdbc_url, ids_jdbc_props)

    create_tmp_tbl = f"""
        CREATE TABLE {IDSOwner}.K_INDV_BE_POP_HLTH_POC_BARR_TRANS_TEMP (
            POP_HLTH_PGM_ENR_ID VARCHAR(255) NOT NULL,
            POC_PROB_ID VARCHAR(255) NOT NULL,
            POC_GOAL_ID VARCHAR(255) NOT NULL,
            POC_BARR_ID VARCHAR(255) NOT NULL,
            ROW_EFF_DT_SK CHAR(10) NOT NULL,
            SRC_SYS_CD_SK INTEGER NOT NULL,
            PRIMARY KEY (
                POP_HLTH_PGM_ENR_ID,
                POC_PROB_ID,
                POC_GOAL_ID,
                POC_BARR_ID,
                ROW_EFF_DT_SK,
                SRC_SYS_CD_SK
            )
        )
    """
    execute_dml(create_tmp_tbl, ids_jdbc_url, ids_jdbc_props)

    df_transform_dedup.write.format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("dbtable", f"{IDSOwner}.K_INDV_BE_POP_HLTH_POC_BARR_TRANS_TEMP") \
        .mode("append") \
        .save()

    extract_query = f"""
        SELECT
            k.INDV_BE_POP_HLTH_POC_BARR_T_SK,
            w.POP_HLTH_PGM_ENR_ID,
            w.POC_PROB_ID,
            w.POC_GOAL_ID,
            w.POC_BARR_ID,
            w.ROW_EFF_DT_SK,
            w.SRC_SYS_CD_SK,
            k.CRT_RUN_CYC_EXCTN_SK
        FROM
            {IDSOwner}.K_INDV_BE_POP_HLTH_POC_BARR_TRANS_TEMP w
        JOIN
            {IDSOwner}.K_INDV_BE_POP_HLTH_POC_BARR_TRANS k
        ON  w.POP_HLTH_PGM_ENR_ID = k.POP_HLTH_PGM_ENR_ID
        AND w.POC_PROB_ID         = k.POC_PROB_ID
        AND w.POC_GOAL_ID         = k.POC_GOAL_ID
        AND w.POC_BARR_ID         = k.POC_BARR_ID
        AND w.ROW_EFF_DT_SK       = k.ROW_EFF_DT_SK
        AND w.SRC_SYS_CD_SK       = k.SRC_SYS_CD_SK

        UNION

        SELECT
            -1                                           AS INDV_BE_POP_HLTH_POC_BARR_T_SK,
            w2.POP_HLTH_PGM_ENR_ID,
            w2.POC_PROB_ID,
            w2.POC_GOAL_ID,
            w2.POC_BARR_ID,
            w2.ROW_EFF_DT_SK,
            w2.SRC_SYS_CD_SK,
            {CurrRunCycle}                               AS CRT_RUN_CYC_EXCTN_SK
        FROM
            {IDSOwner}.K_INDV_BE_POP_HLTH_POC_BARR_TRANS_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_INDV_BE_POP_HLTH_POC_BARR_TRANS k2
            WHERE w2.POP_HLTH_PGM_ENR_ID = k2.POP_HLTH_PGM_ENR_ID
              AND w2.POC_PROB_ID         = k2.POC_PROB_ID
              AND w2.POC_GOAL_ID         = k2.POC_GOAL_ID
              AND w2.POC_BARR_ID         = k2.POC_BARR_ID
              AND w2.ROW_EFF_DT_SK       = k2.ROW_EFF_DT_SK
              AND w2.SRC_SYS_CD_SK       = k2.SRC_SYS_CD_SK
        )
    """

    df_W_Extract = spark.read.format("jdbc") \
        .option("url", ids_jdbc_url) \
        .options(**ids_jdbc_props) \
        .option("query", extract_query) \
        .load()

    # --------------------------------------------------
    # Stage: Primary_Key (Transformer)
    # --------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(F.col("INDV_BE_POP_HLTH_POC_BARR_T_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(F.col("INSRT_UPDT_CD") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Surrogate key generation (KeyMgtGetNextValueConcurrent replacement)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'INDV_BE_POP_HLTH_POC_BARR_T_SK',
        <schema>,
        <secret_name>
    )

    # Keys link
    df_Keys = df_enriched.select(
        "INDV_BE_POP_HLTH_POC_BARR_T_SK",
        F.col("POP_HLTH_PGM_ENR_ID"),
        F.col("POC_PROB_ID"),
        F.col("POC_GOAL_ID"),
        F.col("POC_BARR_ID"),
        F.col("ROW_EFF_DT_SK"),
        F.col("SRC_SYS_CD_SK"),
        F.col("SRC_SYS_CD"),
        F.col("INSRT_UPDT_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK")
    )

    # Updt link (hashed file write)
    df_Updt = df_enriched.select(
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "POC_GOAL_ID",
        "POC_BARR_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "INDV_BE_POP_HLTH_POC_BARR_T_SK"
    )

    write_files(
        df_Updt,
        f"{adls_path}/IndvBePopHlthPocBarrTransPK_Updt.parquet",
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True
    )

    # NewKeys link (sequential file write)
    df_NewKeys = df_enriched.filter(F.col("INSRT_UPDT_CD") == "I").select(
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "POC_GOAL_ID",
        "POC_BARR_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "INDV_BE_POP_HLTH_POC_BARR_T_SK"
    )

    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_INDV_BE_POP_HLTH_POC_BARR_TRANS.dat",
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False
    )

    # --------------------------------------------------
    # Stage: Merge (Transformer)
    # --------------------------------------------------
    df_merge_out = (
        df_AllCol_dedup.alias("all")
        .join(
            df_Keys.alias("k"),
            (F.col("all.PGM_ENROLLMENT_ID") == F.col("k.POP_HLTH_PGM_ENR_ID")) &
            (F.col("all.PROB_ID") == F.col("k.POC_PROB_ID")) &
            (F.col("all.GOAL_ID") == F.col("k.POC_GOAL_ID")) &
            (F.col("all.BARRIER_ID") == F.col("k.POC_BARR_ID")) &
            (F.col("all.ROW_EFF_DT_SK") == F.col("k.ROW_EFF_DT_SK")),
            "left"
        )
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
            F.col("k.INDV_BE_POP_HLTH_POC_BARR_T_SK").alias("INDV_BE_POP_HLTH_POC_BARR_T_SK"),
            F.col("all.PGM_ENROLLMENT_ID").alias("PGM_ENROLLMENT_ID"),
            F.col("all.PROB_ID").alias("PROB_ID"),
            F.col("all.GOAL_ID").alias("GOAL_ID"),
            F.col("all.BARRIER_ID").alias("BARRIER_ID"),
            F.col("all.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.BARRIER_CODE").alias("BARRIER_CODE"),
            F.col("all.BARRIER_DEL_FLAG").alias("BARRIER_DEL_FLAG"),
            F.col("all.IDENTIFICATION_DATE").alias("IDENTIFICATION_DATE"),
            F.col("all.RESOLUTION_DATE").alias("RESOLUTION_DATE"),
            F.col("all.BARRIER_CREATED").alias("BARRIER_CREATED"),
            F.col("all.ROW_TERM_DT_SK").alias("ROW_TERM_DT_SK"),
            F.col("all.INDV_BE_KEY").alias("INDV_BE_KEY"),
            F.col("all.BARRIER_MODIFIED").alias("BARRIER_MODIFIED")
        )
    )

    # --------------------------------------------------
    # Cleanup: drop temp table
    # --------------------------------------------------
    execute_dml(drop_tmp_tbl, ids_jdbc_url, ids_jdbc_props)

    # --------------------------------------------------
    # Return container output
    # --------------------------------------------------
    return df_merge_out
# COMMAND ----------