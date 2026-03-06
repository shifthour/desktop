# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

JOB NAME:  IndvBePopHlthTaskTransPK

DESCRIPTION:   Creates Primary key for INDV_BE_POP_HLTH_TASK_TRANS table

PROCESSING:    Used in jobs IdsIndvBePopHlthTaskTransExtr  

MODIFICATIONS:
Developer           Date                         Project #      Change Description                                        Development Project          Code Reviewer          Date Reviewed  
-----------------------    ----------------------------     ----------------------------------------------------------------------------               ------------------------------------       ----------------------------      -------------------------
Judy Reynolds   11/16/2009              4297-Alineo Ph2    Initial program                                         RebuildIntNewDevl            Steph Goddard          08/05/2010
"""

# Load IDS temp. table
# SQL joins temp table with key table to assign known keys
# Temp table is truncated before load and runstat done after load
# primary key hash file only contains current run keys and is cleared before writing
# update primary key table (K_INDV_BE_POP_HLTH_TASK_TRANS) with new keys created today
# join primary key info with table info
# Assign primary surrogate key
# Assign primary surrogate key
# Apply business logic

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, concat


def run_IndvBePopHlthTaskTransPK(
    df_AlineoIndvBePopHlthTaskDataOut2: DataFrame,
    params: dict
) -> DataFrame:
    """
    Shared-container translation of DataStage job: IndvBePopHlthTaskTransPK
    """

    # ---- unpack parameters ----
    CurrDate = params["CurrDate"]
    CurrRunCycle = params["CurrRunCycle"]
    SourceSK = params["SourceSK"]
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # ---- Transformer: Tr_Alineo_Indv_Be_Pop_Hlth_Task_Data ----
    df_allcol = (
        df_AlineoIndvBePopHlthTaskDataOut2
        .withColumn(
            "JOB_EXCTN_RCRD_ERR_SK",
            lit(0)
        )
        .withColumn(
            "INSRT_UPDT_CD",
            lit("I")
        )
        .withColumn(
            "DISCARD_IN",
            lit("N")
        )
        .withColumn(
            "PASS_THRU_IN",
            lit("Y")
        )
        .withColumn(
            "FIRST_RECYC_DT",
            lit(CurrDate)
        )
        .withColumn(
            "ERR_CT",
            lit(0)
        )
        .withColumn(
            "RECYCLE_CT",
            lit(0)
        )
        .withColumn(
            "SRC_SYS_CD",
            lit("ALINEO")
        )
        .withColumn(
            "PRI_KEY_STRING",
            concat(
                col("TASK_ID"),
                lit(";"),
                col("ROW_EFF_DT_SK"),
                lit(";"),
                lit("ALINEO")
            )
        )
    )

    df_allcol = df_allcol.select(
        "TASK_ID",
        "ROW_EFF_DT_SK",
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "TASK_CLS_ID",
        "TASK_TYPE_CODE",
        "TASK_RSN_CODE",
        "TASK_CLS_CODE",
        "OWNER_UID",
        "MEMBER_ID",
        "TASK_OUTCOME_CODE",
        "PRI_CODE",
        "ORIGNTR_UID",
        "START_DATE",
        "DUE_DATE",
        "COMPLETION_DATE",
        "START_TS",
        "DUE_TS",
        "COMPLETION_TS",
        "TASK_CREATED",
        "TASK_MODIFIED"
    )

    # ---- Hashed-file (scenario a) – deduplicate ----
    df_transform = dedup_sort(
        df_allcol,
        ["TASK_ID", "ROW_EFF_DT_SK"],
        []
    )

    # ---- Temp table replacement logic ----
    df_transform_out = (
        df_AlineoIndvBePopHlthTaskDataOut2
        .select(
            "TASK_ID",
            "ROW_EFF_DT_SK"
        )
        .withColumn(
            "SRC_SYS_CD_SK",
            lit(SourceSK)
        )
    )

    extract_query = f"""
    SELECT
        TASK_ID,
        ROW_EFF_DT_SK,
        SRC_SYS_CD_SK,
        CRT_RUN_CYC_EXCTN_SK,
        INDV_BE_POP_HLTH_TASK_T_SK
    FROM {IDSOwner}.K_INDV_BE_POP_HLTH_TASK_TRANS
    """

    df_key_table = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    df_w_extract = (
        df_transform_out.alias("T")
        .join(
            df_key_table.alias("K"),
            (col("T.TASK_ID") == col("K.TASK_ID")) &
            (col("T.ROW_EFF_DT_SK") == col("K.ROW_EFF_DT_SK")) &
            (col("T.SRC_SYS_CD_SK") == col("K.SRC_SYS_CD_SK")),
            "left"
        )
        .select(
            col("T.TASK_ID").alias("TASK_ID"),
            col("T.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
            col("T.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            when(
                col("K.CRT_RUN_CYC_EXCTN_SK").isNull(),
                lit(CurrRunCycle)
            ).otherwise(col("K.CRT_RUN_CYC_EXCTN_SK")).alias("CRT_RUN_CYC_EXCTN_SK"),
            when(
                col("K.INDV_BE_POP_HLTH_TASK_T_SK").isNull(),
                lit(-1)
            ).otherwise(col("K.INDV_BE_POP_HLTH_TASK_T_SK")).alias("INDV_BE_POP_HLTH_TASK_T_SK")
        )
    )

    # ---- Transformer: PrimaryKey ----
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            when(
                col("INDV_BE_POP_HLTH_TASK_T_SK") == lit(-1),
                lit("I")
            ).otherwise(lit("U"))
        )
        .withColumn(
            "svSrcSysCd",
            lit("ALINEO")
        )
        .withColumn(
            "svSK",
            col("INDV_BE_POP_HLTH_TASK_T_SK")
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            when(
                col("INDV_BE_POP_HLTH_TASK_T_SK") == lit(-1),
                lit(CurrRunCycle)
            ).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        'svSK',
        <schema>,
        <secret_name>
    )

    # updt link dataframe
    df_updt = df_enriched.select(
        "TASK_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("INDV_BE_POP_HLTH_CSTM_FLD_T_SK")
    )

    # NewKeys link dataframe (constraint svInstUpdt='I')
    df_newkeys = (
        df_enriched
        .filter(col("svInstUpdt") == lit("I"))
        .select(
            "TASK_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("INDV_BE_POP_HLTH_CSTM_FLD_T_SK")
        )
    )

    # Keys link dataframe
    df_keys = df_enriched.select(
        "TASK_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        col("svSrcSysCd").alias("SRC_SYS_CD"),
        col("svInstUpdt").alias("INSRT_UPDT_CD"),
        col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("svSK").alias("INDV_BE_POP_HLTH_TASK_T_SK")
    )

    # ---- Write sequential file ----
    seq_file_path = f"{adls_path}/load/K_INDV_BE_POP_HLTH_TASK_TRANS.dat"
    write_files(
        df_newkeys,
        seq_file_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False
    )

    # ---- Write hash-file (scenario c) as parquet ----
    parquet_file_path = f"{adls_path}/IndvBePopHlthTaskTransPK_updt.parquet"
    write_files(
        df_updt,
        parquet_file_path,
        is_pqruet=True
    )

    # ---- Transformer: Merge ----
    df_merge = (
        df_transform.alias("Transform")
        .join(
            df_keys.alias("Keys"),
            (col("Transform.TASK_ID") == col("Keys.TASK_ID")) &
            (col("Transform.ROW_EFF_DT_SK") == col("Keys.ROW_EFF_DT_SK")),
            "left"
        )
    )

    df_key = df_merge.select(
        col("Transform.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("Transform.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("Transform.DISCARD_IN").alias("DISCARD_IN"),
        col("Transform.PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("Transform.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("Transform.ERR_CT").alias("ERR_CT"),
        col("Transform.RECYCLE_CT").alias("RECYCLE_CT"),
        col("Transform.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("Transform.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("Keys.INDV_BE_POP_HLTH_TASK_T_SK").alias("INDV_BE_POP_HLTH_TASK_SK"),
        col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("Transform.MEMBER_ID").alias("INDV_BE_KEY"),
        col("Transform.TASK_ID").alias("TASK_ID"),
        col("Transform.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
        col("Keys.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        col("Transform.TASK_CLS_CODE").alias("TASK_CLASS_CODE"),
        col("Transform.TASK_OUTCOME_CODE").alias("TASK_OUTCOME_CODE"),
        col("Transform.PRI_CODE").alias("TASK_PRTY_CODE"),
        col("Transform.TASK_RSN_CODE").alias("TASK_RSN_CODE"),
        col("Transform.TASK_TYPE_CODE").alias("TASK_TYP_CODE"),
        lit("2199-12-31").alias("ROW_TERM_DT_SK"),
        col("Transform.COMPLETION_DATE").alias("TASK_CMPLTN_DT_SK"),
        col("Transform.COMPLETION_TS").alias("TASK_CMPLTN_DTM"),
        col("Transform.TASK_CREATED").alias("TASK_CRT_DT_SK"),
        col("Transform.DUE_DATE").alias("TASK_DUE_DT_SK"),
        col("Transform.DUE_TS").alias("TASK_DUE_DTM"),
        col("Transform.START_DATE").alias("TASK_STRT_DT_SK"),
        col("Transform.START_TS").alias("TASK_STRT_DTM"),
        col("Transform.OWNER_UID").alias("TASK_ASG_TO_USER_ID"),
        col("Transform.TASK_CLS_ID").alias("TASK_CLS_ID"),
        col("Transform.ORIGNTR_UID").alias("TASK_CRT_BY_USER_ID"),
        col("Transform.TASK_MODIFIED").alias("TASK_MODIFIED")
    )

    return df_key