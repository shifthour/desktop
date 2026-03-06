
# COMMAND ----------
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: IndvBePopHlthPocIntrvTransPK
Folder Path    : Shared Containers/PrimaryKey
Job Category   : DS_Integrate
Job Type       : Server Job

Copyright 2010 Blue Cross/Blue Shield of Kansas City

PROCESSING: Primary Key Shared Container for INDV_BE_POP_HLTH_POC_INTRV_TRANS.

MODIFICATIONS:
Developer                 Date         Project/Altiris #     Change Description                  Development Project     Code Reviewer        Date Reviewed
------------------------- ------------ --------------------- ----------------------------------- ----------------------- -------------------- ----------------
Kalyan Neelam             2010-07-19   4297                  Original Programming                RebuildIntNewDevl       Steph Goddard        07/21/2010

Annotations:
- SQL joins temp table with key table to assign known keys
- Load IDS temp. table
- join primary key info with table info
- Assign primary surrogate key
- This container is used in:
  AlineoIndvBePopHlthPocIntrvTransExtr
  (These programs need to be re-compiled when logic changes)
- Alineo INDV_BE_POP_HLTH_POC_INTRV_TRANS Primary Key Shared Container
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def run_IndvBePopHlthPocIntrvTransPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict,
) -> DataFrame:
    """
    Executes the logic contained in the IndvBePopHlthPocIntrvTransPK shared container.
    """

    # -------------------------------------------------------
    # Parameter unpack (only once, no $ prefixes retained)
    # -------------------------------------------------------
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    CurrRunCycle = params["CurrRunCycle"]
    SrcSysCd = params["SrcSysCd"]
    SrcSysCdSk = params["SrcSysCdSk"]

    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]

    adls_path = params["adls_path"]
    adls_path_raw = params["adls_path_raw"]
    adls_path_publish = params["adls_path_publish"]

    # -------------------------------------------------------
    # 1. Hashed-file replacement – AllCol (scenario a)
    # -------------------------------------------------------
    partition_cols_allcol = [
        "PGM_ENROLLMENT_ID",
        "PROB_ID",
        "GOAL_ID",
        "INVNTN_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols_allcol,
        [],
    )

    # -------------------------------------------------------
    # 2. Hashed-file replacement – Transform (scenario a)
    # -------------------------------------------------------
    partition_cols_transform = [
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "POC_GOAL_ID",
        "POC_INTRV_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
    ]
    df_Transform_dedup = dedup_sort(
        df_Transform,
        partition_cols_transform,
        [],
    )

    # -------------------------------------------------------
    # 3. Read key table from IDS
    # -------------------------------------------------------
    df_k = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_INDV_BE_POP_HLTH_POC_INTRV_TRANS")
        .load()
        .select(
            "INDV_BE_POP_HLTH_POC_INTR_T_SK",
            "POP_HLTH_PGM_ENR_ID",
            "POC_PROB_ID",
            "POC_GOAL_ID",
            "POC_INTRV_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
        )
    )

    # -------------------------------------------------------
    # 4. Build W_Extract (existing keys + new rows with -1)
    # -------------------------------------------------------
    join_cond = (
        (F.col("w.POP_HLTH_PGM_ENR_ID") == F.col("k.POP_HLTH_PGM_ENR_ID"))
        & (F.col("w.POC_PROB_ID") == F.col("k.POC_PROB_ID"))
        & (F.col("w.POC_GOAL_ID") == F.col("k.POC_GOAL_ID"))
        & (F.col("w.POC_INTRV_ID") == F.col("k.POC_INTRV_ID"))
        & (F.col("w.ROW_EFF_DT_SK") == F.col("k.ROW_EFF_DT_SK"))
        & (F.col("w.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"))
    )

    df_found = (
        df_Transform_dedup.alias("w")
        .join(df_k.alias("k"), join_cond, "inner")
        .select(
            "INDV_BE_POP_HLTH_POC_INTR_T_SK",
            "POP_HLTH_PGM_ENR_ID",
            "POC_PROB_ID",
            "POC_GOAL_ID",
            "POC_INTRV_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
        )
    )

    df_notfound = (
        df_Transform_dedup.alias("w2")
        .join(df_k.alias("k2"), join_cond, "left_anti")
        .select(
            F.lit(-1).alias("INDV_BE_POP_HLTH_POC_INTR_T_SK"),
            "POP_HLTH_PGM_ENR_ID",
            "POC_PROB_ID",
            "POC_GOAL_ID",
            "POC_INTRV_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
        )
        .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
    )

    df_W_Extract = df_found.unionByName(df_notfound)

    # -------------------------------------------------------
    # 5. Primary_Key transformer logic
    # -------------------------------------------------------
    df_enriched = (
        df_W_Extract.withColumn(
            "INSRT_UPDT_CD",
            F.when(
                F.col("INDV_BE_POP_HLTH_POC_INTR_T_SK") == -1,
                F.lit("I"),
            ).otherwise(F.lit("U")),
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(
                F.col("INDV_BE_POP_HLTH_POC_INTR_T_SK") == -1,
                F.lit(CurrRunCycle),
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK")),
        )
        .withColumn(
            "INDV_BE_POP_HLTH_POC_INTR_T_SK",
            F.when(
                F.col("INDV_BE_POP_HLTH_POC_INTR_T_SK") == -1,
                F.lit(None).cast("long"),
            ).otherwise(F.col("INDV_BE_POP_HLTH_POC_INTR_T_SK")),
        )
    )

    # Surrogate key generation (per rule)
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "INDV_BE_POP_HLTH_POC_INTR_T_SK",
        <schema>,
        <secret_name>,
    )

    # -------------------------------------------------------
    # 6. Prepare downstream links
    # -------------------------------------------------------
    df_keys = df_enriched.select(
        "INDV_BE_POP_HLTH_POC_INTR_T_SK",
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "POC_GOAL_ID",
        "POC_INTRV_ID",
        "ROW_EFF_DT_SK",
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK",
    )

    df_updt = df_enriched.select(
        "POP_HLTH_PGM_ENR_ID",
        "POC_PROB_ID",
        "POC_GOAL_ID",
        "POC_INTRV_ID",
        "ROW_EFF_DT_SK",
        F.lit(SrcSysCd).alias("SRC_SYS_CD"),
        "CRT_RUN_CYC_EXCTN_SK",
        "INDV_BE_POP_HLTH_POC_INTR_T_SK",
    )

    df_newkeys = (
        df_enriched.filter(F.col("INSRT_UPDT_CD") == "I")
        .select(
            "POP_HLTH_PGM_ENR_ID",
            "POC_PROB_ID",
            "POC_GOAL_ID",
            "POC_INTRV_ID",
            "ROW_EFF_DT_SK",
            "SRC_SYS_CD_SK",
            "CRT_RUN_CYC_EXCTN_SK",
            "INDV_BE_POP_HLTH_POC_INTR_T_SK",
        )
    )

    # -------------------------------------------------------
    # 7. Write out hashed-file (scenario c) & sequential file
    # -------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/IndvBePopHlthPocIntrvTransPK_Updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None,
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_INDV_BE_POP_HLTH_POC_INTRV_TRANS.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None,
    )

    # -------------------------------------------------------
    # 8. Merge transformer
    # -------------------------------------------------------
    merge_cond = (
        (F.col("all.PGM_ENROLLMENT_ID") == F.col("k.POP_HLTH_PGM_ENR_ID"))
        & (F.col("all.PROB_ID") == F.col("k.POC_PROB_ID"))
        & (F.col("all.GOAL_ID") == F.col("k.POC_GOAL_ID"))
        & (F.col("all.INVNTN_ID") == F.col("k.POC_INTRV_ID"))
        & (F.col("all.ROW_EFF_DT_SK") == F.col("k.ROW_EFF_DT_SK"))
        & (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK"))
    )

    df_merge = (
        df_AllCol_dedup.alias("all")
        .join(df_keys.alias("k"), merge_cond, "left")
    )

    df_Key = df_merge.select(
        F.col("all.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("k.INSRT_UPDT_CD"),
        F.col("all.DISCARD_IN"),
        F.col("all.PASS_THRU_IN"),
        F.col("all.FIRST_RECYC_DT"),
        F.col("all.ERR_CT"),
        F.col("all.RECYCLE_CT"),
        F.col("k.SRC_SYS_CD"),
        F.col("all.PRI_KEY_STRING"),
        F.col("k.INDV_BE_POP_HLTH_POC_INTR_T_SK"),
        F.col("all.PGM_ENROLLMENT_ID"),
        F.col("all.PROB_ID"),
        F.col("all.GOAL_ID"),
        F.col("all.INVNTN_ID"),
        F.col("all.ROW_EFF_DT_SK"),
        F.col("k.CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("all.INVNTN_CODE"),
        F.col("all.INVNTN_DEL_FLAG"),
        F.col("all.INVNTN_ACTUAL_COMPLTN_DATE"),
        F.col("all.INVNTN_START_DATE"),
        F.col("all.INVNTN_TGT_COMPLTN_DATE"),
        F.col("all.INVNTN_CREATED"),
        F.col("all.ROW_TERM_DT_SK"),
        F.col("all.INDV_BE_KEY"),
        F.col("all.INVNTN_MODIFIED"),
    )

    # final output of container
    return df_Key
