
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
CustSvcTaskSttusPK  – Shared-Container (Primary Key)

* VC LOGS *
^1_1 10/21/08 10:37:16 Batch  14905_38245 PROMOTE bckcetl ids20 dsadm rc for brent 
^1_1 10/21/08 10:33:47 Batch  14905_38033 INIT bckcett testIDS dsadm rc for brent
^1_1 10/20/08 12:44:46 Batch  14904_45890 PROMOTE bckcett testIDS u08717 Brent
^1_1 10/20/08 12:41:40 Batch  14904_45702 INIT bckcett devlIDS u08717 Brent
^1_1 08/01/08 10:50:09 Batch  14824_39011 INIT bckcett devlIDS u03651 steffy
^1_2 07/23/08 15:42:49 Batch  14815_56575 PROMOTE bckcett devlIDS u08717 Brent
^1_2 07/23/08 15:08:26 Batch  14815_54510 INIT bckcett devlIDScur u08717 Brent
^1_1 05/12/08 08:17:54 Batch  14743_29878 INIT bckcett devlIDScur u08717 Brent

Copyright 2008 Blue Cross/Blue Shield of Kansas City

CALLED BY:  FctsCustSvcTaskSttusExtr
            NascoCustSvcTaskSttusExtr

PROCESSING: Assign primary surrogate key to input records

MODIFICATIONS:
Developer          Date        Project/Altiris #   Change Description
------------------ ----------- ------------------- -------------------------------------------------------------
Brent Leland       2008-03-04  3567                Original Programming. Changed counter IDS_SK to CUST_SVC_TASK_STTUS_SK
Kalyan Neelam      2010-02-01  TTR-604             Added new field RTE_TO_GRP_ID
"""

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# COMMAND ----------
def run_CustSvcTaskSttusPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the CustSvcTaskSttusPK shared-container logic.

    Parameters
    ----------
    df_AllCol   :  DataFrame – container input link “AllCol”
    df_Transform:  DataFrame – container input link “Transform”
    params      :  dict      – runtime parameters & JDBC configs

    Returns
    -------
    DataFrame – container output link “Key”
    """

    # ------------------------------------------------------------------
    # Unpack parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    SrcSysCdSk          = params["SrcSysCdSk"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]

    # ------------------------------------------------------------------
    # 1. Replace intermediate hash file “hf_cust_svc_task_sttus_allcol”
    #    (scenario a) with duplicate-removal logic
    # ------------------------------------------------------------------
    df_allcol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=[
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "STTUS_SEQ_NO"
        ],
        sort_cols=[]
    )

    # ------------------------------------------------------------------
    # 2. Read existing keys from IDS » K_CUST_SVC_TASK_STTUS
    # ------------------------------------------------------------------
    extract_query_k = f"""
    SELECT
        CUST_SVC_TASK_STTUS_SK,
        SRC_SYS_CD_SK,
        CUST_SVC_ID,
        TASK_SEQ_NO,
        STTUS_SEQ_NO,
        CRT_RUN_CYC_EXCTN_SK
    FROM {IDSOwner}.K_CUST_SVC_TASK_STTUS
    """

    df_k = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query_k)
            .load()
    )

    # ------------------------------------------------------------------
    # 3. Re-create the W_Extract link logic
    # ------------------------------------------------------------------
    join_cond = (
        (F.col("w.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("w.CUST_SVC_ID")   == F.col("k.CUST_SVC_ID"))   &
        (F.col("w.TASK_SEQ_NO")   == F.col("k.TASK_SEQ_NO"))   &
        (F.col("w.STTUS_SEQ_NO")  == F.col("k.STTUS_SEQ_NO"))
    )

    df_w_extract = (
        df_Transform.alias("w")
        .join(df_k.alias("k"), join_cond, "left")
        .select(
            F.coalesce("k.CUST_SVC_TASK_STTUS_SK", F.lit(-1))
                .alias("CUST_SVC_TASK_STTUS_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.CUST_SVC_ID"),
            F.col("w.TASK_SEQ_NO"),
            F.col("w.STTUS_SEQ_NO"),
            F.coalesce("k.CRT_RUN_CYC_EXCTN_SK", F.lit(CurrRunCycle))
                .alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # ------------------------------------------------------------------
    # 4. PrimaryKey transformer derivations
    # ------------------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "INSRT_UPDT_CD",
            F.when(
                F.col("CUST_SVC_TASK_STTUS_SK") == F.lit(-1),
                F.lit("I")
            ).otherwise(F.lit("U"))
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn(
            "CRT_RUN_CYC_EXCTN_SK",
            F.when(
                F.col("INSRT_UPDT_CD") == F.lit("I"),
                F.lit(CurrRunCycle)
            ).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    # Call SurrogateKeyGen in the mandated format
    df_enriched = SurrogateKeyGen(
        df_enriched,
        <DB sequence name>,
        "CUST_SVC_TASK_STTUS_SK",
        <schema>,
        <secret_name>
    )

    # ------------------------------------------------------------------
    # 5. Split out transformer output links
    # ------------------------------------------------------------------
    # NewKeys (svInstUpdt = 'I')
    df_newkeys = (
        df_enriched
        .filter(F.col("INSRT_UPDT_CD") == F.lit("I"))
        .select(
            "SRC_SYS_CD_SK",
            "CUST_SVC_ID",
            "TASK_SEQ_NO",
            "STTUS_SEQ_NO",
            "CRT_RUN_CYC_EXCTN_SK",
            "CUST_SVC_TASK_STTUS_SK"
        )
    )

    # Keys (all rows)
    df_keys = df_enriched.select(
        "SRC_SYS_CD_SK",
        "SRC_SYS_CD",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "STTUS_SEQ_NO",
        "INSRT_UPDT_CD",
        "CRT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_STTUS_SK"
    )

    # updt (for hash-file replacement, all rows)
    df_updt = df_enriched.select(
        "SRC_SYS_CD_SK",
        "CUST_SVC_ID",
        "TASK_SEQ_NO",
        "STTUS_SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "CUST_SVC_TASK_STTUS_SK"
    )

    # ------------------------------------------------------------------
    # 6. Write sequential file «K_CUST_SVC_TASK_STTUS.dat»
    # ------------------------------------------------------------------
    seq_file_path = f"{adls_path}/load/K_CUST_SVC_TASK_STTUS.dat"

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

    # ------------------------------------------------------------------
    # 7. Replace hash-file «hf_cust_svc_task_sttus_updt» (scenario c)
    #    with parquet write
    # ------------------------------------------------------------------
    parquet_path = f"{adls_path}/CustSvcTaskSttusPK_updt.parquet"

    write_files(
        df_updt,
        parquet_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # 8. Merge transformer logic – produce container output “Key”
    # ------------------------------------------------------------------
    merge_cond = (
        (F.col("all.SRC_SYS_CD_SK") == F.col("k.SRC_SYS_CD_SK")) &
        (F.col("all.CUST_SVC_ID")   == F.col("k.CUST_SVC_ID"))   &
        (F.col("all.TASK_SEQ_NO")   == F.col("k.TASK_SEQ_NO"))   &
        (F.col("all.STTUS_SEQ_NO")  == F.col("k.STTUS_SEQ_NO"))
    )

    df_key_out = (
        df_allcol_dedup.alias("all")
        .join(df_keys.alias("k"), merge_cond, "left")
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
            F.col("k.CUST_SVC_TASK_STTUS_SK").alias("CUST_SVC_TASK_STTUS_SK"),
            F.col("all.CUST_SVC_ID").alias("CUST_SVC_ID"),
            F.col("all.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
            F.col("all.STTUS_SEQ_NO").alias("STTUS_SEQ_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.CRT_BY_USER_SK").alias("CRT_BY_USER_SK"),
            F.col("all.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
            F.col("all.RTE_TO_USER_SK").alias("RTE_TO_USER_SK"),
            F.col("all.CUST_SVC_TASK_STTUS_CD").alias("CUST_SVC_TASK_STTUS_CD"),
            F.col("all.CUST_SVC_TASK_STTUS_RSN_CD")
                .alias("CUST_SVC_TASK_STTUS_RSN_CD"),
            F.col("all.STTUS_DTM").alias("STTUS_DTM"),
            F.col("all.RTE_TO_GRP_ID").alias("RTE_TO_GRP_ID")
        )
    )

    # ------------------------------------------------------------------
    # 9. Return container output
    # ------------------------------------------------------------------
    return df_key_out
# COMMAND ----------
