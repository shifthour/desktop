# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
Job Name      : BillSummNewPK
Job Type      : Server Job
Folder Path   : Shared Containers/PrimaryKey

Description
-----------
Copyright 2010 Blue Cross and Blue Shield of Kansas City

Called by: FctsBillSumExtr

Processing:
    Creates primary key value for rows with new natural key values.
    Looks up existing primary key value for an existing natural key.
    The hf_bill_sum_allcol hashed file is cleared by the calling job.

Control Job Rerun Information:
    Previous Run Successful : Truncate the K_BILL_SUM table
    Previous Run Aborted    : Restart, no other steps necessary

Annotations
-----------
Assign primary surrogate key
Hashed file (hf_bill_sum_allcol) cleared by calling job
Join primary key info with table info
Load IDS temp table
SQL joins temp table with key table to assign known keys
Primary key hashed file contains only keys from rows in current run.  Hashed file is cleared before writing
Update primary key table (K_BILL_SUM) with new primary key values created during current run
"""

from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def run_BillSummNewPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the BillSummNewPK shared-container logic.

    Parameters
    ----------
    df_AllCol : DataFrame
        Container input link “AllCol”.
    df_Transform : DataFrame
        Container input link “Transform”.
    params : dict
        Runtime parameters and JDBC configurations.

    Returns
    -------
    DataFrame
        Container output link “Key”.
    """

    # ----------------------------------------------------
    # Unpack parameters (each exactly once)
    # ----------------------------------------------------
    CurrRunCycle         = params["CurrRunCycle"]
    SrcSysCd             = params["SrcSysCd"]
    SrcSysCdSk           = params["SrcSysCdSk"]
    IDSOwner             = params["IDSOwner"]
    ids_secret_name      = params["ids_secret_name"]
    ids_jdbc_url         = params["ids_jdbc_url"]
    ids_jdbc_props       = params["ids_jdbc_props"]
    adls_path            = params["adls_path"]
    adls_path_raw        = params["adls_path_raw"]
    adls_path_publish    = params["adls_path_publish"]

    # ----------------------------------------------------
    # 1. hf_bill_sum_allcol  (Scenario-a intermediate hash)
    #    Deduplicate by natural key before downstream use
    # ----------------------------------------------------
    dedup_keys = ["BILL_ENTY_UNIQ_KEY", "BILL_DUE_DT_SK", "SRC_SYS_CD_SK"]
    df_allcol_dedup = df_AllCol.dropDuplicates(dedup_keys)

    # ----------------------------------------------------
    # 2. K_BILL_SUM_TEMP – load temp table then extract
    # ----------------------------------------------------
    truncate_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_BILL_SUM_TEMP')"
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)

    (
        df_Transform.select(
            "BILL_ENTY_UNIQ_KEY",
            "BILL_DUE_DT_SK",
            "SRC_SYS_CD_SK"
        )
        .write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_BILL_SUM_TEMP")
        .mode("append")
        .save()
    )

    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_BILL_SUM_TEMP "
        f"on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    extract_query = f"""
        SELECT
             k.BILL_SUM_SK,
             w.BILL_ENTY_UNIQ_KEY,
             w.BILL_DUE_DT_SK,
             w.SRC_SYS_CD_SK,
             k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_BILL_SUM_TEMP w,
             {IDSOwner}.K_BILL_SUM k
        WHERE
             w.BILL_ENTY_UNIQ_KEY = k.BILL_ENTY_UNIQ_KEY AND
             w.BILL_DUE_DT_SK     = k.BILL_DUE_DT_SK     AND
             w.SRC_SYS_CD_SK      = k.SRC_SYS_CD_SK
        UNION
        SELECT
             -1,
             w2.BILL_ENTY_UNIQ_KEY,
             w2.BILL_DUE_DT_SK,
             w2.SRC_SYS_CD_SK,
             {CurrRunCycle}
        FROM {IDSOwner}.K_BILL_SUM_TEMP w2
        WHERE NOT EXISTS (
            SELECT 1
            FROM {IDSOwner}.K_BILL_SUM k2
            WHERE w2.BILL_ENTY_UNIQ_KEY = k2.BILL_ENTY_UNIQ_KEY AND
                  w2.BILL_DUE_DT_SK     = k2.BILL_DUE_DT_SK     AND
                  w2.SRC_SYS_CD_SK      = k2.SRC_SYS_CD_SK
        )
    """

    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # ----------------------------------------------------
    # 3. PrimaryKey transformer logic
    # ----------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("BILL_SUM_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None)).otherwise(F.col("BILL_SUM_SK"))
        )
    )

    # Surrogate key generation (special-case call; do not substitute params)
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    df_enriched = df_enriched.withColumn(
        "svCrtRunCycExctnSk",
        F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle)).otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
    )

    # updt link – to hf_bill_sum (parquet)
    df_updt = (
        df_enriched.select(
            "BILL_ENTY_UNIQ_KEY",
            "BILL_DUE_DT_SK"
        )
        .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
        .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("svCrtRunCycExctnSk"))
        .withColumn("BILL_SUM_SK", F.col("svSK"))
    )

    write_files(
        df_updt,
        f"{adls_path}/BillSummNewPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )

    # NewKeys link – sequential file
    df_newkeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            "BILL_ENTY_UNIQ_KEY",
            "BILL_DUE_DT_SK",
            "SRC_SYS_CD_SK",
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("svSK").alias("BILL_SUM_SK")
        )
    )

    write_files(
        df_newkeys,
        f"{adls_path}/load/K_BILL_SUM.dat",
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=True,
        quote='"',
        nullValue=None
    )

    # Keys link – for downstream merge
    df_keys = (
        df_enriched.select(
            F.col("svSK").alias("BILL_SUM_SK"),
            "BILL_ENTY_UNIQ_KEY",
            "BILL_DUE_DT_SK",
            "SRC_SYS_CD_SK"
        )
        .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
        .withColumn("INSRT_UPDT_CD", F.col("svInstUpdt"))
        .withColumn("CRT_RUN_CYC_EXCTN_SK", F.col("svCrtRunCycExctnSk"))
    )

    # ----------------------------------------------------
    # 4. Merge transformer
    # ----------------------------------------------------
    df_merge = (
        df_allcol_dedup.alias("all")
        .join(
            df_keys.alias("k"),
            [
                F.col("all.BILL_ENTY_UNIQ_KEY") == F.col("k.BILL_ENTY_UNIQ_KEY"),
                F.col("all.BILL_DUE_DT_SK")     == F.col("k.BILL_DUE_DT_SK"),
                F.col("all.SRC_SYS_CD_SK")      == F.col("k.SRC_SYS_CD_SK")
            ],
            "left"
        )
    )

    df_key_out = (
        df_merge.select(
            F.when(F.col("all.JOB_EXCTN_RCRD_ERR_SK").isNotNull(), F.col("all.JOB_EXCTN_RCRD_ERR_SK")).otherwise(F.lit(0)).alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("k.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.when(F.col("all.DISCARD_IN").isNotNull(), F.col("all.DISCARD_IN")).otherwise(F.lit("U")).alias("DISCARD_IN"),
            F.when(F.col("all.PASS_THRU_IN").isNotNull(), F.col("all.PASS_THRU_IN")).otherwise(F.lit("U")).alias("PASS_THRU_IN"),
            F.when(F.col("all.FIRST_RECYC_DT").isNotNull(), F.col("all.FIRST_RECYC_DT")).otherwise(F.lit("1753-01-01")).alias("FIRST_RECYC_DT"),
            F.when(F.col("all.ERR_CT").isNotNull(), F.col("all.ERR_CT")).otherwise(F.lit(0)).alias("ERR_CT"),
            F.when(F.col("all.RECYCLE_CT").isNotNull(), F.col("all.RECYCLE_CT")).otherwise(F.lit(0)).alias("RECYCLE_CT"),
            F.col("k.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.when(F.col("all.PRI_KEY_STRING").isNotNull(), F.col("all.PRI_KEY_STRING")).otherwise(F.lit("UNK")).alias("PRI_KEY_STRING"),
            F.col("k.BILL_SUM_SK").alias("BILL_SUM_SK"),
            F.when(F.col("all.BILL_ENTY_UNIQ_KEY").isNotNull(), F.col("all.BILL_ENTY_UNIQ_KEY")).otherwise(F.lit(0)).alias("BILL_ENTY_UNIQ_KEY"),
            F.when(F.col("all.BILL_DUE_DT_SK").isNotNull(), F.col("all.BILL_DUE_DT_SK")).otherwise(F.lit("1753-01-01")).alias("BILL_DUE_DT_SK"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.when(F.col("all.BILL_ENTY").isNotNull(), F.col("all.BILL_ENTY")).otherwise(F.lit(0)).alias("BILL_ENTY"),
            F.when(F.col("all.LAST_BILL_INVC").isNotNull(), F.col("all.LAST_BILL_INVC")).otherwise(F.lit("UNK")).alias("LAST_BILL_INVC"),
            F.when(F.col("all.BILL_SUM_BILL_TYP_CD").isNotNull(), F.col("all.BILL_SUM_BILL_TYP_CD")).otherwise(F.lit("")).alias("BILL_SUM_BILL_TYP_CD"),
            F.when(F.col("all.BILL_SUM_BILL_PD_STTUS_CD").isNotNull(), F.col("all.BILL_SUM_BILL_PD_STTUS_CD")).otherwise(F.lit("")).alias("BILL_SUM_BILL_PD_STTUS_CD"),
            F.when(F.col("all.BILL_SUM_SPCL_BILL_CD").isNotNull(), F.col("all.BILL_SUM_SPCL_BILL_CD")).otherwise(F.lit("")).alias("BILL_SUM_SPCL_BILL_CD"),
            F.when(F.col("all.PRORT_IN").isNotNull(), F.col("all.PRORT_IN")).otherwise(F.lit("U")).alias("PRORT_IN"),
            F.when(F.col("all.CRT_DTM").isNotNull(), F.col("all.CRT_DTM")).otherwise(F.lit("1753-01-01 00:00:00.000000")).alias("CRT_DTM"),
            F.when(F.col("all.DLQNCY_DT_SK").isNotNull(), F.col("all.DLQNCY_DT_SK")).otherwise(F.lit("1753-01-01")).alias("DLQNCY_DT_SK"),
            F.when(F.col("all.END_DT_SK").isNotNull(), F.col("all.END_DT_SK")).otherwise(F.lit("1753-01-01")).alias("END_DT_SK"),
            F.when(F.col("all.LAST_ALLOC_DT_SK").isNotNull(), F.col("all.LAST_ALLOC_DT_SK")).otherwise(F.lit("1753-01-01")).alias("LAST_ALLOC_DT_SK"),
            F.when(F.col("all.PRM_UPDT_DTM").isNotNull(), F.col("all.PRM_UPDT_DTM")).otherwise(F.lit("1753-01-01 00:00:00.000000")).alias("PRM_UPDT_DTM"),
            F.when(F.col("all.RECON_DT_SK").isNotNull(), F.col("all.RECON_DT_SK")).otherwise(F.lit("1753-01-01")).alias("RECON_DT_SK"),
            F.when(F.col("all.BILL_AMT").isNotNull(), F.col("all.BILL_AMT")).otherwise(F.lit(0)).alias("BILL_AMT"),
            F.when(F.col("all.RCVD_AMT").isNotNull(), F.col("all.RCVD_AMT")).otherwise(F.lit(0)).alias("RCVD_AMT"),
            F.when(F.col("all.ACTV_SUBS_NO").isNotNull(), F.col("all.ACTV_SUBS_NO")).otherwise(F.lit(0)).alias("ACTV_SUBS_NO"),
            F.when(F.col("all.BILL_DAYS_NO").isNotNull(), F.col("all.BILL_DAYS_NO")).otherwise(F.lit(0)).alias("BILL_DAYS_NO"),
            F.when(F.col("all.PRORT_FCTR").isNotNull(), F.col("all.PRORT_FCTR")).otherwise(F.lit(0)).alias("PRORT_FCTR"),
            F.when(F.col("all.LAST_BILL_INVC_ID").isNotNull(), F.col("all.LAST_BILL_INVC_ID")).otherwise(F.lit("UNK")).alias("LAST_BILL_INVC_ID"),
            F.when(F.col("all.BLBL_SUBSIDY_AMT_NVL").isNull(), F.lit(0.00)).otherwise(F.col("all.BLBL_SUBSIDY_AMT_NVL")).alias("BLBL_SUBSIDY_AMT_NVL"),
            F.when(F.col("all.BLBL_APTC_PAID_STS_NVL").isNull(), F.lit("NA")).otherwise(F.col("all.BLBL_APTC_PAID_STS_NVL")).alias("BLBL_APTC_PAID_STS_NVL"),
            F.when(F.col("all.BLBL_APTC_DLNQ_DT_NVL").isNull(), F.lit("1753-01-01")).otherwise(F.col("all.BLBL_APTC_DLNQ_DT_NVL")).alias("BLBL_APTC_DLNQ_DT_NVL")
        )
    )

    return df_key_out