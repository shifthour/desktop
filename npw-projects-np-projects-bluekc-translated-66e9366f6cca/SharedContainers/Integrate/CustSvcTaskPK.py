# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame, functions as F, types as T

def run_CustSvcTaskPK(df_AllCol: DataFrame, params: dict) -> DataFrame:
    # --- unpack parameters exactly once
    CurrRunCycle        = params["CurrRunCycle"]
    RunID               = params["RunID"]
    CurrDate            = params["CurrDate"]
    SrcSysCdSk          = params["SrcSysCdSk"]
    SrcSysCd            = params["SrcSysCd"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    # ---------------------------------------------------------
    #   Step 1: replace intermediate hashed file hf_cust_svc_task_allcol
    # ---------------------------------------------------------
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRS_SYS_CD_SK", "CUST_SVC_ID", "TASK_SEQ_NO"],
        [("SRS_SYS_CD_SK", "A")]
    )
    # ---------------------------------------------------------
    #   Step 2: simulate K_CUST_SVC_TASK_TEMP logic
    # ---------------------------------------------------------
    extract_query_k = f"""
        SELECT CUST_SVC_TASK_SK,
               SRC_SYS_CD_SK,
               CUST_SVC_ID,
               TASK_SEQ_NO,
               CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_CUST_SVC_TASK
    """
    df_k_cust_svc_task = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query_k)
            .load()
    )
    df_temp = df_AllCol_dedup.select(
        F.col("SRC_SYS_CD_SK"),
        F.col("CUST_SVC_ID"),
        F.col("TASK_SEQ_NO")
    ).distinct()
    # existing keys
    df_existing = (
        df_temp.alias("w")
        .join(
            df_k_cust_svc_task.alias("k"),
            ["SRC_SYS_CD_SK", "CUST_SVC_ID", "TASK_SEQ_NO"],
            "inner"
        )
        .select(
            F.col("k.CUST_SVC_TASK_SK"),
            F.col("w.SRC_SYS_CD_SK"),
            F.col("w.CUST_SVC_ID"),
            F.col("w.TASK_SEQ_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK")
        )
    )
    # missing keys
    df_missing = (
        df_temp.alias("w2")
        .join(
            df_k_cust_svc_task.alias("k2"),
            ["SRC_SYS_CD_SK", "CUST_SVC_ID", "TASK_SEQ_NO"],
            "left_anti"
        )
        .select(
            F.lit(-1).alias("CUST_SVC_TASK_SK"),
            F.col("w2.SRC_SYS_CD_SK"),
            F.col("w2.CUST_SVC_ID"),
            F.col("w2.TASK_SEQ_NO"),
            F.lit(CurrRunCycle).cast("long").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    df_w_extract = df_existing.unionByName(df_missing)
    # ---------------------------------------------------------
    #   Step 3: PrimaryKey transformer logic
    # ---------------------------------------------------------
    df_enriched = (
        df_w_extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("CUST_SVC_TASK_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn("svSrcSysCd", F.lit(SrcSysCd))
        .withColumn(
            "CUST_SVC_TASK_SK",
            F.when(F.col("svInstUpdt") == "I", F.lit(None).cast("long"))
             .otherwise(F.col("CUST_SVC_TASK_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnSk",
            F.when(F.col("svInstUpdt") == "I", F.lit(CurrRunCycle).cast("long"))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_SK"))
        )
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"CUST_SVC_TASK_SK",<schema>,<secret_name>)
    # ---------------------------------------------------------
    #   Step 4: derive NewKeys, Keys, updt streams
    # ---------------------------------------------------------
    df_NewKeys = (
        df_enriched.filter(F.col("svInstUpdt") == "I")
        .select(
            F.col("SRC_SYS_CD_SK"),
            F.col("CUST_SVC_ID"),
            F.col("TASK_SEQ_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("CUST_SVC_TASK_SK")
        )
    )
    df_Keys = (
        df_enriched.select(
            F.col("CUST_SVC_TASK_SK"),
            F.col("SRC_SYS_CD_SK"),
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("CUST_SVC_ID"),
            F.col("TASK_SEQ_NO"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )
    df_updt = (
        df_enriched.select(
            F.col("svSrcSysCd").alias("SRC_SYS_CD"),
            F.col("CUST_SVC_ID"),
            F.col("TASK_SEQ_NO"),
            F.col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.col("CUST_SVC_TASK_SK")
        )
    )
    # ---------------------------------------------------------
    #   Step 5: Merge transformer logic
    # ---------------------------------------------------------
    join_expr = [
        df_AllCol_dedup["CUST_SVC_ID"] == df_Keys["CUST_SVC_ID"],
        df_AllCol_dedup["TASK_SEQ_NO"] == df_Keys["TASK_SEQ_NO"]
    ]
    df_Key = (
        df_AllCol_dedup.alias("all")
        .join(df_Keys.alias("k"), join_expr, "left")
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
            F.col("k.CUST_SVC_TASK_SK").alias("CUST_SVC_TASK_SK"),
            F.col("k.CUST_SVC_ID").alias("CUST_SVC_ID"),
            F.col("k.TASK_SEQ_NO").alias("TASK_SEQ_NO"),
            F.col("k.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).cast("long").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("all.CUST_SVC_SK").alias("CUST_SVC_SK"),
            F.col("all.GRP_SK").alias("GRP_SK"),
            F.col("all.INPT_USER_SK").alias("INPT_USER_SK"),
            F.col("all.LAST_UPDT_USER_SK").alias("LAST_UPDT_USER_SK"),
            F.col("all.MBR_SK").alias("MBR_SK"),
            F.col("all.PROD_SK").alias("PROD_SK"),
            F.col("all.PROV_SK").alias("PROV_SK"),
            F.col("all.SUBGRP_SK").alias("SUBGRP_SK"),
            F.col("all.SUB_SK").alias("SUB_SK"),
            F.col("all.CUST_SVC_TASK_CAT_CD_SK").alias("CUST_SVC_TASK_CAT_CD_SK"),
            F.col("all.CS_TASK_CLS_PLN_PROD_CAT_CD_SK").alias("CS_TASK_CLS_PLN_PROD_CAT_CD_SK"),
            F.col("all.CUST_SVC_TASK_CLSR_PRF_CD_SK").alias("CUST_SVC_TASK_CLSR_PRF_CD_SK"),
            F.col("all.CUST_SVC_TASK_CUST_TYP_CD_SK").alias("CUST_SVC_TASK_CUST_TYP_CD_SK"),
            F.col("all.CUST_SVC_TASK_FINL_ACTN_CD_SK").alias("CUST_SVC_TASK_FINL_ACTN_CD_SK"),
            F.col("all.CUST_SVC_TASK_ITS_TYP_CD_SK").alias("CUST_SVC_TASK_ITS_TYP_CD_SK"),
            F.col("all.CS_TASK_LTR_RECPNT_TYP_CD_SK").alias("CS_TASK_LTR_RECPNT_TYP_CD_SK"),
            F.col("all.CUST_SVC_TASK_PG_TYP_CD_SK").alias("CUST_SVC_TASK_PG_TYP_CD_SK"),
            F.col("all.CUST_SVC_TASK_PRTY_CD_SK").alias("CUST_SVC_TASK_PRTY_CD_SK"),
            F.col("all.CUST_SVC_TASK_STTUS_CD_SK").alias("CUST_SVC_TASK_STTUS_CD_SK"),
            F.col("all.CUST_SVC_TASK_STTUS_RSN_CD_SK").alias("CUST_SVC_TASK_STTUS_RSN_CD_SK"),
            F.col("all.CUST_SVC_TASK_SUBJ_CD_SK").alias("CUST_SVC_TASK_SUBJ_CD_SK"),
            F.col("all.CMPLNT_IN").alias("CMPLNT_IN"),
            F.col("all.CLSD_DT_SK").alias("CLSD_DT_SK"),
            F.col("all.INPT_DTM").alias("INPT_DTM"),
            F.col("all.LAST_UPDT_DTM").alias("LAST_UPDT_DTM"),
            F.col("all.NEXT_RVW_DT_SK").alias("NEXT_RVW_DT_SK"),
            F.col("all.RCVD_DT_SK").alias("RCVD_DT_SK"),
            F.col("all.NEXT_RVW_INTRVL_DAYS").alias("NEXT_RVW_INTRVL_DAYS"),
            F.col("all.CUST_ID").alias("CUST_ID"),
            F.col("all.CUST_SVC_TASK_USER_SITE_ID").alias("CUST_SVC_TASK_USER_SITE_ID"),
            F.col("all.INPT_USER_CC_ID").alias("INPT_USER_CC_ID"),
            F.col("all.SUM_DESC").alias("SUM_DESC"),
            F.col("all.SGSG_ID").alias("SGSG_ID"),
            F.col("all.GRGR_ID").alias("GRGR_ID"),
            F.col("all.USUS_ID").alias("USUS_ID"),
            F.col("all.CSTK_ASSIGN_USID").alias("CSTK_ASSIGN_USID"),
            F.col("all.ATUF_DATE1").alias("ATUF_DATE1"),
            F.col("all.CSTS_STS_DTM").alias("CSTS_STS_DTM"),
            F.col("all.CSTS_STS").alias("CSTS_STS"),
            F.col("all.CSTK_MCTR_SUBJ_1").alias("CSTK_MCTR_SUBJ_1"),
            F.col("all.CSTK_STS").alias("CSTK_STS")
        )
    )
    # ---------------------------------------------------------
    #   Step 6: write outputs
    # ---------------------------------------------------------
    # Sequential file for new keys
    seq_path = f"{adls_path}/load/K_CUST_SVC_TASK.dat"
    write_files(
        df_NewKeys,
        seq_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )
    # Parquet file for updt link
    parquet_path = f"{adls_path}/CustSvcTaskPK_updt.parquet"
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
    # ---------------------------------------------------------
    # return container output link
    # ---------------------------------------------------------
    return df_Key