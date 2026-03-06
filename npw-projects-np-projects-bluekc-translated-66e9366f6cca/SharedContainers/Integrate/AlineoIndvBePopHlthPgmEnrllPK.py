# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
AlineoIndvBePopHlthPgmEnrllPK
****************************************************************************************************************************************************************************
 Copyright 2010 Blue Cross and Blue Shield of Kansas City

Called by: AlineoIndvBePopHlthPgmEnrSeq

Processing:
                    Extracts data from an XML file received from Alineo and creates a crf file in the ../key directory.

Control Job Rerun Information: 
                    Previous Run Successful:    What needs to happen before a special run can occur?
                    Previous Run Aborted:         Restart, no other steps necessary

Modifications:                        
                                                                                        Project/                                                                                     Code                   Date
Developer                                                 Date              Altiris #     Change Description                                                   Reviewer            Reviewed
-------------------------  -------------------   -------------   -----------------------------------------------------------------------------------------------------------------   -------------------------  -------------------

Karthik Chintalapani                                 2015-03-10     5157       Added new column ORIGNT_SRC_SYS_CODE     Kalyan Neelam     2015-03-17
Annotations:
- join primary key info with table info
- update primary key table (K_PAYMT_SUM) with new keys created today
- Assign primary surrogate key
- primary key hash file only contains current run keys and is cleared before writing
- SQL joins temp table with key table to assign known keys
- This container is used in:
AlineoIdsIndvBePopHlthPgmEnrTransExtr

These programs need to be re-compiled when logic changes
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit

def run_AlineoIndvBePopHlthPgmEnrllPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # --- unpack parameters ---
    CurrRunCycle          = params["CurrRunCycle"]
    SrcSysCd              = params["SrcSysCd"]
    IDSOwner              = params["IDSOwner"]
    ids_secret_name       = params["ids_secret_name"]
    ids_jdbc_url          = params["ids_jdbc_url"]
    ids_jdbc_props        = params["ids_jdbc_props"]
    adls_path             = params["adls_path"]

    # --- stage: hf_indv_be_pop_hlth_pgm_enr_trans_allcol (scenario a) ---
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        ["SRC_SYS_CD_SK", "POP_HLTH_PGM_ENR_ID", "ROW_EFF_DT_SK"],
        []
    )

    # --- stage: K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP - before SQL ---
    truncate_sql = f"CALL {IDSOwner}.BCSP_TRUNCATE('{IDSOwner}', 'K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP')"
    execute_dml(truncate_sql, ids_jdbc_url, ids_jdbc_props)

    # --- write df_Transform into temp table ---
    (
        df_Transform
        .write
        .format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", f"{IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP")
        .mode("append")
        .save()
    )

    # --- stage: K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP - after SQL ---
    runstats_sql = (
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')"
    )
    execute_dml(runstats_sql, ids_jdbc_url, ids_jdbc_props)

    # --- stage: K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP - extract ---
    extract_query = f"""
        SELECT k.INDV_BE_POP_HLTH_PGM_ENR_T_SK,
               w.POP_HLTH_PGM_ENR_ID,
               w.ROW_EFF_DT_SK,
               w.SRC_SYS_CD_SK,
               k.CRT_RUN_CYC_EXCTN_SK
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP w
        JOIN {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS k
          ON w.POP_HLTH_PGM_ENR_ID = k.POP_HLTH_PGM_ENR_ID
         AND w.ROW_EFF_DT_SK      = k.ROW_EFF_DT_SK
         AND w.SRC_SYS_CD_SK      = k.SRC_SYS_CD_SK
        UNION
        SELECT -1,
               w2.POP_HLTH_PGM_ENR_ID,
               w2.ROW_EFF_DT_SK,
               w2.SRC_SYS_CD_SK,
               {CurrRunCycle}
        FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS_TEMP w2
        WHERE NOT EXISTS (
              SELECT 1
              FROM {IDSOwner}.K_INDV_BE_POP_HLTH_PGM_ENR_TRANS k2
              WHERE w2.POP_HLTH_PGM_ENR_ID = k2.POP_HLTH_PGM_ENR_ID
                AND w2.ROW_EFF_DT_SK      = k2.ROW_EFF_DT_SK
                AND w2.SRC_SYS_CD_SK      = k2.SRC_SYS_CD_SK
        )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --- transformer: PrimaryKey ---
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            when(col("INDV_BE_POP_HLTH_PGM_ENR_T_SK") == -1, lit("I")).otherwise(lit("U"))
        )
        .withColumn("svSrcSysCd", lit("ALINEO"))
        .withColumn(
            "svCrtRunCycExctnSk",
            when(col("INDV_BE_POP_HLTH_PGM_ENR_T_SK") == -1, lit(CurrRunCycle)).otherwise(col("CRT_RUN_CYC_EXCTN_SK"))
        )
        .withColumn(
            "svSK",
            when(col("INDV_BE_POP_HLTH_PGM_ENR_T_SK") == -1, lit(None)).otherwise(col("INDV_BE_POP_HLTH_PGM_ENR_T_SK"))
        )
    )

    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # --- output link: updt ---
    df_updt = (
        df_enriched.select(
            col("POP_HLTH_PGM_ENR_ID"),
            col("ROW_EFF_DT_SK"),
            lit(SrcSysCd).alias("SRC_SYS_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK")
        )
    )

    # --- output link: NewKeys ---
    df_NewKeys = (
        df_enriched.filter(col("svInstUpdt") == lit("I")).select(
            col("POP_HLTH_PGM_ENR_ID"),
            col("ROW_EFF_DT_SK"),
            col("SRC_SYS_CD_SK"),
            lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
            col("svSK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK")
        )
    )

    # --- output link: Keys ---
    df_Keys = (
        df_enriched.select(
            col("svSK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK"),
            col("SRC_SYS_CD_SK"),
            col("svSrcSysCd").alias("SRC_SYS_CD"),
            col("POP_HLTH_PGM_ENR_ID"),
            col("ROW_EFF_DT_SK"),
            col("svInstUpdt").alias("INSRT_UPDT_CD"),
            col("svCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK")
        )
    )

    # --- write sequential file from NewKeys ---
    write_files(
        df_NewKeys,
        f"{adls_path}/load/K_INDV_BE_POP_HLTH_PGM_ENR_TRANS.dat",
        ",",
        "overwrite",
        False,
        False,
        "\"",
        None
    )

    # --- write parquet file from updt (hash-file target) ---
    write_files(
        df_updt,
        f"{adls_path}/AlineoIndvBePopHlthPgmEnrllPK_updt.parquet",
        ",",
        "overwrite",
        True,
        True,
        "\"",
        None
    )

    # --- transformer: Merge ---
    join_cond = (
        (col("AllColOut.POP_HLTH_PGM_ENR_ID") == col("Keys.POP_HLTH_PGM_ENR_ID")) &
        (col("AllColOut.ROW_EFF_DT_SK") == col("Keys.ROW_EFF_DT_SK")) &
        (col("AllColOut.SRC_SYS_CD_SK") == col("Keys.SRC_SYS_CD_SK"))
    )

    df_merge = (
        df_AllCol_dedup.alias("AllColOut")
        .join(df_Keys.alias("Keys"), join_cond, "left")
    )

    df_Key = (
        df_merge.select(
            col("AllColOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            col("AllColOut.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            col("AllColOut.DISCARD_IN").alias("DISCARD_IN"),
            col("AllColOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
            col("AllColOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            col("AllColOut.ERR_CT").alias("ERR_CT"),
            col("AllColOut.RECYCLE_CT").alias("RECYCLE_CT"),
            col("AllColOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
            col("AllColOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            col("Keys.INDV_BE_POP_HLTH_PGM_ENR_T_SK").alias("INDV_BE_POP_HLTH_PGM_ENR_T_SK"),
            col("AllColOut.POP_HLTH_PGM_ENR_ID").alias("POP_HLTH_PGM_ENR_ID"),
            col("AllColOut.ROW_EFF_DT_SK").alias("ROW_EFF_DT_SK"),
            col("AllColOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            col("Keys.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
            lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            col("AllColOut.ENR_DENIED_RSN_CD").alias("ENR_DENIED_RSN_CD"),
            col("AllColOut.ENR_PROD_CD").alias("ENR_PROD_CD"),
            col("AllColOut.ENR_SVRTY_CD").alias("ENR_SVRTY_CD"),
            col("AllColOut.PGM_CLOSE_RSN_CD").alias("PGM_CLOSE_RSN_CD"),
            col("AllColOut.PGM_SCRN_STTUS_CD").alias("PGM_SCRN_STTUS_CD"),
            col("AllColOut.PGM_SRC_CD").alias("PGM_SRC_CD"),
            col("AllColOut.SCRN_RQST_PROD_CD").alias("SCRN_RQST_PROD_CD"),
            col("AllColOut.SCRN_RQST_SVRTY_CD").alias("SCRN_RQST_SVRTY_CD"),
            col("AllColOut.PGM_CLOSE_DT_SK").alias("PGM_CLOSE_DT_SK"),
            col("AllColOut.PGM_ENR_CRT_DT_SK").alias("PGM_ENR_CRT_DT_SK"),
            col("AllColOut.PGM_ENR_DT_SK").alias("PGM_ENR_DT_SK"),
            col("AllColOut.PGM_RQST_DT_SK").alias("PGM_RQST_DT_SK"),
            col("AllColOut.PGM_SCRN_DT_SK").alias("PGM_SCRN_DT_SK"),
            col("AllColOut.PGM_STRT_DT_SK").alias("PGM_STRT_DT_SK"),
            col("AllColOut.ROW_TERM_DT_SK").alias("ROW_TERM_DT_SK"),
            col("AllColOut.INDV_BE_KEY").alias("INDV_BE_KEY"),
            col("AllColOut.ENRED_BY_USER_ID").alias("ENRED_BY_USER_ID"),
            col("AllColOut.ENR_ASG_TO_USER_ID").alias("ENR_ASG_TO_USER_ID"),
            col("AllColOut.ENR_POP_HLTH_PGM_ID").alias("ENR_POP_HLTH_PGM_ID"),
            col("AllColOut.SCRN_BY_USER_ID").alias("SCRN_BY_USER_ID"),
            col("AllColOut.SCRN_ASG_TO_USER_ID").alias("SCRN_ASG_TO_USER_ID"),
            col("AllColOut.SCRN_POP_HLTH_PGM_ID").alias("SCRN_POP_HLTH_PGM_ID"),
            col("AllColOut.ENROLLMENT_MODIFIED").alias("ENROLLMENT_MODIFIED"),
            col("AllColOut.ORIGNT_SRC_SYS_CODE").alias("ORIGNT_SRC_SYS_CODE")
        )
    )

    return df_Key