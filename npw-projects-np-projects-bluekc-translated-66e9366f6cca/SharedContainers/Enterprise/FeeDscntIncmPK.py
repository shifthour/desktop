
# Databricks notebook source
# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

# COMMAND ----------
"""
Shared Container: FeeDscntIncmPK
Description:
* VC LOGS *
^1_1 07/29/09 09:43:06 Batch  15186_34991 PROMOTE bckcetl:31540 edw10 dsadm bls for sa
^1_1 07/29/09 09:38:54 Batch  15186_34736 INIT bckcett:31540 testEDW dsadm bls for sa
^1_2 07/28/09 15:33:53 Batch  15185_56095 PROMOTE bckcett:31540 testEDW u150906 TTR-565,566,567_Sharon_testEDW                         Maddy
^1_2 07/28/09 15:26:26 Batch  15185_55664 INIT bckcett:31540 devlEDW u150906 TTR-565,566,567_Sharon_devlEDW                    Maddy
^1_1 07/24/09 13:07:33 Batch  15181_47309 INIT bckcett:31540 devlEDW u150906 TTR565,566,567-Sharon_testEDW                 Maddy

***************************************************************************************************************************************************************
COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 

Primary key hash file used by multiple jobs.  Clear done in sequencer.
update primary key table (K_FEE_DSCNT_INCM_F) with new keys created today
Assign primary surrogate key
Hash file (hf_dscrtn_incm_f_allcol) cleared in calling job
join primary key info with table info
Load IDS temp. table
Temp table is tuncated before load and runstat done after load
SQL joins temp table with key table to assign known keys
called by programs EdwIncomeFeeDscntIncmFExtr, EdwIncomeFeeDscntIncmFBal
"""

# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------
def run_FeeDscntIncmPK(
    df_Transform: DataFrame,
    df_AllCol: DataFrame,
    params: dict
) -> DataFrame:
    """
    Executes the logic contained in the FeeDscntIncmPK shared container.
    
    Parameters
    ----------
    df_Transform : DataFrame
        Stream corresponding to container input link 'Transform'.
    df_AllCol : DataFrame
        Stream corresponding to container input link 'AllCol'.
    params : dict
        Dictionary of runtime parameters and pre-assembled JDBC configs.
    
    Returns
    -------
    DataFrame
        Stream corresponding to container output link 'load_file'.
    """

    # ------------------------------------------------------------------
    # Un-pack runtime parameters exactly once
    # ------------------------------------------------------------------
    CurrRunCycle        = params["CurrRunCycle"]
    CurrentDate         = params["CurrentDate"]
    IDSOwner            = params["IDSOwner"]
    ids_secret_name     = params["ids_secret_name"]
    EDWOwner            = params["EDWOwner"]
    edw_secret_name     = params["edw_secret_name"]
    edw_jdbc_url        = params["edw_jdbc_url"]
    edw_jdbc_props      = params["edw_jdbc_props"]
    ids_jdbc_url        = params["ids_jdbc_url"]
    ids_jdbc_props      = params["ids_jdbc_props"]
    adls_path           = params["adls_path"]
    adls_path_raw       = params["adls_path_raw"]
    adls_path_publish   = params["adls_path_publish"]
    FilePath            = params.get("FilePath", "")
    # ------------------------------------------------------------------
    # 1. Prepare Temp-table: {EDWOwner}.K_FEE_DSCNT_INCM_F_TEMP
    # ------------------------------------------------------------------
    execute_dml(
        f"TRUNCATE TABLE {EDWOwner}.K_FEE_DSCNT_INCM_F_TEMP",
        edw_jdbc_url,
        edw_jdbc_props
    )

    (
        df_Transform.write
        .format("jdbc")
        .option("url", edw_jdbc_url)
        .options(**edw_jdbc_props)
        .option("dbtable", f"{EDWOwner}.K_FEE_DSCNT_INCM_F_TEMP")
        .mode("append")
        .save()
    )

    execute_dml(
        f"CALL SYSPROC.ADMIN_CMD('runstats on table {EDWOwner}.K_FEE_DSCNT_INCM_F_TEMP "
        "on key columns with distribution on key columns and detailed indexes all allow write access')",
        edw_jdbc_url,
        edw_jdbc_props
    )

    # ------------------------------------------------------------------
    # 2. Extract W_Extract dataset from EDW
    # ------------------------------------------------------------------
    extract_query = f"""
    SELECT  k.FEE_DSCNT_INCM_SK,
            w.SRC_SYS_CD,
            w.BILL_INVC_ID,
            w.FEE_DSCNT_ID,
            w.INVC_FEE_DSCNT_BILL_DISP_CD,
            w.INVC_FEE_DSCNT_SRC_CD,
            w.GRP_ID,
            w.SUBGRP_ID,
            w.CLS_ID,
            w.CLS_PLN_ID,
            w.PROD_ID,
            k.CRT_RUN_CYC_EXCTN_DT_SK
    FROM    {EDWOwner}.K_FEE_DSCNT_INCM_F_TEMP w,
            {EDWOwner}.K_FEE_DSCNT_INCM_F       k
    WHERE   w.SRC_SYS_CD                       = k.SRC_SYS_CD
        AND w.BILL_INVC_ID                    = k.BILL_INVC_ID
        AND w.FEE_DSCNT_ID                    = k.FEE_DSCNT_ID
        AND w.INVC_FEE_DSCNT_BILL_DISP_CD     = k.INVC_FEE_DSCNT_BILL_DISP_CD
        AND w.INVC_FEE_DSCNT_SRC_CD           = k.INVC_FEE_DSCNT_SRC_CD
        AND w.GRP_ID                          = k.GRP_ID
        AND w.SUBGRP_ID                       = k.SUBGRP_ID
        AND w.CLS_ID                          = k.CLS_ID
        AND w.CLS_PLN_ID                      = k.CLS_PLN_ID
        AND w.PROD_ID                         = k.PROD_ID
    UNION
    SELECT  -1,
            w2.SRC_SYS_CD,
            w2.BILL_INVC_ID,
            w2.FEE_DSCNT_ID,
            w2.INVC_FEE_DSCNT_BILL_DISP_CD,
            w2.INVC_FEE_DSCNT_SRC_CD,
            w2.GRP_ID,
            w2.SUBGRP_ID,
            w2.CLS_ID,
            w2.CLS_PLN_ID,
            w2.PROD_ID,
            '{CurrentDate}'
    FROM    {EDWOwner}.K_FEE_DSCNT_INCM_F_TEMP w2
    WHERE   NOT EXISTS (
            SELECT 1
            FROM   {EDWOwner}.K_FEE_DSCNT_INCM_F k2
            WHERE  w2.SRC_SYS_CD                       = k2.SRC_SYS_CD
               AND w2.BILL_INVC_ID                    = k2.BILL_INVC_ID
               AND w2.FEE_DSCNT_ID                    = k2.FEE_DSCNT_ID
               AND w2.INVC_FEE_DSCNT_BILL_DISP_CD     = k2.INVC_FEE_DSCNT_BILL_DISP_CD
               AND w2.INVC_FEE_DSCNT_SRC_CD           = k2.INVC_FEE_DSCNT_SRC_CD
               AND w2.GRP_ID                          = k2.GRP_ID
               AND w2.SUBGRP_ID                       = k2.SUBGRP_ID
               AND w2.CLS_ID                          = k2.CLS_ID
               AND w2.CLS_PLN_ID                      = k2.CLS_PLN_ID
               AND w2.PROD_ID                         = k2.PROD_ID )
    """
    df_W_Extract = (
        spark.read.format("jdbc")
            .option("url", edw_jdbc_url)
            .options(**edw_jdbc_props)
            .option("query", extract_query)
            .load()
    )

    # ------------------------------------------------------------------
    # 3. PRIMARY KEY transformer logic
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn("svBillInvcId", F.trim(F.col("BILL_INVC_ID")))
        .withColumn("svGrpId",      F.trim(F.col("GRP_ID")))
        .withColumn("svSubGrpId",   F.trim(F.col("SUBGRP_ID")))
        .withColumn("svClsID",      F.trim(F.col("CLS_ID")))
        .withColumn("svClsPlnId",   F.trim(F.col("CLS_PLN_ID")))
        .withColumn("svProdId",     F.trim(F.col("PROD_ID")))
        .withColumn("svFeeDscntId", F.trim(F.col("FEE_DSCNT_ID")))
        .withColumn(
            "svInstUpdt",
            F.when(F.col("FEE_DSCNT_INCM_SK") == -1, F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svCrtRunCycExctnDtSk",
            F.when(F.col("FEE_DSCNT_INCM_SK") == -1, F.lit(CurrentDate))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK"))
        )
        .withColumn("svSK", F.col("FEE_DSCNT_INCM_SK"))
    )

    # Surrogate Key Assignment
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)

    # ------------------------------------------------------------------
    # 4. Build outputs from PrimaryKey transformer
    # ------------------------------------------------------------------
    select_cols = [
        "SRC_SYS_CD","svBillInvcId","svFeeDscntId","INVC_FEE_DSCNT_BILL_DISP_CD",
        "INVC_FEE_DSCNT_SRC_CD","svGrpId","svSubGrpId","svClsID","svClsPlnId",
        "svProdId","svCrtRunCycExctnDtSk","svSK"
    ]

    df_updt = df_enriched.select(
        F.col("SRC_SYS_CD"),
        F.col("svBillInvcId").alias("BILL_INVC_ID"),
        F.col("svFeeDscntId").alias("FEE_DSCNT_ID"),
        F.col("INVC_FEE_DSCNT_BILL_DISP_CD"),
        F.col("INVC_FEE_DSCNT_SRC_CD"),
        F.col("svGrpId").alias("GRP_ID"),
        F.col("svSubGrpId").alias("SUBGRP_ID"),
        F.col("svClsID").alias("CLS_ID"),
        F.col("svClsPlnId").alias("CLS_PLN_ID"),
        F.col("svProdId").alias("PROD_ID"),
        F.col("svCrtRunCycExctnDtSk").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("svSK").alias("FEE_DSCNT_INCM_SK")
    )

    df_newkeys = df_updt.filter(F.col("svInstUpdt") == "I")

    df_keys = (
        df_enriched
        .select(
            F.col("svSK").alias("FEE_DSCNT_INCM_SK"),
            F.col("SRC_SYS_CD"),
            F.col("svBillInvcId").alias("BILL_INVC_ID"),
            F.col("svFeeDscntId").alias("FEE_DSCNT_ID"),
            F.col("INVC_FEE_DSCNT_BILL_DISP_CD"),
            F.col("INVC_FEE_DSCNT_SRC_CD"),
            F.col("svGrpId").alias("GRP_ID"),
            F.col("svSubGrpId").alias("SUBGRP_ID"),
            F.col("svClsID").alias("CLS_ID"),
            F.col("svClsPlnId").alias("CLS_PLN_ID"),
            F.col("svProdId").alias("PROD_ID"),
            F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
            F.col("svCrtRunCycExctnDtSk").alias("CRT_RUN_CYC_EXCTN_DT_SK")
        )
    )

    # ------------------------------------------------------------------
    # 5. Hash-file hf_fee_dscnt_incm_f (scenario c -> parquet write)
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/FeeDscntIncmPK_updt.parquet",
        delimiter=",",
        mode="overwrite",
        is_pqruet=True,
        header=True,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # 6. Sequential File: K_FEE_DSCNT_INCM_F.dat
    # ------------------------------------------------------------------
    seq_path = f"{adls_path}/load/K_FEE_DSCNT_INCM_F.dat"
    write_files(
        df_newkeys,
        seq_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # ------------------------------------------------------------------
    # 7. Hash-file hf_fee_dscnt_incm_f_allcol (scenario a -> dedup)
    # ------------------------------------------------------------------
    key_cols_allcol = [
        "SRC_SYS_CD","BILL_INVC_ID","FEE_DSCNT_ID","INVC_FEE_DSCNT_BILL_DISP_CD",
        "INVC_FEE_DSCNT_SRC_CD","GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PROD_ID"
    ]
    df_AllCol_dedup = dedup_sort(
        df_AllCol,
        partition_cols=key_cols_allcol,
        sort_cols=[]
    )

    # ------------------------------------------------------------------
    # 8. Merge Transformer logic
    # ------------------------------------------------------------------
    join_expr = [
        df_keys.SRC_SYS_CD                 == df_AllCol_dedup.SRC_SYS_CD,
        df_keys.BILL_INVC_ID              == df_AllCol_dedup.BILL_INVC_ID,
        df_keys.FEE_DSCNT_ID              == df_AllCol_dedup.FEE_DSCNT_ID,
        df_keys.INVC_FEE_DSCNT_BILL_DISP_CD == df_AllCol_dedup.INVC_FEE_DSCNT_BILL_DISP_CD,
        df_keys.INVC_FEE_DSCNT_SRC_CD     == df_AllCol_dedup.INVC_FEE_DSCNT_SRC_CD,
        df_keys.GRP_ID                    == df_AllCol_dedup.GRP_ID,
        df_keys.SUBGRP_ID                 == df_AllCol_dedup.SUBGRP_ID,
        df_keys.CLS_ID                    == df_AllCol_dedup.CLS_ID,
        df_keys.CLS_PLN_ID                == df_AllCol_dedup.CLS_PLN_ID,
        df_keys.PROD_ID                   == df_AllCol_dedup.PROD_ID
    ]

    df_merge = (
        df_AllCol_dedup.alias("AllColOut")
        .join(df_keys.alias("Keys"), join_expr, "left")
    )

    # ------------------------------------------------------------------
    # 9. Collector Inputs
    # ------------------------------------------------------------------
    # Key output
    df_key_output = df_merge.select(
        F.col("Keys.FEE_DSCNT_INCM_SK").alias("FEE_DSCNT_INCM_SK"),
        F.col("Keys.SRC_SYS_CD"),
        F.col("Keys.BILL_INVC_ID"),
        F.col("Keys.FEE_DSCNT_ID"),
        F.col("Keys.INVC_FEE_DSCNT_BILL_DISP_CD"),
        F.col("Keys.INVC_FEE_DSCNT_SRC_CD"),
        F.col("Keys.GRP_ID"),
        F.col("Keys.SUBGRP_ID"),
        F.col("Keys.CLS_ID"),
        F.col("Keys.CLS_PLN_ID"),
        F.col("Keys.PROD_ID"),
        F.lit(CurrentDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.col("AllColOut.BILL_ENTY_SK"),
        F.col("AllColOut.CLS_SK"),
        F.col("AllColOut.CLS_PLN_SK"),
        F.col("AllColOut.FEE_DSCNT_SK"),
        F.col("AllColOut.GRP_SK"),
        F.col("AllColOut.SUBGRP_SK"),
        F.col("AllColOut.INVC_TYP_CD"),
        F.col("AllColOut.DSCNT_IN"),
        F.col("AllColOut.CUR_RCRD_IN").alias("INVC_CUR_RCRD_IN"),
        F.col("AllColOut.FEE_DSCNT_YR_MO_SK"),
        F.col("AllColOut.INVC_BILL_DUE_DT_SK"),
        F.col("AllColOut.INVC_BILL_DUE_YR_MO_SK"),
        F.col("AllColOut.INVC_BILL_END_DT_SK"),
        F.col("AllColOut.INVC_BILL_END_YR_MO_SK"),
        F.col("AllColOut.INVC_CRT_DT_SK"),
        F.col("AllColOut.FEE_DSCNT_AMT"),
        F.col("AllColOut.SUB_UNIQ_KEY"),
        F.col("AllColOut.CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("AllColOut.INVC_FEE_DSCNT_SRC_CD_SK"),
        F.col("AllColOut.INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
        F.col("AllColOut.INVC_FEE_DSCNT_SK"),
        F.col("AllColOut.INVC_SK"),
        F.col("AllColOut.INVC_TYP_CD_SK"),
        F.col("AllColOut.SUB_SK"),
        F.col("AllColOut.PROD_SK")
    )

    # UNK row
    df_unk = spark.range(1).select(
        F.lit(0).alias("FEE_DSCNT_INCM_SK"),
        F.lit("UNK").alias("SRC_SYS_CD"),
        F.lit("UNK").alias("BILL_INVC_ID"),
        F.lit("UNK").alias("FEE_DSCNT_ID"),
        F.lit("UNK").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
        F.lit("UNK").alias("INVC_FEE_DSCNT_SRC_CD"),
        F.lit("UNK").alias("GRP_ID"),
        F.lit("UNK").alias("SUBGRP_ID"),
        F.lit("UNK").alias("CLS_ID"),
        F.lit("UNK").alias("CLS_PLN_ID"),
        F.lit("UNK").alias("PROD_ID"),
        F.lit("UNK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("UNK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(0).alias("BILL_ENTY_SK"),
        F.lit(0).alias("CLS_SK"),
        F.lit(0).alias("CLS_PLN_SK"),
        F.lit(0).alias("FEE_DSCNT_SK"),
        F.lit(0).alias("GRP_SK"),
        F.lit(0).alias("SUBGRP_SK"),
        F.lit("UNK").alias("INVC_TYP_CD"),
        F.lit("U").alias("DSCNT_IN"),
        F.lit("U").alias("INVC_CUR_RCRD_IN"),
        F.lit("UNK").alias("FEE_DSCNT_YR_MO_SK"),
        F.lit("UNK").alias("INVC_BILL_DUE_DT_SK"),
        F.lit("UNK").alias("INVC_BILL_DUE_YR_MO_SK"),
        F.lit("UNK").alias("INVC_BILL_END_DT_SK"),
        F.lit("UNK").alias("INVC_BILL_END_YR_MO_SK"),
        F.lit("UNK").alias("INVC_CRT_DT_SK"),
        F.lit(0).alias("FEE_DSCNT_AMT"),
        F.lit(0).alias("SUB_UNIQ_KEY"),
        F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(0).alias("INVC_FEE_DSCNT_SRC_CD_SK"),
        F.lit(0).alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
        F.lit(0).alias("INVC_FEE_DSCNT_SK"),
        F.lit(0).alias("INVC_SK"),
        F.lit(0).alias("INVC_TYP_CD_SK"),
        F.lit(0).alias("SUB_SK"),
        F.lit(0).alias("PROD_SK")
    )

    # NA row
    df_na = spark.range(1).select(
        F.lit(1).alias("FEE_DSCNT_INCM_SK"),
        F.lit("NA").alias("SRC_SYS_CD"),
        F.lit("NA").alias("BILL_INVC_ID"),
        F.lit("NA").alias("FEE_DSCNT_ID"),
        F.lit("NA").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
        F.lit("NA").alias("INVC_FEE_DSCNT_SRC_CD"),
        F.lit("NA").alias("GRP_ID"),
        F.lit("NA").alias("SUBGRP_ID"),
        F.lit("NA").alias("CLS_ID"),
        F.lit("NA").alias("CLS_PLN_ID"),
        F.lit("NA").alias("PROD_ID"),
        F.lit("NA").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.lit("NA").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        F.lit(1).alias("BILL_ENTY_SK"),
        F.lit(1).alias("CLS_SK"),
        F.lit(1).alias("CLS_PLN_SK"),
        F.lit(1).alias("FEE_DSCNT_SK"),
        F.lit(1).alias("GRP_SK"),
        F.lit(1).alias("SUBGRP_SK"),
        F.lit("NA").alias("INVC_TYP_CD"),
        F.lit("X").alias("DSCNT_IN"),
        F.lit("X").alias("INVC_CUR_RCRD_IN"),
        F.lit("NA").alias("FEE_DSCNT_YR_MO_SK"),
        F.lit("NA").alias("INVC_BILL_DUE_DT_SK"),
        F.lit("NA").alias("INVC_BILL_DUE_YR_MO_SK"),
        F.lit("NA").alias("INVC_BILL_END_DT_SK"),
        F.lit("NA").alias("INVC_BILL_END_YR_MO_SK"),
        F.lit("NA").alias("INVC_CRT_DT_SK"),
        F.lit(0).alias("FEE_DSCNT_AMT"),
        F.lit(1).alias("SUB_UNIQ_KEY"),
        F.lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.lit(1).alias("INVC_FEE_DSCNT_SRC_CD_SK"),
        F.lit(1).alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
        F.lit(1).alias("INVC_FEE_DSCNT_SK"),
        F.lit(1).alias("INVC_SK"),
        F.lit(1).alias("INVC_TYP_CD_SK"),
        F.lit(1).alias("SUB_SK"),
        F.lit(1).alias("PROD_SK")
    )

    # Collect (Round-Robin -> Union)
    df_load_file = df_unk.unionByName(df_na).unionByName(df_key_output)

    return df_load_file
