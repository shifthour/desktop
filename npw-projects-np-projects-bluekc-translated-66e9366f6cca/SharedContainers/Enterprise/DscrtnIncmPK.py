# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
"""
Shared Container: DscrtnIncmPK
DESCRIPTION:   Shared container used for Primary Keying of Discretionary Invoice Primary Key

VC LOGS
^1_1 07/29/09 09:43:06 Batch  15186_34991 PROMOTE bckcetl:31540 edw10 dsadm bls for sa
^1_1 07/29/09 09:38:54 Batch  15186_34736 INIT bckcett:31540 testEDW dsadm bls for sa
^1_2 07/28/09 15:33:53 Batch  15185_56095 PROMOTE bckcett:31540 testEDW u150906 TTR-565,566,567_Sharon_testEDW                         Maddy
^1_2 07/28/09 15:26:26 Batch  15185_55664 INIT bckcett:31540 devlEDW u150906 TTR-565,566,567_Sharon_devlEDW                    Maddy
^1_1 07/24/09 13:07:33 Batch  15181_47309 INIT bckcett:31540 devlEDW u150906 TTR565,566,567-Sharon_testEDW                 Maddy

MODIFICATIONS:
Developer           Date                         Project/Altius #             Change Description                                                                                                             Development Project          Code Reviewer          Date Reviewed
SAndrew             2009-07-15                  TTR565                       Used parameter Run Cycle as value for LAST_UPDT_RUN_CYC_EXCTN_SK                       devEDW                          Steph Goddard         07/28/2009
Kimberly Doty       2010-08-25                  TTR 551                      Added 3 new columns - INVC_DSCRTN_DPNDT_CT, INVC_DSCRTN_SUB_CT and INVC_DSCRTN_SELF_BILL_LIFE_IN
"""

# SQL joins temp table with key table to assign known keys
# Temp table is tuncated before load and runstat done after load
# Load IDS temp. table
# join primary key info with table info
# update primary key table (K_DSCRTN_INCM_F) with new keys created today
# primary key hash file used by multiple extracts.  Clear done in sequencer.
# Hash file (hf_dscrtn_incm_f_allcol) cleared in calling job
# Assign primary surrogate key
# called by programs EdwIncomeDscrtnIncmFExtr, EdwIncomeDscrtnIncmFBal

from pyspark.sql import DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql import types as T
from functools import reduce


def run_DscrtnIncmPK(
    df_AllCol: DataFrame,
    df_Transform: DataFrame,
    params: dict
) -> DataFrame:
    # ------------------------------------------------------------------
    # Unpack required parameters (exactly once)
    # ------------------------------------------------------------------
    CurrRunCycle           = params["CurrRunCycle"]
    CurrentDate            = params["CurrentDate"]
    IDSOwner               = params["IDSOwner"]
    ids_secret_name        = params["ids_secret_name"]
    EDWOwner               = params["EDWOwner"]
    edw_secret_name        = params["edw_secret_name"]
    adls_path              = params["adls_path"]
    adls_path_raw          = params["adls_path_raw"]
    adls_path_publish      = params["adls_path_publish"]
    edw_jdbc_url           = params["edw_jdbc_url"]
    edw_jdbc_props         = params["edw_jdbc_props"]
    # ------------------------------------------------------------------
    # Stage: hf_dscrtn_incm_f_allcol  (Scenario-a intermediate hash file)
    # ------------------------------------------------------------------
    hash_partition_cols = [
        "SRC_SYS_CD",
        "BILL_INVC_ID",
        "INVC_DSCRTN_SEQ_NO",
        "INVC_DSCRTN_YR_MO_SK",
        "GRP_ID",
        "SUBGRP_ID",
        "CLS_ID",
        "CLS_PLN_ID",
        "PROD_ID"
    ]
    df_AllColOut = dedup_sort(
        df_AllCol,
        hash_partition_cols,
        []  # sort_cols
    )
    # ------------------------------------------------------------------
    # Stage: K_DSCRTN_INCM_F (lookup from EDW)
    # ------------------------------------------------------------------
    extract_query_k = f"""
        SELECT DSCRTN_INCM_SK,
               SRC_SYS_CD,
               BILL_INVC_ID,
               INVC_DSCRTN_SEQ_NO,
               INVC_DSCRTN_YR_MO_SK,
               GRP_ID,
               SUBGRP_ID,
               CLS_ID,
               CLS_PLN_ID,
               PROD_ID,
               CRT_RUN_CYC_EXCTN_DT_SK
        FROM {EDWOwner}.K_DSCRTN_INCM_F
    """
    df_k = (
        spark.read.format("jdbc")
        .option("url", edw_jdbc_url)
        .options(**edw_jdbc_props)
        .option("query", extract_query_k)
        .load()
    )
    # ------------------------------------------------------------------
    # Build W_Extract equivalent
    # ------------------------------------------------------------------
    join_conditions = [
        F.col("w.SRC_SYS_CD") == F.col("k.SRC_SYS_CD"),
        F.col("w.BILL_INVC_ID") == F.col("k.BILL_INVC_ID"),
        F.col("w.INVC_DSCRTN_SEQ_NO") == F.col("k.INVC_DSCRTN_SEQ_NO"),
        F.col("w.INVC_DSCRTN_YR_MO_SK") == F.col("k.INVC_DSCRTN_YR_MO_SK"),
        F.col("w.GRP_ID") == F.col("k.GRP_ID"),
        F.col("w.SUBGRP_ID") == F.col("k.SUBGRP_ID"),
        F.col("w.CLS_ID") == F.col("k.CLS_ID"),
        F.col("w.CLS_PLN_ID") == F.col("k.CLS_PLN_ID"),
        F.col("w.PROD_ID") == F.col("k.PROD_ID")
    ]
    join_expr = reduce(lambda a, b: a & b, join_conditions)
    df_exist = (
        df_Transform.alias("w")
        .join(df_k.alias("k"), join_expr, "inner")
        .select(
            F.col("k.DSCRTN_INCM_SK"),
            F.col("w.SRC_SYS_CD"),
            F.col("w.BILL_INVC_ID"),
            F.col("w.INVC_DSCRTN_SEQ_NO"),
            F.col("w.INVC_DSCRTN_YR_MO_SK"),
            F.col("w.GRP_ID"),
            F.col("w.SUBGRP_ID"),
            F.col("w.CLS_ID"),
            F.col("w.CLS_PLN_ID"),
            F.col("w.PROD_ID"),
            F.col("k.CRT_RUN_CYC_EXCTN_DT_SK")
        )
    )
    df_new = (
        df_Transform.alias("w2")
        .join(df_k.alias("k2"), join_expr, "left_anti")
        .select(
            F.lit(-1).alias("DSCRTN_INCM_SK"),
            F.col("w2.SRC_SYS_CD"),
            F.col("w2.BILL_INVC_ID"),
            F.col("w2.INVC_DSCRTN_SEQ_NO"),
            F.col("w2.INVC_DSCRTN_YR_MO_SK"),
            F.col("w2.GRP_ID"),
            F.col("w2.SUBGRP_ID"),
            F.col("w2.CLS_ID"),
            F.col("w2.CLS_PLN_ID"),
            F.col("w2.PROD_ID"),
            F.lit(CurrentDate).alias("CRT_RUN_CYC_EXCTN_DT_SK")
        )
    )
    df_W_Extract = df_exist.unionByName(df_new)
    # ------------------------------------------------------------------
    # Transformer: PrimaryKey  (derive columns & surrogate key)
    # ------------------------------------------------------------------
    df_enriched = (
        df_W_Extract
        .withColumn(
            "svInstUpdt",
            F.when(F.col("DSCRTN_INCM_SK") == F.lit(-1), F.lit("I")).otherwise(F.lit("U"))
        )
        .withColumn(
            "svSK",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(None)).otherwise(F.col("DSCRTN_INCM_SK"))
        )
        .withColumn(
            "svCrtRunCycExctnDtSk",
            F.when(F.col("svInstUpdt") == F.lit("I"), F.lit(CurrentDate))
             .otherwise(F.col("CRT_RUN_CYC_EXCTN_DT_SK"))
        )
        .withColumn("svBillInvcId", F.col("BILL_INVC_ID"))
        .withColumn("svGrpId", F.col("GRP_ID"))
        .withColumn("svSubGrpId", F.col("SUBGRP_ID"))
        .withColumn("svClsID", F.col("CLS_ID"))
        .withColumn("svClsPlnId", F.col("CLS_PLN_ID"))
        .withColumn("svProdId", F.col("PROD_ID"))
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,'svSK',<schema>,<secret_name>)
    # ------------------------------------------------------------------
    # Build output links from PrimaryKey transformer
    # ------------------------------------------------------------------
    df_updt = df_enriched.select(
        F.col("SRC_SYS_CD"),
        F.col("svBillInvcId").alias("BILL_INVC_ID"),
        F.col("INVC_DSCRTN_SEQ_NO"),
        F.col("INVC_DSCRTN_YR_MO_SK"),
        F.col("svGrpId").alias("GRP_ID"),
        F.col("svSubGrpId").alias("SUBGRP_ID"),
        F.col("svClsID").alias("CLS_ID"),
        F.col("svClsPlnId").alias("CLS_PLN_ID"),
        F.col("svProdId").alias("PROD_ID"),
        F.col("svCrtRunCycExctnDtSk").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        F.col("svSK").alias("DSCRTN_INCM_SK")
    )
    df_newkeys = (
        df_enriched
        .filter(F.col("svInstUpdt") == F.lit("I"))
        .select(
            F.col("SRC_SYS_CD"),
            F.col("svBillInvcId").alias("BILL_INVC_ID"),
            F.col("INVC_DSCRTN_SEQ_NO"),
            F.col("INVC_DSCRTN_YR_MO_SK"),
            F.col("svGrpId").alias("GRP_ID"),
            F.col("svSubGrpId").alias("SUBGRP_ID"),
            F.col("svClsID").alias("CLS_ID"),
            F.col("svClsPlnId").alias("CLS_PLN_ID"),
            F.col("svProdId").alias("PROD_ID"),
            F.col("svCrtRunCycExctnDtSk").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
            F.col("svSK").alias("DSCRTN_INCM_SK")
        )
    )
    df_keys = df_enriched.select(
        F.col("svSK").alias("DSCRTN_INCM_SK"),
        F.col("svBillInvcId").alias("BILL_INVC_ID"),
        F.col("INVC_DSCRTN_SEQ_NO"),
        F.col("INVC_DSCRTN_YR_MO_SK"),
        F.col("svGrpId").alias("GRP_ID"),
        F.col("svSubGrpId").alias("SUBGRP_ID"),
        F.col("svClsID").alias("CLS_ID"),
        F.col("svClsPlnId").alias("CLS_PLN_ID"),
        F.col("svProdId").alias("PROD_ID"),
        F.col("SRC_SYS_CD"),
        F.col("svInstUpdt").alias("INSRT_UPDT_CD"),
        F.col("svCrtRunCycExctnDtSk").alias("CRT_RUN_CYC_EXCTN_DT_SK")
    )
    # ------------------------------------------------------------------
    # Write hashed-file (scenario-c) as parquet
    # ------------------------------------------------------------------
    write_files(
        df_updt,
        f"{adls_path}/DscrtnIncmPK_updt.parquet",
        delimiter=',',
        mode='overwrite',
        is_pqruet=True,
        header=True,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Write Sequential File K_DSCRTN_INCM_F
    # ------------------------------------------------------------------
    write_files(
        df_newkeys,
        f"{adls_path}/load/K_DSCRTN_INCM_F.dat",
        delimiter=',',
        mode='overwrite',
        is_pqruet=False,
        header=False,
        quote='"',
        nullValue=None
    )
    # ------------------------------------------------------------------
    # Transformer: Merge
    # ------------------------------------------------------------------
    merge_join_conditions = [
        F.col("k.SRC_SYS_CD") == F.col("a.SRC_SYS_CD"),
        F.col("k.BILL_INVC_ID") == F.col("a.BILL_INVC_ID"),
        F.col("k.INVC_DSCRTN_SEQ_NO") == F.col("a.INVC_DSCRTN_SEQ_NO"),
        F.col("k.INVC_DSCRTN_YR_MO_SK") == F.col("a.INVC_DSCRTN_YR_MO_SK"),
        F.col("k.GRP_ID") == F.col("a.GRP_ID"),
        F.col("k.SUBGRP_ID") == F.col("a.SUBGRP_ID"),
        F.col("k.CLS_ID") == F.col("a.CLS_ID"),
        F.col("k.CLS_PLN_ID") == F.col("a.CLS_PLN_ID"),
        F.col("k.PROD_ID") == F.col("a.PROD_ID")
    ]
    merge_join_expr = reduce(lambda a, b: a & b, merge_join_conditions)
    df_keyoutput = (
        df_keys.alias("k")
        .join(df_AllColOut.alias("a"), merge_join_expr, "left")
        .select(
            F.col("k.DSCRTN_INCM_SK"),
            F.col("k.SRC_SYS_CD"),
            F.col("k.BILL_INVC_ID"),
            F.col("k.INVC_DSCRTN_SEQ_NO"),
            F.col("k.INVC_DSCRTN_YR_MO_SK"),
            F.col("k.GRP_ID"),
            F.col("k.SUBGRP_ID"),
            F.col("k.CLS_ID"),
            F.col("k.CLS_PLN_ID"),
            F.col("k.PROD_ID"),
            F.lit(CurrentDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
            F.lit(CurrentDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
            F.col("a.BILL_ENTY_SK"),
            F.col("a.CLS_SK"),
            F.col("a.CLS_PLN_SK"),
            F.col("a.FEE_DSCNT_SK"),
            F.col("a.GRP_SK"),
            F.col("a.PROD_SK"),
            F.col("a.SUBGRP_SK"),
            F.col("a.INVC_CUR_RCRD_IN"),
            F.col("a.INVC_DSCRTN_BILL_DISP_CD"),
            F.col("a.INVC_DSCRTN_PRM_FEE_CD"),
            F.col("a.INVC_TYP_CD"),
            F.col("a.INVC_DSCRTN_BEG_DT_SK"),
            F.col("a.INVC_DSCRTN_END_DT_SK"),
            F.col("a.INVC_BILL_DUE_DT_SK"),
            F.col("a.INVC_BILL_DUE_YR_MO_SK"),
            F.col("a.INVC_BILL_END_DT_SK"),
            F.col("a.INVC_BILL_END_YR_MO_SK"),
            F.col("a.INVC_CRT_DT_SK"),
            F.col("a.INVC_DSCRTN_DUE_DT_SK"),
            F.col("a.INVC_DSCRTN_DPNDT_PRM_AMT"),
            F.col("a.INVC_DSCRTN_FEE_DSCNT_AMT"),
            F.col("a.INVC_DSCRTN_SUB_PRM_AMT"),
            F.col("a.INVC_DSCRTN_MO_QTY"),
            F.col("a.INVC_DSCRTN_DESC"),
            F.col("a.INVC_DSCRTN_PRSN_ID_TX"),
            F.col("a.INVC_DSCRTN_SH_DESC"),
            F.col("a.FEE_DSCNT_ID"),
            F.col("a.PROD_BILL_CMPNT_ID"),
            F.col("a.SUB_UNIQ_KEY"),
            F.col("a.CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("a.INVC_DSCRTN_BILL_DISP_CD_SK"),
            F.col("a.INVC_DSCRTN_PRM_FEE_CD_SK"),
            F.col("a.INVC_DSCRTN_SK"),
            F.col("a.INVC_TYP_CD_SK"),
            F.col("a.INVC_SK"),
            F.col("a.SUB_SK"),
            F.col("a.INVC_DSCRTN_DPNDT_CT"),
            F.col("a.INVC_DSCRTN_SUB_CT"),
            F.col("a.INVC_DSCRTN_SELF_BILL_LIFE_IN")
        )
    )
    # ------------------------------------------------------------------
    # Build UNK and NA rows
    # ------------------------------------------------------------------
    load_columns = df_keyoutput.columns
    unk_values = {
        "DSCRTN_INCM_SK": 0,
        "SRC_SYS_CD": "UNK",
        "BILL_INVC_ID": "UNK",
        "INVC_DSCRTN_SEQ_NO": 0,
        "INVC_DSCRTN_YR_MO_SK": "UNK",
        "GRP_ID": "UNK",
        "SUBGRP_ID": "UNK",
        "CLS_ID": "UNK",
        "CLS_PLN_ID": "UNK",
        "PROD_ID": "UNK",
        "CRT_RUN_CYC_EXCTN_DT_SK": "UNK",
        "LAST_UPDT_RUN_CYC_EXCTN_DT_SK": "UNK",
        "BILL_ENTY_SK": 0,
        "CLS_SK": 0,
        "CLS_PLN_SK": 0,
        "FEE_DSCNT_SK": 0,
        "GRP_SK": 0,
        "PROD_SK": 0,
        "SUBGRP_SK": 0,
        "INVC_CUR_RCRD_IN": "U",
        "INVC_DSCRTN_BILL_DISP_CD": "UNK",
        "INVC_DSCRTN_PRM_FEE_CD": "UNK",
        "INVC_TYP_CD": "UNK",
        "INVC_DSCRTN_BEG_DT_SK": "UNK",
        "INVC_DSCRTN_END_DT_SK": "UNK",
        "INVC_BILL_DUE_DT_SK": "UNK",
        "INVC_BILL_DUE_YR_MO_SK": "UNK",
        "INVC_BILL_END_DT_SK": "UNK",
        "INVC_BILL_END_YR_MO_SK": "UNK",
        "INVC_CRT_DT_SK": "UNK",
        "INVC_DSCRTN_DUE_DT_SK": "UNK",
        "INVC_DSCRTN_DPNDT_PRM_AMT": 0,
        "INVC_DSCRTN_FEE_DSCNT_AMT": 0,
        "INVC_DSCRTN_SUB_PRM_AMT": 0,
        "INVC_DSCRTN_MO_QTY": 0,
        "INVC_DSCRTN_DESC": "UNK",
        "INVC_DSCRTN_PRSN_ID_TX": "UNK",
        "INVC_DSCRTN_SH_DESC": "UNK",
        "FEE_DSCNT_ID": "UNK",
        "PROD_BILL_CMPNT_ID": "UNK",
        "SUB_UNIQ_KEY": 0,
        "CRT_RUN_CYC_EXCTN_SK": 0,
        "LAST_UPDT_RUN_CYC_EXCTN_SK": 0,
        "INVC_DSCRTN_BILL_DISP_CD_SK": 0,
        "INVC_DSCRTN_PRM_FEE_CD_SK": 0,
        "INVC_DSCRTN_SK": 0,
        "INVC_TYP_CD_SK": 0,
        "INVC_SK": 0,
        "SUB_SK": 0,
        "INVC_DSCRTN_DPNDT_CT": 0,
        "INVC_DSCRTN_SUB_CT": 0,
        "INVC_DSCRTN_SELF_BILL_LIFE_IN": "U"
    }
    na_values = unk_values.copy()
    na_values.update({
        "DSCRTN_INCM_SK": 1,
        "SRC_SYS_CD": "NA",
        "BILL_INVC_ID": "NA",
        "INVC_DSCRTN_SEQ_NO": 1,
        "INVC_DSCRTN_YR_MO_SK": "NA",
        "GRP_ID": "NA",
        "SUBGRP_ID": "NA",
        "CLS_ID": "NA",
        "CLS_PLN_ID": "NA",
        "PROD_ID": "NA",
        "INVC_CUR_RCRD_IN": "X",
        "INVC_DSCRTN_BILL_DISP_CD": "NA",
        "INVC_DSCRTN_PRM_FEE_CD": "NA",
        "INVC_TYP_CD": "NA",
        "INVC_DSCRTN_BEG_DT_SK": "NA",
        "INVC_DSCRTN_END_DT_SK": "NA",
        "INVC_BILL_DUE_DT_SK": "NA",
        "INVC_BILL_DUE_YR_MO_SK": "NA",
        "INVC_BILL_END_DT_SK": "NA",
        "INVC_BILL_END_YR_MO_SK": "NA",
        "INVC_CRT_DT_SK": "NA",
        "INVC_DSCRTN_DUE_DT_SK": "NA",
        "INVC_DSCRTN_DESC": "NA",
        "INVC_DSCRTN_PRSN_ID_TX": "NA",
        "INVC_DSCRTN_SH_DESC": "NA",
        "FEE_DSCNT_ID": "NA",
        "PROD_BILL_CMPNT_ID": "NA",
        "LAST_UPDT_RUN_CYC_EXCTN_SK": 1,
        "INVC_DSCRTN_SELF_BILL_LIFE_IN": "X"
    })
    df_unk = spark.createDataFrame([Row(**unk_values)], schema=df_keyoutput.schema)
    df_na  = spark.createDataFrame([Row(**na_values)],  schema=df_keyoutput.schema)
    # ------------------------------------------------------------------
    # Collector
    # ------------------------------------------------------------------
    df_load_file = df_keyoutput.unionByName(df_unk).unionByName(df_na)
    # ------------------------------------------------------------------
    # Return final DataFrame
    # ------------------------------------------------------------------
    return df_load_file