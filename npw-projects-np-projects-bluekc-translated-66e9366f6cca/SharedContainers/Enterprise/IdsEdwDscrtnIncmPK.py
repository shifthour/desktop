# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY

DESCRIPTION:
Shared container used for Primary Keying of Fee Discount  Primary Key

ANNOTATIONS:
- Insert Only the Newly generated Pkeys into the K Table
- New PKEYs are generated in this transformer. A database sequence will be used to create next PKEY value.
- Left Join on Natural Keys
- Left Join primary key info with table info
- Read K_DSCRTN_INCM_F Table to pull the Natural Keys and the Skey.

MODIFICATIONS:
Developer           Date         Project/Altius #   Change Description
Syed Hussaini       2013-10-30   #5114              Parallel conversion from Server
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, lit
from typing import Tuple


def run_IdsEdwDscrtnIncmPK(
    df_lnk_Transforms_Out: DataFrame,
    df_lnk_DscrntIncmData_L_in: DataFrame,
    params: dict,
) -> DataFrame:
    """
    PySpark translation of DataStage shared container 'IdsEdwDscrtnIncmPK'.
    """

    # --------------------------------------------------
    # Unpack runtime parameters (once only)
    # --------------------------------------------------
    IDSIncmBeginCycle = params["IDSIncmBeginCycle"]
    EDWRunCycle = params["EDWRunCycle"]
    EDWRunCycleDate = params["EDWRunCycleDate"]

    EDWOwner = params["EDWOwner"]
    edw_secret_name = params["edw_secret_name"]
    edw_jdbc_url = params["edw_jdbc_url"]
    edw_jdbc_props = params["edw_jdbc_props"]

    # --------------------------------------------------
    # Stage: db2_K_DSCRTN_INCM_F_in  (DB Read)
    # --------------------------------------------------
    extract_query = f"""
    SELECT
        SRC_SYS_CD,
        BILL_INVC_ID,
        INVC_DSCRTN_SEQ_NO,
        INVC_DSCRTN_YR_MO_SK,
        GRP_ID,
        SUBGRP_ID,
        CLS_ID,
        CLS_PLN_ID,
        PROD_ID,
        DSCRTN_INCM_SK
    FROM
        {EDWOwner}.K_DSCRTN_INCM_F
    """
    df_db2_k_dscrtn_incm_f = (
        spark.read.format("jdbc")
        .option("url", edw_jdbc_url)
        .options(**edw_jdbc_props)
        .option("query", extract_query)
        .load()
    )

    # --------------------------------------------------
    # Stage: rdup_Natural_Keys  (Remove Duplicates)
    # --------------------------------------------------
    partition_cols = [
        "SRC_SYS_CD",
        "BILL_INVC_ID",
        "INVC_DSCRTN_SEQ_NO",
        "INVC_DSCRTN_YR_MO_SK",
        "GRP_ID",
        "SUBGRP_ID",
        "CLS_ID",
        "CLS_PLN_ID",
        "PROD_ID",
    ]
    sort_cols = [(c, "A") for c in partition_cols]

    df_rdup_natural_keys = dedup_sort(
        df_lnk_Transforms_Out,
        partition_cols,
        sort_cols,
    )

    # --------------------------------------------------
    # Stage: jn_DscrntIncmF  (Left Join with existing PKeys)
    # --------------------------------------------------
    join_expr = [
        df_rdup_natural_keys[c] == df_db2_k_dscrtn_incm_f[c] for c in partition_cols
    ]
    df_join = (
        df_rdup_natural_keys.alias("nk")
        .join(df_db2_k_dscrtn_incm_f.alias("k"), join_expr, "left")
        .select(
            *[col(f"nk.{c}").alias(c) for c in partition_cols],
            col("k.DSCRTN_INCM_SK"),
        )
    )

    # --------------------------------------------------
    # Stage: xfrm_PKEYgen  (Generate Surrogate Keys)
    # --------------------------------------------------
    df_enriched = df_join.withColumn(
        "orig_dscrtn_null", col("DSCRTN_INCM_SK").isNull()
    )
    df_enriched = SurrogateKeyGen(
        df_enriched, <DB sequence name>, "DSCRTN_INCM_SK", <schema>, <secret_name>
    )

    # Output link lnk_Pkey_out
    df_pkey_out = df_enriched.select(
        *partition_cols,
        col("DSCRTN_INCM_SK"),
    )

    # Output link lnk_KDscrntIncmF_out (rows with newly generated keys)
    df_insert = (
        df_enriched.filter(col("orig_dscrtn_null"))
        .select(
            *partition_cols,
            lit(EDWRunCycleDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
            col("DSCRTN_INCM_SK"),
            lit(IDSIncmBeginCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        )
    )

    if df_insert.rdd.isEmpty() is False:
        (
            df_insert.write.format("jdbc")
            .option("url", edw_jdbc_url)
            .options(**edw_jdbc_props)
            .option("dbtable", f"{EDWOwner}.K_DSCRTN_INCM_F")
            .mode("append")
            .save()
        )

    # --------------------------------------------------
    # Stage: jn_PKey  (Attach surrogate keys to incoming data)
    # --------------------------------------------------
    join_expr_pkey = [
        df_lnk_DscrntIncmData_L_in[c] == df_pkey_out[c] for c in partition_cols
    ]
    df_dscrnt_jn = (
        df_lnk_DscrntIncmData_L_in.alias("d")
        .join(df_pkey_out.alias("p"), join_expr_pkey, "left")
        .select(col("d.*"), col("p.DSCRTN_INCM_SK"))
    )

    # --------------------------------------------------
    # Stage: xfrm_BusinessRules
    # --------------------------------------------------
    df_duplicate = (
        df_dscrnt_jn.withColumn("CRT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_DT_SK", lit(EDWRunCycleDate))
        .withColumn("CRT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
        .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", lit(EDWRunCycle))
    )

    # UNK row
    df_unk = spark.range(1).select(
        lit(0).alias("DSCRTN_INCM_SK"),
        lit("UNK").alias("SRC_SYS_CD"),
        lit("UNK").alias("BILL_INVC_ID"),
        lit(0).alias("INVC_DSCRTN_SEQ_NO"),
        lit("175301").alias("INVC_DSCRTN_YR_MO_SK"),
        lit("UNK").alias("GRP_ID"),
        lit("UNK").alias("SUBGRP_ID"),
        lit("UNK").alias("CLS_ID"),
        lit("UNK").alias("CLS_PLN_ID"),
        lit("UNK").alias("PROD_ID"),
        lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit(0).alias("BILL_ENTY_SK"),
        lit(0).alias("CLS_SK"),
        lit(0).alias("CLS_PLN_SK"),
        lit(0).alias("FEE_DSCNT_SK"),
        lit(0).alias("GRP_SK"),
        lit(0).alias("PROD_SK"),
        lit(0).alias("SUBGRP_SK"),
        lit("N").alias("INVC_CUR_RCRD_IN"),
        lit("UNK").alias("INVC_DSCRTN_BILL_DISP_CD"),
        lit("UNK").alias("INVC_DSCRTN_PRM_FEE_CD"),
        lit("UNK").alias("INVC_TYP_CD"),
        lit("1753-01-01").alias("INVC_DSCRTN_BEG_DT_SK"),
        lit("1753-01-01").alias("INVC_DSCRTN_END_DT_SK"),
        lit("1753-01-01").alias("INVC_BILL_DUE_DT_SK"),
        lit("175301").alias("INVC_BILL_DUE_YR_MO_SK"),
        lit("1753-01-01").alias("INVC_BILL_END_DT_SK"),
        lit("175301").alias("INVC_BILL_END_YR_MO_SK"),
        lit("1753-01-01").alias("INVC_CRT_DT_SK"),
        lit("1753-01-01").alias("INVC_DSCRTN_DUE_DT_SK"),
        lit(0).alias("INVC_DSCRTN_DPNDT_PRM_AMT"),
        lit(0).alias("INVC_DSCRTN_FEE_DSCNT_AMT"),
        lit(0).alias("INVC_DSCRTN_SUB_PRM_AMT"),
        lit(0).alias("INVC_DSCRTN_MO_QTY"),
        lit("").alias("INVC_DSCRTN_DESC"),
        lit("").alias("DSCRTN_PRSN_ID_TX"),
        lit("").alias("INVC_DSCRTN_SH_DESC"),
        lit("UNK").alias("FEE_DSCNT_ID"),
        lit("UNK").alias("PROD_BILL_CMPNT_ID"),
        lit(0).alias("SUB_UNIQ_KEY"),
        lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("INVC_DSCRTN_BILL_DISP_CD_SK"),
        lit(0).alias("INVC_DSCRTN_PRM_FEE_CD_SK"),
        lit(0).alias("INVC_DSCRTN_SK"),
        lit(0).alias("INVC_TYP_CD_SK"),
        lit(0).alias("INVC_SK"),
        lit(0).alias("SUB_SK"),
        lit(0).alias("INVC_DSCRTN_DPNDT_CT"),
        lit(0).alias("INVC_DSCRTN_SUB_CT"),
        lit("N").alias("INVC_DSCRTN_SELF_BILL_LIFE_IN"),
    )

    # NA row
    df_na = spark.range(1).select(
        lit(1).alias("DSCRTN_INCM_SK"),
        lit("NA").alias("SRC_SYS_CD"),
        lit("NA").alias("BILL_INVC_ID"),
        lit(0).alias("INVC_DSCRTN_SEQ_NO"),
        lit("175301").alias("INVC_DSCRTN_YR_MO_SK"),
        lit("NA").alias("GRP_ID"),
        lit("NA").alias("SUBGRP_ID"),
        lit("NA").alias("CLS_ID"),
        lit("NA").alias("CLS_PLN_ID"),
        lit("NA").alias("PROD_ID"),
        lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        lit(1).alias("BILL_ENTY_SK"),
        lit(1).alias("CLS_SK"),
        lit(1).alias("CLS_PLN_SK"),
        lit(1).alias("FEE_DSCNT_SK"),
        lit(1).alias("GRP_SK"),
        lit(1).alias("PROD_SK"),
        lit(1).alias("SUBGRP_SK"),
        lit("N").alias("INVC_CUR_RCRD_IN"),
        lit("NA").alias("INVC_DSCRTN_BILL_DISP_CD"),
        lit("NA").alias("INVC_DSCRTN_PRM_FEE_CD"),
        lit("NA").alias("INVC_TYP_CD"),
        lit("1753-01-01").alias("INVC_DSCRTN_BEG_DT_SK"),
        lit("1753-01-01").alias("INVC_DSCRTN_END_DT_SK"),
        lit("1753-01-01").alias("INVC_BILL_DUE_DT_SK"),
        lit("175301").alias("INVC_BILL_DUE_YR_MO_SK"),
        lit("1753-01-01").alias("INVC_BILL_END_DT_SK"),
        lit("175301").alias("INVC_BILL_END_YR_MO_SK"),
        lit("1753-01-01").alias("INVC_CRT_DT_SK"),
        lit("1753-01-01").alias("INVC_DSCRTN_DUE_DT_SK"),
        lit(0).alias("INVC_DSCRTN_DPNDT_PRM_AMT"),
        lit(0).alias("INVC_DSCRTN_FEE_DSCNT_AMT"),
        lit(0).alias("INVC_DSCRTN_SUB_PRM_AMT"),
        lit(0).alias("INVC_DSCRTN_MO_QTY"),
        lit("").alias("INVC_DSCRTN_DESC"),
        lit("").alias("DSCRTN_PRSN_ID_TX"),
        lit("").alias("INVC_DSCRTN_SH_DESC"),
        lit("NA").alias("FEE_DSCNT_ID"),
        lit("NA").alias("PROD_BILL_CMPNT_ID"),
        lit(1).alias("SUB_UNIQ_KEY"),
        lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("INVC_DSCRTN_BILL_DISP_CD_SK"),
        lit(1).alias("INVC_DSCRTN_PRM_FEE_CD_SK"),
        lit(1).alias("INVC_DSCRTN_SK"),
        lit(1).alias("INVC_TYP_CD_SK"),
        lit(1).alias("INVC_SK"),
        lit(1).alias("SUB_SK"),
        lit(0).alias("INVC_DSCRTN_DPNDT_CT"),
        lit(0).alias("INVC_DSCRTN_SUB_CT"),
        lit("N").alias("INVC_DSCRTN_SELF_BILL_LIFE_IN"),
    )

    # --------------------------------------------------
    # Stage: fnl_FinalLoadData (Funnel / Union)
    # --------------------------------------------------
    common_cols = df_duplicate.columns
    df_out = (
        df_duplicate.select(common_cols)
        .unionByName(df_unk.select(common_cols))
        .unionByName(df_na.select(common_cols))
    )

    # --------------------------------------------------
    # Container output
    # --------------------------------------------------
    return df_out