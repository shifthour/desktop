# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
def run_IndvBeLtrPkey(df_Transform: DataFrame, params: dict) -> DataFrame:
    IDSOwner = params["IDSOwner"]
    ids_secret_name = params["ids_secret_name"]
    CurrRunCycle = params["CurrRunCycle"]
    ids_jdbc_url = params["ids_jdbc_url"]
    ids_jdbc_props = params["ids_jdbc_props"]
    extract_query = """
    SELECT 
      SRC_SYS_CD  AS SRC_SYS_CD_lkp,
      INDV_BE_KEY AS INDV_BE_KEY_lkp,
      LTR_ID      AS LTR_ID_lkp,
      LTR_SENT_DT_SK AS LTR_SENT_DT_SK_lkp,
      CRT_RUN_CYC_EXCTN_SK AS CRT_RUN_CYC_EXCTN_SK_lkp,
      INDV_BE_LTR_SK AS INDV_BE_LTR_SK_lkp
    FROM dummy_hf_indv_be_ltr
    """
    df_hf_indv_be_ltr_lkup = (
        spark.read.format("jdbc")
            .option("url", ids_jdbc_url)
            .options(**ids_jdbc_props)
            .option("query", extract_query)
            .load()
    )
    join_cond = (
        (F.col("t.SRC_SYS_CD") == F.col("lkup.SRC_SYS_CD_lkp")) &
        (F.col("t.MBR_ID") == F.col("lkup.INDV_BE_KEY_lkp")) &
        (F.col("t.LTR_CD") == F.col("lkup.LTR_ID_lkp")) &
        (F.col("t.LTR_SENT_DT") == F.col("lkup.LTR_SENT_DT_SK_lkp"))
    )
    df_join = df_Transform.alias("t").join(df_hf_indv_be_ltr_lkup.alias("lkup"), join_cond, "left")
    df_enriched = df_join.select(
        F.col("t.JOB_EXCTN_RCRD_ERR_SK"),
        F.col("t.INSRT_UPDT_CD"),
        F.col("t.DISCARD_IN"),
        F.col("t.PASS_THRU_IN"),
        F.col("t.FIRST_RECYC_DT"),
        F.col("t.ERR_CT"),
        F.col("t.RECYCLE_CT"),
        F.col("t.SRC_SYS_CD"),
        F.col("t.PRI_KEY_STRING"),
        F.col("lkup.INDV_BE_LTR_SK_lkp").alias("INDV_BE_LTR_SK"),
        F.when(F.col("lkup.INDV_BE_LTR_SK_lkp").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp")).alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("t.MBR_ID").alias("INDV_BE_KEY"),
        F.col("t.LTR_CD").alias("LTR_ID"),
        F.col("t.LTR_SENT_DT").alias("LTR_SENT_DT_SK"),
        F.col("t.TMPLT_LTR_ID"),
        F.col("t.ENR_PGM_CD").alias("POP_HLTH_PGM_ID"),
        F.col("lkup.INDV_BE_LTR_SK_lkp"),
        F.col("lkup.CRT_RUN_CYC_EXCTN_SK_lkp"),
        F.col("lkup.SRC_SYS_CD_lkp"),
        F.col("lkup.INDV_BE_KEY_lkp"),
        F.col("lkup.LTR_ID_lkp"),
        F.col("lkup.LTR_SENT_DT_SK_lkp")
    )
    df_enriched = SurrogateKeyGen(df_enriched,<DB sequence name>,"INDV_BE_LTR_SK",<schema>,<secret_name>)
    df_updt = df_enriched.filter(F.col("INDV_BE_LTR_SK_lkp").isNull()).select(
        F.col("SRC_SYS_CD"),
        F.col("INDV_BE_KEY"),
        F.col("LTR_ID"),
        F.col("LTR_SENT_DT_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("INDV_BE_LTR_SK")
    )
    (
        df_updt.write.format("jdbc")
        .option("url", ids_jdbc_url)
        .options(**ids_jdbc_props)
        .option("dbtable", "dummy_hf_indv_be_ltr")
        .mode("append")
        .save()
    )
    df_key = df_enriched.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "INDV_BE_LTR_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "INDV_BE_KEY",
        "LTR_ID",
        "LTR_SENT_DT_SK",
        "TMPLT_LTR_ID",
        "POP_HLTH_PGM_ID"
    )
    return df_key