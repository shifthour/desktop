# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def run_ClmRemitHistPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]
    df_lkup = spark.read.parquet(f"{adls_path}/ClmRemitHistPK_lkup.parquet").alias("l")
    t = df_Transform.alias("t")
    df_join = t.join(
        df_lkup,
        (F.col("t.SRC_SYS_CD") == F.col("l.src_sys_cd")) & (F.col("t.CLM_ID") == F.col("l.clm_id")),
        "left"
    )
    df_enriched = (
        df_join
        .withColumn("SK", F.when(F.col("l.clm_sk").isNull(), F.lit(0)).otherwise(F.col("l.clm_sk")))
        .withColumn(
            "NewCrtRunCycExctnSk",
            F.when(F.col("l.clm_id").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("l.crt_run_cyc_exctn_sk"))
        )
    )
    df_key = df_enriched.select(
        F.col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.col("t.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        F.col("t.DISCARD_IN").alias("DISCARD_IN"),
        F.col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
        F.col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("t.ERR_CT").alias("ERR_CT"),
        F.col("t.RECYCLE_CT").alias("RECYCLE_CT"),
        F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("SK").alias("CLM_REMIT_HIST_SK"),
        F.col("t.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.col("t.CLM_ID").alias("CLM_ID"),
        F.col("NewCrtRunCycExctnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("t.CLM_SK").alias("CLM_SK"),
        F.col("t.CALC_ACTL_PD_AMT_IN").alias("CALC_ACTL_PD_AMT_IN"),
        F.col("t.SUPRESS_EOB_IN").alias("SUPRESS_EOB_IN"),
        F.col("t.SUPRESS_REMIT_IN").alias("SUPRESS_REMIT_IN"),
        F.col("t.ACTL_PD_AMT").alias("ACTL_PD_AMT"),
        F.col("t.COB_PD_AMT").alias("COB_PD_AMT"),
        F.col("t.COINS_AMT").alias("COINS_AMT"),
        F.col("t.CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
        F.col("t.COPAY_AMT").alias("COPAY_AMT"),
        F.col("t.DEDCT_AMT").alias("DEDCT_AMT"),
        F.col("t.DSALW_AMT").alias("DSALW_AMT"),
        F.col("t.ER_COPAY_AMT").alias("ER_COPAY_AMT"),
        F.col("t.INTRST_AMT").alias("INTRST_AMT"),
        F.col("t.NO_RESP_AMT").alias("NO_RESP_AMT"),
        F.col("t.PATN_RESP_AMT").alias("PATN_RESP_AMT"),
        F.col("t.WRTOFF_AMT").alias("WRTOFF_AMT"),
        F.col("t.PCA_PD_AMT").alias("PCA_PD_AMT"),
        F.col("t.ALT_CHRG_IN").alias("ALT_CHRG_IN"),
        F.col("t.ALT_CHRG_PROV_WRTOFF_AMT").alias("ALT_CHRG_PROV_WRTOFF_AMT")
    )
    return df_key