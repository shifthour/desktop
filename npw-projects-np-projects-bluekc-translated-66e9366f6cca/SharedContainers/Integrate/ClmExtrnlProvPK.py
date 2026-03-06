# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def run_ClmExtrnlProvPK(df_Transform: DataFrame, params: dict) -> DataFrame:
    CurrRunCycle = params["CurrRunCycle"]
    adls_path = params["adls_path"]

    lkup_path = f"{adls_path}/ClmExtrnlProvPK_lkup.parquet"
    df_lkup = spark.read.parquet(lkup_path)

    df_joined = (
        df_Transform.alias("t")
        .join(
            df_lkup.alias("lk"),
            (F.col("t.SRC_SYS_CD") == F.col("lk.SRC_SYS_CD")) & (F.col("t.CLM_ID") == F.col("lk.CLM_ID")),
            "left"
        )
    )

    df_enriched = (
        df_joined
        .withColumn("SK", F.when(F.col("lk.CLM_SK").isNull(), F.lit(0)).otherwise(F.col("lk.CLM_SK")))
        .withColumn(
            "NewCrtRunCycExtcnSk",
            F.when(F.col("lk.CLM_SK").isNull(), F.lit(CurrRunCycle)).otherwise(F.col("lk.CRT_RUN_CYC_EXCTN_SK"))
        )
    )

    df_Key = (
        df_enriched.select(
            F.col("t.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
            F.col("t.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
            F.col("t.DISCARD_IN").alias("DISCARD_IN"),
            F.col("t.PASS_THRU_IN").alias("PASS_THRU_IN"),
            F.col("t.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
            F.col("t.ERR_CT").alias("ERR_CT"),
            F.col("t.RECYCLE_CT").alias("RECYCLE_CT"),
            F.col("t.SRC_SYS_CD").alias("SRC_SYS_CD"),
            F.col("t.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
            F.col("SK").alias("CLM_EXTRNL_PROV_SK"),
            F.col("t.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
            F.col("t.CLM_ID").alias("CLM_ID"),
            F.col("NewCrtRunCycExtcnSk").alias("CRT_RUN_CYC_EXCTN_SK"),
            F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
            F.col("t.CLM_SK").alias("CLM_SK"),
            F.col("t.PROV_NM").alias("PROV_NM"),
            F.col("t.ADDR_LN_1").alias("ADDR_LN_1"),
            F.col("t.ADDR_LN_2").alias("ADDR_LN_2"),
            F.col("t.ADDR_LN_3").alias("ADDR_LN_3"),
            F.col("t.CITY_NM").alias("CITY_NM"),
            F.col("t.CLPP_PR_STATE").alias("CLPP_PR_STATE"),
            F.col("t.CLM_EXTRNL_PROV_ST_CD_SK").alias("CLM_EXTRNL_PROV_ST_CD_SK"),
            F.col("t.POSTAL_CD").alias("POSTAL_CD"),
            F.col("t.CNTY_NM").alias("CNTY_NM"),
            F.col("t.CLPP_CNTRY_CD").alias("CLPP_CNTRY_CD"),
            F.col("t.CLM_EXTRNL_PROV_CTRY_CD_SK").alias("CLM_EXTRNL_PROV_CTRY_CD_SK"),
            F.col("t.PHN_NO").alias("PHN_NO"),
            F.col("t.PROV_ID").alias("PROV_ID"),
            F.col("t.PROV_NPI").alias("PROV_NPI"),
            F.col("t.SVC_PROV_ID").alias("SVC_PROV_ID"),
            F.col("t.SVC_PROV_NPI").alias("SVC_PROV_NPI"),
            F.col("t.SVC_PROV_NM").alias("SVC_PROV_NM"),
            F.col("t.TAX_ID").alias("TAX_ID")
        )
    )

    return df_Key