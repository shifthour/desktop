# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: BCBSADrugExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:     Reads the BCBSADrugClm_Land.dat file and puts the data into the claim remit history common record format and runs through primary key using shared container ClmRemiHistPkey
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                             Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------      -------------------------------       -------------------------------   ----------------------------       
# MAGIC KChintalapani        2016-02-25           5212 PI                        Initial Programming                                               IntegrateDev2         Kalyan Neelam          2016-05-11

# MAGIC Read BCA file from Landing job
# MAGIC This container is used in:
# MAGIC PcsClmRemitHistExtr
# MAGIC BCBSClmRemitHistExtr
# MAGIC NascoClmRemitHistTrns
# MAGIC ESIClmRemitHistExtr
# MAGIC MCSourceClmRemitHistExtr
# MAGIC MedicaidClmRemitHistExtr
# MAGIC WellDyneClmRemitHistExtr
# MAGIC MedtrakClmRemitHistExtr
# MAGIC BCBSSCClmRemitHistExtr
# MAGIC BCBSSCMedClmRemitHistExtr
# MAGIC BCAClmRemitHistExtr
# MAGIC BCBSAClmRemitHistExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Writing Sequential File to /key
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmRemitHistPK
# COMMAND ----------

CurrentDate = get_widget_value("CurrentDate","2016-03-17")
RunCycle = get_widget_value("RunCycle","100")
RunID = get_widget_value("RunID","100")
SrcSysCd = get_widget_value("SrcSysCd","BCBSA")
SrcSysCdSk = get_widget_value("SrcSysCdSk","-1951781674")

schema_BCBSARxClmLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("NDW_HOME_PLN_ID", StringType(), True),
    StructField("CLM_LN_NO", StringType(), True),
    StructField("TRACEABILITY_FLD", StringType(), True),
    StructField("ADJ_SEQ_NO", StringType(), True),
    StructField("HOST_PLN_ID", StringType(), True),
    StructField("HOME_PLAN_PROD_ID_CD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("MBR_ZIP_CD_ON_CLM", StringType(), True),
    StructField("MBR_CTRY_ON_CLM", StringType(), True),
    StructField("NPI_REND_PROV_ID", StringType(), True),
    StructField("PRSCRB_PROV_ID", StringType(), True),
    StructField("NPI_PRSCRB_PROV_ID", StringType(), True),
    StructField("BNF_PAYMT_STTUS_CD", StringType(), True),
    StructField("PDX_CARVE_OUT_SUBMSN_IN", StringType(), True),
    StructField("CAT_OF_SVC", StringType(), True),
    StructField("CLM_PAYMT_STTUS", StringType(), True),
    StructField("DAW_CD", StringType(), True),
    StructField("DAYS_SUPL", StringType(), True),
    StructField("FRMLRY_IN", StringType(), True),
    StructField("PLN_SPEC_DRUG_IN", StringType(), True),
    StructField("PROD_SVC_ID", StringType(), True),
    StructField("QTY_DISPNS", StringType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("DT_OF_SVC", StringType(), True),
    StructField("AVG_WHLSL_PRICE_SUBMT_AMT", StringType(), True),
    StructField("CONSIS_MBR_ID", StringType(), True),
    StructField("MMI_ID", StringType(), True),
    StructField("SUBGRP_SK", IntegerType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("SUB_SK", IntegerType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("MBR_GNDR_CD_SK", IntegerType(), True),
    StructField("MBR_GNDR_CD", StringType(), True),
    StructField("BRTH_DT_SK", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True)
])

df_BCBSARxClmLanding = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_BCBSARxClmLanding)
    .load(f"{adls_path}/verified/BCBSADrugClm_Land.dat.{RunID}")
)

df_BusinessRules = df_BCBSARxClmLanding.withColumn(
    "PkString",
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID"))
).select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.col("PkString").alias("PRI_KEY_STRING"),
    F.lit(0).alias("CLM_REMIT_HIST_SK"),
    F.lit(0).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit(0).alias("CLM_SK"),
    F.lit("N").alias("CALC_ACTL_PD_AMT_IN"),
    F.lit("N").alias("SUPRESS_EOB_IN"),
    F.lit("N").alias("SUPRESS_REMIT_IN"),
    F.lit(0.00).alias("ACTL_PD_AMT"),
    F.lit(0.00).alias("COB_PD_AMT"),
    F.lit(0.00).alias("COINS_AMT"),
    F.when(
        F.col("AVG_WHLSL_PRICE_SUBMT_AMT").isNull() |
        (F.length(trim(F.col("AVG_WHLSL_PRICE_SUBmt_AMT"))) == 0),
        F.lit(0.00)
    ).otherwise(
        F.col("AVG_WHLSL_PRICE_SUBMT_AMT").cast("double")
    ).alias("CNSD_CHRG_AMT"),
    F.lit(0.00).alias("COPAY_AMT"),
    F.lit(0.00).alias("DEDCT_AMT"),
    F.lit(0.00).alias("DSALW_AMT"),
    F.lit(0.00).alias("ER_COPAY_AMT"),
    F.lit(0.00).alias("INTRST_AMT"),
    F.lit(0.00).alias("NO_RESP_AMT"),
    F.lit(0.00).alias("PATN_RESP_AMT"),
    F.lit(0.00).alias("WRTOFF_AMT"),
    F.lit(0.00).alias("PCA_PD_AMT"),
    F.lit("N").alias("ALT_CHRG_IN"),
    F.lit(0.00).alias("ALT_CHRG_PROV_WRTOFF_AMT")
)

df_Pkey = df_BusinessRules.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_REMIT_HIST_SK").alias("CLM_REMIT_HIST_SK"),
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK").alias("CLM_SK"),
    F.col("CALC_ACTL_PD_AMT_IN").alias("CALC_ACTL_PD_AMT_IN"),
    F.col("SUPRESS_EOB_IN").alias("SUPRESS_EOB_IN"),
    F.col("SUPRESS_REMIT_IN").alias("SUPRESS_REMIT_IN"),
    F.col("ACTL_PD_AMT").alias("ACTL_PD_AMT"),
    F.col("COB_PD_AMT").alias("COB_PD_AMT"),
    F.col("COINS_AMT").alias("COINS_AMT"),
    F.col("CNSD_CHRG_AMT").alias("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT").alias("COPAY_AMT"),
    F.col("DEDCT_AMT").alias("DEDCT_AMT"),
    F.col("DSALW_AMT").alias("DSALW_AMT"),
    F.col("ER_COPAY_AMT").alias("ER_COPAY_AMT"),
    F.col("INTRST_AMT").alias("INTRST_AMT"),
    F.col("NO_RESP_AMT").alias("NO_RESP_AMT"),
    F.col("PATN_RESP_AMT").alias("PATN_RESP_AMT"),
    F.col("WRTOFF_AMT").alias("WRTOFF_AMT"),
    F.col("PCA_PD_AMT").alias("PCA_PD_AMT"),
    F.col("ALT_CHRG_IN").alias("ALT_CHRG_IN"),
    F.col("ALT_CHRG_PROV_WRTOFF_AMT").alias("ALT_CHRG_PROV_WRTOFF_AMT")
)

df_RowCount = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

write_files(
    df_RowCount.select("SRC_SYS_CD_SK","CLM_ID"),
    f"{adls_path}/load/B_CLM_REMIT_HIST.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

params_ClmRemitHistPK = {
    "CurrRunCycle": RunCycle
}
df_key = ClmRemitHistPK(df_Pkey, params_ClmRemitHistPK)

df_output = (
    df_key
    .withColumn("INSRT_UPDT_CD", F.rpad(F.col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", F.rpad(F.col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", F.rpad(F.col("PASS_THRU_IN"), 1, " "))
    .withColumn("CALC_ACTL_PD_AMT_IN", F.rpad(F.col("CALC_ACTL_PD_AMT_IN"), 1, " "))
    .withColumn("SUPRESS_EOB_IN", F.rpad(F.col("SUPRESS_EOB_IN"), 1, " "))
    .withColumn("SUPRESS_REMIT_IN", F.rpad(F.col("SUPRESS_REMIT_IN"), 1, " "))
    .withColumn("ALT_CHRG_IN", F.rpad(F.col("ALT_CHRG_IN"), 1, " "))
    .select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "CLM_REMIT_HIST_SK",
        "SRC_SYS_CD_SK",
        "CLM_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLM_SK",
        "CALC_ACTL_PD_AMT_IN",
        "SUPRESS_EOB_IN",
        "SUPRESS_REMIT_IN",
        "ACTL_PD_AMT",
        "COB_PD_AMT",
        "COINS_AMT",
        "CNSD_CHRG_AMT",
        "COPAY_AMT",
        "DEDCT_AMT",
        "DSALW_AMT",
        "ER_COPAY_AMT",
        "INTRST_AMT",
        "NO_RESP_AMT",
        "PATN_RESP_AMT",
        "WRTOFF_AMT",
        "PCA_PD_AMT",
        "ALT_CHRG_IN",
        "ALT_CHRG_PROV_WRTOFF_AMT"
    )
)

write_files(
    df_output,
    f"{adls_path}/key/BCBSAClmRemitHistExtr.DrugClmRemitHist.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)