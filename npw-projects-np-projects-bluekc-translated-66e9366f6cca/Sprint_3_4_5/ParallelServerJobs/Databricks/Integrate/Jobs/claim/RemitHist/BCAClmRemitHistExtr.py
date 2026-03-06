# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2013 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: BCADrugExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:     Reads the BCADrugClm_Land.dat file and puts the data into the claim remit history common record format and runs through primary key using shared container ClmRemiHistPkey
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                         Date                 Project/Altiris #      Change Description                                             Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------                      --------------------     ------------------------      -----------------------------------------------------------------------      -------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam                2013-10-29       5056 FEP Claims     Initial Programming                                               IntegrateNewDevl         Bhoomi Dasari           11/30/2013
# MAGIC Saikiran Mahenderker     2018-03-15       5781 HEDIS           Modified  existing columns PDX_NABP_NO         IntegrateDev2	  Jaideep Mankala        04/04/2018
# MAGIC                                                                                                 PRSCRB_PROV to PDX_NTNL_PROV_ID,       
# MAGIC                                                                                                 PRSCRB_PROV_NTNL_PROV_ID 
# MAGIC                                                                                                 and added  four new source columns                  
# MAGIC Karthik Chintalapani         2019-08-27        5884       Added new columns to BCAClmLand  file stage                       IntegrateDev1	            Kalyan Neelam                2019-09-05         
# MAGIC                                                                                  CLM_STTUS_CD,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID    
# MAGIC                                                                                  for implementing reversals logic

# MAGIC Assign primary surrogate key
# MAGIC Apply business logic
# MAGIC Writing Sequential File to /key
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
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Read BCA file from Landing job
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmRemitHistPK
# COMMAND ----------

CurrentDate = get_widget_value('CurrentDate','')
RunCycle = get_widget_value('RunCycle','1')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

schema_BCADrugClm_Land = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("ALLOCD_CRS_PLN_CD", StringType(), True),
    StructField("ALLOCD_SHIELD_PLN_CD", StringType(), True),
    StructField("PDX_SVC_ID", DecimalType(38,10), True),
    StructField("CLM_LN_NO", DecimalType(38,10), True),
    StructField("RX_FILLED_DT", StringType(), True),
    StructField("CONSIS_MBR_ID", DecimalType(38,10), True),
    StructField("FEP_CNTR_ID", StringType(), True),
    StructField("LGCY_MBR_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("PATN_AGE", DecimalType(38,10), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("PDX_NTNL_PROV_ID", StringType(), True),
    StructField("LGCY_SRC_CD", StringType(), True),
    StructField("PRSCRB_PROV_NTNL_PROV_ID", StringType(), True),
    StructField("PRSCRB_NTWK_CD", StringType(), True),
    StructField("PRSCRB_PROV_PLN_CD", StringType(), True),
    StructField("NDC", StringType(), True),
    StructField("BRND_NM", StringType(), True),
    StructField("LABEL_NM", StringType(), True),
    StructField("THRPTC_CAT_DESC", StringType(), True),
    StructField("GNRC_NM_DRUG_IN", StringType(), True),
    StructField("METH_DRUG_ADM", StringType(), True),
    StructField("RX_CST_EQVLNT", DecimalType(38,10), True),
    StructField("METRIC_UNIT", DecimalType(38,10), True),
    StructField("NON_METRIC_UNIT", DecimalType(38,10), True),
    StructField("DAYS_SUPPLIED", DecimalType(38,10), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("CLM_LOAD_DT", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("DISPNS_DRUG_TYP", StringType(), True),
    StructField("DISPNS_AS_WRTN_STTUS_CD", StringType(), True),
    StructField("ADJ_CD", StringType(), True),
    StructField("PD_DT", StringType(), True)
])

df_BCADrugClm_Land = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_BCADrugClm_Land)
    .csv(f"{adls_path}/verified/BCADrugClm_Land.dat.{RunID}")
)

df_BusinessRules = df_BCADrugClm_Land.select(
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    F.lit(0).alias("ERR_CT"),
    F.lit(0).alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat(F.lit(SrcSysCd), F.lit(";"), F.col("CLM_ID")).alias("PRI_KEY_STRING"),
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
        (F.col("RX_CST_EQVLNT").isNull()) |
        (F.length(trim(F.col("RX_CST_EQVLNT"))) == 0),
        F.lit(0.00)
    ).otherwise(F.col("RX_CST_EQVLNT")).alias("CNSD_CHRG_AMT"),
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

df_Snapshot_Pkey = df_BusinessRules.select(
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

df_Snapshot_RowCount = df_BusinessRules.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

write_files(
    df_Snapshot_RowCount.select("SRC_SYS_CD_SK","CLM_ID"),
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
df_ClmRemitHistPK = ClmRemitHistPK(df_Snapshot_Pkey, params_ClmRemitHistPK)

df_final_ClmRemitHistPK = df_ClmRemitHistPK.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_REMIT_HIST_SK"),
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("CLM_SK"),
    F.rpad(F.col("CALC_ACTL_PD_AMT_IN"), 1, " ").alias("CALC_ACTL_PD_AMT_IN"),
    F.rpad(F.col("SUPRESS_EOB_IN"), 1, " ").alias("SUPRESS_EOB_IN"),
    F.rpad(F.col("SUPRESS_REMIT_IN"), 1, " ").alias("SUPRESS_REMIT_IN"),
    F.col("ACTL_PD_AMT"),
    F.col("COB_PD_AMT"),
    F.col("COINS_AMT"),
    F.col("CNSD_CHRG_AMT"),
    F.col("COPAY_AMT"),
    F.col("DEDCT_AMT"),
    F.col("DSALW_AMT"),
    F.col("ER_COPAY_AMT"),
    F.col("INTRST_AMT"),
    F.col("NO_RESP_AMT"),
    F.col("PATN_RESP_AMT"),
    F.col("WRTOFF_AMT"),
    F.col("PCA_PD_AMT"),
    F.rpad(F.col("ALT_CHRG_IN"), 1, " ").alias("ALT_CHRG_IN"),
    F.col("ALT_CHRG_PROV_WRTOFF_AMT")
)

write_files(
    df_final_ClmRemitHistPK,
    f"{adls_path}/key/BCAClmRemitHistExtr.DrugClmRemitHist.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)