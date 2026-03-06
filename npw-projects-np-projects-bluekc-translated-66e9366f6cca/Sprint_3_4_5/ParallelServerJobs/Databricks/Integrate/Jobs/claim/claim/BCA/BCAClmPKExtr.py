# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC Copyright 2013 Blue Cross/Blue Shield of Kansas City
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCADrugLandSeq
# MAGIC                
# MAGIC 
# MAGIC PROCESSING:   
# MAGIC          *  Assigns / creates claim primary key surrogate keys for all records processed in current run.  Adjusted to/from claims don't need to be in hash file. 
# MAGIC          *  The primary key hash file hf_clm is the output of this job and is used by the following tables for keying
# MAGIC              CLM
# MAGIC              DRUG_CLM
# MAGIC              CLM_REMIT_HIST
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                           Date                 \(9)Project/Altiris #\(9)Change Description\(9)\(9)\(9)\(9)   Development Project\(9)Code Reviewer\(9)Date Reviewed       
# MAGIC ------------------                      --------------------     \(9)------------------------\(9)-----------------------------------------------------------------------\(9)    --------------------------------\(9)-------------------------------\(9)----------------------------       
# MAGIC Kalyan Neelam                2013-10-29                     5056 FEP Claims        Initial Programming                                                      IntegrateNewDevl               Bhoomi Dasari         11/30/2013
# MAGIC 
# MAGIC Saikiran Mahenderker     2018-03-15                     5781 HEDIS            Added 4 columns in 'BCAClmLand' stage                      IntegrateDev2                     Jaideep Mankala      04/04/2018
# MAGIC                                                                                               
# MAGIC 
# MAGIC Karthik Chintalapani         2019-08-27        5884       Added new columns to BCAClmLand  file stage                                            IntegrateDev1\(9)                      Kalyan Neelam             2019-09-05    
# MAGIC                                                                                  CLM_STTUS_CD,ADJ_FROM_CLM_ID,ADJ_TO_CLM_ID    
# MAGIC                                                                                  for implementing reversals logic

# MAGIC This container is used in:
# MAGIC ESIClmInvoicePKExtr
# MAGIC ESIClmPKExtr
# MAGIC FctsClmPKExtr
# MAGIC FctsPcaClmPKExtr
# MAGIC MCSourceClmPKExtr
# MAGIC MedicaidClmPKExtr
# MAGIC NascoClmExtr
# MAGIC NascoClmPKExtr
# MAGIC PcsClmPKExtr
# MAGIC WellDyneClmPKExtr
# MAGIC MedtrakClmPKExtr
# MAGIC BCBSSCClmPKExtr
# MAGIC 
# MAGIC BCBSSCMedClmPKExtr
# MAGIC BCAClmPKExtr
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Get SK for records with out keys
# MAGIC Output required by container but not used here
# MAGIC This job loads primary key hash file with all keys used by tables with natural keys of source system code and claim ID
# MAGIC BCA Claim Primary Key Process
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
from pyspark.sql import functions as F
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


SrcSysCd = get_widget_value("SrcSysCd", "")
SrcSysCdSK = get_widget_value("SrcSysCdSK", "")
CurrRunCycle = get_widget_value("CurrRunCycle", "100")
RunID = get_widget_value("RunID", "")
IDSOwner = get_widget_value("$IDSOwner", "")
ids_secret_name = get_widget_value("ids_secret_name", "")

schema_BCAClmLand = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), False),
    StructField("ADJ_FROM_CLM_ID", StringType(), False),
    StructField("ADJ_TO_CLM_ID", StringType(), False),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("ALLOCD_CRS_PLN_CD", StringType(), True),
    StructField("ALLOCD_SHIELD_PLN_CD", StringType(), True),
    StructField("PDX_SVC_ID", DecimalType(), True),
    StructField("CLM_LN_NO", DecimalType(), True),
    StructField("RX_FILLED_DT", StringType(), True),
    StructField("CONSIS_MBR_ID", DecimalType(), True),
    StructField("FEP_CNTR_ID", StringType(), True),
    StructField("LGCY_MBR_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("PATN_AGE", DecimalType(), True),
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
    StructField("RX_CST_EQVLNT", DecimalType(), True),
    StructField("METRIC_UNIT", DecimalType(), True),
    StructField("NON_METRIC_UNIT", DecimalType(), True),
    StructField("DAYS_SUPPLIED", DecimalType(), True),
    StructField("CLM_PD_DT", StringType(), True),
    StructField("CLM_LOAD_DT", StringType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("DISPNS_DRUG_TYP", StringType(), True),
    StructField("DISPNS_AS_WRTN_STTUS_CD", StringType(), True),
    StructField("ADJ_CD", StringType(), True),
    StructField("PD_DT", StringType(), True)
])

df_BCAClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(schema_BCAClmLand)
    .load(f"{adls_path}/verified/BCADrugClm_Land.dat.{RunID}")
)

df_Trans2 = df_BCAClmLand.select(
    F.lit(SrcSysCdSK).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID")
)

# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLoadPK
# COMMAND ----------

params_ClmLoadPK = {
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSK": SrcSysCdSK,
    "CurrRunCycle": CurrRunCycle,
    "$IDSOwner": IDSOwner
}
df_ClmLoadPK = ClmLoadPK(df_Trans2, params_ClmLoadPK)

df_hf_clm_pk_lkup = df_ClmLoadPK.select(
    "SRC_SYS_CD",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "CLM_SK"
)

write_files(
    df_hf_clm_pk_lkup,
    f"{adls_path}/hf_clm_pk_lkup.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)