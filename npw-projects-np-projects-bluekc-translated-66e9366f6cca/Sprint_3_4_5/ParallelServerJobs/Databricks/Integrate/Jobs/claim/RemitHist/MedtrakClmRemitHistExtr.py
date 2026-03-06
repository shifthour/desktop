# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2010 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC CALLED BY: MedtrakDrugExtrSeq
# MAGIC 
# MAGIC DESCRIPTION:     Reads the MedtrakDrugClm_Land.dat file and puts the data into the claim remit history common record format and runs through primary key using shared container ClmRemiHistPkey
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                Date                 Project/Altiris #      Change Description                                             Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------              --------------------     ------------------------      -----------------------------------------------------------------------      -------------------------------       -------------------------------   ----------------------------       
# MAGIC Kalyan Neelam         2010-12-22       4616                      Initial Programming                                                IntegrateNewDevl        Steph Goddard          12/23/2010
# MAGIC Raja Gummadi          2012-07-23         TTR 1330         Changed RX_NO field size from 9 to 20 in input file  IntegrateWrhsDevl      Bhoomi Dasari           08/08/2012

# MAGIC Read Medtrak file from Landing job
# MAGIC This container is used in:
# MAGIC PcsClmRemitHistExtr
# MAGIC BCBSClmRemitHistExtr
# MAGIC NascoClmRemitHistTrns
# MAGIC ESIClmRemitHistExtr
# MAGIC MCSourceClmRemitHistExtr
# MAGIC MedicaidClmRemitHistExtr
# MAGIC WellDyneClmRemitHistExtr
# MAGIC MedtrakClmRemitHistExtr
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
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType
)
from pyspark.sql.functions import (
    col,
    lit,
    concat,
    when,
    expr,
    monotonically_increasing_id,
    lpad,
    rpad
)
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


CurrentDate = get_widget_value('CurrentDate','')
RunCycle = get_widget_value('RunCycle','1')
RunID = get_widget_value('RunID','')
SrcSysCd = get_widget_value('SrcSysCd','MEDTRAK')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')

# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmRemitHistPK
# COMMAND ----------

schema_MedtrakClmLand = StructType([
    StructField("RCRD_ID", DoubleType(), True),
    StructField("CLAIM_ID", StringType(), True),
    StructField("PRCSR_NO", DoubleType(), True),
    StructField("MEM_CK_KEY", IntegerType(), True),
    StructField("BTCH_NO", DoubleType(), True),
    StructField("PDX_NO", StringType(), True),
    StructField("RX_NO", DoubleType(), True),
    StructField("DT_FILLED", StringType(), True),
    StructField("NDC_NO", DoubleType(), True),
    StructField("DRUG_DESC", StringType(), True),
    StructField("NEW_RFL_CD", DoubleType(), True),
    StructField("METRIC_QTY", DoubleType(), True),
    StructField("DAYS_SUPL", DoubleType(), True),
    StructField("BSS_OF_CST_DTRM", StringType(), True),
    StructField("INGR_CST", DoubleType(), True),
    StructField("DISPNS_FEE", DoubleType(), True),
    StructField("COPAY_AMT", DoubleType(), True),
    StructField("SLS_TAX", DoubleType(), True),
    StructField("AMT_BILL", DoubleType(), True),
    StructField("PATN_FIRST_NM", StringType(), True),
    StructField("PATN_LAST_NM", StringType(), True),
    StructField("DOB", DoubleType(), True),
    StructField("SEX_CD", DoubleType(), True),
    StructField("CARDHLDR_ID_NO", StringType(), True),
    StructField("RELSHP_CD", DoubleType(), True),
    StructField("GRP_NO", StringType(), True),
    StructField("HOME_PLN", StringType(), True),
    StructField("HOST_PLN", DoubleType(), True),
    StructField("PRESCRIBER_ID", StringType(), True),
    StructField("DIAG_CD", StringType(), True),
    StructField("CARDHLDR_FIRST_NM", StringType(), True),
    StructField("CARDHLDR_LAST_NM", StringType(), True),
    StructField("PRAUTH_NO", DoubleType(), True),
    StructField("PA_MC_SC_NO", StringType(), True),
    StructField("CUST_LOC", DoubleType(), True),
    StructField("RESUB_CYC_CT", DoubleType(), True),
    StructField("DT_RX_WRTN", DoubleType(), True),
    StructField("DISPENSE_AS_WRTN_PROD_SEL_CD", StringType(), True),
    StructField("PRSN_CD", StringType(), True),
    StructField("OTHR_COV_CD", DoubleType(), True),
    StructField("ELIG_CLRFCTN_CD", DoubleType(), True),
    StructField("CMPND_CD", DoubleType(), True),
    StructField("NO_OF_RFLS_AUTH", DoubleType(), True),
    StructField("LVL_OF_SVC", DoubleType(), True),
    StructField("RX_ORIG_CD", DoubleType(), True),
    StructField("RX_DENIAL_CLRFCTN", DoubleType(), True),
    StructField("PRI_PRESCRIBER", StringType(), True),
    StructField("CLNC_ID_NO", DoubleType(), True),
    StructField("DRUG_TYP", DoubleType(), True),
    StructField("PRESCRIBER_LAST_NM", StringType(), True),
    StructField("POSTAGE_AMT_CLMED", DoubleType(), True),
    StructField("UNIT_DOSE_IN", DoubleType(), True),
    StructField("OTHR_PAYOR_AMT", DoubleType(), True),
    StructField("BSS_OF_DAYS_SUPL_DTRM", DoubleType(), True),
    StructField("FULL_AWP", DoubleType(), True),
    StructField("EXPNSN_AREA", StringType(), True),
    StructField("MSTR_CAR", StringType(), True),
    StructField("SUB_CAR", StringType(), True),
    StructField("CLM_TYP", StringType(), True),
    StructField("ESI_SUB_GRP", StringType(), True),
    StructField("PLN_DSGNR", StringType(), True),
    StructField("ADJDCT_DT", StringType(), True),
    StructField("ADMIN_FEE", DoubleType(), True),
    StructField("CAP_AMT", DoubleType(), True),
    StructField("INGR_CST_SUB", DoubleType(), True),
    StructField("MBR_NON_COPAY_AMT", DoubleType(), True),
    StructField("MBR_PAY_CD", StringType(), True),
    StructField("INCNTV_FEE", DoubleType(), True),
    StructField("CLM_ADJ_AMT", DoubleType(), True),
    StructField("CLM_ADJ_CD", StringType(), True),
    StructField("FRMLRY_FLAG", StringType(), True),
    StructField("GNRC_CLS_NO", StringType(), True),
    StructField("THRPTC_CLS_AHFS", StringType(), True),
    StructField("PDX_TYP", StringType(), True),
    StructField("BILL_BSS_CD", StringType(), True),
    StructField("USL_AND_CUST_CHRG", DoubleType(), True),
    StructField("PD_DT", StringType(), True),
    StructField("BNF_CD", StringType(), True),
    StructField("DRUG_STRG", StringType(), True),
    StructField("ORIG_MBR", StringType(), True),
    StructField("DT_OF_INJURY", DoubleType(), True),
    StructField("FEE_AMT", DoubleType(), True),
    StructField("ESI_REF_NO", StringType(), True),
    StructField("CLNT_CUST_ID", StringType(), True),
    StructField("PLN_TYP", StringType(), True),
    StructField("ESI_ADJDCT_REF_NO", DoubleType(), True),
    StructField("ESI_ANCLRY_AMT", DoubleType(), True),
    StructField("ESI_CLNT_GNRL_PRPS_AREA", StringType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("SUBGRP_ID", StringType(), True),
    StructField("CLS_PLN_ID", StringType(), True),
    StructField("PAID_DATE", StringType(), True),
    StructField("PRTL_FILL_STTUS_CD", StringType(), True),
    StructField("ESI_BILL_DT", DoubleType(), True),
    StructField("FSA_VNDR_CD", StringType(), True),
    StructField("PICA_DRUG_CD", StringType(), True),
    StructField("AMT_CLMED", DoubleType(), True),
    StructField("AMT_DSALW", DoubleType(), True),
    StructField("FED_DRUG_CLS_CD", StringType(), True),
    StructField("DEDCT_AMT", DoubleType(), True),
    StructField("BNF_COPAY_100", StringType(), True),
    StructField("CLM_PRCS_TYP", StringType(), True),
    StructField("INDEM_HIER_TIER_NO", DoubleType(), True),
    StructField("FLR", StringType(), True),
    StructField("MCARE_D_COV_DRUG", StringType(), True),
    StructField("RETRO_LICS_CD", StringType(), True),
    StructField("RETRO_LICS_AMT", DoubleType(), True),
    StructField("LICS_SBSDY_AMT", DoubleType(), True),
    StructField("MED_B_DRUG", StringType(), True),
    StructField("MED_B_CLM", StringType(), True),
    StructField("PRESCRIBER_QLFR", StringType(), True),
    StructField("PRESCRIBER_ID_NPI", StringType(), True),
    StructField("PDX_QLFR", StringType(), True),
    StructField("PDX_ID_NPI", StringType(), True),
    StructField("HRA_APLD_AMT", DoubleType(), True),
    StructField("ESI_THER_CLS", DoubleType(), True),
    StructField("HIC_NO", StringType(), True),
    StructField("HRA_FLAG", StringType(), True),
    StructField("DOSE_CD", DoubleType(), True),
    StructField("LOW_INCM", StringType(), True),
    StructField("RTE_OF_ADMIN", StringType(), True),
    StructField("DEA_SCHD", DoubleType(), True),
    StructField("COPAY_BNF_OPT", DoubleType(), True),
    StructField("GNRC_PROD_IN_GPI", DoubleType(), True),
    StructField("PRESCRIBER_SPEC", StringType(), True),
    StructField("VAL_CD", StringType(), True),
    StructField("PRI_CARE_PDX", StringType(), True),
    StructField("OFC_OF_INSPECTOR_GNRL_OIG", StringType(), True),
    StructField("FLR3", StringType(), True),
    StructField("PSL_FMLY_MET_AMT", DoubleType(), True),
    StructField("PSL_MBR_MET_AMT", DoubleType(), True),
    StructField("PSL_FMLY_AMT", DoubleType(), True),
    StructField("DED_FMLY_MET_AMT", DoubleType(), True),
    StructField("DED_FMLY_AMT", DoubleType(), True),
    StructField("MOPS_FMLY_AMT", DoubleType(), True),
    StructField("MOPS_FMLY_MET_AMT", DoubleType(), True),
    StructField("MOPS_MBR_MET_AMT", DoubleType(), True),
    StructField("DED_MBR_MET_AMT", DoubleType(), True),
    StructField("PSL_APLD_AMT", DoubleType(), True),
    StructField("MOPS_APLD_AMT", DoubleType(), True),
    StructField("PAR_PDX_IND", StringType(), True),
    StructField("COPAY_PCT_AMT", DoubleType(), True),
    StructField("COPAY_FLAT_AMT", DoubleType(), True),
    StructField("CLM_TRANSMITTAL_METH", StringType(), True),
    StructField("FLR4", StringType(), True)
])

df_MedtrakClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_MedtrakClmLand)
    .load(f"{adls_path}/verified/MedtrakDrugClm_Land.dat.{RunID}")
)

df_Transform = df_MedtrakClmLand.select(
    lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(lit('I'), 10, ' ').alias("INSRT_UPDT_CD"),
    rpad(lit('N'), 1, ' ').alias("DISCARD_IN"),
    rpad(lit('Y'), 1, ' ').alias("PASS_THRU_IN"),
    current_date().alias("FIRST_RECYC_DT"),
    lit(0).alias("ERR_CT"),
    lit(0).alias("RECYCLE_CT"),
    lit(SrcSysCd).alias("SRC_SYS_CD"),
    concat(lit(SrcSysCd), lit(';'), col("CLAIM_ID")).alias("PRI_KEY_STRING"),
    lit(0).alias("CLM_REMIT_HIST_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    col("CLAIM_ID").alias("CLM_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_SK"),
    rpad(lit('N'), 1, ' ').alias("CALC_ACTL_PD_AMT_IN"),
    rpad(lit('N'), 1, ' ').alias("SUPRESS_EOB_IN"),
    rpad(lit('N'), 1, ' ').alias("SUPRESS_REMIT_IN"),
    col("AMT_BILL").alias("ACTL_PD_AMT"),
    col("OTHR_PAYOR_AMT").alias("COB_PD_AMT"),
    lit(0.00).alias("COINS_AMT"),
    (col("INGR_CST") + col("DISPNS_FEE") + col("SLS_TAX")).alias("CNSD_CHRG_AMT"),
    col("COPAY_AMT").alias("COPAY_AMT"),
    col("DEDCT_AMT").alias("DEDCT_AMT"),
    lit(0.00).alias("DSALW_AMT"),
    lit(0.00).alias("ER_COPAY_AMT"),
    lit(0.00).alias("INTRST_AMT"),
    lit(0.00).alias("NO_RESP_AMT"),
    col("COPAY_AMT").alias("PATN_RESP_AMT"),
    lit(0.00).alias("WRTOFF_AMT"),
    lit(0.00).alias("PCA_PD_AMT"),
    rpad(lit('N'), 1, ' ').alias("ALT_CHRG_IN"),
    lit(0.00).alias("ALT_CHRG_PROV_WRTOFF_AMT")
)

df_Pkey = df_Transform.select(
    col("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD"),
    col("DISCARD_IN"),
    col("PASS_THRU_IN"),
    col("FIRST_RECYC_DT"),
    col("ERR_CT"),
    col("RECYCLE_CT"),
    col("SRC_SYS_CD"),
    col("PRI_KEY_STRING"),
    col("CLM_REMIT_HIST_SK"),
    col("SRC_SYS_CD_SK"),
    col("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("CALC_ACTL_PD_AMT_IN"),
    col("SUPRESS_EOB_IN"),
    col("SUPRESS_REMIT_IN"),
    col("ACTL_PD_AMT"),
    col("COB_PD_AMT"),
    col("COINS_AMT"),
    col("CNSD_CHRG_AMT"),
    col("COPAY_AMT"),
    col("DEDCT_AMT"),
    col("DSALW_AMT"),
    col("ER_COPAY_AMT"),
    col("INTRST_AMT"),
    col("NO_RESP_AMT"),
    col("PATN_RESP_AMT"),
    col("WRTOFF_AMT"),
    col("PCA_PD_AMT"),
    col("ALT_CHRG_IN"),
    col("ALT_CHRG_PROV_WRTOFF_AMT")
)

df_RowCount = df_Transform.withColumn("SRC_SYS_CD_SK", lit(SrcSysCdSk)).select(
    col("SRC_SYS_CD_SK"),
    col("CLM_ID")
)

write_files(
    df_RowCount,
    f"{adls_path}/load/B_CLM_REMIT_HIST.{SrcSysCd}.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

params = {
    "CurrRunCycle": RunCycle
}
df_container_out, = ClmRemitHistPK(df_Pkey, params)

df_final = df_container_out.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast("string"), len("JOB_EXCTN_RCRD_ERR_SK"), ' ').alias("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(col("INSRT_UPDT_CD"), 10, ' ').alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, ' ').alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, ' ').alias("PASS_THRU_IN"),
    rpad(col("FIRST_RECYC_DT").cast("string"), len("FIRST_RECYC_DT"), ' ').alias("FIRST_RECYC_DT"),
    rpad(col("ERR_CT").cast("string"), len("ERR_CT"), ' ').alias("ERR_CT"),
    rpad(col("RECYCLE_CT").cast("string"), len("RECYCLE_CT"), ' ').alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), len("SRC_SYS_CD"), ' ').alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), len("PRI_KEY_STRING"), ' ').alias("PRI_KEY_STRING"),
    rpad(col("CLM_REMIT_HIST_SK").cast("string"), len("CLM_REMIT_HIST_SK"), ' ').alias("CLM_REMIT_HIST_SK"),
    rpad(col("SRC_SYS_CD_SK").cast("string"), len("SRC_SYS_CD_SK"), ' ').alias("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), len("CLM_ID"), ' ').alias("CLM_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_SK").cast("string"), len("CRT_RUN_CYC_EXCTN_SK"), ' ').alias("CRT_RUN_CYC_EXCTN_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_SK").cast("string"), len("LAST_UPDT_RUN_CYC_EXCTN_SK"), ' ').alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(col("CLM_SK").cast("string"), len("CLM_SK"), ' ').alias("CLM_SK"),
    rpad(col("CALC_ACTL_PD_AMT_IN"), 1, ' ').alias("CALC_ACTL_PD_AMT_IN"),
    rpad(col("SUPRESS_EOB_IN"), 1, ' ').alias("SUPRESS_EOB_IN"),
    rpad(col("SUPRESS_REMIT_IN"), 1, ' ').alias("SUPRESS_REMIT_IN"),
    rpad(col("ACTL_PD_AMT").cast("string"), len("ACTL_PD_AMT"), ' ').alias("ACTL_PD_AMT"),
    rpad(col("COB_PD_AMT").cast("string"), len("COB_PD_AMT"), ' ').alias("COB_PD_AMT"),
    rpad(col("COINS_AMT").cast("string"), len("COINS_AMT"), ' ').alias("COINS_AMT"),
    rpad(col("CNSD_CHRG_AMT").cast("string"), len("CNSD_CHRG_AMT"), ' ').alias("CNSD_CHRG_AMT"),
    rpad(col("COPAY_AMT").cast("string"), len("COPAY_AMT"), ' ').alias("COPAY_AMT"),
    rpad(col("DEDCT_AMT").cast("string"), len("DEDCT_AMT"), ' ').alias("DEDCT_AMT"),
    rpad(col("DSALW_AMT").cast("string"), len("DSALW_AMT"), ' ').alias("DSALW_AMT"),
    rpad(col("ER_COPAY_AMT").cast("string"), len("ER_COPAY_AMT"), ' ').alias("ER_COPAY_AMT"),
    rpad(col("INTRST_AMT").cast("string"), len("INTRST_AMT"), ' ').alias("INTRST_AMT"),
    rpad(col("NO_RESP_AMT").cast("string"), len("NO_RESP_AMT"), ' ').alias("NO_RESP_AMT"),
    rpad(col("PATN_RESP_AMT").cast("string"), len("PATN_RESP_AMT"), ' ').alias("PATN_RESP_AMT"),
    rpad(col("WRTOFF_AMT").cast("string"), len("WRTOFF_AMT"), ' ').alias("WRTOFF_AMT"),
    rpad(col("PCA_PD_AMT").cast("string"), len("PCA_PD_AMT"), ' ').alias("PCA_PD_AMT"),
    rpad(col("ALT_CHRG_IN"), 1, ' ').alias("ALT_CHRG_IN"),
    rpad(col("ALT_CHRG_PROV_WRTOFF_AMT").cast("string"), len("ALT_CHRG_PROV_WRTOFF_AMT"), ' ').alias("ALT_CHRG_PROV_WRTOFF_AMT")
)

write_files(
    df_final,
    f"{adls_path}/key/MedtrakClmRemitHistExtr.DrugClmRemitHist.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)