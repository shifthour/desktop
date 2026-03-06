# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCAFEPClmDiagExtrSeq
# MAGIC 
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC     Reads the BCAFEPMedClm_ClmLnLanding.dat file and runs through primary key using shared container ClmLnDiagPK
# MAGIC     
# MAGIC 
# MAGIC  
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #          Change Description                                               Development Project         Code Reviewer          Date Reviewed       
# MAGIC ------------------                  --------------------     ------------------------      -----------------------------------------------------------------------                --------------------------------              -------------------------------   ----------------------------      
# MAGIC Ravi Abburi                    2015-01-21     5781 FEP Claims        Original Programming                                                IntegrateDev2                          Kalyan Neelam          2017-10-18
# MAGIC 
# MAGIC 
# MAGIC Saikiran Mahenderker     2018-03-28       5781 HEDIS                       Added new Column                                      IntegrateDev2                         Jaideep Mankala       04/02/2018
# MAGIC                                                                                                            PERFORMING_NTNL_PROV_ID
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC Karthik Chintalapani    2020-03-23       5781 HEDIS     Modified the logic for header level claim for diagnosis codes.  IntegrateDev1   Jaideep Mankala       03/29/2020
# MAGIC                                                                                                      as per the new file format changes.

# MAGIC Process to extract the Admin Diag Codes
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC Diag Cods file is used in 
# MAGIC BCAFEPClmDRGExtr job
# MAGIC Process to extract the Diag Codes
# MAGIC Apply business logic
# MAGIC This container is used in:
# MAGIC FctsClmDiagExtr
# MAGIC NascoClmDiagExtr
# MAGIC BCBSSCClmDiagExtr
# MAGIC BCBSAClmDiagExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
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
    DecimalType
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../shared_containers/PrimaryKey/ClmDiagPK
# COMMAND ----------

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')

# Schema for BCAFepClmLand (CSeqFileStage)
schema_BCAFepClmLand = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", IntegerType(), True),
    StructField("ADJ_FROM_CLM_ID", StringType(), True),
    StructField("ADJ_TO_CLM_ID", StringType(), True),
    StructField("CLM_STTUS_CD", StringType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), True),
    StructField("SUB_UNIQ_KEY", IntegerType(), True),
    StructField("GRP_ID", StringType(), True),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID", StringType(), True),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("SRC_SYS", StringType(), True),
    StructField("RCRD_ID", StringType(), True),
    StructField("ADJ_NO", StringType(), True),
    StructField("PERFORMING_PROV_ID", StringType(), True),
    StructField("FEP_PROD", StringType(), True),
    StructField("MBR_ID", StringType(), True),
    StructField("BILL_TYP__CD", StringType(), True),
    StructField("FEP_SVC_LOC_CD", StringType(), True),
    StructField("IP_CLM_TYP_IN", StringType(), True),
    StructField("DRG_VRSN_IN", StringType(), True),
    StructField("DRG_CD", StringType(), True),
    StructField("PATN_STTUS_CD", StringType(), True),
    StructField("CLM_CLS_IN", StringType(), True),
    StructField("CLM_DENIED_FLAG", StringType(), True),
    StructField("ILNS_DT", StringType(), True),
    StructField("IP_CLM_BEG_DT", StringType(), True),
    StructField("IP_CLM_DSCHG_DT", StringType(), True),
    StructField("CLM_SVC_DT_BEG", StringType(), True),
    StructField("CLM_SVC_DT_END", StringType(), True),
    StructField("FCLTY_CLM_STMNT_BEG_DT", StringType(), True),
    StructField("FCLTY_CLM_STMNT_END_DT", StringType(), True),
    StructField("CLM_PRCS_DT", StringType(), True),
    StructField("IP_ADMS_CT", StringType(), True),
    StructField("NO_COV_DAYS", IntegerType(), True),
    StructField("DIAG_CDNG_TYP", StringType(), True),
    StructField("PRI_DIAG_CD", StringType(), True),
    StructField("PRI_DIAG_POA_IN", StringType(), True),
    StructField("ADM_DIAG_CD", StringType(), True),
    StructField("ADM_DIAG_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_1", StringType(), True),
    StructField("OTHR_DIAG_CD_1_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_2", StringType(), True),
    StructField("OTHR_DIAG_CD_2_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_3", StringType(), True),
    StructField("OTHR_DIAG_CD_3_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_4", StringType(), True),
    StructField("OTHR_DIAG_CD_4_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_5", StringType(), True),
    StructField("OTHR_DIAG_CD_5_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_6", StringType(), True),
    StructField("OTHR_DIAG_CD_6_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_7", StringType(), True),
    StructField("OTHR_DIAG_CD_7_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_8", StringType(), True),
    StructField("OTHR_DIAG_CD_8_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_9", StringType(), True),
    StructField("OTHR_DIAG_CD_9_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_10", StringType(), True),
    StructField("OTHR_DIAG_CD_10_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_11", StringType(), True),
    StructField("OTHR_DIAG_CD_11_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_12", StringType(), True),
    StructField("OTHR_DIAG_CD_12_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_13", StringType(), True),
    StructField("OTHR_DIAG_CD_13_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_14", StringType(), True),
    StructField("OTHR_DIAG_CD_14_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_15", StringType(), True),
    StructField("OTHR_DIAG_CD_15_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_16", StringType(), True),
    StructField("OTHR_DIAG_CD_16_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_17", StringType(), True),
    StructField("OTHR_DIAG_CD_17_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_18", StringType(), True),
    StructField("OTHR_DIAG_CD_18_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_19", StringType(), True),
    StructField("OTHR_DIAG_CD_19_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_20", StringType(), True),
    StructField("OTHR_DIAG_CD_20_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_21", StringType(), True),
    StructField("OTHR_DIAG_CD_21_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_22", StringType(), True),
    StructField("OTHR_DIAG_CD_22_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_23", StringType(), True),
    StructField("OTHR_DIAG_CD_23_POA_IN", StringType(), True),
    StructField("OTHR_DIAG_CD_24", StringType(), True),
    StructField("OTHR_DIAG_CD_24_POA_IN", StringType(), True),
    StructField("PROC_CDNG_TYP", StringType(), True),
    StructField("PRINCIPLE_PROC_CD", StringType(), True),
    StructField("PRINCIPLE_PROC_CD_DT", StringType(), True),
    StructField("OTHR_PROC_CD_1", StringType(), True),
    StructField("OTHR_PROC_CD_1_DT", StringType(), True),
    StructField("OTHR_PROC_CD_2", StringType(), True),
    StructField("OTHR_PROC_CD_2_DT", StringType(), True),
    StructField("OTHR_PROC_CD_3", StringType(), True),
    StructField("OTHR_PROC_CD_3_DT", StringType(), True),
    StructField("OTHR_PROC_CD_4", StringType(), True),
    StructField("OTHR_PROC_CD_4_DT", StringType(), True),
    StructField("OTHR_PROC_CD_5", StringType(), True),
    StructField("OTHR_PROC_CD_5_DT", StringType(), True),
    StructField("OTHR_PROC_CD_6", StringType(), True),
    StructField("OTHR_PROC_CD_6_DT", StringType(), True),
    StructField("OTHR_PROC_CD_7", StringType(), True),
    StructField("OTHR_PROC_CD_7_DT", StringType(), True),
    StructField("OTHR_PROC_CD_8", StringType(), True),
    StructField("OTHR_PROC_CD_8_DT", StringType(), True),
    StructField("OTHR_PROC_CD_9", StringType(), True),
    StructField("OTHR_PROC_CD_9_DT", StringType(), True),
    StructField("OTHR_PROC_CD_10", StringType(), True),
    StructField("OTHR_PROC_CD_10_DT", StringType(), True),
    StructField("OTHR_PROC_CD_11", StringType(), True),
    StructField("OTHR_PROC_CD_11_DT", StringType(), True),
    StructField("OTHR_PROC_CD_12", StringType(), True),
    StructField("OTHR_PROC_CD_12_DT", StringType(), True),
    StructField("OTHR_PROC_CD_13", StringType(), True),
    StructField("OTHR_PROC_CD_13_DT", StringType(), True),
    StructField("OTHR_PROC_CD_14", StringType(), True),
    StructField("OTHR_PROC_CD_14_DT", StringType(), True),
    StructField("OTHR_PROC_CD_15", StringType(), True),
    StructField("OTHR_PROC_CD_15_DT", StringType(), True),
    StructField("OTHR_PROC_CD_16", StringType(), True),
    StructField("OTHR_PROC_CD_16_DT", StringType(), True),
    StructField("OTHR_PROC_CD_17", StringType(), True),
    StructField("OTHR_PROC_CD_17_DT", StringType(), True),
    StructField("OTHR_PROC_CD_18", StringType(), True),
    StructField("OTHR_PROC_CD_18_DT", StringType(), True),
    StructField("OTHR_PROC_CD_19", StringType(), True),
    StructField("OTHR_PROC_CD_19_DT", StringType(), True),
    StructField("OTHR_PROC_CD_20", StringType(), True),
    StructField("OTHR_PROC_CD_20_DT", StringType(), True),
    StructField("OTHR_PROC_CD_21", StringType(), True),
    StructField("OTHR_PROC_CD_21_DT", StringType(), True),
    StructField("OTHR_PROC_CD_22", StringType(), True),
    StructField("OTHR_PROC_CD_22_DT", StringType(), True),
    StructField("OTHR_PROC_CD_23", StringType(), True),
    StructField("OTHR_PROC_CD_23_DT", StringType(), True),
    StructField("OTHR_PROC_CD_24", StringType(), True),
    StructField("OTHR_PROC_CD_24_DT", StringType(), True),
    StructField("RVNU_CD", StringType(), True),
    StructField("PROC_CD_NON_ICD", StringType(), True),
    StructField("PROC_CD_MOD_1", StringType(), True),
    StructField("PROC_CD_MOD_2", StringType(), True),
    StructField("PROC_CD_MOD_3", StringType(), True),
    StructField("PROC_CD_MOD_4", StringType(), True),
    StructField("CLM_UNIT", DecimalType(38,10), True),
    StructField("CLM_LN_TOT_ALL_SVC_CHRG_AMT", DecimalType(38,10), True),
    StructField("CLM_LN_PD_AMT", DecimalType(38,10), True),
    StructField("CLM_ALT_AMT_PD_1", DecimalType(38,10), True),
    StructField("CLM_ALT_AMT_PD_2", DecimalType(38,10), True),
    StructField("LOINC_CD", StringType(), True),
    StructField("CLM_TST_RSLT", StringType(), True),
    StructField("ALT_CLM_TST_RSLT", StringType(), True),
    StructField("DRUG_CD", StringType(), True),
    StructField("DRUG_CLM_INCUR_DT", StringType(), True),
    StructField("CLM_TREAT_DURATN", StringType(), True),
    StructField("DATA_SRC", StringType(), True),
    StructField("SUPLMT_DATA_SRC_TYP", StringType(), True),
    StructField("ADTR_APRV_STTUS", StringType(), True),
    StructField("RUN_DT", StringType(), True),
    StructField("PD_DAYS", IntegerType(), True),
    StructField("PERFORMING_NTNL_PROV_ID", StringType(), True),
])

df_BCAFepClmLand = (
    spark.read.format("csv")
    .option("header", "false")
    .option("delimiter", ",")
    .option("quote", "\"")
    .schema(schema_BCAFepClmLand)
    .load(f"{adls_path}/verified/BCAFEPCMedClm_ClmLanding.dat.{RunID}")
)

# Xfm_Diag_cd (CTransformerStage)
df_Xfm_Diag_cd = df_BCAFepClmLand.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DIAG_CDNG_TYP").alias("DIAG_CDNG_TYP"),
    F.col("CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("ADM_DIAG_CD").alias("ADM_DIAG_CD"),
    F.col("ADM_DIAG_POA_IN").alias("ADM_DIAG_POA_IN"),
    F.col("PRI_DIAG_CD").alias("PRI_DIAG_CD"),
    F.col("OTHR_DIAG_CD_1").alias("OTHR_DIAG_CD_1"),
    F.col("OTHR_DIAG_CD_2").alias("OTHR_DIAG_CD_2"),
    F.col("OTHR_DIAG_CD_3").alias("OTHR_DIAG_CD_3"),
    F.col("OTHR_DIAG_CD_4").alias("OTHR_DIAG_CD_4"),
    F.col("OTHR_DIAG_CD_5").alias("OTHR_DIAG_CD_5"),
    F.col("OTHR_DIAG_CD_6").alias("OTHR_DIAG_CD_6"),
    F.col("OTHR_DIAG_CD_7").alias("OTHR_DIAG_CD_7"),
    F.col("OTHR_DIAG_CD_8").alias("OTHR_DIAG_CD_8"),
    F.col("OTHR_DIAG_CD_9").alias("OTHR_DIAG_CD_9"),
    F.col("OTHR_DIAG_CD_10").alias("OTHR_DIAG_CD_10"),
    F.col("OTHR_DIAG_CD_11").alias("OTHR_DIAG_CD_11"),
    F.col("OTHR_DIAG_CD_12").alias("OTHR_DIAG_CD_12"),
    F.col("OTHR_DIAG_CD_13").alias("OTHR_DIAG_CD_13"),
    F.col("OTHR_DIAG_CD_14").alias("OTHR_DIAG_CD_14"),
    F.col("OTHR_DIAG_CD_15").alias("OTHR_DIAG_CD_15"),
    F.col("OTHR_DIAG_CD_16").alias("OTHR_DIAG_CD_16"),
    F.col("OTHR_DIAG_CD_17").alias("OTHR_DIAG_CD_17"),
    F.col("OTHR_DIAG_CD_18").alias("OTHR_DIAG_CD_18"),
    F.col("OTHR_DIAG_CD_19").alias("OTHR_DIAG_CD_19"),
    F.col("OTHR_DIAG_CD_20").alias("OTHR_DIAG_CD_20"),
    F.col("OTHR_DIAG_CD_21").alias("OTHR_DIAG_CD_21"),
    F.col("OTHR_DIAG_CD_22").alias("OTHR_DIAG_CD_22"),
    F.col("OTHR_DIAG_CD_23").alias("OTHR_DIAG_CD_23"),
    F.col("OTHR_DIAG_CD_24").alias("OTHR_DIAG_CD_24"),
    F.lit("1").alias("DIAGNO1"),
    F.lit("2").alias("DIAGNO2"),
    F.lit("3").alias("DIAGNO3"),
    F.lit("4").alias("DIAGNO4"),
    F.lit("5").alias("DIAGNO5"),
    F.lit("6").alias("DIAGNO6"),
    F.lit("7").alias("DIAGNO7"),
    F.lit("8").alias("DIAGNO8"),
    F.lit("9").alias("DIAGNO9"),
    F.lit("10").alias("DIAGNO10"),
    F.lit("11").alias("DIAGNO11"),
    F.lit("12").alias("DIAGNO12"),
    F.lit("13").alias("DIAGNO13"),
    F.lit("14").alias("DIAGNO14"),
    F.lit("15").alias("DIAGNO15"),
    F.lit("16").alias("DIAGNO16"),
    F.lit("17").alias("DIAGNO17"),
    F.lit("18").alias("DIAGNO18"),
    F.lit("19").alias("DIAGNO19"),
    F.lit("20").alias("DIAGNO20"),
    F.lit("21").alias("DIAGN021"),  # matches the JSON's spelling
    F.lit("22").alias("DIAGNO22"),
    F.lit("23").alias("DIAGNO23"),
    F.lit("24").alias("DIAGNO24"),
    F.lit("25").alias("DIAGNO25"),
    F.col("PRI_DIAG_POA_IN").alias("PRI_DIAG_POA_IN"),
    F.col("OTHR_DIAG_CD_1_POA_IN").alias("OTHR_DIAG_CD_1_POA_IN"),
    F.col("OTHR_DIAG_CD_2_POA_IN").alias("OTHR_DIAG_CD_2_POA_IN"),
    F.col("OTHR_DIAG_CD_3_POA_IN").alias("OTHR_DIAG_CD_3_POA_IN"),
    F.col("OTHR_DIAG_CD_4_POA_IN").alias("OTHR_DIAG_CD_4_POA_IN"),
    F.col("OTHR_DIAG_CD_5_POA_IN").alias("OTHR_DIAG_CD_5_POA_IN"),
    F.col("OTHR_DIAG_CD_6_POA_IN").alias("OTHR_DIAG_CD_6_POA_IN"),
    F.col("OTHR_DIAG_CD_7_POA_IN").alias("OTHR_DIAG_CD_7_POA_IN"),
    F.col("OTHR_DIAG_CD_8_POA_IN").alias("OTHR_DIAG_CD_8_POA_IN"),
    F.col("OTHR_DIAG_CD_9_POA_IN").alias("OTHR_DIAG_CD_9_POA_IN"),
    F.col("OTHR_DIAG_CD_10_POA_IN").alias("OTHR_DIAG_CD_10_POA_IN"),
    F.col("OTHR_DIAG_CD_11_POA_IN").alias("OTHR_DIAG_CD_11_POA_IN"),
    F.col("OTHR_DIAG_CD_12_POA_IN").alias("OTHR_DIAG_CD_12_POA_IN"),
    F.col("OTHR_DIAG_CD_13_POA_IN").alias("OTHR_DIAG_CD_13_POA_IN"),
    F.col("OTHR_DIAG_CD_14_POA_IN").alias("OTHR_DIAG_CD_14_POA_IN"),
    F.col("OTHR_DIAG_CD_15_POA_IN").alias("OTHR_DIAG_CD_15_POA_IN"),
    F.col("OTHR_DIAG_CD_16_POA_IN").alias("OTHR_DIAG_CD_16_POA_IN"),
    F.col("OTHR_DIAG_CD_17_POA_IN").alias("OTHR_DIAG_CD_17_POA_IN"),
    F.col("OTHR_DIAG_CD_18_POA_IN").alias("OTHR_DIAG_CD_18_POA_IN"),
    F.col("OTHR_DIAG_CD_19_POA_IN").alias("OTHR_DIAG_CD_19_POA_IN"),
    F.col("OTHR_DIAG_CD_20_POA_IN").alias("OTHR_DIAG_CD_20_POA_IN"),
    F.col("OTHR_DIAG_CD_21_POA_IN").alias("OTHR_DIAG_CD_21_POA_IN"),
    F.col("OTHR_DIAG_CD_22_POA_IN").alias("OTHR_DIAG_CD_22_POA_IN"),
    F.col("OTHR_DIAG_CD_23_POA_IN").alias("OTHR_DIAG_CD_23_POA_IN"),
    F.col("OTHR_DIAG_CD_24_POA_IN").alias("OTHR_DIAG_CD_24_POA_IN")
)

# DiagCd_Pivot (Pivot)
# We pivot the columns PRI_DIAG_CD, OTHR_DIAG_CD_x into DIAG_CD
# DIAGNOx into DIAG_NO
# PRI_DIAG_POA_IN, OTHR_DIAG_CD_x_POA_IN into DIAG_POA_IN
# We'll use stack to pivot them vertically.
df_DiagCd_Pivot = df_Xfm_Diag_cd.select(
    F.col("CLM_ID"),
    F.col("CLM_LN_NO"),
    F.col("ADM_DIAG_CD"),
    F.col("ADM_DIAG_POA_IN"),
    F.col("DIAG_CDNG_TYP").alias("ICD_VRSN_CD"),
    F.expr(
        """
        stack(
          25,
          PRI_DIAG_CD, DIAGNO1, PRI_DIAG_POA_IN,
          OTHR_DIAG_CD_1, DIAGNO2, OTHR_DIAG_CD_1_POA_IN,
          OTHR_DIAG_CD_2, DIAGNO3, OTHR_DIAG_CD_2_POA_IN,
          OTHR_DIAG_CD_3, DIAGNO4, OTHR_DIAG_CD_3_POA_IN,
          OTHR_DIAG_CD_4, DIAGNO5, OTHR_DIAG_CD_4_POA_IN,
          OTHR_DIAG_CD_5, DIAGNO6, OTHR_DIAG_CD_5_POA_IN,
          OTHR_DIAG_CD_6, DIAGNO7, OTHR_DIAG_CD_6_POA_IN,
          OTHR_DIAG_CD_7, DIAGNO8, OTHR_DIAG_CD_7_POA_IN,
          OTHR_DIAG_CD_8, DIAGNO9, OTHR_DIAG_CD_8_POA_IN,
          OTHR_DIAG_CD_9, DIAGNO10, OTHR_DIAG_CD_9_POA_IN,
          OTHR_DIAG_CD_10, DIAGNO11, OTHR_DIAG_CD_10_POA_IN,
          OTHR_DIAG_CD_11, DIAGNO12, OTHR_DIAG_CD_11_POA_IN,
          OTHR_DIAG_CD_12, DIAGNO13, OTHR_DIAG_CD_12_POA_IN,
          OTHR_DIAG_CD_13, DIAGNO14, OTHR_DIAG_CD_13_POA_IN,
          OTHR_DIAG_CD_14, DIAGNO15, OTHR_DIAG_CD_14_POA_IN,
          OTHR_DIAG_CD_15, DIAGNO16, OTHR_DIAG_CD_15_POA_IN,
          OTHR_DIAG_CD_16, DIAGNO17, OTHR_DIAG_CD_16_POA_IN,
          OTHR_DIAG_CD_17, DIAGNO18, OTHR_DIAG_CD_17_POA_IN,
          OTHR_DIAG_CD_18, DIAGNO19, OTHR_DIAG_CD_18_POA_IN,
          OTHR_DIAG_CD_19, DIAGNO20, OTHR_DIAG_CD_19_POA_IN,
          OTHR_DIAG_CD_20, DIAGN021, OTHR_DIAG_CD_20_POA_IN,
          OTHR_DIAG_CD_21, DIAGNO22, OTHR_DIAG_CD_21_POA_IN,
          OTHR_DIAG_CD_22, DIAGNO23, OTHR_DIAG_CD_22_POA_IN,
          OTHR_DIAG_CD_23, DIAGNO24, OTHR_DIAG_CD_23_POA_IN,
          OTHR_DIAG_CD_24, DIAGNO25, OTHR_DIAG_CD_24_POA_IN
        ) as (DIAG_CD, DIAG_NO, DIAG_POA_IN)
        """
    )
).select(
    F.col("CLM_ID"),
    F.col("DIAG_CD"),
    F.col("CLM_LN_NO"),
    F.col("ADM_DIAG_CD"),
    F.col("DIAG_NO"),
    F.col("ICD_VRSN_CD"),
    F.col("DIAG_POA_IN"),
    F.col("ADM_DIAG_POA_IN")
)

# DiagCds (CTransformerStage)
# Two output links from this stage:
# 1) "Sort" with constraint: IsNull(DIAG_CD)=false AND Len(Trim(DIAG_CD))>0
df_DiagCds_Sort = (
    df_DiagCd_Pivot
    .filter(
        (F.col("DIAG_CD").isNotNull()) &
        (F.length(trim(F.col("DIAG_CD"))) > 0)
    )
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("DIAG_CD").alias("DIAG_CD"),
        F.col("CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("DIAG_NO").alias("DIAG_NO"),
        F.col("ICD_VRSN_CD").alias("ICD_VRSN_CD"),
        F.col("DIAG_POA_IN").alias("DIAG_POA_IN")
    )
)

# 2) "AdmDiagCd" with constraint: IsNull(ADM_DIAG_CD)=false
df_AdmDiagCd = (
    df_DiagCd_Pivot
    .filter(F.col("ADM_DIAG_CD").isNotNull())
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("ADM_DIAG_CD").alias("DIAG_CD"),
        F.lit("A").alias("DIAG_NO"),
        F.col("ICD_VRSN_CD").alias("ICD_VRSN_CD"),
        F.col("ADM_DIAG_POA_IN").alias("DIAG_POA_IN")
    )
)

# hf_bcafep_admdiagcd_dedupe (CHashedFileStage) - Scenario A
# Deduplicate on key column(s). PrimaryKey is "CLM_ID".
df_AdmDiagCdout = df_AdmDiagCd.drop_duplicates(["CLM_ID"])

# This goes to "lnk_AllDiags" input 1
df_lnk_AllDiags_1 = df_AdmDiagCdout

# SortDiagCds1 (sort)
df_SortDiagCds1 = df_DiagCds_Sort.orderBy(
    F.col("CLM_ID").asc(),
    F.col("CLM_LN_NO").desc(),
    F.col("DIAG_NO").desc()
)

# hf_bcafep_clm_diagcd_dedupe (CHashedFileStage) - Scenario A
# Has two primary keys "CLM_ID","DIAG_CD".
df_hf_bcafep_clm_diagcd_in = df_SortDiagCds1
df_hf_bcafep_clm_diagcd_out = df_hf_bcafep_clm_diagcd_in.drop_duplicates(["CLM_ID","DIAG_CD"])

# SortDiagCds2 (sort)
df_SortDiagCds2 = df_hf_bcafep_clm_diagcd_out.orderBy(
    F.col("CLM_ID").asc(),
    F.col("CLM_LN_NO").asc(),
    F.col("DIAG_NO").asc()
)

# Xtfm_DiagNo (CTransformerStage)
# We implement the stage variables approach with a row_number partitioned by CLM_ID in ascending of (CLM_LN_NO, DIAG_NO).
w_diag = Window.partitionBy("CLM_ID").orderBy("CLM_LN_NO", "DIAG_NO")
df_Xtfm_DiagNo = df_SortDiagCds2.withColumn("svOrdnlNo", F.row_number().over(w_diag)).select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("svOrdnlNo").cast(StringType()).alias("DIAG_NO"),
    F.col("ICD_VRSN_CD").alias("ICD_VRSN_CD"),
    F.col("DIAG_POA_IN").alias("DIAG_POA_IN")
)

# lnk_AllDiags (CCollector)
# Input1 = df_lnk_AllDiags_1
# Input2 = df_Xtfm_DiagNo
# Union the two
common_cols_lnk_AllDiags = ["CLM_ID","DIAG_CD","DIAG_NO","ICD_VRSN_CD","DIAG_POA_IN"]
df_lnk_AllDiags_1_sel = df_lnk_AllDiags_1.select(
    F.col("CLM_ID").cast(StringType()),
    F.col("DIAG_CD").cast(StringType()),
    F.col("DIAG_NO").cast(StringType()),
    F.col("ICD_VRSN_CD").cast(StringType()),
    F.col("DIAG_POA_IN").cast(StringType())
)
df_lnk_AllDiags_2_sel = df_Xtfm_DiagNo.select(
    F.col("CLM_ID").cast(StringType()),
    F.col("DIAG_CD").cast(StringType()),
    F.col("DIAG_NO").cast(StringType()),
    F.col("ICD_VRSN_CD").cast(StringType()),
    F.col("DIAG_POA_IN").cast(StringType())
)
df_lnk_AllDiags = df_lnk_AllDiags_1_sel.unionByName(df_lnk_AllDiags_2_sel)

# This collector has output "BCAFEP_Clm" => BusinessRules
df_BCAFEP_Clm = df_lnk_AllDiags

# BusinessRules (CTransformerStage)
# We create two outputs: "TransformIt" => Snapshot, and "DiagCds" => seq_DiagCds
# Stage variables: ClmId = BCAFEP_Clm.CLM_ID
# The expressions on "TransformIt":
df_BusinessRules_TransformIt = df_BCAFEP_Clm.select(
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.lit("I").alias("INSRT_UPDT_CD"),
    F.lit("N").alias("DISCARD_IN"),
    F.lit("Y").alias("PASS_THRU_IN"),
    # CurrentDate -> translate to current_date() per guidelines
    current_date().alias("FIRST_RECYC_DT"),
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    F.concat_ws(";", F.lit(SrcSysCd), F.col("CLM_ID"), F.lit("1")).alias("PRI_KEY_STRING"),
    F.lit("0").alias("CLM_DIAG_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DIAG_NO").alias("CLM_DIAG_ORDNL_CD"),
    F.lit("0").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.lit("0").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.regexp_replace(F.col("DIAG_CD"), "\\.", "").alias("DIAG_CD"),
    F.when(F.col("DIAG_POA_IN").isNull(), F.lit("NA")).otherwise(F.col("DIAG_POA_IN")).alias("CLM_DIAG_POA_CD"),
    F.when(F.col("ICD_VRSN_CD")=="9", F.lit("ICD9"))
     .when(F.col("ICD_VRSN_CD")=="0", F.lit("ICD10"))
     .otherwise(F.lit("UNK")).alias("DIAG_CD_TYP_CD")
)

df_BusinessRules_DiagCds = df_BCAFEP_Clm.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("DIAG_NO").alias("ORDNL_NO"),
    F.regexp_replace(F.col("DIAG_CD"), "\\.", "").alias("DIAG_CD")
)

# seq_DiagCds (CSeqFileStage) => Write the file
df_seq_DiagCds = df_BusinessRules_DiagCds.select(
    "CLM_ID",
    "ORDNL_NO",
    "DIAG_CD"
)
write_files(
    df_seq_DiagCds,
    f"{adls_path}/verified/BCAFEPClmDiagExtr_DiagCd_landing.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Snapshot (CTransformerStage)
# Input is df_BusinessRules_TransformIt => 3 output links:
# 1) "AllCol" => ClmDiagPK
df_Snapshot_AllCol = df_BusinessRules_TransformIt.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),  # but the expression used "SrcSysCdSk"? We do a literal or pass?  We'll do:
    # The JSON shows: "SRC_SYS_CD_SK" => expression "SrcSysCdSk"
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CLM_DIAG_SK").alias("CLM_DIAG_SK"),
    F.col("CRT_RUN_CYC_EXTCN_SK").alias("CRT_RUN_CYC_EXTCN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXTCN_SK").alias("LAST_UPDT_RUN_CYC_EXTCN_SK"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("CLM_DIAG_POA_CD").alias("CLM_DIAG_POA_CD"),
    F.col("DIAG_CD_TYP_CD").alias("DIAG_CD_TYP_CD")
)

# 2) "Snapshot" => next transformer "Transformer" but JSON shows columns "CLM_ID","CLM_DIAG_ORDNL_CD"
df_Snapshot_Snapshot = df_BusinessRules_TransformIt.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD")
)

# 3) "Transform" => ClmDiagPK
df_Snapshot_Transform = df_BusinessRules_TransformIt.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_DIAG_ORDNL_CD").alias("CLM_DIAG_ORDNL_CD")
)

# ClmDiagPK (CContainerStage)
# Two inputs: "AllCol" => df_Snapshot_AllCol, "Transform" => df_Snapshot_Transform
# One output => "Key"
params_ClmDiagPK = {
    "CurrRunCycle": CurrRunCycle,
    "SrcSysCd": SrcSysCd,
    "IDSOwner": IDSOwner
}
df_ClmDiagPK_out = ClmDiagPK(df_Snapshot_AllCol, df_Snapshot_Transform, params_ClmDiagPK)
# Output "Key" => BCAFEPClmDiag

# BCAFEPClmDiag (CSeqFileStage) => write
df_BCAFEPClmDiag = df_ClmDiagPK_out.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "CLM_DIAG_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD",
    "CRT_RUN_CYC_EXTCN_SK",
    "LAST_UPDT_RUN_CYC_EXTCN_SK",
    "DIAG_CD",
    "CLM_DIAG_POA_CD",
    "DIAG_CD_TYP_CD"
)
write_files(
    df_BCAFEPClmDiag,
    f"{adls_path}/key/BCAFEPClmDiagExtr.BCAFEPClmDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# Transformer (StageName="Transformer") with 1 output link => B_CLM_DIAG
# Input is "Snapshot" => df_Snapshot_Snapshot (linkName=V209S9P1)
# StageVariable: svDiagOrdlCd = GetFkeyCodes(SrcSysCd, 0, "DIAGNOSIS ORDINAL", Snapshot.CLM_DIAG_ORDNL_CD, 'X')
# This is a user-defined function, assume available. We do:
df_Transformer = df_Snapshot_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID").alias("CLM_ID"),
    # apply GetFkeyCodes UDF
    GetFkeyCodes(F.lit(SrcSysCd), F.lit(0), F.lit("DIAGNOSIS ORDINAL"), F.col("CLM_DIAG_ORDNL_CD"), F.lit("X")).alias("CLM_DIAG_ORDNL_CD")
)

# B_CLM_DIAG (CSeqFileStage)
df_B_CLM_DIAG = df_Transformer.select(
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CLM_DIAG_ORDNL_CD"
)
write_files(
    df_B_CLM_DIAG,
    f"{adls_path}/load/B_CLM_DIAG.BCAFEP.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)