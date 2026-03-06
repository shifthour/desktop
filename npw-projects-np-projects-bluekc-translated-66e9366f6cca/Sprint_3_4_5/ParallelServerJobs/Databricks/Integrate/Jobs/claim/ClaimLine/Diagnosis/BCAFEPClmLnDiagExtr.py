# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2015 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:  BCAFEPClmLnDiagExtrSeq
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
# MAGIC Ravi Abburi                    2015-01-21     5781 FEP Claims Line     Original Programming                                                IntegrateDev2                     Kalyan Neelam          2017-10-18
# MAGIC 
# MAGIC Saikiran Mahenderker     2018-03-28       5781 HEDIS                       Added new Columns                                      IntegrateDev2                        Jaideep Mankala       04/02/2018
# MAGIC                                                                                                            PERFORMING_NTNL_PROV_ID

# MAGIC Hash file (hf_clm_diag_allcol) cleared in the calling program
# MAGIC Process to extract the Diag Codes
# MAGIC This container is used in:
# MAGIC FctsClmLnDiagExtr
# MAGIC NascoClmLnDiagExtr
# MAGIC BCBSSCClmLnDiagExtr
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Assign primary surrogate key
# MAGIC Writing Sequential File to ../key
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType
)
from pyspark.sql import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/ClmLnDiagPK
# COMMAND ----------

# Retrieve all job parameters using get_widget_value
CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
SrcSysCd = get_widget_value('SrcSysCd','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
CurrentDate = get_widget_value('CurrentDate','')
DriverTable = get_widget_value('DriverTable','')
CurrDateTime = get_widget_value('CurrDateTime','')
FacetsOwner = get_widget_value('FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

# Define the schema for BCAFEPClmLnLanding (CSeqFileStage)
schema_BCAFEPClmLnLanding = StructType([
    StructField("CLM_ID", StringType(), True),
    StructField("CLM_LN_NO", IntegerType(), True),
    StructField("MBR_UNIQ_KEY", IntegerType(), False),
    StructField("SUB_UNIQ_KEY", IntegerType(), False),
    StructField("GRP_ID", StringType(), False),
    StructField("DOB", StringType(), True),
    StructField("GNDR_CD", StringType(), True),
    StructField("SUB_ID", StringType(), False),
    StructField("MBR_SFX_NO", StringType(), True),
    StructField("SRC_SYS", StringType(), False),
    StructField("RCRD_ID", StringType(), False),
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
    StructField("PERFORMING_NTNL_PROV_ID", StringType(), True)
])

# Read the file (CSeqFileStage) BCAFEPClmLnLanding
df_BCA_FEP = (
    spark.read.format("csv")
    .option("delimiter", ",")
    .option("quote", '"')
    .schema(schema_BCAFEPClmLnLanding)
    .load(f"{adls_path}/verified/BCAFEPCMedClm_ClmLnlanding.dat.{RunID}")
)

# Xfm_Diag_cd (CTransformerStage)
df_xfm_Diag_cd = df_BCA_FEP.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("DIAG_CDNG_TYP").alias("DIAG_CDNG_TYP"),
    F.col("ADM_DIAG_CD").alias("ADM_DIAG_CD"),
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
    F.lit("21").alias("DIAGNO21"),
    F.lit("22").alias("DIAGNO22"),
    F.lit("23").alias("DIAGNO23"),
    F.lit("24").alias("DIAGNO24"),
    F.lit("25").alias("DIAGNO25")
)

# DiagCd_Pivot (Pivot stage): unpivot logic
# We have 25 sets: (PRI_DIAG_CD, OTHR_DIAG_CD_1, ..., OTHR_DIAG_CD_24) matched with (DIAGNO1..25)
# The pivot wants columns: CLM_ID, CLM_LN_NO, DIAG_CD, ADM_DIAG_CD, DIAG_NO, ICD_VRSN_CD
# We'll use stack to unpivot them.
stack_expr_items = []
stack_expr_items.append("'PRI_DIAG_CD', DIAGNO1")
stack_expr_items.append("'OTHR_DIAG_CD_1', DIAGNO2")
stack_expr_items.append("'OTHR_DIAG_CD_2', DIAGNO3")
stack_expr_items.append("'OTHR_DIAG_CD_3', DIAGNO4")
stack_expr_items.append("'OTHR_DIAG_CD_4', DIAGNO5")
stack_expr_items.append("'OTHR_DIAG_CD_5', DIAGNO6")
stack_expr_items.append("'OTHR_DIAG_CD_6', DIAGNO7")
stack_expr_items.append("'OTHR_DIAG_CD_7', DIAGNO8")
stack_expr_items.append("'OTHR_DIAG_CD_8', DIAGNO9")
stack_expr_items.append("'OTHR_DIAG_CD_9', DIAGNO10")
stack_expr_items.append("'OTHR_DIAG_CD_10', DIAGNO11")
stack_expr_items.append("'OTHR_DIAG_CD_11', DIAGNO12")
stack_expr_items.append("'OTHR_DIAG_CD_12', DIAGNO13")
stack_expr_items.append("'OTHR_DIAG_CD_13', DIAGNO14")
stack_expr_items.append("'OTHR_DIAG_CD_14', DIAGNO15")
stack_expr_items.append("'OTHR_DIAG_CD_15', DIAGNO16")
stack_expr_items.append("'OTHR_DIAG_CD_16', DIAGNO17")
stack_expr_items.append("'OTHR_DIAG_CD_17', DIAGNO18")
stack_expr_items.append("'OTHR_DIAG_CD_18', DIAGNO19")
stack_expr_items.append("'OTHR_DIAG_CD_19', DIAGNO20")
stack_expr_items.append("'OTHR_DIAG_CD_20', DIAGNO21")
stack_expr_items.append("'OTHR_DIAG_CD_21', DIAGNO22")
stack_expr_items.append("'OTHR_DIAG_CD_22', DIAGNO23")
stack_expr_items.append("'OTHR_DIAG_CD_23', DIAGNO24")
stack_expr_items.append("'OTHR_DIAG_CD_24', DIAGNO25")

stack_sql = "stack(25, " + ", ".join(stack_expr_items) + ") as (col_diag, col_diagno)"

df_DiagCd_Pivot = (
    df_xfm_Diag_cd.select(
        F.col("CLM_ID"),
        F.col("CLM_LN_NO"),
        F.col("ADM_DIAG_CD"),
        F.col("DIAG_CDNG_TYP").alias("ICD_VRSN_CD"),
        F.expr(stack_sql)
    )
    .withColumnRenamed("col_diag", "DIAG_CD")
    .withColumnRenamed("col_diagno", "DIAG_NO")
)

# DiagCds (CTransformerStage) - produce two outputs from df_DiagCd_Pivot
# 1) "Sort" link => filter (IsNull(DIAG_CD)=@FALSE and Len(Trim(DIAG_CD))>0)
df_Sort = (
    df_DiagCd_Pivot
    .filter(
        (F.col("DIAG_CD").isNotNull())
        & (F.length(trim(F.col("DIAG_CD"))) > 0)
    )
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("DIAG_CD").alias("DIAG_CD"),
        F.col("DIAG_NO").alias("DIAG_NO"),
        F.col("ICD_VRSN_CD").alias("ICD_VRSN_CD")
    )
)

# 2) "AdmDiagCd" link => filter (IsNull(ADM_DIAG_CD)=@FALSE)
df_AdmDiagCd = (
    df_DiagCd_Pivot
    .filter(F.col("ADM_DIAG_CD").isNotNull())
    .select(
        F.col("CLM_ID").alias("CLM_ID"),
        F.col("CLM_LN_NO").alias("CLM_LN_NO"),
        F.col("ADM_DIAG_CD").alias("DIAG_CD"),
        F.lit("A").alias("DIAG_NO"),
        F.col("ICD_VRSN_CD").alias("ICD_VRSN_CD")
    )
)

# hf_bcafep_admdiagcd_dedupe (CHashedFileStage) - Scenario A (intermediate hashed file).
# Key columns: CLM_ID, CLM_LN_NO, DIAG_CD
# Deduplicate across those key columns
df_AdmDiagCd_dedup = dedup_sort(df_AdmDiagCd, ["CLM_ID", "CLM_LN_NO", "DIAG_CD"], [])

# lnk_AllDiags (CCollector) with inputs df_AdmDiagCd_dedup and the upcoming "Xfm_DiagNo" output
# but first handle the next chain for df_Sort -> sort -> hashed -> sort -> Xfm_DiagNo

# SortDiagCds1: sort df_Sort by (CLM_ID asc, CLM_LN_NO asc, DIAG_NO desc)
df_SortDiagCds1 = df_Sort.orderBy(["CLM_ID", "CLM_LN_NO", "DIAG_NO"], ascending=[True, True, False])

# hf_bcafep_medclm_diagcd_dedupe (CHashedFileStage) - Scenario A again
# Deduplicate on (CLM_ID, CLM_LN_NO, DIAG_CD)
df_hf_bcafep_medclm_diagcd_dedupe = dedup_sort(
    df_SortDiagCds1,
    ["CLM_ID", "CLM_LN_NO", "DIAG_CD"],
    []
)

# SortDiagCds2: sort by (CLM_ID asc, CLM_LN_NO asc, DIAG_NO asc)
df_SortDiagCds2 = df_hf_bcafep_medclm_diagcd_dedupe.orderBy(["CLM_ID", "CLM_LN_NO", "DIAG_NO"], ascending=[True, True, True])

# Xfm_DiagNo (CTransformerStage)
# We replicate the stage variable logic with a row_number partitioned by (CLM_ID, CLM_LN_NO) ordered by DIAG_NO asc
window_diag = Window.partitionBy("CLM_ID", "CLM_LN_NO").orderBy("DIAG_NO")
df_Xfm_DiagNo_tmp = df_SortDiagCds2.withColumn("svOrdnlCd", F.row_number().over(window_diag))

df_TransformIt = df_Xfm_DiagNo_tmp.select(
    F.col("CLM_ID").alias("CLM_ID"),
    F.col("CLM_LN_NO").alias("CLM_LN_NO"),
    F.col("DIAG_CD").alias("DIAG_CD"),
    F.col("svOrdnlCd").cast(StringType()).alias("DIAG_NO"),
    F.col("ICD_VRSN_CD").alias("ICD_VRSN_CD")
)

# lnk_AllDiags (CCollector) => union the two dataframes: df_AdmDiagCd_dedup and df_TransformIt
df_lnk_AllDiags = df_AdmDiagCd_dedup.unionByName(df_TransformIt)

# BusinessRules (CTransformerStage) => input df_lnk_AllDiags as BCAFEP
# Output columns (rename or derive)
df_BusinessRules = df_lnk_AllDiags.select(
    F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),  # char(18)
    F.col("CLM_LN_NO").alias("CLM_LN_SEQ_NO"),
    # Change(BCAFEP.DIAG_CD, '.', '') => assumed user-defined function "Change"
    F.expr("Change(DIAG_CD, '.', '')").alias("DIAG_CD"),  
    F.col("DIAG_NO").alias("CLM_LN_DIAG_ORDNL_CD"),
    F.lit("0").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.rpad(F.lit("I"), 10, " ").alias("INSRT_UPDT_CD"),  # char(10)
    F.rpad(F.lit("N"), 1, " ").alias("DISCARD_IN"),     # char(1)
    F.rpad(F.lit("Y"), 1, " ").alias("PASS_THRU_IN"),    # char(1)
    F.lit("0").alias("ERR_CT"),
    F.lit("0").alias("RECYCLE_CT"),
    F.col("SrcSysCd").alias("SRC_SYS_CD"),  # from job param usage or env? Stage expression says "SrcSysCd"
    F.concat(F.col("SrcSysCd"), F.lit(";"), F.col("CLM_ID"), F.lit(";"), F.col("CLM_LN_SEQ_NO"), F.lit(";"), F.lit("1")).alias("PRI_KEY_STRING"),
    F.lit("0").alias("CLM_LN_DIAG_SK"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.expr("CASE WHEN ICD_VRSN_CD = '9' THEN 'ICD9' WHEN ICD_VRSN_CD = '0' THEN 'ICD10' ELSE 'UNK' END").alias("DIAG_CD_TYP_CD")
)

# Snapshot (CTransformerStage) => input is df_BusinessRules
# This stage has 3 output links: "AllCol", "Snapshot", "Transform"
# 1) "AllCol" with many columns
df_AllCol = df_BusinessRules.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),  # The job uses "SrcSysCdSk" separately in DS, here we replicate the expression as told
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DIAG_ORDNL_CD"),
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD"),
    F.col("DISCARD_IN"),
    F.col("PASS_THRU_IN"),
    # FIRST_RECYC_DT => CurrentDate from DS param
    F.lit(CurrentDate).alias("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    F.col("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING"),
    F.col("CLM_LN_DIAG_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("DIAG_CD"),
    F.col("DIAG_CD_TYP_CD")
)

# 2) "Snapshot" link => columns [CLCL_ID, CLM_LN_SEQ_NO, CLM_LN_DIAG_ORDNL_CD]
df_Snapshot = df_BusinessRules.select(
    F.col("CLM_ID").alias("CLCL_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DIAG_ORDNL_CD").alias("CLM_LN_DIAG_ORDNL_CD")
)

# 3) "Transform" link => columns [SRC_SYS_CD_SK, CLM_ID, CLM_LN_SEQ_NO, CLM_LN_DIAG_ORDNL_CD]
df_Transform = df_BusinessRules.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DIAG_ORDNL_CD")
)

# Transformer (CTransformerStage) => input df_Snapshot => produce output "RowCount" => B_CLM_LN_DIAG
# stage var: svDiagOrdSk => GetFkeyCodes(SrcSysCd, 0, "DIAGNOSIS ORDINAL", Snapshot.CLM_LN_DIAG_ORDNL_CD, 'X')
# new col "CLM_LN_DIAG_ORDNL_CD_SK"
df_Transformer = df_Snapshot.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("CLCL_ID").alias("CLM_ID"),
    F.col("CLM_LN_SEQ_NO").alias("CLM_LN_SEQ_NO"),
    # call user-defined function
    F.expr(f"GetFkeyCodes(SrcSysCd, 0, 'DIAGNOSIS ORDINAL', CLM_LN_DIAG_ORDNL_CD, 'X')").alias("CLM_LN_DIAG_ORDNL_CD_SK")
)

# B_CLM_LN_DIAG (CSeqFileStage) => writing df_Transformer
df_B_CLM_LN_DIAG_to_write = df_Transformer.select(
    F.col("SRC_SYS_CD_SK"),
    F.col("CLM_ID"),
    F.col("CLM_LN_SEQ_NO"),
    F.col("CLM_LN_DIAG_ORDNL_CD_SK")
)
# Apply rpad if any columns are declared char/varchar with known lengths:
# (No explicit lengths were provided for these four columns in the final stage, so we write as is.)
write_files(
    df_B_CLM_LN_DIAG_to_write,
    f"{adls_path}/load/B_CLM_LN_DIAG.BCAFEP.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)

# Call the Shared Container ClmLnDiagPK with two inputs (AllCol, Transform)
params_ClmLnDiagPK = {
    "DriverTable": DriverTable,
    "RunID": RunID,
    "CurrRunCycle": CurrRunCycle,
    "CurrDateTime": CurrDateTime,
    "$FacetsDB": "",
    "$FacetsOwner": "",
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk,
    "$IDSOwner": ""
}
df_key = ClmLnDiagPK(df_AllCol, df_Transform, params_ClmLnDiagPK)

# BCAFEPClmLnDiagExtr (CSeqFileStage) => input df_key => final write
# Columns in precise order: 
#   JOB_EXCTN_RCRD_ERR_SK
#   INSRT_UPDT_CD (char(10))
#   DISCARD_IN (char(1))
#   PASS_THRU_IN (char(1))
#   FIRST_RECYC_DT
#   ERR_CT
#   RECYCLE_CT
#   SRC_SYS_CD
#   PRI_KEY_STRING
#   CLM_LN_DIAG_SK
#   CLM_ID (char(18))
#   CLM_LN_SEQ_NO
#   CLM_LN_DIAG_ORDNL_CD
#   CRT_RUN_CYC_EXCTN_SK
#   LAST_UPDT_RUN_CYC_EXCTN_SK
#   DIAG_CD (char(8))
#   DIAG_CD_TYP_CD
df_final = (
    df_key
    .select(
        F.col("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT"),
        F.col("ERR_CT"),
        F.col("RECYCLE_CT"),
        F.col("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING"),
        F.col("CLM_LN_DIAG_SK"),
        F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
        F.col("CLM_LN_SEQ_NO"),
        F.col("CLM_LN_DIAG_ORDNL_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.rpad(F.col("DIAG_CD"), 8, " ").alias("DIAG_CD"),
        F.col("DIAG_CD_TYP_CD")
    )
)

write_files(
    df_final,
    f"{adls_path}/key/BCAFEPClmLnDiagExtr.BCAFEPClmLnDiag.dat.{RunID}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote='"',
    nullValue=None
)