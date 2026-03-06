# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC **********************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:  FctsSubPcaExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC                               Pulls data from FACETS to a landing file for the IDS.  Rows from CMC_SBHS_HSA_ACCUM contain data originally 
# MAGIC                               created in Facets while rows from CMC_FAAC_ACCUM where converted from data from an external vendor.
# MAGIC                               Creates an output file in the key directory for input to the IDS transform job.
# MAGIC 3;hf_sub_pca_combined;hf_sub_pca_conv_07_out;hf_sub_pca_allcol
# MAGIC INPUTS:
# MAGIC                               CMC_SBHS_HSA_ACCUM
# MAGIC                               audit.BCBS_CMC_SBHS_HSA_ACCUM
# MAGIC                               CMC_FAAC_ACCUM
# MAGIC                               audit.BCBS_FAAC_ACCUM
# MAGIC                               CMC_GRGR_GROUP 
# MAGIC 
# MAGIC HASH FILES:
# MAGIC                               hf_sub_pca_combined_1, hf_sub_pca_combined_2 --- hf_sub_pca_combined_1 and hf_sub_pca_combined_2 both point to the same hashed file - hf_sub_pca_combined
# MAGIC                               hf_sub_pca_conv_07_out
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                               STRIP.FIELD
# MAGIC                               FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                               The output of the two coversion claim queries may have rows with duplicate keys.  
# MAGIC                               In case of a duplicate key, the row from the Conv06 logic should be used.  
# MAGIC                               The rows from the Curr logic and the Conv06 logic are written to the hf_sub_pca_combined hashed file.
# MAGIC                               The DeDup transform step only passes through a row from the Conv07 logic if there is NOT a matching row from the Conv06 logic already in hf_sub_pca_combined.
# MAGIC                               If there is no matching row, the row from Conv07 is written to hf_sub_pca_combined.
# MAGIC   
# MAGIC OUTPUTS: 
# MAGIC                               Temporary output file
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                               Hugh Sisson     10/10/2006  -  Originally Programmed
# MAGIC                               Hugh Sisson     01/31/2007  -  Added second conversion extract/logic path to meet new requirement 
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                                                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                                                        ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                2008-08-28      3567(Primary Key)        Added primary key process to the job                                                        devlIDS                       Steph Goddard            09/02/2008
# MAGIC Hugh Sisson                    09/16/2008    TTR-374                       Added two new columns, including one in the natural key                                                           Steph Goddard             09/29/2008
# MAGIC Ralph Tucker                  2009-01-09       3567(Primary Key)        Moved updates from production to devl                                                    devlIDS                       Steph Goddard            02/05/2009
# MAGIC Ralph Tucker                  2010-05-06      3556 CDC                    Moved Grp lkup from extract SQL to reference link via Hashfile                 IntegrateCurDevl    
# MAGIC 
# MAGIC Akhila Manickavelu        2016-10-14          5628                           Added filter conditions to Exclude workers comp data                          IntegrateDev2              Jag Yelavarthi             2016-10-14
# MAGIC 
# MAGIC Madhavan B                  2017-08-22       5630                                Removed Conv06Extr and Conv07Extr Source Links                         IntegrateDev1                Kalyan Neelam            2017-08-29
# MAGIC Prabhu ES                     2022-03-03       S2S Remediation          MSSQL ODBC conn added and other param changes                          IntegrateDev5		Ken Bradmon	2022-06-02

# MAGIC hf_sub_pca_combined_1 and hf_sub_pca_combined_2 both point to the same hashed file - hf_sub_pca_combined
# MAGIC This container is used in:
# MAGIC FctsSubPcaExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Hash file (hf_sub_pca_allcol) cleared in calling program
# MAGIC Writing Sequential File to ../key
# MAGIC Extract Facets Data
# MAGIC Strip Carriage Return, Line Feed, and Tab characters.  Trim leading and trailing spaces.  Upcase alphabetic characters.
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('$IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')

FacetsOwner = get_widget_value('$FacetsOwner','')
facets_secret_name = get_widget_value('facets_secret_name','')

BCBSOwner = get_widget_value('$BCBSOwner','')
bcbs_secret_name = get_widget_value('bcbs_secret_name','')

CurrRunCycle = get_widget_value('CurrRunCycle','')
RunID = get_widget_value('RunID','')
TmpTblRunID = get_widget_value('TmpTblRunID','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
CurrDate = get_widget_value('CurrDate','')

# 1) Read CMC_GRGR_GROUP (ODBCConnector) from FacetsOwner
jdbc_url_facets, jdbc_props_facets = get_db_config(facets_secret_name)
extract_query_cmc = f"SELECT * FROM {FacetsOwner}.CMC_GRGR_GROUP"
df_CMC_GRGR_GROUP = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets)
    .options(**jdbc_props_facets)
    .option("query", extract_query_cmc)
    .load()
)

# 2) Scenario A Hashed File: hf_sub_pca_grp_id
#    Deduplicate on the key column GRGR_CK
df_hf_sub_pca_grp_id = df_CMC_GRGR_GROUP.dropDuplicates(["GRGR_CK"])

# 3) Read the Facets stage (ODBCConnector)
jdbc_url_facets2, jdbc_props_facets2 = get_db_config(facets_secret_name)
extract_query_facets = f"""
SELECT DISTINCT
       HSA.SBSB_CK,
       HSA.HSAI_ACC_SFX,
       HSA.SBHS_PLAN_YR_DT,
       HSA.SBHS_EFF_DT,
       HSA.GRGR_CK,
       HSA.SBHS_ALLOC_AMT,
       HSA.SBHS_CO_AMT,
       HSA.SBHS_PAID_AMT,
       HSA.SBHS_BALANCE_AMT,
       HSA.HSAL_CO_CALC_IND,
       HSA.SBHS_MAX_CO_AMT,
       SBHS_TERM_DT
FROM 
       {FacetsOwner}.CMC_SBHS_HSA_ACCUM HSA,
       tempdb..TMP_PROD_CDC_SP_DRVR DRVR
WHERE
       DRVR.SBSB_CK = HSA.SBSB_CK
       AND NOT EXISTS (
          SELECT 'Y'
          FROM 
          {BCBSOwner}.P_SEL_PRCS_CRITR P_SEL_PRCS_CRITR,
          {FacetsOwner}.CMC_GRGR_GROUP CMC_GRGR,
          {FacetsOwner}.CMC_SBSB_SUBSC SBSB
          WHERE P_SEL_PRCS_CRITR.SEL_PRCS_ID = 'WRHSINBEXCL'
          AND P_SEL_PRCS_CRITR.SEL_PRCS_ITEM_ID = 'WORKCOMP'
          AND P_SEL_PRCS_CRITR.SEL_PRCS_CRITR_CD = 'MEDPROC'
          AND CMC_GRGR.GRGR_ID >= P_SEL_PRCS_CRITR.CRITR_VAL_FROM_TX
          AND CMC_GRGR.GRGR_ID <= P_SEL_PRCS_CRITR.CRITR_VAL_THRU_TX
          AND CMC_GRGR.GRGR_CK = SBSB.GRGR_CK
          AND SBSB.SBSB_CK = DRVR.SBSB_CK
       )
"""
df_Facets = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_facets2)
    .options(**jdbc_props_facets2)
    .option("query", extract_query_facets)
    .load()
)

# 4) StripFieldsCurr (CTransformerStage)
df_StripFieldsCurr = df_Facets.select(
    F.col("SBSB_CK").alias("SBSB_CK"),
    UpCase(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", trim(F.col("HSAI_ACC_SFX")))).alias("HSAI_ACC_SFX"),
    F.col("SBHS_PLAN_YR_DT").alias("SBHS_PLAN_YR_DT"),
    F.col("SBHS_EFF_DT").alias("SBHS_EFF_DT"),
    F.col("GRGR_CK").alias("GRGR_CK"),
    F.col("SBHS_ALLOC_AMT").alias("SBHS_ALLOC_AMT"),
    F.col("SBHS_CO_AMT").alias("SBHS_CO_AMT"),
    F.col("SBHS_PAID_AMT").alias("SBHS_PAID_AMT"),
    F.col("SBHS_BALANCE_AMT").alias("SBHS_BALANCE_AMT"),
    UpCase(Convert("CHAR(10) : CHAR(13) : CHAR(9)", "", trim(F.col("HSAL_CO_CALC_IND")))).alias("HSAL_CO_CALC_IND"),
    F.col("SBHS_MAX_CO_AMT").alias("SBHS_MAX_CO_AMT"),
    F.col("SBHS_TERM_DT").alias("SBHS_TERM_DT")
)

# 5) CurrBusinessLogic (CTransformerStage)
#    Stage variable RowPassThru = "Y"
df_CurrBusinessLogic = df_StripFieldsCurr.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),  # Expression: SrcSysCdSk
    F.col("SBSB_CK").alias("SUB_UNIQ_KEY"),
    F.col("HSAI_ACC_SFX").alias("SUB_PCA_ACCUM_PFX_ID"),
    # Left(SBHS_PLAN_YR_DT,10)
    F.substring(F.col("SBHS_PLAN_YR_DT"), 1, 10).alias("PLN_YR_BEG_DT"),
    # if IsNull(Left(SBHS_EFF_DT,10)) then '1753-01-01' else Left(SBHS_EFF_DT,10)
    F.when(
        F.substring(F.col("SBHS_EFF_DT"), 1, 10).isNull(),
        F.lit("1753-01-01")
    ).otherwise(F.substring(F.col("SBHS_EFF_DT"), 1, 10)).alias("SBHS_EFF_DT"),
    # JOB_EXCTN_RCRD_ERR_SK => 0
    F.lit(0).alias("JOB_EXCTN_RCRD_ERR_SK"),
    # INSRT_UPDT_CD => "I"
    F.lit("I").alias("INSRT_UPDT_CD"),
    # DISCARD_IN => "N"
    F.lit("N").alias("DISCARD_IN"),
    # PASS_THRU_IN => StageVariable = "Y"
    F.lit("Y").alias("PASS_THRU_IN"),
    # FIRST_RECYC_DT => CurrDate
    F.lit(CurrDate).alias("FIRST_RECYC_DT"),
    # ERR_CT => 0
    F.lit(0).alias("ERR_CT"),
    # RECYCLE_CT => 0
    F.lit(0).alias("RECYCLE_CT"),
    # SRC_SYS_CD => SrcSysCd
    F.lit(SrcSysCd).alias("SRC_SYS_CD"),
    # PRI_KEY_STRING => SrcSysCd : ";" : SBSB_CK : ";" : HSAI_ACC_SFX : ";" : Left(SBHS_PLAN_YR_DT,10)
    F.concat_ws(";", F.lit(SrcSysCd), F.col("SBSB_CK"), F.col("HSAI_ACC_SFX"), F.substring(F.col("SBHS_PLAN_YR_DT"), 1, 10)).alias("PRI_KEY_STRING"),
    # SUB_PCA_SK => 0
    F.lit(0).alias("SUB_PCA_SK"),
    # CRT_RUN_CYC_EXCTN_SK => 0
    F.lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    # LAST_UPDT_RUN_CYC_EXCTN_SK => 0
    F.lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    # GRP_CK => GRGR_CK
    F.col("GRGR_CK").alias("GRP_CK"),
    # SUB => SBSB_CK
    F.col("SBSB_CK").alias("SUB"),
    # SUB_PCA_CAROVR_CALC_RULE_CD => If (IsNull(HSAL_CO_CALC_IND) or Len=0) Then "NA" Else HSAL_CO_CALC_IND
    F.when(
        F.col("HSAL_CO_CALC_IND").isNull() | (F.length(F.col("HSAL_CO_CALC_IND")) == 0),
        F.lit("NA")
    ).otherwise(F.col("HSAL_CO_CALC_IND")).alias("SUB_PCA_CAROVR_CALC_RULE_CD"),
    # SBHS_TERM_DT => if IsNull(Left(SBHS_TERM_DT,10)) then '1753-01-01' else Left(SBHS_TERM_DT,10)
    F.when(
        F.substring(F.col("SBHS_TERM_DT"), 1, 10).isNull(),
        F.lit("1753-01-01")
    ).otherwise(F.substring(F.col("SBHS_TERM_DT"), 1, 10)).alias("SBHS_TERM_DT"),
    # ALLOC_AMT => If isNull(...) or length=0 or not num(...) => 0 else col
    F.when(
        F.col("SBHS_ALLOC_AMT").isNull()
        | (F.length(trim(F.col("SBHS_ALLOC_AMT"))) == 0)
        | (Num(F.col("SBHS_ALLOC_AMT")) == False),
        F.lit(0)
    ).otherwise(F.col("SBHS_ALLOC_AMT")).alias("ALLOC_AMT"),
    # BAL_AMT
    F.when(
        F.col("SBHS_BALANCE_AMT").isNull()
        | (F.length(trim(F.col("SBHS_BALANCE_AMT"))) == 0)
        | (Num(F.col("SBHS_BALANCE_AMT")) == False),
        F.lit(0)
    ).otherwise(F.col("SBHS_BALANCE_AMT")).alias("BAL_AMT"),
    # CAROVR_AMT
    F.when(
        F.col("SBHS_CO_AMT").isNull()
        | (F.length(trim(F.col("SBHS_CO_AMT"))) == 0)
        | (Num(F.col("SBHS_CO_AMT")) == False),
        F.lit(0)
    ).otherwise(F.col("SBHS_CO_AMT")).alias("CAROVR_AMT"),
    # MAX_CAROVR_AMT
    F.when(
        F.col("SBHS_MAX_CO_AMT").isNull()
        | (F.length(trim(F.col("SBHS_MAX_CO_AMT"))) == 0)
        | (Num(F.col("SBHS_MAX_CO_AMT")) == False),
        F.lit(0)
    ).otherwise(F.col("SBHS_MAX_CO_AMT")).alias("MAX_CAROVR_AMT"),
    # PD_AMT
    F.when(
        F.col("SBHS_PAID_AMT").isNull()
        | (F.length(trim(F.col("SBHS_PAID_AMT"))) == 0)
        | (Num(F.col("SBHS_PAID_AMT")) == False),
        F.lit(0)
    ).otherwise(F.col("SBHS_PAID_AMT")).alias("PD_AMT")
)

# 6) hf_sub_pca_combined_1 (CHashedFileStage) -> scenario A, deduplicate on keys
#    Keys: SRC_SYS_CD_SK, SUB_UNIQ_KEY, SUB_PCA_ACCUM_PFX_ID, PLN_YR_BEG_DT, EFF_DT
df_hf_sub_pca_combined_1 = df_CurrBusinessLogic.dropDuplicates([
    "SRC_SYS_CD_SK","SUB_UNIQ_KEY","SUB_PCA_ACCUM_PFX_ID","PLN_YR_BEG_DT","SBHS_EFF_DT"
])

# 7) DeDup (CTransformerStage)
#    The JSON shows output columns referencing "Combined1Out" => same data, but "TERM_DT" = Combined1Out.SBHS_TERM_DT
df_DeDup = df_hf_sub_pca_combined_1.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
    F.col("PLN_YR_BEG_DT").alias("PLN_YR_BEG_DT"),
    F.col("SBHS_EFF_DT").alias("EFF_DT"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SUB_PCA_SK").alias("SUB_PCA_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_CK").alias("GRP_CK"),
    F.col("SUB").alias("SUB"),
    F.col("SUB_PCA_CAROVR_CALC_RULE_CD").alias("SUB_PCA_CAROVR_CALC_RULE_CD"),
    F.col("SBHS_TERM_DT").alias("SBHS_TERM_DT"),
    F.col("ALLOC_AMT").alias("ALLOC_AMT"),
    F.col("BAL_AMT").alias("BAL_AMT"),
    F.col("CAROVR_AMT").alias("CAROVR_AMT"),
    F.col("MAX_CAROVR_AMT").alias("MAX_CAROVR_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

# 8) hf_sub_pca_combined_2 (CHashedFileStage) -> scenario A, deduplicate again on same 5 key columns
df_hf_sub_pca_combined_2_pretmp = df_DeDup.dropDuplicates([
    "SRC_SYS_CD_SK","SUB_UNIQ_KEY","SUB_PCA_ACCUM_PFX_ID","PLN_YR_BEG_DT","EFF_DT"
])

# But note in the JSON, after DeDup the column is named "TERM_DT" instead of "SBHS_TERM_DT". So rename it for the next stage:
df_hf_sub_pca_combined_2 = df_hf_sub_pca_combined_2_pretmp.select(
    F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
    F.col("PLN_YR_BEG_DT").alias("PLN_YR_BEG_DT"),
    F.col("EFF_DT").alias("EFF_DT"),
    F.col("JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("DISCARD_IN").alias("DISCARD_IN"),
    F.col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("ERR_CT").alias("ERR_CT"),
    F.col("RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("SUB_PCA_SK").alias("SUB_PCA_SK"),
    F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("GRP_CK").alias("GRP_CK"),
    F.col("SUB").alias("SUB"),
    F.col("SUB_PCA_CAROVR_CALC_RULE_CD").alias("SUB_PCA_CAROVR_CALC_RULE_CD"),
    F.col("SBHS_TERM_DT").alias("TERM_DT"),
    F.col("ALLOC_AMT").alias("ALLOC_AMT"),
    F.col("BAL_AMT").alias("BAL_AMT"),
    F.col("CAROVR_AMT").alias("CAROVR_AMT"),
    F.col("MAX_CAROVR_AMT").alias("MAX_CAROVR_AMT"),
    F.col("PD_AMT").alias("PD_AMT")
)

# 9) Trans (CTransformerStage) => Left join with df_hf_sub_pca_grp_id on GRP_CK = GRGR_CK
df_CombOut = df_hf_sub_pca_combined_2.alias("CombOut")
df_refGrp = df_hf_sub_pca_grp_id.alias("refGrp")
condition_join = [F.col("CombOut.GRP_CK") == F.col("refGrp.GRGR_CK")]

df_TransJoined = df_CombOut.join(df_refGrp, condition_join, how="left")

# Produce two outputs: "Trans" and "AllCol" (each has the same columns but separate expressions in JSON).
# Both outputs have identical column logic except that they each reference refGrp.GRGR_ID in an if-Null expression.
# 9a) df_TransOutput
df_TransOutput = df_TransJoined.select(
    F.col("CombOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CombOut.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CombOut.SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
    F.col("CombOut.PLN_YR_BEG_DT").alias("PLN_YR_BEG_DT"),
    F.col("CombOut.EFF_DT").alias("EFF_DT"),
    F.col("CombOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("CombOut.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("CombOut.DISCARD_IN").alias("DISCARD_IN"),
    F.col("CombOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("CombOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("CombOut.ERR_CT").alias("ERR_CT"),
    F.col("CombOut.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("CombOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CombOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CombOut.SUB_PCA_SK").alias("SUB_PCA_SK"),
    F.col("CombOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CombOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("refGrp.GRGR_ID").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("refGrp.GRGR_ID")).alias("GRP"),
    F.col("CombOut.SUB").alias("SUB"),
    F.col("CombOut.SUB_PCA_CAROVR_CALC_RULE_CD").alias("SUB_PCA_CAROVR_CALC_RULE_CD"),
    F.col("CombOut.TERM_DT").alias("TERM_DT"),
    F.col("CombOut.ALLOC_AMT").alias("ALLOC_AMT"),
    F.col("CombOut.BAL_AMT").alias("BAL_AMT"),
    F.col("CombOut.CAROVR_AMT").alias("CAROVR_AMT"),
    F.col("CombOut.MAX_CAROVR_AMT").alias("MAX_CAROVR_AMT"),
    F.col("CombOut.PD_AMT").alias("PD_AMT")
)

# 9b) df_AllColOutput
df_AllColOutput = df_TransJoined.select(
    F.col("CombOut.SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    F.col("CombOut.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    F.col("CombOut.SUB_PCA_ACCUM_PFX_ID").alias("SUB_PCA_ACCUM_PFX_ID"),
    F.col("CombOut.PLN_YR_BEG_DT").alias("PLN_YR_BEG_DT"),
    F.col("CombOut.EFF_DT").alias("EFF_DT"),
    F.col("CombOut.JOB_EXCTN_RCRD_ERR_SK").alias("JOB_EXCTN_RCRD_ERR_SK"),
    F.col("CombOut.INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    F.col("CombOut.DISCARD_IN").alias("DISCARD_IN"),
    F.col("CombOut.PASS_THRU_IN").alias("PASS_THRU_IN"),
    F.col("CombOut.FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    F.col("CombOut.ERR_CT").alias("ERR_CT"),
    F.col("CombOut.RECYCLE_CT").alias("RECYCLE_CT"),
    F.col("CombOut.SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("CombOut.PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    F.col("CombOut.SUB_PCA_SK").alias("SUB_PCA_SK"),
    F.col("CombOut.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.col("CombOut.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.when(
        F.col("refGrp.GRGR_ID").isNull(),
        F.lit("UNK")
    ).otherwise(F.col("refGrp.GRGR_ID")).alias("GRP"),
    F.col("CombOut.SUB").alias("SUB"),
    F.col("CombOut.SUB_PCA_CAROVR_CALC_RULE_CD").alias("SUB_PCA_CAROVR_CALC_RULE_CD"),
    F.col("CombOut.TERM_DT").alias("TERM_DT"),
    F.col("CombOut.ALLOC_AMT").alias("ALLOC_AMT"),
    F.col("CombOut.BAL_AMT").alias("BAL_AMT"),
    F.col("CombOut.CAROVR_AMT").alias("CAROVR_AMT"),
    F.col("CombOut.MAX_CAROVR_AMT").alias("MAX_CAROVR_AMT"),
    F.col("CombOut.PD_AMT").alias("PD_AMT")
)

# 10) SubPcaPK is a Shared Container with 2 inputs, 1 output
#     Include reference and then call it
# MAGIC %run ../../../../shared_containers/PrimaryKey/SubPcaPK
# COMMAND ----------

params_container = {
    "CurrRunCycle": CurrRunCycle,
    "$IDSOwner": IDSOwner,
    "SrcSysCd": SrcSysCd
}
df_key = SubPcaPK(df_TransOutput, df_AllColOutput, params_container)

# 11) Final output: FctsSubPcaExtr (CSeqFileStage)
#     The final columns in the exact order:
final_columns = [
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "SUB_PCA_SK",
    "SUB_UNIQ_KEY",
    "SUB_PCA_ACCUM_PFX_ID",
    "PLN_YR_BEG_DT",
    "EFF_DT",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "GRP",
    "SUB",
    "SUB_PCA_CAROVR_CALC_RULE_CD",
    "TERM_DT",
    "ALLOC_AMT",
    "BAL_AMT",
    "CAROVR_AMT",
    "MAX_CAROVR_AMT",
    "PD_AMT"
]

# For columns of type char or varchar, we must rpad to the specified length or <...> if unknown.
# From the metadata:
#  INSRT_UPDT_CD -> char(10)
#  DISCARD_IN -> char(1)
#  PASS_THRU_IN -> char(1)
#  PLN_YR_BEG_DT -> char(10)
#  EFF_DT -> char(10)
#  GRP -> char(8)
#  SUB_PCA_CAROVR_CALC_RULE_CD -> char(1)
#  TERM_DT -> char(10)
#  SRC_SYS_CD, PRI_KEY_STRING, SUB_PCA_ACCUM_PFX_ID => varchar => length unknown => use <...>
df_final = df_key.select(
    F.col("JOB_EXCTN_RCRD_ERR_SK"),
    rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    F.col("FIRST_RECYC_DT"),
    F.col("ERR_CT"),
    F.col("RECYCLE_CT"),
    rpad(F.col("SRC_SYS_CD"), F.lit(<...>), " ").alias("SRC_SYS_CD"),
    rpad(F.col("PRI_KEY_STRING"), F.lit(<...>), " ").alias("PRI_KEY_STRING"),
    F.col("SUB_PCA_SK"),
    F.col("SUB_UNIQ_KEY"),
    rpad(F.col("SUB_PCA_ACCUM_PFX_ID"), F.lit(<...>), " ").alias("SUB_PCA_ACCUM_PFX_ID"),
    rpad(F.col("PLN_YR_BEG_DT"), 10, " ").alias("PLN_YR_BEG_DT"),
    rpad(F.col("EFF_DT"), 10, " ").alias("EFF_DT"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    rpad(F.col("GRP"), 8, " ").alias("GRP"),
    F.col("SUB"),
    rpad(F.col("SUB_PCA_CAROVR_CALC_RULE_CD"), 1, " ").alias("SUB_PCA_CAROVR_CALC_RULE_CD"),
    rpad(F.col("TERM_DT"), 10, " ").alias("TERM_DT"),
    F.col("ALLOC_AMT"),
    F.col("BAL_AMT"),
    F.col("CAROVR_AMT"),
    F.col("MAX_CAROVR_AMT"),
    F.col("PD_AMT")
)

# Write the final file
file_path = f"{adls_path}/key/FctsSubPcaExtr.SubPca.dat.{RunID}"
write_files(
    df_final,
    file_path,
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)