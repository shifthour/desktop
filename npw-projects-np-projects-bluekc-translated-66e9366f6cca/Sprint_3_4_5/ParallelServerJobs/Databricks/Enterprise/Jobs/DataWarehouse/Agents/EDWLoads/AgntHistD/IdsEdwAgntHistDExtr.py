# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC JOB NAME:     EdwAgntHistExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from ids and edw and checks if the effective or term dates have changed
# MAGIC   The comparison will determine rows that have changed and whether to generate a new history record.
# MAGIC       
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:     AGNT
# MAGIC                             AGNT_INDV
# MAGIC 	EDW:  AGNT_HIST
# MAGIC   
# MAGIC HASH FILES:
# MAGIC                 hf_cdma_codes   - Load the CDMA codes to get cd_mppng_sk, target_cd and Target_cd_nm  from ids code mapping table
# MAGIC                 hf_agnthist_taxid
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC 
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   IDS - read from source all records for a LAST_UPDT_RUN_CYCLE , lookup all code SK values and get the natural codes
# MAGIC                   EDW - Read from source all records that have Y for EDW_CUR_RCRD_IN
# MAGIC                   If IDS record not on EDW, add it
# MAGIC                   If IDS record on EDW and nothing different bettween the two records, then update the Last Run Cycle and Date on EDW record, write out and load
# MAGIC                   If IDS record has differnet Effect date, set EDW record EDW_CURR_RCRD_IN = "N", update EDW_RCRD_END_DT 
# MAGIC                                                                          and make IDS record have EDW_CURR_RCRD_IN = "Y", update EDW_RCRD_END_DT = '2199-12-31'
# MAGIC                   If IDS record has differnet Term date, set EDW record EDW_CURR_RCRD_IN = "N", update EDW_RCRD_END_DT 
# MAGIC                                                                         and make IDS record have EDW_CURR_RCRD_IN = "Y", update EDW_RCRD_END_DT = '2199-12-31'
# MAGIC   
# MAGIC OUTPUTS: 
# MAGIC                     Each of the stream output hash files
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC                                                                                                                                                                                                                 DATASTAGE               CODE                           DATE
# MAGIC DEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW
# MAGIC -----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------
# MAGIC 
# MAGIC Ralph Tucker               12/21/2005-                                          Originally Programmed
# MAGIC 
# MAGIC Rama Kamjula              12/10/2013      #5114                                      Rewritten to parallel from Server version                                   EnterpriseWrhsDevl      Jag Yelavarthi            2013-12-22

# MAGIC JobName: IdsEdwAgntHistDExtr
# MAGIC 
# MAGIC Job creates loadfile for AGNT_HIST_D  in EDW
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, monotonically_increasing_id
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------

# MAGIC %python

# ----------------------------------------------------------------
# Retrieve parameters as required
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrRunCycleDate = get_widget_value('CurrRunCycleDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSRunCycle = get_widget_value('IDSRunCycle','')

# ----------------------------------------------------------------
# db2_EDW_AgntHist (DB2ConnectorPX) - Read from EDW
query_db2_EDW_AgntHist = f"""
SELECT
 AGNT_HIST_D.AGNT_SK,
AGNT_HIST_D.EDW_RCRD_STRT_DT_SK,
AGNT_HIST_D.SRC_SYS_CD,
AGNT_HIST_D.AGNT_ID,
AGNT_HIST_D.CRT_RUN_CYC_EXCTN_DT_SK,
AGNT_HIST_D.LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
AGNT_HIST_D.EDW_CUR_RCRD_IN,
AGNT_HIST_D.EDW_RCRD_END_DT_SK,
AGNT_HIST_D.AGNT_INDV_ID,
AGNT_HIST_D.AGNT_INDV_ID_CHG_IN,
AGNT_HIST_D.CRT_RUN_CYC_EXCTN_SK,
AGNT_HIST_D.LAST_UPDT_RUN_CYC_EXCTN_SK,
AGNT_HIST_D.AGNT_INDV_LAST_NM,
AGNT_HIST_D.AGNT_INDV_LAST_NM_CHG_IN,
AGNT_HIST_D.AGNT_INDV_SSN,
AGNT_HIST_D.AGNT_INDV_SSN_CHG_IN,
AGNT_HIST_D.AGNT_NM,
AGNT_HIST_D.AGNT_NM_CHG_IN,
AGNT_HIST_D.TAX_ID,
AGNT_HIST_D.TAX_ID_CHG_IN,
AGNT_HIST_D.AGNT_SK  AGNT_SK_H

FROM {EDWOwner}.AGNT_HIST_D AGNT_HIST_D

WHERE
 AGNT_HIST_D.EDW_RCRD_STRT_DT_SK = (
     SELECT MAX(AGNT_HIST_D2.EDW_RCRD_STRT_DT_SK)
       FROM {EDWOwner}.AGNT_HIST_D AGNT_HIST_D2
      WHERE AGNT_HIST_D.AGNT_SK = AGNT_HIST_D2.AGNT_SK
)
"""

jdbc_url_EDW, jdbc_props_EDW = get_db_config(edw_secret_name)
df_db2_EDW_AgntHist = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_EDW)
    .options(**jdbc_props_EDW)
    .option("query", query_db2_EDW_AgntHist)
    .load()
)

# ----------------------------------------------------------------
# db2_AGNT (DB2ConnectorPX) - Read from IDS
query_db2_AGNT = f"""
SELECT 
AGNT.AGNT_SK,
COALESCE(CD.TRGT_CD, 'UNK') SRC_SYS_CD,
AGNT.AGNT_ID,
AGNT.CRT_RUN_CYC_EXCTN_SK,
AGNT.LAST_UPDT_RUN_CYC_EXCTN_SK,
AGNT.TAX_DMGRPHC_SK,
AGNT.AGNT_INDV_ID,
AGNT.AGNT_NM,
AGNT_INDV.SSN,
AGNT_INDV.LAST_NM
FROM {IDSOwner}.AGNT AGNT
INNER JOIN {IDSOwner}.AGNT_INDV AGNT_INDV
   ON AGNT.AGNT_INDV_SK = AGNT_INDV.AGNT_INDV_SK
LEFT JOIN {IDSOwner}.CD_MPPNG CD
   ON CD.CD_MPPNG_SK = AGNT.SRC_SYS_CD_SK
WHERE 
 AGNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSRunCycle}
"""

jdbc_url_IDS, jdbc_props_IDS = get_db_config(ids_secret_name)
df_db2_AGNT = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_AGNT)
    .load()
)

# ----------------------------------------------------------------
# db2_TaxID (DB2ConnectorPX) - Read from IDS
query_db2_TaxID = f"""
SELECT
TAX_DMGRPHC_SK,
TAX_ID,
TAX_DMGRPHC_SK TAX_DMGRPHC_SK1
FROM {IDSOwner}.TAX_DMGRPHC
"""

df_db2_TaxID = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_IDS)
    .options(**jdbc_props_IDS)
    .option("query", query_db2_TaxID)
    .load()
)

# ----------------------------------------------------------------
# lkp_Codes1 (PxLookup) - Left Join AGNT -> TAX
df_lkp_Codes1 = (
    df_db2_AGNT.alias("lnk_Agnt_In")
    .join(
        df_db2_TaxID.alias("lnk_TaxId"),
        on=[col("lnk_Agnt_In.TAX_DMGRPHC_SK") == col("lnk_TaxId.TAX_DMGRPHC_SK")],
        how="left"
    )
    .select(
        col("lnk_Agnt_In.AGNT_SK").alias("AGNT_SK"),
        col("lnk_Agnt_In.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Agnt_In.AGNT_ID").alias("AGNT_ID"),
        col("lnk_Agnt_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_Agnt_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_Agnt_In.TAX_DMGRPHC_SK").alias("TAX_DMGRPHC_SK"),
        col("lnk_Agnt_In.AGNT_INDV_ID").alias("AGNT_INDV_ID"),
        col("lnk_Agnt_In.AGNT_NM").alias("AGNT_NM"),
        col("lnk_Agnt_In.SSN").alias("SSN"),
        col("lnk_Agnt_In.LAST_NM").alias("LAST_NM"),
        col("lnk_TaxId.TAX_ID").alias("TAX_ID"),
        col("lnk_TaxId.TAX_DMGRPHC_SK1").alias("TAX_DMGRPHC_SK1")
    )
)

# ----------------------------------------------------------------
# xfm_BusinessLogic1 (CTransformerStage)
# If Trim(TAX_ID) = '' or IsNull(TAX_ID) => TAX_DMGRPHC_SK1 else TAX_ID
df_xfm_BusinessLogic1 = (
    df_lkp_Codes1
    .withColumn(
        "TAX_ID",
        when(
            (trim(col("TAX_ID")) == "") | (col("TAX_ID").isNull()),
            col("TAX_DMGRPHC_SK1")
        ).otherwise(col("TAX_ID"))
    )
    .select(
        "AGNT_SK",
        "SRC_SYS_CD",
        "AGNT_ID",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "TAX_DMGRPHC_SK",
        "AGNT_INDV_ID",
        "AGNT_NM",
        "SSN",
        "LAST_NM",
        "TAX_ID"
    )
)

# ----------------------------------------------------------------
# lkp_Codes2 (PxLookup) - Left Join with EDW AGNT_HIST
df_lkp_Codes2 = (
    df_xfm_BusinessLogic1.alias("lnk_AgntTaxID_In")
    .join(
        df_db2_EDW_AgntHist.alias("lnk_AgntHist_In"),
        on=[col("lnk_AgntTaxID_In.AGNT_SK") == col("lnk_AgntHist_In.AGNT_SK")],
        how="left"
    )
    .select(
        col("lnk_AgntTaxID_In.AGNT_SK").alias("AGNT_SK"),
        col("lnk_AgntTaxID_In.SRC_SYS_CD").alias("SRC_SYS_CD_S"),
        col("lnk_AgntTaxID_In.AGNT_ID").alias("AGNT_ID_S"),
        col("lnk_AgntTaxID_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK_S"),
        col("lnk_AgntTaxID_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK_S"),
        col("lnk_AgntTaxID_In.TAX_DMGRPHC_SK").alias("TAX_DMGRPHC_SK_S"),
        col("lnk_AgntTaxID_In.AGNT_INDV_ID").alias("AGNT_INDV_ID_S"),
        col("lnk_AgntTaxID_In.AGNT_NM").alias("AGNT_NM_S"),
        col("lnk_AgntTaxID_In.SSN").alias("SSN_S"),
        col("lnk_AgntTaxID_In.LAST_NM").alias("LAST_NM_S"),
        col("lnk_AgntTaxID_In.TAX_ID").alias("TAX_ID_S"),
        col("lnk_AgntHist_In.EDW_RCRD_STRT_DT_SK").alias("EDW_RCRD_STRT_DT_SK_H"),
        col("lnk_AgntHist_In.SRC_SYS_CD").alias("SRC_SYS_CD_H"),
        col("lnk_AgntHist_In.AGNT_ID").alias("AGNT_ID_H"),
        col("lnk_AgntHist_In.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK_H"),
        col("lnk_AgntHist_In.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK_H"),
        col("lnk_AgntHist_In.EDW_CUR_RCRD_IN").alias("EDW_CUR_RCRD_IN_H"),
        col("lnk_AgntHist_In.EDW_RCRD_END_DT_SK").alias("EDW_RCRD_END_DT_SK_H"),
        col("lnk_AgntHist_In.AGNT_INDV_ID").alias("AGNT_INDV_ID_H"),
        col("lnk_AgntHist_In.AGNT_INDV_ID_CHG_IN").alias("AGNT_INDV_ID_CHG_IN_H"),
        col("lnk_AgntHist_In.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK_H"),
        col("lnk_AgntHist_In.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK_H"),
        col("lnk_AgntHist_In.AGNT_INDV_LAST_NM").alias("AGNT_INDV_LAST_NM_H"),
        col("lnk_AgntHist_In.AGNT_INDV_LAST_NM_CHG_IN").alias("AGNT_INDV_LAST_NM_CHG_IN_H"),
        col("lnk_AgntHist_In.AGNT_INDV_SSN").alias("AGNT_INDV_SSN_H"),
        col("lnk_AgntHist_In.AGNT_INDV_SSN_CHG_IN").alias("AGNT_INDV_SSN_CHG_IN_H"),
        col("lnk_AgntHist_In.AGNT_NM").alias("AGNT_NM_H"),
        col("lnk_AgntHist_In.AGNT_NM_CHG_IN").alias("AGNT_NM_CHG_IN_H"),
        col("lnk_AgntHist_In.TAX_ID").alias("TAX_ID_H"),
        col("lnk_AgntHist_In.TAX_ID_CHG_IN").alias("TAX_ID_CHG_IN_H"),
        col("lnk_AgntHist_In.AGNT_SK_H").alias("AGNT_SK_H")
    )
)

# ----------------------------------------------------------------
# xfm_BusinessLogic2 (CTransformerStage) - Implement stage variables
df_xfm_BusinessLogic2 = (
    df_lkp_Codes2
    .withColumn(
        "svAgntIndvIdChange",
        when(
            trim(col("AGNT_INDV_ID_S")) != trim(col("AGNT_INDV_ID_H")),
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "svAgntLastNameChange",
        when(
            trim(
                when(col("LAST_NM_S").isNull(), lit("")).otherwise(col("LAST_NM_S")))
            != trim(
                when(col("AGNT_INDV_LAST_NM_H").isNull(), lit("")).otherwise(col("AGNT_INDV_LAST_NM_H")))
            ,
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "svAgntIndvSsnChange",
        when(
            trim(
                when(col("SSN_S").isNull(), lit("")).otherwise(col("SSN_S")))
            != trim(
                when(col("AGNT_INDV_SSN_H").isNull(), lit("")).otherwise(col("AGNT_INDV_SSN_H")))
            ,
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "svAgntNmChange",
        when(
            trim(col("AGNT_NM_S")) != trim(col("AGNT_NM_H")),
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "svTaxIdChange",
        when(
            trim(col("TAX_ID_S")) != trim(col("TAX_ID_H")),
            lit("Y")
        ).otherwise(lit("N"))
    )
    .withColumn(
        "CreateNewRecord",
        when(
            (col("svAgntIndvIdChange") == "Y")
            | (col("svAgntLastNameChange") == "Y")
            | (col("svAgntIndvSsnChange") == "Y")
            | (col("svAgntNmChange") == "Y")
            | (col("svTaxIdChange") == "Y"),
            lit("Y")
        ).otherwise(lit("N"))
    )
)

# We introduce a row counter to replicate constraints on certain links
df_temp = df_xfm_BusinessLogic2.withColumn("ds_row_num", monotonically_increasing_id() + 1)

# ----------------------------------------------------------------
# Link lnk_NA
df_lnk_NA = (
    df_temp
    .filter("ds_row_num = 1")
    .select(
        lit(1).alias("AGNT_SK"),
        rpad(lit("1753-01-01"), 10, " ").alias("EDW_RCRD_STRT_DT_SK"),
        lit("NA").alias("SRC_SYS_CD"),
        lit("NA").alias("AGNT_ID"),
        rpad(lit("1753-01-01"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit("1753-01-01"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit("N"), 1, " ").alias("EDW_CUR_RCRD_IN"),
        rpad(lit("1753-01-01"), 10, " ").alias("EDW_RCRD_END_DT_SK"),
        lit("NA").alias("AGNT_INDV_ID"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_ID_CHG_IN"),
        lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("NA").alias("AGNT_INDV_LAST_NM"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_LAST_NM_CHG_IN"),
        lit(None).alias("AGNT_INDV_SSN"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_SSN_CHG_IN"),
        lit("NA").alias("AGNT_NM"),
        rpad(lit("N"), 1, " ").alias("AGNT_NM_CHG_IN"),
        lit("NA").alias("TAX_ID"),
        rpad(lit("N"), 1, " ").alias("TAX_ID_CHG_IN")
    )
)

# ----------------------------------------------------------------
# Link lnk_UNK
df_lnk_UNK = (
    df_temp
    .filter("ds_row_num = 1")
    .select(
        lit(0).alias("AGNT_SK"),
        rpad(lit("1753-01-01"), 10, " ").alias("EDW_RCRD_STRT_DT_SK"),
        lit("UNK").alias("SRC_SYS_CD"),
        lit("UNK").alias("AGNT_ID"),
        rpad(lit("1753-01-01"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit("1753-01-01"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit("N"), 1, " ").alias("EDW_CUR_RCRD_IN"),
        rpad(lit("1753-01-01"), 10, " ").alias("EDW_RCRD_END_DT_SK"),
        lit("UNK").alias("AGNT_INDV_ID"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_ID_CHG_IN"),
        lit(100).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(100).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit("UNK").alias("AGNT_INDV_LAST_NM"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_LAST_NM_CHG_IN"),
        lit(None).alias("AGNT_INDV_SSN"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_SSN_CHG_IN"),
        lit("UNK").alias("AGNT_NM"),
        rpad(lit("N"), 1, " ").alias("AGNT_NM_CHG_IN"),
        lit("UNK").alias("TAX_ID"),
        rpad(lit("N"), 1, " ").alias("TAX_ID_CHG_IN")
    )
)

# ----------------------------------------------------------------
# Link lnk_Current
df_lnk_Current = (
    df_temp
    .filter("AGNT_SK_H is not null AND CreateNewRecord='Y' AND AGNT_SK != 0 AND AGNT_SK != 1")
    .select(
        col("AGNT_SK"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("EDW_RCRD_STRT_DT_SK"),
        col("SRC_SYS_CD_S").alias("SRC_SYS_CD"),
        col("AGNT_ID_S").alias("AGNT_ID"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit("Y"), 1, " ").alias("EDW_CUR_RCRD_IN"),
        rpad(lit("2199-12-31"), 10, " ").alias("EDW_RCRD_END_DT_SK"),
        col("AGNT_INDV_ID_S").alias("AGNT_INDV_ID"),
        rpad(when(col("svAgntIndvIdChange") == "Y", lit("Y")).otherwise("N"), 1, " ").alias("AGNT_INDV_ID_CHG_IN"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK_S").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("LAST_NM_S").alias("AGNT_INDV_LAST_NM"),
        rpad(when(col("svAgntLastNameChange") == "Y", lit("Y")).otherwise("N"), 1, " ").alias("AGNT_INDV_LAST_NM_CHG_IN"),
        col("SSN_S").alias("AGNT_INDV_SSN"),
        rpad(when(col("svAgntIndvSsnChange") == "Y", lit("Y")).otherwise("N"), 1, " ").alias("AGNT_INDV_SSN_CHG_IN"),
        col("AGNT_NM_S").alias("AGNT_NM"),
        rpad(when(col("svAgntNmChange") == "Y", lit("Y")).otherwise("N"), 1, " ").alias("AGNT_NM_CHG_IN"),
        col("TAX_ID_S").alias("TAX_ID"),
        rpad(when(col("svTaxIdChange") == "Y", lit("Y")).otherwise("N"), 1, " ").alias("TAX_ID_CHG_IN")
    )
)

# ----------------------------------------------------------------
# Link lnk_OldCurrent
df_lnk_OldCurrent = (
    df_temp
    .filter("AGNT_SK_H is not null AND CreateNewRecord='Y' AND AGNT_SK != 0 AND AGNT_SK != 1")
    .select(
        col("AGNT_SK"),
        rpad(col("EDW_RCRD_STRT_DT_SK_H"), 10, " ").alias("EDW_RCRD_STRT_DT_SK"),
        col("SRC_SYS_CD_H").alias("SRC_SYS_CD"),
        col("AGNT_ID_H").alias("AGNT_ID"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit("N"), 1, " ").alias("EDW_CUR_RCRD_IN"),
        rpad(FIND_DATE_EE(lit(CurrRunCycleDate), lit(-1), lit("D"), lit("X"), lit("CCYY-MM-DD")), 10, " ").alias("EDW_RCRD_END_DT_SK"),
        col("AGNT_INDV_ID_H").alias("AGNT_INDV_ID"),
        rpad(col("AGNT_INDV_ID_CHG_IN_H"), 1, " ").alias("AGNT_INDV_ID_CHG_IN"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK_S").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("AGNT_INDV_LAST_NM_H").alias("AGNT_INDV_LAST_NM"),
        rpad(col("AGNT_INDV_LAST_NM_CHG_IN_H"), 1, " ").alias("AGNT_INDV_LAST_NM_CHG_IN"),
        col("AGNT_INDV_SSN_H").alias("AGNT_INDV_SSN"),
        rpad(col("AGNT_INDV_SSN_CHG_IN_H"), 1, " ").alias("AGNT_INDV_SSN_CHG_IN"),
        when(
            col("AGNT_NM_H").isNull() | (trim(col("AGNT_NM_H")) == ""),
            lit("")
        ).otherwise(col("AGNT_NM_H")).alias("AGNT_NM"),
        rpad(col("AGNT_NM_CHG_IN_H"), 1, " ").alias("AGNT_NM_CHG_IN"),
        col("TAX_ID_H").alias("TAX_ID"),
        rpad(col("TAX_ID_CHG_IN_H"), 1, " ").alias("TAX_ID_CHG_IN")
    )
)

# ----------------------------------------------------------------
# Link lnk_Update
df_lnk_Update = (
    df_temp
    .filter("AGNT_SK_H is not null AND CreateNewRecord='N' AND AGNT_SK != 0 AND AGNT_SK != 1")
    .select(
        col("AGNT_SK"),
        rpad(col("EDW_RCRD_STRT_DT_SK_H"), 10, " ").alias("EDW_RCRD_STRT_DT_SK"),
        col("SRC_SYS_CD_S").alias("SRC_SYS_CD"),
        col("AGNT_ID_S").alias("AGNT_ID"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        rpad(col("EDW_CUR_RCRD_IN_H"), 1, " ").alias("EDW_CUR_RCRD_IN"),
        rpad(col("EDW_RCRD_END_DT_SK_H"), 10, " ").alias("EDW_RCRD_END_DT_SK"),
        col("AGNT_INDV_ID_S").alias("AGNT_INDV_ID"),
        rpad(col("AGNT_INDV_ID_CHG_IN_H"), 1, " ").alias("AGNT_INDV_ID_CHG_IN"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK_H").alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("LAST_NM_S").alias("AGNT_INDV_LAST_NM"),
        rpad(col("AGNT_INDV_LAST_NM_CHG_IN_H"), 1, " ").alias("AGNT_INDV_LAST_NM_CHG_IN"),
        col("SSN_S").alias("AGNT_INDV_SSN"),
        rpad(col("AGNT_INDV_SSN_CHG_IN_H"), 1, " ").alias("AGNT_INDV_SSN_CHG_IN"),
        col("AGNT_NM_S").alias("AGNT_NM"),
        rpad(col("AGNT_NM_CHG_IN_H"), 1, " ").alias("AGNT_NM_CHG_IN"),
        col("TAX_ID_S").alias("TAX_ID"),
        rpad(col("TAX_ID_CHG_IN_H"), 1, " ").alias("TAX_ID_CHG_IN")
    )
)

# ----------------------------------------------------------------
# Link lnk_New
df_lnk_New = (
    df_temp
    .filter("AGNT_SK_H is null AND AGNT_SK != 0 AND AGNT_SK != 1")
    .select(
        col("AGNT_SK"),
        rpad(lit("1753-01-01"), 10, " ").alias("EDW_RCRD_STRT_DT_SK"),
        col("SRC_SYS_CD_S").alias("SRC_SYS_CD"),
        col("AGNT_ID_S").alias("AGNT_ID"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit(CurrRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
        rpad(lit("Y"), 1, " ").alias("EDW_CUR_RCRD_IN"),
        rpad(lit("2199-12-31"), 10, " ").alias("EDW_RCRD_END_DT_SK"),
        col("AGNT_INDV_ID_S").alias("AGNT_INDV_ID"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_ID_CHG_IN"),
        lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("LAST_NM_S").alias("AGNT_INDV_LAST_NM"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_LAST_NM_CHG_IN"),
        col("SSN_S").alias("AGNT_INDV_SSN"),
        rpad(lit("N"), 1, " ").alias("AGNT_INDV_SSN_CHG_IN"),
        col("AGNT_NM_S").alias("AGNT_NM"),
        rpad(lit("N"), 1, " ").alias("AGNT_NM_CHG_IN"),
        col("TAX_ID_S").alias("TAX_ID"),
        rpad(lit("N"), 1, " ").alias("TAX_ID_CHG_IN")
    )
)

# ----------------------------------------------------------------
# fnl_Combine (PxFunnel)
df_fnl_Combine = (
    df_lnk_NA
    .unionByName(df_lnk_UNK)
    .unionByName(df_lnk_Current)
    .unionByName(df_lnk_OldCurrent)
    .unionByName(df_lnk_Update)
    .unionByName(df_lnk_New)
)

# ----------------------------------------------------------------
# seq_AGNT_HIST_D (PxSequentialFile) - Write to .dat
# Column order as in the funnel output definition
df_seq_AGNT_HIST_D = df_fnl_Combine.select(
    "AGNT_SK",
    "EDW_RCRD_STRT_DT_SK",
    "SRC_SYS_CD",
    "AGNT_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "EDW_CUR_RCRD_IN",
    "EDW_RCRD_END_DT_SK",
    "AGNT_INDV_ID",
    "AGNT_INDV_ID_CHG_IN",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "AGNT_INDV_LAST_NM",
    "AGNT_INDV_LAST_NM_CHG_IN",
    "AGNT_INDV_SSN",
    "AGNT_INDV_SSN_CHG_IN",
    "AGNT_NM",
    "AGNT_NM_CHG_IN",
    "TAX_ID",
    "TAX_ID_CHG_IN"
)

write_files(
    df_seq_AGNT_HIST_D,
    f"{adls_path}/load/AGNT_HIST_D.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)