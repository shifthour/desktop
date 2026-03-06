# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_1 03/10/09 09:25:21 Batch  15045_33943 INIT bckcetl ids20 dcg01 sa Bringing ALL Claim code down to devlIDS
# MAGIC ^1_3 02/10/09 11:16:45 Batch  15017_40611 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 02/28/08 13:33:27 Batch  14669_48847 PROMOTE bckcetl ids20 dsadm rc for olliver 
# MAGIC ^1_2 02/28/08 13:29:07 Batch  14669_48563 INIT bckcett testIDS dsadm rc for oliver
# MAGIC ^1_1 02/27/08 10:14:57 Batch  14668_36903 PROMOTE bckcett testIDS u03651 steph for Ollie
# MAGIC ^1_1 02/27/08 10:13:12 Batch  14668_36797 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_1 01/29/08 08:55:59 Batch  14639_32164 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 12/28/07 10:13:19 Batch  14607_36806 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 11/26/07 09:37:10 Batch  14575_34636 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 10/02/07 14:11:36 Batch  14520_51104 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_1 10/02/07 13:36:29 Batch  14520_48996 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_2 09/30/07 15:50:06 Batch  14518_57011 PROMOTE bckcett testIDS30 u03651 staeffy
# MAGIC ^1_2 09/30/07 15:29:43 Batch  14518_55788 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_2 09/29/07 17:49:00 Batch  14517_64144 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 09/26/07 12:32:13 Batch  14514_45135 INIT bckcett devlIDS30 u03651 steffy
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC CALLED BY:     IdsClmBalCntl
# MAGIC 
# MAGIC PROCESSING:
# MAGIC                   
# MAGIC                 The Job Updates the P_RUN_CYC table with the ROW_TO_ROW_BAL_IN and CLMN_SUM_BAL_IN set to 'Y' after balancing is done for the subject area
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                             Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------      ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Bhoomi Dasari                 09/12/2007          3264                              Originally Programmed                           devlIDS30                    Steph Goddard           09/28/2007
# MAGIC Oliver Nielsen                  02/25/2008     Prod supp                       Added ROW_CT_BAL_IN into SQL       devlIDS                           Steph Goddard          02/28/2008
# MAGIC                                                                                                        in query and update stages

# MAGIC Update the P_RUN_CYC table with Balancing Indicators set to 'Y' to indicate source system data has been balanced with respect to IDS for each of the run cycles processed.
# MAGIC Extract P_RUN_CYC records for IDS claims with a value \"N\" in the  ROW_CT_BAL_IN, ROW_TO_ROW_BAL_IN and CLMN_SUM_BAL_IN where the run cycle is >= to the current run cycle used for extracting.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
# COMMAND ----------
# MAGIC %run ../../../../Utility_Integrate
# COMMAND ----------


IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
BeginCycle = get_widget_value('BeginCycle','')
SrcSys = get_widget_value('SrcSys','')
TrgtSys = get_widget_value('TrgtSys','')
SubjCd = get_widget_value('SubjCd','')

jdbc_url, jdbc_props = get_db_config(ids_secret_name)

extract_query = f"""
SELECT
    SRC_SYS_CD,
    TRGT_SYS_CD,
    SUBJ_CD,
    max(RUN_CYC_NO) as RUN_CYC_NO
FROM {IDSOwner}.P_RUN_CYC
WHERE TRGT_SYS_CD = '{TrgtSys}'
  AND SRC_SYS_CD = '{SrcSys}'
  AND SUBJ_CD = '{SubjCd}'
  AND ROW_TO_ROW_BAL_IN = 'N'
  AND CLMN_SUM_BAL_IN = 'N'
  AND ROW_CT_BAL_IN = 'N'
  AND RUN_CYC_NO >= {BeginCycle}
GROUP BY
    SRC_SYS_CD,
    TRGT_SYS_CD,
    SUBJ_CD
"""

df_IDS = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("query", extract_query)
    .load()
)

df_BusinessRules = df_IDS.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("SUBJ_CD").alias("SUBJ_CD"),
    col("TRGT_SYS_CD").alias("TRGT_SYS_CD"),
    col("RUN_CYC_NO").alias("RUN_CYC_NO")
)

df_BusinessRules.write.format("jdbc") \
    .option("url", jdbc_url) \
    .options(**jdbc_props) \
    .option("dbtable", "STAGING.BalClmRunCycUpd_P_RUN_CYC_temp") \
    .mode("overwrite") \
    .save()

merge_sql = f"""
MERGE {IDSOwner}.P_RUN_CYC AS T
USING STAGING.BalClmRunCycUpd_P_RUN_CYC_temp AS S
ON
    T.SRC_SYS_CD = S.SRC_SYS_CD
    AND T.SUBJ_CD = S.SUBJ_CD
    AND T.TRGT_SYS_CD = S.TRGT_SYS_CD
    AND T.RUN_CYC_NO <= S.RUN_CYC_NO
    AND T.ROW_CT_BAL_IN = 'N'
    AND T.ROW_TO_ROW_BAL_IN = 'N'
    AND T.CLMN_SUM_BAL_IN = 'N'
WHEN MATCHED THEN
    UPDATE SET
        T.ROW_CT_BAL_IN = 'Y',
        T.ROW_TO_ROW_BAL_IN = 'Y',
        T.CLMN_SUM_BAL_IN = 'Y'
WHEN NOT MATCHED THEN
    INSERT (
        SRC_SYS_CD,
        SUBJ_CD,
        TRGT_SYS_CD,
        RUN_CYC_NO,
        ROW_CT_BAL_IN,
        ROW_TO_ROW_BAL_IN,
        CLMN_SUM_BAL_IN
    )
    VALUES (
        S.SRC_SYS_CD,
        S.SUBJ_CD,
        S.TRGT_SYS_CD,
        S.RUN_CYC_NO,
        'Y',
        'Y',
        'Y'
    );
"""

execute_dml(merge_sql, jdbc_url, jdbc_props)