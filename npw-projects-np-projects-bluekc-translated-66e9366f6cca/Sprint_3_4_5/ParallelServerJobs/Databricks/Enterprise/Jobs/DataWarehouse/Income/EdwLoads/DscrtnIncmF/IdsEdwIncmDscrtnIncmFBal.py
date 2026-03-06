# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 10;hf_dscrtn_incm_non_allocated;hf_dscrtn_incm_allocated1;hf_dscrtn_incm_allocated2;hf_dscrtn_incm_allocated3;hf_dscrtn_incm_allocated4;hf_dscrtn_incm_allocated5;hf_dscrtn_incm_allocated6;hf_dscrtn_incm_allocated7;hf_dscrtn_incm_allocated8;hf_dscrtn_incm_all_allocated_recs
# MAGIC 
# MAGIC JOB NAME:     EdwIncomeAllocateDscrtnIncmF
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC Descretionary Income - Earned Premium Income either at a Grp, Subgroup, Class, ClassPlan, Product or at a Subscriber Level.    In IDS Income Discretionary, there is 1 record in the table with a the total income amount and the total number of months for which this income is for.  In EDW Discretionary Income Fact, this 1 IDS record of income is reflected by many EDW records, each record relecting the each earned month and that month's share of the total income amount.
# MAGIC 
# MAGIC Only for premium at a Grp, Subgroup, Class, ClassPlan, Product level, the income can be for and allocated over many different subgroups, class, class plan, products.     Once the money is divided by the quantity months (EdwIncomeDsctrnIncmFExtr), then EdwIncomeAllocateDscrtnIncmF splits the money across the different Subgroup, Class, ClassPlan, Products for that Group.     EdwIncomeAllocateDscrtnIncmF is only needed for records that are non-subscriber level and has a 0 or 1 for either Subgroup, Class, ClassPlan, Product.    It matches all that it knows to W_MBR_RCST (a working table of Member Recast Counts)  to get all of the possible combinations of Subgroup, Class, ClassPlan, Product.    A record for all possible combinations is written out and the monies is divided by the percentage of members in that Subgroup, Class, ClassPlan, Product.
# MAGIC 
# MAGIC Once the money is divided amoung all possible Subgroup, Class, ClassPlan, Product for that Group, further balancing on the amounts are needed.  Due to rounding issues when multiplying the amounts by  the membership percentage, when all of the amounts are added back up, they do not equal the IDS amounts.   EdwIncomBalanceDscrtnIncmF adds to or subtracts from the EDW amounts so that it will balance IDS.
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	EDW:    W_DSCTRN_INCM
# MAGIC                              W_MBR_RCST (created in EDW\\Income\\W_Member_Recast\\EdwIncomeWMbrCntsForAllocation)
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                
# MAGIC    Reads the working table W_DSCRTN_INCM and tries to match it by group, subgroup, class, ..to records on the W_MBR_RCST.  If found, it can determine the percentage of membership that belonged in the product and will divide the amount fields by that        percentage
# MAGIC    Ran after EdwIncomeDscrnIncmFExtr and before EdwIncomeBalanceDscrtnIncmF
# MAGIC    If the Class Pln is 0, then read W_MBR_RCST for all class plans for that group, subgroup, class, and product.   Build a W_DSCRNT_INCM record for each and divide the amounts by the percentage of members in each.
# MAGIC    Check for any group, subgroup, class, class plan and products not in W_MBR_RCST and don't do anything with the money.
# MAGIC    Write all records back out ot W_DSCRTN_INCM.dat    
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   An EDW load file for W_DSCRTN_INCM.dat.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Sharon Andrew 02/07/2006  - Originally Programmed
# MAGIC               Brent Leland     04/18/2007  - Changed hash file hf_dscrtn_incm_allocated7 to use hash file hf_dscrtn_incm_allocated7 instead of hf_dscrtn_incm_allocated8
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer           Date                 Project/Altiris #                                      Change Description                                                                                                                                            Development Project                Code Reviewer           Date Reviewed       
# MAGIC ------------------        --------------------     ------------------------                                       -----------------------------------------------------------------------                                                                                                    --------------------------------                 -------------------------------   ----------------------------       
# MAGIC Sandrew             2007-09-20      #5137 Project Release 8.1                   changed IDS Run Cycle name.   Changed name hash file name of current record indicator source 
# MAGIC                                                     IAD Quarterly Release                           Changed source of the Current Record Indicator file
# MAGIC                                                                                                                   Corrected any invalid RunCycle SK updates on load records.                                                                             devlEDW10                             Steph Goddard            09/28/2007
# MAGIC Terri O'Bryan       2009-07-10       TTR574                                                Changed all select criteria of where SK > 1 to where SK <> 0 and SK <> 1 due to negative sks
# MAGIC SAndrew             2009-07-21       TTR566                                                Changed keys to join to allocate4 prod was going to class and class plan was going to class                            devlEDW
# MAGIC                                                                                                                   Changed keys to join to allocate5 to have main extract keys match the reference lookup fields                         devlEDW                                 Steph Goddard            07/28/2009
# MAGIC 
# MAGIC Kimberly Doty      09-24-2010         TTR 551                                            Added 3 new columns - INVC_DSCRTN_DPNDT_CT, INVC_DSCRTN_SUB_CT                                              EnterpriseNewDevl                  Steph Goddard           10/01/2010
# MAGIC                                                                                                                 and INVC_DSCRTN_SELF_BILL_LIFE_IN; modified MBR_CT to be decimal 31,1 to accommodate 
# MAGIC                                                                                                                 DataStage's use of decimal 31 when summing a decimal field
# MAGIC  Syed Husseini    09-30-2013            5114                                               Newly Programmed                                                                                                                                                 EnterpriseWrhsDevl               Peter Marshall              12/10/2013

# MAGIC Sums fields from IDS Income Discretionary by Last Update Run cycle.
# MAGIC Assign primary surrogate key using NextSurrogate Generation (\"DSCRTN_INCM_F\")
# MAGIC Summarizes EDW W_DSCTRN_INCM and IDS Income Discretion by invoice and sequence nbr, compares amounts, and applys rounding corrections when needed.
# MAGIC 
# MAGIC Job Name:
# MAGIC EdwIncmDscrtnIncmFBal
# MAGIC This APPENDS to the balancing snapshot of the first DSCRNT_INCM_F Extr.
# MAGIC Remove the duplicates on Natural keys for the given sort order and get the latest records based on the keys for Descretionary Income.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
# MAGIC %run ../../../../../../shared_containers/PrimaryKey/IdsEdwDscrtnIncmPK

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, coalesce, rpad
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, IntegerType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# COMMAND ----------

IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
IDSIncmBeginCycle = get_widget_value('IDSIncmBeginCycle','')
EDWRunCycle = get_widget_value('EDWRunCycle','')
EDWRunCycleDate = get_widget_value('EDWRunCycleDate','')

# COMMAND ----------

# Stage: INCM_INVC_DS (PxDataSet)
df_INCM_INVC_DS_temp = spark.read.parquet(f"{adls_path}/ds/INCM_INVC.parquet")

df_INCM_INVC_DS = df_INCM_INVC_DS_temp.select(
    rpad(col("INVC_SK").cast(StringType()), col("INVC_SK").cast(StringType()).rlike("."), " ").alias("INVC_SK"),  # no fixed length specified; keeping as-is but ensuring the column is present
    "SRC_SYS_CD_SK",
    "BILL_INVC_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "BILL_ENTY_SK",
    "INVC_TYP_CD_SK",
    rpad(col("CUR_RCRD_IN"), 1, " ").alias("CUR_RCRD_IN"),
    rpad(col("BILL_DUE_DT_SK"), 10, " ").alias("BILL_DUE_DT_SK"),
    rpad(col("BILL_END_DT_SK"), 10, " ").alias("BILL_END_DT_SK"),
    rpad(col("CRT_DT_SK"), 10, " ").alias("CRT_DT_SK"),
    "BILL_ENTY_LVL_CD"
)

# Ensure proper column order and final rpad for char columns:
df_INCM_INVC_DS = df_INCM_INVC_DS.select(
    col("INVC_SK"),
    col("SRC_SYS_CD_SK"),
    col("BILL_INVC_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("BILL_ENTY_SK"),
    col("INVC_TYP_CD_SK"),
    col("CUR_RCRD_IN"),
    col("BILL_DUE_DT_SK"),
    col("BILL_END_DT_SK"),
    col("CRT_DT_SK"),
    col("BILL_ENTY_LVL_CD")
)

# COMMAND ----------

# Stage: db2_W_DSCRTN_INCM_Max_in (DB2ConnectorPX -> EDW)
edw_secret_name_value = edw_secret_name
jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name_value)

sql_db2_W_DSCRTN_INCM_Max_in = f"""
SELECT 
TRIM(incm1.SRC_SYS_CD) AS SRC_SYS_CD,
TRIM(incm1.BILL_INVC_ID) AS BILL_INVC_ID_MAX,
incm1.INVC_DSCRTN_SEQ_NO,
incm1.INVC_DSCRTN_YR_MO_SK,
TRIM(incm1.GRP_ID) AS GRP_ID,
TRIM(incm1.SUBGRP_ID) AS SUBGRP_ID,
TRIM(incm1.CLS_ID) AS CLS_ID,
TRIM(incm1.CLS_PLN_ID) AS CLS_PLN_ID,
TRIM(incm1.PROD_ID) AS PROD_ID
FROM {EDWOwner}.W_DSCRTN_INCM incm1
Where   incm1.INVC_DSCRTN_YR_MO_SK = (select max ( incm2.INVC_DSCRTN_YR_MO_SK )
                                      FROM {EDWOwner}.W_DSCRTN_INCM incm2
                                      Where incm2.SRC_SYS_CD = incm1.SRC_SYS_CD                
                                      and   incm2.BILL_INVC_ID = incm1.BILL_INVC_ID
                                      and   incm2.INVC_DSCRTN_SEQ_NO = incm1.INVC_DSCRTN_SEQ_NO)
and incm1.CLS_ID = (select max ( Incm3.CLS_ID )
                    FROM {EDWOwner}.W_DSCRTN_INCM Incm3
                    Where Incm3.SRC_SYS_CD = incm1.SRC_SYS_CD                
                    and   Incm3.BILL_INVC_ID = incm1.BILL_INVC_ID
                    and   Incm3.INVC_DSCRTN_SEQ_NO = incm1.INVC_DSCRTN_SEQ_NO)
and incm1.PROD_ID = (select max ( Incm4.PROD_ID )
                     FROM {EDWOwner}.W_DSCRTN_INCM Incm4
                     Where Incm4.SRC_SYS_CD = incm1.SRC_SYS_CD                
                     and   Incm4.BILL_INVC_ID = incm1.BILL_INVC_ID
                     and   Incm4.INVC_DSCRTN_SEQ_NO = incm1.INVC_DSCRTN_SEQ_NO)
"""

df_db2_W_DSCRTN_INCM_Max_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_db2_W_DSCRTN_INCM_Max_in)
    .load()
)

# COMMAND ----------

# Stage: db2_W_DSCRTN_INCM (DB2ConnectorPX -> EDW)
sql_db2_W_DSCRTN_INCM = f"""
SELECT  SRC_SYS_CD,
BILL_INVC_ID,
INVC_DSCRTN_SEQ_NO,
INVC_DSCRTN_YR_MO_SK,
GRP_ID,
SUBGRP_ID,
CLS_ID,
CLS_PLN_ID,
PROD_ID,
CRT_RUN_CYC_EXCTN_DT_SK,
LAST_UPDT_RUN_CYC_EXCTN_DT_SK,
BILL_ENTY_SK,
CLS_SK,
CLS_PLN_SK,
FEE_DSCNT_SK,
GRP_SK,
PROD_SK,
SUBGRP_SK,
INVC_DSCRTN_BILL_DISP_CD,
INVC_DSCRTN_PRM_FEE_CD,
INVC_TYP_CD,
INVC_DSCRTN_BEG_DT_SK,
INVC_DSCRTN_END_DT_SK,
INVC_BILL_DUE_DT_SK,
INVC_BILL_DUE_YR_MO_SK,
INVC_BILL_END_DT_SK,
INVC_BILL_END_YR_MO_SK,
INVC_CRT_DT_SK,
INVC_DSCRTN_DUE_DT_SK,
INVC_DSCRTN_DPNDT_PRM_AMT,
INVC_DSCRTN_FEE_DSCNT_AMT,
INVC_DSCRTN_SUB_PRM_AMT,
INVC_DSCRTN_MO_QTY,
INVC_DSCRTN_DESC,
INVC_DSCRTN_PRSN_ID_TX,
INVC_DSCRTN_SH_DESC,
FEE_DSCNT_ID,
PROD_BILL_CMPNT_ID,
SUB_UNIQ_KEY,
CRT_RUN_CYC_EXCTN_SK,
LAST_UPDT_RUN_CYC_EXCTN_SK,
INVC_DSCRTN_BILL_DISP_CD_SK,
INVC_DSCRTN_PRM_FEE_CD_SK,
INVC_DSCRTN_SK,
INVC_TYP_CD_SK,
INVC_SK,
SUB_SK,
INVC_DSCRTN_DPNDT_CT,
INVC_DSCRTN_SUB_CT,
INVC_DSCRTN_SELF_BILL_LIFE_IN
FROM {EDWOwner}.W_DSCRTN_INCM
"""

df_db2_W_DSCRTN_INCM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_db2_W_DSCRTN_INCM)
    .load()
)

# COMMAND ----------

# Stage: cpy_Edw_Recs (PxCopy)
df_cpy_Edw_Recs = df_db2_W_DSCRTN_INCM

# Split output pins from cpy_Edw_Recs:
#   lnk_All_Edw_Recs1 -> lkp_Amounts
df_lnk_All_Edw_Recs1 = df_cpy_Edw_Recs.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO").alias("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK").alias("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("CLS_ID").alias("CLS_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("FEE_DSCNT_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD"),
    col("INVC_DSCRTN_PRM_FEE_CD"),
    col("INVC_TYP_CD"),
    rpad(col("INVC_DSCRTN_BEG_DT_SK"), 10, " ").alias("INVC_DSCRTN_BEG_DT_SK"),
    rpad(col("INVC_DSCRTN_END_DT_SK"), 10, " ").alias("INVC_DSCRTN_END_DT_SK"),
    rpad(col("INVC_BILL_DUE_DT_SK"), 10, " ").alias("INVC_BILL_DUE_DT_SK"),
    rpad(col("INVC_BILL_DUE_YR_MO_SK"), 6, " ").alias("INVC_BILL_DUE_YR_MO_SK"),
    rpad(col("INVC_BILL_END_DT_SK"), 10, " ").alias("INVC_BILL_END_DT_SK"),
    rpad(col("INVC_BILL_END_YR_MO_SK"), 6, " ").alias("INVC_BILL_END_YR_MO_SK"),
    rpad(col("INVC_CRT_DT_SK"), 10, " ").alias("INVC_CRT_DT_SK"),
    rpad(col("INVC_DSCRTN_DUE_DT_SK"), 10, " ").alias("INVC_DSCRTN_DUE_DT_SK"),
    col("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("INVC_DSCRTN_SUB_PRM_AMT"),
    col("INVC_DSCRTN_MO_QTY"),
    col("INVC_DSCRTN_DESC"),
    col("INVC_DSCRTN_PRSN_ID_TX"),
    col("INVC_DSCRTN_SH_DESC"),
    col("FEE_DSCNT_ID"),
    col("PROD_BILL_CMPNT_ID"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("INVC_DSCRTN_SK"),
    col("INVC_TYP_CD_SK"),
    col("INVC_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_DPNDT_CT"),
    col("INVC_DSCRTN_SUB_CT"),
    rpad(col("INVC_DSCRTN_SELF_BILL_LIFE_IN"), 1, " ").alias("INVC_DSCRTN_SELF_BILL_LIFE_IN")
)

df_lnk_All_Edw_Recs2 = df_cpy_Edw_Recs.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO").alias("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK").alias("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("CLS_ID").alias("CLS_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("FEE_DSCNT_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD"),
    col("INVC_DSCRTN_PRM_FEE_CD"),
    col("INVC_TYP_CD"),
    rpad(col("INVC_DSCRTN_BEG_DT_SK"), 10, " ").alias("INVC_DSCRTN_BEG_DT_SK"),
    rpad(col("INVC_DSCRTN_END_DT_SK"), 10, " ").alias("INVC_DSCRTN_END_DT_SK"),
    rpad(col("INVC_BILL_DUE_DT_SK"), 10, " ").alias("INVC_BILL_DUE_DT_SK"),
    rpad(col("INVC_BILL_DUE_YR_MO_SK"), 6, " ").alias("INVC_BILL_DUE_YR_MO_SK"),
    rpad(col("INVC_BILL_END_DT_SK"), 10, " ").alias("INVC_BILL_END_DT_SK"),
    rpad(col("INVC_BILL_END_YR_MO_SK"), 6, " ").alias("INVC_BILL_END_YR_MO_SK"),
    rpad(col("INVC_CRT_DT_SK"), 10, " ").alias("INVC_CRT_DT_SK"),
    rpad(col("INVC_DSCRTN_DUE_DT_SK"), 10, " ").alias("INVC_DSCRTN_DUE_DT_SK"),
    col("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("INVC_DSCRTN_SUB_PRM_AMT"),
    col("INVC_DSCRTN_MO_QTY"),
    col("INVC_DSCRTN_DESC"),
    col("INVC_DSCRTN_PRSN_ID_TX"),
    col("INVC_DSCRTN_SH_DESC"),
    col("FEE_DSCNT_ID"),
    col("PROD_BILL_CMPNT_ID"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("INVC_DSCRTN_SK"),
    col("INVC_TYP_CD_SK"),
    col("INVC_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_DPNDT_CT"),
    col("INVC_DSCRTN_SUB_CT"),
    rpad(col("INVC_DSCRTN_SELF_BILL_LIFE_IN"), 1, " ").alias("INVC_DSCRTN_SELF_BILL_LIFE_IN")
)

# COMMAND ----------

# Stage: db2_W_Dscrtn_Incm_Extr (DB2ConnectorPX -> EDW)
sql_db2_W_Dscrtn_Incm_Extr = f"""
SELECT 
 w1.SRC_SYS_CD,
 w1.BILL_INVC_ID,
 w1.INVC_DSCRTN_SEQ_NO,   
 sum ( w1.INVC_DSCRTN_DPNDT_PRM_AMT )  AS INVC_DSCRTN_DPNDT_PRM_AMT,
 sum ( w1.INVC_DSCRTN_FEE_DSCNT_AMT )  AS INVC_DSCRTN_FEE_DSCNT_AMT,
 sum ( w1.INVC_DSCRTN_SUB_PRM_AMT )    AS INVC_DSCRTN_SUB_PRM_AMT
FROM {EDWOwner}.W_DSCRTN_INCM w1
group by 
 w1.SRC_SYS_CD,
 w1.BILL_INVC_ID,
 w1.INVC_DSCRTN_SEQ_NO
"""

df_db2_W_Dscrtn_Incm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option("query", sql_db2_W_Dscrtn_Incm_Extr)
    .load()
)

# COMMAND ----------

# Stage: db2_Ids_Amts_Summed (DB2ConnectorPX -> IDS)
ids_secret_name_value = ids_secret_name
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name_value)

sql_db2_Ids_Amts_Summed = f"""
SELECT 
distinct 
TRIM(MAP.TRGT_CD) AS SRC_SYS_CD,
TRIM(INVC_DSCRTN.BILL_INVC_ID) AS BILL_INVC_ID,
INVC_DSCRTN.SEQ_NO,
CAST(SUM (  INVC_DSCRTN.DPNDT_PRM_AMT) AS DECIMAL(13,2)) AS DPNDT_PRM_AMT,
CAST(SUM (  INVC_DSCRTN.FEE_DSCNT_AMT) AS DECIMAL(13,2)) AS FEE_DSCNT_AMT,
CAST(SUM ( INVC_DSCRTN.SUB_PRM_AMT ) AS DECIMAL(13,2)) AS SUB_PRM_AMT
FROM {IDSOwner}.INVC_DSCRTN INVC_DSCRTN,
     {IDSOwner}.CD_MPPNG   MAP
WHERE INVC_DSCRTN.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSIncmBeginCycle}
  AND INVC_DSCRTN.SRC_SYS_CD_SK = MAP.CD_MPPNG_SK
GROUP BY 
MAP.TRGT_CD,
INVC_DSCRTN.BILL_INVC_ID,
INVC_DSCRTN.SEQ_NO
"""

df_db2_Ids_Amts_Summed = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", sql_db2_Ids_Amts_Summed)
    .load()
)

# COMMAND ----------

# Stage: lkp_Amount (PxLookup)
# Primary link: df_db2_W_Dscrtn_Incm_Extr as P
# Lookup link (left join): df_db2_Ids_Amts_Summed as L on (P.SRC_SYS_CD==L.SRC_SYS_CD and P.BILL_INVC_ID==L.BILL_INVC_ID and P.INVC_DSCRTN_SEQ_NO==L.SEQ_NO)

df_lkp_Amount_joined = df_db2_W_Dscrtn_Incm_Extr.alias("lnk_W_Dscrtn_Incm_inABC").join(
    df_db2_Ids_Amts_Summed.alias("Ref_Ids_Amts_Summed"),
    [
        col("lnk_W_Dscrtn_Incm_inABC.SRC_SYS_CD") == col("Ref_Ids_Amts_Summed.SRC_SYS_CD"),
        col("lnk_W_Dscrtn_Incm_inABC.BILL_INVC_ID") == col("Ref_Ids_Amts_Summed.BILL_INVC_ID"),
        col("lnk_W_Dscrtn_Incm_inABC.INVC_DSCRTN_SEQ_NO") == col("Ref_Ids_Amts_Summed.SEQ_NO"),
    ],
    how="left"
)

df_lkp_Amount = df_lkp_Amount_joined.select(
    col("lnk_W_Dscrtn_Incm_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lnk_W_Dscrtn_Incm_inABC.BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("lnk_W_Dscrtn_Incm_inABC.INVC_DSCRTN_SEQ_NO").alias("INVC_DSCRTN_SEQ_NO"),
    col("lnk_W_Dscrtn_Incm_inABC.INVC_DSCRTN_DPNDT_PRM_AMT").alias("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("lnk_W_Dscrtn_Incm_inABC.INVC_DSCRTN_FEE_DSCNT_AMT").alias("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("lnk_W_Dscrtn_Incm_inABC.INVC_DSCRTN_SUB_PRM_AMT").alias("INVC_DSCRTN_SUB_PRM_AMT"),
    col("Ref_Ids_Amts_Summed.DPNDT_PRM_AMT").alias("DPNDT_PRM_AMT"),
    col("Ref_Ids_Amts_Summed.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    col("Ref_Ids_Amts_Summed.SUB_PRM_AMT").alias("SUB_PRM_AMT")
)

# COMMAND ----------

# Stage: xfrm_BusinessLogic (CTransformerStage)
# We have stage variables:
# svDPNDTPRMAMTDIFF = if IsNull(FEE_DSCNT_AMT) then 0 else (INVC_DSCRTN_DPNDT_PRM_AMT - DPNDT_PRM_AMT)
# Actually from the job: 
#  svDPNDTPRMAMTDIFF = If IsNull(lnk_AmountsLkpData_out.DPNDT_PRM_AMT) = @TRUE then 0 else lnk_AmountsLkpData_out.INVC_DSCRTN_DPNDT_PRM_AMT - lnk_AmountsLkpData_out.DPNDT_PRM_AMT
#  similarly for svFEEDSCRTNAMTDIFF and svSUBPRMAMTDIFF 

df_xfrm_BusinessLogic_stagevars = df_lkp_Amount.select("*").withColumn(
    "_svDPNDTPRMAMTDIFF",
    when(col("DPNDT_PRM_AMT").isNull(), lit(0.0)).otherwise(col("INVC_DSCRTN_DPNDT_PRM_AMT") - col("DPNDT_PRM_AMT"))
).withColumn(
    "_svFEEDSCRTNAMTDIFF",
    when(col("FEE_DSCNT_AMT").isNull(), lit(0.0)).otherwise(col("INVC_DSCRTN_FEE_DSCNT_AMT") - col("FEE_DSCNT_AMT"))
).withColumn(
    "_svSUBPRMAMTDIFF",
    when(col("SUB_PRM_AMT").isNull(), lit(0.0)).otherwise(col("INVC_DSCRTN_SUB_PRM_AMT") - col("SUB_PRM_AMT"))
)

# Two output links: 
# lnk_No_Correction_in => constraint: (svDPNDTPRMAMTDIFF = 0) AND (svFEEDSCRTNAMTDIFF = 0) AND (svSUBPRMAMTDIFF = 0)
# lnk_Needs_Correction_in => constraint: (svDPNDTPRMAMTDIFF <> 0) OR (svFEEDSCRTNAMTDIFF <> 0) OR (svSUBPRMAMTDIFF <> 0)

df_lnk_No_Correction_in = df_xfrm_BusinessLogic_stagevars.filter(
    (col("_svDPNDTPRMAMTDIFF") == 0) & (col("_svFEEDSCRTNAMTDIFF") == 0) & (col("_svSUBPRMAMTDIFF") == 0)
).select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID_NO_CORR"),
    col("INVC_DSCRTN_SEQ_NO")
)

df_lnk_Needs_Correction_in = df_xfrm_BusinessLogic_stagevars.filter(
    (col("_svDPNDTPRMAMTDIFF") != 0) | (col("_svFEEDSCRTNAMTDIFF") != 0) | (col("_svSUBPRMAMTDIFF") != 0)
).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID_NEEDS_CORR"),
    col("INVC_DSCRTN_SEQ_NO"),
    col("_svDPNDTPRMAMTDIFF").alias("DPNT_PRM_AMT_DIFF"),
    col("_svFEEDSCRTNAMTDIFF").alias("FEE_DSCTN_AMT_DIFF"),
    col("_svSUBPRMAMTDIFF").alias("SUB_PRM_AMT_DIFF")
)

# COMMAND ----------

# Stage: lkp_Amounts (PxLookup), with primary link -> df_lnk_All_Edw_Recs1, 
# and lookup links -> lnk_Needs_Correction_in & lnk_Src_Ref
# We have two left joins to produce a single output.

# 1) Join with lnk_Needs_Correction_in on:
#    lnk_All_Edw_Recs1.SRC_SYS_CD == lnk_Needs_Correction_in.SRC_SYS_CD
#    lnk_All_Edw_Recs1.BILL_INVC_ID == lnk_Needs_Correction_in.BILL_INVC_ID_NEEDS_CORR
#    lnk_All_Edw_Recs1.INVC_DSCRTN_SEQ_NO == lnk_Needs_Correction_in.INVC_DSCRTN_SEQ_NO

df_lkp_amt_join_1 = df_lnk_All_Edw_Recs1.alias("lnk_All_Edw_Recs1").join(
    df_lnk_Needs_Correction_in.alias("lnk_Needs_Correction_in"),
    [
        col("lnk_All_Edw_Recs1.SRC_SYS_CD") == col("lnk_Needs_Correction_in.SRC_SYS_CD"),
        col("lnk_All_Edw_Recs1.BILL_INVC_ID") == col("lnk_Needs_Correction_in.BILL_INVC_ID_NEEDS_CORR"),
        col("lnk_All_Edw_Recs1.INVC_DSCRTN_SEQ_NO") == col("lnk_Needs_Correction_in.INVC_DSCRTN_SEQ_NO")
    ],
    how="left"
)

# 2) Then join that result with df_db2_W_DSCRTN_INCM_Max_in on:
#    lnk_All_Edw_Recs1.SRC_SYS_CD == lnk_Src_Ref.SRC_SYS_CD
#    lnk_All_Edw_Recs1.BILL_INVC_ID == lnk_Src_Ref.BILL_INVC_ID_MAX
#    lnk_All_Edw_Recs1.INVC_DSCRTN_SEQ_NO == lnk_Src_Ref.INVC_DSCRTN_SEQ_NO
#    lnk_All_Edw_Recs1.INVC_DSCRTN_YR_MO_SK == lnk_Src_Ref.INVC_DSCRTN_YR_MO_SK
#    lnk_All_Edw_Recs1.GRP_ID == lnk_Src_Ref.GRP_ID
#    lnk_All_Edw_Recs1.SUBGRP_ID == lnk_Src_Ref.SUBGRP_ID
#    lnk_All_Edw_Recs1.CLS_ID == lnk_Src_Ref.CLS_ID
#    lnk_All_Edw_Recs1.CLS_PLN_ID == lnk_Src_Ref.CLS_PLN_ID
#    lnk_All_Edw_Recs1.PROD_ID == lnk_Src_Ref.PROD_ID

df_lkp_amt_join_2 = df_lkp_amt_join_1.alias("lkpA1").join(
    df_db2_W_DSCRTN_INCM_Max_in.alias("lnk_Src_Ref"),
    [
        col("lkpA1.SRC_SYS_CD") == col("lnk_Src_Ref.SRC_SYS_CD"),
        col("lkpA1.BILL_INVC_ID") == col("lnk_Src_Ref.BILL_INVC_ID_MAX"),
        col("lkpA1.INVC_DSCRTN_SEQ_NO") == col("lnk_Src_Ref.INVC_DSCRTN_SEQ_NO"),
        col("lkpA1.INVC_DSCRTN_YR_MO_SK") == col("lnk_Src_Ref.INVC_DSCRTN_YR_MO_SK"),
        col("lkpA1.GRP_ID") == col("lnk_Src_Ref.GRP_ID"),
        col("lkpA1.SUBGRP_ID") == col("lnk_Src_Ref.SUBGRP_ID"),
        col("lkpA1.CLS_ID") == col("lnk_Src_Ref.CLS_ID"),
        col("lkpA1.CLS_PLN_ID") == col("lnk_Src_Ref.CLS_PLN_ID"),
        col("lkpA1.PROD_ID") == col("lnk_Src_Ref.PROD_ID")
    ],
    how="left"
)

df_lkp_Amounts_output = df_lkp_amt_join_2.select(
    col("lkpA1.SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("lkpA1.BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("lkpA1.INVC_DSCRTN_SEQ_NO").alias("INVC_DSCRTN_SEQ_NO"),
    col("lkpA1.INVC_DSCRTN_YR_MO_SK").alias("INVC_DSCRTN_YR_MO_SK"),
    col("lkpA1.GRP_ID").alias("GRP_ID"),
    col("lkpA1.SUBGRP_ID").alias("SUBGRP_ID"),
    col("lkpA1.CLS_ID").alias("CLS_ID"),
    col("lkpA1.CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("lkpA1.PROD_ID").alias("PROD_ID"),
    col("lkpA1.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lkpA1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("lkpA1.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    col("lkpA1.CLS_SK").alias("CLS_SK"),
    col("lkpA1.CLS_PLN_SK").alias("CLS_PLN_SK"),
    col("lkpA1.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    col("lkpA1.GRP_SK").alias("GRP_SK"),
    col("lkpA1.PROD_SK").alias("PROD_SK"),
    col("lkpA1.SUBGRP_SK").alias("SUBGRP_SK"),
    col("lkpA1.INVC_DSCRTN_BILL_DISP_CD").alias("INVC_DSCRTN_BILL_DISP_CD"),
    col("lkpA1.INVC_DSCRTN_PRM_FEE_CD").alias("INVC_DSCRTN_PRM_FEE_CD"),
    col("lkpA1.INVC_TYP_CD").alias("INVC_TYP_CD"),
    col("lkpA1.INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
    col("lkpA1.INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
    col("lkpA1.INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
    col("lkpA1.INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
    col("lkpA1.INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
    col("lkpA1.INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
    col("lkpA1.INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
    col("lkpA1.INVC_DSCRTN_DUE_DT_SK").alias("INVC_DSCRTN_DUE_DT_SK"),
    col("lkpA1.INVC_DSCRTN_DPNDT_PRM_AMT").alias("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("lkpA1.INVC_DSCRTN_FEE_DSCNT_AMT").alias("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("lkpA1.INVC_DSCRTN_SUB_PRM_AMT").alias("INVC_DSCRTN_SUB_PRM_AMT"),
    col("lkpA1.INVC_DSCRTN_MO_QTY").alias("INVC_DSCRTN_MO_QTY"),
    col("lkpA1.INVC_DSCRTN_DESC").alias("INVC_DSCRTN_DESC"),
    col("lkpA1.INVC_DSCRTN_PRSN_ID_TX").alias("INVC_DSCRTN_PRSN_ID_TX"),
    col("lkpA1.INVC_DSCRTN_SH_DESC").alias("INVC_DSCRTN_SH_DESC"),
    col("lkpA1.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    col("lkpA1.PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
    col("lkpA1.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("lkpA1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("lkpA1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lkpA1.INVC_DSCRTN_BILL_DISP_CD_SK").alias("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("lkpA1.INVC_DSCRTN_PRM_FEE_CD_SK").alias("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("lkpA1.INVC_DSCRTN_SK").alias("INVC_DSCRTN_SK"),
    col("lkpA1.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
    col("lkpA1.INVC_SK").alias("INVC_SK"),
    col("lkpA1.SUB_SK").alias("SUB_SK"),
    col("lkpA1.INVC_DSCRTN_DPNDT_CT").alias("INVC_DSCRTN_DPNDT_CT"),
    col("lkpA1.INVC_DSCRTN_SUB_CT").alias("INVC_DSCRTN_SUB_CT"),
    col("lkpA1.INVC_DSCRTN_SELF_BILL_LIFE_IN").alias("INVC_DSCRTN_SELF_BILL_LIFE_IN"),
    col("lnk_Needs_Correction_in.BILL_INVC_ID_NEEDS_CORR").alias("BILL_INVC_ID_NEEDS_CORR"),
    col("lnk_Needs_Correction_in.DPNT_PRM_AMT_DIFF").alias("DPNT_PRM_AMT_DIFF"),
    col("lnk_Needs_Correction_in.FEE_DSCTN_AMT_DIFF").alias("FEE_DSCTN_AMT_DIFF"),
    col("lnk_Needs_Correction_in.SUB_PRM_AMT_DIFF").alias("SUB_PRM_AMT_DIFF"),
    col("lnk_Src_Ref.BILL_INVC_ID_MAX").alias("BILL_INVC_ID_MAX")
)

# COMMAND ----------

# Stage: xfm_BusinessLogic_Amts (CTransformerStage)
# Stage variables: 
# svDPNDNTPRMAMT, svFEEDSCNTAMT, svSUBPRMAMT

df_xfm_BusinessLogic_Amts_sv = df_lkp_Amounts_output.withColumn(
    "_svDPNDNTPRMAMT",
    when(
        ( (col("BILL_INVC_ID_NEEDS_CORR").isNotNull()) & (col("BILL_INVC_ID_NEEDS_CORR") != '') ),
        when(
            (col("BILL_INVC_ID_MAX").isNull()) | (col("BILL_INVC_ID_MAX") == ''),
            col("INVC_DSCRTN_DPNDT_PRM_AMT")
        ).otherwise(
            when(col("DPNT_PRM_AMT_DIFF") < 0,
                 col("INVC_DSCRTN_DPNDT_PRM_AMT") - col("DPNT_PRM_AMT_DIFF")
            ).otherwise(
                col("INVC_DSCRTN_DPNDT_PRM_AMT") + (col("DPNT_PRM_AMT_DIFF") * lit(-1))
            )
        )
    ).otherwise(lit(0))
).withColumn(
    "_svFEEDSCNTAMT",
    when(
        ( (col("BILL_INVC_ID_NEEDS_CORR").isNotNull()) & (col("BILL_INVC_ID_NEEDS_CORR") != '') ),
        when(
            (col("BILL_INVC_ID_MAX").isNull()) | (col("BILL_INVC_ID_MAX") == ''),
            col("INVC_DSCRTN_FEE_DSCNT_AMT")
        ).otherwise(
            when(col("FEE_DSCTN_AMT_DIFF") < 0,
                 col("INVC_DSCRTN_FEE_DSCNT_AMT") - col("FEE_DSCTN_AMT_DIFF")
            ).otherwise(
                col("INVC_DSCRTN_FEE_DSCNT_AMT") + (col("FEE_DSCTN_AMT_DIFF") * lit(-1))
            )
        )
    ).otherwise(lit(0))
).withColumn(
    "_svSUBPRMAMT",
    when(
        ( (col("BILL_INVC_ID_NEEDS_CORR").isNotNull()) & (col("BILL_INVC_ID_NEEDS_CORR") != '') ),
        when(
            (col("BILL_INVC_ID_MAX").isNull()) | (col("BILL_INVC_ID_MAX") == ''),
            col("INVC_DSCRTN_SUB_PRM_AMT")
        ).otherwise(
            when(col("SUB_PRM_AMT_DIFF") < 0,
                 col("INVC_DSCRTN_SUB_PRM_AMT") - col("SUB_PRM_AMT_DIFF")
            ).otherwise(
                col("INVC_DSCRTN_SUB_PRM_AMT") + (col("SUB_PRM_AMT_DIFF") * lit(-1))
            )
        )
    ).otherwise(lit(0))
)

# Output link: lnk_Dscrtn_Incm_Amts_in => constraint => IsNull(BILL_INVC_ID_NEEDS_CORR) = false
df_lnk_Dscrtn_Incm_Amts_in = df_xfm_BusinessLogic_Amts_sv.filter(
    col("BILL_INVC_ID_NEEDS_CORR").isNotNull()
).select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("FEE_DSCNT_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD"),
    col("INVC_DSCRTN_PRM_FEE_CD"),
    col("INVC_TYP_CD"),
    rpad(col("INVC_DSCRTN_BEG_DT_SK"), 10, " ").alias("INVC_DSCRTN_BEG_DT_SK"),
    rpad(col("INVC_DSCRTN_END_DT_SK"), 10, " ").alias("INVC_DSCRTN_END_DT_SK"),
    rpad(col("INVC_BILL_DUE_DT_SK"), 10, " ").alias("INVC_BILL_DUE_DT_SK"),
    rpad(col("INVC_BILL_DUE_YR_MO_SK"), 6, " ").alias("INVC_BILL_DUE_YR_MO_SK"),
    rpad(col("INVC_BILL_END_DT_SK"), 10, " ").alias("INVC_BILL_END_DT_SK"),
    rpad(col("INVC_BILL_END_YR_MO_SK"), 6, " ").alias("INVC_BILL_END_YR_MO_SK"),
    rpad(col("INVC_CRT_DT_SK"), 10, " ").alias("INVC_CRT_DT_SK"),
    rpad(col("INVC_DSCRTN_DUE_DT_SK"), 10, " ").alias("INVC_DSCRTN_DUE_DT_SK"),
    col("_svDPNDNTPRMAMT").alias("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("_svFEEDSCNTAMT").alias("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("_svSUBPRMAMT").alias("INVC_DSCRTN_SUB_PRM_AMT"),
    col("INVC_DSCRTN_MO_QTY"),
    col("INVC_DSCRTN_DESC"),
    col("INVC_DSCRTN_PRSN_ID_TX"),
    col("INVC_DSCRTN_SH_DESC"),
    col("FEE_DSCNT_ID"),
    col("PROD_BILL_CMPNT_ID"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("INVC_DSCRTN_SK"),
    col("INVC_TYP_CD_SK"),
    col("INVC_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_DPNDT_CT"),
    col("INVC_DSCRTN_SUB_CT"),
    rpad(col("INVC_DSCRTN_SELF_BILL_LIFE_IN"), 1, " ").alias("INVC_DSCRTN_SELF_BILL_LIFE_IN")
)

# COMMAND ----------

# Stage: lkp_dscrtn_incm_balance (PxLookup)
# primary link => df_lnk_All_Edw_Recs2
# lookup link => df_lnk_No_Correction_in 
# join keys => left join on (SRC_SYS_CD, BILL_INVC_ID, INVC_DSCRTN_SEQ_NO)

df_lkp_dscrtn_incm_balance_join = df_lnk_All_Edw_Recs2.alias("lnk_All_Edw_Recs2").join(
    df_lnk_No_Correction_in.alias("lnk_No_Correction_in"),
    [
        col("lnk_All_Edw_Recs2.SRC_SYS_CD") == col("lnk_No_Correction_in.SRC_SYS_CD"),
        col("lnk_All_Edw_Recs2.BILL_INVC_ID") == col("lnk_No_Correction_in.BILL_INVC_ID_NO_CORR"),
        col("lnk_All_Edw_Recs2.INVC_DSCRTN_SEQ_NO") == col("lnk_No_Correction_in.INVC_DSCRTN_SEQ_NO")
    ],
    how="left"
)

df_lkp_dscrtn_incm_balance_out = df_lkp_dscrtn_incm_balance_join.select(
    col("lnk_All_Edw_Recs2.SRC_SYS_CD"),
    col("lnk_All_Edw_Recs2.BILL_INVC_ID"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_SEQ_NO"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_YR_MO_SK"),
    col("lnk_All_Edw_Recs2.GRP_ID"),
    col("lnk_All_Edw_Recs2.SUBGRP_ID"),
    col("lnk_All_Edw_Recs2.CLS_ID"),
    col("lnk_All_Edw_Recs2.CLS_PLN_ID"),
    col("lnk_All_Edw_Recs2.PROD_ID"),
    col("lnk_All_Edw_Recs2.CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lnk_All_Edw_Recs2.LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("lnk_All_Edw_Recs2.BILL_ENTY_SK"),
    col("lnk_All_Edw_Recs2.CLS_SK"),
    col("lnk_All_Edw_Recs2.CLS_PLN_SK"),
    col("lnk_All_Edw_Recs2.FEE_DSCNT_SK"),
    col("lnk_All_Edw_Recs2.GRP_SK"),
    col("lnk_All_Edw_Recs2.PROD_SK"),
    col("lnk_All_Edw_Recs2.SUBGRP_SK"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_BILL_DISP_CD"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_PRM_FEE_CD"),
    col("lnk_All_Edw_Recs2.INVC_TYP_CD"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_BEG_DT_SK"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_END_DT_SK"),
    col("lnk_All_Edw_Recs2.INVC_BILL_DUE_DT_SK"),
    col("lnk_All_Edw_Recs2.INVC_BILL_DUE_YR_MO_SK"),
    col("lnk_All_Edw_Recs2.INVC_BILL_END_DT_SK"),
    col("lnk_All_Edw_Recs2.INVC_BILL_END_YR_MO_SK"),
    col("lnk_All_Edw_Recs2.INVC_CRT_DT_SK"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_DUE_DT_SK"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_SUB_PRM_AMT"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_MO_QTY"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_DESC"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_PRSN_ID_TX"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_SH_DESC"),
    col("lnk_All_Edw_Recs2.FEE_DSCNT_ID"),
    col("lnk_All_Edw_Recs2.PROD_BILL_CMPNT_ID"),
    col("lnk_All_Edw_Recs2.SUB_UNIQ_KEY"),
    col("lnk_All_Edw_Recs2.CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_All_Edw_Recs2.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_SK"),
    col("lnk_All_Edw_Recs2.INVC_TYP_CD_SK"),
    col("lnk_All_Edw_Recs2.INVC_SK"),
    col("lnk_All_Edw_Recs2.SUB_SK"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_DPNDT_CT"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_SUB_CT"),
    col("lnk_All_Edw_Recs2.INVC_DSCRTN_SELF_BILL_LIFE_IN"),
    col("lnk_No_Correction_in.BILL_INVC_ID_NO_CORR")
)

# COMMAND ----------

# Stage: xfm_BusinessLogic_Bal_Recs (CTransformerStage)
df_xfm_BusinessLogic_Bal_Recs = df_lkp_dscrtn_incm_balance_out.filter(
    col("BILL_INVC_ID_NO_CORR").isNotNull()
).select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("FEE_DSCNT_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD"),
    col("INVC_DSCRTN_PRM_FEE_CD"),
    col("INVC_TYP_CD"),
    col("INVC_DSCRTN_BEG_DT_SK"),
    col("INVC_DSCRTN_END_DT_SK"),
    col("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK"),
    col("INVC_DSCRTN_DUE_DT_SK"),
    col("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("INVC_DSCRTN_SUB_PRM_AMT"),
    col("INVC_DSCRTN_MO_QTY"),
    col("INVC_DSCRTN_DESC"),
    col("INVC_DSCRTN_PRSN_ID_TX"),
    col("INVC_DSCRTN_SH_DESC"),
    col("FEE_DSCNT_ID"),
    col("PROD_BILL_CMPNT_ID"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("INVC_DSCRTN_SK"),
    col("INVC_TYP_CD_SK"),
    col("INVC_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_DPNDT_CT"),
    col("INVC_DSCRTN_SUB_CT"),
    col("INVC_DSCRTN_SELF_BILL_LIFE_IN")
)

# COMMAND ----------

# Stage: fnl_Data_Links (PxFunnel)
# inputs: df_lnk_Dscrtn_Incm_Amts_in and df_xfm_BusinessLogic_Bal_Recs
df_fnl_Data_Links = df_lnk_Dscrtn_Incm_Amts_in.unionByName(df_xfm_BusinessLogic_Bal_Recs, allowMissingColumns=True)

# COMMAND ----------

# Stage: rdp_Dscrnt_Incm_Allocated_All (PxRemDup)
# "RetainRecord": "last"
# Keys: SRC_SYS_CD, BILL_INVC_ID, INVC_DSCRTN_SEQ_NO, INVC_DSCRTN_YR_MO_SK, GRP_ID, SUBGRP_ID, CLS_ID, CLS_PLN_ID, PROD_ID
# We use dedup_sort. The job also shows sort mode is -nonStable with KeySortOrder ascending. We'll replicate by dedup_sort.

df_rdp_sort_input = dedup_sort(
    df_fnl_Data_Links,
    partition_cols=[
        "SRC_SYS_CD","BILL_INVC_ID","INVC_DSCRTN_SEQ_NO","INVC_DSCRTN_YR_MO_SK",
        "GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PROD_ID"
    ],
    sort_cols=[("SRC_SYS_CD","A"),("BILL_INVC_ID","A"),("INVC_DSCRTN_SEQ_NO","A"),("INVC_DSCRTN_YR_MO_SK","A"),
               ("GRP_ID","A"),("SUBGRP_ID","A"),("CLS_ID","A"),("CLS_PLN_ID","A"),("PROD_ID","A")]
)

df_rdp_Dscrnt_Incm_Allocated_All = df_rdp_sort_input

# COMMAND ----------

# Now we have the output link lnk_Dscrtn_Incm_Alloc_All:
df_lnk_Dscrtn_Incm_Alloc_All = df_rdp_Dscrnt_Incm_Allocated_All.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("FEE_DSCNT_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD"),
    col("INVC_DSCRTN_PRM_FEE_CD"),
    col("INVC_TYP_CD"),
    col("INVC_DSCRTN_BEG_DT_SK"),
    col("INVC_DSCRTN_END_DT_SK"),
    col("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK"),
    col("INVC_DSCRTN_DUE_DT_SK"),
    col("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("INVC_DSCRTN_SUB_PRM_AMT"),
    col("INVC_DSCRTN_MO_QTY"),
    col("INVC_DSCRTN_DESC"),
    col("INVC_DSCRTN_PRSN_ID_TX"),
    col("INVC_DSCRTN_SH_DESC"),
    col("FEE_DSCNT_ID"),
    col("PROD_BILL_CMPNT_ID"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("INVC_DSCRTN_SK"),
    col("INVC_TYP_CD_SK"),
    col("INVC_SK").alias("INVC_SK_MAIN"),
    col("SUB_SK"),
    col("INVC_DSCRTN_DPNDT_CT"),
    col("INVC_DSCRTN_SUB_CT"),
    col("INVC_DSCRTN_SELF_BILL_LIFE_IN")
)

# COMMAND ----------

# Stage: lkp_Invoice (PxLookup)
# Primary link: df_lnk_Dscrtn_Incm_Alloc_All
# Lookup link: df_INCM_INVC_DS on INVC_SK_MAIN == INVC_SK
df_lkp_Invoice_join = df_lnk_Dscrtn_Incm_Alloc_All.alias("lnk_Dscrtn_Incm_Alloc_All").join(
    df_INCM_INVC_DS.alias("Ref_InvoiceSk"),
    [
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_SK_MAIN") == col("Ref_InvoiceSk.INVC_SK")
    ],
    how="left"
)

df_lkp_Invoice_out = df_lkp_Invoice_join.select(
    col("lnk_Dscrtn_Incm_Alloc_All.SRC_SYS_CD"),
    col("lnk_Dscrtn_Incm_Alloc_All.BILL_INVC_ID"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_SEQ_NO"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_YR_MO_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.GRP_ID"),
    col("lnk_Dscrtn_Incm_Alloc_All.SUBGRP_ID"),
    col("lnk_Dscrtn_Incm_Alloc_All.CLS_ID"),
    col("lnk_Dscrtn_Incm_Alloc_All.CLS_PLN_ID"),
    col("lnk_Dscrtn_Incm_Alloc_All.PROD_ID"),
    col("lnk_Dscrtn_Incm_Alloc_All.CRT_RUN_CYC_EXCTN_DT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.BILL_ENTY_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.CLS_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.CLS_PLN_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.FEE_DSCNT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.GRP_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.PROD_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.SUBGRP_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_BILL_DISP_CD"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_PRM_FEE_CD"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_TYP_CD"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_BEG_DT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_END_DT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_BILL_DUE_DT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_BILL_DUE_YR_MO_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_BILL_END_DT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_BILL_END_YR_MO_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_CRT_DT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_DUE_DT_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_SUB_PRM_AMT"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_MO_QTY"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_DESC"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_PRSN_ID_TX"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_SH_DESC"),
    col("lnk_Dscrtn_Incm_Alloc_All.FEE_DSCNT_ID"),
    col("lnk_Dscrtn_Incm_Alloc_All.PROD_BILL_CMPNT_ID"),
    col("lnk_Dscrtn_Incm_Alloc_All.SUB_UNIQ_KEY"),
    col("lnk_Dscrtn_Incm_Alloc_All.CRT_RUN_CYC_EXCTN_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_TYP_CD_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_SK_MAIN"),
    col("lnk_Dscrtn_Incm_Alloc_All.SUB_SK"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_DPNDT_CT"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_SUB_CT"),
    col("lnk_Dscrtn_Incm_Alloc_All.INVC_DSCRTN_SELF_BILL_LIFE_IN"),
    col("Ref_InvoiceSk.INVC_SK").alias("INVC_SK"),
    rpad(col("Ref_InvoiceSk.CUR_RCRD_IN"), 1, " ").alias("CUR_RCRD_IN")
)

# COMMAND ----------

# Stage: xfm_BusinessLogic_Rec_In (CTransformerStage)
# We have expression referencing a job parameter EDWRunCycleDate => "EDWRunCycleDate"
# The output pin => lnk_PKey_Out => columns. Some columns set to EDWRunCycleDate for CRT_RUN_CYC_EXCTN_DT_SK, etc.

df_xfm_BusinessLogic_Rec_In = df_lkp_Invoice_out.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    rpad(lit(EDWRunCycleDate), 10, " ").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    rpad(lit(EDWRunCycleDate), 10, " ").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("FEE_DSCNT_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD"),
    col("INVC_DSCRTN_PRM_FEE_CD"),
    col("INVC_TYP_CD"),
    rpad(col("INVC_DSCRTN_BEG_DT_SK"), 10, " ").alias("INVC_DSCRTN_BEG_DT_SK"),
    rpad(col("INVC_DSCRTN_END_DT_SK"), 10, " ").alias("INVC_DSCRTN_END_DT_SK"),
    rpad(col("INVC_BILL_DUE_DT_SK"), 10, " ").alias("INVC_BILL_DUE_DT_SK"),
    rpad(col("INVC_BILL_DUE_YR_MO_SK"), 6, " ").alias("INVC_BILL_DUE_YR_MO_SK"),
    rpad(col("INVC_BILL_END_DT_SK"), 10, " ").alias("INVC_BILL_END_DT_SK"),
    rpad(col("INVC_BILL_END_YR_MO_SK"), 6, " ").alias("INVC_BILL_END_YR_MO_SK"),
    rpad(col("INVC_CRT_DT_SK"), 10, " ").alias("INVC_CRT_DT_SK"),
    rpad(col("INVC_DSCRTN_DUE_DT_SK"), 10, " ").alias("INVC_DSCRTN_DUE_DT_SK"),
    col("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("INVC_DSCRTN_SUB_PRM_AMT"),
    col("INVC_DSCRTN_MO_QTY"),
    col("INVC_DSCRTN_DESC"),
    col("INVC_DSCRTN_PRSN_ID_TX"),
    col("INVC_DSCRTN_SH_DESC"),
    col("FEE_DSCNT_ID"),
    col("PROD_BILL_CMPNT_ID"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("INVC_DSCRTN_SK"),
    col("INVC_TYP_CD_SK"),
    col("INVC_SK_MAIN").alias("INVC_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_DPNDT_CT"),
    col("INVC_DSCRTN_SUB_CT"),
    rpad(col("INVC_DSCRTN_SELF_BILL_LIFE_IN"), 1, " ").alias("INVC_DSCRTN_SELF_BILL_LIFE_IN"),
    when(col("INVC_SK").isNull(), lit("U")).otherwise(col("CUR_RCRD_IN")).alias("CUR_RCRD_IN")
)

# COMMAND ----------

# Stage: xfm_BusinessLogic_SplitData_PKey (CTransformerStage)
# Output pins:
#  1) lnk_Snapshot_Out => columns
#  2) lnk_Transforms_Out => columns
#  3) lnk_DscrntIncmData_L_in => columns

df_lnk_Snapshot_Out = df_xfm_BusinessLogic_Rec_In.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("INVC_DSCRTN_SUB_PRM_AMT"),
    col("INVC_TYP_CD")
)

df_lnk_Transforms_Out = df_xfm_BusinessLogic_Rec_In.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID")
)

df_lnk_DscrntIncmData_L_in = df_xfm_BusinessLogic_Rec_In.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("INVC_DSCRTN_SEQ_NO"),
    col("INVC_DSCRTN_YR_MO_SK"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("FEE_DSCNT_SK"),
    col("GRP_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    rpad(col("CUR_RCRD_IN"), 1, " ").alias("INVC_CUR_RCRD_IN"),
    col("INVC_DSCRTN_BILL_DISP_CD"),
    col("INVC_DSCRTN_PRM_FEE_CD"),
    col("INVC_TYP_CD"),
    col("INVC_DSCRTN_BEG_DT_SK"),
    col("INVC_DSCRTN_END_DT_SK"),
    col("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK"),
    col("INVC_DSCRTN_DUE_DT_SK"),
    col("INVC_DSCRTN_DPNDT_PRM_AMT"),
    col("INVC_DSCRTN_FEE_DSCNT_AMT"),
    col("INVC_DSCRTN_SUB_PRM_AMT"),
    col("INVC_DSCRTN_MO_QTY"),
    col("INVC_DSCRTN_DESC"),
    col("INVC_DSCRTN_PRSN_ID_TX"),
    col("INVC_DSCRTN_SH_DESC"),
    col("FEE_DSCNT_ID"),
    col("PROD_BILL_CMPNT_ID"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("INVC_DSCRTN_SK"),
    col("INVC_TYP_CD_SK"),
    col("INVC_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_DPNDT_CT"),
    col("INVC_DSCRTN_SUB_CT"),
    col("INVC_DSCRTN_SELF_BILL_LIFE_IN")
)

# COMMAND ----------

# Stage: seq_B_DSCRTN_INCM_dat (PxSequentialFile)
# Input: df_lnk_Snapshot_Out
# Writes to B_DSCRTN_INCM_F.dat with append, no header, delimiter=",", quote="^", nullValue=None
df_seq_B_DSCRTN_INCM_dat = df_lnk_Snapshot_Out
write_files(
    df_seq_B_DSCRTN_INCM_dat,
    f"{adls_path}/load/B_DSCRTN_INCM_F.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# COMMAND ----------

# Stage: IdsEdwDscrtnIncmPK (CContainerStage)
# We have 2 inputs => df_lnk_Transforms_Out, df_lnk_DscrntIncmData_L_in
# 1 output => lnk_IdsEdwDscrntIncmFPkey_OutABC

params_IdsEdwDscrtnIncmPK = {}
df_IdsEdwDscrtnIncmPK_out = IdsEdwDscrtnIncmPK(df_lnk_Transforms_Out, df_lnk_DscrntIncmData_L_in, params_IdsEdwDscrtnIncmPK)

# COMMAND ----------

# Stage: seq_DSCRTN_INCM_F_load (PxSequentialFile)
# Input: df_IdsEdwDscrtnIncmPK_out
# Writes to DSCRTN_INCM_F.dat with overwrite, no header, delimiter=",", quote="^", nullValue=None

df_seq_DSCRTN_INCM_F_load = df_IdsEdwDscrtnIncmPK_out
write_files(
    df_seq_DSCRTN_INCM_F_load,
    f"{adls_path}/load/DSCRTN_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)