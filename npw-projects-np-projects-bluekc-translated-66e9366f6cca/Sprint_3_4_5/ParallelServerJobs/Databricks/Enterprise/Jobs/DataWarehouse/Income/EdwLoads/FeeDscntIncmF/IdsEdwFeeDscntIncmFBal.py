# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2006 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwIncomeBalanceDscrtnIncmF
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC Descretionary Income - Earned Premium Income either at a Grp, Subgroup, Class, ClassPlan, Product or at a Subscriber Level.    In IDS Income Discretionary, there is 1 record in the table with a the total income amount and the total number of months for which this income is for.  In EDW Discretionary Income Fact, this 1 IDS record of income is reflected by many EDW records, each record relecting the each earned month and that month's share of the total income amount.
# MAGIC 
# MAGIC Two ways to tell which type of level.  
# MAGIC 1.     Check value of Sub_SK.  Its either 0 or >0.  If 0, then 
# MAGIC 2.     Check value of the BILL_ENTY_LVL_CD for that invoice tied to the Discretionary Income.    It will be either INDVSUB (Subscriber) 
# MAGIC         or SUBGRP (Grp, Subgroup, Class, ClassPlan, Product)
# MAGIC 
# MAGIC If the Sub_Sk = 0 then it is earned premium income at a Grp, Subgroup, Class, ClassPlan, Product Level.  Sub Unique Key should = 0.  Any of the Grp, Subgroup, Class, ClassPlan, Product SK's can be 0 or 1.
# MAGIC 
# MAGIC If the Sub_Sk > 1, then it is earned premium income at a Subscriber level.  Sub Unique Key should not = 0.  Any of the Grp, Subgroup, Class, ClassPlan, Product SK's should not be 0 or 1.
# MAGIC 
# MAGIC For both levels, this income can be allocated over one to many months, even years.    That role of EdwIncomeDsctrnIncmFExtr is to divide the monies down by the Month Quantity into the right number of Year/Months.     It sets the correct dates for each record.  
# MAGIC 
# MAGIC Only for premium at a Grp, Subgroup, Class, ClassPlan, Product level, the income can be for and allocated over many different subgroups, class, class plan, products.     Once the money is divided by the quantity months (EdwIncomeDsctrnIncmFExtr), then EdwIncomeAllocateDscrtnIncmF splits the money across the different Subgroup, Class, ClassPlan, Products for that Group.     EdwIncomeAllocateDscrtnIncmF is only needed for records that are non-subscriber level and has a 0 or 1 for either Subgroup, Class, ClassPlan, Product.    It matches all that it knows to W_MBR_RCST (a working table of Member Recast Counts)  to get all of the possible combinations of Subgroup, Class, ClassPlan, Product.    A record for all possible combinations is written out and the monies is divided by the percentage of members in that Subgroup, Class, ClassPlan, Product.
# MAGIC 
# MAGIC Once the money is divided amoung all possible Subgroup, Class, ClassPlan, Product for that Group, further adjustments on the amounts may be needed due to rounding when multiplying the amounts by the membership percentage.  When all of the amounts are added back up, they do not equal the IDS amounts.   EdwIncomBalanceDscrtnIncmF adds to or subtracts from the EDW amounts so that it will balance IDS.
# MAGIC 
# MAGIC INPUTS:
# MAGIC                 IDS:      INCM_DSCRTN
# MAGIC 	EDW:    W_DSCTRN_INCM
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                  Runs after the load file W_DSCRTN_INCM.dat  from EdwIncomaAllocateIncmF is done.  
# MAGIC                  If any record on the W_DSCRTN_INCM table still has a 0/1 for Group, Subgroup, Class, ClassPlan, Product, then it is not corrected, but simply pumped thru and will be loaded to the DSCRNT_INCM_F table.
# MAGIC                  Summarizes all the amount fileds for all records in W_DSCRTN_INCM by invoice that do not have a 0/1 for any of the fields SK:  Group, Subgroup, Class, ClassPlan, Product.  This means that they were successfully matched to a W_MBR_RCST 
# MAGIC                       record in EdwIncomeAllocateDscrtnIncmF and all possible combinations were determined, and amounts were calculated based on membership counts. 
# MAGIC                  Summarizes all the amount fields of all records with Last Run Cycle > BeginCycle on the IDS Income Discretionary table.
# MAGIC                  Ccompares each amount and calculates a difference.
# MAGIC                  If any amount field has a diffeence <> 0, then further corrections is needed and these records are sent to hash file ... for further processing.
# MAGIC                  If all the amount fields compares has a difference = 0, then no futher correctsions are needed, and the records are 
# MAGIC 
# MAGIC                 If 
# MAGIC 
# MAGIC  
# MAGIC OUTPUTS: 
# MAGIC                   An EDW load file.   Since no history, then the output is a load file to update the EDW table.
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC               Sharon Andrew 12/07/2005  - Originally Programmed
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer            Date               Project/Altiris #                                        Change Description                                                                      Develop Project          Code Reviewer              Date Reviewed       
# MAGIC ------------------        --------------------     ------------------------                                       -----------------------------------------------------------------------                              ------------------------------       -------------------------------   ----------------------------       
# MAGIC Sandrew            2007-09-20      eproject #5137 Project Release 8.1          Changed source of the Current Record Incidator file
# MAGIC                                                   IAD Quarterly Release                              Corrected any invalid RunCycle SK updates on load records.      devlEDW10                 Steph Goddard              09/28/2007
# MAGIC O. Nielsen         2008-05-21      Production Support                                    Added Balancing Snapshot                                                          devlEDW                    Steph Goddard              05/25/2008
# MAGIC 
# MAGIC Kimberly Doty    2010-09-29       TTR 551                                                 Modified FEE_DSCNT_AMT on IDS_AMTS_SUMMED             EnterpriseNewDevl        Steph Goddard               10/01/2010
# MAGIC                                                                                                                   and Allocated_W_Recs from Decimal 13,2 [15] to 
# MAGIC                                                                                                                   Decimal 32,2 [34] b/c DataStage uses a Decimal 31 when summing
# MAGIC 
# MAGIC Srikanth Mettpalli  2013-10-01          5114                                             Original Programming                                                                      EnterpriseWrhsDevl       Peter Marshall                12/12/2013           
# MAGIC                                                                                                              (Server to Parallel Conversion)

# MAGIC Remove the duplicates on Natural keys for the given sort order and get the latest records based on the keys for Descretionary Income.
# MAGIC Load FEE_DSCNT_INCM_F.ds dataset for the PKey Job
# MAGIC Load B_FEE_DSCNT_INCM_F balancing table
# MAGIC Sums fields from IDS Income Fee Discount by Last Update Run cycle.
# MAGIC Job Name: IdsEdwFeeDscntIncmFBal
# MAGIC Summarizes EDW W_DSCTRN_INCM and IDS Income Fee Discount by invoice, compares amounts, and applys rounding corrections when needed.
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql.functions import col, lit, when, rpad
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


EDWOwner = get_widget_value('EDWOwner','')
IDSOwner = get_widget_value('IDSOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
ids_secret_name = get_widget_value('ids_secret_name','')
IDSIncmBeginCycle = get_widget_value('IDSIncmBeginCycle','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrentRunDate = get_widget_value('CurrentRunDate','')

jdbc_url_edw, jdbc_props_edw = get_db_config(edw_secret_name)
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)

df_INCM_INVC_DS = spark.read.parquet(f"{adls_path}/ds/INCM_INVC.parquet")
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

df_db2_W_FEE_DSCNT_INCM_Max_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        "SELECT distinct \nfee1.SRC_SYS_CD,\nfee1.BILL_INVC_ID,\nfee1.FEE_DSCNT_ID,\nfee1.INVC_FEE_DSCNT_BILL_DISP_CD,\nfee1.FEE_DSCNT_YR_MO_SK,\nfee1.GRP_ID,\nfee1.SUBGRP_ID,\nfee1.CLS_ID,\nfee1.CLS_PLN_ID,\nfee1.PROD_ID,\n'Z' AS Dummy\n\nFROM "
        + EDWOwner
        + ".W_FEE_DSCNT_INCM fee1"
    )
    .load()
)

df_db2_W_FEE_DSCNT_INCM = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        "SELECT \nSRC_SYS_CD,\nBILL_INVC_ID,\nFEE_DSCNT_ID,\nINVC_FEE_DSCNT_BILL_DISP_CD,\nINVC_FEE_DSCNT_SRC_CD,\nGRP_ID,\nSUBGRP_ID,\nCLS_ID,\nCLS_PLN_ID,\nPROD_ID,\nCRT_RUN_CYC_EXCTN_DT_SK,\nLAST_UPDT_RUN_CYC_EXCTN_DT_SK,\nBILL_ENTY_SK,\nCLS_SK,\nCLS_PLN_SK,\nFEE_DSCNT_SK,\nGRP_SK,\nSUBGRP_SK,\nINVC_TYP_CD,\nDSCNT_IN,\nFEE_DSCNT_YR_MO_SK,\nINVC_BILL_DUE_DT_SK,\nINVC_BILL_DUE_YR_MO_SK,\nINVC_BILL_END_DT_SK,\nINVC_BILL_END_YR_MO_SK,\nINVC_CRT_DT_SK,\nFEE_DSCNT_AMT,\nSUB_UNIQ_KEY,\nCRT_RUN_CYC_EXCTN_SK,\nLAST_UPDT_RUN_CYC_EXCTN_SK,\nINVC_FEE_DSCNT_SRC_CD_SK,\nINVC_FEE_DSCNT_BILL_DISP_CD_SK,\nINVC_FEE_DSCNT_SK,\nINVC_SK,\nINVC_TYP_CD_SK,\nSUB_SK,\nPROD_SK\nFROM "
        + EDWOwner
        + ".W_FEE_DSCNT_INCM"
    )
    .load()
)

df_cpy_Edw_Recs = df_db2_W_FEE_DSCNT_INCM

df_lnk_All_Edw_Recs1 = df_cpy_Edw_Recs.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    col("INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("CLS_ID").alias("CLS_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    col("CLS_SK").alias("CLS_SK"),
    col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("SUBGRP_SK").alias("SUBGRP_SK"),
    col("INVC_TYP_CD").alias("INVC_TYP_CD"),
    col("DSCNT_IN").alias("DSCNT_IN"),
    col("FEE_DSCNT_YR_MO_SK").alias("FEE_DSCNT_YR_MO_SK"),
    col("INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
    col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    col("INVC_FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
    col("INVC_SK").alias("INVC_SK"),
    col("INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("PROD_SK").alias("PROD_SK")
)

df_lnk_All_Edw_Recs2 = df_cpy_Edw_Recs.select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    col("INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
    col("GRP_ID").alias("GRP_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("CLS_ID").alias("CLS_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID"),
    col("CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK").alias("BILL_ENTY_SK"),
    col("CLS_SK").alias("CLS_SK"),
    col("CLS_PLN_SK").alias("CLS_PLN_SK"),
    col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    col("GRP_SK").alias("GRP_SK"),
    col("SUBGRP_SK").alias("SUBGRP_SK"),
    col("INVC_TYP_CD").alias("INVC_TYP_CD"),
    col("DSCNT_IN").alias("DSCNT_IN"),
    col("FEE_DSCNT_YR_MO_SK").alias("FEE_DSCNT_YR_MO_SK"),
    col("INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
    col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    col("SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    col("INVC_FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
    col("INVC_SK").alias("INVC_SK"),
    col("INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
    col("SUB_SK").alias("SUB_SK"),
    col("PROD_SK").alias("PROD_SK")
)

df_db2_Ids_Amts_Summed = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option(
        "query",
        "SELECT \nMAP1.TRGT_CD AS SRC_SYS_CD,\nFEE.BILL_INVC_ID AS BILL_INVC_ID,\nFEE.FEE_DSCNT_ID AS FEE_DSCNT_ID,\nMAP2.TRGT_CD AS INVC_FEE_DSCNT_BILL_DISP_CD,\nCAST(SUM (FEE.FEE_DSCNT_AMT) AS DECIMAL(13,2)) AS FEE_DSCNT_AMT\n\nFROM "
        + IDSOwner
        + ".INVC_FEE_DSCNT   FEE,\n            "
        + IDSOwner
        + ".CD_MPPNG   MAP1,\n            "
        + IDSOwner
        + ".CD_MPPNG   MAP2\n\nWHERE FEE.LAST_UPDT_RUN_CYC_EXCTN_SK       >=  "
        + IDSIncmBeginCycle
        + "\n   AND    FEE.SRC_SYS_CD_SK                                        = MAP1.CD_MPPNG_SK\n   AND    FEE.INVC_FEE_DSCNT_BILL_DISP_CD_SK     = MAP2.CD_MPPNG_SK\n\nGROUP BY \nMAP1.TRGT_CD,\nFEE.BILL_INVC_ID,\nFEE.FEE_DSCNT_ID,\nMAP2.TRGT_CD"
    )
    .load()
)

df_db2_W_Fee_Dscrtn_Incm_Extr = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_edw)
    .options(**jdbc_props_edw)
    .option(
        "query",
        "SELECT \n w1.SRC_SYS_CD,\n w1.BILL_INVC_ID,\n w1.FEE_DSCNT_ID,\n w1.INVC_FEE_DSCNT_BILL_DISP_CD,\n CAST(sum (w1.FEE_DSCNT_AMT) AS DECIMAL(13,2)) AS FEE_DSCNT_AMT\n\nFROM "
        + EDWOwner
        + ".W_FEE_DSCNT_INCM  w1\n\ngroup by \n w1.SRC_SYS_CD,\n w1.BILL_INVC_ID,\n w1.FEE_DSCNT_ID,\n w1.INVC_FEE_DSCNT_BILL_DISP_CD"
    )
    .load()
)

df_lkp_Amount = (
    df_db2_W_Fee_Dscrtn_Incm_Extr.alias("lnk_IdsEdwFeeDscntIncmFBal_inABC")
    .join(
        df_db2_Ids_Amts_Summed.alias("Ref_Ids_Amts_Summed"),
        on=[
            col("lnk_IdsEdwFeeDscntIncmFBal_inABC.SRC_SYS_CD")
            == col("Ref_Ids_Amts_Summed.SRC_SYS_CD"),
            col("lnk_IdsEdwFeeDscntIncmFBal_inABC.BILL_INVC_ID")
            == col("Ref_Ids_Amts_Summed.BILL_INVC_ID"),
            col("lnk_IdsEdwFeeDscntIncmFBal_inABC.FEE_DSCNT_ID")
            == col("Ref_Ids_Amts_Summed.FEE_DSCNT_ID"),
            col("lnk_IdsEdwFeeDscntIncmFBal_inABC.INVC_FEE_DSCNT_BILL_DISP_CD")
            == col("Ref_Ids_Amts_Summed.INVC_FEE_DSCNT_BILL_DISP_CD"),
        ],
        how="left",
    )
    .select(
        col("lnk_IdsEdwFeeDscntIncmFBal_inABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_IdsEdwFeeDscntIncmFBal_inABC.BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("lnk_IdsEdwFeeDscntIncmFBal_inABC.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("lnk_IdsEdwFeeDscntIncmFBal_inABC.INVC_FEE_DSCNT_BILL_DISP_CD").alias(
            "INVC_FEE_DSCNT_BILL_DISP_CD"
        ),
        col("lnk_IdsEdwFeeDscntIncmFBal_inABC.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        col("Ref_Ids_Amts_Summed.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT_1"),
    )
)

df_xfrm_BusinessLogic_stagevars = (
    df_lkp_Amount.withColumn(
        "svNullChk",
        when(col("FEE_DSCNT_AMT_1").isNull(), lit(0.0)).otherwise(col("FEE_DSCNT_AMT_1")),
    )
    .withColumn("svFeeDscntAmtDiff", col("FEE_DSCNT_AMT") - col("svNullChk"))
    .withColumn(
        "svValidateResults",
        when(col("svFeeDscntAmtDiff").cast(DecimalType(38, 10)).isNotNull(), lit(1)).otherwise(
            lit(0)
        ),
    )
)

df_lnk_No_Correction_in = df_xfrm_BusinessLogic_stagevars.filter(
    (col("svFeeDscntAmtDiff") == 0) & (col("svValidateResults") == 1)
).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    lit("Z").alias("Dummy"),
)

df_lnk_Needs_Correction_in = df_xfrm_BusinessLogic_stagevars.filter(
    (col("svFeeDscntAmtDiff") != 0) & (col("svValidateResults") == 1)
).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    col("svFeeDscntAmtDiff").alias("FEE_DSCNT_AMT_DIFF"),
    lit("Z").alias("Dummy"),
)

df_lkp_Fee_dscrtn_incm_balance = (
    df_lnk_All_Edw_Recs2.alias("lnk_All_Edw_Recs2")
    .join(
        df_lnk_No_Correction_in.alias("lnk_No_Correction_in"),
        on=[
            col("lnk_All_Edw_Recs2.SRC_SYS_CD") == col("lnk_No_Correction_in.SRC_SYS_CD"),
            col("lnk_All_Edw_Recs2.BILL_INVC_ID") == col("lnk_No_Correction_in.BILL_INVC_ID"),
            col("lnk_All_Edw_Recs2.FEE_DSCNT_ID") == col("lnk_No_Correction_in.FEE_DSCNT_ID"),
            col("lnk_All_Edw_Recs2.INVC_FEE_DSCNT_BILL_DISP_CD")
            == col("lnk_No_Correction_in.INVC_FEE_DSCNT_BILL_DISP_CD"),
        ],
        how="left",
    )
    .select(
        col("lnk_All_Edw_Recs2.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_All_Edw_Recs2.BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("lnk_All_Edw_Recs2.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("lnk_All_Edw_Recs2.INVC_FEE_DSCNT_BILL_DISP_CD").alias(
            "INVC_FEE_DSCNT_BILL_DISP_CD"
        ),
        col("lnk_All_Edw_Recs2.INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
        col("lnk_All_Edw_Recs2.GRP_ID").alias("GRP_ID"),
        col("lnk_All_Edw_Recs2.SUBGRP_ID").alias("SUBGRP_ID"),
        col("lnk_All_Edw_Recs2.CLS_ID").alias("CLS_ID"),
        col("lnk_All_Edw_Recs2.CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("lnk_All_Edw_Recs2.PROD_ID").alias("PROD_ID"),
        col("lnk_All_Edw_Recs2.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("lnk_All_Edw_Recs2.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias(
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK"
        ),
        col("lnk_All_Edw_Recs2.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        col("lnk_All_Edw_Recs2.CLS_SK").alias("CLS_SK"),
        col("lnk_All_Edw_Recs2.CLS_PLN_SK").alias("CLS_PLN_SK"),
        col("lnk_All_Edw_Recs2.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        col("lnk_All_Edw_Recs2.GRP_SK").alias("GRP_SK"),
        col("lnk_All_Edw_Recs2.SUBGRP_SK").alias("SUBGRP_SK"),
        col("lnk_All_Edw_Recs2.INVC_TYP_CD").alias("INVC_TYP_CD"),
        col("lnk_All_Edw_Recs2.DSCNT_IN").alias("DSCNT_IN"),
        col("lnk_All_Edw_Recs2.FEE_DSCNT_YR_MO_SK").alias("FEE_DSCNT_YR_MO_SK"),
        col("lnk_All_Edw_Recs2.INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
        col("lnk_All_Edw_Recs2.INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
        col("lnk_All_Edw_Recs2.INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
        col("lnk_All_Edw_Recs2.INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
        col("lnk_All_Edw_Recs2.INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
        col("lnk_All_Edw_Recs2.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        col("lnk_All_Edw_Recs2.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("lnk_All_Edw_Recs2.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_All_Edw_Recs2.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_All_Edw_Recs2.INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
        col("lnk_All_Edw_Recs2.INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias(
            "INVC_FEE_DSCNT_BILL_DISP_CD_SK"
        ),
        col("lnk_All_Edw_Recs2.INVC_FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
        col("lnk_All_Edw_Recs2.INVC_SK").alias("INVC_SK"),
        col("lnk_All_Edw_Recs2.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
        col("lnk_All_Edw_Recs2.SUB_SK").alias("SUB_SK"),
        col("lnk_All_Edw_Recs2.PROD_SK").alias("PROD_SK"),
        col("lnk_No_Correction_in.Dummy").alias("Dummy"),
    )
)

df_xfm_BusinessLogic_Bal_Recs = df_lkp_Fee_dscrtn_incm_balance.filter(
    col("Dummy").isNotNull()
).select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    col("INVC_FEE_DSCNT_SRC_CD"),
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
    col("SUBGRP_SK"),
    col("INVC_TYP_CD"),
    col("DSCNT_IN"),
    col("FEE_DSCNT_YR_MO_SK"),
    col("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK"),
    col("FEE_DSCNT_AMT"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_FEE_DSCNT_SRC_CD_SK"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    col("INVC_FEE_DSCNT_SK"),
    col("INVC_SK"),
    col("INVC_TYP_CD_SK"),
    col("SUB_SK"),
    col("PROD_SK"),
)

df_lkp_Amounts = (
    df_lnk_All_Edw_Recs1.alias("lnk_All_Edw_Recs1")
    .join(
        df_lnk_Needs_Correction_in.alias("lnk_Needs_Correction_in"),
        on=[
            col("lnk_All_Edw_Recs1.SRC_SYS_CD") == col("lnk_Needs_Correction_in.SRC_SYS_CD"),
            col("lnk_All_Edw_Recs1.BILL_INVC_ID") == col("lnk_Needs_Correction_in.BILL_INVC_ID"),
            col("lnk_All_Edw_Recs1.FEE_DSCNT_ID") == col("lnk_Needs_Correction_in.FEE_DSCNT_ID"),
            col("lnk_All_Edw_Recs1.INVC_FEE_DSCNT_BILL_DISP_CD")
            == col("lnk_Needs_Correction_in.INVC_FEE_DSCNT_BILL_DISP_CD"),
        ],
        how="left",
    )
    .join(
        df_db2_W_FEE_DSCNT_INCM_Max_in.alias("lnk_Src_Ref"),
        on=[
            col("lnk_All_Edw_Recs1.SRC_SYS_CD") == col("lnk_Src_Ref.SRC_SYS_CD"),
            col("lnk_All_Edw_Recs1.BILL_INVC_ID") == col("lnk_Src_Ref.BILL_INVC_ID"),
            col("lnk_All_Edw_Recs1.FEE_DSCNT_ID") == col("lnk_Src_Ref.FEE_DSCNT_ID"),
            col("lnk_All_Edw_Recs1.INVC_FEE_DSCNT_BILL_DISP_CD")
            == col("lnk_Src_Ref.INVC_FEE_DSCNT_BILL_DISP_CD"),
            col("lnk_All_Edw_Recs1.FEE_DSCNT_YR_MO_SK")
            == col("lnk_Src_Ref.FEE_DSCNT_YR_MO_SK"),
            col("lnk_All_Edw_Recs1.GRP_ID") == col("lnk_Src_Ref.GRP_ID"),
            col("lnk_All_Edw_Recs1.SUBGRP_ID") == col("lnk_Src_Ref.SUBGRP_ID"),
            col("lnk_All_Edw_Recs1.CLS_ID") == col("lnk_Src_Ref.CLS_ID"),
            col("lnk_All_Edw_Recs1.CLS_PLN_ID") == col("lnk_Src_Ref.CLS_PLN_ID"),
            col("lnk_All_Edw_Recs1.PROD_ID") == col("lnk_Src_Ref.PROD_ID"),
        ],
        how="left",
    )
    .select(
        col("lnk_All_Edw_Recs1.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_All_Edw_Recs1.BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("lnk_All_Edw_Recs1.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("lnk_All_Edw_Recs1.INVC_FEE_DSCNT_BILL_DISP_CD").alias(
            "INVC_FEE_DSCNT_BILL_DISP_CD"
        ),
        col("lnk_All_Edw_Recs1.INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
        col("lnk_All_Edw_Recs1.GRP_ID").alias("GRP_ID"),
        col("lnk_All_Edw_Recs1.SUBGRP_ID").alias("SUBGRP_ID"),
        col("lnk_All_Edw_Recs1.CLS_ID").alias("CLS_ID"),
        col("lnk_All_Edw_Recs1.CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("lnk_All_Edw_Recs1.PROD_ID").alias("PROD_ID"),
        col("lnk_All_Edw_Recs1.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("lnk_All_Edw_Recs1.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias(
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK"
        ),
        col("lnk_All_Edw_Recs1.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        col("lnk_All_Edw_Recs1.CLS_SK").alias("CLS_SK"),
        col("lnk_All_Edw_Recs1.CLS_PLN_SK").alias("CLS_PLN_SK"),
        col("lnk_All_Edw_Recs1.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        col("lnk_All_Edw_Recs1.GRP_SK").alias("GRP_SK"),
        col("lnk_All_Edw_Recs1.SUBGRP_SK").alias("SUBGRP_SK"),
        col("lnk_All_Edw_Recs1.INVC_TYP_CD").alias("INVC_TYP_CD"),
        col("lnk_All_Edw_Recs1.DSCNT_IN").alias("DSCNT_IN"),
        col("lnk_All_Edw_Recs1.FEE_DSCNT_YR_MO_SK").alias("FEE_DSCNT_YR_MO_SK"),
        col("lnk_All_Edw_Recs1.INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
        col("lnk_All_Edw_Recs1.INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
        col("lnk_All_Edw_Recs1.INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
        col("lnk_All_Edw_Recs1.INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
        col("lnk_All_Edw_Recs1.INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
        col("lnk_All_Edw_Recs1.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        col("lnk_All_Edw_Recs1.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("lnk_All_Edw_Recs1.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_All_Edw_Recs1.LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("lnk_All_Edw_Recs1.INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
        col("lnk_All_Edw_Recs1.INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias(
            "INVC_FEE_DSCNT_BILL_DISP_CD_SK"
        ),
        col("lnk_All_Edw_Recs1.INVC_FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
        col("lnk_All_Edw_Recs1.INVC_SK").alias("INVC_SK"),
        col("lnk_All_Edw_Recs1.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
        col("lnk_All_Edw_Recs1.SUB_SK").alias("SUB_SK"),
        col("lnk_All_Edw_Recs1.PROD_SK").alias("PROD_SK"),
        col("lnk_Needs_Correction_in.FEE_DSCNT_AMT_DIFF").alias("FEE_DSCNT_AMT_DIFF"),
        col("lnk_Needs_Correction_in.Dummy").alias("Dummy"),
        col("lnk_Src_Ref.Dummy").alias("Dummy_1"),
    )
)

df_xfrm_BusinessLogic_Amts_sv = df_lkp_Amounts.withColumn(
    "svFeeDscntAmt",
    when(
        (col("Dummy").isNotNull()) & (col("Dummy_1").isNull()),
        col("FEE_DSCNT_AMT"),
    )
    .when(
        (col("Dummy").isNotNull()) & (col("FEE_DSCNT_AMT_DIFF") > 0),
        col("FEE_DSCNT_AMT") - col("FEE_DSCNT_AMT_DIFF"),
    )
    .when(
        (col("Dummy").isNotNull()) & (col("FEE_DSCNT_AMT_DIFF") <= 0),
        col("FEE_DSCNT_AMT") + (col("FEE_DSCNT_AMT_DIFF") * lit(-1)),
    )
    .otherwise(lit(0)),
)

df_lnk_Fee_Dscrtn_Incm_Amts_in = df_xfrm_BusinessLogic_Amts_sv.filter(
    col("Dummy").isNotNull()
).select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    col("INVC_FEE_DSCNT_SRC_CD"),
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
    col("SUBGRP_SK"),
    col("INVC_TYP_CD"),
    col("DSCNT_IN"),
    col("FEE_DSCNT_YR_MO_SK"),
    col("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK"),
    col("svFeeDscntAmt").alias("FEE_DSCNT_AMT"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_FEE_DSCNT_SRC_CD_SK"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    col("INVC_FEE_DSCNT_SK"),
    col("INVC_SK"),
    col("INVC_TYP_CD_SK"),
    col("SUB_SK"),
    col("PROD_SK"),
)

df_lnk_Bal_Recs = df_xfm_BusinessLogic_Bal_Recs

df_fnl_Data_Links = df_lnk_Fee_Dscrtn_Incm_Amts_in.unionByName(df_lnk_Bal_Recs, allowMissingColumns=True)

df_rdp_Dscrnt_Incm_Mnth_Out = dedup_sort(
    df_fnl_Data_Links,
    partition_cols=[
        "SRC_SYS_CD",
        "BILL_INVC_ID",
        "FEE_DSCNT_ID",
        "INVC_FEE_DSCNT_BILL_DISP_CD",
        "INVC_FEE_DSCNT_SRC_CD",
        "GRP_ID",
        "SUBGRP_ID",
        "CLS_ID",
        "CLS_PLN_ID",
        "PROD_ID",
    ],
    sort_cols=[
        ("SRC_SYS_CD", "A"),
        ("BILL_INVC_ID", "A"),
        ("FEE_DSCNT_ID", "A"),
        ("INVC_FEE_DSCNT_BILL_DISP_CD", "A"),
        ("INVC_FEE_DSCNT_SRC_CD", "A"),
        ("GRP_ID", "A"),
        ("SUBGRP_ID", "A"),
        ("CLS_ID", "A"),
        ("CLS_PLN_ID", "A"),
        ("PROD_ID", "A"),
    ],
)

df_lkp_Invoice = (
    df_rdp_Dscrnt_Incm_Mnth_Out.alias("lnk_Dscrtn_Incm_Alloc_All")
    .join(
        df_INCM_INVC_DS.alias("Ref_InvoiceSk"),
        on=[col("lnk_Dscrtn_Incm_Alloc_All.INVC_SK") == col("Ref_InvoiceSk.INVC_SK")],
        how="left",
    )
    .select(
        col("lnk_Dscrtn_Incm_Alloc_All.SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("lnk_Dscrtn_Incm_Alloc_All.BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("lnk_Dscrtn_Incm_Alloc_All.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_FEE_DSCNT_BILL_DISP_CD").alias(
            "INVC_FEE_DSCNT_BILL_DISP_CD"
        ),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
        col("lnk_Dscrtn_Incm_Alloc_All.GRP_ID").alias("GRP_ID"),
        col("lnk_Dscrtn_Incm_Alloc_All.SUBGRP_ID").alias("SUBGRP_ID"),
        col("lnk_Dscrtn_Incm_Alloc_All.CLS_ID").alias("CLS_ID"),
        col("lnk_Dscrtn_Incm_Alloc_All.CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("lnk_Dscrtn_Incm_Alloc_All.PROD_ID").alias("PROD_ID"),
        col("lnk_Dscrtn_Incm_Alloc_All.CRT_RUN_CYC_EXCTN_DT_SK").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.LAST_UPDT_RUN_CYC_EXCTN_DT_SK").alias(
            "LAST_UPDT_RUN_CYC_EXCTN_DT_SK"
        ),
        col("lnk_Dscrtn_Incm_Alloc_All.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.CLS_SK").alias("CLS_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.CLS_PLN_SK").alias("CLS_PLN_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.GRP_SK").alias("GRP_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.SUBGRP_SK").alias("SUBGRP_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_TYP_CD").alias("INVC_TYP_CD"),
        col("lnk_Dscrtn_Incm_Alloc_All.DSCNT_IN").alias("DSCNT_IN"),
        col("lnk_Dscrtn_Incm_Alloc_All.FEE_DSCNT_YR_MO_SK").alias("FEE_DSCNT_YR_MO_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        col("lnk_Dscrtn_Incm_Alloc_All.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        col("lnk_Dscrtn_Incm_Alloc_All.CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.LAST_UPDT_RUN_CYC_EXCTN_SK").alias(
            "LAST_UPDT_RUN_CYC_EXCTN_SK"
        ),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias(
            "INVC_FEE_DSCNT_BILL_DISP_CD_SK"
        ),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_SK").alias("INVC_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.SUB_SK").alias("SUB_SK"),
        col("lnk_Dscrtn_Incm_Alloc_All.PROD_SK").alias("PROD_SK"),
        when(trim(col("Ref_InvoiceSk.CUR_RCRD_IN")) == lit(""), lit("N")).otherwise(
            col("Ref_InvoiceSk.CUR_RCRD_IN")
        ).alias("CUR_RCRD_IN"),
    )
)

df_xfrm_BusinessLogic3 = df_lkp_Invoice.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    col("INVC_FEE_DSCNT_SRC_CD"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    lit(CurrentRunDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    lit(CurrentRunDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    col("BILL_ENTY_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("FEE_DSCNT_SK"),
    col("GRP_SK"),
    col("SUBGRP_SK"),
    col("INVC_TYP_CD"),
    col("DSCNT_IN"),
    when(trim(col("CUR_RCRD_IN")) == lit(""), lit("N")).otherwise(col("CUR_RCRD_IN")).alias(
        "INVC_CUR_RCRD_IN"
    ),
    col("FEE_DSCNT_YR_MO_SK"),
    col("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK"),
    col("FEE_DSCNT_AMT"),
    col("SUB_UNIQ_KEY"),
    lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_FEE_DSCNT_SRC_CD_SK"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    col("INVC_FEE_DSCNT_SK"),
    col("INVC_SK"),
    col("INVC_TYP_CD_SK"),
    col("SUB_SK"),
    col("PROD_SK"),
)

df_Copy_127 = df_xfrm_BusinessLogic3

df_lnk_IdsEdwRcvdIncmFExtr_OutABC = df_Copy_127.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    col("INVC_FEE_DSCNT_SRC_CD"),
    col("GRP_ID"),
    col("SUBGRP_ID"),
    col("CLS_ID"),
    col("CLS_PLN_ID"),
    col("PROD_ID"),
    col("FEE_DSCNT_AMT"),
)

df_lnk_IdsEdwErnIncmFExtrr_OutABC = df_Copy_127.select(
    col("SRC_SYS_CD"),
    col("BILL_INVC_ID"),
    col("FEE_DSCNT_ID"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    col("INVC_FEE_DSCNT_SRC_CD"),
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
    col("SUBGRP_SK"),
    col("INVC_TYP_CD"),
    col("DSCNT_IN"),
    col("INVC_CUR_RCRD_IN"),
    col("FEE_DSCNT_YR_MO_SK"),
    col("INVC_BILL_DUE_DT_SK"),
    col("INVC_BILL_DUE_YR_MO_SK"),
    col("INVC_BILL_END_DT_SK"),
    col("INVC_BILL_END_YR_MO_SK"),
    col("INVC_CRT_DT_SK"),
    col("FEE_DSCNT_AMT"),
    col("SUB_UNIQ_KEY"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("INVC_FEE_DSCNT_SRC_CD_SK"),
    col("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    col("INVC_FEE_DSCNT_SK"),
    col("INVC_SK"),
    col("INVC_TYP_CD_SK"),
    col("SUB_SK"),
    col("PROD_SK"),
)

df_ds_FEE_DSCNT_INCM_F_out = df_lnk_IdsEdwErnIncmFExtrr_OutABC

df_ds_FEE_DSCNT_INCM_F_out = df_ds_FEE_DSCNT_INCM_F_out.withColumn(
    "CRT_RUN_CYC_EXCTN_DT_SK", rpad(col("CRT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK", rpad(col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"), 10, " ")
).withColumn(
    "DSCNT_IN", rpad(col("DSCNT_IN"), 1, " ")
).withColumn(
    "INVC_CUR_RCRD_IN", rpad(col("INVC_CUR_RCRD_IN"), 1, " ")
).withColumn(
    "FEE_DSCNT_YR_MO_SK", rpad(col("FEE_DSCNT_YR_MO_SK"), 6, " ")
).withColumn(
    "INVC_BILL_DUE_DT_SK", rpad(col("INVC_BILL_DUE_DT_SK"), 10, " ")
).withColumn(
    "INVC_BILL_DUE_YR_MO_SK", rpad(col("INVC_BILL_DUE_YR_MO_SK"), 6, " ")
).withColumn(
    "INVC_BILL_END_DT_SK", rpad(col("INVC_BILL_END_DT_SK"), 10, " ")
).withColumn(
    "INVC_BILL_END_YR_MO_SK", rpad(col("INVC_BILL_END_YR_MO_SK"), 6, " ")
).withColumn(
    "INVC_CRT_DT_SK", rpad(col("INVC_CRT_DT_SK"), 10, " ")
)

df_ds_FEE_DSCNT_INCM_F_out = df_ds_FEE_DSCNT_INCM_F_out.select(
    "SRC_SYS_CD",
    "BILL_INVC_ID",
    "FEE_DSCNT_ID",
    "INVC_FEE_DSCNT_BILL_DISP_CD",
    "INVC_FEE_DSCNT_SRC_CD",
    "GRP_ID",
    "SUBGRP_ID",
    "CLS_ID",
    "CLS_PLN_ID",
    "PROD_ID",
    "CRT_RUN_CYC_EXCTN_DT_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BILL_ENTY_SK",
    "CLS_SK",
    "CLS_PLN_SK",
    "FEE_DSCNT_SK",
    "GRP_SK",
    "SUBGRP_SK",
    "INVC_TYP_CD",
    "DSCNT_IN",
    "INVC_CUR_RCRD_IN",
    "FEE_DSCNT_YR_MO_SK",
    "INVC_BILL_DUE_DT_SK",
    "INVC_BILL_DUE_YR_MO_SK",
    "INVC_BILL_END_DT_SK",
    "INVC_BILL_END_YR_MO_SK",
    "INVC_CRT_DT_SK",
    "FEE_DSCNT_AMT",
    "SUB_UNIQ_KEY",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INVC_FEE_DSCNT_SRC_CD_SK",
    "INVC_FEE_DSCNT_BILL_DISP_CD_SK",
    "INVC_FEE_DSCNT_SK",
    "INVC_SK",
    "INVC_TYP_CD_SK",
    "SUB_SK",
    "PROD_SK",
)

write_files(
    df_ds_FEE_DSCNT_INCM_F_out,
    "FEE_DSCNT_INCM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote='"',
    nullValue=None,
)

df_seq_B_FEE_DSCNT_INCM_F_csv_load = df_lnk_IdsEdwRcvdIncmFExtr_OutABC
df_seq_B_FEE_DSCNT_INCM_F_csv_load = df_seq_B_FEE_DSCNT_INCM_F_csv_load.select(
    "SRC_SYS_CD",
    "BILL_INVC_ID",
    "FEE_DSCNT_ID",
    "INVC_FEE_DSCNT_BILL_DISP_CD",
    "INVC_FEE_DSCNT_SRC_CD",
    "GRP_ID",
    "SUBGRP_ID",
    "CLS_ID",
    "CLS_PLN_ID",
    "PROD_ID",
    "FEE_DSCNT_AMT",
)

write_files(
    df_seq_B_FEE_DSCNT_INCM_F_csv_load,
    f"{adls_path}/load/B_FEE_DSCNT_INCM_F.dat",
    delimiter=",",
    mode="append",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None,
)