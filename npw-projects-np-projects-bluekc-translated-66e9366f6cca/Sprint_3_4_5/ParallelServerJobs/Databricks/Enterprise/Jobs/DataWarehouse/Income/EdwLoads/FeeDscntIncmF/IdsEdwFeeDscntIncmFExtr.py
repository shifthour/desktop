# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     EdwIncomeFeeDscntIncmFExtr
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC 
# MAGIC Fee Discount Income - Earned Fee Income either at a Grp, Subgroup, Class, ClassPlan, Product or at a Subscriber Level.    In IDS Income Fee Discount, there is one record in the table for the total fee discount amount for either the Subscriber or at a Group, Subgroup, Class, ClassPlan, and Product level.  
# MAGIC 
# MAGIC In EDW Fee Discount Income Fact, this one IDS record of income will be equated to one EDW record if the fee discount is at a subscrber level.  
# MAGIC 
# MAGIC If the fee is at a Group, Subgroup, Class, ClassPlan, and Product level and all of these fields have a value other than 0/1 in IDS, then there be one record in EDW.   
# MAGIC 
# MAGIC If the fee is at a Group, Subgroup, Class, ClassPlan, and Product level and at least one of these fields has a value of either 0/1 IDS, then further allocation will be required, and there may be multiple rows in EDW.  
# MAGIC 
# MAGIC Two ways to tell which type of level.  
# MAGIC 1.	Check value of Sub_SK.  Its either 0 or >0.  If 0, then 
# MAGIC 2.	Check value of the BILL_ENTY_LVL_CD for that invoice tied to the Discretionary Income.    It will be either INDVSUB (Subscriber) 
# MAGIC       or SUBGRP (Grp, Subgroup, Class, ClassPlan, Product)
# MAGIC 
# MAGIC Best way to determine is by BILL_ENTY_LVL_CD.
# MAGIC 
# MAGIC If the Sub_Sk = 0 then it is earned fee income at a Grp, Subgroup, Class, ClassPlan, Product Level.  Sub Unique Key should = 0.  Any of the Grp, Subgroup, Class, ClassPlan, Product SK's can be 0 or 1.
# MAGIC 
# MAGIC If the Sub_Sk > 1, then it is earned fee income at a Subscriber level.  Sub Unique Key should not = 0.  Any of the Grp, Subgroup, Class, ClassPlan, Product SK's should not be 0 or 1.
# MAGIC 
# MAGIC Fee Discount does not need to break down the amount across months like Discretionary Income does.  This is a one-time fee discount.   Discretionary Income has MonthQuantity field.  Fee Discount does not.
# MAGIC  
# MAGIC Only for fees at a Grp, Subgroup, Class, ClassPlan, Product level, the income can be for and allocated over many different subgroups, class, class plan, products.     Once EdwIncomeFeeDscntIncmFExtr has determined the fee needs to be allocated further across the Subgroup, Class, ClassPlan, Product for that Group, then it will be processed in EdwIncomeAllocateFeeDscntIncm.   EdwIncomeAllocateFeeDscntIncm divides the the money across the different Subgroup, Class, ClassPlan, Products for that Group.     It matches all that it knows to W_MBR_RCST (a working table of Member Recast Counts)  to get all of the possible combinations of Subgroup, Class, ClassPlan, Product.    A record for all possible combinations is written out and the monies is divided by the percentage of members in that Subgroup, Class, ClassPlan, Product.
# MAGIC 
# MAGIC Once the money is divided amoung all possible Subgroup, Class, ClassPlan, Product for that Group, further balancing on the amounts are needed.  Due to rounding issues when multiplying the amounts by the membership percentage, when all of the amounts are added back up, they do not equal the IDS amounts.   EdwIncomBalanceFeeDscntIncmF adds to or subtracts from the EDW amounts so that it will balance IDS.
# MAGIC 
# MAGIC 
# MAGIC INPUTS:
# MAGIC 	IDS:     INVC_FEE_DSCNT
# MAGIC                             INVC
# MAGIC                             GRP
# MAGIC                            SUB
# MAGIC                            CLS
# MAGIC                            SUBGRP
# MAGIC                            CLASS PLAN
# MAGIC                             PRODUCT
# MAGIC 
# MAGIC HASH FILES:
# MAGIC                  hf_cdma_codes         - code mapping loaded in the CDMA extract.
# MAGIC                  hf_fee_dscnt_incm_ids_invc;
# MAGIC                  hf_fee_dscnt_incm_ids_group;
# MAGIC                  hf_fee_dscnt_incm_ids_sub  
# MAGIC                  hf_fee_dscnt_incm_ids_cls 
# MAGIC                  hf_fee_dscnt_incm_ids_subgrp
# MAGIC                  hf_fee_dscnt_incm_ids_clspln
# MAGIC                  hf_fee_dscnt_incm_ids_prod
# MAGIC 
# MAGIC                  hf_fee_dscnt_f
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC                   TRIM all the fields that are extracted or used in lookups
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC     Reads the IDS Income Fee Discount table for all records with a Last Activity Run Cycle GREATER than what EDW has on file as the last run cycle EDW processed.   
# MAGIC    The IDS Income Fee Discount   is processed daily.  EDW Fee Discount  Fact  will be processed monthly.  
# MAGIC     Looks up Invoice information , Subgroup ids, Class ids,  Class Plan ids, Product ids and decodes SK from hf_cdma_lookup.
# MAGIC    Then test if further allocation is needed (EdwIncomeAllocateFeeDscntF).  If yes, then write the record out to W_FEE_DSCNT  If no, pkey the record and write to FEE_DSCNT_F.
# MAGIC 
# MAGIC    Pkeys are assigned here and in EdwIncomeBalanceFeeDscntF
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                   FEE_DSCNT_F.dat  - loads to FEE_DSCNT_F
# MAGIC                  W_FEE_DSCNT_INCM.dat   - loads to W_FEE_DSCNT_INCM
# MAGIC 
# MAGIC MODIFICATIONS:a
# MAGIC               Sharon Andrew 2/07/2006  - Originally Programmed
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer          Date                Project/Altiris #                                        Change Description                                                                                                           Develop Project       Code Reviewer       Date Reviewed       
# MAGIC ------------------        --------------------     ------------------------                                       -----------------------------------------------------------------------                                                                   ------------------------       -------------------------------   ----------------------------       
# MAGIC Sandrew            2007-09-20      eproject #5137 Project Release 8.1          Changed source of the Current Record Incidator file                                                        Steph Goddard        devlEDW10             09/28/2007
# MAGIC                                                   IAD Quarterly Release                              Corrected any invalid RunCycle SK updates on load records.
# MAGIC                                                                                                                    Created new program EdwIncomePreRequHashFile will creat hash files used in this job
# MAGIC O. Nielsen          2008-05-21     Production Support                                   Added Balancing snapshot                                                                                                 devlEDW                 Steph Goddard        05/25/2008
# MAGIC Bhoomi Dasari   10/10/2008    3567 - Primary keying                                Added new primary key process                                                                                         devlEDW                Steph Goddard        10/15/2008
# MAGIC 
# MAGIC Bhoomi Dasari   2009-02-09     3567 - Primary keying                               Pulling from FEE_DSCNT_INCM_F instead of K_FEE_DSCNT_INCM_F                            devlEDW
# MAGIC                                                                                                                  on the K_table extract.
# MAGIC 
# MAGIC SAndrew       2009-07-10       TTR567                                                         Ensure Current Run Cycle getting pushed to Last Activity Run Cycle                              devlEDW                 Steph Goddard         07/28/2009            
# MAGIC 
# MAGIC Srikanth Mettpalli  2013-10-01          5114                            Original Programming                                                                                                                               EnterpriseWrhsDevl  Peter Marshall         12/12/2013           
# MAGIC                                                                                              (Server to Parallel Conversion)

# MAGIC Datasets are created in IdsEdwIncomePreReqExtr Job
# MAGIC W_FEE_DSCNT:     This working table is used by the Allocation job and stores all records has a 1 for Grp, SubGrp, Cls, ClsPln or Prod, therefore, requires MbrRecast data for further allocations.
# MAGIC Code SK lookups for Denormalization
# MAGIC Load B_FEE_DSCNT_INCM_F balancing table
# MAGIC Load FEE_DSCNT_INCM_F.ds dataset for the PKey Job
# MAGIC Add Defaults and Null Handling.
# MAGIC Code SK lookups for Denormalization
# MAGIC Job Name: IdsEdwFeeDscntIncmFExtr
# MAGIC Pull From IDS INVC_FEE_DSNT  and Updates  EDW  FEE_DSCNT_INCM_F
# MAGIC ** If all of the key fields are known, then they are immedidately loaded into FEE_DSCNT_INCM_F.  
# MAGIC ** If there any of the key fields are unknown,  those records are loaded into W_FEE_DSCNT for further processing.
# MAGIC ** Key fields are defined in trnsCodes transform
# MAGIC ** This job assigns surrogate keys for FEE_DSCNT_INCM_F
# MAGIC The BeginCycle parameter is for IDS Invoices
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame, functions as F, types as T
# COMMAND ----------
# MAGIC %run ../../../../../../Utility_Enterprise
# COMMAND ----------


# Parameters
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
EDWOwner = get_widget_value('EDWOwner','')
edw_secret_name = get_widget_value('edw_secret_name','')
CurrentRunDate = get_widget_value('CurrentRunDate','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
IDSIncmBeginCycle = get_widget_value('IDSIncmBeginCycle','')

# --------------------------------------------------------------------------------
# db2_CD_MPPNG_in (DB2ConnectorPX, database=IDS)
# --------------------------------------------------------------------------------
extract_query_db2_CD_MPPNG_in = f"SELECT CD_MPPNG_SK,COALESCE(TRGT_CD,'UNK') TRGT_CD,COALESCE(TRGT_CD_NM,'UNK') TRGT_CD_NM from {IDSOwner}.CD_MPPNG"
jdbc_url_ids, jdbc_props_ids = get_db_config(ids_secret_name)
df_db2_CD_MPPNG_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_CD_MPPNG_in)
    .load()
)

df_cpy_cd_mppng = df_db2_CD_MPPNG_in.select(
    F.col("CD_MPPNG_SK"),
    F.col("TRGT_CD"),
    F.col("TRGT_CD_NM")
)

# --------------------------------------------------------------------------------
# db2_INVC_FEE_DSCNT_in (DB2ConnectorPX, database=IDS)
# --------------------------------------------------------------------------------
extract_query_db2_INVC_FEE_DSCNT_in = (
    f"SELECT INVC_FEE_DSCNT.INVC_FEE_DSCNT_SK,"
    f"COALESCE(CD.TRGT_CD,'UNK') SRC_SYS_CD,"
    f"INVC_FEE_DSCNT.BILL_INVC_ID,"
    f"INVC_FEE_DSCNT.FEE_DSCNT_ID,"
    f"INVC_FEE_DSCNT.INVC_FEE_DSCNT_SRC_CD_SK,"
    f"INVC_FEE_DSCNT.INVC_FEE_DSCNT_BILL_DISP_CD_SK,"
    f"INVC_FEE_DSCNT.CLS_SK,"
    f"INVC_FEE_DSCNT.FEE_DSCNT_SK,"
    f"INVC_FEE_DSCNT.GRP_SK,"
    f"INVC_FEE_DSCNT.INVC_SK,"
    f"INVC_FEE_DSCNT.SUBGRP_SK,"
    f"INVC_FEE_DSCNT.SUB_SK,"
    f"INVC_FEE_DSCNT.DSCNT_IN,"
    f"INVC_FEE_DSCNT.FEE_DSCNT_AMT,"
    f"INVC_FEE_DSCNT.CLS_PLN_SK,"
    f"INVC_FEE_DSCNT.PROD_SK "
    f"FROM {IDSOwner}.INVC_FEE_DSCNT INVC_FEE_DSCNT "
    f"LEFT JOIN {IDSOwner}.CD_MPPNG CD "
    f"ON INVC_FEE_DSCNT.SRC_SYS_CD_SK = CD.CD_MPPNG_SK "
    f"WHERE INVC_FEE_DSCNT.LAST_UPDT_RUN_CYC_EXCTN_SK >= {IDSIncmBeginCycle} "
    f"and INVC_FEE_DSCNT.INVC_FEE_DSCNT_SK <> 0 "
    f"and INVC_FEE_DSCNT.INVC_FEE_DSCNT_SK <> 1"
)
df_db2_INVC_FEE_DSCNT_in = (
    spark.read.format("jdbc")
    .option("url", jdbc_url_ids)
    .options(**jdbc_props_ids)
    .option("query", extract_query_db2_INVC_FEE_DSCNT_in)
    .load()
)

# --------------------------------------------------------------------------------
# INCM_SUB_DS, INCM_CLSP_LN_DS, INCM_SUBGRP_DS, INCM_GROUP_DS, INCM_CLS_DS,
# INCM_INVC_DS, INCM_PROD_DS  (PxDataSet => read as parquet)
# --------------------------------------------------------------------------------
df_INCM_SUB_DS = spark.read.parquet(f"{adls_path}/ds/INCM_SUB.parquet")
df_INCM_CLSP_LN_DS = spark.read.parquet(f"{adls_path}/ds/INCM_CLSP_LN.parquet")
df_INCM_SUBGRP_DS = spark.read.parquet(f"{adls_path}/ds/INCM_SUBGRP.parquet")
df_INCM_GROUP_DS = spark.read.parquet(f"{adls_path}/ds/INCM_GROUP.parquet")
df_INCM_CLS_DS = spark.read.parquet(f"{adls_path}/ds/INCM_CLS.parquet")
df_INCM_INVC_DS = spark.read.parquet(f"{adls_path}/ds/INCM_INVC.parquet")
df_INCM_PROD_DS = spark.read.parquet(f"{adls_path}/ds/INCM_PROD.parquet")

# --------------------------------------------------------------------------------
# lkp_MbrData (PxLookup)
# --------------------------------------------------------------------------------
df_lkp_MbrData = (
    df_db2_INVC_FEE_DSCNT_in.alias("lnk_IdsEdwFeeDscntIncmFExtr_InABC")
    .join(
        df_INCM_INVC_DS.alias("Ref_InvoiceSk"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.INVC_SK") == F.col("Ref_InvoiceSk.INVC_SK"),
        "left"
    )
    .join(
        df_INCM_GROUP_DS.alias("Ref_GrpSk"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.GRP_SK") == F.col("Ref_GrpSk.GRP_SK"),
        "left"
    )
    .join(
        df_INCM_SUB_DS.alias("Ref_SubSk"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.SUB_SK") == F.col("Ref_SubSk.SUB_SK"),
        "left"
    )
    .join(
        df_INCM_CLS_DS.alias("Ref_ClsSk"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.CLS_SK") == F.col("Ref_ClsSk.CLS_SK"),
        "left"
    )
    .join(
        df_INCM_SUBGRP_DS.alias("Ref_SubgrpSK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.SUBGRP_SK") == F.col("Ref_SubgrpSK.SUBGRP_SK"),
        "left"
    )
    .join(
        df_INCM_CLSP_LN_DS.alias("Ref_ClspLnSK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.CLS_PLN_SK") == F.col("Ref_ClspLnSK.CLS_PLN_SK"),
        "left"
    )
    .join(
        df_INCM_PROD_DS.alias("Ref_ProdSk"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.PROD_SK") == F.col("Ref_ProdSk.PROD_SK"),
        "left"
    )
    .select(
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.BILL_INVC_ID").alias("BILL_INVC_ID"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        F.col("Ref_GrpSk.GRP_ID").alias("GRP_ID"),
        F.col("Ref_SubgrpSK.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("Ref_ClsSk.CLS_ID").alias("CLS_ID"),
        F.col("Ref_ClspLnSK.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("Ref_ProdSk.PROD_ID").alias("PROD_ID"),
        F.col("Ref_InvoiceSk.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.CLS_SK").alias("CLS_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.GRP_SK").alias("GRP_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.DSCNT_IN").alias("DSCNT_IN"),
        F.col("Ref_InvoiceSk.CUR_RCRD_IN").alias("INVC_CUR_RCRD_IN"),
        F.col("Ref_InvoiceSk.CRT_DT_SK").alias("INVC_CRT_DT_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        F.col("Ref_InvoiceSk.BILL_DUE_DT_SK").alias("BILL_DUE_DT_SK"),
        F.col("Ref_InvoiceSk.BILL_END_DT_SK").alias("BILL_END_DT_SK"),
        F.col("Ref_SubSk.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.INVC_FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.INVC_SK").alias("INVC_SK"),
        F.col("Ref_InvoiceSk.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.SUB_SK").alias("SUB_SK"),
        F.col("lnk_IdsEdwFeeDscntIncmFExtr_InABC.PROD_SK").alias("PROD_SK"),
    )
)

# --------------------------------------------------------------------------------
# xfrm_BusinessLogic1 (CTransformerStage)
# --------------------------------------------------------------------------------
df_xfrm_BusinessLogic1 = (
    df_lkp_MbrData
    .withColumn("SRC_SYS_CD", F.when(F.trim(F.col("SRC_SYS_CD")) == "", "UNK").otherwise(F.trim(F.col("SRC_SYS_CD"))))
    .withColumn("BILL_INVC_ID", F.col("BILL_INVC_ID"))
    .withColumn("FEE_DSCNT_ID", F.col("FEE_DSCNT_ID"))
    .withColumn("GRP_ID", F.when(F.trim(F.col("GRP_ID")) == "", "UNK").otherwise(F.trim(F.col("GRP_ID"))))
    .withColumn("SUBGRP_ID", F.when(F.trim(F.col("SUBGRP_ID")) == "", "UNK").otherwise(F.col("SUBGRP_ID")))
    .withColumn("CLS_ID", F.when(F.trim(F.col("CLS_ID")) == "", "UNK").otherwise(F.trim(F.col("CLS_ID"))))
    .withColumn("CLS_PLN_ID", F.when(F.trim(F.col("CLS_PLN_ID")) == "", "UNK").otherwise(F.trim(F.col("CLS_PLN_ID"))))
    .withColumn("PROD_ID", F.when(F.trim(F.col("PROD_ID")) == "", "UNK").otherwise(F.trim(F.col("PROD_ID"))))
    .withColumn("BILL_ENTY_SK", F.col("BILL_ENTY_SK"))
    .withColumn("CLS_SK", F.col("CLS_SK"))
    .withColumn("CLS_PLN_SK", F.col("CLS_PLN_SK"))
    .withColumn("FEE_DSCNT_SK", F.col("FEE_DSCNT_SK"))
    .withColumn("GRP_SK", F.col("GRP_SK"))
    .withColumn("SUBGRP_SK", F.col("SUBGRP_SK"))
    .withColumn("DSCNT_IN", F.col("DSCNT_IN"))
    .withColumn("INVC_CUR_RCRD_IN", F.col("INVC_CUR_RCRD_IN"))
    .withColumn(
        "FEE_DSCNT_YR_MO_SK",
        F.when(F.trim(F.col("BILL_DUE_DT_SK")) == "", "UNK")
        .otherwise(F.col("BILL_DUE_DT_SK").substr(F.lit(1), F.lit(4)).concat(F.col("BILL_DUE_DT_SK").substr(F.lit(6), F.lit(2))))
    )
    .withColumn(
        "INVC_BILL_DUE_DT_SK",
        F.when(F.trim(F.col("BILL_DUE_DT_SK")) == "", "UNK").otherwise(F.col("BILL_DUE_DT_SK"))
    )
    .withColumn(
        "INVC_BILL_DUE_YR_MO_SK",
        F.when(F.trim(F.col("BILL_DUE_DT_SK")) == "", "UNK")
        .otherwise(
            F.when(F.trim(F.col("BILL_DUE_DT_SK")) == "NA", "NA")
            .otherwise(F.col("BILL_DUE_DT_SK").substr(F.lit(1), F.lit(4)).concat(F.col("BILL_DUE_DT_SK").substr(F.lit(6), F.lit(2))))
        )
    )
    .withColumn(
        "INVC_BILL_END_DT_SK",
        F.when(F.trim(F.col("BILL_END_DT_SK")) == "", "UNK").otherwise(F.col("BILL_END_DT_SK"))
    )
    .withColumn(
        "INVC_BILL_END_YR_MO_SK",
        F.when(F.trim(F.col("BILL_END_DT_SK")) == "", "UNK")
        .otherwise(
            F.when(F.trim(F.col("BILL_END_DT_SK")) == "NA", "NA")
            .otherwise(F.col("BILL_END_DT_SK").substr(F.lit(1), F.lit(4)).concat(F.col("BILL_END_DT_SK").substr(F.lit(6), F.lit(2))))
        )
    )
    .withColumn(
        "INVC_CRT_DT_SK",
        F.when(F.trim(F.col("INVC_CRT_DT_SK")) == "", "UNK").otherwise(F.col("INVC_CRT_DT_SK"))
    )
    .withColumn("FEE_DSCNT_AMT", F.col("FEE_DSCNT_AMT"))
    .withColumn("SUB_UNIQ_KEY", F.col("SUB_UNIQ_KEY"))
    .withColumn("INVC_FEE_DSCNT_SRC_CD_SK", F.col("INVC_FEE_DSCNT_SRC_CD_SK"))
    .withColumn("INVC_FEE_DSCNT_BILL_DISP_CD_SK", F.col("INVC_FEE_DSCNT_BILL_DISP_CD_SK"))
    .withColumn("INVC_FEE_DSCNT_SK", F.col("INVC_FEE_DSCNT_SK"))
    .withColumn("INVC_SK", F.col("INVC_SK"))
    .withColumn("INVC_TYP_CD_SK", F.col("INVC_TYP_CD_SK"))
    .withColumn("SUB_SK", F.col("SUB_SK"))
    .withColumn("PROD_SK", F.col("PROD_SK"))
)

# --------------------------------------------------------------------------------
# lkp_ZipCd (PxLookup with references from cpy_cd_mppng)
# --------------------------------------------------------------------------------
df_lkp_ZipCd = (
    df_xfrm_BusinessLogic1.alias("lnk_Mbr_XfmData_out")
    .join(
        df_cpy_cd_mppng.alias("ref_FeeDscntSrc_Lkp"),
        F.col("lnk_Mbr_XfmData_out.INVC_FEE_DSCNT_SRC_CD_SK") == F.col("ref_FeeDscntSrc_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("ref_BillDisp_Lkp"),
        F.col("lnk_Mbr_XfmData_out.INVC_FEE_DSCNT_BILL_DISP_CD_SK") == F.col("ref_BillDisp_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .join(
        df_cpy_cd_mppng.alias("ref_InvcTyp_Lkp"),
        F.col("lnk_Mbr_XfmData_out.INVC_TYP_CD_SK") == F.col("ref_InvcTyp_Lkp.CD_MPPNG_SK"),
        "left"
    )
    .select(
        F.col("lnk_Mbr_XfmData_out.SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("lnk_Mbr_XfmData_out.BILL_INVC_ID").alias("BILL_INVC_ID"),
        F.col("lnk_Mbr_XfmData_out.FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        F.col("ref_BillDisp_Lkp.TRGT_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
        F.col("ref_FeeDscntSrc_Lkp.TRGT_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
        F.col("lnk_Mbr_XfmData_out.GRP_ID").alias("GRP_ID"),
        F.col("lnk_Mbr_XfmData_out.SUBGRP_ID").alias("SUBGRP_ID"),
        F.col("lnk_Mbr_XfmData_out.CLS_ID").alias("CLS_ID"),
        F.col("lnk_Mbr_XfmData_out.CLS_PLN_ID").alias("CLS_PLN_ID"),
        F.col("lnk_Mbr_XfmData_out.PROD_ID").alias("PROD_ID"),
        F.col("lnk_Mbr_XfmData_out.BILL_ENTY_SK").alias("BILL_ENTY_SK"),
        F.col("lnk_Mbr_XfmData_out.CLS_SK").alias("CLS_SK"),
        F.col("lnk_Mbr_XfmData_out.CLS_PLN_SK").alias("CLS_PLN_SK"),
        F.col("lnk_Mbr_XfmData_out.FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
        F.col("lnk_Mbr_XfmData_out.GRP_SK").alias("GRP_SK"),
        F.col("lnk_Mbr_XfmData_out.SUBGRP_SK").alias("SUBGRP_SK"),
        F.col("ref_InvcTyp_Lkp.TRGT_CD").alias("INVC_TYP_CD"),
        F.col("lnk_Mbr_XfmData_out.DSCNT_IN").alias("DSCNT_IN"),
        F.col("lnk_Mbr_XfmData_out.INVC_CUR_RCRD_IN").alias("INVC_CUR_RCRD_IN"),
        F.col("lnk_Mbr_XfmData_out.FEE_DSCNT_YR_MO_SK").alias("FEE_DSCNT_YR_MO_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_BILL_DUE_DT_SK").alias("INVC_BILL_DUE_DT_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_BILL_DUE_YR_MO_SK").alias("INVC_BILL_DUE_YR_MO_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_BILL_END_DT_SK").alias("INVC_BILL_END_DT_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_BILL_END_YR_MO_SK").alias("INVC_BILL_END_YR_MO_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_CRT_DT_SK").alias("INVC_CRT_DT_SK"),
        F.col("lnk_Mbr_XfmData_out.FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        F.col("lnk_Mbr_XfmData_out.SUB_UNIQ_KEY").alias("SUB_UNIQ_KEY"),
        F.col("lnk_Mbr_XfmData_out.INVC_FEE_DSCNT_SRC_CD_SK").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_FEE_DSCNT_BILL_DISP_CD_SK").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_SK").alias("INVC_SK"),
        F.col("lnk_Mbr_XfmData_out.INVC_TYP_CD_SK").alias("INVC_TYP_CD_SK"),
        F.col("lnk_Mbr_XfmData_out.SUB_SK").alias("SUB_SK"),
        F.col("lnk_Mbr_XfmData_out.PROD_SK").alias("PROD_SK"),
    )
)

# --------------------------------------------------------------------------------
# xfrm_BusinessLogic2 (CTransformerStage) => 3 output pins
# --------------------------------------------------------------------------------
df_xfrm_BusinessLogic2_in = df_lkp_ZipCd

df_lnk_main_data_in = df_xfrm_BusinessLogic2_in.select(
    F.col("SRC_SYS_CD"),
    F.col("BILL_INVC_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("INVC_FEE_DSCNT_SRC_CD"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.lit(CurrentRunDate).alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit(CurrentRunDate).alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.col("FEE_DSCNT_SK"),
    F.col("GRP_SK"),
    F.col("SUBGRP_SK"),
    F.col("INVC_TYP_CD"),
    F.col("DSCNT_IN"),
    F.col("INVC_CUR_RCRD_IN"),
    F.col("FEE_DSCNT_YR_MO_SK"),
    F.col("INVC_BILL_DUE_DT_SK"),
    F.col("INVC_BILL_DUE_YR_MO_SK"),
    F.col("INVC_BILL_END_DT_SK"),
    F.col("INVC_BILL_END_YR_MO_SK"),
    F.col("INVC_CRT_DT_SK"),
    F.col("FEE_DSCNT_AMT"),
    F.col("SUB_UNIQ_KEY"),
    F.lit(CurrRunCycle).alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit(CurrRunCycle).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INVC_FEE_DSCNT_SRC_CD_SK"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    F.col("INVC_FEE_DSCNT_SK"),
    F.col("INVC_SK"),
    F.col("INVC_TYP_CD_SK"),
    F.col("SUB_SK"),
    F.col("PROD_SK"),
)

df_lnk_NA_in_temp = df_xfrm_BusinessLogic2_in.limit(1)
df_lnk_NA_in = df_lnk_NA_in_temp.select(
    F.lit("NA").alias("SRC_SYS_CD"),
    F.lit("NA").alias("BILL_INVC_ID"),
    F.lit("NA").alias("FEE_DSCNT_ID"),
    F.lit("NA").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.lit("NA").alias("INVC_FEE_DSCNT_SRC_CD"),
    F.lit("NA").alias("GRP_ID"),
    F.lit("NA").alias("SUBGRP_ID"),
    F.lit("NA").alias("CLS_ID"),
    F.lit("NA").alias("CLS_PLN_ID"),
    F.lit("NA").alias("PROD_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1").alias("BILL_ENTY_SK"),
    F.lit("1").alias("CLS_SK"),
    F.lit("1").alias("CLS_PLN_SK"),
    F.lit("1").alias("FEE_DSCNT_SK"),
    F.lit("1").alias("GRP_SK"),
    F.lit("1").alias("SUBGRP_SK"),
    F.lit("NA").alias("INVC_TYP_CD"),
    F.lit("N").alias("DSCNT_IN"),
    F.lit("N").alias("INVC_CUR_RCRD_IN"),
    F.lit("175301").alias("FEE_DSCNT_YR_MO_SK"),
    F.lit("1753-01-01").alias("INVC_BILL_DUE_DT_SK"),
    F.lit("175301").alias("INVC_BILL_DUE_YR_MO_SK"),
    F.lit("1753-01-01").alias("INVC_BILL_END_DT_SK"),
    F.lit("175301").alias("INVC_BILL_END_YR_MO_SK"),
    F.lit("1753-01-01").alias("INVC_CRT_DT_SK"),
    F.lit("0").alias("FEE_DSCNT_AMT"),
    F.lit("1").alias("SUB_UNIQ_KEY"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("1").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    F.lit("1").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    F.lit("1").alias("INVC_FEE_DSCNT_SK"),
    F.lit("1").alias("INVC_SK"),
    F.lit("1").alias("INVC_TYP_CD_SK"),
    F.lit("1").alias("SUB_SK"),
    F.lit("1").alias("PROD_SK"),
)

df_lnk_UNK_in_temp = df_xfrm_BusinessLogic2_in.limit(1)
df_lnk_UNK_in = df_lnk_UNK_in_temp.select(
    F.lit("UNK").alias("SRC_SYS_CD"),
    F.lit("UNK").alias("BILL_INVC_ID"),
    F.lit("UNK").alias("FEE_DSCNT_ID"),
    F.lit("UNK").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.lit("UNK").alias("INVC_FEE_DSCNT_SRC_CD"),
    F.lit("UNK").alias("GRP_ID"),
    F.lit("UNK").alias("SUBGRP_ID"),
    F.lit("UNK").alias("CLS_ID"),
    F.lit("UNK").alias("CLS_PLN_ID"),
    F.lit("UNK").alias("PROD_ID"),
    F.lit("1753-01-01").alias("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("1753-01-01").alias("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.lit("0").alias("BILL_ENTY_SK"),
    F.lit("0").alias("CLS_SK"),
    F.lit("0").alias("CLS_PLN_SK"),
    F.lit("0").alias("FEE_DSCNT_SK"),
    F.lit("0").alias("GRP_SK"),
    F.lit("0").alias("SUBGRP_SK"),
    F.lit("UNK").alias("INVC_TYP_CD"),
    F.lit("N").alias("DSCNT_IN"),
    F.lit("N").alias("INVC_CUR_RCRD_IN"),
    F.lit("175301").alias("FEE_DSCNT_YR_MO_SK"),
    F.lit("1753-01-01").alias("INVC_BILL_DUE_DT_SK"),
    F.lit("175301").alias("INVC_BILL_DUE_YR_MO_SK"),
    F.lit("1753-01-01").alias("INVC_BILL_END_DT_SK"),
    F.lit("175301").alias("INVC_BILL_END_YR_MO_SK"),
    F.lit("1753-01-01").alias("INVC_CRT_DT_SK"),
    F.lit("0").alias("FEE_DSCNT_AMT"),
    F.lit("0").alias("SUB_UNIQ_KEY"),
    F.lit("100").alias("CRT_RUN_CYC_EXCTN_SK"),
    F.lit("100").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.lit("0").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    F.lit("0").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    F.lit("0").alias("INVC_FEE_DSCNT_SK"),
    F.lit("0").alias("INVC_SK"),
    F.lit("0").alias("INVC_TYP_CD_SK"),
    F.lit("0").alias("SUB_SK"),
    F.lit("0").alias("PROD_SK"),
)

# --------------------------------------------------------------------------------
# fnl_Data (PxFunnel)
# --------------------------------------------------------------------------------
common_cols_fnl = [
    "SRC_SYS_CD","BILL_INVC_ID","FEE_DSCNT_ID","INVC_FEE_DSCNT_BILL_DISP_CD","INVC_FEE_DSCNT_SRC_CD",
    "GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PROD_ID","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BILL_ENTY_SK","CLS_SK","CLS_PLN_SK","FEE_DSCNT_SK","GRP_SK","SUBGRP_SK","INVC_TYP_CD","DSCNT_IN",
    "INVC_CUR_RCRD_IN","FEE_DSCNT_YR_MO_SK","INVC_BILL_DUE_DT_SK","INVC_BILL_DUE_YR_MO_SK","INVC_BILL_END_DT_SK",
    "INVC_BILL_END_YR_MO_SK","INVC_CRT_DT_SK","FEE_DSCNT_AMT","SUB_UNIQ_KEY","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INVC_FEE_DSCNT_SRC_CD_SK","INVC_FEE_DSCNT_BILL_DISP_CD_SK","INVC_FEE_DSCNT_SK","INVC_SK","INVC_TYP_CD_SK","SUB_SK","PROD_SK"
]
df_lnk_NA_in_sel = df_lnk_NA_in.select(*common_cols_fnl)
df_lnk_main_data_in_sel = df_lnk_main_data_in.select(*common_cols_fnl)
df_lnk_UNK_in_sel = df_lnk_UNK_in.select(*common_cols_fnl)

df_fnl_Data = df_lnk_NA_in_sel.unionByName(df_lnk_main_data_in_sel).unionByName(df_lnk_UNK_in_sel)

# --------------------------------------------------------------------------------
# xfrm_BusinessLogic3 (CTransformerStage) => 2 output pins
# --------------------------------------------------------------------------------
df_xfrm_BusinessLogic3_in = df_fnl_Data

df_lnk_xfrm_BusinessLogic3_Data_out = df_xfrm_BusinessLogic3_in.filter(
    (
        (
            (F.col("GRP_SK") != 0) & (F.col("GRP_SK") != 1)
            & (F.col("SUBGRP_SK") != 0) & (F.col("SUBGRP_SK") != 1)
            & (F.col("CLS_SK") != 0) & (F.col("CLS_SK") != 1)
            & (F.col("CLS_PLN_SK") != 0) & (F.col("CLS_PLN_SK") != 1)
            & (F.col("PROD_SK") != 0) & (F.col("PROD_SK") != 1)
        )
        | (
            (F.col("GRP_SK") == 0)
            & (F.col("SUBGRP_SK") == 0)
            & (F.col("CLS_SK") == 0)
            & (F.col("CLS_PLN_SK") == 0)
            & (F.col("PROD_SK") == 0)
        )
        | (
            (F.col("GRP_SK") == 1)
            & (F.col("SUBGRP_SK") == 1)
            & (F.col("CLS_SK") == 1)
            & (F.col("CLS_PLN_SK") == 1)
            & (F.col("PROD_SK") == 1)
        )
        | (F.col("SUB_SK") != 0)
        | (F.col("SUB_SK") != 1)
    )
)

df_lnk_W_FEE_DSCNT_INCM_OutABC = df_xfrm_BusinessLogic3_in.filter(
    (
        (
            (F.col("GRP_SK") == 0)
            | (F.col("GRP_SK") == 1)
            | (F.col("SUBGRP_SK") == 0)
            | (F.col("SUBGRP_SK") == 1)
            | (F.col("CLS_SK") == 0)
            | (F.col("CLS_SK") == 1)
            | (F.col("CLS_PLN_SK") == 0)
            | (F.col("CLS_PLN_SK") == 1)
            | (F.col("PROD_SK") == 0)
            | (F.col("PROD_SK") == 1)
        )
        & (
            ~(
                (F.col("GRP_SK") == 0)
                & (F.col("SUBGRP_SK") == 0)
                & (F.col("CLS_SK") == 0)
                & (F.col("CLS_PLN_SK") == 0)
                & (F.col("PROD_SK") == 0)
            )
        )
        & (
            ~(
                (F.col("GRP_SK") == 1)
                & (F.col("SUBGRP_SK") == 1)
                & (F.col("CLS_SK") == 1)
                & (F.col("CLS_PLN_SK") == 1)
                & (F.col("PROD_SK") == 1)
            )
        )
        & (
            (F.col("SUB_SK") == 0)
            | (F.col("SUB_SK") == 1)
        )
    )
)

# --------------------------------------------------------------------------------
# seq_W_FEE_DSCNT_INCM_csv_load (PxSequentialFile => write)
# --------------------------------------------------------------------------------
write_cols_WFEE = [
    "SRC_SYS_CD","BILL_INVC_ID","FEE_DSCNT_ID","INVC_FEE_DSCNT_BILL_DISP_CD","INVC_FEE_DSCNT_SRC_CD",
    "GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PROD_ID","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BILL_ENTY_SK","CLS_SK","CLS_PLN_SK","FEE_DSCNT_SK","GRP_SK","SUBGRP_SK","INVC_TYP_CD","DSCNT_IN",
    "FEE_DSCNT_YR_MO_SK","INVC_BILL_DUE_DT_SK","INVC_BILL_DUE_YR_MO_SK","INVC_BILL_END_DT_SK",
    "INVC_BILL_END_YR_MO_SK","INVC_CRT_DT_SK","FEE_DSCNT_AMT","SUB_UNIQ_KEY","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INVC_FEE_DSCNT_SRC_CD_SK","INVC_FEE_DSCNT_BILL_DISP_CD_SK","INVC_FEE_DSCNT_SK","INVC_SK","INVC_TYP_CD_SK","SUB_SK","PROD_SK"
]
df_seq_W_FEE_DSCNT_INCM_csv_load_write = df_lnk_W_FEE_DSCNT_INCM_OutABC.select(*write_cols_WFEE)

rpad_cols_WFEE = {
    "DSCNT_IN":1,"CRT_RUN_CYC_EXCTN_DT_SK":10,"LAST_UPDT_RUN_CYC_EXCTN_DT_SK":10,
    "FEE_DSCNT_YR_MO_SK":6,"INVC_BILL_DUE_DT_SK":10,"INVC_BILL_DUE_YR_MO_SK":6,
    "INVC_BILL_END_DT_SK":10,"INVC_BILL_END_YR_MO_SK":6,"INVC_CRT_DT_SK":10
}
for c, l in rpad_cols_WFEE.items():
    df_seq_W_FEE_DSCNT_INCM_csv_load_write = df_seq_W_FEE_DSCNT_INCM_csv_load_write.withColumn(c, F.rpad(F.col(c), l, " "))

write_files(
    df_seq_W_FEE_DSCNT_INCM_csv_load_write,
    f"{adls_path}/load/W_FEE_DSCNT_INCM.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# --------------------------------------------------------------------------------
# Copy_127 (PxCopy => 2 output pins)
# --------------------------------------------------------------------------------
df_Copy_127_in = df_lnk_xfrm_BusinessLogic3_Data_out

df_lnk_IdsEdwRcvdIncmFExtr_OutABC = df_Copy_127_in.select(
    F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    F.col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    F.col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD").alias("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("INVC_FEE_DSCNT_SRC_CD").alias("INVC_FEE_DSCNT_SRC_CD"),
    F.col("GRP_ID").alias("GRP_ID"),
    F.col("SUBGRP_ID").alias("SUBGRP_ID"),
    F.col("CLS_ID").alias("CLS_ID"),
    F.col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    F.col("PROD_ID").alias("PROD_ID"),
    F.col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT")
)

df_lnk_IdsEdwErnIncmFExtrr_OutABC = df_Copy_127_in.select(
    F.col("SRC_SYS_CD"),
    F.col("BILL_INVC_ID"),
    F.col("FEE_DSCNT_ID"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD"),
    F.col("INVC_FEE_DSCNT_SRC_CD"),
    F.col("GRP_ID"),
    F.col("SUBGRP_ID"),
    F.col("CLS_ID"),
    F.col("CLS_PLN_ID"),
    F.col("PROD_ID"),
    F.col("CRT_RUN_CYC_EXCTN_DT_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_DT_SK"),
    F.col("BILL_ENTY_SK"),
    F.col("CLS_SK"),
    F.col("CLS_PLN_SK"),
    F.col("FEE_DSCNT_SK"),
    F.col("GRP_SK"),
    F.col("SUBGRP_SK"),
    F.col("INVC_TYP_CD"),
    F.col("DSCNT_IN"),
    F.col("INVC_CUR_RCRD_IN"),
    F.col("FEE_DSCNT_YR_MO_SK"),
    F.col("INVC_BILL_DUE_DT_SK"),
    F.col("INVC_BILL_DUE_YR_MO_SK"),
    F.col("INVC_BILL_END_DT_SK"),
    F.col("INVC_BILL_END_YR_MO_SK"),
    F.col("INVC_CRT_DT_SK"),
    F.col("FEE_DSCNT_AMT"),
    F.col("SUB_UNIQ_KEY"),
    F.col("CRT_RUN_CYC_EXCTN_SK"),
    F.col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    F.col("INVC_FEE_DSCNT_SRC_CD_SK"),
    F.col("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    F.col("INVC_FEE_DSCNT_SK"),
    F.col("INVC_SK"),
    F.col("INVC_TYP_CD_SK"),
    F.col("SUB_SK"),
    F.col("PROD_SK")
)

# --------------------------------------------------------------------------------
# seq_B_FEE_DSCNT_INCM_F_csv_load (PxSequentialFile => write)
# --------------------------------------------------------------------------------
df_seq_B_FEE_DSCNT_INCM_F_csv_load_write = df_lnk_IdsEdwRcvdIncmFExtr_OutABC.select(
    "SRC_SYS_CD","BILL_INVC_ID","FEE_DSCNT_ID","INVC_FEE_DSCNT_BILL_DISP_CD","INVC_FEE_DSCNT_SRC_CD","GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PROD_ID",
    "FEE_DSCNT_AMT"
)
write_files(
    df_seq_B_FEE_DSCNT_INCM_F_csv_load_write,
    f"{adls_path}/load/B_FEE_DSCNT_INCM_F.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="^",
    nullValue=None
)

# --------------------------------------------------------------------------------
# ds_FEE_DSCNT_INCM_F_out (PxDataSet => write parquet)
# --------------------------------------------------------------------------------
ds_FEE_cols = [
    "SRC_SYS_CD","BILL_INVC_ID","FEE_DSCNT_ID","INVC_FEE_DSCNT_BILL_DISP_CD","INVC_FEE_DSCNT_SRC_CD",
    "GRP_ID","SUBGRP_ID","CLS_ID","CLS_PLN_ID","PROD_ID","CRT_RUN_CYC_EXCTN_DT_SK","LAST_UPDT_RUN_CYC_EXCTN_DT_SK",
    "BILL_ENTY_SK","CLS_SK","CLS_PLN_SK","FEE_DSCNT_SK","GRP_SK","SUBGRP_SK","INVC_TYP_CD","DSCNT_IN",
    "INVC_CUR_RCRD_IN","FEE_DSCNT_YR_MO_SK","INVC_BILL_DUE_DT_SK","INVC_BILL_DUE_YR_MO_SK","INVC_BILL_END_DT_SK",
    "INVC_BILL_END_YR_MO_SK","INVC_CRT_DT_SK","FEE_DSCNT_AMT","SUB_UNIQ_KEY","CRT_RUN_CYC_EXCTN_SK","LAST_UPDT_RUN_CYC_EXCTN_SK",
    "INVC_FEE_DSCNT_SRC_CD_SK","INVC_FEE_DSCNT_BILL_DISP_CD_SK","INVC_FEE_DSCNT_SK","INVC_SK","INVC_TYP_CD_SK","SUB_SK","PROD_SK"
]
df_ds_FEE_DSCNT_INCM_F_out_write = df_lnk_IdsEdwErnIncmFExtrr_OutABC.select(ds_FEE_cols)

rpad_cols_dsFEE = {
    "CRT_RUN_CYC_EXCTN_DT_SK":10,"LAST_UPDT_RUN_CYC_EXCTN_DT_SK":10,
    "DSCNT_IN":1,"INVC_CUR_RCRD_IN":1,"FEE_DSCNT_YR_MO_SK":6,
    "INVC_BILL_DUE_DT_SK":10,"INVC_BILL_DUE_YR_MO_SK":6,
    "INVC_BILL_END_DT_SK":10,"INVC_BILL_END_YR_MO_SK":6,
    "INVC_CRT_DT_SK":10
}
for c, l in rpad_cols_dsFEE.items():
    df_ds_FEE_DSCNT_INCM_F_out_write = df_ds_FEE_DSCNT_INCM_F_out_write.withColumn(c, F.rpad(F.col(c), l, " "))

write_files(
    df_ds_FEE_DSCNT_INCM_F_out_write,
    f"{adls_path}/ds/FEE_DSCNT_INCM_F.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=False,
    quote='"',
    nullValue=None
)