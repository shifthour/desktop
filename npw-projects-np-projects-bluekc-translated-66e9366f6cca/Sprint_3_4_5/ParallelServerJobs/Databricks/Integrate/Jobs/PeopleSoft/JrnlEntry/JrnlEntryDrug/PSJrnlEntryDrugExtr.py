# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:    PSJrnlEntryDrugTrns
# MAGIC 
# MAGIC DESCRIPTION:  
# MAGIC   Pulls data from a flat file (GL_DRUG_DTL_TEMP) to common directory
# MAGIC   Currently, no need to do extract since data is already in flat file 
# MAGIC 
# MAGIC *******NOTE:  Flat file is pulled from /landing/ directory, not /verified/.  
# MAGIC 
# MAGIC     
# MAGIC INPUTS:
# MAGIC 	GL_DRUG_DTL_TEMP
# MAGIC 
# MAGIC 
# MAGIC HASH FILES: none
# MAGIC 
# MAGIC TRANSFORMS:  
# MAGIC 
# MAGIC                   FORMAT.DATE
# MAGIC                            
# MAGIC PROCESSING:
# MAGIC                   Create file in common directory for creating jrnl_entry_trans and jrnl_entry_prcs files in IDS
# MAGIC                   Both files are fed through transform, primary, and foreign key programs together - the files are split out in the foreign key program.
# MAGIC                   The primary key for both tables is the same.
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC                     Sequential file name is created in the job control ( FinalOutFile Parameter )
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC              Steph Goddard    10/2004-   Originally Programmed
# MAGIC              Steph Goddard    11/11/2004   Moved PRODUCT field to FNCL_LOB_SK instead of PDBL_ACCT_CAT.  PRODUCT is not product as we know it, but the financial line of business.  It is populated differently in PRODUCT than it is in PDBL_ACCT_CAT.
# MAGIC              Steph Goddard    07/18/2005   Changes for sequencer
# MAGIC 
# MAGIC 
# MAGIC Developer                          Date                 Project/Altiris #                Change Description                                         Development Project    Code Reviewer            Date Reviewed
# MAGIC ----------------------------------      -------------------    -----------------------------------    ---------------------------------------------------------                    ----------------------------------   ---------------------------------    -------------------------   
# MAGIC Parik                                 5/9/2007          3264                              Added Balancing process to the overall                 devlIDS30                 Steph Goddard             09/15/2007
# MAGIC                                                                                                       job that takes a snapshot of the source data   
# MAGIC 
# MAGIC Parik                               2008-08-19       3567(Primary Key)          Added primary Key process to the job                       devlIDS                     Steph Goddard            08/22/2008
# MAGIC Steph Goddard               02/27/2009      prod supp                      changed format date call                                           devl
# MAGIC 
# MAGIC 
# MAGIC Sudheer Champati           2013-07-25           4701                         Changed date format in svACCTGDT in                   IDSDevl                    Bhoomi Dasari             9/5/2013         
# MAGIC                                                                                                      BusinessRules and Transform
# MAGIC 	                                                                                     transformer to get required format for IDS tables
# MAGIC                                                                                                      Replacing Sequential file stage with Sybase stage
# MAGIC                                                                                                      to extract GL transactions from SYBASE.
# MAGIC                                                                                                      Adding  parameters to use in Sybase stage
# MAGIC 
# MAGIC Manasa Andru                  2017-06-26            TFS - 19354               Updated the performance parameters with         IntegrateDev1              Jag Yelavarthi              2017-06-30
# MAGIC                                                                                                the right values to avoid the job abend due to mutex error.     
# MAGIC 
# MAGIC Sudheer                            2017-09-11             5599                      Modiifed the Source Sql to pull the workday data  IntegrateDev2             Jag Yelavarthi              2017-09-12
# MAGIC 
# MAGIC Tim Sieg		        2018-02-01	       5599		    Removed the packet size on the Database	   IntegrateDev1

# MAGIC Pulling PeopleSoft data from Tables
# MAGIC Hash file (hf_jrnl_entry_trans_allcol) cleared in calling program
# MAGIC This container is used in:
# MAGIC PSJrnlEntryCapExtr
# MAGIC PSJrnlEntryClmExtr
# MAGIC PSJrnlEntryComsnExtr
# MAGIC PSJrnlEntryDrugExtr
# MAGIC PSJrnlEntryIncmExtr
# MAGIC 
# MAGIC These programs need to be re-compiled when logic changes
# MAGIC Apply business logic
# MAGIC Writing Sequential File to /key
# MAGIC Balancing snapshot of source file
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# MAGIC %run ../../../../../shared_containers/PrimaryKey/JrnlEntryTransPK
# COMMAND ----------

# Parameter retrieval
BCBSFINOwner = get_widget_value('BCBSFINOwner','')
bcbsfin_secret_name = get_widget_value('bcbsfin_secret_name','')
IDSOwner = get_widget_value('IDSOwner','')
ids_secret_name = get_widget_value('ids_secret_name','')
TmpOutFile = get_widget_value('TmpOutFile','')
CurrRunCycle = get_widget_value('CurrRunCycle','')
CurrDate = get_widget_value('CurrDate','')
SrcSysCd = get_widget_value('SrcSysCd','')
SrcSysCdSk = get_widget_value('SrcSysCdSk','')
InFile = get_widget_value('InFile','')  # From container parameters

# ------------------------------------------------------------------------------
# GL_DRUG_DTL_BCP_Table (SYBASEOC) - Reading from BCBS database
# ------------------------------------------------------------------------------
jdbc_url, jdbc_props = get_db_config(bcbsfin_secret_name)
df_GL_DRUG_DTL_BCP_Table = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", f"{BCBSFINOwner}.GL_DRUG_DTL_BCP")
    .load()
)

# ------------------------------------------------------------------------------
# BusinessRules (CTransformerStage)
# ------------------------------------------------------------------------------
df_BusinessRulesBase = df_GL_DRUG_DTL_BCP_Table.withColumn(
    "SnapActDt",
    F.date_format(F.col("SNAP_ACT_DT"), "yyyy-MM-dd")
)

df_BusinessRules_AllCol = (
    df_BusinessRulesBase
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("SRC_TRANS_CK", F.col("GL_DRG_DTL_CK"))
    .withColumn("SRC_TRANS_TYP_CD", F.lit("DRUG"))
    .withColumn("ACCTG_DT_SK", F.col("SnapActDt"))
    .withColumn("JOB_EXCTN_RCRD_ERR_SK", F.lit(0))
    .withColumn("INSRT_UPDT_CD", F.lit("I"))
    .withColumn("DISCARD_IN", F.lit("N"))
    .withColumn("PASS_THRU_IN", F.lit("Y"))
    .withColumn("FIRST_RECYC_DT", F.lit(CurrDate))
    .withColumn("ERR_CT", F.lit(0))
    .withColumn("RECYCLE_CT", F.lit(0))
    .withColumn("SRC_SYS_CD", F.lit(SrcSysCd))
    .withColumn(
        "PRI_KEY_STRING",
        F.concat_ws(";", F.lit(SrcSysCd), F.col("GL_DRG_DTL_CK"), F.lit("DRUG"), F.col("SnapActDt"))
    )
    .withColumn("JRNL_ENTRY_TRANS_SK", F.lit(0))
    .withColumn("CRT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("LAST_UPDT_RUN_CYC_EXCTN_SK", F.lit(0))
    .withColumn("TRANS_TBL_SK", F.lit(0))
    .withColumn(
        "FNCL_LOB",
        F.rpad(
            F.when(
                F.length(trim(F.col("PRODUCT"))) == 0, F.lit("NA")
            ).otherwise(F.col("PRODUCT")),
            4, " "
        )
    )
    .withColumn(
        "JRNL_ENTRY_TRANS_DR_CR_CD",
        F.rpad(
            F.when(
                F.length(trim(F.col("DR_CR_CD"))) == 0, F.lit("NA")
            ).otherwise(F.col("DR_CR_CD")),
            2, " "
        )
    )
    .withColumn("DIST_GL_IN", F.rpad(F.col("MAP_IN"), 1, " "))
    .withColumn("JRNL_ENTRY_TRANS_AMT", F.col("TX_AMT"))
    .withColumn("TRANS_LN_NO", F.col("TRANSACTION_LINE"))
    .withColumn("ACCT_NO", F.rpad(F.col("WD_ACCT_NO"), 10, " "))
    .withColumn(
        "AFFILIAT_NO",
        F.rpad(
            F.when(
                F.col("WD_AFFIL_ID").isNull(), F.lit("     ")
            ).otherwise(F.col("WD_AFFIL_ID")),
            5, " "
        )
    )
    .withColumn("APPL_JRNL_ID", F.rpad(F.col("APPL_JRNL_ID"), 10, " "))
    .withColumn("BUS_UNIT_GL_NO", F.rpad(F.col("BUSINESS_UNIT_GL"), 5, " "))
    .withColumn("BUS_UNIT_NO", F.rpad(F.col("BUSINESS_UNIT"), 5, " "))
    .withColumn(
        "CC_ID",
        F.rpad(
            F.when(
                F.col("WD_CC_ID").isNull(), F.lit("          ")
            ).otherwise(F.col("WD_CC_ID")),
            10, " "
        )
    )
    .withColumn("JRNL_LN_DESC", F.rpad(F.col("LINE_DESCR"), 30, " "))
    .withColumn("JRNL_LN_REF_NO", F.rpad(F.col("JRNL_LN_REF"), 10, " "))
    .withColumn(
        "OPR_UNIT_NO",
        F.rpad(
            F.when(
                F.col("WD_DNR_ID").isNull(), F.lit("    ")
            ).otherwise(F.col("WD_DNR_ID")),
            8, " "
        )
    )
    .withColumn(
        "SUB_ACCT_NO",
        F.rpad(
            F.when(
                F.col("WD_CUST_ID").isNull(), F.lit("    ")
            ).otherwise(F.col("WD_CUST_ID")),
            10, " "
        )
    )
    .withColumn("TRANS_ID", F.rpad(F.col("TRANSACTION_ID"), 10, " "))
    .withColumn("PRCS_MAP_1_TX", F.rpad(F.col("MAP_FLD_1_CD"), 4, " "))
    .withColumn("PRCS_MAP_2_TX", F.rpad(F.col("MAP_FLD_2_CD"), 4, " "))
    .withColumn("PRCS_MAP_3_TX", F.rpad(F.col("MAP_FLD_3_CD"), 4, " "))
    .withColumn("PRCS_MAP_4_TX", F.rpad(F.col("MAP_FLD_4_CD"), 4, " "))
    .withColumn("PRCS_SUB_SRC_CD", F.rpad(F.col("FA_SUB_SRC_CD"), 5, " "))
    .withColumn("CLCL_ID", F.rpad(F.lit(""), 12, " "))
    .withColumn("WD_LOB_CD", F.col("WD_LOB_CD"))
    .withColumn("WD_BANK_ACCT_NM", F.col("WD_BANK_ACCT_NM"))
    .withColumn("WD_RVNU_CAT_ID", F.col("WD_RVNU_CAT_ID"))
    .withColumn("WD_SPEND_CAT_ID", F.col("WD_SPEND_CAT_ID"))
    .select(
        "SRC_SYS_CD_SK",
        "SRC_TRANS_CK",
        "SRC_TRANS_TYP_CD",
        "ACCTG_DT_SK",
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "JRNL_ENTRY_TRANS_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "TRANS_TBL_SK",
        "FNCL_LOB",
        "JRNL_ENTRY_TRANS_DR_CR_CD",
        "DIST_GL_IN",
        "JRNL_ENTRY_TRANS_AMT",
        "TRANS_LN_NO",
        "ACCT_NO",
        "AFFILIAT_NO",
        "APPL_JRNL_ID",
        "BUS_UNIT_GL_NO",
        "BUS_UNIT_NO",
        "CC_ID",
        "JRNL_LN_DESC",
        "JRNL_LN_REF_NO",
        "OPR_UNIT_NO",
        "SUB_ACCT_NO",
        "TRANS_ID",
        "PRCS_MAP_1_TX",
        "PRCS_MAP_2_TX",
        "PRCS_MAP_3_TX",
        "PRCS_MAP_4_TX",
        "PRCS_SUB_SRC_CD",
        "CLCL_ID",
        "WD_LOB_CD",
        "WD_BANK_ACCT_NM",
        "WD_RVNU_CAT_ID",
        "WD_SPEND_CAT_ID"
    )
)

df_BusinessRules_Transform = (
    df_BusinessRulesBase
    .withColumn("SRC_SYS_CD_SK", F.lit(SrcSysCdSk))
    .withColumn("SRC_TRANS_CK", F.col("GL_DRG_DTL_CK"))
    .withColumn("SRC_TRANS_TYP_CD", F.lit("DRUG"))
    .withColumn("ACCTG_DT_SK", F.col("SnapActDt"))
    .select(
        "SRC_SYS_CD_SK",
        "SRC_TRANS_CK",
        "SRC_TRANS_TYP_CD",
        "ACCTG_DT_SK"
    )
)

# ------------------------------------------------------------------------------
# JrnlEntryTransPK (CContainerStage)
# ------------------------------------------------------------------------------
params_JrnlEntryTransPK = {
    "$IDSOwner": IDSOwner,
    "TmpOutFile": TmpOutFile,
    "InFile": InFile,
    "CurrRunCycle": CurrRunCycle,
    "CurrDate": CurrDate,
    "SrcSysCd": SrcSysCd,
    "SrcSysCdSk": SrcSysCdSk
}
df_JrnlEntryTransPK = JrnlEntryTransPK(df_BusinessRules_Transform, df_BusinessRules_AllCol, params_JrnlEntryTransPK)

# ------------------------------------------------------------------------------
# IdsJrnlEntryDrugExtr (CSeqFileStage)
# ------------------------------------------------------------------------------
df_IdsJrnlEntryDrugExtr = df_JrnlEntryTransPK.select(
    "JOB_EXCTN_RCRD_ERR_SK",
    "INSRT_UPDT_CD",
    "DISCARD_IN",
    "PASS_THRU_IN",
    "FIRST_RECYC_DT",
    "ERR_CT",
    "RECYCLE_CT",
    "SRC_SYS_CD",
    "PRI_KEY_STRING",
    "JRNL_ENTRY_TRANS_SK",
    "SRC_TRANS_CK",
    "SRC_TRANS_TYP_CD",
    "ACCTG_DT_SK",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "TRANS_TBL_SK",
    "FNCL_LOB",
    "JRNL_ENTRY_TRANS_DR_CR_CD",
    "DIST_GL_IN",
    "JRNL_ENTRY_TRANS_AMT",
    "TRANS_LN_NO",
    "ACCT_NO",
    "AFFILIAT_NO",
    "APPL_JRNL_ID",
    "BUS_UNIT_GL_NO",
    "BUS_UNIT_NO",
    "CC_ID",
    "JRNL_LN_DESC",
    "JRNL_LN_REF_NO",
    "OPR_UNIT_NO",
    "SUB_ACCT_NO",
    "TRANS_ID",
    "PRCS_MAP_1_TX",
    "PRCS_MAP_2_TX",
    "PRCS_MAP_3_TX",
    "PRCS_MAP_4_TX",
    "PRCS_SUB_SRC_CD",
    "CLCL_ID",
    "WD_LOB_CD",
    "WD_BANK_ACCT_NM",
    "WD_RVNU_CAT_ID",
    "WD_SPEND_CAT_ID"
)

write_files(
    df_IdsJrnlEntryDrugExtr,
    f"{adls_path}/key/PSDrugJrnlEntryTransExtr.JrnlEntryTrans.dat.20080821",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)

# ------------------------------------------------------------------------------
# PSI_Source (SYBASEOC) - Reading from BCBS database
# ------------------------------------------------------------------------------
df_PSI_Source = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .options(**jdbc_props)
    .option("dbtable", f"{BCBSFINOwner}.PSI_Source")
    .load()
)

# ------------------------------------------------------------------------------
# Transform (CTransformerStage)
# ------------------------------------------------------------------------------
df_TransformBase = (
    df_PSI_Source
    .withColumn("svACCTDT", F.date_format(F.col("SNAP_ACT_DT"), "yyyy-MM-dd"))
    .withColumn("svAcctDtSk", GetFkeyDate(F.lit("IDS"), F.lit(100), F.col("svACCTDT"), F.lit("X")))
    .withColumn("svSrcTransTypCdSk", GetFkeyCodes(F.lit("FACETS"), F.lit(110), F.lit("SOURCE TRANSACTION TYPE CODE"), F.lit("DRUG"), F.lit("X")))
)

df_Transform_Output = df_TransformBase.select(
    F.lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    F.col("GL_DRG_DTL_CK").alias("SRC_TRANS_CK"),
    F.col("svSrcTransTypCdSk").alias("SRC_TRANS_TYP_CD_SK"),
    F.col("svAcctDtSk").alias("ACCTG_DT_SK")
)

# ------------------------------------------------------------------------------
# Snapshot_File (CSeqFileStage)
# ------------------------------------------------------------------------------
df_Snapshot_File = (
    df_Transform_Output
    .withColumn("ACCTG_DT_SK", F.rpad(F.col("ACCTG_DT_SK"), 10, " "))
    .select(
        "SRC_SYS_CD_SK",
        "SRC_TRANS_CK",
        "SRC_TRANS_TYP_CD_SK",
        "ACCTG_DT_SK"
    )
)

write_files(
    df_Snapshot_File,
    f"{adls_path}/load/B_JRNL_ENTRY_TRANS.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)