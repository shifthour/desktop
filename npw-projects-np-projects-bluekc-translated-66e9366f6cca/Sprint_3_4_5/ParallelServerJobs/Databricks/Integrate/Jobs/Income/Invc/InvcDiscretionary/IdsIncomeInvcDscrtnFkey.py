# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC * VC LOGS *
# MAGIC ^1_3 01/14/09 10:18:26 Batch  14990_37109 PROMOTE bckcetl ids20 dsadm bls for sg
# MAGIC ^1_3 01/14/09 10:10:44 Batch  14990_36646 INIT bckcett testIDS dsadm BLS FOR SG
# MAGIC ^1_1 12/16/08 22:11:05 Batch  14961_79874 PROMOTE bckcett testIDS u03651 steph - income primary key conversion
# MAGIC ^1_1 12/16/08 22:03:32 Batch  14961_79415 INIT bckcett devlIDS u03651 steffy
# MAGIC ^1_2 11/12/07 10:00:11 Batch  14561_36015 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_2 11/02/07 13:07:35 Batch  14551_47275 PROMOTE bckcetl ids20 dsadm bls for on
# MAGIC ^1_2 11/02/07 12:52:45 Batch  14551_46368 INIT bckcett testIDS30 dsadm bls for on
# MAGIC ^1_1 10/31/07 13:17:35 Batch  14549_47857 PROMOTE bckcett testIDS30 u03651 steffy
# MAGIC ^1_1 10/31/07 13:09:37 Batch  14549_47387 INIT bckcett devlIDS30 u03651 steffy
# MAGIC ^1_1 06/07/07 15:08:56 Batch  14403_54540 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 06/07/07 15:06:33 Batch  14403_54395 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 04/09/07 08:24:30 Batch  14344_30274 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_1 03/28/07 06:42:38 Batch  14332_24162 INIT bckcetl ids20 dsadm dsadm
# MAGIC ^1_2 03/05/07 16:34:28 Batch  14309_59670 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_1 02/11/07 12:33:13 Batch  14287_45195 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_4 12/18/06 12:24:28 Batch  14232_44709 INIT bckcetl ids20 dsadm Income Backup for 12/18/2006 install
# MAGIC ^1_1 11/03/06 13:03:29 Batch  14187_47011 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 05/22/06 11:41:30 Batch  14022_42099 PROMOTE bckcetl ids20 dsadm Jim Hart
# MAGIC ^1_3 05/22/06 11:21:03 Batch  14022_40870 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_2 05/22/06 11:13:00 Batch  14022_40388 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_1 05/22/06 10:32:59 Batch  14022_37985 INIT bckcett testIDS30 dsadm Jim Hart
# MAGIC ^1_5 05/12/06 15:17:06 Batch  14012_55029 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_5 05/12/06 15:03:19 Batch  14012_54207 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_4 05/11/06 16:20:33 Batch  14011_58841 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_3 05/10/06 16:34:22 Batch  14010_59666 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_2 05/10/06 16:00:21 Batch  14010_57626 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_1 05/02/06 14:21:23 Batch  14002_51686 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_8 05/01/06 11:14:49 Batch  14001_45485 PROMOTE bckcett devlIDS30 u10157 sa
# MAGIC ^1_8 05/01/06 11:11:02 Batch  14001_40263 INIT bckcetl ids20 dcg01 sa
# MAGIC ^1_3 02/17/06 15:08:39 Batch  13928_54530 PROMOTE bckcetl ids20 dsadm J.Mahaffey
# MAGIC ^1_3 02/17/06 15:06:27 Batch  13928_54398 INIT bckcett testIDS30 dsadm J.Mahaffey
# MAGIC ^1_4 02/17/06 15:00:01 Batch  13928_54002 INIT bckcett testIDS30 u10157 sa
# MAGIC ^1_3 02/17/06 14:56:58 Batch  13928_53825 INIT bckcett testIDS30 dsadm J. Mahaffey
# MAGIC ^1_7 02/16/06 16:01:26 Batch  13927_57688 PROMOTE bckcett testIDS30 u10157 sa
# MAGIC ^1_7 02/16/06 15:58:47 Batch  13927_57529 INIT bckcett devlIDS30 u10157 sa
# MAGIC ^1_6 01/24/06 14:04:00 Batch  13904_50645 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_5 01/23/06 11:38:25 Batch  13903_41909 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_4 01/20/06 13:22:04 Batch  13900_48128 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_3 01/11/06 10:57:33 Batch  13891_39458 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 01/11/06 10:53:41 Batch  13891_39225 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_1 01/11/06 10:50:42 Batch  13891_39083 INIT bckcett devlIDS30 u06640 Ralph
# MAGIC ^1_2 12/23/05 11:12:01 Batch  13872_40337 INIT bckcett devlIDS30 u10913 Ollie Move Income/Commisions to test
# MAGIC ^1_1 12/23/05 10:39:46 Batch  13872_38400 INIT bckcett devlIDS30 u10913 Ollie Moving Income / Commission to Test
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsInvcDscrtnFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   File created by IdsInvcDscrtnExtr
# MAGIC 	
# MAGIC   
# MAGIC HASH FILES:     hf_recycle
# MAGIC 
# MAGIC 
# MAGIC TRANSFORMS:  Fkey lookups
# MAGIC                            
# MAGIC 
# MAGIC PROCESSING:   Finds foreign keys for surrogate key fields
# MAGIC 
# MAGIC OUTPUTS: 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Ralph Tucker   11/10/2005  -   Originally Programmed
# MAGIC        
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #          Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -----------------------------------------------------------------------                                   ----------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari     2008-09-26                   Added SrcSysCdSk parameter                                                          3567                 devlIDS                              Steph Goddard           10/03/2008
# MAGIC 
# MAGIC Kimberly Doty         08-25-2010                Added 3 new fields at end - DPNDT_CT, SUB_CT and 
# MAGIC                                                                 SELF_BILL_LIFE_IN                                                                         TTR 551           RebuildIntNewDevl            Steph Goddard            10/01/2010
# MAGIC 
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24                Changed Datatype length for field                                                    258186                IntegrateDev1                  Reddy Sanam               04/01/2021
# MAGIC                                                                                               BLIV_ID
# MAGIC                                                                                             char(12) to Varchar(15)

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
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
    DecimalType,
    TimestampType
)
from pyspark.sql.functions import col, lit, rpad, monotonically_increasing_id
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


InFile = get_widget_value("InFile","INVC_DSCRTN.Tmp")
Logging = get_widget_value("Logging","Y")
OutFile = get_widget_value("OutFile","INVC_DSCRTN.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

schema_IdsInvcDscrtn = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("INVC_SK", IntegerType(), nullable=False),
    StructField("BILL_INVC_ID", StringType(), nullable=False),
    StructField("BLDI_SEQ_NO", IntegerType(), nullable=False),
    StructField("CLS_ID", StringType(), nullable=False),
    StructField("CLS_PLN_ID", StringType(), nullable=False),
    StructField("GRGR_ID", StringType(), nullable=False),
    StructField("FEE_DSCNT_ID", StringType(), nullable=False),
    StructField("PROD_ID", StringType(), nullable=False),
    StructField("PROD_BILL_CMPNT_ID", StringType(), nullable=False),
    StructField("SUBGRP_ID", StringType(), nullable=False),
    StructField("INVC_DSCRTN_BILL_DISP_CD", StringType(), nullable=False),
    StructField("INVC_DSCRTN_PRM_FEE_CD", StringType(), nullable=False),
    StructField("DUE_DT", StringType(), nullable=False),
    StructField("INVC_DSCRTN_BEG_DT_SK", StringType(), nullable=False),
    StructField("INVC_DSCRTN_END_DT_SK", StringType(), nullable=False),
    StructField("DPNDT_PRM_AMT", DecimalType(38,10), nullable=False),
    StructField("FEE_DSCNT_AMT", DecimalType(38,10), nullable=False),
    StructField("SUB_PRM_AMT", DecimalType(38,10), nullable=False),
    StructField("DSCRTN_MO_QTY", IntegerType(), nullable=False),
    StructField("DSCRTN_DESC", StringType(), nullable=False),
    StructField("DSCRTN_PRSN_ID_TX", StringType(), nullable=False),
    StructField("DSCRTN_SH_DESC", StringType(), nullable=False),
    StructField("SBSB_CK", IntegerType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("DPNDT_CT", IntegerType(), nullable=False),
    StructField("SUB_CT", IntegerType(), nullable=False),
    StructField("SELF_BILL_LIFE_IN", StringType(), nullable=False)
])

df_IdsInvcDscrtn = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .schema(schema_IdsInvcDscrtn)
    .load(f"{adls_path}/key/{InFile}")
)

df_foreignKeyVars = (
    df_IdsInvcDscrtn
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svClsId", GetFkeyCls(col("SRC_SYS_CD"), col("INVC_SK"), col("GRGR_ID"), col("CLS_ID"), lit(Logging)))
    .withColumn("svClsPln", GetFkeyClsPln(col("SRC_SYS_CD"), col("INVC_SK"), col("CLS_PLN_ID"), lit(Logging)))
    .withColumn("svGrp", GetFkeyGrp(col("SRC_SYS_CD"), col("INVC_SK"), col("GRGR_ID"), lit(Logging)))
    .withColumn("svFeeDscnt", GetFkeyFeeDscnt(col("SRC_SYS_CD"), col("INVC_SK"), col("FEE_DSCNT_ID"), lit(Logging)))
    .withColumn("svInvc", GetFkeyInvc(col("SRC_SYS_CD"), col("INVC_SK"), col("BILL_INVC_ID"), lit(Logging)))
    .withColumn("svProd", GetFkeyProd(col("SRC_SYS_CD"), col("INVC_SK"), col("PROD_ID"), lit(Logging)))
    .withColumn("svSubGrp", GetFkeySubgrp(col("SRC_SYS_CD"), col("INVC_SK"), col("GRGR_ID"), col("SUBGRP_ID"), lit(Logging)))
    .withColumn("svSub", GetFkeySub(col("SRC_SYS_CD"), col("INVC_SK"), col("SBSB_CK"), lit(Logging)))
    .withColumn("svInvcDscrtnBillDisp", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SK"), lit("BILLING DISPOSITION"), col("INVC_DSCRTN_BILL_DISP_CD"), lit(Logging)))
    .withColumn("svInvcDscrtnPrmFee", GetFkeyCodes(col("SRC_SYS_CD"), col("INVC_SK"), lit("INVOICE DISCRETIONARY PREMIUM FEE"), col("INVC_DSCRTN_PRM_FEE_CD"), lit(Logging)))
    .withColumn("svDueDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("DUE_DT"), lit(Logging)))
    .withColumn("svBegDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("INVC_DSCRTN_BEG_DT_SK"), lit(Logging)))
    .withColumn("svEndDt", GetFkeyDate(lit("IDS"), col("INVC_SK"), col("INVC_DSCRTN_END_DT_SK"), lit(Logging)))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("INVC_SK")))
    .withColumn("SrcSysCdSk", lit(SrcSysCdSk))
)

df_ForeignKeyFkey = (
    df_foreignKeyVars
    .filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
    .select(
        col("INVC_SK").alias("INVC_DSCRTN_SK"),
        col("SrcSysCdSk").alias("SRC_SYS_CD_SK"),
        col("BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("BLDI_SEQ_NO").alias("SEQ_NO"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("svClsId").alias("CLS_SK"),
        col("svClsPln").alias("CLS_PLN_SK"),
        col("svGrp").alias("GRP_SK"),
        col("svFeeDscnt").alias("FEE_DSCNT_SK"),
        col("svInvc").alias("INVC_SK"),
        col("svProd").alias("PROD_SK"),
        col("svSubGrp").alias("SUBGRP_SK"),
        col("svSub").alias("SUB_SK"),
        col("svInvcDscrtnBillDisp").alias("INVC_DSCRTN_BILL_DISP_CD_SK"),
        col("svInvcDscrtnPrmFee").alias("INVC_DSCRTN_PRM_FEE_CD_SK"),
        col("svDueDt").alias("DUE_DT_SK"),
        col("svBegDt").alias("INVC_DSCRTN_BEG_DT_SK"),
        col("svEndDt").alias("INVC_DSCRTN_END_DT_SK"),
        col("DPNDT_PRM_AMT").alias("DPNDT_PRM_AMT"),
        col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        col("SUB_PRM_AMT").alias("SUB_PRM_AMT"),
        col("DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY"),
        col("DSCRTN_DESC").alias("DSCRTN_DESC"),
        col("DSCRTN_PRSN_ID_TX").alias("DSCRTN_PRSN_ID_TX"),
        col("DSCRTN_SH_DESC").alias("DSCRTN_SH_DESC"),
        col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        col("DPNDT_CT").alias("DPNDT_CT"),
        col("SUB_CT").alias("SUB_CT"),
        col("SELF_BILL_LIFE_IN").alias("SELF_BILL_LIFE_IN")
    )
)

df_ForeignKeyDefaultUNK = (
    df_foreignKeyVars
    .limit(1)
    .select(
        lit(0).alias("INVC_DSCRTN_SK"),
        lit(0).alias("SRC_SYS_CD_SK"),
        lit("UNK").alias("BILL_INVC_ID"),
        lit(0).alias("SEQ_NO"),
        lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(0).alias("CLS_SK"),
        lit(0).alias("CLS_PLN_SK"),
        lit(0).alias("GRP_SK"),
        lit(0).alias("FEE_DSCNT_SK"),
        lit(0).alias("INVC_SK"),
        lit(0).alias("PROD_SK"),
        lit(0).alias("SUBGRP_SK"),
        lit(0).alias("SUB_SK"),
        lit(0).alias("INVC_DSCRTN_BILL_DISP_CD_SK"),
        lit(0).alias("INVC_DSCRTN_PRM_FEE_CD_SK"),
        lit("UNK").alias("DUE_DT_SK"),
        lit("UNK").alias("INVC_DSCRTN_BEG_DT_SK"),
        lit("UNK").alias("INVC_DSCRTN_END_DT_SK"),
        lit(0).alias("DPNDT_PRM_AMT"),
        lit(0).alias("FEE_DSCNT_AMT"),
        lit(0).alias("SUB_PRM_AMT"),
        lit(0).alias("DSCRTN_MO_QTY"),
        lit("UNK").alias("DSCRTN_DESC"),
        lit("UNK").alias("DSCRTN_PRSN_ID_TX"),
        lit("UNK").alias("DSCRTN_SH_DESC"),
        lit("UNK").alias("PROD_BILL_CMPNT_ID"),
        lit(0).alias("DPNDT_CT"),
        lit(0).alias("SUB_CT"),
        lit("U").alias("SELF_BILL_LIFE_IN")
    )
)

df_ForeignKeyDefaultNA = (
    df_foreignKeyVars
    .limit(1)
    .select(
        lit(1).alias("INVC_DSCRTN_SK"),
        lit(1).alias("SRC_SYS_CD_SK"),
        lit("NA").alias("BILL_INVC_ID"),
        lit(0).alias("SEQ_NO"),
        lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        lit(1).alias("CLS_SK"),
        lit(1).alias("CLS_PLN_SK"),
        lit(1).alias("GRP_SK"),
        lit(1).alias("FEE_DSCNT_SK"),
        lit(1).alias("INVC_SK"),
        lit(1).alias("PROD_SK"),
        lit(1).alias("SUBGRP_SK"),
        lit(1).alias("SUB_SK"),
        lit(1).alias("INVC_DSCRTN_BILL_DISP_CD_SK"),
        lit(1).alias("INVC_DSCRTN_PRM_FEE_CD_SK"),
        lit("NA").alias("DUE_DT_SK"),
        lit("NA").alias("INVC_DSCRTN_BEG_DT_SK"),
        lit("NA").alias("INVC_DSCRTN_END_DT_SK"),
        lit(0).alias("DPNDT_PRM_AMT"),
        lit(0).alias("FEE_DSCNT_AMT"),
        lit(0).alias("SUB_PRM_AMT"),
        lit(0).alias("DSCRTN_MO_QTY"),
        lit("NA").alias("DSCRTN_DESC"),
        lit("NA").alias("DSCRTN_PRSN_ID_TX"),
        lit("NA").alias("DSCRTN_SH_DESC"),
        lit("NA").alias("PROD_BILL_CMPNT_ID"),
        lit(0).alias("DPNDT_CT"),
        lit(0).alias("SUB_CT"),
        lit("X").alias("SELF_BILL_LIFE_IN")
    )
)

df_ForeignKeyRecycle = (
    df_foreignKeyVars
    .filter(col("ErrCount") > 0)
    .select(
        GetRecycleKey(col("INVC_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
        col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
        col("DISCARD_IN").alias("DISCARD_IN"),
        col("PASS_THRU_IN").alias("PASS_THRU_IN"),
        col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        col("ErrCount").alias("ERR_CT"),
        col("RECYCLE_CT").alias("RECYCLE_CT"),
        col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        col("INVC_SK").alias("INVC_SK"),
        col("BILL_INVC_ID").alias("BILL_INVC_ID"),
        col("BLDI_SEQ_NO").alias("BLDI_SEQ_NO"),
        col("CLS_ID").alias("CLS_ID"),
        col("CLS_PLN_ID").alias("CLS_PLN_ID"),
        col("GRGR_ID").alias("GRGR_ID"),
        col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
        col("PROD_ID").alias("PROD_ID"),
        col("SUBGRP_ID").alias("SUBGRP_ID"),
        col("INVC_DSCRTN_BILL_DISP_CD").alias("INVC_DSCRTN_BILL_DISP_CD"),
        col("INVC_DSCRTN_PRM_FEE_CD").alias("INVC_DSCRTN_PRM_FEE_CD"),
        col("DUE_DT").alias("DUE_DT"),
        col("INVC_DSCRTN_BEG_DT_SK").alias("INVC_DSCRTN_BEG_DT_SK"),
        col("INVC_DSCRTN_END_DT_SK").alias("INVC_DSCRTN_END_DT_SK"),
        col("DPNDT_PRM_AMT").alias("DPNDT_PRM_AMT"),
        col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
        col("SUB_PRM_AMT").alias("SUB_PRM_AMT"),
        col("DSCRTN_MO_QTY").alias("DSCRTN_MO_QTY"),
        col("DSCRTN_DESC").alias("DSCRTN_DESC"),
        col("DSCRTN_PRSN_ID_TX").alias("DSCRTN_PRSN_ID_TX"),
        col("DSCRTN_SH_DESC").alias("DSCRTN_SH_DESC"),
        col("SBSB_CK").alias("SBSB_CK"),
        col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        col("PROD_BILL_CMPNT_ID").alias("PROD_BILL_CMPNT_ID"),
        col("DPNDT_CT").alias("DPNDT_CT"),
        col("SUB_CT").alias("SUB_CT"),
        col("SELF_BILL_LIFE_IN").alias("SELF_BILL_LIFE_IN")
    )
)

df_ForeignKeyRecycle_char_padded = (
    df_ForeignKeyRecycle
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("CLS_ID", rpad(col("CLS_ID"), 4, " "))
    .withColumn("CLS_PLN_ID", rpad(col("CLS_PLN_ID"), 8, " "))
    .withColumn("GRGR_ID", rpad(col("GRGR_ID"), 8, " "))
    .withColumn("FEE_DSCNT_ID", rpad(col("FEE_DSCNT_ID"), 2, " "))
    .withColumn("PROD_ID", rpad(col("PROD_ID"), 8, " "))
    .withColumn("SUBGRP_ID", rpad(col("SUBGRP_ID"), 4, " "))
    .withColumn("INVC_DSCRTN_BILL_DISP_CD", rpad(col("INVC_DSCRTN_BILL_DISP_CD"), 1, " "))
    .withColumn("INVC_DSCRTN_PRM_FEE_CD", rpad(col("INVC_DSCRTN_PRM_FEE_CD"), 1, " "))
    .withColumn("DUE_DT", rpad(col("DUE_DT"), 10, " "))
    .withColumn("INVC_DSCRTN_BEG_DT_SK", rpad(col("INVC_DSCRTN_BEG_DT_SK"), 10, " "))
    .withColumn("INVC_DSCRTN_END_DT_SK", rpad(col("INVC_DSCRTN_END_DT_SK"), 10, " "))
    .withColumn("DSCRTN_DESC", rpad(col("DSCRTN_DESC"), 70, " "))
    .withColumn("DSCRTN_SH_DESC", rpad(col("DSCRTN_SH_DESC"), 80, " "))
    .withColumn("PROD_BILL_CMPNT_ID", rpad(col("PROD_BILL_CMPNT_ID"), 4, " "))
    .withColumn("SELF_BILL_LIFE_IN", rpad(col("SELF_BILL_LIFE_IN"), 1, " "))
)

write_files(
    df_ForeignKeyRecycle_char_padded,
    "hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Fkey2 = df_ForeignKeyFkey.select(
    col("INVC_DSCRTN_SK"),
    col("SRC_SYS_CD_SK"),
    col("BILL_INVC_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("GRP_SK"),
    col("FEE_DSCNT_SK"),
    col("INVC_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("DUE_DT_SK"),
    col("INVC_DSCRTN_BEG_DT_SK"),
    col("INVC_DSCRTN_END_DT_SK"),
    col("DPNDT_PRM_AMT"),
    col("FEE_DSCNT_AMT"),
    col("SUB_PRM_AMT"),
    col("DSCRTN_MO_QTY"),
    col("DSCRTN_DESC"),
    col("DSCRTN_PRSN_ID_TX"),
    col("DSCRTN_SH_DESC"),
    col("PROD_BILL_CMPNT_ID"),
    col("DPNDT_CT"),
    col("SUB_CT"),
    col("SELF_BILL_LIFE_IN")
)

df_DefaultUNK2 = df_ForeignKeyDefaultUNK.select(
    col("INVC_DSCRTN_SK"),
    col("SRC_SYS_CD_SK"),
    col("BILL_INVC_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("GRP_SK"),
    col("FEE_DSCNT_SK"),
    col("INVC_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("DUE_DT_SK"),
    col("INVC_DSCRTN_BEG_DT_SK"),
    col("INVC_DSCRTN_END_DT_SK"),
    col("DPNDT_PRM_AMT"),
    col("FEE_DSCNT_AMT"),
    col("SUB_PRM_AMT"),
    col("DSCRTN_MO_QTY"),
    col("DSCRTN_DESC"),
    col("DSCRTN_PRSN_ID_TX"),
    col("DSCRTN_SH_DESC"),
    col("PROD_BILL_CMPNT_ID"),
    col("DPNDT_CT"),
    col("SUB_CT"),
    col("SELF_BILL_LIFE_IN")
)

df_DefaultNA2 = df_ForeignKeyDefaultNA.select(
    col("INVC_DSCRTN_SK"),
    col("SRC_SYS_CD_SK"),
    col("BILL_INVC_ID"),
    col("SEQ_NO"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLS_SK"),
    col("CLS_PLN_SK"),
    col("GRP_SK"),
    col("FEE_DSCNT_SK"),
    col("INVC_SK"),
    col("PROD_SK"),
    col("SUBGRP_SK"),
    col("SUB_SK"),
    col("INVC_DSCRTN_BILL_DISP_CD_SK"),
    col("INVC_DSCRTN_PRM_FEE_CD_SK"),
    col("DUE_DT_SK"),
    col("INVC_DSCRTN_BEG_DT_SK"),
    col("INVC_DSCRTN_END_DT_SK"),
    col("DPNDT_PRM_AMT"),
    col("FEE_DSCNT_AMT"),
    col("SUB_PRM_AMT"),
    col("DSCRTN_MO_QTY"),
    col("DSCRTN_DESC"),
    col("DSCRTN_PRSN_ID_TX"),
    col("DSCRTN_SH_DESC"),
    col("PROD_BILL_CMPNT_ID"),
    col("DPNDT_CT"),
    col("SUB_CT"),
    col("SELF_BILL_LIFE_IN")
)

df_collector = df_Fkey2.unionByName(df_DefaultUNK2).unionByName(df_DefaultNA2)

df_collector_char_padded = (
    df_collector
    .withColumn("BILL_INVC_ID", rpad(col("BILL_INVC_ID"), 0, " "))
    .withColumn("DUE_DT_SK", rpad(col("DUE_DT_SK"), 10, " "))
    .withColumn("INVC_DSCRTN_BEG_DT_SK", rpad(col("INVC_DSCRTN_BEG_DT_SK"), 10, " "))
    .withColumn("INVC_DSCRTN_END_DT_SK", rpad(col("INVC_DSCRTN_END_DT_SK"), 10, " "))
    .withColumn("DSCRTN_DESC", rpad(col("DSCRTN_DESC"), 70, " "))
    .withColumn("DSCRTN_PRSN_ID_TX", rpad(col("DSCRTN_PRSN_ID_TX"), 0, " "))
    .withColumn("DSCRTN_SH_DESC", rpad(col("DSCRTN_SH_DESC"), 80, " "))
    .withColumn("PROD_BILL_CMPNT_ID", rpad(col("PROD_BILL_CMPNT_ID"), 4, " "))
    .withColumn("SELF_BILL_LIFE_IN", rpad(col("SELF_BILL_LIFE_IN"), 1, " "))
)

write_files(
    df_collector_char_padded.select(
        "INVC_DSCRTN_SK",
        "SRC_SYS_CD_SK",
        "BILL_INVC_ID",
        "SEQ_NO",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLS_SK",
        "CLS_PLN_SK",
        "GRP_SK",
        "FEE_DSCNT_SK",
        "INVC_SK",
        "PROD_SK",
        "SUBGRP_SK",
        "SUB_SK",
        "INVC_DSCRTN_BILL_DISP_CD_SK",
        "INVC_DSCRTN_PRM_FEE_CD_SK",
        "DUE_DT_SK",
        "INVC_DSCRTN_BEG_DT_SK",
        "INVC_DSCRTN_END_DT_SK",
        "DPNDT_PRM_AMT",
        "FEE_DSCNT_AMT",
        "SUB_PRM_AMT",
        "DSCRTN_MO_QTY",
        "DSCRTN_DESC",
        "DSCRTN_PRSN_ID_TX",
        "DSCRTN_SH_DESC",
        "PROD_BILL_CMPNT_ID",
        "DPNDT_CT",
        "SUB_CT",
        "SELF_BILL_LIFE_IN"
    ),
    f"{adls_path}/load/{OutFile}",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)