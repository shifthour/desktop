# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2005 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC 
# MAGIC JOB NAME:     IdsInvcFeeDscntFkey
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the foreign keys.   
# MAGIC       
# MAGIC 
# MAGIC INPUTS:   File created by IdsInvcFeeDscntExtr
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
# MAGIC             Ralph Tucker   12/15/2005  -   Originally Programmed
# MAGIC        
# MAGIC MODIFICATIONS:
# MAGIC Developer               Date                         Change Description                                                                           Project #                Development Project          Code Reviewer          Date Reviewed  
# MAGIC ------------------             ----------------------------     -------------------------------------------------------------------------------------------------         ---------------------         ------------------------------------       ----------------------------      ----------------
# MAGIC Bhoomi Dasari        2008-09-28                Added SrcSysCdSk parameter                                                          3567                      devlIDS                               Steph Goddard         10/03/2008
# MAGIC Shanmugam A.       2018-03-22                Added Logic for fields ClsSK, ClsPlnSK, SubSk and ProdSK            TFS20071             Integrate Dev2                    Jaideep Mankala       03/27/2018
# MAGIC 
# MAGIC Goutham Kalidindi  2021-03-24                                 Changed Datatype length for field                                     258186                   IntegrateDev1                   Reddy Sanam            04/01/2021
# MAGIC                                                                                              BLIV_ID
# MAGIC                                                                                                char(12) to Varchar(15)

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
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType
from pyspark.sql.functions import col, lit, row_number, when, rpad
from pyspark.sql.window import Window
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Retrieve job parameters
InFile = get_widget_value("InFile","INVC_FEE_DSCNT.Tmp")
Logging = get_widget_value("Logging","Y")
OutFile = get_widget_value("OutFile","INVC_FEE_DSCNT.dat")
SrcSysCdSk = get_widget_value("SrcSysCdSk","105838")

# -----------------------------------------------------------------------------
# Stage: IdsInvcFeeDscnt (CSeqFileStage)
# -----------------------------------------------------------------------------
schema_IdsInvcFeeDscnt = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), nullable=False),
    StructField("INSRT_UPDT_CD", StringType(), nullable=False),
    StructField("DISCARD_IN", StringType(), nullable=False),
    StructField("PASS_THRU_IN", StringType(), nullable=False),
    StructField("FIRST_RECYC_DT", TimestampType(), nullable=False),
    StructField("ERR_CT", IntegerType(), nullable=False),
    StructField("RECYCLE_CT", DecimalType(38,10), nullable=False),
    StructField("SRC_SYS_CD", StringType(), nullable=False),
    StructField("PRI_KEY_STRING", StringType(), nullable=False),
    StructField("FEE_DSCNT_SK", IntegerType(), nullable=False),
    StructField("FEE_DSCNT_ID", StringType(), nullable=False),
    StructField("PMFA_ID", StringType(), nullable=False),
    StructField("BLFD_SOURCE", StringType(), nullable=False),
    StructField("BLFD_DISP_CD", StringType(), nullable=False),
    StructField("PMFA_FEE_DISC_IND", StringType(), nullable=False),
    StructField("GRGR_ID", StringType(), nullable=False),
    StructField("SGSG_ID", StringType(), nullable=False),
    StructField("SBSB_ID", StringType(), nullable=False),
    StructField("INVC_SK", IntegerType(), nullable=False),
    StructField("BILL_INVC_ID", StringType(), nullable=False),
    StructField("GRP_ID", StringType(), nullable=False),
    StructField("INVC_ID", StringType(), nullable=False),
    StructField("SUBGRP_ID", StringType(), nullable=False),
    StructField("DSCNT_IN", StringType(), nullable=False),
    StructField("FEE_DSCNT_AMT", DecimalType(38,10), nullable=False),
    StructField("SBSB_CK", IntegerType(), nullable=False),
    StructField("CSCS_ID", StringType(), nullable=False),
    StructField("CLS_PLN_ID", StringType(), nullable=False),
    StructField("PROD_ID", StringType(), nullable=False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), nullable=False)
])

df_IdsInvcFeeDscnt = (
    spark.read.format("csv")
    .option("header", "false")
    .option("quote", "\"")
    .option("delimiter", ",")
    .schema(schema_IdsInvcFeeDscnt)
    .load(f"{adls_path}/key/{InFile}")
)

# -----------------------------------------------------------------------------
# Stage: ForeignKey (CTransformerStage)
# -----------------------------------------------------------------------------
windowSpec = Window.orderBy(lit(1))
df_IdsInvcFeeDscnt_enriched = (
    df_IdsInvcFeeDscnt
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("svFeeDscnt", GetFkeyFeeDscnt(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), col("PMFA_ID"), Logging))
    .withColumn("svBlfdSource", GetFkeyCodes(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), lit("INVOICE FEE DISCOUNT SOURCE"), col("BLFD_SOURCE"), Logging))
    .withColumn("svBlfdDispCd", GetFkeyCodes(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), lit("BILLING DISPOSITION"), col("BLFD_DISP_CD"), Logging))
    .withColumn("svGrp", GetFkeyGrp(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), col("GRP_ID"), Logging))
    .withColumn("svInvc", GetFkeyInvc(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), col("INVC_ID"), Logging))
    .withColumn("svSubGrp", GetFkeySubgrp(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), col("GRP_ID"), col("SGSG_ID"), Logging))
    .withColumn("svSub",
        when(
            col("SBSB_CK").isNull() | (trim(col("SBSB_CK")) == "") | (trim(col("SBSB_CK")) == "0"),
            lit(1)
        ).otherwise(GetFkeySub(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), col("SBSB_CK"), Logging))
    )
    .withColumn("svClsPln",
        when(
            col("SBSB_CK").isNull() | (trim(col("SBSB_CK")) == "") | (trim(col("SBSB_CK")) == "0"),
            lit(1)
        ).otherwise(GetFkeyClsPln(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), col("CLS_PLN_ID"), Logging))
    )
    .withColumn("svProd",
        when(
            col("SBSB_CK").isNull() | (trim(col("SBSB_CK")) == "") | (trim(col("SBSB_CK")) == "0"),
            lit(1)
        ).otherwise(GetFkeyProd(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), col("PROD_ID"), Logging))
    )
    .withColumn("svCls",
        when(
            col("SBSB_CK").isNull() | (trim(col("SBSB_CK")) == "") | (trim(col("SBSB_CK")) == "0"),
            lit(1)
        ).otherwise(GetFkeyCls(col("SRC_SYS_CD"), col("FEE_DSCNT_SK"), col("GRGR_ID"), col("CSCS_ID"), Logging))
    )
    .withColumn("ErrCount", GetFkeyErrorCnt(col("FEE_DSCNT_SK")))
    .withColumn("INROWNUM", row_number().over(windowSpec))
)

df_ForeignKey_Fkey = df_IdsInvcFeeDscnt_enriched.filter((col("ErrCount") == 0) | (col("PassThru") == "Y"))
df_ForeignKey_DefaultUNK = df_IdsInvcFeeDscnt_enriched.filter(col("INROWNUM") == 1)
df_ForeignKey_DefaultNA = df_IdsInvcFeeDscnt_enriched.filter(col("INROWNUM") == 1)
df_ForeignKey_Recycle = df_IdsInvcFeeDscnt_enriched.filter(col("ErrCount") > 0)

# -----------------------------------------------------------------------------
# Create the output links (columns) as specified
# -----------------------------------------------------------------------------
# Fkey link
df_ForeignKey_Fkey_selected = df_ForeignKey_Fkey.select(
    col("FEE_DSCNT_SK").alias("INVC_FEE_DSCNT_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    col("svBlfdSource").alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    col("svBlfdDispCd").alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("svCls").alias("CLS_SK"),
    col("svFeeDscnt").alias("FEE_DSCNT_SK"),
    col("svGrp").alias("GRP_SK"),
    col("svInvc").alias("INVC_SK"),
    col("svSubGrp").alias("SUBGRP_SK"),
    col("svSub").alias("SUB_SK"),
    col("DSCNT_IN").alias("DSCNT_IN"),
    col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    col("svClsPln").alias("CLS_PLN_SK"),
    col("svProd").alias("PROD_SK")
)

# DefaultUNK link
df_ForeignKey_DefaultUNK_selected = df_ForeignKey_DefaultUNK.select(
    lit(0).alias("INVC_FEE_DSCNT_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("BILL_INVC_ID"),
    lit("UNK").alias("FEE_DSCNT_ID"),
    lit(0).alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    lit(0).alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLS_SK"),
    lit(0).alias("FEE_DSCNT_SK"),
    lit(0).alias("GRP_SK"),
    lit(0).alias("INVC_SK"),
    lit(0).alias("SUBGRP_SK"),
    lit(0).alias("SUB_SK"),
    lit("X").alias("DSCNT_IN"),
    lit(0).alias("FEE_DSCNT_AMT"),
    lit(0).alias("CLS_PLN_SK"),
    lit(0).alias("PROD_SK")
)

# DefaultNA link
df_ForeignKey_DefaultNA_selected = df_ForeignKey_DefaultNA.select(
    lit(1).alias("INVC_FEE_DSCNT_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("BILL_INVC_ID"),
    lit("NA").alias("FEE_DSCNT_ID"),
    lit(1).alias("INVC_FEE_DSCNT_SRC_CD_SK"),
    lit(1).alias("INVC_FEE_DSCNT_BILL_DISP_CD_SK"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLS_SK"),
    lit(1).alias("FEE_DSCNT_SK"),
    lit(1).alias("GRP_SK"),
    lit(1).alias("INVC_SK"),
    lit(1).alias("SUBGRP_SK"),
    lit(1).alias("SUB_SK"),
    lit("X").alias("DSCNT_IN"),
    lit(0).alias("FEE_DSCNT_AMT"),
    lit(1).alias("CLS_PLN_SK"),
    lit(1).alias("PROD_SK")
)

# Recycle link
df_ForeignKey_Recycle_selected = df_ForeignKey_Recycle.select(
    GetRecycleKey(col("FEE_DSCNT_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("FEE_DSCNT_SK").alias("FEE_DSCNT_SK"),
    col("FEE_DSCNT_ID").alias("FEE_DSCNT_ID"),
    col("PMFA_ID").alias("PMFA_ID"),
    col("BLFD_SOURCE").alias("BLFD_SOURCE"),
    col("BLFD_DISP_CD").alias("BLFD_DISP_CD"),
    col("PMFA_FEE_DISC_IND").alias("PMFA_FEE_DISC_IND"),
    col("GRGR_ID").alias("GRGR_ID"),
    col("SGSG_ID").alias("SGSG_ID"),
    col("SBSB_ID").alias("SBSB_ID"),
    col("INVC_SK").alias("INVC_SK"),
    col("BILL_INVC_ID").alias("BILL_INVC_ID"),
    col("GRP_ID").alias("GRP_ID"),
    col("INVC_ID").alias("INVC_ID"),
    col("SUBGRP_ID").alias("SUBGRP_ID"),
    col("DSCNT_IN").alias("DSCNT_IN"),
    col("FEE_DSCNT_AMT").alias("FEE_DSCNT_AMT"),
    col("SBSB_CK").alias("SBSB_CK"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CSCS_ID").alias("CSCS_ID"),
    col("CLS_PLN_ID").alias("CLS_PLN_ID"),
    col("PROD_ID").alias("PROD_ID")
)

# -----------------------------------------------------------------------------
# Stage: hf_recycle (CHashedFileStage) -> Scenario C => write as parquet
# -----------------------------------------------------------------------------
df_ForeignKey_Recycle_out = (
    df_ForeignKey_Recycle_selected
    .withColumn("INSRT_UPDT_CD", rpad(col("INSRT_UPDT_CD"), 10, " "))
    .withColumn("DISCARD_IN", rpad(col("DISCARD_IN"), 1, " "))
    .withColumn("PASS_THRU_IN", rpad(col("PASS_THRU_IN"), 1, " "))
    .withColumn("SRC_SYS_CD", rpad(col("SRC_SYS_CD"), <...>, " "))
    .withColumn("PRI_KEY_STRING", rpad(col("PRI_KEY_STRING"), <...>, " "))
    .withColumn("FEE_DSCNT_ID", rpad(col("FEE_DSCNT_ID"), <...>, " "))
    .withColumn("PMFA_ID", rpad(col("PMFA_ID"), 2, " "))
    .withColumn("BLFD_SOURCE", rpad(col("BLFD_SOURCE"), 1, " "))
    .withColumn("BLFD_DISP_CD", rpad(col("BLFD_DISP_CD"), 1, " "))
    .withColumn("PMFA_FEE_DISC_IND", rpad(col("PMFA_FEE_DISC_IND"), 1, " "))
    .withColumn("GRGR_ID", rpad(col("GRGR_ID"), 8, " "))
    .withColumn("SGSG_ID", rpad(col("SGSG_ID"), 4, " "))
    .withColumn("SBSB_ID", rpad(col("SBSB_ID"), 9, " "))
    .withColumn("BILL_INVC_ID", rpad(col("BILL_INVC_ID"), <...>, " "))
    .withColumn("GRP_ID", rpad(col("GRP_ID"), 8, " "))
    .withColumn("INVC_ID", rpad(col("INVC_ID"), <...>, " "))
    .withColumn("SUBGRP_ID", rpad(col("SUBGRP_ID"), 4, " "))
    .withColumn("DSCNT_IN", rpad(col("DSCNT_IN"), 1, " "))
    .withColumn("CSCS_ID", rpad(col("CSCS_ID"), 4, " "))
    .withColumn("CLS_PLN_ID", rpad(col("CLS_PLN_ID"), 8, " "))
    .withColumn("PROD_ID", rpad(col("PROD_ID"), 8, " "))
)

write_files(
    df_ForeignKey_Recycle_out.select(
        "JOB_EXCTN_RCRD_ERR_SK",
        "INSRT_UPDT_CD",
        "DISCARD_IN",
        "PASS_THRU_IN",
        "FIRST_RECYC_DT",
        "ERR_CT",
        "RECYCLE_CT",
        "SRC_SYS_CD",
        "PRI_KEY_STRING",
        "FEE_DSCNT_SK",
        "FEE_DSCNT_ID",
        "PMFA_ID",
        "BLFD_SOURCE",
        "BLFD_DISP_CD",
        "PMFA_FEE_DISC_IND",
        "GRGR_ID",
        "SGSG_ID",
        "SBSB_ID",
        "INVC_SK",
        "BILL_INVC_ID",
        "GRP_ID",
        "INVC_ID",
        "SUBGRP_ID",
        "DSCNT_IN",
        "FEE_DSCNT_AMT",
        "SBSB_CK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CSCS_ID",
        "CLS_PLN_ID",
        "PROD_ID"
    ),
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# -----------------------------------------------------------------------------
# Stage: Collector (CCollector)
# Union the three outputs: Fkey, DefaultUNK, DefaultNA
# -----------------------------------------------------------------------------
df_Collector = (
    df_ForeignKey_Fkey_selected.unionByName(df_ForeignKey_DefaultUNK_selected)
    .unionByName(df_ForeignKey_DefaultNA_selected)
)

# -----------------------------------------------------------------------------
# Stage: InvcFeeDscnt (CSeqFileStage) -> write final file
# -----------------------------------------------------------------------------
df_Collector_out = (
    df_Collector
    .withColumn("BILL_INVC_ID", rpad(col("BILL_INVC_ID"), <...>, " "))
    .withColumn("FEE_DSCNT_ID", rpad(col("FEE_DSCNT_ID"), <...>, " "))
    .withColumn("DSCNT_IN", rpad(col("DSCNT_IN"), 1, " "))
)

write_files(
    df_Collector_out.select(
        "INVC_FEE_DSCNT_SK",
        "SRC_SYS_CD_SK",
        "BILL_INVC_ID",
        "FEE_DSCNT_ID",
        "INVC_FEE_DSCNT_SRC_CD_SK",
        "INVC_FEE_DSCNT_BILL_DISP_CD_SK",
        "CRT_RUN_CYC_EXCTN_SK",
        "LAST_UPDT_RUN_CYC_EXCTN_SK",
        "CLS_SK",
        "FEE_DSCNT_SK",
        "GRP_SK",
        "INVC_SK",
        "SUBGRP_SK",
        "SUB_SK",
        "DSCNT_IN",
        "FEE_DSCNT_AMT",
        "CLS_PLN_SK",
        "PROD_SK"
    ),
    f"{adls_path}/load/{OutFile}",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)