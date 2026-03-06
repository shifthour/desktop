# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-24
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC       
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC             Oliver Nielsen  04/2004  -   Originally Programmed
# MAGIC             Oliver Nielsen  06/2004  -   Change name of FCLTY_TYP_CD_SK to  FCLTY_CLM_BILL_TYP_CD_SK moved up 3 spaces in file format.
# MAGIC             SAndrew          08/07/2004-    IDS 2.0    Task tracker issues #1163, #1125, #1279.  Removed duplicate step.  updated documentation
# MAGIC            Brent Leland      09/08/2004   -  Fixed erronious addition of default rows for UNK and NA.
# MAGIC            R Tucker           12/30/2004   -  Added FCLTY_CLM_ADMS_SRC_CD_SK and removed Adms_Src.
# MAGIC             Brent Leland      01/19/2005  -  Changed parameter CLM_SK to FCLTY_CLM_SK for 3 foriegn key lookup routines
# MAGIC            BJ Luce             10/20/2005      set logging to X for submitted drg foreign key lookup
# MAGIC            Steph Goddard   02/13/2006     Changes for sequencer implementation
# MAGIC            Steph Goddard   06/02/2006    reset claim fkey lookup - different from history/development!!
# MAGIC 
# MAGIC 
# MAGIC Developer                Date                Project/Altiris #           Change Description                                                                                               Development Project      Code Reviewer          Date Reviewed       
# MAGIC ------------------            -----------------   ---------------------------   ------------------------------------------------------------------------------------------------------   --------------------------------   -------------------------------   ----------------------------       
# MAGIC SAndrew                  2008-02-10    DRG Phase II            Changed the signature of the GetFkeyDRG  for stage variable                     devlIDS                             Steph Goddard          02/21/2008
# MAGIC                                                                                                 svNrmtvDrgSK (Normative DRG) to always use DRG Method Code of MS.                         
# MAGIC Bhoomi Dasari        2008-07-28     3567(Primary Key)    Added new parameter SrcSysCdSk and passing it directly from source        devlIDS                            Steph Goddard          07/29/2008
# MAGIC Kalyan Neelam        2012-07-19     4784                           Added BCBSSC source                                                                                         IntegrateWrhsDevl       
# MAGIC Kalyan Neelam        2012-10-17     4784                          Updated the rule for stage variable FcltyClmAdmTypCdSk to                          IntegrateWrhsDevl         Bhoomi Dasari         10/20/2012
# MAGIC                                                                                                use domain FACILITY ADMISSION TYPE for FACETS
# MAGIC Sudhir Bomshetty   2017-11-03     5781                          Added check for SRC_SYS_CD = 'BCA' for FcltyClmSubTypCdSk,              IntegrateDev2                 Jag Yelavarthi            2017-11-10
# MAGIC                                                                                                FcltyClmDschgSttusCdSk, FcltyClmBillTypCdSk, FcltyClmBillFreqCdSk, 
# MAGIC                                                                                                FcltyClmBillClsCdSk stage variable
# MAGIC Reddy Sanam                                                                      Changed stage variables to pass 'FACETS' when the source code is 
# MAGIC                                                                                                'LUMERIS' and the SK is a type code
# MAGIC Sunitha Ganta         10-12-2020                                       Brought up to standards            
# MAGIC Hugh Sisson             2023-09-28  ProdSupp                  Changed FcltyClmDschgSttusCdSk to use the GetFkeySrcTrgtClctnCodes
# MAGIC                                                                                               instead of GetFkeyCodes

# MAGIC Assign foreign keys and create default records for unknown and not applicable.
# MAGIC Read common record format file created in primary key job.
# MAGIC Writing Sequential File to /load
# MAGIC The Facility Claim SK and the Claim Sk are the same for each data row.
# MAGIC This hash file is written to by all claim foriegn key jobs.
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
    TimestampType,
    DecimalType
)
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    when,
    length,
    rpad,
    monotonically_increasing_id
)
from pyspark.sql import DataFrame
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


# Parameters
Source = get_widget_value("Source", "")
InFile = get_widget_value("InFile", "")
Logging = get_widget_value("Logging", "")
SrcSysCdSk = get_widget_value("SrcSysCdSk", "")

# Schema for ClmFcltyCrf (Stage: CSeqFileStage)
ClmFcltyCrf_schema = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),  # char(10)
    StructField("DISCARD_IN", StringType(), False),      # char(1)
    StructField("PASS_THRU_IN", StringType(), False),    # char(1)
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),  # numeric
    StructField("SRC_SYS_CD", StringType(), False),        # varchar(unknown length)
    StructField("PRI_KEY_STRING", StringType(), False),    # varchar(unknown length)
    StructField("FCLTY_CLM_SK", IntegerType(), False),     # primary key
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),            # char(18)
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("CLM_SK", IntegerType(), False),
    StructField("FCLTY_CLM_ADMS_SRC_CD", StringType(), False),  # char(2)
    StructField("FCLTY_CLM_ADMS_TYP_CD", StringType(), False),  # char(10)
    StructField("FCLTY_CLM_BILL_CLS_CD", StringType(), False),  # char(10)
    StructField("FCLTY_CLM_BILL_FREQ_CD", StringType(), False), # char(10)
    StructField("FCLTY_CLM_DSCHG_STTUS_CD", StringType(), False),# char(10)
    StructField("FCLTY_CLM_SUBTYP_CD", StringType(), False),    # char(10)
    StructField("FCLTY_CLM_PROC_BILL_METH_CD", StringType(), False), # char(10)
    StructField("FCLTY_TYP_CD", StringType(), False),           # char(10)
    StructField("IDS_GNRT_DRG_IN", StringType(), False),        # char(1)
    StructField("ADMS_DT", StringType(), False),                # char(10)
    StructField("BILL_STMNT_BEG_DT", StringType(), False),      # char(10)
    StructField("BILL_STMNT_END_DT", StringType(), False),      # char(10)
    StructField("DSCHG_DT", StringType(), False),               # char(10)
    StructField("HOSP_BEG_DT", StringType(), False),            # char(10)
    StructField("HOSP_END_DT", StringType(), False),            # char(10)
    StructField("ADMS_HR", DecimalType(38,10), False),          # decimal
    StructField("DSCHG_HR", DecimalType(38,10), False),         # decimal
    StructField("HOSP_COV_DAYS", DecimalType(38,10), False),    # decimal
    StructField("LOS_DAYS", IntegerType(), True),
    StructField("ADM_PHYS_PROV_ID", StringType(), False),       # varchar(unknown length)
    StructField("FCLTY_BILL_TYP_TX", StringType(), True),       # char(3) (nullable)
    StructField("GNRT_DRG_CD", StringType(), True),             # char(4) (nullable)
    StructField("MED_RCRD_NO", StringType(), True),             # varchar(unknown length)
    StructField("OTHER_PROV_ID_1", StringType(), False),        # varchar(unknown length)
    StructField("OTHER_PROV_ID_2", StringType(), False),        # varchar(unknown length)
    StructField("SUBMT_DRG_CD", StringType(), True),            # char(4) (nullable)
    StructField("NRMTV_DRG_CD", StringType(), False),           # char(4)
    StructField("DRG_METHOD_CD", StringType(), False)           # varchar(unknown length)
])

# Read ClmFcltyCrf
df_ClmFcltyCrf = (
    spark.read.format("csv")
    .option("header", "false")
    .option("sep", ",")
    .option("quote", "\"")
    .schema(ClmFcltyCrf_schema)
    .load(f"{adls_path}/key/{InFile}")
)

# Add stage variables (Transformer: ForeignKey)
df_foreignKey_enriched = (
    df_ClmFcltyCrf
    .withColumn(
        "GnrtDrgSK",
        GetFkeyDRG(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            col("GNRT_DRG_CD"),
            trim(col("DRG_METHOD_CD")),
            lit(Logging)
        )
    )
    .withColumn(
        "SubmtDrgSK",
        GetFkeyDRG(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            lit("NA"),
            lit("NA"),
            lit("X")
        )
    )
    .withColumn(
        "svNrmtvDrgSK",
        GetFkeyDRG(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            trim(col("NRMTV_DRG_CD")),
            trim(lit("MS")),
            lit("X")
        )
    )
    .withColumn(
        "FcltyClmAdmSrcCdSk",
        when(
            (col("SRC_SYS_CD") == "BCBSSC") | (col("SRC_SYS_CD") == "LUMERIS"),
            GetFkeyCodes(
                lit("FACETS"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM ADMISSION SOURCE"),
                col("FCLTY_CLM_ADMS_SRC_CD"),
                lit("Y")
            )
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM ADMISSION SOURCE"),
                col("FCLTY_CLM_ADMS_SRC_CD"),
                lit("Y")
            )
        )
    )
    .withColumn(
        "FcltyClmAdmTypCdSk",
        when(
            (col("SRC_SYS_CD") == "BCBSSC") | (col("SRC_SYS_CD") == "LUMERIS"),
            GetFkeyCodes(
                lit("FACETS"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM ADMISSION TYPE"),
                col("FCLTY_CLM_ADMS_TYP_CD"),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM ADMISSION TYPE"),
                col("FCLTY_CLM_ADMS_TYP_CD"),
                lit(Logging)
            )
        )
    )
    .withColumn(
        "FcltyClmBillClsCdSk",
        when(
            (col("SRC_SYS_CD") == "BCBSSC") | (col("SRC_SYS_CD") == "BCA") | (col("SRC_SYS_CD") == "LUMERIS"),
            GetFkeyCodes(
                lit("FACETS"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM BILL CLASSIFICATION"),
                col("FCLTY_CLM_BILL_CLS_CD"),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM BILL CLASSIFICATION"),
                col("FCLTY_CLM_BILL_CLS_CD"),
                lit(Logging)
            )
        )
    )
    .withColumn(
        "FcltyClmBillFreqCdSk",
        when(
            (col("SRC_SYS_CD") == "BCBSSC") | (col("SRC_SYS_CD") == "BCA") | (col("SRC_SYS_CD") == "LUMERIS"),
            GetFkeyCodes(
                lit("FACETS"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM BILL FREQUENCY"),
                col("FCLTY_CLM_BILL_FREQ_CD"),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM BILL FREQUENCY"),
                col("FCLTY_CLM_BILL_FREQ_CD"),
                lit(Logging)
            )
        )
    )
    .withColumn(
        "FcltyClmDschgSttusCdSk",
        when(
            (col("SRC_SYS_CD") == "BCBSSC") | (col("SRC_SYS_CD") == "BCA") | (col("SRC_SYS_CD") == "LUMERIS"),
            GetFkeySrcTrgtClctnCodes(
                lit("FACETS"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM DISCHARGE STATUS"),
                col("FCLTY_CLM_DSCHG_STTUS_CD"),
                lit("FACETS DBO"),
                lit("IDS"),
                lit(Logging)
            )
        ).otherwise(
            GetFkeySrcTrgtClctnCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM DISCHARGE STATUS"),
                col("FCLTY_CLM_DSCHG_STTUS_CD"),
                lit("FACETS DBO"),
                lit("IDS"),
                lit(Logging)
            )
        )
    )
    .withColumn(
        "FcltyClmSubTypCdSk",
        when(
            ( (col("SRC_SYS_CD") == "BCBSSC") | (col("SRC_SYS_CD") == "LUMERIS") ),
            when(
                (col("FCLTY_CLM_ADMS_SRC_CD") == "UNK") &
                (col("FCLTY_CLM_ADMS_TYP_CD") == "UNK") &
                (col("FCLTY_CLM_BILL_CLS_CD") == "UNK"),
                when(
                    GetFkeyCodes(
                        col("SRC_SYS_CD"),
                        col("FCLTY_CLM_SK"),
                        lit("FACILITY CLAIM SUBTYPE"),
                        col("FCLTY_CLM_SUBTYP_CD"),
                        lit("Y")
                    ) == lit(0),
                    GetFkeyCodes(
                        lit("FACETS"),
                        col("FCLTY_CLM_SK"),
                        lit("FACILITY CLAIM SUBTYPE"),
                        col("FCLTY_CLM_SUBTYP_CD"),
                        lit("Y")
                    )
                ).otherwise(
                    GetFkeyCodes(
                        col("SRC_SYS_CD"),
                        col("FCLTY_CLM_SK"),
                        lit("FACILITY CLAIM SUBTYPE"),
                        col("FCLTY_CLM_SUBTYP_CD"),
                        lit("Y")
                    )
                )
            ).otherwise(
                GetFkeyCodes(
                    lit("FACETS"),
                    col("FCLTY_CLM_SK"),
                    lit("FACILITY CLAIM SUBTYPE"),
                    col("FCLTY_CLM_SUBTYP_CD"),
                    lit("Y")
                )
            )
        ).when(
            col("SRC_SYS_CD") == "BCA",
            GetFkeyCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("CLAIM SUBTYPE"),
                col("FCLTY_CLM_SUBTYP_CD"),
                lit("Y")
            )
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM SUBTYPE"),
                col("FCLTY_CLM_SUBTYP_CD"),
                lit("Y")
            )
        )
    )
    .withColumn(
        "FcltyClmProcBillBethCdSk",
        when(
            (col("SRC_SYS_CD") == "BCBSSC") | (col("SRC_SYS_CD") == "LUMERIS"),
            when(
                (col("FCLTY_CLM_ADMS_SRC_CD") == "UNK") &
                (col("FCLTY_CLM_ADMS_TYP_CD") == "UNK") &
                (col("FCLTY_CLM_BILL_CLS_CD") == "UNK"),
                when(
                    GetFkeyCodes(
                        col("SRC_SYS_CD"),
                        col("FCLTY_CLM_SK"),
                        lit("FACILITY CLAIM PROCEDURE BILLING METHOD"),
                        col("FCLTY_CLM_PROC_BILL_METH_CD"),
                        lit(Logging)
                    ) == lit(0),
                    GetFkeyCodes(
                        lit("FACETS"),
                        col("FCLTY_CLM_SK"),
                        lit("FACILITY CLAIM PROCEDURE BILLING METHOD"),
                        col("FCLTY_CLM_PROC_BILL_METH_CD"),
                        lit(Logging)
                    )
                ).otherwise(
                    GetFkeyCodes(
                        col("SRC_SYS_CD"),
                        col("FCLTY_CLM_SK"),
                        lit("FACILITY CLAIM PROCEDURE BILLING METHOD"),
                        col("FCLTY_CLM_PROC_BILL_METH_CD"),
                        lit(Logging)
                    )
                )
            ).otherwise(
                GetFkeyCodes(
                    lit("FACETS"),
                    col("FCLTY_CLM_SK"),
                    lit("FACILITY CLAIM PROCEDURE BILLING METHOD"),
                    col("FCLTY_CLM_PROC_BILL_METH_CD"),
                    lit(Logging)
                )
            )
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY CLAIM PROCEDURE BILLING METHOD"),
                col("FCLTY_CLM_PROC_BILL_METH_CD"),
                lit(Logging)
            )
        )
    )
    .withColumn(
        "FcltyClmBillTypCdSk",
        when(
            (col("SRC_SYS_CD") == "BCBSSC") | (col("SRC_SYS_CD") == "BCA") | (col("SRC_SYS_CD") == "LUMERIS"),
            GetFkeyCodes(
                lit("FACETS"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY TYPE"),
                col("FCLTY_TYP_CD"),
                lit(Logging)
            )
        ).otherwise(
            GetFkeyCodes(
                col("SRC_SYS_CD"),
                col("FCLTY_CLM_SK"),
                lit("FACILITY TYPE"),
                col("FCLTY_TYP_CD"),
                lit(Logging)
            )
        )
    )
    .withColumn(
        "BillStmtBegDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            col("BILL_STMNT_BEG_DT"),
            lit(Logging)
        )
    )
    .withColumn(
        "BillStmtEndDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            col("BILL_STMNT_END_DT"),
            lit(Logging)
        )
    )
    .withColumn(
        "DschgDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            col("DSCHG_DT"),
            lit(Logging)
        )
    )
    .withColumn(
        "AdmsnDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            col("ADMS_DT"),
            lit(Logging)
        )
    )
    .withColumn(
        "HospBegDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            col("HOSP_BEG_DT"),
            lit(Logging)
        )
    )
    .withColumn(
        "HospEndDtSk",
        GetFkeyDate(
            lit("IDS"),
            col("FCLTY_CLM_SK"),
            col("HOSP_END_DT"),
            lit(Logging)
        )
    )
    .withColumn(
        "ClmFkey",
        GetFkeyClm(
            col("SRC_SYS_CD"),
            col("FCLTY_CLM_SK"),
            col("CLM_ID"),
            lit(Logging)
        )
    )
    .withColumn("PassThru", col("PASS_THRU_IN"))
    .withColumn("ErrCount", GetFkeyErrorCnt(col("FCLTY_CLM_SK")))
)

# Outputs from ForeignKey

# 1) Fkey link => Constraint: "ErrCount = 0 Or PassThru = 'Y'"
df_fkey_pre = df_foreignKey_enriched.filter(
    (col("ErrCount") == lit(0)) | (col("PassThru") == lit("Y"))
)
df_fkey = df_fkey_pre.select(
    col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
    lit(SrcSysCdSk).alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("ClmFkey").alias("CLM_SK"),
    col("GnrtDrgSK").alias("GNRT_DRG_SK"),
    col("SubmtDrgSK").alias("SUBMT_DRG_SK"),
    col("FcltyClmAdmSrcCdSk").alias("FCLTY_CLM_ADMS_SRC_CD_SK"),
    col("FcltyClmAdmTypCdSk").alias("FCLTY_CLM_ADMS_TYP_CD_SK"),
    col("FcltyClmBillClsCdSk").alias("FCLTY_CLM_BILL_CLS_CD_SK"),
    col("FcltyClmBillFreqCdSk").alias("FCLTY_CLM_BILL_FREQ_CD_SK"),
    col("FcltyClmBillTypCdSk").alias("FCLTY_CLM_BILL_TYP_CD_SK"),
    col("FcltyClmDschgSttusCdSk").alias("FCLTY_CLM_DSCHG_STTUS_CD_SK"),
    col("FcltyClmProcBillBethCdSk").alias("FCLTY_CLM_PROC_BILL_METH_CD_SK"),
    col("FcltyClmSubTypCdSk").alias("FCLTY_CLM_SUBTYP_CD_SK"),
    col("IDS_GNRT_DRG_IN").alias("GNRT_DRG_IN"),
    col("AdmsnDtSk").alias("ADMS_DT_SK"),
    col("BillStmtBegDtSk").alias("BILL_STMNT_BEG_DT_SK"),
    col("BillStmtEndDtSk").alias("BILL_STMNT_END_DT_SK"),
    col("DschgDtSk").alias("DSCHG_DT_SK"),
    col("HospBegDtSk").alias("HOSP_BEG_DT_SK"),
    col("HospEndDtSk").alias("HOSP_END_DT_SK"),
    col("ADMS_HR").alias("ADMS_HR"),
    col("DSCHG_HR").alias("DSCHG_HR"),
    col("HOSP_COV_DAYS").alias("HOSP_COV_DAYS"),
    col("LOS_DAYS").alias("LOS_DAYS"),
    when(
        length(trim(col("ADM_PHYS_PROV_ID"))) == lit(0),
        lit("NA")
    ).otherwise(
        trim(col("ADM_PHYS_PROV_ID"))
    ).alias("ADM_PHYS_PROV_ID"),
    col("FCLTY_BILL_TYP_TX").alias("FCLTY_BILL_TYP_TX"),
    col("MED_RCRD_NO").alias("MED_RCRD_NO"),
    col("OTHER_PROV_ID_1").alias("OTHR_PROV_ID_1"),
    col("OTHER_PROV_ID_2").alias("OTHR_PROV_ID_2"),
    col("svNrmtvDrgSK").alias("NRMTV_DRG_SK"),
    col("SUBMT_DRG_CD").alias("SUBMIT_DRG_TX")
)

# 2) recycle link => Constraint: "ErrCount > 0"
df_recycle_pre = df_foreignKey_enriched.filter(col("ErrCount") > lit(0))
df_recycle = df_recycle_pre.select(
    GetRecycleKey(col("FCLTY_CLM_SK")).alias("JOB_EXCTN_RCRD_ERR_SK"),
    col("INSRT_UPDT_CD").alias("INSRT_UPDT_CD"),
    col("DISCARD_IN").alias("DISCARD_IN"),
    col("PASS_THRU_IN").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ErrCount").alias("ERR_CT"),
    (col("RECYCLE_CT") + lit(1)).alias("RECYCLE_CT"),
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
    col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    col("CLM_ID").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK").alias("CLM_SK"),
    col("FCLTY_CLM_ADMS_SRC_CD").alias("FCLTY_CLM_ADMS_SRC_CD"),
    col("FCLTY_CLM_ADMS_TYP_CD").alias("FCLTY_CLM_ADMS_TYP_CD"),
    col("FCLTY_CLM_BILL_CLS_CD").alias("FCLTY_CLM_BILL_CLS_CD"),
    col("FCLTY_CLM_BILL_FREQ_CD").alias("FCLTY_CLM_BILL_FREQ_CD"),
    col("FCLTY_CLM_DSCHG_STTUS_CD").alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    col("FCLTY_CLM_SUBTYP_CD").alias("FCLTY_CLM_SUBTYP_CD"),
    col("FCLTY_CLM_PROC_BILL_METH_CD").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    col("FCLTY_TYP_CD").alias("FCLTY_TYP_CD"),
    col("IDS_GNRT_DRG_IN").alias("IDS_GNRT_DRG_IN"),
    col("ADMS_DT").alias("ADMS_DT"),
    col("BILL_STMNT_BEG_DT").alias("BILL_STMNT_BEG_DT"),
    col("BILL_STMNT_END_DT").alias("BILL_STMNT_END_DT"),
    col("DSCHG_DT").alias("DSCHG_DT"),
    col("HOSP_BEG_DT").alias("HOSP_BEG_DT"),
    col("HOSP_END_DT").alias("HOSP_END_DT"),
    col("ADMS_HR").alias("ADMS_HR"),
    col("DSCHG_HR").alias("DSCHG_HR"),
    col("HOSP_COV_DAYS").alias("HOSP_COV_DAYS"),
    col("LOS_DAYS").alias("LOS_DAYS"),
    col("ADM_PHYS_PROV_ID").alias("ADM_PHYS_PROV_ID"),
    col("FCLTY_BILL_TYP_TX").alias("FCLTY_BILL_TYP_TX"),
    col("GNRT_DRG_CD").alias("GNRT_DRG_CD"),
    col("MED_RCRD_NO").alias("MED_RCRD_NO"),
    col("OTHER_PROV_ID_1").alias("OTHER_PROV_ID_1"),
    col("OTHER_PROV_ID_2").alias("OTHER_PROV_ID_2"),
    col("SUBMT_DRG_CD").alias("SUBMT_DRG_CD"),
    col("NRMTV_DRG_CD").alias("NRMTV_DRG_CD"),
    col("DRG_METHOD_CD").alias("DRG_METHOD_CD")
)

# 3) DefaultUNK link => Constraint: "@INROWNUM = 1"
df_one_row_unk_pre = df_foreignKey_enriched.limit(1)
df_DefaultUNK = df_one_row_unk_pre.select(
    lit(0).alias("FCLTY_CLM_SK"),
    lit(0).alias("SRC_SYS_CD_SK"),
    lit("UNK").alias("CLM_ID"),
    lit(0).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(0).alias("CLM_SK"),
    lit(0).alias("GNRT_DRG_SK"),
    lit(0).alias("SUBMT_DRG_SK"),
    lit(0).alias("FCLTY_CLM_ADMS_SRC_CD_SK"),
    lit(0).alias("FCLTY_CLM_ADMS_TYP_CD_SK"),
    lit(0).alias("FCLTY_CLM_BILL_CLS_CD_SK"),
    lit(0).alias("FCLTY_CLM_BILL_FREQ_CD_SK"),
    lit(0).alias("FCLTY_CLM_BILL_TYP_CD_SK"),
    lit(0).alias("FCLTY_CLM_DSCHG_STTUS_CD_SK"),
    lit(0).alias("FCLTY_CLM_PROC_BILL_METH_CD_SK"),
    lit(0).alias("FCLTY_CLM_SUBTYP_CD_SK"),
    lit("U").alias("GNRT_DRG_IN"),
    lit("NA").alias("ADMS_DT_SK"),
    lit("NA").alias("BILL_STMNT_BEG_DT_SK"),
    lit("NA").alias("BILL_STMNT_END_DT_SK"),
    lit("NA").alias("DSCHG_DT_SK"),
    lit("NA").alias("HOSP_BEG_DT_SK"),
    lit("NA").alias("HOSP_END_DT_SK"),
    lit(0).alias("ADMS_HR"),
    lit(0).alias("DSCHG_HR"),
    lit(0).alias("HOSP_COV_DAYS"),
    lit(0).alias("LOS_DAYS"),
    lit("UNK").alias("ADM_PHYS_PROV_ID"),
    lit("UNK").alias("FCLTY_BILL_TYP_TX"),
    lit("UNK").alias("MED_RCRD_NO"),
    lit("UNK").alias("OTHR_PROV_ID_1"),
    lit("UNK").alias("OTHR_PROV_ID_2"),
    lit(0).alias("NRMTV_DRG_SK"),
    lit("UNK").alias("SUBMIT_DRG_TX")
)

# 4) DefaultNA link => Constraint: "@INROWNUM = 1"
df_one_row_na_pre = df_foreignKey_enriched.limit(1)
df_DefaultNA = df_one_row_na_pre.select(
    lit(1).alias("FCLTY_CLM_SK"),
    lit(1).alias("SRC_SYS_CD_SK"),
    lit("NA").alias("CLM_ID"),
    lit(1).alias("CRT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    lit(1).alias("CLM_SK"),
    lit(1).alias("GNRT_DRG_SK"),
    lit(1).alias("SUBMT_DRG_SK"),
    lit(1).alias("FCLTY_CLM_ADMS_SRC_CD_SK"),
    lit(1).alias("FCLTY_CLM_ADMS_TYP_CD_SK"),
    lit(1).alias("FCLTY_CLM_BILL_CLS_CD_SK"),
    lit(1).alias("FCLTY_CLM_BILL_FREQ_CD_SK"),
    lit(1).alias("FCLTY_CLM_BILL_TYP_CD_SK"),
    lit(1).alias("FCLTY_CLM_DSCHG_STTUS_CD_SK"),
    lit(1).alias("FCLTY_CLM_PROC_BILL_METH_CD_SK"),
    lit(1).alias("FCLTY_CLM_SUBTYP_CD_SK"),
    lit("X").alias("GNRT_DRG_IN"),
    lit("NA").alias("ADMS_DT_SK"),
    lit("NA").alias("BILL_STMNT_BEG_DT_SK"),
    lit("NA").alias("BILL_STMNT_END_DT_SK"),
    lit("NA").alias("DSCHG_DT_SK"),
    lit("NA").alias("HOSP_BEG_DT_SK"),
    lit("NA").alias("HOSP_END_DT_SK"),
    lit(0).alias("ADMS_HR"),
    lit(0).alias("DSCHG_HR"),
    lit(0).alias("HOSP_COV_DAYS"),
    lit(0).alias("LOS_DAYS"),
    lit("NA").alias("ADM_PHYS_PROV_ID"),
    lit("NA").alias("FCLTY_BILL_TYP_TX"),
    lit("NA").alias("MED_RCRD_NO"),
    lit("NA").alias("OTHR_PROV_ID_1"),
    lit("NA").alias("OTHR_PROV_ID_2"),
    lit(1).alias("NRMTV_DRG_SK"),
    lit("NA").alias("SUBMIT_DRG_TX")
)

# 5) Recycle_Clms link => Constraint: "ErrCount > 0"
df_recycle_clms = df_foreignKey_enriched.filter(col("ErrCount") > lit(0)).select(
    col("SRC_SYS_CD").alias("SRC_SYS_CD"),
    col("CLM_ID").alias("CLM_ID")
)

# Write hashed file hf_recycle (Scenario C => write parquet)
# Before writing, apply rpad for char/varchar columns if known length, or <...> if unknown.
df_recycle_for_write = df_recycle.select(
    rpad(col("JOB_EXCTN_RCRD_ERR_SK").cast(StringType()), <...>, " ").alias("JOB_EXCTN_RCRD_ERR_SK") if False else col("JOB_EXCTN_RCRD_ERR_SK"),  # int, ignoring rpad
    rpad(col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
    rpad(col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
    rpad(col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
    col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
    col("ERR_CT").alias("ERR_CT"),
    col("RECYCLE_CT").alias("RECYCLE_CT"),
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("PRI_KEY_STRING"), <...>, " ").alias("PRI_KEY_STRING"),
    col("FCLTY_CLM_SK").alias("FCLTY_CLM_SK"),
    col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK").alias("CLM_SK"),
    rpad(col("FCLTY_CLM_ADMS_SRC_CD"), 2, " ").alias("FCLTY_CLM_ADMS_SRC_CD"),
    rpad(col("FCLTY_CLM_ADMS_TYP_CD"), 10, " ").alias("FCLTY_CLM_ADMS_TYP_CD"),
    rpad(col("FCLTY_CLM_BILL_CLS_CD"), 10, " ").alias("FCLTY_CLM_BILL_CLS_CD"),
    rpad(col("FCLTY_CLM_BILL_FREQ_CD"), 10, " ").alias("FCLTY_CLM_BILL_FREQ_CD"),
    rpad(col("FCLTY_CLM_DSCHG_STTUS_CD"), 10, " ").alias("FCLTY_CLM_DSCHG_STTUS_CD"),
    rpad(col("FCLTY_CLM_SUBTYP_CD"), 10, " ").alias("FCLTY_CLM_SUBTYP_CD"),
    rpad(col("FCLTY_CLM_PROC_BILL_METH_CD"), 10, " ").alias("FCLTY_CLM_PROC_BILL_METH_CD"),
    rpad(col("FCLTY_TYP_CD"), 10, " ").alias("FCLTY_TYP_CD"),
    rpad(col("IDS_GNRT_DRG_IN"), 1, " ").alias("IDS_GNRT_DRG_IN"),
    rpad(col("ADMS_DT"), 10, " ").alias("ADMS_DT"),
    rpad(col("BILL_STMNT_BEG_DT"), 10, " ").alias("BILL_STMNT_BEG_DT"),
    rpad(col("BILL_STMNT_END_DT"), 10, " ").alias("BILL_STMNT_END_DT"),
    rpad(col("DSCHG_DT"), 10, " ").alias("DSCHG_DT"),
    rpad(col("HOSP_BEG_DT"), 10, " ").alias("HOSP_BEG_DT"),
    rpad(col("HOSP_END_DT"), 10, " ").alias("HOSP_END_DT"),
    col("ADMS_HR").alias("ADMS_HR"),
    col("DSCHG_HR").alias("DSCHG_HR"),
    col("HOSP_COV_DAYS").alias("HOSP_COV_DAYS"),
    col("LOS_DAYS").alias("LOS_DAYS"),
    rpad(col("ADM_PHYS_PROV_ID"), <...>, " ").alias("ADM_PHYS_PROV_ID"),
    rpad(col("FCLTY_BILL_TYP_TX"), 3, " ").alias("FCLTY_BILL_TYP_TX"),
    rpad(col("GNRT_DRG_CD"), 4, " ").alias("GNRT_DRG_CD"),
    rpad(col("MED_RCRD_NO"), <...>, " ").alias("MED_RCRD_NO"),
    rpad(col("OTHER_PROV_ID_1"), <...>, " ").alias("OTHER_PROV_ID_1"),
    rpad(col("OTHER_PROV_ID_2"), <...>, " ").alias("OTHER_PROV_ID_2"),
    rpad(col("SUBMT_DRG_CD"), 4, " ").alias("SUBMT_DRG_CD"),
    rpad(col("NRMTV_DRG_CD"), 4, " ").alias("NRMTV_DRG_CD"),
    rpad(col("DRG_METHOD_CD"), <...>, " ").alias("DRG_METHOD_CD")
)
write_files(
    df_recycle_for_write,
    "hf_recycle.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Write hashed file hf_claim_recycle_keys (Scenario C => write parquet)
df_recycle_clms_for_write = df_recycle_clms.select(
    rpad(col("SRC_SYS_CD"), <...>, " ").alias("SRC_SYS_CD"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID")
)
write_files(
    df_recycle_clms_for_write,
    "hf_claim_recycle_keys.parquet",
    delimiter=",",
    mode="overwrite",
    is_pqruet=True,
    header=True,
    quote="\"",
    nullValue=None
)

# Collector => union of DefaultUNK, DefaultNA, and Fkey
# All share this final schema:
collector_columns = [
    "FCLTY_CLM_SK",
    "SRC_SYS_CD_SK",
    "CLM_ID",
    "CRT_RUN_CYC_EXCTN_SK",
    "LAST_UPDT_RUN_CYC_EXCTN_SK",
    "CLM_SK",
    "GNRT_DRG_SK",
    "SUBMT_DRG_SK",
    "FCLTY_CLM_ADMS_SRC_CD_SK",
    "FCLTY_CLM_ADMS_TYP_CD_SK",
    "FCLTY_CLM_BILL_CLS_CD_SK",
    "FCLTY_CLM_BILL_FREQ_CD_SK",
    "FCLTY_CLM_BILL_TYP_CD_SK",
    "FCLTY_CLM_DSCHG_STTUS_CD_SK",
    "FCLTY_CLM_PROC_BILL_METH_CD_SK",
    "FCLTY_CLM_SUBTYP_CD_SK",
    "GNRT_DRG_IN",
    "ADMS_DT_SK",
    "BILL_STMNT_BEG_DT_SK",
    "BILL_STMNT_END_DT_SK",
    "DSCHG_DT_SK",
    "HOSP_BEG_DT_SK",
    "HOSP_END_DT_SK",
    "ADMS_HR",
    "DSCHG_HR",
    "HOSP_COV_DAYS",
    "LOS_DAYS",
    "ADM_PHYS_PROV_ID",
    "FCLTY_BILL_TYP_TX",
    "MED_RCRD_NO",
    "OTHR_PROV_ID_1",
    "OTHR_PROV_ID_2",
    "NRMTV_DRG_SK",
    "SUBMIT_DRG_TX"
]

df_collector = (
    df_DefaultUNK.select(collector_columns)
    .unionByName(df_DefaultNA.select(collector_columns))
    .unionByName(df_fkey.select(collector_columns))
)

# Apply rpad to final columns (char/varchar) before writing to FCLTY_CLM.#Source#.dat
df_collector_for_write = df_collector.select(
    col("FCLTY_CLM_SK"),
    col("SRC_SYS_CD_SK"),
    rpad(col("CLM_ID"), 18, " ").alias("CLM_ID"),
    col("CRT_RUN_CYC_EXCTN_SK"),
    col("LAST_UPDT_RUN_CYC_EXCTN_SK"),
    col("CLM_SK"),
    col("GNRT_DRG_SK"),
    col("SUBMT_DRG_SK"),
    col("FCLTY_CLM_ADMS_SRC_CD_SK"),
    col("FCLTY_CLM_ADMS_TYP_CD_SK"),
    col("FCLTY_CLM_BILL_CLS_CD_SK"),
    col("FCLTY_CLM_BILL_FREQ_CD_SK"),
    col("FCLTY_CLM_BILL_TYP_CD_SK"),
    col("FCLTY_CLM_DSCHG_STTUS_CD_SK"),
    col("FCLTY_CLM_PROC_BILL_METH_CD_SK"),
    col("FCLTY_CLM_SUBTYP_CD_SK"),
    rpad(col("GNRT_DRG_IN"), 1, " ").alias("GNRT_DRG_IN"),
    rpad(col("ADMS_DT_SK"), 10, " ").alias("ADMS_DT_SK"),
    rpad(col("BILL_STMNT_BEG_DT_SK"), 10, " ").alias("BILL_STMNT_BEG_DT_SK"),
    rpad(col("BILL_STMNT_END_DT_SK"), 10, " ").alias("BILL_STMNT_END_DT_SK"),
    rpad(col("DSCHG_DT_SK"), 10, " ").alias("DSCHG_DT_SK"),
    rpad(col("HOSP_BEG_DT_SK"), 10, " ").alias("HOSP_BEG_DT_SK"),
    rpad(col("HOSP_END_DT_SK"), 10, " ").alias("HOSP_END_DT_SK"),
    col("ADMS_HR"),
    col("DSCHG_HR"),
    col("HOSP_COV_DAYS"),
    col("LOS_DAYS"),
    rpad(col("ADM_PHYS_PROV_ID"), <...>, " ").alias("ADM_PHYS_PROV_ID"),
    rpad(col("FCLTY_BILL_TYP_TX"), 3, " ").alias("FCLTY_BILL_TYP_TX"),
    rpad(col("MED_RCRD_NO"), <...>, " ").alias("MED_RCRD_NO"),
    rpad(col("OTHR_PROV_ID_1"), <...>, " ").alias("OTHR_PROV_ID_1"),
    rpad(col("OTHR_PROV_ID_2"), <...>, " ").alias("OTHR_PROV_ID_2"),
    col("NRMTV_DRG_SK"),
    rpad(col("SUBMIT_DRG_TX"), 4, " ").alias("SUBMIT_DRG_TX")
)

# Write final file (Stage: FCLTY_CLM => CSeqFileStage)
write_files(
    df_collector_for_write,
    f"{adls_path}/load/FCLTY_CLM.{Source}.dat",
    delimiter=",",
    mode="overwrite",
    is_pqruet=False,
    header=False,
    quote="\"",
    nullValue=None
)