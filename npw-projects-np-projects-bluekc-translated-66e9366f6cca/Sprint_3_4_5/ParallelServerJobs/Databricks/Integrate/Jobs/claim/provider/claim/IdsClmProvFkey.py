# Databricks notebook source
# MAGIC %md
# MAGIC **Date:** 2025-08-23
# MAGIC """
# MAGIC *****************************************************************************************************************************************************************************
# MAGIC COPYRIGHT 2004 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2007 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC COPYRIGHT 2009 BLUE CROSS/BLUE SHIELD OF KANSAS CITY 
# MAGIC 
# MAGIC DESCRIPTION:     Takes the sorted file and applies the primary key.   Preps the file for the foreign key lookup process that follows.
# MAGIC 
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC 
# MAGIC MODIFICATIONS:
# MAGIC Developer              Date                 Project/Altiris #      Change Description                                                                                                     Development Project      Code Reviewer          Date Reviewed      
# MAGIC ------------------            --------------------     ------------------------      -----------------------------------------------------------------------                                                           --------------------------------       -------------------------------   ----------------------------       
# MAGIC Tom Harrocks        04/12/2004                                   Originally Programmed
# MAGIC  BJ Luce                08/2004                                        Phase 2 Added stage variable to transform SRC_SYS_CD to SrcSysCdProv 
# MAGIC                                                                                     using TRANSFORM SrcSysCdProvFkey. 
# MAGIC                                                                                     This SrcSysCdProv will be used as the source system in the provider foreign keying. 
# MAGIC                                                                                     Change input format to match pkey format
# MAGIC  Brent Leland         08/26/2004                                   Added default rows.
# MAGIC Steph Goddard      09/02/2004                                   Changed SK on default rows from 'NA' and 'UNK' to 1 and 0
# MAGIC Brent Leland          11/01/2004                                   Corrected the error recycle common record format.
# MAGIC Steph Goddard      02/14/2006                                   Changed for sequencer
# MAGIC SAndrew               12/05/2007      IAD Qrt Release    TTR204 - Changed stage variable ProvSK rule if provider role                                         devlIDS                          Steph Goddard          01/10/2008
# MAGIC                                                                                      type is for PCP, set the source system code value used in GetFkeyProv to be FACETS.                                                                                                                                         
# MAGIC                                                                                      All PCP Providers are FACETS providers.	
# MAGIC SAndrew                  2008-10-10  3784 PBM Vnedr    Changed rule behind called routine SrcSysCdProvFkey .                                                  devlIDSnew                    Steph Goddard           10/27/2008
# MAGIC                                                                                     Changed rule for stage variable ProvSk
# MAGIC SAndrew               2009-02-24      494 Labor Accnt      Changed stage variable ProvSK rule from                                                                       devlIDS                           Steph Goddard           03/30/2009
# MAGIC                                                                              If (Key.CLM_PROV_ROLE_TYP_CD="PCP") or (Key.CLM_PROV_ROLE_TYP_CD="PRSCRB" AND Key.SRC_SYS_CD='ESI') then GetFkeyProv("FACETS",Key.CLM_PROV_SK,Key.PROV_ID, Logging) 
# MAGIC                                                                                else  GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)
# MAGIC                                                                             TO If ( Key.CLM_PROV_ROLE_TYP_CD = "PCP" ) or ((Key.CLM_PROV_ROLE_TYP_CD = "PRSCRB" AND Key.SRC_SYS_CD = 'ESI')  OR
# MAGIC                                                                             ( Key.CLM_PROV_ROLE_TYP_CD = "PRSCRB" AND Key.SRC_SYS_CD = 'WELLDYNERX' )) then  GetFkeyProv("FACETS", Key.CLM_PROV_SK, Key.PROV_ID, Logging)  
# MAGIC                                                                             else  GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)
# MAGIC Kalyan Neelam     2009-10-07       4098                    Changed stage variable ClmProvRoleSk rule                                                                                                devlIDS Steph Goddard          10/15/2009
# MAGIC                                                                                   If Key.SRC_SYS_CD = 'MCSOURCE' Then 
# MAGIC                                                                                   GetFkeyCodes('FACETS', Key.CLM_PROV_SK, "CLAIM PROVIDER ROLE TYPE", Key.CLM_PROV_ROLE_TYP_CD, Logging) 
# MAGIC                                                                                   Else GetFkeyCodes(Key.SRC_SYS_CD, Key.CLM_PROV_SK, "CLAIM PROVIDER ROLE TYPE", Key.CLM_PROV_ROLE_TYP_CD, Logging)
# MAGIC Kalyan Neelam     2010-01-04       4110                    Added new option 'MCAID' in stage variable provsk also added new logic for MCAID svc_prov_sk  IntegrateCurDevl  Steph Goddard      01/11/2010
# MAGIC 
# MAGIC Kalyan Neelam     2010-05-24       TTR729              Added new logic for PCS and CAREMARK Prov Sk                                                        IntegrateCurDevl                  Steph Goddard           05/28/2010
# MAGIC Kalyan Neelam     2010-12-22       4616                   Added new option 'MEDTRAK' in stage variable provsk                                                  IntegrateNewDevl                Steph Goddard           12/23/2010
# MAGIC 
# MAGIC Karthik Chintalapani 2012-01-22  4784              Added SRC_SYS_CD = 'BCBSSC' for stage variable ProvSk                                                IntegrateCurDevl                   Brent Leland               02-08-2012
# MAGIC Kalyan Neelam       2013-11-12    5056 FEP Claims      Added new option 'BCA' in stage variable provsk                                                        IntegrateNewDevl                Bhoomi Dasari             11/30/2013
# MAGIC                                                                                  
# MAGIC Karthik Chintalapani 2016-05-20  5212             Added  'BCA'  for SRC_SYS_CD = 'BCBSA' for stage variable ClmProvRoleSk                      IntegrateDev2                        Kalyan Neelam            2016-05-23
# MAGIC                                                                          to retrieve the correct SK's
# MAGIC 
# MAGIC Madhavan B	2017-06-20   5788 - BHI Updates    Added new column                                                                                                  IntegrateDev2                        Kalyan Neelam            2017-07-12
# MAGIC                                                                                         SVC_FCLTY_LOC_NTNL_PROV_ID
# MAGIC 
# MAGIC Sudhir Bomshetty     2017-10-19   5781          In Stage Variable ProvSk, added If  Key.SRC_SYS_CD = 'BCA' Then If                                     IntegrateDev2                    Kalyan Neelam            2017-10-23
# MAGIC                                                                         (GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging) = 0) 
# MAGIC 	     			        Then GetFkeyProv(SrcSysCdProv, Key.CLM_PROV_SK, Key.PROV_ID, Logging)
# MAGIC 	     	                                         Else GetFkeyProv('BCA', Key.CLM_PROV_SK, Key.PROV_ID, Logging)
# MAGIC Manasa Andru    2018-03-04   TFS21154     Modified ProvSk stage variable to not log errors when looking Fkey in If logic                        IntegrateDev2                          Kalyan Neelam            2018-03-12
# MAGIC 
# MAGIC Kaushik Kapoor  2018-03-19    5828             Adding SAVRX and LDI source system code in PROV_SK                                                     IntegrateDev2                         Jaideep Mankala        03/21/2018
# MAGIC 
# MAGIC Ravi Abburi        2018-01-29    5781 - HEDIS            Added the NTNL_PROV_ID to lookup for the NTNL_PROV_SK                               IntegrateDev1                          Kalyan Neelam            2018-04-25
# MAGIC                                                                                         by using the GetFkeyNtnlProvId
# MAGIC 
# MAGIC Kaushik Kapoor     2018-09-20    5828                                   Adding CVS source system code in PROV_SK                                           IntegrateDev2                             Kalyan Neelam            2018-10-01
# MAGIC 
# MAGIC Srinidhi Kambham  2019-10-11   6131- PBM Replacement    Added "OPTUMRX" Value to 'ProvSk' stage variable  field  in Transformer Derivation.        IntegrateDev1  Kalyan Neelam            2019-11-20
# MAGIC 
# MAGIC Reddy Sanam       2020-10-09                                             ClmProvRoleSk stage variable derivation is mapped to 
# MAGIC                                                                                              FACETS when src sys cd is 'LUMERIS'
# MAGIC 
# MAGIC Goutham K           2020-11-12      US-283560                    Added MEDIMPACT to StageVar ProvSK and ClmProvRoleSk                        IntegrateDev2                           Kalyan Neelam           2020-11-12
# MAGIC 
# MAGIC Sravya Sree Y     2021-02-03      RA                         Updated Stage Variable "ClmProvRoleSk" and "PROV_SK" to Include "DentaQuest"        IntegrateDev2                Kalyan Neelam          2021-02-03
# MAGIC Mrudula Kodali    2021-15-03      311337                         Updated Stage Variable svNtnlProvSk for Livongo                                                               IntegrateDev2                Jaideep Mankala      03/18/2021
# MAGIC                                                                                    and Stage Variable "ProvSk" to Include "SOLUTRAN"
# MAGIC Mrudula Kodali                05/03/2021  US-373652        Updated the stage variables ClmProvRoleSk					IntegrateDev2     Jaideep Mankala     05/10/2021
# MAGIC                                                                                        Removed usages of FACETS for EYEMED, DENTAQUEST, SOLUTRON, LVNGHLTH, EMR
# MAGIC Abhishek Pulluri     2021-06 -03     RA                    Removed LIVONGO If condition in Stage Varibale- svNtnlProvSk                 IntegrateDev2                                          Kalyan Nelam            20201-06-03                    
# MAGIC 
# MAGIC Manisha Gandra    2021-11-29               US 459610        Added NATIONS to StageVar ProvSK                                                  IntegrateDev2                    Jeyaprasanna                          2022-01-05     
# MAGIC  
# MAGIC Sham Shankaranarayana 2023-12-21    US 599810       Updated existing StageVar ProvSk in Transformer ForeignKey                 IntegrateDev1                 Jeyaprasanna                          2023-12-21

# MAGIC Read common record format file.
# MAGIC Writing Sequential File to /load
# MAGIC This hash file is written to by all claim foriegn key jobs.
# MAGIC Lookup foreign keys and create default rows for UNK and NA
# MAGIC Recycle records with ErrCount > 0
# MAGIC """
# COMMAND ----------
# MAGIC %run ../../../../../Routine_Functions
# COMMAND ----------
# Using the below functions as UDF(inherited from Routine_Functions notebook) instead of pyspark function
# 1) current_timestamp, current_date - To convert to CST timezone.
# 2) trim - To mimic Datastage behavior in trimming spaces within the string (Not leading or trailing spaces)
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    TimestampType,
    DecimalType
)
# COMMAND ----------
# MAGIC %run ../../../../../Utility_Integrate
# COMMAND ----------


Logging = get_widget_value("Logging","X")
Source = get_widget_value("Source","")
InFile = get_widget_value("InFile","ClmProv.dat")

schema_ClmProvCrf = StructType([
    StructField("JOB_EXCTN_RCRD_ERR_SK", IntegerType(), False),
    StructField("INSRT_UPDT_CD", StringType(), False),
    StructField("DISCARD_IN", StringType(), False),
    StructField("PASS_THRU_IN", StringType(), False),
    StructField("FIRST_RECYC_DT", TimestampType(), False),
    StructField("ERR_CT", IntegerType(), False),
    StructField("RECYCLE_CT", DecimalType(38,10), False),
    StructField("SRC_SYS_CD", StringType(), False),
    StructField("PRI_KEY_STRING", StringType(), False),
    StructField("CLM_PROV_SK", IntegerType(), False),
    StructField("SRC_SYS_CD_SK", IntegerType(), False),
    StructField("CLM_ID", StringType(), False),
    StructField("CLM_PROV_ROLE_TYP_CD", StringType(), False),
    StructField("CRT_RUN_CYC_EXCTN_SK", IntegerType(), False),
    StructField("LAST_UPDT_RUN_CYC_EXCTN_SK", IntegerType(), True),
    StructField("PROV_ID", StringType(), False),
    StructField("TAX_ID", StringType(), False),
    StructField("SVC_FCLTY_LOC_NTNL_PROV_ID", StringType(), False),
    StructField("NTNL_PROV_ID", StringType(), False)
])

df_ClmProvCrf = (
    spark.read
    .option("sep", ",")
    .option("quote", "\"")
    .option("header", "false")
    .schema(schema_ClmProvCrf)
    .csv(f"{adls_path}/key/{InFile}")
)

w = Window.orderBy(F.lit(1))

df_00 = df_ClmProvCrf.withColumn("SrcSysCdTrim", F.trim(F.col("SRC_SYS_CD")))

df_01 = df_00.withColumn(
    "SrcSysCdProv",
    F.when(
        (F.col("SrcSysCdTrim").isNull()) | (F.length(F.col("SrcSysCdTrim")) == 0),
        F.lit("UNK")
    )
    .when(
        F.col("SrcSysCdTrim").isin(
            "PCS","CAREMARK","ARGUS","ESI","OPTUMRX","WELLDYNERX",
            "MCSOURCE","MCAID","MEDTRAK","BCBSSC","BCA"
        ),
        F.lit("NABP")
    )
    .when(
        F.col("SrcSysCdTrim").isin("PCT","BCBSA","LUMERIS"),
        F.lit("FACETS")
    )
    .when(
        F.col("SrcSysCdTrim") == "EYEMED",
        F.lit("EYEMED")
    )
    .otherwise(F.lit("FACETS"))
)

df_02 = df_01.withColumn(
    "TrimProvId",
    F.trim(F.col("PROV_ID"))
)

df_03 = df_02.withColumn(
    "ClmProvRoleSk",
    F.when(
        F.col("SRC_SYS_CD") == "BCBSA",
        GetFkeyCodes(
            F.lit("BCA"),
            F.col("CLM_PROV_SK"),
            F.lit("CLAIM PROVIDER ROLE TYPE"),
            F.col("CLM_PROV_ROLE_TYP_CD"),
            F.lit(Logging)
        )
    )
    .when(
        F.col("SRC_SYS_CD") == "LUMERIS",
        GetFkeyCodes(
            F.lit("FACETS"),
            F.col("CLM_PROV_SK"),
            F.lit("CLAIM PROVIDER ROLE TYPE"),
            F.col("CLM_PROV_ROLE_TYP_CD"),
            F.lit(Logging)
        )
    )
    .when(
        (F.col("SRC_SYS_CD").isin("OPTUMRX","MEDIMPACT")) &
        (F.col("CLM_PROV_ROLE_TYP_CD").isin("PCP","PRSCRB")),
        GetFkeyCodes(
            F.lit("FACETS"),
            F.col("CLM_PROV_SK"),
            F.lit("CLAIM PROVIDER ROLE TYPE"),
            F.col("CLM_PROV_ROLE_TYP_CD"),
            F.lit(Logging)
        )
    )
    .when(
        (F.col("SRC_SYS_CD") == "DENTAQUEST") &
        (F.col("CLM_PROV_ROLE_TYP_CD").isin("BILL","SVC")),
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("CLM_PROV_SK"),
            F.lit("CLAIM PROVIDER ROLE TYPE"),
            F.col("CLM_PROV_ROLE_TYP_CD"),
            F.lit(Logging)
        )
    )
    .otherwise(
        GetFkeyCodes(
            F.col("SRC_SYS_CD"),
            F.col("CLM_PROV_SK"),
            F.lit("CLAIM PROVIDER ROLE TYPE"),
            F.col("CLM_PROV_ROLE_TYP_CD"),
            F.lit(Logging)
        )
    )
)

df_04 = df_03.withColumn(
    "ProvSk",
    F.when(
        F.col("TrimProvId") == "UNK",
        F.lit(0)
    )
    .when(
        F.col("TrimProvId") == "NA",
        F.lit(1)
    )
    .when(
        (
            (F.col("CLM_PROV_ROLE_TYP_CD") == "PCP") |
            (
                (F.col("CLM_PROV_ROLE_TYP_CD") == "PRSCRB") &
                F.col("SRC_SYS_CD").isin(
                    "OPTUMRX","MEDIMPACT","ESI","WELLDYNERX","MCSOURCE",
                    "MCAID","PCS","CAREMARK","MEDTRAK","BCBSSC","BCA"
                )
            )
        ),
        GetFkeyProv(
            F.lit("FACETS"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        (
            (F.col("CLM_PROV_ROLE_TYP_CD") == "PRSCRB") &
            F.col("SRC_SYS_CD").isin("BCBSA","SAVRX","LDI","CVS") &
            (GetFkeyProv(F.lit("NABP"),F.col("CLM_PROV_SK"),F.col("PROV_ID"),F.lit("X")) != 0)
        ),
        GetFkeyProv(
            F.lit("NABP"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        (
            (F.col("CLM_PROV_ROLE_TYP_CD") == "PRSCRB") &
            F.col("SRC_SYS_CD").isin("BCBSA","SAVRX","LDI","CVS") &
            (GetFkeyProv(F.lit("FACETS"),F.col("CLM_PROV_SK"),F.col("PROV_ID"),F.lit("X")) != 0)
        ),
        GetFkeyProv(
            F.lit("FACETS"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        (
            (F.col("CLM_PROV_ROLE_TYP_CD") == "SVC") &
            F.col("SRC_SYS_CD").isin("MCAID","PCS","CAREMARK") &
            (GetFkeyProv(F.col("SrcSysCdProv"),F.col("CLM_PROV_SK"),F.col("PROV_ID"),F.lit("X")) == 0)
        ),
        GetFkeyProv(
            F.lit("FACETS"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        (F.col("SRC_SYS_CD") == "BCBSSC") &
        (GetFkeyProv(F.lit("FACETS"),F.col("CLM_PROV_SK"),F.col("PROV_ID"),F.lit("X")) != 0),
        GetFkeyProv(
            F.lit("FACETS"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        F.col("SRC_SYS_CD") == "BCBSSC",
        GetFkeyProv(
            F.col("SrcSysCdProv"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        F.col("SRC_SYS_CD").isin("BCBSA","SAVRX","LDI","CVS") &
        (GetFkeyProv(F.lit("NABP"),F.col("CLM_PROV_SK"),F.col("PROV_ID"),F.lit("X")) == 0),
        GetFkeyProv(
            F.col("SrcSysCdProv"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        F.col("SRC_SYS_CD").isin("BCBSA","SAVRX","LDI","CVS"),
        GetFkeyProv(
            F.lit("NABP"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        (F.col("SRC_SYS_CD")=="BCA") &
        (GetFkeyProv(F.lit("BCA"),F.col("CLM_PROV_SK"),F.col("PROV_ID"),F.lit("X")) == 0),
        GetFkeyProv(
            F.col("SrcSysCdProv"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        F.col("SRC_SYS_CD")=="BCA",
        GetFkeyProv(
            F.lit("BCA"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .when(
        F.col("SRC_SYS_CD").isin("DENTAQUEST","SOLUTRAN","DOMINION","MARC"),
        GetFkeyProv(
            F.col("SRC_SYS_CD"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
    .otherwise(
        GetFkeyProv(
            F.col("SrcSysCdProv"),
            F.col("CLM_PROV_SK"),
            F.col("PROV_ID"),
            F.lit(Logging)
        )
    )
)

df_05 = df_04.withColumn(
    "ClmSk",
    GetFkeyClm(
        F.col("SRC_SYS_CD"),
        F.col("CLM_PROV_SK"),
        F.col("CLM_ID"),
        F.lit(Logging)
    )
)

df_06 = df_05.withColumn(
    "ErrCount",
    GetFkeyErrorCnt(F.col("CLM_PROV_SK"))
)

df_07 = df_06.withColumn(
    "PassThru",
    F.col("PASS_THRU_IN")
)

df_08 = df_07.withColumn(
    "JobExecRecSK",
    F.when(
        F.col("ErrCount") > 0,
        GetRecycleKey(F.col("CLM_PROV_SK"))
    ).otherwise(F.lit(0))
)

df_09 = df_08.withColumn(
    "svNtnlProvSK",
    GetFkeyNtnlProvId(
        F.lit("CMS"),
        F.col("CLM_PROV_SK"),
        F.col("NTNL_PROV_ID"),
        F.lit(Logging)
    )
)

df_Transformed = df_09.withColumn("rn", F.row_number().over(w))

df_ForeignKey_Fkey = df_Transformed.filter(
    (F.col("ErrCount") == 0) | (F.col("PassThru") == F.lit("Y"))
)

df_ForeignKey_Recycle = df_Transformed.filter(
    F.col("ErrCount") > 0
)

df_ForeignKey_Recycle_Clms = df_Transformed.filter(
    F.col("ErrCount") > 0
)

df_ForeignKey_DefaultUNK = (
    df_Transformed
    .filter(F.col("rn") == 1)
    .selectExpr(
        "0 as CLM_PROV_SK",
        "0 as SRC_SYS_CD_SK",
        "'UNK' as CLM_ID",
        "0 as CLM_PROV_ROLE_TYP_CD_SK",
        "0 as CRT_RUN_CYC_EXCTN_SK",
        "0 as LAST_UPDT_RUN_CYC_EXCTN_SK",
        "0 as CLM_SK",
        "0 as PROV_SK",
        "'UNK' as PROV_ID",
        "'UNK' as TAX_ID",
        "'UNK' as SVC_FCLTY_LOC_NTNL_PROV_ID",
        "0 as NTNL_PROV_SK"
    )
)

df_ForeignKey_DefaultNA = (
    df_Transformed
    .filter(F.col("rn") == 1)
    .selectExpr(
        "1 as CLM_PROV_SK",
        "1 as SRC_SYS_CD_SK",
        "'NA' as CLM_ID",
        "1 as CLM_PROV_ROLE_TYP_CD_SK",
        "1 as CRT_RUN_CYC_EXCTN_SK",
        "1 as LAST_UPDT_RUN_CYC_EXCTN_SK",
        "1 as CLM_SK",
        "1 as PROV_SK",
        "'NA' as PROV_ID",
        "'NA' as TAX_ID",
        "'NA' as SVC_FCLTY_LOC_NTNL_PROV_ID",
        "1 as NTNL_PROV_SK"
    )
)

df_Recycle_out = (
    df_ForeignKey_Recycle.select(
        F.col("JobExecRecSK").alias("JOB_EXCTN_RCRD_ERR_SK"),
        F.rpad(F.col("INSRT_UPDT_CD"), 10, " ").alias("INSRT_UPDT_CD"),
        F.rpad(F.col("DISCARD_IN"), 1, " ").alias("DISCARD_IN"),
        F.rpad(F.col("PASS_THRU_IN"), 1, " ").alias("PASS_THRU_IN"),
        F.col("FIRST_RECYC_DT").alias("FIRST_RECYC_DT"),
        F.col("ErrCount").alias("ERR_CT"),
        (F.col("RECYCLE_CT") + F.lit(1)).alias("RECYCLE_CT"),
        F.col("SRC_SYS_CD").alias("SRC_SYS_CD"),
        F.col("PRI_KEY_STRING").alias("PRI_KEY_STRING"),
        F.col("CLM_PROV_SK").alias("CLM_PROV_SK"),
        F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
        F.rpad(F.col("CLM_PROV_ROLE_TYP_CD"), 10, " ").alias("CLM_PROV_ROLE_TYP_CD"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("PROV_ID").alias("PROV_ID"),
        F.rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID"),
        F.col("SVC_FCLTY_LOC_NTNL_PROV_ID").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
        F.col("NTNL_PROV_ID").alias("NTNL_PROV_ID")
    )
)

write_files(
    df_Recycle_out,
    f"{adls_path}/hf_recycle.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Recycle_Clms_out = (
    df_ForeignKey_Recycle_Clms.select(
        F.rpad(F.col("SRC_SYS_CD"), F.length(F.col("SRC_SYS_CD")), " ").alias("SRC_SYS_CD"),
        F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID")
    )
)

write_files(
    df_Recycle_Clms_out,
    f"{adls_path}/hf_claim_recycle_keys.parquet",
    ",",
    "overwrite",
    True,
    True,
    "\"",
    None
)

df_Collector = (
    df_ForeignKey_Fkey.select(
        F.col("CLM_PROV_SK").alias("CLM_PROV_SK"),
        F.col("SRC_SYS_CD_SK").alias("SRC_SYS_CD_SK"),
        F.rpad(F.col("CLM_ID"), 18, " ").alias("CLM_ID"),
        F.col("ClmProvRoleSk").alias("CLM_PROV_ROLE_TYP_CD_SK"),
        F.col("CRT_RUN_CYC_EXCTN_SK").alias("CRT_RUN_CYC_EXCTN_SK"),
        F.col("LAST_UPDT_RUN_CYC_EXCTN_SK").alias("LAST_UPDT_RUN_CYC_EXCTN_SK"),
        F.col("ClmSk").alias("CLM_SK"),
        F.col("ProvSk").alias("PROV_SK"),
        F.col("PROV_ID").alias("PROV_ID"),
        F.rpad(F.col("TAX_ID"), 9, " ").alias("TAX_ID"),
        F.col("SVC_FCLTY_LOC_NTNL_PROV_ID").alias("SVC_FCLTY_LOC_NTNL_PROV_ID"),
        F.when(
            (F.col("CLM_PROV_ROLE_TYP_CD") == "PCP") |
            (F.col("SRC_SYS_CD") == "FACETS") |
            (F.col("SRC_SYS_CD") == "LVNGHLTH"),
            F.when(F.col("svNtnlProvSK") == 0, F.lit(1)).otherwise(F.col("svNtnlProvSK"))
        )
        .otherwise(
            F.when(
                F.col("NTNL_PROV_ID") == "NA",
                F.lit(1)
            ).when(
                F.col("NTNL_PROV_ID").isNull(),
                F.lit(0)
            )
            .otherwise(F.col("svNtnlProvSK"))
        )
        .alias("NTNL_PROV_SK")
    )
    .unionByName(df_ForeignKey_DefaultUNK)
    .unionByName(df_ForeignKey_DefaultNA)
)

write_files(
    df_Collector,
    f"{adls_path}/load/CLM_PROV.{Source}.dat",
    ",",
    "overwrite",
    False,
    False,
    "\"",
    None
)