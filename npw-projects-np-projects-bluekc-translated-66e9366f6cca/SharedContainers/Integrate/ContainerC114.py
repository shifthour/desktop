# MAGIC %run ./Utility
# COMMAND ----------
# MAGIC %run ./Routine_Functions
# COMMAND ----------

"""
JobName: ContainerC114
JobType: Server Job
JobCategory: DS_Integrate
FolderPath: Jobs/SharedContainerDsx
"""

import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)

def run_ContainerC114(params: dict) -> None:
    """
    This function encapsulates the logic of the ContainerC114 shared container.
    It reads data from a database (FacetsOwner) and a source file,
    performs aggregations and transformations, and finally writes out to a file.
    """
    # Unpack only the parameters needed (ignore other $-prefixed items unless needed).
    FacetsOwner       = params["FacetsOwner"]
    facets_secret_name = params["facets_secret_name"]
    facets_jdbc_url   = params["facets_jdbc_url"]
    facets_jdbc_props = params["facets_jdbc_props"]
    adls_path         = params["adls_path"]

    # 1) Extract from cdml_cap (ODBCConnector) => "cob_extr"
    query_cob_extr = f"""
SELECT
  CL_LINE.CLCL_ID,
  CL_LINE.CDML_SEQ_NO,
  CL_LINE.CDML_DISALL_AMT AS DISALL_AMT,
  CLM.CLCL_PAID_DT,
  CL_LINE.CDML_CONSIDER_CHG,
  CL_LINE.CDML_DED_AMT,
  CL_LINE.CDML_COPAY_AMT,
  CL_LINE.CDML_COINS_AMT
FROM {FacetsOwner}.CMC_CDML_CL_LINE CL_LINE,
     tempdb..#DriverTable# TMP,
     {FacetsOwner}.CMC_CLCL_CLAIM CLM
WHERE TMP.CLM_ID = CL_LINE.CLCL_ID
  AND CLM.CLCL_ID = TMP.CLM_ID
  AND CL_LINE.CDML_CAP_IND = 'Y'
"""
    df_cob_extr = (
        spark.read.format("jdbc")
        .option("url", facets_jdbc_url)
        .options(**facets_jdbc_props)
        .option("query", query_cob_extr)
        .load()
    )

    # 2) Extract from cdml_cap (ODBCConnector) => "cap_ln_extr"
    query_cap_ln_extr = f"""
SELECT
  CL_LINE.CLCL_ID,
  CL_LINE.CDML_SEQ_NO,
  CL_LINE.CDML_DISALL_AMT AS DISALL_AMT,
  CLM.CLCL_PAID_DT,
  CL_LINE.CDML_CONSIDER_CHG,
  CL_LINE.CDML_DED_AMT,
  CL_LINE.CDML_COPAY_AMT,
  CL_LINE.CDML_COINS_AMT
FROM {FacetsOwner}.CMC_CDML_CL_LINE CL_LINE,
     tempdb..#DriverTable# TMP,
     {FacetsOwner}.CMC_CLCL_CLAIM CLM
WHERE TMP.CLM_ID = CL_LINE.CLCL_ID
  AND CLM.CLCL_ID = TMP.CLM_ID
  AND CL_LINE.CDML_CAP_IND = 'Y'
"""
    df_cap_ln_extr = (
        spark.read.format("jdbc")
        .option("url", facets_jdbc_url)
        .options(**facets_jdbc_props)
        .option("query", query_cap_ln_extr)
        .load()
    )

    # -- Scenario a Hashed File: cob_non_zero => direct link from df_cob_extr
    #    Key columns: CLCL_ID(char(12)), CDML_SEQ_NO
    #    Output columns: CLCL_ID, CDML_SEQ_NO, CDCB_COB_AMT
    #    We do dedup, fill CDCB_COB_AMT with Null (unused).
    df_cob_amt_lkup_tmp = dedup_sort(
        df_cob_extr,
        ["CLCL_ID","CDML_SEQ_NO"],
        [("CLCL_ID","A"),("CDML_SEQ_NO","A")]
    )
    df_cob_amt_lkup = (
        df_cob_amt_lkup_tmp
        .withColumn("CLCL_ID", F.rpad(F.col("CLCL_ID"),12," "))
        .select(
            "CLCL_ID",
            "CDML_SEQ_NO",
            F.lit(None).alias("CDCB_COB_AMT")
        )
    )

    # 3) Read from Sequential_File_0 => "SourceFile" => produce 7 pinned outputs => collect them
    #    The collector merges them in Round-Robin. We'll approximate by union all.
    #    Output has columns => CLCL_ID(char(12)), CDML_SEQ_NO, DSALW_TYP(char(2)), DSALW_AMT, DSALW_EXCD(char(3)),
    #                          EXCD_RESP_CD, CDML_DISALL_AMT, BYPS_IN(char(1))
    seq_file_0_path = f"{adls_path}/SourceFile"
    schema_seq_file_0 = StructType([
        StructField("CLCL_ID", StringType()),
        StructField("CDML_SEQ_NO", StringType()),
        StructField("DSALW_TYP", StringType()),
        StructField("DSALW_AMT", DecimalType(18,2)),
        StructField("DSALW_EXCD", StringType()),
        StructField("EXCD_RESP_CD", StringType()),
        StructField("CDML_DISALL_AMT", DecimalType(18,2)),
        StructField("BYPS_IN", StringType())
    ])
    df_Sequential_File_0 = (
        spark.read.format("csv")
        .option("header", "false")
        .option("quote", "\"")
        .schema(schema_seq_file_0)
        .load(seq_file_0_path)
    )
    # For each link, no additional constraints given, so replicate.
    df_tu_cap   = df_Sequential_File_0
    df_a_cap    = df_Sequential_File_0
    df_x_cap    = df_Sequential_File_0
    df_cddd_cap = df_Sequential_File_0
    df_cdmd_cap = df_Sequential_File_0
    df_cdml_cap = df_Sequential_File_0
    df_cddl_cap = df_Sequential_File_0

    df_disalw_out = (
        df_tu_cap
        .union(df_a_cap)
        .union(df_x_cap)
        .union(df_cddd_cap)
        .union(df_cdmd_cap)
        .union(df_cdml_cap)
        .union(df_cddl_cap)
    )

    # Ensure char columns are padded
    df_disalw_out = (
        df_disalw_out
        .withColumn("CLCL_ID", F.rpad(F.col("CLCL_ID"), 12, " "))
        .withColumn("DSALW_TYP", F.rpad(F.col("DSALW_TYP"), 2, " "))
        .withColumn("DSALW_EXCD", F.rpad(F.col("DSALW_EXCD"), 3, " "))
        .withColumn("BYPS_IN", F.rpad(F.col("BYPS_IN"), 1, " "))
    )

    # 4) sum_by_clm_ln => aggregator => sumofdsalw
    df_sumofdsalw = (
        df_disalw_out
        .groupBy("CLCL_ID","CDML_SEQ_NO")
        .agg(F.sum("DSALW_AMT").alias("DSALW_AMT"))
    )
    # Now pad CLCL_ID again
    df_sumofdsalw = df_sumofdsalw.withColumn("CLCL_ID", F.rpad(F.col("CLCL_ID"),12," "))

    # 5) dsalw_sum => hashed => scenario a => "dsalw_sum_lkup"
    df_dsalw_sum_lkup_tmp = dedup_sort(
        df_sumofdsalw,
        ["CLCL_ID","CDML_SEQ_NO"],
        [("CLCL_ID","A"),("CDML_SEQ_NO","A")]
    )
    df_dsalw_sum_lkup = df_dsalw_sum_lkup_tmp.select("CLCL_ID","CDML_SEQ_NO","DSALW_AMT")

    # 6) create_cap_dsalw => 3 input pins => transform
    #    stream: cap_ln_extr, references: cob_amt_lkup, dsalw_sum_lkup
    #    We'll do left joins in sequence: first with cob_amt_lkup, then with dsalw_sum_lkup
    df_enriched = (
        df_cap_ln_extr.alias("cap_ln_extr")
        .join(
            df_cob_amt_lkup.alias("cob_amt_lkup"),
            [
                F.col("cap_ln_extr.CLCL_ID")==F.col("cob_amt_lkup.CLCL_ID"),
                F.col("cap_ln_extr.CDML_SEQ_NO")==F.col("cob_amt_lkup.CDML_SEQ_NO")
            ],
            "left"
        )
        .join(
            df_dsalw_sum_lkup.alias("dsalw_sum_lkup"),
            [
                F.col("cap_ln_extr.CLCL_ID")==F.col("dsalw_sum_lkup.CLCL_ID"),
                F.col("cap_ln_extr.CDML_SEQ_NO")==F.col("dsalw_sum_lkup.CDML_SEQ_NO")
            ],
            "left"
        )
    )

    # Define stage variables in a chain of withColumn calls:
    # svCapDsalw = If cap_ln_extr.CDML_CONSIDER_CHG = cap_ln_extr.DISALL_AMT then 'N' else 'Y'
    df_enriched = df_enriched.withColumn(
        "svCapDsalw",
        F.when(
            F.col("cap_ln_extr.CDML_CONSIDER_CHG") == F.col("cap_ln_extr.DISALL_AMT"),
            F.lit("N")
        ).otherwise(F.lit("Y"))
    )
    # svDsalwSum = cap_ln_extr.CDML_CONSIDER_CHG - cap_ln_extr.CDML_COPAY_AMT - cap_ln_extr.CDML_COINS_AMT - cap_ln_extr.CDML_DED_AMT
    df_enriched = df_enriched.withColumn(
        "svDsalwSum",
        F.col("cap_ln_extr.CDML_CONSIDER_CHG")
        - F.col("cap_ln_extr.CDML_COPAY_AMT")
        - F.col("cap_ln_extr.CDML_COINS_AMT")
        - F.col("cap_ln_extr.CDML_DED_AMT")
    )
    # svCobIn = If IsNull(cob_amt_lkup.CLCL_ID) then 'N'
    #           else if cap_ln_extr.CLCL_PAID_DT[1,10] < '2003-09-30' then 'N'
    #           else 'Y'
    df_enriched = df_enriched.withColumn(
        "svCobIn",
        F.when(
            F.col("cob_amt_lkup.CLCL_ID").isNull(),
            F.lit("N")
        ).otherwise(
            F.when(
                F.substring(F.col("cap_ln_extr.CLCL_PAID_DT"),1,10) < F.lit("2003-09-30"),
                F.lit("N")
            ).otherwise(F.lit("Y"))
        )
    )
    # svTotal = If svCobIn = 'Y' then cap_ln_extr.CDML_CONSIDER_CHG else svDsalwSum
    df_enriched = df_enriched.withColumn(
        "svTotal",
        F.when(
            F.col("svCobIn") == F.lit("Y"),
            F.col("cap_ln_extr.CDML_CONSIDER_CHG")
        ).otherwise(
            F.col("svDsalwSum")
        )
    )
    # svDiff = If IsNull(dsalw_sum_lkup.CLCL_ID) then svTotal else svTotal - dsalw_sum_lkup.DSALW_AMT
    df_enriched = df_enriched.withColumn(
        "svDiff",
        F.when(
            F.col("dsalw_sum_lkup.CLCL_ID").isNull(),
            F.col("svTotal")
        ).otherwise(
            F.col("svTotal") - F.col("dsalw_sum_lkup.DSALW_AMT")
        )
    )

    # Output constraint: svDiff <> 0 and svCapDsalw = 'Y'
    df_cap_dsalw = df_enriched.filter(
        (F.col("svDiff") != 0) &
        (F.col("svCapDsalw") == F.lit("Y"))
    )

    # Output columns (cap_dsalw) => hashed => scenario a => "dsalw_cap"
    df_cap_dsalw_out = df_cap_dsalw.select(
        F.rpad(F.col("cap_ln_extr.CLCL_ID"),12," ").alias("CLCL_ID"),
        "cap_ln_extr.CDML_SEQ_NO",
        F.rpad(F.lit("CAP"),2," ").alias("DSALW_TYP"),
        F.col("svDiff").alias("DSALW_AMT"),
        F.rpad(F.lit("CAP"),3," ").alias("DSALW_EXCD"),
        F.lit("X").alias("EXCD_RESP_CD"),
        F.col("cap_ln_extr.DISALL_AMT").alias("CDML_DISALL_AMT"),
        F.rpad(F.lit("X"),1," ").alias("BYPS_IN")
    )

    df_dsalw_cap_tmp = dedup_sort(
        df_cap_dsalw_out,
        ["CLCL_ID","CDML_SEQ_NO","DSALW_TYP"],
        [("CLCL_ID","A"),("CDML_SEQ_NO","A"),("DSALW_TYP","A")]
    )
    df_dsalw_cap = df_dsalw_cap_tmp.select(
        "CLCL_ID",
        "CDML_SEQ_NO",
        "DSALW_TYP",
        "DSALW_AMT",
        "DSALW_EXCD",
        "EXCD_RESP_CD",
        "CDML_DISALL_AMT",
        "BYPS_IN"
    )

    # Finally write dsalw_cap to Sequential_File_1 => "OutputFiles"
    out_seq_file_1_path = f"{adls_path}/OutputFiles"
    write_files(
        df_dsalw_cap,
        out_seq_file_1_path,
        delimiter=",",
        mode="overwrite",
        is_pqruet=False,
        header=False,
        quote="\"",
        nullValue=None
    )

    # No return DataFrame links (the design ends in a file), so return None.
    return None