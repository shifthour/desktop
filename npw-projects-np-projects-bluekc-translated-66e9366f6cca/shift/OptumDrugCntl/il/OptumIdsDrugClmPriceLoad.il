model:
  config:
    origin: datastage
  jobs:
    - name: OptumIdsDrugClmPriceLoad
      origin: datastage
      type: dataflow
      folder: "claim/DrugClmPrice"
      runAfterSubrOnJobSuccess: enabled
      description: "This Job Loads Data from Sequential File created in Extract Job and loads into IDS DRUG_CLM_PRICE table And this job is called in OptumIdsDrugClmPriceLoadSeq sequence job"
      fullDescription: "MODIFICATIONS:\n                                                                                                                                                                                                                                                                              \x09\x09\x09DATASTAGE\x09CODE\x09\x09DATE\nDEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                                                                                    \x09\x09ENVIRONMENT\x09REVIEWER\x09REVIEW\n------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\nSri Nannapaneni        10-22-2019        Spread price              This Job Loads Data from Sequential File created in fkey Job and loads into IDS DRUG_CLM_PRICE table    IntegrateDev2\x09Kalyan Neelam\x092019-11-26\nRekha Radhakrishna  2020-02-05                                             Mapped new fields SELL_INCNTV_FEE_AMT,BUY_INCNTV_FEE_AMT                                                                       IntegrateDev2\x09Kalyan Neelam\x092020-02-06\n                                                                                                              ,SELL_OTHR_PAYOR_AMT,SELL_OTHR_AMT and SPREAD_INCNTV_ FEE_AMT\nGeetanjali Rajendran    2021-06-03   PBM PhaseII              Mapped new fields FRMLRY_SK, DRUG_TYP_CD_SK and TIER_ID                                                                                IntegrateDev2\x09ABhiram Dasarathy\x092021-06-23\n                  \nArpitha V                      2023-11-07     US 600305               Mapped new field PLN_DRUG_STTUS_CD_SK  to DRUG_CLM_PRICE table                                                                  IntegrateDevB              Jeyaprasanna           2024-01-01\n\nAshok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                                                             IntegrateDev2              Jeyaprasanna           2024-03-14"
      parameters:
        - name: FilePath
          type: string
          description: "EDW File Path"
          defaultType: project
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSDB2ArraySize
          type: string
          description: "IDS DB2 Array Size for Load"
          defaultType: project
        - name: IDSDB2RecordCount
          type: string
          description: "IDS DB2 Record Count for Load"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
      afterSubr:
        subrType: "DSU.UnixSH"
        subr: "#$FilePath#/scripts/db2_stats.ksh #$IDSInstance# #$IDSDB# #$IDSOwner# DRUG_CLM_PRICE #$IDSAcct# #$IDSPW# #$FilePath# DRUG_CLM_PRICE.dat Y Y"
      steps:
        - name: seq_DRUG_CLM_PRICE
          type: fileReader
          urls: 
            - "#$FilePath#/load/DRUG_CLM_PRICE.dat"
          rejectMode: continue
          format: 
            header: false
            quotes: ""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: FRMLRY_PROTOCOL_IN
              type: char
              width: 1
              nullable: N
            - name: RECON_IN
              type: char
              width: 1
            - name: SPEC_PGM_IN
              type: char
              width: 1
              nullable: N
            - name: FINL_PLN_EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: ORIG_PD_TRANS_SUBMT_DT_SK
              type: char
              width: 10
              nullable: N
            - name: PRORTD_DISPNS_QTY
              type: decimal
              width: 11
              precision: 3
              nullable: N
            - name: SUBMT_DISPNS_METRIC_QTY
              type: int
              width: 4
              nullable: N
            - name: AVG_WHLSL_PRICE_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
            - name: WHLSL_ACQSTN_CST_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
            - name: BUY_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BUY_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BUY_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
            - name: BUY_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BUY_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BUY_TOT_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
            - name: INVC_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SELL_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
            - name: SELL_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
            - name: SPREAD_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SPREAD_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SPREAD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BUY_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: BUY_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: DRUG_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: FINL_PLN_ID
              type: varchar
              width: 20
              nullable: N
            - name: GNRC_PROD_ID
              type: varchar
              width: 20
              nullable: N
            - name: SELL_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
            - name: SPEC_PGM_ID
              type: varchar
              width: 20
              nullable: N
            - name: BUY_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: SELL_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: SPREAD_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: SELL_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
            - name: SELL_OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
            - name: FRMLRY_SK
              type: int
              width: 4
              nullable: N
            - name: TIER_ID
              type: varchar
              width: 2
            - name: DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PLN_DRUG_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DRUG_PLN_TYP_ID
              type: varchar
              width: 20
          keys:
            - DRUG_CLM_SK
        - name: Db2Load_DRUG_CLM_PRICE
          type: dbWriter
          originalDb: db2
          table: "#$IDSOwner#.DRUG_CLM_PRICE"
          tableAction: append
          continueOnRowError: false
          writeMode: "insert then update"
          source: cpy_forBuffer
        - name: cpy_forBuffer
          type: map
          source: seq_DRUG_CLM_PRICE
          alias: Lnk_DRUG_CLM_PRICE_In
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_CST_SRC_CD_SK"
            - name: GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.GNRC_OVRD_CD_SK"
            - name: PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.PDX_NTWK_QLFR_CD_SK"
            - name: FRMLRY_PROTOCOL_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.FRMLRY_PROTOCOL_IN"
            - name: RECON_IN
              type: char
              width: 1
              map: "Lnk_DRUG_CLM_PRICE_In.RECON_IN"
            - name: SPEC_PGM_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SPEC_PGM_IN"
            - name: FINL_PLN_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.FINL_PLN_EFF_DT_SK"
            - name: ORIG_PD_TRANS_SUBMT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.ORIG_PD_TRANS_SUBMT_DT_SK"
            - name: PRORTD_DISPNS_QTY
              type: decimal
              width: 11
              precision: 3
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.PRORTD_DISPNS_QTY"
            - name: SUBMT_DISPNS_METRIC_QTY
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SUBMT_DISPNS_METRIC_QTY"
            - name: AVG_WHLSL_PRICE_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.AVG_WHLSL_PRICE_UNIT_CST_AMT"
            - name: WHLSL_ACQSTN_CST_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.WHLSL_ACQSTN_CST_UNIT_CST_AMT"
            - name: BUY_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_DISPNS_FEE_AMT"
            - name: BUY_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_INGR_CST_AMT"
            - name: BUY_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_RATE_PCT"
            - name: BUY_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_SLS_TAX_AMT"
            - name: BUY_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_TOT_DUE_AMT"
            - name: BUY_TOT_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_TOT_OTHR_AMT"
            - name: CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.CST_TYP_UNIT_CST_AMT"
            - name: INVC_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.INVC_TOT_DUE_AMT"
            - name: SELL_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SELL_CST_TYP_UNIT_CST_AMT"
            - name: SELL_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SELL_RATE_PCT"
            - name: SPREAD_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SPREAD_DISPNS_FEE_AMT"
            - name: SPREAD_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SPREAD_INGR_CST_AMT"
            - name: SPREAD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SPREAD_SLS_TAX_AMT"
            - name: BUY_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_CST_TYP_ID"
            - name: BUY_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_PRICE_TYP_ID"
            - name: CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.CST_TYP_ID"
            - name: DRUG_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.DRUG_TYP_ID"
            - name: FINL_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.FINL_PLN_ID"
            - name: GNRC_PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.GNRC_PROD_ID"
            - name: SELL_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SELL_PRICE_TYP_ID"
            - name: SPEC_PGM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.SPEC_PGM_ID"
            - name: BUY_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_DRUG_CLM_PRICE_In.BUY_INCNTV_FEE_AMT"
            - name: SELL_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_DRUG_CLM_PRICE_In.SELL_INCNTV_FEE_AMT"
            - name: SPREAD_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_DRUG_CLM_PRICE_In.SPREAD_INCNTV_FEE_AMT"
            - name: SELL_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_DRUG_CLM_PRICE_In.SELL_OTHR_AMT"
            - name: SELL_OTHR_PAYER_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_DRUG_CLM_PRICE_In.SELL_OTHR_PAYOR_AMT"
            - name: FRMLRY_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.FRMLRY_SK"
            - name: TIER_ID
              type: varchar
              width: 2
              map: "Lnk_DRUG_CLM_PRICE_In.TIER_ID"
            - name: DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.DRUG_TYP_CD_SK"
            - name: PLN_DRUG_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_PRICE_In.PLN_DRUG_STTUS_CD_SK"
            - name: DRUG_PLN_TYP_ID
              type: varchar
              width: 20
              map: "Lnk_DRUG_CLM_PRICE_In.DRUG_PLN_TYP_ID"
          keys:
            - DRUG_CLM_SK


