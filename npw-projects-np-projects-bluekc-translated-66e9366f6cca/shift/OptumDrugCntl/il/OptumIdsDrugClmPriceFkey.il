model:
  config:
    origin: datastage
  jobs:
    - name: OptumIdsDrugClmPriceFkey
      origin: datastage
      type: dataflow
      folder: "claim/DrugClmPrice"
      description: "Perform Foreign Key and code mapping Lookups to populate DRUG_CLM_PRICE File And this job is called in OptumIdsDrugClmPriceLoadSeq sequence job"
      fullDescription: "MODIFICATIONS:\n                                                                                                                                                                                                                                                                                            DATASTAGE               CODE                           DATE\nDEVELOPER             DATE                PROJECT                                   DESCRIPTION                                                                                                                                                    ENVIRONMENT          REVIEWER                 REVIEW\n------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\nSri Nannapaneni        10-22-2019        Spread price                             Perform Foreign Key and code mapping Lookups to populate DRUG_CLM_PRICE File                                      IntegrateDev2             Sharon Andrew              11/11/2019\nSri Nannapaneni         11-21-2-19                                                         updated based on new mapping                                                                                                                                                              Kalyan Neelam             2019-11-26\nRekha Radhakrishna  2020-01-30                                                       Mapped new fields SELL_INCNTV_FEE_AMT,BUY_INCNTV_FEE_AMT                                                           IntegrateDev2            Kalyan Neelam             2020-02-06\n                                                                                                            ,SELL_OTHR_PAYOR_AMT,SELL_OTHR_AMT and SPREAD_INCNTV_ FEE_AMT\n\nRekha Radhakrishna    2020-02-10    6131- PBM Replacement         Mapped fields  CLT2PSTAX and CLT2FSTAX to calculate BUY_SLS_TAX_AMT                                               IntegrateDev2            Kalyan Neelam             2020-02-10     \n                                                                                                             and changed SPREAD_SLS_TAX_AMT calculation\n\nVelmani Kondappan     2020-04-02   6131- PBM Replacement           Removed the errror warnings coming from Lkup_Fkey lookup                                                                                                                  Kalyan Neelam            2020-04-09\n\nRekha Radhakrishna   2020-10-14     6131 - PBM Replacement         parameterized input source file                                                                                                                         IntegrateDev2                   Sravya Gorla\x092020-12-09\nGeetanjali Rajendran    2021-06-03        PBM PhaseII                              Perform code mapping lookup to populate DRUG_TYPE_CD_SK and FRMLRY lookup          IntegrateDev2\x09\x09Abhiram Dasarathy\x092021-06-23\n                                                                                                                                   to populate FRMLRY_SK \n\nArpitha V                      2023-11-07       US 600305                              Perform code mapping lookup to populate PLN_DRUG_STTUS_CD_SK                                                        IntegrateDevB               Jeyaprasanna           2024-01-03\n\nAshok kumar B            2024-02-01        US 608682                        Added  PLAN_TYPE  to the DrugClmPrice Landing file                                                                                        IntegrateDev2                 Jeyaprasanna            2024-03-14"
      parameters:
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: UWSOwner
          type: string
          description: "UWS Table Owner"
          defaultType: project
        - name: UWSDB
          type: string
          description: "UWS Database"
          defaultType: project
        - name: UWSAcct
          type: string
          description: "UWS Account"
        - name: UWSPW
          type: encrypted
          description: "UWS Password"
        - name: SrcSysCd
          type: string
          description: SrcSysCd
        - name: RunID
          type: string
          description: RunID
        - name: IDSRunCycle
          type: string
          description: IDSRunCycle
        - name: CurrentDate
          type: string
          description: CurrentDate
      steps:
        - name: Lkup_DrugClm
          type: lookup
          option:
            joinType: inner
          sources: 
            - name: xfm_DrugClmPriceSrc
              alias: xfm_src_results
              joinFilter: ""
              joinExpression: []
            - name: db2_ds_DRUG_CLM
              alias: Lnk_DRUG_CLM_SK
              joinFilter: ""
              joinExpression: []
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_SK.DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_SK.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "xfm_src_results.CLM_ID"
            - name: RXCLMNBR
              type: varchar
              width: 15
              map: "xfm_src_results.RXCLMNBR"
            - name: CLMSEQNBR
              type: varchar
              width: 3
              map: "xfm_src_results.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "xfm_src_results.CLAIMSTS"
            - name: ORGPDSBMDT
              type: varchar
              width: 10
              map: "xfm_src_results.ORGPDSBMDT"
            - name: METRICQTY
              type: varchar
              width: 6
              map: "xfm_src_results.METRICQTY"
            - name: GPINUMBER
              type: varchar
              width: 20
              map: "xfm_src_results.GPINUMBER"
            - name: GENINDOVER
              type: char
              width: 1
              map: "xfm_src_results.GENINDOVER"
            - name: CPQSPCPRG
              type: varchar
              width: 10
              map: "xfm_src_results.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "xfm_src_results.CPQSPCPGIN"
            - name: FNLPLANCDE
              type: varchar
              width: 10
              map: "xfm_src_results.FNLPLANCDE"
            - name: FNLPLANDTE
              type: varchar
              width: 10
              map: "xfm_src_results.FNLPLANDTE"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "xfm_src_results.FORMLRFLAG"
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "xfm_src_results.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "xfm_src_results.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
              map: "xfm_src_results.CTYPEUCOST"
            - name: PROQTY
              type: varchar
              width: 12
              map: "xfm_src_results.PROQTY"
            - name: CLTCOSTTYP
              type: varchar
              width: 10
              map: "xfm_src_results.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: varchar
              width: 10
              map: "xfm_src_results.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
              map: "xfm_src_results.CLTRATE"
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
              map: "xfm_src_results.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLT2DISPFEE"
            - name: CLT2COSTSRC
              type: varchar
              width: 1
              map: "xfm_src_results.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: varchar
              width: 10
              map: "xfm_src_results.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: varchar
              width: 10
              map: "xfm_src_results.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 7
              precision: 2
              map: "xfm_src_results.CLT2RATE"
            - name: RXNETWRKQL
              type: varchar
              width: 1
              map: "xfm_src_results.RXNETWRKQL"
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLT2SLSTAX"
            - name: CLT2PSTAX
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLT2FSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLT2DUEAMT"
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLT2OTHAMT"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_SK.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_CLM_SK.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_SK.DISPNS_FEE_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_SK.INGR_CST_CHRGD_AMT"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              map: "Lnk_DRUG_CLM_SK.GNRC_DRUG_IN"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_DRUG_CLM_SK.SLS_TAX_AMT"
            - name: CLTINCENTV
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLTINCENTV"
            - name: CALINCENTV2
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CALINCENTV2"
            - name: CLTOTHPAYA
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLTOTHPAYA"
            - name: CLTOTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "xfm_src_results.CLTOTHAMT"
            - name: CLNTDEF2
              type: varchar
              width: 10
              map: "xfm_src_results.CLNTDEF2"
            - name: TIER_ID
              type: varchar
              width: 2
              map: "xfm_src_results.TIER_ID"
            - name: DRUG_TYP_CD
              type: varchar
              width: 1
              map: "xfm_src_results.DRUG_TYP_CD"
            - name: PLN_DRUG_STTUS_CD
              type: char
              width: 1
              map: "xfm_src_results.PLN_DRUG_STTUS_CD"
            - name: PLANTYPE
              type: char
              width: 8
              map: "xfm_src_results.PLANTYPE"
        - name: Lkp_Frmly
          type: lookup
          option:
            joinType: left
          sources: 
            - name: Lkup_Fkey
              alias: Lnk_IdsDrugClmPriceFkey_Lkp_Out1
              joinFilter: ""
              joinExpression: []
            - name: db2_ds_FRMLRY
              alias: Lnk_FRMLRY_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_FRMLRY_SK.FRMLRY_ID
                toExpr: Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLNTDEF2
                caseInSensitive: false
          joinCondition: 
            - expression: Lnk_FRMLRY_SK.FRMLRY_ID = Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLNTDEF2
              caseInSensitive: false
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLM_ID"
            - name: BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.BUY_CST_SRC_CD_SK"
            - name: BUY_CST_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.BUY_CST_TYP_CD_SK"
            - name: BUY_PRICE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.BUY_PRICE_TYP_CD_SK"
            - name: CST_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CST_TYP_CD_SK"
            - name: GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.GNRC_OVRD_CD_SK"
            - name: PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.PDX_NTWK_QLFR_CD_SK"
            - name: RXCLMNBR
              type: varchar
              width: 15
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.RXCLMNBR"
            - name: CLMSEQNBR
              type: varchar
              width: 3
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLAIMSTS"
            - name: ORGPDSBMDT
              type: varchar
              width: 10
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.ORGPDSBMDT"
            - name: METRICQTY
              type: varchar
              width: 6
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.METRICQTY"
            - name: GPINUMBER
              type: varchar
              width: 20
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.GPINUMBER"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.GENINDOVER"
            - name: CPQSPCPRG
              type: varchar
              width: 10
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CPQSPCPGIN"
            - name: FNLPLANCDE
              type: varchar
              width: 10
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.FNLPLANCDE"
            - name: FNLPLANDTE
              type: varchar
              width: 10
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.FNLPLANDTE"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.FORMLRFLAG"
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CTYPEUCOST"
            - name: PROQTY
              type: varchar
              width: 12
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.PROQTY"
            - name: CLTCOSTTYP
              type: varchar
              width: 10
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: varchar
              width: 10
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTRATE"
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2DISPFEE"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: varchar
              width: 10
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: varchar
              width: 10
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 7
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2RATE"
            - name: RXNETWRKQL
              type: varchar
              width: 1
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.RXNETWRKQL"
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2SLSTAX"
            - name: CLT2PSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2FSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2DUEAMT"
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLT2OTHAMT"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.DISPNS_FEE_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.INGR_CST_CHRGD_AMT"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.GNRC_DRUG_IN"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.SLS_TAX_AMT"
            - name: CLTINCENTV
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTINCENTV"
            - name: CALINCENTV2
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CALINCENTV2"
            - name: CLTOTHPAYA
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTOTHPAYA"
            - name: CLTOTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.CLTOTHAMT"
            - name: FRMLRY_SK
              type: int
              width: 4
              map: "Lnk_FRMLRY_SK.FRMLRY_SK"
            - name: TIER_ID
              type: varchar
              width: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.TIER_ID"
            - name: DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.DRUG_TYP_CD_SK"
            - name: PLN_DRUG_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.PLN_DRUG_STTUS_CD_SK"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out1.PLANTYPE"
          keys:
            - CLM_ID
        - name: Lkp_SrcDomain
          type: lookup
          option:
            joinType: left
          sources: 
            - name: xfm_Src_Domain_Trnsltn
              alias: Lnk_Src_Domain_Trnsltn_out
              joinFilter: ""
              joinExpression: []
            - name: fltr_Data.V493S0P10
              alias: Lnk_PLN_DRUG_STTUS_CD
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_PLN_DRUG_STTUS_CD.SRC_CD
                toExpr: Lnk_Src_Domain_Trnsltn_out.TRGT_DOMAIN_TX
                caseInSensitive: false
          joinCondition: 
            - expression: Lnk_PLN_DRUG_STTUS_CD.SRC_CD = Lnk_Src_Domain_Trnsltn_out.TRGT_DOMAIN_TX
              caseInSensitive: false
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_PLN_DRUG_STTUS_CD.CD_MPPNG_SK"
            - name: SRC_DOMAIN_TX
              type: varchar
              width: 40
              nullable: N
              map: "Lnk_Src_Domain_Trnsltn_out.SRC_DOMAIN_TX"
          keys:
            - SRC_DOMAIN_TX
        - name: Lkup_Fkey
          type: lookup
          option:
            joinType: left
          sources: 
            - name: Lkup_DrugClm
              alias: Lnk_src_lkp
              joinFilter: ""
              joinExpression: []
            - name: fltr_Data.V493S0P2
              alias: Lnk_SRC_SYS_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_SRC_SYS_CD_SK.CD_MPPNG_SK
                toExpr: Lnk_src_lkp.SRC_SYS_CD_SK
                caseInSensitive: false
            - name: fltr_Data.V493S0P3
              alias: Lnk_BUY_CST_SRC_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_BUY_CST_SRC_CD_SK.SRC_CD
                toExpr: Lnk_src_lkp.CLT2COSTSRC
                caseInSensitive: false
            - name: fltr_Data.V493S0P4
              alias: Lnk_BUY_CST_TYP_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_BUY_CST_TYP_CD_SK.SRC_CD
                toExpr: Lnk_src_lkp.CLT2COSTTYP
                caseInSensitive: false
            - name: fltr_Data.V493S0P5
              alias: Lnk_BUY_PRICE_TYP_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_BUY_PRICE_TYP_CD_SK.SRC_CD
                toExpr: Lnk_src_lkp.CLT2PRCTYPE
                caseInSensitive: false
            - name: fltr_Data.V493S0P6
              alias: Lnk_CST_TYP_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_CST_TYP_CD_SK.SRC_CD
                toExpr: Lnk_src_lkp.CLTCOSTTYP
                caseInSensitive: false
            - name: fltr_Data.V493S0P7
              alias: Lnk_GNRC_OVRD_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_GNRC_OVRD_CD_SK.SRC_CD
                toExpr: Lnk_src_lkp.GENINDOVER
                caseInSensitive: false
            - name: fltr_Data.V493S0P8
              alias: Lnk_PDX_NTWK_QLFR_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_PDX_NTWK_QLFR_CD_SK.SRC_CD
                toExpr: Lnk_src_lkp.RXNETWRKQL
                caseInSensitive: false
            - name: fltr_Data.V493S0P9
              alias: Lnk_DRUG_TYP_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_DRUG_TYP_CD_SK.SRC_CD
                toExpr: Lnk_src_lkp.DRUG_TYP_CD
                caseInSensitive: false
            - name: Lkp_SrcDomain
              alias: Lnk_PLN_DRUG_STTUS_CD_SK
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_PLN_DRUG_STTUS_CD_SK.SRC_DOMAIN_TX
                toExpr: Lnk_src_lkp.PLN_DRUG_STTUS_CD
                caseInSensitive: false
            # mutiple reference detected
          joinCondition: 
            - expression: Lnk_SRC_SYS_CD_SK.CD_MPPNG_SK = Lnk_src_lkp.SRC_SYS_CD_SK
              caseInSensitive: false
            - expression: Lnk_BUY_CST_SRC_CD_SK.SRC_CD = Lnk_src_lkp.CLT2COSTSRC
              caseInSensitive: false
            - expression: Lnk_BUY_CST_TYP_CD_SK.SRC_CD = Lnk_src_lkp.CLT2COSTTYP
              caseInSensitive: false
            - expression: Lnk_BUY_PRICE_TYP_CD_SK.SRC_CD = Lnk_src_lkp.CLT2PRCTYPE
              caseInSensitive: false
            - expression: Lnk_CST_TYP_CD_SK.SRC_CD = Lnk_src_lkp.CLTCOSTTYP
              caseInSensitive: false
            - expression: Lnk_GNRC_OVRD_CD_SK.SRC_CD = Lnk_src_lkp.GENINDOVER
              caseInSensitive: false
            - expression: Lnk_PDX_NTWK_QLFR_CD_SK.SRC_CD = Lnk_src_lkp.RXNETWRKQL
              caseInSensitive: false
            - expression: Lnk_DRUG_TYP_CD_SK.SRC_CD = Lnk_src_lkp.DRUG_TYP_CD
              caseInSensitive: false
            - expression: Lnk_PLN_DRUG_STTUS_CD_SK.SRC_DOMAIN_TX = Lnk_src_lkp.PLN_DRUG_STTUS_CD
              caseInSensitive: false
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_src_lkp.DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_src_lkp.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_src_lkp.CLM_ID"
            - name: BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_BUY_CST_SRC_CD_SK.CD_MPPNG_SK"
            - name: BUY_CST_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_BUY_CST_TYP_CD_SK.CD_MPPNG_SK"
            - name: BUY_PRICE_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_BUY_PRICE_TYP_CD_SK.CD_MPPNG_SK"
            - name: CST_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_CST_TYP_CD_SK.CD_MPPNG_SK"
            - name: GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_GNRC_OVRD_CD_SK.CD_MPPNG_SK"
            - name: PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_PDX_NTWK_QLFR_CD_SK.CD_MPPNG_SK"
            - name: RXCLMNBR
              type: varchar
              width: 15
              map: "Lnk_src_lkp.RXCLMNBR"
            - name: CLMSEQNBR
              type: varchar
              width: 3
              map: "Lnk_src_lkp.CLMSEQNBR"
            - name: CLAIMSTS
              type: char
              width: 1
              map: "Lnk_src_lkp.CLAIMSTS"
            - name: ORGPDSBMDT
              type: varchar
              width: 10
              map: "Lnk_src_lkp.ORGPDSBMDT"
            - name: METRICQTY
              type: varchar
              width: 6
              map: "Lnk_src_lkp.METRICQTY"
            - name: GPINUMBER
              type: varchar
              width: 20
              map: "Lnk_src_lkp.GPINUMBER"
            - name: GENINDOVER
              type: char
              width: 1
              map: "Lnk_src_lkp.GENINDOVER"
            - name: CPQSPCPRG
              type: varchar
              width: 10
              map: "Lnk_src_lkp.CPQSPCPRG"
            - name: CPQSPCPGIN
              type: char
              width: 1
              map: "Lnk_src_lkp.CPQSPCPGIN"
            - name: FNLPLANCDE
              type: varchar
              width: 10
              map: "Lnk_src_lkp.FNLPLANCDE"
            - name: FNLPLANDTE
              type: varchar
              width: 10
              map: "Lnk_src_lkp.FNLPLANDTE"
            - name: FORMLRFLAG
              type: char
              width: 1
              map: "Lnk_src_lkp.FORMLRFLAG"
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Lnk_src_lkp.AWPUNITCST"
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "Lnk_src_lkp.WACUNITCST"
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
              map: "Lnk_src_lkp.CTYPEUCOST"
            - name: PROQTY
              type: varchar
              width: 12
              map: "Lnk_src_lkp.PROQTY"
            - name: CLTCOSTTYP
              type: varchar
              width: 10
              map: "Lnk_src_lkp.CLTCOSTTYP"
            - name: CLTPRCTYPE
              type: varchar
              width: 10
              map: "Lnk_src_lkp.CLTPRCTYPE"
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
              map: "Lnk_src_lkp.CLTRATE"
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
              map: "Lnk_src_lkp.CLTTYPUCST"
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLT2INGRCST"
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLT2DISPFEE"
            - name: CLT2COSTSRC
              type: char
              width: 1
              map: "Lnk_src_lkp.CLT2COSTSRC"
            - name: CLT2COSTTYP
              type: varchar
              width: 10
              map: "Lnk_src_lkp.CLT2COSTTYP"
            - name: CLT2PRCTYPE
              type: varchar
              width: 10
              map: "Lnk_src_lkp.CLT2PRCTYPE"
            - name: CLT2RATE
              type: decimal
              width: 7
              precision: 2
              map: "Lnk_src_lkp.CLT2RATE"
            - name: RXNETWRKQL
              type: varchar
              width: 1
              map: "Lnk_src_lkp.RXNETWRKQL"
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLT2SLSTAX"
            - name: CLT2PSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLT2FSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLT2DUEAMT"
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLT2OTHAMT"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_src_lkp.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_src_lkp.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_src_lkp.DISPNS_FEE_AMT"
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_src_lkp.INGR_CST_CHRGD_AMT"
            - name: GNRC_DRUG_IN
              type: char
              width: 1
              map: "Lnk_src_lkp.GNRC_DRUG_IN"
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_src_lkp.SLS_TAX_AMT"
            - name: CLTINCENTV
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLTINCENTV"
            - name: CALINCENTV2
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CALINCENTV2"
            - name: CLTOTHPAYA
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLTOTHPAYA"
            - name: CLTOTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_src_lkp.CLTOTHAMT"
            - name: CLNTDEF2
              type: varchar
              width: 10
              map: "Lnk_src_lkp.CLNTDEF2"
            - name: TIER_ID
              type: varchar
              width: 2
              map: "Lnk_src_lkp.TIER_ID"
            - name: DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_DRUG_TYP_CD_SK.CD_MPPNG_SK"
            - name: PLN_DRUG_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_PLN_DRUG_STTUS_CD_SK.CD_MPPNG_SK"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Lnk_src_lkp.PLANTYPE"
          keys:
            - CLNTDEF2
        - name: Seq_W_DRUG_CLM_PRICE
          type: fileReader
          urls: 
            - "#$FilePath#/verified/DrugClmPrice_Land_#SrcSysCd#.dat.#RunID#"
          rejectMode: continue
          format: 
            header: true
            quotes: "\""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
            - name: RXCLMNBR
              type: decimal
              width: 15
            - name: CLMSEQNBR
              type: decimal
              width: 3
            - name: CLAIMSTS
              type: char
              width: 1
            - name: ORGPDSBMDT
              type: decimal
              width: 8
            - name: METRICQTY
              type: decimal
              width: 6
            - name: GPINUMBER
              type: varchar
              width: 14
            - name: GENINDOVER
              type: char
              width: 1
            - name: CPQSPCPRG
              type: char
              width: 10
            - name: CPQSPCPGIN
              type: char
              width: 1
            - name: FNLPLANCDE
              type: char
              width: 10
            - name: FNLPLANDTE
              type: decimal
              width: 7
            - name: FORMLRFLAG
              type: char
              width: 1
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
            - name: PROQTY
              type: decimal
              width: 12
            - name: CLTCOSTTYP
              type: char
              width: 10
            - name: CLTPRCTYPE
              type: char
              width: 10
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
            - name: CLT2COSTSRC
              type: char
              width: 1
            - name: CLT2COSTTYP
              type: char
              width: 10
            - name: CLT2PRCTYPE
              type: char
              width: 10
            - name: CLT2RATE
              type: decimal
              width: 7
              precision: 2
            - name: RXNETWRKQL
              type: varchar
              width: 1
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
            - name: CLT2PSTAX
              type: decimal
              width: 13
              precision: 2
            - name: CLT2FSTAX
              type: decimal
              width: 13
              precision: 2
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
            - name: CLTINCENTV
              type: decimal
              width: 13
              precision: 2
            - name: CALINCENTV2
              type: decimal
              width: 13
              precision: 2
            - name: CLTOTHPAYA
              type: decimal
              width: 13
              precision: 2
            - name: CLTTOTHAMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNTDEF2
              type: varchar
              width: 10
            - name: TIER_ID
              type: varchar
              width: 2
            - name: DRUG_TYP_CD
              type: varchar
              width: 1
            - name: PLN_DRUG_STTUS_CD
              type: char
              width: 1
            - name: PLANTYPE
              type: char
              width: 8
        - name: Seq_Drug_Clm_Price_Fkey
          type: fileWriter
          urls: 
            - "#$FilePath#/load/DRUG_CLM_PRICE.dat"
          source: xfm_CheckLkpResults
          rejectMode: continue
          option: 
            updateMode: overwrite
            writeMode: SingleFile
          format: 
            header: false
            quotes: ""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
        - name: fltr_Data.V493S0P2
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'SOURCE SYSTEM' And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'SOURCE SYSTEM' And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Codes.SRC_SYS_CD"
          keys:
            - CD_MPPNG_SK
        - name: fltr_Data.V493S0P3
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'BUY COST SOURCE' And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'BUY COST SOURCE' And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: fltr_Data.V493S0P4
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'DRUG COST TYPE' And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'DRUG COST TYPE' And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: fltr_Data.V493S0P5
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'DRUG PRICE TYPE' And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'DRUG PRICE TYPE' And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: fltr_Data.V493S0P6
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'DRUG COST TYPE' And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'DRUG COST TYPE' And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: fltr_Data.V493S0P7
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'DRUG GENERIC OVERRIDE' And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'DRUG GENERIC OVERRIDE' And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: fltr_Data.V493S0P8
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'PHARMACY NETWORK QUALIFIER' And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'PHARMACY NETWORK QUALIFIER'  And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: fltr_Data.V493S0P9
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'DRUG TYPE'  And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'DRUG TYPE' And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
          keys:
            - SRC_CD
        - name: fltr_Data.V493S0P10
          type: filter
          source: db2_CD_MPPNG_SK
          alias: lnk_Codes
          filter: "(SRC_SYS_CD = '#SrcSysCd#' And SRC_DOMAIN_NM = 'PLAN DRUG STATUS'  And SRC_CLCTN_CD = '#SrcSysCd#' And TRGT_DOMAIN_NM = 'PLAN DRUG STATUS' And TRGT_CLCTN_CD = 'IDS')"
          schema: 
            - name: SRC_CD
              type: varchar
              width: 20
              map: "lnk_Codes.SRC_CD"
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Codes.CD_MPPNG_SK"
          keys:
            - SRC_CD
        - name: db2_ds_DRUG_CLM
          type: dbReader
          originalDb: db2
          query: "Select DRUG_CLM_SK, SRC_SYS_CD_SK, CLM_ID as CLM_ID2 , CRT_RUN_CYC_EXCTN_SK, LAST_UPDT_RUN_CYC_EXCTN_SK, DISPNS_FEE_AMT, INGR_CST_CHRGD_AMT,GNRC_DRUG_IN,SLS_TAX_AMT  from #$IDSOwner#.DRUG_CLM Where CLM_ID= ORCHESTRATE.CLM_ID and SRC_SYS_CD_SK = ORCHESTRATE.SRC_SYS_CD_SK"
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID2
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
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INGR_CST_CHRGD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: GNRC_DRUG_IN
              type: char
              width: 1
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
          keys:
            - CLM_ID2
        - name: db2_CD_MPPNG_SK
          type: dbReader
          originalDb: db2
          query: "SELECT DISTINCT\nCD_MPPNG_SK,\nSRC_CD,\nSRC_CD_NM,\nSRC_CLCTN_CD,\nSRC_DRVD_LKUP_VAL,\nSRC_DOMAIN_NM,\nSRC_SYS_CD,\nTRGT_CD,\nTRGT_CD_NM,\nTRGT_CLCTN_CD,\nTRGT_DOMAIN_NM\nFROM #$IDSOwner#.CD_MPPNG;\n"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
            - name: SRC_CLCTN_CD
              type: varchar
              width: 20
              nullable: N
            - name: SRC_DRVD_LKUP_VAL
              type: varchar
              width: 20
            - name: SRC_DOMAIN_NM
              type: varchar
              width: 50
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: TRGT_CLCTN_CD
              type: varchar
              width: 20
            - name: TRGT_DOMAIN_NM
              type: varchar
              width: 50
          keys:
            - CD_MPPNG_SK
        - name: db2_ds_FRMLRY
          type: dbReader
          originalDb: db2
          query: "SELECT DISTINCT\nFRMLRY_ID,\nFRMLRY_SK\nFROM #$IDSOwner#.FRMLRY"
          schema: 
            - name: FRMLRY_SK
              type: int
              width: 4
              nullable: N
            - name: FRMLRY_ID
              type: varchar
              width: 20
          keys:
            - FRMLRY_ID
        - name: xfm_Src_Domain_Trnsltn
          type: map
          source: SRC_DOMAIN_TRNSLTN
          alias: Lnk_Src_Domain_Trnsltn
          schema: 
            - name: SRC_DOMAIN_TX
              type: varchar
              width: 40
              nullable: N
              map: "Lnk_Src_Domain_Trnsltn.SRC_DOMAIN_TX"
            - name: TRGT_DOMAIN_TX
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Src_Domain_Trnsltn.TRGT_DOMAIN_TX"
          keys:
            - SRC_DOMAIN_TX
        - name: xfm_DrugClmPriceSrc
          type: map
          source: Seq_W_DRUG_CLM_PRICE
          alias: Lnk_W_Drug_Clm_Price
          stageVars: 
            - name: StageVar
              type: varchar
              width: 255
              map: "Trim(Lnk_W_Drug_Clm_Price.ORGPDSBMDT, '0', 'L')"
              initValue: ""
            - name: SVClmId
              type: varchar
              width: 255
              map: " If TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLAIMSTS) = 'X' Then TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.RXCLMNBR) : TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLMSEQNBR) : 'R' Else TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.RXCLMNBR) : TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLMSEQNBR)"
              initValue: ""
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_W_Drug_Clm_Price.SRC_SYS_CD"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_W_Drug_Clm_Price.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_W_Drug_Clm_Price.CLM_ID"
            - name: RXCLMNBR
              type: varchar
              width: 15
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.RXCLMNBR)"
            - name: CLMSEQNBR
              type: varchar
              width: 3
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLMSEQNBR)"
            - name: CLAIMSTS
              type: varchar
              width: 1
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLAIMSTS)"
            - name: ORGPDSBMDT
              type: varchar
              width: 10
              map: " If Len(TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.ORGPDSBMDT)) = 0 or Trim(Lnk_W_Drug_Clm_Price.ORGPDSBMDT) = 0 Or IsNull(Lnk_W_Drug_Clm_Price.ORGPDSBMDT) Then '1753-01-01' Else TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.ORGPDSBMDT)"
            - name: METRICQTY
              type: varchar
              width: 6
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.METRICQTY)"
            - name: GPINUMBER
              type: varchar
              width: 20
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.GPINUMBER)"
            - name: GENINDOVER
              type: varchar
              width: 1
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.GENINDOVER)"
            - name: CPQSPCPRG
              type: varchar
              width: 10
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CPQSPCPRG)"
            - name: CPQSPCPGIN
              type: varchar
              width: 1
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CPQSPCPGIN)"
            - name: FNLPLANCDE
              type: varchar
              width: 10
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.FNLPLANCDE)"
            - name: FNLPLANDTE
              type: varchar
              width: 10
              map: " If Len(TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.FNLPLANDTE)) = 0 or Trim(Lnk_W_Drug_Clm_Price.FNLPLANDTE) = 0 Then '1753-01-01' Else TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.FNLPLANDTE)"
            - name: FORMLRFLAG
              type: varchar
              width: 1
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.FORMLRFLAG)"
            - name: AWPUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.AWPUNITCST)"
            - name: WACUNITCST
              type: decimal
              width: 13
              precision: 5
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.WACUNITCST)"
            - name: CTYPEUCOST
              type: decimal
              width: 13
              precision: 5
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CTYPEUCOST)"
            - name: PROQTY
              type: varchar
              width: 12
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.PROQTY)"
            - name: CLTCOSTTYP
              type: varchar
              width: 10
              map: "trim(Lnk_W_Drug_Clm_Price.CLTCOSTTYP)"
            - name: CLTPRCTYPE
              type: varchar
              width: 10
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLTPRCTYPE)"
            - name: CLTRATE
              type: decimal
              width: 7
              precision: 2
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLTRATE)"
            - name: CLTTYPUCST
              type: decimal
              width: 13
              precision: 5
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLTTYPUCST)"
            - name: CLT2INGRCST
              type: decimal
              width: 13
              precision: 2
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLT2INGRCST)"
            - name: CLT2DISPFEE
              type: decimal
              width: 13
              precision: 2
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLT2DISPFEE)"
            - name: CLT2COSTSRC
              type: varchar
              width: 1
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLT2COSTSRC)"
            - name: CLT2COSTTYP
              type: varchar
              width: 10
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLT2COSTTYP)"
            - name: CLT2PRCTYPE
              type: varchar
              width: 10
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLT2PRCTYPE)"
            - name: CLT2RATE
              type: decimal
              width: 7
              precision: 2
              map: "TrimLeadingTrailing(Lnk_W_Drug_Clm_Price.CLT2RATE)"
            - name: RXNETWRKQL
              type: varchar
              width: 1
              map: "Trim(Lnk_W_Drug_Clm_Price.RXNETWRKQL)"
            - name: CLT2SLSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CLT2SLSTAX"
            - name: CLT2PSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CLT2PSTAX"
            - name: CLT2FSTAX
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CLT2FSTAX"
            - name: CLT2DUEAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CLT2DUEAMT"
            - name: CLT2OTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CLT2OTHAMT"
            - name: CLTINCENTV
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CLTINCENTV"
            - name: CALINCENTV2
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CALINCENTV2"
            - name: CLTOTHPAYA
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CLTOTHPAYA"
            - name: CLTOTHAMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_W_Drug_Clm_Price.CLTTOTHAMT"
            - name: CLNTDEF2
              type: varchar
              width: 10
              map: "Lnk_W_Drug_Clm_Price.CLNTDEF2"
            - name: TIER_ID
              type: varchar
              width: 2
              map: "Lnk_W_Drug_Clm_Price.TIER_ID"
            - name: DRUG_TYP_CD
              type: varchar
              width: 1
              map: "Lnk_W_Drug_Clm_Price.DRUG_TYP_CD"
            - name: PLN_DRUG_STTUS_CD
              type: char
              width: 1
              map: "Lnk_W_Drug_Clm_Price.PLN_DRUG_STTUS_CD"
            - name: PLANTYPE
              type: char
              width: 8
              map: "Lnk_W_Drug_Clm_Price.PLANTYPE"
            - name: StageVar
              type: varchar
              width: 255
              map: StageVar
              dropStageVar: true
              initValue: ""
            - name: SVClmId
              type: varchar
              width: 255
              map: SVClmId
              dropStageVar: true
              initValue: ""
        - name: xfm_CheckLkpResults
          type: map
          source: Lkp_Frmly
          alias: Lnk_IdsDrugClmPriceFkey_Lkp_Out
          stageVars: 
            - name: SVDate1
              type: varchar
              width: 7
              map: "TrimLeadingTrailing(Convert('-', '', Lnk_IdsDrugClmPriceFkey_Lkp_Out.FNLPLANDTE))"
              initValue: ""
            - name: SVDate2
              type: varchar
              width: 7
              map: "SVDate1[1, 1]"
              initValue: ""
            - name: SVDate3
              type: varchar
              width: 10
              map: " If SVDate2 = '1' Then '20' : SVDate1[2, 6] Else '19' : SVDate1[2, 6]"
              initValue: ""
            - name: SVDrugTypCd
              type: varchar
              width: 20
              map: " If Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.GNRC_DRUG_IN) = 'Y' Then 'GENERIC' Else 'BRAND'"
              initValue: ""
            - name: SVBuySlsTaxAmt
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2PSTAX + Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2FSTAX"
              initValue: "0"
          schema: 
            - name: DRUG_CLM_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.DRUG_CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLM_ID)"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: IDSRunCycle
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: IDSRunCycle
            - name: BUY_CST_SRC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.BUY_CST_SRC_CD_SK"
            - name: GNRC_OVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.GNRC_OVRD_CD_SK"
            - name: PDX_NTWK_QLFR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.PDX_NTWK_QLFR_CD_SK"
            - name: FRMLRY_PROTOCOL_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.FORMLRFLAG"
            - name: RECON_IN
              type: char
              width: 1
              map: "setnull()"
            - name: SPEC_PGM_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CPQSPCPGIN"
            - name: FINL_PLN_EFF_DT_SK
              type: char
              width: 10
              nullable: N
              map: " If Lnk_IdsDrugClmPriceFkey_Lkp_Out.FNLPLANDTE = '0000000' Then StringToDate('1753-01-01', \"%yyyy-%mm-%dd\") Else StringToDate(SVDate3[1, 4] : '-' : SVDate3[5, 2] : '-' : SVDate3[7, 2], \"%yyyy-%mm-%dd\")"
            - name: ORIG_PD_TRANS_SUBMT_DT_SK
              type: char
              width: 10
              nullable: N
              map: " If Lnk_IdsDrugClmPriceFkey_Lkp_Out.ORGPDSBMDT = '00000000' Then StringToDate('1753-01-01', \"%yyyy-%mm-%dd\") Else StringToDate(Lnk_IdsDrugClmPriceFkey_Lkp_Out.ORGPDSBMDT[1, 4] : '-' : Lnk_IdsDrugClmPriceFkey_Lkp_Out.ORGPDSBMDT[5, 2] : '-' : Lnk_IdsDrugClmPriceFkey_Lkp_Out.ORGPDSBMDT[7, 2], \"%yyyy-%mm-%dd\")"
            - name: PRORTD_DISPNS_QTY
              type: decimal
              width: 11
              precision: 3
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.PROQTY"
            - name: SUBMT_DISPNS_METRIC_QTY
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.METRICQTY"
            - name: AVG_WHLSL_PRICE_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.AWPUNITCST"
            - name: WHLSL_ACQSTN_CST_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.WACUNITCST"
            - name: BUY_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2DISPFEE"
            - name: BUY_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2INGRCST"
            - name: BUY_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2RATE"
            - name: BUY_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: SVBuySlsTaxAmt
            - name: BUY_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2DUEAMT"
            - name: BUY_TOT_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2OTHAMT"
            - name: CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CTYPEUCOST"
            - name: INVC_TOT_DUE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: SELL_CST_TYP_UNIT_CST_AMT
              type: decimal
              width: 13
              precision: 5
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLTTYPUCST"
            - name: SELL_RATE_PCT
              type: decimal
              width: 7
              precision: 2
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLTRATE)"
            - name: SPREAD_DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2DISPFEE) Then 0 Else  If Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2DISPFEE = 0 Then Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2DISPFEE Else Lnk_IdsDrugClmPriceFkey_Lkp_Out.DISPNS_FEE_AMT - Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2DISPFEE"
            - name: SPREAD_INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2INGRCST) Then 0 Else  If Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2INGRCST = 0 Then Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2INGRCST Else Lnk_IdsDrugClmPriceFkey_Lkp_Out.INGR_CST_CHRGD_AMT - Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2INGRCST"
            - name: SPREAD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.SLS_TAX_AMT - SVBuySlsTaxAmt"
            - name: BUY_CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2COSTTYP)"
            - name: BUY_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLT2PRCTYPE)"
            - name: CST_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLTCOSTTYP)"
            - name: DRUG_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: SVDrugTypCd
            - name: FINL_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.FNLPLANCDE)"
            - name: GNRC_PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.GPINUMBER)"
            - name: SELL_PRICE_TYP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLTPRCTYPE)"
            - name: SPEC_PGM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Trim(Lnk_IdsDrugClmPriceFkey_Lkp_Out.CPQSPCPRG)"
            - name: BUY_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CALINCENTV2"
            - name: SELL_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLTINCENTV"
            - name: SPREAD_INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLTINCENTV - Lnk_IdsDrugClmPriceFkey_Lkp_Out.CALINCENTV2"
            - name: SELL_OTHR_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLTOTHAMT"
            - name: SELL_OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.CLTOTHPAYA"
            - name: FRMLRY_SK
              type: int
              width: 4
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.FRMLRY_SK"
            - name: TIER_ID
              type: varchar
              width: 2
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.TIER_ID"
            - name: DRUG_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.DRUG_TYP_CD_SK"
            - name: PLN_DRUG_STTUS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.PLN_DRUG_STTUS_CD_SK"
            - name: DRUG_PLN_TYP_ID
              type: varchar
              width: 20
              map: "Lnk_IdsDrugClmPriceFkey_Lkp_Out.PLANTYPE"
            - name: SVDate1
              type: varchar
              width: 7
              map: SVDate1
              dropStageVar: true
              initValue: ""
            - name: SVDate2
              type: varchar
              width: 7
              map: SVDate2
              dropStageVar: true
              initValue: ""
            - name: SVDate3
              type: varchar
              width: 10
              map: SVDate3
              dropStageVar: true
              initValue: ""
            - name: SVDrugTypCd
              type: varchar
              width: 20
              map: SVDrugTypCd
              dropStageVar: true
              initValue: ""
            - name: SVBuySlsTaxAmt
              type: decimal
              width: 13
              precision: 2
              map: SVBuySlsTaxAmt
              dropStageVar: true
              initValue: "0"
        - name: SRC_DOMAIN_TRNSLTN
          type: dbReader
          originalDb: ""
          origin: odbcConnector
          query: "SELECT SRC_DOMAIN_TX, TRGT_DOMAIN_TX \nFROM #$UWSOwner#.SRC_DOMAIN_TRNSLTN \nWHERE EFF_DT_SK <= '#CurrentDate#' \nAND TERM_DT_SK >= '#CurrentDate#'\nAND SRC_SYS_CD = '#SrcSysCd#'\nAND DOMAIN_ID = 'PLAN_DRUG_ST' \n--AND SRC_DOMAIN_TX in ('3','5','7','8','9','A','a','B','b','C','c','D','E','e','F','f','G','g','H','h','J','K','L','M','N','O','P','Q','R','S','s','T','t','U','V','W','x','Y','y','Z','z')\n;"
          schema: 
            - name: SRC_DOMAIN_TX
              type: varchar
              width: 40
              nullable: N
            - name: TRGT_DOMAIN_TX
              type: varchar
              width: 40
              nullable: N
          keys:
            - SRC_DOMAIN_TX


