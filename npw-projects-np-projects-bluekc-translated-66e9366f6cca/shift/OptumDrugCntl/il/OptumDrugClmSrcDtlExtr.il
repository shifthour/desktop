model:
  config:
    origin: datastage
  jobs:
    - name: OptumDrugClmSrcDtlExtr
      origin: datastage
      type: dataflow
      folder: "claim/claim/Optum"
      description: "Optum ClaimSourceDetail Extract"
      fullDescription: "© Copyright 2020 Blue Cross/Blue Shield of Kansas City\n\nCALLED BY :  OptumACADrugCntl/OptumDrugCntl/OptumMedDDrugCntl\n \nDESCRIPTION:  This Job Create the Load File from Sequential File created in Extract Job.\n\nMODIFICATIONS:\n\nDeveloper                         Date                 Project/Altiris #              Change Description                                 Development Project      Code Reviewer          Date Reviewed\n------------------                      --------------------      ----------------------               ------------------------------------                             --------------------------------    -------------------------------      ----------------------------              \nRevathi BoojiReddy        10-11-2022            US 559701                  Original Programming                              IntegrateDevB               Jeyaprasanna              2023-01-14"
      parameters:
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
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: SrcSysCd
          type: string
          description: SrcSysCd
        - name: RunID
          type: string
          description: RunID
        - name: IDSRunCycle
          type: string
          description: IDSRunCycle
        - name: SrcSysCdSK
          type: integer
          description: SrcSysCdSK
        - name: LOB
          type: string
          description: LOB
      steps:
        - name: Lkp_Clm
          type: lookup
          option:
            joinType: left
          sources: 
            - name: Fnl
              alias: Lnk_seq
              joinFilter: ""
              joinExpression: []
            - name: db2_Clm
              alias: Ref_db2_Clm
              joinFilter: ""
              joinExpression: 
              - fromExpr: Ref_db2_Clm.CLM_ID
                toExpr: Lnk_seq.CLM_ID
                caseInSensitive: false
            - name: db2_ClmSrcDtl
              alias: Ref_db2_ClmSrcDtl
              joinFilter: ""
              joinExpression: 
              - fromExpr: Ref_db2_ClmSrcDtl.CLM_ID
                toExpr: Lnk_seq.CLM_ID
                caseInSensitive: false
            # mutiple reference detected
          joinCondition: 
            - expression: Ref_db2_Clm.CLM_ID = Lnk_seq.CLM_ID
              caseInSensitive: false
            - expression: Ref_db2_ClmSrcDtl.CLM_ID = Lnk_seq.CLM_ID
              caseInSensitive: false
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              map: "Ref_db2_Clm.CLM_SK"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "Ref_db2_ClmSrcDtl.CRT_RUN_CYC_EXCTN_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Lnk_seq.CLM_ID"
        - name: seq_PDX_CLM_STD_INPT_Land
          type: fileReader
          urls: 
            - "#$FilePath#/verified/PDX_CLM_STD_INPT_Land_#SrcSysCd#.dat.#RunID#"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: FILE_RCVD_DT
              type: char
              width: 10
            - name: RCRD_ID
              type: int
              width: 4
            - name: PRCSR_NO
              type: varchar
              width: 10
            - name: BTCH_NO
              type: int
              width: 4
            - name: PDX_NO
              type: varchar
              width: 20
            - name: RX_NO
              type: varchar
              width: 20
            - name: FILL_DT
              type: char
              width: 10
            - name: NDC
              type: varchar
              width: 20
            - name: DRUG_DESC
              type: varchar
              width: 30
            - name: NEW_OR_RFL_CD
              type: int
              width: 4
            - name: METRIC_QTY
              type: decimal
              width: 10
              precision: 3
            - name: DAYS_SUPL
              type: int
              width: 4
            - name: BSS_OF_CST_DTRM
              type: varchar
              width: 2
            - name: INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BILL_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PATN_FIRST_NM
              type: varchar
              width: 35
            - name: PATN_LAST_NM
              type: varchar
              width: 75
            - name: BRTH_DT
              type: char
              width: 10
            - name: SEX_CD
              type: int
              width: 4
            - name: CARDHLDR_ID_NO
              type: varchar
              width: 20
            - name: RELSHP_CD
              type: int
              width: 4
            - name: GRP_NO
              type: varchar
              width: 20
            - name: HOME_PLN
              type: varchar
              width: 3
            - name: HOST_PLN
              type: varchar
              width: 3
            - name: PRSCRBR_ID
              type: varchar
              width: 20
            - name: DIAG_CD
              type: varchar
              width: 20
            - name: CARDHLDR_FIRST_NM
              type: varchar
              width: 35
            - name: CARDHLDR_LAST_NM
              type: varchar
              width: 75
            - name: PRAUTH_NO
              type: varchar
              width: 12
            - name: PA_MC_SC_NO
              type: varchar
              width: 7
            - name: CUST_LOC
              type: int
              width: 4
            - name: RESUB_CYC_CT
              type: int
              width: 4
            - name: RX_DT
              type: varchar
              width: 10
            - name: DISPENSE_AS_WRTN_PROD_SEL_CD
              type: varchar
              width: 1
            - name: PRSN_CD
              type: varchar
              width: 3
            - name: OTHR_COV_CD
              type: int
              width: 4
            - name: ELIG_CLRFCTN_CD
              type: int
              width: 4
            - name: CMPND_CD
              type: int
              width: 4
            - name: NO_OF_RFLS_AUTH
              type: int
              width: 4
            - name: LVL_OF_SVC
              type: int
              width: 4
            - name: RX_ORIG_CD
              type: int
              width: 4
            - name: RX_DENIAL_CLRFCTN
              type: int
              width: 4
            - name: PRI_PRSCRBR
              type: varchar
              width: 20
            - name: CLNC_ID_NO
              type: int
              width: 4
            - name: DRUG_TYP
              type: int
              width: 4
            - name: PRSCRBR_LAST_NM
              type: varchar
              width: 75
            - name: POSTAGE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: UNIT_DOSE_IN
              type: int
              width: 4
            - name: OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BSS_OF_DAYS_SUPL_DTRM
              type: int
              width: 4
            - name: FULL_AVG_WHLSL_PRICE
              type: decimal
              width: 13
              precision: 2
            - name: EXPNSN_AREA
              type: varchar
              width: 1
            - name: MSTR_CAR
              type: varchar
              width: 20
            - name: SUBCAR
              type: varchar
              width: 20
            - name: CLM_TYP
              type: varchar
              width: 1
            - name: SUBGRP
              type: varchar
              width: 20
            - name: PLN_DSGNR
              type: varchar
              width: 1
            - name: ADJDCT_DT
              type: char
              width: 10
            - name: ADMIN_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CAP_AMT
              type: decimal
              width: 13
              precision: 2
            - name: INGR_CST_SUB_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MBR_NON_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MBR_PAY_CD
              type: varchar
              width: 2
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_ADJ_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_ADJ_CD
              type: varchar
              width: 2
            - name: FRMLRY_FLAG
              type: varchar
              width: 1
            - name: GNRC_CLS_NO
              type: varchar
              width: 20
            - name: THRPTC_CLS_AHFS
              type: varchar
              width: 20
            - name: PDX_TYP
              type: varchar
              width: 3
            - name: BILL_BSS_CD
              type: varchar
              width: 2
            - name: USL_AND_CUST_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PD_DT
              type: char
              width: 10
            - name: BNF_CD
              type: varchar
              width: 10
            - name: DRUG_STRG
              type: varchar
              width: 20
            - name: ORIG_MBR
              type: varchar
              width: 20
            - name: INJRY_DT
              type: varchar
              width: 10
            - name: FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: REF_NO
              type: varchar
              width: 20
            - name: CLNT_CUST_ID
              type: varchar
              width: 20
            - name: PLN_TYP
              type: varchar
              width: 20
            - name: ADJDCT_REF_NO
              type: int
              width: 4
            - name: ANCLRY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_GNRL_PRPS_AREA
              type: varchar
              width: 75
            - name: PRTL_FILL_STTUS_CD
              type: varchar
              width: 20
            - name: BILL_DT
              type: char
              width: 10
            - name: FSA_VNDR_CD
              type: varchar
              width: 20
            - name: PICA_DRUG_CD
              type: varchar
              width: 1
            - name: CLM_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
            - name: FED_DRUG_CLS_CD
              type: varchar
              width: 1
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BNF_COPAY_100
              type: varchar
              width: 1
            - name: CLM_PRCS_TYP
              type: varchar
              width: 1
            - name: INDEM_HIER_TIER_NO
              type: varchar
              width: 4
            - name: MCARE_D_COV_DRUG
              type: varchar
              width: 3
            - name: RETRO_LICS_CD
              type: varchar
              width: 1
            - name: RETRO_LICS_AMT
              type: decimal
              width: 13
              precision: 2
            - name: LICS_SBSDY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MCARE_B_DRUG
              type: varchar
              width: 1
            - name: MCARE_B_CLM
              type: varchar
              width: 1
            - name: PRSCRBR_QLFR
              type: char
              width: 2
            - name: PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: PDX_QLFR
              type: char
              width: 2
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: THER_CLS
              type: varchar
              width: 6
            - name: HIC_NO
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_FLAG
              type: varchar
              width: 1
            - name: DOSE_CD
              type: varchar
              width: 4
            - name: LOW_INCM
              type: varchar
              width: 1
            - name: RTE_OF_ADMIN
              type: varchar
              width: 2
            - name: DEA_SCHD
              type: varchar
              width: 1
            - name: COPAY_BNF_OPT
              type: varchar
              width: 10
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
            - name: PRSCRBR_SPEC
              type: varchar
              width: 20
            - name: VAL_CD
              type: varchar
              width: 20
            - name: PRI_CARE_PDX
              type: varchar
              width: 20
            - name: OFC_OF_INSPECTOR_GNRL
              type: varchar
              width: 1
            - name: PATN_SSN
              type: varchar
              width: 11
            - name: CARDHLDR_SSN
              type: varchar
              width: 11
            - name: CARDHLDR_BRTH_DT
              type: varchar
              width: 10
            - name: CARDHLDR_ADDR
              type: varchar
              width: 75
            - name: CARDHLDR_CITY
              type: varchar
              width: 35
            - name: CHADHLDR_ST
              type: varchar
              width: 2
            - name: CARDHLDR_ZIP_CD
              type: varchar
              width: 5
            - name: PSL_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PAR_PDX_IN
              type: varchar
              width: 1
            - name: COPAY_PCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: COPAY_FLAT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_TRNSMSN_METH
              type: varchar
              width: 1
            - name: RX_NO_2012
              type: varchar
              width: 12
            - name: CLM_ID
              type: varchar
              width: 20
            - name: CLM_STTUS_CD
              type: varchar
              width: 2
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
            - name: LOB_IN
              type: varchar
              width: 5
        - name: SeqTgt_CLM_SRC_DTL
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_SRC_DTL_#SrcSysCd#.dat.#RunID#"
          source: XfmLoad
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
        - name: db2_ClmSrcDtl
          type: dbReader
          originalDb: db2
          query: "SELECT \n\nCLM_ID,\nCRT_RUN_CYC_EXCTN_SK,\nLAST_UPDT_RUN_CYC_EXCTN_SK\n\n \n\n FROM #$IDSOwner#.CLM_SRC_DTL\n \n\n"
          schema: 
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
          keys:
            - CLM_ID
        - name: db2_Clm
          type: dbReader
          originalDb: db2
          query: "SELECT  \n\nCLM_SK,\nCLM_ID\n \nFROM #$IDSOwner#.CLM\n\nwhere \nLAST_UPDT_RUN_CYC_EXCTN_SK='#IDSRunCycle#'  AND\nSRC_SYS_CD_SK=#SrcSysCdSK#\n\n  \n \n\n"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_ID
        - name: Xfm1.V0S161P2
          type: map
          source: seq_PDX_CLM_STD_INPT_Land
          alias: Lnk_Xfm_logic
          schema: 
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Lnk_Xfm_logic.CLM_ID"
        - name: Xfm1.V0S161P5
          type: map
          source: Xfm1.V0S161P5.filter
          alias: Lnk_Xfm_logic
          schema: 
            - name: CLM_ID
              type: varchar
              width: 20
              map: "Lnk_Xfm_logic.CLM_ID[1, len(trim(Lnk_Xfm_logic.CLM_ID)) - 1]"
        - name: Xfm1.V0S161P5.filter
          type: filter
          source: seq_PDX_CLM_STD_INPT_Land
          filter: "CLM_TYP[1, 1] = 'X' Or CLM_TYP[1, 1] = 'R'"
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: FILE_RCVD_DT
              type: char
              width: 10
            - name: RCRD_ID
              type: int
              width: 4
            - name: PRCSR_NO
              type: varchar
              width: 10
            - name: BTCH_NO
              type: int
              width: 4
            - name: PDX_NO
              type: varchar
              width: 20
            - name: RX_NO
              type: varchar
              width: 20
            - name: FILL_DT
              type: char
              width: 10
            - name: NDC
              type: varchar
              width: 20
            - name: DRUG_DESC
              type: varchar
              width: 30
            - name: NEW_OR_RFL_CD
              type: int
              width: 4
            - name: METRIC_QTY
              type: decimal
              width: 10
              precision: 3
            - name: DAYS_SUPL
              type: int
              width: 4
            - name: BSS_OF_CST_DTRM
              type: varchar
              width: 2
            - name: INGR_CST_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DISPNS_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BILL_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PATN_FIRST_NM
              type: varchar
              width: 35
            - name: PATN_LAST_NM
              type: varchar
              width: 75
            - name: BRTH_DT
              type: char
              width: 10
            - name: SEX_CD
              type: int
              width: 4
            - name: CARDHLDR_ID_NO
              type: varchar
              width: 20
            - name: RELSHP_CD
              type: int
              width: 4
            - name: GRP_NO
              type: varchar
              width: 20
            - name: HOME_PLN
              type: varchar
              width: 3
            - name: HOST_PLN
              type: varchar
              width: 3
            - name: PRSCRBR_ID
              type: varchar
              width: 20
            - name: DIAG_CD
              type: varchar
              width: 20
            - name: CARDHLDR_FIRST_NM
              type: varchar
              width: 35
            - name: CARDHLDR_LAST_NM
              type: varchar
              width: 75
            - name: PRAUTH_NO
              type: varchar
              width: 12
            - name: PA_MC_SC_NO
              type: varchar
              width: 7
            - name: CUST_LOC
              type: int
              width: 4
            - name: RESUB_CYC_CT
              type: int
              width: 4
            - name: RX_DT
              type: varchar
              width: 10
            - name: DISPENSE_AS_WRTN_PROD_SEL_CD
              type: varchar
              width: 1
            - name: PRSN_CD
              type: varchar
              width: 3
            - name: OTHR_COV_CD
              type: int
              width: 4
            - name: ELIG_CLRFCTN_CD
              type: int
              width: 4
            - name: CMPND_CD
              type: int
              width: 4
            - name: NO_OF_RFLS_AUTH
              type: int
              width: 4
            - name: LVL_OF_SVC
              type: int
              width: 4
            - name: RX_ORIG_CD
              type: int
              width: 4
            - name: RX_DENIAL_CLRFCTN
              type: int
              width: 4
            - name: PRI_PRSCRBR
              type: varchar
              width: 20
            - name: CLNC_ID_NO
              type: int
              width: 4
            - name: DRUG_TYP
              type: int
              width: 4
            - name: PRSCRBR_LAST_NM
              type: varchar
              width: 75
            - name: POSTAGE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: UNIT_DOSE_IN
              type: int
              width: 4
            - name: OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BSS_OF_DAYS_SUPL_DTRM
              type: int
              width: 4
            - name: FULL_AVG_WHLSL_PRICE
              type: decimal
              width: 13
              precision: 2
            - name: EXPNSN_AREA
              type: varchar
              width: 1
            - name: MSTR_CAR
              type: varchar
              width: 20
            - name: SUBCAR
              type: varchar
              width: 20
            - name: CLM_TYP
              type: varchar
              width: 1
            - name: SUBGRP
              type: varchar
              width: 20
            - name: PLN_DSGNR
              type: varchar
              width: 1
            - name: ADJDCT_DT
              type: char
              width: 10
            - name: ADMIN_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CAP_AMT
              type: decimal
              width: 13
              precision: 2
            - name: INGR_CST_SUB_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MBR_NON_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MBR_PAY_CD
              type: varchar
              width: 2
            - name: INCNTV_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_ADJ_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_ADJ_CD
              type: varchar
              width: 2
            - name: FRMLRY_FLAG
              type: varchar
              width: 1
            - name: GNRC_CLS_NO
              type: varchar
              width: 20
            - name: THRPTC_CLS_AHFS
              type: varchar
              width: 20
            - name: PDX_TYP
              type: varchar
              width: 3
            - name: BILL_BSS_CD
              type: varchar
              width: 2
            - name: USL_AND_CUST_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PD_DT
              type: char
              width: 10
            - name: BNF_CD
              type: varchar
              width: 10
            - name: DRUG_STRG
              type: varchar
              width: 20
            - name: ORIG_MBR
              type: varchar
              width: 20
            - name: INJRY_DT
              type: varchar
              width: 10
            - name: FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: REF_NO
              type: varchar
              width: 20
            - name: CLNT_CUST_ID
              type: varchar
              width: 20
            - name: PLN_TYP
              type: varchar
              width: 20
            - name: ADJDCT_REF_NO
              type: int
              width: 4
            - name: ANCLRY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_GNRL_PRPS_AREA
              type: varchar
              width: 75
            - name: PRTL_FILL_STTUS_CD
              type: varchar
              width: 20
            - name: BILL_DT
              type: char
              width: 10
            - name: FSA_VNDR_CD
              type: varchar
              width: 20
            - name: PICA_DRUG_CD
              type: varchar
              width: 1
            - name: CLM_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
            - name: FED_DRUG_CLS_CD
              type: varchar
              width: 1
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BNF_COPAY_100
              type: varchar
              width: 1
            - name: CLM_PRCS_TYP
              type: varchar
              width: 1
            - name: INDEM_HIER_TIER_NO
              type: varchar
              width: 4
            - name: MCARE_D_COV_DRUG
              type: varchar
              width: 3
            - name: RETRO_LICS_CD
              type: varchar
              width: 1
            - name: RETRO_LICS_AMT
              type: decimal
              width: 13
              precision: 2
            - name: LICS_SBSDY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MCARE_B_DRUG
              type: varchar
              width: 1
            - name: MCARE_B_CLM
              type: varchar
              width: 1
            - name: PRSCRBR_QLFR
              type: char
              width: 2
            - name: PRSCRBR_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: PDX_QLFR
              type: char
              width: 2
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: THER_CLS
              type: varchar
              width: 6
            - name: HIC_NO
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_FLAG
              type: varchar
              width: 1
            - name: DOSE_CD
              type: varchar
              width: 4
            - name: LOW_INCM
              type: varchar
              width: 1
            - name: RTE_OF_ADMIN
              type: varchar
              width: 2
            - name: DEA_SCHD
              type: varchar
              width: 1
            - name: COPAY_BNF_OPT
              type: varchar
              width: 10
            - name: GNRC_PROD_IN
              type: varchar
              width: 14
            - name: PRSCRBR_SPEC
              type: varchar
              width: 20
            - name: VAL_CD
              type: varchar
              width: 20
            - name: PRI_CARE_PDX
              type: varchar
              width: 20
            - name: OFC_OF_INSPECTOR_GNRL
              type: varchar
              width: 1
            - name: PATN_SSN
              type: varchar
              width: 11
            - name: CARDHLDR_SSN
              type: varchar
              width: 11
            - name: CARDHLDR_BRTH_DT
              type: varchar
              width: 10
            - name: CARDHLDR_ADDR
              type: varchar
              width: 75
            - name: CARDHLDR_CITY
              type: varchar
              width: 35
            - name: CHADHLDR_ST
              type: varchar
              width: 2
            - name: CARDHLDR_ZIP_CD
              type: varchar
              width: 5
            - name: PSL_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_FMLY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_FMLY_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: DEDCT_MBR_MET_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PSL_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MOPS_APLD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: PAR_PDX_IN
              type: varchar
              width: 1
            - name: COPAY_PCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: COPAY_FLAT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLM_TRNSMSN_METH
              type: varchar
              width: 1
            - name: RX_NO_2012
              type: varchar
              width: 12
            - name: CLM_ID
              type: varchar
              width: 20
            - name: CLM_STTUS_CD
              type: varchar
              width: 2
            - name: ADJ_FROM_CLM_ID
              type: varchar
              width: 20
            - name: ADJ_TO_CLM_ID
              type: varchar
              width: 20
            - name: SUBMT_PROD_ID_QLFR
              type: varchar
              width: 2
            - name: CNTNGNT_THER_FLAG
              type: varchar
              width: 1
            - name: CNTNGNT_THER_SCHD
              type: varchar
              width: 20
            - name: CLNT_PATN_PAY_ATRBD_PROD_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_SLS_TAX_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_PRCSR_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_PATN_PAY_ATRBD_NTWK_AMT
              type: decimal
              width: 13
              precision: 2
            - name: LOB_IN
              type: varchar
              width: 5
        - name: XfmLoad
          type: map
          source: XfmLoad.filter
          alias: LnkToXfm
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "LnkToXfm.CLM_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSK
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "LnkToXfm.CLM_ID"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: " If Isnull(LnkToXfm.CRT_RUN_CYC_EXCTN_SK) then IDSRunCycle else LnkToXfm.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: IDSRunCycle
            - name: SRC_FILE_ID
              type: varchar
              width: 20
              nullable: N
              map: LOB
        - name: XfmLoad.filter
          type: filter
          source: Lkp_Clm
          filter: "IsNotNull(CLM_SK)"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
            - name: CLM_ID
              type: varchar
              width: 20
        - name: Fnl
          type: union
          sources: 
            - Xfm1.V0S161P2
            - Xfm1.V0S161P5
          option: 
            unionType: all
          schema: 
            - name: CLM_ID
              type: varchar
              width: 20
              map: CLM_ID


