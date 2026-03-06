model:
  config:
    origin: datastage
  jobs:
    - name: BCBSKCCommClmLnExtr
      origin: datastage
      type: dataflow
      folder: "claim/ClaimLine/ClmLn/BCBSKCCommon"
      runAfterSubrOnJobSuccess: enabled
      description: "BCBSKCComm Claim Line"
      fullDescription: "COPYRIGHT 2018 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nCALLED BY:  BCBSKCCommDrugExtrSeq\n\nDESCRIPTION:  \n    Reads the PDX_CLM_STD_INPT_Land file and runs through primary key using shared container ClmLnPk\n    \n\nMODIFICATIONS:\n\nDeveloper                       Date                 Project/Altiris #      Change Description                                                                                              Development Project      Code Reviewer          Date Reviewed       \n------------------                     --------------------     ------------------------      -------------------------------------------------------------------------------------------------------------------------    --------------------------------       -------------------------------   ----------------------------       \nKaushik Kapoor              2018-02-26      5828                       Original Programming: Reads data from PDX_CLM_STD_INPT_Land.dat file     IntegrateDev2                 Kalyan Neelam           2018-02-26\n                                                                                              and creates an extract file for CLM_LN which will be read \n                                                                                               by IdsClmLnFKey job to create CLM_LN load file\nKaushik Kapoor              2018-03-16      5828                       Changed SVC_PRICE_RULE_ID from ' ' to 'NA'                                                  IntegrateDev2\x09   Jaideep Mankala       03/20/2018\nMadhavan B                   2018-02-06      5792 \x09              Changed the datatype of the column                                                                   IntegrateDev1                  Kalyan Neelam          2018-02-08\n                                                             \x09\x09              NDC_UNIT_CT from SMALLINT to DECIMAL (21,10)\nRekha Radhakrishna     2020-02-27       6131                      Changed ALW_AMT calculation for OPTUMRX                                                  IntegrateDev2                  Hugh Sisson              2020-03-06\n\nGoutham Kalidindi        10/22/2020       US-283560          Added MEDIMPACT  derivation for CLM_LN_POS_CD                                         IntegrateDev2                Reddy Sanam             2020-11-09\n                                                                                             and PAYBL_AMT\n\n\nGoutham Kalidindi        10/22/2020       US-283560          Added MEDIMPACT derivation for   ALLWD_AMT                                                IntegrateDev2                  Kalyan Neelam        2020-11-10\n\n\nGoutham Kalidindi        11/11/2020       US-283560                           Changed MEDIMPACT derivation for   ALLWD_AMT                 IntegrateDev2                \n                                                                                                           Added MEDIMPACT derivation for CNSD_CHRG_AMT\n                                                                                                           CHRG_AMT  \n\n                                                                                            \n\n\nRekha Radhakrishna     2020-07-27       6131                     Modified common file layout to include157 columns                                              IntegrateDev2                 Kalyan Neelam         2020-11-12\n                                                                                             and updated CLM_LN_POS_CD mapping for MedD and ACA\n\nRekha Radhakrishna     2020-08-13       6131                    Modified source file common layout to include LOB_IN                                         IntegrateDev2       \nRekha Radhakrishna     2020-08-17       6131                    Mapped APC_ID and APC_STTUS_ID to Null for 'OPTUMRX'                            IntegrateDev2 \n                    \nRekha Radhakrishna     2020-09-25       6131                    UpdatedCLM_LN_POS_CD for  'OPTUMRX'                                                         IntegrateDev2             Sravya Gorla              2020-09-12       \n\nAmritha A J                      2023-07-31   US 589700           Added two new fields SNOMED_CT_CD,CVX_VCCN_CD with a default                IntegrateDevB\x09Harsha Ravuri\x092023-08-31\n                                                                                            value in BusinessRules stage and mapped it till target"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: IDSInstance
          type: string
          description: "IDS Instance"
          defaultType: project
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: CurrentDate
          type: string
          description: "Current Date"
        - name: SrcSysCd
          type: string
          description: "Source System Code"
        - name: SrcSysCdSk
          type: string
          description: "Source System Code Surrogate Key"
        - name: RunID
          type: string
          description: "Run ID"
        - name: RunCycle
          type: integer
          description: "Current Run Cycle"
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\1;hf_bcbskccomm_drg_clm_ln_prov"
        subr: "1;hf_bcbskccomm_drg_clm_ln_prov"
      steps:
        - name: BCBSKCCommClmLand
          type: fileReader
          urls: 
            - "#$FilePath#/verified/PDX_CLM_STD_INPT_Land_#SrcSysCd#.dat.#RunID#"
          rejectMode: continue
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: FILE_RCVD_DT
              type: char
              width: 10
            - name: RCRD_ID
              type: decimal
              width: 2
            - name: PRCSR_NO
              type: varchar
              width: 10
            - name: BTCH_NO
              type: decimal
              width: 5
            - name: PDX_NO
              type: decimal
              width: 12
            - name: RX_NO
              type: decimal
              width: 9
            - name: FILL_DT
              type: char
              width: 10
            - name: NDC
              type: decimal
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
              type: decimal
              width: 3
            - name: BSS_OF_CST_DTRM
              type: char
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
              type: decimal
              width: 1
            - name: CARDHLDR_ID_NO
              type: varchar
              width: 20
            - name: RELSHP_CD
              type: decimal
              width: 1
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
              type: decimal
              width: 2
            - name: RESUB_CYC_CT
              type: decimal
              width: 2
            - name: RX_DT
              type: char
              width: 10
            - name: DISPENSE_AS_WRTN_PROD_SEL_CD
              type: char
              width: 1
            - name: PRSN_CD
              type: varchar
              width: 3
            - name: OTHR_COV_CD
              type: decimal
              width: 2
            - name: ELIG_CLRFCTN_CD
              type: decimal
              width: 1
            - name: CMPND_CD
              type: decimal
              width: 1
            - name: NO_OF_RFLS_AUTH
              type: decimal
              width: 2
            - name: LVL_OF_SVC
              type: decimal
              width: 2
            - name: RX_ORIG_CD
              type: decimal
              width: 1
            - name: RX_DENIAL_CLRFCTN
              type: decimal
              width: 2
            - name: PRI_PRSCRBR
              type: varchar
              width: 20
            - name: CLNC_ID_NO
              type: decimal
              width: 5
            - name: DRUG_TYP
              type: decimal
              width: 1
            - name: PRSCRBR_LAST_NM
              type: varchar
              width: 75
            - name: POSTAGE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: UNIT_DOSE_IN
              type: decimal
              width: 1
            - name: OTHR_PAYOR_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BSS_OF_DAYS_SUPL_DTRM
              type: decimal
              width: 1
            - name: FULL_AVG_WHLSL_PRICE
              type: decimal
              width: 13
              precision: 2
            - name: EXPNSN_AREA
              type: char
              width: 1
            - name: MSTR_CAR
              type: varchar
              width: 20
            - name: SUBCAR
              type: varchar
              width: 20
            - name: CLM_TYP
              type: char
              width: 1
            - name: SUBGRP
              type: varchar
              width: 20
            - name: PLN_DSGNR
              type: char
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
              type: char
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
              type: char
              width: 2
            - name: FRMLRY_FLAG
              type: char
              width: 1
            - name: GNRC_CLS_NO
              type: varchar
              width: 20
            - name: THRPTC_CLS_AHFS
              type: varchar
              width: 20
            - name: PDX_TYP
              type: char
              width: 1
            - name: BILL_BSS_CD
              type: char
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
              type: char
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
              type: decimal
              width: 9
            - name: ANCLRY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: CLNT_GNRL_PRPS_AREA
              type: varchar
              width: 100
            - name: PRTL_FILL_STTUS_CD
              type: char
              width: 1
            - name: BILL_DT
              type: char
              width: 10
            - name: FSA_VNDR_CD
              type: varchar
              width: 20
            - name: PICA_DRUG_CD
              type: char
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
              type: char
              width: 1
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BNF_COPAY_100
              type: char
              width: 1
            - name: CLM_PRCS_TYP
              type: char
              width: 1
            - name: INDEM_HIER_TIER_NO
              type: decimal
              width: 4
            - name: MCARE_D_COV_DRUG
              type: char
              width: 1
            - name: RETRO_LICS_CD
              type: char
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
              type: char
              width: 1
            - name: MCARE_B_CLM
              type: char
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
              width: 11
            - name: THER_CLS
              type: decimal
              width: 6
            - name: HIC_NO
              type: varchar
              width: 20
            - name: HLTH_RMBRMT_ARGMT_FLAG
              type: char
              width: 1
            - name: DOSE_CD
              type: decimal
              width: 4
            - name: LOW_INCM
              type: char
              width: 1
            - name: RTE_OF_ADMIN
              type: char
              width: 2
            - name: DEA_SCHD
              type: decimal
              width: 1
            - name: COPAY_BNF_OPT
              type: decimal
              width: 10
            - name: GNRC_PROD_IN
              type: decimal
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
              type: char
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
              width: 40
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
              type: varchar
              width: 20
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
              type: char
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
              type: char
              width: 1
            - name: RX_NO_2012
              type: decimal
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
        - name: BCBSKCCommClmLnExtr
          type: fileWriter
          urls: 
            - "#$FilePath#/key/BCBSKCCommClmLnExtr_#SrcSysCd#.DrugClmLn.dat.#RunID#"
          source: ClmLnPK
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: B_CLM_LN
          type: fileWriter
          urls: 
            - "#$FilePath#/load/B_CLM_LN.#SrcSysCd#.dat.#RunID#"
          source: Transformer
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_bcbskccomm_prov
          type: map
          source: IDS_NPI
          alias: Npi
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Npi.NTNL_PROV_ID"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Npi.PROV_ID"
        - name: IDS_NPI
          type: dbReader
          originalDb: db2
          query: "SELECT \n\nPROV.NTNL_PROV_ID,\nPROV.PROV_ID\n\nFROM\n#$IDSOwner#.PROV PROV\n"
          tableAction: append
          schema: 
            - name: NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
          keys:
            - NTNL_PROV_ID
        - name: BusinessRules
          type: lookup
          stageVars:
            - name: svServicingProvID
              type: decimal
              width: 0
              map: " If IsNull(Extract.PDX_NO) = @FALSE then Extract.PDX_NO else  If IsNull(natln_prov.NTNL_PROV_ID) = @FALSE then natln_prov.PROV_ID else 'UNK'"
              initValue: "0"
            - name: svAllowedAmt
              type: decimal
              width: 0
              map: " If Trim(Extract.CLNT_GNRL_PRPS_AREA[61, 2]) <> '01' Then (((Extract.SLS_TAX_AMT + Extract.INGR_CST_AMT + Extract.DISPNS_FEE_AMT)) - Extract.OTHR_PAYOR_AMT) Else (Extract.INGR_CST_AMT + Extract.DISPNS_FEE_AMT + Extract.SLS_TAX_AMT)"
              initValue: "0"
            - name: svChrgAmt
              type: decimal
              width: 0
              map: " If Extract.SRC_SYS_CD = 'OPTUMRX' Then Extract.FULL_AVG_WHLSL_PRICE Else  If Extract.SRC_SYS_CD = 'MEDIMPACT' Then Extract.CLM_AMT Else 0.00"
              initValue: "0"
            - name: svCnsdChrgAmt
              type: decimal
              width: 0
              map: " If Extract.SRC_SYS_CD = 'OPTUMRX' Then Extract.FULL_AVG_WHLSL_PRICE Else  If Extract.SRC_SYS_CD = 'MEDIMPACT' Then Extract.CLM_AMT Else 0.00"
              initValue: "0"
            - name: svNdc
              type: decimal
              width: 0
              map: " If IsNull(Trim(Extract.NDC)) = @True Then 'UNK' Else Extract.NDC"
              initValue: "0"
            - name: svCoinsAmt
              type: decimal
              width: 0
              map: " If Extract.SRC_SYS_CD = 'OPTUMRX' Then Extract.COPAY_PCT_AMT Else 0.00"
              initValue: "0"
            - name: svMIAllowedAmt
              type: decimal
              width: 0
              map: " If Extract.OTHR_COV_CD = '2' Then (Extract.INGR_CST_AMT + Extract.DISPNS_FEE_AMT + Extract.SLS_TAX_AMT) - Extract.OTHR_PAYOR_AMT Else Extract.INGR_CST_AMT + Extract.DISPNS_FEE_AMT + Extract.SLS_TAX_AMT"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: BCBSKCCommClmLand
              alias: Extract
              joinFilter: ""
              joinExpression: []
            - name: hf_bcbskccomm_prov
              alias: natln_prov
              joinFilter: ""
              joinExpression:
              - fromExpr: "natln_prov.NTNL_PROV_ID"
                toExpr: "Extract.PDX_NTNL_PROV_ID"
                caseInSensitive: true
          joinCondition: 
            - expression: "natln_prov.NTNL_PROV_ID = Extract.PDX_NTNL_PROV_ID"
              caseInSensitive: true
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "\"I\""
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "\"N\""
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "\"Y\""
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: CurrentDate
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "0"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: SrcSysCd
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "SrcSysCd : \";\" : Extract.CLM_ID : \";\" : \"1\""
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "1"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: svServicingProvID
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "\"ACPTD\""
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
              map: " If Extract.SRC_SYS_CD = 'MEDIMPACT' Then Trim(Extract.CLNT_GNRL_PRPS_AREA) Else  If Len(Trim(Extract.CLNT_GNRL_PRPS_AREA[65, 2])) = 0 Then 'NA' Else  If Extract.SRC_SYS_CD = 'OPTUMRX' Then Trim(Extract.CLNT_GNRL_PRPS_AREA[65, 2]) Else 'NA'"
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
              map: "'NA'"
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: SVC_END_DT
              type: timestamp
              nullable: N
              map: "Extract.FILL_DT"
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "Extract.FILL_DT"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Extract.SRC_SYS_CD = 'OPTUMRX' Then svAllowedAmt Else  If Extract.SRC_SYS_CD = 'MEDIMPACT' Then svMIAllowedAmt Else 0.00"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: svChrgAmt
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: svCoinsAmt
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: svCnsdChrgAmt
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Isnull(Extract.COPAY_AMT) or Len(Trim(Extract.COPAY_AMT)) = 0 then '0.00' else Trim(Extract.COPAY_AMT)"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Isnull(Extract.DEDCT_AMT) or Len(Trim(Extract.DEDCT_AMT)) = 0 then '0.00' else Trim(Extract.DEDCT_AMT)"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Extract.SRC_SYS_CD = 'OPTUMRX' Then Trim(Extract.CLM_AMT) Else  If (Isnull(Extract.COPAY_AMT) or Len(Trim(Extract.COPAY_AMT)) = 0) then '0.00' Else Trim(Extract.COPAY_AMT)"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If Isnull(Extract.BILL_AMT) or Len(Trim(Extract.BILL_AMT)) = 0 then '0.00' else  If Extract.SRC_SYS_CD = 'OPTUMRX' OR Extract.SRC_SYS_CD = 'MEDIMPACT' Then Trim(Extract.BILL_AMT) Else '0.00'"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "0"
            - name: DEDCT_AMT_ACCUM_ID
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: PREAUTH_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: RFRL_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: LMT_PFX_ID
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: PREAUTH_ID
              type: char
              width: 9
              nullable: N
              map: "'NA'"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: RFRL_ID_TX
              type: char
              width: 9
              nullable: N
              map: " If Extract.SRC_SYS_CD = 'OPTUMRX' Then @Null Else 'NA'"
            - name: SVC_ID
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
              map: "'NA'"
            - name: SVC_RULE_TYP_TX
              type: char
              width: 3
              nullable: N
              map: " If Extract.SRC_SYS_CD = 'OPTUMRX' Then @Null Else 'NA'"
            - name: CLM_TYPE
              type: char
              width: 1
              nullable: N
              map: "'M'"
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
              map: "'NA'"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: "'NA'"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: " If Extract.SRC_SYS_CD = 'MEDIMPACT' then Extract.NDC Else  If Extract.SRC_SYS_CD <> 'OPTUMRX' Then 'NA' Else  If IsNull(Trim(Extract.NDC)) = @True Then 'UNK' Else  If Extract.CMPND_CD = '2' Then '99999999999' Else Extract.NDC"
            - name: APC_ID
              type: varchar
              width: 20
              map: " If Extract.SRC_SYS_CD <> 'OPTUMRX' Then @Null Else @Null"
            - name: APC_STTUS_ID
              type: varchar
              width: 20
              map: " If Extract.SRC_SYS_CD <> 'OPTUMRX' Then @Null Else @Null"
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
              map: "'NA'"
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
              map: "'NA'"
        - name: Snapshot.V0S133P2
          type: map
          source: BusinessRules
          alias: Transform
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.JOB_EXCTN_RCRD_ERR_SK"
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
              map: "Transform.INSRT_UPDT_CD"
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.DISCARD_IN"
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.PASS_THRU_IN"
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
              map: "Transform.FIRST_RECYC_DT"
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
              map: "Transform.ERR_CT"
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
              map: "Transform.RECYCLE_CT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.SRC_SYS_CD"
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
              map: "Transform.PRI_KEY_STRING"
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.CLM_LN_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "Transform.CLM_LN_SEQ_NO"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Transform.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.PROC_CD"
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.SVC_PROV_ID"
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_DSALW_EXCD"
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_EOB_EXCD"
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_FINL_DISP_CD"
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_LOB_CD"
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_POS_CD"
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_PREAUTH_CD"
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_PREAUTH_SRC_CD"
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_PRICE_SRC_CD"
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_RFRL_CD"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_RVNU_CD"
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
              map: "Transform.CLM_LN_ROOM_PRICE_METH_CD"
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_ROOM_TYP_CD"
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_TOS_CD"
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_UNIT_TYP_CD"
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.CAP_LN_IN"
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
              map: "Transform.PRI_LOB_IN"
            - name: SVC_END_DT
              type: timestamp
              nullable: N
              map: "Transform.SVC_END_DT"
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "Transform.SVC_STRT_DT"
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.AGMNT_PRICE_AMT"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ALW_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.CHRG_AMT"
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.COINS_AMT"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.CNSD_CHRG_AMT"
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.COPAY_AMT"
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.DEDCT_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.DSALW_AMT"
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ITS_HOME_DSCNT_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.NO_RESP_AMT"
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.MBR_LIAB_BSS_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PATN_RESP_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PAYBL_AMT"
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PAYBL_TO_PROV_AMT"
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PAYBL_TO_SUB_AMT"
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PROC_TBL_PRICE_AMT"
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PROFL_PRICE_AMT"
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PROV_WRT_OFF_AMT"
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.RISK_WTHLD_AMT"
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.SVC_PRICE_AMT"
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.SUPLMT_DSCNT_AMT"
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "Transform.ALW_PRICE_UNIT_CT"
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
              map: "Transform.UNIT_CT"
            - name: DEDCT_AMT_ACCUM_ID
              type: char
              width: 4
              nullable: N
              map: "Transform.DEDCT_AMT_ACCUM_ID"
            - name: PREAUTH_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
              map: "Transform.PREAUTH_SVC_SEQ_NO"
            - name: RFRL_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
              map: "Transform.RFRL_SVC_SEQ_NO"
            - name: LMT_PFX_ID
              type: char
              width: 4
              nullable: N
              map: "Transform.LMT_PFX_ID"
            - name: PREAUTH_ID
              type: char
              width: 9
              nullable: N
              map: "Transform.PREAUTH_ID"
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: char
              width: 4
              nullable: N
              map: "Transform.PROD_CMPNT_DEDCT_PFX_ID"
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: char
              width: 4
              nullable: N
              map: "Transform.PROD_CMPNT_SVC_PAYMT_ID"
            - name: RFRL_ID_TX
              type: char
              width: 9
              nullable: N
              map: "Transform.RFRL_ID_TX"
            - name: SVC_ID
              type: char
              width: 4
              nullable: N
              map: "Transform.SVC_ID"
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
              map: "Transform.SVC_PRICE_RULE_ID"
            - name: SVC_RULE_TYP_TX
              type: char
              width: 3
              nullable: N
              map: "Transform.SVC_RULE_TYP_TX"
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
              map: "Transform.SVC_LOC_TYP_CD"
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.NON_PAR_SAV_AMT"
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.PROC_CD_TYP_CD"
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.PROC_CD_CAT_CD"
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
              map: "'NA'"
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
              map: "'NA'"
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
              map: "'N'"
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "0.00"
            - name: NDC
              type: varchar
              width: 20
              map: "Transform.NDC"
            - name: NDC_DRUG_FORM_CD
              type: varchar
              width: 20
              map: "'NA'"
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
              map: "@Null"
            - name: MED_PDX_IND
              type: char
              width: 3
              map: "'PDX'"
            - name: APC_ID
              type: varchar
              width: 20
              map: "Transform.APC_ID"
            - name: APC_STTUS_ID
              type: varchar
              width: 20
              map: "Transform.APC_STTUS_ID"
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
              map: "Transform.SNOMED_CT_CD"
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
              map: "Transform.CVX_VCCN_CD"
        - name: Snapshot.V0S133P3
          type: map
          source: BusinessRules
          alias: Transform
          schema: 
            - name: CLCL_ID
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
              map: "Transform.CLM_LN_SEQ_NO"
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.PROC_CD"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.CLM_LN_RVNU_CD"
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.ALW_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.CHRG_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Transform.PAYBL_AMT"
            - name: CLM_TYPE
              type: char
              width: 1
              nullable: N
              map: "Transform.CLM_TYPE"
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.PROC_CD_TYP_CD"
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
              map: "Transform.PROC_CD_CAT_CD"
          keys:
            - CLCL_ID
        - name: Transformer
          type: map
          source: Snapshot.V0S133P3
          alias: Snapshot
          stageVars: 
            - name: ProcCdSk
              type: decimal
              width: 0
              map: "GetFkeyProcCd(\"FACETS\", 0, Snapshot.PROC_CD[1, 5], Snapshot.PROC_CD_TYP_CD, Snapshot.PROC_CD_CAT_CD, 'X')"
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: decimal
              width: 0
              map: "GetFkeyRvnu(\"FACETS\", 0, Snapshot.CLM_LN_RVNU_CD, 'X')"
              initValue: "0"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: SrcSysCdSk
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.CLCL_ID"
            - name: CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: "Snapshot.CLM_LN_SEQ_NO"
            - name: PROC_CD_SK
              type: int
              width: 4
              nullable: N
              map: ProcCdSk
            - name: CLM_LN_RVNU_CD_SK
              type: int
              width: 4
              nullable: N
              map: ClmLnRvnuCdSk
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.ALW_AMT"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.CHRG_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.PAYBL_AMT"
            - name: ProcCdSk
              type: varchar
              width: 0
              map: ProcCdSk
              dropStageVar: true
              initValue: "0"
            - name: ClmLnRvnuCdSk
              type: varchar
              width: 0
              map: ClmLnRvnuCdSk
              dropStageVar: true
              initValue: "0"
        - name: ClmLnPK
          type: sharedContainer
          containerName: ClmLnPK
          parameters:
            - name: "CurrRunCycle"
              expression: "#RunCycle#"
          schema: 
            - name: JOB_EXCTN_RCRD_ERR_SK
              type: int
              width: 4
              nullable: N
            - name: INSRT_UPDT_CD
              type: char
              width: 10
              nullable: N
            - name: DISCARD_IN
              type: char
              width: 1
              nullable: N
            - name: PASS_THRU_IN
              type: char
              width: 1
              nullable: N
            - name: FIRST_RECYC_DT
              type: timestamp
              nullable: N
            - name: ERR_CT
              type: smallint
              width: 2
              nullable: N
            - name: RECYCLE_CT
              type: decimal
              width: 6
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: PRI_KEY_STRING
              type: varchar
              width: 1023
              nullable: N
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_SEQ_NO
              type: smallint
              width: 2
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_DSALW_EXCD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_EOB_EXCD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_LOB_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PREAUTH_SRC_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_PRICE_SRC_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: char
              width: 2
              nullable: N
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CLM_LN_UNIT_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: CAP_LN_IN
              type: char
              width: 1
              nullable: N
            - name: PRI_LOB_IN
              type: char
              width: 1
              nullable: N
            - name: SVC_END_DT
              type: timestamp
              nullable: N
            - name: SVC_STRT_DT
              type: timestamp
              nullable: N
            - name: AGMNT_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_HOME_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: MBR_LIAB_BSS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PAYBL_TO_SUB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_TBL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROFL_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: RISK_WTHLD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SVC_PRICE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ALW_PRICE_UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: UNIT_CT
              type: smallint
              width: 2
              nullable: N
            - name: DEDCT_AMT_ACCUM_ID
              type: char
              width: 4
              nullable: N
            - name: PREAUTH_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
            - name: RFRL_SVC_SEQ_NO
              type: char
              width: 4
              nullable: N
            - name: LMT_PFX_ID
              type: char
              width: 4
              nullable: N
            - name: PREAUTH_ID
              type: char
              width: 9
              nullable: N
            - name: PROD_CMPNT_DEDCT_PFX_ID
              type: char
              width: 4
              nullable: N
            - name: PROD_CMPNT_SVC_PAYMT_ID
              type: char
              width: 4
              nullable: N
            - name: RFRL_ID_TX
              type: char
              width: 9
              nullable: N
            - name: SVC_ID
              type: char
              width: 4
              nullable: N
            - name: SVC_PRICE_RULE_ID
              type: char
              width: 4
              nullable: N
            - name: SVC_RULE_TYP_TX
              type: char
              width: 3
              nullable: N
            - name: SVC_LOC_TYP_CD
              type: char
              width: 20
              nullable: N
            - name: NON_PAR_SAV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: PROC_CD_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: PROC_CD_CAT_CD
              type: varchar
              width: 20
              nullable: N
            - name: VBB_RULE_ID
              type: varchar
              width: 4
              nullable: N
            - name: VBB_EXCD_ID
              type: varchar
              width: 3
              nullable: N
            - name: CLM_LN_VBB_IN
              type: char
              width: 1
              nullable: N
            - name: ITS_SUPLMT_DSCNT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: ITS_SRCHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: NDC
              type: varchar
              width: 20
            - name: NDC_DRUG_FORM_CD
              type: varchar
              width: 20
            - name: NDC_UNIT_CT
              type: decimal
              width: 21
              precision: 10
            - name: MED_PDX_IND
              type: char
              width: 3
            - name: APC_ID
              type: varchar
              width: 20
            - name: APC_STTUS_ID
              type: varchar
              width: 20
            - name: SNOMED_CT_CD
              type: varchar
              width: 20
            - name: CVX_VCCN_CD
              type: varchar
              width: 20
          source: Snapshot.V0S133P2


