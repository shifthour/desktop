model:
  config:
    origin: datastage
  jobs:
    - name: OptumIdsMbrPdxDeniedClmXfm
      origin: datastage
      type: dataflow
      folder: "DataWarehouse/Claim/DeniedPDXClaims"
      runAfterSubrOnJobSuccess: enabled
      description: "Extract OPTUMRX  Claims denied data, do Member and provider matching details"
      fullDescription: "*****************************************************************************************************************************************************************************\nCOPYRIGHT 2019 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nDESCRIPTION:  OPTUMRX  Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table\n\n\nCalled By: OptumIdsMbrPdxDeniedTransLoadSeq\n\nMODIFICATIONS:\nDeveloper                 Date                  Project/Altiris #                                                     Change Description                                                      Development Project           Code Reviewer                \n------------------              --------------------     ------------------------                                                    --------------------------------------------------------------                      --------------------------------------         --------------------------------\nPeter Gichiri              2019-10-27      6131 - PBM REPLACEMENT  to OPTUMRX     Original Devlopment Integrate                                       IntegrateDev2                         Kalyan Neelam        2019-11-21\n\nRekha Radhakrishna 2020-10-12  Phase II Government Programs                             TRANS_MO_NO derivation changed                           IntegrateDev2\n                                                                                                                                      to show two digit month\nRekha Radhakrishna 2020-11-05 Phase II Government Programs                               Changed Member Matching logic for Med-D                IntegrateDev2                        Kalyan Neelam         2020-12-10"
      parameters:
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: APT_CONFIG_FILE
          type: string
          description: "Configuration file"
          longDescription: "The Parallel job configuration file."
          defaultType: project
        - name: FilePath
          type: string
          description: "File Path"
          defaultType: project
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: IDSDB2ArraySize
          type: string
          description: "IDS DB2 Array Size for Load"
          defaultType: project
        - name: IDSDB2RecordCount
          type: string
          description: "IDS DB2 Record Count for Load"
          defaultType: project
        - name: IDSRunCycle
          type: integer
          description: IDSRunCycle
        - name: SrcSysCd
          type: string
          description: SrcSysCd
        - name: RunID
          type: string
          description: RunID
        - name: CurrDate
          type: string
          description: CurrDate
      steps:
        - name: Lkp_prov
          type: lookup
          option:
            joinType: left
          sources: 
            - name: Jn_Mbr
              alias: Lnk_mbr
              joinFilter: ""
              joinExpression: []
            - name: db2_prov_dea
              alias: lnk_prov_extr
              joinFilter: ""
              joinExpression: []
          schema: 
            - name: CLNT_ORG_OPTRNAL_ID
              type: varchar
              width: 20
              map: "Lnk_mbr.CLNT_ORG_OPTRNAL_ID"
            - name: CLNT_ORG_NM
              type: varchar
              width: 20
              map: "Lnk_mbr.CLNT_ORG_NM"
            - name: MAIL_ORDER_IN
              type: varchar
              width: 100
              map: "Lnk_mbr.MAIL_ORDER_IN"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 50
              map: "Lnk_mbr.SRC_SYS_CAR_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              map: "Lnk_mbr.TRANS_TYP_CD"
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
              map: "Lnk_mbr.CLNT_MBRSH_ID"
            - name: PATN_ID
              type: varchar
              width: 20
              map: "Lnk_mbr.PATN_ID"
            - name: CLNT_ID
              type: varchar
              width: 22
              map: "Lnk_mbr.CLNT_ID"
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
              map: "Lnk_mbr.CLNT_ELIG_MBRSH_ID"
            - name: MBR_SSN
              type: varchar
              width: 20
              map: "Lnk_mbr.MBR_SSN"
            - name: PRSN_NO
              type: varchar
              width: 20
              map: "Lnk_mbr.PRSN_NO"
            - name: RELSHP_CD
              type: varchar
              width: 20
              map: "Lnk_mbr.RELSHP_CD"
            - name: GNDR_CD
              type: varchar
              width: 20
              map: "Lnk_mbr.GNDR_CD"
            - name: SUB_BRTH_DT
              type: varchar
              width: 35
              map: "Lnk_mbr.SUB_BRTH_DT"
            - name: MBR_BRTH_DT
              type: varchar
              width: 35
              map: "Lnk_mbr.MBR_BRTH_DT"
            - name: MBR_FIRST_NM
              type: varchar
              width: 100
              map: "Lnk_mbr.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 100
              map: "Lnk_mbr.MBR_LAST_NM"
            - name: SUBMT_SUB_FIRST_NM
              type: varchar
              width: 100
              map: "Lnk_mbr.SUBMT_SUB_FIRST_NM"
            - name: SUBMT_SUB_LAST_NM
              type: varchar
              width: 100
              map: "Lnk_mbr.SUBMT_SUB_LAST_NM"
            - name: RX_LABEL_TX
              type: varchar
              width: 200
              map: "Lnk_mbr.RX_LABEL_TX"
            - name: SVC_DT
              type: date
              map: "Lnk_mbr.SVC_DT"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 30
              map: "Lnk_mbr.SRC_SYS_GRP_ID"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              map: "Lnk_mbr.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              map: "Lnk_mbr.SRC_SYS_CLM_RCVD_TM"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "Lnk_mbr.MSG_TYP_CD"
            - name: MSG_TYP
              type: varchar
              width: 20
              map: "Lnk_mbr.MSG_TYP"
            - name: MSG_TX
              type: varchar
              width: 200
              map: "Lnk_mbr.MSG_TX"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_mbr.INGR_CST_ALW_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_mbr.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_mbr.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_mbr.BILL_RX_PATN_PAY_AMT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_mbr.BILL_RX_DISPENSE_FEE_AMT"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "Lnk_mbr.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "Lnk_mbr.RSPN_CD"
            - name: DESC
              type: varchar
              width: 500
              map: "Lnk_mbr.DESC"
            - name: CHAPTER_ID
              type: varchar
              width: 20
              map: "Lnk_mbr.CHAPTER_ID"
            - name: CHAPTER_DESC
              type: varchar
              width: 500
              map: "Lnk_mbr.CHAPTER_DESC"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              map: "Lnk_mbr.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "Lnk_mbr.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 100
              map: "Lnk_mbr.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 100
              map: "Lnk_mbr.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 250
              map: "Lnk_mbr.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 100
              map: "Lnk_mbr.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 10
              map: "Lnk_mbr.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              map: "Lnk_mbr.PHYS_POSTAL_CD"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "Lnk_mbr.PDX_NTNL_PROV_ID"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 20
              map: "Lnk_mbr.SVC_PROV_NABP_NM"
            - name: PDX_NM
              type: varchar
              width: 200
              map: "Lnk_mbr.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              map: "Lnk_mbr.PDX_PHN_NO"
            - name: NDC
              type: varchar
              width: 20
              map: "Lnk_mbr.NDC"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              map: "Lnk_mbr.MBR_UNIQ_KEY"
            - name: MBR_SK
              type: int
              width: 4
              map: "Lnk_mbr.MBR_SK"
            - name: GRP_SK
              type: int
              width: 4
              map: "Lnk_mbr.GRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              map: "Lnk_mbr.SUB_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              map: "Lnk_mbr.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              map: "Lnk_mbr.MBR_RELSHP_CD_SK"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Lnk_mbr.MBR_SFX_NO"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_prov_extr.PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_prov_extr.PROV_SPEC_CD_SK"
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_prov_extr.PROV_DEA_SK"
            - name: EFF_DT_SK
              type: varchar
              width: 20
              nullable: N
              map: "lnk_prov_extr.EFF_DT_SK"
            - name: TERM_DT_SK
              type: varchar
              width: 20
              nullable: N
              map: "lnk_prov_extr.TERM_DT_SK"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              map: "Lnk_mbr.SRC_SYS_CD"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Lnk_mbr.CLMORIGIN"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Lnk_mbr.RX_NO"
            - name: MBR_ID
              type: varchar
              width: 22
              map: "Lnk_mbr.MBR_ID"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              map: "Lnk_mbr.SRC_SYS_CNTR_ID"
            - name: GRP_ID
              type: varchar
              width: 20
              map: "Lnk_mbr.GRP_ID"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              map: "Lnk_mbr.PDX_RSPN_RSN_CD_SK"
            - name: PRCS_DT
              type: date
              map: "Lnk_mbr.PRCS_DT"
            - name: LOB_IN
              type: varchar
              width: 5
              nullable: N
              map: "Lnk_mbr.LOB_IN"
        - name: OptumRxDeniedInputFile
          type: fileReader
          urls: 
            - "#$FilePath#/verified/#SrcSysCd#_DeniedClaims_Landing.dat.#RunID#"
          rejectMode: continue
          format: 
            header: false
            quotes: "^"
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
          schema: 
            - name: MEMBERID
              type: varchar
              width: 20
            - name: ACCOUNTID
              type: varchar
              width: 15
            - name: NPIPROV
              type: varchar
              width: 10
            - name: DENIALDTE
              type: varchar
              width: 10
            - name: RXNUMBER
              type: varchar
              width: 12
            - name: DATESBM
              type: date
            - name: TIMESBM
              type: varchar
              width: 8
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: MBRSEX
              type: varchar
              width: 1
            - name: MBRRELCDE
              type: varchar
              width: 1
            - name: REJCDE1
              type: varchar
              width: 3
            - name: REJCDE2
              type: varchar
              width: 3
            - name: RXNETWRKQL
              type: varchar
              width: 1
            - name: MBRBIRTH
              type: date
            - name: PHRDISPFEE
              type: decimal
              width: 11
              precision: 2
            - name: CLTDUEAMT
              type: decimal
              width: 11
              precision: 2
            - name: CLT2DUEAMT
              type: decimal
              width: 11
              precision: 2
            - name: CLTPATPAY
              type: decimal
              width: 11
              precision: 2
            - name: CLTINGRCST
              type: decimal
              width: 11
              precision: 2
            - name: MBRFSTNME
              type: varchar
              width: 15
            - name: MBRLSTNME
              type: varchar
              width: 25
            - name: SOCSECNBR
              type: varchar
              width: 9
            - name: SRVPROVNME
              type: varchar
              width: 55
            - name: PRESCDEAID
              type: varchar
              width: 15
            - name: NPIPRESCR
              type: varchar
              width: 10
            - name: PPRSFSTNME
              type: varchar
              width: 15
            - name: PPRSLSTNME
              type: varchar
              width: 25
            - name: PPRSTATE
              type: varchar
              width: 3
            - name: LABELNAME
              type: varchar
              width: 30
            - name: SRVPROVID
              type: varchar
              width: 15
            - name: CARID
              type: varchar
              width: 9
            - name: GROUPID
              type: varchar
              width: 15
            - name: CLMORIGIN
              type: varchar
              width: 1
            - name: RXCLMNBR
              type: decimal
              width: 15
            - name: PRODUCTID
              type: varchar
              width: 20
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
            - name: LOB_IN
              type: varchar
              width: 5
              nullable: N
        - name: Seq_error
          type: fileWriter
          urls: 
            - "#$FilePath#/load/MBR_PDX_DENIED_TRANS_ERROR.#CurrDate#.dat"
          source: "xfm_optumrx.V0S26P6"
          rejectMode: continue
          option: 
            updateMode: overwrite
            writeMode: SingleFile
          format: 
            header: true
            quotes: "\""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
        - name: ds_MBR_PDX_DENIED_TRANS_xfm
          type: datasetWriter
          urls: 
            - "#$FilePath#/ds/MBR_PDX_DENIED_TRANS.#SrcSysCd#.xfrm.#RunID#.ds"
          source: Rv_Natural_Keys
          option: 
            writeMode: SingleFile
            updateMode: Overwrite
        - name: Rv_Natural_Keys
          type: dedup
          dedupKey: 
            - name: MBR_UNIQ_KEY
            - name: PDX_NTNL_PROV_ID
            - name: TRANS_TYP_CD
            - name: TRANS_DENIED_DT
            - name: RX_NO
            - name: PRCS_DT
            - name: SRC_SYS_CLM_RCVD_DT
            - name: SRC_SYS_CLM_RCVD_TM
            - name: SRC_SYS_CD
          source: xfm_OptumRx_pdx
          alias: Lnk_OptumRx_mbr_denied_tgt
          option:
            retain: first
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: varchar
              width: 1
              map: "Lnk_OptumRx_mbr_denied_tgt.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              map: "Lnk_OptumRx_mbr_denied_tgt.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              map: "Lnk_OptumRx_mbr_denied_tgt.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_tgt.SUB_LAST_NM"
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.CLNT_ELIG_MBRSH_ID"
            - name: PRSN_NO
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.PRSN_NO"
            - name: RELSHP_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.RELSHP_CD"
            - name: GNDR_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.GNDR_CD"
            - name: CLM_RCVD_TM
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.CLM_RCVD_TM"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "Lnk_OptumRx_mbr_denied_tgt.MSG_TYP_CD"
            - name: MSG_TYP
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.MSG_TYP"
            - name: MSG_TX
              type: varchar
              width: 200
              map: "Lnk_OptumRx_mbr_denied_tgt.MSG_TX"
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
              map: "Lnk_OptumRx_mbr_denied_tgt.CLNT_MBRSH_ID"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 1
              map: "Lnk_OptumRx_mbr_denied_tgt.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.RSPN_CD"
            - name: DESC
              type: varchar
              width: 500
              map: "Lnk_OptumRx_mbr_denied_tgt.DESC"
            - name: CHAPTER_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.CHAPTER_ID"
            - name: CHAPTER_DESC
              type: varchar
              width: 500
              map: "Lnk_OptumRx_mbr_denied_tgt.CHAPTER_DESC"
            - name: PDX_NPI
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.PDX_NPI"
            - name: NDC
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_tgt.NDC"
          keys:
            - MBR_PDX_DENIED_TRANS_SK
        - name: db2_MBR
          type: dbReader
          originalDb: db2
          query: "SELECT\nS.SUB_ID||M.MBR_SFX_NO AS MBR_ID,\nM.MBR_SK AS MBR_SK,\nM.FIRST_NM AS SUB_FIRST_NM,\nM.LAST_NM AS SUB_LAST_NM,\nM.BRTH_DT_SK AS SUB_BRTH_DT,\nS.GRP_SK AS GRP_SK,\nS.SUB_SK AS SUB_SK,\nG.GRP_ID as ACCOUNTID,\nM.MBR_GNDR_CD_SK MBR_GNDR_CD_SK,\nM.MBR_RELSHP_CD_SK MBR_RELSHP_CD_SK\n----M.MBR_SFX_NO MBR_SFX_NO \nFROM #$IDSOwner#.MBR M, #$IDSOwner#.SUB S, #$IDSOwner#.GRP G\n\nWHERE\nS.SUB_SK = M.SUB_SK\nAND \nS.GRP_SK = G.GRP_SK\n\nORDER BY  S.SUB_ID||M.MBR_SFX_NO, G.GRP_ID\n with ur;"
          schema: 
            - name: MBR_ID
              type: varchar
              width: 22
              nullable: N
            - name: MBR_SK
              type: int
              width: 4
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
            - name: SUB_LAST_NM
              type: varchar
              width: 75
            - name: GRP_SK
              type: int
              width: 4
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: ACCOUNTID
              type: varchar
              width: 20
            - name: SUB_BRTH_DT
              type: varchar
              width: 35
        - name: db2_PROV
          type: dbReader
          originalDb: db2
          query: "    SELECT \n               PROV.PROV_SK AS PROV_SK_SRV_PROV_SK,\n               PROV.SRC_SYS_CD_SK AS SRC_SYS_CD_SK_SRV_PROV_SK,\n               PROV.PROV_ID AS SVC_PROV_NABP_NM        \n               \n               FROM #$IDSOwner#.PROV PROV               \n               INNER JOIN #$IDSOwner#.CD_MPPNG CD_MPPNG\n               ON PROV.SRC_SYS_CD_SK=CD_MPPNG.CD_MPPNG_SK \n               WHERE   CD_MPPNG.TRGT_CD = 'NABP'\n\nORDER BY  PROV_ID ASC with ur;"
          schema: 
            - name: PROV_SK_SRV_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SRC_SYS_CD_SK_SRV_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 20
              nullable: N
        - name: db2_prov_dea
          type: dbReader
          originalDb: db2
          query: "SELECT   \n         MAX(PROV.PROV_SK) AS PROV_SK,      \n         PROV_DEA.PROV_DEA_SK,        \n         PROV.PROV_SPEC_CD_SK AS PROV_SPEC_CD_SK,\x09\n         ----PROV_DEA.DEA_NO AS PHYS_DEA_NO,\n         PROV_DEA.NTNL_PROV_ID AS NPI_NO,\n         PROV_DEA.EFF_DT_SK AS EFF_DT_SK,\n         PROV_DEA.TERM_DT_SK AS TERM_DT_SK\nFROM     \n         #$IDSOwner#.PROV_DEA PROV_DEA \n      INNER JOIN #$IDSOwner#.PROV PROV \n      ON PROV_DEA.CMN_PRCT_SK=PROV.CMN_PRCT_SK AND\n      PROV_DEA.NTNL_PROV_ID=PROV.NTNL_PROV_ID \n      INNER JOIN (SELECT\n --                  MAX(PROV.PROV_SK) PROV_SK\n                   PROV.CMN_PRCT_SK\n                  , PROV.NTNL_PROV_ID\n                  , MAX(PROV.TERM_DT_SK) AS TERM_DT_SK \n               FROM\n                  #$IDSOwner#.PROV PROV \n               GROUP BY\n                 PROV.CMN_PRCT_SK\n                  , PROV.NTNL_PROV_ID ) A \n      ON PROV.CMN_PRCT_SK = A.CMN_PRCT_SK AND\n--      PROV.PROV_SK = A.PROV_SK AND\n      PROV.NTNL_PROV_ID = A.NTNL_PROV_ID AND\n      PROV.TERM_DT_SK = A.TERM_DT_SK\n\x09  \nWHERE  PROV_DEA.CMN_PRCT_SK  NOT IN ('0','1')\nAND PROV.TERM_DT_SK >= ORCHESTRATE.SVC_DT\nAND PROV_DEA.EFF_DT_SK <= ORCHESTRATE.SVC_DT\nAND PROV_DEA.TERM_DT_SK >= ORCHESTRATE.SVC_DT\nAND PROV_DEA.DEA_NO= ORCHESTRATE.PHYS_DEA_NO\nAND PROV_DEA.NTNL_PROV_ID = ORCHESTRATE.PHYS_NTNL_PROV_ID\n\nGROUP BY \n         PROV_DEA.PROV_DEA_SK, \n         PROV.PROV_SPEC_CD_SK, \n         PROV_DEA.DEA_NO, \n         PROV_DEA.NTNL_PROV_ID, \n         PROV_DEA.EFF_DT_SK, \n         PROV_DEA.TERM_DT_SK with ur;"
          schema: 
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_DEA_SK
              type: int
              width: 4
              nullable: N
            - name: NPI_NO
              type: varchar
              width: 20
              nullable: N
            - name: EFF_DT_SK
              type: char
              width: 10
              nullable: N
            - name: TERM_DT_SK
              type: char
              width: 10
              nullable: N
          keys:
            - EFF_DT_SK
            - TERM_DT_SK
        - name: xfm_optumrx.V0S26P5.map
          type: map
          source: xfmDataClean
          alias: toOptumxfm
          stageVars: 
            - name: svPdxNtnlProvId
              type: varchar
              width: 255
              map: " IF (IsNull(toOptumxfm.PDX_NTNL_PROV_ID) Or Len(Trim(toOptumxfm.PDX_NTNL_PROV_ID)) = 0) THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svTransDeniedDt
              type: varchar
              width: 255
              map: " IF toOptumxfm.TRANS_DENIED_DT = \"1753-01-01\" THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svRxNo
              type: varchar
              width: 255
              map: " IF (IsNull(toOptumxfm.RX_NO) Or Len(Trim(toOptumxfm.RX_NO)) = 0) THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svMbrUniqKey
              type: varchar
              width: 255
              map: " IF (IsNull(toOptumxfm.MBR_UNIQ_KEY) Or Len(Trim(toOptumxfm.MBR_UNIQ_KEY)) = 0) Or Trim(toOptumxfm.MBR_UNIQ_KEY) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svSrcSysClmRcdDt
              type: varchar
              width: 255
              map: " IF toOptumxfm.TRANS_DENIED_DT = \"1753-01-01\" THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svSrcSysClmRcdTm
              type: varchar
              width: 255
              map: " IF (IsNull(toOptumxfm.SRC_SYS_CLM_RCVD_TM) Or Len(Trim(toOptumxfm.SRC_SYS_CLM_RCVD_TM)) = 0) THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              map: "toOptumxfm.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              map: "TRIM(toOptumxfm.MBR_UNIQ_KEY)"
            - name: ACCOUNTID
              type: char
              width: 20
              map: "toOptumxfm.ACCOUNTID"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "toOptumxfm.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              map: "toOptumxfm.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              map: "toOptumxfm.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              map: "toOptumxfm.RX_NO"
            - name: PRCS_DT
              type: date
              map: "toOptumxfm.PRCS_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              map: "toOptumxfm.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              map: "toOptumxfm.SRC_SYS_CLM_RCVD_TM"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              map: "toOptumxfm.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: varchar
              width: 3
              nullable: N
              map: "toOptumxfm.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              map: "toOptumxfm.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: char
              width: 0
              nullable: N
              map: "toOptumxfm.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: varchar
              width: 100
              map: "toOptumxfm.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: varchar
              width: 35
              map: "toOptumxfm.MBR_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "toOptumxfm.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              map: "toOptumxfm.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              map: "toOptumxfm.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "toOptumxfm.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              map: "toOptumxfm.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 22
              map: "toOptumxfm.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "toOptumxfm.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 100
              map: "toOptumxfm.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 100
              map: "toOptumxfm.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              map: "toOptumxfm.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 200
              map: "toOptumxfm.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              map: "toOptumxfm.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              map: "toOptumxfm.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "toOptumxfm.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 100
              map: "toOptumxfm.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 100
              map: "toOptumxfm.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 250
              map: "toOptumxfm.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 100
              map: "toOptumxfm.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 10
              map: "toOptumxfm.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              map: "toOptumxfm.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 200
              map: " If IsNull(toOptumxfm.RX_LABEL_TX) or Len(toOptumxfm.RX_LABEL_TX) = 0 Then 'UNK' ELSE UpCase(TRIM(toOptumxfm.RX_LABEL_TX))"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 20
              map: "TRIM(toOptumxfm.SVC_PROV_NABP_NM)"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 50
              map: "toOptumxfm.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "toOptumxfm.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 30
              map: "toOptumxfm.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "toOptumxfm.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "toOptumxfm.SUB_LAST_NM"
            - name: NDC
              type: varchar
              width: 20
              map: "toOptumxfm.NDC"
            - name: MEMBERID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.MEMBERID"
            - name: GRP_ID
              type: varchar
              width: 20
              map: "TRIM(toOptumxfm.GRP_ID)"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "TRIM(toOptumxfm.CLMORIGIN)"
            - name: LOB_IN
              type: varchar
              width: 5
              nullable: N
              map: "toOptumxfm.LOB_IN"
            - name: CLNT_ORG_OPTRNAL_ID
              type: varchar
              width: 20
              map: "toOptumxfm.SRC_SYS_CLNT_ORG_ID"
            - name: CLNT_ORG_NM
              type: varchar
              width: 20
              map: "TRIM(toOptumxfm.SRC_SYS_CLNT_ORG_NM)"
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
              map: "''"
            - name: PATN_ID
              type: varchar
              width: 20
              map: "toOptumxfm.MEMBERID"
            - name: CLNT_ID
              type: varchar
              width: 22
              map: " If toOptumxfm.LOB_IN = 'MCARE' Then toOptumxfm.SUB_ID Else toOptumxfm.SUB_ID[1, 9]"
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
              map: "''"
            - name: PRSN_NO
              type: varchar
              width: 20
              map: "''"
            - name: RELSHP_CD
              type: varchar
              width: 20
              map: "toOptumxfm.MBR_RELSHP_CD_SK"
            - name: GNDR_CD
              type: varchar
              width: 20
              map: "toOptumxfm.MBR_GNDR_CD_SK"
            - name: SVC_DT
              type: date
              map: "toOptumxfm.TRANS_DENIED_DT"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "toOptumxfm.TRANS_TYP_CD_SK"
            - name: MSG_TYP
              type: varchar
              width: 20
              map: "''"
            - name: MSG_TX
              type: varchar
              width: 200
              map: "''"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "toOptumxfm.MEDIA_TYP_CD_SK"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "''"
            - name: DESC
              type: varchar
              width: 500
              map: "''"
            - name: CHAPTER_ID
              type: varchar
              width: 20
              map: "''"
            - name: CHAPTER_DESC
              type: varchar
              width: 500
              map: "''"
            - name: svPdxNtnlProvId
              type: varchar
              width: 255
              map: svPdxNtnlProvId
              initValue: ""
            - name: svTransDeniedDt
              type: varchar
              width: 255
              map: svTransDeniedDt
              initValue: ""
            - name: svRxNo
              type: varchar
              width: 255
              map: svRxNo
              initValue: ""
            - name: svMbrUniqKey
              type: varchar
              width: 255
              map: svMbrUniqKey
              initValue: ""
            - name: svSrcSysClmRcdDt
              type: varchar
              width: 255
              map: svSrcSysClmRcdDt
              initValue: ""
            - name: svSrcSysClmRcdTm
              type: varchar
              width: 255
              map: svSrcSysClmRcdTm
              initValue: ""
        - name: xfm_optumrx.V0S26P5
          type: filter
          source: xfm_optumrx.V0S26P5.map
          filter: "svPdxNtnlProvId = 'N' AND svTransDeniedDt = 'N' AND svRxNo = 'N' AND svMbrUniqKey = 'N' AND svSrcSysClmRcdDt = 'N' AND svSrcSysClmRcdTm = 'N'"
          schema: 
            - name: CLNT_ORG_OPTRNAL_ID
              type: varchar
              width: 20
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
            - name: ACCOUNTID
              type: char
              width: 20
            - name: CLNT_ORG_NM
              type: varchar
              width: 20
            - name: MAIL_ORDER_IN
              type: varchar
              width: 100
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 50
            - name: RX_NO
              type: varchar
              width: 20
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
            - name: PATN_ID
              type: varchar
              width: 20
            - name: CLNT_ID
              type: varchar
              width: 22
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: MBR_SSN
              type: varchar
              width: 20
            - name: PRSN_NO
              type: varchar
              width: 20
            - name: RELSHP_CD
              type: varchar
              width: 20
            - name: GNDR_CD
              type: varchar
              width: 20
            - name: MBR_BRTH_DT
              type: varchar
              width: 35
            - name: MBR_FIRST_NM
              type: varchar
              width: 100
            - name: MBR_LAST_NM
              type: varchar
              width: 100
            - name: RX_LABEL_TX
              type: varchar
              width: 200
            - name: SVC_DT
              type: date
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 30
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
            - name: MSG_TYP_CD
              type: varchar
              width: 10
            - name: MSG_TYP
              type: varchar
              width: 20
            - name: MSG_TX
              type: varchar
              width: 200
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
            - name: RSPN_CD
              type: varchar
              width: 20
            - name: DESC
              type: varchar
              width: 500
            - name: CHAPTER_ID
              type: varchar
              width: 20
            - name: CHAPTER_DESC
              type: varchar
              width: 500
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: PHYS_FIRST_NM
              type: varchar
              width: 100
            - name: PHYS_LAST_NM
              type: varchar
              width: 100
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 250
            - name: PHYS_CITY_NM
              type: varchar
              width: 100
            - name: PHYS_ST_CD
              type: varchar
              width: 10
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 20
            - name: PDX_NM
              type: varchar
              width: 200
            - name: PDX_PHN_NO
              type: varchar
              width: 20
            - name: NDC
              type: varchar
              width: 20
            - name: MBR_SFX_NO
              type: char
              width: 2
            - name: MBR_ID
              type: varchar
              width: 22
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
            - name: GRP_ID
              type: varchar
              width: 20
            - name: CLMORIGIN
              type: char
              width: 1
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
            - name: PRCS_DT
              type: date
            - name: LOB_IN
              type: varchar
              width: 5
              nullable: N
        - name: xfm_optumrx.V0S26P6.map
          type: map
          source: xfmDataClean
          alias: toOptumxfm
          stageVars: 
            - name: svPdxNtnlProvId
              type: varchar
              width: 255
              map: " IF (IsNull(toOptumxfm.PDX_NTNL_PROV_ID) Or Len(Trim(toOptumxfm.PDX_NTNL_PROV_ID)) = 0) THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svTransDeniedDt
              type: varchar
              width: 255
              map: " IF toOptumxfm.TRANS_DENIED_DT = \"1753-01-01\" THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svRxNo
              type: varchar
              width: 255
              map: " IF (IsNull(toOptumxfm.RX_NO) Or Len(Trim(toOptumxfm.RX_NO)) = 0) THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svMbrUniqKey
              type: varchar
              width: 255
              map: " IF (IsNull(toOptumxfm.MBR_UNIQ_KEY) Or Len(Trim(toOptumxfm.MBR_UNIQ_KEY)) = 0) Or Trim(toOptumxfm.MBR_UNIQ_KEY) = 0 THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svSrcSysClmRcdDt
              type: varchar
              width: 255
              map: " IF toOptumxfm.TRANS_DENIED_DT = \"1753-01-01\" THEN 'Y' ELSE 'N'"
              initValue: ""
            - name: svSrcSysClmRcdTm
              type: varchar
              width: 255
              map: " IF (IsNull(toOptumxfm.SRC_SYS_CLM_RCVD_TM) Or Len(Trim(toOptumxfm.SRC_SYS_CLM_RCVD_TM)) = 0) THEN 'Y' ELSE 'N'"
              initValue: ""
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              map: "toOptumxfm.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              map: "toOptumxfm.MBR_UNIQ_KEY"
            - name: ACCOUNTID
              type: varchar
              width: 20
              map: "toOptumxfm.ACCOUNTID"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "TRIM(toOptumxfm.PDX_NTNL_PROV_ID)"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              map: "TRIM(toOptumxfm.TRANS_TYP_CD)"
            - name: TRANS_DENIED_DT
              type: date
              map: "toOptumxfm.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              map: "TRIM(toOptumxfm.RX_NO)"
            - name: PRCS_DT
              type: varchar
              width: 20
              map: "toOptumxfm.PRCS_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              map: "toOptumxfm.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              map: "toOptumxfm.SRC_SYS_CLM_RCVD_TM"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              map: "toOptumxfm.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: varchar
              width: 3
              nullable: N
              map: "toOptumxfm.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              nullable: N
              map: "toOptumxfm.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: char
              width: 0
              nullable: N
              map: "toOptumxfm.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 0
              nullable: N
              map: "toOptumxfm.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "toOptumxfm.MBR_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "toOptumxfm.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              map: "toOptumxfm.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "toOptumxfm.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "toOptumxfm.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "toOptumxfm.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "toOptumxfm.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 22
              nullable: N
              map: "toOptumxfm.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "toOptumxfm.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "toOptumxfm.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "toOptumxfm.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "toOptumxfm.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "toOptumxfm.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "toOptumxfm.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "toOptumxfm.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "toOptumxfm.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "toOptumxfm.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "toOptumxfm.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "toOptumxfm.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "toOptumxfm.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "toOptumxfm.SUB_LAST_NM"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.NDC"
            - name: MEMBERID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.MEMBERID"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "toOptumxfm.GRP_ID"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "toOptumxfm.CLMORIGIN"
            - name: LOB_IN
              type: varchar
              width: 5
              nullable: N
              map: "toOptumxfm.LOB_IN"
            - name: CLNT_ID
              type: varchar
              width: 22
              map: "TRIM(toOptumxfm.MBR_ID)"
            - name: DEA_NO
              type: varchar
              width: 20
              map: "TRIM(toOptumxfm.PHYS_DEA_NO)"
            - name: NPI_NO
              type: varchar
              width: 20
              map: "TRIM(toOptumxfm.PHYS_NTNL_PROV_ID)"
            - name: svPdxNtnlProvId
              type: varchar
              width: 255
              map: svPdxNtnlProvId
              initValue: ""
            - name: svTransDeniedDt
              type: varchar
              width: 255
              map: svTransDeniedDt
              initValue: ""
            - name: svRxNo
              type: varchar
              width: 255
              map: svRxNo
              initValue: ""
            - name: svMbrUniqKey
              type: varchar
              width: 255
              map: svMbrUniqKey
              initValue: ""
            - name: svSrcSysClmRcdDt
              type: varchar
              width: 255
              map: svSrcSysClmRcdDt
              initValue: ""
            - name: svSrcSysClmRcdTm
              type: varchar
              width: 255
              map: svSrcSysClmRcdTm
              initValue: ""
        - name: xfm_optumrx.V0S26P6
          type: filter
          source: xfm_optumrx.V0S26P6.map
          filter: "svPdxNtnlProvId = 'Y' OR svTransDeniedDt = 'Y' OR svRxNo = 'Y' OR svMbrUniqKey = 'Y' OR svSrcSysClmRcdDt = 'Y' OR svSrcSysClmRcdTm = 'Y'"
          schema: 
            - name: CLNT_ID
              type: varchar
              width: 22
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
            - name: TRANS_DENIED_DT
              type: date
            - name: RX_NO
              type: varchar
              width: 20
            - name: PRCS_DT
              type: varchar
              width: 20
            - name: SRC_SYS_CD
              type: varchar
              width: 20
            - name: DEA_NO
              type: varchar
              width: 20
            - name: NPI_NO
              type: varchar
              width: 20
        - name: xfm_OptumRx_pdx
          type: map
          source: Jn_NABP
          alias: Lnk_OptumRx_mbr_denied_out
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              map: "TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.TRANS_TYP_CD))"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.SVC_DT"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "currentdate()"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.SRC_SYS_CD)"
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
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.GRP_SK) Or Len(Lnk_OptumRx_mbr_denied_out.GRP_SK) = 0 THEN 0 ELSE Lnk_OptumRx_mbr_denied_out.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.MBR_SK) Or Len(Lnk_OptumRx_mbr_denied_out.MBR_SK) = 0 THEN 0 ELSE Lnk_OptumRx_mbr_denied_out.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "'0'"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.PROV_SK) Or Len(Lnk_OptumRx_mbr_denied_out.PROV_SK) = 0 THEN 0 ELSE Lnk_OptumRx_mbr_denied_out.PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.PROV_SPEC_CD_SK) Or Len(Lnk_OptumRx_mbr_denied_out.PROV_SPEC_CD_SK) = 0 THEN 0 ELSE Lnk_OptumRx_mbr_denied_out.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.SRV_PROV_SK) Or Len(Lnk_OptumRx_mbr_denied_out.SRV_PROV_SK) = 0 THEN 0 ELSE Lnk_OptumRx_mbr_denied_out.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.SUB_SK) Or Len(Lnk_OptumRx_mbr_denied_out.SUB_SK) = 0 THEN 0 ELSE Lnk_OptumRx_mbr_denied_out.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: varchar
              width: 1
              map: "Lnk_OptumRx_mbr_denied_out.CLMORIGIN"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.MBR_GNDR_CD_SK) Or Len(Lnk_OptumRx_mbr_denied_out.MBR_GNDR_CD_SK) = 0 THEN 0 ELSE Lnk_OptumRx_mbr_denied_out.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.MBR_RELSHP_CD_SK) Or Len(Lnk_OptumRx_mbr_denied_out.MBR_RELSHP_CD_SK) = 0 THEN 0 ELSE Lnk_OptumRx_mbr_denied_out.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              map: "TRIM(Lnk_OptumRx_mbr_denied_out.PDX_RSPN_RSN_CD_SK)"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              map: "0"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.MAIL_ORDER_IN) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.MAIL_ORDER_IN)) = 0) THEN 'R' ELSE Lnk_OptumRx_mbr_denied_out.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.MBR_BRTH_DT) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.MBR_BRTH_DT)) = 0) THEN '1753-01-01' ELSE Lnk_OptumRx_mbr_denied_out.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.SUB_BRTH_DT) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.SUB_BRTH_DT)) = 0) THEN '1753-01-01' ELSE Lnk_OptumRx_mbr_denied_out.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.BILL_RX_DISPENSE_FEE_AMT) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.BILL_RX_DISPENSE_FEE_AMT)) = 0) THEN '0.00 ' ELSE Lnk_OptumRx_mbr_denied_out.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.BILL_RX_GROS_APRV_AMT) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.BILL_RX_GROS_APRV_AMT)) = 0) THEN '0.00 ' ELSE Lnk_OptumRx_mbr_denied_out.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.BILL_RX_NET_CHK_AMT) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.BILL_RX_NET_CHK_AMT)) = 0) THEN '0.00 ' ELSE Lnk_OptumRx_mbr_denied_out.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.BILL_RX_PATN_PAY_AMT) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.BILL_RX_PATN_PAY_AMT)) = 0) THEN '0.00 ' ELSE Lnk_OptumRx_mbr_denied_out.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.INGR_CST_ALW_AMT) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.INGR_CST_ALW_AMT)) = 0) THEN '0.00 ' ELSE Lnk_OptumRx_mbr_denied_out.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: " If Len(MonthFromDate(currentdate())) = 1 Then 0 : MonthFromDate(currentdate()) Else MonthFromDate(currentdate())"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "YearFromDate(currentdate())"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: " If Lnk_OptumRx_mbr_denied_out.LOB_IN = 'MCARE' Then Lnk_OptumRx_mbr_denied_out.SRC_SYS_CNTR_ID Else Lnk_OptumRx_mbr_denied_out.MBR_ID[1, 20]"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.MBR_SFX_NO) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.MBR_SFX_NO)) = 0 THEN '00' ELSE TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.MBR_SFX_NO)"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.MBR_FIRST_NM) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.MBR_FIRST_NM)) = 0) THEN 'UNK' ELSE Trim(UpCase(Lnk_OptumRx_mbr_denied_out.MBR_FIRST_NM))"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.MBR_LAST_NM) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.MBR_LAST_NM)) = 0) THEN 'UNK' ELSE Trim(UpCase(Lnk_OptumRx_mbr_denied_out.MBR_LAST_NM))"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: " IF IsNull(Lnk_OptumRx_mbr_denied_out.MBR_SSN) OR Len(Lnk_OptumRx_mbr_denied_out.MBR_SSN) = 0 Then \"UNK\" Else Lnk_OptumRx_mbr_denied_out.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.PDX_NM) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.PDX_NM)) = 0) THEN '' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.PDX_NM))"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.PHYS_NTNL_PROV_ID) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.PHYS_NTNL_PROV_ID)) = 0 Or Lnk_OptumRx_mbr_denied_out.PHYS_NTNL_PROV_ID = '0') Or (Lnk_OptumRx_mbr_denied_out.PHYS_NTNL_PROV_ID = 'NA') THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.PHYS_NTNL_PROV_ID))"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.PHYS_FIRST_NM) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.PHYS_FIRST_NM)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.PHYS_FIRST_NM))"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.PHYS_LAST_NM) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.PHYS_LAST_NM)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.PHYS_LAST_NM))"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.PHYS_ST_CD) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.PHYS_ST_CD)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.PHYS_ST_CD))"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_OptumRx_mbr_denied_out.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.RX_LABEL_TX) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.RX_LABEL_TX)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.RX_LABEL_TX))"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.SVC_PROV_NABP_NM) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.SVC_PROV_NABP_NM)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.SVC_PROV_NABP_NM))"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.SRC_SYS_CAR_ID) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.SRC_SYS_CAR_ID)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.SRC_SYS_CAR_ID))"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "'BCBSKC'"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "'BCBS OF KANSAS CITY'"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.SRC_SYS_CNTR_ID) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.SRC_SYS_CNTR_ID)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.SRC_SYS_CNTR_ID))"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.SRC_SYS_GRP_ID) Or Len(Trim(Lnk_OptumRx_mbr_denied_out.SRC_SYS_GRP_ID)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.SRC_SYS_GRP_ID))"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: " IF (IsNull(Lnk_OptumRx_mbr_denied_out.CLNT_ID) Or Len(TrimLeadingTrailing(Lnk_OptumRx_mbr_denied_out.CLNT_ID)) = 0) THEN 'UNK' ELSE TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.CLNT_ID))"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "TRIM(UpCase(Lnk_OptumRx_mbr_denied_out.SUBMT_SUB_FIRST_NM))"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "TRIM(UpCase(Lnk_OptumRx_mbr_denied_out.SUBMT_SUB_LAST_NM))"
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_out.CLNT_ELIG_MBRSH_ID"
            - name: PRSN_NO
              type: varchar
              width: 20
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.PRSN_NO))"
            - name: RELSHP_CD
              type: varchar
              width: 20
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.RELSHP_CD))"
            - name: GNDR_CD
              type: varchar
              width: 20
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.GNDR_CD))"
            - name: CLM_RCVD_TM
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_out.SRC_SYS_CLM_RCVD_TM"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.MSG_TYP_CD))"
            - name: MSG_TYP
              type: varchar
              width: 20
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.MSG_TYP))"
            - name: MSG_TX
              type: varchar
              width: 200
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.MSG_TX))"
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.CLNT_MBRSH_ID))"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 1
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.MEDIA_TYP_CD))"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.RSPN_CD))"
            - name: DESC
              type: varchar
              width: 500
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.DESC))"
            - name: CHAPTER_ID
              type: varchar
              width: 20
              map: "Trim(UpCase(Lnk_OptumRx_mbr_denied_out.CHAPTER_ID))"
            - name: CHAPTER_DESC
              type: varchar
              width: 500
              map: "TrimLeadingTrailing(UpCase(Lnk_OptumRx_mbr_denied_out.CHAPTER_DESC))"
            - name: PDX_NPI
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_out.PDX_NTNL_PROV_ID"
            - name: NDC
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied_out.NDC"
          keys:
            - MBR_PDX_DENIED_TRANS_SK
        - name: xfmDataClean
          type: map
          source: OptumRxDeniedInputFile
          alias: Lnk_Optum_mbr_denied_src
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              map: "''"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              map: "TRIM(Lnk_Optum_mbr_denied_src.MBR_UNIQ_KEY)"
            - name: ACCOUNTID
              type: varchar
              width: 20
              map: "TRIM(Lnk_Optum_mbr_denied_src.ACCOUNTID)"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.NPIPROV"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              map: "\"31\""
            - name: TRANS_DENIED_DT
              type: date
              map: "Lnk_Optum_mbr_denied_src.DATESBM"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Lnk_Optum_mbr_denied_src.RXNUMBER"
            - name: PRCS_DT
              type: date
              nullable: N
              map: CurrDate
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              map: "Lnk_Optum_mbr_denied_src.DATESBM"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              map: "StringToTime(Lnk_Optum_mbr_denied_src.TIMESBM[1, 2] : ':' : Lnk_Optum_mbr_denied_src.TIMESBM[3, 2] : ':' : Lnk_Optum_mbr_denied_src.TIMESBM[5, 2], \"%hh:%nn:%ss\")"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.SRC_SYS_CD"
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
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.MBRSEX"
            - name: MBR_RELSHP_CD_SK
              type: varchar
              width: 3
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.MBRRELCDE"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.REJCDE1"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: TRANS_TYP_CD_SK
              type: char
              width: 0
              nullable: N
              map: "''"
            - name: MAIL_ORDER_IN
              type: char
              width: 0
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.RXNETWRKQL"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.MBRBIRTH"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Optum_mbr_denied_src.PHRDISPFEE"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Optum_mbr_denied_src.CLTDUEAMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.CLT2DUEAMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.CLTPATPAY"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.CLTINGRCST"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "0"
            - name: MBR_ID
              type: varchar
              width: 22
              nullable: N
              map: " If Trim(Lnk_Optum_mbr_denied_src.LOB_IN) = 'MCARE' Then Trim(Lnk_Optum_mbr_denied_src.MEMBERID[4, len(Lnk_Optum_mbr_denied_src.MEMBERID)]) : '00' Else TRIM(Lnk_Optum_mbr_denied_src.MEMBERID)"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: " If Lnk_Optum_mbr_denied_src.LOB_IN = 'MCARE' Then '00' Else TRIM(Lnk_Optum_mbr_denied_src.MEMBERID[10, 2])"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.MBRFSTNME"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.MBRLSTNME"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.SOCSECNBR"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.SRVPROVNME"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "\"UNK\""
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: " IF Lnk_Optum_mbr_denied_src.PRESCDEAID = \"UNK\" THEN \"NA\" ELSE Lnk_Optum_mbr_denied_src.PRESCDEAID"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.NPIPRESCR"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.PPRSFSTNME"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.PPRSLSTNME"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "\"UNK\""
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "\"UNK\""
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.PPRSTATE"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "\"UNK\""
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.LABELNAME"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "TRIM(Lnk_Optum_mbr_denied_src.SRVPROVID)"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.CARID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "\"20165\""
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "\"BCBS OF KANSAS CITY\""
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.MEMBERID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.ACCOUNTID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: " If Trim(Lnk_Optum_mbr_denied_src.LOB_IN) = 'MCARE' Then Trim(Lnk_Optum_mbr_denied_src.MEMBERID[4, len(Lnk_Optum_mbr_denied_src.MEMBERID)]) Else TRIM(Lnk_Optum_mbr_denied_src.MEMBERID)"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "''"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "''"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "TRIM(Lnk_Optum_mbr_denied_src.PRODUCTID)"
            - name: MEMBERID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.MEMBERID"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.GRP_ID"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Lnk_Optum_mbr_denied_src.CLMORIGIN"
            - name: LOB_IN
              type: varchar
              width: 5
              nullable: N
              map: "Lnk_Optum_mbr_denied_src.LOB_IN"
        - name: Jn_Mbr
          type: join
          sources: 
            - name: xfm_optumrx.V0S26P5
              alias: Lnk_OptumRx_mbr_denied
              joinFields: 
              - field: MBR_ID
              - field: ACCOUNTID
            - name: db2_MBR
              alias: lnk_mbr_extr
              joinFields: 
              - field: MBR_ID
              - field: ACCOUNTID
          joinCondition: 
            - expression: Lnk_OptumRx_mbr_denied.MBR_ID = lnk_mbr_extr.MBR_ID
            - expression: Lnk_OptumRx_mbr_denied.ACCOUNTID = lnk_mbr_extr.ACCOUNTID
          option: 
            joinType: inner
          schema: 
            - name: CLNT_ORG_OPTRNAL_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.CLNT_ORG_OPTRNAL_ID"
            - name: CLNT_ORG_NM
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.CLNT_ORG_NM"
            - name: MAIL_ORDER_IN
              type: varchar
              width: 100
              map: "Lnk_OptumRx_mbr_denied.MAIL_ORDER_IN"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 50
              map: "Lnk_OptumRx_mbr_denied.SRC_SYS_CAR_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.TRANS_TYP_CD"
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
              map: "Lnk_OptumRx_mbr_denied.CLNT_MBRSH_ID"
            - name: PATN_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.PATN_ID"
            - name: CLNT_ID
              type: varchar
              width: 22
              map: "Lnk_OptumRx_mbr_denied.CLNT_ID"
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.CLNT_ELIG_MBRSH_ID"
            - name: MBR_SSN
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.MBR_SSN"
            - name: PRSN_NO
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.PRSN_NO"
            - name: RELSHP_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.RELSHP_CD"
            - name: GNDR_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.GNDR_CD"
            - name: SUB_BRTH_DT
              type: varchar
              width: 35
              map: "lnk_mbr_extr.SUB_BRTH_DT"
            - name: MBR_BRTH_DT
              type: varchar
              width: 35
              map: "Lnk_OptumRx_mbr_denied.MBR_BRTH_DT"
            - name: MBR_FIRST_NM
              type: varchar
              width: 100
              map: "Lnk_OptumRx_mbr_denied.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 100
              map: "Lnk_OptumRx_mbr_denied.MBR_LAST_NM"
            - name: SUBMT_SUB_FIRST_NM
              type: varchar
              width: 100
              map: "lnk_mbr_extr.SUB_FIRST_NM"
            - name: SUBMT_SUB_LAST_NM
              type: varchar
              width: 100
              map: "lnk_mbr_extr.SUB_LAST_NM"
            - name: RX_LABEL_TX
              type: varchar
              width: 200
              map: "Lnk_OptumRx_mbr_denied.RX_LABEL_TX"
            - name: SVC_DT
              type: date
              map: "Lnk_OptumRx_mbr_denied.SVC_DT"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 30
              map: "Lnk_OptumRx_mbr_denied.SRC_SYS_GRP_ID"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              map: "Lnk_OptumRx_mbr_denied.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              map: "Lnk_OptumRx_mbr_denied.SRC_SYS_CLM_RCVD_TM"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "Lnk_OptumRx_mbr_denied.MSG_TYP_CD"
            - name: MSG_TYP
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.MSG_TYP"
            - name: MSG_TX
              type: varchar
              width: 200
              map: "Lnk_OptumRx_mbr_denied.MSG_TX"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_OptumRx_mbr_denied.INGR_CST_ALW_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_OptumRx_mbr_denied.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_OptumRx_mbr_denied.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_OptumRx_mbr_denied.BILL_RX_PATN_PAY_AMT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_OptumRx_mbr_denied.BILL_RX_DISPENSE_FEE_AMT"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.RSPN_CD"
            - name: DESC
              type: varchar
              width: 500
              map: "Lnk_OptumRx_mbr_denied.DESC"
            - name: CHAPTER_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.CHAPTER_ID"
            - name: CHAPTER_DESC
              type: varchar
              width: 500
              map: "Lnk_OptumRx_mbr_denied.CHAPTER_DESC"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 100
              map: "Lnk_OptumRx_mbr_denied.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 100
              map: "Lnk_OptumRx_mbr_denied.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 250
              map: "Lnk_OptumRx_mbr_denied.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 100
              map: "Lnk_OptumRx_mbr_denied.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 10
              map: "Lnk_OptumRx_mbr_denied.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.PHYS_POSTAL_CD"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.PDX_NTNL_PROV_ID"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.SVC_PROV_NABP_NM"
            - name: PDX_NM
              type: varchar
              width: 200
              map: "Lnk_OptumRx_mbr_denied.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.PDX_PHN_NO"
            - name: NDC
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.NDC"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              map: "Lnk_OptumRx_mbr_denied.MBR_UNIQ_KEY"
            - name: MBR_SK
              type: int
              width: 4
              map: "lnk_mbr_extr.MBR_SK"
            - name: GRP_SK
              type: int
              width: 4
              map: "lnk_mbr_extr.GRP_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_mbr_extr.SUB_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              map: "lnk_mbr_extr.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_mbr_extr.MBR_RELSHP_CD_SK"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Lnk_OptumRx_mbr_denied.MBR_SFX_NO"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.SRC_SYS_CD"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.RX_NO"
            - name: MBR_ID
              type: varchar
              width: 22
              map: "Lnk_OptumRx_mbr_denied.MBR_ID"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.SRC_SYS_CNTR_ID"
            - name: GRP_ID
              type: varchar
              width: 20
              map: "Lnk_OptumRx_mbr_denied.GRP_ID"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Lnk_OptumRx_mbr_denied.CLMORIGIN"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              map: "Lnk_OptumRx_mbr_denied.PDX_RSPN_RSN_CD_SK"
            - name: PRCS_DT
              type: date
              map: "Lnk_OptumRx_mbr_denied.PRCS_DT"
            - name: LOB_IN
              type: varchar
              width: 5
              nullable: N
              map: "Lnk_OptumRx_mbr_denied.LOB_IN"
        - name: Jn_NABP
          type: join
          sources: 
            - name: Lkp_prov
              alias: Jn_ndc
              joinFields: 
              - field: SVC_PROV_NABP_NM
            - name: db2_PROV
              alias: lnk_SRV_PROV_SK
              joinFields: 
              - field: SVC_PROV_NABP_NM
          joinCondition: 
            - expression: Jn_ndc.SVC_PROV_NABP_NM = lnk_SRV_PROV_SK.SVC_PROV_NABP_NM
          option: 
            joinType: left
            fillNull: true
          schema: 
            - name: CLNT_ORG_OPTRNAL_ID
              type: varchar
              width: 20
              map: "Jn_ndc.CLNT_ORG_OPTRNAL_ID"
            - name: CLNT_ORG_NM
              type: varchar
              width: 20
              map: "Jn_ndc.CLNT_ORG_NM"
            - name: MAIL_ORDER_IN
              type: varchar
              width: 100
              map: "Jn_ndc.MAIL_ORDER_IN"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 50
              map: "Jn_ndc.SRC_SYS_CAR_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              map: "Jn_ndc.TRANS_TYP_CD"
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
              map: "Jn_ndc.CLNT_MBRSH_ID"
            - name: PATN_ID
              type: varchar
              width: 20
              map: "Jn_ndc.PATN_ID"
            - name: CLNT_ID
              type: varchar
              width: 22
              map: "Jn_ndc.CLNT_ID"
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
              map: "Jn_ndc.CLNT_ELIG_MBRSH_ID"
            - name: MBR_SSN
              type: varchar
              width: 20
              map: "Jn_ndc.MBR_SSN"
            - name: PRSN_NO
              type: varchar
              width: 20
              map: "Jn_ndc.PRSN_NO"
            - name: RELSHP_CD
              type: varchar
              width: 20
              map: "Jn_ndc.RELSHP_CD"
            - name: GNDR_CD
              type: varchar
              width: 20
              map: "Jn_ndc.GNDR_CD"
            - name: SUB_BRTH_DT
              type: varchar
              width: 35
              map: "Jn_ndc.SUB_BRTH_DT"
            - name: MBR_BRTH_DT
              type: varchar
              width: 35
              map: "Jn_ndc.MBR_BRTH_DT"
            - name: MBR_FIRST_NM
              type: varchar
              width: 100
              map: "Jn_ndc.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 100
              map: "Jn_ndc.MBR_LAST_NM"
            - name: SUBMT_SUB_FIRST_NM
              type: varchar
              width: 100
              map: "Jn_ndc.SUBMT_SUB_FIRST_NM"
            - name: SUBMT_SUB_LAST_NM
              type: varchar
              width: 100
              map: "Jn_ndc.SUBMT_SUB_LAST_NM"
            - name: RX_LABEL_TX
              type: varchar
              width: 200
              map: "Jn_ndc.RX_LABEL_TX"
            - name: SVC_DT
              type: date
              map: "Jn_ndc.SVC_DT"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 30
              map: "Jn_ndc.SRC_SYS_GRP_ID"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              map: "Jn_ndc.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              map: "Jn_ndc.SRC_SYS_CLM_RCVD_TM"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "Jn_ndc.MSG_TYP_CD"
            - name: MSG_TYP
              type: varchar
              width: 20
              map: "Jn_ndc.MSG_TYP"
            - name: MSG_TX
              type: varchar
              width: 200
              map: "Jn_ndc.MSG_TX"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              map: "NullToZero(Jn_ndc.INGR_CST_ALW_AMT)"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              map: "NullToZero(Jn_ndc.BILL_RX_GROS_APRV_AMT)"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              map: "NullToZero(Jn_ndc.BILL_RX_NET_CHK_AMT)"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              map: "NullToZero(Jn_ndc.BILL_RX_PATN_PAY_AMT)"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              map: "NullToZero(Jn_ndc.BILL_RX_DISPENSE_FEE_AMT)"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "Jn_ndc.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "Jn_ndc.RSPN_CD"
            - name: DESC
              type: varchar
              width: 500
              map: "Jn_ndc.DESC"
            - name: CHAPTER_ID
              type: varchar
              width: 20
              map: "Jn_ndc.CHAPTER_ID"
            - name: CHAPTER_DESC
              type: varchar
              width: 500
              map: "Jn_ndc.CHAPTER_DESC"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              map: "Jn_ndc.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "Jn_ndc.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 100
              map: "Jn_ndc.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 100
              map: "Jn_ndc.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 250
              map: "Jn_ndc.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 100
              map: "Jn_ndc.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 10
              map: "Jn_ndc.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              map: "Jn_ndc.PHYS_POSTAL_CD"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              map: "Jn_ndc.PDX_NTNL_PROV_ID"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 20
              map: "Jn_ndc.SVC_PROV_NABP_NM"
            - name: PDX_NM
              type: varchar
              width: 200
              map: "Jn_ndc.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              map: "Jn_ndc.PDX_PHN_NO"
            - name: NDC
              type: varchar
              width: 20
              map: "Jn_ndc.NDC"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.MBR_UNIQ_KEY)"
            - name: MBR_SK
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.MBR_SK)"
            - name: GRP_SK
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.GRP_SK)"
            - name: SUB_SK
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.SUB_SK)"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.MBR_GNDR_CD_SK)"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.MBR_RELSHP_CD_SK)"
            - name: MBR_SFX_NO
              type: char
              width: 2
              map: "Jn_ndc.MBR_SFX_NO"
            - name: PROV_SK
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.PROV_SK)"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.PROV_SPEC_CD_SK)"
            - name: PROV_DEA_SK
              type: int
              width: 4
              map: "NullToZero(Jn_ndc.PROV_DEA_SK)"
            - name: EFF_DT_SK
              type: varchar
              width: 20
              map: "Jn_ndc.EFF_DT_SK"
            - name: TERM_DT_SK
              type: varchar
              width: 20
              map: "Jn_ndc.TERM_DT_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              map: "NullToZero(lnk_SRV_PROV_SK.PROV_SK_SRV_PROV_SK)"
            - name: SRC_SYS_CD_SK_SRV_PROV_SK
              type: int
              width: 4
              map: "NullToZero(lnk_SRV_PROV_SK.SRC_SYS_CD_SK_SRV_PROV_SK)"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              map: "Jn_ndc.SRC_SYS_CD"
            - name: RX_NO
              type: varchar
              width: 20
              map: "Jn_ndc.RX_NO"
            - name: MBR_ID
              type: varchar
              width: 22
              map: "Jn_ndc.MBR_ID"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              map: "Jn_ndc.SRC_SYS_CNTR_ID"
            - name: GRP_ID
              type: varchar
              width: 20
              map: "Jn_ndc.GRP_ID"
            - name: CLMORIGIN
              type: char
              width: 1
              map: "Jn_ndc.CLMORIGIN"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              map: "Jn_ndc.PDX_RSPN_RSN_CD_SK"
            - name: PRCS_DT
              type: date
              map: "Jn_ndc.PRCS_DT"
            - name: LOB_IN
              type: varchar
              width: 5
              nullable: N
              map: "Jn_ndc.LOB_IN"


