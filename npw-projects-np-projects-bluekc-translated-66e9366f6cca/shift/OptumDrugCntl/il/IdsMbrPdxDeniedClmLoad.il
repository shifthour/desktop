model:
  config:
    origin: datastage
  jobs:
    - name: IdsMbrPdxDeniedClmLoad
      origin: datastage
      type: dataflow
      folder: "DataWarehouse/Claim/DeniedPDXClaims"
      runAfterSubrOnJobSuccess: enabled
      description: "This Job Loads Data from Sequential File created in FKEY Job and loads into MBR_PDX_DENIED_TRANS table Triggered by: OptumIdsMbrPdxDeniedTransLoadSeq"
      fullDescription: "**********************************************************************************************************************************************************************\nCOPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nDESCRIPTION:  OPTUM Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table\n\n\nCalled By: OptumIdsMbrPdxDeniedTransLoadSeq\n\n\nInitial Development: Peter Gichiri \n                                                                                                                                                                                                                DATASTAGE               CODE                           DATE\nDEVELOPER             DATE                PROJECT                     DESCRIPTION                                                                                      ENVIRONMENT          REVIEWER                 REVIEW\n-----------------------------    ----------------------     ------------------------------        -----------------------------------------------------------------------------------------------------------  ------------------------------    ------------------------------       --------------------\n                                            \n\nRavi Singh               2018-08-14              5796                             Original Programming                                                                         IntegrateDev1             Kalyan Neelam            2018-09-25\nPeter Gichiri              2019-10-02             6131 - PBM REPLACEMENT  to OPTUMRX                                                                                                             Kalyan Neelam            2019-11-21"
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
        - name: SrcSysCd
          type: string
          description: SrcSysCd
        - name: RunID
          type: string
          description: RunID
      steps:
        - name: seq_MBR_PDX_DENIED_TRANS_load
          type: fileReader
          urls: 
            - "#$FilePath#/load/MBR_PDX_DENIED_TRANS.#SrcSysCd#.#RunID#.dat"
          rejectMode: continue
          format: 
            header: false
            quotes: ""
            fieldDelimiter: "|"
            rowDelimiter: "\n"
            fixedLength: false
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
            - name: PRCS_DT
              type: date
              nullable: N
            - name: SRC_SYS_CD
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
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
            - name: MBR_BRTH_DT
              type: date
              nullable: N
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
            - name: SUB_BRTH_DT
              type: date
              nullable: N
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
          keys:
            - MBR_PDX_DENIED_TRANS_SK
        - name: Db2_Ext_MBR_PDX_DENIED_TRANS_Data
          type: dbWriter
          originalDb: db2
          table: "#$IDSOwner#.MBR_PDX_DENIED_TRANS"
          tableAction: append
          continueOnRowError: false
          writeMode: "insert then update"
          source: cpy_forBuffer
        - name: cpy_forBuffer
          type: map
          source: seq_MBR_PDX_DENIED_TRANS_load
          alias: lnk_IdsMbrPdxDeniedTransLoad_InABC
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbrPdxDeniedTransLoad_InABC.SUB_LAST_NM"
          keys:
            - MBR_PDX_DENIED_TRANS_SK


