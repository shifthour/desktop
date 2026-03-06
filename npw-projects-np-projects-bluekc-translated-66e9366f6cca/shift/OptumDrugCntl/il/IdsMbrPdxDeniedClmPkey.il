model:
  config:
    origin: datastage
  jobs:
    - name: IdsMbrPdxDeniedClmPkey
      origin: datastage
      type: dataflow
      folder: "DataWarehouse/Claim/DeniedPDXClaims"
      description: "Build primary key for  MBR_PDX_DENIED_TRANS.  K table will be read to see if there is an SK already available for the Natural Keys. "
      fullDescription: "**********************************************************************************************************************************************************************\nCOPYRIGHT 2014 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\n\nDESCRIPTION:  OPTUM Claims denied data is extracted from source file and loaded into the  MBR_PDX_DENIED_TRANS Table\n\n\nCalled By: OptumIdsMbrPdxDeniedTransLoadSeq\n\n\nDEVELOPER             DATE                PROJECT                                                          DESCRIPTION                                                                                     ENVIRONMENT          REVIEWER                 REVIEW\n-----------------------------    ----------------------     ------------------------------                                             -----------------------------------------------------------------------------------                           ------------------------------    ------------------------------       --------------------                                          \nPeter Gichiri              2019-10-02           6131 - PBM REPLACEMENT  to OPTUMRX  Initial Devlopment                                                                                  Integrate2                     Kalyan Neelam             2019-11-21\nPeter Gichiri              2020-01-09           6131 - PBM REPLACEMENT                          If NullToEmpty(lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DE-                                               Kalyan Neelam             2020-01-09\n                                                                                                                                      NIED_TRANS_SK) ='' then IDSRunCycle else  lnk_Mbr_PdxDen-\n                                                                                                                                      iedJnData_out.CRT_RUN_CYC_EXCTN_SK\nVelmani Kondappan  20200-04-02      6131 - PBM REPLACEMEN                             Changed the sorting order within the rdup_Natural_Keys to remove        IntegrateDev2                     Kalyan Neelam            2020-04-09\n                                                                                                                                    error warinings\n\nRekha Radhakrishna 2020-10-20        6343 - PhaseII governmanet program             Added Keys in Join to K_ table                                                                 IntegrateDev2"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: APT_CONFIG_FILE
          type: string
          description: "Configuration file"
          longDescription: "The Parallel job configuration file."
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
        - name: IDSRunCycle
          type: string
          description: IDSRunCycle
        - name: SrcSysCd
          type: string
          description: SrcSysCd
        - name: IDSDB2ArraySize
          type: string
          description: "IDS DB2 Array Size for Load"
          defaultType: project
        - name: IDSDB2RecordCount
          type: string
          description: "IDS DB2 Record Count for Load"
          defaultType: project
        - name: RunID
          type: string
          description: RunID
      steps:
        - name: seq_MBR_PDX_DENIED_TRANS_Pkey
          type: fileWriter
          urls: 
            - "#$FilePath#/key/MBR_PDX_DENIED_TRANS.#SrcSysCd#.pkey.#RunID#.dat"
          source: jn_PKey
          rejectMode: continue
          option: 
            updateMode: overwrite
            writeMode: SingleFile
          format: 
            header: false
            quotes: "^"
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
        - name: ds_MBR_PDX_DENIED_TRANS_xfm
          type: datasetReader
          urls: 
            - "#$FilePath#/ds/MBR_PDX_DENIED_TRANS.#SrcSysCd#.xfrm.#RunID#.ds"
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
              type: varchar
              width: 1
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
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
            - name: CLNT_ELIG_MBRSH_ID
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
            - name: CLM_RCVD_TM
              type: varchar
              width: 20
            - name: MSG_TYP_CD
              type: varchar
              width: 10
            - name: MSG_TYP
              type: varchar
              width: 20
            - name: MSG_TX
              type: varchar
              width: 200
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
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
            - name: PDX_NPI
              type: varchar
              width: 20
            - name: NDC
              type: varchar
              width: 20
          keys:
            - MBR_PDX_DENIED_TRANS_SK
        - name: rdup_Natural_Keys
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
          source: Cpy.V0S33P5
          alias: lnk_FctsIdsMbr_PdxDeniedPkey_dedup
          option:
            retain: first
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.PRCS_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.SRC_SYS_CLM_RCVD_TM"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_FctsIdsMbr_PdxDeniedPkey_dedup.SRC_SYS_CD"
          keys:
            - MBR_UNIQ_KEY
            - PDX_NTNL_PROV_ID
            - TRANS_TYP_CD
            - TRANS_DENIED_DT
            - RX_NO
            - PRCS_DT
            - SRC_SYS_CLM_RCVD_DT
            - SRC_SYS_CLM_RCVD_TM
            - SRC_SYS_CD
        - name: db2_K_MBR_PDX_DENIED_TRANS_in
          type: dbReader
          originalDb: db2
          query: "SELECT\nMBR_UNIQ_KEY,\nPDX_NTNL_PROV_ID,\nTRANS_TYP_CD,\nTRANS_DENIED_DT,\nRX_NO,\nPRCS_DT AS PRCS_DT,\nSRC_SYS_CLM_RCVD_DT  AS SRC_SYS_CLM_RCVD_DT,\nSRC_SYS_CLM_RCVD_TM AS SRC_SYS_CLM_RCVD_TM,\nSRC_SYS_CD,\nCRT_RUN_CYC_EXCTN_SK,\nMBR_PDX_DENIED_TRANS_SK\n\nFROM\n#$IDSOwner#.K_MBR_PDX_DENIED_TRANS;"
          schema: 
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
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
          keys:
            - MBR_UNIQ_KEY
            - PDX_NTNL_PROV_ID
            - TRANS_TYP_CD
            - TRANS_DENIED_DT
            - RX_NO
            - PRCS_DT
            - SRC_SYS_CLM_RCVD_DT
            - SRC_SYS_CLM_RCVD_TM
            - SRC_SYS_CD
        - name: db2_K_MBR_PDX_DENIED_TRANS_load
          type: dbWriter
          originalDb: db2
          table: "#$IDSOwner#.K_MBR_PDX_DENIED_TRANS"
          tableAction: append
          continueOnRowError: false
          writeMode: insert
          source: "xfrm_PKEYgen.V116S6P3"
        - name: xfrm_PKEYgen.V116S6P3.map
          type: map
          source: jn_Mbr_Pdx
          alias: lnk_Mbr_PdxDeniedJnData_out
          stageVars: 
            - name: svMbrPdxDeniedTransSK
              type: int
              width: 4
              map: " If ( IF IsNotNull((lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK)) THEN (lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK) ELSE 0) = 0 Then NextSurrogateKey() Else TRIM(lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK)"
              initValue: "0"
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "currentdate()"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.SRC_SYS_CLM_RCVD_TM"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: IDSRunCycle
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: svMbrPdxDeniedTransSK
            - name: svMbrPdxDeniedTransSK
              type: int
              width: 4
              map: svMbrPdxDeniedTransSK
              initValue: "0"
        - name: xfrm_PKEYgen.V116S6P3
          type: filter
          source: xfrm_PKEYgen.V116S6P3.map
          filter: "( IF IsNotNull((MBR_PDX_DENIED_TRANS_SK)) THEN (MBR_PDX_DENIED_TRANS_SK) ELSE 0) = 0"
          schema: 
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
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
          keys:
            - MBR_UNIQ_KEY
            - PDX_NTNL_PROV_ID
            - TRANS_TYP_CD
            - TRANS_DENIED_DT
            - RX_NO
            - PRCS_DT
            - SRC_SYS_CLM_RCVD_DT
            - SRC_SYS_CLM_RCVD_TM
            - SRC_SYS_CD
        - name: xfrm_PKEYgen.V116S6P2
          type: map
          source: jn_Mbr_Pdx
          alias: lnk_Mbr_PdxDeniedJnData_out
          stageVars: 
            - name: svMbrPdxDeniedTransSK
              type: int
              width: 4
              map: " If ( IF IsNotNull((lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK)) THEN (lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK) ELSE 0) = 0 Then NextSurrogateKey() Else TRIM(lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK)"
              initValue: "0"
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: " If ( IF IsNotNull((lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK)) THEN (lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK) ELSE \"\") = '' then currentdate() else lnk_Mbr_PdxDeniedJnData_out.PRCS_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.SRC_SYS_CLM_RCVD_TM"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Mbr_PdxDeniedJnData_out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: " If ( IF IsNotNull((lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK)) THEN (lnk_Mbr_PdxDeniedJnData_out.MBR_PDX_DENIED_TRANS_SK) ELSE \"\") = '' then IDSRunCycle else lnk_Mbr_PdxDeniedJnData_out.CRT_RUN_CYC_EXCTN_SK"
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: svMbrPdxDeniedTransSK
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: IDSRunCycle
            - name: svMbrPdxDeniedTransSK
              type: int
              width: 4
              map: svMbrPdxDeniedTransSK
              dropStageVar: true
              initValue: "0"
        - name: jn_PKey
          type: join
          sources: 
            - name: xfrm_PKEYgen.V116S6P2
              alias: lnk_Pkey_out
              joinFields: 
              - field: MBR_UNIQ_KEY
              - field: PDX_NTNL_PROV_ID
              - field: TRANS_TYP_CD
              - field: TRANS_DENIED_DT
              - field: RX_NO
              - field: SRC_SYS_CLM_RCVD_DT
              - field: SRC_SYS_CLM_RCVD_TM
              - field: SRC_SYS_CD
            - name: Cpy.V0S33P4
              alias: lnk_IdsMbr_PdxDeniedPkey_All
              joinFields: 
              - field: MBR_UNIQ_KEY
              - field: PDX_NTNL_PROV_ID
              - field: TRANS_TYP_CD
              - field: TRANS_DENIED_DT
              - field: RX_NO
              - field: SRC_SYS_CLM_RCVD_DT
              - field: SRC_SYS_CLM_RCVD_TM
              - field: SRC_SYS_CD
          joinCondition: 
            - expression: lnk_Pkey_out.MBR_UNIQ_KEY = lnk_IdsMbr_PdxDeniedPkey_All.MBR_UNIQ_KEY
            - expression: lnk_Pkey_out.PDX_NTNL_PROV_ID = lnk_IdsMbr_PdxDeniedPkey_All.PDX_NTNL_PROV_ID
            - expression: lnk_Pkey_out.TRANS_TYP_CD = lnk_IdsMbr_PdxDeniedPkey_All.TRANS_TYP_CD
            - expression: lnk_Pkey_out.TRANS_DENIED_DT = lnk_IdsMbr_PdxDeniedPkey_All.TRANS_DENIED_DT
            - expression: lnk_Pkey_out.RX_NO = lnk_IdsMbr_PdxDeniedPkey_All.RX_NO
            - expression: lnk_Pkey_out.SRC_SYS_CLM_RCVD_DT = lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CLM_RCVD_DT
            - expression: lnk_Pkey_out.SRC_SYS_CLM_RCVD_TM = lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CLM_RCVD_TM
            - expression: lnk_Pkey_out.SRC_SYS_CD = lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CD
          option: 
            joinType: inner
          schema: 
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Pkey_out.MBR_PDX_DENIED_TRANS_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_Pkey_out.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Pkey_out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Pkey_out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_Pkey_out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Pkey_out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_Pkey_out.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Pkey_out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Pkey_out.CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_Pkey_out.LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: varchar
              width: 1
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              map: "lnk_IdsMbr_PdxDeniedPkey_All.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_Pkey_out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_Pkey_out.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_All.SUB_LAST_NM"
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.CLNT_ELIG_MBRSH_ID"
            - name: PRSN_NO
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PRSN_NO"
            - name: RELSHP_CD
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.RELSHP_CD"
            - name: GNDR_CD
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.GNDR_CD"
            - name: CLM_RCVD_TM
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.CLM_RCVD_TM"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MSG_TYP_CD"
            - name: MSG_TYP
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MSG_TYP"
            - name: MSG_TX
              type: varchar
              width: 200
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MSG_TX"
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
              map: "lnk_IdsMbr_PdxDeniedPkey_All.CLNT_MBRSH_ID"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.RSPN_CD"
            - name: DESC
              type: varchar
              width: 500
              map: "lnk_IdsMbr_PdxDeniedPkey_All.DESC"
            - name: CHAPTER_ID
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.CHAPTER_ID"
            - name: CHAPTER_DESC
              type: varchar
              width: 500
              map: "lnk_IdsMbr_PdxDeniedPkey_All.CHAPTER_DESC"
            - name: PDX_NPI
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.PDX_NPI"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_All.NDC"
          keys:
            - MBR_PDX_DENIED_TRANS_SK
        - name: jn_Mbr_Pdx
          type: join
          sources: 
            - name: rdup_Natural_Keys
              alias: lnk_Natural_Keys_out
              joinFields: 
              - field: MBR_UNIQ_KEY
              - field: PDX_NTNL_PROV_ID
              - field: TRANS_TYP_CD
              - field: TRANS_DENIED_DT
              - field: RX_NO
              - field: SRC_SYS_CD
              - field: PRCS_DT
              - field: SRC_SYS_CLM_RCVD_DT
              - field: SRC_SYS_CLM_RCVD_TM
            - name: db2_K_MBR_PDX_DENIED_TRANS_in
              alias: lnk_KMbrPdxDeniedTransPkey_extr
              joinFields: 
              - field: MBR_UNIQ_KEY
              - field: PDX_NTNL_PROV_ID
              - field: TRANS_TYP_CD
              - field: TRANS_DENIED_DT
              - field: RX_NO
              - field: SRC_SYS_CD
              - field: PRCS_DT
              - field: SRC_SYS_CLM_RCVD_DT
              - field: SRC_SYS_CLM_RCVD_TM
          joinCondition: 
            - expression: lnk_Natural_Keys_out.MBR_UNIQ_KEY = lnk_KMbrPdxDeniedTransPkey_extr.MBR_UNIQ_KEY
            - expression: lnk_Natural_Keys_out.PDX_NTNL_PROV_ID = lnk_KMbrPdxDeniedTransPkey_extr.PDX_NTNL_PROV_ID
            - expression: lnk_Natural_Keys_out.TRANS_TYP_CD = lnk_KMbrPdxDeniedTransPkey_extr.TRANS_TYP_CD
            - expression: lnk_Natural_Keys_out.TRANS_DENIED_DT = lnk_KMbrPdxDeniedTransPkey_extr.TRANS_DENIED_DT
            - expression: lnk_Natural_Keys_out.RX_NO = lnk_KMbrPdxDeniedTransPkey_extr.RX_NO
            - expression: lnk_Natural_Keys_out.SRC_SYS_CD = lnk_KMbrPdxDeniedTransPkey_extr.SRC_SYS_CD
            - expression: lnk_Natural_Keys_out.PRCS_DT = lnk_KMbrPdxDeniedTransPkey_extr.PRCS_DT
            - expression: lnk_Natural_Keys_out.SRC_SYS_CLM_RCVD_DT = lnk_KMbrPdxDeniedTransPkey_extr.SRC_SYS_CLM_RCVD_DT
            - expression: lnk_Natural_Keys_out.SRC_SYS_CLM_RCVD_TM = lnk_KMbrPdxDeniedTransPkey_extr.SRC_SYS_CLM_RCVD_TM
          option: 
            joinType: left
            fillNull: true
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "NullToZero(lnk_Natural_Keys_out.MBR_UNIQ_KEY)"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Natural_Keys_out.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Natural_Keys_out.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_Natural_Keys_out.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Natural_Keys_out.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_Natural_Keys_out.PRCS_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_Natural_Keys_out.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_Natural_Keys_out.SRC_SYS_CLM_RCVD_TM"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_Natural_Keys_out.SRC_SYS_CD"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              map: "NullToZero(lnk_KMbrPdxDeniedTransPkey_extr.CRT_RUN_CYC_EXCTN_SK)"
            - name: MBR_PDX_DENIED_TRANS_SK
              type: int
              width: 4
              map: "NullToZero(lnk_KMbrPdxDeniedTransPkey_extr.MBR_PDX_DENIED_TRANS_SK)"
          keys:
            - SRC_SYS_CD
        - name: Cpy.V0S33P4
          type: map
          source: ds_MBR_PDX_DENIED_TRANS_xfm
          alias: lnk_IdsMbr_PdxDeniedPkey_in
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.RX_NO"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CD"
            - name: GRP_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.GRP_SK"
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_SK"
            - name: NDC_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.NDC_SK"
            - name: PRSCRB_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PRSCRB_PROV_SK"
            - name: PROV_SPEC_CD_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PROV_SPEC_CD_SK"
            - name: SRV_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRV_PROV_SK"
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SUB_SK"
            - name: MEDIA_TYP_CD_SK
              type: varchar
              width: 1
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MEDIA_TYP_CD_SK"
            - name: MBR_GNDR_CD_SK
              type: int
              width: 4
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_GNDR_CD_SK"
            - name: MBR_RELSHP_CD_SK
              type: int
              width: 4
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_RELSHP_CD_SK"
            - name: PDX_RSPN_RSN_CD_SK
              type: varchar
              width: 3
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PDX_RSPN_RSN_CD_SK"
            - name: PDX_RSPN_TYP_CD_SK
              type: int
              width: 4
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PDX_RSPN_TYP_CD_SK"
            - name: TRANS_TYP_CD_SK
              type: int
              width: 4
              map: "lnk_IdsMbr_PdxDeniedPkey_in.TRANS_TYP_CD_SK"
            - name: MAIL_ORDER_IN
              type: char
              width: 1
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MAIL_ORDER_IN"
            - name: MBR_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_BRTH_DT"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CLM_RCVD_TM"
            - name: SUB_BRTH_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SUB_BRTH_DT"
            - name: BILL_RX_DISPENSE_FEE_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.BILL_RX_DISPENSE_FEE_AMT"
            - name: BILL_RX_GROS_APRV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.BILL_RX_GROS_APRV_AMT"
            - name: BILL_RX_NET_CHK_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.BILL_RX_NET_CHK_AMT"
            - name: BILL_RX_PATN_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.BILL_RX_PATN_PAY_AMT"
            - name: INGR_CST_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.INGR_CST_ALW_AMT"
            - name: TRANS_MO_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.TRANS_MO_NO"
            - name: TRANS_YR_NO
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.TRANS_YR_NO"
            - name: MBR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_ID"
            - name: MBR_SFX_NO
              type: char
              width: 2
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_SFX_NO"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_FIRST_NM"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_LAST_NM"
            - name: MBR_SSN
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_SSN"
            - name: PDX_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PDX_NM"
            - name: PDX_PHN_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PDX_PHN_NO"
            - name: PHYS_DEA_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PHYS_DEA_NO"
            - name: PHYS_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PHYS_NTNL_PROV_ID"
            - name: PHYS_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PHYS_FIRST_NM"
            - name: PHYS_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PHYS_LAST_NM"
            - name: PHYS_ST_ADDR_LN
              type: varchar
              width: 40
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PHYS_ST_ADDR_LN"
            - name: PHYS_CITY_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PHYS_CITY_NM"
            - name: PHYS_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PHYS_ST_CD"
            - name: PHYS_POSTAL_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PHYS_POSTAL_CD"
            - name: RX_LABEL_TX
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.RX_LABEL_TX"
            - name: SVC_PROV_NABP_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SVC_PROV_NABP_NM"
            - name: SRC_SYS_CAR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CAR_ID"
            - name: SRC_SYS_CLNT_ORG_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CLNT_ORG_ID"
            - name: SRC_SYS_CLNT_ORG_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CLNT_ORG_NM"
            - name: SRC_SYS_CNTR_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CNTR_ID"
            - name: SRC_SYS_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_GRP_ID"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SUB_ID"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SUB_FIRST_NM"
            - name: SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SUB_LAST_NM"
            - name: CLNT_ELIG_MBRSH_ID
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.CLNT_ELIG_MBRSH_ID"
            - name: PRSN_NO
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PRSN_NO"
            - name: RELSHP_CD
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.RELSHP_CD"
            - name: GNDR_CD
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.GNDR_CD"
            - name: CLM_RCVD_TM
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.CLM_RCVD_TM"
            - name: MSG_TYP_CD
              type: varchar
              width: 10
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MSG_TYP_CD"
            - name: MSG_TYP
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MSG_TYP"
            - name: MSG_TX
              type: varchar
              width: 200
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MSG_TX"
            - name: CLNT_MBRSH_ID
              type: varchar
              width: 50
              map: "lnk_IdsMbr_PdxDeniedPkey_in.CLNT_MBRSH_ID"
            - name: MEDIA_TYP_CD
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MEDIA_TYP_CD"
            - name: RSPN_CD
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.RSPN_CD"
            - name: DESC
              type: varchar
              width: 500
              map: "lnk_IdsMbr_PdxDeniedPkey_in.DESC"
            - name: CHAPTER_ID
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.CHAPTER_ID"
            - name: CHAPTER_DESC
              type: varchar
              width: 500
              map: "lnk_IdsMbr_PdxDeniedPkey_in.CHAPTER_DESC"
            - name: PDX_NPI
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PDX_NPI"
            - name: NDC
              type: varchar
              width: 20
              map: "lnk_IdsMbr_PdxDeniedPkey_in.NDC"
        - name: Cpy.V0S33P5
          type: map
          source: ds_MBR_PDX_DENIED_TRANS_xfm
          alias: lnk_IdsMbr_PdxDeniedPkey_in
          schema: 
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.MBR_UNIQ_KEY"
            - name: PDX_NTNL_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PDX_NTNL_PROV_ID"
            - name: TRANS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.TRANS_TYP_CD"
            - name: TRANS_DENIED_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.TRANS_DENIED_DT"
            - name: RX_NO
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.RX_NO"
            - name: PRCS_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.PRCS_DT"
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CD"
            - name: SRC_SYS_CLM_RCVD_DT
              type: date
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CLM_RCVD_DT"
            - name: SRC_SYS_CLM_RCVD_TM
              type: time
              nullable: N
              map: "lnk_IdsMbr_PdxDeniedPkey_in.SRC_SYS_CLM_RCVD_TM"


