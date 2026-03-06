model:
  config:
    origin: datastage
  jobs:
    - name: IdsDMDrugClmAccumImpctExtr
      origin: datastage
      type: dataflow
      folder: "WebDataMart/Claim/Claim"
      description: "Extract from IDS table DRUG_CLM_ACCUM_IMPCT to create input file for DataMart table CLM_DM_DRUG_CLM_ACCUM_IMPCT."
      fullDescription: "COPYRIGHT 2023 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n\nCALLED BY :  OptumDrugCntl\n \nDESCRIPTION:  Extract from IDS table DRUG_CLM_ACCUM_IMPCT to create input file forDataMart table CLM_DM_DRUG_CLM_ACCUM_IMPCT.\n\n\nMODIFICATIONS:\n\nDeveloper                     Date               Project                 Change Description                                                                                                   Development Project    Code Reviewer               Date Reviewed\n--------------------------          --------------------    ----------------------      ------------------------------------------------------------------------------------------------------------------------------    ---------------------------------    ------------------------------------    ----------------------------              \nBill Schroeder              07/27/2023     US-586570          Original Programming                                                                                                 IntegrateDev2                Jeyaprasanna                2023-08-15"
      parameters:
        - name: IDSAcct
          type: string
          description: "IDS Account"
        - name: IDSDB
          type: string
          description: "IDS Database"
          defaultType: project
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
        - name: IDSOwner
          type: string
          description: "IDS Table Owner"
          defaultType: project
        - name: RunCycle
          type: string
          description: RunCycle
      steps:
        - name: Lkup_Codes
          type: lookup
          option:
            joinType: left
          sources: 
            - name: Trans_Coupon
              alias: Lnk_Trans_Extr
              joinFilter: ""
              joinExpression: []
            - name: DB2_CD_MPPNG_Coupon_Nm
              alias: Lnk_Lkup_Coupon_Nm
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_Lkup_Coupon_Nm.SRC_CD
                toExpr: Lnk_Trans_Extr.NCP_COUPON_TYP_CD
                caseInSensitive: false
            - name: DB2_CD_MPPNG_Src_Cd
              alias: Lnk_Lkup_Src_Cd
              joinFilter: ""
              joinExpression: 
              - fromExpr: Lnk_Lkup_Src_Cd.CD_MPPNG_SK
                toExpr: Lnk_Trans_Extr.SRC_SYS_CD_SK
                caseInSensitive: false
            # mutiple reference detected
          joinCondition: 
            - expression: Lnk_Lkup_Coupon_Nm.SRC_CD = Lnk_Trans_Extr.NCP_COUPON_TYP_CD
              caseInSensitive: false
            - expression: Lnk_Lkup_Src_Cd.CD_MPPNG_SK = Lnk_Trans_Extr.SRC_SYS_CD_SK
              caseInSensitive: false
          schema: 
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Trans_Extr.CLM_ID"
            - name: CCAA_APLD_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Trans_Extr.CCAA_APLD_IN"
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Trans_Extr.CCAA_COUPON_AMT"
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Trans_Extr.FMLY_ACCUM_DEDCT_AMT"
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Trans_Extr.FMLY_ACCUM_OOP_AMT"
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Trans_Extr.INDV_ACCUM_DEDCT_AMT"
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Trans_Extr.INDV_ACCUM_OOP_AMT"
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Trans_Extr.INDV_APLD_DEDCT_AMT"
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Trans_Extr.INDV_APLD_OOP_AMT"
            - name: CD_MPPNG_SK_Src_Sys
              type: int
              width: 4
              nullable: N
              map: "Lnk_Lkup_Src_Cd.CD_MPPNG_SK"
            - name: TRGT_CD_Src_Sys
              type: varchar
              width: 20
              map: "Lnk_Lkup_Src_Cd.TRGT_CD"
            - name: NCP_COUPON_TYP_CD
              type: varchar
              width: 20
              map: "Lnk_Lkup_Coupon_Nm.TRGT_CD"
            - name: NCP_COUPON_TYP_NM
              type: varchar
              width: 255
              map: "Lnk_Lkup_Coupon_Nm.TRGT_CD_NM"
          keys:
            - CD_MPPNG_SK_Src_Sys
        - name: CLM_DM_DRUG_CLM_ACCUM_IMPCT_extract
          type: fileWriter
          urls: 
            - "#$FilePath#/load/CLM_DM_DRUG_CLM_ACCUM_IMPCT.dat"
          source: Trans_Src_Sys
          rejectMode: continue
          option: 
            updateMode: overwrite
            writeMode: SingleFile
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
            rowDelimiter: "\n"
            fixedLength: false
        - name: DRUG_CLM_ACCUM_IMPCT
          type: dbReader
          originalDb: db2
          query: "SELECT \n\x09CLM.DRUG_CLM_SK, \n\x09CLM.SRC_SYS_CD_SK, \n\x09CLM.CLM_ID, \n\x09CLM.CRT_RUN_CYC_EXCTN_SK, \n\x09CLM.LAST_UPDT_RUN_CYC_EXCTN_SK, \n\x09CLM.NCP_COUPON_TYP_CD_SK, \n\x09CLM.CCAA_APLD_IN, \n\x09CLM.CCAA_COUPON_AMT, \n\x09CLM.FMLY_ACCUM_DEDCT_AMT, \n\x09CLM.FMLY_ACCUM_OOP_AMT, \n                CLM.INDV_ACCUM_DEDCT_AMT, \n\x09CLM.INDV_ACCUM_OOP_AMT, \n\x09CLM.INDV_APLD_DEDCT_AMT, \n\x09CLM.INDV_APLD_OOP_AMT\nFROM\n    #$IDSOwner#.DRUG_CLM_ACCUM_IMPCT CLM,\n #$IDSOwner#.W_WEBDM_ETL_DRVR DRVR\nWHERE  \nDRVR.SRC_SYS_CD_SK = CLM.SRC_SYS_CD_SK  AND\nDRVR.CLM_ID = CLM.CLM_ID \nWITH UR;"
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
            - name: NCP_COUPON_TYP_CD_SK
              type: int
              width: 4
              nullable: N
            - name: CCAA_APLD_IN
              type: char
              width: 1
              nullable: N
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
          keys:
            - DRUG_CLM_SK
        - name: DB2_CD_MPPNG_Coupon_Nm
          type: dbReader
          originalDb: db2
          query: "SELECT CD_MPPNG_SK as SRC_CD,TRGT_CD_NM,TRGT_CD\n  FROM #$IDSOwner#.CD_MPPNG \n WHERE SRC_CLCTN_CD   = 'OPTUMRX' \n   AND SRC_SYS_CD     = 'OPTUMRX' \n   AND SRC_DOMAIN_NM  = 'NCP COUPON TYPE' \n   AND TRGT_CLCTN_CD  = 'IDS' \n   AND TRGT_DOMAIN_NM = 'NCP COUPON TYPE' \nunion\nSELECT \n CD_MPPNG_SK as SRC_CD,TRGT_CD_NM,TRGT_CD\n  FROM #$IDSOwner#.CD_MPPNG where  CD_MPPNG_SK='1'\nWITH UR;"
          schema: 
            - name: SRC_CD
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: TRGT_CD
              type: varchar
              width: 20
          keys:
            - SRC_CD
        - name: DB2_CD_MPPNG_Src_Cd
          type: dbReader
          originalDb: db2
          query: "SELECT \nCD_MPPNG_SK,\nTRGT_CD\nFROM #$IDSOwner#.CD_MPPNG \nWHERE\nTRGT_DOMAIN_NM = 'SOURCE SYSTEM'\nWITH UR;"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
          keys:
            - CD_MPPNG_SK
        - name: Trans_Src_Sys
          type: map
          source: Lkup_Codes
          alias: Lnk_Accum_trans
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(Lnk_Accum_trans.CD_MPPNG_SK_Src_Sys) Then ' ' Else Lnk_Accum_trans.TRGT_CD_Src_Sys"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_Accum_trans.CLM_ID"
            - name: NCP_COUPON_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(Lnk_Accum_trans.NCP_COUPON_TYP_CD) Then ' ' Else Lnk_Accum_trans.NCP_COUPON_TYP_CD"
            - name: NCP_COUPON_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: " If IsNull(Lnk_Accum_trans.NCP_COUPON_TYP_NM) Then ' ' Else Lnk_Accum_trans.NCP_COUPON_TYP_NM"
            - name: CCAA_APLD_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_Accum_trans.CCAA_APLD_IN"
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_Accum_trans.CCAA_COUPON_AMT"
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Accum_trans.FMLY_ACCUM_DEDCT_AMT"
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Accum_trans.FMLY_ACCUM_OOP_AMT"
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Accum_trans.INDV_ACCUM_DEDCT_AMT"
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Accum_trans.INDV_ACCUM_OOP_AMT"
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Accum_trans.INDV_APLD_DEDCT_AMT"
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_Accum_trans.INDV_APLD_OOP_AMT"
            - name: LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
              map: RunCycle
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Trans_Coupon
          type: map
          source: DRUG_CLM_ACCUM_IMPCT
          alias: Lnk_IDS_Accum
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "Lnk_IDS_Accum.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Lnk_IDS_Accum.CLM_ID"
            - name: CCAA_APLD_IN
              type: char
              width: 1
              nullable: N
              map: "Lnk_IDS_Accum.CCAA_APLD_IN"
            - name: CCAA_COUPON_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Lnk_IDS_Accum.CCAA_COUPON_AMT"
            - name: FMLY_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IDS_Accum.FMLY_ACCUM_DEDCT_AMT"
            - name: FMLY_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IDS_Accum.FMLY_ACCUM_OOP_AMT"
            - name: INDV_ACCUM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IDS_Accum.INDV_ACCUM_DEDCT_AMT"
            - name: INDV_ACCUM_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IDS_Accum.INDV_ACCUM_OOP_AMT"
            - name: INDV_APLD_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IDS_Accum.INDV_APLD_DEDCT_AMT"
            - name: INDV_APLD_OOP_AMT
              type: decimal
              width: 13
              precision: 2
              map: "Lnk_IDS_Accum.INDV_APLD_OOP_AMT"
            - name: NCP_COUPON_TYP_CD
              type: int
              width: 4
              nullable: N
              map: "Lnk_IDS_Accum.NCP_COUPON_TYP_CD_SK"


