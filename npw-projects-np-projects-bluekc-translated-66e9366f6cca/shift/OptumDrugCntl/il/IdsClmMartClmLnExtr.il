model:
  config:
    origin: datastage
  jobs:
    - name: IdsClmMartClmLnExtr
      origin: datastage
      type: dataflow
      folder: "WebDataMart/Claim/ClaimLine/ClaimLine"
      runAfterSubrOnJobSuccess: enabled
      description: "Claim Mart Claim Line Extract - Called by IdsClmMartExtrLoadSeq"
      fullDescription: "* VC LOGS *\n^1_2 11/04/09 07:53:48 Batch  15284_28434 PROMOTE bckcetl:31540 updt dsadm dsadm\n^1_2 10/19/09 16:31:27 Batch  15268_59491 PROMOTE bckcetl:31540 ids20 dsadm bls for sa\n^1_2 10/19/09 10:39:41 Batch  15268_38385 INIT bckcett:31540 testIDSnew dsadm bls for sa\n^1_2 10/05/09 18:22:38 Batch  15254_66186 PROMOTE bckcett:31540 testIDSnew u150906 4113-MBR360DM_Sharon_testIDSnew             Maddy\n^1_2 10/05/09 18:12:10 Batch  15254_65619 INIT bckcett:31540 devlIDSnew u150906 4113-Mbr360DM_Sharon_devlIDSnew             Maddy\n^1_1 09/25/09 17:45:02 Batch  15244_63926 INIT bckcett:31540 devlIDSnew u150906 4113-Mbr360_Sharon_devlIDSnew          Maddy\n^1_1 08/28/08 11:14:23 Batch  14851_40472 PROMOTE bckcetl ids20 dsadm bls for sa\n^1_1 08/28/08 11:06:31 Batch  14851_39994 INIT bckcett testIDS dsadm BLS FOR SA\n^1_1 08/22/08 07:08:02 Batch  14845_25699 PROMOTE bckcett testIDS u03651 steph for sharon 3057\n^1_1 08/22/08 07:06:03 Batch  14845_25567 INIT bckcett devlIDSnew u03651 steffy\n^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm\n^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm\n^1_8 07/18/07 14:21:56 Batch  14444_51727 PROMOTE bckcetl ids20 dsadm bls for hs\n^1_8 07/18/07 14:18:50 Batch  14444_51533 INIT bckcett testIDSnew dsadm bls for hs\n^1_2 07/11/07 15:03:17 Batch  14437_54205 PROMOTE bckcett testIDSnew u11141 Hugh Sisson\n^1_2 07/11/07 15:00:27 Batch  14437_54033 INIT bckcett devlIDS30 u11141 Hugh Sisson\n^1_1 07/11/07 10:52:00 Batch  14437_39130 INIT bckcett devlIDS30 u11141 Hugh Sisson\n^1_2 04/18/07 14:50:09 Batch  14353_53412 INIT bckcetl ids20 dcg01 sa\n^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm\n^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm\n^1_7 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006\n^1_7 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006\n^1_1 11/17/06 16:01:03 Batch  14201_57667 PROMOTE bckcett testIDS30 u11141 CDHP system testing - Hugh Sisson\n^1_1 11/17/06 15:57:58 Batch  14201_57500 INIT bckcett devlIDS30 u11141 CDHP system testing - Hugh Sisson\n^1_5 08/10/06 07:54:51 Batch  14102_28496 INIT bckcett devlIDS30 u08717 Brent\n^1_4 07/21/06 15:13:25 Batch  14082_54812 INIT bckcett devlIDS30 u11141 Hugh Sisson\n^1_3 07/18/06 15:24:59 Batch  14079_55507 PROMOTE bckcett devlIDS30 u11141 Hugh Sisson\n^1_5 07/14/06 14:47:09 Batch  14075_53239 PROMOTE bckcetl ids20 dsadm Jim Hart\n^1_5 07/14/06 14:42:55 Batch  14075_52993 INIT bckcett testIDS30 dsadm Jim Hart\n^1_2 07/14/06 12:15:25 Batch  14075_44129 PROMOTE bckcett testIDS30 u11141 Hugh Sisson\n^1_2 07/14/06 12:13:43 Batch  14075_44082 INIT bckcett devlIDS30 u11141 Hugh Sisson\n^1_1 07/07/06 14:45:43 Batch  14068_53151 PROMOTE bckcett devlIDS30 u11141 Hugh Sisson\n^1_4 06/05/06 10:28:06 Batch  14036_37701 PROMOTE bckcetl ids20 dsadm J. Mahaffey for O. Nielsen\n^1_4 06/05/06 10:23:52 Batch  14036_37451 INIT bckcett testIDS30 dsadm J. Mahaffey for O. Nielsen\n^1_7 05/19/06 09:50:58 Batch  14019_35469 PROMOTE bckcett testIDS30 u10913 Ollie moving KCREE to test\n^1_7 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test\n^1_6 05/01/06 10:02:53 Batch  14001_36182 INIT bckcett devlIDS30 u11141 Hugh Sisson\n^1_5 04/28/06 14:03:05 Batch  13998_50597 INIT bckcett devlIDS30 u11141 Hugh Sisson\n^1_3 04/07/06 12:18:30 Batch  13977_44314 INIT bckcett devlIDS30 u08717 Brent\n\n\n© Copyright 2007 Blue Cross and Blue Shield of Kansas City\n\nCalled by:\n                    IdsClmMartExtrLoadSeq\n\nProcessing:\n                    Extract and load claim line data based on driver table W_WEBDM_ETL_DRVR\n\nControl Job Rerun Information: \n             Previous Run Successful:    Verify contents of driver table - W_WEBDM_ETL_DRVR\n             Previous Run Aborted:         Restart\n\nModifications:                        \n                                              Project/                                                                                                                                                                              Development              Code                     Date\nDeveloper           Date              Altiris #           Change Description                                                                                                                                  Project                       Reviewer               Reviewed\n----------------------     -------------------   -------------------   -------------------------------------------------------------------------------------------------------------------------------------------------------------    -------------------------     -------------------             ---------------------\nSteph Goddard   03/22/2005                         Originally Programmed\nRalph Tucker     04/19/2005                          Added Proc_Cd_Desc field; populated by hit of hf_clmmart_proc_cd_xref hash file.\nSteph Goddard   04/29/2005                         Changed room type code to room price method code - name and source changed, \n                                                                           not position in output record\nSteph Goddard   05/06/2005                         Check from/to dates for UNK and NA\nBJ Luce              08/29/2005                         Change SQL parameters to ClmMart for job sequencer\nBrent Leland       12/29/2005                         Added extract and load for balancing\nBrent Leland       04/03/2006                         Changed to use evironment parameters\n                                                                       Changed name of driver table\nBrent Leland       04/06/2006                         Added run cycle parameter for new table field LAST_UPDT_RUN_CYC_NO\nHugh Sisson       04/24/2006                         Added 23 new columns to support Care Management Implementation (Project #1762)\n                                                                       Included Procedure Code lookup into the SQL and dropped the hf_clmmart_proc_cd_xref hash file\nHugh Sisson       07/12/2006                         Made diagnosis codes extraction into a separate SQL to ensure that claim lines that do not \n                                                                          have a primary diagnosis code (e.g. drug and dental claims) will be pulled into the Mart.              \nBrent Leland       07/15/2006                         Changed Claim Diag SQL\nRalph Tucker     10/25/2006                         Added  new PCA fields to the end of the table.\nHugh Sisson       05/22/2007  CDHP Drug    Removed reference to PCA in CLM_LN_PCA_LOB_CD, CLM_LN_PCA_LOB_NM, \n                                                                          CLM_LN_PCA_PRCS_CD, CLM_LN_PCA_PRCS_NM, CLM_LN_PCA_CNSD_AMT, \n                                                                          CLM_LN_PCA_PD_AMT, and CLM_LN_PCA_SUB_PD_AMT\n                                                                       Added HASH.CLEAR call\n\nParik                  2008-08-01  3057(web claim)   Added the new Claim Line Remit lookup logic                                                                                       devlIDSnew             Steph Goddard        08/11/2008\n\nSAndrew            2008-08-15  3057(web claim)   Adding more detailed description \n\x09                                                          Added 4 new fields to end of DM table CLM_LN_REMIT_PATN_RESP_AMT,  \n                                                                          CLM_LN_REMIT_PROV_WRT_OFF_AMT,  CLM_LN_REMIT_MBR_OTHR_LIAB_AMT, CLM_LN_REMIT_NO_RESP_AMT.                                                                                                                                                        \n                                                                          Incorporated lookup to new ids table #$IDSOwner#.CLM_LN_REMIT REMIT.    added hash file hf_cmcl_clmln_remit\n\nTerri O'Bryan    2008-09-01  4113 -Member360   Added 2 new fields to end of DM table CLM_DM_CLM_LN:                                                               devlIDSnew              Steph Goddard        09/22/2009\n                                                                            CLM_LN_FINL_DISP_CD, CLM_LN_FINL_DISP_NM"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: RunCycle
          type: integer
          description: "Run Cycle"
          defaultType: expr
          defaultValue: "901"
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
          defaultType: expr
          defaultValue: u150610
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
          defaultType: expr
          defaultValue: "LEI@1KVHN9;>0O4I<;N=AO@F"
        - name: ClmMartServer
          type: string
          description: "Claim Mart Server"
          defaultType: project
        - name: ClmMartDB
          type: string
          description: "Claim Mart Database"
          defaultType: project
        - name: ClmMartOwner
          type: string
          description: "Claim Mart Table Owner"
          defaultType: project
        - name: ClmMartAcct
          type: string
          description: "Claim Mart Account"
          defaultType: expr
          defaultValue: s150610
        - name: ClmMartPW
          type: encrypted
          description: "Claim Mart Password"
          defaultType: expr
          defaultValue: "LEJ@1HVHO92<0G7I=8J<0OAF"
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\8;hf_clmmart_excd_xref;hf_cmcl_diag_cd;hf_cmcl_diag_ord;hf_clmmart_clmln_pca;hf_cmcl_clmln_lob;hf_cmcl_pca_cnsd_amt;hf_cmcl_pca_prcs_cd;hf_cmcl_clmln_remit"
        subr: "8;hf_clmmart_excd_xref;hf_cmcl_diag_cd;hf_cmcl_diag_ord;hf_clmmart_clmln_pca;hf_cmcl_clmln_lob;hf_cmcl_pca_cnsd_amt;hf_cmcl_pca_prcs_cd;hf_cmcl_clmln_remit"
      steps:
        - name: cd_mppng
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnRoomPricMeth
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refSrcSys
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnTos
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnPos
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnPreauth
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnReferral
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnRoomTyp
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnLob
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnPrcs
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_etrnl_cd_mppng.refClmLnFinlDisp
          type: dbReader
          originalDb: hashedFile
          table: "/hf_etrnl_cd_mppng"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
            - name: TRGT_CD_NM
              type: varchar
              width: 255
            - name: SRC_CD
              type: varchar
              width: 20
            - name: SRC_CD_NM
              type: varchar
              width: 255
          keys:
            - CD_MPPNG_SK
            # Hashed File has no directory specified
        - name: hf_clmmart_excd_xref.refDsalwExcd
          type: map
          source: ""
          alias: lnkExcdXref
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "lnkExcdXref.EXCD_SK"
            - name: EXCD_ID
              type: char
              width: 4
              nullable: N
              map: "lnkExcdXref.EXCD_ID"
            - name: EXCD_SH_TX
              type: varchar
              width: 70
              map: "lnkExcdXref.EXCD_SH_TX"
        - name: hf_clmmart_excd_xref.refEobExcd
          type: map
          source: ""
          alias: lnkExcdXref
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: EXCD_SK
              type: int
              width: 4
              nullable: N
              map: "lnkExcdXref.EXCD_SK"
            - name: EXCD_ID
              type: char
              width: 4
              nullable: N
              map: "lnkExcdXref.EXCD_ID"
            - name: EXCD_SH_TX
              type: varchar
              width: 70
              map: "lnkExcdXref.EXCD_SH_TX"
        - name: hf_cmcl_diag_cd
          type: map
          source: Trans3
          alias: DiagCd
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DiagCd.SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DiagCd.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: "DiagCd.CLM_LN_SEQ_NO"
            - name: DIAG_CD
              type: varchar
              width: 20
              nullable: N
              map: "DiagCd.DIAG_CD"
            - name: DIAG_CD_DESC
              type: varchar
              width: 255
              nullable: N
              map: "DiagCd.DIAG_CD_DESC"
            - name: CLM_LN_DIAG_ORDNL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DiagCd.CLM_LN_DIAG_ORDNL_CD_SK"
        - name: hf_cmcl_diag_ord
          type: map
          source: ""
          alias: CodeMapping
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CD_MPPNG_SK
              type: int
              width: 4
              nullable: N
              map: "CodeMapping.CD_MPPNG_SK"
        - name: hf_cmcl_clmln_lob
          type: map
          source: ""
          alias: ClmLnPcaLobCdExtract
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLnPcaLobCdExtract.CLM_SK"
            - name: CLM_LN_PCA_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLnPcaLobCdExtract.CLM_LN_PCA_LOB_CD"
            - name: CLM_LN_PCA_LOB_NM
              type: varchar
              width: 255
              nullable: N
              map: "ClmLnPcaLobCdExtract.CLM_LN_PCA_LOB_NM"
        - name: hf_cmcl_pca_prcs_cd
          type: map
          source: ""
          alias: PcaPrcsCdExtract
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "PcaPrcsCdExtract.CLM_SK"
            - name: PCA_PRCS_CD
              type: varchar
              width: 20
              nullable: N
              map: "PcaPrcsCdExtract.PCA_PRCS_CD"
            - name: PCA_PRCS_CD_NM
              type: varchar
              width: 255
              nullable: N
              map: "PcaPrcsCdExtract.PCA_PRCS_CD_NM"
        - name: hf_cmcl_pca_cnsd_amt
          type: map
          source: ""
          alias: PcaAmtExtract
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "PcaAmtExtract.CLM_SK"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "PcaAmtExtract.CHRG_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "PcaAmtExtract.PAYBL_AMT"
        - name: hf_cmcl_clmln_remit
          type: map
          source: ""
          alias: ClmLnRemit
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLnRemit.CLM_LN_SK"
            - name: REMIT_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnRemit.REMIT_PATN_RESP_AMT"
            - name: REMIT_PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnRemit.REMIT_PROV_WRT_OFF_AMT"
            - name: REMIT_MBR_OTHR_LIAB_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnRemit.REMIT_MBR_OTHR_LIAB_AMT"
            - name: REMIT_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnRemit.REMIT_NO_RESP_AMT"
        - name: hf_clmmart_clmln_pca
          type: map
          source: Trans4
          alias: ClmLnPca
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: "ClmLnPca.CLM_LN_SK"
            - name: CLM_LN_PCA_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLnPca.CLM_LN_PCA_LOB_CD"
            - name: CLM_LN_PCA_LOB_NM
              type: varchar
              width: 100
              map: "ClmLnPca.CLM_LN_PCA_LOB_NM"
            - name: CLM_LN_PCA_PRCS_CD
              type: varchar
              width: 20
              nullable: N
              map: "ClmLnPca.CLM_LN_PCA_PRCS_CD"
            - name: CLM_LN_PCA_PRCS_NM
              type: varchar
              width: 100
              nullable: N
              map: "ClmLnPca.CLM_LN_PCA_PRCS_NM"
            - name: CNSD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPca.CNSD_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPca.DSALW_AMT"
            - name: NONCNSD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPca.NONCNSD_AMT"
            - name: PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPca.PD_AMT"
            - name: PROV_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPca.PROV_PD_AMT"
            - name: SUB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPca.SUB_PD_AMT"
            - name: EXCD_ID
              type: char
              width: 4
              nullable: N
              map: "ClmLnPca.EXCD_ID"
            - name: EXCD_SH_TX
              type: varchar
              width: 70
              map: "ClmLnPca.EXCD_SH_TX"
        - name: IDSClm_Ln
          type: dbReader
          originalDb: db2
        - name: IDS_ClmLn
          type: dbReader
          originalDb: db2
          query: "SELECT LN.SRC_SYS_CD_SK as SRC_SYS_CD_SK,LN.CLM_ID as CLM_ID,LN.CLM_LN_SEQ_NO as CLM_LN_SEQ_NO FROM #$IDSOwner#.CLM_LN LN, #$IDSOwner#.W_WEBDM_ETL_DRVR EXTR WHERE LN.SRC_SYS_CD_SK = EXTR.SRC_SYS_CD_SK and LN.CLM_ID = EXTR.CLM_ID"
          table: "#$IDSOwner#.CLM_LN LN, #$IDSOwner#.W_WEBDM_ETL_DRVR EXTR"
          tableAction: append
          schema: 
            - name: SRC_SYS_CD_SK
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
        - name: Trans1
          type: lookup
          stageVars:
            - name: startdate
              type: decimal
              width: 0
              map: " If Trim(Extract.SVC_STRT_DT_SK) = 'NA' Or Trim(Extract.SVC_STRT_DT_SK) = 'UNK' then @NULL else Extract.SVC_STRT_DT_SK"
              initValue: "0"
            - name: enddate
              type: decimal
              width: 0
              map: " If Trim(Extract.SVC_END_DT_SK) = 'NA' Or Trim(Extract.SVC_END_DT_SK) = 'UNK' then @NULL else Extract.SVC_END_DT_SK"
              initValue: "0"
          option:
            joinType: left
          sources: 
            - name: 
              alias: Extract
              joinFilter: ""
              joinExpression: []
            - name: hf_etrnl_cd_mppng.refClmLnRoomPricMeth
              alias: refClmLnRoomPricMeth
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnRoomPricMeth.CD_MPPNG_SK"
                toExpr: "Extract.CLM_LN_ROOM_PRICE_METH_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.refSrcSys
              alias: refSrcSys
              joinFilter: ""
              joinExpression:
              - fromExpr: "refSrcSys.CD_MPPNG_SK"
                toExpr: "Extract.SRC_SYS_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.refClmLnTos
              alias: refClmLnTos
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnTos.CD_MPPNG_SK"
                toExpr: "Extract.CLM_LN_TOS_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.refClmLnPos
              alias: refClmLnPos
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnPos.CD_MPPNG_SK"
                toExpr: "Extract.CLM_LN_POS_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.refClmLnPreauth
              alias: refClmLnPreauth
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnPreauth.CD_MPPNG_SK"
                toExpr: "Extract.CLM_LN_PREAUTH_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.refClmLnReferral
              alias: refClmLnReferral
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnReferral.CD_MPPNG_SK"
                toExpr: "Extract.CLM_LN_RFRL_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.refClmLnRoomTyp
              alias: refClmLnRoomTyp
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnRoomTyp.CD_MPPNG_SK"
                toExpr: "Extract.CLM_LN_ROOM_TYP_CD_SK"
                caseInSensitive: true
            - name: hf_clmmart_excd_xref.refDsalwExcd
              alias: refDsalwExcd
              joinFilter: ""
              joinExpression:
              - fromExpr: "refDsalwExcd.EXCD_SK"
                toExpr: "Extract.CLM_LN_DSALW_EXCD_SK"
                caseInSensitive: true
            - name: hf_clmmart_excd_xref.refEobExcd
              alias: refEobExcd
              joinFilter: ""
              joinExpression:
              - fromExpr: "refEobExcd.EXCD_SK"
                toExpr: "Extract.CLM_LN_EOB_EXCD_SK"
                caseInSensitive: true
            - name: hf_cmcl_diag_cd
              alias: refDiagCd
              joinFilter: ""
              joinExpression:
              - fromExpr: "refDiagCd.SRC_SYS_CD_SK"
                toExpr: "Extract.SRC_SYS_CD_SK"
                caseInSensitive: true
              - fromExpr: "refDiagCd.CLM_ID"
                toExpr: "Extract.CLM_ID"
                caseInSensitive: true
              - fromExpr: "refDiagCd.CLM_LN_SEQ_NO"
                toExpr: "Extract.CLM_LN_SEQ_NO"
                caseInSensitive: true
            - name: hf_clmmart_clmln_pca
              alias: refClmLnPca
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnPca.CLM_LN_SK"
                toExpr: "Extract.CLM_LN_SK"
                caseInSensitive: true
            - name: hf_cmcl_clmln_lob
              alias: refClmTypCd
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmTypCd.CLM_SK"
                toExpr: "Extract.CLM_SK"
                caseInSensitive: true
            - name: hf_cmcl_pca_prcs_cd
              alias: refPcaPrcs
              joinFilter: ""
              joinExpression:
              - fromExpr: "refPcaPrcs.CLM_SK"
                toExpr: "Extract.CLM_SK"
                caseInSensitive: true
            - name: hf_cmcl_pca_cnsd_amt
              alias: refPcaAmt
              joinFilter: ""
              joinExpression:
              - fromExpr: "refPcaAmt.CLM_SK"
                toExpr: "Extract.CLM_SK"
                caseInSensitive: true
            - name: hf_cmcl_clmln_remit
              alias: ClmLnRemitCd
              joinFilter: ""
              joinExpression:
              - fromExpr: "ClmLnRemitCd.CLM_LN_SK"
                toExpr: "Extract.CLM_LN_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.refClmLnFinlDisp
              alias: refClmLnFinlDisp
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnFinlDisp.CD_MPPNG_SK"
                toExpr: "Extract.CLM_LN_FINL_DISP_CD_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "refClmLnRoomPricMeth.CD_MPPNG_SK = Extract.CLM_LN_ROOM_PRICE_METH_CD_SK"
              caseInSensitive: true
            - expression: "refSrcSys.CD_MPPNG_SK = Extract.SRC_SYS_CD_SK"
              caseInSensitive: true
            - expression: "refClmLnTos.CD_MPPNG_SK = Extract.CLM_LN_TOS_CD_SK"
              caseInSensitive: true
            - expression: "refClmLnPos.CD_MPPNG_SK = Extract.CLM_LN_POS_CD_SK"
              caseInSensitive: true
            - expression: "refClmLnPreauth.CD_MPPNG_SK = Extract.CLM_LN_PREAUTH_CD_SK"
              caseInSensitive: true
            - expression: "refClmLnReferral.CD_MPPNG_SK = Extract.CLM_LN_RFRL_CD_SK"
              caseInSensitive: true
            - expression: "refClmLnRoomTyp.CD_MPPNG_SK = Extract.CLM_LN_ROOM_TYP_CD_SK"
              caseInSensitive: true
            - expression: "refDsalwExcd.EXCD_SK = Extract.CLM_LN_DSALW_EXCD_SK"
              caseInSensitive: true
            - expression: "refEobExcd.EXCD_SK = Extract.CLM_LN_EOB_EXCD_SK"
              caseInSensitive: true
            - expression: "refDiagCd.SRC_SYS_CD_SK = Extract.SRC_SYS_CD_SK"
              caseInSensitive: true
            - expression: "refDiagCd.CLM_ID = Extract.CLM_ID"
              caseInSensitive: true
            - expression: "refDiagCd.CLM_LN_SEQ_NO = Extract.CLM_LN_SEQ_NO"
              caseInSensitive: true
            - expression: "refClmLnPca.CLM_LN_SK = Extract.CLM_LN_SK"
              caseInSensitive: true
            - expression: "refClmTypCd.CLM_SK = Extract.CLM_SK"
              caseInSensitive: true
            - expression: "refPcaPrcs.CLM_SK = Extract.CLM_SK"
              caseInSensitive: true
            - expression: "refPcaAmt.CLM_SK = Extract.CLM_SK"
              caseInSensitive: true
            - expression: "ClmLnRemitCd.CLM_LN_SK = Extract.CLM_LN_SK"
              caseInSensitive: true
            - expression: "refClmLnFinlDisp.CD_MPPNG_SK = Extract.CLM_LN_FINL_DISP_CD_SK"
              caseInSensitive: true
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(refSrcSys.TRGT_CD) = @TRUE then ' ' else refSrcSys.TRGT_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: "Extract.CLM_LN_SEQ_NO"
            - name: CLM_LN_RVNU_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.RVNU_CD"
            - name: CLM_LN_ROOM_PRICE_METH_CD
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(refClmLnRoomPricMeth.TRGT_CD) = @TRUE then ' ' else refClmLnRoomPricMeth.TRGT_CD"
            - name: CLM_LN_TOS_CD
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(refClmLnTos.TRGT_CD) = @TRUE then ' ' else refClmLnTos.TRGT_CD"
            - name: PROC_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.PROC_CD"
            - name: CLM_LN_CAP_LN_IN
              type: char
              width: 1
              nullable: N
              map: "Extract.CAP_LN_IN"
            - name: CLM_LN_SVC_STRT_DT
              type: timestamp
              map: startdate
            - name: CLM_LN_SVC_END_DT
              type: timestamp
              map: enddate
            - name: CLM_LN_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CHRG_AMT"
            - name: CLM_LN_PAYBL_TO_PROV_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.PAYBL_TO_PROV_AMT"
            - name: CLM_LN_UNIT_CT
              type: int
              width: 4
              nullable: N
              map: "Extract.UNIT_CT"
            - name: CLM_LN_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.SVC_ID"
            - name: PROC_CD_DESC
              type: varchar
              width: 255
              nullable: N
              map: "Extract.PROC_CD_DESC"
            - name: CLM_LN_POS_CD
              type: varchar
              width: 20
              map: " If ISNULL(refClmLnPos.TRGT_CD) = @TRUE Or Len(Trim(refClmLnPos.TRGT_CD)) = 0 then ' ' else refClmLnPos.TRGT_CD"
            - name: CLM_LN_POS_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(refClmLnPos.TRGT_CD_NM) = @TRUE Or Len(Trim(refClmLnPos.TRGT_CD_NM)) = 0 then ' ' else refClmLnPos.TRGT_CD_NM"
            - name: CLM_LN_PREAUTH_CD
              type: varchar
              width: 20
              map: " If ISNULL(refClmLnPreauth.TRGT_CD) = @TRUE Or Len(Trim(refClmLnPreauth.TRGT_CD)) = 0 then ' ' else refClmLnPreauth.TRGT_CD"
            - name: CLM_LN_PREAUTH_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(refClmLnPreauth.TRGT_CD_NM) = @TRUE Or Len(Trim(refClmLnPreauth.TRGT_CD_NM)) = 0 then ' ' else refClmLnPreauth.TRGT_CD_NM"
            - name: CLM_LN_RFRL_CD
              type: varchar
              width: 20
              map: " If ISNULL(refClmLnReferral.TRGT_CD) = @TRUE Or Len(Trim(refClmLnReferral.TRGT_CD)) = 0 then ' ' else refClmLnReferral.TRGT_CD"
            - name: CLM_LN_RFRL_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(refClmLnReferral.TRGT_CD_NM) = @TRUE Or Len(Trim(refClmLnReferral.TRGT_CD_NM)) = 0 then ' ' else refClmLnReferral.TRGT_CD_NM"
            - name: CLM_LN_RVNU_NM
              type: varchar
              width: 255
              nullable: N
              map: "Extract.RVNU_CD_DESC"
            - name: CLM_LN_ROOM_TYP_CD
              type: varchar
              width: 20
              map: " If ISNULL(refClmLnRoomTyp.TRGT_CD) = @TRUE Or Len(Trim(refClmLnRoomTyp.TRGT_CD)) = 0 then ' ' else refClmLnRoomTyp.TRGT_CD"
            - name: CLM_LN_ROOM_TYP_NM
              type: varchar
              width: 75
              nullable: N
              map: " If ISNULL(refClmLnRoomTyp.TRGT_CD_NM) = @TRUE Or Len(Trim(refClmLnRoomTyp.TRGT_CD_NM)) = 0 then ' ' else refClmLnRoomTyp.TRGT_CD_NM"
            - name: CLM_LN_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.ALW_AMT"
            - name: CLM_LN_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.COINS_AMT"
            - name: CLM_LN_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CNSD_CHRG_AMT"
            - name: CLM_LN_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.COPAY_AMT"
            - name: CLM_LN_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.DEDCT_AMT"
            - name: CLM_LN_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.DSALW_AMT"
            - name: CLM_LN_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.PAYBL_AMT"
            - name: CLM_LN_ALW_PRICE_UNIT_CT
              type: int
              width: 4
              nullable: N
              map: "Extract.ALW_PRICE_UNIT_CT"
            - name: DSALW_EXCD_ID
              type: varchar
              width: 20
              nullable: N
              map: " if len(trim(refDsalwExcd.EXCD_ID)) = 0 then ' ' else  if trim(refDsalwExcd.EXCD_ID) = 'NA' or trim(refDsalwExcd.EXCD_ID) = 'UNK' then ' ' else refDsalwExcd.EXCD_ID"
            - name: DSALW_EXCD_DESC
              type: varchar
              width: 255
              nullable: N
              map: " If IsNull(refDsalwExcd.EXCD_SH_TX) = @TRUE Then \" \" Else refDsalwExcd.EXCD_SH_TX"
            - name: EOB_EXCD_ID
              type: varchar
              width: 20
              nullable: N
              map: " if len(trim(refEobExcd.EXCD_ID)) = 0 then ' ' else  if trim(refEobExcd.EXCD_ID) = 'NA' or trim(refEobExcd.EXCD_ID) = 'UNK' then ' ' else refEobExcd.EXCD_ID"
            - name: PRI_DIAG_CD
              type: varchar
              width: 20
              nullable: N
              map: " If (IsNull(refDiagCd.DIAG_CD) = @TRUE Or Len(refDiagCd.DIAG_CD) = 0) Then Space(1) Else TRIM(refDiagCd.DIAG_CD)"
            - name: PRI_DIAG_DESC
              type: varchar
              width: 255
              nullable: N
              map: " If (IsNull(refDiagCd.DIAG_CD_DESC) = @TRUE Or Len(refDiagCd.DIAG_CD_DESC) = 0) Then Space(1) Else TRIM(refDiagCd.DIAG_CD_DESC)"
            - name: SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.PROV_ID"
            - name: LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: CLM_LN_PCA_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: " if len(trim(refClmLnPca.CLM_LN_PCA_LOB_CD)) = 0 then  if IsNull(refClmTypCd.CLM_LN_PCA_LOB_CD) = @TRUE then ' ' else refClmTypCd.CLM_LN_PCA_LOB_CD else refClmLnPca.CLM_LN_PCA_LOB_CD"
            - name: CLM_LN_PCA_LOB_NM
              type: varchar
              width: 255
              nullable: N
              map: " if len(trim(refClmLnPca.CLM_LN_PCA_LOB_NM)) = 0 then  if IsNull(refClmTypCd.CLM_LN_PCA_LOB_NM) = @TRUE then ' ' else refClmTypCd.CLM_LN_PCA_LOB_NM else refClmLnPca.CLM_LN_PCA_LOB_NM"
            - name: CLM_LN_PCA_PRCS_CD
              type: varchar
              width: 20
              nullable: N
              map: " if len(trim(refClmLnPca.CLM_LN_PCA_PRCS_CD)) = 0 then  if IsNull(refPcaPrcs.PCA_PRCS_CD) = @TRUE then ' ' else refPcaPrcs.PCA_PRCS_CD else refClmLnPca.CLM_LN_PCA_PRCS_CD"
            - name: CLM_LN_PCA_PRCS_NM
              type: varchar
              width: 255
              nullable: N
              map: " if len(trim(refClmLnPca.CLM_LN_PCA_PRCS_NM)) = 0 then  if IsNull(refPcaPrcs.PCA_PRCS_CD_NM) = @TRUE then ' ' else refPcaPrcs.PCA_PRCS_CD_NM else refClmLnPca.CLM_LN_PCA_PRCS_NM"
            - name: CLM_LN_PCA_CNSD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(refClmLnPca.CNSD_AMT) = @TRUE then  if IsNull(refPcaAmt.CHRG_AMT) = @TRUE then 0 else refPcaAmt.CHRG_AMT else refClmLnPca.CNSD_AMT"
            - name: CLM_LN_PCA_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(refClmLnPca.DSALW_AMT) = @TRUE then 0 else refClmLnPca.DSALW_AMT"
            - name: CLM_LN_PCA_NONCNSD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(refClmLnPca.NONCNSD_AMT) = @TRUE then 0 else refClmLnPca.NONCNSD_AMT"
            - name: CLM_LN_PCA_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(refClmLnPca.PD_AMT) = @TRUE then  if IsNull(refPcaAmt.PAYBL_AMT) = @TRUE then 0 else refPcaAmt.PAYBL_AMT else refClmLnPca.PD_AMT"
            - name: CLM_LN_PCA_PROV_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(refClmLnPca.PROV_PD_AMT) = @TRUE then 0 else refClmLnPca.PROV_PD_AMT"
            - name: CLM_LN_PCA_SUB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " if IsNull(refClmLnPca.SUB_PD_AMT) = @TRUE then  if IsNull(refPcaAmt.PAYBL_AMT) = @TRUE then 0 else refPcaAmt.PAYBL_AMT else refClmLnPca.SUB_PD_AMT"
            - name: PCA_DSALW_EXCD_ID
              type: varchar
              width: 20
              nullable: N
              map: " if len(trim(refClmLnPca.EXCD_ID)) = 0 then ' ' else  if trim(refClmLnPca.EXCD_ID) = 'NA' or trim(refClmLnPca.EXCD_ID) = 'UNK' then ' ' else refClmLnPca.EXCD_ID"
            - name: PCA_DSALW_EXCD_DESC
              type: varchar
              width: 255
              nullable: N
              map: " if len(trim(refClmLnPca.EXCD_SH_TX)) = 0 then ' ' else  if refClmLnPca.EXCD_SH_TX = 'NA' or refClmLnPca.EXCD_SH_TX = 'UNK' then ' ' else refClmLnPca.EXCD_SH_TX"
            - name: CLM_LN_REMIT_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(ClmLnRemitCd.REMIT_PATN_RESP_AMT) = @TRUE then 0 else ClmLnRemitCd.REMIT_PATN_RESP_AMT"
            - name: CLM_LN_REMIT_PROV_WRT_OFF_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(ClmLnRemitCd.REMIT_PROV_WRT_OFF_AMT) = @TRUE then 0 else ClmLnRemitCd.REMIT_PROV_WRT_OFF_AMT"
            - name: CLM_LN_REMIT_MBR_OTHR_LIAB_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(ClmLnRemitCd.REMIT_MBR_OTHR_LIAB_AMT) = @TRUE then 0 else ClmLnRemitCd.REMIT_MBR_OTHR_LIAB_AMT"
            - name: CLM_LN_REMIT_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(ClmLnRemitCd.REMIT_NO_RESP_AMT) = @TRUE then 0 else ClmLnRemitCd.REMIT_NO_RESP_AMT"
            - name: CLM_LN_FINL_DISP_CD
              type: varchar
              width: 20
              map: " If (ISNULL(refClmLnFinlDisp.TRGT_CD) = @TRUE Or Len(Trim(refClmLnFinlDisp.TRGT_CD)) = 0) then ' ' else refClmLnFinlDisp.TRGT_CD"
            - name: CLM_LN_FINL_DISP_NM
              type: varchar
              width: 255
              map: " If (ISNULL(refClmLnFinlDisp.TRGT_CD_NM) = @TRUE Or Len(Trim(refClmLnFinlDisp.TRGT_CD_NM)) = 0) then ' ' else refClmLnFinlDisp.TRGT_CD_NM"
        - name: Trans3.lookup
          type: lookup
          option:
            joinType: left
          sources: 
            - name: 
              alias: DiagCdExtract
              joinFilter: ""
              joinExpression: []
            - name: hf_cmcl_diag_ord
              alias: CdMppng
              joinFilter: ""
              joinExpression:
              - fromExpr: "CdMppng.CD_MPPNG_SK"
                toExpr: "DiagCdExtract.CLM_LN_DIAG_ORDNL_CD_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "CdMppng.CD_MPPNG_SK = DiagCdExtract.CLM_LN_DIAG_ORDNL_CD_SK"
              caseInSensitive: true
          schema: 
            - name: DiagCd_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DiagCdExtract.SRC_SYS_CD_SK"
            - name: DiagCd_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DiagCdExtract.CLM_ID"
            - name: DiagCd_CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: "DiagCdExtract.CLM_LN_SEQ_NO"
            - name: DiagCd_DIAG_CD
              type: varchar
              width: 20
              nullable: N
              map: "DiagCdExtract.DIAG_CD"
            - name: DiagCd_DIAG_CD_DESC
              type: varchar
              width: 255
              nullable: N
              map: "DiagCdExtract.DIAG_CD_DESC"
            - name: DiagCd_CLM_LN_DIAG_ORDNL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DiagCdExtract.CLM_LN_DIAG_ORDNL_CD_SK"
            - name: filter_DiagCd
              type: smallint
              width: 2
              nullable: N
              map: "if (IsNull(CdMppng.CD_MPPNG_SK) = @FALSE) then 1 else 0"
        - name: Trans3.filter
          type: filter
          source: Trans3.lookup
          filter: "filter_DiagCd = 1"
          schema: 
            - name: DiagCd_SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
            - name: DiagCd_CLM_ID
              type: varchar
              width: 20
              nullable: N
            - name: DiagCd_CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
            - name: DiagCd_DIAG_CD
              type: varchar
              width: 20
              nullable: N
            - name: DiagCd_DIAG_CD_DESC
              type: varchar
              width: 255
              nullable: N
            - name: DiagCd_CLM_LN_DIAG_ORDNL_CD_SK
              type: int
              width: 4
              nullable: N
        - name: Trans3
          type: map
          source: Trans3.filter
          alias: DiagCd
          schema: 
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DiagCd.DiagCd_SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "DiagCd.DiagCd_CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: "DiagCd.DiagCd_CLM_LN_SEQ_NO"
            - name: DIAG_CD
              type: varchar
              width: 20
              nullable: N
              map: "DiagCd.DiagCd_DIAG_CD"
            - name: DIAG_CD_DESC
              type: varchar
              width: 255
              nullable: N
              map: "DiagCd.DiagCd_DIAG_CD_DESC"
            - name: CLM_LN_DIAG_ORDNL_CD_SK
              type: int
              width: 4
              nullable: N
              map: "DiagCd.DiagCd_CLM_LN_DIAG_ORDNL_CD_SK"
        - name: Trans2
          type: lookup
          option:
            joinType: left
          sources: 
            - name: IDS_ClmLn
              alias: ClmLn
              joinFilter: ""
              joinExpression: []
            - name: cd_mppng
              alias: SrcSysCd
              joinFilter: ""
              joinExpression:
              - fromExpr: "SrcSysCd.CD_MPPNG_SK"
                toExpr: "ClmLn.SRC_SYS_CD_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "SrcSysCd.CD_MPPNG_SK = ClmLn.SRC_SYS_CD_SK"
              caseInSensitive: true
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(SrcSysCd.TRGT_CD) = @TRUE then ' ' else SrcSysCd.TRGT_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "ClmLn.CLM_ID"
            - name: CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: "ClmLn.CLM_LN_SEQ_NO"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: RunCycle
        - name: Trans4
          type: lookup
          option:
            joinType: left
          sources: 
            - name: 
              alias: ClmLnPcaExtract
              joinFilter: ""
              joinExpression: []
            - name: hf_etrnl_cd_mppng.refClmLnLob
              alias: refClmLnLob
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnLob.CD_MPPNG_SK"
                toExpr: "ClmLnPcaExtract.CLM_LN_PCA_LOB_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.refClmLnPrcs
              alias: refClmLnPrcs
              joinFilter: ""
              joinExpression:
              - fromExpr: "refClmLnPrcs.CD_MPPNG_SK"
                toExpr: "ClmLnPcaExtract.CLM_LN_PCA_PRCS_CD_SK"
                caseInSensitive: true
          joinCondition: 
            - expression: "refClmLnLob.CD_MPPNG_SK = ClmLnPcaExtract.CLM_LN_PCA_LOB_CD_SK"
              caseInSensitive: true
            - expression: "refClmLnPrcs.CD_MPPNG_SK = ClmLnPcaExtract.CLM_LN_PCA_PRCS_CD_SK"
              caseInSensitive: true
          schema: 
            - name: CLM_LN_SK
              type: int
              width: 4
              nullable: N
              map: " If Trim(ClmLnPcaExtract.CLM_LN_SK) = 'NA' Or Trim(ClmLnPcaExtract.CLM_LN_SK) = 'UNK' then @NULL else ClmLnPcaExtract.CLM_LN_SK"
            - name: CLM_LN_PCA_LOB_CD
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(refClmLnLob.TRGT_CD) = @TRUE then ' ' else refClmLnLob.TRGT_CD"
            - name: CLM_LN_PCA_LOB_NM
              type: varchar
              width: 100
              map: " If ISNULL(refClmLnLob.TRGT_CD_NM) = @TRUE Or Len(Trim(refClmLnLob.TRGT_CD_NM)) = 0 then ' ' else refClmLnLob.TRGT_CD_NM"
            - name: CLM_LN_PCA_PRCS_CD
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(refClmLnPrcs.TRGT_CD) = @TRUE then ' ' else refClmLnPrcs.TRGT_CD"
            - name: CLM_LN_PCA_PRCS_NM
              type: varchar
              width: 100
              nullable: N
              map: " If ISNULL(refClmLnPrcs.TRGT_CD_NM) = @TRUE Or Len(Trim(refClmLnPrcs.TRGT_CD_NM)) = 0 then ' ' else refClmLnPrcs.TRGT_CD_NM"
            - name: CNSD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPcaExtract.CNSD_AMT"
            - name: DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPcaExtract.DSALW_AMT"
            - name: NONCNSD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPcaExtract.NONCNSD_AMT"
            - name: PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPcaExtract.PD_AMT"
            - name: PROV_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPcaExtract.PROV_PD_AMT"
            - name: SUB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "ClmLnPcaExtract.SUB_PD_AMT"
            - name: EXCD_ID
              type: char
              width: 4
              nullable: N
              map: "ClmLnPcaExtract.EXCD_ID"
            - name: EXCD_SH_TX
              type: varchar
              width: 70
              map: "ClmLnPcaExtract.EXCD_SH_TX"
        - name: CLM_DM_CLM_LN
          type: unknown
          source: Trans1Trans3.lookupTrans2Trans4CLM_DM_CLM_LNW_BAL_DM_CLM_LN
          origType: CODBCStage
        - name: W_BAL_DM_CLM_LN
          type: unknown
          source: Trans1Trans3.lookupTrans2Trans4CLM_DM_CLM_LNW_BAL_DM_CLM_LN
          origType: CODBCStage


