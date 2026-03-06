model:
  config:
    origin: datastage
  jobs:
    - name: IdsClmMartClmInitUpd
      origin: datastage
      type: dataflow
      folder: "WebDataMart/Claim/ClaimInit"
      description: "Claim Datamart CLM_DM_INIT_CLM update"
      fullDescription: "* VC LOGS *\n^1_1 04/03/08 10:27:42 Batch  14704_37683 PROMOTE bckcetl ids20 dsadm rc for brent \n^1_1 04/03/08 10:21:04 Batch  14704_37276 INIT bckcett testIDS dsadm rc for brent \n^1_2 04/01/08 14:08:56 Batch  14702_50941 PROMOTE bckcett testIDS u08717 Brent\n^1_2 04/01/08 14:04:06 Batch  14702_50657 INIT bckcett devlIDS u08717 Brent\n^1_1 01/29/08 14:29:13 Batch  14639_52158 INIT bckcetl ids20 dsadm dsadm\n^1_1 01/09/08 13:32:13 Batch  14619_48738 INIT bckcetl ids20 dsadm dsadm\n^1_1 11/24/07 09:35:50 Batch  14573_34558 INIT bckcetl ids20 dsadm dsadm\n^1_2 04/18/07 14:50:09 Batch  14353_53412 INIT bckcetl ids20 dcg01 sa\n^1_3 03/28/07 06:21:15 Batch  14332_22879 INIT bckcetl ids20 dsadm dsadm\n^1_1 03/07/07 16:30:43 Batch  14311_59451 INIT bckcetl ids20 dsadm dsadm\n^1_2 12/18/06 15:24:00 Batch  14232_55459 PROMOTE bckcetl ids20 dsadm Keith for Ralph install 12/18/2006\n^1_2 12/18/06 14:39:27 Batch  14232_52797 INIT bckcett testIDS30 dsadm Keith for Ralph CDHP Install 12182006\n^1_1 11/17/06 16:01:03 Batch  14201_57667 PROMOTE bckcett testIDS30 u11141 CDHP system testing - Hugh Sisson\n^1_1 11/17/06 15:57:58 Batch  14201_57500 INIT bckcett devlIDS30 u11141 CDHP system testing - Hugh Sisson\n^1_1 08/10/06 07:54:51 Batch  14102_28496 INIT bckcett devlIDS30 u08717 Brent\n^1_4 05/19/06 09:43:19 Batch  14019_35008 INIT bckcett devlIDS30 u10913 Ollie move KCREE from devl to test\n^1_3 05/05/06 12:52:22 Batch  14005_46350 INIT bckcett devlIDS30 u10913 Ollie move from devl to test\n^1_2 04/07/06 12:18:30 Batch  13977_44314 INIT bckcett devlIDS30 u08717 Brent\n^1_1 03/29/06 06:58:50 Batch  13968_25134 INIT bckcett devlIDS30 u08717 Brent\n\n\n\nCOPYRIGHT 2006, 2007, 2008 BLUE CROSS/BLUE SHIELD OF KANSAS CITY \n                           \n\nPROCESSING:  This job is called multiple times with a different CommitCnt value.  The CommitCnt value deteremines which batch of records to extract from the W_CLM_INIT table.  Those records are written to the CLM_DM_INIT_CLM table in the Claim Datamart.  The CLM_DM_INIT_CLM table has so many indexes that row updates occur at 5 rows a second.  By looping through the data, restarts after an abort don't have to process records already updated.  DBA suggested multiple updates can occur similtainioiusly so 3 more updates were added.  If no rows exist for CommitCnt SQL does not complain.\n\n\nMODIFICATIONS:\nDeveloper                Date                 \x09Project/Altiris #\x09Change Description\x09\x09\x09\x09Development Project\x09Code Reviewer\x09Date Reviewed       \n------------------              --------------------     \x09------------------------\x09-----------------------------------------------------------------------\x09--------------------------------\x09-------------------------------\x09----------------------------       \nBrent Leland            03/28/2006                                             Originally Programmed\nBrent Leland            04/06/2006                                             Added run cycle parameter for new table field \n                                                                                                LAST_UPDT_RUN_CYC_NO\nRalph Tucker          11/02/2006                                             Added PCA_TYP_CD; Direct Map\nBrent Leland            04/01/2008                                             Added three additional parralle update SQL.           devlIDS                                   Steph Goddard            04/01/2008"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: CommitCnt
          type: integer
          description: "Record batch Commt Count"
          defaultType: expr
          defaultValue: "1"
        - name: CommitCnt2
          type: integer
          description: "Second commit count"
        - name: CommitCnt3
          type: integer
          description: "Third commit count"
        - name: CommitCnt4
          type: integer
          description: "Forth commit count"
        - name: ClmMartServer
          type: string
          description: "Claim Mart Server"
          defaultType: project
        - name: RunCycle
          type: integer
          description: "Run Cycle"
          defaultType: expr
          defaultValue: "666"
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
          defaultValue: s06640
        - name: ClmMartPW
          type: encrypted
          description: "Claim Mart Password"
          defaultType: expr
          defaultValue: "LL:@99V@<9;M0F7I4IJ5@"
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
          defaultValue: u06640
        - name: IDSPW
          type: encrypted
          description: "IDS Password"
          defaultType: expr
          defaultValue: "LDH@1KVHN92<0G7I<HJ4CKI7F<?"
      steps:
        - name: Trans1
          type: map
          source: W_CLM_INIT
          alias: Extract
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_ID"
            - name: CLM_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_FINL_DISP_CD"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_STTUS_CD"
            - name: CLM_TRNSMSN_STTUS_CD
              type: varchar
              width: 20
              map: "Extract.CLM_TRNSMSN_STTUS_CD"
            - name: CLM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_TYP_CD"
            - name: CLM_REMIT_HIST_SUPRES_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "Extract.CLM_REMIT_HIST_SUPRES_EOB_IN"
            - name: CLM_REMIT_HIST_SUPRES_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "Extract.CLM_REMIT_HIST_SUPRES_REMIT_IN"
            - name: CMPL_MBRSH_IN
              type: char
              width: 1
              nullable: N
              map: "Extract.CMPL_MBRSH_IN"
            - name: CLM_RCVD_DT
              type: timestamp
              map: "Extract.CLM_RCVD_DT"
            - name: CLM_SRCH_DT
              type: timestamp
              map: "Extract.CLM_SRCH_DT"
            - name: CLM_SVC_STRT_DT
              type: timestamp
              map: "Extract.CLM_SVC_STRT_DT"
            - name: CLM_SVC_END_DT
              type: timestamp
              map: "Extract.CLM_SVC_END_DT"
            - name: MBR_BRTH_DT
              type: timestamp
              map: "Extract.MBR_BRTH_DT"
            - name: CLM_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CLM_CHRG_AMT"
            - name: CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CLM_PAYBL_AMT"
            - name: CLM_REMIT_HIST_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CLM_REMIT_HIST_CNSD_CHRG_AMT"
            - name: CLM_REMIT_HIST_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CLM_REMIT_HIST_NO_RESP_AMT"
            - name: CLM_REMIT_HIST_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract.CLM_REMIT_HIST_PATN_RESP_AMT"
            - name: ALPHA_PFX_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.ALPHA_PFX_SUB_ID"
            - name: CLM_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CLM_SUB_ID"
            - name: CMN_PRCT_PRSCRB_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.CMN_PRCT_PRSCRB_CMN_PRCT_ID"
            - name: GRP_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract.GRP_UNIQ_KEY"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.GRP_ID"
            - name: GRP_NM
              type: varchar
              width: 75
              map: "Extract.GRP_NM"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract.MBR_UNIQ_KEY"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              map: "Extract.MBR_FIRST_NM"
            - name: MBR_MIDINIT
              type: char
              width: 1
              map: "Extract.MBR_MIDINIT"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              map: "Extract.MBR_LAST_NM"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.PROV_BILL_SVC_ID"
            - name: PROV_PD_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.PROV_PD_PROV_ID"
            - name: PROV_PCP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.PROV_PCP_PROV_ID"
            - name: PROV_REL_GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.PROV_REL_GRP_PROV_ID"
            - name: PROV_REL_IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.PROV_REL_IPA_PROV_ID"
            - name: PROV_SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract.PROV_SVC_PROV_ID"
            - name: SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract.SUB_UNIQ_KEY"
            - name: PCA_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(Extract.PCA_TYP_CD) = @TRUE then @NULL else  if Extract.PCA_TYP_CD = 'NA' then @NULL else Extract.PCA_TYP_CD"
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Trans2
          type: map
          source: W_CLM_INIT2
          alias: Extract2
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.CLM_ID"
            - name: CLM_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.CLM_FINL_DISP_CD"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.CLM_STTUS_CD"
            - name: CLM_TRNSMSN_STTUS_CD
              type: varchar
              width: 20
              map: "Extract2.CLM_TRNSMSN_STTUS_CD"
            - name: CLM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.CLM_TYP_CD"
            - name: CLM_REMIT_HIST_SUPRES_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "Extract2.CLM_REMIT_HIST_SUPRES_EOB_IN"
            - name: CLM_REMIT_HIST_SUPRES_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "Extract2.CLM_REMIT_HIST_SUPRES_REMIT_IN"
            - name: CMPL_MBRSH_IN
              type: char
              width: 1
              nullable: N
              map: "Extract2.CMPL_MBRSH_IN"
            - name: CLM_RCVD_DT
              type: timestamp
              map: "Extract2.CLM_RCVD_DT"
            - name: CLM_SRCH_DT
              type: timestamp
              map: "Extract2.CLM_SRCH_DT"
            - name: CLM_SVC_STRT_DT
              type: timestamp
              map: "Extract2.CLM_SVC_STRT_DT"
            - name: CLM_SVC_END_DT
              type: timestamp
              map: "Extract2.CLM_SVC_END_DT"
            - name: MBR_BRTH_DT
              type: timestamp
              map: "Extract2.MBR_BRTH_DT"
            - name: CLM_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract2.CLM_CHRG_AMT"
            - name: CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract2.CLM_PAYBL_AMT"
            - name: CLM_REMIT_HIST_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract2.CLM_REMIT_HIST_CNSD_CHRG_AMT"
            - name: CLM_REMIT_HIST_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract2.CLM_REMIT_HIST_NO_RESP_AMT"
            - name: CLM_REMIT_HIST_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract2.CLM_REMIT_HIST_PATN_RESP_AMT"
            - name: ALPHA_PFX_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.ALPHA_PFX_SUB_ID"
            - name: CLM_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.CLM_SUB_ID"
            - name: CMN_PRCT_PRSCRB_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.CMN_PRCT_PRSCRB_CMN_PRCT_ID"
            - name: GRP_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract2.GRP_UNIQ_KEY"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.GRP_ID"
            - name: GRP_NM
              type: varchar
              width: 75
              map: "Extract2.GRP_NM"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract2.MBR_UNIQ_KEY"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              map: "Extract2.MBR_FIRST_NM"
            - name: MBR_MIDINIT
              type: char
              width: 1
              map: "Extract2.MBR_MIDINIT"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              map: "Extract2.MBR_LAST_NM"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.PROV_BILL_SVC_ID"
            - name: PROV_PD_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.PROV_PD_PROV_ID"
            - name: PROV_PCP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.PROV_PCP_PROV_ID"
            - name: PROV_REL_GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.PROV_REL_GRP_PROV_ID"
            - name: PROV_REL_IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.PROV_REL_IPA_PROV_ID"
            - name: PROV_SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract2.PROV_SVC_PROV_ID"
            - name: SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract2.SUB_UNIQ_KEY"
            - name: PCA_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(Extract2.PCA_TYP_CD) = @TRUE then @NULL else  if Extract2.PCA_TYP_CD = 'NA' then @NULL else Extract2.PCA_TYP_CD"
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Trans3
          type: map
          source: W_CLM_INIT3
          alias: Extract3
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.CLM_ID"
            - name: CLM_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.CLM_FINL_DISP_CD"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.CLM_STTUS_CD"
            - name: CLM_TRNSMSN_STTUS_CD
              type: varchar
              width: 20
              map: "Extract3.CLM_TRNSMSN_STTUS_CD"
            - name: CLM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.CLM_TYP_CD"
            - name: CLM_REMIT_HIST_SUPRES_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "Extract3.CLM_REMIT_HIST_SUPRES_EOB_IN"
            - name: CLM_REMIT_HIST_SUPRES_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "Extract3.CLM_REMIT_HIST_SUPRES_REMIT_IN"
            - name: CMPL_MBRSH_IN
              type: char
              width: 1
              nullable: N
              map: "Extract3.CMPL_MBRSH_IN"
            - name: CLM_RCVD_DT
              type: timestamp
              map: "Extract3.CLM_RCVD_DT"
            - name: CLM_SRCH_DT
              type: timestamp
              map: "Extract3.CLM_SRCH_DT"
            - name: CLM_SVC_STRT_DT
              type: timestamp
              map: "Extract3.CLM_SVC_STRT_DT"
            - name: CLM_SVC_END_DT
              type: timestamp
              map: "Extract3.CLM_SVC_END_DT"
            - name: MBR_BRTH_DT
              type: timestamp
              map: "Extract3.MBR_BRTH_DT"
            - name: CLM_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract3.CLM_CHRG_AMT"
            - name: CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract3.CLM_PAYBL_AMT"
            - name: CLM_REMIT_HIST_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract3.CLM_REMIT_HIST_CNSD_CHRG_AMT"
            - name: CLM_REMIT_HIST_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract3.CLM_REMIT_HIST_NO_RESP_AMT"
            - name: CLM_REMIT_HIST_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract3.CLM_REMIT_HIST_PATN_RESP_AMT"
            - name: ALPHA_PFX_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.ALPHA_PFX_SUB_ID"
            - name: CLM_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.CLM_SUB_ID"
            - name: CMN_PRCT_PRSCRB_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.CMN_PRCT_PRSCRB_CMN_PRCT_ID"
            - name: GRP_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract3.GRP_UNIQ_KEY"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.GRP_ID"
            - name: GRP_NM
              type: varchar
              width: 75
              map: "Extract3.GRP_NM"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract3.MBR_UNIQ_KEY"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              map: "Extract3.MBR_FIRST_NM"
            - name: MBR_MIDINIT
              type: char
              width: 1
              map: "Extract3.MBR_MIDINIT"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              map: "Extract3.MBR_LAST_NM"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.PROV_BILL_SVC_ID"
            - name: PROV_PD_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.PROV_PD_PROV_ID"
            - name: PROV_PCP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.PROV_PCP_PROV_ID"
            - name: PROV_REL_GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.PROV_REL_GRP_PROV_ID"
            - name: PROV_REL_IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.PROV_REL_IPA_PROV_ID"
            - name: PROV_SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract3.PROV_SVC_PROV_ID"
            - name: SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract3.SUB_UNIQ_KEY"
            - name: PCA_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(Extract3.PCA_TYP_CD) = @TRUE then @NULL else  if Extract3.PCA_TYP_CD = 'NA' then @NULL else Extract3.PCA_TYP_CD"
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Trans4
          type: map
          source: W_CLM_INIT4
          alias: Extract4
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.CLM_ID"
            - name: CLM_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.CLM_FINL_DISP_CD"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.CLM_STTUS_CD"
            - name: CLM_TRNSMSN_STTUS_CD
              type: varchar
              width: 20
              map: "Extract4.CLM_TRNSMSN_STTUS_CD"
            - name: CLM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.CLM_TYP_CD"
            - name: CLM_REMIT_HIST_SUPRES_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "Extract4.CLM_REMIT_HIST_SUPRES_EOB_IN"
            - name: CLM_REMIT_HIST_SUPRES_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "Extract4.CLM_REMIT_HIST_SUPRES_REMIT_IN"
            - name: CMPL_MBRSH_IN
              type: char
              width: 1
              nullable: N
              map: "Extract4.CMPL_MBRSH_IN"
            - name: CLM_RCVD_DT
              type: timestamp
              map: "Extract4.CLM_RCVD_DT"
            - name: CLM_SRCH_DT
              type: timestamp
              map: "Extract4.CLM_SRCH_DT"
            - name: CLM_SVC_STRT_DT
              type: timestamp
              map: "Extract4.CLM_SVC_STRT_DT"
            - name: CLM_SVC_END_DT
              type: timestamp
              map: "Extract4.CLM_SVC_END_DT"
            - name: MBR_BRTH_DT
              type: timestamp
              map: "Extract4.MBR_BRTH_DT"
            - name: CLM_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract4.CLM_CHRG_AMT"
            - name: CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract4.CLM_PAYBL_AMT"
            - name: CLM_REMIT_HIST_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract4.CLM_REMIT_HIST_CNSD_CHRG_AMT"
            - name: CLM_REMIT_HIST_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract4.CLM_REMIT_HIST_NO_RESP_AMT"
            - name: CLM_REMIT_HIST_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Extract4.CLM_REMIT_HIST_PATN_RESP_AMT"
            - name: ALPHA_PFX_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.ALPHA_PFX_SUB_ID"
            - name: CLM_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.CLM_SUB_ID"
            - name: CMN_PRCT_PRSCRB_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.CMN_PRCT_PRSCRB_CMN_PRCT_ID"
            - name: GRP_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract4.GRP_UNIQ_KEY"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.GRP_ID"
            - name: GRP_NM
              type: varchar
              width: 75
              map: "Extract4.GRP_NM"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract4.MBR_UNIQ_KEY"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              map: "Extract4.MBR_FIRST_NM"
            - name: MBR_MIDINIT
              type: char
              width: 1
              map: "Extract4.MBR_MIDINIT"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              map: "Extract4.MBR_LAST_NM"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.PROV_BILL_SVC_ID"
            - name: PROV_PD_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.PROV_PD_PROV_ID"
            - name: PROV_PCP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.PROV_PCP_PROV_ID"
            - name: PROV_REL_GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.PROV_REL_GRP_PROV_ID"
            - name: PROV_REL_IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.PROV_REL_IPA_PROV_ID"
            - name: PROV_SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "Extract4.PROV_SVC_PROV_ID"
            - name: SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Extract4.SUB_UNIQ_KEY"
            - name: PCA_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: " if IsNull(Extract4.PCA_TYP_CD) = @TRUE then @NULL else  if Extract4.PCA_TYP_CD = 'NA' then @NULL else Extract4.PCA_TYP_CD"
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: W_CLM_INIT
          type: unknown
          origType: CODBCStage
        - name: W_CLM_INIT2
          type: unknown
          origType: CODBCStage
        - name: W_CLM_INIT3
          type: unknown
          origType: CODBCStage
        - name: W_CLM_INIT4
          type: unknown
          origType: CODBCStage
        - name: CLM_DM_INIT_CLM
          type: unknown
          source: CLM_DM_INIT_CLMCLM_DM_INIT_CLM2CLM_DM_INIT_CLM3CLM_DM_INIT_CLM4
          origType: CODBCStage
        - name: CLM_DM_INIT_CLM2
          type: unknown
          source: CLM_DM_INIT_CLMCLM_DM_INIT_CLM2CLM_DM_INIT_CLM3CLM_DM_INIT_CLM4
          origType: CODBCStage
        - name: CLM_DM_INIT_CLM3
          type: unknown
          source: CLM_DM_INIT_CLMCLM_DM_INIT_CLM2CLM_DM_INIT_CLM3CLM_DM_INIT_CLM4
          origType: CODBCStage
        - name: CLM_DM_INIT_CLM4
          type: unknown
          source: CLM_DM_INIT_CLMCLM_DM_INIT_CLM2CLM_DM_INIT_CLM3CLM_DM_INIT_CLM4
          origType: CODBCStage


