model:
  config:
    origin: datastage
  jobs:
    - name: IdsClmMartClmExtr
      origin: datastage
      type: dataflow
      folder: "WebDataMart/Claim/Claim"
      runAfterSubrOnJobSuccess: enabled
      description: "Claim Mart Claim and Initial Claim Extract \n- Called by IdsClmMartExtrLoadSeq\n"
      fullDescription: "© Copyright 2007 Blue Cross and Blue Shield of Kansas City\n\nCalled by:\n                    IdsClmMartExtrLoadSeq\n\nProcessing:\n                    hf_etrnl_cd_mppng  created in the daily code mapping process - DO NOT CLEAR THIS HASH FILE\n\nControl Job Rerun Information: \n             Previous Run Successful:    Rerun\n             Previous Run Aborted:         Restart\n\nModifications:                        \n                                                            Project/                                                                                                                                                                                           Code                     Date\nDeveloper            Date                        Altiris #                      Change Description                                                                                                                                       Reviewer               Reviewed\n-------------------------   --------------------            -------------------              -------------------------------------------------------------------------------------------------------------------------                                            ---------------------------   -------------------   \nBJ Luce               03/22/2004                                             Originally Programmed\n                            04/05/2005                                             use host_in = 'Y' to identify external membership. pull member and sub data from \n                                                                                                external membership when host_in = Y in clm\n                            04/07/2005                                             change check to 'ACPTD' instead of PAID for search date\n                            04/27/2005                                             add 'UNK' and 'NA' to stage variable logic for AlphaPfxSSN\n                            05/04/2005                                             change logic to populate actl_pd_amt from clm_remit_hist instead of clm\n                                                                                            populate search date with status date\n                            06/02/2005                                             add new fields for rx deductibles\n                                                                                            use host_in = 'Y' to identify external membership. pull member and sub data from\n                                                                                                external membership when host_in = Y in init clm\n                            08/01/2005                                             use PROV_DEA name for prescribing name if common pract unknown\n                            08/24/2005                                             add CLM_TRSNMSN_STTUS_CD\n                            08/29/2005                                             change SQL parameters to ClmMart for sequencer\nBJ Luce               09/29/2005                                             Use ALPHA_PFX table to get ALPHA_PFX_CD instead of CD_MPPNG\nBJ Luce               10/11/2005                                             Remove clear of hash file hf_clmmart_clndr\nBJ Luce               11/04/2005                                             create hash file for ALPHA_PFX lookup instead of joining with CLM\nBrent Leland        03/29/2006                                             Changed output of CLM_INIT data from ODBC update to sequential file.\n                                                                                            Changed input parameters to environment parameters\n                                                                                            Changed name of driver table.\n                                                                                            Added run cycle parameter for new table field LAST_UPDT_RUN_CYC_NO\nOliver Nielsen       04/17/2006                                            Added 41 new columns for KCREE and Care Advance projects, and \n                                                                                               added 7 new hashed files\nOliver Nielsen       06/12/2006                                            Change W_CLM_INT to REPLACE when building.\nBrent Leland         07/19/2006                                            Added +0 option to DB joins with CD_MPPNG table.\nBrent Leland         07/24/2006                                            Added check for UNK on FCLTY_CLM_ADMS_DT column.\nParikshith Chada  10/13/2006                                            Modified six existing fields and Added 22 new fields to the table\nRalph Tucker       11/07/2006                                            Chaged previous logic from 10/13 and added new fields. \n                                                                                                i.e.  PCA_TYP_CD, PCA_TYP_NM\nRalph Tucker       01/30/2007                                            Changed TOT_PD_AMT and TOT_CNSD_AMT to look for values not = to 0 on\n                                                                                               CLM_PCA_TOT_PD_AMT and  CLM_PCA_TOT_CNSD_AMT.\nSteph Goddard    03/28/2007                                            changed CLM_PCA_TOT_PD_AMT to look for pca_typ_cd of RUNOUT, PCA, \n                                                                                              or EMPWBNF before moving PAYBL_AMT\nHugh Sisson        05/22/2007            CDHP Drug             Removed reference to PCA in CLM_PCA_TOT_CNSD_AMT and CLM_PCA_TOT_PD_AMT\nHugh Sisson        07/16/2007            CDHP Drug             Changed claim_pull SQL to get SRC_SYS_CD_SK for the related base claim\nSteph Goddard    08/28/07                ProdSupt                 Added code to check for servicing provider if claim status is not A02, A08, or A09                                                  Brent Leland              08-29-2007\n                                                                                           Also changed direct lookups to hash file lookups\nBhoomi D             09/15/2007                                           Added balancing snapshot for claim                                                                                                                         Steph Goddard          09-28-2007\nSAndrew             10/25/2007            DRG project               Changed ODBC IDS FCLTY_CLM, the source of the DataMart Claim table's SUBMT_DRG_TX to a direct map \nBrent Leland        04/08/2008             IAD Prod. Sup.            Changed Mbr, SubAddr and SubName SQL to use driver table.  \n                                                                                                Subscriber SQL performace required filtering on the target                                                                                  Steph Goddard          04-04-2008\n                                                                                                code in the transform.\nRalph Tucker       09/30/2008                                               Added two new fields (BCBS_PLN_CD, SCCF_NO)                                                                devlIDScurr          Steph Goddard          12/04/2008\n\nSAndrew            2009-06-23      #3833 Remit Alt Chrg      Added two new fields to end of CLM_DM_CLM \n                                                                                          CLM_REMIT_HIST_ALT_CHRG_IN  and  ALT_CHRG_PROV_WRTOFF_AMT                         devlIDSnew        Steph Goddard          07/01/2009\n                                                                                          Added new CLM_REMIT_HIST fields as part of extract in remit_hist which is loaded to hf_clmmart_clm_remit_hist\n\nShanmugam A \x09 2017-03-02         5321                     Link pdProvAddr SQL  column will be aliased from NG3.TRGT_CD  to  PROV_ADDR_ST_CD\x09IntegrateDev2       Jag Yelavarthi          2017-03-07\n\nMadhavan B             2017-07-10    5788 BHI Updates    Updated Field PROV_ID length from Char(12) to Varchar(20) in CLM_PROV table                        IntegrateDev1"
      parameters:
        - name: FilePath
          type: string
          description: "IDS File Path"
          defaultType: project
        - name: CommitPoint
          type: integer
          description: "Clm Init Commit Point"
          longDescription: "Sets the number of claims to be updated per loop"
          defaultType: expr
          defaultValue: "20000"
        - name: RunCycle
          type: integer
          description: "Run Cycle"
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
        - name: ClmMartPW
          type: encrypted
          description: "Claim Mart Password"
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
      afterSubr:
        subrType: "DSU.HASH.CLEAR\\\\31;hf_clmmart_clm_fclty_clm;hf_clmmart_clm_svc_prov;hf_clmmart_clm_drug;hf_clmmart_clm_cob;hf_clmmart_clm_remit_hist;hf_clmmart_clm_pcp_prov;hf_clmmart_clm_pd_prov_addr;hf_clmmart_clm_pd_prov;hf_clmmart_clm_extrnl_mbrsh;hf_clmmart_clm_bill_svc_prov;hf_clmmart_clm_bill_svc_grp;hf_clmmart_clm_its_clm;hf_clmmart_clm_alpha_pfx;hf_clmmart_clm_sttus_audit;hf_clmmart_fclty_clm;hf_clmmart_mbr;hf_clmmart_cls;hf_clmmart_cls_pln;hf_clmmart_ntwk;hf_clmmart_prod;hf_clmmart_clm_ln;hf_clmmart_clm_sub_name;hf_clmmart_clm_sub_addr;hf_clmmart_clm_sub_addr;hf_clmmart_pca_clmchk;hf_clmmart_clm_pca;hf_clmmart_clm_extrnl_prov;hf_clmmart_clm_extrnl_ref;hf_clmmart_clm_clm_ck;hf_clmmart_clm_ln_paybl;hf_clmmart_bill_svc_prov_svc;hf_clmmart_bill_svc_grp_svc"
        subr: "31;hf_clmmart_clm_fclty_clm;hf_clmmart_clm_svc_prov;hf_clmmart_clm_drug;hf_clmmart_clm_cob;hf_clmmart_clm_remit_hist;hf_clmmart_clm_pcp_prov;hf_clmmart_clm_pd_prov_addr;hf_clmmart_clm_pd_prov;hf_clmmart_clm_extrnl_mbrsh;hf_clmmart_clm_bill_svc_prov;hf_clmmart_clm_bill_svc_grp;hf_clmmart_clm_its_clm;hf_clmmart_clm_alpha_pfx;hf_clmmart_clm_sttus_audit;hf_clmmart_fclty_clm;hf_clmmart_mbr;hf_clmmart_cls;hf_clmmart_cls_pln;hf_clmmart_ntwk;hf_clmmart_prod;hf_clmmart_clm_ln;hf_clmmart_clm_sub_name;hf_clmmart_clm_sub_addr;hf_clmmart_clm_sub_addr;hf_clmmart_pca_clmchk;hf_clmmart_clm_pca;hf_clmmart_clm_extrnl_prov;hf_clmmart_clm_extrnl_ref;hf_clmmart_clm_clm_ck;hf_clmmart_clm_ln_paybl;hf_clmmart_bill_svc_prov_svc;hf_clmmart_bill_svc_grp_svc"
      steps:
        - name: Null
          type: fileWriter
          urls: 
            - "/dev/null"
          source: Trans2
          option: 
            updateMode: overwrite
          format: 
            header: false
            quotes: "\""
            fieldDelimiter: ","
        - name: hf_etrnl_cd_mppng.src_sys_lookup
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
        - name: hf_etrnl_cd_mppng.clm_cap_lookup
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
        - name: hf_etrnl_cd_mppng.inter_pln_pgm_lookup
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
        - name: hf_etrnl_cd_mppng.clm_sttus_lookup
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
        - name: hf_etrnl_cd_mppng.clm_typ_lookup
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
        - name: hf_etrnl_cd_mppng.clm_paye_lookup
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
        - name: hf_etrnl_cd_mppng.clm_subtyp_lookup
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
        - name: hf_etrnl_cd_mppng.finl_disp_lookup
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
        - name: hf_etrnl_cd_mppng.ntwk_sttus_lookup
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
        - name: hf_etrnl_cd_mppng.pca_typ_cd_lookup
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
        - name: hf_etrnl_cd_mppng.rbc_src_sys_lookup
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
        - name: hf_clmmart_base_clm
          type: map
          source: ""
          alias: base_clm
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "base_clm.CLM_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "base_clm.CLM_ID"
        - name: hf_clmmart_pca_clm
          type: map
          source: ""
          alias: pca_clm
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "pca_clm.CLM_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "pca_clm.CLM_ID"
        - name: hf_clmmart_date
          type: map
          source: cur_date
          alias: date
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: VALUE
              type: int
              width: 4
              nullable: N
              map: "date.VALUE"
            - name: CURDATE
              type: char
              width: 10
              nullable: N
              map: "date.CURDATE"
        - name: sk_to_code.svc_prov_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_NM
              type: varchar
              width: 75
              map: ".PROV_NM"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROV_ID"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: ".CMN_PRCT_SK"
            - name: CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CMN_PRCT_ID"
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
              map: ".REL_GRP_PROV_SK"
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
              map: ".REL_IPA_PROV_SK"
            - name: GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: ".GRP_PROV_ID"
            - name: IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: ".IPA_PROV_ID"
        - name: sk_to_code.fclty_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: DRG_CD
              type: varchar
              width: 20
              map: ".DRG_CD"
            - name: GNRT_DRG_IN
              type: char
              width: 1
              nullable: N
              map: ".GNRT_DRG_IN"
        - name: sk_to_code.drug_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: ".NDC"
            - name: RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: ".RX_SUBMT_QTY"
            - name: RX_NO
              type: varchar
              width: 20
              map: ".RX_NO"
            - name: DRUG_LABEL_NM
              type: varchar
              width: 35
              map: ".DRUG_LABEL_NM"
            - name: CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CMN_PRCT_ID"
            - name: FIRST_NM
              type: varchar
              width: 35
              map: ".FIRST_NM"
            - name: LAST_NM
              type: varchar
              width: 35
              map: ".LAST_NM"
            - name: MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".MBR_DIFF_PD_AMT"
            - name: RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: ".RX_ALW_QTY"
            - name: PROV_NM
              type: varchar
              width: 75
              nullable: N
              map: ".PROV_NM"
        - name: sk_to_code.cob_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".PD_AMT"
        - name: sk_to_code.remit_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: SUPRESS_EOB_IN
              type: char
              width: 1
              nullable: N
              map: ".SUPRESS_EOB_IN"
            - name: SUPRESS_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: ".SUPRESS_REMIT_IN"
            - name: CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".CNSD_CHRG_AMT"
            - name: ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".ER_COPAY_AMT"
            - name: INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".INTRST_AMT"
            - name: NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".NO_RESP_AMT"
            - name: PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".PATN_RESP_AMT"
            - name: PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".PROV_WRTOFF_AMT"
            - name: ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".ACTL_PD_AMT"
            - name: ALT_CHRG_IN
              type: char
              width: 1
              nullable: N
              map: ".ALT_CHRG_IN"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".ALT_CHRG_PROV_WRTOFF_AMT"
        - name: sk_to_code.sub_name_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: ".SUB_SK"
            - name: FIRST_NM
              type: varchar
              width: 35
              map: ".FIRST_NM"
            - name: MIDINIT
              type: char
              width: 1
              map: ".MIDINIT"
            - name: LAST_NM
              type: varchar
              width: 75
              map: ".LAST_NM"
        - name: sk_to_code.sub_addr_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: ".SUB_SK"
            - name: ADDR_LN_1
              type: varchar
              width: 40
              map: ".ADDR_LN_1"
            - name: ADDR_LN_2
              type: varchar
              width: 40
              map: ".ADDR_LN_2"
            - name: CITY_NM
              type: varchar
              width: 35
              map: ".CITY_NM"
            - name: POSTAL_CD
              type: varchar
              width: 20
              map: ".POSTAL_CD"
            - name: SUB_ADDR_ST_CD
              type: varchar
              width: 20
              map: ".SUB_ADDR_ST_CD"
        - name: sk_to_code.pd_prov_addr_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROV_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: ".PROV_NM"
            - name: ADDR_LN_1
              type: varchar
              width: 40
              map: ".ADDR_LN_1"
            - name: ADDR_LN_2
              type: varchar
              width: 40
              map: ".ADDR_LN_2"
            - name: ADDR_LN_3
              type: varchar
              width: 40
              map: ".ADDR_LN_3"
            - name: CITY_NM
              type: varchar
              width: 35
              map: ".CITY_NM"
            - name: POSTAL_CD
              type: varchar
              width: 20
              map: ".POSTAL_CD"
            - name: PROV_ADDR_ST_CD
              type: varchar
              width: 20
              map: ".PROV_ADDR_ST_CD"
        - name: sk_to_code.pcp_prov_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROV_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: ".PROV_NM"
        - name: sk_to_code.pd_prov_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROV_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: ".PROV_NM"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: ".PROV_SK"
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
              map: ".REL_GRP_PROV_SK"
        - name: sk_to_code.external_mbr_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: MBR_BRTH_DT_SK
              type: char
              width: 10
              nullable: N
              map: ".MBR_BRTH_DT_SK"
            - name: GRP_NM
              type: varchar
              width: 75
              map: ".GRP_NM"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              map: ".MBR_FIRST_NM"
            - name: MBR_MIDINIT
              type: char
              width: 1
              map: ".MBR_MIDINIT"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              map: ".MBR_LAST_NM"
            - name: SUB_FIRST_NM
              type: varchar
              width: 35
              map: ".SUB_FIRST_NM"
            - name: SUB_MIDINIT
              type: char
              width: 1
              map: ".SUB_MIDINIT"
            - name: SUB_ADDR_LN_1
              type: varchar
              width: 40
              map: ".SUB_ADDR_LN_1"
            - name: SUB_ADDR_LN_2
              type: varchar
              width: 40
              map: ".SUB_ADDR_LN_2"
            - name: SUB_CITY_NM
              type: varchar
              width: 35
              map: ".SUB_CITY_NM"
            - name: CLM_EXTRNL_MBRSH_SUB_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: ".CLM_EXTRNL_MBRSH_SUB_ST_CD"
            - name: SUB_POSTAL_CD
              type: varchar
              width: 20
              map: ".SUB_POSTAL_CD"
            - name: SUB_LAST_NM
              type: varchar
              width: 35
              map: ".SUB_LAST_NM"
            - name: SUB_GRP_BASE_NO
              type: char
              width: 9
              map: ".SUB_GRP_BASE_NO"
        - name: sk_to_code.bill_svc_prov_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROV_BILL_SVC_ID"
        - name: sk_to_code.bill_svc_grp_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROV_BILL_SVC_ID"
        - name: sk_to_code.its_clm_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: TRGT_CD
              type: varchar
              width: 20
              map: ".TRGT_CD"
            - name: SCCF_NO
              type: varchar
              width: 20
              nullable: N
              map: ".SCCF_NO"
        - name: sk_to_code.alpha_pfx_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: ALPHA_PFX_SK
              type: int
              width: 4
              nullable: N
              map: ".ALPHA_PFX_SK"
            - name: ALPHA_PFX_CD
              type: varchar
              width: 20
              nullable: N
              map: ".ALPHA_PFX_CD"
        - name: sk_to_code.sttus_audit_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: TRGT_CD
              type: varchar
              width: 20
              map: ".TRGT_CD"
            - name: TRGT_CD_NM
              type: varchar
              width: 255
              map: ".TRGT_CD_NM"
        - name: sk_to_code.fclty_clm_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: FCLTY_CLM_ADMS_SRC_CD
              type: varchar
              width: 20
              nullable: N
              map: ".FCLTY_CLM_ADMS_SRC_CD"
            - name: FCLTY_CLM_ADMS_SRC_NM
              type: varchar
              width: 255
              nullable: N
              map: ".FCLTY_CLM_ADMS_SRC_NM"
            - name: FCLTY_CLM_ADMS_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: ".FCLTY_CLM_ADMS_TYP_CD"
            - name: FCLTY_CLM_ADMS_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: ".FCLTY_CLM_ADMS_TYP_NM"
            - name: FCLTY_CLM_BILL_CLS_CD
              type: varchar
              width: 20
              nullable: N
              map: ".FCLTY_CLM_BILL_CLS_CD"
            - name: FCLTY_CLM_BILL_CLS_NM
              type: varchar
              width: 255
              nullable: N
              map: ".FCLTY_CLM_BILL_CLS_NM"
            - name: FCLTY_CLM_BILL_FREQ_CD
              type: varchar
              width: 20
              nullable: N
              map: ".FCLTY_CLM_BILL_FREQ_CD"
            - name: FCLTY_CLM_BILL_FREQ_NM
              type: varchar
              width: 255
              nullable: N
              map: ".FCLTY_CLM_BILL_FREQ_NM"
            - name: FCLTY_CLM_BILL_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: ".FCLTY_CLM_BILL_TYP_CD"
            - name: FCLTY_CLM_BILL_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: ".FCLTY_CLM_BILL_TYP_NM"
            - name: FCLTY_CLM_DSCHG_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: ".FCLTY_CLM_DSCHG_STTUS_CD"
            - name: FCLTY_CLM_DSCHG_STTUS_NM
              type: varchar
              width: 255
              nullable: N
              map: ".FCLTY_CLM_DSCHG_STTUS_NM"
            - name: ADMS_DT_SK
              type: char
              width: 10
              nullable: N
              map: ".ADMS_DT_SK"
            - name: DSCHG_DT_SK
              type: char
              width: 10
              nullable: N
              map: ".DSCHG_DT_SK"
            - name: ADM_PHYS_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: ".ADM_PHYS_PROV_ID"
            - name: GNRT_DRG_CD
              type: varchar
              width: 20
              nullable: N
              map: ".GNRT_DRG_CD"
            - name: SUBMT_DRG_TX
              type: varchar
              width: 20
              nullable: N
              map: ".SUBMT_DRG_TX"
        - name: sk_to_code.mbr_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: MBR_SK
              type: int
              width: 4
              nullable: N
              map: ".MBR_SK"
            - name: BRTH_DT_SK
              type: char
              width: 10
              nullable: N
              map: ".BRTH_DT_SK"
            - name: MBR_GNDR_CD
              type: varchar
              width: 20
              map: ".MBR_GNDR_CD"
            - name: MBR_GNDR_NM
              type: varchar
              width: 255
              map: ".MBR_GNDR_NM"
            - name: MBR_RELSHP_CD
              type: varchar
              width: 20
              map: ".MBR_RELSHP_CD"
        - name: sk_to_code.cls_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLS_SK
              type: int
              width: 4
              nullable: N
              map: ".CLS_SK"
            - name: CLS_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CLS_ID"
            - name: CLS_DESC
              type: varchar
              width: 70
              map: ".CLS_DESC"
        - name: sk_to_code.cls_pln_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLS_PLN_SK
              type: int
              width: 4
              nullable: N
              map: ".CLS_PLN_SK"
            - name: CLS_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CLS_PLN_ID"
            - name: CLS_PLN_DESC
              type: varchar
              width: 70
              nullable: N
              map: ".CLS_PLN_DESC"
        - name: sk_to_code.ntwk_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: NTWK_SK
              type: int
              width: 4
              nullable: N
              map: ".NTWK_SK"
            - name: NTWK_ID
              type: varchar
              width: 20
              nullable: N
              map: ".NTWK_ID"
            - name: NTWK_NM
              type: varchar
              width: 75
              map: ".NTWK_NM"
        - name: sk_to_code.prod_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: PROD_SK
              type: int
              width: 4
              nullable: N
              map: ".PROD_SK"
            - name: PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROD_ID"
        - name: sk_to_code.clm_ln_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_LN_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: ".CLM_LN_SEQ_NO"
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PREAUTH_ID
              type: varchar
              width: 40
              nullable: N
              map: ".PREAUTH_ID"
        - name: sk_to_code.pca_clmchk_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: CHK_NET_PAYMT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".CHK_NET_PAYMT_AMT"
            - name: CHK_NO
              type: int
              width: 4
              nullable: N
              map: ".CHK_NO"
            - name: CHK_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: ".CHK_SEQ_NO"
            - name: CHK_PAYE_NM
              type: varchar
              width: 75
              map: ".CHK_PAYE_NM"
            - name: CHK_PAYMT_REF_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CHK_PAYMT_REF_ID"
            - name: PCA_CLM_CHK_PD_DT
              type: varchar
              width: 10
              map: ".PCA_CLM_CHK_PD_DT"
            - name: PCA_CLM_CHK_PAYE_TYP_CD
              type: varchar
              width: 20
              map: ".PCA_CLM_CHK_PAYE_TYP_CD"
            - name: PCA_CLM_CHK_PAYE_TYP_NM
              type: varchar
              width: 255
              map: ".PCA_CLM_CHK_PAYE_TYP_NM"
            - name: PCA_CLM_CHK_PAYMT_METH_CD
              type: varchar
              width: 20
              map: ".PCA_CLM_CHK_PAYMT_METH_CD"
            - name: PCA_CLM_CHK_PAYMT_METH_NM
              type: varchar
              width: 255
              map: ".PCA_CLM_CHK_PAYMT_METH_NM"
        - name: sk_to_code.clm_pca_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: TOT_CNSD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".TOT_CNSD_AMT"
            - name: TOT_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".TOT_PD_AMT"
            - name: SUB_TOT_PCA_AVLBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".SUB_TOT_PCA_AVLBL_AMT"
            - name: SUB_TOT_PCA_PD_TO_DT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".SUB_TOT_PCA_PD_TO_DT_AMT"
        - name: sk_to_code.clm_extrnl_prov_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_NM
              type: varchar
              width: 75
              map: ".PROV_NM"
        - name: sk_to_code.clm_extrnl_ref_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PCA_EXTRNL_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PCA_EXTRNL_ID"
        - name: sk_to_code.clm_clm_ck_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: CLM_CHK_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_CHK_SK"
            - name: SRC_SYS_CD_SK
              type: int
              width: 4
              nullable: N
              map: ".SRC_SYS_CD_SK"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CLM_ID"
            - name: CLM_CHK_PAYE_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: ".CLM_CHK_PAYE_TYP_CD"
            - name: CLM_CHK_LOB_CD_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_CHK_LOB_CD_SK"
            - name: CRT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: ".CRT_RUN_CYC_EXCTN_SK"
            - name: LAST_UPDT_RUN_CYC_EXCTN_SK
              type: int
              width: 4
              nullable: N
              map: ".LAST_UPDT_RUN_CYC_EXCTN_SK"
            - name: CLM_CHK_PAYMT_METH_CD
              type: varchar
              width: 20
              nullable: N
              map: ".CLM_CHK_PAYMT_METH_CD"
            - name: CLM_REMIT_HIST_PAYMTOVRD_CD_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_REMIT_HIST_PAYMTOVRD_CD_SK"
            - name: EXTRNL_CHK_IN
              type: char
              width: 1
              nullable: N
              map: ".EXTRNL_CHK_IN"
            - name: PCA_CHK_IN
              type: char
              width: 1
              nullable: N
              map: ".PCA_CHK_IN"
            - name: CHK_PD_DT_SK
              type: varchar
              width: 20
              nullable: N
              map: ".CHK_PD_DT_SK"
            - name: CHK_NET_PAYMT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".CHK_NET_PAYMT_AMT"
            - name: CHK_ORIG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".CHK_ORIG_AMT"
            - name: CHK_NO
              type: int
              width: 4
              nullable: N
              map: ".CHK_NO"
            - name: CHK_SEQ_NO
              type: int
              width: 4
              nullable: N
              map: ".CHK_SEQ_NO"
            - name: CHK_PAYE_NM
              type: varchar
              width: 75
              map: ".CHK_PAYE_NM"
            - name: CHK_PAYMT_REF_ID
              type: varchar
              width: 20
              nullable: N
              map: ".CHK_PAYMT_REF_ID"
        - name: sk_to_code.clm_ln_sum_paybl_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: ".PAYBL_AMT"
        - name: sk_to_code.bill_svc_prov_svc_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROV_BILL_SVC_ID"
        - name: sk_to_code.bill_svc_grp_svc_lookup
          type: map
          source: "CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm"
          alias: ""
          description: "The map is converted instance of Hashed File source"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: ".CLM_SK"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: ".PROV_BILL_SVC_ID"
        - name: ids
          type: dbReader
          originalDb: db2
        - name: SYSDUMMY1
          type: dbReader
          originalDb: db2
          query: "SELECT IBMREQD  FROM sysibm.sysdummy1;"
          tableAction: append
          schema: 
            - name: IBMREQD
              type: char
              width: 1
              nullable: N
        - name: Trans1.lookup
          type: lookup
          stageVars:
            - name: svSrcSysCd
              type: decimal
              width: 18
              precision: 4
              map: " If IsNull(src_sys_lookup.CD_MPPNG_SK) = @TRUE then Space(1) else src_sys_lookup.TRGT_CD"
              initValue: "0"
            - name: svExtrnlMbrsh
              type: decimal
              width: 18
              precision: 4
              map: " If claim_pull.HOST_IN = 'Y' then 'Y' else 'N'"
              initValue: "0"
            - name: svMbrIn
              type: decimal
              width: 18
              precision: 4
              map: " If svExtrnlMbrsh = 'Y' then 'N' else 'Y'"
              initValue: "0"
            - name: svClmCapCd
              type: decimal
              width: 0
              map: " If IsNull(clm_cap_lookup.TRGT_CD) = @TRUE then Space(1) else clm_cap_lookup.TRGT_CD"
              initValue: "0"
            - name: svClmFinlDispCd
              type: decimal
              width: 0
              map: " If IsNull(finl_disp_lookup.TRGT_CD) = @TRUE then Space(1) else finl_disp_lookup.TRGT_CD"
              initValue: "0"
            - name: svClmInterPlnPgmCd
              type: decimal
              width: 0
              map: " If IsNull(inter_pln_pgm_lookup.CD_MPPNG_SK) = @TRUE then Space(1) else inter_pln_pgm_lookup.TRGT_CD"
              initValue: "0"
            - name: svClmPayeCd
              type: decimal
              width: 0
              map: " If IsNull(clm_paye_lookup.CD_MPPNG_SK) = @TRUE then Space(1) else clm_paye_lookup.TRGT_CD"
              initValue: "0"
            - name: svClmSttusCd
              type: decimal
              width: 0
              map: " If IsNull(clm_sttus_lookup.CD_MPPNG_SK) = @TRUE then Space(1) else clm_sttus_lookup.TRGT_CD"
              initValue: "0"
            - name: svClmType
              type: decimal
              width: 0
              map: " If IsNull(clm_typ_lookup.CD_MPPNG_SK) = @TRUE then Space(1) else clm_typ_lookup.TRGT_CD"
              initValue: "0"
            - name: svDrg
              type: decimal
              width: 0
              map: " If IsNull(fclty_lookup.CLM_SK) = @TRUE then Space(1) else  if fclty_lookup.GNRT_DRG_IN = 'Y' then Space(1) else fclty_lookup.DRG_CD"
              initValue: "0"
            - name: svClmSubType
              type: decimal
              width: 0
              map: " If IsNull(clm_subtyp_lookup.CD_MPPNG_SK) = @TRUE then Space(1) else clm_subtyp_lookup.TRGT_CD"
              initValue: "0"
            - name: svPdDt
              type: decimal
              width: 0
              map: " If Trim(claim_pull.PD_DT_SK) = 'NA' Or Trim(claim_pull.PD_DT_SK) = 'UNK' then @NULL else claim_pull.PD_DT_SK"
              initValue: "0"
            - name: svRcvdDt
              type: decimal
              width: 0
              map: " If Trim(claim_pull.RCVD_DT_SK) = 'NA' Or Trim(claim_pull.RCVD_DT_SK) = 'UNK' then @NULL else claim_pull.RCVD_DT_SK"
              initValue: "0"
            - name: svChkPdDt
              type: decimal
              width: 0
              map: " If IsNull(clm_clm_ck_lookup.CHK_PD_DT_SK) = @TRUE then @NULL else clm_clm_ck_lookup.CHK_PD_DT_SK"
              initValue: "0"
            - name: svSvcStrtDt
              type: decimal
              width: 0
              map: " If Trim(claim_pull.SVC_STRT_DT_SK) = 'NA' Or Trim(claim_pull.SVC_STRT_DT_SK) = 'UNK' then @NULL else claim_pull.SVC_STRT_DT_SK"
              initValue: "0"
            - name: svSvcEndDt
              type: decimal
              width: 0
              map: " If Trim(claim_pull.SVC_END_DT_SK) = 'NA' Or Trim(claim_pull.SVC_END_DT_SK) = 'UNK' then @NULL else claim_pull.SVC_END_DT_SK"
              initValue: "0"
            - name: svSttusDt
              type: decimal
              width: 0
              map: " If Trim(claim_pull.STTUS_DT_SK) = 'NA' Or Trim(claim_pull.STTUS_DT_SK) = 'UNK' then @NULL else claim_pull.STTUS_DT_SK"
              initValue: "0"
            - name: svBrthDtSk
              type: decimal
              width: 18
              precision: 4
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.CLM_SK) = @TRUE then @NULL else  If Trim(external_mbr_lookup.MBR_BRTH_DT_SK) = 'NA' Or Trim(external_mbr_lookup.MBR_BRTH_DT_SK) = 'UNK' Then @NULL Else external_mbr_lookup.MBR_BRTH_DT_SK Else  If Trim(claim_pull.BRTH_DT_SK) = 'NA' Or Trim(claim_pull.BRTH_DT_SK) = 'UNK' Then @NULL Else claim_pull.BRTH_DT_SK"
              initValue: "0"
            - name: svAlphaPfx
              type: decimal
              width: 0
              map: " If IsNull(alpha_pfx_lookup.ALPHA_PFX_SK) = @TRUE then space(1) else trim(alpha_pfx_lookup.ALPHA_PFX_CD)"
              initValue: "0"
            - name: svAlphaPfxSSN
              type: decimal
              width: 0
              map: " If svAlphaPfx = ' ' then Space(1) else  if svAlphaPfx = 'NA' then Space(1) else  if svAlphaPfx = 'UNK' then Space(1) else svAlphaPfx : claim_pull.SUB_ID"
              initValue: "0"
            - name: svPrscrbID
              type: decimal
              width: 0
              map: " If IsNull(drug_lookup.CLM_SK) = @TRUE then Space(1) else  if drug_lookup.CMN_PRCT_ID = 'UNK' then Space(1) else drug_lookup.CMN_PRCT_ID"
              initValue: "0"
            - name: svPrscbName
              type: decimal
              width: 0
              map: " If IsNull(drug_lookup.CLM_SK) = @TRUE then Space(1) else  if drug_lookup.CMN_PRCT_ID = 'UNK' then drug_lookup.PROV_NM else  if trim(drug_lookup.FIRST_NM) : ' ' : trim(drug_lookup.LAST_NM) = @NULL then Space(1) else trim(drug_lookup.FIRST_NM) : ' ' : trim(drug_lookup.LAST_NM)"
              initValue: "0"
            - name: svCommitCnt
              type: decimal
              width: 0
              map: " If MOD(@INROWNUM, CommitPoint) = 0 Then svCommitCnt + 1 Else svCommitCnt"
              initValue: "1"
            - name: svPcaTypCd
              type: decimal
              width: 0
              map: " if IsNull(pca_typ_cd_lookup.TRGT_CD) = @TRUE then @NULL else pca_typ_cd_lookup.TRGT_CD"
              initValue: "-1"
            - name: svPcaTypNm
              type: decimal
              width: 0
              map: " if IsNull(pca_typ_cd_lookup.TRGT_CD_NM) = @TRUE then 'NA' else pca_typ_cd_lookup.TRGT_CD_NM"
              initValue: "-1"
            - name: svClmSubBcbsPlnCd
              type: decimal
              width: 0
              map: "claim_pull.CLM_SUB_BCBS_PLN_CD_SK"
              initValue: "-1"
            - name: svItsClmSccfNo
              type: decimal
              width: 0
              map: " if IsNull(its_clm_lookup.SCCF_NO) = @TRUE then @NULL else  if its_clm_lookup.SCCF_NO = 'NA' then @NULL else its_clm_lookup.SCCF_NO"
              initValue: "-1"
          option:
            joinType: left
          sources: 
            - name: 
              alias: claim_pull
              joinFilter: ""
              joinExpression: []
            - name: sk_to_code.svc_prov_lookup
              alias: svc_prov_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "svc_prov_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.fclty_lookup
              alias: fclty_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "fclty_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.src_sys_lookup
              alias: src_sys_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "src_sys_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.SRC_SYS_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.clm_cap_lookup
              alias: clm_cap_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_cap_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.CLM_CAP_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.inter_pln_pgm_lookup
              alias: inter_pln_pgm_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "inter_pln_pgm_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.CLM_INTER_PLN_PGM_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.clm_sttus_lookup
              alias: clm_sttus_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_sttus_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.CLM_STTUS_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.clm_typ_lookup
              alias: clm_typ_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_typ_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.CLM_TYP_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.clm_paye_lookup
              alias: clm_paye_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_paye_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.CLM_PAYE_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.clm_subtyp_lookup
              alias: clm_subtyp_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_subtyp_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.CLM_SUBTYP_CD_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.finl_disp_lookup
              alias: finl_disp_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "finl_disp_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.CLM_FINL_DISP_CD_SK"
                caseInSensitive: true
            - name: sk_to_code.drug_lookup
              alias: drug_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "drug_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.cob_lookup
              alias: cob_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "cob_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.remit_lookup
              alias: remit_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "remit_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.sub_name_lookup
              alias: sub_name_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "sub_name_lookup.SUB_SK"
                toExpr: "claim_pull.SUB_SK"
                caseInSensitive: true
            - name: sk_to_code.sub_addr_lookup
              alias: sub_addr_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "sub_addr_lookup.SUB_SK"
                toExpr: "claim_pull.SUB_SK"
                caseInSensitive: true
            - name: sk_to_code.pd_prov_addr_lookup
              alias: pd_prov_addr_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "pd_prov_addr_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.pcp_prov_lookup
              alias: pcp_prov_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "pcp_prov_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.pd_prov_lookup
              alias: pd_prov_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "pd_prov_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.external_mbr_lookup
              alias: external_mbr_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "external_mbr_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.bill_svc_prov_lookup
              alias: bill_svc_prov_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "bill_svc_prov_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.bill_svc_grp_lookup
              alias: bill_svc_grp_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "bill_svc_grp_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.its_clm_lookup
              alias: its_clm_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "its_clm_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.alpha_pfx_lookup
              alias: alpha_pfx_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "alpha_pfx_lookup.ALPHA_PFX_SK"
                toExpr: "claim_pull.ALPHA_PFX_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.ntwk_sttus_lookup
              alias: ntwk_sttus_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "ntwk_sttus_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.CLM_NTWK_STTUS_CD_SK"
                caseInSensitive: true
            - name: sk_to_code.sttus_audit_lookup
              alias: sttus_audit_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "sttus_audit_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.fclty_clm_lookup
              alias: fclty_clm_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "fclty_clm_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.mbr_lookup
              alias: mbr_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "mbr_lookup.MBR_SK"
                toExpr: "claim_pull.MBR_SK"
                caseInSensitive: true
            - name: sk_to_code.cls_lookup
              alias: cls_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "cls_lookup.CLS_SK"
                toExpr: "claim_pull.CLS_SK"
                caseInSensitive: true
            - name: sk_to_code.cls_pln_lookup
              alias: cls_pln_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "cls_pln_lookup.CLS_PLN_SK"
                toExpr: "claim_pull.CLS_PLN_SK"
                caseInSensitive: true
            - name: sk_to_code.ntwk_lookup
              alias: ntwk_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "ntwk_lookup.NTWK_SK"
                toExpr: "claim_pull.NTWK_SK"
                caseInSensitive: true
            - name: sk_to_code.prod_lookup
              alias: prod_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "prod_lookup.PROD_SK"
                toExpr: "claim_pull.PROD_SK"
                caseInSensitive: true
            - name: sk_to_code.clm_ln_lookup
              alias: clm_ln_lookup
              joinFilter: "clm_ln_lookup.CLM_LN_SEQ_NO = 1"
              joinExpression:
              - fromExpr: "clm_ln_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.pca_clmchk_lookup
              alias: pca_clmchk_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "pca_clmchk_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.clm_pca_lookup
              alias: clm_pca_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_pca_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.clm_extrnl_prov_lookup
              alias: clm_extrnl_prov_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_extrnl_prov_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.clm_extrnl_ref_lookup
              alias: clm_extrnl_ref_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_extrnl_ref_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.clm_clm_ck_lookup
              alias: clm_clm_ck_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_clm_ck_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.pca_typ_cd_lookup
              alias: pca_typ_cd_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "pca_typ_cd_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.PCA_TYP_CD_SK"
                caseInSensitive: true
            - name: sk_to_code.clm_ln_sum_paybl_lookup
              alias: clm_ln_sum_paybl_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "clm_ln_sum_paybl_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: hf_clmmart_base_clm
              alias: refGetClmId
              joinFilter: ""
              joinExpression:
              - fromExpr: "refGetClmId.CLM_SK"
                toExpr: "claim_pull.REL_BASE_CLM_SK"
                caseInSensitive: true
            - name: hf_clmmart_pca_clm
              alias: refGetPcaClmId
              joinFilter: ""
              joinExpression:
              - fromExpr: "refGetPcaClmId.CLM_SK"
                toExpr: "claim_pull.REL_PCA_CLM_SK"
                caseInSensitive: true
            - name: hf_etrnl_cd_mppng.rbc_src_sys_lookup
              alias: rbc_src_sys_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "rbc_src_sys_lookup.CD_MPPNG_SK"
                toExpr: "claim_pull.RBC_SRC_SYS_CD_SK"
                caseInSensitive: true
            - name: sk_to_code.bill_svc_prov_svc_lookup
              alias: bill_svc_prov_svc_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "bill_svc_prov_svc_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: sk_to_code.bill_svc_grp_svc_lookup
              alias: bill_svc_grp_svc_lookup
              joinFilter: ""
              joinExpression:
              - fromExpr: "bill_svc_grp_svc_lookup.CLM_SK"
                toExpr: "claim_pull.CLM_SK"
                caseInSensitive: true
            - name: hf_clmmart_date
              alias: date_lookup
              joinFilter: "date_lookup.VALUE = 1 AND date_lookup.CURDATE = ''"
              joinExpression:
              - fromExpr: "1"
                toExpr: "1"
                caseInSensitive: true
          joinCondition: 
            - expression: "svc_prov_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "fclty_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "src_sys_lookup.CD_MPPNG_SK = claim_pull.SRC_SYS_CD_SK"
              caseInSensitive: true
            - expression: "clm_cap_lookup.CD_MPPNG_SK = claim_pull.CLM_CAP_CD_SK"
              caseInSensitive: true
            - expression: "inter_pln_pgm_lookup.CD_MPPNG_SK = claim_pull.CLM_INTER_PLN_PGM_CD_SK"
              caseInSensitive: true
            - expression: "clm_sttus_lookup.CD_MPPNG_SK = claim_pull.CLM_STTUS_CD_SK"
              caseInSensitive: true
            - expression: "clm_typ_lookup.CD_MPPNG_SK = claim_pull.CLM_TYP_CD_SK"
              caseInSensitive: true
            - expression: "clm_paye_lookup.CD_MPPNG_SK = claim_pull.CLM_PAYE_CD_SK"
              caseInSensitive: true
            - expression: "clm_subtyp_lookup.CD_MPPNG_SK = claim_pull.CLM_SUBTYP_CD_SK"
              caseInSensitive: true
            - expression: "finl_disp_lookup.CD_MPPNG_SK = claim_pull.CLM_FINL_DISP_CD_SK"
              caseInSensitive: true
            - expression: "drug_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "cob_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "remit_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "sub_name_lookup.SUB_SK = claim_pull.SUB_SK"
              caseInSensitive: true
            - expression: "sub_addr_lookup.SUB_SK = claim_pull.SUB_SK"
              caseInSensitive: true
            - expression: "pd_prov_addr_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "pcp_prov_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "pd_prov_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "external_mbr_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "bill_svc_prov_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "bill_svc_grp_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "its_clm_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "alpha_pfx_lookup.ALPHA_PFX_SK = claim_pull.ALPHA_PFX_SK"
              caseInSensitive: true
            - expression: "ntwk_sttus_lookup.CD_MPPNG_SK = claim_pull.CLM_NTWK_STTUS_CD_SK"
              caseInSensitive: true
            - expression: "sttus_audit_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "fclty_clm_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "mbr_lookup.MBR_SK = claim_pull.MBR_SK"
              caseInSensitive: true
            - expression: "cls_lookup.CLS_SK = claim_pull.CLS_SK"
              caseInSensitive: true
            - expression: "cls_pln_lookup.CLS_PLN_SK = claim_pull.CLS_PLN_SK"
              caseInSensitive: true
            - expression: "ntwk_lookup.NTWK_SK = claim_pull.NTWK_SK"
              caseInSensitive: true
            - expression: "prod_lookup.PROD_SK = claim_pull.PROD_SK"
              caseInSensitive: true
            - expression: clm_ln_lookup.CLM_LN_SEQ_NO = 1
            - expression: "clm_ln_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "pca_clmchk_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "clm_pca_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "clm_extrnl_prov_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "clm_extrnl_ref_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "clm_clm_ck_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "pca_typ_cd_lookup.CD_MPPNG_SK = claim_pull.PCA_TYP_CD_SK"
              caseInSensitive: true
            - expression: "clm_ln_sum_paybl_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "refGetClmId.CLM_SK = claim_pull.REL_BASE_CLM_SK"
              caseInSensitive: true
            - expression: "refGetPcaClmId.CLM_SK = claim_pull.REL_PCA_CLM_SK"
              caseInSensitive: true
            - expression: "rbc_src_sys_lookup.CD_MPPNG_SK = claim_pull.RBC_SRC_SYS_CD_SK"
              caseInSensitive: true
            - expression: "bill_svc_prov_svc_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: "bill_svc_grp_svc_lookup.CLM_SK = claim_pull.CLM_SK"
              caseInSensitive: true
            - expression: date_lookup.VALUE = 1
            - expression: date_lookup.CURDATE = ''
          schema: 
            - name: OutFile3_Counter
              type: int
              width: 4
              nullable: N
              map: svCommitCnt
            - name: OutFile_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: svSrcSysCd
            - name: OutFile_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.CLM_ID"
            - name: OutFile_CLM_CAP_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmCapCd
            - name: OutFile_CLM_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmFinlDispCd
            - name: OutFile_CLM_INTER_PLN_PGM_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmInterPlnPgmCd
            - name: OutFile_CLM_PAYE_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmPayeCd
            - name: OutFile_CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmSttusCd
            - name: OutFile_CLM_SUBTYP_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmSubType
            - name: OutFile_CLM_TRNSMSN_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(its_clm_lookup.CLM_SK) = @TRUE then @NULL else its_clm_lookup.TRGT_CD"
            - name: OutFile_CLM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmType
            - name: OutFile_FCLTY_CLM_SRC_SYS_GNRT_DRG_CD
              type: varchar
              width: 20
              nullable: N
              map: svDrg
            - name: OutFile_NDC
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(drug_lookup.CLM_SK) = @TRUE then ' ' else drug_lookup.NDC"
            - name: OutFile_CMPL_MBRSH_IN
              type: char
              width: 1
              nullable: N
              map: svMbrIn
            - name: OutFile_CLM_PD_DT
              type: timestamp
              map: svPdDt
            - name: OutFile_CLM_RCVD_DT
              type: timestamp
              map: svRcvdDt
            - name: OutFile_CLM_REMIT_HIST_CHK_PD_DT
              type: timestamp
              map: " if IsNull(clm_clm_ck_lookup.CHK_PD_DT_SK) = @TRUE then @NULL else clm_clm_ck_lookup.CHK_PD_DT_SK"
            - name: OutFile_CLM_SVC_STRT_DT
              type: timestamp
              map: svSvcStrtDt
            - name: OutFile_CLM_SVC_END_DT
              type: timestamp
              map: svSvcEndDt
            - name: OutFile_CLM_STTUS_DT
              type: timestamp
              map: svSttusDt
            - name: OutFile_CLM_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If remit_lookup.ACTL_PD_AMT >= 0 then remit_lookup.ACTL_PD_AMT else 0.00"
            - name: OutFile_CLM_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.ALW_AMT"
            - name: OutFile_CLM_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.CHRG_AMT"
            - name: OutFile_CLM_COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(cob_lookup.CLM_SK) = @TRUE then 0.00 else cob_lookup.PD_AMT"
            - name: OutFile_CLM_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.COINS_AMT"
            - name: OutFile_CLM_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.CNSD_CHRG_AMT"
            - name: OutFile_CLM_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.COPAY_AMT"
            - name: OutFile_CLM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.DEDCT_AMT"
            - name: OutFile_CLM_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.DSALW_AMT"
            - name: OutFile_CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.PAYBL_AMT"
            - name: OutFile_CLM_REMIT_HIST_CHK_NET_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(clm_clm_ck_lookup.CLM_SK) = @TRUE then 0.0 else clm_clm_ck_lookup.CHK_NET_PAYMT_AMT"
            - name: OutFile_CLM_REMIT_HIST_ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(remit_lookup.CLM_SK) = @TRUE then 0.0 else remit_lookup.ER_COPAY_AMT"
            - name: OutFile_CLM_REMIT_HIST_INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(remit_lookup.INTRST_AMT) = @TRUE then 0.0 else remit_lookup.INTRST_AMT"
            - name: OutFile_CLM_REMIT_HIST_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(remit_lookup.NO_RESP_AMT) = @TRUE then 0.0 else remit_lookup.NO_RESP_AMT"
            - name: OutFile_CLM_REMIT_HIST_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(remit_lookup.PATN_RESP_AMT) = @TRUE then 0.0 else remit_lookup.PATN_RESP_AMT"
            - name: OutFile_CLM_REMIT_HIST_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If remit_lookup.PROV_WRTOFF_AMT >= 0 then remit_lookup.PROV_WRTOFF_AMT else 0.00"
            - name: OutFile_DRUG_CLM_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(drug_lookup.CLM_SK) = @TRUE then 0 else drug_lookup.MBR_DIFF_PD_AMT"
            - name: OutFile_CLM_REMIT_HIST_CHK_NO
              type: int
              width: 4
              nullable: N
              map: " If IsNull(clm_clm_ck_lookup.CLM_SK) = @TRUE then 0 else clm_clm_ck_lookup.CHK_NO"
            - name: OutFile_DRUG_CLM_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: " If IsNull(drug_lookup.CLM_SK) = @TRUE then 0 else drug_lookup.RX_ALW_QTY"
            - name: OutFile_DRUG_CLM_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: " If IsNull(drug_lookup.CLM_SK) = @TRUE then 0 else drug_lookup.RX_SUBMT_QTY"
            - name: OutFile_ALPHA_PFX_CD
              type: varchar
              width: 20
              nullable: N
              map: svAlphaPfx
            - name: OutFile_CLM_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.ADJ_FROM_CLM_ID"
            - name: OutFile_CLM_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.ADJ_TO_CLM_ID"
            - name: OutFile_CLM_PATN_ACCT_NO
              type: varchar
              width: 35
              map: " if ISNull(claim_pull.PATN_ACCT_NO) = @TRUE then @NULL else claim_pull.PATN_ACCT_NO"
            - name: OutFile_CLM_RFRNG_PROV_TX
              type: varchar
              width: 70
              map: " if IsNull(claim_pull.RFRNG_PROV_TX) = @TRUE then @NULL else claim_pull.RFRNG_PROV_TX"
            - name: OutFile_CLM_REMIT_HIST_CHK_PAYE_NM
              type: varchar
              width: 75
              map: " If IsNull(clm_clm_ck_lookup.CLM_SK) = @TRUE then @NULL else clm_clm_ck_lookup.CHK_PAYE_NM"
            - name: OutFile_CLM_REMIT_HIST_CHK_PAY_REF_ID
              type: varchar
              width: 20
              map: " If IsNull(clm_clm_ck_lookup.CLM_SK) = @TRUE then @NULL else clm_clm_ck_lookup.CHK_PAYMT_REF_ID"
            - name: OutFile_CLM_STTUS_CD_NM
              type: varchar
              width: 255
              nullable: N
              map: " If IsNull(clm_sttus_lookup.CD_MPPNG_SK) = @TRUE then ' ' else clm_sttus_lookup.TRGT_CD_NM"
            - name: OutFile_CLM_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.SUB_ID"
            - name: OutFile_CMN_PRCT_PRSCRB_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: svPrscrbID
            - name: OutFile_CMN_PRCT_PRSCRB_CMN_PRCT_NM
              type: varchar
              width: 75
              map: svPrscbName
            - name: OutFile_CMN_PRCT_SVC_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(svc_prov_lookup.CLM_SK) = @TRUE then ' ' else svc_prov_lookup.CMN_PRCT_ID"
            - name: OutFile_DRUG_CLM_RX_NO
              type: varchar
              width: 20
              map: " If IsNull(drug_lookup.CLM_SK) = @TRUE then ' ' else drug_lookup.RX_NO"
            - name: OutFile_GRP_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "claim_pull.GRP_UNIQ_KEY"
            - name: OutFile_MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "claim_pull.MBR_UNIQ_KEY"
            - name: OutFile_MBR_FIRST_NM
              type: varchar
              width: 35
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.MBR_FIRST_NM) = @TRUE then ' ' else external_mbr_lookup.MBR_FIRST_NM else  if IsNull(claim_pull.FIRST_NM) = @TRUE then ' ' else claim_pull.FIRST_NM"
            - name: OutFile_MBR_MIDINIT
              type: char
              width: 1
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.MBR_MIDINIT) = @TRUE then ' ' else external_mbr_lookup.MBR_MIDINIT else  if IsNull(claim_pull.MIDINIT) = @TRUE then ' ' else claim_pull.MIDINIT"
            - name: OutFile_MBR_LAST_NM
              type: varchar
              width: 75
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.MBR_LAST_NM) = @TRUE then ' ' else external_mbr_lookup.MBR_LAST_NM else  if IsNull(claim_pull.LAST_NM) = @TRUE then ' ' else claim_pull.LAST_NM"
            - name: OutFile_MBR_SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.SUB_FIRST_NM) = @TRUE then ' ' else external_mbr_lookup.SUB_FIRST_NM else  If IsNull(sub_name_lookup.FIRST_NM) = @TRUE then ' ' else sub_name_lookup.FIRST_NM"
            - name: OutFile_MBR_SUB_MIDINIT
              type: char
              width: 1
              nullable: N
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.SUB_MIDINIT) = @TRUE then ' ' else external_mbr_lookup.SUB_MIDINIT else  If IsNull(sub_name_lookup.MIDINIT) = @TRUE then ' ' else sub_name_lookup.MIDINIT"
            - name: OutFile_MBR_SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.SUB_LAST_NM) = @TRUE then ' ' else external_mbr_lookup.SUB_LAST_NM else  If IsNull(sub_name_lookup.LAST_NM) = @TRUE then ' ' else sub_name_lookup.LAST_NM"
            - name: OutFile_NDC_DRUG_LABEL_NM
              type: varchar
              width: 35
              nullable: N
              map: " If IsNull(drug_lookup.DRUG_LABEL_NM) = @TRUE then ' ' else drug_lookup.DRUG_LABEL_NM"
            - name: OutFile_PROV_ADDR_PD_REMIT_ADDR_LN_1
              type: varchar
              width: 40
              map: " If IsNull(pd_prov_addr_lookup.ADDR_LN_1) = @TRUE then ' ' else pd_prov_addr_lookup.ADDR_LN_1"
            - name: OutFile_PROV_ADDR_PD_REMIT_ADDR_LN_2
              type: varchar
              width: 40
              nullable: N
              map: " If IsNull(pd_prov_addr_lookup.ADDR_LN_2) = @TRUE then ' ' else pd_prov_addr_lookup.ADDR_LN_2"
            - name: OutFile_PROV_ADDR_PD_REMIT_ADDR__LN_3
              type: varchar
              width: 40
              map: " If IsNull(pd_prov_addr_lookup.ADDR_LN_3) = @TRUE then ' ' else pd_prov_addr_lookup.ADDR_LN_3"
            - name: OutFile_PROV_ADDR_PD_REMIT_CITY_NM
              type: varchar
              width: 35
              map: " If IsNull(pd_prov_addr_lookup.CITY_NM) = @TRUE then ' ' else pd_prov_addr_lookup.CITY_NM"
            - name: OutFile_PROV_ADDR_PD_REMIT_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(pd_prov_addr_lookup.PROV_ADDR_ST_CD) = @TRUE then ' ' else  if pd_prov_addr_lookup.PROV_ADDR_ST_CD = 'UNK' then ' ' else pd_prov_addr_lookup.PROV_ADDR_ST_CD"
            - name: OutFile_PROV_ADDR_PD_REMIT_POSTAL_CD
              type: char
              width: 11
              map: " If IsNull(pd_prov_addr_lookup.POSTAL_CD) = @TRUE then ' ' else pd_prov_addr_lookup.POSTAL_CD"
            - name: OutFile_PROV_PD_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(pd_prov_lookup.PROV_ID) = @TRUE then ' ' else pd_prov_lookup.PROV_ID"
            - name: OutFile_PROV_PD_PROV_NM
              type: varchar
              width: 75
              nullable: N
              map: " If IsNull(pd_prov_lookup.PROV_NM) = @TRUE then ' ' else pd_prov_lookup.PROV_NM"
            - name: OutFile_PROV_PCP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(pcp_prov_lookup.PROV_ID) = @TRUE then ' ' else pcp_prov_lookup.PROV_ID"
            - name: OutFile_PROV_PCP_PROV_NM
              type: varchar
              width: 75
              nullable: N
              map: " If IsNull(pcp_prov_lookup.PROV_NM) = @TRUE then ' ' else pcp_prov_lookup.PROV_NM"
            - name: OutFile_PROV_REL_GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(svc_prov_lookup.GRP_PROV_ID) = @TRUE then ' ' else svc_prov_lookup.GRP_PROV_ID"
            - name: OutFile_PROV_REL_IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(svc_prov_lookup.IPA_PROV_ID) = @TRUE then ' ' else svc_prov_lookup.IPA_PROV_ID"
            - name: OutFile_PROV_SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(svc_prov_lookup.PROV_ID) = @TRUE then ' ' else svc_prov_lookup.PROV_ID"
            - name: OutFile_PROV_SVC_PROV_NM
              type: varchar
              width: 75
              map: " If IsNull(svc_prov_lookup.PROV_NM) = @TRUE then ' ' else svc_prov_lookup.PROV_NM"
            - name: OutFile_SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "claim_pull.SUB_UNIQ_KEY"
            - name: OutFile_SUB_ADDR_LN_1
              type: varchar
              width: 40
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.SUB_ADDR_LN_1) = @TRUE then @NULL else external_mbr_lookup.SUB_ADDR_LN_1 else  If IsNull(sub_addr_lookup.ADDR_LN_1) = @TRUE then @NULL else sub_addr_lookup.ADDR_LN_1"
            - name: OutFile_SUB_ADDR_LN_2
              type: varchar
              width: 40
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.CLM_SK) = @TRUE then @NULL else external_mbr_lookup.SUB_ADDR_LN_2 else  If IsNull(sub_addr_lookup.ADDR_LN_2) = @TRUE then @NULL else sub_addr_lookup.ADDR_LN_2"
            - name: OutFile_SUB_ADDR_CITY_NM
              type: varchar
              width: 35
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.CLM_SK) = @TRUE then @NULL else external_mbr_lookup.SUB_CITY_NM else  If IsNull(sub_addr_lookup.CITY_NM) = @TRUE then @NULL else sub_addr_lookup.CITY_NM"
            - name: OutFile_SUB_ADDR_ST_CD
              type: char
              width: 2
              nullable: N
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.CLM_SK) = @TRUE then ' ' else  if external_mbr_lookup.CLM_EXTRNL_MBRSH_SUB_ST_CD = 'UNK' then ' ' else external_mbr_lookup.CLM_EXTRNL_MBRSH_SUB_ST_CD[1, 2] else  If IsNull(sub_addr_lookup.SUB_ADDR_ST_CD) = @TRUE then ' ' else  if sub_addr_lookup.SUB_ADDR_ST_CD = 'UNK' then ' ' else sub_addr_lookup.SUB_ADDR_ST_CD[1, 2]"
            - name: OutFile_SUB_ADDR_POSTAL_CD
              type: char
              width: 11
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.CLM_SK) = @TRUE then @NULL else external_mbr_lookup.SUB_POSTAL_CD else  If IsNull(sub_addr_lookup.POSTAL_CD) = @TRUE then @NULL else sub_addr_lookup.POSTAL_CD"
            - name: OutFile_CLM_NTWK_STTUS_CD
              type: varchar
              width: 20
              map: " if ISNULL(ntwk_sttus_lookup.TRGT_CD) = @TRUE then ' ' else ntwk_sttus_lookup.TRGT_CD"
            - name: OutFile_CLM_NTWK_STTUS_NM
              type: varchar
              width: 255
              nullable: N
              map: " if ISNULL(ntwk_sttus_lookup.TRGT_CD_NM) = @TRUE then ' ' else ntwk_sttus_lookup.TRGT_CD_NM"
            - name: OutFile_CLM_REMIT_HIST_PAYMT_METH_CD
              type: varchar
              width: 20
              map: " if ISNULL(clm_clm_ck_lookup.CLM_CHK_PAYMT_METH_CD) = @TRUE then ' ' else  if clm_clm_ck_lookup.CLM_CHK_PAYMT_METH_CD = 'NA' then ' ' else clm_clm_ck_lookup.CLM_CHK_PAYMT_METH_CD"
            - name: OutFile_CLM_STTUS_CHG_RSN_CD
              type: varchar
              width: 20
              map: " if ISNULL(sttus_audit_lookup.TRGT_CD) = @TRUE then ' ' else sttus_audit_lookup.TRGT_CD"
            - name: OutFile_CLM_STTUS_CHG_RSN_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(sttus_audit_lookup.TRGT_CD_NM) = @TRUE then ' ' else sttus_audit_lookup.TRGT_CD_NM"
            - name: OutFile_FCLTY_CLM_ADMS_TYP_CD
              type: varchar
              width: 20
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_ADMS_TYP_CD) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_ADMS_TYP_CD"
            - name: OutFile_FCLTY_CLM_ADMS_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_ADMS_TYP_NM) = @TRUE then ' ' Else fclty_clm_lookup.FCLTY_CLM_ADMS_TYP_NM"
            - name: OutFile_FCLTY_CLM_ADMS_SRC_CD
              type: varchar
              width: 20
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_ADMS_SRC_CD) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_ADMS_SRC_CD"
            - name: OutFile_FCLTY_CLM_ADMS_SRC_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_ADMS_SRC_NM) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_ADMS_SRC_NM"
            - name: OutFile_FCLTY_CLM_BILL_CLS_CD
              type: varchar
              width: 20
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_BILL_CLS_CD) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_BILL_CLS_CD"
            - name: OutFile_FCLTY_CLM_BILL_CLS_NM
              type: varchar
              width: 255
              nullable: N
              map: " IF ISNULL(fclty_clm_lookup.FCLTY_CLM_BILL_CLS_NM) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_BILL_CLS_NM"
            - name: OutFile_FCLTY_CLM_BILL_FREQ_CD
              type: varchar
              width: 20
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_BILL_FREQ_CD) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_BILL_FREQ_CD"
            - name: OutFile_FCLTY_CLM_BILL_FREQ_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_BILL_FREQ_NM) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_BILL_FREQ_NM"
            - name: OutFile_FCLTY_CLM_BILL_TYP_CD
              type: varchar
              width: 20
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_BILL_TYP_CD) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_BILL_TYP_CD"
            - name: OutFile_FCLTY_CLM_BILL_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_BILL_TYP_NM) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_BILL_TYP_NM"
            - name: OutFile_FCLTY_CLM_DSCHG_STTUS_CD
              type: varchar
              width: 20
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_DSCHG_STTUS_CD) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_DSCHG_STTUS_CD"
            - name: OutFile_FCLTY_CLM_DSCHG_STTUS_NM
              type: varchar
              width: 255
              nullable: N
              map: " If ISNULL(fclty_clm_lookup.FCLTY_CLM_DSCHG_STTUS_NM) = @TRUE then ' ' else fclty_clm_lookup.FCLTY_CLM_DSCHG_STTUS_NM"
            - name: OutFile_MBR_GNDR_CD
              type: varchar
              width: 20
              nullable: N
              map: " if ISNULL(mbr_lookup.MBR_GNDR_CD) = @TRUE then ' ' else mbr_lookup.MBR_GNDR_CD"
            - name: OutFile_MBR_GNDR_NM
              type: varchar
              width: 20
              nullable: N
              map: " If ISNULL(mbr_lookup.MBR_GNDR_NM) = @TRUE then ' ' else mbr_lookup.MBR_GNDR_NM"
            - name: OutFile_CLM_HOST_IN
              type: char
              width: 1
              nullable: N
              map: "claim_pull.HOST_IN"
            - name: OutFile_CLM_PRCS_DT
              type: timestamp
              map: "claim_pull.PRCS_DT_SK"
            - name: OutFile_DM_LAST_UPDT_DT
              type: timestamp
              map: "date_lookup.CURDATE"
            - name: OutFile_FCLTY_CLM_ADMS_DT
              type: timestamp
              map: " If len(trim(fclty_clm_lookup.ADMS_DT_SK)) < 1 or trim(fclty_clm_lookup.ADMS_DT_SK) = 'UNK' then @NULL else fclty_clm_lookup.ADMS_DT_SK"
            - name: OutFile_FCLTY_CLM_DSCHG_DT
              type: timestamp
              map: " if len(trim(fclty_clm_lookup.DSCHG_DT_SK)) < 1 then @NULL else fclty_clm_lookup.DSCHG_DT_SK"
            - name: OutFile_MBR_BRTH_DT
              type: timestamp
              map: " If trim(mbr_lookup.BRTH_DT_SK) = 'NA' or trim(mbr_lookup.BRTH_DT_SK) = 'UNK' then @NULL else mbr_lookup.BRTH_DT_SK"
            - name: OutFile_CLM_AUTH_NO
              type: varchar
              width: 20
              nullable: N
              map: " If ISNULL(clm_ln_lookup.PREAUTH_ID) = @TRUE then ' ' else clm_ln_lookup.PREAUTH_ID"
            - name: OutFile_CLS_ID
              type: varchar
              width: 20
              nullable: N
              map: " If ISNULL(cls_lookup.CLS_ID) = @TRUE then ' ' else cls_lookup.CLS_ID"
            - name: OutFile_CLS_DESC
              type: varchar
              width: 70
              map: " IF ISNULL(cls_lookup.CLS_DESC) = @TRUE then ' ' else cls_lookup.CLS_DESC"
            - name: OutFile_CLS_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: " if ISNULL(cls_pln_lookup.CLS_PLN_ID) = @TRUE then ' ' else cls_pln_lookup.CLS_PLN_ID"
            - name: OutFile_CLS_PLN_DESC
              type: varchar
              width: 70
              map: " If ISNULL(cls_pln_lookup.CLS_PLN_DESC) = @TRUE then ' ' else cls_pln_lookup.CLS_PLN_DESC"
            - name: OutFile_FCLTY_CLM_ADM_PHYS_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " IF ISNULL(fclty_clm_lookup.ADM_PHYS_PROV_ID) = @TRUE then ' ' else fclty_clm_lookup.ADM_PHYS_PROV_ID"
            - name: OutFile_GNRT_DRG_CD
              type: varchar
              width: 20
              nullable: N
              map: " If ISNULL(fclty_clm_lookup.GNRT_DRG_CD) = @TRUE then ' ' else fclty_clm_lookup.GNRT_DRG_CD"
            - name: OutFile_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.GRP_ID"
            - name: OutFile_MBR_RELSHP_CD
              type: varchar
              width: 20
              nullable: N
              map: " if ISNULL(mbr_lookup.MBR_RELSHP_CD) = @TRUE then ' ' else mbr_lookup.MBR_RELSHP_CD"
            - name: OutFile_MBR_SFX_NO
              type: varchar
              width: 20
              map: " if ISNULL(claim_pull.MBR_SFX_NO) = @TRUE then ' ' else claim_pull.MBR_SFX_NO"
            - name: OutFile_NTWK_ID
              type: varchar
              width: 20
              nullable: N
              map: " if ISNULL(ntwk_lookup.NTWK_ID) = @TRUE then ' ' else ntwk_lookup.NTWK_ID"
            - name: OutFile_NTWK_NM
              type: varchar
              width: 75
              map: " if ISNULL(ntwk_lookup.NTWK_NM) = @TRUE then ' ' else ntwk_lookup.NTWK_NM"
            - name: OutFile_PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: " if ISNULL(prod_lookup.PROD_ID) = @TRUE then ' ' else prod_lookup.PROD_ID"
            - name: OutFile_PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "claim_pull.PROV_AGMNT_ID"
            - name: OutFile_SUBMT_DRG_CD
              type: varchar
              width: 20
              nullable: N
              map: " If len(trim(fclty_clm_lookup.SUBMT_DRG_TX)) < 1 then ' ' else fclty_clm_lookup.SUBMT_DRG_TX"
            - name: OutFile_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.SUB_ID"
            - name: OutFile_LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: OutFile_PCA_CLM_CHK_PAYE_TYP_CD
              type: varchar
              width: 20
              map: " If IsNull(pca_clmchk_lookup.PCA_CLM_CHK_PAYE_TYP_CD) = @TRUE then @NULL else pca_clmchk_lookup.PCA_CLM_CHK_PAYE_TYP_CD"
            - name: OutFile_PCA_CLM_CHK_PAYE_TYP_NM
              type: varchar
              width: 255
              map: " If IsNull(pca_clmchk_lookup.PCA_CLM_CHK_PAYE_TYP_NM) = @TRUE then @NULL else pca_clmchk_lookup.PCA_CLM_CHK_PAYE_TYP_NM"
            - name: OutFile_PCA_CLM_CHK_PAYMT_METH_CD
              type: varchar
              width: 20
              map: " If IsNull(pca_clmchk_lookup.PCA_CLM_CHK_PAYMT_METH_CD) = @TRUE then @NULL else  if pca_clmchk_lookup.PCA_CLM_CHK_PAYMT_METH_CD = 'NA' then @NULL else pca_clmchk_lookup.PCA_CLM_CHK_PAYMT_METH_CD"
            - name: OutFile_PCA_CLM_CHK_PAYMT_METH_NM
              type: varchar
              width: 255
              map: " If IsNull(pca_clmchk_lookup.PCA_CLM_CHK_PAYMT_METH_NM) = @TRUE then @NULL else  if pca_clmchk_lookup.PCA_CLM_CHK_PAYMT_METH_NM = 'NA' then @NULL else pca_clmchk_lookup.PCA_CLM_CHK_PAYMT_METH_NM"
            - name: OutFile_PCA_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: " if IsNull(pca_typ_cd_lookup.TRGT_CD_NM) = @TRUE or pca_typ_cd_lookup.TRGT_CD_NM = 'NA' then @NULL else pca_typ_cd_lookup.TRGT_CD_NM"
            - name: OutFile_CLM_HOME_IN
              type: char
              width: 1
              nullable: N
              map: " if claim_pull.CLM_ID[6, 1] = 'K' or claim_pull.CLM_ID[6, 1] = 'G' or claim_pull.CLM_ID[6, 1] = 'H' or claim_pull.CLM_ID[6, 2] = 'RH' then 'Y' else 'N'"
            - name: OutFile_PCA_CLM_CHK_PD_DT
              type: timestamp
              map: " If IsNull(pca_clmchk_lookup.PCA_CLM_CHK_PD_DT) = @TRUE then @NULL else pca_clmchk_lookup.PCA_CLM_CHK_PD_DT"
            - name: OutFile_CLM_PCA_TOT_CNSD_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(clm_pca_lookup.TOT_CNSD_AMT) = @FALSE then clm_pca_lookup.TOT_CNSD_AMT else  if pca_typ_cd_lookup.TRGT_CD = 'RUNOUT' OR pca_typ_cd_lookup.TRGT_CD = 'EMPWBNF' then claim_pull.CHRG_AMT ELSE 0.00"
            - name: OutFile_CLM_PCA_TOT_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(clm_pca_lookup.TOT_PD_AMT) = @FALSE then clm_pca_lookup.TOT_PD_AMT else  if pca_typ_cd_lookup.TRGT_CD = 'RUNOUT' or pca_typ_cd_lookup.TRGT_CD = 'EMPWBNF' then clm_ln_sum_paybl_lookup.PAYBL_AMT else 0.00"
            - name: OutFile_CLM_PCA_SUB_TOT_PCA_AVLBL_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(clm_pca_lookup.SUB_TOT_PCA_AVLBL_AMT) = @FALSE then clm_pca_lookup.SUB_TOT_PCA_AVLBL_AMT else 0.00"
            - name: OutFile_CLM_PCA_SUB_TOT_PCA_PDTODT_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(clm_pca_lookup.SUB_TOT_PCA_PD_TO_DT_AMT) = @FALSE then clm_pca_lookup.SUB_TOT_PCA_PD_TO_DT_AMT else 0.00"
            - name: OutFile_PCA_CLM_CHK_NET_PAYMT_AMT
              type: decimal
              width: 13
              precision: 2
              map: " If IsNull(pca_clmchk_lookup.CHK_NET_PAYMT_AMT) = @FALSE then pca_clmchk_lookup.CHK_NET_PAYMT_AMT else 0.00"
            - name: OutFile_PCA_CLM_CHK_NO
              type: int
              width: 4
              map: " If IsNull(pca_clmchk_lookup.CHK_NO) = @FALSE then pca_clmchk_lookup.CHK_NO else 0"
            - name: OutFile_PCA_CLM_CHK_SEQ_NO
              type: int
              width: 4
              map: " If IsNull(pca_clmchk_lookup.CHK_SEQ_NO) = @FALSE then pca_clmchk_lookup.CHK_SEQ_NO else 0"
            - name: OutFile_CLM_EXTRNL_PROV_NM
              type: varchar
              width: 75
              nullable: N
              map: " if IsNull(clm_extrnl_prov_lookup.PROV_NM) = @TRUE then @NULL else clm_extrnl_prov_lookup.PROV_NM"
            - name: OutFile_CLM_EXTRNL_REF_PCA_EXTRNL_ID
              type: varchar
              width: 20
              map: " If (clm_extrnl_ref_lookup.PCA_EXTRNL_ID = ' ' Or IsNull(clm_extrnl_ref_lookup.PCA_EXTRNL_ID) = @TRUE) then @NULL else clm_extrnl_ref_lookup.PCA_EXTRNL_ID"
            - name: OutFile_PCA_CLM_CHK_PAYE_NM
              type: varchar
              width: 75
              map: " If IsNull(pca_clmchk_lookup.CHK_PAYE_NM) = @TRUE then @NULL else pca_clmchk_lookup.CHK_PAYE_NM"
            - name: OutFile_PCA_CLM_CHK_PAYMT_REF_ID
              type: varchar
              width: 20
              map: " If IsNull(pca_clmchk_lookup.CHK_PAYMT_REF_ID) = @TRUE then @NULL else pca_clmchk_lookup.CHK_PAYMT_REF_ID"
            - name: OutFile_REL_PCA_SRC_SYS_CD
              type: varchar
              width: 20
              map: " if claim_pull.REL_PCA_CLM_SK > 1 then \"FACETS\" else Space(1)"
            - name: OutFile_REL_PCA_CLM_ID
              type: varchar
              width: 20
              map: " if claim_pull.REL_PCA_CLM_SK > 1 then  if IsNull(refGetPcaClmId.CLM_ID) = @FALSE then refGetPcaClmId.CLM_ID else @NULL else @NULL"
            - name: OutFile_REL_BASE_SRC_SYS_CD
              type: varchar
              width: 20
              map: " if claim_pull.REL_BASE_CLM_SK > 1 then  If IsNull(rbc_src_sys_lookup.CD_MPPNG_SK) = @TRUE then Space(1) else rbc_src_sys_lookup.TRGT_CD else Space(1)"
            - name: OutFile_REL_BASE_CLM_ID
              type: varchar
              width: 20
              map: " if claim_pull.REL_BASE_CLM_SK > 1 then  if IsNull(refGetClmId.CLM_ID) = @FALSE then refGetClmId.CLM_ID else @NULL else @NULL"
            - name: OutFile_PCA_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: " if svPcaTypCd = 'NA' then @NULL else svPcaTypCd"
            - name: OutFile_CLM_SUB_BCBS_PLN_CD
              type: varchar
              width: 20
              map: svClmSubBcbsPlnCd
            - name: OutFile_ITS_CLM_SCCF_NO
              type: varchar
              width: 20
              map: svItsClmSccfNo
            - name: OutFile_CLM_REMIT_HIST_ALT_CHRG_IN
              type: char
              width: 1
              map: " if IsNull(remit_lookup.ALT_CHRG_IN) = @TRUE then @NULL else remit_lookup.ALT_CHRG_IN"
            - name: OutFile_ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              map: " if IsNull(remit_lookup.ALT_CHRG_IN) = @TRUE then 0.00 else remit_lookup.ALT_CHRG_PROV_WRTOFF_AMT"
            - name: OutFile2_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: svSrcSysCd
            - name: OutFile2_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.CLM_ID"
            - name: OutFile2_CLM_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmFinlDispCd
            - name: OutFile2_CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmSttusCd
            - name: OutFile2_CLM_TRNSMSN_STTUS_CD
              type: varchar
              width: 20
              map: " If IsNull(its_clm_lookup.CLM_SK) = @TRUE then @NULL else its_clm_lookup.TRGT_CD"
            - name: OutFile2_CLM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: svClmType
            - name: OutFile2_CLM_REMIT_HIST_SUPRES_EOB_IN
              type: char
              width: 1
              nullable: N
              map: " If IsNull(remit_lookup.CLM_SK) = @TRUE then ' ' else remit_lookup.SUPRESS_EOB_IN"
            - name: OutFile2_CLM_REMIT_HIST_SUPRES_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: " If IsNull(remit_lookup.CLM_SK) = @TRUE then ' ' else remit_lookup.SUPRESS_REMIT_IN"
            - name: OutFile2_CMPL_MBRSH_IN
              type: char
              width: 1
              nullable: N
              map: svMbrIn
            - name: OutFile2_CLM_RCVD_DT
              type: timestamp
              map: svRcvdDt
            - name: OutFile2_CLM_SRCH_DT
              type: timestamp
              map: svSttusDt
            - name: OutFile2_CLM_SVC_STRT_DT
              type: timestamp
              map: svSvcStrtDt
            - name: OutFile2_CLM_SVC_END_DT
              type: timestamp
              map: svSvcEndDt
            - name: OutFile2_MBR_BRTH_DT
              type: timestamp
              map: svBrthDtSk
            - name: OutFile2_CLM_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.CHRG_AMT"
            - name: OutFile2_CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.PAYBL_AMT"
            - name: OutFile2_CLM_REMIT_HIST_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(remit_lookup.CLM_SK) = @TRUE then 0.00 else remit_lookup.CNSD_CHRG_AMT"
            - name: OutFile2_CLM_REMIT_HIST_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(remit_lookup.CLM_SK) = @TRUE then 0.00 else remit_lookup.NO_RESP_AMT"
            - name: OutFile2_CLM_REMIT_HIST_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: " If IsNull(remit_lookup.CLM_SK) = @TRUE then 0.00 else remit_lookup.PATN_RESP_AMT"
            - name: OutFile2_ALPHA_PFX_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: svAlphaPfxSSN
            - name: OutFile2_CLM_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.SUB_ID"
            - name: OutFile2_CMN_PRCT_PRSCRB_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: svPrscrbID
            - name: OutFile2_GRP_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "claim_pull.GRP_UNIQ_KEY"
            - name: OutFile2_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.SUB_GRP_BASE_NO) = @TRUE then ' ' else external_mbr_lookup.SUB_GRP_BASE_NO else claim_pull.GRP_ID"
            - name: OutFile2_GRP_NM
              type: varchar
              width: 75
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.GRP_NM) = @TRUE then @NULL else external_mbr_lookup.GRP_NM else claim_pull.GRP_NM"
            - name: OutFile2_MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "claim_pull.MBR_UNIQ_KEY"
            - name: OutFile2_MBR_FIRST_NM
              type: varchar
              width: 35
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.MBR_FIRST_NM) = @TRUE then ' ' else external_mbr_lookup.MBR_FIRST_NM else  if IsNull(claim_pull.FIRST_NM) = @TRUE then ' ' else claim_pull.FIRST_NM"
            - name: OutFile2_MBR_MIDINIT
              type: char
              width: 1
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.MBR_MIDINIT) = @TRUE then ' ' else external_mbr_lookup.MBR_MIDINIT else  if IsNull(claim_pull.MIDINIT) = @TRUE then ' ' else claim_pull.MIDINIT"
            - name: OutFile2_MBR_LAST_NM
              type: varchar
              width: 75
              map: " If svExtrnlMbrsh = 'Y' then  if IsNull(external_mbr_lookup.MBR_LAST_NM) = @TRUE then ' ' else external_mbr_lookup.MBR_LAST_NM else  if IsNull(claim_pull.LAST_NM) = @TRUE then ' ' else claim_pull.LAST_NM"
            - name: OutFile2_PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(clm_sttus_lookup.SRC_CD) = @TRUE then ' ' else  if clm_sttus_lookup.SRC_CD = 'A02' or clm_sttus_lookup.SRC_CD = 'A08' or clm_sttus_lookup.SRC_CD = 'A09' then  If IsNull(bill_svc_prov_lookup.CLM_SK) = @TRUE then  if IsNull(bill_svc_grp_lookup.CLM_SK) = @TRUE then ' ' else bill_svc_grp_lookup.PROV_BILL_SVC_ID else bill_svc_prov_lookup.PROV_BILL_SVC_ID else  if IsNull(bill_svc_prov_svc_lookup.CLM_SK) = @TRUE then  if IsNull(bill_svc_grp_svc_lookup.CLM_SK) = @TRUE then ' ' else bill_svc_grp_svc_lookup.PROV_BILL_SVC_ID else bill_svc_prov_svc_lookup.PROV_BILL_SVC_ID"
            - name: OutFile2_PROV_PD_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(pd_prov_lookup.PROV_ID) = @TRUE then ' ' else pd_prov_lookup.PROV_ID"
            - name: OutFile2_PROV_PCP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(pcp_prov_lookup.PROV_ID) = @TRUE then ' ' else pcp_prov_lookup.PROV_ID"
            - name: OutFile2_PROV_REL_GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(svc_prov_lookup.GRP_PROV_ID) = @TRUE then ' ' else svc_prov_lookup.GRP_PROV_ID"
            - name: OutFile2_PROV_REL_IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(svc_prov_lookup.IPA_PROV_ID) = @TRUE then ' ' else svc_prov_lookup.IPA_PROV_ID"
            - name: OutFile2_PROV_SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: " If IsNull(svc_prov_lookup.PROV_ID) = @TRUE then ' ' else svc_prov_lookup.PROV_ID"
            - name: OutFile2_SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "claim_pull.SUB_UNIQ_KEY"
            - name: OutFile2_CMT_CT
              type: int
              width: 4
              nullable: N
              map: svCommitCnt
            - name: OutFile2_LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
              map: RunCycle
            - name: OutFile2_PCA_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: svPcaTypCd
            - name: Snapshot_SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: svSrcSysCd
            - name: Snapshot_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.CLM_ID"
            - name: Snapshot_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.CHRG_AMT"
            - name: Snapshot_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "claim_pull.PAYBL_AMT"
            - name: Snapshot_GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "claim_pull.GRP_ID"
            - name: Snapshot_SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: svSvcStrtDt
            - name: Snapshot_MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "claim_pull.MBR_UNIQ_KEY"
        - name: Trans1.V0S20P69
          type: map
          source: Trans1.lookup
          alias: OutFile3
          schema: 
            - name: Counter
              type: int
              width: 4
              nullable: N
              map: "OutFile3.OutFile3_Counter"
        - name: Trans1.V0S20P82
          type: map
          source: Trans1.lookup
          alias: OutFile
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_ID"
            - name: CLM_CAP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_CAP_CD"
            - name: CLM_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_FINL_DISP_CD"
            - name: CLM_INTER_PLN_PGM_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_INTER_PLN_PGM_CD"
            - name: CLM_PAYE_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_PAYE_CD"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_STTUS_CD"
            - name: CLM_SUBTYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_SUBTYP_CD"
            - name: CLM_TRNSMSN_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_TRNSMSN_STTUS_CD"
            - name: CLM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_TYP_CD"
            - name: FCLTY_CLM_SRC_SYS_GNRT_DRG_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_FCLTY_CLM_SRC_SYS_GNRT_DRG_CD"
            - name: NDC
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_NDC"
            - name: CMPL_MBRSH_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.OutFile_CMPL_MBRSH_IN"
            - name: CLM_PD_DT
              type: timestamp
              map: "OutFile.OutFile_CLM_PD_DT"
            - name: CLM_RCVD_DT
              type: timestamp
              map: "OutFile.OutFile_CLM_RCVD_DT"
            - name: CLM_REMIT_HIST_CHK_PD_DT
              type: timestamp
              map: "OutFile.OutFile_CLM_REMIT_HIST_CHK_PD_DT"
            - name: CLM_SVC_STRT_DT
              type: timestamp
              map: "OutFile.OutFile_CLM_SVC_STRT_DT"
            - name: CLM_SVC_END_DT
              type: timestamp
              map: "OutFile.OutFile_CLM_SVC_END_DT"
            - name: CLM_STTUS_DT
              type: timestamp
              map: "OutFile.OutFile_CLM_STTUS_DT"
            - name: CLM_ACTL_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_ACTL_PD_AMT"
            - name: CLM_ALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_ALW_AMT"
            - name: CLM_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_CHRG_AMT"
            - name: CLM_COB_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_COB_PD_AMT"
            - name: CLM_COINS_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_COINS_AMT"
            - name: CLM_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_CNSD_CHRG_AMT"
            - name: CLM_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_COPAY_AMT"
            - name: CLM_DEDCT_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_DEDCT_AMT"
            - name: CLM_DSALW_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_DSALW_AMT"
            - name: CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_PAYBL_AMT"
            - name: CLM_REMIT_HIST_CHK_NET_PAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_REMIT_HIST_CHK_NET_PAY_AMT"
            - name: CLM_REMIT_HIST_ER_COPAY_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_REMIT_HIST_ER_COPAY_AMT"
            - name: CLM_REMIT_HIST_INTRST_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_REMIT_HIST_INTRST_AMT"
            - name: CLM_REMIT_HIST_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_REMIT_HIST_NO_RESP_AMT"
            - name: CLM_REMIT_HIST_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_REMIT_HIST_PATN_RESP_AMT"
            - name: CLM_REMIT_HIST_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_CLM_REMIT_HIST_PROV_WRTOFF_AMT"
            - name: DRUG_CLM_MBR_DIFF_PD_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile.OutFile_DRUG_CLM_MBR_DIFF_PD_AMT"
            - name: CLM_REMIT_HIST_CHK_NO
              type: int
              width: 4
              nullable: N
              map: "OutFile.OutFile_CLM_REMIT_HIST_CHK_NO"
            - name: DRUG_CLM_RX_ALW_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "OutFile.OutFile_DRUG_CLM_RX_ALW_QTY"
            - name: DRUG_CLM_RX_SUBMT_QTY
              type: decimal
              width: 13
              precision: 3
              nullable: N
              map: "OutFile.OutFile_DRUG_CLM_RX_SUBMT_QTY"
            - name: ALPHA_PFX_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_ALPHA_PFX_CD"
            - name: CLM_ADJ_FROM_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_ADJ_FROM_CLM_ID"
            - name: CLM_ADJ_TO_CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_ADJ_TO_CLM_ID"
            - name: CLM_PATN_ACCT_NO
              type: varchar
              width: 35
              map: "OutFile.OutFile_CLM_PATN_ACCT_NO"
            - name: CLM_RFRNG_PROV_TX
              type: varchar
              width: 70
              map: "OutFile.OutFile_CLM_RFRNG_PROV_TX"
            - name: CLM_REMIT_HIST_CHK_PAYE_NM
              type: varchar
              width: 75
              map: "OutFile.OutFile_CLM_REMIT_HIST_CHK_PAYE_NM"
            - name: CLM_REMIT_HIST_CHK_PAY_REF_ID
              type: varchar
              width: 20
              map: "OutFile.OutFile_CLM_REMIT_HIST_CHK_PAY_REF_ID"
            - name: CLM_STTUS_CD_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_CLM_STTUS_CD_NM"
            - name: CLM_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_SUB_ID"
            - name: CMN_PRCT_PRSCRB_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CMN_PRCT_PRSCRB_CMN_PRCT_ID"
            - name: CMN_PRCT_PRSCRB_CMN_PRCT_NM
              type: varchar
              width: 75
              map: "OutFile.OutFile_CMN_PRCT_PRSCRB_CMN_PRCT_NM"
            - name: CMN_PRCT_SVC_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CMN_PRCT_SVC_CMN_PRCT_ID"
            - name: DRUG_CLM_RX_NO
              type: varchar
              width: 20
              map: "OutFile.OutFile_DRUG_CLM_RX_NO"
            - name: GRP_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "OutFile.OutFile_GRP_UNIQ_KEY"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "OutFile.OutFile_MBR_UNIQ_KEY"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              map: "OutFile.OutFile_MBR_FIRST_NM"
            - name: MBR_MIDINIT
              type: char
              width: 1
              map: "OutFile.OutFile_MBR_MIDINIT"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              map: "OutFile.OutFile_MBR_LAST_NM"
            - name: MBR_SUB_FIRST_NM
              type: varchar
              width: 35
              nullable: N
              map: "OutFile.OutFile_MBR_SUB_FIRST_NM"
            - name: MBR_SUB_MIDINIT
              type: char
              width: 1
              nullable: N
              map: "OutFile.OutFile_MBR_SUB_MIDINIT"
            - name: MBR_SUB_LAST_NM
              type: varchar
              width: 75
              nullable: N
              map: "OutFile.OutFile_MBR_SUB_LAST_NM"
            - name: NDC_DRUG_LABEL_NM
              type: varchar
              width: 35
              nullable: N
              map: "OutFile.OutFile_NDC_DRUG_LABEL_NM"
            - name: PROV_ADDR_PD_REMIT_ADDR_LN_1
              type: varchar
              width: 40
              map: "OutFile.OutFile_PROV_ADDR_PD_REMIT_ADDR_LN_1"
            - name: PROV_ADDR_PD_REMIT_ADDR_LN_2
              type: varchar
              width: 40
              nullable: N
              map: "OutFile.OutFile_PROV_ADDR_PD_REMIT_ADDR_LN_2"
            - name: PROV_ADDR_PD_REMIT_ADDR__LN_3
              type: varchar
              width: 40
              map: "OutFile.OutFile_PROV_ADDR_PD_REMIT_ADDR__LN_3"
            - name: PROV_ADDR_PD_REMIT_CITY_NM
              type: varchar
              width: 35
              map: "OutFile.OutFile_PROV_ADDR_PD_REMIT_CITY_NM"
            - name: PROV_ADDR_PD_REMIT_ST_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_PROV_ADDR_PD_REMIT_ST_CD"
            - name: PROV_ADDR_PD_REMIT_POSTAL_CD
              type: char
              width: 11
              map: "OutFile.OutFile_PROV_ADDR_PD_REMIT_POSTAL_CD"
            - name: PROV_PD_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_PROV_PD_PROV_ID"
            - name: PROV_PD_PROV_NM
              type: varchar
              width: 75
              nullable: N
              map: "OutFile.OutFile_PROV_PD_PROV_NM"
            - name: PROV_PCP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_PROV_PCP_PROV_ID"
            - name: PROV_PCP_PROV_NM
              type: varchar
              width: 75
              nullable: N
              map: "OutFile.OutFile_PROV_PCP_PROV_NM"
            - name: PROV_REL_GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_PROV_REL_GRP_PROV_ID"
            - name: PROV_REL_IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_PROV_REL_IPA_PROV_ID"
            - name: PROV_SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_PROV_SVC_PROV_ID"
            - name: PROV_SVC_PROV_NM
              type: varchar
              width: 75
              map: "OutFile.OutFile_PROV_SVC_PROV_NM"
            - name: SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "OutFile.OutFile_SUB_UNIQ_KEY"
            - name: SUB_ADDR_LN_1
              type: varchar
              width: 40
              map: "OutFile.OutFile_SUB_ADDR_LN_1"
            - name: SUB_ADDR_LN_2
              type: varchar
              width: 40
              map: "OutFile.OutFile_SUB_ADDR_LN_2"
            - name: SUB_ADDR_CITY_NM
              type: varchar
              width: 35
              map: "OutFile.OutFile_SUB_ADDR_CITY_NM"
            - name: SUB_ADDR_ST_CD
              type: char
              width: 2
              nullable: N
              map: "OutFile.OutFile_SUB_ADDR_ST_CD"
            - name: SUB_ADDR_POSTAL_CD
              type: char
              width: 11
              map: "OutFile.OutFile_SUB_ADDR_POSTAL_CD"
            - name: CLM_NTWK_STTUS_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_CLM_NTWK_STTUS_CD"
            - name: CLM_NTWK_STTUS_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_CLM_NTWK_STTUS_NM"
            - name: CLM_REMIT_HIST_PAYMT_METH_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_CLM_REMIT_HIST_PAYMT_METH_CD"
            - name: CLM_STTUS_CHG_RSN_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_CLM_STTUS_CHG_RSN_CD"
            - name: CLM_STTUS_CHG_RSN_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_CLM_STTUS_CHG_RSN_NM"
            - name: FCLTY_CLM_ADMS_TYP_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_FCLTY_CLM_ADMS_TYP_CD"
            - name: FCLTY_CLM_ADMS_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_FCLTY_CLM_ADMS_TYP_NM"
            - name: FCLTY_CLM_ADMS_SRC_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_FCLTY_CLM_ADMS_SRC_CD"
            - name: FCLTY_CLM_ADMS_SRC_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_FCLTY_CLM_ADMS_SRC_NM"
            - name: FCLTY_CLM_BILL_CLS_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_FCLTY_CLM_BILL_CLS_CD"
            - name: FCLTY_CLM_BILL_CLS_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_FCLTY_CLM_BILL_CLS_NM"
            - name: FCLTY_CLM_BILL_FREQ_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_FCLTY_CLM_BILL_FREQ_CD"
            - name: FCLTY_CLM_BILL_FREQ_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_FCLTY_CLM_BILL_FREQ_NM"
            - name: FCLTY_CLM_BILL_TYP_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_FCLTY_CLM_BILL_TYP_CD"
            - name: FCLTY_CLM_BILL_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_FCLTY_CLM_BILL_TYP_NM"
            - name: FCLTY_CLM_DSCHG_STTUS_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_FCLTY_CLM_DSCHG_STTUS_CD"
            - name: FCLTY_CLM_DSCHG_STTUS_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_FCLTY_CLM_DSCHG_STTUS_NM"
            - name: MBR_GNDR_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_MBR_GNDR_CD"
            - name: MBR_GNDR_NM
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_MBR_GNDR_NM"
            - name: CLM_HOST_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.OutFile_CLM_HOST_IN"
            - name: CLM_PRCS_DT
              type: timestamp
              map: "OutFile.OutFile_CLM_PRCS_DT"
            - name: DM_LAST_UPDT_DT
              type: timestamp
              map: "OutFile.OutFile_DM_LAST_UPDT_DT"
            - name: FCLTY_CLM_ADMS_DT
              type: timestamp
              map: "OutFile.OutFile_FCLTY_CLM_ADMS_DT"
            - name: FCLTY_CLM_DSCHG_DT
              type: timestamp
              map: "OutFile.OutFile_FCLTY_CLM_DSCHG_DT"
            - name: MBR_BRTH_DT
              type: timestamp
              map: "OutFile.OutFile_MBR_BRTH_DT"
            - name: CLM_AUTH_NO
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLM_AUTH_NO"
            - name: CLS_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLS_ID"
            - name: CLS_DESC
              type: varchar
              width: 70
              map: "OutFile.OutFile_CLS_DESC"
            - name: CLS_PLN_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_CLS_PLN_ID"
            - name: CLS_PLN_DESC
              type: varchar
              width: 70
              map: "OutFile.OutFile_CLS_PLN_DESC"
            - name: FCLTY_CLM_ADM_PHYS_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_FCLTY_CLM_ADM_PHYS_PROV_ID"
            - name: GNRT_DRG_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_GNRT_DRG_CD"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_GRP_ID"
            - name: MBR_RELSHP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_MBR_RELSHP_CD"
            - name: MBR_SFX_NO
              type: varchar
              width: 20
              map: "OutFile.OutFile_MBR_SFX_NO"
            - name: NTWK_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_NTWK_ID"
            - name: NTWK_NM
              type: varchar
              width: 75
              map: "OutFile.OutFile_NTWK_NM"
            - name: PROD_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_PROD_ID"
            - name: PROV_AGMNT_ID
              type: char
              width: 12
              nullable: N
              map: "OutFile.OutFile_PROV_AGMNT_ID"
            - name: SUBMT_DRG_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_SUBMT_DRG_CD"
            - name: SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_SUB_ID"
            - name: LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
              map: "OutFile.OutFile_LAST_UPDT_RUN_CYC_NO"
            - name: PCA_CLM_CHK_PAYE_TYP_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_PCA_CLM_CHK_PAYE_TYP_CD"
            - name: PCA_CLM_CHK_PAYE_TYP_NM
              type: varchar
              width: 255
              map: "OutFile.OutFile_PCA_CLM_CHK_PAYE_TYP_NM"
            - name: PCA_CLM_CHK_PAYMT_METH_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_PCA_CLM_CHK_PAYMT_METH_CD"
            - name: PCA_CLM_CHK_PAYMT_METH_NM
              type: varchar
              width: 255
              map: "OutFile.OutFile_PCA_CLM_CHK_PAYMT_METH_NM"
            - name: PCA_TYP_NM
              type: varchar
              width: 255
              nullable: N
              map: "OutFile.OutFile_PCA_TYP_NM"
            - name: CLM_HOME_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile.OutFile_CLM_HOME_IN"
            - name: PCA_CLM_CHK_PD_DT
              type: timestamp
              map: "OutFile.OutFile_PCA_CLM_CHK_PD_DT"
            - name: CLM_PCA_TOT_CNSD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "OutFile.OutFile_CLM_PCA_TOT_CNSD_AMT"
            - name: CLM_PCA_TOT_PD_AMT
              type: decimal
              width: 13
              precision: 2
              map: "OutFile.OutFile_CLM_PCA_TOT_PD_AMT"
            - name: CLM_PCA_SUB_TOT_PCA_AVLBL_AMT
              type: decimal
              width: 13
              precision: 2
              map: "OutFile.OutFile_CLM_PCA_SUB_TOT_PCA_AVLBL_AMT"
            - name: CLM_PCA_SUB_TOT_PCA_PDTODT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "OutFile.OutFile_CLM_PCA_SUB_TOT_PCA_PDTODT_AMT"
            - name: PCA_CLM_CHK_NET_PAYMT_AMT
              type: decimal
              width: 13
              precision: 2
              map: "OutFile.OutFile_PCA_CLM_CHK_NET_PAYMT_AMT"
            - name: PCA_CLM_CHK_NO
              type: int
              width: 4
              map: "OutFile.OutFile_PCA_CLM_CHK_NO"
            - name: PCA_CLM_CHK_SEQ_NO
              type: int
              width: 4
              map: "OutFile.OutFile_PCA_CLM_CHK_SEQ_NO"
            - name: CLM_EXTRNL_PROV_NM
              type: varchar
              width: 75
              nullable: N
              map: "OutFile.OutFile_CLM_EXTRNL_PROV_NM"
            - name: CLM_EXTRNL_REF_PCA_EXTRNL_ID
              type: varchar
              width: 20
              map: "OutFile.OutFile_CLM_EXTRNL_REF_PCA_EXTRNL_ID"
            - name: PCA_CLM_CHK_PAYE_NM
              type: varchar
              width: 75
              map: "OutFile.OutFile_PCA_CLM_CHK_PAYE_NM"
            - name: PCA_CLM_CHK_PAYMT_REF_ID
              type: varchar
              width: 20
              map: "OutFile.OutFile_PCA_CLM_CHK_PAYMT_REF_ID"
            - name: REL_PCA_SRC_SYS_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_REL_PCA_SRC_SYS_CD"
            - name: REL_PCA_CLM_ID
              type: varchar
              width: 20
              map: "OutFile.OutFile_REL_PCA_CLM_ID"
            - name: REL_BASE_SRC_SYS_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_REL_BASE_SRC_SYS_CD"
            - name: REL_BASE_CLM_ID
              type: varchar
              width: 20
              map: "OutFile.OutFile_REL_BASE_CLM_ID"
            - name: PCA_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile.OutFile_PCA_TYP_CD"
            - name: CLM_SUB_BCBS_PLN_CD
              type: varchar
              width: 20
              map: "OutFile.OutFile_CLM_SUB_BCBS_PLN_CD"
            - name: ITS_CLM_SCCF_NO
              type: varchar
              width: 20
              map: "OutFile.OutFile_ITS_CLM_SCCF_NO"
            - name: CLM_REMIT_HIST_ALT_CHRG_IN
              type: char
              width: 1
              map: "OutFile.OutFile_CLM_REMIT_HIST_ALT_CHRG_IN"
            - name: ALT_CHRG_PROV_WRTOFF_AMT
              type: decimal
              width: 13
              precision: 2
              map: "OutFile.OutFile_ALT_CHRG_PROV_WRTOFF_AMT"
        - name: Trans1.V0S20P83
          type: map
          source: Trans1.lookup
          alias: OutFile2
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_CLM_ID"
            - name: CLM_FINL_DISP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_CLM_FINL_DISP_CD"
            - name: CLM_STTUS_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_CLM_STTUS_CD"
            - name: CLM_TRNSMSN_STTUS_CD
              type: varchar
              width: 20
              map: "OutFile2.OutFile2_CLM_TRNSMSN_STTUS_CD"
            - name: CLM_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_CLM_TYP_CD"
            - name: CLM_REMIT_HIST_SUPRES_EOB_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile2.OutFile2_CLM_REMIT_HIST_SUPRES_EOB_IN"
            - name: CLM_REMIT_HIST_SUPRES_REMIT_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile2.OutFile2_CLM_REMIT_HIST_SUPRES_REMIT_IN"
            - name: CMPL_MBRSH_IN
              type: char
              width: 1
              nullable: N
              map: "OutFile2.OutFile2_CMPL_MBRSH_IN"
            - name: CLM_RCVD_DT
              type: timestamp
              map: "OutFile2.OutFile2_CLM_RCVD_DT"
            - name: CLM_SRCH_DT
              type: timestamp
              map: "OutFile2.OutFile2_CLM_SRCH_DT"
            - name: CLM_SVC_STRT_DT
              type: timestamp
              map: "OutFile2.OutFile2_CLM_SVC_STRT_DT"
            - name: CLM_SVC_END_DT
              type: timestamp
              map: "OutFile2.OutFile2_CLM_SVC_END_DT"
            - name: MBR_BRTH_DT
              type: timestamp
              map: "OutFile2.OutFile2_MBR_BRTH_DT"
            - name: CLM_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile2.OutFile2_CLM_CHRG_AMT"
            - name: CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile2.OutFile2_CLM_PAYBL_AMT"
            - name: CLM_REMIT_HIST_CNSD_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile2.OutFile2_CLM_REMIT_HIST_CNSD_CHRG_AMT"
            - name: CLM_REMIT_HIST_NO_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile2.OutFile2_CLM_REMIT_HIST_NO_RESP_AMT"
            - name: CLM_REMIT_HIST_PATN_RESP_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "OutFile2.OutFile2_CLM_REMIT_HIST_PATN_RESP_AMT"
            - name: ALPHA_PFX_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_ALPHA_PFX_SUB_ID"
            - name: CLM_SUB_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_CLM_SUB_ID"
            - name: CMN_PRCT_PRSCRB_CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_CMN_PRCT_PRSCRB_CMN_PRCT_ID"
            - name: GRP_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "OutFile2.OutFile2_GRP_UNIQ_KEY"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_GRP_ID"
            - name: GRP_NM
              type: varchar
              width: 75
              map: "OutFile2.OutFile2_GRP_NM"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "OutFile2.OutFile2_MBR_UNIQ_KEY"
            - name: MBR_FIRST_NM
              type: varchar
              width: 35
              map: "OutFile2.OutFile2_MBR_FIRST_NM"
            - name: MBR_MIDINIT
              type: char
              width: 1
              map: "OutFile2.OutFile2_MBR_MIDINIT"
            - name: MBR_LAST_NM
              type: varchar
              width: 75
              map: "OutFile2.OutFile2_MBR_LAST_NM"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_PROV_BILL_SVC_ID"
            - name: PROV_PD_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_PROV_PD_PROV_ID"
            - name: PROV_PCP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_PROV_PCP_PROV_ID"
            - name: PROV_REL_GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_PROV_REL_GRP_PROV_ID"
            - name: PROV_REL_IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_PROV_REL_IPA_PROV_ID"
            - name: PROV_SVC_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_PROV_SVC_PROV_ID"
            - name: SUB_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "OutFile2.OutFile2_SUB_UNIQ_KEY"
            - name: CMT_CT
              type: int
              width: 4
              nullable: N
              map: "OutFile2.OutFile2_CMT_CT"
            - name: LAST_UPDT_RUN_CYC_NO
              type: int
              width: 4
              nullable: N
              map: "OutFile2.OutFile2_LAST_UPDT_RUN_CYC_NO"
            - name: PCA_TYP_CD
              type: varchar
              width: 20
              nullable: N
              map: "OutFile2.OutFile2_PCA_TYP_CD"
        - name: Trans1.V0S20P101
          type: map
          source: Trans1.lookup
          alias: Snapshot
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.Snapshot_SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.Snapshot_CLM_ID"
            - name: CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.Snapshot_CHRG_AMT"
            - name: PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.Snapshot_PAYBL_AMT"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.Snapshot_GRP_ID"
            - name: SVC_STRT_DT_SK
              type: char
              width: 10
              nullable: N
              map: "Snapshot.Snapshot_SVC_STRT_DT_SK"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Snapshot.Snapshot_MBR_UNIQ_KEY"
        - name: Trans2.map
          type: map
          source: Trans1.V0S20P69
          alias: OutFile3
          stageVars: 
            - name: NewRow
              type: decimal
              width: 0
              map: " If OutFile3.Counter = PrevCountNum Then @FALSE Else @TRUE"
              initValue: "@TRUE"
            - name: PrevCountNum
              type: decimal
              width: 0
              map: "OutFile3.Counter"
              initValue: "-1"
          schema: 
            - name: Counter
              type: int
              width: 4
              nullable: N
              map: "OutFile3.Counter"
            - name: NewRow
              type: decimal
              width: 0
              map: NewRow
              initValue: "@TRUE"
            - name: PrevCountNum
              type: decimal
              width: 0
              map: PrevCountNum
              initValue: "-1"
        - name: Trans2
          type: filter
          source: Trans2.map
          filter: "NewRow = @TRUE"
          schema: 
            - name: Counter
              type: int
              width: 4
              nullable: N
        - name: cur_date
          type: map
          source: SYSDUMMY1
          alias: in
          stageVars: 
            - name: datelkup
              type: decimal
              width: 0
              map: "FORMAT.DATE(@DATE, 'DATE', 'CURRENT', 'SYBTIMESTAMP')[1, 10]"
              initValue: "0"
          schema: 
            - name: VALUE
              type: int
              width: 4
              nullable: N
              map: "1"
            - name: CURDATE
              type: char
              width: 10
              nullable: N
              map: datelkup
            - name: datelkup
              type: varchar
              width: 0
              map: datelkup
              dropStageVar: true
              initValue: "0"
        - name: Trans7.V0S284P1
          type: map
          source: Trans7.V0S284P1.filter
          alias: billSvcGrp
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "billSvcGrp.CLM_SK"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "billSvcGrp.PROV_BILL_SVC_ID"
        - name: Trans7.V0S284P1.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'PD'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_ID
              type: char
              width: 12
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: Trans7.V0S284P3
          type: map
          source: Trans7.V0S284P3.filter
          alias: billSvcGrp
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "billSvcGrp.CLM_SK"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "billSvcGrp.PROV_BILL_SVC_ID"
        - name: Trans7.V0S284P3.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'SVC'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_ID
              type: char
              width: 12
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: Trans8.V0S347P2
          type: map
          source: Trans8.V0S347P2.filter
          alias: pdProv
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "pdProv.CLM_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "pdProv.PROV_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: "pdProv.PROV_NM"
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
              map: "pdProv.PROV_SK"
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "pdProv.REL_GRP_PROV_SK"
        - name: Trans8.V0S347P2.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'PD'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: Trans8.V0S347P3
          type: map
          source: Trans8.V0S347P3.filter
          alias: pdProv
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "pdProv.CLM_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "pdProv.PROV_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: "pdProv.PROV_NM"
        - name: Trans8.V0S347P3.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'PCP'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: Trans6.V102S0P2
          type: map
          source: Trans6.V102S0P2.filter
          alias: billSvcProv
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "billSvcProv.CLM_SK"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "billSvcProv.PROV_BILL_SVC_ID"
        - name: Trans6.V102S0P2.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'PD'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_ID
              type: char
              width: 12
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: Trans6.V102S0P3
          type: map
          source: Trans6.V102S0P3.filter
          alias: billSvcProv
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "billSvcProv.CLM_SK"
            - name: PROV_BILL_SVC_ID
              type: varchar
              width: 20
              nullable: N
              map: "billSvcProv.PROV_BILL_SVC_ID"
        - name: Trans6.V102S0P3.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'SVC'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_BILL_SVC_ID
              type: char
              width: 12
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: Trans10
          type: map
          source: Trans10.filter
          alias: pdProvAddr
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "pdProvAddr.CLM_SK"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "pdProvAddr.PROV_ID"
            - name: PROV_NM
              type: varchar
              width: 75
              map: "pdProvAddr.PROV_NM"
            - name: ADDR_LN_1
              type: varchar
              width: 40
              map: "pdProvAddr.ADDR_LN_1"
            - name: ADDR_LN_2
              type: varchar
              width: 40
              map: "pdProvAddr.ADDR_LN_2"
            - name: ADDR_LN_3
              type: varchar
              width: 40
              map: "pdProvAddr.ADDR_LN_3"
            - name: CITY_NM
              type: varchar
              width: 35
              map: "pdProvAddr.CITY_NM"
            - name: POSTAL_CD
              type: varchar
              width: 20
              map: "pdProvAddr.POSTAL_CD"
            - name: PROV_ADDR_ST_CD
              type: varchar
              width: 20
              map: "pdProvAddr.PROV_ADDR_ST_CD"
        - name: Trans10.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'PD'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: ADDR_LN_1
              type: varchar
              width: 40
            - name: ADDR_LN_2
              type: varchar
              width: 40
            - name: ADDR_LN_3
              type: varchar
              width: 40
            - name: CITY_NM
              type: varchar
              width: 35
            - name: POSTAL_CD
              type: varchar
              width: 20
            - name: PROV_ADDR_ST_CD
              type: varchar
              width: 20
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: Trans9
          type: map
          source: Trans9.filter
          alias: svcProv
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
              map: "svcProv.CLM_SK"
            - name: PROV_NM
              type: varchar
              width: 75
              map: "svcProv.PROV_NM"
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "svcProv.PROV_ID"
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
              map: "svcProv.CMN_PRCT_SK"
            - name: CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
              map: "svcProv.CMN_PRCT_ID"
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "svcProv.REL_GRP_PROV_SK"
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
              map: "svcProv.REL_IPA_PROV_SK"
            - name: GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "svcProv.GRP_PROV_ID"
            - name: IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
              map: "svcProv.IPA_PROV_ID"
        - name: Trans9.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'SVC'"
          schema: 
            - name: CLM_SK
              type: int
              width: 4
              nullable: N
            - name: PROV_NM
              type: varchar
              width: 75
            - name: PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: CMN_PRCT_SK
              type: int
              width: 4
              nullable: N
            - name: CMN_PRCT_ID
              type: varchar
              width: 20
              nullable: N
            - name: REL_GRP_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: REL_IPA_PROV_SK
              type: int
              width: 4
              nullable: N
            - name: GRP_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: IPA_PROV_ID
              type: varchar
              width: 20
              nullable: N
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - CLM_SK
        - name: Transform
          type: map
          source: Trans1.V0S20P101
          alias: Snapshot
          schema: 
            - name: SRC_SYS_CD
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.SRC_SYS_CD"
            - name: CLM_ID
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.CLM_ID"
            - name: CLM_SVC_STRT_DT
              type: timestamp
              nullable: N
              map: "Snapshot.SVC_STRT_DT_SK"
            - name: CLM_LN_TOT_CHRG_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.CHRG_AMT"
            - name: CLM_PAYBL_AMT
              type: decimal
              width: 13
              precision: 2
              nullable: N
              map: "Snapshot.PAYBL_AMT"
            - name: MBR_UNIQ_KEY
              type: int
              width: 4
              nullable: N
              map: "Snapshot.MBR_UNIQ_KEY"
            - name: GRP_ID
              type: varchar
              width: 20
              nullable: N
              map: "Snapshot.GRP_ID"
          keys:
            - SRC_SYS_CD
            - CLM_ID
        - name: Trans5
          type: map
          source: Trans5.filter
          alias: subAddr
          schema: 
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "subAddr.SUB_SK"
            - name: ADDR_LN_1
              type: varchar
              width: 40
              map: "subAddr.ADDR_LN_1"
            - name: ADDR_LN_2
              type: varchar
              width: 40
              map: "subAddr.ADDR_LN_2"
            - name: CITY_NM
              type: varchar
              width: 35
              map: "subAddr.CITY_NM"
            - name: POSTAL_CD
              type: varchar
              width: 20
              map: "subAddr.POSTAL_CD"
            - name: SUB_ADDR_ST_CD_SK
              type: varchar
              width: 20
              map: "subAddr.TRGT_CD"
        - name: Trans5.filter
          type: filter
          source: 
          filter: "SUB_ADDR_CD = 'SUBHOME'"
          schema: 
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: ADDR_LN_1
              type: varchar
              width: 40
            - name: ADDR_LN_2
              type: varchar
              width: 40
            - name: CITY_NM
              type: varchar
              width: 35
            - name: POSTAL_CD
              type: varchar
              width: 20
            - name: SUB_ADDR_ST_CD_SK
              type: varchar
              width: 20
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
            - name: SUB_ADDR_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - SUB_SK
        - name: Trans4
          type: map
          source: Trans4.filter
          alias: subName
          schema: 
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
              map: "subName.SUB_SK"
            - name: FIRST_NM
              type: varchar
              width: 35
              map: "subName.FIRST_NM"
            - name: MIDINIT
              type: char
              width: 1
              map: "subName.MIDINIT"
            - name: LAST_NM
              type: varchar
              width: 75
              map: "subName.LAST_NM"
        - name: Trans4.filter
          type: filter
          source: 
          filter: "Trim(TRGT_CD) = 'SUB'"
          schema: 
            - name: SUB_SK
              type: int
              width: 4
              nullable: N
            - name: FIRST_NM
              type: varchar
              width: 35
            - name: MIDINIT
              type: char
              width: 1
            - name: LAST_NM
              type: varchar
              width: 75
            - name: TRGT_CD
              type: varchar
              width: 20
              nullable: N
          keys:
            - SUB_SK
        - name: CLM_DM_CLM
          type: unknown
          source: CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm
          origType: CODBCStage
        - name: W_CLM_INIT
          type: unknown
          source: CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm
          origType: CODBCStage
        - name: BClmDmClm
          type: unknown
          source: CLM_DM_CLMNullTrans1.lookupW_CLM_INITBClmDmClm
          origType: CODBCStage


