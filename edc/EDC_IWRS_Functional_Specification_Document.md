# Functional Specification Document
# DataTrial EDC/IWRS System

---

**Document Version:** 1.1
**Date:** February 16, 2026
**Classification:** Confidential

**Revision History:**

| Version | Date | Description | Author |
|---------|------|-------------|--------|
| 1.0 | January 27, 2026 | Initial document | DataTrial Team |
| 1.1 | February 16, 2026 | Updated based on client review comments | DataTrial Team |

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Purpose and Scope](#2-purpose-and-scope)
3. [Regulatory Compliance](#3-regulatory-compliance)
4. [System Overview](#4-system-overview)
5. [Functional Requirements](#5-functional-requirements)
   - 5.1 [User Management](#51-user-management)
   - 5.2 [Navigation Administration](#52-navigation-administration)
   - 5.3 [Data Security](#53-data-security)
   - 5.4 [Home Page](#54-home-page)
   - 5.5 [Audit Trail](#55-audit-trail)
   - 5.6 [Scalability](#56-scalability)
   - 5.7 [Study Setup](#57-study-setup)
   - 5.8 [CRF Builder](#58-crf-builder)
   - 5.9 [Query Management](#59-query-management)
   - 5.10 [Lab Data Upload](#510-lab-data-upload)
   - 5.11 [IWRS Module](#511-iwrs-module)
   - 5.12 [Subject Visit Management](#512-subject-visit-management)
   - 5.13 [IP Management](#513-ip-management)
   - 5.14 [Reports](#514-reports)
   - 5.15 [Data Extract](#515-data-extract)
6. [User Roles and Responsibilities](#6-user-roles-and-responsibilities)
7. [Backup and Recovery](#7-backup-and-recovery)
8. [Issue Management](#8-issue-management)
9. [Appendices](#9-appendices)

---

## 1. Introduction

This Functional Specification Document (FSD) defines the functional requirements for the DataTrial EDC/IWRS (Electronic Data Capture / Interactive Web Response System) application. The system is designed to enable clinical research organizations and sponsors to plan and manage clinical trial operations effectively and efficiently.

The DataTrial EDC/IWRS is a fully web-based solution that provides a centralized clinical trial database, offering users access to the most relevant and up-to-date information based on their specific roles and responsibilities.

---

## 2. Purpose and Scope

### 2.1 Purpose

The purpose of this document is to:
- Define the functions and capabilities the software system must provide
- Define the requirements of the DataTrial EDC/IWRS application
- Enable global clinical research organizations and sponsors to maintain a centralized clinical trial database

### 2.2 Scope

The system provides real-time and relevant information accessible to all trial participants including:
- CRAs (Clinical Research Associates) managing sites
- Regional Managers responsible for geographic areas
- Global Trial Managers managing global trials
- Sponsor Project Managers

### 2.3 Intended Users

| Role | Access Type |
|------|-------------|
| CRA (Clinical Research Associate) | Blinded |
| PM (Project Manager) | Blinded |
| DM (Data Manager) | Blinded |
| Sponsor | Blinded |
| Depot Manager/Pharmacist | Unblinded |
| Medical Monitor | Blinded |

---

## 3. Regulatory Compliance

### 3.1 ICH-GCP Compliance

The system complies with Good Clinical Practice (GCP), an international quality standard provided by ICH (International Council for Harmonisation). This standard:
- Defines a set of standards which governments can transpose into regulations for clinical trials involving human subjects
- Assures ethical conduct of clinical trials and safety and well-being of subjects

### 3.2 US FDA 21 CFR Part 11 Compliance (URS 1.8)

The system shall comply with US FDA 21 CFR Part 11 requirements which define criteria under which:
- Electronic records are reliable and equivalent to paper records
- Electronic signatures are reliable and equivalent to handwritten signatures

| Req ID | Description |
|--------|-------------|
| URS 1.8.1 | System shall comply with the US FDA 21 CFR Part 11 requirements |
| URS 1.8.2 | The application will be a secure, web-based application that will be hosted online using a private cloud with high level of data security and 64 bit data encryption for record confidentiality |
| URS 1.8.3 | The system shall deploy electronic signatures for record authenticity and integrity |

### 3.3 OS Requirements (URS 1.9)

| Req ID | Description |
|--------|-------------|
| URS 1.9.1 | EDC/IWRS is a light weight application developed on Supabase. Port number 8181 should be unblocked. User can access the software using web-browser. Web browser must support Java Script, HTML 4.0 and CSS 1.0 standards |
| URS 1.9.2 | **Hardware Requirements:** CPU: Intel P2 or above, RAM: Space of at least 2GB. **Operating System:** Browser (web) based application functional in all Operating System. **Browser:** Mozilla Firefox version 3.5 or later, Google Chrome version 4.0 or later, Microsoft Internet Explorer version 7.0 or later, Apple Safari version 4.0 or later. **Internet Connection:** 512 Kbps or above |

---

## 4. System Overview

### 4.1 System Description

DataTrial EDC/IWRS application is a fully web-based solution that enables clinical research organizations and sponsors to:
- Plan and manage clinical trial operations effectively and efficiently
- Streamline and optimize their entire clinical trial operations
- Maintain high-quality, audit-ready documentation and data

### 4.2 Key Features

The system provides:
- Centralized database to access all trial-related data
- Role-based access control for limited access to trial data based on functional role
- Real-time and relevant information accessible to all trial participants

---

## 5. Functional Requirements

### 5.1 User Management

#### 5.1.1 Authentication
| Req ID | Description |
|--------|-------------|
| URS 1.1.1 | System must be accessible using valid User ID and Password |
| URS 1.1.2 | System must allow creation/revoking of user accounts |
| URS 1.1.3 | Invalid User ID/Password must be denied access to the system |

#### 5.1.2 Password Management
| Req ID | Description |
|--------|-------------|
| URS 1.1.4 | System must allow setting of password maintenance policies (password aging, change password, session timeout) |
| URS 1.1.5 | System must allow locking and unlocking of users on unauthenticated attempts. After 3 attempts, user account shall be locked |
| URS 1.1.7 | System must allow forgot password functionality |

#### 5.1.3 Role Management
| Req ID | Description |
|--------|-------------|
| URS 1.1.6 | System must allow authorized user to create custom roles and assign permissions. System should allow authorized user to add/modify/remove study/site level access rights |

#### 5.1.4 General Access Control (URS 1.2)
| Req ID | Description |
|--------|-------------|
| URS 1.2.1 | The system must allow secure access to Monitors/Data Manager/Depot manager/Medical Monitor/Sponsor/Lab/Project manager/Sponsor |
| URS 1.2.2 | The system must allow secure access to Investigators |
| URS 1.2.3 | The system must allow secure access to Study Coordinators |
| URS 1.2.4 | System must follow Role Based Access Controls for each role and module in the system |

#### 5.1.4 Password Strength Requirements
- New password should match details mentioned in Password Strength policy
- If password does not match requirements, validation message should be raised restricting user from updating password
- On successful password change, email should be received by user with password details

### 5.2 Navigation Administration

| Req ID | Description |
|--------|-------------|
| URS 1.3.1 | System must allow navigation within the application menu using a mouse |
| URS 1.3.2 | System must allow navigation within the application menu using the keyboard |

### 5.3 Data Security

| Req ID | Description |
|--------|-------------|
| URS 1.4.1 | If no action is performed for a configured period of time, the session should timeout |
| URS 1.4.2 | System should have capability of real-time data collection and dissemination. It should be a secure web-based solution accessible via internet and compatible with multiple internet browsers |
| URS 1.4.3 | System should be mobile-enabled. Users should be able to use the system on any mobile/tablet platform |

### 5.4 Home Page

| Req ID | Description |
|--------|-------------|
| URS 1.5 | Home Page functionality as entry point after successful login |
| URS 1.5.1 | System must have Home Page with Query Statistics, Study details, Visit Statistics |

**Home Page Features (observed from demo):**
- Study Information display with list of assigned studies
- Query Statistics dashboard
- Visit Statistics overview
- Quick access to study modules
- Development Requirements notifications
- Post-UAT Confirmation notifications
- Documentation notes

### 5.5 Audit Trail

| Req ID | Description |
|--------|-------------|
| URS 1.6.1 | System must provide Audit Trail for each and every User Activity with User Details, Activity Details, Date & Time Stamp (IST - Local time stamp) |
| URS 1.6.2 | System must provide Audit Trail for each and every Subject Record Activity with Event, Old value, New value, Reason for Change and Date and Time stamp (IST - Local time stamp) |
| URS 1.6.3 | Audit trail should show the original entry along with date, time (IST - Local time stamp) and user details |

**Audit Trail Report Types (observed from demo):**
- CRF Data Audit Trail
- Study Creation
- User Creation Details
- Study Team
- CRF Design - CRF
- CRF Design - Fields
- CRF Design - Enable Disable
- CRF Design - Queries
- CRF Design - LOV
- CRF Design - LOV Detail
- CRF Delete Data Audit Trail

### 5.6 Scalability

| Req ID | Description |
|--------|-------------|
| URS 1.7.1 | System should allow concurrent users to login at any given time without affecting performance or crashing |
| URS 1.7.2 | System should allow several records to be populated and yet show optimal performance |

### 5.7 Study Setup (Study Builder)

#### 5.7.1 Study Builder Activity Flow
| Req ID | Description |
|--------|-------------|
| URS 2.14 | Study Builder Activity flow: Study creation → Visit creation → LOV creation → Panel/CRF creation → Allocation of CRF/Panel to particular visits → Edit check design → Assignment of edit checks in applicable visits |

#### 5.7.2 Study Configuration Requirements
| Req ID | Description |
|--------|-------------|
| URS 2.1 | System must allow authorized users to create/modify/delete Study |
| URS 2.2 | System must allow authorized users to create/modify/delete Visits |
| URS 2.3 | System must allow authorized users to create/modify/delete Panels/CRFs |
| URS 2.4 | System will allow authorized users to create/apply/modify/delete validation rules including edit checks/warnings/dependencies as per study requirements |
| URS 2.5 | System will allow authorized users to copy validation rules including edit checks/warnings/dependencies as per study requirements from one visit to other, from 1 study to other |
| URS 2.6 | System must allow creation of unscheduled Visits |
| URS 2.7 | System must allow to add list of values |
| URS 2.8 | System must allow to configure patient visit schedule |
| URS 2.9 | System must allow single row and multiple row data entry design whenever required |
| URS 2.10 | System should allow users to store and manage Study Arm details |
| URS 2.11 | System should provide the capability to plan and manage study in multiple countries including any currencies and specific regulatory documents |
| URS 2.13 | System must allow authorized users to print blank CRF, blank annotated CRF in PDF format |

#### 5.7.3 Data Capture (URS 3.0)
| Req ID | Description |
|--------|-------------|
| URS 3.1 | System must allow authorized user to enroll/edit a Site for all Studies. It should provide ability to recruit one or more sites for a study |
| URS 3.2 | System must allow authorized user to enroll/modify Subject/Patient details in study |
| URS 3.3 | System must allow authorized users to access for assigned studies only |

#### 5.7.4 Study Information Fields

**Study Setup Fields (observed from demo):**
- Protocol Number
- Project Number
- Sponsor
- Project Manager
- Title
- Study Type (EDC+IWRS)
- Number of Subjects
- Study Phase
- Study Status
- Lab Data Upload (Yes/No)

#### 5.7.2 Study Documents
| Req ID | Description |
|--------|-------------|
| URS 2.12 | System should provide the capability to store and manage documents of Study |

#### 5.7.3 Study Arm
- System should provide capability to plan and manage study in multiple countries including any currencies and specific regulatory documents

#### 5.7.4 Kit Definition
| Req ID | Description |
|--------|-------------|
| | System shall allow defining Kit Name and Description for Test and Reference drugs |

**Observed Kit Types:**
- Test (e.g., "T of XXXXX 2mg + XXXXXXX 325mg Capsule")
- Reference (e.g., "R of XXXXXX 60mg + XXXXXXXXX 325mg Capsule")

#### 5.7.5 Kit Generate
- System shall allow generation of kits for the study

#### 5.7.6 Enrolled Sites
| Field | Description |
|-------|-------------|
| Site Number | Unique site identifier |
| Site Code | Site code |
| Site | Site name |
| Site Initials | Abbreviated site name |
| Status | Qualified/Active/Recruiting |
| Site Location | Geographic location (Country) |
| CRF Workflow | ECRF |
| Site Zone | Optional zone classification |
| Site Sector | Optional sector classification |
| Cap Size | Maximum subjects allowed |
| Comments | Additional notes |

#### 5.7.7 Study Team
- Assign team members to the study with specific roles

#### 5.7.8 Study Schedule
| Field | Description |
|-------|-------------|
| Visit Title | Name of the visit (e.g., "VISIT 1 (SCREENING)") |
| Visit Type | First Visit, Visit, Common Forms |
| Duration | Duration in days |
| Window Period (+/-) | Allowable deviation from scheduled date |
| Display Sequence | Order of display |
| CRF | Associated CRF forms |

**Standard Visit Types (observed):**
- VISIT 1 (SCREENING)
- VISIT 2 (RANDOMIZATION)
- VISIT 3 (EOT)
- VISIT 4 (EOS)
- Common Forms
- Unscheduled Visit

#### 5.7.9 Study Schedule (URS 11.7)
| Req ID | Description |
|--------|-------------|
| URS 11.7 | System shall allow authorized users to configure and manage study visit schedules including visit windows, duration, and CRF assignments |

**Study Schedule Configuration Parameters:**
| Parameter | Description |
|-----------|-------------|
| Visit Title | Name of the visit |
| Visit Type | First Visit, Visit, Common Forms, Unscheduled |
| Duration (Days) | Number of days for the visit |
| Window Period (+/-) | Allowable deviation from scheduled date |
| From Visit | Reference visit for window calculation |
| Display Sequence | Order in which visits appear |
| Alert Configuration | Alert settings for visit windows |
| CRF Assignment | CRFs allocated to the visit |

### 5.8 CRF Builder

#### 5.8.1 CRF Setup Types
| Type | Description |
|------|-------------|
| Dynamic Pages CRF Setup | Configurable CRF pages |
| Static Pages CRF Setup | Fixed format CRF pages |
| Multi Column CRF Setup | Multiple column layouts |
| Dropped CRFs | Deactivated CRFs |

#### 5.8.2 CRF Page Configuration
| Field | Description |
|-------|-------------|
| CRF Name | Full name of the CRF |
| CRF Short Name | Abbreviated identifier |
| Display Short | Short display name |
| Visit Names | Associated visits |
| Designed | Yes/No |
| CRF Type | Visit Parent Page, Simple CRF, Visit Child Page |
| Parent Page | Reference to parent CRF if applicable |
| Audit | Link to audit trail |

**Standard CRF Pages (observed):**
- DATE OF VISIT (DOV)
- INFORMED CONSENT (ICF)
- DEMOGRAPHICS (DMG)
- PRIMARY DIAGNOSIS (PDG)
- OTHER MEDICAL AND SURGICAL HISTORY (MSH/MSHD)
- SERUM/URINE PREGNANCY TEST (SUPT)
- VITAL SIGNS (VS)
- PHYSICAL EXAMINATION (PE)
- 12-LEAD RESTING ECG (ECG)
- SEROLOGY TESTS
- INCLUSION CRITERIA
- EXCLUSION CRITERIA
- ELIGIBILITY VERIFICATION (ELIV)
- HEMATOLOGY
- BIOCHEMISTRY
- COMPLETE URINE EXAMINATION (URIN)
- ADVERSE EVENT FORM (AE)
- PRIOR AND CONCOMITANT MEDICATIONS (CM)

**Sample 12-Lead ECG Fields:**
| Field | Item Type |
|-------|-----------|
| Date of ECG | DATE |
| Time of ECG | TEXT |
| Heart Rate (bpm) | NUMBER |
| PR Interval (ms) | NUMBER |
| QRS Duration (ms) | NUMBER |
| QT Interval (ms) | NUMBER |
| QTc Interval (ms) | NUMBER |
| ECG Interpretation | SELECT LIST (Normal/Abnormal/Not Done) |
| If Abnormal, Clinically Significant | SELECT LIST (Yes/No) |
| Comments | TEXT |

**Sample Inclusion/Exclusion Criteria Fields:**
| Field | Item Type |
|-------|-----------|
| Criteria Number | TEXT (auto-generated) |
| Criteria Description | TEXT (display only) |
| Met | SELECT LIST (Yes/No) |
| Comment | TEXT |

**Sample Informed Consent Fields:**
| Field | Item Type |
|-------|-----------|
| Date ICF Signed by Subject | DATE |
| Time ICF Signed | TEXT |
| ICF Version Number | TEXT |
| ICF Version Date | DATE |
| Subject Signed | SELECT LIST (Yes/No) |
| LAR Signed (if applicable) | SELECT LIST (Yes/No/NA) |
| Investigator/Designee Signed | SELECT LIST (Yes/No) |
| Impartial Witness Present | SELECT LIST (Yes/No/NA) |

#### 5.8.3 Field Declaration
| Field Property | Description |
|----------------|-------------|
| CRF Short Name | Parent CRF identifier |
| Item Type | SELECT LIST, NUMBER, TEXT, DATE |
| Column Name | Database column name |
| Item Label | Display label |
| Item Seq | Display sequence |
| Item Size | Field size |
| No. of Digits after Decimal | Decimal precision |

**Sample Demographics Fields:**
- Gender (SELECT LIST)
- Date of Birth (DATE)
- Age (Years) (NUMBER)
- Height (cm) (NUMBER)
- Ethnicity (SELECT LIST)
- Race (SELECT LIST)
- Reproductive status (SELECT LIST)
- Is the Subject Pregnant? (Yes/No/NA)
- Is the Subject giving Breast feeding? (Yes/No/NA)
- Is the Subject Current Smoker?
- Is the Subject alcoholic/drug or any substance used?

**Sample Vital Signs Fields:**
| Field | Item Type | Size |
|-------|-----------|------|
| Date of Visit | DATE | - |
| Time of Assessment | TEXT | 20 |
| Weight (Kg) | NUMBER | 5 |
| Height (cm) | NUMBER | 5 |
| BMI (kg/m²) | NUMBER | 5 (auto-calculated) |
| Systolic Blood Pressure (mmHg) | NUMBER | 5 |
| Diastolic Blood Pressure (mmHg) | NUMBER | 5 |
| Pulse Rate (beats/min) | NUMBER | 5 |
| Temperature (°C) | NUMBER | 5 |
| Respiratory Rate (breaths/min) | NUMBER | 5 |

**Sample Physical Examination Fields:**
| Field | Item Type |
|-------|-----------|
| Assessment Date | DATE |
| General Appearance | SELECT LIST (Normal/Abnormal) |
| Skin | SELECT LIST (Normal/Abnormal) |
| Head/Eyes/Ears/Nose/Throat | SELECT LIST (Normal/Abnormal) |
| Cardiovascular | SELECT LIST (Normal/Abnormal) |
| Respiratory | SELECT LIST (Normal/Abnormal) |
| Gastrointestinal | SELECT LIST (Normal/Abnormal) |
| Musculoskeletal | SELECT LIST (Normal/Abnormal) |
| Neurological | SELECT LIST (Normal/Abnormal) |
| Other | TEXT |
| Comments | TEXT |

#### 5.8.4 Query Setup - Type of Queries
| Query Type | Description |
|------------|-------------|
| Same Panel Single Item | Query on single item within same panel |
| Same Panel Multiple Item | Query comparing multiple items in same panel |
| Cross Panel | Query across different panels |
| Cross Visit | Query across different visits |

**Query Configuration Fields:**
- CRF Name
- Rulename
- Visit Title
- Column
- Condition 1 (equals, is null, etc.)
- Value 1
- Column 2
- Condition 2
- Value 2
- Query Message
- Active (Yes/No)
- Audit

#### 5.8.5 CRF Status Workflow
| Status | Return Value | Description |
|--------|--------------|-------------|
| No Visit Generated | 1 | Initial state |
| Incomplete | 2 | Data entry started |
| Complete | 3 | All data entered |
| Data Submitted | 4 | Data submitted for review |
| Query Generated by CRA | 5 | CRA raised query |
| SDV | 6 | Source Data Verification |
| Query Generated by DM | 7 | Data Manager raised query |
| DM Reviewed | 8 | Data Manager reviewed |
| Query Generated by QA | 8.3 | QA raised query |
| QA Reviewed | 8.6 | QA reviewed |
| PI Reviewed | 9 | Principal Investigator reviewed |
| Locked | 10 | Record locked |
| Unlocked | 11 | Record unlocked |

#### 5.8.6 Dynamic Workflow Study Wise
Configuration of workflow transitions based on:
- CRF Workflow
- Display Val
- Sequence
- Role
- Stored Value
- Next Sec Status
- Auto Query, CRA Query, DM Query, QA Query status transitions

### 5.8.5 Data Capture (URS 3.0)

| Req ID | Description |
|--------|-------------|
| URS 3.1 | The system must allow authorized user to enroll/edit a Site for all Studies. It should provide ability to recruit one or more sites for a study |
| URS 3.2 | The system must allow authorized user to enroll/modify Subject/Patient details in study |
| URS 3.3 | The system must allow authorized users to access for assigned studies only |
| URS 3.4 | The system must allow authorized users to enter/modify Data. System should have search functionality |
| URS 3.5 | The system must allow authorized user to Submit data in particular study for further review process |
| URS 3.6 | The system must allow authorized user to perform Review/SDV/UnSDV at Panel level in particular study |
| URS 3.7 | The system must allow authorized user to e-Sign Off |
| URS 3.8 | System should allow user to change value of any field, after changing value system should ask for reason for change |

### 5.8.6 Medical Coding (URS 5.0)

| Req ID | Description |
|--------|-------------|
| URS 5.1 | The system should have Auto-coding functionality using MedDRA Coding |
| URS 5.2 | Audit Log should be available for every coding term on the coding interface |

### 5.8.7 Data Lock (URS 6.0)

| Req ID | Description |
|--------|-------------|
| URS 6.1 | The system must allow authorized person to perform database lock with a reason. It should be panel wise, Visit wise, Site wise and Study wise. Lock should be allowed only when all queries are at resolved stage for particular panel/Visit/Subject/Study |
| URS 6.2 | Once the data is locked, no further data modification or Query management should be allowed |

### 5.8.8 Data Unlock (URS 7.0)

| Req ID | Description |
|--------|-------------|
| URS 7.1 | The system must allow authorized person to unlock the locked data with a reason |

### 5.9 Query Management

| Req ID | Description |
|--------|-------------|
| URS 4.1 | System must be able to raise an automatic query based on validation rules applied on study on real time basis |
| URS 4.2 | System must allow authorized user to raise a manual query on any field |
| URS 4.3 | System must allow to close queries automatically based on edit checks fulfillment |
| URS 4.4 | System must allow authorized user to answer queries |
| URS 4.5 | System must automatically assign queries to authorized person as per assignment |

#### Query Management Workflow
```
Open → Responded → Closed
         ↓
      Reopened → Responded → Closed
```

| Req ID | Description |
|--------|-------------|
| URS 4.6 | Authorized person should be allowed to change the query status irrespective of query workflow. System must allow linking multiple queries together. So, at the same time, status of multiple queries can be changed together |
| URS 4.7 | System must support multiple Query flow configuration based on location |
| URS 4.8 | **Query Flow Rules**: On save button no Auto query should get fire and reason for change should not be populated if data is changed. On Save & Submit button, all Auto query should get fire and reason for change should come if data is updated. Once save and submit is clicked, Add query button should be seen to CRA and Data Manager (both can raise manual query). For auto query, CRC or PI can respond. Data Manager should be allowed reopen/close autoquery. CRA can SDV data; once SDV button is clicked, Save&Submit should be disabled from CRC login. All query raised by CRA should be closed before SDV is clicked |

**Query Flow Rules:**
- On save button no Auto query should get fire and reason for change should not be populated for any field if data is changed in particular field
- On Save & Submit button, All Auto query should get fire. Once Save & Submit button is clicked, reason for change should come if data is updated in any field
- Once save and submit button is clicked, Add query button should be seen to CRA/DM/QA based on workflow configuration

**Query Response Interface:**
- Previous Response(s) display
- Response text field
- Status selection (Closed/Reopened)
- Created By, Created On, Contact Role information
- Query Response tab

**Query Indicators (observed):**
- Yellow flag: Open Query
- Green flag: Responded Query
- Blue flag: Closed Query
- Orange flag: Reopened Query

#### SDV Process
- All queries raised by CRA should be closed before SDV is clicked
- If any query is in open, responded, or reopen status, then on click of SDV button CRA should get an alert to close all queries
- For Data Manager, there is no alert - he has to manually verify to close his respective queries

### 5.10 Lab Data Upload

| Req ID | Description |
|--------|-------------|
| URS 8.1 | System must support Lab Data upload functionality (Hematology, Biochemistry, Urine Analysis results in Lab pages). Should be uploaded via excel. Interpretation shall be entered manually by respective user |
| | PM shall have access to upload lab results in a defined format |
| | System should support cumulative lab data upload for Hematology, Biochemistry and Urine Analysis |

#### Lab Pages Declaration
- Configuration of lab data panels per study
- Define which lab pages receive uploaded data

**Lab Pages Configuration (observed from demo):**
| Lab Page | Short Name | Description |
|----------|------------|-------------|
| HEMATOLOGY | LB1 | Complete blood count and related tests |
| BIOCHEMISTRY | LB2 | Blood chemistry panel |
| COMPLETE URINE EXAMINATION | URIN | Urine analysis |
| SEROLOGY TESTS | - | Serology panel |

**Lab Data Upload Process:**
1. PM uploads lab results in defined Excel format
2. System validates data against configured lab pages
3. Data is auto-populated to respective CRF fields
4. Interpretation (Normal/Abnormal/Clinically Significant) entered manually by authorized user
5. Cumulative upload supported for Hematology, Biochemistry, and Urine Analysis

### 5.11 IWRS Module

| Req ID | Description |
|--------|-------------|
| URS 9.1 | System must follow below requirements for IWRS |

#### 5.11.1 Study Configuration
**Study Title Format:** A Prospective, Multi-Centric, Double Blind, Parallel Group, Active Controlled study

**IWRS Configuration Parameters:**
| Parameter | Value |
|-----------|-------|
| Treatment allocation Ratio | 1:1 |
| Blinding | Double Blinded |
| Randomization Algorithm | Provided by XXXX team and uploaded by DataTrial team |
| Stratification Ratio | No Stratification Ratio |
| Arm issue Sequence to subjects | Provided by XXXX team and uploaded by DataTrial team |
| IP Number Format | Studynumber1001 (e.g., 19040001, 19040002, 19040003) |
| Site level randomization | Central randomization |
| Site ID Format | 101, 102, etc. |
| Screening ID Format | Studynumber/SiteID/S001 (e.g., 1904SiteIDS001, 1904SiteIDS002) - System generated |
| Randomization ID Format | Studynumber/SiteID/R001 (e.g., 1904SiteIDR001) - System generated |

#### 5.11.2 Randomization
| Req ID | Description |
|--------|-------------|
| URS 10.1 | After subject screening, Screening Visit shall only be available |
| URS 10.2 | Kit Request shall be generated manually by CRA in system. After kit Request generation, PM shall approve/Decline kit request. If kit request is Declined, Depot manager should not be allowed to generate shipment for that site |
| | After PM approves, Depot Manager shall perform shipment create/dispatch process. While creating shipment, Depot manager can define the number of test and reference arm. IWRS system will pick the Kit numbers from the available depot inventory and shipment request alert will be sent to designated users. IWRS should pick the Kit# in sequence |
| URS 10.3 | Kit shall be received by designated Site user (e.g., CRC), as soon as the shipment is accepted by site users, system shall send acknowledgement to designated users. Site user shall have following option to mark the kits included in the shipment: Received, Damaged, Quarantined |

#### 5.11.3 Kit Status Options
| Status | Description |
|--------|-------------|
| Received | Received drugs shall be available for dispensing to subjects |
| Damaged | Damaged drugs, if available at site, won't be selected for dispensing to subjects |
| Quarantined | Site user shall have an option to Quarantine a shipment at the time of receipt. A quarantined shipment can later be accepted/rejected by site user only in case of e-mail approval from sponsor |

#### 5.11.4 IP Return Process
| Req ID | Description |
|--------|-------------|
| URS 10.4 | **IP Replacement** - Provision shall be there for "IP replacement procedure" in case of damage/lost. If a kit of Test arm is damaged then next available kit i.e Test arm will only be assigned to subject. Only Received drugs that are not damaged/lost shall be assigned to subject |
| | IP Return Process - At the end of the study, post final accountability Independent CRC shall update the kit inventory status as DESTROYED AT SITE for the kits that are destroyed at site. Site users can return unused/damaged drugs or quarantined shipment to Depot |
| URS 10.5 | Depot shall be able to receive and accept the shipment dispatched from Site. Site user shall be allowed to dispatch shipment through separate tab 'Return to Depot'. Depot user shall have following options to mark the drugs included in the shipment: Received, Damaged, Quarantined, UnQuarantined, Expired, Destroyed at Depot, Lost, Temperature Excursion |

**Complete IP Status Options:**
| Status | Location | Description |
|--------|----------|-------------|
| Received | Site/Depot | Drug received successfully |
| Damaged | Site/Depot | Drug damaged, not available for dispensing |
| Quarantined | Site/Depot | Drug quarantined pending review |
| UnQuarantined | Depot | Drug released from quarantine |
| Expired | Site/Depot | Drug past expiry date, not available for dispensing |
| Lost | Site/Depot | Drug lost in transit, not included in available quantity count |
| Temperature Excursion | Site | Drug had temperature excursion at receipt, not available for dispensing |
| Retention | Site | Drug with 'Retention' status shall not be selected for dispensing and shall not be included in return shipment to Depot |
| Destroyed at Site | Site | Kit destroyed at site post final accountability |
| Destroyed at Depot | Depot | Kit destroyed at depot |
| Dispensed to Patient | Site | Kit dispensed to subject |
| URS 10.6 | Unblinding Procedure - No Unblinding Procedure is required from IWRS system |

### 5.12 Subject Visit Management

| Req ID | Description |
|--------|-------------|
| URS 11.0 | Subject Visit management |

#### 5.12.1 Subject Status Flow
| Status | Description |
|--------|-------------|
| Screened | Subject has completed screening |
| Screen Failed | Subject failed screening criteria |
| Enrolled | Subject enrolled in study |
| Discontinue | Subject discontinued from study |
| Completed | Subject completed study |

| Req ID | Description |
|--------|-------------|
| URS 11.1 | Subject enrollment and randomization flow (detailed below) |
| | If age is not between 18-65, system should Screenfail subject |
| URS 11.2 | Pre mature termination should be available at subject level to discontinue subjects. Once Pre mature termination is done, subject status should be discontinued and End of study visit should get autogenerated |

**URS 11.1 - Detailed Randomization Flow:**
1. CRC/Investigator shall enroll the subject in IWRS. EDC panels should be visible once data entry is completed in IWRS panels
2. CRC/Investigator will raise the request for randomization once data entry is completed in screening visit IWRS panel
3. After eligibility check, medical monitor approves the randomization request. Medical monitor should have 'send back to CRC' and 'Reject' option if any error identified during review
4. After medical monitor approves the randomization request, CRC/Investigator shall randomize the subject in IWRS. Once the information is approved, IWRS shall assign randomization number as per study arm and kit number to the subject, based on the randomization sequence. IP Dispensing shall be done in Visit 2 (Randomization) and in subsequent visit (if applicable).

**Screening Visit Fields:**
- Hospital OP Registration number
- Patient Signed - Yes/No
- PI/Designee Signed - Yes/No
- LAR - Yes/No
- IW - Yes/No

**Eligibility Request Fields:**
- Is Allergic Rhinitis previously diagnosed? If yes since when (In years)
- IgE test result – Positive/Negative
- IgE value
- 3 days of total Nasal Symptoms Scores - Yes/No
- Lab Report - Yes/No
- ECG - Yes/No
- Previous Prescriptions - Yes/No
- Patient Screening Diary - Yes/No

*Note: Send for Approval button should be seen once data is filled in all fields of demographic and Eligibility request*

#### 5.12.2 Screening/Randomization Cap Size
| Req ID | Description |
|--------|-------------|
| URS 11.3 | ADMIN/ PM should have rights to define Screening/Randomization Cap Size. Authorized user can define maximum number of subjects allowed to be Screened/Randomized per site. **Screening/Randomization Allocation** - PM should have rights to define Screening/Randomization Allocation. Authorized user can release the subject for Screening/Randomization from number defined in Screening/Randomization Cap Size |
| URS 11.4 | **Data reflection from IWRS to EDC** - Data entered in Following fields in IWRS shall be prefilled in EDC CRF pages. User shall manually save the data for EDC pages: Visit Date, Randomization number, Kit number, Date of dispensed, Number of tablets dispensed |
| | PM should have rights to define Screening/Randomization Allocation. Authorized user can release the subject for Screening/Randomization from number defined in Screening/Randomization Cap Size |

#### 5.12.3 Data Reflection from IWRS to EDC
Data entered in Following fields in IWRS shall be prefilled in EDC CRF pages. User shall manually save the data for EDC pages:

**Visit 2 (Randomization):**
- Subject Initial
- Screening Number
- Visit date
- Randomization number - Autogenerated
- Assigned Kit number - Autogenerated
- Number of tablets dispensed in current visit - Autocalculated once dispense kit button is clicked

**Visit 3:**
- Subject Initial
- Screening Number
- Visit Date
- Kit dispensed in Visit 2 - Autoreflect data of visit 2
- Number of tablets dispensed in Visit 2 - Autoreflect data of visit 2
- Number of tablets returned
- Number of tablets missed/disposed
- Assigned Kit number - Autogenerated
- Number of tablets dispensed in current visit - Autocalculated once dispense kit button is clicked

**Visit 4 (EOT):**
- Subject Initial
- Screening Number
- Visit Date

#### 5.12.4 Visit Schedule Configuration (Eg.)
| Visit | Type | Duration (days) | Window | From Visit | Alert |
|-------|------|-----------------|--------|------------|-------|
| Screening Visit | Screening | 14 | Actual date | 0/0 | NA |
| Visit 2 (Randomization) | Randomization | 0 | Date of Randomization | Actual date | 0/0 | NA |
| Visit 3 | Visit | 14 | Randomization Visit | Actual date | 2/2 | NA |
| Visit 4 (EOT) | Visit | 28 | Randomization Visit | Actual date | 2/2 | NA |
| FUP | Visit | 42 | Randomization Visit | Actual date | 2/2 | NA |

#### 5.12.5 Email Notifications (URS 11.8)
| Req ID | Description |
|--------|-------------|
| URS 11.8 | Email Notification/alerts is to be sent for different activities based on the assigned role. In case of any change in member, DataTrial team shall be intimated for updating |

**Email Notification Triggers:**
| Activity | Recipients |
|----------|------------|
| Shipment request generated | PM, CRA, and Depot Manager |
| Kit dispatched to site | PM, CRA, Site users |
| Kit received at site | PM, CRA, Depot Manager |
| Subject screened | PM, CRA |
| Randomization request raised | Medical Monitor |
| Subject randomized | PM, CRA |
| Subject discontinued | PM, CRA |
| Query raised | Assigned user based on role |
| Query responded | Query originator |

#### 5.12.6 TRF, Comments and Documents Upload
| Req ID | Description |
|--------|-------------|
| URS 11.5 | **TRF** - Add TRF should be allowed in (Screening, Visit 4(EOT) and End of study). Only CRC/PI should have rights to add TRF. Once TRF is added, email alert should be received to Lab. Lab should be allowed to download TRF and upload relevant documents. CRA, Medical monitor and PM should have only view rights of TRF. **Comment** - Add comment should be allowed in all visits except FUP (Physical-Telephonic) and End of study. PI, CRC, CRA and Medical Monitor should have rights to add/view comments. Added comments should be seen to each other. **Documents Upload** - System should allow to upload documents in all visits except FUP (Physical-Telephonic). Upload documents should be allowed to CRC, PI, CRA, PM, Medical Monitor and Lab roles. System shall allow to download uploaded documents |
| URS 11.6 | **IWRS - Subject Visit Management** - System shall capture visit-specific data for each subject visit |

**URS 11.6 - Visit Details (Eg.):**

| Visit Details | Fields |
|---------------|--------|
| **Screening Visit** | Demographic Details: Visit Date, Subject Initial, Screening Number, Gender (Male/Female), Date of Birth, Informed Consent Date, Age (Autogenerated), Height(cm), Weight(kg). **Eligibility Request**: 5 days of Total Nasal Symptoms Scores (Yes/No), Lab Report (Yes/No), ECG (Yes/No), Previous Prescriptions (Yes/No), Patient Screening Diary (Yes/No). Note: Send for Approval button should be seen once data is filled in all fields of demographic and Eligibility request |
| **Visit 2 (Randomization)** | Subject Initial, Screening Number, Visit date, Randomization number (Autogenerated), Assigned Kit number (Autogenerated), Number of tablets dispensed in current visit (Autocalculated once dispense kit button is clicked) |
| **Visit 3** | Subject Initial, Screening Number, Visit Date, Kit dispensed in Visit 2 (Autoreflect data of visit 2), Number of tablets dispensed in Visit 2 (Autoreflect data of visit 2), Number of tablets returned, Number of tablets missed/disposed, Number of tablets dispensed in current visit (Autocalculated once dispense kit button is clicked) |
| | **Comment** - Add comment should be allowed in all visits except FUP (Physical-Telephonic) and End of study. PI, CRC, CRA and Medical Monitor should have rights to add/view comments. Added comments should be seen to each other |
| | **Documents Upload** - System should allow to upload documents in all visits except FUP (Physical-Telephonic). Upload documents should be allowed to CRC, PI, CRA, PM, Medical Monitor and Lab roles. System shall allow to download uploaded documents |

#### 5.12.6 Subject Matrix
Display of all subjects with:
- Screening Number
- Subject Initials
- Sites filter
- Subject Status filter
- Visit status indicators per visit
- Legend for status icons (Incomplete, Complete, Data Submitted, Query Generated by CRA, SDV, UnSDV, Query Generated by DM, PI Reviewed, Locked, Unlocked)

#### 5.12.7 Unscheduled Visit
- Add Unscheduled Visit functionality
- Fields: Unscheduled Visit Number, Visit Name, Date Of Unscheduled Visit, Reason For Unscheduled Visit, Select CRF (multi-select from available CRFs)

#### 5.12.8 Adverse Event Form
| Field | Description |
|-------|-------------|
| Adverse Event Term | Description of the event |
| Start date/time | When event started |
| Stop date/time | When event stopped |
| Ongoing | Yes/No |
| Severity | Severity level |
| Action Taken | Action taken for the event |
| Action Take Other (specify) | If other action |
| Relationship | Relationship to study drug |
| Outcome | Current outcome |
| Any Concomitant medication given? | Yes/No |
| If medication given, Specify the Sequence # no. (CM page) | Reference to CM |
| Is the Event Serious? | Yes/No |
| Seriousness Criteria | Criteria if serious |
| Comments regarding SAE | Additional comments |
| Date of Death | If applicable |
| Is an autopsy performed? | Yes/No |
| Reason for death | If applicable |

### 5.13 IP Management

#### 5.13.1 Inward IP
| Field | Description |
|-------|-------------|
| Batch No | Unique batch identifier |
| Study Arm | Test/Reference |
| Study Arm Detail | Drug description |
| Unit Type | Capsules, etc. |
| Number of Units | Quantity |
| Qty per Unit | Units per package |
| Total Qty | Total quantity |
| Mfg Date | Manufacturing date |
| Exp Date | Expiry date |
| Comments | Additional notes |

#### 5.13.2 IP Inventory
| Field | Description |
|-------|-------------|
| IP No | IP number |
| Batch No | Batch identifier |
| Site | Associated site |
| Kit Status | Dispensed to Patient, Damaged at Depot, Lost at Depot, Quarantined at Depot |
| Screening No. | If dispensed, associated screening number |
| Visit | Associated visit |
| Study Arm | Test/Reference |
| Expiry Date | Kit expiration date |
| Audit | Link to audit trail |

**Actions Available:**
- Show All IP Inventory
- Edit IP at Site
- Edit IP at Depot

#### 5.13.3 Delivery to Site
| Field | Description |
|-------|-------------|
| Shipment No. | Unique shipment identifier |
| Shipment Date | Date of shipment |
| Site | Destination site |
| IP Request # | Associated IP request |
| Available Test | Available test drug quantity |
| Available Reference | Available reference drug quantity |
| IP | Number of IPs |
| Test | Test drug quantity |
| Reference | Reference drug quantity |
| Comment | Additional notes |

#### 5.13.4 Received at Site
| Field | Description |
|-------|-------------|
| Shipment No. | Shipment identifier |
| Shipment Date | Date shipped |
| Site | Receiving site |
| Received Date | Date received |
| Status | Dispatched/Delivered |
| Review Status | Review state |
| Comments | Additional notes |

**Shipment Status Flow:**
```
Shipment Created → Dispatched → Delivered
```

| Status | Description |
|--------|-------------|
| Shipment Created | Shipment record created by Depot Manager |
| Dispatched | Shipment physically sent to site |
| Delivered | Shipment received and accepted at site |

#### 5.13.5 Delivery to Depot
- Return shipments from site to depot

#### 5.13.6 Received at Depot
| Field | Description |
|-------|-------------|
| Ship No | Shipment number |
| Shipment Date | Date of shipment |
| Site | Origin site |
| Received Date | Date received at depot |
| Status | Dispatched/Delivered |

#### 5.13.7 IP Request
| Field | Description |
|-------|-------------|
| IP Request # | Unique request identifier |
| Site | Requesting site |
| Request Date | Date of request |
| IP(s) | Number of IPs requested |
| PM Approved? | Approval status (Pending/Approved/Declined) |
| Request Status | Pending/Shipped |
| Shipment No. | Associated shipment if shipped |

**IP Request Detail:**
- IP Request #
- Site
- Request Date
- IP
- Total IP Quantity
- Expected Delivery Date
- Request Status (Open/Approved/Declined)

### 5.14 Reports

| Req ID | Description |
|--------|-------------|
| URS 13 | Data Extract |
| URS 14.1 | System must be able to generate reports based on Subject Visit and Kit dispensing status |
| URS 14.2 | System must allow authorized user to Export standard reports in XLS and PDF Format |

#### Available Reports (observed from demo):
| Report Category | Reports |
|-----------------|---------|
| Study Reports | Study Personnel Sheet |
| Data Reports | Data Extraction |
| Query Reports | Query Report, Query Report Details, Query Ageing Report, Query List Report, Static Pages - Query List Report |
| Visit Reports | Visit Status Report |
| Subject Reports | Subject Status Report |
| Access Reports | Login Attempts |
| Audit Reports | Audit Trail Report |
| CRF Reports | CRF Annotations, CRF Visit Percentage Report |
| IP Reports | Inventory at Site, Site Wise IP Status, Expired IP, Site wise Subject Summary, Visit wise Subject Summary, IP Dispensing Status, IP Reconciliation Report |
| Other Reports | Project Status Report, CRF Ageing Report, Randomization Status Report, Attached CRF Report, Enable/Disable List Report |

#### 5.14.1 Data Extraction
Multi-select interface for:
- Subjects (by screening number)
- Visits
- CRFs

Export Options:
- Download ACRF
- Download ECRF
- Download ACRF_ECRF
- Download PDF
- Request XLS

#### 5.14.2 Query Report
Filter by:
- Sites
- Subject Initial
- Screening No.
- Visit Title

Display Columns:
- Close Bulk Query
- Query#
- Site Name
- CRF Name
- Item Name
- Query Description
- Query Status (Open/Closed)
- Query Response
- Other Specify
- User Role
- Created By
- Created On

**Bulk Query Close:** Ability to close multiple queries at once

#### 5.14.3 Query Ageing Count Report
| Column | Description |
|--------|-------------|
| Form | CRF form name |
| Total number of queries raised | Total queries |
| Total no of queries opened from more than 2 days but less than 7 days | Aging category 1 |
| Total number of queries opened from more than 7 days but less than 10 days | Aging category 2 |
| Total number of queries opened from more than 10 days | Aging category 3 |

#### 5.14.4 Visit Status Report
Status tracking per visit by:
- Site
- Screening Number
- CRF Name
- Status (Incomplete/Complete)
- Actual Status

#### 5.14.5 Subject Status Report
| Column | Description |
|--------|-------------|
| Site Name | Site identifier |
| Screening Number | Subject screening number |
| Subject Initial | Subject initials |
| Subject Status | Screened/Screen Failure/Enrolled/Discontinued/Completed |
| VISIT 1 (SCREENING) | Visit 1 status |
| VISIT 2 (RANDOMIZATION) | Visit 2 status |
| VISIT 3 (EOT) | Visit 3 status |
| VISIT 4 (EOS) | Visit 4 status |
| Unscheduled Visit | Unscheduled visit status |
| Adverse Event Form | AE form status |
| Prior And Concomitant Medications | CM form status |
| Nprs/Par/Pgi-I | Assessment form status |

#### 5.14.6 Audit Trail Report
Filter by:
- Report Type (CRF Data Audit Trail, Study Creation, User Creation Details, Study Team, CRF Design - CRF, CRF Design - Fields, CRF Design - Enable Disable, CRF Design - Queries, CRF Design - LOV, CRF Design - LOV Detail, CRF Delete Data Audit Trail)
- Sites

Display Columns:
- CRF Short Name
- Label
- Old Value
- New Value
- Action (INSERT/UPDATE)
- Username
- Role Name
- Log Timestamp

### 5.15 Data Extract (URS 13)

| Req ID | Description |
|--------|-------------|
| URS 13.1 | The system must allow data extraction in CSV and Excel formats |
| URS 13.2 | The system must allow Study wise, Site wise, Subject wise data extraction |
| URS 13.3 | The system must allow CRF data extraction (CRF with annotation/ CRF with data/ Blank CRF) |
| URS 13.4 | The system should allow query management details extraction |
| URS 13.5 | System should have option to export multiple panels at single go instead of exporting datasets single panel wise |

**Export Formats:**
- ACRF (Annotated CRF)
- ECRF (Electronic CRF)
- ACRF_ECRF (Combined)
- PDF
- XLS (Excel)

---

## 6. User Roles and Responsibilities

### 6.1 EDC/IWRS Team (URS 12.1)
| Responsibility | Description |
|----------------|-------------|
| Setup the study in EDC/IWRS | Creates/defines roles and accessibility, users as per the study requirements. Get EDC/IWRS system ready for usage |
| Configure CRF, Visit Schedule, IP Workflow and Reasons | |
| Upload Randomization logic and Drug information | |

### 6.2 Site (Investigator PI/CRC) - (URS 12.2)
| Responsibility | Description |
|----------------|-------------|
| Acknowledge receipt of Drugs at site/Accept Shipment | |
| Screening of Subjects | |
| Randomizing the subjects using IWRS | |
| Assign IP/treatment IP as per the system generated kit numbers | |
| Track/registered visit dates | |
| Reporting of 'Screen Failure', Randomized, 'Discontinue' | |
| Reissue IPs | |
| Update damage, lost, Expired IP status | |
| CRC/PI shall be site specific | |
| Return unused IP to Depot | |

### 6.3 CRA (Clinical Research Associate) - (URS 12.3)
| Responsibility | Description |
|----------------|-------------|
| View only access of Subject management | |
| CRA shall have access of both EDC/IWRS CRF panels | Can raise manual queries on EDC panels only |
| CRA shall be Site Specific | |
| Generate Kit request for respective site | |
| Randomization/kit dispensed to subject | PM, CRA |
| Subject Discontinue | PM, CRA |
| TRF generation | PM, CRA, MM |

### 6.4 Project Manager - (URS 12.4)
| Responsibility | Description |
|----------------|-------------|
| View only access of Subject management | |
| PM shall have view access of both EDC/IWRS CRF panels | |
| Access of all sites | |
| Upload Study Documents in Study setup | |
| Enroll Sites, Manage Site Status (Site status - Active/Inactive, Screening/Randomization Cap size and Allocation size) | |
| Approve/Decline kit request | |
| Lab data upload functionality of EDC | |
| Reports Access - IP and Subject related reports | |

### 6.5 Depot Manager/Pharmacist - (URS 12.5)
| Responsibility | Description |
|----------------|-------------|
| No access of Subject management | |
| Access of all sites | |
| Create and dispatch Shipment requests to Site | |
| Dispatch IP to site | |
| Receive used and unused IP from site | |
| Update damage, lost, Expired IP status on depot | |
| Create Study Arm in study setup | |
| Create Kit Definition in study setup | |
| Generate Kit in study setup | |
| Add/Manage inventory in Inward | |
| Access of study arm | |
| Report access - IP related reports | |

### 6.6 Sponsor - (URS 12.7)
| Responsibility | Description |
|----------------|-------------|
| Sponsor shall have view access of both EDC/IWRS CRF panels | |
| Access of all sites | |

### 6.7 Data Manager
| Responsibility | Description |
|----------------|-------------|
| No access of IP management | |
| Data Manager shall have access of EDC CRF panels. It can raise manual queries on EDC panels | |
| IWRS panel should not be seen | |
| Access of all sites | |

### 6.8 Lab
| Responsibility | Description |
|----------------|-------------|
| No access of IP management and Subject management. Only Lab Tab should be seen | |
| Access of all sites | |

### 6.9 Medical Monitor - (URS 12.6)
| Responsibility | Description |
|----------------|-------------|
| Review eligibility request for randomization | |
| Approve/Reject/Send back randomization requests | |
| View access of both EDC/IWRS CRF panels | |
| Add comments on visits | |
| Access of all sites | |

### 6.10 Quality Assurance (QA)
| Responsibility | Description |
|----------------|-------------|
| Review CRF data after DM review | QA review step in workflow |
| Raise queries on EDC CRF panels | Query Generated by QA status |
| Access of all sites | Cross-site visibility for quality review |
| Generate QA reports | Quality metrics and audit reports |

### 6.11 Data Manager - (URS 12.8)
| Responsibility | Description |
|----------------|-------------|
| No access of IP management | Blinded to treatment allocation |
| Data Manager shall have access of EDC CRF panels | Can view and review EDC data |
| It can raise manual queries on EDC panels | Manual query generation capability |
| IWRS panel should not be seen | No visibility to randomization data |
| Access of all sites | Cross-site data management |

### 6.12 Lab - (URS 12.9)
| Responsibility | Description |
|----------------|-------------|
| No access of IP management and Subject management | Limited to lab functions only |
| Only Lab Tab should be seen | Restricted interface view |
| Access of all sites | Can view lab data across all sites |

**CRF Review Workflow by Role:**
```
CRC (Data Entry) → Complete → Data Submitted
                              ↓
CRA (Site Monitor) → SDV → Query Generated by CRA (if issues)
                              ↓
DM (Data Manager) → DM Reviewed → Query Generated by DM (if issues)
                              ↓
QA (Quality Assurance) → QA Reviewed → Query Generated by QA (if issues)
                              ↓
PI (Principal Investigator) → PI Reviewed
                              ↓
                           Locked
```

---

## 7. Backup and Recovery

### 7.1 Infrastructure Requirements
| Req ID | Description |
|--------|-------------|
| | Three instances (Development, Validation and Production) should be created and each instance should be having database and Application sections |
| | Two servers should be maintained at different locations |
| | Encrypted data back-up should be taken on daily basis. Same should be documented |

### 7.2 Data Recovery
This section specifies the requirements for how DataTrial data can be recovered in the case of non-catastrophic failure.

---

## 8. Issue Management

### 8.1 Helpdesk Integration
Helpdesk (feedback) application will be integrated with the DataTrial platform.

### 8.2 Issue E-form Fields
| Field | Description |
|-------|-------------|
| Ticket Number | Auto-generated unique identifier |
| Module | Affected module |
| Page | Affected page |
| Description | Issue description |
| Attachment | Supporting files |
| Severity | Issue severity level |

### 8.3 View Ticket Functionality
- View Ticket link seen on Home page
- Click on pen icon in Ticket report to edit ticket details and update:
  - Impact analysis
  - Change approval
  - Preventive action

### 8.4 Ticket Administration
DataTrial admin should have provision to:
- Edit ticket details
- Update Impact analysis
- Change approval
- Preventive action

---

## 9. Appendices

### Appendix A: Glossary

| Term | Definition |
|------|------------|
| EDC | Electronic Data Capture |
| IWRS | Interactive Web Response System |
| CRF | Case Report Form |
| CRA | Clinical Research Associate |
| CRC | Clinical Research Coordinator |
| DM | Data Manager |
| PI | Principal Investigator |
| PM | Project Manager |
| IP | Investigational Product |
| SDV | Source Data Verification |
| ICH | International Council for Harmonisation |
| GCP | Good Clinical Practice |
| 21 CFR Part 11 | FDA regulations for electronic records |
| EOT | End of Treatment |
| EOS | End of Study |
| FUP | Follow-Up |
| SAE | Serious Adverse Event |
| AE | Adverse Event |
| CM | Concomitant Medications |
| LOV | List of Values |
| QA | Quality Assurance |
| TRF | Test Requisition Form |
| LAR | Legally Authorized Representative |
| IW | Impartial Witness |
| ACRF | Annotated Case Report Form |
| ECRF | Electronic Case Report Form |
| BMI | Body Mass Index |
| ICF | Informed Consent Form |
| MM | Medical Monitor |
| UAT | User Acceptance Testing |
| IST | Indian Standard Time |

### Appendix B: CRF Status Icons Legend

| Icon | Status |
|------|--------|
| White/Empty | Incomplete |
| Green checkmark | Complete |
| Blue document | Data Submitted |
| Red circle | Query Generated by CRA |
| Blue checkmark | SDV |
| Red X | UnSDV |
| Red X (different) | Query Generated by DM |
| Green circle | PI Reviewed |
| Lock icon | Locked |
| Unlock icon | Unlocked |

### Appendix C: Query Status Colors

| Color | Status |
|-------|--------|
| Yellow | Open Query |
| Green | Responded Query |
| Blue | Closed Query |
| Orange | Reopened Query |

---

**Document End**

*This Functional Specification Document is based on the DataTrial EDC/IWRS User Requirements Specification and reference tool demonstration.*
