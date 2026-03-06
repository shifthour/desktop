# Understanding Document
# DataTrial EDC/IWRS System

---

**Document Version:** 1.1
**Date:** February 16, 2026
**Purpose:** To provide a clear, non-technical understanding of the DataTrial EDC/IWRS system for all stakeholders

**Revision History:**

| Version | Date | Description | Author |
|---------|------|-------------|--------|
| 1.0 | January 27, 2026 | Initial document | DataTrial Team |
| 1.1 | February 16, 2026 | Updated based on client review comments | DataTrial Team |

---

## 1. What is DataTrial EDC/IWRS?

DataTrial EDC/IWRS is a **web-based clinical trial management system** that combines two essential functions:

- **EDC (Electronic Data Capture):** Replaces paper-based data collection in clinical trials with electronic forms
- **IWRS (Interactive Web Response System):** Manages patient randomization and drug supply/distribution

Think of it as a **digital command center** for clinical trials that helps pharmaceutical companies, research organizations, and hospitals manage every aspect of testing new medicines on patients.

---

## 2. Why Do We Need This System?

### The Problem (Without EDC/IWRS):
- Paper forms get lost, damaged, or have illegible handwriting
- Data entry errors are common and hard to track
- Drug supplies at trial sites are managed manually (risk of shortages or expiry)
- No real-time visibility into trial progress
- Compliance with regulations is difficult to prove
- Query resolution takes weeks via mail/fax

### The Solution (With DataTrial):

**EDC:**
- Facilitate the collection, cleaning, and management of patient data in real-time, serving as the foundation for all clinical trial data.
- All data is captured electronically with validation
- Comprehensive Audit Trail
  - Single, unalterable trail tracks all actions
  - Complete history of trial demonstrated
  - Robust oversight to regulatory authorities
- Real-time dashboards and reports
- Built-in regulatory compliance (FDA 21 CFR Part 11), HIPAA, ICH GCP, FDA and GDPR
- External data import will reduce data entry time compared with traditional methods.
- Clinical Data Acquisition Standards Harmonization model for standardized collection.
- Intelligent data capture significantly reduces query rates by validating data at source, improving first-pass accuracy.
- Real-time query management and resolution
- Pull real-world patient data directly into trial databases, enhancing trial efficiency and patient outcomes.
- Accelerates database lock by enabling real-time data verification and automated quality checks.
- Inspection Readiness

**IWRS:**
- Manages patient enrollment, randomization, and drug supply logistics for efficient clinical trials
- Maintains Study Integrity
  - Ensures integrity of blinded studies.
- Optimizes Drug Supply
  - Tracks inventory for correct medication.
- Streamlined Processes
  - Centralized platform for trial data.

**RSDV:**
- Ensuring data integrity without the need for constant on-site visits, allowing for efficient, centralized monitoring of trial data.
- Key Benefits:
  - Significantly reduces travel costs and time associated with traditional monitoring.
  - Remote verification allows for quicker detection of data discrepancies or protocol deviations.
  - Enables focus on high-risk areas, improving overall monitoring effectiveness.

---

## 3. Who Uses This System?

### 3.1 User Roles Overview

| Role | What They Do | Access Level |
|------|--------------|--------------|
| **Site Staff (PI/CRC)** | Enter patient data, dispense drugs | Site-specific, Blinded/ UnBlinded |
| **CRA (Monitor)** | Review data quality, verify source documents | Site-specific, Blinded/ UnBlinded |
| **Project Manager** | Oversee trial setup, manage sites | All sites, Blinded/ UnBlinded |
| **Data Manager** | Review data, raise queries, ensure quality | All sites, Blinded/ UnBlinded |
| **Medical Monitor** | Approve patient eligibility for randomization | All sites, Blinded/ UnBlinded |
| **Depot Manager** | Manage drug inventory, create shipments | All sites, Unblinded |
| **Sponsor** | View trial progress and reports | All sites, Blinded/ UnBlinded |
| **Lab** | Upload and manage lab results | All sites, Blinded/ UnBlinded |
| **QA** | Quality review of data | All sites, Blinded/ UnBlinded |

### 3.2 Blinded vs Unblinded

- **Blinded Users:** Cannot see which treatment (test drug vs placebo or Reference) a patient receives
- **Unblinded Users:** Can see treatment assignments (Eg. Depot Manager for drug management)

---

## 4. Key Functions Explained

### 4.1 Study Setup (Before Trial Starts)

**What happens:**
1. EDC/IWRS team configures the study in the system
2. Creates electronic forms (CRFs) matching the paper protocol
3. Sets up visit schedule (Screening → Randomization → Treatment → Follow-up)
4. Configures drug arms (Test Drug vs Placebo)
5. Uploads randomization list (secret assignment of patients to treatments)
6. Enrolls trial sites

**Example:**
> A pharmaceutical company is testing a new diabetes drug. They set up a study with:
> - 10 hospital sites across India
> - 5 visits per patient over 3 months
> - 2:1 randomization (2 patients get test drug for every 1 getting placebo)
> - 15 electronic forms to capture vital signs, lab results, adverse events, etc.

---

### 4.2 Patient Journey Through the System

```
SCREENING (Visit 1)
    │
    ├── Site staff enters patient demographics
    ├── Records informed consent
    ├── Captures screening assessments (vitals, labs, ECG)
    ├── System checks eligibility criteria
    │
    ▼
ELIGIBILITY REVIEW
    │
    ├── Medical Monitor reviews patient data
    ├── Approves / Rejects / Sends back for correction
    │
    ▼
RANDOMIZATION (Visit 2)
    │
    ├── System assigns patient to treatment arm (blinded)
    ├── Generates unique randomization number
    ├── Assigns drug kit number
    ├── Site dispenses drug to patient
    │
    ▼
TREATMENT VISITS (Visit 3, 4, ...)
    │
    ├── Patient returns for follow-up
    ├── Site captures efficacy and safety data
    ├── Drug accountability (returned tablets counted)
    ├── New drug dispensed if needed
    │
    ▼
END OF STUDY
    │
    ├── Final assessments captured
    ├── Patient completes trial
    └── Data locked for analysis
```

---

### 4.3 Data Entry and Quality Control

**How data flows:**

1. **Site Staff Enters Data**
   - Opens patient's visit
   - Fills in electronic form (CRF)
   - Clicks "Save & Submit"

2. **System Validates Data**
   - Checks for missing fields
   - Validates ranges (e.g., blood pressure 60-200)
   - Fires "auto-queries" for out-of-range values

3. **CRA Reviews (SDV)**
   - Compares electronic data against source documents
   - Marks data as "Source Data Verified"
   - Raises manual queries if discrepancies found

4. **Data Manager Reviews**
   - Reviews all data for consistency
   - Raises cross-form queries
   - Ensures data is analysis-ready

5. **QA Reviews**
   - Final quality check
   - Ensures compliance with protocol

6. **PI Sign-off**
   - Principal Investigator electronically signs
   - Confirms data accuracy

7. **Data Lock**
   - No further changes allowed
   - Ready for statistical analysis

---

### 4.4 Query Management

**What is a Query?**
A query is a question or clarification request about data that seems incorrect or incomplete.

**Query Workflow:**
```
OPEN → RESPONDED → CLOSED
         ↓
      REOPENED (if response unsatisfactory)
```

**Types of Queries:**
| Type | Raised By | Example |
|------|-----------|---------|
| Auto Query | System | "Blood pressure 300/150 is out of range. Please verify." |
| Manual Query | CRA | "Consent date is after screening date. Please clarify." |
| DM Query | Data Manager | "Lab value conflicts with AE reported. Please reconcile." |

---

### 4.5 Drug (IP) Management

**The Drug Supply Chain:**

```
MANUFACTURER
    │
    ▼
CENTRAL DEPOT
    │
    ├── Depot Manager receives drugs (Inward)
    ├── Creates kits (Kit Generation)
    ├── Manages inventory
    │
    ▼
SHIPMENT TO SITE
    │
    ├── CRA requests kits for site
    ├── PM approves request
    ├── Depot creates shipment
    ├── Ships to site
    │
    ▼
SITE RECEIVES
    │
    ├── Site acknowledges receipt
    ├── Marks kits as: Received / Damaged / Quarantined
    │
    ▼
DISPENSED TO PATIENT
    │
    ├── System assigns kit based on randomization
    ├── Site dispenses to patient
    │
    ▼
ACCOUNTABILITY
    │
    ├── Patient returns unused tablets
    ├── Site records returned count
    └── Unused/damaged drugs returned to depot
```

**Drug Statuses:**
| Status | Meaning |
|--------|---------|
| Available | Ready for dispensing |
| Dispensed | Given to patient |
| Damaged | Cannot be used |
| Quarantined | Under investigation |
| Expired | Past expiry date |
| Destroyed | Disposed of |

---

### 4.6 Reports Available

| Report | Purpose | Who Uses It |
|--------|---------|-------------|
| Subject Status | Track patient progress through visits | ADMIN, PM, CRA |
| Visit Status | Monitor visit completion rates | ADMIN, PM, CRA |
| Query Report | Track open/closed queries | ADMIN, DM, CRA |
| Query Ageing | Identify long-pending queries | ADMIN, DM |
| Audit Trail | Complete history of all changes | ADMIN, QA, Auditors |
| IP Inventory | Drug stock at each site | ADMIN, PM, Depot |
| Data Extraction | Export data for analysis | ADMIN, DM, Statistician |

---

## 5. Regulatory Compliance

### 5.1 FDA 21 CFR Part 11

This US regulation ensures electronic records are trustworthy. DataTrial complies by providing:

- **Audit Trail:** Every change is logged with who, what, when, why
- **Electronic Signatures:** Legally binding digital signatures
- **Access Controls:** Role-based permissions
- **Data Integrity:** Validation and encryption

### 5.2 ICH-GCP (Good Clinical Practice)

International standard for ethical clinical trials. DataTrial supports:

- Informed consent documentation
- Protocol compliance checks
- Source data verification
- Safety reporting

---

## 6. Security Features

| Feature | Description |
|---------|-------------|
| Password Policy | Minimum length, complexity, expiry, history |
| Account Lockout | 3 failed attempts = locked account |
| Session Timeout | Auto-logout after inactivity |
| 64-bit Encryption | All data encrypted in transit and at rest |
| Role-Based Access | Users see only what they need |
| Audit Logging | Every action is recorded |

---

## 7. Key Benefits Summary

| Stakeholder | Benefits |
|-------------|----------|
| **Sponsor** | Real-time visibility, faster trials, reduced costs |
| **Sites** | Less paperwork, guided data entry, instant query resolution |
| **Patients** | Safer trials, better drug accountability |
| **Regulators** | Complete audit trail, compliance documentation |
| **Data Managers** | Clean data, automated checks, efficient review |

---

## 8. Glossary of Terms

| Term | Meaning |
|------|---------|
| CRF | Case Report Form - electronic form for data entry |
| SDV | Source Data Verification - comparing e-data to paper source |
| IP | Investigational Product - the drug being tested |
| Randomization | Random assignment of patients to treatment groups |
| Blinding | Keeping treatment assignment secret |
| Query | Question about data accuracy |
| Audit Trail | Log of all system activities |
| e-Signature | Electronic equivalent of handwritten signature |
| Protocol | The "recipe" for how the trial should be conducted |
| ICF | Informed Consent Form - patient agrees to participate |

---

## 9. System Access

- **URL:** Provided by DataTrial team
- **Browser:** Chrome, Firefox, Edge, Safari (latest versions)
- **Internet:** Minimum 512 Kbps
- **Training:** Required before access is granted

---

*This document provides a conceptual overview. For detailed functional requirements, refer to the Functional Specification Document (FSD).*
