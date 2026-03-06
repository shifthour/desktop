# Functional Specification Document
## AI-First Tech Stack for Intercity Bus Operator

**Document Version:** 1.0
**Date:** December 2024
**Status:** Draft
**Author:** Technical Team
**Stakeholders:** Operations, IT, Finance, Customer Service, Maintenance, HR, Commercial

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [System Overview](#2-system-overview)
3. [User Roles & Personas](#3-user-roles--personas)
4. [Functional Requirements by Module](#4-functional-requirements-by-module)
5. [Agent Specifications](#5-agent-specifications)
6. [User Interface Requirements](#6-user-interface-requirements)
7. [Integration Requirements](#7-integration-requirements)
8. [Reporting & Analytics Requirements](#8-reporting--analytics-requirements)
9. [Notification & Communication Requirements](#9-notification--communication-requirements)
10. [Security & Access Control](#10-security--access-control)
11. [Business Rules](#11-business-rules)
12. [Acceptance Criteria](#12-acceptance-criteria)
13. [Appendices](#13-appendices)

---

## 1. Introduction

### 1.1 Purpose

This Functional Specification Document (FSD) defines the functional requirements for an AI-powered operations platform designed for a 100-bus multi-state intercity bus operator. The document describes what the system will do from a user and business perspective.

### 1.2 Scope

The system covers the following operational areas:

| Area | In Scope | Out of Scope |
|------|----------|--------------|
| Central Operations & Control Room | ✓ | |
| Depot Operations & Dispatch | ✓ | |
| Fleet Maintenance | ✓ | |
| Safety & Compliance | ✓ | |
| Customer Service | ✓ | |
| Revenue Management | ✓ | |
| Sales & Distribution | ✓ | |
| Finance & Reconciliation | ✓ | |
| HR & Rostering | ✓ | |
| Ticketing System (Bitla) | Integration only | Core ticketing changes |
| GPS/Telematics | Integration only | Hardware changes |
| Payment Processing | Integration only | Payment gateway changes |

### 1.3 Definitions & Abbreviations

| Term | Definition |
|------|------------|
| Agent | An AI-powered assistant that can take actions autonomously |
| Supervisor Agent | The central AI that routes requests to specialist agents |
| HITL | Human-in-the-Loop - requiring human approval for actions |
| Trip | A single scheduled bus journey from origin to destination |
| Service | A route with defined schedule (e.g., HYD-BLR 10PM Sleeper) |
| Depot | Physical location where buses are stationed and dispatched |
| OTA | Online Travel Agent (RedBus, MakeMyTrip, etc.) |
| DTC | Diagnostic Trouble Code from vehicle telematics |
| Job Card | A maintenance work order |

### 1.4 Document Conventions

- **SHALL** - Mandatory requirement
- **SHOULD** - Recommended requirement
- **MAY** - Optional requirement
- **[Pn]** - Priority level (P1 = Critical, P2 = High, P3 = Medium, P4 = Low)

---

## 2. System Overview

### 2.1 System Context

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL SYSTEMS                                  │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │
│  │  Bitla  │ │  GPS    │ │  OTAs   │ │ Payment │ │ WhatsApp│            │
│  │Ticketing│ │Provider │ │(RedBus) │ │Gateways │ │Business │            │
│  └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘            │
└───────┼──────────┼──────────┼──────────┼──────────┼─────────────────────┘
        │          │          │          │          │
        ▼          ▼          ▼          ▼          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                                                                          │
│                    AI-POWERED OPERATIONS PLATFORM                        │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                    AGENTIC AI LAYER                                │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ │  │
│  │  │   Ops    │ │  Maint   │ │    CS    │ │ Revenue  │ │ Finance  │ │  │
│  │  │  Agent   │ │  Agent   │ │  Agent   │ │  Agent   │ │  Agent   │ │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └──────────┘ │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
        │          │          │          │          │
        ▼          ▼          ▼          ▼          ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                            USERS                                          │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐            │
│  │ Control │ │  Depot  │ │Workshop │ │  CS     │ │  Mgmt   │            │
│  │  Room   │ │Dispatch │ │  Team   │ │  Team   │ │ Leaders │            │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘            │
└──────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Key Capabilities

| Capability | Description |
|------------|-------------|
| **Intelligent Automation** | AI agents that can take actions across systems |
| **Predictive Analytics** | Forecast breakdowns, demand, delays before they occur |
| **Real-time Monitoring** | Live tracking of fleet, operations, and KPIs |
| **Natural Language Interface** | Conversational interaction for staff and passengers |
| **Cross-System Orchestration** | Coordinate actions across multiple departments |
| **Decision Support** | AI recommendations with human approval workflows |

### 2.3 System Goals

| Goal | Metric | Target |
|------|--------|--------|
| Reduce breakdown-related cancellations | % reduction | 20-30% |
| Faster disruption response | Response time | 30-40% faster |
| Automate customer service queries | % Tier-1 automation | 50-70% |
| Reduce maintenance downtime | Downtime per bus | 10-15% reduction |
| Improve load factor on weak routes | Occupancy % | 5-8% improvement |
| Accelerate financial close | Days to close | 50% reduction |

---

## 3. User Roles & Personas

### 3.1 User Role Matrix

| Role | Department | System Access | Primary Functions |
|------|------------|---------------|-------------------|
| Control Room Operator | Central Ops | Full ops dashboard | Monitor fleet, handle disruptions |
| Control Room Supervisor | Central Ops | Full ops + approvals | Approve diversions, escalations |
| Network Planner | Central Ops | Planning module | Create schedules, routes |
| Depot Manager | Depot Ops | Depot dashboard | Oversee depot operations |
| Dispatcher | Depot Ops | Dispatch module | Assign buses, crews, departures |
| Workshop Manager | Maintenance | Maintenance dashboard | Oversee repairs, maintenance |
| Mechanic | Maintenance | Mobile app | View job cards, update status |
| Safety Officer | Safety | Safety dashboard | Monitor compliance, incidents |
| CS Agent | Customer Service | CS console | Handle customer queries |
| CS Supervisor | Customer Service | CS console + reports | Escalations, team management |
| Revenue Manager | Commercial | Revenue dashboard | Pricing, route analysis |
| Sales Manager | Sales | Sales dashboard | Channel performance, targets |
| Finance Manager | Finance | Finance dashboard | Reconciliation, approvals |
| Accountant | Finance | Finance module | Daily reconciliation |
| HR Manager | HR | HR dashboard | Rostering, compliance |
| IT Admin | IT | Admin console | System configuration |
| Executive | Management | Executive dashboard | KPIs, reports |
| Passenger | External | WhatsApp/App | Booking queries, support |

### 3.2 Persona Details

#### 3.2.1 Control Room Operator - "Ravi"

**Background:**
- Age: 28-40
- Experience: 3-8 years in transport operations
- Works in rotating shifts (8 hours)
- Monitors 100 buses across 4-6 states

**Goals:**
- Quickly identify and respond to disruptions
- Minimize passenger impact from delays
- Keep buses on schedule
- Reduce manual tracking effort

**Pain Points:**
- Juggling multiple screens and phone calls
- Late information about breakdowns
- No single view of fleet status
- Manual communication with depots

**System Needs:**
- Single dashboard with live fleet view
- Automatic delay/breakdown alerts
- AI-suggested responses to disruptions
- One-click communication to passengers and depots

---

#### 3.2.2 Dispatcher - "Lakshmi"

**Background:**
- Age: 25-45
- Experience: 2-10 years
- Works at depot level
- Handles 15-25 buses daily

**Goals:**
- Ensure on-time departures
- Correct bus-crew allocation
- Compliance with regulations
- Minimize last-minute scrambling

**Pain Points:**
- Manual checks for permits, fitness, crew hours
- Last-minute crew absences
- Wrong bus type allocations
- Paper-based checklists

**System Needs:**
- Digital pre-departure checklist
- Automatic validation of compliance
- Crew availability view
- Alerts for missing requirements

---

#### 3.2.3 Customer Service Agent - "Priya"

**Background:**
- Age: 22-35
- Handles 80-120 queries per shift
- Multi-channel (calls, WhatsApp, email)
- High pressure during disruptions

**Goals:**
- Resolve queries quickly
- Accurate information to passengers
- Process refunds correctly
- Maintain customer satisfaction

**Pain Points:**
- Repetitive booking status queries
- Manual lookup across systems
- Inconsistent refund decisions
- No proactive delay information

**System Needs:**
- AI bot handling routine queries
- Single view of booking + trip status
- Automatic refund calculation
- Proactive delay notifications to passengers

---

#### 3.2.4 Workshop Manager - "Suresh"

**Background:**
- Age: 35-50
- Technical background
- Manages 8-15 mechanics
- Responsible for 30-40 buses at workshop

**Goals:**
- Minimize bus downtime
- Prevent on-road breakdowns
- Optimize parts inventory
- Meet fitness/compliance deadlines

**Pain Points:**
- Reactive maintenance (fix after breakdown)
- No visibility into bus health
- Manual job card creation
- Parts stockouts

**System Needs:**
- Predictive maintenance alerts
- Automatic job card creation
- Parts demand forecasting
- Mechanic workload view

---

#### 3.2.5 Passenger - "Anjali"

**Background:**
- Age: 20-55
- Books 2-10 trips per year
- Uses WhatsApp frequently
- Expects quick responses

**Goals:**
- Easy booking and changes
- Real-time trip information
- Quick refunds when eligible
- Help during disruptions

**Pain Points:**
- Long wait times for support
- No proactive delay updates
- Complicated refund process
- Inconsistent information

**System Needs:**
- 24/7 WhatsApp support
- Proactive delay notifications
- Easy reschedule/refund
- Real-time bus tracking link

---

## 4. Functional Requirements by Module

### 4.1 Central Operations & Control Room Module

#### FR-OPS-001: Real-Time Fleet Dashboard [P1]

**Description:** The system SHALL provide a real-time map-based view of all buses in the fleet.

**Functional Details:**
| Feature | Requirement |
|---------|-------------|
| Map View | Display all buses on interactive map |
| Bus Status | Show running/stopped/delayed/breakdown status |
| Update Frequency | Refresh every 30 seconds |
| Filters | Filter by route, state, status, depot |
| Bus Details | Click bus to see details (speed, ETA, crew, passengers) |
| Historical Playback | View past 24 hours of any bus movement |

**User Story:**
> As a Control Room Operator, I want to see all buses on a live map so that I can quickly identify any issues.

**Acceptance Criteria:**
- [ ] Map loads within 3 seconds
- [ ] All 100 buses visible with correct positions
- [ ] Status colors clearly distinguish running/delayed/breakdown
- [ ] Click on bus shows popup with key details
- [ ] Filter changes reflect within 1 second

---

#### FR-OPS-002: Delay Detection & Alerting [P1]

**Description:** The system SHALL automatically detect delays and alert operators.

**Functional Details:**
| Feature | Requirement |
|---------|-------------|
| Detection | Identify when bus is >15 min behind schedule |
| Prediction | Predict final delay based on current position + historical patterns |
| Alert | Push notification to Control Room dashboard |
| Escalation | Auto-escalate if delay >45 min |
| Threshold Config | Configurable delay thresholds per route |

**Business Rules:**
- BR-OPS-001: Minor delay = 15-30 min behind schedule
- BR-OPS-002: Major delay = 30-60 min behind schedule
- BR-OPS-003: Critical delay = >60 min behind schedule

**User Story:**
> As a Control Room Operator, I want to be automatically alerted when a bus is delayed so that I can take proactive action.

---

#### FR-OPS-003: AI-Powered Disruption Response [P1]

**Description:** The system SHALL provide AI-suggested responses for disruptions.

**Functional Details:**
| Disruption Type | AI Suggestions |
|-----------------|----------------|
| Delay (traffic) | ETA update, passenger notification, connection handling |
| Breakdown | Nearest spare bus, mechanic dispatch, passenger rebooking options |
| Accident | Emergency contacts, nearest hospital, incident logging |
| Crew Issue | Available standby crew, alternate crew from nearby bus |
| Road Block | Alternate routes, delay estimation, passenger communication |

**Workflow:**
```
Disruption Detected → Ops Agent Analyzes → Generates Options →
→ Operator Reviews → Approves/Modifies → System Executes
```

**User Story:**
> As a Control Room Operator, I want AI to suggest response options during disruptions so that I can respond faster and more consistently.

---

#### FR-OPS-004: Daily Operations Brief [P2]

**Description:** The system SHALL generate a daily AI-written operations summary.

**Functional Details:**
| Content | Description |
|---------|-------------|
| Performance Summary | On-time %, cancellations, delays |
| Incident Analysis | What went wrong and why |
| Trends | Comparison vs last week/month |
| Recommendations | AI suggestions for improvement |
| Delivery | Email at 6 AM, dashboard widget |

**User Story:**
> As an Ops Manager, I want a daily brief explaining operational performance so that I can focus on areas needing attention.

---

#### FR-OPS-005: Trip Schedule Management [P2]

**Description:** The system SHALL allow creation and modification of trip schedules.

**Functional Details:**
- Create new services/routes
- Set schedules (departure time, frequency, days of operation)
- Assign bus type requirements
- Set crew requirements
- Manage seasonal schedule variations
- Bulk schedule updates

---

### 4.2 Depot Operations Module

#### FR-DEP-001: Pre-Departure Checklist [P1]

**Description:** The system SHALL provide a digital pre-departure validation checklist.

**Checklist Items:**
| Check | Validation |
|-------|------------|
| Bus Fitness | Valid fitness certificate (not expired) |
| Insurance | Valid insurance (not expired) |
| Permits | Valid permits for route states |
| Driver License | Valid license, correct category |
| Driver Hours | Not exceeding legal driving hours |
| Driver Rest | Minimum rest period completed |
| Bus Type | Correct bus type for service |
| Fuel Level | Minimum 50% tank |
| Cleanliness | Cleaning completed (manual confirm) |

**Workflow:**
```
30 min before departure → System runs auto-checks →
→ Displays pass/fail for each item →
→ Dispatcher confirms manual items →
→ Green light for departure OR blocks with reason
```

**User Story:**
> As a Dispatcher, I want automatic validation of all departure requirements so that I don't miss any compliance issue.

---

#### FR-DEP-002: Bus-Crew Allocation [P1]

**Description:** The system SHALL facilitate optimal bus and crew allocation.

**Functional Details:**
| Feature | Requirement |
|---------|-------------|
| Crew Roster View | Show available drivers/attendants for shift |
| Auto-Suggestion | AI suggests optimal allocation based on rules |
| Conflict Detection | Alert if allocation violates rules |
| Manual Override | Allow manual changes with reason |
| Swap Management | Handle last-minute crew swaps |

**Business Rules:**
- BR-DEP-001: Driver must have valid license for bus category
- BR-DEP-002: Driver must not exceed 8 hours continuous driving
- BR-DEP-003: Driver must have 10 hours rest before duty
- BR-DEP-004: Sleeper services require 2 drivers for >500km

---

#### FR-DEP-003: Departure/Arrival Logging [P2]

**Description:** The system SHALL log actual departure and arrival times.

**Functional Details:**
- Auto-capture departure when bus crosses geofence
- Manual departure confirmation by dispatcher
- Auto-capture arrival at destination depot
- Variance tracking vs scheduled time
- Integration with passenger notifications

---

#### FR-DEP-004: Incident Logging [P2]

**Description:** The system SHALL enable quick incident logging with AI assistance.

**Incident Types:**
- Breakdown
- Accident
- Passenger complaint
- Crew issue
- Route deviation
- Delay (with reason)

**AI Assistance:**
- Auto-populate known details (bus, crew, location, time)
- Suggest incident category
- Auto-notify relevant stakeholders
- Create follow-up tasks

---

### 4.3 Fleet Maintenance Module

#### FR-MNT-001: Predictive Maintenance Alerts [P1]

**Description:** The system SHALL predict maintenance needs before failures occur.

**Prediction Categories:**
| Category | Signals Used | Prediction Horizon |
|----------|--------------|-------------------|
| Engine | DTC codes, oil pressure, temperature, km run | 7-14 days |
| Brakes | Brake wear sensors, km since replacement | 14-30 days |
| AC/HVAC | Temperature variance, compressor data | 7-14 days |
| Battery | Voltage patterns, age, cold start performance | 7-30 days |
| Tyres | Tread depth, km run, pressure patterns | 14-30 days |

**Alert Levels:**
| Level | Description | Action |
|-------|-------------|--------|
| Watch | Potential issue in 2-4 weeks | Schedule inspection |
| Warning | Likely issue in 1-2 weeks | Schedule maintenance |
| Critical | Imminent failure risk | Pull from service |

**User Story:**
> As a Workshop Manager, I want to know which buses are likely to fail soon so that I can fix them before they break down on road.

---

#### FR-MNT-002: Automated Job Card Creation [P1]

**Description:** The system SHALL auto-generate job cards for predicted and reported issues.

**Job Card Contents:**
- Bus details (number, type, depot)
- Issue description
- Predicted/reported cause
- Recommended actions (AI-generated)
- Required parts (AI-suggested)
- Estimated labor hours
- Priority level
- Assigned mechanic (suggested)

**Workflow:**
```
Prediction Alert OR Breakdown Report →
→ Maintenance Agent creates draft job card →
→ Workshop Manager reviews/approves →
→ Assigned to mechanic → Work begins
```

---

#### FR-MNT-003: Mechanic Mobile App [P2]

**Description:** The system SHALL provide mechanics with a mobile interface.

**Features:**
| Feature | Description |
|---------|-------------|
| Job Queue | View assigned job cards |
| Job Details | See issue, parts, instructions |
| Status Update | Mark in-progress, completed |
| Parts Request | Request parts from store |
| Photo Upload | Document before/after |
| AI Assistant | Ask questions about repairs (RAG over manuals) |
| Time Logging | Track labor hours |

**User Story:**
> As a Mechanic, I want to see my jobs on my phone and get AI help for complex repairs so that I can work more efficiently.

---

#### FR-MNT-004: Parts Inventory Management [P2]

**Description:** The system SHALL manage parts inventory with demand forecasting.

**Features:**
- Current stock levels by depot/central
- Minimum stock alerts
- AI demand forecasting based on:
  - Scheduled maintenance
  - Predicted failures
  - Historical consumption
- Auto-generate purchase orders
- Vendor management

---

#### FR-MNT-005: Tyre Lifecycle Management [P3]

**Description:** The system SHALL track tyre lifecycle and optimize replacement.

**Features:**
- Individual tyre tracking (serial number)
- Position history (which wheel, which bus)
- Km run per tyre
- Tread depth tracking
- Rotation recommendations
- Retread tracking
- Cost per km analysis

---

### 4.4 Safety & Compliance Module

#### FR-SAF-001: Driver Behavior Scoring [P1]

**Description:** The system SHALL score driver behavior based on telematics.

**Scoring Factors:**
| Factor | Weight | Measurement |
|--------|--------|-------------|
| Speeding Events | 25% | Instances exceeding limit by >10% |
| Harsh Braking | 20% | Deceleration >0.4g |
| Harsh Acceleration | 15% | Acceleration >0.3g |
| Sharp Turns | 15% | Lateral g-force >0.3g |
| Night Driving Violations | 15% | Driving during restricted hours |
| Rest Period Violations | 10% | Insufficient rest between duties |

**Score Ranges:**
| Score | Category | Action |
|-------|----------|--------|
| 90-100 | Excellent | Recognition, incentives |
| 75-89 | Good | No action |
| 60-74 | Needs Improvement | Coaching recommended |
| Below 60 | At Risk | Mandatory retraining |

**User Story:**
> As a Safety Officer, I want to identify high-risk drivers automatically so that I can intervene before accidents happen.

---

#### FR-SAF-002: Compliance Tracking [P1]

**Description:** The system SHALL track and alert on compliance deadlines.

**Tracked Items:**
| Item | Renewal Frequency | Alert Lead Time |
|------|-------------------|-----------------|
| Fitness Certificate | Annual | 30, 15, 7 days |
| Insurance | Annual | 30, 15, 7 days |
| State Permits | Varies by state | 30, 15, 7 days |
| Pollution Certificate | 6 months | 15, 7 days |
| Driver License | As per validity | 60, 30, 15 days |
| Driver Medical | Annual | 30, 15, 7 days |

**User Story:**
> As a Compliance Manager, I want automatic alerts for expiring documents so that we never operate non-compliant.

---

#### FR-SAF-003: Incident Investigation Support [P2]

**Description:** The system SHALL assist in incident investigation with AI.

**AI Assistance:**
- Auto-gather relevant data (GPS trail, speed, driver history, bus maintenance history)
- Generate draft RCA (Root Cause Analysis)
- Suggest contributing factors
- Recommend corrective actions
- Track corrective action completion

---

### 4.5 Customer Service Module

#### FR-CS-001: AI Customer Support Bot [P1]

**Description:** The system SHALL provide 24/7 AI-powered customer support via WhatsApp and other channels.

**Supported Queries:**
| Query Type | Bot Capability |
|------------|----------------|
| Booking Status | Retrieve and explain booking details |
| PNR Lookup | Find booking by PNR |
| Bus Location | Real-time location with ETA |
| Reschedule Request | Process reschedule per policy |
| Cancellation Request | Process cancellation, calculate refund |
| Refund Status | Check refund processing status |
| Boarding Point Info | Provide exact location, landmarks |
| Amenities Query | Explain bus amenities |
| Luggage Policy | Explain luggage rules |
| General FAQs | Answer common questions |

**Languages:**
- Phase 1: English, Hindi
- Phase 2: Telugu, Kannada
- Phase 3: Tamil, Marathi, others

**User Story:**
> As a Passenger, I want to get instant answers to my questions on WhatsApp 24/7 so that I don't have to wait for business hours.

---

#### FR-CS-002: Proactive Delay Notification [P1]

**Description:** The system SHALL proactively notify passengers of delays.

**Notification Triggers:**
| Trigger | Notification Content |
|---------|---------------------|
| Delay >20 min predicted | "Your bus is running approximately X min late. New ETA: Y" |
| Delay >45 min | Above + options (wait, reschedule, refund) |
| Breakdown | Immediate notification + resolution timeline |
| Trip Cancelled | Notification + automatic refund/rebooking options |

**Channels:**
- WhatsApp (primary)
- SMS (fallback)
- App push notification (if app installed)

**User Story:**
> As a Passenger, I want to be notified of delays before I reach the boarding point so that I can plan accordingly.

---

#### FR-CS-003: Automated Refund Processing [P1]

**Description:** The system SHALL automatically calculate and process eligible refunds.

**Refund Rules Engine:**
| Cancellation Time | Refund % | Processing |
|-------------------|----------|------------|
| >48 hours before | 90% | Auto-approve |
| 24-48 hours before | 75% | Auto-approve |
| 12-24 hours before | 50% | Auto-approve |
| 6-12 hours before | 25% | Auto-approve |
| <6 hours before | 0% | Auto-reject with explanation |
| Operator cancellation | 100% | Auto-approve |
| Delay >2 hours | 100% | Auto-approve |

**Special Cases (require human review):**
- Medical emergencies (with documentation)
- Death in family (with documentation)
- Natural disasters
- Disputes on delay duration

**User Story:**
> As a Passenger, I want my refund processed automatically based on policy so that I don't have to wait for manual processing.

---

#### FR-CS-004: Human Handoff [P2]

**Description:** The system SHALL seamlessly hand off complex queries to human agents.

**Handoff Triggers:**
- Passenger requests human agent
- Bot confidence score below threshold
- Query type not in bot capability
- Passenger expresses frustration (sentiment detection)
- Third failed attempt to resolve

**Handoff Includes:**
- Full conversation history
- Booking details
- Trip status
- Passenger history
- Bot's assessment of issue

---

#### FR-CS-005: Agent Assist Dashboard [P2]

**Description:** The system SHALL provide human CS agents with AI-powered assistance.

**Features:**
| Feature | Description |
|---------|-------------|
| Single Customer View | All bookings, history, interactions |
| Response Suggestions | AI-suggested responses |
| Knowledge Base Search | Quick access to policies, FAQs |
| Refund Calculator | Instant refund calculation |
| Escalation Workflow | One-click escalation with context |
| Post-Call Summary | AI-generated call summary |

---

### 4.6 Revenue Management Module

#### FR-REV-001: Demand Forecasting [P1]

**Description:** The system SHALL forecast demand by route, date, and service type.

**Forecast Dimensions:**
- Route (origin-destination pair)
- Date (specific date)
- Day of week
- Service type (sleeper/seater/AC/non-AC)
- Booking lead time

**Inputs:**
- Historical booking data
- Seasonal patterns
- Festival/holiday calendar
- Events (concerts, exams, etc.)
- Competitor pricing (if available)

**Output:**
- Predicted bookings per service
- Confidence interval
- Comparison vs same period last year

---

#### FR-REV-002: Dynamic Pricing Recommendations [P1]

**Description:** The system SHALL recommend optimal pricing based on demand.

**Pricing Factors:**
| Factor | Impact |
|--------|--------|
| Current booking pace | Fast pace → increase price |
| Days to departure | Closer → adjust based on occupancy |
| Current occupancy | Low → decrease or promote |
| Competitor pricing | Benchmark against alternatives |
| Historical demand | Seasonal adjustments |
| Customer segment | Corporate vs retail |

**Pricing Rules:**
- BR-REV-001: Price cannot exceed 2x base fare
- BR-REV-002: Price cannot go below 0.7x base fare
- BR-REV-003: Price changes limited to 3 per day per service
- BR-REV-004: Booked passengers unaffected by price changes

**Workflow:**
```
Revenue Agent analyzes demand →
→ Generates pricing recommendation →
→ Revenue Manager reviews →
→ Approves/Modifies → System updates prices
```

---

#### FR-REV-003: Route Profitability Analysis [P2]

**Description:** The system SHALL provide route-level profitability analysis.

**Metrics per Route:**
| Metric | Description |
|--------|-------------|
| Revenue per km | Total revenue / total km |
| Cost per km | (Fuel + toll + maintenance + crew) / km |
| Load Factor | Actual passengers / capacity |
| Yield per seat-km | Revenue / (seats × km) |
| Cancellation Rate | Cancelled trips / scheduled trips |
| On-time Performance | On-time arrivals / total arrivals |
| Profit/Loss | Revenue - direct costs |

**AI Insights:**
- Why is route underperforming?
- Recommended actions (pricing, schedule, marketing)
- Comparison with similar routes

---

#### FR-REV-004: Promotion Management [P3]

**Description:** The system SHALL support targeted promotions.

**Promotion Types:**
- Flat discount (₹X off)
- Percentage discount (X% off)
- Route-specific offers
- Time-based offers (weekday special)
- Customer segment offers (first-time, loyalty)
- Bundle offers (round trip)

**AI Recommendations:**
- Which routes need promotions
- Optimal discount level
- Target customer segments
- Expected impact on bookings

---

### 4.7 Sales & Distribution Module

#### FR-SAL-001: Channel Performance Dashboard [P2]

**Description:** The system SHALL provide visibility into sales channel performance.

**Channels Tracked:**
- Own website/app
- OTAs (RedBus, MakeMyTrip, AbhiBus, etc.)
- Counters (own)
- Agents (third-party)
- Corporate

**Metrics per Channel:**
| Metric | Description |
|--------|-------------|
| Bookings | Number of bookings |
| Revenue | Total revenue |
| Commission | Commission paid/payable |
| Cancellation Rate | Cancellations / bookings |
| Refund Rate | Refunds / revenue |
| Net Revenue | Revenue - commission - refunds |

---

#### FR-SAL-002: Agent Reconciliation [P2]

**Description:** The system SHALL automate agent/OTA reconciliation.

**Reconciliation Process:**
```
Daily at EOD:
1. Fetch bookings from agent/OTA
2. Match with internal records
3. Calculate commission due
4. Identify discrepancies
5. Generate reconciliation report
6. Flag anomalies for review
```

**Anomaly Detection:**
- Missing bookings (in agent, not in system)
- Commission miscalculation
- Unusual refund patterns
- Duplicate transactions

---

#### FR-SAL-003: Corporate Sales Management [P3]

**Description:** The system SHALL support corporate sales workflow.

**Features:**
- Corporate account management
- Contract terms storage
- Credit limit management
- Bulk booking facility
- Corporate pricing tiers
- Invoice generation
- Payment tracking

---

### 4.8 Finance & Reconciliation Module

#### FR-FIN-001: Daily Cash Reconciliation [P1]

**Description:** The system SHALL automate daily cash reconciliation.

**Sources to Reconcile:**
| Source | Data |
|--------|------|
| Bitla | Booking transactions |
| Payment Gateways | Online payments |
| Cash Collections | Counter collections |
| Agent Settlements | Agent payments |
| Refunds Issued | Refund transactions |

**Process:**
```
Morning (6 AM):
1. Fetch all transactions from previous day
2. Auto-match transactions across sources
3. Identify unmatched items
4. Generate reconciliation report
5. Flag discrepancies >₹500 for review
```

**User Story:**
> As an Accountant, I want reconciliation done automatically overnight so that I can focus on exceptions only.

---

#### FR-FIN-002: Anomaly Detection [P1]

**Description:** The system SHALL detect financial anomalies automatically.

**Anomaly Types:**
| Anomaly | Detection Logic |
|---------|-----------------|
| Unusual refund pattern | Refunds >2 std dev from normal for agent/route |
| Cash shortage | Cash collected < expected |
| Duplicate payments | Same amount, same booking, multiple times |
| Commission variance | Commission claimed ≠ commission calculated |
| Revenue leakage | Bookings without payment |
| Fuel anomaly | Fuel cost/km >threshold |
| Toll anomaly | Toll charges inconsistent with route |

---

#### FR-FIN-003: Route/State P&L [P2]

**Description:** The system SHALL generate profit & loss by route and state.

**P&L Structure:**
```
REVENUE
  + Ticket Sales
  + Parcel Revenue
  + Other Income
  = TOTAL REVENUE

DIRECT COSTS
  - Fuel
  - Tolls
  - Driver Wages
  - Attendant Wages
  - Commission (OTA/Agent)
  = TOTAL DIRECT COSTS

CONTRIBUTION MARGIN = Revenue - Direct Costs

ALLOCATED COSTS
  - Maintenance (per km allocation)
  - Depreciation
  - Insurance
  - Permits & Taxes
  = TOTAL ALLOCATED COSTS

ROUTE PROFIT/LOSS = Contribution Margin - Allocated Costs
```

---

#### FR-FIN-004: AI Financial Narratives [P3]

**Description:** The system SHALL generate AI-written financial commentary.

**Content:**
- Why did revenue change vs last period?
- Which routes drove profit/loss?
- Cost anomalies identified
- Recommendations for improvement

---

### 4.9 HR & Rostering Module

#### FR-HR-001: Roster Generation [P2]

**Description:** The system SHALL assist in generating crew rosters.

**Constraints:**
| Constraint | Rule |
|------------|------|
| Max driving hours | 8 hours continuous, 10 hours total/day |
| Rest period | Minimum 10 hours between duties |
| Weekly off | 1 day off per 7 days |
| Route familiarity | Driver knows the route (training completed) |
| License validity | Valid license for bus category |
| Medical fitness | Valid medical certificate |

**AI Optimization:**
- Minimize deadhead (empty travel)
- Balance workload across drivers
- Respect driver preferences where possible
- Optimize for cost (reduce overtime)

---

#### FR-HR-002: Attendance Management [P2]

**Description:** The system SHALL track crew attendance.

**Features:**
- Shift check-in/check-out
- GPS-based presence verification
- Leave management
- Absence alerts
- Standby crew management

---

#### FR-HR-003: Training Management [P3]

**Description:** The system SHALL manage driver training requirements.

**Training Types:**
| Training | Frequency | Trigger |
|----------|-----------|---------|
| Induction | Once (new join) | Hiring |
| Route Training | Per route | New route assignment |
| Safety Refresher | Annual | Anniversary |
| Defensive Driving | As needed | Low safety score |
| First Aid | Biennial | Compliance |

**AI Integration:**
- Auto-assign training based on triggers
- Track completion
- Block assignment until training complete
- Generate training content recommendations

---

## 5. Agent Specifications

### 5.1 Supervisor Agent

**Purpose:** Route incoming requests and events to appropriate specialist agents.

**Capabilities:**
| Capability | Description |
|------------|-------------|
| Intent Classification | Determine which agent should handle request |
| Context Gathering | Collect necessary information before routing |
| Multi-Agent Coordination | Coordinate when multiple agents needed |
| Guardrail Enforcement | Apply system-wide rules and permissions |
| Escalation Handling | Route to human when needed |

**Routing Rules:**
| Intent/Event | Route To |
|--------------|----------|
| Passenger query | CS Agent |
| Delay detected | Ops Agent |
| Breakdown reported | Ops Agent + Maintenance Agent |
| Pricing question | Revenue Agent |
| Settlement query | Finance Agent |
| Roster request | HR Agent |
| Multi-department issue | Coordinate multiple agents |

---

### 5.2 Operations Agent

**Purpose:** Handle operational decisions and disruption management.

**Tools Available:**
| Tool | Function | Permission |
|------|----------|------------|
| get_trip_status | Retrieve current trip details | Auto |
| get_bus_location | Get real-time bus location | Auto |
| get_available_buses | List available spare buses | Auto |
| get_available_crew | List available standby crew | Auto |
| send_delay_alert | Notify passengers of delay | Auto |
| suggest_diversion | Recommend alternate route | Auto |
| assign_spare_bus | Assign replacement bus | Requires approval |
| cancel_trip | Cancel a scheduled trip | Requires approval |
| create_incident | Log an incident | Auto |

**Example Interactions:**
```
Event: Bus KA01AB1234 delayed by 45 minutes

Ops Agent:
1. Retrieves affected passengers (get_trip_status)
2. Checks for connecting trips
3. Sends delay notification (send_delay_alert)
4. Assesses if spare bus needed
5. If needed, identifies nearest spare (get_available_buses)
6. Recommends action to operator
7. On approval, executes assignment (assign_spare_bus)
```

---

### 5.3 Maintenance Agent

**Purpose:** Handle maintenance predictions, job cards, and mechanic support.

**Tools Available:**
| Tool | Function | Permission |
|------|----------|------------|
| get_bus_health | Retrieve health scores and alerts | Auto |
| get_maintenance_history | Get past maintenance records | Auto |
| create_job_card | Create new maintenance job | Requires approval |
| suggest_parts | Recommend parts for repair | Auto |
| search_manual | Search repair manuals (RAG) | Auto |
| update_job_status | Update job card status | Auto |
| schedule_maintenance | Book maintenance slot | Requires approval |
| pull_from_service | Mark bus as not serviceable | Requires approval |

**Example Interactions:**
```
Query from Mechanic: "Bus KA05XY5678 showing P0420 code. What should I check?"

Maintenance Agent:
1. Retrieves bus details and code meaning
2. Searches past repairs for this code (get_maintenance_history)
3. Searches repair manual (search_manual)
4. Provides step-by-step troubleshooting guide
5. Suggests likely parts needed (suggest_parts)
```

---

### 5.4 Customer Service Agent

**Purpose:** Handle passenger queries and support requests.

**Tools Available:**
| Tool | Function | Permission |
|------|----------|------------|
| lookup_booking | Find booking by PNR/phone | Auto |
| get_bus_location | Real-time location for tracking | Auto |
| calculate_refund | Calculate refund per policy | Auto |
| process_refund | Initiate refund | Auto (<₹500), Approval (>₹500) |
| reschedule_booking | Change to different trip | Auto (if available) |
| send_notification | Send WhatsApp/SMS to passenger | Auto |
| create_complaint | Log customer complaint | Auto |
| escalate_to_human | Transfer to human agent | Auto |

**Example Interactions:**
```
Passenger: "I want to cancel my booking PNR ABC123"

CS Agent:
1. Looks up booking (lookup_booking)
2. Checks trip time and cancellation policy
3. Calculates refund amount (calculate_refund)
4. Explains: "Your booking is for tomorrow 10 PM. As per policy, you'll receive 75% refund (₹675 of ₹900). Should I proceed?"
5. On confirmation, processes refund (process_refund)
6. Sends confirmation (send_notification)
```

---

### 5.5 Revenue Agent

**Purpose:** Handle pricing decisions and route analysis.

**Tools Available:**
| Tool | Function | Permission |
|------|----------|------------|
| get_demand_forecast | Retrieve demand predictions | Auto |
| get_current_pricing | Get current fares | Auto |
| get_competitor_pricing | Get competitor fares (if available) | Auto |
| get_booking_pace | Current booking velocity | Auto |
| recommend_price | Generate pricing recommendation | Auto |
| update_pricing | Change published fares | Requires approval |
| get_route_performance | Route-level metrics | Auto |
| generate_promo | Create promotion recommendation | Auto |

---

### 5.6 Finance Agent

**Purpose:** Handle reconciliation and financial queries.

**Tools Available:**
| Tool | Function | Permission |
|------|----------|------------|
| get_daily_collections | Retrieve collection summary | Auto |
| get_reconciliation_status | Check reconciliation status | Auto |
| get_discrepancies | List unmatched transactions | Auto |
| match_transaction | Manually match transactions | Auto |
| flag_anomaly | Mark transaction as suspicious | Auto |
| generate_report | Create financial report | Auto |
| approve_payment | Approve vendor payment | Requires approval |

---

### 5.7 HR Agent

**Purpose:** Handle rostering and crew-related queries.

**Tools Available:**
| Tool | Function | Permission |
|------|----------|------------|
| get_crew_availability | Check who's available | Auto |
| get_crew_hours | Check hours worked | Auto |
| suggest_roster | Generate roster recommendation | Auto |
| assign_duty | Assign crew to trip | Requires approval |
| get_training_status | Check training compliance | Auto |
| schedule_training | Book training session | Auto |

---

## 6. User Interface Requirements

### 6.1 General UI Principles

| Principle | Requirement |
|-----------|-------------|
| Responsive | Work on desktop, tablet, mobile |
| Performance | Page load <3 seconds |
| Accessibility | WCAG 2.1 AA compliance |
| Real-time | Live updates without refresh |
| Offline | Basic functionality when offline (mobile) |

### 6.2 Control Room Dashboard

**Layout:**
```
┌─────────────────────────────────────────────────────────────────────────┐
│  HEADER: Logo | Search | Notifications (🔔 12) | User Menu              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────────────────────────┐  ┌──────────────────────────┐ │
│  │                                     │  │    ALERTS PANEL          │ │
│  │                                     │  │                          │ │
│  │         LIVE MAP                    │  │  🔴 KA01AB1234 Breakdown │ │
│  │       (Fleet View)                  │  │  🟡 TN02XY5678 Delayed   │ │
│  │                                     │  │  🟡 AP03CD9012 Delayed   │ │
│  │                                     │  │                          │ │
│  │                                     │  │  [View All Alerts]       │ │
│  └─────────────────────────────────────┘  └──────────────────────────┘ │
│                                                                         │
│  ┌─────────────────────────────────────┐  ┌──────────────────────────┐ │
│  │      TODAY'S METRICS                │  │    AI ASSISTANT          │ │
│  │                                     │  │                          │ │
│  │  Running: 87  Delayed: 8  Down: 5   │  │  💬 "3 buses running     │ │
│  │  On-Time: 91%  Occupancy: 78%       │  │      >30min late on      │ │
│  │                                     │  │      BLR-HYD route..."   │ │
│  └─────────────────────────────────────┘  └──────────────────────────┘ │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

**Key Features:**
- Live map with bus icons
- Color-coded status (green/yellow/red)
- Click bus for details popup
- Alert panel with severity sorting
- One-click actions from alerts
- AI assistant chat widget
- KPI ticker

---

### 6.3 Depot Dispatch Screen

**Layout:**
```
┌─────────────────────────────────────────────────────────────────────────┐
│  HEADER: Depot Name | Date | Shift | User                               │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  UPCOMING DEPARTURES (Next 2 Hours)                                     │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ Time  │ Service      │ Bus       │ Driver    │ Status    │ Action  ││
│  │───────│──────────────│───────────│───────────│───────────│─────────││
│  │ 18:00 │ HYD-BLR Slpr │ KA01AB123 │ Ramesh K  │ ✅ Ready   │ [View]  ││
│  │ 18:30 │ HYD-CHN AC   │ KA02CD456 │ Suresh P  │ ⚠️ Check  │ [View]  ││
│  │ 19:00 │ HYD-VJA Semi │ KA03EF789 │ Pending   │ ❌ Assign │ [Assign]││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                         │
│  PRE-DEPARTURE CHECKLIST (Selected: HYD-BLR 18:00)                      │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ ✅ Fitness Valid (Exp: 15-Mar-2025)                                 ││
│  │ ✅ Insurance Valid (Exp: 20-Jun-2025)                               ││
│  │ ✅ Permits Valid (KA, TS, AP)                                       ││
│  │ ✅ Driver License Valid                                             ││
│  │ ✅ Driver Hours OK (4.5 hrs today)                                  ││
│  │ ⬜ Cleaning Completed [Confirm]                                     ││
│  │ ⬜ Fuel Level OK [Confirm]                                          ││
│  │                                                                     ││
│  │ [Clear for Departure]  [Report Issue]                               ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

---

### 6.4 Customer Service Console

**Layout:**
```
┌─────────────────────────────────────────────────────────────────────────┐
│  CS CONSOLE                                              Agent: Priya   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│ ┌────────────────┐ ┌────────────────────────────────────────────────┐  │
│ │ QUEUE          │ │ CURRENT CONVERSATION                           │  │
│ │                │ │                                                 │  │
│ │ 🟢 Active: 3   │ │ Customer: +91 98765 43210                      │  │
│ │ ⏳ Waiting: 12 │ │ Booking: PNR ABC123                            │  │
│ │                │ │ Trip: HYD-BLR, Tomorrow 10 PM                  │  │
│ │ ─────────────  │ │                                                 │  │
│ │ Anjali S.      │ │ ┌─────────────────────────────────────────────┐│  │
│ │ Refund query   │ │ │ Customer: I want to cancel my booking      ││  │
│ │ 2 min ago      │ │ │                                             ││  │
│ │                │ │ │ Bot: I can help with that. Your booking     ││  │
│ │ Ravi M.        │ │ │ PNR ABC123 is for tomorrow 10 PM. Per       ││  │
│ │ Location query │ │ │ policy, you'll receive 75% refund (₹675).   ││  │
│ │ 5 min ago      │ │ │ Should I proceed?                          ││  │
│ │                │ │ │                                             ││  │
│ │ Kumar P.       │ │ │ Customer: Yes please                        ││  │
│ │ Complaint      │ │ │                                             ││  │
│ │ 8 min ago      │ │ │ [Transferred to Agent]                     ││  │
│ │                │ │ └─────────────────────────────────────────────┘│  │
│ └────────────────┘ │                                                 │  │
│                    │ SUGGESTED RESPONSE:                             │  │
│ ┌────────────────┐ │ "I've processed your refund of ₹675. It will   │  │
│ │ CUSTOMER INFO  │ │ be credited to your original payment method    │  │
│ │                │ │ within 5-7 business days."                     │  │
│ │ Name: Anjali   │ │                                                 │  │
│ │ Phone: 98765.. │ │ [Send] [Edit] [Escalate]                       │  │
│ │ Trips: 8       │ │                                                 │  │
│ │ Lifetime: ₹12K │ └────────────────────────────────────────────────┘  │
│ │                │                                                      │
│ │ PAST ISSUES:   │ ┌────────────────────────────────────────────────┐  │
│ │ - Refund (2mo) │ │ QUICK ACTIONS                                  │  │
│ │ - Delay (4mo)  │ │ [Process Refund] [Reschedule] [Send Location]  │  │
│ └────────────────┘ │ [Create Complaint] [Send to Supervisor]        │  │
│                    └────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

### 6.5 Maintenance Dashboard

**Layout:**
```
┌─────────────────────────────────────────────────────────────────────────┐
│  MAINTENANCE DASHBOARD                                    Workshop: HYD │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  FLEET HEALTH OVERVIEW                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │  🟢 Healthy: 72    🟡 Watch: 18    🟠 Warning: 7    🔴 Critical: 3  ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                                                         │
│  ┌──────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ CRITICAL ALERTS              │  │ TODAY'S JOBS                   │  │
│  │                              │  │                                │  │
│  │ 🔴 KA01AB123 - Engine Risk   │  │ Total: 12  Done: 5  Active: 4  │  │
│  │    Predicted failure: 3 days │  │                                │  │
│  │    [Create Job] [Details]    │  │ ● KA02CD456 - Brake service   │  │
│  │                              │  │   Mechanic: Raju | 60% done   │  │
│  │ 🔴 KA04GH012 - AC Compressor │  │                                │  │
│  │    Predicted failure: 5 days │  │ ● KA05IJ789 - AC repair       │  │
│  │    [Create Job] [Details]    │  │   Mechanic: Sunil | 30% done  │  │
│  │                              │  │                                │  │
│  │ 🔴 KA07MN345 - Battery       │  │ [View All Jobs]                │  │
│  │    Predicted failure: 2 days │  │                                │  │
│  │    [Create Job] [Details]    │  └────────────────────────────────┘  │
│  └──────────────────────────────┘                                       │
│                                                                         │
│  UPCOMING SCHEDULED MAINTENANCE                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ Bus        │ Service Type │ Due Date  │ Km Since Last │ Status     ││
│  │────────────│──────────────│───────────│───────────────│────────────││
│  │ KA01AB123  │ B Service    │ 05-Dec    │ 9,500 km      │ Scheduled  ││
│  │ KA02CD456  │ Oil Change   │ 06-Dec    │ 4,800 km      │ Pending    ││
│  │ KA03EF789  │ A Service    │ 07-Dec    │ 14,200 km     │ Pending    ││
│  └─────────────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────────────┘
```

---

### 6.6 Mobile App Requirements

**Mechanic App:**
| Screen | Features |
|--------|----------|
| Job List | Assigned jobs, priority order |
| Job Details | Issue, instructions, parts |
| AI Assistant | Ask questions about repairs |
| Update Status | In-progress, completed, blocked |
| Parts Request | Request parts from store |
| Camera | Photo documentation |

**Driver App:**
| Screen | Features |
|--------|----------|
| Today's Duties | Assigned trips |
| Trip Details | Route, passengers, stops |
| Check-in | Start duty confirmation |
| Navigation | Integrated maps |
| Report Issue | Breakdown, delay, incident |
| Safety Score | View personal score |

---

## 7. Integration Requirements

### 7.1 Bitla Ticketing System

**Integration Type:** REST API

**Data Exchange:**
| Direction | Data | Frequency |
|-----------|------|-----------|
| Inbound | Bookings (new, modified, cancelled) | Real-time webhook |
| Inbound | Passenger details | On-demand |
| Inbound | Schedule/service master | Daily sync |
| Outbound | Trip status updates | Real-time |
| Outbound | Delay notifications trigger | Real-time |

**Key APIs Required:**
- GET /bookings/{pnr}
- GET /bookings/trip/{trip_id}
- GET /trips/{trip_id}/status
- POST /bookings/{pnr}/cancel
- POST /bookings/{pnr}/reschedule
- GET /services (master data)
- GET /routes (master data)

---

### 7.2 GPS/Telematics Provider

**Integration Type:** Streaming + REST API

**Data Exchange:**
| Direction | Data | Frequency |
|-----------|------|-----------|
| Inbound | Location (lat, long, speed, heading) | Every 30 seconds |
| Inbound | DTC codes | On occurrence |
| Inbound | Fuel level | Every 5 minutes |
| Inbound | Ignition status | On change |
| Inbound | Harsh events (braking, acceleration) | On occurrence |

**Protocol Options:**
- MQTT for streaming
- REST for historical queries
- Webhook for events

---

### 7.3 Payment Gateways

**Integration Type:** REST API + Webhooks

**Gateways:**
- Razorpay
- PayU
- Paytm
- UPI

**Data Exchange:**
| Direction | Data | Frequency |
|-----------|------|-----------|
| Inbound | Payment confirmations | Webhook |
| Inbound | Refund status | Webhook |
| Outbound | Refund initiation | On-demand |

---

### 7.4 WhatsApp Business API

**Integration Type:** REST API (via Gupshup/Twilio)

**Capabilities:**
| Feature | Description |
|---------|-------------|
| Receive messages | Customer queries |
| Send messages | Responses, notifications |
| Template messages | Booking confirmations, delays |
| Interactive messages | Buttons, lists |
| Media | Images, documents |

---

### 7.5 OTA Integrations

**OTAs to Integrate:**
- RedBus
- MakeMyTrip
- AbhiBus
- Paytm Travel
- Goibibo

**Data Exchange:**
| Direction | Data | Frequency |
|-----------|------|-----------|
| Outbound | Inventory (seats available) | Real-time |
| Outbound | Pricing | Real-time |
| Inbound | Bookings | Webhook |
| Inbound | Cancellations | Webhook |
| Outbound | Trip status | Batch (daily) |

---

### 7.6 Integration Summary Matrix

| System | Protocol | Auth | Real-time | Batch |
|--------|----------|------|-----------|-------|
| Bitla | REST | API Key | Webhooks | Daily |
| GPS Provider | MQTT + REST | Token | ✓ | Daily |
| Payment Gateway | REST | OAuth | Webhooks | Daily |
| WhatsApp | REST | Bearer | Webhooks | - |
| OTAs | REST | Varies | Webhooks | Daily |
| CMMS | REST/DB | API Key | - | Hourly |
| ERP | REST/DB | API Key | - | Daily |
| HR System | REST | API Key | - | Daily |

---

## 8. Reporting & Analytics Requirements

### 8.1 Operational Reports

| Report | Frequency | Users | Content |
|--------|-----------|-------|---------|
| Daily Operations Summary | Daily 6 AM | Ops Manager, Executives | On-time %, delays, cancellations, occupancy |
| Disruption Report | Daily | Ops Manager | All disruptions with RCA |
| Route Performance | Weekly | Ops, Commercial | Per-route metrics |
| Depot Performance | Weekly | Depot Managers | Per-depot comparison |
| Safety Dashboard | Weekly | Safety Officer | Driver scores, incidents |

### 8.2 Commercial Reports

| Report | Frequency | Users | Content |
|--------|-----------|-------|---------|
| Revenue Dashboard | Daily | Revenue Manager | Bookings, revenue, yield |
| Load Factor Analysis | Daily | Revenue Manager | Occupancy by route/service |
| Pricing Effectiveness | Weekly | Revenue Manager | Price changes vs bookings |
| Channel Performance | Weekly | Sales Manager | Per-channel metrics |
| Agent Reconciliation | Daily | Finance | Settlement status |

### 8.3 Maintenance Reports

| Report | Frequency | Users | Content |
|--------|-----------|-------|---------|
| Fleet Health Status | Daily | Workshop Manager | Health scores, alerts |
| Maintenance Completion | Daily | Workshop Manager | Jobs completed vs pending |
| Breakdown Analysis | Weekly | Engineering Head | Causes, trends |
| Parts Consumption | Weekly | Procurement | Usage, forecasts |
| Cost per Km | Monthly | Finance, Engineering | Maintenance cost trends |

### 8.4 Customer Service Reports

| Report | Frequency | Users | Content |
|--------|-----------|-------|---------|
| CS Dashboard | Real-time | CS Supervisor | Queue, resolution metrics |
| Bot Performance | Daily | CS Manager | Automation rate, handoffs |
| Query Analysis | Weekly | CS Manager | Top queries, trends |
| CSAT Report | Weekly | CS Manager | Satisfaction scores |
| Refund Summary | Daily | Finance, CS | Refunds processed |

### 8.5 Executive Dashboard

| Metric | Visualization |
|--------|---------------|
| Fleet Utilization | Gauge chart |
| On-time Performance | Trend line |
| Revenue vs Target | Bar chart |
| Customer Satisfaction | Score card |
| Breakdown Rate | Trend line |
| Operational Cost/Km | Trend line |

---

## 9. Notification & Communication Requirements

### 9.1 Passenger Notifications

| Event | Channel | Timing | Content |
|-------|---------|--------|---------|
| Booking Confirmation | WhatsApp, SMS, Email | Immediate | PNR, trip details, boarding point |
| Reminder | WhatsApp | 24 hours before | Trip reminder, bus tracking link |
| Delay Alert | WhatsApp, SMS | When detected | Delay duration, new ETA |
| Cancellation (by operator) | WhatsApp, SMS | Immediate | Reason, refund info, alternatives |
| Refund Processed | WhatsApp, SMS | When processed | Amount, timeline |
| Trip Started | WhatsApp | At departure | Bus tracking link |
| Arrival | WhatsApp | 30 min before arrival | ETA, drop point |

### 9.2 Internal Notifications

| Event | Recipients | Channel | Priority |
|-------|------------|---------|----------|
| Bus Breakdown | Control Room, Depot | Dashboard, Push | Critical |
| Major Delay (>45 min) | Control Room | Dashboard, Push | High |
| Minor Delay (15-45 min) | Control Room | Dashboard | Medium |
| Compliance Expiry (7 days) | Compliance, Depot | Email, Dashboard | High |
| Maintenance Alert (Critical) | Workshop | Dashboard, Push | Critical |
| Financial Anomaly | Finance | Email, Dashboard | High |
| Driver Safety Score Drop | Safety, HR | Email, Dashboard | Medium |

### 9.3 Notification Preferences

- Users SHALL be able to configure notification preferences
- Critical notifications cannot be disabled
- Channel preferences (email, push, SMS) configurable
- Quiet hours support (except critical)

---

## 10. Security & Access Control

### 10.1 Authentication

| Requirement | Specification |
|-------------|---------------|
| Method | Username/Password + OTP |
| Password Policy | Min 8 chars, complexity required |
| Session Timeout | 8 hours (configurable) |
| Failed Attempts | Lock after 5 failures |
| MFA | Required for admin users |

### 10.2 Role-Based Access Control

| Role | Ops | Maintenance | CS | Finance | HR | Admin |
|------|-----|-------------|----|---------|----|-------|
| Control Room Operator | Full | View | View | - | - | - |
| Depot Manager | Depot | View | View | - | - | - |
| Workshop Manager | View | Full | - | View | - | - |
| CS Agent | View | - | Own | - | - | - |
| CS Supervisor | View | - | Full | View | - | - |
| Finance Manager | View | View | View | Full | View | - |
| HR Manager | View | - | - | View | Full | - |
| IT Admin | View | View | View | View | View | Full |
| Executive | View | View | View | View | View | View |

### 10.3 Data Security

| Requirement | Specification |
|-------------|---------------|
| Encryption at Rest | AES-256 |
| Encryption in Transit | TLS 1.3 |
| PII Protection | Masked in logs, encrypted storage |
| Audit Logging | All data access logged |
| Data Retention | Per legal requirements |

### 10.4 AI-Specific Security

| Requirement | Specification |
|-------------|---------------|
| Prompt Injection Prevention | Input sanitization, guardrails |
| PII in Prompts | Masked before sending to LLM |
| Action Audit | All AI actions logged |
| Human Override | Always available |
| Confidence Thresholds | Low confidence → human review |

---

## 11. Business Rules

### 11.1 Operations Rules

| Rule ID | Rule | Enforcement |
|---------|------|-------------|
| BR-OPS-001 | Bus cannot depart without valid fitness certificate | Hard block |
| BR-OPS-002 | Bus cannot depart without valid insurance | Hard block |
| BR-OPS-003 | Bus cannot depart without valid permits for route states | Hard block |
| BR-OPS-004 | Driver cannot be assigned if license expired | Hard block |
| BR-OPS-005 | Driver cannot exceed 8 hours continuous driving | Warning at 7h, block at 8h |
| BR-OPS-006 | Driver must have 10 hours rest between duties | Hard block |
| BR-OPS-007 | Sleeper service >500km requires 2 drivers | Hard block |

### 11.2 Customer Service Rules

| Rule ID | Rule | Enforcement |
|---------|------|-------------|
| BR-CS-001 | Refund >48h before = 90% | Auto-apply |
| BR-CS-002 | Refund 24-48h before = 75% | Auto-apply |
| BR-CS-003 | Refund 12-24h before = 50% | Auto-apply |
| BR-CS-004 | Refund 6-12h before = 25% | Auto-apply |
| BR-CS-005 | Refund <6h before = 0% | Auto-apply |
| BR-CS-006 | Operator cancellation = 100% refund | Auto-apply |
| BR-CS-007 | Delay >2h = 100% refund option | Auto-apply |
| BR-CS-008 | Special case refunds need supervisor approval | Workflow |

### 11.3 Maintenance Rules

| Rule ID | Rule | Enforcement |
|---------|------|-------------|
| BR-MNT-001 | Bus with Critical health score cannot be dispatched | Hard block |
| BR-MNT-002 | Predicted failure <7 days triggers job card | Auto-create |
| BR-MNT-003 | Overdue scheduled maintenance blocks dispatch | Hard block |
| BR-MNT-004 | Breakdown on road triggers immediate job card | Auto-create |

### 11.4 Finance Rules

| Rule ID | Rule | Enforcement |
|---------|------|-------------|
| BR-FIN-001 | Refund >₹2000 requires manager approval | Workflow |
| BR-FIN-002 | Discrepancy >₹500 flagged for review | Auto-flag |
| BR-FIN-003 | Agent settlement requires reconciliation | Workflow |
| BR-FIN-004 | Vendor payment >₹50000 requires approval | Workflow |

### 11.5 Pricing Rules

| Rule ID | Rule | Enforcement |
|---------|------|-------------|
| BR-PRX-001 | Price cannot exceed 2x base fare | Hard limit |
| BR-PRX-002 | Price cannot go below 0.7x base fare | Hard limit |
| BR-PRX-003 | Max 3 price changes per service per day | Hard limit |
| BR-PRX-004 | Price change requires Revenue Manager approval | Workflow |

---

## 12. Acceptance Criteria

### 12.1 Phase 1 Acceptance Criteria

**Customer Service Bot:**
- [ ] Bot responds to messages within 5 seconds
- [ ] Bot handles booking lookup correctly 95% of time
- [ ] Bot handles refund calculation correctly 99% of time
- [ ] Bot successfully processes auto-refunds per policy
- [ ] Human handoff works with full context transfer
- [ ] Supports English and Hindi

**Delay Detection:**
- [ ] Delays detected within 2 minutes of occurrence
- [ ] Delay predictions within ±10 minutes accuracy
- [ ] Passenger notifications sent within 5 minutes
- [ ] All notifications delivered (WhatsApp + SMS fallback)

**Predictive Maintenance:**
- [ ] Risk scores generated for all buses daily
- [ ] Critical alerts generated for risk score >0.8
- [ ] Prediction accuracy >65% for top 3 failure categories
- [ ] Auto job card creation works correctly

**Control Room Dashboard:**
- [ ] Map shows all buses with <30s data latency
- [ ] Status colors correctly reflect bus state
- [ ] Alerts appear within 1 minute of detection
- [ ] Dashboard loads within 3 seconds

### 12.2 Phase 2 Acceptance Criteria

**Dispatcher Co-pilot:**
- [ ] Pre-departure checklist validates all items automatically
- [ ] Invalid items block departure with clear reason
- [ ] Manual confirmation items work correctly
- [ ] Compliance violations logged

**Dynamic Pricing:**
- [ ] Demand forecasts generated daily
- [ ] Pricing recommendations generated with rationale
- [ ] Approval workflow works correctly
- [ ] Price updates reflected in Bitla within 5 minutes

**Safety Scoring:**
- [ ] Driver scores calculated daily
- [ ] All scoring factors weighted correctly
- [ ] Training triggers generated for low scores
- [ ] Scores visible to drivers and managers

### 12.3 Phase 3 Acceptance Criteria

**Finance Reconciliation:**
- [ ] Daily reconciliation completes by 6 AM
- [ ] Auto-match rate >90%
- [ ] Discrepancies flagged correctly
- [ ] Anomalies detected with <5% false positive rate

**Roster Optimization:**
- [ ] Roster suggestions generated weekly
- [ ] All constraints respected
- [ ] Cost reduction vs manual roster >5%
- [ ] Manager approval workflow works

---

## 13. Appendices

### Appendix A: Glossary

| Term | Definition |
|------|------------|
| Agentic AI | AI that can take autonomous actions |
| CMMS | Computerized Maintenance Management System |
| DTC | Diagnostic Trouble Code |
| ERP | Enterprise Resource Planning |
| ETA | Estimated Time of Arrival |
| HITL | Human-in-the-Loop |
| LLM | Large Language Model |
| MFA | Multi-Factor Authentication |
| NPS | Net Promoter Score |
| OTA | Online Travel Agent |
| PII | Personally Identifiable Information |
| PNR | Passenger Name Record |
| RAG | Retrieval-Augmented Generation |
| RCA | Root Cause Analysis |
| SOP | Standard Operating Procedure |

### Appendix B: User Story Index

| ID | User Story | Module | Priority |
|----|------------|--------|----------|
| US-001 | As a Control Room Operator, I want to see all buses on a live map | Ops | P1 |
| US-002 | As a Control Room Operator, I want automatic delay alerts | Ops | P1 |
| US-003 | As a Control Room Operator, I want AI-suggested disruption responses | Ops | P1 |
| US-004 | As an Ops Manager, I want a daily operations brief | Ops | P2 |
| US-005 | As a Dispatcher, I want automatic pre-departure validation | Depot | P1 |
| US-006 | As a Workshop Manager, I want predictive maintenance alerts | Maintenance | P1 |
| US-007 | As a Mechanic, I want AI help for repairs | Maintenance | P2 |
| US-008 | As a Safety Officer, I want automatic driver risk identification | Safety | P1 |
| US-009 | As a Passenger, I want 24/7 WhatsApp support | CS | P1 |
| US-010 | As a Passenger, I want proactive delay notifications | CS | P1 |
| US-011 | As a Passenger, I want automatic refund processing | CS | P1 |
| US-012 | As a Revenue Manager, I want demand-based pricing recommendations | Revenue | P1 |
| US-013 | As an Accountant, I want automated reconciliation | Finance | P1 |
| US-014 | As an HR Manager, I want optimized roster suggestions | HR | P2 |

### Appendix C: Reference Documents

1. Original Requirements Document: "Tech stack for intercity bus operator to handle the business.docx"
2. Analysis & Recommendations Document: "01_Analysis_and_Recommendations.md"
3. Technical Specification Document: "03_Technical_Specification.md" (companion document)

---

**Document End**

*Functional Specification Document v1.0*
*December 2024*
