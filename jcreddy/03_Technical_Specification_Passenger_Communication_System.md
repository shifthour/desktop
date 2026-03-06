# Technical Specification Document
## Passenger Communication System (PCS) - MVP

**Document Version:** 1.0
**Date:** December 2024
**Status:** Draft
**Author:** Technical Team
**Project:** AI-First Tech Stack for Intercity Bus Operator
**Module:** Passenger Communication System

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [System Overview](#2-system-overview)
3. [Architecture Design](#3-architecture-design)
4. [API Integration Specification](#4-api-integration-specification)
5. [Database Design](#5-database-design)
6. [Service Specifications](#6-service-specifications)
7. [Message Templates](#7-message-templates)
8. [Scheduling Logic](#8-scheduling-logic)
9. [Delivery Channels](#9-delivery-channels)
10. [Security Specifications](#10-security-specifications)
11. [Error Handling & Retry Logic](#11-error-handling--retry-logic)
12. [Monitoring & Observability](#12-monitoring--observability)
13. [Deployment Architecture](#13-deployment-architecture)
14. [Technology Stack](#14-technology-stack)
15. [Development Phases](#15-development-phases)
16. [API Reference](#16-api-reference)
17. [Appendices](#17-appendices)

---

## 1. Executive Summary

### 1.1 Purpose

This Technical Specification Document defines the architecture, components, and implementation details for the Passenger Communication System (PCS) MVP. The system will automate passenger notifications for intercity bus operators using data from the Bitla ticketSimply platform.

### 1.2 Scope

| In Scope | Out of Scope (Future Phases) |
|----------|------------------------------|
| Automated trip reminders (24h, 2h before) | Real-time delay notifications |
| Boarding point information with GPS | Live bus tracking integration |
| Wake-up calls/messages before arrival | AI-powered customer support bot |
| WhatsApp & SMS delivery | Voice call automation |
| Multi-language support (English, Telugu) | Full multi-language support |
| Single operator pilot (Mythri Travels) | Multi-operator deployment |
| Scheduled batch notifications | Real-time booking confirmations |

### 1.3 Key Metrics

| Metric | Target |
|--------|--------|
| Message delivery rate | > 95% |
| Notification timing accuracy | Within 5 minutes of scheduled time |
| System uptime | 99.5% |
| API response time | < 500ms |
| Message queue processing | < 30 seconds |

### 1.4 Pilot Operator

| Attribute | Value |
|-----------|-------|
| Operator | Mythri Travels |
| API Base URL | `http://myth.mythribus.com/api/` |
| Active Routes | ~25 routes |
| Estimated Daily Passengers | 500-1000 |
| Languages | Telugu, English |

---

## 2. System Overview

### 2.1 High-Level System Context

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           EXTERNAL SYSTEMS                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │    Bitla     │    │   WhatsApp   │    │     SMS      │                  │
│  │  ticketSimply│    │  Business    │    │   Gateway    │                  │
│  │     API      │    │     API      │    │   (MSG91)    │                  │
│  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘                  │
│         │                   │                   │                          │
└─────────┼───────────────────┼───────────────────┼──────────────────────────┘
          │                   │                   │
          ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                                                             │
│                    PASSENGER COMMUNICATION SYSTEM (PCS)                     │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         CORE SERVICES                                │   │
│  │                                                                      │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐    │   │
│  │  │   Data     │  │ Scheduler  │  │  Message   │  │  Delivery  │    │   │
│  │  │  Fetcher   │  │  Service   │  │  Builder   │  │  Service   │    │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘    │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                         DATA LAYER                                   │   │
│  │                                                                      │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                     │   │
│  │  │ PostgreSQL │  │   Redis    │  │   S3/Minio │                     │   │
│  │  │  (Primary) │  │  (Queue &  │  │   (Logs)   │                     │   │
│  │  │            │  │   Cache)   │  │            │                     │   │
│  │  └────────────┘  └────────────┘  └────────────┘                     │   │
│  │                                                                      │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              PASSENGERS                                      │
│                                                                             │
│     📱 WhatsApp          📱 SMS           📧 Email (Future)                │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Key Capabilities

| Capability | Description |
|------------|-------------|
| **Data Synchronization** | Periodic fetch of passenger data from Bitla API |
| **Smart Scheduling** | Calculate optimal notification times based on trip schedule |
| **Multi-Channel Delivery** | Send via WhatsApp (primary) with SMS fallback |
| **Template Management** | Multilingual message templates with dynamic variables |
| **Delivery Tracking** | Track message delivery status and failures |
| **Retry Logic** | Automatic retry for failed deliveries |
| **Audit Logging** | Complete audit trail of all notifications |

### 2.3 Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            DATA FLOW DIAGRAM                                 │
└─────────────────────────────────────────────────────────────────────────────┘

1. DATA FETCH FLOW (Every 15 minutes)
   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
   │   Bitla     │ ──── │   Data      │ ──── │  PostgreSQL │
   │   API       │ GET  │   Fetcher   │ SAVE │  Database   │
   └─────────────┘      └─────────────┘      └─────────────┘

2. SCHEDULING FLOW (After each fetch)
   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
   │  PostgreSQL │ ──── │  Scheduler  │ ──── │   Redis     │
   │  (Trips)    │ READ │  Service    │ QUEUE│  (Jobs)     │
   └─────────────┘      └─────────────┘      └─────────────┘

3. NOTIFICATION FLOW (Triggered by scheduler)
   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
   │   Redis     │ ──── │  Message    │ ──── │  Delivery   │
   │  (Jobs)     │ POP  │  Builder    │ SEND │  Service    │
   └─────────────┘      └─────────────┘      └─────────────┘
                                                    │
                                    ┌───────────────┼───────────────┐
                                    ▼               ▼               ▼
                              ┌──────────┐   ┌──────────┐   ┌──────────┐
                              │ WhatsApp │   │   SMS    │   │  Email   │
                              └──────────┘   └──────────┘   └──────────┘

4. STATUS UPDATE FLOW (Webhooks from delivery providers)
   ┌─────────────┐      ┌─────────────┐      ┌─────────────┐
   │  WhatsApp/  │ ──── │  Webhook    │ ──── │  PostgreSQL │
   │  SMS Status │ POST │  Handler    │UPDATE│  (Status)   │
   └─────────────┘      └─────────────┘      └─────────────┘
```

---

## 3. Architecture Design

### 3.1 Microservices Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        MICROSERVICES ARCHITECTURE                            │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              API GATEWAY                                     │
│                         (nginx / Kong / Traefik)                            │
│                                                                             │
│  • Rate Limiting    • Authentication    • Request Routing    • SSL/TLS     │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│                 │    │                 │    │                 │
│  DATA FETCHER   │    │   SCHEDULER     │    │    DELIVERY     │
│    SERVICE      │    │    SERVICE      │    │    SERVICE      │
│                 │    │                 │    │                 │
│  • Bitla API    │    │  • Job Queue    │    │  • WhatsApp     │
│  • Data Sync    │    │  • Cron Jobs    │    │  • SMS          │
│  • Validation   │    │  • Timing Logic │    │  • Email        │
│                 │    │                 │    │  • Retry Logic  │
└────────┬────────┘    └────────┬────────┘    └────────┬────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            MESSAGE BROKER                                    │
│                          (Redis / RabbitMQ)                                 │
│                                                                             │
│  Queues:                                                                    │
│  • notification:24h      (24-hour reminders)                               │
│  • notification:2h       (2-hour reminders)                                │
│  • notification:wakeup   (Wake-up notifications)                           │
│  • notification:failed   (Failed messages for retry)                       │
│  • notification:dlq      (Dead letter queue)                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             DATA STORES                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐ │
│  │     PostgreSQL      │  │       Redis         │  │     S3 / MinIO      │ │
│  │                     │  │                     │  │                     │ │
│  │  • Operators        │  │  • Job Queue        │  │  • Message Logs     │ │
│  │  • Routes           │  │  • Cache            │  │  • Audit Logs       │ │
│  │  • Trips            │  │  • Rate Limiting    │  │  • Delivery Reports │ │
│  │  • Passengers       │  │  • Session Data     │  │                     │ │
│  │  • Notifications    │  │                     │  │                     │ │
│  │  • Delivery Status  │  │                     │  │                     │ │
│  └─────────────────────┘  └─────────────────────┘  └─────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Component Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          COMPONENT DIAGRAM                                   │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA FETCHER SERVICE                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────┐            │
│  │  BitlaClient   │    │  DataValidator │    │  DataMapper    │            │
│  │                │    │                │    │                │            │
│  │  • login()     │    │  • validate()  │    │  • toTrip()    │            │
│  │  • getRoutes() │    │  • sanitize()  │    │  • toPassenger │            │
│  │  • getPassengers│   │  • checkPerms  │    │  • toBoarding  │            │
│  └────────────────┘    └────────────────┘    └────────────────┘            │
│                                                                             │
│  ┌────────────────┐    ┌────────────────┐                                  │
│  │  SyncManager   │    │  ChangeDetector│                                  │
│  │                │    │                │                                  │
│  │  • syncAll()   │    │  • detectNew() │                                  │
│  │  • syncRoute() │    │  • detectMod() │                                  │
│  │  • syncTrip()  │    │  • detectCancel│                                  │
│  └────────────────┘    └────────────────┘                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                          SCHEDULER SERVICE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────┐            │
│  │ NotificationJob│    │  TimeCalculator│    │  JobEnqueuer   │            │
│  │                │    │                │    │                │            │
│  │  • calculate() │    │  • get24hTime()│    │  • enqueue()   │            │
│  │  • shouldSend()│    │  • get2hTime() │    │  • dequeue()   │            │
│  │  • getType()   │    │  • getWakeup() │    │  • retry()     │            │
│  └────────────────┘    └────────────────┘    └────────────────┘            │
│                                                                             │
│  ┌────────────────┐    ┌────────────────┐                                  │
│  │  CronManager   │    │ DuplicateChecker│                                 │
│  │                │    │                │                                  │
│  │  • start()     │    │  • isDuplicate │                                  │
│  │  • stop()      │    │  • markSent()  │                                  │
│  │  • runNow()    │    │  • cleanup()   │                                  │
│  └────────────────┘    └────────────────┘                                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                          DELIVERY SERVICE                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────┐            │
│  │ MessageBuilder │    │TemplateEngine  │    │ ChannelRouter  │            │
│  │                │    │                │    │                │            │
│  │  • build()     │    │  • render()    │    │  • route()     │            │
│  │  • format()    │    │  • translate() │    │  • fallback()  │            │
│  │  • addMaps()   │    │  • validate()  │    │  • retry()     │            │
│  └────────────────┘    └────────────────┘    └────────────────┘            │
│                                                                             │
│  ┌────────────────┐    ┌────────────────┐    ┌────────────────┐            │
│  │WhatsAppProvider│    │  SMSProvider   │    │ StatusTracker  │            │
│  │                │    │                │    │                │            │
│  │  • send()      │    │  • send()      │    │  • update()    │            │
│  │  • getStatus() │    │  • getStatus() │    │  • getStats()  │            │
│  │  • webhook()   │    │  • webhook()   │    │  • report()    │            │
│  └────────────────┘    └────────────────┘    └────────────────┘            │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 Sequence Diagram - Notification Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SEQUENCE DIAGRAM: SEND NOTIFICATION                       │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐   ┌─────────┐
    │  Cron   │   │Scheduler│   │  Redis  │   │Delivery │   │WhatsApp │
    │  Job    │   │ Service │   │  Queue  │   │ Service │   │   API   │
    └────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘   └────┬────┘
         │             │             │             │             │
         │ trigger     │             │             │             │
         │────────────>│             │             │             │
         │             │             │             │             │
         │             │ get pending │             │             │
         │             │────────────>│             │             │
         │             │             │             │             │
         │             │ jobs list   │             │             │
         │             │<────────────│             │             │
         │             │             │             │             │
         │             │ process job │             │             │
         │             │─────────────────────────>│             │
         │             │             │             │             │
         │             │             │             │ build msg   │
         │             │             │             │────┐        │
         │             │             │             │    │        │
         │             │             │             │<───┘        │
         │             │             │             │             │
         │             │             │             │ send        │
         │             │             │             │────────────>│
         │             │             │             │             │
         │             │             │             │  response   │
         │             │             │             │<────────────│
         │             │             │             │             │
         │             │             │ update      │             │
         │             │             │ status      │             │
         │             │             │<────────────│             │
         │             │             │             │             │
         │ complete    │             │             │             │
         │<────────────│             │             │             │
         │             │             │             │             │
    └────┴────┘   └────┴────┘   └────┴────┘   └────┴────┘   └────┴────┘
```

---

## 4. API Integration Specification

### 4.1 Bitla API Integration

#### 4.1.1 Authentication

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         BITLA API AUTHENTICATION                             │
└─────────────────────────────────────────────────────────────────────────────┘

Endpoint: POST /api/login.json
Base URL: http://myth.mythribus.com/api/

Request:
  URL: /api/login.json?login={username}&password={password}
  Method: POST
  Headers:
    Content-Type: application/json

Response (Success - 200):
  {
    "code": 200,
    "user_id": 562,
    "key": "YFZLLEATDHP6PPLNA3P7J0BJU1B2XZ3S",
    "name": "Admin Vja",
    "travel_branch_id": 1
  }

Response (Failure - 400):
  {
    "code": 400,
    "message": "Not a valid request"
  }

Token Handling:
  • Store API key securely (encrypted in database or secrets manager)
  • API key does not expire (based on documentation)
  • Implement key rotation mechanism for security
```

#### 4.1.2 Get All Routes

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            GET ALL ROUTES                                    │
└─────────────────────────────────────────────────────────────────────────────┘

Endpoint: GET /api/all_routes.json
Parameters:
  • api_key (required): Authentication key

Request:
  URL: /api/all_routes.json?api_key={api_key}
  Method: GET

Response (Success - 200):
  [
    {
      "id": 1,
      "route_num": "MYTHRI - 001-(No Dinner Break)",
      "route_status": "Active"
    },
    {
      "id": 14,
      "route_num": "MYTHRI - 004",
      "route_status": "Active"
    }
    // ... more routes
  ]

Filtering Logic:
  • Only process routes with route_status = "Active"
  • Cache route list (refresh every 6 hours)
  • Track route changes for alerts
```

#### 4.1.3 Get Passenger Details

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        GET PASSENGER DETAILS                                 │
└─────────────────────────────────────────────────────────────────────────────┘

Endpoint: GET /api/get_passenger_details/{date}.json
Parameters:
  • api_key (required): Authentication key
  • route_id (required): Route identifier
  • date (in URL): Travel date (YYYY-MM-DD format)

Request:
  URL: /api/get_passenger_details/2025-12-03.json?api_key={api_key}&route_id=14
  Method: GET

Response Structure:
```

```json
{
  "route_num": "MYTHRI - 004",
  "route_id": 14,
  "travel_date": "03-12-2025",
  "coach_num": "NL-01 B-3057",
  "available_seats": 2,
  "blocked_count": 2,
  "captain1_details": "Akula Maheswararao(dorababu) - 9705425228",
  "captain2_details": "Tv Sai Kumar (Sai) - 9133483837",
  "origin": "Yanam",
  "destination": "Hyderabad",
  "reservation_id": 26705,
  "route_departure_time": "19:45",
  "route_duration": "10:45",
  "trip_departure_date_time": "03-12-2025 07:45 PM",
  "trip_arrival_date_time": "04-12-2025 06:30 AM",
  "bus_type": "BRAND NEW 13.5 Mtr VEERA MAHA SAMRAT Sleeper",
  "customer_helpline_number": "7095666619",
  "passenger_details": [
    {
      "pnr_number": "840204",
      "title": "Mr",
      "name": "Sandeep kumar",
      "age": 34,
      "mobile": "7386681268",
      "email": "saikiranreddy1369@gmail.com",
      "seat_number": "F4",
      "is_ticket_confirm": true,
      "origin": "Yanam",
      "destination": "Hyderabad",
      "booked_by": "Paytm",
      "is_boarded": 0,
      "boarding_at": "Yanam - 07:45 PM",
      "drop_off": "Ameerpet Metro Station - 06:20 AM",
      "boarding_address": "By Pass Road Near Nallam Family Restrent",
      "boarding_landmark": "By Pass Road Near Nallam Family Restrent",
      "bording_date_time": "2025-12-03 07:45 PM",
      "bp_stage_latitude": "16.734295",
      "bp_stage_longitude": "82.202514",
      "dp_stage_latitude": "17.435807",
      "dp_stage_longitude": "78.444415",
      "wake_up_call_applicable": true,
      "pre_boarding_applicable": true,
      "welcome_call_applicable": true,
      "is_trackingo_sms_allowed": true,
      "booked_date": "02/12/2025"
    }
  ]
}
```

#### 4.1.4 Data Fetching Strategy

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        DATA FETCHING STRATEGY                                │
└─────────────────────────────────────────────────────────────────────────────┘

FETCH SCHEDULE:
┌──────────────────┬─────────────────┬────────────────────────────────────────┐
│ Time             │ Frequency       │ Data Fetched                           │
├──────────────────┼─────────────────┼────────────────────────────────────────┤
│ Every 15 min     │ Continuous      │ Passenger details for today + tomorrow │
│ Every 6 hours    │ 4x daily        │ Full route list refresh                │
│ On demand        │ As needed       │ Specific route/date                    │
└──────────────────┴─────────────────┴────────────────────────────────────────┘

POLLING ALGORITHM:
1. Get list of active routes (cached)
2. For each active route:
   a. Fetch passengers for TODAY
   b. Fetch passengers for TOMORROW
   c. Fetch passengers for DAY AFTER (for 24h reminders)
3. Compare with existing data
4. Insert new passengers
5. Update changed passengers
6. Mark cancelled passengers
7. Schedule notifications for new passengers

RATE LIMITING:
• Max 10 requests per second to Bitla API
• Implement exponential backoff on errors
• Circuit breaker after 5 consecutive failures
```

---

## 5. Database Design

### 5.1 Entity Relationship Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ENTITY RELATIONSHIP DIAGRAM                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────┐       ┌─────────────────┐       ┌─────────────────┐
│    operators    │       │     routes      │       │     trips       │
├─────────────────┤       ├─────────────────┤       ├─────────────────┤
│ id (PK)         │───┐   │ id (PK)         │───┐   │ id (PK)         │
│ name            │   │   │ operator_id(FK) │   │   │ route_id (FK)   │
│ code            │   └──>│ bitla_route_id  │   └──>│ travel_date     │
│ api_base_url    │       │ route_num       │       │ reservation_id  │
│ api_key_enc     │       │ origin          │       │ coach_num       │
│ is_active       │       │ destination     │       │ bus_type        │
│ created_at      │       │ status          │       │ departure_time  │
│ updated_at      │       │ created_at      │       │ arrival_time    │
└─────────────────┘       │ updated_at      │       │ captain1_name   │
                          └─────────────────┘       │ captain1_phone  │
                                                    │ captain2_name   │
                                                    │ captain2_phone  │
                                                    │ helpline        │
                                                    │ status          │
                                                    │ created_at      │
                                                    │ updated_at      │
                                                    └────────┬────────┘
                                                             │
                                                             │
┌─────────────────┐       ┌─────────────────┐                │
│ boarding_points │       │   passengers    │<───────────────┘
├─────────────────┤       ├─────────────────┤
│ id (PK)         │<──┐   │ id (PK)         │
│ trip_id (FK)    │   │   │ trip_id (FK)    │
│ name            │   │   │ pnr_number      │
│ address         │   │   │ name            │
│ landmark        │   │   │ title           │
│ latitude        │   └───│ boarding_point_id│
│ longitude       │       │ dropoff_point_id│───>│ drop_points │
│ scheduled_time  │       │ mobile          │
│ sequence        │       │ email           │
└─────────────────┘       │ seat_number     │
                          │ age             │
                          │ booked_by       │
                          │ booking_date    │
┌─────────────────┐       │ is_confirmed    │
│  notifications  │       │ is_boarded      │
├─────────────────┤       │ wake_up_call    │
│ id (PK)         │       │ pre_boarding    │
│ passenger_id(FK)│<──────│ welcome_call    │
│ trip_id (FK)    │       │ sms_allowed     │
│ type            │       │ created_at      │
│ scheduled_at    │       │ updated_at      │
│ sent_at         │       └─────────────────┘
│ channel         │
│ status          │
│ message_id      │
│ error_message   │
│ retry_count     │
│ created_at      │
│ updated_at      │
└─────────────────┘
```

### 5.2 Table Definitions

#### 5.2.1 operators

```sql
CREATE TABLE operators (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    code            VARCHAR(50) UNIQUE NOT NULL,
    api_base_url    VARCHAR(500) NOT NULL,
    api_key_enc     TEXT NOT NULL,  -- Encrypted API key
    api_username    VARCHAR(255),
    is_active       BOOLEAN DEFAULT true,
    settings        JSONB DEFAULT '{}',  -- Operator-specific settings
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Settings JSONB structure:
-- {
--   "default_language": "te",
--   "notification_channels": ["whatsapp", "sms"],
--   "reminder_24h_enabled": true,
--   "reminder_2h_enabled": true,
--   "wakeup_enabled": true,
--   "whatsapp_template_namespace": "mythri_travels"
-- }
```

#### 5.2.2 routes

```sql
CREATE TABLE routes (
    id              SERIAL PRIMARY KEY,
    operator_id     INTEGER REFERENCES operators(id),
    bitla_route_id  INTEGER NOT NULL,
    route_num       VARCHAR(255) NOT NULL,
    origin          VARCHAR(255),
    destination     VARCHAR(255),
    status          VARCHAR(50) DEFAULT 'active',
    last_synced_at  TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(operator_id, bitla_route_id)
);

CREATE INDEX idx_routes_operator ON routes(operator_id);
CREATE INDEX idx_routes_status ON routes(status);
```

#### 5.2.3 trips

```sql
CREATE TABLE trips (
    id                  SERIAL PRIMARY KEY,
    route_id            INTEGER REFERENCES routes(id),
    travel_date         DATE NOT NULL,
    reservation_id      INTEGER,
    coach_num           VARCHAR(100),
    bus_type            VARCHAR(255),
    departure_time      TIME,
    arrival_time        TIME,
    departure_datetime  TIMESTAMP WITH TIME ZONE,
    arrival_datetime    TIMESTAMP WITH TIME ZONE,
    captain1_name       VARCHAR(255),
    captain1_phone      VARCHAR(20),
    captain2_name       VARCHAR(255),
    captain2_phone      VARCHAR(20),
    helpline_number     VARCHAR(20),
    total_seats         INTEGER,
    available_seats     INTEGER,
    status              VARCHAR(50) DEFAULT 'scheduled',
    raw_data            JSONB,  -- Store complete API response
    last_synced_at      TIMESTAMP WITH TIME ZONE,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(route_id, travel_date, reservation_id)
);

CREATE INDEX idx_trips_travel_date ON trips(travel_date);
CREATE INDEX idx_trips_departure ON trips(departure_datetime);
CREATE INDEX idx_trips_status ON trips(status);
```

#### 5.2.4 boarding_points

```sql
CREATE TABLE boarding_points (
    id              SERIAL PRIMARY KEY,
    trip_id         INTEGER REFERENCES trips(id),
    bitla_stage_id  INTEGER,
    name            VARCHAR(255) NOT NULL,
    address         TEXT,
    landmark        TEXT,
    latitude        DECIMAL(10, 8),
    longitude       DECIMAL(11, 8),
    scheduled_time  TIME,
    sequence_order  INTEGER,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(trip_id, bitla_stage_id)
);

CREATE INDEX idx_boarding_points_trip ON boarding_points(trip_id);
```

#### 5.2.5 drop_points

```sql
CREATE TABLE drop_points (
    id              SERIAL PRIMARY KEY,
    trip_id         INTEGER REFERENCES trips(id),
    bitla_stage_id  INTEGER,
    name            VARCHAR(255) NOT NULL,
    address         TEXT,
    landmark        TEXT,
    latitude        DECIMAL(10, 8),
    longitude       DECIMAL(11, 8),
    scheduled_time  TIME,
    sequence_order  INTEGER,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(trip_id, bitla_stage_id)
);

CREATE INDEX idx_drop_points_trip ON drop_points(trip_id);
```

#### 5.2.6 passengers

```sql
CREATE TABLE passengers (
    id                  SERIAL PRIMARY KEY,
    trip_id             INTEGER REFERENCES trips(id),
    pnr_number          VARCHAR(50) NOT NULL,
    title               VARCHAR(10),
    name                VARCHAR(255) NOT NULL,
    age                 INTEGER,
    mobile              VARCHAR(20) NOT NULL,
    email               VARCHAR(255),
    seat_number         VARCHAR(20),
    boarding_point_id   INTEGER REFERENCES boarding_points(id),
    dropoff_point_id    INTEGER REFERENCES drop_points(id),
    boarding_datetime   TIMESTAMP WITH TIME ZONE,
    booked_by           VARCHAR(100),  -- OTA name (Redbus, Paytm, etc.)
    booked_by_login     VARCHAR(100),
    booking_date        DATE,
    is_confirmed        BOOLEAN DEFAULT true,
    is_boarded          BOOLEAN DEFAULT false,
    is_primary          BOOLEAN DEFAULT false,
    wake_up_call_applicable     BOOLEAN DEFAULT false,
    pre_boarding_applicable     BOOLEAN DEFAULT false,
    welcome_call_applicable     BOOLEAN DEFAULT false,
    is_trackingo_sms_allowed    BOOLEAN DEFAULT true,
    language_preference VARCHAR(10) DEFAULT 'en',
    raw_data            JSONB,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(trip_id, pnr_number, seat_number)
);

CREATE INDEX idx_passengers_trip ON passengers(trip_id);
CREATE INDEX idx_passengers_mobile ON passengers(mobile);
CREATE INDEX idx_passengers_pnr ON passengers(pnr_number);
CREATE INDEX idx_passengers_boarding ON passengers(boarding_datetime);
```

#### 5.2.7 notifications

```sql
CREATE TABLE notifications (
    id                  SERIAL PRIMARY KEY,
    passenger_id        INTEGER REFERENCES passengers(id),
    trip_id             INTEGER REFERENCES trips(id),
    notification_type   VARCHAR(50) NOT NULL,  -- 'reminder_24h', 'reminder_2h', 'wakeup', 'custom'
    scheduled_at        TIMESTAMP WITH TIME ZONE NOT NULL,
    sent_at             TIMESTAMP WITH TIME ZONE,
    channel             VARCHAR(20) NOT NULL,  -- 'whatsapp', 'sms', 'email'
    status              VARCHAR(20) DEFAULT 'pending',  -- 'pending', 'sent', 'delivered', 'failed', 'cancelled'
    template_id         VARCHAR(100),
    message_content     TEXT,
    message_id          VARCHAR(255),  -- External message ID from provider
    provider_response   JSONB,
    error_message       TEXT,
    retry_count         INTEGER DEFAULT 0,
    max_retries         INTEGER DEFAULT 3,
    next_retry_at       TIMESTAMP WITH TIME ZONE,
    metadata            JSONB DEFAULT '{}',
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_notifications_passenger ON notifications(passenger_id);
CREATE INDEX idx_notifications_trip ON notifications(trip_id);
CREATE INDEX idx_notifications_scheduled ON notifications(scheduled_at);
CREATE INDEX idx_notifications_status ON notifications(status);
CREATE INDEX idx_notifications_type ON notifications(notification_type);
CREATE INDEX idx_notifications_pending ON notifications(status, scheduled_at)
    WHERE status = 'pending';
```

#### 5.2.8 notification_logs

```sql
CREATE TABLE notification_logs (
    id                  SERIAL PRIMARY KEY,
    notification_id     INTEGER REFERENCES notifications(id),
    event_type          VARCHAR(50) NOT NULL,  -- 'created', 'sent', 'delivered', 'failed', 'retry'
    event_data          JSONB,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_notification_logs_notification ON notification_logs(notification_id);
CREATE INDEX idx_notification_logs_created ON notification_logs(created_at);
```

#### 5.2.9 message_templates

```sql
CREATE TABLE message_templates (
    id                  SERIAL PRIMARY KEY,
    operator_id         INTEGER REFERENCES operators(id),
    template_code       VARCHAR(100) NOT NULL,
    notification_type   VARCHAR(50) NOT NULL,
    channel             VARCHAR(20) NOT NULL,
    language            VARCHAR(10) NOT NULL,
    subject             VARCHAR(255),
    body                TEXT NOT NULL,
    variables           JSONB,  -- List of variables used in template
    whatsapp_template_name VARCHAR(255),
    whatsapp_namespace  VARCHAR(255),
    is_active           BOOLEAN DEFAULT true,
    created_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    UNIQUE(operator_id, template_code, channel, language)
);

CREATE INDEX idx_templates_operator ON message_templates(operator_id);
CREATE INDEX idx_templates_type ON message_templates(notification_type);
```

### 5.3 Database Indexes Summary

```sql
-- Performance indexes for common queries

-- Find pending notifications to send
CREATE INDEX idx_notifications_pending_send
ON notifications(scheduled_at, status)
WHERE status = 'pending';

-- Find passengers by trip and boarding time
CREATE INDEX idx_passengers_trip_boarding
ON passengers(trip_id, boarding_datetime);

-- Find trips by date range
CREATE INDEX idx_trips_date_range
ON trips(travel_date, departure_datetime);

-- Lookup notifications by external message ID
CREATE INDEX idx_notifications_message_id
ON notifications(message_id)
WHERE message_id IS NOT NULL;
```

---

## 6. Service Specifications

### 6.1 Data Fetcher Service

#### 6.1.1 Service Overview

```yaml
Service Name: data-fetcher
Port: 8001
Language: Python 3.11+
Framework: FastAPI
Dependencies:
  - httpx (async HTTP client)
  - sqlalchemy (ORM)
  - apscheduler (job scheduling)
  - redis (caching)
```

#### 6.1.2 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | /health | Health check |
| POST | /sync/operator/{id} | Trigger sync for operator |
| POST | /sync/route/{id} | Sync specific route |
| POST | /sync/trip/{id} | Sync specific trip |
| GET | /status | Get sync status |

#### 6.1.3 Core Logic

```python
# Pseudocode for Data Fetcher

class BitlaClient:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url
        self.api_key = api_key
        self.rate_limiter = RateLimiter(requests_per_second=10)

    async def get_routes(self) -> List[Route]:
        """Fetch all active routes"""
        url = f"{self.base_url}/all_routes.json?api_key={self.api_key}"
        response = await self.http_client.get(url)
        return [Route.from_dict(r) for r in response.json() if r['route_status'] == 'Active']

    async def get_passengers(self, route_id: int, date: date) -> TripData:
        """Fetch passenger details for route on date"""
        url = f"{self.base_url}/get_passenger_details/{date}.json"
        params = {"api_key": self.api_key, "route_id": route_id}
        response = await self.http_client.get(url, params=params)
        return TripData.from_dict(response.json())


class SyncManager:
    def __init__(self, db: Database, bitla_client: BitlaClient):
        self.db = db
        self.bitla = bitla_client

    async def sync_operator(self, operator_id: int):
        """Full sync for an operator"""
        # 1. Get all active routes
        routes = await self.bitla.get_routes()

        # 2. For each route, sync today + next 2 days
        for route in routes:
            for day_offset in [0, 1, 2]:
                target_date = date.today() + timedelta(days=day_offset)
                await self.sync_trip(route.id, target_date)

    async def sync_trip(self, route_id: int, travel_date: date):
        """Sync a specific trip"""
        # 1. Fetch from API
        trip_data = await self.bitla.get_passengers(route_id, travel_date)

        # 2. Upsert trip
        trip = await self.db.upsert_trip(trip_data)

        # 3. Upsert boarding/drop points
        await self.db.upsert_boarding_points(trip.id, trip_data.boarding_points)

        # 4. Upsert passengers
        for passenger in trip_data.passengers:
            existing = await self.db.get_passenger(trip.id, passenger.pnr, passenger.seat)
            if existing:
                await self.db.update_passenger(existing.id, passenger)
            else:
                new_passenger = await self.db.create_passenger(trip.id, passenger)
                # 5. Schedule notifications for new passenger
                await self.schedule_notifications(new_passenger)

    async def schedule_notifications(self, passenger: Passenger):
        """Create notification jobs for passenger"""
        notifications = []

        # 24-hour reminder
        if passenger.pre_boarding_applicable:
            reminder_24h = passenger.boarding_datetime - timedelta(hours=24)
            if reminder_24h > datetime.now():
                notifications.append(Notification(
                    passenger_id=passenger.id,
                    trip_id=passenger.trip_id,
                    notification_type='reminder_24h',
                    scheduled_at=reminder_24h,
                    channel='whatsapp'
                ))

        # 2-hour reminder
        if passenger.welcome_call_applicable:
            reminder_2h = passenger.boarding_datetime - timedelta(hours=2)
            if reminder_2h > datetime.now():
                notifications.append(Notification(
                    passenger_id=passenger.id,
                    trip_id=passenger.trip_id,
                    notification_type='reminder_2h',
                    scheduled_at=reminder_2h,
                    channel='whatsapp'
                ))

        # Wake-up notification (30 min before arrival)
        if passenger.wake_up_call_applicable:
            trip = await self.db.get_trip(passenger.trip_id)
            wakeup_time = trip.arrival_datetime - timedelta(minutes=30)
            if wakeup_time > datetime.now():
                notifications.append(Notification(
                    passenger_id=passenger.id,
                    trip_id=passenger.trip_id,
                    notification_type='wakeup',
                    scheduled_at=wakeup_time,
                    channel='whatsapp'
                ))

        # Save notifications
        await self.db.create_notifications(notifications)
```

### 6.2 Scheduler Service

#### 6.2.1 Service Overview

```yaml
Service Name: scheduler
Port: 8002
Language: Python 3.11+
Framework: FastAPI + APScheduler
Dependencies:
  - apscheduler
  - redis
  - sqlalchemy
```

#### 6.2.2 Scheduling Logic

```python
# Scheduler Service Pseudocode

class NotificationScheduler:
    def __init__(self, db: Database, redis: Redis, delivery_service: DeliveryService):
        self.db = db
        self.redis = redis
        self.delivery = delivery_service

    async def process_pending_notifications(self):
        """Main job - runs every minute"""
        # Get notifications due in next 2 minutes
        cutoff_time = datetime.now() + timedelta(minutes=2)

        notifications = await self.db.get_pending_notifications(
            status='pending',
            scheduled_before=cutoff_time
        )

        for notification in notifications:
            # Check if already processing (distributed lock)
            lock_key = f"notification:lock:{notification.id}"
            if await self.redis.set(lock_key, "1", nx=True, ex=300):
                try:
                    await self.process_notification(notification)
                finally:
                    await self.redis.delete(lock_key)

    async def process_notification(self, notification: Notification):
        """Process a single notification"""
        # 1. Check if still valid
        passenger = await self.db.get_passenger(notification.passenger_id)
        if not passenger or not passenger.is_confirmed:
            await self.db.update_notification(notification.id, status='cancelled')
            return

        # 2. Check for duplicates
        if await self.is_duplicate(notification):
            await self.db.update_notification(notification.id, status='cancelled')
            return

        # 3. Build message
        message = await self.build_message(notification, passenger)

        # 4. Send via delivery service
        result = await self.delivery.send(notification.channel, passenger.mobile, message)

        # 5. Update status
        if result.success:
            await self.db.update_notification(
                notification.id,
                status='sent',
                sent_at=datetime.now(),
                message_id=result.message_id
            )
        else:
            await self.handle_failure(notification, result.error)

    async def is_duplicate(self, notification: Notification) -> bool:
        """Check if similar notification was sent recently"""
        key = f"sent:{notification.passenger_id}:{notification.notification_type}"
        exists = await self.redis.get(key)
        if exists:
            return True
        # Mark as sent for 1 hour
        await self.redis.set(key, "1", ex=3600)
        return False

    async def handle_failure(self, notification: Notification, error: str):
        """Handle failed notification"""
        if notification.retry_count < notification.max_retries:
            # Schedule retry with exponential backoff
            delay = 2 ** notification.retry_count * 60  # 1, 2, 4 minutes
            next_retry = datetime.now() + timedelta(seconds=delay)

            await self.db.update_notification(
                notification.id,
                status='pending',
                retry_count=notification.retry_count + 1,
                next_retry_at=next_retry,
                error_message=error
            )
        else:
            # Max retries exceeded - try fallback channel
            if notification.channel == 'whatsapp':
                # Create SMS fallback
                await self.db.create_notification(
                    passenger_id=notification.passenger_id,
                    trip_id=notification.trip_id,
                    notification_type=notification.notification_type,
                    scheduled_at=datetime.now(),
                    channel='sms'
                )

            await self.db.update_notification(
                notification.id,
                status='failed',
                error_message=error
            )
```

### 6.3 Delivery Service

#### 6.3.1 Service Overview

```yaml
Service Name: delivery
Port: 8003
Language: Python 3.11+
Framework: FastAPI
Dependencies:
  - httpx
  - jinja2 (templates)
  - redis
```

#### 6.3.2 Delivery Logic

```python
# Delivery Service Pseudocode

class DeliveryService:
    def __init__(self, whatsapp: WhatsAppProvider, sms: SMSProvider, templates: TemplateEngine):
        self.whatsapp = whatsapp
        self.sms = sms
        self.templates = templates

    async def send(self, channel: str, mobile: str, message: Message) -> DeliveryResult:
        """Send message via specified channel"""
        if channel == 'whatsapp':
            return await self.whatsapp.send(mobile, message)
        elif channel == 'sms':
            return await self.sms.send(mobile, message)
        else:
            raise ValueError(f"Unknown channel: {channel}")


class WhatsAppProvider:
    """Gupshup WhatsApp Business API integration"""

    def __init__(self, api_key: str, app_name: str):
        self.api_key = api_key
        self.app_name = app_name
        self.base_url = "https://api.gupshup.io/sm/api/v1"

    async def send(self, mobile: str, message: Message) -> DeliveryResult:
        """Send WhatsApp message"""
        # Format mobile number (add country code if needed)
        formatted_mobile = self.format_mobile(mobile)

        if message.template_name:
            # Send template message
            return await self.send_template(formatted_mobile, message)
        else:
            # Send session message
            return await self.send_text(formatted_mobile, message.text)

    async def send_template(self, mobile: str, message: Message) -> DeliveryResult:
        """Send WhatsApp template message"""
        payload = {
            "channel": "whatsapp",
            "source": self.app_name,
            "destination": mobile,
            "template": {
                "id": message.template_name,
                "params": message.template_params
            }
        }

        response = await self.http_client.post(
            f"{self.base_url}/template/msg",
            headers={"apikey": self.api_key},
            json=payload
        )

        if response.status_code == 200:
            data = response.json()
            return DeliveryResult(
                success=True,
                message_id=data.get("messageId"),
                provider_response=data
            )
        else:
            return DeliveryResult(
                success=False,
                error=response.text
            )


class SMSProvider:
    """MSG91 SMS Gateway integration"""

    def __init__(self, auth_key: str, sender_id: str):
        self.auth_key = auth_key
        self.sender_id = sender_id
        self.base_url = "https://api.msg91.com/api/v5"

    async def send(self, mobile: str, message: Message) -> DeliveryResult:
        """Send SMS"""
        payload = {
            "sender": self.sender_id,
            "route": "4",  # Transactional
            "country": "91",
            "sms": [
                {
                    "message": message.text,
                    "to": [mobile]
                }
            ]
        }

        response = await self.http_client.post(
            f"{self.base_url}/flow/",
            headers={"authkey": self.auth_key},
            json=payload
        )

        if response.status_code == 200:
            data = response.json()
            return DeliveryResult(
                success=True,
                message_id=data.get("request_id"),
                provider_response=data
            )
        else:
            return DeliveryResult(
                success=False,
                error=response.text
            )


class TemplateEngine:
    """Message template rendering"""

    def __init__(self, db: Database):
        self.db = db
        self.jinja = Environment(loader=BaseLoader())

    async def render(
        self,
        notification_type: str,
        channel: str,
        language: str,
        variables: dict
    ) -> Message:
        """Render message from template"""
        # Get template from database
        template = await self.db.get_template(
            notification_type=notification_type,
            channel=channel,
            language=language
        )

        if not template:
            raise TemplateNotFoundError(
                f"Template not found: {notification_type}/{channel}/{language}"
            )

        # Render template
        jinja_template = self.jinja.from_string(template.body)
        rendered_text = jinja_template.render(**variables)

        return Message(
            text=rendered_text,
            template_name=template.whatsapp_template_name,
            template_params=self.extract_params(template, variables)
        )

    def extract_params(self, template: MessageTemplate, variables: dict) -> List[str]:
        """Extract ordered parameters for WhatsApp template"""
        params = []
        for var_name in template.variables:
            params.append(str(variables.get(var_name, "")))
        return params
```

---

## 7. Message Templates

### 7.1 Template Variables

```yaml
Available Variables:
  # Passenger Info
  passenger_name: "Sandeep kumar"
  passenger_mobile: "7386681268"
  seat_number: "F4"
  pnr_number: "840204"

  # Trip Info
  route_name: "MYTHRI - 004"
  origin: "Yanam"
  destination: "Hyderabad"
  travel_date: "03 Dec 2025"

  # Boarding Info
  boarding_point: "Yanam"
  boarding_time: "07:45 PM"
  boarding_address: "By Pass Road Near Nallam Family Restaurant"
  boarding_landmark: "By Pass Road Near Nallam Family Restaurant"
  boarding_maps_link: "https://maps.google.com/?q=16.734295,82.202514"

  # Drop Info
  drop_point: "Ameerpet Metro Station"
  drop_time: "06:20 AM"
  drop_maps_link: "https://maps.google.com/?q=17.435807,78.444415"

  # Bus Info
  bus_number: "NL-01 B-3057"
  bus_type: "VEERA MAHA SAMRAT Sleeper"

  # Driver Info
  driver_name: "Akula Maheswararao"
  driver_phone: "9705425228"

  # Support
  helpline_number: "7095666619"
  operator_name: "Mythri Travels"
```

### 7.2 English Templates

#### 7.2.1 24-Hour Reminder (English)

```
Template Code: reminder_24h_en
Channel: WhatsApp
WhatsApp Template Name: trip_reminder_24h

---

🚌 *{{operator_name}} - Trip Reminder*

Hello {{passenger_name}},

Your bus journey is *tomorrow*!

📅 *Date:* {{travel_date}}
🚌 *Route:* {{origin}} → {{destination}}

*Boarding Details:*
📍 Point: {{boarding_point}}
⏰ Time: {{boarding_time}}
📌 Address: {{boarding_address}}
🗺️ Location: {{boarding_maps_link}}

*Your Booking:*
🎫 PNR: {{pnr_number}}
💺 Seat: {{seat_number}}
🚌 Bus: {{bus_number}}

👨‍✈️ Driver: {{driver_name}} - {{driver_phone}}
📞 Helpline: {{helpline_number}}

Have a safe journey! 🙏
```

#### 7.2.2 2-Hour Reminder (English)

```
Template Code: reminder_2h_en
Channel: WhatsApp
WhatsApp Template Name: trip_reminder_2h

---

🚌 *{{operator_name}} - Departing Soon!*

Hi {{passenger_name}},

Your bus departs in *2 hours*!

⏰ Departure: {{boarding_time}} from {{boarding_point}}
🚌 Bus No: {{bus_number}}
💺 Your Seat: {{seat_number}}

📍 *Reach here:*
{{boarding_address}}
{{boarding_maps_link}}

📞 Need help? Call {{helpline_number}}

See you soon! 🚌
```

#### 7.2.3 Wake-up Notification (English)

```
Template Code: wakeup_en
Channel: WhatsApp
WhatsApp Template Name: trip_wakeup

---

⏰ *{{operator_name}} - Wake Up Call*

Good morning {{passenger_name}}!

Your bus will reach *{{drop_point}}* in approximately *30 minutes*.

🕐 Expected Arrival: {{drop_time}}
📍 Drop Location: {{drop_maps_link}}

Please collect your belongings and be ready.

📞 Helpline: {{helpline_number}}

Thank you for traveling with us! 🙏
```

### 7.3 Telugu Templates

#### 7.3.1 24-Hour Reminder (Telugu)

```
Template Code: reminder_24h_te
Channel: WhatsApp
WhatsApp Template Name: trip_reminder_24h_te

---

🚌 *{{operator_name}} - ప్రయాణ రిమైండర్*

నమస్కారం {{passenger_name}} గారు,

మీ బస్ ప్రయాణం *రేపు*!

📅 *తేదీ:* {{travel_date}}
🚌 *రూట్:* {{origin}} → {{destination}}

*బోర్డింగ్ వివరాలు:*
📍 పాయింట్: {{boarding_point}}
⏰ సమయం: {{boarding_time}}
📌 చిరునామా: {{boarding_address}}
🗺️ లొకేషన్: {{boarding_maps_link}}

*మీ బుకింగ్:*
🎫 PNR: {{pnr_number}}
💺 సీట్: {{seat_number}}
🚌 బస్: {{bus_number}}

👨‍✈️ డ్రైవర్: {{driver_name}} - {{driver_phone}}
📞 హెల్ప్‌లైన్: {{helpline_number}}

శుభ ప్రయాణం! 🙏
```

#### 7.3.2 2-Hour Reminder (Telugu)

```
Template Code: reminder_2h_te
Channel: WhatsApp
WhatsApp Template Name: trip_reminder_2h_te

---

🚌 *{{operator_name}} - బస్ త్వరలో బయలుదేరుతుంది!*

{{passenger_name}} గారు,

మీ బస్ *2 గంటల్లో* బయలుదేరుతుంది!

⏰ బయలుదేరే సమయం: {{boarding_time}}, {{boarding_point}} నుండి
🚌 బస్ నంబర్: {{bus_number}}
💺 మీ సీట్: {{seat_number}}

📍 *ఇక్కడికి చేరుకోండి:*
{{boarding_address}}
{{boarding_maps_link}}

📞 సహాయం కావాలా? {{helpline_number}} కు కాల్ చేయండి

త్వరలో కలుద్దాం! 🚌
```

#### 7.3.3 Wake-up Notification (Telugu)

```
Template Code: wakeup_te
Channel: WhatsApp
WhatsApp Template Name: trip_wakeup_te

---

⏰ *{{operator_name}} - వేక్ అప్ కాల్*

శుభోదయం {{passenger_name}} గారు!

మీ బస్ *{{drop_point}}* కు సుమారుగా *30 నిమిషాల్లో* చేరుకుంటుంది.

🕐 అంచనా రాక సమయం: {{drop_time}}
📍 డ్రాప్ లొకేషన్: {{drop_maps_link}}

దయచేసి మీ సామాన్లను సేకరించి సిద్ధంగా ఉండండి.

📞 హెల్ప్‌లైన్: {{helpline_number}}

మాతో ప్రయాణించినందుకు ధన్యవాదాలు! 🙏
```

### 7.4 SMS Templates (Fallback)

```
Template Code: reminder_24h_sms
Channel: SMS

---

MYTHRI TRAVELS: Hi {{passenger_name}}, your bus tomorrow at {{boarding_time}} from {{boarding_point}}. PNR:{{pnr_number}} Seat:{{seat_number}}. Location: {{boarding_maps_link}} Helpline:{{helpline_number}}
```

```
Template Code: reminder_2h_sms
Channel: SMS

---

MYTHRI: Bus in 2hrs! {{boarding_time}} at {{boarding_point}}. Bus:{{bus_number}} Seat:{{seat_number}}. Maps:{{boarding_maps_link}} Help:{{helpline_number}}
```

```
Template Code: wakeup_sms
Channel: SMS

---

MYTHRI: Wake up! Reaching {{drop_point}} in 30 mins at {{drop_time}}. Please be ready. Help:{{helpline_number}}
```

---

## 8. Scheduling Logic

### 8.1 Notification Timing Rules

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                       NOTIFICATION TIMING RULES                              │
└─────────────────────────────────────────────────────────────────────────────┘

BOARDING TIME: 07:45 PM on Dec 3, 2025 (Example)

┌──────────────────┬─────────────────────┬─────────────────────────────────────┐
│ Notification     │ Scheduled Time      │ Conditions                          │
├──────────────────┼─────────────────────┼─────────────────────────────────────┤
│ 24-Hour Reminder │ 07:45 PM, Dec 2     │ pre_boarding_applicable = true      │
│                  │                     │ AND scheduled_time > now()          │
├──────────────────┼─────────────────────┼─────────────────────────────────────┤
│ 2-Hour Reminder  │ 05:45 PM, Dec 3     │ welcome_call_applicable = true      │
│                  │                     │ AND scheduled_time > now()          │
├──────────────────┼─────────────────────┼─────────────────────────────────────┤
│ Wake-up Call     │ 06:00 AM, Dec 4     │ wake_up_call_applicable = true      │
│                  │ (30 min before      │ AND is_boarded = true (optional)    │
│                  │  arrival)           │ AND scheduled_time > now()          │
└──────────────────┴─────────────────────┴─────────────────────────────────────┘
```

### 8.2 Scheduling Algorithm

```python
def calculate_notification_times(passenger: Passenger, trip: Trip) -> List[ScheduledNotification]:
    """Calculate all notification times for a passenger"""
    notifications = []
    now = datetime.now(timezone.utc)

    # Parse boarding time
    boarding_dt = parse_datetime(passenger.bording_date_time)

    # 1. 24-Hour Reminder
    if passenger.pre_boarding_applicable:
        reminder_24h = boarding_dt - timedelta(hours=24)

        # Adjust if falls during quiet hours (11 PM - 7 AM)
        reminder_24h = adjust_for_quiet_hours(reminder_24h)

        if reminder_24h > now:
            notifications.append(ScheduledNotification(
                type='reminder_24h',
                scheduled_at=reminder_24h,
                priority=1
            ))

    # 2. 2-Hour Reminder
    if passenger.welcome_call_applicable:
        reminder_2h = boarding_dt - timedelta(hours=2)

        if reminder_2h > now:
            notifications.append(ScheduledNotification(
                type='reminder_2h',
                scheduled_at=reminder_2h,
                priority=2
            ))

    # 3. Wake-up Notification
    if passenger.wake_up_call_applicable:
        # Parse arrival time
        arrival_dt = parse_datetime(trip.trip_arrival_date_time)
        wakeup_time = arrival_dt - timedelta(minutes=30)

        if wakeup_time > now:
            notifications.append(ScheduledNotification(
                type='wakeup',
                scheduled_at=wakeup_time,
                priority=3
            ))

    return notifications


def adjust_for_quiet_hours(dt: datetime) -> datetime:
    """Adjust notification time to avoid quiet hours (11 PM - 7 AM)"""
    hour = dt.hour

    if hour >= 23 or hour < 7:
        if hour >= 23:
            # Move to 7 AM next day
            return dt.replace(hour=7, minute=0, second=0) + timedelta(days=1)
        else:
            # Move to 7 AM same day
            return dt.replace(hour=7, minute=0, second=0)

    return dt
```

### 8.3 Duplicate Prevention

```python
class DuplicateChecker:
    """Prevent duplicate notifications"""

    def __init__(self, redis: Redis):
        self.redis = redis

    async def can_send(self, passenger_id: int, notification_type: str) -> bool:
        """Check if notification can be sent (not a duplicate)"""
        key = f"notification:sent:{passenger_id}:{notification_type}"

        # Check if already sent in last 4 hours
        if await self.redis.exists(key):
            return False

        return True

    async def mark_sent(self, passenger_id: int, notification_type: str):
        """Mark notification as sent"""
        key = f"notification:sent:{passenger_id}:{notification_type}"

        # Set with 4-hour TTL
        await self.redis.set(key, "1", ex=14400)

    async def check_booking_change(self, passenger_id: int, current_data: dict) -> bool:
        """Check if booking details changed (need to re-notify)"""
        key = f"passenger:hash:{passenger_id}"

        # Create hash of relevant fields
        data_hash = hashlib.md5(
            json.dumps({
                'boarding_point': current_data['boarding_at'],
                'boarding_time': current_data['bording_date_time'],
                'seat_number': current_data['seat_number']
            }, sort_keys=True).encode()
        ).hexdigest()

        # Compare with stored hash
        stored_hash = await self.redis.get(key)

        if stored_hash and stored_hash == data_hash:
            return False  # No change

        # Store new hash
        await self.redis.set(key, data_hash, ex=86400 * 3)  # 3 days TTL

        return True  # Changed
```

---

## 9. Delivery Channels

### 9.1 WhatsApp Business API (Gupshup)

#### 9.1.1 Configuration

```yaml
Provider: Gupshup
API Base URL: https://api.gupshup.io/sm/api/v1
Authentication: API Key (header)
Rate Limit: 80 messages/second
Delivery Reports: Webhook

Required Setup:
  1. WhatsApp Business Account
  2. Gupshup Account with WhatsApp channel
  3. Approved message templates
  4. Webhook endpoint for delivery reports

Environment Variables:
  GUPSHUP_API_KEY: "sk-xxxxxxxxxxxx"
  GUPSHUP_APP_NAME: "MythriTravels"
  GUPSHUP_SOURCE_NUMBER: "917095666619"
  GUPSHUP_WEBHOOK_SECRET: "xxxxxxxxxxxx"
```

#### 9.1.2 API Endpoints

```
Send Template Message:
  POST https://api.gupshup.io/sm/api/v1/template/msg
  Headers:
    apikey: {GUPSHUP_API_KEY}
    Content-Type: application/json
  Body:
    {
      "channel": "whatsapp",
      "source": "917095666619",
      "destination": "917386681268",
      "template": {
        "id": "trip_reminder_24h",
        "params": ["Sandeep kumar", "03 Dec 2025", "Yanam", "07:45 PM", ...]
      }
    }

Send Session Message:
  POST https://api.gupshup.io/sm/api/v1/msg
  Body:
    {
      "channel": "whatsapp",
      "source": "917095666619",
      "destination": "917386681268",
      "message": {
        "type": "text",
        "text": "Your message here"
      }
    }

Webhook (Delivery Reports):
  POST /webhooks/gupshup
  Body:
    {
      "app": "MythriTravels",
      "timestamp": 1701619200000,
      "version": 2,
      "type": "message-event",
      "payload": {
        "id": "msg-xxxxx",
        "type": "delivered",
        "destination": "917386681268",
        "payload": {...}
      }
    }
```

### 9.2 SMS Gateway (MSG91)

#### 9.2.1 Configuration

```yaml
Provider: MSG91
API Base URL: https://api.msg91.com/api/v5
Authentication: authkey (header)
Rate Limit: 100 SMS/second
Delivery Reports: Webhook

Required Setup:
  1. MSG91 Account
  2. DLT Registration (for India)
  3. Approved SMS templates with DLT
  4. Sender ID registration

Environment Variables:
  MSG91_AUTH_KEY: "xxxxxxxxxxxx"
  MSG91_SENDER_ID: "MYTHRI"
  MSG91_DLT_TE_ID: "1234567890"  # Template Entity ID
  MSG91_WEBHOOK_SECRET: "xxxxxxxxxxxx"
```

#### 9.2.2 API Endpoints

```
Send SMS:
  POST https://api.msg91.com/api/v5/flow/
  Headers:
    authkey: {MSG91_AUTH_KEY}
    Content-Type: application/json
  Body:
    {
      "template_id": "xxxxxx",  # DLT Template ID
      "sender": "MYTHRI",
      "short_url": "0",
      "mobiles": "917386681268",
      "VAR1": "Sandeep kumar",
      "VAR2": "07:45 PM",
      ...
    }

Check Status:
  GET https://api.msg91.com/api/v5/report?request_id={request_id}

Webhook (Delivery Reports):
  POST /webhooks/msg91
  Body:
    {
      "request_id": "xxxx",
      "status": "delivered",
      "mobile": "917386681268",
      "timestamp": "2025-12-03 19:45:00"
    }
```

### 9.3 Fallback Logic

```python
class ChannelRouter:
    """Route messages to appropriate channel with fallback"""

    CHANNEL_PRIORITY = ['whatsapp', 'sms', 'email']

    async def send_with_fallback(
        self,
        mobile: str,
        message: Message,
        primary_channel: str = 'whatsapp'
    ) -> DeliveryResult:
        """Send message with automatic fallback"""

        channels = self._get_channel_order(primary_channel)

        for channel in channels:
            result = await self._send(channel, mobile, message)

            if result.success:
                return result

            # Log failure and try next channel
            logger.warning(
                f"Channel {channel} failed for {mobile}: {result.error}"
            )

        # All channels failed
        return DeliveryResult(
            success=False,
            error="All delivery channels failed"
        )

    def _get_channel_order(self, primary: str) -> List[str]:
        """Get ordered list of channels to try"""
        channels = [primary]
        for ch in self.CHANNEL_PRIORITY:
            if ch != primary:
                channels.append(ch)
        return channels

    async def _send(self, channel: str, mobile: str, message: Message) -> DeliveryResult:
        """Send via specific channel"""
        if channel == 'whatsapp':
            return await self.whatsapp.send(mobile, message)
        elif channel == 'sms':
            return await self.sms.send(mobile, message)
        elif channel == 'email':
            return await self.email.send(mobile, message)
        else:
            raise ValueError(f"Unknown channel: {channel}")
```

---

## 10. Security Specifications

### 10.1 Data Security

```yaml
Encryption:
  At Rest:
    - Database: PostgreSQL with TDE (Transparent Data Encryption)
    - API Keys: AES-256 encrypted, stored in secrets manager
    - Backups: Encrypted with customer-managed keys

  In Transit:
    - All APIs: TLS 1.3
    - Internal services: mTLS
    - Database connections: SSL required

PII Handling:
  - Mobile numbers: Stored as-is (required for delivery)
  - Emails: Stored as-is (optional field)
  - Names: Stored as-is
  - All PII access logged
  - Data retention: 90 days after trip completion
  - Right to deletion: Supported via API

Access Control:
  - Service-to-service: JWT with short expiry
  - External APIs: API key + IP whitelist
  - Database: Role-based access per service
  - Admin access: MFA required
```

### 10.2 API Security

```yaml
Authentication:
  Internal Services:
    Type: JWT (RS256)
    Issuer: auth-service
    Expiry: 15 minutes
    Refresh: Via refresh token (7 days)

  External Webhooks:
    Type: HMAC-SHA256 signature
    Secret: Per-provider secrets
    Timestamp validation: ±5 minutes

Rate Limiting:
  Bitla API Calls: 10 req/sec per operator
  WhatsApp Sends: 80 msg/sec
  SMS Sends: 100 msg/sec
  Webhook Ingestion: 1000 req/sec

Input Validation:
  - All inputs sanitized
  - Mobile number format validation
  - Template variable escaping
  - SQL injection prevention (parameterized queries)
  - No user-supplied content in templates
```

### 10.3 Audit Logging

```yaml
Logged Events:
  - All API calls (request/response)
  - All database writes
  - All notification sends
  - All delivery status updates
  - Authentication events
  - Configuration changes
  - Error events

Log Format:
  {
    "timestamp": "2025-12-03T19:45:00Z",
    "service": "delivery",
    "event_type": "notification_sent",
    "correlation_id": "uuid",
    "data": {
      "notification_id": 12345,
      "passenger_id": 67890,
      "channel": "whatsapp",
      "status": "sent"
    },
    "metadata": {
      "ip": "10.0.0.1",
      "user_agent": "service/1.0"
    }
  }

Retention:
  - Application logs: 30 days (CloudWatch/ELK)
  - Audit logs: 1 year (S3/Glacier)
  - Security logs: 2 years
```

---

## 11. Error Handling & Retry Logic

### 11.1 Error Categories

```yaml
Transient Errors (Retry):
  - Network timeout
  - Rate limit exceeded (429)
  - Service temporarily unavailable (503)
  - Gateway timeout (504)
  - WhatsApp/SMS provider temporary failure

Permanent Errors (No Retry):
  - Invalid phone number (400)
  - Authentication failed (401)
  - Forbidden / blocked number (403)
  - Template not found (404)
  - Invalid template parameters
  - Phone number opted out

Business Errors (Special Handling):
  - Passenger cancelled booking
  - Trip cancelled
  - Duplicate notification
  - Outside notification window
```

### 11.2 Retry Strategy

```python
RETRY_CONFIG = {
    'max_retries': 3,
    'initial_delay': 60,  # 1 minute
    'max_delay': 900,  # 15 minutes
    'exponential_base': 2,
    'jitter': True
}

def calculate_retry_delay(attempt: int) -> int:
    """Calculate delay for retry attempt with exponential backoff"""
    delay = RETRY_CONFIG['initial_delay'] * (RETRY_CONFIG['exponential_base'] ** attempt)
    delay = min(delay, RETRY_CONFIG['max_delay'])

    if RETRY_CONFIG['jitter']:
        # Add random jitter (±20%)
        jitter = delay * 0.2 * (random.random() * 2 - 1)
        delay = delay + jitter

    return int(delay)

# Retry delays: 60s, 120s, 240s (then fail)
```

### 11.3 Circuit Breaker

```python
class CircuitBreaker:
    """Circuit breaker for external service calls"""

    STATES = ['closed', 'open', 'half_open']

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,
        half_open_requests: int = 3
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_requests = half_open_requests
        self.state = 'closed'
        self.failures = 0
        self.last_failure_time = None
        self.half_open_successes = 0

    async def call(self, func, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == 'open':
            if self._should_try_recovery():
                self.state = 'half_open'
                self.half_open_successes = 0
            else:
                raise CircuitOpenError("Circuit is open")

        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        if self.state == 'half_open':
            self.half_open_successes += 1
            if self.half_open_successes >= self.half_open_requests:
                self.state = 'closed'
                self.failures = 0
        else:
            self.failures = 0

    def _on_failure(self):
        self.failures += 1
        self.last_failure_time = time.time()

        if self.failures >= self.failure_threshold:
            self.state = 'open'

    def _should_try_recovery(self) -> bool:
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.recovery_timeout
```

---

## 12. Monitoring & Observability

### 12.1 Metrics

```yaml
Business Metrics:
  - notifications_scheduled_total (counter)
      Labels: operator, type
  - notifications_sent_total (counter)
      Labels: operator, type, channel, status
  - notifications_delivered_total (counter)
      Labels: operator, type, channel
  - notifications_failed_total (counter)
      Labels: operator, type, channel, error_type
  - notification_delivery_time_seconds (histogram)
      Labels: operator, type, channel

Technical Metrics:
  - api_requests_total (counter)
      Labels: service, endpoint, method, status
  - api_request_duration_seconds (histogram)
      Labels: service, endpoint
  - queue_depth (gauge)
      Labels: queue_name
  - queue_processing_time_seconds (histogram)
      Labels: queue_name
  - database_connections_active (gauge)
  - database_query_duration_seconds (histogram)

External Service Metrics:
  - bitla_api_requests_total (counter)
      Labels: endpoint, status
  - bitla_api_latency_seconds (histogram)
  - whatsapp_api_requests_total (counter)
      Labels: status
  - sms_api_requests_total (counter)
      Labels: status
```

### 12.2 Dashboards

```yaml
Dashboard: PCS Operations
Panels:
  1. Notifications Overview (24h)
     - Total scheduled
     - Total sent
     - Total delivered
     - Total failed
     - Delivery rate %

  2. Notifications by Type
     - 24h reminders
     - 2h reminders
     - Wake-up calls

  3. Channel Performance
     - WhatsApp delivery rate
     - SMS delivery rate
     - Fallback rate

  4. Queue Status
     - Pending notifications
     - Processing rate
     - Queue depth trend

  5. API Health
     - Bitla API success rate
     - WhatsApp API success rate
     - SMS API success rate

  6. Error Analysis
     - Errors by type
     - Errors by channel
     - Retry attempts

Dashboard: PCS Technical
Panels:
  1. Service Health
     - Uptime per service
     - Error rates
     - Response times

  2. Resource Usage
     - CPU utilization
     - Memory usage
     - Database connections

  3. Queue Metrics
     - Enqueue rate
     - Dequeue rate
     - Dead letter queue size

  4. External Dependencies
     - Bitla API latency
     - WhatsApp API latency
     - SMS API latency
```

### 12.3 Alerts

```yaml
Critical Alerts (Page immediately):
  - Service down (any service unreachable > 2 min)
  - Delivery rate < 50% (30 min window)
  - Queue processing stopped (no dequeue > 10 min)
  - Database connection failures
  - All retries exhausted rate > 10%

Warning Alerts (Notify team):
  - Delivery rate < 80% (1h window)
  - Queue depth > 10000
  - API latency > 5s (p95)
  - Error rate > 5%
  - Bitla API failures > 10 consecutive

Info Alerts (Log only):
  - High queue depth (> 5000)
  - Elevated latency (p95 > 2s)
  - Individual message failures
```

---

## 13. Deployment Architecture

### 13.1 Infrastructure Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        AWS DEPLOYMENT ARCHITECTURE                           │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────┐
                              │   Route 53      │
                              │   (DNS)         │
                              └────────┬────────┘
                                       │
                              ┌────────▼────────┐
                              │  CloudFront     │
                              │  (CDN)          │
                              └────────┬────────┘
                                       │
                              ┌────────▼────────┐
                              │      ALB        │
                              │ (Load Balancer) │
                              └────────┬────────┘
                                       │
┌──────────────────────────────────────┼──────────────────────────────────────┐
│                            VPC                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        Public Subnet                                    │ │
│  │  ┌─────────────────┐                      ┌─────────────────┐          │ │
│  │  │   NAT Gateway   │                      │   Bastion Host  │          │ │
│  │  └─────────────────┘                      └─────────────────┘          │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        Private Subnet (AZ-a)                           │ │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                       │ │
│  │  │   ECS      │  │   ECS      │  │   ECS      │                       │ │
│  │  │ data-fetch │  │ scheduler  │  │ delivery   │                       │ │
│  │  └────────────┘  └────────────┘  └────────────┘                       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        Private Subnet (AZ-b)                           │ │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐                       │ │
│  │  │   ECS      │  │   ECS      │  │   ECS      │                       │ │
│  │  │ data-fetch │  │ scheduler  │  │ delivery   │                       │ │
│  │  └────────────┘  └────────────┘  └────────────┘                       │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │                        Data Subnet                                      │ │
│  │  ┌────────────────────┐  ┌────────────────────┐                        │ │
│  │  │     RDS            │  │   ElastiCache      │                        │ │
│  │  │   PostgreSQL       │  │     Redis          │                        │ │
│  │  │  (Multi-AZ)        │  │   (Cluster)        │                        │ │
│  │  └────────────────────┘  └────────────────────┘                        │ │
│  └────────────────────────────────────────────────────────────────────────┘ │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

External Services:
  ├── Gupshup WhatsApp API
  ├── MSG91 SMS API
  └── Bitla ticketSimply API
```

### 13.2 Container Specifications

```yaml
# data-fetcher service
Service: data-fetcher
Image: pcs/data-fetcher:latest
CPU: 512
Memory: 1024MB
Min Instances: 2
Max Instances: 4
Health Check: /health
Environment:
  - DATABASE_URL
  - REDIS_URL
  - BITLA_API_BASE_URL
  - LOG_LEVEL=INFO

# scheduler service
Service: scheduler
Image: pcs/scheduler:latest
CPU: 256
Memory: 512MB
Min Instances: 2
Max Instances: 2  # Fixed (cron-based)
Health Check: /health
Environment:
  - DATABASE_URL
  - REDIS_URL
  - LOG_LEVEL=INFO

# delivery service
Service: delivery
Image: pcs/delivery:latest
CPU: 512
Memory: 1024MB
Min Instances: 2
Max Instances: 8  # Auto-scale based on queue
Health Check: /health
Environment:
  - DATABASE_URL
  - REDIS_URL
  - GUPSHUP_API_KEY
  - MSG91_AUTH_KEY
  - LOG_LEVEL=INFO
Auto Scaling:
  - Target: SQS Queue Depth
  - Scale Up: Queue > 1000
  - Scale Down: Queue < 100
```

### 13.3 Database Specifications

```yaml
PostgreSQL (RDS):
  Engine: PostgreSQL 15
  Instance Class: db.t3.medium (MVP), db.r6g.large (Production)
  Storage: 100GB gp3
  Multi-AZ: Yes
  Backup Retention: 7 days
  Encryption: Yes (KMS)
  Performance Insights: Enabled

Redis (ElastiCache):
  Engine: Redis 7.0
  Node Type: cache.t3.medium
  Cluster Mode: Disabled (MVP), Enabled (Production)
  Replicas: 1
  Encryption: In-transit and at-rest
```

---

## 14. Technology Stack

### 14.1 Complete Stack Summary

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          TECHNOLOGY STACK                                    │
└─────────────────────────────────────────────────────────────────────────────┘

BACKEND SERVICES
├── Language: Python 3.11+
├── Framework: FastAPI
├── ORM: SQLAlchemy 2.0 + Alembic (migrations)
├── Task Queue: Celery + Redis
├── Scheduling: APScheduler
├── HTTP Client: httpx (async)
└── Validation: Pydantic v2

DATA STORES
├── Primary DB: PostgreSQL 15
├── Cache/Queue: Redis 7
└── Object Storage: S3 / MinIO

MESSAGE PROVIDERS
├── WhatsApp: Gupshup Business API
├── SMS: MSG91
└── Email: AWS SES (future)

INFRASTRUCTURE
├── Cloud: AWS
├── Container: Docker
├── Orchestration: ECS Fargate
├── Load Balancer: ALB
├── CDN: CloudFront
└── DNS: Route 53

OBSERVABILITY
├── Metrics: Prometheus + Grafana
├── Logs: CloudWatch Logs / ELK
├── Tracing: AWS X-Ray / Jaeger
└── Alerts: PagerDuty / Opsgenie

CI/CD
├── Source Control: GitHub
├── CI: GitHub Actions
├── CD: AWS CodePipeline
└── IaC: Terraform

DEVELOPMENT
├── IDE: VS Code
├── API Docs: OpenAPI/Swagger
├── Testing: pytest + pytest-asyncio
└── Linting: ruff + black
```

### 14.2 Python Dependencies

```txt
# requirements.txt

# Web Framework
fastapi==0.104.1
uvicorn[standard]==0.24.0
pydantic==2.5.2
pydantic-settings==2.1.0

# Database
sqlalchemy==2.0.23
asyncpg==0.29.0
alembic==1.12.1

# Redis
redis==5.0.1
aioredis==2.0.1

# HTTP Client
httpx==0.25.2

# Task Queue
celery==5.3.4
celery[redis]==5.3.4

# Scheduling
apscheduler==3.10.4

# Template Engine
jinja2==3.1.2

# Utilities
python-dateutil==2.8.2
pytz==2023.3
phonenumbers==8.13.26

# Security
cryptography==41.0.7
python-jose[cryptography]==3.3.0
passlib[bcrypt]==1.7.4

# Observability
prometheus-client==0.19.0
structlog==23.2.0
opentelemetry-api==1.21.0
opentelemetry-sdk==1.21.0

# Testing
pytest==7.4.3
pytest-asyncio==0.21.1
pytest-cov==4.1.0
httpx-mock==0.7.0
factory-boy==3.3.0

# Development
black==23.11.0
ruff==0.1.6
mypy==1.7.1
pre-commit==3.6.0
```

---

## 15. Development Phases

### 15.1 Phase Breakdown

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DEVELOPMENT PHASES                                   │
└─────────────────────────────────────────────────────────────────────────────┘

PHASE 1: Foundation (Week 1-2)
├── Day 1-2: Project setup, CI/CD pipeline
├── Day 3-4: Database schema, migrations
├── Day 5-6: Bitla API adapter
├── Day 7-8: Basic data sync service
├── Day 9-10: Unit tests, documentation
└── Deliverable: Data fetching working

PHASE 2: Scheduling (Week 3)
├── Day 1-2: Notification scheduling logic
├── Day 3-4: Redis queue setup
├── Day 5-6: Duplicate prevention
├── Day 7: Testing & bug fixes
└── Deliverable: Notifications scheduled correctly

PHASE 3: Delivery (Week 4)
├── Day 1-2: Template engine
├── Day 3-4: WhatsApp integration (Gupshup)
├── Day 5-6: SMS integration (MSG91)
├── Day 7: Fallback logic, retry handling
└── Deliverable: Messages sent via WhatsApp/SMS

PHASE 4: Polish & Deploy (Week 5)
├── Day 1-2: Webhook handlers for delivery status
├── Day 3-4: Monitoring, dashboards, alerts
├── Day 5-6: Load testing, performance tuning
├── Day 7: Production deployment
└── Deliverable: System live with Mythri Travels

PHASE 5: Pilot & Iterate (Week 6+)
├── Monitor pilot operation
├── Gather feedback
├── Bug fixes and improvements
├── Documentation updates
└── Deliverable: Stable production system
```

### 15.2 Milestones

| Milestone | Target Date | Criteria |
|-----------|-------------|----------|
| M1: Data Sync Working | Week 2 | Successfully fetching & storing Mythri data |
| M2: Scheduling Working | Week 3 | Notifications scheduled correctly in queue |
| M3: Delivery Working | Week 4 | WhatsApp & SMS messages being sent |
| M4: Production Ready | Week 5 | Deployed, monitored, documented |
| M5: Pilot Complete | Week 8 | 2 weeks of stable operation |

### 15.3 Resource Requirements

```yaml
Development Team:
  Backend Developer: 1 (full-time)
  DevOps Engineer: 0.5 (part-time)
  QA Engineer: 0.5 (part-time, Week 4-5)

Infrastructure (Monthly Cost Estimate):
  ECS Fargate: $150-200
  RDS PostgreSQL: $100-150
  ElastiCache Redis: $50-75
  ALB: $25
  CloudWatch: $25
  S3: $10
  Total: ~$400-500/month (MVP)

External Services (Monthly):
  Gupshup WhatsApp: Pay per message (~$0.004/msg)
  MSG91 SMS: Pay per SMS (~$0.01/SMS)
  Estimated for 1000 passengers/day: $300-500/month
```

---

## 16. API Reference

### 16.1 Internal APIs

#### Data Fetcher Service

```yaml
Base URL: http://data-fetcher:8001

Endpoints:

GET /health
  Response: { "status": "healthy", "timestamp": "..." }

POST /sync/operator/{operator_id}
  Description: Trigger full sync for operator
  Response: { "job_id": "uuid", "status": "started" }

POST /sync/route/{route_id}
  Description: Sync specific route
  Query Params:
    - date (optional): YYYY-MM-DD
  Response: { "job_id": "uuid", "status": "started" }

GET /sync/status/{job_id}
  Description: Get sync job status
  Response: {
    "job_id": "uuid",
    "status": "completed|running|failed",
    "progress": { "routes": 10, "passengers": 150 },
    "errors": []
  }

GET /operators
  Description: List all operators
  Response: [{ "id": 1, "name": "Mythri Travels", ... }]

GET /operators/{id}/routes
  Description: List routes for operator
  Response: [{ "id": 1, "route_num": "MYTHRI-001", ... }]

GET /trips
  Description: List trips
  Query Params:
    - operator_id
    - route_id
    - date_from
    - date_to
  Response: [{ "id": 1, "travel_date": "2025-12-03", ... }]
```

#### Scheduler Service

```yaml
Base URL: http://scheduler:8002

Endpoints:

GET /health
  Response: { "status": "healthy", "timestamp": "..." }

GET /notifications
  Description: List notifications
  Query Params:
    - status: pending|sent|delivered|failed
    - type: reminder_24h|reminder_2h|wakeup
    - date_from
    - date_to
    - limit
    - offset
  Response: {
    "total": 1000,
    "items": [{ "id": 1, "passenger_id": 123, ... }]
  }

POST /notifications/{id}/retry
  Description: Retry failed notification
  Response: { "status": "queued" }

POST /notifications/{id}/cancel
  Description: Cancel pending notification
  Response: { "status": "cancelled" }

GET /stats
  Description: Get notification statistics
  Query Params:
    - date_from
    - date_to
  Response: {
    "total_scheduled": 1000,
    "total_sent": 950,
    "total_delivered": 920,
    "total_failed": 30,
    "by_type": { ... },
    "by_channel": { ... }
  }
```

#### Delivery Service

```yaml
Base URL: http://delivery:8003

Endpoints:

GET /health
  Response: { "status": "healthy", "timestamp": "..." }

POST /send
  Description: Send notification immediately
  Body: {
    "passenger_id": 123,
    "notification_type": "reminder_24h",
    "channel": "whatsapp"
  }
  Response: { "message_id": "...", "status": "sent" }

POST /webhooks/gupshup
  Description: Gupshup delivery webhook
  Body: { ... gupshup payload ... }
  Response: { "status": "ok" }

POST /webhooks/msg91
  Description: MSG91 delivery webhook
  Body: { ... msg91 payload ... }
  Response: { "status": "ok" }

GET /templates
  Description: List message templates
  Query Params:
    - operator_id
    - type
    - channel
    - language
  Response: [{ "id": 1, "template_code": "reminder_24h_en", ... }]

POST /templates
  Description: Create/update template
  Body: {
    "operator_id": 1,
    "template_code": "reminder_24h_en",
    "notification_type": "reminder_24h",
    "channel": "whatsapp",
    "language": "en",
    "body": "..."
  }
  Response: { "id": 1, ... }
```

---

## 17. Appendices

### Appendix A: Environment Variables

```bash
# Application
APP_ENV=production
APP_DEBUG=false
LOG_LEVEL=INFO
SECRET_KEY=your-secret-key

# Database
DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/pcs
DATABASE_POOL_SIZE=10
DATABASE_MAX_OVERFLOW=20

# Redis
REDIS_URL=redis://host:6379/0
REDIS_QUEUE_DB=1

# Bitla API (per operator - stored in DB)
# BITLA_BASE_URL=http://myth.mythribus.com/api
# BITLA_API_KEY=encrypted-in-db

# WhatsApp (Gupshup)
GUPSHUP_API_KEY=your-api-key
GUPSHUP_APP_NAME=MythriTravels
GUPSHUP_SOURCE_NUMBER=917095666619
GUPSHUP_WEBHOOK_SECRET=your-webhook-secret

# SMS (MSG91)
MSG91_AUTH_KEY=your-auth-key
MSG91_SENDER_ID=MYTHRI
MSG91_DLT_TE_ID=1234567890

# AWS
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key

# Monitoring
PROMETHEUS_ENABLED=true
PROMETHEUS_PORT=9090
```

### Appendix B: Sample Cron Schedule

```yaml
# Cron Jobs Configuration

data_sync_today:
  schedule: "*/15 * * * *"  # Every 15 minutes
  description: Sync today's passenger data
  command: sync_passengers --date=today

data_sync_tomorrow:
  schedule: "0 */2 * * *"  # Every 2 hours
  description: Sync tomorrow's passenger data
  command: sync_passengers --date=tomorrow

data_sync_day_after:
  schedule: "0 6,12,18 * * *"  # 3 times daily
  description: Sync day-after-tomorrow data
  command: sync_passengers --date=+2

route_refresh:
  schedule: "0 */6 * * *"  # Every 6 hours
  description: Refresh route list
  command: sync_routes

notification_processor:
  schedule: "* * * * *"  # Every minute
  description: Process pending notifications
  command: process_notifications

cleanup_old_data:
  schedule: "0 3 * * *"  # Daily at 3 AM
  description: Clean up old data (>90 days)
  command: cleanup --days=90

health_check:
  schedule: "*/5 * * * *"  # Every 5 minutes
  description: System health check
  command: health_check --alert-on-failure
```

### Appendix C: WhatsApp Template Registration

```yaml
# Templates to register with WhatsApp Business API

Template 1: trip_reminder_24h
  Category: UTILITY
  Language: en
  Header: None
  Body: |
    🚌 *{{1}} - Trip Reminder*

    Hello {{2}},

    Your bus journey is *tomorrow*!

    📅 *Date:* {{3}}
    🚌 *Route:* {{4}} → {{5}}

    *Boarding Details:*
    📍 Point: {{6}}
    ⏰ Time: {{7}}
    📌 Address: {{8}}
    🗺️ Location: {{9}}

    *Your Booking:*
    🎫 PNR: {{10}}
    💺 Seat: {{11}}
    🚌 Bus: {{12}}

    👨‍✈️ Driver: {{13}} - {{14}}
    📞 Helpline: {{15}}

    Have a safe journey! 🙏
  Footer: None
  Buttons: None

Template 2: trip_reminder_2h
  Category: UTILITY
  Language: en
  Body: |
    🚌 *{{1}} - Departing Soon!*

    Hi {{2}},

    Your bus departs in *2 hours*!

    ⏰ Departure: {{3}} from {{4}}
    🚌 Bus No: {{5}}
    💺 Your Seat: {{6}}

    📍 *Reach here:*
    {{7}}
    {{8}}

    📞 Need help? Call {{9}}

    See you soon! 🚌
```

### Appendix D: Glossary

| Term | Definition |
|------|------------|
| PCS | Passenger Communication System |
| PNR | Passenger Name Record (booking reference) |
| OTA | Online Travel Agent (RedBus, Paytm, etc.) |
| BP | Boarding Point |
| DP | Drop Point |
| DLT | Distributed Ledger Technology (SMS registration in India) |
| WABA | WhatsApp Business API |
| TTL | Time To Live |
| DLQ | Dead Letter Queue |

---

## Document History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | Dec 2024 | Technical Team | Initial version |

---

**Document End**

*Technical Specification Document v1.0*
*Passenger Communication System - MVP*
*December 2024*
