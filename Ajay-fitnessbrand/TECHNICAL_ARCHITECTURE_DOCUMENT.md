# Health Band Software - Technical Architecture

---

## Tech Stack

| Layer | Technology |
|-------|------------|
| iOS App | Swift + SwiftUI |
| Android App | Kotlin + Jetpack Compose |
| Web Dashboard | Next.js (React) |
| Backend API | Node.js + Express.js |
| Database | PostgreSQL |
| Cache | Redis |
| Real-time | WebSocket (Socket.io) |
| Push Notifications | Firebase Cloud Messaging |
| File Storage | AWS S3 / Cloudinary |

---

## System Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    MOBILE APPS                          │
│         iOS (Swift)  +  Android (Kotlin)                │
└──────────────────────┬──────────────────────────────────┘
                       │ HTTPS / WebSocket
                       ▼
┌─────────────────────────────────────────────────────────┐
│                 BACKEND API (Node.js)                   │
│   Auth | Users | Health | Devices | Alerts | Reports   │
└──────────────────────┬──────────────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│ PostgreSQL  │ │    Redis    │ │ File Storage│
│  (Primary)  │ │   (Cache)   │ │    (S3)     │
└─────────────┘ └─────────────┘ └─────────────┘
```

---

## Device Integration

| Device | Integration Method |
|--------|-------------------|
| QC Band | Direct SDK (QCBandSDK.framework / qc_sdk.aar) |
| Apple Watch | Apple HealthKit API |
| Samsung Watch | Health Connect API |

---

## API Endpoints Summary

| Category | Endpoints |
|----------|-----------|
| Auth | POST /auth/register, /login, /logout, /refresh |
| Users | GET/PUT /users/me |
| Health Profile | GET/POST/PUT /health-profile, /health-profile/conditions |
| Devices | GET/POST/DELETE /devices |
| Health Data | POST /health/sync, GET /health/metrics, /heart-rate, /sleep |
| Alerts | GET /alerts, PUT /alerts/:id/acknowledge |
| Recommendations | GET /recommendations, /insights |
| Admin | GET /admin/users, /admin/analytics |

---

## Database Tables

| Table | Purpose |
|-------|---------|
| users | User accounts & profiles |
| health_conditions | Body map conditions (migraine, heart, etc.) |
| devices | Paired devices |
| health_metrics | All health data (HR, BP, SpO2, sleep, steps) |
| alerts | Generated alerts |
| alert_rules | User's alert thresholds |
| recommendations | AI-generated recommendations |

---

## Authentication

- **Method:** JWT (Access Token + Refresh Token)
- **Access Token Expiry:** 15 minutes
- **Refresh Token Expiry:** 7 days
- **Password:** bcrypt hashing (12 rounds)

---

## Intelligence Engine Flow

```
User Health Profile + Device Data
            ↓
    Rule Engine (Thresholds)
            ↓
    Pattern Analyzer (Trends)
            ↓
   ┌────────┼────────┐
   ↓        ↓        ↓
ALERTS  INSIGHTS  RECOMMENDATIONS
```

---

## Security

| Layer | Implementation |
|-------|---------------|
| Transport | HTTPS/TLS |
| Auth | JWT + Refresh Tokens |
| Passwords | bcrypt |
| API | Rate limiting (100 req/min) |
| Data | Encryption at rest |

---

## Deployment

| Component | Platform |
|-----------|----------|
| Backend | AWS / DigitalOcean / Railway |
| Database | Managed PostgreSQL |
| Cache | Managed Redis |
| Files | S3 / Cloudinary |
| iOS App | App Store |
| Android App | Play Store |

---

*Document Version 1.0 | December 2025*
