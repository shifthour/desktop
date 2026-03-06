# Health Band Software - Overview

---

## Core Concept

**Intelligent health platform** that combines:
1. User's health profile (body map conditions)
2. Real-time device data (QC Band / Apple Watch / Samsung Watch)
3. Intelligence engine → Personalized alerts, insights & recommendations

---

## User Flow

```
Sign Up → Body Map (select conditions) → Pair Device → Sync Data → Get Personalized Health Insights
```

---

## Supported Devices

| Device | Connection Method | Data Access |
|--------|------------------|-------------|
| QC Band | Direct SDK (Bluetooth) | Real-time |
| Apple Watch | Apple HealthKit API | Periodic sync |
| Samsung Watch | Health Connect API | Periodic sync |

---

## Health Metrics Collected

| Metric | QC Band | Apple Watch | Samsung Watch |
|--------|---------|-------------|---------------|
| Heart Rate | ✅ | ✅ | ✅ |
| Blood Pressure | ✅ | ✅ | ✅ |
| Blood Oxygen (SpO2) | ✅ | ✅ | ✅ |
| Sleep | ✅ | ✅ | ✅ |
| Steps/Calories | ✅ | ✅ | ✅ |
| Temperature | ✅ | - | - |
| Stress/HRV | ✅ | ✅ | - |

---

## Intelligence Engine Outputs

| Output Type | Example |
|-------------|---------|
| **Alerts** | "High HR detected. Given your heart condition, please rest." |
| **Insights** | "Poor sleep pattern may trigger your migraines." |
| **Recommendations** | "With kidney issues, increase water intake today." |

---

## SDK Status

| Platform | SDK | Documentation | Sample Code |
|----------|-----|---------------|-------------|
| iOS | ✅ QCBandSDK.framework | ✅ 22-page PDF | ✅ Demo project |
| Android | ✅ qc_sdk.aar | ✅ 38-page PDF | ✅ SDKSample.zip |

---

## System Components

```
Mobile Apps (iOS/Android)
        ↓
   Backend API
        ↓
┌───────┼───────┐
↓       ↓       ↓
Database  Cache  Storage
```

**User Side:** Band ↔ Mobile App (Bluetooth)
**Server Side:** API Server ↔ Database
**Admin Side:** Web Dashboard

---

## Infrastructure Costs (Monthly)

| Users | Cost (INR) |
|-------|------------|
| 1-100 | ₹0 - ₹2,500 |
| 100-1,000 | ₹2,500 - ₹8,000 |
| 1,000-10,000 | ₹8,000 - ₹20,000 |
| 10,000+ | ₹30,000+ |

---

## File Locations

| Item | Path |
|------|------|
| iOS SDK | /QCBandSDKDemo/QCBandSDK.framework |
| iOS Docs | /QCBandSDKDemo/iOS SDK Development Guide.pdf |
| Android SDK | /ANDROID_SDK/qc_sdk.aar |
| Android Docs | /ANDROID_SDK/sdk_en.pdf |

---

*Document Version 1.0 | December 2025*
