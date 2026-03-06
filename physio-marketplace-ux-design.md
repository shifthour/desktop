# PhysioConnect — UX Design Document
## Physiotherapy Marketplace Web Application

---

## 1. SITEMAP

```
PhysioConnect
│
├── PUBLIC PAGES
│   ├── / ............................ Home (Landing)
│   ├── /search ..................... Search & Listing (filterable)
│   ├── /physio/:slug .............. Physiotherapist Profile
│   ├── /book/:physioId ............ Booking Flow (multi-step)
│   │   ├── Step 1: Select Service
│   │   ├── Step 2: Pick Date & Time
│   │   ├── Step 3: Patient Details
│   │   └── Step 4: Confirm & Pay
│   ├── /booking/confirmed/:id ..... Booking Confirmation
│   ├── /about ..................... About / How It Works
│   └── /contact .................. Contact / Support
│
├── PATIENT PORTAL (/patient)
│   ├── /patient/dashboard ........ Upcoming & Past Bookings
│   ├── /patient/bookings/:id ..... Booking Detail
│   ├── /patient/profile .......... Edit Patient Profile
│   ├── /patient/favorites ........ Saved Physiotherapists
│   └── /patient/reviews .......... My Reviews
│
├── PHYSIOTHERAPIST PORTAL (/provider)
│   ├── /provider/onboarding ...... Registration & Profile Setup
│   ├── /provider/dashboard ....... Overview / Schedule
│   ├── /provider/profile ......... Edit Public Profile
│   ├── /provider/availability .... Manage Time Slots (Calendar)
│   ├── /provider/bookings ........ Booking Management
│   ├── /provider/patients ........ Patient History
│   └── /provider/earnings ........ Payments & Payout
│
├── ADMIN PANEL (/admin)
│   ├── /admin/dashboard .......... Analytics Overview
│   ├── /admin/providers .......... Physiotherapist Approvals
│   ├── /admin/providers/:id ...... Provider Detail / Approve / Reject
│   ├── /admin/bookings ........... All Bookings
│   ├── /admin/time-slots ......... Block / Override Slots
│   ├── /admin/users .............. Patient Management
│   ├── /admin/reviews ............ Review Moderation
│   └── /admin/reports ............ Revenue, Usage Reports
│
└── AUTH
    ├── /login .................... Unified Login (role-routed)
    ├── /register ................. Patient Registration
    ├── /register/physio .......... Physiotherapist Registration
    └── /forgot-password .......... Password Recovery
```

---

## 2. WIREFRAME DESCRIPTIONS

### 2.1 HOME PAGE (/)

```
┌─────────────────────────────────────────────────────┐
│  NAVBAR                                             │
│  [Logo]           [Search] [Login] [Register ▾]     │
├─────────────────────────────────────────────────────┤
│                                                     │
│  HERO SECTION                                       │
│  ┌───────────────────────────────────────────────┐  │
│  │  "Expert Physiotherapy, One Click Away"       │  │
│  │                                               │  │
│  │  ┌─────────────────────────────────────────┐  │  │
│  │  │ 🔍 [Specialization ▾] [Location] [Go]  │  │  │
│  │  └─────────────────────────────────────────┘  │  │
│  │                                               │  │
│  │  Popular: Sports Injury · Back Pain · Neuro   │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  HOW IT WORKS (3-step horizontal strip)             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐          │
│  │ 1. Search│  │ 2. Book  │  │ 3. Heal  │          │
│  │ Find your│  │ Pick a   │  │ Attend   │          │
│  │ physio   │  │ time slot│  │ session  │          │
│  └──────────┘  └──────────┘  └──────────┘          │
│                                                     │
│  TOP-RATED PHYSIOTHERAPISTS                         │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐               │
│  │Photo │ │Photo │ │Photo │ │Photo │               │
│  │Name  │ │Name  │ │Name  │ │Name  │               │
│  │★ 4.9 │ │★ 4.8 │ │★ 4.8 │ │★ 4.7 │               │
│  │Spec. │ │Spec. │ │Spec. │ │Spec. │               │
│  │₹ Fee │ │₹ Fee │ │₹ Fee │ │₹ Fee │               │
│  └──────┘ └──────┘ └──────┘ └──────┘               │
│  (horizontal scroll on mobile)                      │
│                                                     │
│  BROWSE BY SPECIALIZATION                           │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐       │
│  │ Sports │ │ Neuro  │ │ Ortho  │ │Pediatr.│       │
│  │ Injury │ │ Rehab  │ │        │ │        │       │
│  └────────┘ └────────┘ └────────┘ └────────┘       │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐       │
│  │Geriatr.│ │Cardio  │ │Women's │ │ Post-  │       │
│  │        │ │ Pulm.  │ │ Health │ │ Surg.  │       │
│  └────────┘ └────────┘ └────────┘ └────────┘       │
│                                                     │
│  TRUST INDICATORS                                   │
│  ┌───────────────────────────────────────────────┐  │
│  │ ✓ Verified    ✓ 10,000+     ✓ Secure         │  │
│  │   Providers     Sessions      Payments        │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  PATIENT TESTIMONIALS (carousel)                    │
│  ┌───────────────────────────────────────────────┐  │
│  │  "After my knee surgery, Dr. Sharma's rehab   │  │
│  │   program got me back to running in 8 weeks." │  │
│  │   — Priya M. ★★★★★                           │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  FOOTER                                             │
│  About | Contact | For Physiotherapists | Privacy   │
│  Terms | © 2026 PhysioConnect                       │
└─────────────────────────────────────────────────────┘
```

**Mobile adaptation:** Hero search stacks vertically. Provider cards become a horizontal swipeable carousel. Specialization grid collapses to 2 columns.

---

### 2.2 SEARCH & LISTING PAGE (/search)

```
┌─────────────────────────────────────────────────────┐
│  NAVBAR (sticky)                                    │
├─────────────────────────────────────────────────────┤
│                                                     │
│  SEARCH BAR (sticky below nav on scroll)            │
│  ┌─────────────────────────────────────────────┐    │
│  │ [Specialization ▾] [Location 📍] [Date ▾]  │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  ┌──────────────┬──────────────────────────────┐    │
│  │ FILTERS      │  RESULTS                     │    │
│  │ (sidebar)    │                               │    │
│  │              │  Showing 47 physiotherapists   │    │
│  │ Specializ.   │  Sort: [Relevance ▾]          │    │
│  │ ☑ Sports     │                               │    │
│  │ ☐ Neuro      │  ┌───────────────────────────┐│    │
│  │ ☐ Ortho      │  │ ┌─────┐                   ││    │
│  │ ☐ Pediatric  │  │ │Photo│ Dr. Ananya Sharma  ││    │
│  │ ☐ Geriatric  │  │ │     │ Sports & Ortho     ││    │
│  │              │  │ └─────┘ ★ 4.9 (127 reviews)││    │
│  │ Visit Type   │  │ 📍 Koramangala, 2.3 km    ││    │
│  │ ○ Any        │  │ 🏥 Clinic · 🏠 Home Visit ││    │
│  │ ● Clinic     │  │ 💰 ₹800 / session          ││    │
│  │ ○ Home Visit │  │ ✅ Next: Today, 4:00 PM    ││    │
│  │              │  │ [View Profile] [Book Now]  ││    │
│  │ Price Range  │  └───────────────────────────┘│    │
│  │ ₹300 ━━●━━ ₹│  ┌───────────────────────────┐│    │
│  │ 3000         │  │ (next result card...)      ││    │
│  │              │  └───────────────────────────┘│    │
│  │ Availability │  ┌───────────────────────────┐│    │
│  │ ☐ Today      │  │ (next result card...)      ││    │
│  │ ☐ Tomorrow   │  └───────────────────────────┘│    │
│  │ ☐ This Week  │                               │    │
│  │              │  PAGINATION                   │    │
│  │ Rating       │  [< 1 2 3 ... 5 >]           │    │
│  │ ★★★★☆ & up  │                               │    │
│  │              │                               │    │
│  │ Gender       │                               │    │
│  │ ○ Any        │                               │    │
│  │ ○ Male       │                               │    │
│  │ ○ Female     │                               │    │
│  │              │                               │    │
│  │ [Clear All]  │                               │    │
│  └──────────────┴──────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
```

**Mobile adaptation:**
- Filters collapse into a bottom sheet triggered by a floating "Filter" button
- Each filter group is an accordion section inside the sheet
- Results are single-column cards
- "Book Now" button is prominent on each card
- Infinite scroll replaces pagination
- Map toggle available for location-based browsing

---

### 2.3 PHYSIOTHERAPIST PROFILE PAGE (/physio/:slug)

```
┌─────────────────────────────────────────────────────┐
│  NAVBAR                                             │
├─────────────────────────────────────────────────────┤
│  BREADCRUMB: Home > Sports Injury > Dr. A. Sharma   │
│                                                     │
│  PROFILE HEADER                                     │
│  ┌───────────────────────────────────────────────┐  │
│  │  ┌────────┐                                   │  │
│  │  │        │  Dr. Ananya Sharma, MPT            │  │
│  │  │ PHOTO  │  Sports Injury & Orthopedic Rehab  │  │
│  │  │        │  ★ 4.9 (127 reviews) · 8 yrs exp  │  │
│  │  └────────┘                                   │  │
│  │  📍 Koramangala, Bangalore                    │  │
│  │  🏥 Clinic Visit  ·  🏠 Home Visit Available  │  │
│  │                                               │  │
│  │  ┌──────────────┐  ┌─────────────────┐        │  │
│  │  │ ₹800 / visit │  │ 🗓 Book Now     │        │  │
│  │  └──────────────┘  └─────────────────┘        │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  TAB NAVIGATION                                     │
│  [ About ]  [ Services ]  [ Reviews ]  [ Location ] │
│                                                     │
│  ─── ABOUT TAB (default) ───────────────────────    │
│                                                     │
│  QUALIFICATIONS                                     │
│  • MPT (Orthopaedics) — Rajiv Gandhi University     │
│  • BPT — JSS College of Physiotherapy               │
│  • Certified Sports Rehabilitation Specialist       │
│  • Member, Indian Association of Physiotherapists   │
│                                                     │
│  SPECIALIZATIONS                    (tag chips)     │
│  ┌──────────┐ ┌──────────┐ ┌────────────────┐      │
│  │ ACL Rehab│ │ Shoulder │ │ Post-Surgical  │      │
│  └──────────┘ └──────────┘ └────────────────┘      │
│  ┌──────────┐ ┌──────────────┐                      │
│  │ Back Pain│ │ Sports Injury│                      │
│  └──────────┘ └──────────────┘                      │
│                                                     │
│  ABOUT ME (free text)                               │
│  "I specialize in sports rehabilitation and have    │
│   worked with professional athletes including..."   │
│                                                     │
│  CONSULTATION TYPES & PRICING                       │
│  ┌───────────────────────────────────────────────┐  │
│  │ Service            │ Duration │ Fee           │  │
│  ├───────────────────────────────────────────────┤  │
│  │ Initial Assessment │ 45 min   │ ₹1,000       │  │
│  │ Follow-up Session  │ 30 min   │ ₹800         │  │
│  │ Home Visit         │ 60 min   │ ₹1,500       │  │
│  │ Online Consult     │ 30 min   │ ₹600         │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  ─── QUICK BOOKING WIDGET (sticky sidebar on        │
│       desktop / sticky bottom bar on mobile) ─────  │
│  ┌───────────────────────────────────────────────┐  │
│  │  Select Date:                                 │  │
│  │  [< Mon 10] [Tue 11] [Wed 12] [Thu 13] [>]   │  │
│  │                                               │  │
│  │  Available Slots:                             │  │
│  │  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐         │  │
│  │  │10:00 │ │10:30 │ │11:00 │ │11:30 │         │  │
│  │  └──────┘ └──────┘ └──────┘ └──────┘         │  │
│  │  ┌──────┐ ┌──────┐ ┌──────┐                   │  │
│  │  │14:00 │ │14:30 │ │16:00 │  (greyed=taken)  │  │
│  │  └──────┘ └──────┘ └──────┘                   │  │
│  │                                               │  │
│  │  [Continue to Booking →]                      │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  ─── REVIEWS TAB ────────────────────────────────   │
│  Overall: ★ 4.9 / 5  (127 reviews)                 │
│  ┌───────────────────────────────────────────────┐  │
│  │  Priya M. · ★★★★★ · 2 weeks ago              │  │
│  │  "Excellent rehab program for my ACL tear.    │  │
│  │   Very professional and caring approach."     │  │
│  │  Verified Booking                             │  │
│  └───────────────────────────────────────────────┘  │
│  [Load More Reviews]                                │
│                                                     │
│  ─── LOCATION TAB ───────────────────────────────   │
│  ┌───────────────────────────────────────────────┐  │
│  │          [MAP EMBED]                          │  │
│  │    📍 PhysioFit Clinic                        │  │
│  │    123 MG Road, Koramangala                   │  │
│  │    Bangalore 560034                           │  │
│  │    [Get Directions]                           │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  FOOTER                                             │
└─────────────────────────────────────────────────────┘
```

**Mobile adaptation:**
- Profile header stacks vertically (photo centered above text)
- Tabs become horizontally scrollable
- Quick Booking widget docks to bottom of screen as a persistent bar: "₹800 · Book Now" — tapping expands the full slot picker
- Reviews load lazily with "See all" link

---

### 2.4 BOOKING FLOW (/book/:physioId)

**4-step linear flow with a horizontal progress indicator**

```
  ● ─────── ○ ─────── ○ ─────── ○
  Service    Schedule   Details   Confirm
```

#### STEP 1 — SELECT SERVICE

```
┌─────────────────────────────────────────────────────┐
│  PROGRESS: [● Service] — [○ Schedule] — [○ Detail]  │
│            — [○ Confirm]                             │
│                                                     │
│  Booking with Dr. Ananya Sharma                     │
│                                                     │
│  Select Consultation Type:                          │
│                                                     │
│  ┌───────────────────────────────────────────────┐  │
│  │  ○  Initial Assessment                        │  │
│  │     45 min · ₹1,000                           │  │
│  │     Comprehensive evaluation & treatment plan │  │
│  ├───────────────────────────────────────────────┤  │
│  │  ●  Follow-up Session          ← selected     │  │
│  │     30 min · ₹800                             │  │
│  │     Continued treatment & exercises           │  │
│  ├───────────────────────────────────────────────┤  │
│  │  ○  Home Visit                                │  │
│  │     60 min · ₹1,500                           │  │
│  │     Physio comes to your location             │  │
│  ├───────────────────────────────────────────────┤  │
│  │  ○  Online Consultation                       │  │
│  │     30 min · ₹600                             │  │
│  │     Video call assessment                     │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│                              [Next: Pick a Time →]  │
└─────────────────────────────────────────────────────┘
```

#### STEP 2 — PICK DATE & TIME

```
┌─────────────────────────────────────────────────────┐
│  PROGRESS: [✓ Service] — [● Schedule] — [○ Detail]  │
│            — [○ Confirm]                             │
│                                                     │
│  Select Date & Time                                 │
│                                                     │
│  ┌───────────────────────────────────────────────┐  │
│  │         FEBRUARY 2026                         │  │
│  │  Mo  Tu  We  Th  Fr  Sa  Su                   │  │
│  │                           1                   │  │
│  │   2   3   4   5   6   7   8                   │  │
│  │  [9] 10  11  12  13  14  15   ← today         │  │
│  │  16  17  18  19  20  21  22                   │  │
│  │  23  24  25  26  27  28                       │  │
│  │                                               │  │
│  │  ● = available days (bold)                    │  │
│  │  ○ = unavailable (greyed, no slots)           │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  Available Slots for Mon, Feb 9:                    │
│                                                     │
│  Morning                                            │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐               │
│  │10:00 │ │10:30 │ │11:00 │ │11:30 │               │
│  └──────┘ └──────┘ └──────┘ └──────┘               │
│                                                     │
│  Afternoon                                          │
│  ┌──────┐ ┌──────┐ ┌──────────┐                     │
│  │14:00 │ │14:30 │ │✓ 16:00  │ ← selected          │
│  └──────┘ └──────┘ └──────────┘                     │
│                                                     │
│  Evening                                            │
│  ┌──────┐                                           │
│  │18:00 │                                           │
│  └──────┘                                           │
│                                                     │
│  ⚡ Slots update in real time. Selected slots are   │
│     held for 10 minutes.                            │
│                                                     │
│  [← Back]                    [Next: Your Details →] │
└─────────────────────────────────────────────────────┘
```

#### STEP 3 — PATIENT DETAILS

```
┌─────────────────────────────────────────────────────┐
│  PROGRESS: [✓ Service] — [✓ Schedule] — [● Detail]  │
│            — [○ Confirm]                             │
│                                                     │
│  Your Details                                       │
│                                                     │
│  Full Name *        [________________________]      │
│  Phone *            [+91 __________________ ]       │
│  Email *            [________________________]      │
│                                                     │
│  Reason for Visit   [________________________]      │
│  (optional)         [________________________]      │
│                     [________________________]      │
│                                                     │
│  Upload Reports     [📎 Choose files]               │
│  (optional)         Accepted: PDF, JPG (max 10MB)   │
│                                                     │
│  ☐ This appointment is for someone else             │
│    (reveals additional name/age/gender fields)      │
│                                                     │
│  ☑ I agree to the Terms & Privacy Policy            │
│                                                     │
│  [← Back]                  [Review & Confirm →]     │
└─────────────────────────────────────────────────────┘
```

#### STEP 4 — CONFIRM & PAY

```
┌─────────────────────────────────────────────────────┐
│  PROGRESS: [✓ Service] — [✓ Schedule] — [✓ Detail]  │
│            — [● Confirm]                             │
│                                                     │
│  Review Your Booking                                │
│                                                     │
│  ┌───────────────────────────────────────────────┐  │
│  │  BOOKING SUMMARY                              │  │
│  │                                               │  │
│  │  Physiotherapist:  Dr. Ananya Sharma          │  │
│  │  Service:          Follow-up Session          │  │
│  │  Date:             Monday, 9 Feb 2026         │  │
│  │  Time:             4:00 PM — 4:30 PM          │  │
│  │  Location:         PhysioFit Clinic,          │  │
│  │                    Koramangala                 │  │
│  │  ──────────────────────────────────           │  │
│  │  Consultation Fee         ₹800                │  │
│  │  Platform Fee             ₹50                 │  │
│  │  ──────────────────────────────────           │  │
│  │  Total                    ₹850                │  │
│  │                                               │  │
│  │  [Edit]                                       │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  CANCELLATION POLICY                                │
│  Free cancellation up to 4 hours before the         │
│  appointment. After that, ₹200 cancellation fee.    │
│                                                     │
│  PAYMENT METHOD                                     │
│  ○ UPI (GPay, PhonePe, Paytm)                      │
│  ● Credit / Debit Card                              │
│  ○ Net Banking                                      │
│  ○ Pay at Clinic (if enabled by provider)           │
│                                                     │
│  [← Back]                     [Pay ₹850 & Book →]  │
└─────────────────────────────────────────────────────┘
```

#### BOOKING CONFIRMATION (/booking/confirmed/:id)

```
┌─────────────────────────────────────────────────────┐
│                                                     │
│              ✓ Booking Confirmed!                   │
│                                                     │
│  Booking ID: #PHC-20260209-4731                     │
│                                                     │
│  ┌───────────────────────────────────────────────┐  │
│  │  Dr. Ananya Sharma                            │  │
│  │  Follow-up Session · 30 min                   │  │
│  │  Mon, 9 Feb 2026 · 4:00 PM                   │  │
│  │  PhysioFit Clinic, Koramangala                │  │
│  └───────────────────────────────────────────────┘  │
│                                                     │
│  Confirmation sent to priya@email.com               │
│  SMS reminder will be sent 2 hours before.          │
│                                                     │
│  [Add to Calendar 📅]  [Get Directions 📍]          │
│                                                     │
│  WHAT TO BRING:                                     │
│  • Previous medical reports / X-rays                │
│  • Comfortable clothing                             │
│  • List of current medications                      │
│                                                     │
│  [View My Bookings]    [Back to Home]               │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

### 2.5 ADMIN DASHBOARD (/admin/dashboard)

```
┌─────────────────────────────────────────────────────┐
│  ADMIN NAV                                          │
│  [Logo] Dashboard | Providers | Bookings | Slots    │
│         Users | Reviews | Reports          [Logout] │
├─────────────────────────────────────────────────────┤
│                                                     │
│  KEY METRICS (cards row)                            │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │
│  │Total     │ │Bookings  │ │Revenue   │ │Pending │ │
│  │Providers │ │This Month│ │This Month│ │Approval│ │
│  │   142    │ │  1,847   │ │₹14.2L    │ │   7    │ │
│  │ +12 new  │ │ +18% ↑   │ │ +22% ↑   │ │ action │ │
│  └──────────┘ └──────────┘ └──────────┘ └────────┘ │
│                                                     │
│  ┌──────────────────────┬────────────────────────┐  │
│  │ BOOKINGS CHART       │ PENDING APPROVALS      │  │
│  │ (line chart,         │                        │  │
│  │  last 30 days)       │ • Dr. Raj Kumar        │  │
│  │                      │   Applied: 2 days ago  │  │
│  │   📈                 │   [Review] [Approve]   │  │
│  │                      │                        │  │
│  │                      │ • Dr. Meera Nair       │  │
│  │                      │   Applied: 3 days ago  │  │
│  │                      │   [Review] [Approve]   │  │
│  └──────────────────────┴────────────────────────┘  │
│                                                     │
│  ┌──────────────────────┬────────────────────────┐  │
│  │ TOP SPECIALIZATIONS  │ RECENT BOOKINGS        │  │
│  │ (donut chart)        │                        │  │
│  │                      │ #4731 Priya → Dr.Sharma│  │
│  │  Sports 34%          │ Today 4:00 PM          │  │
│  │  Ortho  28%          │                        │  │
│  │  Neuro  18%          │ #4730 Ravi → Dr.Gupta  │  │
│  │  Other  20%          │ Today 2:30 PM          │  │
│  └──────────────────────┴────────────────────────┘  │
│                                                     │
│  ADMIN ACTIONS (quick links)                        │
│  [Block Time Slots] [Export Report] [Send Notice]   │
│                                                     │
└─────────────────────────────────────────────────────┘
```

---

## 3. MOBILE-FIRST UX STRATEGY

### 3.1 Responsive Breakpoints

| Breakpoint | Width | Layout |
|---|---|---|
| Mobile | < 640px | Single column, bottom nav, stacked cards |
| Tablet | 640–1024px | 2-column grid, collapsible sidebar |
| Desktop | > 1024px | Full sidebar + content, multi-column grids |

### 3.2 Mobile-Specific Patterns

| Pattern | Implementation |
|---|---|
| **Bottom Navigation** | 4 tabs: Home, Search, Bookings, Profile (patient); Dashboard, Schedule, Bookings, Profile (provider) |
| **Sticky Booking Bar** | On profile pages: persistent bottom bar showing fee + "Book Now" CTA |
| **Bottom Sheet Filters** | Filter panel slides up from bottom, partial-screen with drag handle |
| **Swipeable Cards** | Featured physios on home, date picker on booking |
| **Touch Targets** | Minimum 44x44px for all interactive elements (WCAG) |
| **Thumb Zone Design** | Primary actions placed in lower third of screen |
| **Pull to Refresh** | On listings, bookings dashboard |
| **Skeleton Loading** | Placeholder shimmer UI while content loads |

### 3.3 Mobile Booking Flow

```
┌────────────────────┐
│ ≡  Book Appointment│
├────────────────────┤
│ ● ── ○ ── ○ ── ○  │  (compact progress)
│ Service            │
│                    │
│ ┌────────────────┐ │
│ │ Follow-up      │ │  (radio card, full width)
│ │ 30 min · ₹800  │ │
│ └────────────────┘ │
│ ┌────────────────┐ │
│ │ Home Visit     │ │
│ │ 60 min · ₹1500 │ │
│ └────────────────┘ │
│                    │
│                    │
│ ┌────────────────┐ │
│ │   Next →       │ │  (full-width sticky bottom CTA)
│ └────────────────┘ │
└────────────────────┘
```

---

## 4. UX BEST PRACTICES FOR HEALTHCARE BOOKING

### 4.1 Trust & Credibility

| Practice | Rationale |
|---|---|
| **Verified badges** on approved profiles | Patients must trust the provider with their health |
| **Real patient reviews** with "Verified Booking" tags | Social proof reduces anxiety about trying a new provider |
| **Display credentials prominently** (degrees, certifications, registration numbers) | Healthcare requires credential transparency |
| **Show clinic photos** | Reduces uncertainty about the physical environment |
| **Secure payment indicators** (SSL badge, PCI compliance note) | Financial trust is critical for conversion |
| **Clear cancellation policy** shown before payment | Reduces booking anxiety ("what if I need to cancel?") |

### 4.2 Accessibility & Inclusivity

| Practice | Implementation |
|---|---|
| **WCAG 2.1 AA compliance** | Color contrast ratios ≥ 4.5:1, keyboard navigation, screen reader support |
| **Large text options** | Font scaling support; base font ≥ 16px |
| **Language support** | i18n ready; start with English + Hindi; add regional languages |
| **Alt text on all images** | Profile photos, specialization icons |
| **Form field labels** always visible | Never rely solely on placeholder text |
| **Error messages** that are specific | "Please enter a valid 10-digit phone number" not "Invalid input" |
| **Color-blind safe palette** | Never use color alone to convey slot availability — use icons + text |

### 4.3 Booking-Specific UX

| Practice | Rationale |
|---|---|
| **Real-time slot availability** via WebSocket/polling | Stale availability leads to failed bookings and frustration |
| **Temporary slot hold** (10-min reservation) | Prevents double-booking while patient completes the flow |
| **Minimal form fields** | Only collect what's essential at booking time; gather medical history later |
| **Guest checkout option** | Don't force account creation before first booking (create account post-booking) |
| **Progress indicator** in booking flow | Patients need to know how many steps remain |
| **Edit without restarting** | Let users change date/service from the summary without losing other data |
| **Confirmation via multiple channels** | Email + SMS + in-app notification |
| **Calendar integration** | .ics download / "Add to Google Calendar" button on confirmation |
| **Automated reminders** | 24hr + 2hr before appointment |
| **Easy rebooking** | "Book again" button on past appointments |

### 4.4 Healthcare-Specific Compliance

| Consideration | Implementation |
|---|---|
| **Data privacy** | Encrypt PII at rest and in transit; comply with local health data regulations |
| **Consent** | Explicit consent checkbox for data processing, separate from T&C |
| **Medical disclaimers** | "This platform facilitates booking only. It does not provide medical advice." |
| **Report uploads** | Secure file handling with virus scanning; auto-delete after retention period |
| **Audit trail** | Log all booking modifications, cancellations, and admin actions |

### 4.5 Performance & Reliability

| Practice | Target |
|---|---|
| **First Contentful Paint** | < 1.5s |
| **Time to Interactive** | < 3.0s |
| **Slot API response** | < 300ms |
| **Image optimization** | WebP with fallback, lazy loading, responsive srcset |
| **Offline resilience** | Show cached results with "last updated" timestamp if network drops |
| **Error recovery** | If payment fails, preserve booking state and allow retry without re-entering details |

---

## 5. KEY UI COMPONENTS

### 5.1 Component Library

| Component | Description | Used On |
|---|---|---|
| **ProviderCard** | Photo, name, rating, specializations, fee, next available slot, CTA buttons. Two variants: horizontal (list) and vertical (carousel). | Home, Search |
| **SpecializationChip** | Colored pill with icon + label. Filterable and clickable. | Home, Search, Profile |
| **SlotPicker** | Horizontal date strip (scrollable) + time slot grid below. Slots color-coded: available (primary), selected (accent), unavailable (grey), held (amber). Real-time updates via WebSocket. | Profile, Booking Step 2 |
| **MiniCalendar** | Month view with availability dots on dates. Tapping a date scrolls to time slots. | Booking Step 2 |
| **BookingSummaryCard** | Compact card showing provider, service, date/time, location, fee breakdown. Editable fields link back to respective steps. | Booking Step 4, Confirmation, Patient Dashboard |
| **ReviewCard** | Avatar, name, star rating, date, review text, "Verified Booking" badge. | Profile, Admin |
| **FilterPanel** | Accordion-style filter groups (checkboxes, radio, range slider). Responsive: sidebar on desktop, bottom sheet on mobile. Active filter count badge. | Search |
| **StepIndicator** | Horizontal progress bar with numbered steps. Completed steps show checkmarks. Current step is highlighted. Compact variant for mobile. | Booking Flow |
| **SearchBar** | Combo input: specialization dropdown + location autocomplete + date picker. Unified on desktop, stacked on mobile. | Home (hero), Search (sticky) |
| **PriceTag** | Displays fee with optional "from" prefix and per-session label. Supports multiple currencies. | Card, Profile, Booking |
| **AvailabilityBadge** | Green dot + "Next: Today, 4:00 PM" or amber "Next: Tomorrow". Urgency indicator that drives conversions. | ProviderCard |
| **TrustBadge** | Verified checkmark, years of experience, number of sessions completed. | Profile Header |
| **BottomBar** (mobile) | Persistent booking CTA at bottom of profile page. Shows fee + "Book Now". Expands to mini slot picker on tap. | Profile (mobile) |
| **EmptyState** | Illustrated placeholder when no results / no bookings. Includes suggested action button. | Search (no results), Dashboard (no bookings) |
| **ConfirmationHero** | Large checkmark animation, booking ID, summary, action buttons (Add to Calendar, Get Directions). | Booking Confirmation |
| **AdminMetricCard** | KPI card with value, label, trend arrow, and sparkline. | Admin Dashboard |
| **StatusPill** | Colored pill indicating booking status: Confirmed (green), Pending (amber), Cancelled (red), Completed (blue). | Booking lists |
| **NotificationBanner** | Top-of-page dismissible banner for system notices, slot hold expiry warnings, etc. | Global |

### 5.2 Design Tokens

```
COLORS
  primary:        #0066CC   (trust blue — healthcare standard)
  primary-light:  #E6F0FF
  accent:         #00A86B   (action green — book/confirm)
  warning:        #F5A623   (held slots, pending states)
  error:          #D63031   (cancellation, validation errors)
  text-primary:   #1A1A2E
  text-secondary: #6B7280
  bg-primary:     #FFFFFF
  bg-secondary:   #F8F9FB
  border:         #E5E7EB

TYPOGRAPHY
  font-family:    'Inter', system-ui, sans-serif
  heading-1:      32px / 40px / 700
  heading-2:      24px / 32px / 600
  heading-3:      20px / 28px / 600
  body:           16px / 24px / 400
  body-small:     14px / 20px / 400
  caption:        12px / 16px / 400

SPACING (8px base grid)
  xs:  4px
  sm:  8px
  md:  16px
  lg:  24px
  xl:  32px
  2xl: 48px

ELEVATION
  card:    0 1px 3px rgba(0,0,0,0.08)
  modal:   0 8px 32px rgba(0,0,0,0.12)
  sticky:  0 2px 8px rgba(0,0,0,0.06)

BORDER RADIUS
  sm:   4px   (chips, inputs)
  md:   8px   (cards, buttons)
  lg:   12px  (modals, sheets)
  full: 9999px (avatars, pills)
```

### 5.3 Interaction States (Slot Picker)

```
┌─────────────────────────────────────────────┐
│  SLOT STATES                                │
│                                             │
│  ┌────────┐  Available (default)            │
│  │ 10:00  │  bg: white, border: primary     │
│  └────────┘                                 │
│                                             │
│  ┌────────┐  Hovered                        │
│  │ 10:00  │  bg: primary-light              │
│  └────────┘                                 │
│                                             │
│  ┌████████┐  Selected                       │
│  │ 10:00  │  bg: primary, text: white       │
│  └████████┘                                 │
│                                             │
│  ┌────────┐  Unavailable / Booked           │
│  │ 10:00  │  bg: bg-secondary, text: grey   │
│  └────────┘  strikethrough, not clickable   │
│                                             │
│  ┌────────┐  Held (by another user)         │
│  │ 10:00  │  bg: warning-light, dashed      │
│  └────────┘  border, tooltip on hover       │
│                                             │
│  ┌────────┐  Blocked (by admin)             │
│  │ 10:00  │  bg: error-light, lock icon     │
│  └────────┘  tooltip: "Blocked by admin"    │
└─────────────────────────────────────────────┘
```

---

## 6. USER FLOWS (SUMMARY)

### 6.1 Patient Booking Flow
```
Home → Search (filtered) → Provider Profile → Select Service
→ Pick Date/Time → Enter Details → Review & Pay → Confirmation
                                                    ↓
                                          Email + SMS + Calendar
```

### 6.2 Physiotherapist Onboarding Flow
```
Register → Email Verification → Profile Setup (multi-step):
  1. Personal Details + Photo
  2. Qualifications + Documents
  3. Specializations + Services + Pricing
  4. Availability / Weekly Schedule
  5. Bank Details (for payouts)
→ Submit for Review → Admin Approval → Profile Goes Live
```

### 6.3 Admin Approval Flow
```
Notification: New Application → Review Profile + Documents
→ [Approve] → Provider profile goes live, welcome email sent
→ [Reject with reason] → Provider notified, can re-apply
→ [Request changes] → Provider edits and re-submits
```

---

## 7. INFORMATION ARCHITECTURE — KEY DECISIONS

| Decision | Rationale |
|---|---|
| **No login required to browse** | Reduce friction; only require auth at booking step |
| **Unified login, role-based routing** | Single /login page; redirect to patient/provider/admin dashboard based on role |
| **Calendar-first availability** | Visual calendar is more intuitive than list-based time picking for appointments |
| **Step-by-step booking (not single-page)** | Reduces cognitive load; each step has one clear decision |
| **Provider-managed availability** | Providers set their own recurring schedule + exceptions; admin can override |
| **Sticky booking widget** | Reduces drop-off by keeping the CTA always visible on profile pages |
| **Search on homepage hero** | The primary user intent is finding a physiotherapist — surface it immediately |

---

*Document Version: 1.0*
*Last Updated: February 2026*
*Designed for: PhysioConnect Marketplace*
