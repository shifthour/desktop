import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

// ──────────────────────────────────────────────
// Specialization master data
// ──────────────────────────────────────────────
const specializations = [
  { name: "Sports Injury", icon: "🏃", count: 24, image: "/pics/home-visit-stretch.jpg" },
  { name: "Neurological", icon: "🧠", count: 18, image: "/pics/walker-assist.jpg" },
  { name: "Orthopedic", icon: "🦴", count: 31, image: "/pics/shoulder-exam.jpg" },
  { name: "Paediatric", icon: "👶", count: 12, image: "/pics/paediatric-physio.jpg" },
  { name: "Geriatric", icon: "🧓", count: 15, image: "/pics/elderly-arms-up.jpg" },
  { name: "Cardiopulmonary", icon: "❤️", count: 9, image: "/pics/weights-exercise.jpg" },
  { name: "Women's Health", icon: "🌸", count: 14, image: "/pics/ball-therapy.jpg" },
  { name: "Post-Surgical", icon: "🏥", count: 19, image: "/pics/wheelchair-bands.jpg" },
];

// ──────────────────────────────────────────────
// Physiotherapist data
// ──────────────────────────────────────────────
const physioData = [
  {
    id: "1",
    slug: "dr-sarah-mitchell",
    name: "Dr. Sarah Mitchell",
    photo: "",
    gender: "female",
    experience: 9,
    bio: "I specialise in sports rehabilitation and have worked with athletes from Premier League and England Rugby. My approach combines hands-on manual therapy with evidence-based exercise prescription to get you back to peak performance. Every patient gets a bespoke treatment plan.",
    rating: 4.9,
    reviewCount: 127,
    clinicName: "PhysioFit London",
    locationArea: "Richmond",
    locationCity: "London",
    locationLat: 51.4613,
    locationLng: -0.3037,
    verified: true,
    totalSessions: 1840,
    specializations: ["Sports Injury", "Orthopedic"],
    qualifications: [
      "MSc Physiotherapy — King's College London",
      "BSc (Hons) Physiotherapy — University of Birmingham",
      "HCPC Registered (PH123456)",
      "CSP Member · Certified Sports Rehabilitation Specialist",
    ],
    visitTypes: ["clinic", "home", "online"],
    services: [
      { name: "Initial Assessment", duration: 45, price: 85, description: "Comprehensive evaluation & treatment plan" },
      { name: "Follow-up Session", duration: 30, price: 65, description: "Continued treatment & exercise prescription" },
      { name: "Home Visit", duration: 60, price: 110, description: "Physiotherapy at your doorstep" },
      { name: "Online Consultation", duration: 30, price: 50, description: "Video call assessment & guidance" },
    ],
  },
  {
    id: "2",
    slug: "dr-james-thompson",
    name: "Dr. James Thompson",
    photo: "",
    gender: "male",
    experience: 14,
    bio: "With over 14 years of experience in neurological physiotherapy, I help patients recover from stroke, spinal cord injuries, and Parkinson's disease. I trained at the National Hospital for Neurology and use the latest rehabilitation techniques.",
    rating: 4.8,
    reviewCount: 203,
    clinicName: "NeuroRehab UK",
    locationArea: "Islington",
    locationCity: "London",
    locationLat: 51.5362,
    locationLng: -0.1033,
    verified: true,
    totalSessions: 3200,
    specializations: ["Neurological", "Geriatric"],
    qualifications: [
      "PhD Neurorehabilitation — UCL",
      "MSc Physiotherapy — University of Manchester",
      "HCPC Registered (PH234567)",
      "CSP Member · Certified Neuro-Rehabilitation Specialist",
    ],
    visitTypes: ["clinic", "home"],
    services: [
      { name: "Neuro Assessment", duration: 60, price: 95, description: "Full neurological evaluation" },
      { name: "Rehabilitation Session", duration: 45, price: 75, description: "Targeted neuro rehab exercises" },
      { name: "Home Visit", duration: 60, price: 120, description: "Home-based neuro rehabilitation" },
    ],
  },
  {
    id: "3",
    slug: "dr-emily-chen",
    name: "Dr. Emily Chen",
    photo: "",
    gender: "female",
    experience: 7,
    bio: "I'm passionate about helping children reach their developmental milestones. I work with children with cerebral palsy, developmental delays, and sports injuries. My sessions are play-based and designed to be fun whilst being therapeutic.",
    rating: 4.9,
    reviewCount: 89,
    clinicName: "Little Steps Therapy",
    locationArea: "Kensington",
    locationCity: "London",
    locationLat: 51.499,
    locationLng: -0.1942,
    verified: true,
    totalSessions: 920,
    specializations: ["Pediatric", "Neurological"],
    qualifications: [
      "MSc Paediatric Physiotherapy — Great Ormond Street",
      "BSc (Hons) Physiotherapy — Cardiff University",
      "HCPC Registered (PH345678)",
      "CSP Member · APCP Member",
    ],
    visitTypes: ["clinic", "home", "online"],
    services: [
      { name: "Paediatric Assessment", duration: 45, price: 80, description: "Developmental evaluation for children" },
      { name: "Therapy Session", duration: 30, price: 60, description: "Play-based therapy session" },
      { name: "Home Visit", duration: 45, price: 95, description: "Therapy in comfortable home setting" },
      { name: "Parent Consultation", duration: 30, price: 45, description: "Guidance for home exercises" },
    ],
  },
  {
    id: "4",
    slug: "dr-marcus-williams",
    name: "Dr. Marcus Williams",
    photo: "",
    gender: "male",
    experience: 11,
    bio: "Former head physiotherapist for a Championship football club, I now focus on private practice. Specialising in ACL reconstruction rehab, shoulder injuries, and return-to-sport protocols. DBS enhanced checked.",
    rating: 4.7,
    reviewCount: 156,
    clinicName: "SportsFit Rehab",
    locationArea: "Wimbledon",
    locationCity: "London",
    locationLat: 51.4214,
    locationLng: -0.2064,
    verified: true,
    totalSessions: 2100,
    specializations: ["Sports Injury", "Post-Surgical"],
    qualifications: [
      "MSc Sports Physiotherapy — Loughborough University",
      "BSc (Hons) Physiotherapy — University of Nottingham",
      "HCPC Registered (PH456789)",
      "FA Medical License · CSP Member",
    ],
    visitTypes: ["clinic", "home"],
    services: [
      { name: "Sports Assessment", duration: 60, price: 100, description: "Comprehensive sports injury evaluation" },
      { name: "Rehab Session", duration: 45, price: 80, description: "Sport-specific rehabilitation" },
      { name: "Home Visit", duration: 60, price: 120, description: "Home-based sports rehab" },
    ],
  },
  {
    id: "5",
    slug: "dr-hannah-wright",
    name: "Dr. Hannah Wright",
    photo: "",
    gender: "female",
    experience: 8,
    bio: "I specialise in women's health physiotherapy including prenatal & postnatal care, pelvic floor rehabilitation, and osteoporosis management. I provide a safe, comfortable environment for women's health concerns.",
    rating: 4.8,
    reviewCount: 112,
    clinicName: "Bloom Women's Physio",
    locationArea: "Clapham",
    locationCity: "London",
    locationLat: 51.4627,
    locationLng: -0.138,
    verified: true,
    totalSessions: 1350,
    specializations: ["Women's Health", "Orthopedic"],
    qualifications: [
      "MSc Women's Health Physiotherapy — King's College London",
      "BSc (Hons) Physiotherapy — University of Southampton",
      "HCPC Registered (PH567890)",
      "CSP Member · Pelvic Floor Rehabilitation Specialist",
    ],
    visitTypes: ["clinic", "home", "online"],
    services: [
      { name: "Initial Consultation", duration: 45, price: 85, description: "Comprehensive women's health assessment" },
      { name: "Therapy Session", duration: 30, price: 65, description: "Targeted treatment session" },
      { name: "Prenatal Physio", duration: 30, price: 60, description: "Pregnancy-safe exercises & pain relief" },
      { name: "Home Visit", duration: 45, price: 100, description: "Home-based women's health physio" },
    ],
  },
  {
    id: "6",
    slug: "dr-oliver-patel",
    name: "Dr. Oliver Patel",
    photo: "",
    gender: "male",
    experience: 10,
    bio: "I help patients with cardiac and respiratory conditions improve their quality of life through specialised rehabilitation. Experienced in post-cardiac surgery rehab, COPD management, and long COVID recovery.",
    rating: 4.6,
    reviewCount: 78,
    clinicName: "CardioFit Rehab",
    locationArea: "Greenwich",
    locationCity: "London",
    locationLat: 51.4769,
    locationLng: -0.0005,
    verified: true,
    totalSessions: 1560,
    specializations: ["Cardiopulmonary", "Geriatric"],
    qualifications: [
      "MSc Cardiopulmonary Physiotherapy — Imperial College London",
      "BSc (Hons) Physiotherapy — University of Leeds",
      "HCPC Registered (PH678901)",
      "CSP Member · BTS Certified Pulmonary Rehab Specialist",
    ],
    visitTypes: ["clinic", "home"],
    services: [
      { name: "Cardio Assessment", duration: 60, price: 90, description: "Cardiac rehabilitation evaluation" },
      { name: "Rehab Session", duration: 45, price: 70, description: "Supervised cardiac exercise programme" },
      { name: "Home Visit", duration: 60, price: 110, description: "Home-based cardiac rehab" },
    ],
  },
];

// ──────────────────────────────────────────────
// Reviews data
// ──────────────────────────────────────────────
const reviewsData = [
  { id: "r1", physioId: "1", patientName: "Claire M.", rating: 5, date: "2026-01-25", text: "Excellent rehab programme for my ACL tear. Dr. Mitchell is incredibly professional and caring. She explained every exercise and why it mattered. I'm back playing netball!", verified: true },
  { id: "r2", physioId: "1", patientName: "Tom K.", rating: 5, date: "2026-01-20", text: "Best sports physio in London. Helped me recover from a shoulder injury in record time. Highly recommend the home visit option!", verified: true },
  { id: "r3", physioId: "1", patientName: "Sophie D.", rating: 4, date: "2026-01-15", text: "Very knowledgeable and thorough. The clinic is well-equipped and spotless. Only giving 4 stars as the first appointment was slightly delayed.", verified: true },
  { id: "r4", physioId: "1", patientName: "David S.", rating: 5, date: "2026-01-10", text: "Dr. Mitchell's treatment plan for my lower back pain worked wonders. 6 sessions and I was pain-free. She also taught me preventive exercises I still use daily.", verified: true },
  { id: "r5", physioId: "2", patientName: "Margaret R.", rating: 5, date: "2026-01-28", text: "Dr. Thompson helped my father recover from a stroke. His patience and expertise are remarkable. The home visit rehab programme was tailored perfectly.", verified: true },
  { id: "r6", physioId: "2", patientName: "Andrew N.", rating: 5, date: "2026-01-22", text: "Outstanding neuro rehabilitation. After my spinal surgery, Dr. Thompson's programme helped me walk independently again. Forever grateful.", verified: true },
  { id: "r7", physioId: "3", patientName: "Rachel P.", rating: 5, date: "2026-01-26", text: "My daughter loves going to Dr. Chen's sessions! She makes therapy feel like play. We've seen brilliant progress in just 3 months.", verified: true },
  { id: "r8", physioId: "4", patientName: "Chris V.", rating: 5, date: "2026-01-24", text: "As a marathon runner, Dr. Williams understood exactly what I needed. His sport-specific rehab got me back to training within weeks.", verified: true },
  { id: "r9", physioId: "5", patientName: "Laura G.", rating: 5, date: "2026-01-27", text: "Dr. Wright was brilliant during my pregnancy-related back pain. So gentle and understanding. The prenatal exercises were genuinely life-changing.", verified: true },
  { id: "r10", physioId: "6", patientName: "Robert R.", rating: 4, date: "2026-01-23", text: "Excellent cardiac rehab programme. Dr. Patel monitors everything carefully and adjusts exercises based on my heart rate response. Really professional.", verified: true },
];

async function main() {
  console.log("🌱 Seeding database...\n");

  // Clear existing data
  await prisma.booking.deleteMany();
  await prisma.review.deleteMany();
  await prisma.service.deleteMany();
  await prisma.physioVisitType.deleteMany();
  await prisma.qualification.deleteMany();
  await prisma.physioSpecialization.deleteMany();
  await prisma.physiotherapist.deleteMany();
  await prisma.specialization.deleteMany();
  console.log("  ✓ Cleared existing data");

  // 1. Create specializations
  const specMap: Record<string, string> = {};
  for (const spec of specializations) {
    const created = await prisma.specialization.create({
      data: {
        name: spec.name,
        icon: spec.icon,
        count: spec.count,
        image: spec.image,
      },
    });
    specMap[spec.name] = created.id;
  }
  console.log(`  ✓ Created ${specializations.length} specializations`);

  // 2. Create physiotherapists with relations
  // Track service IDs for booking references
  const serviceIdMap: Record<string, Record<string, string>> = {};

  for (const p of physioData) {
    const physio = await prisma.physiotherapist.create({
      data: {
        id: p.id,
        slug: p.slug,
        name: p.name,
        photo: p.photo,
        gender: p.gender,
        experience: p.experience,
        bio: p.bio,
        rating: p.rating,
        reviewCount: p.reviewCount,
        clinicName: p.clinicName,
        locationArea: p.locationArea,
        locationCity: p.locationCity,
        locationLat: p.locationLat,
        locationLng: p.locationLng,
        verified: p.verified,
        totalSessions: p.totalSessions,
      },
    });

    // Create specialization links
    for (const specName of p.specializations) {
      const specId = specMap[specName];
      if (specId) {
        await prisma.physioSpecialization.create({
          data: { physioId: physio.id, specializationId: specId },
        });
      }
    }

    // Create qualifications
    for (let i = 0; i < p.qualifications.length; i++) {
      await prisma.qualification.create({
        data: { physioId: physio.id, text: p.qualifications[i], sortOrder: i },
      });
    }

    // Create visit types
    for (const vt of p.visitTypes) {
      await prisma.physioVisitType.create({
        data: { physioId: physio.id, visitType: vt },
      });
    }

    // Create services and track IDs
    serviceIdMap[p.id] = {};
    for (let i = 0; i < p.services.length; i++) {
      const svc = p.services[i];
      const created = await prisma.service.create({
        data: {
          physioId: physio.id,
          name: svc.name,
          duration: svc.duration,
          price: svc.price,
          description: svc.description,
          sortOrder: i,
        },
      });
      // Map old "s1", "s2", etc. to new CUID
      serviceIdMap[p.id][`s${i + 1}`] = created.id;
    }

    console.log(`  ✓ Created physio: ${p.name}`);
  }

  // 3. Create reviews
  for (const r of reviewsData) {
    await prisma.review.create({
      data: {
        id: r.id,
        physioId: r.physioId,
        patientName: r.patientName,
        rating: r.rating,
        date: r.date,
        text: r.text,
        verified: r.verified,
      },
    });
  }
  console.log(`  ✓ Created ${reviewsData.length} reviews`);

  // 4. Create sample bookings
  const bookingsData = [
    { id: "PHC-20260209-4731", physioId: "1", physioName: "Dr. Sarah Mitchell", patientName: "Claire Mehta", patientEmail: "claire@email.com", patientPhone: "+44 7700 900123", oldServiceId: "s2", serviceName: "Follow-up Session", date: "2026-02-09", time: "16:00", duration: 30, fee: 65, platformFee: 5, status: "confirmed", createdAt: "2026-02-09T10:30:00Z" },
    { id: "PHC-20260209-4730", physioId: "2", physioName: "Dr. James Thompson", patientName: "Robert Kumar", patientEmail: "robert@email.com", patientPhone: "+44 7700 900124", oldServiceId: "s2", serviceName: "Rehabilitation Session", date: "2026-02-09", time: "14:30", duration: 45, fee: 75, platformFee: 5, status: "confirmed", createdAt: "2026-02-09T09:15:00Z" },
    { id: "PHC-20260208-4729", physioId: "3", physioName: "Dr. Emily Chen", patientName: "Rachel Prakash", patientEmail: "rachel@email.com", patientPhone: "+44 7700 900125", oldServiceId: "s2", serviceName: "Therapy Session", date: "2026-02-08", time: "10:00", duration: 30, fee: 60, platformFee: 5, status: "completed", createdAt: "2026-02-07T14:00:00Z" },
    { id: "PHC-20260210-4732", physioId: "4", physioName: "Dr. Marcus Williams", patientName: "Chris V.", patientEmail: "chris@email.com", patientPhone: "+44 7700 900126", oldServiceId: "s1", serviceName: "Sports Assessment", date: "2026-02-10", time: "09:00", duration: 60, fee: 100, platformFee: 5, status: "pending", createdAt: "2026-02-09T11:00:00Z" },
    { id: "PHC-20260207-4728", physioId: "5", physioName: "Dr. Hannah Wright", patientName: "Laura G.", patientEmail: "laura@email.com", patientPhone: "+44 7700 900127", oldServiceId: "s3", serviceName: "Prenatal Physio", date: "2026-02-07", time: "11:00", duration: 30, fee: 60, platformFee: 5, status: "completed", createdAt: "2026-02-06T16:00:00Z" },
    { id: "PHC-20260209-4733", physioId: "1", physioName: "Dr. Sarah Mitchell", patientName: "David Singh", patientEmail: "david@email.com", patientPhone: "+44 7700 900128", oldServiceId: "s1", serviceName: "Initial Assessment", date: "2026-02-11", time: "10:00", duration: 45, fee: 85, platformFee: 5, status: "confirmed", createdAt: "2026-02-09T12:00:00Z" },
  ];

  for (const b of bookingsData) {
    const realServiceId = serviceIdMap[b.physioId]?.[b.oldServiceId];
    if (!realServiceId) {
      console.log(`  ⚠ Skipping booking ${b.id}: service mapping not found`);
      continue;
    }
    await prisma.booking.create({
      data: {
        id: b.id,
        physioId: b.physioId,
        physioName: b.physioName,
        patientName: b.patientName,
        patientEmail: b.patientEmail,
        patientPhone: b.patientPhone,
        serviceId: realServiceId,
        serviceName: b.serviceName,
        date: b.date,
        time: b.time,
        duration: b.duration,
        fee: b.fee,
        platformFee: b.platformFee,
        status: b.status,
        createdAt: new Date(b.createdAt),
      },
    });
  }
  console.log(`  ✓ Created ${bookingsData.length} bookings`);

  console.log("\n✅ Database seeded successfully!");
}

main()
  .catch((e) => {
    console.error("❌ Seed error:", e);
    process.exit(1);
  })
  .finally(() => prisma.$disconnect());
