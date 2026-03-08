-- ================================================
-- PhysioConnect — Supabase Schema + Seed Data
-- Run this in Supabase SQL Editor (single execution)
-- All tables prefixed with physioconnect_
-- ================================================

-- ────────────────────────────────────────────────
-- 1. SCHEMA
-- ────────────────────────────────────────────────

-- Specializations
CREATE TABLE physioconnect_specializations (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  name text UNIQUE NOT NULL,
  icon text DEFAULT '',
  count integer DEFAULT 0,
  image text DEFAULT ''
);

-- Physiotherapists
CREATE TABLE physioconnect_physiotherapists (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  slug text UNIQUE NOT NULL,
  name text NOT NULL,
  photo text DEFAULT '',
  gender text NOT NULL,
  experience integer NOT NULL,
  bio text NOT NULL,
  rating double precision DEFAULT 0,
  "reviewCount" integer DEFAULT 0,
  "clinicName" text NOT NULL,
  "locationArea" text NOT NULL,
  "locationCity" text NOT NULL,
  "locationLat" double precision NOT NULL,
  "locationLng" double precision NOT NULL,
  verified boolean DEFAULT false,
  "totalSessions" integer DEFAULT 0,
  "createdAt" timestamptz DEFAULT now(),
  "updatedAt" timestamptz DEFAULT now()
);

CREATE INDEX idx_physio_slug ON physioconnect_physiotherapists (slug);
CREATE INDEX idx_physio_city ON physioconnect_physiotherapists ("locationCity");
CREATE INDEX idx_physio_rating ON physioconnect_physiotherapists (rating);

-- Physio ↔ Specialization junction
CREATE TABLE physioconnect_physio_specializations (
  "physioId" text NOT NULL REFERENCES physioconnect_physiotherapists(id) ON DELETE CASCADE,
  "specializationId" text NOT NULL REFERENCES physioconnect_specializations(id) ON DELETE CASCADE,
  PRIMARY KEY ("physioId", "specializationId")
);

-- Qualifications
CREATE TABLE physioconnect_qualifications (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "physioId" text NOT NULL REFERENCES physioconnect_physiotherapists(id) ON DELETE CASCADE,
  text text NOT NULL,
  "sortOrder" integer DEFAULT 0
);

CREATE INDEX idx_qual_physio ON physioconnect_qualifications ("physioId");

-- Visit Types
CREATE TABLE physioconnect_physio_visit_types (
  "physioId" text NOT NULL REFERENCES physioconnect_physiotherapists(id) ON DELETE CASCADE,
  "visitType" text NOT NULL,
  PRIMARY KEY ("physioId", "visitType")
);

-- Services
CREATE TABLE physioconnect_services (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "physioId" text NOT NULL REFERENCES physioconnect_physiotherapists(id) ON DELETE CASCADE,
  name text NOT NULL,
  duration integer NOT NULL,
  price double precision NOT NULL,
  description text NOT NULL,
  "sortOrder" integer DEFAULT 0
);

CREATE INDEX idx_service_physio ON physioconnect_services ("physioId");

-- Reviews
CREATE TABLE physioconnect_reviews (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "physioId" text NOT NULL REFERENCES physioconnect_physiotherapists(id) ON DELETE CASCADE,
  "patientName" text NOT NULL,
  rating integer NOT NULL,
  date text NOT NULL,
  text text NOT NULL,
  verified boolean DEFAULT false
);

CREATE INDEX idx_review_physio ON physioconnect_reviews ("physioId");

-- Bookings
CREATE TABLE physioconnect_bookings (
  id text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "physioId" text NOT NULL REFERENCES physioconnect_physiotherapists(id) ON DELETE CASCADE,
  "physioName" text NOT NULL,
  "patientName" text NOT NULL,
  "patientEmail" text NOT NULL,
  "patientPhone" text NOT NULL,
  "serviceId" text NOT NULL REFERENCES physioconnect_services(id) ON DELETE CASCADE,
  "serviceName" text NOT NULL,
  date text NOT NULL,
  time text NOT NULL,
  duration integer NOT NULL,
  fee double precision NOT NULL,
  "platformFee" double precision NOT NULL,
  status text DEFAULT 'pending',
  reason text,
  "createdAt" timestamptz DEFAULT now()
);

CREATE INDEX idx_booking_physio ON physioconnect_bookings ("physioId");
CREATE INDEX idx_booking_status ON physioconnect_bookings (status);

-- ────────────────────────────────────────────────
-- 2. ROW LEVEL SECURITY — public read access
-- ────────────────────────────────────────────────

ALTER TABLE physioconnect_specializations ENABLE ROW LEVEL SECURITY;
ALTER TABLE physioconnect_physiotherapists ENABLE ROW LEVEL SECURITY;
ALTER TABLE physioconnect_physio_specializations ENABLE ROW LEVEL SECURITY;
ALTER TABLE physioconnect_qualifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE physioconnect_physio_visit_types ENABLE ROW LEVEL SECURITY;
ALTER TABLE physioconnect_services ENABLE ROW LEVEL SECURITY;
ALTER TABLE physioconnect_reviews ENABLE ROW LEVEL SECURITY;
ALTER TABLE physioconnect_bookings ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Public read" ON physioconnect_specializations FOR SELECT USING (true);
CREATE POLICY "Public read" ON physioconnect_physiotherapists FOR SELECT USING (true);
CREATE POLICY "Public read" ON physioconnect_physio_specializations FOR SELECT USING (true);
CREATE POLICY "Public read" ON physioconnect_qualifications FOR SELECT USING (true);
CREATE POLICY "Public read" ON physioconnect_physio_visit_types FOR SELECT USING (true);
CREATE POLICY "Public read" ON physioconnect_services FOR SELECT USING (true);
CREATE POLICY "Public read" ON physioconnect_reviews FOR SELECT USING (true);
CREATE POLICY "Public read" ON physioconnect_bookings FOR SELECT USING (true);

-- ────────────────────────────────────────────────
-- 3. SEED DATA
-- ────────────────────────────────────────────────

-- Specializations
INSERT INTO physioconnect_specializations (id, name, icon, count, image) VALUES
  ('spec-1', 'Sports Injury', '🏃', 24, '/pics/home-visit-stretch.jpg'),
  ('spec-2', 'Neurological', '🧠', 18, '/pics/walker-assist.jpg'),
  ('spec-3', 'Orthopedic', '🦴', 31, '/pics/shoulder-exam.jpg'),
  ('spec-4', 'Paediatric', '👶', 12, '/pics/paediatric-physio.jpg'),
  ('spec-5', 'Geriatric', '🧓', 15, '/pics/elderly-arms-up.jpg'),
  ('spec-6', 'Cardiopulmonary', '❤️', 9, '/pics/weights-exercise.jpg'),
  ('spec-7', 'Women''s Health', '🌸', 14, '/pics/ball-therapy.jpg'),
  ('spec-8', 'Post-Surgical', '🏥', 19, '/pics/wheelchair-bands.jpg');

-- Physiotherapists
INSERT INTO physioconnect_physiotherapists (id, slug, name, photo, gender, experience, bio, rating, "reviewCount", "clinicName", "locationArea", "locationCity", "locationLat", "locationLng", verified, "totalSessions") VALUES
  ('1', 'dr-sarah-mitchell', 'Dr. Sarah Mitchell', '', 'female', 9,
   'I specialise in sports rehabilitation and have worked with athletes from Premier League and England Rugby. My approach combines hands-on manual therapy with evidence-based exercise prescription to get you back to peak performance. Every patient gets a bespoke treatment plan.',
   4.9, 127, 'PhysioFit London', 'Richmond', 'London', 51.4613, -0.3037, true, 1840),

  ('2', 'dr-james-thompson', 'Dr. James Thompson', '', 'male', 14,
   'With over 14 years of experience in neurological physiotherapy, I help patients recover from stroke, spinal cord injuries, and Parkinson''s disease. I trained at the National Hospital for Neurology and use the latest rehabilitation techniques.',
   4.8, 203, 'NeuroRehab UK', 'Islington', 'London', 51.5362, -0.1033, true, 3200),

  ('3', 'dr-emily-chen', 'Dr. Emily Chen', '', 'female', 7,
   'I''m passionate about helping children reach their developmental milestones. I work with children with cerebral palsy, developmental delays, and sports injuries. My sessions are play-based and designed to be fun whilst being therapeutic.',
   4.9, 89, 'Little Steps Therapy', 'Kensington', 'London', 51.499, -0.1942, true, 920),

  ('4', 'dr-marcus-williams', 'Dr. Marcus Williams', '', 'male', 11,
   'Former head physiotherapist for a Championship football club, I now focus on private practice. Specialising in ACL reconstruction rehab, shoulder injuries, and return-to-sport protocols. DBS enhanced checked.',
   4.7, 156, 'SportsFit Rehab', 'Wimbledon', 'London', 51.4214, -0.2064, true, 2100),

  ('5', 'dr-hannah-wright', 'Dr. Hannah Wright', '', 'female', 8,
   'I specialise in women''s health physiotherapy including prenatal & postnatal care, pelvic floor rehabilitation, and osteoporosis management. I provide a safe, comfortable environment for women''s health concerns.',
   4.8, 112, 'Bloom Women''s Physio', 'Clapham', 'London', 51.4627, -0.138, true, 1350),

  ('6', 'dr-oliver-patel', 'Dr. Oliver Patel', '', 'male', 10,
   'I help patients with cardiac and respiratory conditions improve their quality of life through specialised rehabilitation. Experienced in post-cardiac surgery rehab, COPD management, and long COVID recovery.',
   4.6, 78, 'CardioFit Rehab', 'Greenwich', 'London', 51.4769, -0.0005, true, 1560);

-- Physio ↔ Specialization links
INSERT INTO physioconnect_physio_specializations ("physioId", "specializationId") VALUES
  ('1', 'spec-1'), ('1', 'spec-3'),
  ('2', 'spec-2'), ('2', 'spec-5'),
  ('3', 'spec-4'), ('3', 'spec-2'),
  ('4', 'spec-1'), ('4', 'spec-8'),
  ('5', 'spec-7'), ('5', 'spec-3'),
  ('6', 'spec-6'), ('6', 'spec-5');

-- Qualifications
INSERT INTO physioconnect_qualifications (id, "physioId", text, "sortOrder") VALUES
  ('q1',  '1', 'MSc Physiotherapy — King''s College London', 0),
  ('q2',  '1', 'BSc (Hons) Physiotherapy — University of Birmingham', 1),
  ('q3',  '1', 'HCPC Registered (PH123456)', 2),
  ('q4',  '1', 'CSP Member · Certified Sports Rehabilitation Specialist', 3),
  ('q5',  '2', 'PhD Neurorehabilitation — UCL', 0),
  ('q6',  '2', 'MSc Physiotherapy — University of Manchester', 1),
  ('q7',  '2', 'HCPC Registered (PH234567)', 2),
  ('q8',  '2', 'CSP Member · Certified Neuro-Rehabilitation Specialist', 3),
  ('q9',  '3', 'MSc Paediatric Physiotherapy — Great Ormond Street', 0),
  ('q10', '3', 'BSc (Hons) Physiotherapy — Cardiff University', 1),
  ('q11', '3', 'HCPC Registered (PH345678)', 2),
  ('q12', '3', 'CSP Member · APCP Member', 3),
  ('q13', '4', 'MSc Sports Physiotherapy — Loughborough University', 0),
  ('q14', '4', 'BSc (Hons) Physiotherapy — University of Nottingham', 1),
  ('q15', '4', 'HCPC Registered (PH456789)', 2),
  ('q16', '4', 'FA Medical License · CSP Member', 3),
  ('q17', '5', 'MSc Women''s Health Physiotherapy — King''s College London', 0),
  ('q18', '5', 'BSc (Hons) Physiotherapy — University of Southampton', 1),
  ('q19', '5', 'HCPC Registered (PH567890)', 2),
  ('q20', '5', 'CSP Member · Pelvic Floor Rehabilitation Specialist', 3),
  ('q21', '6', 'MSc Cardiopulmonary Physiotherapy — Imperial College London', 0),
  ('q22', '6', 'BSc (Hons) Physiotherapy — University of Leeds', 1),
  ('q23', '6', 'HCPC Registered (PH678901)', 2),
  ('q24', '6', 'CSP Member · BTS Certified Pulmonary Rehab Specialist', 3);

-- Visit Types
INSERT INTO physioconnect_physio_visit_types ("physioId", "visitType") VALUES
  ('1', 'clinic'), ('1', 'home'), ('1', 'online'),
  ('2', 'clinic'), ('2', 'home'),
  ('3', 'clinic'), ('3', 'home'), ('3', 'online'),
  ('4', 'clinic'), ('4', 'home'),
  ('5', 'clinic'), ('5', 'home'), ('5', 'online'),
  ('6', 'clinic'), ('6', 'home');

-- Services
INSERT INTO physioconnect_services (id, "physioId", name, duration, price, description, "sortOrder") VALUES
  ('s1-1', '1', 'Initial Assessment',   45, 85,  'Comprehensive evaluation & treatment plan', 0),
  ('s1-2', '1', 'Follow-up Session',    30, 65,  'Continued treatment & exercise prescription', 1),
  ('s1-3', '1', 'Home Visit',           60, 110, 'Physiotherapy at your doorstep', 2),
  ('s1-4', '1', 'Online Consultation',  30, 50,  'Video call assessment & guidance', 3),
  ('s2-1', '2', 'Neuro Assessment',     60, 95,  'Full neurological evaluation', 0),
  ('s2-2', '2', 'Rehabilitation Session', 45, 75, 'Targeted neuro rehab exercises', 1),
  ('s2-3', '2', 'Home Visit',           60, 120, 'Home-based neuro rehabilitation', 2),
  ('s3-1', '3', 'Paediatric Assessment', 45, 80, 'Developmental evaluation for children', 0),
  ('s3-2', '3', 'Therapy Session',      30, 60,  'Play-based therapy session', 1),
  ('s3-3', '3', 'Home Visit',           45, 95,  'Therapy in comfortable home setting', 2),
  ('s3-4', '3', 'Parent Consultation',  30, 45,  'Guidance for home exercises', 3),
  ('s4-1', '4', 'Sports Assessment',    60, 100, 'Comprehensive sports injury evaluation', 0),
  ('s4-2', '4', 'Rehab Session',        45, 80,  'Sport-specific rehabilitation', 1),
  ('s4-3', '4', 'Home Visit',           60, 120, 'Home-based sports rehab', 2),
  ('s5-1', '5', 'Initial Consultation', 45, 85,  'Comprehensive women''s health assessment', 0),
  ('s5-2', '5', 'Therapy Session',      30, 65,  'Targeted treatment session', 1),
  ('s5-3', '5', 'Prenatal Physio',      30, 60,  'Pregnancy-safe exercises & pain relief', 2),
  ('s5-4', '5', 'Home Visit',           45, 100, 'Home-based women''s health physio', 3),
  ('s6-1', '6', 'Cardio Assessment',    60, 90,  'Cardiac rehabilitation evaluation', 0),
  ('s6-2', '6', 'Rehab Session',        45, 70,  'Supervised cardiac exercise programme', 1),
  ('s6-3', '6', 'Home Visit',           60, 110, 'Home-based cardiac rehab', 2);

-- Reviews
INSERT INTO physioconnect_reviews (id, "physioId", "patientName", rating, date, text, verified) VALUES
  ('r1', '1', 'Claire M.', 5, '2026-01-25',
   'Excellent rehab programme for my ACL tear. Dr. Mitchell is incredibly professional and caring. She explained every exercise and why it mattered. I''m back playing netball!', true),
  ('r2', '1', 'Tom K.', 5, '2026-01-20',
   'Best sports physio in London. Helped me recover from a shoulder injury in record time. Highly recommend the home visit option!', true),
  ('r3', '1', 'Sophie D.', 4, '2026-01-15',
   'Very knowledgeable and thorough. The clinic is well-equipped and spotless. Only giving 4 stars as the first appointment was slightly delayed.', true),
  ('r4', '1', 'David S.', 5, '2026-01-10',
   'Dr. Mitchell''s treatment plan for my lower back pain worked wonders. 6 sessions and I was pain-free. She also taught me preventive exercises I still use daily.', true),
  ('r5', '2', 'Margaret R.', 5, '2026-01-28',
   'Dr. Thompson helped my father recover from a stroke. His patience and expertise are remarkable. The home visit rehab programme was tailored perfectly.', true),
  ('r6', '2', 'Andrew N.', 5, '2026-01-22',
   'Outstanding neuro rehabilitation. After my spinal surgery, Dr. Thompson''s programme helped me walk independently again. Forever grateful.', true),
  ('r7', '3', 'Rachel P.', 5, '2026-01-26',
   'My daughter loves going to Dr. Chen''s sessions! She makes therapy feel like play. We''ve seen brilliant progress in just 3 months.', true),
  ('r8', '4', 'Chris V.', 5, '2026-01-24',
   'As a marathon runner, Dr. Williams understood exactly what I needed. His sport-specific rehab got me back to training within weeks.', true),
  ('r9', '5', 'Laura G.', 5, '2026-01-27',
   'Dr. Wright was brilliant during my pregnancy-related back pain. So gentle and understanding. The prenatal exercises were genuinely life-changing.', true),
  ('r10', '6', 'Robert R.', 4, '2026-01-23',
   'Excellent cardiac rehab programme. Dr. Patel monitors everything carefully and adjusts exercises based on my heart rate response. Really professional.', true);

-- Bookings
INSERT INTO physioconnect_bookings (id, "physioId", "physioName", "patientName", "patientEmail", "patientPhone", "serviceId", "serviceName", date, time, duration, fee, "platformFee", status, "createdAt") VALUES
  ('PHC-20260209-4731', '1', 'Dr. Sarah Mitchell', 'Claire Mehta', 'claire@email.com', '+44 7700 900123', 's1-2', 'Follow-up Session', '2026-02-09', '16:00', 30, 65, 5, 'confirmed', '2026-02-09T10:30:00Z'),
  ('PHC-20260209-4730', '2', 'Dr. James Thompson', 'Robert Kumar', 'robert@email.com', '+44 7700 900124', 's2-2', 'Rehabilitation Session', '2026-02-09', '14:30', 45, 75, 5, 'confirmed', '2026-02-09T09:15:00Z'),
  ('PHC-20260208-4729', '3', 'Dr. Emily Chen', 'Rachel Prakash', 'rachel@email.com', '+44 7700 900125', 's3-2', 'Therapy Session', '2026-02-08', '10:00', 30, 60, 5, 'completed', '2026-02-07T14:00:00Z'),
  ('PHC-20260210-4732', '4', 'Dr. Marcus Williams', 'Chris V.', 'chris@email.com', '+44 7700 900126', 's4-1', 'Sports Assessment', '2026-02-10', '09:00', 60, 100, 5, 'pending', '2026-02-09T11:00:00Z'),
  ('PHC-20260207-4728', '5', 'Dr. Hannah Wright', 'Laura G.', 'laura@email.com', '+44 7700 900127', 's5-3', 'Prenatal Physio', '2026-02-07', '11:00', 30, 60, 5, 'completed', '2026-02-06T16:00:00Z'),
  ('PHC-20260209-4733', '1', 'Dr. Sarah Mitchell', 'David Singh', 'david@email.com', '+44 7700 900128', 's1-1', 'Initial Assessment', '2026-02-11', '10:00', 45, 85, 5, 'confirmed', '2026-02-09T12:00:00Z');
