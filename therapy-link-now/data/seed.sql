-- PhysioConnect Seed Data
-- Run this in the Supabase SQL Editor AFTER running schema.sql
-- Passwords are pre-computed bcrypt hashes (cost 10)

-- ============================================================
-- Clear existing data (in FK order)
-- ============================================================
DELETE FROM physioconnect_blocked_slots;
DELETE FROM physioconnect_availability_overrides;
DELETE FROM physioconnect_reviews;
DELETE FROM physioconnect_appointments;
DELETE FROM physioconnect_physiotherapists;
DELETE FROM physioconnect_users;

-- ============================================================
-- Users
-- ============================================================
-- admin123  → $2a$10$umfFpxHOkbtlC4vSXQfew.7iPXwypd6kcFgDGLM4jkdkPJITzQBze
-- patient123 → $2a$10$aNlR9dWxdKPDlTgH9bILXO31L3PHih2wdmtOdt86l8L49PfP4Z8Uu

INSERT INTO physioconnect_users (id, email, password, full_name, phone, role) VALUES
  ('a0000000-0000-0000-0000-000000000001', 'admin@physioconnect.com',
   '$2a$10$umfFpxHOkbtlC4vSXQfew.7iPXwypd6kcFgDGLM4jkdkPJITzQBze',
   'Admin User', '+44 7700 900000', 'admin'),
  ('a0000000-0000-0000-0000-000000000002', 'patient@example.com',
   '$2a$10$aNlR9dWxdKPDlTgH9bILXO31L3PHih2wdmtOdt86l8L49PfP4Z8Uu',
   'Jane Patient', '+44 7700 900001', 'patient');

-- ============================================================
-- Physiotherapists (8 physios, hardcoded UUIDs)
-- ============================================================
INSERT INTO physioconnect_physiotherapists
  (id, user_id, full_name, email, phone, bio, photo_url,
   specializations, qualifications, certificate_urls,
   experience_years, consultation_fee, visit_type,
   clinic_name, clinic_address, city,
   session_duration, languages, available_days,
   working_hours_start, working_hours_end,
   status, rating, review_count, gender)
VALUES
  -- 1. Dr. Ananya Sharma
  ('b0000000-0000-0000-0000-000000000001', NULL,
   'Dr. Ananya Sharma', 'ananya@physioconnect.com', '+44 7700 100001',
   'Specialist in sports rehabilitation with 8+ years experience. Worked with professional athletes and weekend warriors alike. Passionate about evidence-based treatment and helping patients return to their active lifestyles.',
   'https://images.unsplash.com/photo-1643297654416-05795d62e39c?w=400&h=400&fit=crop&crop=face&q=80',
   '["Sports Injury", "Orthopedic"]'::jsonb,
   '["BPT", "MPT Sports Medicine", "Certified Sports Physiotherapist"]'::jsonb,
   '[]'::jsonb,
   8, 80, 'both',
   'ActiveLife Physio Clinic', '45 High Street, Kensington', 'London',
   45, '["English", "Hindi"]'::jsonb,
   '["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]'::jsonb,
   '09:00', '18:00',
   'approved', 4.9, 47, 'female'),

  -- 2. Dr. Rajesh Gupta
  ('b0000000-0000-0000-0000-000000000002', NULL,
   'Dr. Rajesh Gupta', 'rajesh@physioconnect.com', '+44 7700 100002',
   'Expert in neurological rehabilitation and elderly care. Over a decade of experience helping stroke survivors and patients with Parkinson''s disease regain mobility and independence.',
   'https://images.unsplash.com/photo-1612349317150-e413f6a5b16d?w=400&h=400&fit=crop&crop=face&q=80',
   '["Neurological", "Geriatric"]'::jsonb,
   '["BPT", "MPT Neurology", "PhD Rehabilitation Sciences"]'::jsonb,
   '[]'::jsonb,
   12, 100, 'clinic',
   'NeuroRehab Centre', '12 Oxford Road', 'Manchester',
   60, '["English", "Hindi", "Punjabi"]'::jsonb,
   '["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]'::jsonb,
   '08:00', '17:00',
   'approved', 4.8, 63, 'male'),

  -- 3. Dr. Meera Nair
  ('b0000000-0000-0000-0000-000000000003', NULL,
   'Dr. Meera Nair', 'meera@physioconnect.com', '+44 7700 100003',
   'Passionate about helping children reach developmental milestones. Specializes in pediatric neurological conditions including cerebral palsy and developmental delay.',
   'https://images.unsplash.com/photo-1559839734-2b71ea197ec2?w=400&h=400&fit=crop&crop=face&q=80',
   '["Pediatric", "Neurological"]'::jsonb,
   '["BPT", "MPT Pediatrics", "Bobath Certified"]'::jsonb,
   '[]'::jsonb,
   6, 70, 'both',
   'Little Steps Therapy', '88 Harley Street', 'London',
   45, '["English", "Malayalam"]'::jsonb,
   '["Monday", "Wednesday", "Thursday", "Friday"]'::jsonb,
   '09:00', '17:00',
   'approved', 4.9, 35, 'female'),

  -- 4. Dr. Vikram Singh
  ('b0000000-0000-0000-0000-000000000004', NULL,
   'Dr. Vikram Singh', 'vikram@physioconnect.com', '+44 7700 100004',
   'Former professional sports team physiotherapist specializing in ACL rehabilitation and post-surgical recovery. Combines manual therapy with cutting-edge rehabilitation protocols.',
   'https://images.unsplash.com/photo-1622253692010-333f2da6031d?w=400&h=400&fit=crop&crop=face&q=80',
   '["Sports Injury", "Post-Surgical"]'::jsonb,
   '["BPT", "MSc Sports Physiotherapy", "FIFA Diploma Sports Medicine"]'::jsonb,
   '[]'::jsonb,
   10, 120, 'clinic',
   'Elite Sports Physio', '3 Broad Street', 'Birmingham',
   60, '["English", "Hindi"]'::jsonb,
   '["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]'::jsonb,
   '07:00', '16:00',
   'approved', 4.7, 52, 'male'),

  -- 5. Dr. Priya Patel
  ('b0000000-0000-0000-0000-000000000005', NULL,
   'Dr. Priya Patel', 'priya@physioconnect.com', '+44 7700 100005',
   'Specializing in prenatal, postnatal, and pelvic floor rehabilitation. Dedicated to women''s health physiotherapy with a holistic approach to recovery.',
   'https://images.unsplash.com/photo-1614608682850-e0d6ed316d47?w=400&h=400&fit=crop&crop=face&q=80',
   '["Women''s Health", "Orthopedic"]'::jsonb,
   '["BPT", "MSc Women''s Health", "Certified Pelvic Floor Therapist"]'::jsonb,
   '[]'::jsonb,
   7, 85, 'both',
   'Bloom Wellness Physio', '22 King''s Road, Chelsea', 'London',
   50, '["English", "Gujarati", "Hindi"]'::jsonb,
   '["Monday", "Tuesday", "Wednesday", "Friday"]'::jsonb,
   '09:00', '17:00',
   'approved', 4.8, 41, 'female'),

  -- 6. Dr. Arjun Menon
  ('b0000000-0000-0000-0000-000000000006', NULL,
   'Dr. Arjun Menon', 'arjun@physioconnect.com', '+44 7700 100006',
   'Cardiac and respiratory rehabilitation specialist. Helps patients recover from heart surgery, COPD, and other cardiopulmonary conditions through tailored exercise programmes.',
   'https://images.unsplash.com/photo-1537368910025-700350fe46c7?w=400&h=400&fit=crop&crop=face&q=80',
   '["Cardiopulmonary", "Geriatric"]'::jsonb,
   '["BPT", "MPT Cardiopulmonary", "ACPRC Member"]'::jsonb,
   '[]'::jsonb,
   9, 95, 'home_visit',
   NULL, NULL, 'Leeds',
   45, '["English", "Tamil"]'::jsonb,
   '["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]'::jsonb,
   '08:00', '18:00',
   'approved', 4.6, 28, 'male'),

  -- 7. Dr. Sarah Thompson
  ('b0000000-0000-0000-0000-000000000007', NULL,
   'Dr. Sarah Thompson', 'sarah@physioconnect.com', '+44 7700 100007',
   'Hip and knee replacement rehabilitation expert. Over 11 years of experience working with orthopaedic surgeons and delivering exceptional post-operative outcomes.',
   'https://images.unsplash.com/photo-1551836022-d5d88e9218df?w=400&h=400&fit=crop&crop=face&q=80',
   '["Orthopedic", "Post-Surgical"]'::jsonb,
   '["BSc Physiotherapy", "MSc Musculoskeletal", "MACP"]'::jsonb,
   '[]'::jsonb,
   11, 110, 'clinic',
   'Thompson Ortho Physio', '15 Wimpole Street', 'London',
   60, '["English", "French"]'::jsonb,
   '["Monday", "Tuesday", "Wednesday", "Thursday"]'::jsonb,
   '08:30', '17:30',
   'approved', 4.7, 56, 'female'),

  -- 8. Dr. James Wilson
  ('b0000000-0000-0000-0000-000000000008', NULL,
   'Dr. James Wilson', 'james@physioconnect.com', '+44 7700 100008',
   'Young and energetic physiotherapist focused on getting athletes back on track. Combines sports injury rehabilitation with respiratory physiotherapy for comprehensive care.',
   'https://images.unsplash.com/photo-1582750433449-648ed127bb54?w=400&h=400&fit=crop&crop=face&q=80',
   '["Sports Injury", "Respiratory"]'::jsonb,
   '["BSc Physiotherapy", "MSc Sports Rehabilitation"]'::jsonb,
   '[]'::jsonb,
   5, 90, 'both',
   'Wilson Sports Therapy', '7 Deansgate', 'Manchester',
   45, '["English"]'::jsonb,
   '["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]'::jsonb,
   '07:30', '19:00',
   'approved', 4.5, 19, 'male');

-- ============================================================
-- Reviews (5 reviews using hardcoded UUIDs above)
-- ============================================================
INSERT INTO physioconnect_reviews (id, user_id, physio_id, patient_name, rating, comment, created_date) VALUES
  ('c0000000-0000-0000-0000-000000000001',
   'a0000000-0000-0000-0000-000000000002',
   'b0000000-0000-0000-0000-000000000001',
   'Michael Brown', 5,
   'Dr. Sharma was absolutely fantastic! My knee pain is completely gone after 6 sessions. Highly recommend her sports rehab programme.',
   '2025-12-15'),

  ('c0000000-0000-0000-0000-000000000002',
   'a0000000-0000-0000-0000-000000000002',
   'b0000000-0000-0000-0000-000000000001',
   'Emily Clark', 5,
   'Very professional and knowledgeable. She explained everything clearly and the exercises really helped my recovery.',
   '2025-11-20'),

  ('c0000000-0000-0000-0000-000000000003',
   'a0000000-0000-0000-0000-000000000002',
   'b0000000-0000-0000-0000-000000000002',
   'Robert Taylor', 5,
   'Dr. Gupta helped my father recover from his stroke. The improvement in just 3 months was remarkable. We are very grateful.',
   '2025-10-30'),

  ('c0000000-0000-0000-0000-000000000004',
   'a0000000-0000-0000-0000-000000000002',
   'b0000000-0000-0000-0000-000000000003',
   'Sophie Williams', 5,
   'Dr. Nair worked wonders with my daughter who has cerebral palsy. She is so patient and caring with children.',
   '2025-11-10'),

  ('c0000000-0000-0000-0000-000000000005',
   'a0000000-0000-0000-0000-000000000002',
   'b0000000-0000-0000-0000-000000000005',
   'Hannah Jones', 4,
   'Great experience with postnatal recovery. Dr. Patel really understands women''s health issues. Would recommend to all new mums.',
   '2025-12-01');
