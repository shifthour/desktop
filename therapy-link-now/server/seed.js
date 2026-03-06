import { createClient } from '@supabase/supabase-js';
import bcrypt from 'bcryptjs';
import { v4 as uuid } from 'uuid';

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseServiceKey) {
  console.error('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseServiceKey);

async function seed() {
  console.log('Seeding database...');

  // Clear existing data (in FK order)
  await supabase.from('physioconnect_blocked_slots').delete().neq('id', '00000000-0000-0000-0000-000000000000');
  await supabase.from('physioconnect_availability_overrides').delete().neq('id', '00000000-0000-0000-0000-000000000000');
  await supabase.from('physioconnect_reviews').delete().neq('id', '00000000-0000-0000-0000-000000000000');
  await supabase.from('physioconnect_appointments').delete().neq('id', '00000000-0000-0000-0000-000000000000');
  await supabase.from('physioconnect_physiotherapists').delete().neq('id', '00000000-0000-0000-0000-000000000000');
  await supabase.from('physioconnect_users').delete().neq('id', '00000000-0000-0000-0000-000000000000');

  // --- Users ---
  const adminId = uuid();
  const patientId = uuid();

  const { error: userError } = await supabase.from('physioconnect_users').insert([
    { id: adminId, email: 'admin@physioconnect.com', password: bcrypt.hashSync('admin123', 10), full_name: 'Admin User', phone: '+44 7700 900000', role: 'admin' },
    { id: patientId, email: 'patient@example.com', password: bcrypt.hashSync('patient123', 10), full_name: 'Jane Patient', phone: '+44 7700 900001', role: 'patient' },
  ]);

  if (userError) { console.error('User insert error:', userError); return; }
  console.log('  Users created');

  // --- Physiotherapists ---
  const physios = [
    {
      full_name: 'Dr. Ananya Sharma', email: 'ananya@physioconnect.com', phone: '+44 7700 100001',
      bio: 'Specialist in sports rehabilitation with 8+ years experience. Worked with professional athletes and weekend warriors alike. Passionate about evidence-based treatment and helping patients return to their active lifestyles.',
      photo_url: 'https://images.unsplash.com/photo-1643297654416-05795d62e39c?w=400&h=400&fit=crop&crop=face&q=80',
      specializations: ['Sports Injury', 'Orthopedic'],
      qualifications: ['BPT', 'MPT Sports Medicine', 'Certified Sports Physiotherapist'],
      experience_years: 8, consultation_fee: 80, visit_type: 'both',
      clinic_name: 'ActiveLife Physio Clinic', clinic_address: '45 High Street, Kensington', city: 'London',
      session_duration: 45, languages: ['English', 'Hindi'],
      available_days: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
      working_hours_start: '09:00', working_hours_end: '18:00',
      status: 'approved', rating: 4.9, review_count: 47, gender: 'female',
    },
    {
      full_name: 'Dr. Rajesh Gupta', email: 'rajesh@physioconnect.com', phone: '+44 7700 100002',
      bio: "Expert in neurological rehabilitation and elderly care. Over a decade of experience helping stroke survivors and patients with Parkinson's disease regain mobility and independence.",
      photo_url: 'https://images.unsplash.com/photo-1612349317150-e413f6a5b16d?w=400&h=400&fit=crop&crop=face&q=80',
      specializations: ['Neurological', 'Geriatric'],
      qualifications: ['BPT', 'MPT Neurology', 'PhD Rehabilitation Sciences'],
      experience_years: 12, consultation_fee: 100, visit_type: 'clinic',
      clinic_name: 'NeuroRehab Centre', clinic_address: '12 Oxford Road', city: 'Manchester',
      session_duration: 60, languages: ['English', 'Hindi', 'Punjabi'],
      available_days: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
      working_hours_start: '08:00', working_hours_end: '17:00',
      status: 'approved', rating: 4.8, review_count: 63, gender: 'male',
    },
    {
      full_name: 'Dr. Meera Nair', email: 'meera@physioconnect.com', phone: '+44 7700 100003',
      bio: 'Passionate about helping children reach developmental milestones. Specializes in pediatric neurological conditions including cerebral palsy and developmental delay.',
      photo_url: 'https://images.unsplash.com/photo-1559839734-2b71ea197ec2?w=400&h=400&fit=crop&crop=face&q=80',
      specializations: ['Pediatric', 'Neurological'],
      qualifications: ['BPT', 'MPT Pediatrics', 'Bobath Certified'],
      experience_years: 6, consultation_fee: 70, visit_type: 'both',
      clinic_name: 'Little Steps Therapy', clinic_address: '88 Harley Street', city: 'London',
      session_duration: 45, languages: ['English', 'Malayalam'],
      available_days: ['Monday', 'Wednesday', 'Thursday', 'Friday'],
      working_hours_start: '09:00', working_hours_end: '17:00',
      status: 'approved', rating: 4.9, review_count: 35, gender: 'female',
    },
    {
      full_name: 'Dr. Vikram Singh', email: 'vikram@physioconnect.com', phone: '+44 7700 100004',
      bio: 'Former professional sports team physiotherapist specializing in ACL rehabilitation and post-surgical recovery. Combines manual therapy with cutting-edge rehabilitation protocols.',
      photo_url: 'https://images.unsplash.com/photo-1622253692010-333f2da6031d?w=400&h=400&fit=crop&crop=face&q=80',
      specializations: ['Sports Injury', 'Post-Surgical'],
      qualifications: ['BPT', 'MSc Sports Physiotherapy', 'FIFA Diploma Sports Medicine'],
      experience_years: 10, consultation_fee: 120, visit_type: 'clinic',
      clinic_name: 'Elite Sports Physio', clinic_address: '3 Broad Street', city: 'Birmingham',
      session_duration: 60, languages: ['English', 'Hindi'],
      available_days: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday'],
      working_hours_start: '07:00', working_hours_end: '16:00',
      status: 'approved', rating: 4.7, review_count: 52, gender: 'male',
    },
    {
      full_name: 'Dr. Priya Patel', email: 'priya@physioconnect.com', phone: '+44 7700 100005',
      bio: "Specializing in prenatal, postnatal, and pelvic floor rehabilitation. Dedicated to women's health physiotherapy with a holistic approach to recovery.",
      photo_url: 'https://images.unsplash.com/photo-1614608682850-e0d6ed316d47?w=400&h=400&fit=crop&crop=face&q=80',
      specializations: ["Women's Health", 'Orthopedic'],
      qualifications: ['BPT', "MSc Women's Health", 'Certified Pelvic Floor Therapist'],
      experience_years: 7, consultation_fee: 85, visit_type: 'both',
      clinic_name: 'Bloom Wellness Physio', clinic_address: "22 King's Road, Chelsea", city: 'London',
      session_duration: 50, languages: ['English', 'Gujarati', 'Hindi'],
      available_days: ['Monday', 'Tuesday', 'Wednesday', 'Friday'],
      working_hours_start: '09:00', working_hours_end: '17:00',
      status: 'approved', rating: 4.8, review_count: 41, gender: 'female',
    },
    {
      full_name: 'Dr. Arjun Menon', email: 'arjun@physioconnect.com', phone: '+44 7700 100006',
      bio: 'Cardiac and respiratory rehabilitation specialist. Helps patients recover from heart surgery, COPD, and other cardiopulmonary conditions through tailored exercise programmes.',
      photo_url: 'https://images.unsplash.com/photo-1537368910025-700350fe46c7?w=400&h=400&fit=crop&crop=face&q=80',
      specializations: ['Cardiopulmonary', 'Geriatric'],
      qualifications: ['BPT', 'MPT Cardiopulmonary', 'ACPRC Member'],
      experience_years: 9, consultation_fee: 95, visit_type: 'home_visit',
      clinic_name: null, clinic_address: null, city: 'Leeds',
      session_duration: 45, languages: ['English', 'Tamil'],
      available_days: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
      working_hours_start: '08:00', working_hours_end: '18:00',
      status: 'approved', rating: 4.6, review_count: 28, gender: 'male',
    },
    {
      full_name: 'Dr. Sarah Thompson', email: 'sarah@physioconnect.com', phone: '+44 7700 100007',
      bio: 'Hip and knee replacement rehabilitation expert. Over 11 years of experience working with orthopaedic surgeons and delivering exceptional post-operative outcomes.',
      photo_url: 'https://images.unsplash.com/photo-1551836022-d5d88e9218df?w=400&h=400&fit=crop&crop=face&q=80',
      specializations: ['Orthopedic', 'Post-Surgical'],
      qualifications: ['BSc Physiotherapy', 'MSc Musculoskeletal', 'MACP'],
      experience_years: 11, consultation_fee: 110, visit_type: 'clinic',
      clinic_name: 'Thompson Ortho Physio', clinic_address: '15 Wimpole Street', city: 'London',
      session_duration: 60, languages: ['English', 'French'],
      available_days: ['Monday', 'Tuesday', 'Wednesday', 'Thursday'],
      working_hours_start: '08:30', working_hours_end: '17:30',
      status: 'approved', rating: 4.7, review_count: 56, gender: 'female',
    },
    {
      full_name: 'Dr. James Wilson', email: 'james@physioconnect.com', phone: '+44 7700 100008',
      bio: 'Young and energetic physiotherapist focused on getting athletes back on track. Combines sports injury rehabilitation with respiratory physiotherapy for comprehensive care.',
      photo_url: 'https://images.unsplash.com/photo-1582750433449-648ed127bb54?w=400&h=400&fit=crop&crop=face&q=80',
      specializations: ['Sports Injury', 'Respiratory'],
      qualifications: ['BSc Physiotherapy', 'MSc Sports Rehabilitation'],
      experience_years: 5, consultation_fee: 90, visit_type: 'both',
      clinic_name: 'Wilson Sports Therapy', clinic_address: '7 Deansgate', city: 'Manchester',
      session_duration: 45, languages: ['English'],
      available_days: ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'],
      working_hours_start: '07:30', working_hours_end: '19:00',
      status: 'approved', rating: 4.5, review_count: 19, gender: 'male',
    },
  ];

  const physioRows = physios.map((p) => ({
    id: uuid(),
    user_id: null,
    certificate_urls: [],
    ...p,
  }));

  const { error: physioError } = await supabase.from('physioconnect_physiotherapists').insert(physioRows);
  if (physioError) { console.error('Physio insert error:', physioError); return; }
  console.log(`  ${physios.length} physiotherapists created`);

  // --- Reviews ---
  const reviews = [
    { physio_idx: 0, patient_name: 'Michael Brown', rating: 5, comment: 'Dr. Sharma was absolutely fantastic! My knee pain is completely gone after 6 sessions. Highly recommend her sports rehab programme.', created_date: '2025-12-15' },
    { physio_idx: 0, patient_name: 'Emily Clark', rating: 5, comment: 'Very professional and knowledgeable. She explained everything clearly and the exercises really helped my recovery.', created_date: '2025-11-20' },
    { physio_idx: 1, patient_name: 'Robert Taylor', rating: 5, comment: 'Dr. Gupta helped my father recover from his stroke. The improvement in just 3 months was remarkable. We are very grateful.', created_date: '2025-10-30' },
    { physio_idx: 2, patient_name: 'Sophie Williams', rating: 5, comment: 'Dr. Nair worked wonders with my daughter who has cerebral palsy. She is so patient and caring with children.', created_date: '2025-11-10' },
    { physio_idx: 4, patient_name: 'Hannah Jones', rating: 4, comment: "Great experience with postnatal recovery. Dr. Patel really understands women's health issues. Would recommend to all new mums.", created_date: '2025-12-01' },
  ];

  const reviewRows = reviews.map((r) => ({
    id: uuid(),
    user_id: patientId,
    physio_id: physioRows[r.physio_idx].id,
    patient_name: r.patient_name,
    rating: r.rating,
    comment: r.comment,
    created_date: r.created_date,
  }));

  const { error: reviewError } = await supabase.from('physioconnect_reviews').insert(reviewRows);
  if (reviewError) { console.error('Review insert error:', reviewError); return; }
  console.log(`  ${reviews.length} reviews created`);

  console.log('Seed complete!');
}

seed().catch(console.error);
