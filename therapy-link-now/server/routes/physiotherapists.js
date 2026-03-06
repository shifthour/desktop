import { Router } from 'express';
import { v4 as uuid } from 'uuid';
import bcrypt from 'bcryptjs';
import supabase from '../supabase.js';
import { optionalAuth, signToken } from '../middleware/auth.js';

const router = Router();

// GET /api/physiotherapists?status=approved&id=xxx&user_id=xxx&sort=-rating&limit=10
router.get('/', async (req, res) => {
  try {
    const { status, id, user_id, sort, limit } = req.query;

    let query = supabase.from('physioconnect_physiotherapists').select('*');

    if (status) {
      query = query.eq('status', status);
    }
    if (id) {
      query = query.eq('id', id);
    }
    if (user_id) {
      query = query.eq('user_id', user_id);
    }

    if (sort) {
      const desc = sort.startsWith('-');
      const col = desc ? sort.slice(1) : sort;
      const allowed = ['rating', 'consultation_fee', 'experience_years', 'created_at', 'full_name'];
      if (allowed.includes(col)) {
        query = query.order(col, { ascending: !desc });
      }
    } else {
      query = query.order('rating', { ascending: false });
    }

    if (limit) {
      query = query.limit(Number(limit));
    }

    const { data, error } = await query;
    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/physiotherapists
router.post('/', optionalAuth, async (req, res) => {
  try {
    const data = req.body;
    const id = uuid();
    let userId = req.user?.id || null;

    // If password provided and no existing user, create a user account with role "physio"
    if (data.password && !userId) {
      const { data: existing } = await supabase
        .from('physioconnect_users')
        .select('id')
        .eq('email', data.email)
        .maybeSingle();

      if (existing) {
        return res.status(400).json({ error: 'An account with this email already exists' });
      }

      userId = uuid();
      const hashed = bcrypt.hashSync(data.password, 10);

      const { error: userError } = await supabase
        .from('physioconnect_users')
        .insert({
          id: userId,
          email: data.email,
          password: hashed,
          full_name: data.full_name,
          phone: data.phone || null,
          role: 'physio',
        });

      if (userError) throw userError;
    }

    const { data: row, error } = await supabase
      .from('physioconnect_physiotherapists')
      .insert({
        id,
        user_id: userId,
        full_name: data.full_name,
        email: data.email || null,
        phone: data.phone || null,
        bio: data.bio || null,
        photo_url: data.photo_url || null,
        specializations: data.specializations || [],
        qualifications: data.qualifications || [],
        certificate_urls: data.certificate_urls || [],
        experience_years: data.experience_years || 0,
        consultation_fee: data.consultation_fee || 0,
        visit_type: data.visit_type || 'clinic',
        clinic_name: data.clinic_name || null,
        clinic_address: data.clinic_address || null,
        city: data.city || null,
        session_duration: data.session_duration || 45,
        languages: data.languages || ['English'],
        available_days: data.available_days || [],
        working_hours_start: data.working_hours_start || '09:00',
        working_hours_end: data.working_hours_end || '17:00',
        status: data.status || 'pending',
        rating: data.rating || 0,
        review_count: data.review_count || 0,
        gender: data.gender || null,
      })
      .select()
      .single();

    if (error) throw error;

    const result = { ...row };

    // If we created a user account, return a token so the physio is auto-logged-in
    if (data.password && userId) {
      result.token = signToken({ id: userId, email: data.email, role: 'physio', full_name: data.full_name });
    }

    res.json(result);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/physiotherapists/:id
router.patch('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const data = req.body;

    const { data: existing } = await supabase
      .from('physioconnect_physiotherapists')
      .select('*')
      .eq('id', id)
      .maybeSingle();

    if (!existing) return res.status(404).json({ error: 'Not found' });

    const updates = {};
    for (const [key, value] of Object.entries(data)) {
      if (key === 'id' || key === 'created_at') continue;
      updates[key] = value;
    }

    const { data: row, error } = await supabase
      .from('physioconnect_physiotherapists')
      .update(updates)
      .eq('id', id)
      .select()
      .single();

    if (error) throw error;

    res.json(row);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/physiotherapists/:id
router.delete('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: existing } = await supabase
      .from('physioconnect_physiotherapists')
      .select('*')
      .eq('id', id)
      .maybeSingle();

    if (!existing) return res.status(404).json({ error: 'Not found' });

    // Delete related records first (cascade)
    await supabase.from('physioconnect_reviews').delete().eq('physio_id', id);
    await supabase.from('physioconnect_blocked_slots').delete().eq('physio_id', id);
    await supabase.from('physioconnect_availability_overrides').delete().eq('physio_id', id);
    await supabase.from('physioconnect_appointments').delete().eq('physio_id', id);

    const { error } = await supabase
      .from('physioconnect_physiotherapists')
      .delete()
      .eq('id', id);

    if (error) throw error;

    // Also delete linked user account if exists
    if (existing.user_id) {
      await supabase.from('physioconnect_users').delete().eq('id', existing.user_id);
    }

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
