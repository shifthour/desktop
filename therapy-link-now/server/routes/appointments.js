import { Router } from 'express';
import { v4 as uuid } from 'uuid';
import supabase from '../supabase.js';
import { optionalAuth } from '../middleware/auth.js';

const router = Router();

// GET /api/appointments?physio_id=xxx&user_id=xxx&status=xxx&sort=-appointment_date
router.get('/', optionalAuth, async (req, res) => {
  try {
    const { physio_id, user_id, status, sort } = req.query;

    let query = supabase.from('physioconnect_appointments').select('*');

    if (physio_id) {
      query = query.eq('physio_id', physio_id);
    }
    if (user_id) {
      query = query.eq('user_id', user_id);
    } else if (req.user && !physio_id && !req.query.all) {
      query = query.eq('user_id', req.user.id);
    }
    if (status) {
      query = query.eq('status', status);
    }

    if (sort) {
      const desc = sort.startsWith('-');
      const col = desc ? sort.slice(1) : sort;
      const allowed = ['appointment_date', 'created_at', 'status'];
      if (allowed.includes(col)) {
        query = query.order(col, { ascending: !desc });
      }
    } else {
      query = query.order('appointment_date', { ascending: false });
    }

    const { data, error } = await query;
    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/appointments
router.post('/', optionalAuth, async (req, res) => {
  try {
    const data = req.body;
    const id = uuid();
    const userId = req.user?.id || null;

    const { data: row, error } = await supabase
      .from('physioconnect_appointments')
      .insert({
        id,
        user_id: userId,
        physio_id: data.physio_id,
        physio_name: data.physio_name,
        appointment_date: data.appointment_date,
        time_slot: data.time_slot,
        visit_type: data.visit_type || 'clinic',
        consultation_fee: data.consultation_fee || 0,
        patient_name: data.patient_name,
        patient_email: data.patient_email,
        patient_phone: data.patient_phone || null,
        patient_address: data.patient_address || null,
        notes: data.notes || null,
        status: data.status || 'confirmed',
      })
      .select()
      .single();

    if (error) throw error;

    res.json(row);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// PATCH /api/appointments/:id
router.patch('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const data = req.body;

    const { data: existing } = await supabase
      .from('physioconnect_appointments')
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
      .from('physioconnect_appointments')
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

export default router;
