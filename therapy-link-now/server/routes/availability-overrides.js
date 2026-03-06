import { Router } from 'express';
import supabase from '../supabase.js';

const router = Router();

// GET /api/availability-overrides?physio_id=xxx&from_date=2025-01-01&to_date=2025-01-07
router.get('/', async (req, res) => {
  try {
    const { physio_id, from_date, to_date } = req.query;

    let query = supabase.from('physioconnect_availability_overrides').select('*');

    if (physio_id) {
      query = query.eq('physio_id', physio_id);
    }
    if (from_date) {
      query = query.gte('date', from_date);
    }
    if (to_date) {
      query = query.lte('date', to_date);
    }

    query = query.order('date', { ascending: true });

    const { data, error } = await query;
    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/availability-overrides — upsert (insert or update by physio_id + date)
router.post('/', async (req, res) => {
  try {
    const { physio_id, date, is_available, start_time, end_time } = req.body;

    if (!physio_id || !date) {
      return res.status(400).json({ error: 'physio_id and date are required' });
    }

    const { data, error } = await supabase
      .from('physioconnect_availability_overrides')
      .upsert(
        {
          physio_id,
          date,
          is_available: is_available !== undefined ? Boolean(is_available) : true,
          start_time: start_time || null,
          end_time: end_time || null,
        },
        { onConflict: 'physio_id,date' }
      )
      .select()
      .single();

    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// DELETE /api/availability-overrides/:id — remove override (revert to defaults)
router.delete('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: existing } = await supabase
      .from('physioconnect_availability_overrides')
      .select('*')
      .eq('id', id)
      .maybeSingle();

    if (!existing) return res.status(404).json({ error: 'Not found' });

    const { error } = await supabase
      .from('physioconnect_availability_overrides')
      .delete()
      .eq('id', id);

    if (error) throw error;

    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
