import { Router } from 'express';
import supabase from '../supabase.js';

const router = Router();

// GET /api/blocked-slots?physio_id=xxx
router.get('/', async (req, res) => {
  try {
    const { physio_id } = req.query;

    let query = supabase.from('physioconnect_blocked_slots').select('*');

    if (physio_id) {
      query = query.eq('physio_id', physio_id);
    }

    const { data, error } = await query;
    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
