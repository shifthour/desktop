import { Router } from 'express';
import supabase from '../supabase.js';

const router = Router();

// GET /api/reviews?physio_id=xxx&sort=-created_date
router.get('/', async (req, res) => {
  try {
    const { physio_id, sort } = req.query;

    let query = supabase.from('physioconnect_reviews').select('*');

    if (physio_id) {
      query = query.eq('physio_id', physio_id);
    }

    if (sort) {
      const desc = sort.startsWith('-');
      const col = desc ? sort.slice(1) : sort;
      const allowed = ['created_date', 'rating'];
      if (allowed.includes(col)) {
        query = query.order(col, { ascending: !desc });
      }
    } else {
      query = query.order('created_date', { ascending: false });
    }

    const { data, error } = await query;
    if (error) throw error;

    res.json(data);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
