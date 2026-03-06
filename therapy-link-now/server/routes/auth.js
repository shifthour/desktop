import { Router } from 'express';
import bcrypt from 'bcryptjs';
import { v4 as uuid } from 'uuid';
import supabase from '../supabase.js';
import { signToken, requireAuth } from '../middleware/auth.js';

const router = Router();

// POST /api/auth/register
router.post('/register', async (req, res) => {
  try {
    const { email, password, full_name, phone } = req.body;
    if (!email || !password || !full_name) {
      return res.status(400).json({ error: 'email, password, and full_name are required' });
    }

    const { data: existing } = await supabase
      .from('physioconnect_users')
      .select('id')
      .eq('email', email)
      .maybeSingle();

    if (existing) {
      return res.status(409).json({ error: 'Email already registered' });
    }

    const id = uuid();
    const hashed = bcrypt.hashSync(password, 10);

    const { error } = await supabase
      .from('physioconnect_users')
      .insert({ id, email, password: hashed, full_name, phone: phone || null, role: 'patient' });

    if (error) throw error;

    const token = signToken({ id, email, full_name, role: 'patient' });
    res.json({ token, user: { id, email, full_name, phone, role: 'patient' } });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// POST /api/auth/login
router.post('/login', async (req, res) => {
  try {
    const { email, password } = req.body;
    if (!email || !password) {
      return res.status(400).json({ error: 'email and password are required' });
    }

    const { data: user, error } = await supabase
      .from('physioconnect_users')
      .select('*')
      .eq('email', email)
      .maybeSingle();

    if (error) throw error;
    if (!user || !bcrypt.compareSync(password, user.password)) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    const token = signToken({ id: user.id, email: user.email, full_name: user.full_name, role: user.role });
    res.json({
      token,
      user: { id: user.id, email: user.email, full_name: user.full_name, phone: user.phone, role: user.role },
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/auth/me
router.get('/me', requireAuth, async (req, res) => {
  try {
    const { data: user, error } = await supabase
      .from('physioconnect_users')
      .select('id, email, full_name, phone, role, created_at')
      .eq('id', req.user.id)
      .maybeSingle();

    if (error) throw error;
    if (!user) return res.status(404).json({ error: 'User not found' });
    res.json(user);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
