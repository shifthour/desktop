import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import supabase from '../config/database.js';

export const register = async (req, res) => {
  try {
    const { email, password, name } = req.body;

    // Check if user already exists
    const { data: existingUser } = await supabase
      .from('serene_users')
      .select('*')
      .eq('email', email)
      .single();

    if (existingUser) {
      return res.status(400).json({ error: 'User already exists' });
    }

    // Hash password
    const hashedPassword = bcrypt.hashSync(password, 10);

    // Insert user
    const { data, error } = await supabase
      .from('serene_users')
      .insert([{ email, password: hashedPassword, name }])
      .select()
      .single();

    if (error) throw error;

    res.status(201).json({
      message: 'User registered successfully',
      userId: data.id
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const login = async (req, res) => {
  try {
    const { email, password } = req.body;
    console.log('🔐 Login attempt for:', email);

    // Find user
    const { data: user, error } = await supabase
      .from('serene_users')
      .select('*')
      .eq('email', email)
      .single();

    if (error || !user) {
      console.log('❌ User not found:', email, error?.message);
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    console.log('✅ User found:', user.email);

    // Verify password
    const isValid = bcrypt.compareSync(password, user.password);
    if (!isValid) {
      console.log('❌ Invalid password for:', email);
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    console.log('✅ Password verified for:', email);

    // Generate token
    const token = jwt.sign(
      { id: user.id, email: user.email, name: user.name },
      process.env.JWT_SECRET,
      { expiresIn: '24h' }
    );

    console.log('✅ Login successful for:', email);

    res.json({
      token,
      user: {
        id: user.id,
        email: user.email,
        name: user.name,
        role: user.role
      }
    });
  } catch (error) {
    console.log('❌ Login error:', error.message);
    res.status(500).json({ error: error.message });
  }
};

export const getCurrentUser = async (req, res) => {
  try {
    const { data: user, error } = await supabase
      .from('serene_users')
      .select('id, email, name, role')
      .eq('id', req.user.id)
      .single();

    if (error) throw error;

    res.json(user);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const changePassword = async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;
    const userId = req.user.id;

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ error: 'Current password and new password are required' });
    }

    if (newPassword.length < 6) {
      return res.status(400).json({ error: 'New password must be at least 6 characters long' });
    }

    // Get current user
    const { data: user, error } = await supabase
      .from('serene_users')
      .select('*')
      .eq('id', userId)
      .single();

    if (error || !user) {
      return res.status(404).json({ error: 'User not found' });
    }

    // Verify current password
    const isValid = bcrypt.compareSync(currentPassword, user.password);
    if (!isValid) {
      return res.status(401).json({ error: 'Current password is incorrect' });
    }

    // Hash new password
    const hashedPassword = bcrypt.hashSync(newPassword, 10);

    // Update password
    const { error: updateError } = await supabase
      .from('serene_users')
      .update({ password: hashedPassword })
      .eq('id', userId);

    if (updateError) throw updateError;

    res.json({ message: 'Password changed successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
