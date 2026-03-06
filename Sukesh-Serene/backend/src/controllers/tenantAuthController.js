import supabase from '../config/database.js';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';

const JWT_SECRET = process.env.JWT_SECRET || 'serene-living-secret-key';

export const tenantLogin = async (req, res) => {
  try {
    const { email, password } = req.body;

    if (!email || !password) {
      return res.status(400).json({ error: 'Email and password are required' });
    }

    // Get tenant by email with room info
    const { data: tenantData, error } = await supabase
      .from('serene_tenants')
      .select(`
        *,
        serene_rooms!left (
          room_number
        )
      `)
      .eq('email', email)
      .eq('status', 'active')
      .single();

    if (error || !tenantData) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    // Flatten the room data
    const tenant = {
      ...tenantData,
      room_number: tenantData.serene_rooms?.room_number || null
    };
    delete tenant.serene_rooms;

    if (!tenant.password) {
      return res.status(401).json({ error: 'Account not set up. Please contact admin.' });
    }

    // Verify password
    const isValidPassword = bcrypt.compareSync(password, tenant.password);
    if (!isValidPassword) {
      return res.status(401).json({ error: 'Invalid email or password' });
    }

    // Generate JWT token
    const token = jwt.sign(
      { id: tenant.id, email: tenant.email, role: 'tenant' },
      JWT_SECRET,
      { expiresIn: '24h' }
    );

    // Remove password from response
    const { password: _, ...tenantData2 } = tenant;

    res.json({
      message: 'Login successful',
      token,
      tenant: tenantData2,
      firstLogin: tenant.first_login === 1
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const resetPassword = async (req, res) => {
  try {
    const { email } = req.body;

    if (!email) {
      return res.status(400).json({ error: 'Email is required' });
    }

    // Get tenant by email
    const { data: tenant, error } = await supabase
      .from('serene_tenants')
      .select('id, email, name')
      .eq('email', email)
      .eq('status', 'active')
      .single();

    if (error || !tenant) {
      return res.status(404).json({ error: 'No active account found with this email' });
    }

    // Reset password to default "user@123"
    const defaultPassword = 'user@123';
    const hashedPassword = bcrypt.hashSync(defaultPassword, 10);

    // Update password and set first_login to 1 so they must change it
    const { error: updateError } = await supabase
      .from('serene_tenants')
      .update({ password: hashedPassword, first_login: 1 })
      .eq('id', tenant.id);

    if (updateError) throw updateError;

    res.json({
      message: 'Password has been reset successfully. Your new password is: user@123',
      success: true
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const changePassword = async (req, res) => {
  try {
    const { tenantId } = req.params;
    const { currentPassword, newPassword } = req.body;

    if (!newPassword || newPassword.length < 6) {
      return res.status(400).json({ error: 'New password must be at least 6 characters' });
    }

    // Get tenant
    const { data: tenant, error } = await supabase
      .from('serene_tenants')
      .select('*')
      .eq('id', tenantId)
      .single();

    if (error || !tenant) {
      return res.status(404).json({ error: 'Tenant not found' });
    }

    // Verify current password if not first login
    if (tenant.first_login === 0 && currentPassword) {
      const isValidPassword = bcrypt.compareSync(currentPassword, tenant.password);
      if (!isValidPassword) {
        return res.status(401).json({ error: 'Current password is incorrect' });
      }
    }

    // Hash new password
    const hashedPassword = bcrypt.hashSync(newPassword, 10);

    // Update password and set first_login to 0
    const { error: updateError } = await supabase
      .from('serene_tenants')
      .update({ password: hashedPassword, first_login: 0 })
      .eq('id', tenantId);

    if (updateError) throw updateError;

    res.json({ message: 'Password changed successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
