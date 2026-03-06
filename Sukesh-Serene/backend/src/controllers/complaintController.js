import supabase from '../config/database.js';

// Create a new complaint (tenant)
export const createComplaint = async (req, res) => {
  try {
    const { tenant_id, complaint_description } = req.body;

    console.log('Creating complaint:', { tenant_id, complaint_description });

    if (!tenant_id) {
      return res.status(400).json({ error: 'Tenant ID is required' });
    }

    if (!complaint_description || complaint_description.trim() === '') {
      return res.status(400).json({ error: 'Complaint description is required' });
    }

    const { data, error } = await supabase
      .from('serene_complaints')
      .insert([{
        tenant_id,
        complaint_description: complaint_description.trim(),
        status: 'raised'
      }])
      .select()
      .single();

    if (error) {
      console.error('Supabase error creating complaint:', error);
      throw error;
    }

    console.log('Complaint created successfully:', data);

    res.status(201).json({
      message: 'Complaint raised successfully',
      complaint: data
    });
  } catch (error) {
    console.error('Error in createComplaint:', error);
    res.status(500).json({
      error: error.message,
      details: error.details || 'No additional details',
      hint: error.hint || 'No hint available'
    });
  }
};

// Get all complaints (admin/supervisor)
export const getAllComplaints = async (req, res) => {
  try {
    const { status } = req.query;

    let query = supabase
      .from('serene_complaints')
      .select(`
        *,
        serene_tenants!inner(
          name,
          phone,
          room_id,
          serene_rooms!left(
            room_number,
            pg_id,
            serene_pg_properties!left(
              pg_name
            )
          )
        ),
        serene_users!serene_complaints_resolved_by_fkey(
          name
        )
      `)
      .order('created_at', { ascending: false });

    if (status) {
      query = query.eq('status', status);
    }

    const { data: complaints, error } = await query;

    if (error) throw error;

    // Flatten nested data structures
    const flattenedComplaints = complaints.map(complaint => ({
      ...complaint,
      tenant_name: complaint.serene_tenants?.name,
      tenant_phone: complaint.serene_tenants?.phone,
      room_id: complaint.serene_tenants?.room_id,
      room_number: complaint.serene_tenants?.serene_rooms?.room_number,
      pg_id: complaint.serene_tenants?.serene_rooms?.pg_id,
      pg_name: complaint.serene_tenants?.serene_rooms?.serene_pg_properties?.pg_name,
      resolved_by_name: complaint.serene_users?.name,
      serene_tenants: undefined,
      serene_users: undefined
    }));

    res.json(flattenedComplaints);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Get complaints for a specific tenant
export const getTenantComplaints = async (req, res) => {
  try {
    const { tenant_id } = req.params;

    const { data: complaints, error } = await supabase
      .from('serene_complaints')
      .select('*')
      .eq('tenant_id', tenant_id)
      .order('created_at', { ascending: false });

    if (error) throw error;

    res.json(complaints);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Update complaint status (admin/supervisor)
export const updateComplaintStatus = async (req, res) => {
  try {
    const { id } = req.params;
    const { status } = req.body;

    if (!['raised', 'in_progress', 'resolved'].includes(status)) {
      return res.status(400).json({ error: 'Invalid status' });
    }

    const updateData = { status };

    const { error } = await supabase
      .from('serene_complaints')
      .update(updateData)
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Complaint status updated successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Resolve complaint (admin/supervisor)
export const resolveComplaint = async (req, res) => {
  try {
    const { id } = req.params;
    const { resolution_description, resolved_by } = req.body;

    if (!resolution_description || resolution_description.trim() === '') {
      return res.status(400).json({ error: 'Resolution description is required' });
    }

    const { error } = await supabase
      .from('serene_complaints')
      .update({
        status: 'resolved',
        resolution_description: resolution_description.trim(),
        resolved_by,
        resolved_at: new Date().toISOString()
      })
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Complaint resolved successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

// Delete complaint (admin only)
export const deleteComplaint = async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('serene_complaints')
      .delete()
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Complaint deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
