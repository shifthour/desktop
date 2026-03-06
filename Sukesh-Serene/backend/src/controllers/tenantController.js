import supabase from '../config/database.js';
import bcrypt from 'bcryptjs';

export const getAllTenants = async (req, res) => {
  try {
    const { status } = req.query;

    // Query with JOIN using Supabase nested select
    const { data: tenants, error } = await supabase
      .from('serene_tenants')
      .select(`
        *,
        serene_rooms!left(
          room_number,
          floor,
          pg_id,
          serene_pg_properties!left(
            pg_name
          )
        )
      `)
      .eq('status', status || 'active')
      .order('created_at', { ascending: false });

    if (error) throw error;

    // Flatten nested structure to match original flat format
    const flattenedTenants = tenants.map(tenant => {
      const room = tenant.serene_rooms;
      const pg = room?.serene_pg_properties;

      return {
        ...tenant,
        room_number: room?.room_number || null,
        floor: room?.floor || null,
        pg_id: room?.pg_id || null,
        pg_name: pg?.pg_name || null,
        serene_rooms: undefined,
      };
    });

    res.json(flattenedTenants);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getTenantById = async (req, res) => {
  try {
    const { id } = req.params;

    // Query with JOIN using Supabase nested select
    const { data: tenant, error } = await supabase
      .from('serene_tenants')
      .select(`
        *,
        serene_rooms!left(
          room_number,
          floor,
          pg_id,
          serene_pg_properties!left(
            pg_name
          )
        )
      `)
      .eq('id', id)
      .single();

    if (error) throw error;

    if (!tenant) {
      return res.status(404).json({ error: 'Tenant not found' });
    }

    // Flatten nested structure to match original flat format
    const room = tenant.serene_rooms;
    const pg = room?.serene_pg_properties;

    const flattenedTenant = {
      ...tenant,
      room_number: room?.room_number || null,
      floor: room?.floor || null,
      pg_id: room?.pg_id || null,
      pg_name: pg?.pg_name || null,
      serene_rooms: undefined,
    };

    res.json(flattenedTenant);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const createTenant = async (req, res) => {
  try {
    const {
      room_id,
      name,
      email,
      phone,
      aadhar_number,
      aadhar_card_image,
      occupation,
      emergency_contact,
      address,
      check_in_date,
      monthly_rent,
      security_deposit,
      password,
      payment_qr_code,
      upi_number,
      remarks,
      security_refunded,
      security_refunded_amount
    } = req.body;

    // Check if email already exists (only check active tenants)
    if (email) {
      const { data: existingTenant, error } = await supabase
        .from('serene_tenants')
        .select('id')
        .eq('email', email)
        .eq('status', 'active')
        .single();

      if (error && error.code !== 'PGRST116') throw error; // PGRST116 = no rows returned

      if (existingTenant) {
        return res.status(400).json({ error: 'Email already exists in the system' });
      }
    }

    // Check if phone already exists (only check active tenants)
    if (phone) {
      const { data: existingPhone, error } = await supabase
        .from('serene_tenants')
        .select('id')
        .eq('phone', phone)
        .eq('status', 'active')
        .single();

      if (error && error.code !== 'PGRST116') throw error;

      if (existingPhone) {
        return res.status(400).json({ error: 'Phone number already exists in the system' });
      }
    }

    // Update room status if room assigned
    if (room_id) {
      // Get room total_beds
      const { data: room, error: roomError } = await supabase
        .from('serene_rooms')
        .select('total_beds')
        .eq('id', room_id)
        .single();

      if (roomError) throw roomError;

      // Count occupied tenants in this room
      const { data: occupiedTenants, error: countError } = await supabase
        .from('serene_tenants')
        .select('id')
        .eq('room_id', room_id)
        .eq('status', 'active');

      if (countError) throw countError;

      const occupied = occupiedTenants?.length || 0;

      if (occupied >= room.total_beds) {
        return res.status(400).json({ error: 'Room is fully occupied' });
      }

      // Update room to occupied if this is the last available bed
      if (occupied + 1 === room.total_beds) {
        const { error: updateError } = await supabase
          .from('serene_rooms')
          .update({ status: 'occupied' })
          .eq('id', room_id);

        if (updateError) throw updateError;
      }
    }

    // Hash password if provided
    const hashedPassword = password ? bcrypt.hashSync(password, 10) : null;

    // Insert new tenant
    const { data: newTenant, error: insertError } = await supabase
      .from('serene_tenants')
      .insert([{
        room_id: room_id || null,
        name,
        email,
        phone,
        aadhar_number,
        aadhar_card_image,
        occupation,
        emergency_contact,
        address,
        check_in_date,
        monthly_rent,
        security_deposit,
        password: hashedPassword,
        payment_qr_code,
        upi_number,
        remarks,
        security_refunded: security_refunded || 'no',
        security_refunded_amount: security_refunded_amount || 0
      }])
      .select()
      .single();

    if (insertError) throw insertError;

    res.status(201).json({
      message: 'Tenant added successfully',
      tenantId: newTenant.id
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const updateTenant = async (req, res) => {
  try {
    const { id } = req.params;
    const {
      room_id,
      name,
      email,
      phone,
      aadhar_number,
      aadhar_card_image,
      occupation,
      emergency_contact,
      address,
      check_in_date,
      monthly_rent,
      security_deposit,
      status,
      password,
      payment_qr_code,
      upi_number,
      remarks,
      security_refunded,
      security_refunded_amount
    } = req.body;

    // Get current tenant
    const { data: currentTenant, error: currentError } = await supabase
      .from('serene_tenants')
      .select('room_id, password')
      .eq('id', id)
      .single();

    if (currentError) throw currentError;

    // Handle room changes
    if (currentTenant.room_id !== room_id) {
      // Free up old room
      if (currentTenant.room_id) {
        // Count remaining active tenants in old room (excluding current tenant)
        const { data: oldOccupiedTenants, error: oldCountError } = await supabase
          .from('serene_tenants')
          .select('id')
          .eq('room_id', currentTenant.room_id)
          .eq('status', 'active')
          .neq('id', id);

        if (oldCountError) throw oldCountError;

        const oldOccupied = oldOccupiedTenants?.length || 0;

        // Get old room details
        const { data: oldRoom, error: oldRoomError } = await supabase
          .from('serene_rooms')
          .select('total_beds')
          .eq('id', currentTenant.room_id)
          .single();

        if (oldRoomError) throw oldRoomError;

        // If old room has available beds, mark it as available
        if (oldOccupied < oldRoom.total_beds) {
          const { error: updateOldError } = await supabase
            .from('serene_rooms')
            .update({ status: 'available' })
            .eq('id', currentTenant.room_id);

          if (updateOldError) throw updateOldError;
        }
      }

      // Update new room
      if (room_id) {
        // Count active tenants in new room
        const { data: newOccupiedTenants, error: newCountError } = await supabase
          .from('serene_tenants')
          .select('id')
          .eq('room_id', room_id)
          .eq('status', 'active');

        if (newCountError) throw newCountError;

        const newOccupied = newOccupiedTenants?.length || 0;

        // Get new room details
        const { data: newRoom, error: newRoomError } = await supabase
          .from('serene_rooms')
          .select('total_beds')
          .eq('id', room_id)
          .single();

        if (newRoomError) throw newRoomError;

        if (newOccupied >= newRoom.total_beds) {
          return res.status(400).json({ error: 'Room is fully occupied' });
        }

        // If new room will be fully occupied, mark it as occupied
        if (newOccupied + 1 === newRoom.total_beds) {
          const { error: updateNewError } = await supabase
            .from('serene_rooms')
            .update({ status: 'occupied' })
            .eq('id', room_id);

          if (updateNewError) throw updateNewError;
        }
      }
    }

    // Hash password if it's being updated and different from current
    const hashedPassword = password && password !== currentTenant?.password
      ? bcrypt.hashSync(password, 10)
      : currentTenant?.password;

    // Update tenant
    const { error: updateError } = await supabase
      .from('serene_tenants')
      .update({
        room_id: room_id || null,
        name,
        email,
        phone,
        aadhar_number,
        aadhar_card_image,
        occupation,
        emergency_contact,
        address,
        check_in_date,
        monthly_rent,
        security_deposit,
        status: status || 'active',
        password: hashedPassword,
        payment_qr_code,
        upi_number,
        remarks,
        security_refunded,
        security_refunded_amount
      })
      .eq('id', id);

    if (updateError) throw updateError;

    res.json({ message: 'Tenant updated successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const deleteTenant = async (req, res) => {
  try {
    const { id } = req.params;

    // Get tenant info
    const { data: tenant, error: tenantError } = await supabase
      .from('serene_tenants')
      .select('room_id')
      .eq('id', id)
      .single();

    if (tenantError) throw tenantError;

    // Update room status
    if (tenant && tenant.room_id) {
      // Count remaining active tenants in room (excluding current tenant)
      const { data: occupiedTenants, error: countError } = await supabase
        .from('serene_tenants')
        .select('id')
        .eq('room_id', tenant.room_id)
        .eq('status', 'active')
        .neq('id', id);

      if (countError) throw countError;

      const occupied = occupiedTenants?.length || 0;

      // Get room details
      const { data: room, error: roomError } = await supabase
        .from('serene_rooms')
        .select('total_beds')
        .eq('id', tenant.room_id)
        .single();

      if (roomError) throw roomError;

      // If room has available beds after deletion, mark it as available
      if (occupied < room.total_beds) {
        const { error: updateError } = await supabase
          .from('serene_rooms')
          .update({ status: 'available' })
          .eq('id', tenant.room_id);

        if (updateError) throw updateError;
      }
    }

    // Soft delete tenant
    const { error: deleteError } = await supabase
      .from('serene_tenants')
      .update({
        status: 'inactive',
        check_out_date: new Date().toISOString().split('T')[0]
      })
      .eq('id', id);

    if (deleteError) throw deleteError;

    res.json({ message: 'Tenant removed successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
