import supabase from '../config/database.js';

export const getAllRooms = async (req, res) => {
  try {
    // Fetch all rooms with their PG properties and tenants
    const { data: rooms, error } = await supabase
      .from('serene_rooms')
      .select(`
        *,
        serene_pg_properties!left (
          pg_name
        ),
        serene_tenants!left (
          id,
          status
        )
      `)
      .order('room_number');

    if (error) throw error;

    // Flatten the nested structure and calculate occupied_count
    const flattenedRooms = rooms.map(room => {
      const pg_name = room.serene_pg_properties?.pg_name || null;
      const tenants = room.serene_tenants || [];
      const occupied_count = tenants.filter(t => t.status === 'active').length;

      // Remove the nested objects and return flat structure
      const { serene_pg_properties, serene_tenants, ...roomData } = room;

      return {
        ...roomData,
        pg_name,
        occupied_count
      };
    });

    res.json(flattenedRooms);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getRoomById = async (req, res) => {
  try {
    const { id } = req.params;

    // Fetch room with PG property
    const { data: room, error: roomError } = await supabase
      .from('serene_rooms')
      .select(`
        *,
        serene_pg_properties!left (
          pg_name
        )
      `)
      .eq('id', id)
      .single();

    if (roomError) throw roomError;

    if (!room) {
      return res.status(404).json({ error: 'Room not found' });
    }

    // Fetch active tenants for this room
    const { data: tenants, error: tenantsError } = await supabase
      .from('serene_tenants')
      .select('*')
      .eq('room_id', id)
      .eq('status', 'active');

    if (tenantsError) throw tenantsError;

    // Flatten the nested pg_name
    const pg_name = room.serene_pg_properties?.pg_name || null;
    const { serene_pg_properties, ...roomData } = room;

    res.json({
      ...roomData,
      pg_name,
      tenants: tenants || []
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const createRoom = async (req, res) => {
  try {
    const { pg_id, room_number, floor, rent_amount, total_beds } = req.body;

    // Convert empty string to null for pg_id
    const pgIdValue = pg_id && pg_id !== '' ? parseInt(pg_id) : null;

    const { data, error } = await supabase
      .from('serene_rooms')
      .insert([{
        pg_id: pgIdValue,
        room_number,
        floor,
        room_type: null,
        rent_amount,
        total_beds: total_beds || 1,
        status: 'available'
      }])
      .select()
      .single();

    if (error) throw error;

    res.status(201).json({
      message: 'Room created successfully',
      roomId: data.id
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const updateRoom = async (req, res) => {
  try {
    const { id } = req.params;
    const { pg_id, room_number, floor, rent_amount, total_beds } = req.body;

    // Convert empty string to null for pg_id
    const pgIdValue = pg_id && pg_id !== '' ? parseInt(pg_id) : null;

    const { error } = await supabase
      .from('serene_rooms')
      .update({
        pg_id: pgIdValue,
        room_number,
        floor,
        room_type: null,
        rent_amount,
        total_beds,
        status: 'available'
      })
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Room updated successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const deleteRoom = async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('serene_rooms')
      .delete()
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'Room deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
