import supabase from '../config/database.js';

export const getAllPGs = async (req, res) => {
  try {
    const { data: pgs, error } = await supabase
      .from('serene_pg_properties')
      .select('*')
      .order('created_at', { ascending: false });

    if (error) throw error;

    res.json(pgs);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const getPGById = async (req, res) => {
  try {
    const { id } = req.params;
    const { data: pg, error } = await supabase
      .from('serene_pg_properties')
      .select('*')
      .eq('id', id)
      .single();

    if (error || !pg) {
      return res.status(404).json({ error: 'PG not found' });
    }

    res.json(pg);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const createPG = async (req, res) => {
  try {
    const { pg_name, address, total_floors, image_url } = req.body;

    const { data, error } = await supabase
      .from('serene_pg_properties')
      .insert([{
        pg_name,
        address,
        total_floors: total_floors || null,
        image_url: image_url || null
      }])
      .select()
      .single();

    if (error) throw error;

    res.status(201).json({
      message: 'PG created successfully',
      pgId: data.id
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const updatePG = async (req, res) => {
  try {
    const { id } = req.params;
    const { pg_name, address, total_floors, image_url } = req.body;

    const { error } = await supabase
      .from('serene_pg_properties')
      .update({
        pg_name,
        address,
        total_floors,
        image_url
      })
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'PG updated successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

export const deletePG = async (req, res) => {
  try {
    const { id } = req.params;

    const { error } = await supabase
      .from('serene_pg_properties')
      .delete()
      .eq('id', id);

    if (error) throw error;

    res.json({ message: 'PG deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
