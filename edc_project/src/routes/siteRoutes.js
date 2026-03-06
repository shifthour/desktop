const express = require('express');
const supabase = require('../config/supabase');
const router = express.Router();

// Get all sites with filtering
router.get('/', async (req, res) => {
  try {
    const { study_id, status } = req.query;

    let query = supabase
      .from('edc_sites')
      .select(`
        *,
        edc_studies!left(protocol_number, title)
      `);

    // Apply filters
    if (study_id) query = query.eq('study_id', study_id);
    if (status) query = query.eq('status', status);

    const { data: sites, error } = await query
      .order('site_number');

    if (error) throw error;

    res.json({
      status: 'success',
      data: sites || []
    });
  } catch (error) {
    console.error('Sites fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch sites',
      error: error.message
    });
  }
});

// Create new site
router.post('/', async (req, res) => {
  try {
    const {
      study_id,
      name,
      site_number,
      principal_investigator,
      address,
      city,
      state,
      country,
      coordinator_name,
      coordinator_email,
      coordinator_phone
    } = req.body;

    if (!study_id || !name || !site_number) {
      return res.status(400).json({
        status: 'error',
        message: 'Study ID, name, and site number are required'
      });
    }

    // Check if site number already exists in the study
    const { data: existingSite } = await supabase
      .from('edc_sites')
      .select('id')
      .eq('study_id', study_id)
      .eq('site_number', site_number)
      .single();

    if (existingSite) {
      return res.status(409).json({
        status: 'error',
        message: 'Site number already exists in this study'
      });
    }

    const { data: site, error } = await supabase
      .from('edc_sites')
      .insert([{
        study_id,
        name,
        site_number,
        principal_investigator,
        address,
        city,
        state,
        country,
        coordinator_name,
        coordinator_email,
        coordinator_phone,
        status: 'ACTIVE'
      }])
      .select()
      .single();

    if (error) throw error;

    res.status(201).json({
      status: 'success',
      message: 'Site created successfully',
      data: site
    });
  } catch (error) {
    console.error('Site creation error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to create site',
      error: error.message
    });
  }
});

module.exports = router;
