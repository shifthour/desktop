const express = require('express');
const supabase = require('../config/supabase');
const router = express.Router();

// Get all queries with filtering and pagination
router.get('/', async (req, res) => {
  try {
    const { 
      study_id, 
      site_id, 
      subject_id,
      status, 
      severity,
      priority,
      raised_by,
      assigned_to,
      page = 1, 
      limit = 25,
      search 
    } = req.query;

    let query = supabase
      .from('edc_queries')
      .select(`
        *,
        edc_studies(protocol_number, title),
        edc_sites(name, site_number),
        edc_subjects(subject_number, initials),
        raised_by_user:edc_users!raised_by(first_name, last_name),
        assigned_to_user:edc_users!assigned_to(first_name, last_name)
      `);

    // Apply filters
    if (study_id) query = query.eq('study_id', study_id);
    if (site_id) query = query.eq('site_id', site_id);
    if (subject_id) query = query.eq('subject_id', subject_id);
    if (status) query = query.eq('status', status);
    if (severity) query = query.eq('severity', severity);
    if (priority) query = query.eq('priority', priority);
    if (raised_by) query = query.eq('raised_by', raised_by);
    if (assigned_to) query = query.eq('assigned_to', assigned_to);
    if (search) {
      query = query.or(`query_text.ilike.%${search}%, field_name.ilike.%${search}%`);
    }

    // Apply pagination
    const offset = (page - 1) * limit;
    query = query.range(offset, offset + limit - 1);

    const { data: queries, error, count } = await query
      .order('raised_date', { ascending: false });

    if (error) throw error;

    // Get total count for pagination
    const { count: totalCount } = await supabase
      .from('edc_queries')
      .select('*', { count: 'exact', head: true });

    res.json({
      status: 'success',
      data: queries || [],
      pagination: {
        page: parseInt(page),
        limit: parseInt(limit),
        total: totalCount,
        pages: Math.ceil(totalCount / limit)
      }
    });
  } catch (error) {
    console.error('Queries fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch queries',
      error: error.message
    });
  }
});

// Get query by ID
router.get('/:id', async (req, res) => {
  try {
    const { id } = req.params;

    const { data: query, error } = await supabase
      .from('edc_queries_with_age')
      .select(`
        *,
        edc_studies!inner(protocol_number, title),
        edc_sites!inner(name, site_number, principal_investigator),
        edc_subjects!inner(subject_number, initials),
        edc_visits(visit_name, visit_number),
        edc_form_templates(name, version),
        raised_by_user:edc_users!raised_by(first_name, last_name, email),
        assigned_to_user:edc_users!assigned_to(first_name, last_name, email),
        responded_by_user:edc_users!responded_by(first_name, last_name, email),
        closed_by_user:edc_users!closed_by(first_name, last_name, email)
      `)
      .eq('id', id)
      .single();

    if (error) throw error;
    if (!query) {
      return res.status(404).json({
        status: 'error',
        message: 'Query not found'
      });
    }

    res.json({
      status: 'success',
      data: query
    });
  } catch (error) {
    console.error('Query fetch error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch query',
      error: error.message
    });
  }
});

// Create new query
router.post('/', async (req, res) => {
  try {
    const {
      study_id,
      site_id,
      subject_id,
      visit_id,
      form_id,
      form_data_id,
      field_name,
      query_type = 'MANUAL',
      severity = 'MEDIUM',
      priority = 'MEDIUM',
      query_text,
      original_value,
      suggested_value,
      raised_by,
      assigned_to,
      due_date
    } = req.body;

    if (!study_id || !query_text || !raised_by) {
      return res.status(400).json({
        status: 'error',
        message: 'Study ID, query text, and raised by user are required'
      });
    }

    const { data: query, error } = await supabase
      .from('edc_queries')
      .insert([{
        study_id,
        site_id,
        subject_id,
        visit_id,
        form_id,
        form_data_id,
        field_name,
        query_type,
        severity,
        priority,
        query_text,
        original_value,
        suggested_value,
        raised_by,
        assigned_to,
        due_date,
        status: 'OPEN'
      }])
      .select(`
        *,
        edc_studies(protocol_number, title),
        edc_sites(name, site_number),
        edc_subjects(subject_number, initials)
      `)
      .single();

    if (error) throw error;

    res.status(201).json({
      status: 'success',
      message: 'Query created successfully',
      data: query
    });
  } catch (error) {
    console.error('Query creation error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to create query',
      error: error.message
    });
  }
});

// Update query
router.put('/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;

    // Remove fields that shouldn't be updated directly
    delete updates.id;
    delete updates.raised_date;
    delete updates.raised_by;
    delete updates.age_in_days; // This is a computed field

    updates.updated_at = new Date().toISOString();

    const { data: query, error } = await supabase
      .from('edc_queries')
      .update(updates)
      .eq('id', id)
      .select(`
        *,
        edc_studies!inner(protocol_number, title),
        edc_sites!inner(name, site_number),
        edc_subjects!inner(subject_number, initials)
      `)
      .single();

    if (error) throw error;
    if (!query) {
      return res.status(404).json({
        status: 'error',
        message: 'Query not found'
      });
    }

    res.json({
      status: 'success',
      message: 'Query updated successfully',
      data: query
    });
  } catch (error) {
    console.error('Query update error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to update query',
      error: error.message
    });
  }
});

// Respond to query
router.post('/:id/respond', async (req, res) => {
  try {
    const { id } = req.params;
    const { response_text, responded_by, resolution_notes } = req.body;

    if (!response_text || !responded_by) {
      return res.status(400).json({
        status: 'error',
        message: 'Response text and responded by user are required'
      });
    }

    const { data: query, error } = await supabase
      .from('edc_queries')
      .update({
        response_text,
        responded_by,
        response_date: new Date().toISOString(),
        resolution_notes,
        status: 'ANSWERED',
        updated_at: new Date().toISOString()
      })
      .eq('id', id)
      .select(`
        *,
        edc_studies!inner(protocol_number, title),
        edc_subjects!inner(subject_number, initials)
      `)
      .single();

    if (error) throw error;
    if (!query) {
      return res.status(404).json({
        status: 'error',
        message: 'Query not found'
      });
    }

    res.json({
      status: 'success',
      message: 'Query response submitted successfully',
      data: query
    });
  } catch (error) {
    console.error('Query response error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to respond to query',
      error: error.message
    });
  }
});

// Close query
router.post('/:id/close', async (req, res) => {
  try {
    const { id } = req.params;
    const { closed_by, resolution_notes } = req.body;

    if (!closed_by) {
      return res.status(400).json({
        status: 'error',
        message: 'Closed by user is required'
      });
    }

    const { data: query, error } = await supabase
      .from('edc_queries')
      .update({
        closed_by,
        closed_date: new Date().toISOString(),
        resolution_notes,
        status: 'CLOSED',
        updated_at: new Date().toISOString()
      })
      .eq('id', id)
      .select(`
        *,
        edc_studies!inner(protocol_number, title),
        edc_subjects!inner(subject_number, initials)
      `)
      .single();

    if (error) throw error;
    if (!query) {
      return res.status(404).json({
        status: 'error',
        message: 'Query not found'
      });
    }

    res.json({
      status: 'success',
      message: 'Query closed successfully',
      data: query
    });
  } catch (error) {
    console.error('Query closure error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to close query',
      error: error.message
    });
  }
});

// Assign query
router.post('/:id/assign', async (req, res) => {
  try {
    const { id } = req.params;
    const { assigned_to, assigned_by } = req.body;

    if (!assigned_to) {
      return res.status(400).json({
        status: 'error',
        message: 'Assigned to user is required'
      });
    }

    const { data: query, error } = await supabase
      .from('edc_queries')
      .update({
        assigned_to,
        assigned_date: new Date().toISOString(),
        updated_at: new Date().toISOString()
      })
      .eq('id', id)
      .select(`
        *,
        assigned_to_user:edc_users!assigned_to(first_name, last_name)
      `)
      .single();

    if (error) throw error;
    if (!query) {
      return res.status(404).json({
        status: 'error',
        message: 'Query not found'
      });
    }

    res.json({
      status: 'success',
      message: 'Query assigned successfully',
      data: query
    });
  } catch (error) {
    console.error('Query assignment error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to assign query',
      error: error.message
    });
  }
});

// Get query statistics
router.get('/stats/overview', async (req, res) => {
  try {
    const { study_id, site_id } = req.query;

    let baseQuery = supabase.from('edc_queries');
    
    if (study_id) baseQuery = baseQuery.eq('study_id', study_id);
    if (site_id) baseQuery = baseQuery.eq('site_id', site_id);

    // Get counts by status
    const { data: statusStats } = await baseQuery
      .select('status')
      .then(({ data }) => ({
        data: data?.reduce((acc, query) => {
          acc[query.status] = (acc[query.status] || 0) + 1;
          return acc;
        }, {}) || {}
      }));

    // Get counts by severity
    const { data: severityStats } = await baseQuery
      .select('severity')
      .then(({ data }) => ({
        data: data?.reduce((acc, query) => {
          acc[query.severity] = (acc[query.severity] || 0) + 1;
          return acc;
        }, {}) || {}
      }));

    // Get aging statistics
    const { data: agingStats } = await baseQuery
      .select('age_in_days, status')
      .eq('status', 'OPEN')
      .then(({ data }) => {
        const aging = { overdue: 0, due_soon: 0, on_track: 0 };
        data?.forEach(query => {
          if (query.age_in_days > 30) aging.overdue++;
          else if (query.age_in_days > 14) aging.due_soon++;
          else aging.on_track++;
        });
        return { data: aging };
      });

    res.json({
      status: 'success',
      data: {
        status_counts: statusStats.data || {},
        severity_counts: severityStats.data || {},
        aging_stats: agingStats.data || {},
        total_queries: Object.values(statusStats.data || {}).reduce((sum, count) => sum + count, 0)
      }
    });
  } catch (error) {
    console.error('Query stats error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch query statistics',
      error: error.message
    });
  }
});

// Get queries by study
router.get('/study/:study_id', async (req, res) => {
  try {
    const { study_id } = req.params;
    const { status, severity, site_id } = req.query;

    let query = supabase
      .from('edc_queries_with_age')
      .select(`
        *,
        edc_sites!inner(name, site_number),
        edc_subjects!inner(subject_number, initials)
      `)
      .eq('study_id', study_id);

    if (status) query = query.eq('status', status);
    if (severity) query = query.eq('severity', severity);
    if (site_id) query = query.eq('site_id', site_id);

    const { data: queries, error } = await query
      .order('raised_date', { ascending: false });

    if (error) throw error;

    res.json({
      status: 'success',
      data: queries || []
    });
  } catch (error) {
    console.error('Study queries error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to fetch study queries',
      error: error.message
    });
  }
});

module.exports = router;
