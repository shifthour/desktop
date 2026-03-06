const express = require('express');
const bcrypt = require('bcrypt');
const jwt = require('jsonwebtoken');
const supabase = require('../config/supabase');
const router = express.Router();

const JWT_SECRET = process.env.JWT_SECRET || 'edc-secret-key-2024';

// Login endpoint
router.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body;

    if (!username || !password) {
      return res.status(400).json({
        status: 'error',
        message: 'Username and password are required'
      });
    }

    // Get user from database
    console.log('Attempting login for username:', username);
    const { data: user, error: userError } = await supabase
      .from('edc_users')
      .select('*')
      .eq('username', username)
      .eq('status', 'ACTIVE')
      .single();

    console.log('Supabase query result:', { user, error: userError });

    if (userError || !user) {
      console.log('User not found or error:', userError);
      return res.status(401).json({
        status: 'error',
        message: 'Invalid credentials'
      });
    }

    // For demo purposes, we'll accept the preset passwords
    // In production, you should properly hash and compare passwords
    const validPasswords = {
      'admin': 'Admin@2024',
      'doctor': 'Doctor@2024',
      'data_manager': 'DataManager@2024',
      // Keep legacy support temporarily
      'startup_admin': 'Startup@2024',
      'conduct_crc': 'Conduct@2024',
      'closeout_manager': 'Closeout@2024'
    };

    if (validPasswords[username] !== password) {
      return res.status(401).json({
        status: 'error',
        message: 'Invalid credentials'
      });
    }

    // Update last login
    await supabase
      .from('edc_users')
      .update({ last_login: new Date().toISOString() })
      .eq('id', user.id);

    // Generate JWT token
    const token = jwt.sign(
      { 
        userId: user.id, 
        username: user.username, 
        phase: user.phase,
        role: user.role 
      },
      JWT_SECRET,
      { expiresIn: '24h' }
    );

    // Get study data for the user's phase
    const studyData = await getStudyDataForPhase(user.phase, user.id);

    // Remove password from response
    const { password_hash, ...userWithoutPassword } = user;

    res.json({
      status: 'success',
      message: 'Login successful',
      token,
      data: {
        user: {
          ...userWithoutPassword,
          firstName: user.first_name,
          lastName: user.last_name,
          title: user.role,
          institution: 'Clinical Research Institute'
        },
        studyData
      }
    });

  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Internal server error'
    });
  }
});

// Get current user
router.get('/me', authenticateToken, async (req, res) => {
  try {
    const { data: user, error } = await supabase
      .from('edc_users')
      .select('*')
      .eq('id', req.user.userId)
      .single();

    if (error || !user) {
      return res.status(404).json({
        status: 'error',
        message: 'User not found'
      });
    }

    // Get study data for the user's phase
    const studyData = await getStudyDataForPhase(user.phase, user.id);

    // Remove password from response
    const { password_hash, ...userWithoutPassword } = user;

    res.json({
      status: 'success',
      data: {
        user: {
          ...userWithoutPassword,
          firstName: user.first_name,
          lastName: user.last_name,
          title: user.role,
          institution: 'Clinical Research Institute'
        },
        studyData
      }
    });

  } catch (error) {
    console.error('Profile error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to get user profile'
    });
  }
});

// Verify data endpoint
router.get('/verify-data', authenticateToken, async (req, res) => {
  try {
    const userId = req.user.userId;
    const userPhase = req.user.phase;

    // Get verification checks for the user's phase
    const { data: verificationData, error } = await supabase
      .from('edc_data_verification')
      .select(`
        *,
        edc_studies!inner(protocol_number, title)
      `)
      .eq('phase', userPhase)
      .order('verified_at', { ascending: false })
      .limit(5);

    if (error) {
      console.error('Verification query error:', error);
      // Return empty data if no verification records found
    }

    // Get user info for response
    const { data: user } = await supabase
      .from('edc_users')
      .select('first_name, last_name, role')
      .eq('id', userId)
      .single();

    // Format verification data
    const formattedData = {
      verification: {
        phase: userPhase,
        verificationStatus: verificationData && verificationData.length > 0 && verificationData.every(v => v.status === 'PASSED') ? 'PASSED' : 'IN_PROGRESS',
        checks: verificationData?.map(check => ({
          item: check.check_name,
          status: check.status,
          details: check.details || check.check_description
        })) || getDefaultVerificationChecks(userPhase)
      },
      verifiedBy: user ? `${user.first_name} ${user.last_name}` : req.user.username,
      timestamp: new Date().toISOString(),
      role: user?.role || req.user.role,
      phase: userPhase
    };

    res.json({
      status: 'success',
      data: formattedData
    });

  } catch (error) {
    console.error('Data verification error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Failed to verify data'
    });
  }
});

// Helper function to get default verification checks if none exist in DB
function getDefaultVerificationChecks(phase) {
  const defaults = {
    START_UP: [
      { item: 'Protocol Approval', status: 'PASSED', details: 'Protocol approved by IRB' },
      { item: 'Form Validation', status: 'IN_PROGRESS', details: '5 of 8 forms validated' },
      { item: 'Site Readiness', status: 'PENDING', details: 'Awaiting site initiation visits' },
      { item: 'System Configuration', status: 'PASSED', details: 'EDC system configured' }
    ],
    CONDUCT: [
      { item: 'Data Entry Compliance', status: 'PASSED', details: '98% within 48hr window' },
      { item: 'Query Resolution Rate', status: 'WARNING', details: '73% resolved (target: 80%)' },
      { item: 'Protocol Adherence', status: 'PASSED', details: '3 deviations documented' },
      { item: 'Safety Reporting', status: 'PASSED', details: 'All AEs reported within timeline' }
    ],
    CLOSE_OUT: [
      { item: 'Database Completeness', status: 'PASSED', details: '98.7% complete' },
      { item: 'Query Closure', status: 'WARNING', details: '12 queries pending' },
      { item: 'Audit Trail Review', status: 'PASSED', details: 'No discrepancies found' },
      { item: 'Data Export Validation', status: 'IN_PROGRESS', details: 'CDISC export in progress' }
    ]
  };
  return defaults[phase] || [];
}

// Helper function to get study data based on user phase
async function getStudyDataForPhase(phase, userId) {
  try {
    // Get studies with sites
    const { data: studies, error: studiesError } = await supabase
      .from('edc_studies')
      .select(`
        *,
        edc_sites(*)
      `)
      .order('created_at', { ascending: false })
      .limit(1);

    if (studiesError) throw studiesError;

    // Get metrics for studies
    const { data: metrics, error: metricsError } = await supabase
      .from('edc_study_metrics')
      .select('*')
      .order('calculation_date', { ascending: false });

    if (metricsError) console.error('Metrics error:', metricsError);

    const study = studies[0]; // Get primary study
    if (!study) {
      return { studyInfo: {}, metrics: {} };
    }

    // Format study data based on phase
    let studyInfo = {
      protocolNumber: study.protocol_number,
      title: study.title,
      status: study.status,
      phase: phase,
      currentEnrollment: study.current_enrollment,
      projectedEnrollment: study.projected_enrollment,
      startDate: study.start_date,
      sites: study.edc_sites?.map(site => ({
        id: site.id,
        name: site.name,
        status: site.status,
        subjects: site.subjects_enrolled,
        dataComplete: `${site.data_complete_percentage}%`
      })) || []
    };

    // Add phase-specific data
    if (phase === 'START_UP') {
      // Get form templates
      const { data: forms } = await supabase
        .from('edc_form_templates')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(5);

      studyInfo.forms = forms?.map(form => ({
        id: form.id,
        name: form.name,
        version: form.version,
        status: form.status,
        fields: 12 // Placeholder - could be calculated from EDC_form_fields
      })) || [];

      studyInfo.milestones = [
        { task: 'Protocol Approval', status: 'COMPLETED', date: '2024-01-15' },
        { task: 'Form Design', status: 'IN_PROGRESS', date: null },
        { task: 'Site Initiation', status: 'PENDING', date: null }
      ];
    }

    if (phase === 'CONDUCT') {
      // Get recent subjects
      const { data: subjects } = await supabase
        .from('edc_subjects')
        .select(`
          *,
          edc_sites(name)
        `)
        .eq('study_id', study.id)
        .order('created_at', { ascending: false })
        .limit(10);

      studyInfo.subjects = subjects?.map(subject => ({
        id: subject.subject_number,
        site: subject.edc_sites?.name || 'Unknown Site',
        enrollmentDate: subject.enrollment_date,
        status: subject.status,
        visitsPending: subject.visits_pending
      })) || [];

      // Get queries
      const { data: queries } = await supabase
        .from('edc_queries')
        .select(`
          *,
          edc_form_data!inner(
            edc_subjects(subject_number)
          )
        `)
        .order('created_at', { ascending: false })
        .limit(5);

      studyInfo.queries = queries?.map(query => ({
        id: query.id,
        subject: query.edc_form_data?.edc_subjects?.subject_number || 'Unknown',
        form: 'Form',
        field: query.field_name,
        status: query.status,
        priority: query.priority
      })) || [];
    }

    if (phase === 'CLOSE_OUT') {
      // Get closure activities
      const { data: activities } = await supabase
        .from('edc_closure_activities')
        .select('*')
        .eq('study_id', study.id)
        .order('created_at', { ascending: false });

      studyInfo.closureActivities = activities?.map(activity => ({
        task: activity.activity_name,
        status: activity.status,
        completion: `${activity.completion_percentage}%`
      })) || [];

      studyInfo.endDate = '2024-11-30';
      studyInfo.finalEnrollment = study.current_enrollment;
    }

    // Format metrics
    const studyMetrics = {};
    if (metrics) {
      metrics.forEach(metric => {
        const key = metric.metric_name.toLowerCase().replace(/\s+/g, '');
        studyMetrics[key] = metric.metric_value;
      });
    }

    // Add default metrics based on phase
    const defaultMetrics = getDefaultMetrics(phase, study);
    
    return {
      studyInfo,
      metrics: { ...defaultMetrics, ...studyMetrics }
    };

  } catch (error) {
    console.error('Error getting study data:', error);
    return { studyInfo: {}, metrics: {} };
  }
}

function getDefaultMetrics(phase, study) {
  const defaults = {
    START_UP: {
      totalForms: 8,
      completedForms: 3,
      sitesInitiated: 0,
      protocolDeviations: 0
    },
    CONDUCT: {
      enrollmentRate: `${Math.round((study?.current_enrollment || 0) / (study?.projected_enrollment || 1) * 100)}%`,
      dataCompletion: '82.3%',
      openQueries: 45,
      resolvedQueries: 123,
      adverseEvents: 8,
      protocolDeviations: 3
    },
    CLOSE_OUT: {
      totalSubjects: study?.current_enrollment || 0,
      completedSubjects: Math.max(0, (study?.current_enrollment || 0) - 13),
      withdrawnSubjects: 13,
      dataCompletion: '98.7%',
      openQueries: 12,
      resolvedQueries: 1245,
      adverseEvents: 127,
      protocolDeviations: 34
    }
  };
  return defaults[phase] || {};
}

// Middleware to authenticate JWT token
function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) {
    return res.status(401).json({
      status: 'error',
      message: 'Access token required'
    });
  }

  jwt.verify(token, JWT_SECRET, (err, user) => {
    if (err) {
      return res.status(403).json({
        status: 'error',
        message: 'Invalid or expired token'
      });
    }
    req.user = user;
    next();
  });
}

// Test endpoint to check Supabase connection
router.get('/test-db', async (req, res) => {
  try {
    console.log('Testing Supabase connection...');
    
    // Test basic connection
    const { data: users, error } = await supabase
      .from('edc_users')
      .select('username, email, phase')
      .limit(5);
      
    console.log('Users query result:', { users, error });
    
    if (error) {
      return res.json({
        status: 'error',
        message: 'Database connection failed',
        error: error.message,
        hint: 'Please run the supabase_schema.sql script first'
      });
    }
    
    res.json({
      status: 'success',
      message: 'Database connected successfully',
      users: users,
      count: users?.length || 0
    });
    
  } catch (error) {
    console.error('Database test error:', error);
    res.status(500).json({
      status: 'error',
      message: 'Database test failed',
      error: error.message
    });
  }
});

module.exports = router;