const express = require('express');
const jwt = require('jsonwebtoken');
const router = express.Router();

// Mock users for three phases with different permissions
const users = [
  {
    id: '1',
    username: 'startup_admin',
    password: 'Startup@2024',
    email: 'startup@edc.com',
    firstName: 'Sarah',
    lastName: 'Johnson',
    role: 'STUDY_DESIGNER',
    phase: 'START_UP',
    title: 'Study Designer',
    institution: 'Clinical Research Institute',
    permissions: {
      canCreateStudy: true,
      canDesignForms: true,
      canSetupSites: true,
      canConfigureWorkflows: true,
      canEnrollSubjects: false,
      canEnterData: false,
      canCloseStudy: false,
      canExportData: false,
      canViewReports: true,
      canManageQueries: false
    }
  },
  {
    id: '2', 
    username: 'conduct_crc',
    password: 'Conduct@2024',
    email: 'conduct@edc.com',
    firstName: 'Michael',
    lastName: 'Chen',
    role: 'CRC',
    phase: 'CONDUCT',
    title: 'Clinical Research Coordinator',
    institution: 'City Medical Center',
    permissions: {
      canCreateStudy: false,
      canDesignForms: false,
      canSetupSites: false,
      canConfigureWorkflows: false,
      canEnrollSubjects: true,
      canEnterData: true,
      canCloseStudy: false,
      canExportData: false,
      canViewReports: true,
      canManageQueries: true
    }
  },
  {
    id: '3',
    username: 'closeout_manager',
    password: 'Closeout@2024',
    email: 'closeout@edc.com',
    firstName: 'Emily',
    lastName: 'Williams',
    role: 'DATA_MANAGER',
    phase: 'CLOSE_OUT',
    title: 'Data Manager',
    institution: 'Research Analytics Corp',
    permissions: {
      canCreateStudy: false,
      canDesignForms: false,
      canSetupSites: false,
      canConfigureWorkflows: false,
      canEnrollSubjects: false,
      canEnterData: false,
      canCloseStudy: true,
      canExportData: true,
      canViewReports: true,
      canManageQueries: true
    }
  }
];

// Mock study data for different phases
const studyData = {
  START_UP: {
    studyInfo: {
      protocolNumber: 'EDC-2024-001',
      title: 'Phase III Clinical Trial - Setup Phase',
      status: 'SETUP',
      phase: 'START_UP',
      startDate: null,
      projectedEnrollment: 500,
      sites: [
        { id: 1, name: 'Site 001 - Boston Medical', status: 'INITIATING', subjects: 0 },
        { id: 2, name: 'Site 002 - NYC Hospital', status: 'INITIATING', subjects: 0 },
        { id: 3, name: 'Site 003 - LA Clinical Center', status: 'PENDING', subjects: 0 }
      ],
      forms: [
        { id: 1, name: 'Demographics Form', version: '1.0', status: 'DRAFT', fields: 12 },
        { id: 2, name: 'Medical History', version: '1.0', status: 'DRAFT', fields: 18 },
        { id: 3, name: 'Adverse Events', version: '1.0', status: 'IN_REVIEW', fields: 15 }
      ],
      milestones: [
        { task: 'Protocol Approval', status: 'COMPLETED', date: '2024-01-15' },
        { task: 'Form Design', status: 'IN_PROGRESS', date: null },
        { task: 'Site Initiation', status: 'PENDING', date: null },
        { task: 'First Patient In', status: 'PENDING', date: null }
      ]
    },
    metrics: {
      totalForms: 8,
      completedForms: 3,
      sitesInitiated: 0,
      protocolDeviations: 0
    }
  },
  CONDUCT: {
    studyInfo: {
      protocolNumber: 'EDC-2024-001',
      title: 'Phase III Clinical Trial - Active Enrollment',
      status: 'ACTIVE',
      phase: 'CONDUCT',
      startDate: '2024-02-01',
      currentEnrollment: 287,
      projectedEnrollment: 500,
      sites: [
        { id: 1, name: 'Site 001 - Boston Medical', status: 'ACTIVE', subjects: 98 },
        { id: 2, name: 'Site 002 - NYC Hospital', status: 'ACTIVE', subjects: 124 },
        { id: 3, name: 'Site 003 - LA Clinical Center', status: 'ACTIVE', subjects: 65 }
      ],
      subjects: [
        { id: 'S001', site: 'Boston Medical', enrollmentDate: '2024-02-15', visitsPending: 2, status: 'ACTIVE' },
        { id: 'S002', site: 'NYC Hospital', enrollmentDate: '2024-02-16', visitsPending: 1, status: 'ACTIVE' },
        { id: 'S003', site: 'LA Clinical Center', enrollmentDate: '2024-02-20', visitsPending: 3, status: 'ACTIVE' },
        { id: 'S004', site: 'Boston Medical', enrollmentDate: '2024-02-22', visitsPending: 0, status: 'COMPLETED' },
        { id: 'S005', site: 'NYC Hospital', enrollmentDate: '2024-02-25', visitsPending: 2, status: 'ACTIVE' }
      ],
      queries: [
        { id: 1, subject: 'S001', form: 'Demographics', field: 'DOB', status: 'OPEN', priority: 'HIGH' },
        { id: 2, subject: 'S002', form: 'Medical History', field: 'Medications', status: 'ANSWERED', priority: 'MEDIUM' },
        { id: 3, subject: 'S003', form: 'Adverse Events', field: 'Severity', status: 'OPEN', priority: 'HIGH' }
      ]
    },
    metrics: {
      enrollmentRate: '57.4%',
      dataCompletion: '82.3%',
      openQueries: 45,
      resolvedQueries: 123,
      adverseEvents: 8,
      protocolDeviations: 3
    }
  },
  CLOSE_OUT: {
    studyInfo: {
      protocolNumber: 'EDC-2024-001',
      title: 'Phase III Clinical Trial - Database Lock',
      status: 'CLOSING',
      phase: 'CLOSE_OUT',
      startDate: '2024-02-01',
      endDate: '2024-11-30',
      finalEnrollment: 498,
      sites: [
        { id: 1, name: 'Site 001 - Boston Medical', status: 'CLOSING', subjects: 165, dataComplete: '98.5%' },
        { id: 2, name: 'Site 002 - NYC Hospital', status: 'CLOSED', subjects: 198, dataComplete: '100%' },
        { id: 3, name: 'Site 003 - LA Clinical Center', status: 'CLOSING', subjects: 135, dataComplete: '97.2%' }
      ],
      closureActivities: [
        { task: 'Query Resolution', status: 'IN_PROGRESS', completion: '92%' },
        { task: 'Data Verification', status: 'IN_PROGRESS', completion: '88%' },
        { task: 'SDV Completion', status: 'COMPLETED', completion: '100%' },
        { task: 'Database Lock', status: 'PENDING', completion: '0%' },
        { task: 'Final Report Generation', status: 'PENDING', completion: '0%' }
      ],
      exports: [
        { format: 'SAS', date: '2024-11-15', records: 498000, status: 'COMPLETED' },
        { format: 'CSV', date: '2024-11-18', records: 498000, status: 'COMPLETED' },
        { format: 'CDISC', date: '2024-11-20', records: 498000, status: 'IN_PROGRESS' }
      ]
    },
    metrics: {
      totalSubjects: 498,
      completedSubjects: 485,
      withdrawnSubjects: 13,
      dataCompletion: '98.7%',
      openQueries: 12,
      resolvedQueries: 1245,
      adverseEvents: 127,
      seriousAdverseEvents: 8,
      protocolDeviations: 34
    }
  }
};

const signToken = (id) => {
  return jwt.sign({ id }, process.env.JWT_SECRET || 'edc-secret-key-2024', {
    expiresIn: '7d'
  });
};

// Simple login endpoint
router.post('/login', async (req, res) => {
  try {
    const { username, password } = req.body;

    // Find user
    const user = users.find(u => u.username === username && u.password === password);

    if (!user) {
      return res.status(401).json({
        status: 'error',
        message: 'Invalid credentials'
      });
    }

    // Create token
    const token = signToken(user.id);

    // Get phase-specific data
    const phaseData = studyData[user.phase];

    // Remove password from response
    const userResponse = { ...user };
    delete userResponse.password;

    res.status(200).json({
      status: 'success',
      message: 'Login successful',
      token,
      data: {
        user: userResponse,
        studyData: phaseData
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Server error during authentication'
    });
  }
});

// Get current user
router.get('/me', async (req, res) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        status: 'error',
        message: 'No token provided'
      });
    }

    const token = authHeader.split(' ')[1];
    
    // Verify token
    let decoded;
    try {
      decoded = jwt.verify(token, process.env.JWT_SECRET || 'edc-secret-key-2024');
    } catch (error) {
      return res.status(401).json({
        status: 'error',
        message: 'Invalid token'
      });
    }

    // Find user
    const user = users.find(u => u.id === decoded.id);
    if (!user) {
      return res.status(404).json({
        status: 'error',
        message: 'User not found'
      });
    }

    // Get phase-specific data
    const phaseData = studyData[user.phase];

    // Remove password from response
    const userResponse = { ...user };
    delete userResponse.password;

    res.status(200).json({
      status: 'success',
      data: {
        user: userResponse,
        studyData: phaseData
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Server error'
    });
  }
});

// Verify data endpoint - returns phase-specific verification results
router.get('/verify-data', async (req, res) => {
  try {
    // Get token from header
    const authHeader = req.headers.authorization;
    if (!authHeader || !authHeader.startsWith('Bearer ')) {
      return res.status(401).json({
        status: 'error',
        message: 'No token provided'
      });
    }

    const token = authHeader.split(' ')[1];
    
    // Verify token
    let decoded;
    try {
      decoded = jwt.verify(token, process.env.JWT_SECRET || 'edc-secret-key-2024');
    } catch (error) {
      return res.status(401).json({
        status: 'error',
        message: 'Invalid token'
      });
    }

    // Find user
    const user = users.find(u => u.id === decoded.id);
    if (!user) {
      return res.status(404).json({
        status: 'error',
        message: 'User not found'
      });
    }

    // Return phase-specific verification data
    const verificationData = {
      START_UP: {
        phase: 'START_UP',
        verificationStatus: 'IN_PROGRESS',
        checks: [
          { item: 'Protocol Approval', status: 'PASSED', details: 'Protocol approved by IRB' },
          { item: 'Form Validation', status: 'IN_PROGRESS', details: '5 of 8 forms validated' },
          { item: 'Site Readiness', status: 'PENDING', details: 'Awaiting site initiation visits' },
          { item: 'System Configuration', status: 'PASSED', details: 'EDC system configured' },
          { item: 'User Training', status: 'IN_PROGRESS', details: '60% of users trained' }
        ]
      },
      CONDUCT: {
        phase: 'CONDUCT',
        verificationStatus: 'ONGOING',
        checks: [
          { item: 'Data Entry Compliance', status: 'PASSED', details: '98% within 48hr window' },
          { item: 'Query Resolution Rate', status: 'WARNING', details: '73% resolved (target: 80%)' },
          { item: 'Protocol Adherence', status: 'PASSED', details: '3 deviations documented' },
          { item: 'Safety Reporting', status: 'PASSED', details: 'All AEs reported within timeline' },
          { item: 'Source Data Verification', status: 'IN_PROGRESS', details: '65% SDV complete' }
        ]
      },
      CLOSE_OUT: {
        phase: 'CLOSE_OUT',
        verificationStatus: 'FINALIZING',
        checks: [
          { item: 'Database Completeness', status: 'PASSED', details: '98.7% complete' },
          { item: 'Query Closure', status: 'WARNING', details: '12 queries pending' },
          { item: 'Audit Trail Review', status: 'PASSED', details: 'No discrepancies found' },
          { item: 'Data Export Validation', status: 'IN_PROGRESS', details: 'CDISC export in progress' },
          { item: 'Regulatory Compliance', status: 'PASSED', details: '21 CFR Part 11 compliant' }
        ]
      }
    };

    res.status(200).json({
      status: 'success',
      data: {
        verification: verificationData[user.phase],
        timestamp: new Date().toISOString(),
        verifiedBy: `${user.firstName} ${user.lastName}`,
        role: user.role,
        phase: user.phase
      }
    });
  } catch (error) {
    res.status(500).json({
      status: 'error',
      message: 'Server error during verification'
    });
  }
});

module.exports = router;