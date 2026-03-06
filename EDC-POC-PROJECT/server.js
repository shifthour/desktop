const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const cors = require('cors');
const { v4: uuidv4 } = require('uuid');
const path = require('path');

const app = express();
const PORT = 3001;
const JWT_SECRET = 'edc-poc-secret-key-2024';

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

const db = new sqlite3.Database('./edc_database.db');

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        password TEXT,
        role TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS studies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        study_id TEXT UNIQUE,
        name TEXT,
        phase TEXT DEFAULT 'setup',
        created_by INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (created_by) REFERENCES users(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS forms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        form_id TEXT UNIQUE,
        study_id TEXT,
        name TEXT,
        description TEXT,
        fields TEXT,
        validation_rules TEXT,
        version INTEGER DEFAULT 1,
        status TEXT DEFAULT 'draft',
        created_by INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (study_id) REFERENCES studies(study_id),
        FOREIGN KEY (created_by) REFERENCES users(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS form_submissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        submission_id TEXT UNIQUE,
        form_id TEXT,
        study_id TEXT,
        patient_id TEXT,
        data TEXT,
        status TEXT DEFAULT 'draft',
        submitted_by INTEGER,
        submitted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (form_id) REFERENCES forms(form_id),
        FOREIGN KEY (study_id) REFERENCES studies(study_id),
        FOREIGN KEY (submitted_by) REFERENCES users(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS discrepancies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        discrepancy_id TEXT UNIQUE,
        submission_id TEXT,
        field_name TEXT,
        field_value TEXT,
        expected_value TEXT,
        message TEXT,
        severity TEXT,
        status TEXT DEFAULT 'open',
        phase TEXT,
        created_by INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        resolved_at DATETIME,
        resolution_note TEXT,
        FOREIGN KEY (submission_id) REFERENCES form_submissions(submission_id),
        FOREIGN KEY (created_by) REFERENCES users(id)
    )`);

    db.run(`CREATE TABLE IF NOT EXISTS audit_trail (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT,
        entity_type TEXT,
        entity_id TEXT,
        old_value TEXT,
        new_value TEXT,
        reason TEXT,
        ip_address TEXT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id)
    )`);

    db.get("SELECT COUNT(*) as count FROM users", (err, row) => {
        if (row.count === 0) {
            const defaultPassword = bcrypt.hashSync('password123', 10);
            db.run("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", 
                ['admin', defaultPassword, 'admin']);
            db.run("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", 
                ['doctor', defaultPassword, 'doctor']);
            db.run("INSERT INTO users (username, password, role) VALUES (?, ?, ?)", 
                ['datamanager', defaultPassword, 'data_manager']);
            console.log('Default users created: admin, doctor, datamanager (password: password123)');
        }
    });
});

const authenticateToken = (req, res, next) => {
    const token = req.headers['authorization']?.split(' ')[1];
    if (!token) return res.status(401).json({ error: 'Access denied' });

    jwt.verify(token, JWT_SECRET, (err, user) => {
        if (err) return res.status(403).json({ error: 'Invalid token' });
        req.user = user;
        next();
    });
};

const logAudit = (userId, action, entityType, entityId, oldValue, newValue, reason, ip) => {
    db.run(`INSERT INTO audit_trail (user_id, action, entity_type, entity_id, old_value, new_value, reason, ip_address) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
        [userId, action, entityType, entityId, oldValue, newValue, reason, ip]);
};

app.post('/api/login', (req, res) => {
    const { username, password } = req.body;
    
    db.get("SELECT * FROM users WHERE username = ?", [username], (err, user) => {
        if (err || !user) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }
        
        if (!bcrypt.compareSync(password, user.password)) {
            return res.status(401).json({ error: 'Invalid credentials' });
        }
        
        const token = jwt.sign({ id: user.id, username: user.username, role: user.role }, JWT_SECRET);
        logAudit(user.id, 'LOGIN', 'user', user.id, null, null, 'User login', req.ip);
        res.json({ token, user: { id: user.id, username: user.username, role: user.role } });
    });
});

app.post('/api/studies', authenticateToken, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Only admins can create studies' });
    }
    
    const { name } = req.body;
    const studyId = `STUDY-${uuidv4().substring(0, 8).toUpperCase()}`;
    
    db.run("INSERT INTO studies (study_id, name, created_by) VALUES (?, ?, ?)",
        [studyId, name, req.user.id], function(err) {
        if (err) return res.status(500).json({ error: err.message });
        
        logAudit(req.user.id, 'CREATE', 'study', studyId, null, JSON.stringify({name}), 'Study created', req.ip);
        res.json({ id: this.lastID, study_id: studyId, name });
    });
});

app.get('/api/studies', authenticateToken, (req, res) => {
    db.all("SELECT * FROM studies ORDER BY created_at DESC", (err, studies) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(studies);
    });
});

app.put('/api/studies/:studyId/phase', authenticateToken, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Only admins can change study phase' });
    }
    
    const { studyId } = req.params;
    const { phase } = req.body;
    
    if (!['setup', 'conduct', 'closeout'].includes(phase)) {
        return res.status(400).json({ error: 'Invalid phase' });
    }
    
    db.get("SELECT phase FROM studies WHERE study_id = ?", [studyId], (err, study) => {
        if (err || !study) return res.status(404).json({ error: 'Study not found' });
        
        const oldPhase = study.phase;
        db.run("UPDATE studies SET phase = ? WHERE study_id = ?", [phase, studyId], (err) => {
            if (err) return res.status(500).json({ error: err.message });
            
            logAudit(req.user.id, 'UPDATE', 'study', studyId, oldPhase, phase, 'Phase change', req.ip);
            res.json({ message: 'Phase updated successfully' });
        });
    });
});

app.post('/api/forms', authenticateToken, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Only admins can create forms' });
    }
    
    const { study_id, name, description, fields, validation_rules } = req.body;
    const formId = `FORM-${uuidv4().substring(0, 8).toUpperCase()}`;
    
    db.run(`INSERT INTO forms (form_id, study_id, name, description, fields, validation_rules, created_by) 
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [formId, study_id, name, description, JSON.stringify(fields), JSON.stringify(validation_rules), req.user.id],
        function(err) {
            if (err) return res.status(500).json({ error: err.message });
            
            logAudit(req.user.id, 'CREATE', 'form', formId, null, JSON.stringify({name, study_id}), 'Form created', req.ip);
            res.json({ id: this.lastID, form_id: formId, name });
        });
});

app.get('/api/forms/:studyId', authenticateToken, (req, res) => {
    const { studyId } = req.params;
    
    db.all("SELECT * FROM forms WHERE study_id = ? ORDER BY created_at DESC", [studyId], (err, forms) => {
        if (err) return res.status(500).json({ error: err.message });
        
        forms = forms.map(form => ({
            ...form,
            fields: JSON.parse(form.fields || '[]'),
            validation_rules: JSON.parse(form.validation_rules || '[]')
        }));
        
        res.json(forms);
    });
});

app.post('/api/submissions', authenticateToken, (req, res) => {
    if (req.user.role === 'data_manager') {
        return res.status(403).json({ error: 'Data managers cannot submit forms (read-only)' });
    }
    
    const { form_id, study_id, patient_id, data, status } = req.body;
    const submissionId = `SUB-${uuidv4().substring(0, 8).toUpperCase()}`;
    
    db.run(`INSERT INTO form_submissions (submission_id, form_id, study_id, patient_id, data, status, submitted_by) 
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
        [submissionId, form_id, study_id, patient_id, JSON.stringify(data), status || 'draft', req.user.id],
        function(err) {
            if (err) return res.status(500).json({ error: err.message });
            
            logAudit(req.user.id, 'CREATE', 'submission', submissionId, null, JSON.stringify({form_id, patient_id}), 'Form submitted', req.ip);
            
            if (status === 'submitted') {
                validateSubmission(submissionId, form_id, data, req.user.id);
            }
            
            res.json({ id: this.lastID, submission_id: submissionId });
        });
});

function validateSubmission(submissionId, formId, data, userId) {
    db.get("SELECT validation_rules FROM forms WHERE form_id = ?", [formId], (err, form) => {
        if (err || !form) return;
        
        const rules = JSON.parse(form.validation_rules || '[]');
        const submissionData = typeof data === 'string' ? JSON.parse(data) : data;
        
        rules.forEach(rule => {
            const fieldValue = submissionData[rule.field];
            let isValid = true;
            let message = '';
            
            switch(rule.type) {
                case 'required':
                    if (!fieldValue || fieldValue === '') {
                        isValid = false;
                        message = `${rule.field} is required`;
                    }
                    break;
                case 'range':
                    const numValue = parseFloat(fieldValue);
                    if (numValue < rule.min || numValue > rule.max) {
                        isValid = false;
                        message = `${rule.field} must be between ${rule.min} and ${rule.max}`;
                    }
                    break;
                case 'datatype':
                    if (rule.datatype === 'number' && isNaN(fieldValue)) {
                        isValid = false;
                        message = `${rule.field} must be a number`;
                    }
                    break;
            }
            
            if (!isValid) {
                const discrepancyId = `DISC-${uuidv4().substring(0, 8).toUpperCase()}`;
                db.run(`INSERT INTO discrepancies (discrepancy_id, submission_id, field_name, field_value, expected_value, message, severity, phase, created_by) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
                    [discrepancyId, submissionId, rule.field, fieldValue, rule.expected || '', message, rule.severity || 'warning', 'before_submit', userId]);
            }
        });
    });
}

app.get('/api/submissions/:studyId', authenticateToken, (req, res) => {
    const { studyId } = req.params;
    
    db.all(`SELECT s.*, u.username as submitted_by_name 
            FROM form_submissions s 
            JOIN users u ON s.submitted_by = u.id 
            WHERE s.study_id = ? 
            ORDER BY s.submitted_at DESC`, [studyId], (err, submissions) => {
        if (err) return res.status(500).json({ error: err.message });
        
        submissions = submissions.map(sub => ({
            ...sub,
            data: JSON.parse(sub.data || '{}')
        }));
        
        res.json(submissions);
    });
});

app.post('/api/discrepancies', authenticateToken, (req, res) => {
    const { submission_id, field_name, field_value, expected_value, message, severity, phase } = req.body;
    const discrepancyId = `DISC-${uuidv4().substring(0, 8).toUpperCase()}`;
    
    db.run(`INSERT INTO discrepancies (discrepancy_id, submission_id, field_name, field_value, expected_value, message, severity, phase, created_by) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [discrepancyId, submission_id, field_name, field_value, expected_value, message, severity, phase || 'after_submit', req.user.id],
        function(err) {
            if (err) return res.status(500).json({ error: err.message });
            
            logAudit(req.user.id, 'CREATE', 'discrepancy', discrepancyId, null, JSON.stringify({submission_id, field_name}), 'Discrepancy raised', req.ip);
            res.json({ id: this.lastID, discrepancy_id: discrepancyId });
        });
});

app.get('/api/discrepancies/:submissionId', authenticateToken, (req, res) => {
    const { submissionId } = req.params;
    
    db.all(`SELECT d.*, u.username as created_by_name 
            FROM discrepancies d 
            JOIN users u ON d.created_by = u.id 
            WHERE d.submission_id = ? 
            ORDER BY d.created_at DESC`, [submissionId], (err, discrepancies) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(discrepancies);
    });
});

app.put('/api/discrepancies/:discrepancyId/resolve', authenticateToken, (req, res) => {
    const { discrepancyId } = req.params;
    const { resolution_note } = req.body;
    
    db.get("SELECT status FROM discrepancies WHERE discrepancy_id = ?", [discrepancyId], (err, disc) => {
        if (err || !disc) return res.status(404).json({ error: 'Discrepancy not found' });
        
        db.run(`UPDATE discrepancies SET status = 'resolved', resolved_at = CURRENT_TIMESTAMP, resolution_note = ? 
                WHERE discrepancy_id = ?`, [resolution_note, discrepancyId], (err) => {
            if (err) return res.status(500).json({ error: err.message });
            
            logAudit(req.user.id, 'UPDATE', 'discrepancy', discrepancyId, disc.status, 'resolved', resolution_note, req.ip);
            res.json({ message: 'Discrepancy resolved' });
        });
    });
});

app.get('/api/audit-trail', authenticateToken, (req, res) => {
    if (req.user.role !== 'admin') {
        return res.status(403).json({ error: 'Only admins can view audit trail' });
    }
    
    db.all(`SELECT a.*, u.username 
            FROM audit_trail a 
            JOIN users u ON a.user_id = u.id 
            ORDER BY a.timestamp DESC 
            LIMIT 100`, (err, audits) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(audits);
    });
});

app.listen(PORT, () => {
    console.log(`EDC POC Server running on http://localhost:${PORT}`);
    console.log('\nDefault credentials:');
    console.log('  Admin: admin / password123');
    console.log('  Doctor: doctor / password123');
    console.log('  Data Manager: datamanager / password123');
});