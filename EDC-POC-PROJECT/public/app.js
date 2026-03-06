let currentUser = null;
let token = null;
let formFields = [];
let selectedField = null;
let studies = [];
let currentStudy = null;

const API_URL = 'http://localhost:3001/api';

async function apiCall(endpoint, method = 'GET', body = null) {
    const options = {
        method,
        headers: {
            'Content-Type': 'application/json'
        }
    };

    if (token) {
        options.headers['Authorization'] = `Bearer ${token}`;
    }

    if (body) {
        options.body = JSON.stringify(body);
    }

    try {
        const response = await fetch(`${API_URL}${endpoint}`, options);
        const data = await response.json();
        
        if (!response.ok) {
            throw new Error(data.error || 'API call failed');
        }
        
        return data;
    } catch (error) {
        console.error('API Error:', error);
        alert(error.message);
        throw error;
    }
}

document.getElementById('loginForm').addEventListener('submit', async (e) => {
    e.preventDefault();
    const username = document.getElementById('username').value;
    const password = document.getElementById('password').value;

    try {
        const response = await apiCall('/login', 'POST', { username, password });
        token = response.token;
        currentUser = response.user;
        
        document.getElementById('loginSection').classList.remove('active');
        document.getElementById('mainSection').classList.add('active');
        document.getElementById('userDisplay').textContent = currentUser.username;
        document.getElementById('roleDisplay').textContent = currentUser.role;
        document.getElementById('roleDisplay').className = `role-badge ${currentUser.role}`;
        document.body.className = `role-${currentUser.role}`;
        
        loadStudies();
        setupFormBuilder();
        
        if (currentUser.role === 'admin') {
            loadAuditTrail();
        }
    } catch (error) {
        console.error('Login failed:', error);
    }
});

function logout() {
    token = null;
    currentUser = null;
    document.getElementById('loginSection').classList.add('active');
    document.getElementById('mainSection').classList.remove('active');
    document.getElementById('username').value = '';
    document.getElementById('password').value = '';
}

function switchTab(tabName) {
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    
    event.target.classList.add('active');
    document.getElementById(`${tabName}Tab`).classList.add('active');
    
    if (tabName === 'studies') loadStudies();
    if (tabName === 'dataEntry') loadStudiesForDataEntry();
    if (tabName === 'auditTrail' && currentUser.role === 'admin') loadAuditTrail();
}

async function loadStudies() {
    try {
        studies = await apiCall('/studies');
        const studiesList = document.getElementById('studiesList');
        const studySelect = document.getElementById('studySelect');
        
        studiesList.innerHTML = '';
        studySelect.innerHTML = '<option value="">Select Study</option>';
        
        studies.forEach(study => {
            const card = document.createElement('div');
            card.className = 'study-card';
            card.innerHTML = `
                <div class="study-info">
                    <h3>${study.name}</h3>
                    <div class="study-meta">
                        <span>ID: ${study.study_id}</span>
                        <span class="phase-badge ${study.phase}">${study.phase}</span>
                    </div>
                </div>
                <div class="study-actions">
                    ${currentUser.role === 'admin' ? `
                        <button onclick="changePhase('${study.study_id}')">Change Phase</button>
                    ` : ''}
                    <button onclick="selectStudy('${study.study_id}')">Select</button>
                </div>
            `;
            studiesList.appendChild(card);
            
            const option = document.createElement('option');
            option.value = study.study_id;
            option.textContent = study.name;
            studySelect.appendChild(option);
        });
    } catch (error) {
        console.error('Failed to load studies:', error);
    }
}

function showCreateStudyModal() {
    document.getElementById('createStudyModal').classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

async function createStudy() {
    const name = document.getElementById('newStudyName').value;
    if (!name) return;

    try {
        await apiCall('/studies', 'POST', { name });
        closeModal('createStudyModal');
        document.getElementById('newStudyName').value = '';
        loadStudies();
        alert('Study created successfully!');
    } catch (error) {
        console.error('Failed to create study:', error);
    }
}

async function changePhase(studyId) {
    const study = studies.find(s => s.study_id === studyId);
    const phases = ['setup', 'conduct', 'closeout'];
    const currentIndex = phases.indexOf(study.phase);
    const nextPhase = phases[(currentIndex + 1) % phases.length];
    
    if (confirm(`Change phase from ${study.phase} to ${nextPhase}?`)) {
        try {
            await apiCall(`/studies/${studyId}/phase`, 'PUT', { phase: nextPhase });
            loadStudies();
            alert('Phase updated successfully!');
        } catch (error) {
            console.error('Failed to update phase:', error);
        }
    }
}

function selectStudy(studyId) {
    currentStudy = studies.find(s => s.study_id === studyId);
    document.getElementById('studySelect').value = studyId;
    alert(`Selected study: ${currentStudy.name}`);
}

function setupFormBuilder() {
    const fieldTypes = document.querySelectorAll('.field-type');
    const formCanvas = document.getElementById('formCanvas');
    
    fieldTypes.forEach(fieldType => {
        fieldType.addEventListener('dragstart', (e) => {
            e.dataTransfer.setData('fieldType', e.target.dataset.type);
        });
    });
    
    formCanvas.addEventListener('dragover', (e) => {
        e.preventDefault();
        formCanvas.classList.add('drag-over');
    });
    
    formCanvas.addEventListener('dragleave', () => {
        formCanvas.classList.remove('drag-over');
    });
    
    formCanvas.addEventListener('drop', (e) => {
        e.preventDefault();
        formCanvas.classList.remove('drag-over');
        
        const fieldType = e.dataTransfer.getData('fieldType');
        addFieldToForm(fieldType);
    });
}

function addFieldToForm(type) {
    const fieldId = `field_${Date.now()}`;
    const field = {
        id: fieldId,
        type: type,
        label: `${type.charAt(0).toUpperCase() + type.slice(1)} Field`,
        name: fieldId,
        required: false,
        validation: []
    };
    
    formFields.push(field);
    renderFormBuilder();
}

function renderFormBuilder() {
    const formCanvas = document.getElementById('formCanvas');
    
    if (formFields.length === 0) {
        formCanvas.innerHTML = '<p class="drop-hint">Drag fields here to build your form</p>';
        return;
    }
    
    formCanvas.innerHTML = '';
    
    formFields.forEach(field => {
        const fieldElement = document.createElement('div');
        fieldElement.className = 'form-field';
        fieldElement.dataset.fieldId = field.id;
        
        if (selectedField === field.id) {
            fieldElement.classList.add('selected');
        }
        
        fieldElement.innerHTML = `
            <div class="field-actions">
                <button onclick="editField('${field.id}')">Edit</button>
                <button onclick="removeField('${field.id}')">Remove</button>
            </div>
            <div class="field-label">${field.label} ${field.required ? '*' : ''}</div>
            ${renderFieldInput(field)}
        `;
        
        fieldElement.addEventListener('click', () => selectField(field.id));
        formCanvas.appendChild(fieldElement);
    });
}

function renderFieldInput(field) {
    switch(field.type) {
        case 'text':
        case 'number':
        case 'date':
            return `<input type="${field.type}" class="field-input" placeholder="${field.label}" disabled>`;
        case 'textarea':
            return `<textarea class="field-input" placeholder="${field.label}" disabled></textarea>`;
        case 'dropdown':
            return `<select class="field-input" disabled><option>Select...</option></select>`;
        case 'radio':
            return `
                <div>
                    <label><input type="radio" disabled> Option 1</label><br>
                    <label><input type="radio" disabled> Option 2</label>
                </div>
            `;
        case 'checkbox':
            return `<label><input type="checkbox" disabled> Check this option</label>`;
        default:
            return '';
    }
}

function selectField(fieldId) {
    selectedField = fieldId;
    renderFormBuilder();
    showFieldProperties(fieldId);
}

function showFieldProperties(fieldId) {
    const field = formFields.find(f => f.id === fieldId);
    const propertiesDiv = document.getElementById('fieldProperties');
    
    propertiesDiv.innerHTML = `
        <div class="property-group">
            <label>Label</label>
            <input type="text" value="${field.label}" onchange="updateFieldProperty('${fieldId}', 'label', this.value)">
        </div>
        <div class="property-group">
            <label>Field Name</label>
            <input type="text" value="${field.name}" onchange="updateFieldProperty('${fieldId}', 'name', this.value)">
        </div>
        <div class="property-group">
            <label>Required</label>
            <input type="checkbox" ${field.required ? 'checked' : ''} onchange="updateFieldProperty('${fieldId}', 'required', this.checked)">
        </div>
        ${field.type === 'number' ? `
            <div class="property-group">
                <label>Min Value</label>
                <input type="number" value="${field.min || ''}" onchange="updateFieldProperty('${fieldId}', 'min', this.value)">
            </div>
            <div class="property-group">
                <label>Max Value</label>
                <input type="number" value="${field.max || ''}" onchange="updateFieldProperty('${fieldId}', 'max', this.value)">
            </div>
        ` : ''}
    `;
}

function updateFieldProperty(fieldId, property, value) {
    const field = formFields.find(f => f.id === fieldId);
    field[property] = value;
    renderFormBuilder();
}

function editField(fieldId) {
    const field = formFields.find(f => f.id === fieldId);
    const modal = document.getElementById('fieldPropertiesModal');
    const content = document.getElementById('fieldConfigContent');
    
    content.innerHTML = `
        <div class="property-group">
            <label>Label</label>
            <input type="text" id="modalFieldLabel" value="${field.label}">
        </div>
        <div class="property-group">
            <label>Field Name</label>
            <input type="text" id="modalFieldName" value="${field.name}">
        </div>
        <div class="property-group">
            <label>Required</label>
            <input type="checkbox" id="modalFieldRequired" ${field.required ? 'checked' : ''}>
        </div>
        <div class="property-group">
            <label>Validation Rules</label>
            <div class="validation-rule">
                <select id="validationType">
                    <option value="">Select validation type</option>
                    <option value="required">Required</option>
                    <option value="range">Range</option>
                    <option value="datatype">Data Type</option>
                </select>
            </div>
        </div>
    `;
    
    modal.classList.add('active');
    modal.dataset.fieldId = fieldId;
}

function saveFieldProperties() {
    const fieldId = document.getElementById('fieldPropertiesModal').dataset.fieldId;
    const field = formFields.find(f => f.id === fieldId);
    
    field.label = document.getElementById('modalFieldLabel').value;
    field.name = document.getElementById('modalFieldName').value;
    field.required = document.getElementById('modalFieldRequired').checked;
    
    closeModal('fieldPropertiesModal');
    renderFormBuilder();
}

function removeField(fieldId) {
    formFields = formFields.filter(f => f.id !== fieldId);
    renderFormBuilder();
}

function clearForm() {
    if (confirm('Clear all fields?')) {
        formFields = [];
        renderFormBuilder();
    }
}

async function saveForm() {
    const studyId = document.getElementById('studySelect').value;
    const formName = document.getElementById('formName').value;
    const formDescription = document.getElementById('formDescription').value;
    
    if (!studyId || !formName || formFields.length === 0) {
        alert('Please select a study, enter a form name, and add at least one field');
        return;
    }
    
    const validationRules = formFields
        .filter(f => f.required || f.min || f.max)
        .map(f => {
            const rules = [];
            if (f.required) {
                rules.push({
                    field: f.name,
                    type: 'required',
                    severity: 'error'
                });
            }
            if (f.type === 'number' && (f.min || f.max)) {
                rules.push({
                    field: f.name,
                    type: 'range',
                    min: f.min || 0,
                    max: f.max || 999999,
                    severity: 'warning'
                });
            }
            return rules;
        }).flat();
    
    try {
        await apiCall('/forms', 'POST', {
            study_id: studyId,
            name: formName,
            description: formDescription,
            fields: formFields,
            validation_rules: validationRules
        });
        
        alert('Form saved successfully!');
        clearForm();
        document.getElementById('formName').value = '';
        document.getElementById('formDescription').value = '';
    } catch (error) {
        console.error('Failed to save form:', error);
    }
}

function previewForm() {
    if (formFields.length === 0) {
        alert('Add fields to preview the form');
        return;
    }
    
    alert('Form preview:\n' + formFields.map(f => `- ${f.label} (${f.type})`).join('\n'));
}

async function loadStudiesForDataEntry() {
    try {
        const studies = await apiCall('/studies');
        const studySelect = document.getElementById('studySelectEntry');
        
        studySelect.innerHTML = '<option value="">Select Study</option>';
        studies.forEach(study => {
            const option = document.createElement('option');
            option.value = study.study_id;
            option.textContent = `${study.name} (${study.phase})`;
            studySelect.appendChild(option);
        });
    } catch (error) {
        console.error('Failed to load studies:', error);
    }
}

async function loadFormsForEntry() {
    const studyId = document.getElementById('studySelectEntry').value;
    if (!studyId) return;
    
    try {
        const forms = await apiCall(`/forms/${studyId}`);
        const formSelect = document.getElementById('formSelectEntry');
        
        formSelect.innerHTML = '<option value="">Select Form</option>';
        forms.forEach(form => {
            const option = document.createElement('option');
            option.value = form.form_id;
            option.textContent = form.name;
            option.dataset.formData = JSON.stringify(form);
            formSelect.appendChild(option);
        });
        
        loadSubmissions(studyId);
    } catch (error) {
        console.error('Failed to load forms:', error);
    }
}

async function loadFormForEntry() {
    const formSelect = document.getElementById('formSelectEntry');
    const selectedOption = formSelect.options[formSelect.selectedIndex];
    const patientId = document.getElementById('patientId').value;
    
    if (!selectedOption || !selectedOption.dataset.formData || !patientId) {
        alert('Please select a form and enter a patient ID');
        return;
    }
    
    const form = JSON.parse(selectedOption.dataset.formData);
    const entryDiv = document.getElementById('dataEntryForm');
    
    entryDiv.innerHTML = `
        <h3>${form.name}</h3>
        <p>${form.description || ''}</p>
        <div id="entryFields"></div>
        <div style="margin-top: 20px;">
            <button onclick="saveSubmission('draft')">Save as Draft</button>
            <button onclick="saveSubmission('submitted')">Submit</button>
        </div>
    `;
    
    const fieldsDiv = document.getElementById('entryFields');
    form.fields.forEach(field => {
        const fieldDiv = document.createElement('div');
        fieldDiv.style.marginBottom = '15px';
        
        fieldDiv.innerHTML = `
            <label style="display: block; margin-bottom: 5px; font-weight: 600;">
                ${field.label} ${field.required ? '*' : ''}
            </label>
            ${renderEntryField(field)}
        `;
        
        fieldsDiv.appendChild(fieldDiv);
    });
    
    entryDiv.dataset.formId = form.form_id;
    entryDiv.dataset.studyId = document.getElementById('studySelectEntry').value;
}

function renderEntryField(field) {
    switch(field.type) {
        case 'text':
            return `<input type="text" id="${field.name}" class="field-input" style="width: 100%; padding: 8px;">`;
        case 'number':
            return `<input type="number" id="${field.name}" class="field-input" style="width: 100%; padding: 8px;" ${field.min ? `min="${field.min}"` : ''} ${field.max ? `max="${field.max}"` : ''}>`;
        case 'date':
            return `<input type="date" id="${field.name}" class="field-input" style="width: 100%; padding: 8px;">`;
        case 'textarea':
            return `<textarea id="${field.name}" class="field-input" style="width: 100%; padding: 8px;" rows="3"></textarea>`;
        case 'dropdown':
            return `<select id="${field.name}" class="field-input" style="width: 100%; padding: 8px;">
                        <option value="">Select...</option>
                        <option value="option1">Option 1</option>
                        <option value="option2">Option 2</option>
                    </select>`;
        case 'radio':
            return `
                <div>
                    <label><input type="radio" name="${field.name}" value="option1"> Option 1</label><br>
                    <label><input type="radio" name="${field.name}" value="option2"> Option 2</label>
                </div>
            `;
        case 'checkbox':
            return `<input type="checkbox" id="${field.name}">`;
        default:
            return '';
    }
}

async function saveSubmission(status) {
    const entryDiv = document.getElementById('dataEntryForm');
    const formId = entryDiv.dataset.formId;
    const studyId = entryDiv.dataset.studyId;
    const patientId = document.getElementById('patientId').value;
    
    const formSelect = document.getElementById('formSelectEntry');
    const selectedOption = formSelect.options[formSelect.selectedIndex];
    const form = JSON.parse(selectedOption.dataset.formData);
    
    const data = {};
    form.fields.forEach(field => {
        if (field.type === 'radio') {
            const selected = document.querySelector(`input[name="${field.name}"]:checked`);
            data[field.name] = selected ? selected.value : '';
        } else if (field.type === 'checkbox') {
            data[field.name] = document.getElementById(field.name).checked;
        } else {
            data[field.name] = document.getElementById(field.name).value;
        }
    });
    
    try {
        await apiCall('/submissions', 'POST', {
            form_id: formId,
            study_id: studyId,
            patient_id: patientId,
            data: data,
            status: status
        });
        
        alert(`Form ${status === 'draft' ? 'saved as draft' : 'submitted'} successfully!`);
        
        form.fields.forEach(field => {
            if (field.type !== 'radio' && field.type !== 'checkbox') {
                document.getElementById(field.name).value = '';
            }
        });
        
        loadSubmissions(studyId);
    } catch (error) {
        console.error('Failed to save submission:', error);
    }
}

async function loadSubmissions(studyId) {
    try {
        const submissions = await apiCall(`/submissions/${studyId}`);
        const submissionsDiv = document.getElementById('submissionsList');
        
        submissionsDiv.innerHTML = '<h3>Recent Submissions</h3>';
        
        if (submissions.length === 0) {
            submissionsDiv.innerHTML += '<p>No submissions yet</p>';
            return;
        }
        
        submissions.forEach(submission => {
            const card = document.createElement('div');
            card.className = 'submission-card';
            card.innerHTML = `
                <div class="submission-header">
                    <div>
                        <strong>Patient: ${submission.patient_id}</strong><br>
                        <small>Submission ID: ${submission.submission_id}</small>
                    </div>
                    <div>
                        <span class="submission-status ${submission.status}">${submission.status}</span>
                    </div>
                </div>
                <div>
                    <small>Submitted by: ${submission.submitted_by_name} at ${new Date(submission.submitted_at).toLocaleString()}</small>
                </div>
                <button onclick="viewDiscrepancies('${submission.submission_id}')" style="margin-top: 10px;">View Discrepancies</button>
            `;
            submissionsDiv.appendChild(card);
        });
        
        loadAllDiscrepancies();
    } catch (error) {
        console.error('Failed to load submissions:', error);
    }
}

async function viewDiscrepancies(submissionId) {
    try {
        const discrepancies = await apiCall(`/discrepancies/${submissionId}`);
        
        if (discrepancies.length === 0) {
            alert('No discrepancies found for this submission');
            return;
        }
        
        let message = 'Discrepancies:\n\n';
        discrepancies.forEach(disc => {
            message += `Field: ${disc.field_name}\n`;
            message += `Value: ${disc.field_value}\n`;
            message += `Message: ${disc.message}\n`;
            message += `Status: ${disc.status}\n`;
            message += `Phase: ${disc.phase}\n\n`;
        });
        
        alert(message);
    } catch (error) {
        console.error('Failed to load discrepancies:', error);
    }
}

async function loadAllDiscrepancies() {
    const studyId = document.getElementById('studySelectEntry').value;
    if (!studyId) return;
    
    try {
        const submissions = await apiCall(`/submissions/${studyId}`);
        const discrepanciesDiv = document.getElementById('discrepanciesList');
        discrepanciesDiv.innerHTML = '';
        
        for (const submission of submissions) {
            const discrepancies = await apiCall(`/discrepancies/${submission.submission_id}`);
            
            discrepancies.forEach(disc => {
                const card = document.createElement('div');
                card.className = `discrepancy-card ${disc.status === 'resolved' ? 'resolved' : ''}`;
                card.dataset.phase = disc.phase;
                card.dataset.status = disc.status;
                
                card.innerHTML = `
                    <div class="discrepancy-header">
                        <div>
                            <strong>${disc.field_name}</strong> - Patient: ${submission.patient_id}
                        </div>
                        <div>
                            <span class="severity-badge ${disc.severity}">${disc.severity}</span>
                            <span class="phase-badge ${disc.phase === 'before_submit' ? 'setup' : 'conduct'}">${disc.phase}</span>
                        </div>
                    </div>
                    <p>Value: ${disc.field_value} | Expected: ${disc.expected_value || 'Valid value'}</p>
                    <p>${disc.message}</p>
                    <small>Created: ${new Date(disc.created_at).toLocaleString()}</small>
                    ${disc.status === 'open' && currentUser.role !== 'data_manager' ? `
                        <div style="margin-top: 10px;">
                            <input type="text" id="resolution_${disc.discrepancy_id}" placeholder="Resolution note" style="padding: 5px;">
                            <button onclick="resolveDiscrepancy('${disc.discrepancy_id}')">Resolve</button>
                        </div>
                    ` : ''}
                    ${disc.status === 'resolved' ? `
                        <p style="margin-top: 10px; color: green;">Resolved: ${disc.resolution_note}</p>
                    ` : ''}
                `;
                
                discrepanciesDiv.appendChild(card);
            });
        }
    } catch (error) {
        console.error('Failed to load discrepancies:', error);
    }
}

async function resolveDiscrepancy(discrepancyId) {
    const resolutionNote = document.getElementById(`resolution_${discrepancyId}`).value;
    
    if (!resolutionNote) {
        alert('Please enter a resolution note');
        return;
    }
    
    try {
        await apiCall(`/discrepancies/${discrepancyId}/resolve`, 'PUT', {
            resolution_note: resolutionNote
        });
        
        alert('Discrepancy resolved successfully!');
        loadAllDiscrepancies();
    } catch (error) {
        console.error('Failed to resolve discrepancy:', error);
    }
}

function filterDiscrepancies() {
    const phaseFilter = document.getElementById('discrepancyPhaseFilter').value;
    const statusFilter = document.getElementById('discrepancyStatusFilter').value;
    
    const cards = document.querySelectorAll('.discrepancy-card');
    cards.forEach(card => {
        const cardPhase = card.dataset.phase;
        const cardStatus = card.dataset.status;
        
        let show = true;
        
        if (phaseFilter !== 'all' && cardPhase !== phaseFilter) {
            show = false;
        }
        
        if (statusFilter !== 'all' && cardStatus !== statusFilter) {
            show = false;
        }
        
        card.style.display = show ? 'block' : 'none';
    });
}

async function loadAuditTrail() {
    if (currentUser.role !== 'admin') return;
    
    try {
        const audits = await apiCall('/audit-trail');
        const auditDiv = document.getElementById('auditTrailList');
        
        auditDiv.innerHTML = '';
        
        audits.forEach(audit => {
            const entry = document.createElement('div');
            entry.className = 'audit-entry';
            entry.innerHTML = `
                <div class="timestamp">${new Date(audit.timestamp).toLocaleString()}</div>
                <div class="action">${audit.action}</div>
                <div>${audit.username}</div>
                <div>${audit.entity_type}: ${audit.entity_id} ${audit.reason ? `- ${audit.reason}` : ''}</div>
            `;
            auditDiv.appendChild(entry);
        });
    } catch (error) {
        console.error('Failed to load audit trail:', error);
    }
}