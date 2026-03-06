let strategicData = null;
let advancedData = null;
let charts = {};

async function loadStrategicData() {
    try {
        const [strategic, advanced] = await Promise.all([
            fetch('/api/strategic-analysis').then(r => r.json()),
            fetch('/api/advanced-data').then(r => r.json())
        ]);
        
        strategicData = strategic;
        advancedData = advanced;
        
        updateAllSections();
    } catch (error) {
        console.error('Error loading strategic data:', error);
    }
}

function updateAllSections() {
    updateWinProbability();
    updateAlerts();
    updateRecommendations();
    updateDemographics();
    updateRegional();
    updateIssues();
    updateCampaign();
    updateRealtime();
    updateEconomic();
    updateActionTracker();
    
    document.getElementById('last-updated').textContent = new Date().toLocaleString();
    document.getElementById('confidence-level').textContent = 
        `${advancedData.realTime.predictive_analytics.confidence_level}%`;
}

function updateWinProbability() {
    const prob = strategicData.winProbability;
    const prediction = advancedData.realTime.predictive_analytics;
    
    document.getElementById('win-probability').textContent = `${prob.current_probability}%`;
    document.getElementById('prob-trend').textContent = 
        `Trend: ${prob.trend === 'favorable' ? '📈 Favorable' : '📉 Concerning'}`;
    
    const fillElement = document.getElementById('prob-fill');
    fillElement.style.width = `${prob.current_probability}%`;
    
    document.getElementById('seat-min').textContent = prediction.seat_projection.ysrcp.min;
    document.getElementById('seat-likely').textContent = prediction.seat_projection.ysrcp.likely;
    document.getElementById('seat-max').textContent = prediction.seat_projection.ysrcp.max;
    
    document.getElementById('vote-share').textContent = 
        `${prediction.vote_share_projection.ysrcp}%`;
}

function updateAlerts() {
    const risks = advancedData.realTime.risk_alerts;
    const opportunities = advancedData.realTime.opportunity_alerts;
    
    const risksHtml = risks.map(risk => `
        <div class="alert-item ${risk.level}">
            <div class="alert-title">${risk.issue}</div>
            <div class="alert-description">${risk.impact}</div>
            <div class="alert-action">Action: ${risk.mitigation}</div>
        </div>
    `).join('');
    
    const oppsHtml = opportunities.map(opp => `
        <div class="alert-item ${opp.level}">
            <div class="alert-title">${opp.opportunity}</div>
            <div class="alert-description">Potential: ${opp.potential}</div>
            <div class="alert-action">Action: ${opp.action}</div>
        </div>
    `).join('');
    
    document.getElementById('risk-alerts').innerHTML = risksHtml;
    document.getElementById('opportunity-alerts').innerHTML = oppsHtml;
}

function updateRecommendations() {
    const recs = strategicData.recommendations;
    showImmediateActions();
    
    // Tab functionality
    document.querySelectorAll('.tab-button').forEach(btn => {
        btn.addEventListener('click', function() {
            document.querySelectorAll('.tab-button').forEach(b => b.classList.remove('active'));
            this.classList.add('active');
            
            const tab = this.dataset.tab;
            switch(tab) {
                case 'immediate':
                    showImmediateActions();
                    break;
                case 'medium':
                    showMediumTermStrategy();
                    break;
                case 'communication':
                    showCommunicationStrategy();
                    break;
                case 'competitive':
                    showCompetitivePosition();
                    break;
            }
        });
    });
}

function showImmediateActions() {
    const actions = strategicData.recommendations.immediate_actions;
    const html = actions.map(action => `
        <div class="action-item">
            <span class="action-priority">Priority ${action.priority}</span>
            <div class="action-title">${action.action}</div>
            <div class="action-details">${action.details}</div>
            <div class="action-meta">
                <span>📅 Timeline: ${action.timeline}</span>
                <span>💥 Impact: ${action.impact}</span>
                <span>💰 Resources: ${action.resources}</span>
            </div>
        </div>
    `).join('');
    
    document.getElementById('recommendations-content').innerHTML = html;
}

function showMediumTermStrategy() {
    const strategy = strategicData.recommendations.medium_term_strategy;
    const html = strategy.map(item => `
        <div class="action-item">
            <div class="action-title">${item.initiative}</div>
            <div class="action-details">Area: ${item.area} | Target: ${item.target}</div>
            <div class="action-meta">
                <span>📅 Timeline: ${item.timeline}</span>
                <span>📈 Expected Impact: ${item.expected_impact}</span>
            </div>
        </div>
    `).join('');
    
    document.getElementById('recommendations-content').innerHTML = html;
}

function showCommunicationStrategy() {
    const comm = strategicData.recommendations.communication_strategy;
    let html = '<div class="comm-strategy">';
    
    html += '<h3>Key Messages</h3><ul>';
    comm.key_messages.forEach(msg => {
        html += `<li>${msg}</li>`;
    });
    html += '</ul>';
    
    html += '<h3>Counter Narratives</h3>';
    Object.entries(comm.counter_narratives).forEach(([issue, response]) => {
        html += `<div class="counter-item"><strong>${issue}:</strong> ${response}</div>`;
    });
    
    html += '</div>';
    document.getElementById('recommendations-content').innerHTML = html;
}

function showCompetitivePosition() {
    const comp = strategicData.recommendations.competitive_positioning;
    let html = '<div class="competitive-analysis">';
    
    ['strengths_to_leverage', 'weaknesses_to_address', 'opportunities', 'threats'].forEach(type => {
        const label = type.replace(/_/g, ' ').toUpperCase();
        html += `<h3>${label}</h3><ul>`;
        comp[type].forEach(item => {
            html += `<li>${item}</li>`;
        });
        html += '</ul>';
    });
    
    html += '</div>';
    document.getElementById('recommendations-content').innerHTML = html;
}

function updateDemographics() {
    const demo = strategicData.demographics;
    
    // Demographics Chart
    const ctx1 = document.getElementById('demographicsChart');
    if (charts.demographics) charts.demographics.destroy();
    
    charts.demographics = new Chart(ctx1, {
        type: 'bar',
        data: {
            labels: ['Youth (18-25)', 'Adults (26-40)', 'Middle (41-60)', 'Seniors (60+)'],
            datasets: [{
                label: 'YSRCP Support %',
                data: [
                    demo.ysrcp.youth_18_25.support,
                    demo.ysrcp.adults_26_40.support,
                    demo.ysrcp.middle_aged_41_60.support,
                    demo.ysrcp.seniors_60_plus.support
                ],
                backgroundColor: 'rgba(245, 87, 108, 0.8)'
            }, {
                label: 'TDP Support %',
                data: [
                    demo.tdp.youth_18_25.support,
                    demo.tdp.adults_26_40.support,
                    demo.tdp.middle_aged_41_60.support,
                    demo.tdp.seniors_60_plus.support
                ],
                backgroundColor: 'rgba(254, 225, 64, 0.8)'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true, max: 100 } }
        }
    });
    
    // Segments Chart
    const ctx2 = document.getElementById('segmentsChart');
    if (charts.segments) charts.segments.destroy();
    
    const segments = strategicData.segments.segments;
    charts.segments = new Chart(ctx2, {
        type: 'doughnut',
        data: {
            labels: segments.map(s => s.name),
            datasets: [{
                data: segments.map(s => parseInt(s.size)),
                backgroundColor: [
                    'rgba(102, 126, 234, 0.8)',
                    'rgba(118, 75, 162, 0.8)',
                    'rgba(245, 87, 108, 0.8)',
                    'rgba(254, 225, 64, 0.8)'
                ]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false
        }
    });
    
    // Voter Segments Cards
    const segmentsHtml = segments.map(seg => `
        <div class="segment-card">
            <div class="segment-name">${seg.name}</div>
            <div class="segment-size">${seg.size}</div>
            <div class="segment-support">
                <span>Support: ${seg.ysrcp_support}%</span>
                <span>Risk: ${seg.risk || seg.opportunity || 'moderate'}</span>
            </div>
        </div>
    `).join('');
    
    document.getElementById('voter-segments').innerHTML = segmentsHtml;
}

function updateRegional() {
    const regions = strategicData.regions;
    const districts = regions.districts;
    
    // Regional Chart
    const ctx = document.getElementById('regionalChart');
    if (charts.regional) charts.regional.destroy();
    
    charts.regional = new Chart(ctx, {
        type: 'horizontalBar',
        data: {
            labels: Object.keys(districts),
            datasets: [{
                label: 'YSRCP Support %',
                data: Object.values(districts).map(d => d.ysrcp),
                backgroundColor: Object.values(districts).map(d => 
                    d.ysrcp > 60 ? 'rgba(72, 187, 120, 0.8)' :
                    d.ysrcp > 50 ? 'rgba(246, 173, 85, 0.8)' :
                    'rgba(245, 101, 101, 0.8)'
                )
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { x: { beginAtZero: true, max: 100 } }
        }
    });
    
    // District List
    const districtHtml = Object.entries(districts).map(([name, data]) => {
        const cssClass = data.stronghold === 'ysrcp' ? 'stronghold' :
                        data.swing ? 'competitive' : 'weak';
        return `
            <div class="district-item ${cssClass}">
                <span>${name}</span>
                <span>${data.ysrcp}%</span>
            </div>
        `;
    }).join('');
    
    document.getElementById('district-list').innerHTML = districtHtml;
}

function updateIssues() {
    const issues = strategicData.issues.top_issues;
    
    // Issues Chart
    const ctx = document.getElementById('issuesChart');
    if (charts.issues) charts.issues.destroy();
    
    charts.issues = new Chart(ctx, {
        type: 'radar',
        data: {
            labels: issues.map(i => i.issue),
            datasets: [{
                label: 'YSRCP Perception',
                data: issues.map(i => i.ysrcp_perception),
                borderColor: 'rgba(245, 87, 108, 1)',
                backgroundColor: 'rgba(245, 87, 108, 0.2)'
            }, {
                label: 'TDP Perception',
                data: issues.map(i => i.tdp_perception),
                borderColor: 'rgba(254, 225, 64, 1)',
                backgroundColor: 'rgba(254, 225, 64, 0.2)'
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scale: { ticks: { beginAtZero: true, max: 100 } }
        }
    });
    
    // Issues Grid
    const issuesHtml = issues.map(issue => `
        <div class="issue-card ${issue.action_needed === 'critical' ? 'critical' : issue.action_needed === 'urgent' ? 'urgent' : ''}">
            <div class="issue-name">${issue.issue}</div>
            <div class="issue-metrics">
                <span>Importance: ${issue.importance}%</span>
                <span>Gap: ${issue.gap > 0 ? '+' : ''}${issue.gap}%</span>
            </div>
            <div>Action: ${issue.action_needed}</div>
        </div>
    `).join('');
    
    document.getElementById('issues-grid').innerHTML = issuesHtml;
}

function updateCampaign() {
    const campaigns = strategicData.campaigns.recent_campaigns;
    const media = strategicData.mediaPerception;
    
    // Campaign List
    const campaignHtml = campaigns.map(camp => `
        <div class="campaign-item">
            <strong>${camp.name}</strong>
            <div>Reach: ${(camp.reach/1000000).toFixed(1)}M | Engagement: ${camp.engagement}%</div>
            <div>Impact: ${camp.impact} | ROI: ${camp.cost_effectiveness}%</div>
        </div>
    `).join('');
    
    document.getElementById('campaign-list').innerHTML = campaignHtml;
    
    // Media Chart
    const ctx = document.getElementById('mediaChart');
    if (charts.media) charts.media.destroy();
    
    charts.media = new Chart(ctx, {
        type: 'pie',
        data: {
            labels: ['Positive', 'Neutral', 'Negative'],
            datasets: [{
                data: [
                    media.coverage_tone.positive,
                    media.coverage_tone.neutral,
                    media.coverage_tone.negative
                ],
                backgroundColor: [
                    'rgba(72, 187, 120, 0.8)',
                    'rgba(156, 163, 175, 0.8)',
                    'rgba(245, 101, 101, 0.8)'
                ]
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false
        }
    });
}

function updateRealtime() {
    const realtime = advancedData.realTime.live_metrics;
    
    document.getElementById('social-mentions').textContent = 
        (realtime.social_media_mentions.ysrcp / 1000).toFixed(1) + 'K';
    document.getElementById('news-articles').textContent = 
        realtime.news_coverage.articles_today;
    document.getElementById('rallies-count').textContent = 
        realtime.ground_reports.rallies_today.ysrcp;
    document.getElementById('volunteer-activity').textContent = 
        (realtime.ground_reports.volunteer_activity.door_to_door / 1000).toFixed(1) + 'K';
}

function updateEconomic() {
    const economy = advancedData.economy;
    
    // Economic Chart
    const ctx = document.getElementById('economicChart');
    if (charts.economic) charts.economic.destroy();
    
    charts.economic = new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['Agriculture', 'Industry', 'Services'],
            datasets: [{
                label: 'Growth Rate %',
                data: [
                    economy.sector_performance.agriculture.growth,
                    economy.sector_performance.industry.growth,
                    economy.sector_performance.services.growth
                ],
                borderColor: 'rgba(102, 126, 234, 1)',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                tension: 0.4
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true } }
        }
    });
    
    // Economic Indicators
    const indicatorsHtml = `
        <div class="indicator-item">
            <span>GSDP Growth</span>
            <strong>${economy.state_economy.gsdp_growth}%</strong>
        </div>
        <div class="indicator-item">
            <span>Unemployment</span>
            <strong>${economy.state_economy.unemployment_rate}%</strong>
        </div>
        <div class="indicator-item">
            <span>Fiscal Deficit</span>
            <strong>${economy.state_economy.fiscal_deficit}%</strong>
        </div>
        <div class="indicator-item">
            <span>Welfare Beneficiaries</span>
            <strong>${(economy.welfare_spending.beneficiaries.total/1000000).toFixed(1)}M</strong>
        </div>
    `;
    
    document.getElementById('economic-indicators').innerHTML = indicatorsHtml;
}

function updateActionTracker() {
    const actions = [
        { status: 'completed', emoji: '✅', title: 'Welfare Wednesday campaign launched' },
        { status: 'progress', emoji: '⏳', title: 'Youth employment program development' },
        { status: 'progress', emoji: '⏳', title: 'Social media counter-narrative campaign' },
        { status: 'pending', emoji: '📋', title: 'Metro Rail announcement preparation' },
        { status: 'pending', emoji: '📋', title: 'Industrial MOU negotiations' }
    ];
    
    const actionsHtml = actions.map(action => `
        <div class="action-track-item">
            <div class="action-status ${action.status}">${action.emoji}</div>
            <div>${action.title}</div>
            <div>${action.status === 'completed' ? 'Done' : 
                   action.status === 'progress' ? 'In Progress' : 'Pending'}</div>
        </div>
    `).join('');
    
    document.getElementById('actions-grid').innerHTML = actionsHtml;
}

// Event Listeners
document.getElementById('refreshAllBtn').addEventListener('click', loadStrategicData);

document.getElementById('exportReportBtn').addEventListener('click', () => {
    alert('Report export functionality would generate PDF/Excel report');
});

document.getElementById('emergencyAlertBtn').addEventListener('click', () => {
    document.getElementById('alerts-section').scrollIntoView({ behavior: 'smooth' });
});

// Initialize
loadStrategicData();
setInterval(loadStrategicData, 60000); // Refresh every minute