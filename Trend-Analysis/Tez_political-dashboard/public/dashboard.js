let popularityChart = null;
let sentimentChart = null;
let socialComparisonChart = null;
let socialGrowthChart = null;
let engagementChart = null;
let socialMediaData = null;
let allNewsData = { ysrcp: [], tdp: [] }; // Store all news for filtering
let selectedDistrict = 'all';

async function fetchDashboardData() {
    try {
        const response = await fetch('/api/dashboard-data');
        const data = await response.json();
        updateDashboard(data);

        const socialResponse = await fetch('/api/social-media-data');
        socialMediaData = await socialResponse.json();
        updateSocialMedia(socialMediaData);
    } catch (error) {
        console.error('Error fetching dashboard data:', error);
        showError('Failed to load dashboard data');
    }
}

function updateDashboard(data) {
    updatePartyMetrics('ysrcp', data.ysrcp);
    updatePartyMetrics('tdp', data.tdp);

    updateComparison(data.comparison);

    updateCharts(data);

    updateProsCons('ysrcp', data.ysrcp);
    updateProsCons('tdp', data.tdp);

    // Store all news data
    allNewsData.ysrcp = data.ysrcp.recentNews || [];
    allNewsData.tdp = data.tdp.recentNews || [];

    // Filter and display based on selected district
    filterAndDisplayNews();

    document.getElementById('last-updated').textContent = new Date().toLocaleString();
}

function filterAndDisplayNews() {
    const ysrcpFiltered = selectedDistrict === 'all'
        ? allNewsData.ysrcp
        : allNewsData.ysrcp.filter(news => news.district === selectedDistrict);

    const tdpFiltered = selectedDistrict === 'all'
        ? allNewsData.tdp
        : allNewsData.tdp.filter(news => news.district === selectedDistrict);

    updateNews('ysrcp', ysrcpFiltered);
    updateNews('tdp', tdpFiltered);
}

function updatePartyMetrics(party, partyData) {
    document.getElementById(`${party}-popularity`).textContent = 
        `${partyData.trends.currentMetrics.popularity}%`;
    
    const sentimentElement = document.getElementById(`${party}-sentiment`);
    sentimentElement.textContent = partyData.sentiment.overall.toFixed(2);
    sentimentElement.className = `value sentiment ${getSentimentClass(partyData.sentiment.overall)}`;
    
    const trendElement = document.getElementById(`${party}-trend`);
    trendElement.textContent = partyData.trends.trend;
    trendElement.innerHTML = getTrendIcon(partyData.trends.trend) + ' ' + partyData.trends.trend;
}

function updateComparison(comparison) {
    document.getElementById('leading-party').textContent = comparison.sentimentComparison.leader;
    document.getElementById('competition-index').textContent = comparison.sentimentComparison.difference.toFixed(2);
    document.getElementById('trend-direction').textContent = comparison.popularityTrend.convergenceTrend;
}

function updateCharts(data) {
    const ctx1 = document.getElementById('popularityChart').getContext('2d');
    const ctx2 = document.getElementById('sentimentChart').getContext('2d');
    
    if (popularityChart) popularityChart.destroy();
    if (sentimentChart) sentimentChart.destroy();
    
    const months = data.ysrcp.trends.historical.map(h => h.month);
    
    popularityChart = new Chart(ctx1, {
        type: 'line',
        data: {
            labels: months,
            datasets: [{
                label: 'YSRCP',
                data: data.ysrcp.trends.historical.map(h => h.popularity),
                borderColor: '#f5576c',
                backgroundColor: 'rgba(245, 87, 108, 0.1)',
                tension: 0.4,
                borderWidth: 2
            }, {
                label: 'TDP',
                data: data.tdp.trends.historical.map(h => h.popularity),
                borderColor: '#fee140',
                backgroundColor: 'rgba(254, 225, 64, 0.1)',
                tension: 0.4,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            aspectRatio: 2,
            plugins: {
                legend: {
                    position: 'top',
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    }
                }
            }
        }
    });
    
    sentimentChart = new Chart(ctx2, {
        type: 'bar',
        data: {
            labels: ['Positive', 'Negative', 'Neutral'],
            datasets: [{
                label: 'YSRCP',
                data: [
                    data.ysrcp.sentiment.news.distribution.positive,
                    data.ysrcp.sentiment.news.distribution.negative,
                    data.ysrcp.sentiment.news.distribution.neutral
                ],
                backgroundColor: 'rgba(245, 87, 108, 0.8)',
                borderColor: '#f5576c',
                borderWidth: 2
            }, {
                label: 'TDP',
                data: [
                    data.tdp.sentiment.news.distribution.positive,
                    data.tdp.sentiment.news.distribution.negative,
                    data.tdp.sentiment.news.distribution.neutral
                ],
                backgroundColor: 'rgba(254, 225, 64, 0.8)',
                borderColor: '#fee140',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            aspectRatio: 2,
            plugins: {
                legend: {
                    position: 'top',
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    }
                },
                x: {
                    grid: {
                        display: false
                    }
                }
            }
        }
    });
}

function updateSocialMedia(data) {
    if (!data || !data.platforms) return;
    
    const ysrcpData = data.platforms.ysrcp;
    const tdpData = data.platforms.tdp;
    
    // Update overview metrics
    document.getElementById('ysrcp-total-followers').textContent = 
        formatNumber(ysrcpData.aggregateMetrics.totalFollowers);
    document.getElementById('tdp-total-followers').textContent = 
        formatNumber(tdpData.aggregateMetrics.totalFollowers);
    
    document.getElementById('ysrcp-engagement-rate').textContent = 
        ysrcpData.aggregateMetrics.viralityScore.toFixed(1) + '%';
    document.getElementById('tdp-engagement-rate').textContent = 
        tdpData.aggregateMetrics.viralityScore.toFixed(1) + '%';
    
    document.getElementById('ysrcp-virality').textContent = 
        getViralityLabel(ysrcpData.aggregateMetrics.viralityScore);
    document.getElementById('tdp-virality').textContent = 
        getViralityLabel(tdpData.aggregateMetrics.viralityScore);
    
    // Update social comparison chart
    updateSocialComparisonChart(data.platforms);
    
    // Update social growth chart
    updateSocialGrowthChart(data.platforms);
    
    // Update engagement metrics chart
    updateEngagementChart(data.platforms);
}

function updateSocialComparisonChart(platforms) {
    const ctx = document.getElementById('socialComparisonChart');
    if (!ctx) return;
    
    if (socialComparisonChart) socialComparisonChart.destroy();
    
    socialComparisonChart = new Chart(ctx.getContext('2d'), {
        type: 'radar',
        data: {
            labels: ['Twitter', 'YouTube', 'Facebook', 'Instagram', 'Google Trends', 'LinkedIn'],
            datasets: [{
                label: 'YSRCP',
                data: [
                    platforms.ysrcp.twitter.followers / 10000,
                    platforms.ysrcp.youtube.subscribers / 10000,
                    platforms.ysrcp.facebook.pageFollowers / 10000,
                    platforms.ysrcp.instagram.followers / 10000,
                    platforms.ysrcp.googleTrends.searchInterest,
                    platforms.ysrcp.linkedin.followers / 10000
                ],
                borderColor: '#f5576c',
                backgroundColor: 'rgba(245, 87, 108, 0.2)',
                borderWidth: 2
            }, {
                label: 'TDP',
                data: [
                    platforms.tdp.twitter.followers / 10000,
                    platforms.tdp.youtube.subscribers / 10000,
                    platforms.tdp.facebook.pageFollowers / 10000,
                    platforms.tdp.instagram.followers / 10000,
                    platforms.tdp.googleTrends.searchInterest,
                    platforms.tdp.linkedin.followers / 10000
                ],
                borderColor: '#fee140',
                backgroundColor: 'rgba(254, 225, 64, 0.2)',
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            aspectRatio: 1.5,
            plugins: {
                legend: {
                    position: 'top',
                }
            },
            scales: {
                r: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    }
                }
            }
        }
    });
}

function updateSocialGrowthChart(platforms) {
    const ctx = document.getElementById('socialGrowthChart');
    if (!ctx) return;
    
    if (socialGrowthChart) socialGrowthChart.destroy();
    
    const days = platforms.ysrcp.googleTrends.weeklyTrend.map(d => d.date.split('-').slice(-1)[0]);
    
    socialGrowthChart = new Chart(ctx.getContext('2d'), {
        type: 'line',
        data: {
            labels: days,
            datasets: [{
                label: 'YSRCP Trend',
                data: platforms.ysrcp.googleTrends.weeklyTrend.map(d => d.value),
                borderColor: '#f5576c',
                backgroundColor: 'rgba(245, 87, 108, 0.1)',
                tension: 0.4,
                borderWidth: 2
            }, {
                label: 'TDP Trend',
                data: platforms.tdp.googleTrends.weeklyTrend.map(d => d.value),
                borderColor: '#fee140',
                backgroundColor: 'rgba(254, 225, 64, 0.1)',
                tension: 0.4,
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            aspectRatio: 2,
            plugins: {
                legend: {
                    position: 'top',
                },
                title: {
                    display: true,
                    text: '7-Day Google Search Trend'
                }
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    }
                },
                x: {
                    grid: {
                        color: 'rgba(0, 0, 0, 0.05)'
                    }
                }
            }
        }
    });
}

function updateEngagementChart(platforms) {
    const ctx = document.getElementById('engagementChart');
    if (!ctx) return;
    
    if (engagementChart) engagementChart.destroy();
    
    engagementChart = new Chart(ctx.getContext('2d'), {
        type: 'doughnut',
        data: {
            labels: ['Twitter', 'YouTube', 'Facebook', 'Instagram'],
            datasets: [{
                label: 'YSRCP Engagement',
                data: [
                    platforms.ysrcp.twitter.engagement.likes,
                    platforms.ysrcp.youtube.engagement.likes,
                    platforms.ysrcp.facebook.engagement.likes,
                    platforms.ysrcp.instagram.engagement.likes
                ],
                backgroundColor: [
                    'rgba(29, 161, 242, 0.8)',
                    'rgba(255, 0, 0, 0.8)',
                    'rgba(24, 119, 242, 0.8)',
                    'rgba(228, 64, 95, 0.8)'
                ],
                borderWidth: 2
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            aspectRatio: 2,
            plugins: {
                legend: {
                    position: 'right',
                },
                title: {
                    display: true,
                    text: 'Platform Engagement Distribution (YSRCP)'
                }
            }
        }
    });
}

function formatNumber(num) {
    if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
    if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
    return num.toString();
}

function getViralityLabel(score) {
    if (score > 80) return 'Viral 🔥';
    if (score > 60) return 'High';
    if (score > 40) return 'Medium';
    if (score > 20) return 'Low';
    return 'Very Low';
}

function updateProsCons(party, partyData) {
    const prosList = document.getElementById(`${party}-pros`);
    const consList = document.getElementById(`${party}-cons`);
    
    prosList.innerHTML = '';
    consList.innerHTML = '';
    
    partyData.pros.forEach(pro => {
        const li = document.createElement('li');
        li.textContent = pro;
        prosList.appendChild(li);
    });
    
    partyData.cons.forEach(con => {
        const li = document.createElement('li');
        li.textContent = con;
        consList.appendChild(li);
    });
}

function updateNews(party, newsItems) {
    const newsContainer = document.getElementById(`${party}-news`);
    newsContainer.innerHTML = '';

    if (!newsItems || newsItems.length === 0) {
        newsContainer.innerHTML = '<p>No recent news available for this district</p>';
        return;
    }

    newsItems.slice(0, 8).forEach(item => {
        const newsElement = document.createElement('div');
        newsElement.className = 'news-item';

        const sentimentClass = item.sentiment || 'neutral';
        const sentimentIcon = sentimentClass === 'positive' ? '✓' :
                              sentimentClass === 'negative' ? '✗' : '○';
        const districtLabel = item.district ? `📍 ${item.district}` : '';

        newsElement.innerHTML = `
            <h4>${item.title}</h4>
            <div class="news-meta">
                <span>${new Date(item.date).toLocaleDateString()}</span>
                <span class="sentiment-badge ${sentimentClass}">${sentimentIcon} ${sentimentClass.toUpperCase()}</span>
                ${districtLabel ? `<span class="district-badge">${districtLabel}</span>` : ''}
            </div>
        `;

        newsContainer.appendChild(newsElement);
    });
}

function getSentimentClass(score) {
    if (score > 0.2) return 'positive';
    if (score < -0.2) return 'negative';
    return 'neutral';
}

function getTrendIcon(trend) {
    switch(trend) {
        case 'upward': return '📈';
        case 'downward': return '📉';
        case 'stable': return '➡️';
        default: return '';
    }
}

function showError(message) {
    console.error(message);
}

document.getElementById('refreshBtn').addEventListener('click', async () => {
    const btn = document.getElementById('refreshBtn');
    btn.disabled = true;
    btn.textContent = 'Refreshing...';
    
    try {
        await fetch('/api/refresh-data');
        await fetchDashboardData();
    } catch (error) {
        console.error('Error refreshing data:', error);
    } finally {
        btn.disabled = false;
        btn.textContent = '🔄 Refresh Data';
    }
});

// Tab functionality for social media platforms
document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', function() {
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        this.classList.add('active');
        
        const platform = this.dataset.platform;
        
        if (platform === 'overview') {
            document.getElementById('social-overview').style.display = 'block';
            document.getElementById('platform-details').style.display = 'none';
        } else {
            document.getElementById('social-overview').style.display = 'none';
            document.getElementById('platform-details').style.display = 'block';
            showPlatformDetails(platform);
        }
    });
});

function showPlatformDetails(platform) {
    if (!socialMediaData) return;
    
    const detailsContainer = document.getElementById('platform-details');
    const ysrcp = socialMediaData.platforms.ysrcp[platform];
    const tdp = socialMediaData.platforms.tdp[platform];
    
    if (!ysrcp || !tdp) return;
    
    let html = `
        <h4>${platform.charAt(0).toUpperCase() + platform.slice(1)} Analytics</h4>
        <div class="platform-metrics-grid">
            <div class="platform-metric-card">
                <h5>YSRCP</h5>
                <div class="metric-row">
                    <span>Followers:</span>
                    <strong>${formatNumber(ysrcp.followers || ysrcp.subscribers || ysrcp.pageFollowers || 0)}</strong>
                </div>
                <div class="metric-row">
                    <span>24h Change:</span>
                    <strong class="${(ysrcp.followersChange || ysrcp.subscriberChange || 0) >= 0 ? 'positive' : 'negative'}">
                        ${(ysrcp.followersChange || ysrcp.subscriberChange || 0) >= 0 ? '+' : ''}${formatNumber(ysrcp.followersChange || ysrcp.subscriberChange || 0)}
                    </strong>
                </div>
                ${ysrcp.engagement ? `
                    <div class="metric-row">
                        <span>Engagement:</span>
                        <strong>${formatNumber(Object.values(ysrcp.engagement).reduce((a, b) => typeof b === 'number' ? a + b : a, 0))}</strong>
                    </div>
                ` : ''}
            </div>
            <div class="platform-metric-card">
                <h5>TDP</h5>
                <div class="metric-row">
                    <span>Followers:</span>
                    <strong>${formatNumber(tdp.followers || tdp.subscribers || tdp.pageFollowers || 0)}</strong>
                </div>
                <div class="metric-row">
                    <span>24h Change:</span>
                    <strong class="${(tdp.followersChange || tdp.subscriberChange || 0) >= 0 ? 'positive' : 'negative'}">
                        ${(tdp.followersChange || tdp.subscriberChange || 0) >= 0 ? '+' : ''}${formatNumber(tdp.followersChange || tdp.subscriberChange || 0)}
                    </strong>
                </div>
                ${tdp.engagement ? `
                    <div class="metric-row">
                        <span>Engagement:</span>
                        <strong>${formatNumber(Object.values(tdp.engagement).reduce((a, b) => typeof b === 'number' ? a + b : a, 0))}</strong>
                    </div>
                ` : ''}
            </div>
        </div>
    `;
    
    detailsContainer.innerHTML = html;
}

// District selector event listener
document.getElementById('district-select').addEventListener('change', (e) => {
    selectedDistrict = e.target.value;
    filterAndDisplayNews();
});

fetchDashboardData();
setInterval(fetchDashboardData, 60000);