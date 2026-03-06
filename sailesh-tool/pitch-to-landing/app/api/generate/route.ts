import { NextRequest } from 'next/server'
import Anthropic from '@anthropic-ai/sdk'

export const runtime = 'nodejs'
export const maxDuration = 300

// Helper to stream JSON responses
function createStream() {
  const encoder = new TextEncoder()
  let controller: ReadableStreamDefaultController<Uint8Array>

  const stream = new ReadableStream({
    start(c) {
      controller = c
    },
  })

  const send = (data: object) => {
    controller.enqueue(encoder.encode(JSON.stringify(data) + '\n'))
  }

  const close = () => {
    controller.close()
  }

  return { stream, send, close }
}

// Parse PDF using pdf-parse
async function parsePDF(buffer: Buffer): Promise<string> {
  // @ts-ignore - pdf-parse types issue
  const pdfParse = require('pdf-parse')
  const data = await pdfParse(buffer)
  return data.text
}

// Use Claude to structure the content
async function analyzeWithClaude(pdfText: string): Promise<object> {
  const client = new Anthropic({
    apiKey: process.env.ANTHROPIC_API_KEY,
  })

  const prompt = `You are an expert at analyzing pitch decks and business documents. Analyze the following text extracted from a pitch deck PDF and structure it into a JSON format for creating an interactive landing page.

Extract and structure the following information (use null for any field you cannot find):

{
  "company_name": "Company name",
  "tagline": "Main tagline or slogan",
  "year": "Year mentioned",
  "vision": "Vision statement",
  "mission": "Mission statement if available",

  "key_metrics": [
    {"value": "number/text", "label": "description", "detail": "additional context"}
  ],

  "business_verticals": [
    {
      "name": "Vertical name",
      "icon": "suggested font-awesome icon name (e.g., 'film', 'building', 'robot')",
      "description": "Brief description",
      "highlights": ["highlight 1", "highlight 2"]
    }
  ],

  "products_or_projects": [
    {
      "name": "Product/Project name",
      "year": "Year",
      "type": "Type (e.g., Film, Product, Service)",
      "description": "Brief description",
      "metrics": {"budget": "if available", "revenue": "if available", "roi": "if available"}
    }
  ],

  "financials": {
    "current_revenue": "Current revenue if mentioned",
    "projected_revenue": [
      {"year": "FY27", "amount": "amount", "growth": "growth %"}
    ],
    "key_financial_points": ["point 1", "point 2"]
  },

  "investment": {
    "amount_seeking": "Amount being raised",
    "use_of_funds": [
      {"category": "Category name", "amount": "Amount", "percentage": "Percentage", "description": "What it's for"}
    ],
    "investment_highlights": ["highlight 1", "highlight 2"]
  },

  "team": [
    {
      "name": "Person name",
      "role": "Title/Role",
      "description": "Brief bio or achievements"
    }
  ],

  "achievements": [
    {"title": "Achievement", "description": "Details"}
  ],

  "contact": {
    "email": "email if available",
    "website": "website if available",
    "address": "address if available"
  },

  "color_scheme": {
    "primary": "suggested primary color hex based on brand",
    "secondary": "suggested secondary color hex",
    "accent": "suggested accent color hex"
  }
}

Here is the pitch deck text:

${pdfText.slice(0, 50000)}

Respond ONLY with valid JSON, no markdown formatting or explanation.`

  const response = await client.messages.create({
    model: 'claude-sonnet-4-20250514',
    max_tokens: 8000,
    messages: [
      { role: 'user', content: prompt }
    ],
  })

  const content = response.content[0]
  if (content.type !== 'text') {
    throw new Error('Unexpected response type')
  }

  // Parse the JSON response
  let jsonStr = content.text.trim()
  // Remove markdown code blocks if present
  if (jsonStr.startsWith('```')) {
    jsonStr = jsonStr.replace(/```json?\n?/g, '').replace(/```$/g, '').trim()
  }

  return JSON.parse(jsonStr)
}

// Generate the landing page HTML
function generateLandingPage(data: any): string {
  const {
    company_name = 'Company Name',
    tagline = 'Your Tagline Here',
    year = '2024',
    vision = '',
    key_metrics = [],
    business_verticals = [],
    products_or_projects = [],
    financials = {},
    investment = {},
    team = [],
    achievements = [],
    contact = {},
    color_scheme = { primary: '#D4A84B', secondary: '#1a1a5e', accent: '#0d0d3d' }
  } = data

  const primaryColor = color_scheme.primary || '#D4A84B'
  const secondaryColor = color_scheme.secondary || '#1a1a5e'
  const accentColor = color_scheme.accent || '#0d0d3d'

  return `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>${company_name} | Investor Pitch</title>
    <link href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        :root {
            --primary: ${primaryColor};
            --secondary: ${secondaryColor};
            --accent: ${accentColor};
            --white: #ffffff;
            --gray: #6c757d;
            --light-gray: #f8f9fa;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Poppins', sans-serif; background: var(--white); color: #333; }

        nav {
            position: fixed; top: 0; width: 100%; background: rgba(26, 26, 94, 0.95);
            backdrop-filter: blur(10px); z-index: 1000; padding: 15px 5%;
            display: flex; justify-content: space-between; align-items: center;
        }
        .logo { font-size: 1.5rem; font-weight: 700; color: var(--primary); }
        .logo span { color: var(--white); }
        .nav-links { display: flex; gap: 30px; list-style: none; }
        .nav-links a { color: var(--white); text-decoration: none; font-weight: 500; transition: color 0.3s; }
        .nav-links a:hover { color: var(--primary); }

        .hero {
            min-height: 100vh; background: linear-gradient(135deg, var(--accent) 0%, var(--secondary) 50%, #2a2a7a 100%);
            display: flex; align-items: center; justify-content: center; position: relative; padding-top: 80px;
        }
        .hero-content { text-align: center; z-index: 10; padding: 0 20px; }
        .hero-badge {
            display: inline-block; background: rgba(212, 168, 75, 0.2); color: var(--primary);
            padding: 8px 20px; border-radius: 50px; font-size: 0.9rem; margin-bottom: 20px; border: 1px solid var(--primary);
        }
        .hero h1 { font-size: 3.5rem; color: var(--primary); margin-bottom: 10px; font-weight: 800; }
        .hero h2 { font-size: 1.8rem; color: var(--white); font-weight: 400; margin-bottom: 30px; }
        .hero p { font-size: 1.2rem; color: rgba(255,255,255,0.8); max-width: 600px; margin: 0 auto 40px; }

        .btn {
            padding: 15px 40px; border-radius: 50px; font-weight: 600; text-decoration: none;
            transition: all 0.3s; cursor: pointer; border: none; font-size: 1rem; display: inline-block;
        }
        .btn-primary { background: var(--primary); color: var(--accent); }
        .btn-primary:hover { transform: translateY(-3px); box-shadow: 0 10px 30px rgba(212, 168, 75, 0.3); }
        .btn-outline { background: transparent; color: var(--white); border: 2px solid var(--white); margin-left: 15px; }
        .btn-outline:hover { background: var(--white); color: var(--accent); }

        .stats-bar {
            background: var(--white); padding: 40px 5%;
            display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 30px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.1);
        }
        .stat-item { text-align: center; padding: 20px; cursor: pointer; transition: transform 0.3s; border-radius: 10px; }
        .stat-item:hover { transform: translateY(-5px); background: var(--light-gray); }
        .stat-number { font-size: 2.5rem; font-weight: 800; color: var(--secondary); }
        .stat-label { color: var(--gray); font-size: 0.95rem; margin-top: 5px; }

        section { padding: 100px 5%; }
        .section-title { text-align: center; margin-bottom: 60px; }
        .section-title h2 { font-size: 2.5rem; color: var(--secondary); margin-bottom: 15px; }
        .section-title p { color: var(--gray); font-size: 1.1rem; }

        .verticals { background: var(--light-gray); }
        .verticals-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 30px; max-width: 1200px; margin: 0 auto; }
        .vertical-card {
            background: var(--white); border-radius: 20px; padding: 40px 30px; text-align: center;
            cursor: pointer; transition: all 0.3s; border: 2px solid transparent;
        }
        .vertical-card:hover { transform: translateY(-10px); border-color: var(--primary); box-shadow: 0 20px 40px rgba(0,0,0,0.1); }
        .vertical-icon {
            width: 80px; height: 80px; background: linear-gradient(135deg, var(--primary), #c49a3d);
            border-radius: 50%; display: flex; align-items: center; justify-content: center;
            margin: 0 auto 20px; font-size: 2rem; color: var(--white);
        }
        .vertical-card h3 { font-size: 1.4rem; color: var(--secondary); margin-bottom: 15px; }
        .vertical-card p { color: var(--gray); font-size: 0.95rem; line-height: 1.6; }

        .projects-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 25px; max-width: 1200px; margin: 0 auto; }
        .project-card {
            background: var(--white); border-radius: 15px; overflow: hidden;
            box-shadow: 0 5px 20px rgba(0,0,0,0.1); cursor: pointer; transition: all 0.3s;
        }
        .project-card:hover { transform: scale(1.03); box-shadow: 0 15px 40px rgba(0,0,0,0.15); }
        .project-header {
            height: 120px; background: linear-gradient(135deg, var(--secondary), var(--accent));
            display: flex; align-items: center; justify-content: center; position: relative;
        }
        .project-header i { font-size: 2.5rem; color: var(--primary); }
        .project-badge {
            position: absolute; top: 15px; right: 15px; background: var(--primary);
            color: var(--accent); padding: 5px 12px; border-radius: 20px; font-weight: 700; font-size: 0.8rem;
        }
        .project-info { padding: 20px; }
        .project-info h4 { color: var(--secondary); font-size: 1.1rem; margin-bottom: 8px; }
        .project-info p { color: var(--gray); font-size: 0.85rem; }

        .financials { background: linear-gradient(135deg, var(--accent), var(--secondary)); color: var(--white); }
        .financials .section-title h2 { color: var(--primary); }
        .financials .section-title p { color: rgba(255,255,255,0.7); }
        .financial-cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 25px; max-width: 1000px; margin: 0 auto; }
        .financial-card {
            background: rgba(255,255,255,0.1); backdrop-filter: blur(10px); border-radius: 20px;
            padding: 30px; text-align: center; cursor: pointer; transition: all 0.3s; border: 1px solid rgba(255,255,255,0.1);
        }
        .financial-card:hover { background: rgba(255,255,255,0.15); transform: translateY(-5px); }
        .financial-card h4 { color: var(--primary); font-size: 0.9rem; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1px; }
        .financial-card .amount { font-size: 2rem; font-weight: 800; margin-bottom: 5px; }
        .financial-card p { color: rgba(255,255,255,0.6); font-size: 0.85rem; }

        .team-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 30px; max-width: 1000px; margin: 0 auto; }
        .team-card {
            text-align: center; padding: 30px; background: var(--white); border-radius: 20px;
            box-shadow: 0 5px 20px rgba(0,0,0,0.08); cursor: pointer; transition: all 0.3s;
        }
        .team-card:hover { transform: translateY(-10px); box-shadow: 0 20px 40px rgba(0,0,0,0.12); }
        .team-avatar {
            width: 100px; height: 100px; background: linear-gradient(135deg, var(--primary), #c49a3d);
            border-radius: 50%; margin: 0 auto 20px; display: flex; align-items: center; justify-content: center;
            font-size: 2.5rem; color: var(--white);
        }
        .team-card h4 { color: var(--secondary); font-size: 1.2rem; margin-bottom: 5px; }
        .team-card .role { color: var(--primary); font-size: 0.9rem; font-weight: 600; margin-bottom: 15px; }
        .team-card p { color: var(--gray); font-size: 0.85rem; line-height: 1.6; }

        .investment { background: var(--light-gray); }
        .investment-content { display: grid; grid-template-columns: 1fr 1fr; gap: 50px; align-items: center; max-width: 1100px; margin: 0 auto; }
        .investment-details h3 { font-size: 2rem; color: var(--secondary); margin-bottom: 20px; }
        .investment-details p { color: var(--gray); line-height: 1.8; margin-bottom: 30px; }
        .use-of-funds { background: var(--white); border-radius: 20px; padding: 40px; box-shadow: 0 10px 40px rgba(0,0,0,0.1); }
        .fund-item { display: flex; align-items: center; margin-bottom: 20px; padding: 10px; border-radius: 10px; transition: background 0.3s; }
        .fund-item:hover { background: var(--light-gray); }
        .fund-bar { flex: 1; height: 12px; background: #e9ecef; border-radius: 10px; margin: 0 15px; overflow: hidden; }
        .fund-bar-fill { height: 100%; background: linear-gradient(90deg, var(--primary), #c49a3d); border-radius: 10px; }
        .fund-label { min-width: 120px; font-weight: 600; color: var(--secondary); }
        .fund-amount { min-width: 80px; text-align: right; font-weight: 700; color: var(--primary); }

        footer { background: var(--accent); color: var(--white); padding: 60px 5% 30px; }
        .footer-content { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 40px; margin-bottom: 40px; max-width: 1200px; margin: 0 auto 40px; }
        .footer-section h4 { color: var(--primary); margin-bottom: 20px; font-size: 1.1rem; }
        .footer-section p { color: rgba(255,255,255,0.7); line-height: 1.8; }
        .footer-bottom { text-align: center; padding-top: 30px; border-top: 1px solid rgba(255,255,255,0.1); color: rgba(255,255,255,0.5); font-size: 0.9rem; }

        .modal-overlay {
            position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7);
            display: flex; align-items: center; justify-content: center; z-index: 2000;
            opacity: 0; visibility: hidden; transition: all 0.3s;
        }
        .modal-overlay.active { opacity: 1; visibility: visible; }
        .modal {
            background: var(--white); border-radius: 20px; max-width: 600px; width: 90%;
            max-height: 80vh; overflow-y: auto; transform: scale(0.8); transition: transform 0.3s;
        }
        .modal-overlay.active .modal { transform: scale(1); }
        .modal-header { background: linear-gradient(135deg, var(--secondary), var(--accent)); padding: 30px; color: var(--white); position: relative; }
        .modal-header h3 { font-size: 1.5rem; color: var(--primary); margin-bottom: 10px; }
        .modal-header p { color: rgba(255,255,255,0.8); }
        .modal-close {
            position: absolute; top: 20px; right: 20px; width: 40px; height: 40px;
            background: rgba(255,255,255,0.1); border: none; border-radius: 50%;
            color: var(--white); font-size: 1.2rem; cursor: pointer;
        }
        .modal-body { padding: 30px; }
        .modal-body ul { list-style: none; }
        .modal-body li { padding: 12px 0; border-bottom: 1px solid #eee; display: flex; align-items: flex-start; gap: 12px; }
        .modal-body li i { color: var(--primary); margin-top: 3px; }

        @media (max-width: 768px) {
            .hero h1 { font-size: 2.5rem; }
            .hero h2 { font-size: 1.3rem; }
            .nav-links { display: none; }
            .investment-content { grid-template-columns: 1fr; }
            .stat-number { font-size: 2rem; }
        }
    </style>
</head>
<body>
    <nav>
        <div class="logo">${company_name.split(' ')[0]} <span>${company_name.split(' ').slice(1).join(' ') || ''}</span></div>
        <ul class="nav-links">
            <li><a href="#verticals">Verticals</a></li>
            <li><a href="#projects">Portfolio</a></li>
            <li><a href="#financials">Financials</a></li>
            <li><a href="#team">Team</a></li>
            <li><a href="#investment">Investment</a></li>
        </ul>
    </nav>

    <section class="hero">
        <div class="hero-content">
            ${investment.amount_seeking ? `<div class="hero-badge">Seeking ${investment.amount_seeking} Investment</div>` : ''}
            <h1>${company_name}</h1>
            <h2>${tagline}</h2>
            ${vision ? `<p>${vision}</p>` : ''}
            <div>
                <button class="btn btn-primary" onclick="openModal('overview')">View Overview</button>
                <button class="btn btn-outline" onclick="openModal('vision')">Our Vision</button>
            </div>
        </div>
    </section>

    ${key_metrics.length > 0 ? `
    <div class="stats-bar">
        ${key_metrics.map((m: any) => `
        <div class="stat-item" onclick="openModal('metric-${key_metrics.indexOf(m)}')">
            <div class="stat-number">${m.value}</div>
            <div class="stat-label">${m.label}</div>
        </div>
        `).join('')}
    </div>
    ` : ''}

    ${business_verticals.length > 0 ? `
    <section class="verticals" id="verticals">
        <div class="section-title">
            <h2>Our Business Verticals</h2>
            <p>Building value across multiple sectors</p>
        </div>
        <div class="verticals-grid">
            ${business_verticals.map((v: any, i: number) => `
            <div class="vertical-card" onclick="openModal('vertical-${i}')">
                <div class="vertical-icon"><i class="fas fa-${v.icon || 'building'}"></i></div>
                <h3>${v.name}</h3>
                <p>${v.description}</p>
            </div>
            `).join('')}
        </div>
    </section>
    ` : ''}

    ${products_or_projects.length > 0 ? `
    <section id="projects">
        <div class="section-title">
            <h2>Our Portfolio</h2>
            <p>Track record of successful projects</p>
        </div>
        <div class="projects-grid">
            ${products_or_projects.map((p: any, i: number) => `
            <div class="project-card" onclick="openModal('project-${i}')">
                <div class="project-header">
                    <i class="fas fa-${p.type?.toLowerCase().includes('film') ? 'film' : 'project-diagram'}"></i>
                    ${p.metrics?.roi ? `<div class="project-badge">${p.metrics.roi} ROI</div>` : ''}
                </div>
                <div class="project-info">
                    <h4>${p.name}</h4>
                    <p>${p.year || ''} ${p.type ? `| ${p.type}` : ''}</p>
                </div>
            </div>
            `).join('')}
        </div>
    </section>
    ` : ''}

    ${financials.projected_revenue?.length > 0 ? `
    <section class="financials" id="financials">
        <div class="section-title">
            <h2>Financial Projections</h2>
            <p>Revenue growth trajectory</p>
        </div>
        <div class="financial-cards">
            ${financials.projected_revenue.map((f: any) => `
            <div class="financial-card">
                <h4>${f.year}</h4>
                <div class="amount">${f.amount}</div>
                ${f.growth ? `<p>Growth: ${f.growth}</p>` : ''}
            </div>
            `).join('')}
        </div>
    </section>
    ` : ''}

    ${team.length > 0 ? `
    <section id="team">
        <div class="section-title">
            <h2>Leadership Team</h2>
            <p>Experienced professionals driving growth</p>
        </div>
        <div class="team-grid">
            ${team.map((t: any, i: number) => `
            <div class="team-card" onclick="openModal('team-${i}')">
                <div class="team-avatar"><i class="fas fa-user"></i></div>
                <h4>${t.name}</h4>
                <div class="role">${t.role}</div>
                <p>${t.description?.slice(0, 100) || ''}${t.description?.length > 100 ? '...' : ''}</p>
            </div>
            `).join('')}
        </div>
    </section>
    ` : ''}

    ${investment.amount_seeking ? `
    <section class="investment" id="investment">
        <div class="section-title">
            <h2>Investment Opportunity</h2>
            <p>Join us in building the future</p>
        </div>
        <div class="investment-content">
            <div class="investment-details">
                <h3>${investment.amount_seeking} Capital Raise</h3>
                <p>${investment.investment_highlights?.join('. ') || 'Strategic investment opportunity with strong growth potential.'}</p>
                <button class="btn btn-primary" onclick="openModal('investment-details')">View Details</button>
            </div>
            ${investment.use_of_funds?.length > 0 ? `
            <div class="use-of-funds">
                <h4 style="margin-bottom: 25px; color: var(--secondary);">Use of Proceeds</h4>
                ${investment.use_of_funds.map((f: any) => `
                <div class="fund-item">
                    <span class="fund-label">${f.category}</span>
                    <div class="fund-bar">
                        <div class="fund-bar-fill" style="width: ${f.percentage || '25%'};"></div>
                    </div>
                    <span class="fund-amount">${f.amount}</span>
                </div>
                `).join('')}
            </div>
            ` : ''}
        </div>
    </section>
    ` : ''}

    <footer>
        <div class="footer-content">
            <div class="footer-section">
                <h4>About ${company_name}</h4>
                <p>${vision || tagline}</p>
            </div>
            ${contact.email || contact.website ? `
            <div class="footer-section">
                <h4>Contact</h4>
                <p>
                    ${contact.email ? `Email: ${contact.email}<br>` : ''}
                    ${contact.website ? `Website: ${contact.website}<br>` : ''}
                    ${contact.address || ''}
                </p>
            </div>
            ` : ''}
            <div class="footer-section">
                <h4>Disclaimer</h4>
                <p>This information is for qualified investors only. Forward-looking statements are subject to risks.</p>
            </div>
        </div>
        <div class="footer-bottom">
            <p>&copy; ${year} ${company_name}. All rights reserved.</p>
        </div>
    </footer>

    <div class="modal-overlay" id="modalOverlay" onclick="closeModal(event)">
        <div class="modal" onclick="event.stopPropagation()">
            <div class="modal-header">
                <h3 id="modalTitle">Title</h3>
                <p id="modalSubtitle">Subtitle</p>
                <button class="modal-close" onclick="closeModal()"><i class="fas fa-times"></i></button>
            </div>
            <div class="modal-body" id="modalBody"></div>
        </div>
    </div>

    <script>
        const modalData = {
            'overview': {
                title: 'Company Overview',
                subtitle: '${company_name}',
                content: \`<ul>
                    ${vision ? `<li><i class="fas fa-eye"></i> <strong>Vision:</strong> ${vision}</li>` : ''}
                    ${key_metrics.map((m: any) => `<li><i class="fas fa-chart-line"></i> <strong>${m.label}:</strong> ${m.value}</li>`).join('')}
                </ul>\`
            },
            'vision': {
                title: 'Our Vision',
                subtitle: 'Building the Future',
                content: '<p style="font-size: 1.1rem; line-height: 1.8;">${vision || tagline}</p>'
            },
            ${key_metrics.map((m: any, i: number) => `'metric-${i}': {
                title: '${m.label}',
                subtitle: '${m.value}',
                content: '<p>${m.detail || m.label}</p>'
            }`).join(',\n            ')},
            ${business_verticals.map((v: any, i: number) => `'vertical-${i}': {
                title: '${v.name}',
                subtitle: '${v.description?.slice(0, 50) || ''}',
                content: \`<p style="margin-bottom: 15px;">${v.description}</p>
                ${v.highlights?.length ? `<ul>${v.highlights.map((h: string) => `<li><i class="fas fa-check-circle"></i> ${h}</li>`).join('')}</ul>` : ''}\`
            }`).join(',\n            ')},
            ${products_or_projects.map((p: any, i: number) => `'project-${i}': {
                title: '${p.name}',
                subtitle: '${p.year || ''} | ${p.type || ''}',
                content: \`<p style="margin-bottom: 15px;">${p.description || ''}</p>
                ${p.metrics ? `<ul>
                    ${p.metrics.budget ? `<li><i class="fas fa-money-bill"></i> <strong>Budget:</strong> ${p.metrics.budget}</li>` : ''}
                    ${p.metrics.revenue ? `<li><i class="fas fa-chart-line"></i> <strong>Revenue:</strong> ${p.metrics.revenue}</li>` : ''}
                    ${p.metrics.roi ? `<li><i class="fas fa-percentage"></i> <strong>ROI:</strong> ${p.metrics.roi}</li>` : ''}
                </ul>` : ''}\`
            }`).join(',\n            ')},
            ${team.map((t: any, i: number) => `'team-${i}': {
                title: '${t.name}',
                subtitle: '${t.role}',
                content: '<p>${t.description || ''}</p>'
            }`).join(',\n            ')},
            'investment-details': {
                title: 'Investment Details',
                subtitle: '${investment.amount_seeking || ''}',
                content: \`<ul>
                    ${investment.investment_highlights?.map((h: string) => `<li><i class="fas fa-check-circle"></i> ${h}</li>`).join('') || ''}
                </ul>\`
            }
        };

        function openModal(id) {
            const data = modalData[id];
            if (!data) return;
            document.getElementById('modalTitle').textContent = data.title;
            document.getElementById('modalSubtitle').textContent = data.subtitle;
            document.getElementById('modalBody').innerHTML = data.content;
            document.getElementById('modalOverlay').classList.add('active');
        }

        function closeModal(e) {
            if (e && e.target !== e.currentTarget) return;
            document.getElementById('modalOverlay').classList.remove('active');
        }

        document.addEventListener('keydown', (e) => { if (e.key === 'Escape') closeModal(); });
    </script>
</body>
</html>`
}

// Deploy to Vercel
async function deployToVercel(html: string, projectName: string): Promise<string> {
  const token = process.env.VERCEL_TOKEN

  if (!token) {
    throw new Error('Vercel token not configured')
  }

  // Create deployment with files
  const files = [
    {
      file: 'index.html',
      data: Buffer.from(html).toString('base64'),
      encoding: 'base64'
    }
  ]

  const deployResponse = await fetch('https://api.vercel.com/v13/deployments', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${token}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      name: projectName,
      files: files,
      projectSettings: {
        framework: null,
      },
      target: 'production',
    }),
  })

  if (!deployResponse.ok) {
    const error = await deployResponse.text()
    throw new Error(`Vercel deployment failed: ${error}`)
  }

  const deployment = await deployResponse.json()

  // Wait for deployment to be ready
  let deploymentUrl = deployment.url
  let attempts = 0
  const maxAttempts = 30

  while (attempts < maxAttempts) {
    const statusResponse = await fetch(`https://api.vercel.com/v13/deployments/${deployment.id}`, {
      headers: {
        'Authorization': `Bearer ${token}`,
      },
    })

    const statusData = await statusResponse.json()

    if (statusData.readyState === 'READY') {
      deploymentUrl = statusData.url
      break
    }

    if (statusData.readyState === 'ERROR') {
      throw new Error('Deployment failed')
    }

    await new Promise(resolve => setTimeout(resolve, 2000))
    attempts++
  }

  return `https://${deploymentUrl}`
}

export async function POST(request: NextRequest) {
  const { stream, send, close } = createStream()

  // Process in background
  ;(async () => {
    try {
      // Parse form data
      const formData = await request.formData()
      const file = formData.get('pdf') as File
      const projectName = formData.get('projectName') as string

      if (!file || !projectName) {
        send({ error: 'Missing PDF file or project name' })
        close()
        return
      }

      send({ status: 'parsing' })

      // Read file buffer
      const buffer = Buffer.from(await file.arrayBuffer())

      // Parse PDF
      const pdfText = await parsePDF(buffer)

      if (!pdfText || pdfText.trim().length < 100) {
        send({ error: 'Could not extract text from PDF. Please ensure it contains readable text.' })
        close()
        return
      }

      send({ status: 'analyzing' })

      // Analyze with Claude
      const structuredData = await analyzeWithClaude(pdfText)

      send({ status: 'generating' })

      // Generate landing page
      const html = generateLandingPage(structuredData)

      send({ status: 'deploying' })

      // Deploy to Vercel
      const url = await deployToVercel(html, projectName)

      send({ status: 'complete', url })
    } catch (error) {
      console.error('Error:', error)
      send({ error: error instanceof Error ? error.message : 'An error occurred' })
    } finally {
      close()
    }
  })()

  return new Response(stream, {
    headers: {
      'Content-Type': 'text/plain; charset=utf-8',
      'Transfer-Encoding': 'chunked',
    },
  })
}
