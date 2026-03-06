export function getPromptTemplate(userCustomization: string): string {
  return `You are an elite web designer who creates stunning, award-winning, TRULY INTERACTIVE investor landing pages.

Analyze the attached PDF pitch deck thoroughly. Extract:
- Company name, tagline, and value proposition
- Key financial metrics, growth numbers, and traction data
- Team information and credentials
- Product/service description and screenshots
- Market opportunity and TAM/SAM/SOM
- Investment highlights and competitive advantages
- Any brand colors, logos, or visual identity cues
- All data points, tables, percentages, and breakdowns from the PDF

Then generate a SINGLE, COMPLETE, SELF-CONTAINED HTML file for a premium investor landing page.

CRITICAL REQUIREMENTS:
1. The HTML must be a SINGLE file with ALL CSS and JavaScript inlined/embedded
2. Load ONLY these CDN resources in the <head>:
   - <script src="https://cdn.tailwindcss.com"></script>
   - <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js"></script>
   - <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/ScrollTrigger.min.js"></script>
   - <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
   - <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&family=Playfair+Display:wght@400;500;600;700;800&display=swap" rel="stylesheet">

3. DESIGN MUST BE STUNNING AND ULTRA-MODERN:
   - Dark premium theme (backgrounds: #080C14, #0D1320, #131B2E)
   - Gradient accents using brand colors extracted from the PDF (fallback: cyan #00D4FF and emerald #00E89D)
   - Glassmorphism cards (backdrop-filter:blur(20px), semi-transparent backgrounds, subtle borders with rgba color at 0.08-0.15 opacity)
   - Gradient text effects for headings (background:linear-gradient, -webkit-background-clip:text)
   - Subtle animated grid or dot pattern backgrounds
   - Smooth, professional micro-interactions on ALL clickable elements

4. *** INTERACTIVE FEATURES — THIS IS THE MOST IMPORTANT REQUIREMENT ***
   The page MUST be genuinely interactive, NOT just scroll animations. Every data point should be explorable:

   a) MODAL POPUP SYSTEM:
      - Create a full-screen modal overlay (fixed, inset:0, bg: rgba(0,0,0,0.8) with backdrop-blur)
      - Clicking ANY metric card, data point, or chart should open a detailed modal
      - Modal content: deep-dive analysis, additional data, mini charts, key insights bullet points
      - Close via X button, clicking overlay background, or Escape key
      - Each modal should have UNIQUE, DETAILED content specific to that metric
      - Include at least 8-12 different modal content definitions

   b) CHART.JS INTERACTIVE CHARTS:
      - Use Chart.js (already loaded via CDN) for ALL data visualizations
      - Donut/pie charts for market share, funding sources, revenue breakdowns
      - Horizontal bar charts for comparisons (e.g., top markets, competitors, facility types)
      - Line charts for historical trends, growth trajectories
      - ALL charts must have: hover tooltips with formatted values, click handlers that open modals, responsive sizing
      - Style charts to match dark theme: grid colors rgba(255,255,255,0.03), label colors #94a3b8/#e2e8f0, tooltip bg #0D1320 with border rgba(0,212,255,0.3)

   c) TAB NAVIGATION SYSTEM:
      - For complex data sections, add horizontal tab buttons (styled as pill/rounded buttons)
      - Each tab reveals different content panel (e.g., "Funding Sources", "Market Breakdown", "Historical Data", "Key Points")
      - Active tab: highlighted with brand color background + border
      - Lazy-initialize charts only when their tab is first activated
      - Pass the button element (this) in onclick: onclick="switchTab('name', this)"

   d) EXPANDABLE/COLLAPSIBLE ACCORDION SECTIONS:
      - Data items should be clickable to expand detailed descriptions
      - Use max-height CSS transition (0 → 600px) for smooth animation
      - Include a chevron icon that rotates 180deg when expanded
      - Each expanded section reveals 2-3 sentences of detailed analysis

   e) INTERACTIVE DATA TABLES:
      - Tabular data should have hover:bg highlighting on rows
      - Clickable rows that open modal with more detail
      - Clean styling with subtle border separators

   f) TOGGLE/SWITCH CONTROLS:
      - Where applicable, add toggle buttons (e.g., different time periods, scenarios)
      - Clicking toggles should update displayed values dynamically via JavaScript

   g) CLICKABLE CARD STYLING:
      - All clickable cards must have: cursor:pointer, translateY(-4px) on hover, border color change on hover, box-shadow glow on hover
      - Add a subtle "Click to explore →" hint that appears on hover (use ::after pseudo-element, opacity transition)

5. SCROLL ANIMATIONS (use GSAP + ScrollTrigger):
   - Hero: Timeline with staggered text reveal using gsap.from() on individual spans
   - Stats/Metrics: Animated number counters (gsap.to with onUpdate callback)
   - All sections: Use .section-reveal class with opacity:0, translateY(40px) initial state
   - Animate with ScrollTrigger: start:'top 85%', toggleActions:'play none none none'
   - Stagger delays: delay: i % 3 * 0.1 for grid items
   - Navigation background: transparent → solid on scroll using ScrollTrigger.create

6. SECTIONS (include all that are relevant based on PDF content):
   - Fixed navigation bar with brand logo, section links, and CTA button
   - Full-viewport hero with animated grid background, staggered title, stats highlight, dual CTAs
   - Key Metrics bar: 3-4 clickable metric cards in a grid, each opens a modal with deep-dive
   - Market Overview with TAB navigation (4 tabs minimum) containing charts + expandable details
   - TAM/Market Sizing section with interactive scenario toggles (e.g., different time cycles)
   - Industry Analysis with clickable cards that open modals with charts
   - Competitive Landscape / Opportunity section with expandable items
   - Contact / CTA footer section
   - Floating "Schedule a Call" button (fixed bottom-right, gradient bg, z-index:90)

7. TYPOGRAPHY:
   - Headings: 'Playfair Display', serif — use for section titles
   - Body: 'Inter', sans-serif — use for everything else
   - Use font weights strategically (300 for subtle, 600-700 for labels, 800-900 for hero)

8. RESPONSIVE: Must work perfectly on mobile, tablet, and desktop (use Tailwind responsive prefixes)

9. DATA FIDELITY:
   - Extract EVERY number, percentage, data point, and fact from the PDF
   - Populate all charts with REAL data from the PDF, not placeholder data
   - Modal content should contain detailed analysis based on the PDF's actual content
   - If the PDF has tables, reproduce them as interactive HTML tables

ADDITIONAL USER INSTRUCTIONS:
${userCustomization}

OUTPUT: Return ONLY the complete HTML file starting with <!DOCTYPE html>. No explanations, no markdown code fences, no commentary. Just the raw HTML.`;
}

export function getDefaultPrompt(): string {
  return \`Create a premium, dark-themed investor landing page that is TRULY INTERACTIVE — not just scroll animations.

Key priorities:
- Every metric card and data point must be CLICKABLE, opening a detailed modal popup with deep-dive analysis
- Include Chart.js interactive charts (donut, bar, line) with hover tooltips — at least 3-4 charts
- Add a TAB navigation system for the main data section with at least 4 tabs
- Include EXPANDABLE accordion sections for detailed breakdowns
- Add toggle/switch controls where different scenarios or time periods can be compared
- Use glassmorphism-styled clickable cards with hover effects ("Click to explore →" hints)
- Hero section should be striking with gradient text and staggered GSAP reveals
- All numbers/metrics should have counting animations when scrolled into view
- Include interactive data tables with hover highlighting and clickable rows
- Floating "Schedule a Call" CTA button fixed at bottom-right
- Extract and display EVERY data point from the PDF — no data should be left out
- The page should feel like a premium, institutional-grade investment presentation that investors can explore deeply\`;
}
