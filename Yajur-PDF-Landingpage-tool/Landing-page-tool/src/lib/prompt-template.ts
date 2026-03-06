import type { ResearchMode } from "@/lib/types";

/**
 * This prompt is used in Pass 2 of the two-pass generation.
 * Pass 1 already extracted all PDF data as structured JSON.
 * This prompt tells Claude HOW to build the HTML using that data.
 */
export function getPromptTemplate(
  userCustomization: string,
  mode: ResearchMode = "pdf_only"
): string {
  const coreInstructions = `Generate a SINGLE, COMPLETE, SELF-CONTAINED HTML file — a premium investor landing page using the extracted data provided above.

CDN RESOURCES — Include ALL of these in <head>:
<script src="https://cdn.tailwindcss.com"><\/script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/gsap.min.js"><\/script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.5/ScrollTrigger.min.js"><\/script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"><\/script>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
<link href="https://unpkg.com/lucide-static@latest/font/lucide.min.css" rel="stylesheet">

TAILWIND CONFIG — Add this right after the tailwindcss script tag:
<script>
tailwind.config = {
  theme: {
    extend: {
      fontFamily: { sans: ['Inter', 'system-ui', 'sans-serif'] },
    }
  }
}
<\/script>

═══════════════════════════════════
VISUAL DESIGN SYSTEM (CRITICAL)
═══════════════════════════════════

COLOR STRATEGY:
- Use the brand_primary_color and brand_secondary_color from the extracted data as the PRIMARY palette
- Create a gradient from primary → secondary for hero backgrounds, buttons, and accent elements
- If no brand colors found, use: primary=#6366F1 (indigo), secondary=#8B5CF6 (violet)
- Background: Use a deep dark base (#0A0F1C or #080C14) — NOT pure black
- Cards: Use rgba(255,255,255,0.04) with border rgba(255,255,255,0.08) and backdrop-blur-xl
- Text: White (#FFFFFF) for headings, #94A3B8 (slate-400) for body text, #64748B for muted

GLASSMORPHISM CARDS (use this exact pattern):
  background: rgba(255, 255, 255, 0.04);
  backdrop-filter: blur(20px);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 16px;

GRADIENT RECIPES:
- Hero background: radial-gradient(ellipse at 30% 0%, {primary}15 0%, transparent 50%), radial-gradient(ellipse at 70% 0%, {secondary}10 0%, transparent 50%)
- CTA buttons: linear-gradient(135deg, {primary}, {secondary})
- Section title accent: linear-gradient(90deg, {primary}, {secondary}) with background-clip: text
- Stat value numbers: gradient text using the brand colors
- Glowing orbs: position:absolute circles with brand colors at 10-20% opacity, blur(100px), for ambient lighting

TYPOGRAPHY:
- Page title / Hero heading: text-5xl md:text-7xl font-black, gradient text or white
- Section headings: text-3xl md:text-4xl font-bold text-white
- Section subtitles: text-lg text-slate-400 max-w-2xl
- Card titles: text-lg font-semibold text-white
- Body text: text-sm md:text-base text-slate-400 leading-relaxed
- Stat numbers: text-3xl md:text-4xl font-bold with gradient text
- Small labels: text-xs uppercase tracking-wider text-slate-500

SPACING & LAYOUT:
- Sections: py-20 md:py-32 px-6 — generous vertical spacing
- Max width container: max-w-7xl mx-auto
- Card grid: grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6
- Card padding: p-6 md:p-8
- Section gap from heading to content: mb-12 md:mb-16

HOVER & MICRO-INTERACTIONS (CSS only, applied to every card):
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  &:hover { transform: translateY(-4px); border-color: rgba({primary-rgb}, 0.3); box-shadow: 0 20px 40px rgba(0,0,0,0.3), 0 0 30px rgba({primary-rgb}, 0.1); }

═══════════════════════════════════
PAGE SECTIONS (use ALL extracted data)
═══════════════════════════════════

1. NAVIGATION:
   - Fixed/sticky top, glass background (bg-[#0A0F1C]/80 backdrop-blur-xl), z-50
   - Company name on left, smooth-scroll section links on right
   - Mobile hamburger menu (JS toggle)

2. HERO SECTION:
   - Full viewport height (min-h-screen), centered content
   - Ambient glow orbs (2-3 large blurred circles with brand colors at 10-15% opacity, absolute positioned)
   - Company name: huge gradient text (text-5xl md:text-7xl font-black)
   - Tagline below in text-xl text-slate-300
   - Hero stats in a row of glass cards (3-4 key metrics from hero_stats)
   - Two CTA buttons: primary (gradient bg) + secondary (glass outline)
   - Subtle animated grid or dot pattern in background (CSS only)

3. KEY METRICS — Use ALL items from key_metrics array:
   - Grid of glass cards, each showing: icon area, large number (gradient text), label, trend indicator
   - GSAP number counter animation (count from 0 to target value on scroll)
   - Make each card clickable → opens a modal with more detail

4. ABOUT / OVERVIEW — Use description field:
   - Two-column layout: text left, feature highlights right
   - Use product_features data for highlight cards

5. MARKET OPPORTUNITY — Use market_opportunity data:
   - Chart.js DOUGHNUT chart for TAM/SAM/SOM (use brand colors with transparency)
   - Chart styling: dark background, white labels, brand color segments
   - Display data_points as metric cards beside the chart

6. PRODUCT / FEATURES — Use product_features array:
   - Feature cards in grid with icon, title, description
   - Use Lucide icon classes for feature icons (e.g., <i class="lucide-zap"></i>)

7. TRACTION — Use traction data:
   - Chart.js LINE or BAR chart for growth_data (gradient fill under line)
   - Timeline of milestones (vertical timeline with dots and lines)

8. TEAM — Use team array:
   - Cards with avatar placeholder (gradient circle with initials), name, role, bio
   - Glass card style with hover effect

9. COMPETITIVE ADVANTAGES — Use competitive_advantages array:
   - Cards or comparison grid
   - Use checkmark icons, brand color accents

10. FINANCIALS — Use financials data:
    - Chart.js BAR chart for projections (gradient bars)
    - Key financial data_points as glass stat cards

11. FOOTER / CTA:
    - Final call-to-action section with gradient background
    - Contact info from contact data
    - Company name, links to sections

═══════════════════════════════════
CHART.JS STYLING (CRITICAL FOR QUALITY)
═══════════════════════════════════

All charts MUST use this styling approach:
- Dark background: set Chart.defaults.color = '#94A3B8' and Chart.defaults.borderColor = 'rgba(255,255,255,0.06)'
- Use brand colors with transparency for fills: e.g., rgba(primary, 0.2) for area fills
- Gradient fills for bar/line charts (use canvas createLinearGradient)
- Rounded bars: borderRadius: 8
- Remove grid lines or make them very subtle (rgba(255,255,255,0.04))
- Larger font sizes: Chart.defaults.font.size = 13, font.family = 'Inter'
- Doughnut charts: cutout: '65%', with total value displayed in center using a plugin
- Responsive: true, maintainAspectRatio: false
- Wrap each chart in a container with a fixed height (h-[300px] or h-[400px])

═══════════════════════════════════
GSAP ANIMATIONS
═══════════════════════════════════

Add this at the end of <body> in a <script> tag:
- Register ScrollTrigger plugin
- Animate all sections: fade in from y:40, opacity:0, stagger children
- Number counters: use GSAP to count from 0 to target number on scroll into view
  - For elements with data-count="1234", animate innerText from 0→1234
  - Format with commas, handle decimals, add prefix ($) or suffix (%) from data attributes
- Stagger card appearances: each card in a grid fades in 0.1s after the previous
- Chart canvas elements: trigger chart rendering when scrolled into view (IntersectionObserver)

═══════════════════════════════════
MODAL SYSTEM
═══════════════════════════════════

- Create a reusable modal with JS: showModal(title, content) function
- Fixed overlay with bg-black/60 backdrop-blur-sm
- Modal box: max-w-lg, glass card style, p-8
- Close on X button, Escape key, and backdrop click
- Add modals to key metric cards (onclick) showing more detail from the extracted data
- Smooth fade-in animation (opacity + scale)

═══════════════════════════════════
CRITICAL QUALITY RULES
═══════════════════════════════════

1. USE EVERY PIECE OF DATA from the extracted JSON. If there are 30 data points, all 30 must appear.
2. Charts use REAL numbers from the data — never placeholder or example data.
3. Skip sections with no data rather than showing empty sections.
4. Brand colors must be VISIBLE — use them in gradients, buttons, chart colors, stat numbers, and accents.
5. The page must feel PREMIUM and DATA-RICH — like a YC Demo Day or Stripe-quality presentation.
6. Every section must have substantial content — not just a title and one sentence.
7. Mobile responsive: all grids collapse to single column, text sizes adjust.
8. NO placeholder images — use CSS gradients, icons, and data visualizations instead.
9. ALL JavaScript must be in <script> tags at the end of body — no inline onclick except for modals.
10. The complete HTML MUST start with <!DOCTYPE html> and end with </html>.`;

  if (mode === "pdf_only") {
    return `${coreInstructions}

MODE: Use ONLY the extracted data provided above. Do not fabricate any numbers, metrics, or facts. Every piece of information must come from the extracted data. If data for a section is missing, skip it entirely.

USER INSTRUCTIONS:
${userCustomization}

OUTPUT: Return ONLY the complete HTML starting with <!DOCTYPE html>. No explanations, no code fences, no markdown.`;
  }

  // Deep Research mode
  return `${coreInstructions}

MODE: DEEP RESEARCH — Use the extracted data as the primary source. Additionally, use your web search tool to:
- Find latest industry trends and market data for this sector
- Research competitor landscape and positioning
- Gather industry benchmarks to contextualize the company's metrics
- Check for recent news about the company (if public)
Add a "Market Intelligence" section with researched insights. Include source attributions. Never contradict the PDF data.

USER INSTRUCTIONS:
${userCustomization}

OUTPUT: Return ONLY the complete HTML starting with <!DOCTYPE html>. No explanations, no code fences, no markdown.`;
}

export function getDefaultPrompt(mode: ResearchMode = "pdf_only"): string {
  if (mode === "pdf_only") {
    return `Create a stunning, premium investor landing page. Use the company's brand colors prominently throughout. Include interactive Chart.js charts, GSAP scroll animations with number counters, clickable metric cards with detail modals, and a polished glass-morphism design. Make it data-rich — display every metric, fact, and number from the PDF.`;
  }

  return `Create a stunning, premium investor landing page with deep research. Use the company's brand colors prominently. Include interactive Chart.js charts, GSAP scroll animations, clickable metrics with modals, and glass-morphism design. Supplement PDF data with web-researched industry trends, competitor insights, and market benchmarks. Add a Market Intelligence section.`;
}
