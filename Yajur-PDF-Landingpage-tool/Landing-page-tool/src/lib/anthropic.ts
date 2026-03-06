import Anthropic from "@anthropic-ai/sdk";
import { createAdminClient } from "@/lib/supabase/admin";
import { getPromptTemplate } from "./prompt-template";
import type { ResearchMode } from "@/lib/types";

const client = new Anthropic({
  apiKey: process.env.ANTHROPIC_API_KEY!,
});

// ═══════════════════════════════════════════════
// Quality Check Types
// ═══════════════════════════════════════════════

interface QualityCheckResult {
  passed: boolean;
  score: number; // 0-100
  issues: string[];
  suggestions: string[];
  checks: {
    name: string;
    passed: boolean;
    detail: string;
  }[];
}

// ═══════════════════════════════════════════════
// MAIN: Three-pass generation pipeline
// Pass 1: Extract data from PDF → JSON
// Pass 2: Generate interactive HTML
// Pass 3: Quality verification & auto-fix
// ═══════════════════════════════════════════════

export async function analyzePdfAndGenerate(
  pdfStoragePath: string,
  userPrompt: string,
  researchMode: ResearchMode = "pdf_only"
): Promise<string> {
  // Download PDF from Supabase Storage
  const supabase = createAdminClient();
  const { data: pdfData, error: downloadError } = await supabase.storage
    .from("yajur-pdfs")
    .download(pdfStoragePath);

  if (downloadError || !pdfData) {
    throw new Error(
      `Failed to download PDF: ${downloadError?.message || "No data returned"}`
    );
  }

  const pdfBuffer = Buffer.from(await pdfData.arrayBuffer());
  const pdfBase64 = pdfBuffer.toString("base64");

  // ============================================
  // PASS 1: Extract ALL data from PDF as JSON
  // ============================================
  console.log("Pass 1: Extracting data from PDF...");

  const extractionPrompt = `Analyze this PDF pitch deck thoroughly and extract ALL information into a structured JSON format.

Extract EVERYTHING — do not skip any data, number, percentage, or fact from the PDF.

Return a JSON object with these keys (include ALL that exist in the PDF):
{
  "company_name": "",
  "tagline": "",
  "description": "",
  "logo_description": {
    "shape": "describe the logo shape (circle, square, abstract, text-only, icon+text, etc.)",
    "text_in_logo": "any text visible in the logo",
    "visual_elements": "describe visual elements (arrows, globe, shield, etc.)",
    "colors_used": ["#hex1", "#hex2"],
    "style": "modern/minimal/bold/playful/corporate/etc.",
    "svg_suggestion": "provide a simple SVG markup that approximates the logo using the described colors and shapes"
  },
  "logo_colors": ["#hex1", "#hex2"],
  "brand_primary_color": "#hex (dominant color from logo/visuals)",
  "brand_secondary_color": "#hex (secondary accent color)",
  "brand_gradient": "direction and colors if gradient is used in PDF",
  "hero_stats": [{"label": "", "value": "", "detail": ""}],
  "key_metrics": [{"label": "", "value": "", "description": "", "trend": ""}],
  "market_opportunity": {
    "tam": "", "sam": "", "som": "",
    "description": "",
    "data_points": [{"label": "", "value": ""}]
  },
  "product_features": [{"title": "", "description": "", "icon_suggestion": "lucide icon name"}],
  "traction": {
    "description": "",
    "milestones": [{"label": "", "value": "", "date": ""}],
    "growth_data": [{"period": "", "value": ""}]
  },
  "team": [{"name": "", "role": "", "bio": "", "initials": ""}],
  "competitive_advantages": [{"title": "", "description": ""}],
  "competitors": [{"name": "", "comparison": ""}],
  "financials": {
    "funding": "",
    "revenue": "",
    "projections": [{"period": "", "value": ""}],
    "data_points": [{"label": "", "value": ""}]
  },
  "charts_data": [
    {"title": "", "type": "donut|bar|line", "labels": [], "values": [], "description": ""}
  ],
  "additional_sections": [{"title": "", "content": "", "data_points": []}],
  "contact": {"email": "", "website": "", "phone": "", "address": "", "cta_text": ""},
  "all_tables": [{"title": "", "headers": [], "rows": [[]]}],
  "all_remaining_data": [],
  "total_data_points_extracted": 0
}

CRITICAL:
- For brand_primary_color: Pick the MOST DOMINANT color from the logo/brand visuals. This will be the primary color for the entire landing page.
- For logo_description.svg_suggestion: Create a simple but recognizable SVG that uses the actual brand colors and approximates the logo shape. This SVG will be used in the landing page header.
- Count all data points extracted and put the number in total_data_points_extracted.
- Return ONLY the JSON. No explanations. Include every single piece of data from the PDF.`;

  const extractParams: Anthropic.MessageCreateParams = {
    model: "claude-sonnet-4-20250514",
    max_tokens: 10000,
    temperature: 0,
    system:
      "You are a data extraction specialist with expertise in analyzing pitch decks and brand identity. Extract ALL data from documents into structured JSON. Pay special attention to brand colors, logo details, and every numerical data point. Never skip any data point. Always provide accurate hex color codes from the visual elements you see.",
    messages: [
      {
        role: "user",
        content: [
          {
            type: "document",
            source: {
              type: "base64",
              media_type: "application/pdf",
              data: pdfBase64,
            },
          },
          {
            type: "text",
            text: extractionPrompt,
          },
        ],
      },
    ],
  };

  let extractedData = "";

  try {
    const stream1 = client.messages.stream(extractParams);
    stream1.on("text", (text) => {
      extractedData += text;
    });
    await stream1.finalMessage();
    console.log(
      `Pass 1 complete. Extracted ${extractedData.length} chars of data.`
    );
  } catch (apiError) {
    const errMsg =
      apiError instanceof Error ? apiError.message : String(apiError);
    throw new Error(`Data extraction failed: ${errMsg}`);
  }

  // ============================================
  // PASS 2: Generate interactive HTML
  // ============================================
  console.log("Pass 2: Generating interactive HTML...");

  const htmlPrompt = getPromptTemplate(userPrompt, researchMode);

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const htmlParams: any = {
    model: "claude-sonnet-4-20250514",
    max_tokens: 16384,
    temperature: 0.3,
    system: `You are a world-class frontend developer specializing in premium, data-rich investor landing pages. Your work is comparable to the best startup websites — think Stripe, Linear, Vercel, or YC Demo Day presentations.

You always produce:
- Complete, production-quality, self-contained HTML files
- Beautiful dark-themed designs with glassmorphism and ambient lighting effects
- Rich Chart.js data visualizations with gradient fills and dark styling
- Smooth GSAP scroll animations with number counters
- Interactive elements: clickable cards with detail modals
- Full responsive design (mobile-first with Tailwind)
- Every piece of provided data displayed prominently on the page
- Company logo (as SVG or styled text) in the navigation bar using brand colors

You NEVER produce:
- Skeleton pages with only a hero section
- Pages with placeholder or fake data
- Pages that ignore the brand colors
- Minimal pages with little content
- Pages without charts or interactive elements
- Generic logos — always use the logo_description.svg_suggestion or a styled text logo with brand colors

Your output is ONLY the complete HTML file. No explanations, no markdown, no code fences.`,
    messages: [
      {
        role: "user",
        content: `Here is ALL the extracted data from the company's pitch deck PDF:\n\n\`\`\`json\n${extractedData}\n\`\`\`\n\n---\n\n${htmlPrompt}`,
      },
    ],
  };

  // For deep research mode, enable web search tool
  if (researchMode === "deep_research") {
    htmlParams.tools = [
      {
        type: "web_search_20250305",
        name: "web_search",
        max_uses: 10,
      },
    ];
  }

  let fullText = "";

  try {
    const stream2 = client.messages.stream(htmlParams);
    stream2.on("text", (text) => {
      fullText += text;
    });

    const finalMessage = await stream2.finalMessage();

    if (!fullText || fullText.length < 100) {
      const textBlocks = finalMessage.content.filter(
        (c): c is Anthropic.TextBlock => c.type === "text"
      );
      if (textBlocks.length === 0) {
        throw new Error("No text content in Claude response");
      }
      fullText = textBlocks.map((b) => b.text).join("\n");
    }

    console.log(
      `Pass 2 complete. HTML length: ${fullText.length} chars, Stop reason: ${finalMessage.stop_reason}`
    );

    if (finalMessage.stop_reason === "max_tokens") {
      console.warn(
        "WARNING: HTML output was truncated due to max_tokens limit."
      );
    }
  } catch (apiError) {
    const errMsg =
      apiError instanceof Error ? apiError.message : String(apiError);

    if (errMsg.includes("timeout") || errMsg.includes("ETIMEDOUT")) {
      throw new Error("Claude API timed out. Try with a smaller PDF.");
    }
    if (errMsg.includes("rate_limit") || errMsg.includes("429")) {
      throw new Error(
        "API rate limit reached. Please wait a minute and try again."
      );
    }
    if (errMsg.includes("overloaded") || errMsg.includes("529")) {
      throw new Error(
        "Claude is currently overloaded. Please wait and try again."
      );
    }

    throw new Error(`HTML generation failed: ${errMsg}`);
  }

  // Extract HTML from Pass 2 response
  let html = extractHtml(fullText);

  // ============================================
  // PASS 3: Quality Verification & Auto-Fix
  // ============================================
  console.log("Pass 3: Running quality verification...");

  const qualityCheck = performQualityChecks(html, extractedData);

  console.log(
    `Quality score: ${qualityCheck.score}/100 | Issues: ${qualityCheck.issues.length} | Suggestions: ${qualityCheck.suggestions.length}`
  );

  // Log each check result
  for (const check of qualityCheck.checks) {
    console.log(
      `  ${check.passed ? "✓" : "✗"} ${check.name}: ${check.detail}`
    );
  }

  // If quality check fails, run auto-fix pass
  if (!qualityCheck.passed) {
    console.log(
      `Quality check FAILED (${qualityCheck.issues.length} issues). Running auto-fix...`
    );

    try {
      html = await verifyAndFixHtml(html, extractedData, qualityCheck);
      console.log(`Pass 3 auto-fix complete. Final HTML: ${html.length} chars`);
    } catch (fixError) {
      // If fix fails, still return the original HTML (better than nothing)
      console.error(
        "Pass 3 auto-fix failed, using original HTML:",
        fixError instanceof Error ? fixError.message : String(fixError)
      );
    }
  } else {
    console.log("Quality check PASSED — no fixes needed.");
  }

  return html;
}

// ═══════════════════════════════════════════════
// PASS 3: Quality Verification Engine
// ═══════════════════════════════════════════════

function performQualityChecks(
  html: string,
  extractedDataJson: string
): QualityCheckResult {
  const issues: string[] = [];
  const suggestions: string[] = [];
  const checks: { name: string; passed: boolean; detail: string }[] = [];

  // Parse extracted data for verification
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  let data: any;
  try {
    const cleanJson = extractedDataJson
      .replace(/```json\s*/g, "")
      .replace(/```\s*/g, "")
      .trim();
    data = JSON.parse(cleanJson);
  } catch {
    issues.push(
      "Could not parse extracted data JSON for verification — skipping data-dependent checks"
    );
    return { passed: false, score: 0, issues, suggestions, checks };
  }

  const htmlLower = html.toLowerCase();

  // ─────────────────────────────────────────────
  // CHECK 1: Brand Primary Color Usage
  // ─────────────────────────────────────────────
  const primaryColor = data.brand_primary_color;
  if (primaryColor) {
    const colorUsed = htmlLower.includes(primaryColor.toLowerCase());
    checks.push({
      name: "Brand Primary Color",
      passed: colorUsed,
      detail: colorUsed
        ? `${primaryColor} found in HTML`
        : `${primaryColor} NOT found — brand color must be used`,
    });
    if (!colorUsed) {
      issues.push(
        `CRITICAL: Brand primary color ${primaryColor} is completely absent from the HTML. The page MUST use the brand's primary color in gradients, buttons, stat numbers, chart colors, and accents.`
      );
    }
  }

  // ─────────────────────────────────────────────
  // CHECK 2: Brand Secondary Color Usage
  // ─────────────────────────────────────────────
  const secondaryColor = data.brand_secondary_color;
  if (secondaryColor) {
    const colorUsed = htmlLower.includes(secondaryColor.toLowerCase());
    checks.push({
      name: "Brand Secondary Color",
      passed: colorUsed,
      detail: colorUsed
        ? `${secondaryColor} found in HTML`
        : `${secondaryColor} NOT found`,
    });
    if (!colorUsed) {
      issues.push(
        `Brand secondary color ${secondaryColor} is missing. Use it in secondary gradients, hover effects, and chart accents.`
      );
    }
  }

  // ─────────────────────────────────────────────
  // CHECK 3: Logo Colors Consistency
  // ─────────────────────────────────────────────
  if (data.logo_colors?.length > 0) {
    const missingColors = data.logo_colors.filter(
      (c: string) => !htmlLower.includes(c.toLowerCase())
    );
    const allPresent = missingColors.length === 0;
    checks.push({
      name: "Logo Colors",
      passed: allPresent,
      detail: allPresent
        ? `All ${data.logo_colors.length} logo colors found`
        : `Missing: ${missingColors.join(", ")}`,
    });
    if (!allPresent) {
      issues.push(
        `Logo colors ${missingColors.join(", ")} are missing from the page. These colors define the brand identity and must be visible.`
      );
    }
  }

  // ─────────────────────────────────────────────
  // CHECK 4: Logo / SVG Representation
  // ─────────────────────────────────────────────
  const hasSvgLogo = html.includes("<svg") && htmlLower.includes("logo");
  const hasStyledLogo =
    htmlLower.includes("logo") || htmlLower.includes("brand");
  const hasNavLogo =
    hasSvgLogo ||
    (hasStyledLogo &&
      (html.includes("nav") || html.includes("header") || html.includes("Nav")));
  checks.push({
    name: "Logo Representation",
    passed: hasNavLogo,
    detail: hasSvgLogo
      ? "SVG logo found in navigation"
      : hasNavLogo
        ? "Styled logo found in navigation area"
        : "NO logo representation found — must add SVG or styled text logo",
  });
  if (!hasNavLogo) {
    const logoSvg = data.logo_description?.svg_suggestion;
    if (logoSvg) {
      issues.push(
        `No logo found in the navigation. Use this SVG logo from the extracted data: ${logoSvg.substring(0, 200)}... Place it in the nav/header area.`
      );
    } else {
      issues.push(
        `No logo found in the navigation. Create a styled text logo using the company name "${data.company_name || "Company"}" with brand colors, or create an SVG representation.`
      );
    }
  }

  // ─────────────────────────────────────────────
  // CHECK 5: Chart.js Charts Present
  // ─────────────────────────────────────────────
  const chartCanvasCount = (html.match(/<canvas/gi) || []).length;
  const hasChartJs =
    htmlLower.includes("new chart") || htmlLower.includes("chart(");
  const chartsOk = chartCanvasCount >= 2 && hasChartJs;
  checks.push({
    name: "Chart.js Visualizations",
    passed: chartsOk,
    detail: chartsOk
      ? `${chartCanvasCount} canvas elements with Chart.js initialization found`
      : chartCanvasCount === 0
        ? "NO charts found — page must have data visualizations"
        : `Only ${chartCanvasCount} chart(s) — need at least 2`,
  });
  if (!chartsOk) {
    issues.push(
      `CRITICAL: ${chartCanvasCount === 0 ? "NO" : "Only " + chartCanvasCount} Chart.js chart(s) found. The page MUST include at least 2-3 interactive charts (doughnut for market data, bar for financials, line for growth trends). Use the charts_data, market_opportunity, traction.growth_data, and financials.projections from the extracted data.`
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 6: Chart.js Dark Theme Styling
  // ─────────────────────────────────────────────
  const hasChartDefaults =
    html.includes("Chart.defaults") || html.includes("chart.defaults");
  const hasGradientFills =
    html.includes("createLinearGradient") ||
    html.includes("createRadialGradient");
  checks.push({
    name: "Chart Dark Theme",
    passed: hasChartDefaults,
    detail: hasChartDefaults
      ? `Chart.defaults configured${hasGradientFills ? " with gradient fills" : ""}`
      : "Chart.defaults not set — charts may look unstyled",
  });
  if (!hasChartDefaults && hasChartJs) {
    suggestions.push(
      "Set Chart.defaults.color = '#94A3B8' and Chart.defaults.borderColor = 'rgba(255,255,255,0.06)' for dark theme chart styling."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 7: GSAP Scroll Animations
  // ─────────────────────────────────────────────
  const hasGsap =
    html.includes("gsap.") || html.includes("gsap.from") || html.includes("gsap.to");
  const hasScrollTrigger = html.includes("ScrollTrigger");
  const gsapOk = hasGsap && hasScrollTrigger;
  checks.push({
    name: "GSAP Animations",
    passed: gsapOk,
    detail: gsapOk
      ? "GSAP with ScrollTrigger found"
      : !hasGsap
        ? "NO GSAP animations found"
        : "GSAP found but ScrollTrigger not registered",
  });
  if (!gsapOk) {
    issues.push(
      "CRITICAL: No GSAP scroll animations found. Add gsap.from() with ScrollTrigger for section fade-ins, card staggers, and number counter animations."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 8: Number Counter Animations
  // ─────────────────────────────────────────────
  const hasCounters =
    htmlLower.includes("data-count") ||
    htmlLower.includes("counter") ||
    htmlLower.includes("countup") ||
    (html.includes("innerText") && html.includes("gsap.to"));
  checks.push({
    name: "Number Counters",
    passed: hasCounters,
    detail: hasCounters
      ? "Number counter animation logic found"
      : "No animated number counters — stats should count up on scroll",
  });
  if (!hasCounters) {
    issues.push(
      "No number counter animations. Key statistics and metrics MUST animate from 0 to their value when scrolled into view (using GSAP or requestAnimationFrame)."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 9: Interactive Modals
  // ─────────────────────────────────────────────
  const hasModal =
    htmlLower.includes("modal") ||
    htmlLower.includes("popup") ||
    htmlLower.includes("overlay");
  const hasModalClose =
    html.includes("Escape") ||
    html.includes("closeModal") ||
    html.includes("close-modal");
  const modalOk = hasModal && hasModalClose;
  checks.push({
    name: "Interactive Modals",
    passed: modalOk,
    detail: modalOk
      ? "Modal system with close functionality found"
      : !hasModal
        ? "NO modal/popup system — metric cards should be clickable"
        : "Modal found but no close mechanism (Escape/backdrop/X)",
  });
  if (!modalOk) {
    issues.push(
      "No interactive modal system. Add a reusable showModal(title, content) function. Key metric cards MUST be clickable to show detail modals with deeper data. Modals should close on X button, Escape key, and backdrop click."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 10: Hover Effects & Micro-interactions
  // ─────────────────────────────────────────────
  const hasHoverCSS = html.includes(":hover") || html.includes("hover:");
  const hasTransition =
    html.includes("transition") || html.includes("transform");
  const hoverOk = hasHoverCSS && hasTransition;
  checks.push({
    name: "Hover Effects",
    passed: hoverOk,
    detail: hoverOk
      ? "CSS hover effects with transitions found"
      : "Missing hover effects — cards should lift and glow on hover",
  });
  if (!hoverOk) {
    issues.push(
      "No hover effects on cards. Add CSS transitions with transform: translateY(-4px) and box-shadow changes on :hover for all cards."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 11: Responsive Design
  // ─────────────────────────────────────────────
  const hasMdBreakpoints = html.includes("md:");
  const hasLgBreakpoints = html.includes("lg:");
  const responsiveOk = hasMdBreakpoints;
  checks.push({
    name: "Responsive Design",
    passed: responsiveOk,
    detail: responsiveOk
      ? `Tailwind breakpoints found (md: ${hasMdBreakpoints}, lg: ${hasLgBreakpoints})`
      : "NO responsive breakpoints — page won't work on mobile",
  });
  if (!responsiveOk) {
    issues.push(
      "No responsive breakpoints found. Use Tailwind's md: and lg: prefixes for responsive grids, font sizes, and spacing."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 12: Company Name Present
  // ─────────────────────────────────────────────
  if (data.company_name) {
    const namePresent = html.includes(data.company_name);
    checks.push({
      name: "Company Name",
      passed: namePresent,
      detail: namePresent
        ? `"${data.company_name}" found in HTML`
        : `"${data.company_name}" NOT found`,
    });
    if (!namePresent) {
      issues.push(
        `Company name "${data.company_name}" is missing from the page. It must appear in the navigation, hero section, and footer.`
      );
    }
  }

  // ─────────────────────────────────────────────
  // CHECK 13: Key Metrics Data Coverage
  // ─────────────────────────────────────────────
  if (data.key_metrics?.length > 0) {
    let metricsFound = 0;
    for (const metric of data.key_metrics) {
      if (
        html.includes(metric.value?.toString()) ||
        html.includes(metric.label)
      ) {
        metricsFound++;
      }
    }
    const total = data.key_metrics.length;
    const coverage = Math.round((metricsFound / total) * 100);
    const metricsOk = coverage >= 70;
    checks.push({
      name: "Key Metrics Coverage",
      passed: metricsOk,
      detail: `${metricsFound}/${total} metrics found (${coverage}%)`,
    });
    if (!metricsOk) {
      const missing = data.key_metrics
        .filter(
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          (m: any) =>
            !html.includes(m.value?.toString()) && !html.includes(m.label)
        )
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        .map((m: any) => `${m.label}: ${m.value}`)
        .join(", ");
      issues.push(
        `Only ${metricsFound}/${total} key metrics displayed (${coverage}%). Missing: ${missing}. ALL metrics must appear on the page.`
      );
    }
  }

  // ─────────────────────────────────────────────
  // CHECK 14: Team Members Present
  // ─────────────────────────────────────────────
  if (data.team?.length > 0) {
    let teamFound = 0;
    for (const member of data.team) {
      if (html.includes(member.name)) teamFound++;
    }
    const total = data.team.length;
    const teamOk = teamFound >= total * 0.8;
    checks.push({
      name: "Team Section",
      passed: teamOk,
      detail:
        teamFound > 0
          ? `${teamFound}/${total} team members found`
          : "NO team member names found in HTML",
    });
    if (!teamOk) {
      issues.push(
        `Only ${teamFound}/${total} team members displayed. Add a Team section with cards for: ${data.team.map((t: { name: string }) => t.name).join(", ")}.`
      );
    }
  }

  // ─────────────────────────────────────────────
  // CHECK 15: CDN Resources Loaded
  // ─────────────────────────────────────────────
  const cdnChecks = [
    { name: "Tailwind CSS", pattern: "cdn.tailwindcss.com" },
    { name: "GSAP", pattern: "gsap" },
    { name: "Chart.js", pattern: "chart.js" },
    { name: "Inter Font", pattern: "fonts.googleapis.com" },
  ];
  const missingCdns = cdnChecks.filter((c) => !htmlLower.includes(c.pattern));
  const cdnOk = missingCdns.length === 0;
  checks.push({
    name: "CDN Resources",
    passed: cdnOk,
    detail: cdnOk
      ? "All 4 CDN resources loaded"
      : `Missing: ${missingCdns.map((c) => c.name).join(", ")}`,
  });
  if (!cdnOk) {
    issues.push(
      `Missing CDN resources: ${missingCdns.map((c) => c.name).join(", ")}. All must be included in <head>.`
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 16: Glassmorphism Design
  // ─────────────────────────────────────────────
  const hasBackdropBlur =
    html.includes("backdrop-blur") || html.includes("backdrop-filter");
  const hasGlassCards =
    html.includes("rgba(255") || html.includes("rgba(255, 255, 255");
  const glassOk = hasBackdropBlur && hasGlassCards;
  checks.push({
    name: "Glassmorphism Design",
    passed: glassOk,
    detail: glassOk
      ? "Backdrop blur and semi-transparent cards found"
      : "Missing glassmorphism effects — cards should have blur and transparency",
  });
  if (!glassOk) {
    suggestions.push(
      "Add glassmorphism to cards: background: rgba(255,255,255,0.04), backdrop-filter: blur(20px), border: 1px solid rgba(255,255,255,0.08)."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 17: Ambient Glow Effects
  // ─────────────────────────────────────────────
  const hasGlowOrbs =
    html.includes("blur(") &&
    (html.includes("absolute") || html.includes("position: absolute"));
  checks.push({
    name: "Ambient Glow Effects",
    passed: hasGlowOrbs,
    detail: hasGlowOrbs
      ? "Ambient glow/blur effects found"
      : "No ambient lighting — add blurred gradient orbs for depth",
  });
  if (!hasGlowOrbs) {
    suggestions.push(
      "Add 2-3 large ambient glow orbs in the hero section: position:absolute circles with brand colors at 10-15% opacity and blur(100px) for premium depth."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 18: Section Count (Content Completeness)
  // ─────────────────────────────────────────────
  const sectionElements = (html.match(/<section/gi) || []).length;
  const idSections = (html.match(/id="[^"]+"/gi) || []).length;
  const sectionCount = Math.max(sectionElements, idSections);
  const sectionsOk = sectionCount >= 5;
  checks.push({
    name: "Content Sections",
    passed: sectionsOk,
    detail: `${sectionCount} sections found${!sectionsOk ? " — need at least 5 content sections" : ""}`,
  });
  if (!sectionsOk) {
    issues.push(
      `Only ${sectionCount} sections found. A complete landing page needs at least 5-7 distinct sections (Hero, Metrics, About, Market, Traction, Team, CTA). The page feels incomplete.`
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 19: Smooth Scroll Navigation
  // ─────────────────────────────────────────────
  const hasSmoothScroll =
    html.includes("scroll-behavior: smooth") ||
    html.includes("smooth") ||
    html.includes("scrollIntoView");
  checks.push({
    name: "Smooth Scroll",
    passed: hasSmoothScroll,
    detail: hasSmoothScroll
      ? "Smooth scroll behavior found"
      : "No smooth scrolling — navigation links should scroll smoothly",
  });
  if (!hasSmoothScroll) {
    suggestions.push(
      "Add scroll-behavior: smooth to <html> tag and use anchor links (href='#section') in navigation."
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 20: HTML Structure Valid
  // ─────────────────────────────────────────────
  const hasDoctype = html.includes("<!DOCTYPE") || html.includes("<!doctype");
  const hasClosingHtml = html.includes("</html>");
  const hasClosingBody = html.includes("</body>");
  const structureOk = hasDoctype && hasClosingHtml && hasClosingBody;
  checks.push({
    name: "HTML Structure",
    passed: structureOk,
    detail: structureOk
      ? "Valid DOCTYPE, closing body and html tags found"
      : `Missing: ${!hasDoctype ? "DOCTYPE " : ""}${!hasClosingBody ? "</body> " : ""}${!hasClosingHtml ? "</html>" : ""}`,
  });
  if (!structureOk) {
    issues.push("HTML structure is incomplete — missing DOCTYPE, </body>, or </html> tags.");
  }

  // ─────────────────────────────────────────────
  // CHECK 21: Page Size (Not Too Small)
  // ─────────────────────────────────────────────
  const htmlLength = html.length;
  const sizeOk = htmlLength >= 8000;
  checks.push({
    name: "Page Size",
    passed: sizeOk,
    detail: sizeOk
      ? `${htmlLength.toLocaleString()} chars — good content volume`
      : `Only ${htmlLength.toLocaleString()} chars — page is too minimal`,
  });
  if (!sizeOk) {
    issues.push(
      `HTML is only ${htmlLength.toLocaleString()} characters. A complete, data-rich landing page should be 15,000+ characters. The page needs more sections, content, and interactivity.`
    );
  }

  // ─────────────────────────────────────────────
  // CHECK 22: Mobile Navigation (Hamburger Menu)
  // ─────────────────────────────────────────────
  const hasMobileNav =
    htmlLower.includes("hamburger") ||
    htmlLower.includes("mobile-menu") ||
    htmlLower.includes("menu-toggle") ||
    (html.includes("md:hidden") && html.includes("md:flex"));
  checks.push({
    name: "Mobile Navigation",
    passed: hasMobileNav,
    detail: hasMobileNav
      ? "Mobile navigation toggle found"
      : "No mobile hamburger menu — navigation hidden on mobile",
  });
  if (!hasMobileNav) {
    suggestions.push(
      "Add a hamburger menu for mobile: hide nav links on small screens (hidden md:flex), show a toggle button (md:hidden) that reveals a mobile menu."
    );
  }

  // ─────────────────────────────────────────────
  // CALCULATE SCORE
  // ─────────────────────────────────────────────
  const totalChecks = checks.length;
  const passedChecks = checks.filter((c) => c.passed).length;
  const score = Math.round((passedChecks / totalChecks) * 100);

  // Page passes if no CRITICAL issues and score >= 70
  const hasCriticalIssues = issues.some((i) => i.startsWith("CRITICAL:"));
  const passed = !hasCriticalIssues && score >= 70;

  return { passed, score, issues, suggestions, checks };
}

// ═══════════════════════════════════════════════
// PASS 3: Auto-Fix Engine
// ═══════════════════════════════════════════════

async function verifyAndFixHtml(
  html: string,
  extractedDataJson: string,
  qualityCheck: QualityCheckResult
): Promise<string> {
  const issuesList = qualityCheck.issues
    .map((i, idx) => `${idx + 1}. ${i}`)
    .join("\n");
  const suggestionsList = qualityCheck.suggestions
    .map((s, idx) => `${idx + 1}. ${s}`)
    .join("\n");

  const checkSummary = qualityCheck.checks
    .map((c) => `${c.passed ? "✓ PASS" : "✗ FAIL"} | ${c.name}: ${c.detail}`)
    .join("\n");

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const fixParams: any = {
    model: "claude-sonnet-4-20250514",
    max_tokens: 16384,
    temperature: 0,
    system: `You are a senior QA engineer and frontend developer. You receive an HTML landing page that failed quality checks, along with the source data and specific issues to fix.

Your job:
1. Fix ALL listed issues — these are mandatory fixes
2. Implement as many suggestions as possible
3. Return the COMPLETE fixed HTML file
4. Do NOT remove any existing content — only add/fix what's needed
5. Ensure brand colors, charts, animations, and interactivity are all present
6. The page must be complete, premium-quality, and data-rich

Return ONLY the complete fixed HTML starting with <!DOCTYPE html>. No explanations.`,
    messages: [
      {
        role: "user",
        content: `Here is a landing page HTML that failed quality verification (score: ${qualityCheck.score}/100):

\`\`\`html
${html}
\`\`\`

Here is the source data that must be reflected on the page:

\`\`\`json
${extractedDataJson}
\`\`\`

═══ QUALITY CHECK RESULTS ═══
${checkSummary}

═══ ISSUES TO FIX (MANDATORY) ═══
${issuesList}

═══ SUGGESTIONS (IMPLEMENT IF POSSIBLE) ═══
${suggestionsList || "None"}

Fix ALL issues above. Return the COMPLETE corrected HTML starting with <!DOCTYPE html>. No explanations, no markdown, no code fences.`,
      },
    ],
  };

  let fixedText = "";

  try {
    const stream = client.messages.stream(fixParams);
    stream.on("text", (text) => {
      fixedText += text;
    });

    const finalMessage = await stream.finalMessage();

    if (!fixedText || fixedText.length < 100) {
      const textBlocks = finalMessage.content.filter(
        (c): c is Anthropic.TextBlock => c.type === "text"
      );
      if (textBlocks.length === 0) {
        throw new Error("No text content in fix response");
      }
      fixedText = textBlocks.map((b) => b.text).join("\n");
    }

    console.log(
      `Fix response: ${fixedText.length} chars, Stop: ${finalMessage.stop_reason}`
    );

    return extractHtml(fixedText);
  } catch (apiError) {
    const errMsg =
      apiError instanceof Error ? apiError.message : String(apiError);
    throw new Error(`Quality fix failed: ${errMsg}`);
  }
}

// ═══════════════════════════════════════════════
// Edit existing HTML + Quality Verification
// ═══════════════════════════════════════════════

export async function editExistingHtml(
  existingHtml: string,
  editInstructions: string
): Promise<string> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const messageParams: any = {
    model: "claude-sonnet-4-20250514",
    max_tokens: 16384,
    temperature: 0,
    system: `You are an expert web developer who edits existing HTML files. You make ONLY the changes the user asks for and keep everything else EXACTLY intact. You always return the COMPLETE modified HTML file.`,
    messages: [
      {
        role: "user",
        content: `Here is the existing landing page HTML:\n\n${existingHtml}`,
      },
      {
        role: "assistant",
        content:
          "I've read the complete HTML file. What changes would you like me to make?",
      },
      {
        role: "user",
        content: `Make these specific changes:\n\n${editInstructions}\n\nRules:\n- ONLY modify what I asked for\n- Keep ALL other content, styling, charts, animations, and data exactly the same\n- Return the COMPLETE HTML starting with <!DOCTYPE html>\n- No explanations, no code fences, just the HTML`,
      },
    ],
  };

  let fullText = "";

  try {
    const stream = client.messages.stream(messageParams);
    stream.on("text", (text) => {
      fullText += text;
    });

    const finalMessage = await stream.finalMessage();

    if (!fullText || fullText.length < 100) {
      const textBlocks = finalMessage.content.filter(
        (c): c is Anthropic.TextBlock => c.type === "text"
      );
      if (textBlocks.length === 0) {
        throw new Error("No text content in Claude response");
      }
      fullText = textBlocks.map((b) => b.text).join("\n");
    }

    console.log(
      `Edit complete. Response: ${fullText.length} chars, Stop: ${finalMessage.stop_reason}`
    );
  } catch (apiError) {
    const errMsg =
      apiError instanceof Error ? apiError.message : String(apiError);

    if (errMsg.includes("rate_limit") || errMsg.includes("429")) {
      throw new Error(
        "API rate limit reached. Please wait a minute and try again."
      );
    }
    if (errMsg.includes("overloaded") || errMsg.includes("529")) {
      throw new Error(
        "Claude is currently overloaded. Please wait and try again."
      );
    }

    throw new Error(`AI edit failed: ${errMsg}`);
  }

  let html = extractHtml(fullText);

  // ── Run structural quality checks after edit ──
  console.log("Post-edit: Running structural quality checks...");
  const qualityCheck = performStructuralChecks(html);

  console.log(
    `Post-edit quality score: ${qualityCheck.score}/100 | Issues: ${qualityCheck.issues.length}`
  );
  for (const check of qualityCheck.checks) {
    console.log(
      `  ${check.passed ? "✓" : "✗"} ${check.name}: ${check.detail}`
    );
  }

  if (!qualityCheck.passed) {
    console.log(
      `Post-edit quality FAILED (${qualityCheck.issues.length} issues). Running auto-fix...`
    );
    try {
      html = await fixEditedHtml(html, editInstructions, qualityCheck);
      console.log(`Post-edit fix complete. Final HTML: ${html.length} chars`);
    } catch (fixError) {
      console.error(
        "Post-edit fix failed, using edited HTML as-is:",
        fixError instanceof Error ? fixError.message : String(fixError)
      );
    }
  } else {
    console.log("Post-edit quality check PASSED.");
  }

  return html;
}

// ═══════════════════════════════════════════════
// Structural-only quality checks (no extracted data needed)
// Used after edit mode to verify the edit didn't break anything
// ═══════════════════════════════════════════════

function performStructuralChecks(html: string): QualityCheckResult {
  const issues: string[] = [];
  const suggestions: string[] = [];
  const checks: { name: string; passed: boolean; detail: string }[] = [];

  const htmlLower = html.toLowerCase();

  // 1. Chart.js Charts Present
  const chartCanvasCount = (html.match(/<canvas/gi) || []).length;
  const hasChartJs =
    htmlLower.includes("new chart") || htmlLower.includes("chart(");
  const chartsOk = chartCanvasCount >= 2 && hasChartJs;
  checks.push({
    name: "Chart.js Visualizations",
    passed: chartsOk,
    detail: chartsOk
      ? `${chartCanvasCount} canvas elements with Chart.js found`
      : chartCanvasCount === 0
        ? "NO charts found — edit may have removed charts"
        : `Only ${chartCanvasCount} chart(s) remain`,
  });
  if (!chartsOk) {
    issues.push(
      `CRITICAL: ${chartCanvasCount === 0 ? "NO" : "Only " + chartCanvasCount} Chart.js chart(s) found. Charts must be preserved during edits. Restore any removed doughnut, bar, or line charts.`
    );
  }

  // 2. GSAP Scroll Animations
  const hasGsap =
    html.includes("gsap.") || html.includes("gsap.from") || html.includes("gsap.to");
  const hasScrollTrigger = html.includes("ScrollTrigger");
  const gsapOk = hasGsap && hasScrollTrigger;
  checks.push({
    name: "GSAP Animations",
    passed: gsapOk,
    detail: gsapOk
      ? "GSAP with ScrollTrigger intact"
      : "GSAP animations may have been removed",
  });
  if (!gsapOk) {
    issues.push(
      "CRITICAL: GSAP scroll animations are missing. Restore gsap.from() with ScrollTrigger for section fade-ins and number counters."
    );
  }

  // 3. Number Counter Animations
  const hasCounters =
    htmlLower.includes("data-count") ||
    htmlLower.includes("counter") ||
    htmlLower.includes("countup") ||
    (html.includes("innerText") && html.includes("gsap.to"));
  checks.push({
    name: "Number Counters",
    passed: hasCounters,
    detail: hasCounters
      ? "Number counter animations intact"
      : "Counter animations may be missing",
  });
  if (!hasCounters) {
    suggestions.push(
      "Number counter animations may have been removed. Ensure stat numbers still animate from 0 to target."
    );
  }

  // 4. Interactive Modals
  const hasModal =
    htmlLower.includes("modal") ||
    htmlLower.includes("popup") ||
    htmlLower.includes("overlay");
  const hasModalClose =
    html.includes("Escape") ||
    html.includes("closeModal") ||
    html.includes("close-modal");
  const modalOk = hasModal && hasModalClose;
  checks.push({
    name: "Interactive Modals",
    passed: modalOk,
    detail: modalOk
      ? "Modal system intact"
      : "Modal system may be broken or removed",
  });
  if (!modalOk) {
    issues.push(
      "Modal system is missing or broken. Metric cards should be clickable with detail modals. Modals must close on X, Escape, and backdrop click."
    );
  }

  // 5. Hover Effects
  const hasHoverCSS = html.includes(":hover") || html.includes("hover:");
  const hasTransition =
    html.includes("transition") || html.includes("transform");
  const hoverOk = hasHoverCSS && hasTransition;
  checks.push({
    name: "Hover Effects",
    passed: hoverOk,
    detail: hoverOk ? "Hover effects intact" : "Hover effects may be missing",
  });
  if (!hoverOk) {
    suggestions.push(
      "Hover effects may be missing. Cards should have CSS transitions with translateY and box-shadow on :hover."
    );
  }

  // 6. Responsive Design
  const hasMdBreakpoints = html.includes("md:");
  const hasLgBreakpoints = html.includes("lg:");
  const responsiveOk = hasMdBreakpoints;
  checks.push({
    name: "Responsive Design",
    passed: responsiveOk,
    detail: responsiveOk
      ? `Tailwind breakpoints intact (md: ${hasMdBreakpoints}, lg: ${hasLgBreakpoints})`
      : "Responsive breakpoints may be missing",
  });
  if (!responsiveOk) {
    issues.push(
      "Responsive breakpoints missing. Ensure Tailwind md: and lg: prefixes are used throughout."
    );
  }

  // 7. CDN Resources
  const cdnChecks = [
    { name: "Tailwind CSS", pattern: "cdn.tailwindcss.com" },
    { name: "GSAP", pattern: "gsap" },
    { name: "Chart.js", pattern: "chart.js" },
    { name: "Inter Font", pattern: "fonts.googleapis.com" },
  ];
  const missingCdns = cdnChecks.filter((c) => !htmlLower.includes(c.pattern));
  const cdnOk = missingCdns.length === 0;
  checks.push({
    name: "CDN Resources",
    passed: cdnOk,
    detail: cdnOk
      ? "All CDN resources intact"
      : `Missing: ${missingCdns.map((c) => c.name).join(", ")}`,
  });
  if (!cdnOk) {
    issues.push(
      `CRITICAL: CDN resources removed: ${missingCdns.map((c) => c.name).join(", ")}. Restore all <script>/<link> tags in <head>.`
    );
  }

  // 8. Content Sections
  const sectionElements = (html.match(/<section/gi) || []).length;
  const idSections = (html.match(/id="[^"]+"/gi) || []).length;
  const sectionCount = Math.max(sectionElements, idSections);
  const sectionsOk = sectionCount >= 4;
  checks.push({
    name: "Content Sections",
    passed: sectionsOk,
    detail: `${sectionCount} sections found${!sectionsOk ? " — sections may have been removed" : ""}`,
  });
  if (!sectionsOk) {
    issues.push(
      `Only ${sectionCount} sections remain. Edit may have accidentally removed sections. Restore missing sections.`
    );
  }

  // 9. HTML Structure
  const hasDoctype = html.includes("<!DOCTYPE") || html.includes("<!doctype");
  const hasClosingHtml = html.includes("</html>");
  const hasClosingBody = html.includes("</body>");
  const structureOk = hasDoctype && hasClosingHtml && hasClosingBody;
  checks.push({
    name: "HTML Structure",
    passed: structureOk,
    detail: structureOk
      ? "Valid HTML structure"
      : `Missing: ${!hasDoctype ? "DOCTYPE " : ""}${!hasClosingBody ? "</body> " : ""}${!hasClosingHtml ? "</html>" : ""}`,
  });
  if (!structureOk) {
    issues.push(
      "HTML structure is broken — missing DOCTYPE, </body>, or </html>."
    );
  }

  // 10. Page Size
  const htmlLength = html.length;
  const sizeOk = htmlLength >= 5000;
  checks.push({
    name: "Page Size",
    passed: sizeOk,
    detail: sizeOk
      ? `${htmlLength.toLocaleString()} chars`
      : `Only ${htmlLength.toLocaleString()} chars — edit may have truncated the page`,
  });
  if (!sizeOk) {
    issues.push(
      `HTML is only ${htmlLength.toLocaleString()} characters. The edit may have truncated content. Ensure the complete page is returned.`
    );
  }

  // 11. Glassmorphism preserved
  const hasBackdropBlur =
    html.includes("backdrop-blur") || html.includes("backdrop-filter");
  const hasGlassCards =
    html.includes("rgba(255") || html.includes("rgba(255, 255, 255");
  const glassOk = hasBackdropBlur && hasGlassCards;
  checks.push({
    name: "Glassmorphism Design",
    passed: glassOk,
    detail: glassOk
      ? "Glass effects intact"
      : "Glassmorphism effects may be missing",
  });
  if (!glassOk) {
    suggestions.push(
      "Glassmorphism may be missing. Ensure cards have backdrop-blur and semi-transparent backgrounds."
    );
  }

  // 12. Smooth Scroll
  const hasSmoothScroll =
    html.includes("scroll-behavior: smooth") ||
    html.includes("smooth") ||
    html.includes("scrollIntoView");
  checks.push({
    name: "Smooth Scroll",
    passed: hasSmoothScroll,
    detail: hasSmoothScroll
      ? "Smooth scroll intact"
      : "Smooth scroll may be missing",
  });
  if (!hasSmoothScroll) {
    suggestions.push("Ensure scroll-behavior: smooth is on the <html> tag.");
  }

  // Calculate score
  const totalChecks = checks.length;
  const passedChecks = checks.filter((c) => c.passed).length;
  const score = Math.round((passedChecks / totalChecks) * 100);
  const hasCritical = issues.some((i) => i.startsWith("CRITICAL:"));
  const passed = !hasCritical && score >= 65;

  return { passed, score, issues, suggestions, checks };
}

// ═══════════════════════════════════════════════
// Auto-fix for edited HTML
// ═══════════════════════════════════════════════

async function fixEditedHtml(
  html: string,
  originalEditInstructions: string,
  qualityCheck: QualityCheckResult
): Promise<string> {
  const issuesList = qualityCheck.issues
    .map((i, idx) => `${idx + 1}. ${i}`)
    .join("\n");
  const suggestionsList = qualityCheck.suggestions
    .map((s, idx) => `${idx + 1}. ${s}`)
    .join("\n");

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const fixParams: any = {
    model: "claude-sonnet-4-20250514",
    max_tokens: 16384,
    temperature: 0,
    system: `You are a QA engineer fixing an HTML landing page that was edited but failed quality checks. The user's edit instructions were applied, but the edit accidentally broke or removed some features. Fix the broken parts WITHOUT undoing the user's intended changes. Return the COMPLETE fixed HTML.`,
    messages: [
      {
        role: "user",
        content: `This HTML was edited with these instructions: "${originalEditInstructions}"

The edit was applied but broke these quality checks:

═══ ISSUES TO FIX ═══
${issuesList}

═══ SUGGESTIONS ═══
${suggestionsList || "None"}

Here is the current (broken) HTML:

\`\`\`html
${html}
\`\`\`

Fix ALL issues while keeping the user's edit changes intact. Return the COMPLETE corrected HTML starting with <!DOCTYPE html>. No explanations.`,
      },
    ],
  };

  let fixedText = "";

  const stream = client.messages.stream(fixParams);
  stream.on("text", (text) => {
    fixedText += text;
  });

  const finalMessage = await stream.finalMessage();

  if (!fixedText || fixedText.length < 100) {
    const textBlocks = finalMessage.content.filter(
      (c): c is Anthropic.TextBlock => c.type === "text"
    );
    if (textBlocks.length === 0) {
      throw new Error("No text content in edit fix response");
    }
    fixedText = textBlocks.map((b) => b.text).join("\n");
  }

  console.log(
    `Edit fix: ${fixedText.length} chars, Stop: ${finalMessage.stop_reason}`
  );
  return extractHtml(fixedText);
}

// ═══════════════════════════════════════════════
// HTML Extraction Helper
// ═══════════════════════════════════════════════

function extractHtml(response: string): string {
  // Try to extract from ```html ... ``` code fences
  const htmlMatch = response.match(/```html\s*([\s\S]*?)\s*```/);
  if (htmlMatch) return htmlMatch[1].trim();

  // Try to extract from <!DOCTYPE or <html
  const docMatch = response.match(/(<!DOCTYPE[\s\S]*<\/html>)/i);
  if (docMatch) return docMatch[1].trim();

  // Check if it starts with DOCTYPE or html tag
  const trimmed = response.trim();
  if (
    trimmed.startsWith("<!DOCTYPE") ||
    trimmed.startsWith("<html") ||
    trimmed.startsWith("<HTML")
  ) {
    return trimmed;
  }

  // If response contains partial HTML (truncated), salvage it
  const partialMatch = response.match(/(<!DOCTYPE[\s\S]*)/i);
  if (partialMatch) {
    const partial = partialMatch[1].trim();
    if (!partial.includes("</html>")) {
      return partial + "\n</body>\n</html>";
    }
    return partial;
  }

  throw new Error(
    "Claude did not return valid HTML. Length: " +
      response.length +
      " chars. Starts with: " +
      response.substring(0, 200)
  );
}
