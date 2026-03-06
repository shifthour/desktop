# Project Plan: Landing Page Generation Tools
### 1-Month Development Roadmap

**Prepared for:** Aldrin Raphael / A R Business Brokers Inc.
**Date:** February 28, 2026
**Duration:** 4 Weeks (March 3 - March 28, 2026)

---

## Executive Summary

This document outlines the 1-month development plan for two landing page generation tools:

| | Tool 1: PDF-to-Landing Page | Tool 2: Google Drive-to-Landing Page |
|---|---|---|
| **Input** | PDF pitch deck / investor document | Google Drive folder link (Docs, Sheets, Slides, Images) |
| **Output** | Interactive, deployed landing page | Interactive, deployed landing page |
| **AI Engine** | Claude AI (content extraction + HTML generation) | Claude AI (content extraction + HTML generation) |
| **Deployment** | GitHub + Vercel (auto-deployed) | GitHub + Vercel (auto-deployed) |
| **Status** | Core workflow exists; needs productization | New build from scratch |

**Daily Schedule:** Each working day is split into two half-day blocks:
- **AM (Morning):** Tool 1 - PDF-to-Landing Page
- **PM (Afternoon):** Tool 2 - Google Drive-to-Landing Page

---

## Tool 1: PDF-to-Landing Page - Task Breakdown

### Current State
- Next.js application with PDF upload, Claude AI analysis, HTML generation, and Vercel deployment
- Working end-to-end for single projects (manually assisted)
- File-based project storage, no user auth yet

### What Needs to Be Built
- Template system for different industries/styles
- Robust error handling & retry logic
- Preview & editing capabilities
- Analytics dashboard integration (auto-provisioned)
- Admin panel for managing all generated pages
- Multi-page support (landing + analytics page bundled)
- Production hardening (rate limits, validation, logging)

---

## Tool 2: Google Drive-to-Landing Page - Task Breakdown

### Current State
- Does not exist yet

### What Needs to Be Built
- Google Drive API integration (OAuth + folder access)
- Document parser for multiple file types (Google Docs, Sheets, Slides, PDFs, Images)
- Content extraction & structuring pipeline
- Claude AI integration for Drive content analysis
- Landing page generation from extracted content
- Full deployment pipeline (GitHub + Vercel)
- Dashboard UI for Drive-based projects

---

## Week 1: Foundation & Core Architecture
**March 3 - March 7, 2026**

### Day 1 (Monday, March 3)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Project audit & architecture cleanup** | Review existing codebase. Document current flow (upload -> generate -> deploy). Identify bugs, tech debt, and missing error handling. Create task backlog. Refactor project structure for scalability. |
| PM | Tool 2 | **Google Drive API setup & OAuth flow** | Set up Google Cloud Console project. Enable Drive API & Docs/Sheets/Slides APIs. Implement OAuth 2.0 consent flow (read-only scope). Store refresh tokens securely. Test folder listing & file metadata retrieval. |

### Day 2 (Tuesday, March 4)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **PDF processing pipeline hardening** | Improve PDF text extraction accuracy. Add support for image-heavy PDFs (OCR fallback). Handle multi-page PDFs with proper chunking. Add PDF validation (corrupt file detection, size limits, page limits). Implement progress tracking for large files. |
| PM | Tool 2 | **Google Drive file discovery & metadata extraction** | Build recursive folder traversal (list all files in a Drive folder). Extract file metadata: type, name, size, modified date, mime type. Categorize files: documents, spreadsheets, presentations, images, PDFs. Build a structured manifest of all discoverable content. Create UI for displaying Drive folder contents. |

### Day 3 (Wednesday, March 5)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Template system - Phase 1** | Design template architecture (base layout + industry-specific variants). Create 3 base templates: Investor Deck, Company Profile, Product Showcase. Build template selection UI in the project wizard. Implement template variables (colors, fonts, section order). Store template configs as JSON schemas. |
| PM | Tool 2 | **Google Docs parser** | Implement Google Docs content extraction via Docs API. Parse: headings, paragraphs, tables, lists, bold/italic formatting. Extract inline images from Docs. Convert structured Doc content into a normalized JSON format (title, sections, key metrics, text blocks). Handle multiple Docs in a single folder. |

### Day 4 (Thursday, March 6)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Template system - Phase 2** | Build color scheme extraction from PDF (dominant colors, brand palette). Implement font detection/suggestion based on PDF content. Create template preview thumbnails. Add "Customize" step in wizard: pick template, adjust colors, toggle sections. |
| PM | Tool 2 | **Google Sheets parser** | Implement Sheets content extraction via Sheets API. Parse: cell values, sheet names, headers, data ranges. Detect data patterns: financial tables, metrics, timelines, KPIs. Convert tabular data into structured JSON (charts-ready format). Handle formulas (extract computed values, not formulas). |

### Day 5 (Friday, March 7)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Claude AI prompt optimization** | Refine prompt template for better data extraction accuracy. Add industry-specific prompt variants (SaaS, services, manufacturing, real estate). Implement prompt chaining: Step 1 = extract data, Step 2 = generate HTML. Add structured output validation (ensure all metrics are captured). Test with 5+ diverse PDFs and document quality scores. |
| PM | Tool 2 | **Google Slides parser** | Implement Slides content extraction via Slides API. Parse: slide titles, text boxes, shapes, tables, images. Extract speaker notes (often contain key details). Detect slide types: title, content, chart, team, financials. Map slide content to landing page sections. Handle embedded charts as images (export via Drive API). |

---

## Week 2: Generation Engine & Integration
**March 10 - March 14, 2026**

### Day 6 (Monday, March 10)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Preview & live editor - Phase 1** | Build HTML preview iframe with live rendering. Add split-pane view: generated code (left) + preview (right). Implement basic HTML editing (code editor with syntax highlighting). Auto-refresh preview on code changes. Add mobile/tablet/desktop responsive preview toggle. |
| PM | Tool 2 | **Image & PDF extraction from Drive** | Implement image download from Drive (logos, team photos, charts). Support formats: PNG, JPG, SVG, WebP. Build PDF extraction for Drive-hosted PDFs (reuse Tool 1's PDF parser). Implement image optimization (resize, compress for web). Create image hosting pipeline (embed as base64 or upload to CDN). |

### Day 7 (Tuesday, March 11)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Preview & live editor - Phase 2** | Add section reordering via drag-and-drop. Implement "Edit Content" mode: click any text to edit inline. Add "Regenerate Section" button per section (re-prompt Claude for just that section). Build undo/redo history stack. Add "Reset to Original" option. |
| PM | Tool 2 | **Content normalization & merging pipeline** | Build unified content schema that merges all Drive file types. De-duplicate overlapping information across files. Prioritize content sources (Slides > Docs > Sheets for narrative; Sheets > Slides for data). Create a merged "content manifest" JSON structure. Map merged content to landing page sections (hero, metrics, team, financials, CTA). |

### Day 8 (Wednesday, March 12)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Analytics auto-provisioning** | Auto-generate analytics.html alongside index.html. Auto-create Supabase tables per project (prefixed by project slug). Embed analytics tracking code in every generated landing page. Add analytics page link in admin dashboard. Generate unique Supabase anon key or use shared instance with project isolation. |
| PM | Tool 2 | **Claude AI integration for Drive content** | Build prompt template for Drive-sourced content (different from PDF prompt). Send merged content manifest + images to Claude. Implement structured generation: data extraction -> section mapping -> HTML generation. Handle larger content volumes (Drive folders may contain more data than a single PDF). Add token usage tracking and cost estimation. |

### Day 9 (Thursday, March 13)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Deployment pipeline improvements** | Add custom domain support (user provides their domain). Implement deployment status webhooks (notify when live). Add rollback capability (redeploy previous version). Build deployment history log. Add "Unpublish" / take-down feature. |
| PM | Tool 2 | **Landing page generation from Drive content** | Reuse Tool 1's HTML generation engine with Drive-specific adaptations. Ensure charts are populated from Sheets data. Ensure team sections use images from Drive. Generate complete single-file HTML (same quality as PDF tool). Test with sample Drive folder containing Docs + Sheets + Slides. |

### Day 10 (Friday, March 14)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Form system: Buyer Profile + NDA** | Create configurable form builder (fields vary per project). Build form templates: Buyer Profile, NDA, Contact, Lead Capture. Auto-inject selected forms into generated landing pages. Add form field validation rules. Implement form submission tracking with Supabase integration. |
| PM | Tool 2 | **Drive project wizard UI** | Build "New Project from Google Drive" flow in the dashboard. Step 1: Connect Google Drive (OAuth consent). Step 2: Paste/browse Drive folder URL. Step 3: Preview discovered files & content summary. Step 4: Select template & customize. Step 5: Generate & deploy. Add progress indicators for each parsing stage. |

---

## Week 3: Polish, Admin & Advanced Features
**March 17 - March 21, 2026**

### Day 11 (Monday, March 17)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Admin dashboard - Project management** | Build project listing with filters: status, date, client name. Add project status indicators: Draft, Generating, Live, Archived. Implement bulk actions: archive, delete, redeploy. Add project duplication (clone & modify). Build project settings page (name, slug, domain, forms, analytics). |
| PM | Tool 2 | **Drive sync & change detection** | Implement Drive webhook/polling for file changes. Detect when Drive folder content is updated. Show "Content changed - Regenerate?" notification. Build diff view: what changed since last generation. Add auto-regenerate option (on Drive file update). |

### Day 12 (Tuesday, March 18)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Admin dashboard - Lead management** | Build centralized lead inbox across all projects. Show buyer profiles and NDA submissions in unified table. Add lead status workflow: New -> Reviewed -> Approved -> Rejected. Implement email notifications on new lead submission. Add lead export (CSV/Excel). |
| PM | Tool 2 | **Multi-folder & workspace support** | Support multiple Drive folders per project (e.g., "Financials" folder + "Marketing" folder). Implement folder selection UI (tree view of Drive structure). Add per-folder content role assignment (this folder = financials, this = team bios). Handle shared Drive / Team Drive permissions. |

### Day 13 (Wednesday, March 19)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Interactive chart system** | Build chart configuration panel (select chart type, data source, colors). Support: bar, line, donut, pie, combo, area, stacked bar. Add chart data editor (edit values directly in a table). Implement chart animation settings (duration, easing, triggers). Auto-detect best chart type from data patterns. |
| PM | Tool 2 | **Google Sheets to Chart mapping** | Build smart mapping: Sheets data -> Chart.js configurations. Auto-detect chart-worthy data (time series, categories, comparisons). Generate multiple chart options per dataset. Allow user to pick which charts to include. Support financial metrics: revenue, EBITDA, margins, growth rates. |

### Day 14 (Thursday, March 20)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Mobile responsiveness & performance** | Audit all generated pages for mobile layout issues. Optimize: image lazy loading, CSS critical path, font loading. Add mobile hamburger menu to generated navbars. Test on real devices (iOS Safari, Android Chrome). Implement Lighthouse score tracking per deployment. |
| PM | Tool 2 | **Error handling & edge cases** | Handle: empty Drive folders, permission denied, deleted files. Handle: unsupported file types, corrupted files, huge files. Add retry logic with exponential backoff for API calls. Build user-friendly error messages for each failure type. Implement timeout handling for long-running extractions. |

### Day 15 (Friday, March 21)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **SEO & meta tags** | Auto-generate SEO meta tags (title, description, OG tags). Add favicon generation from brand colors / logo. Implement Twitter Card & LinkedIn preview meta. Add structured data (JSON-LD) for search engines. Create sitemap.xml per deployment. |
| PM | Tool 2 | **Testing with real client data** | Test complete Drive-to-Landing workflow with 3+ real Drive folders. Validate content extraction accuracy across file types. Compare output quality: Drive-generated vs PDF-generated pages. Document edge cases and quality gaps. Create test suite with sample Drive folders. |

---

## Week 4: Integration, Testing & Production Launch
**March 24 - March 28, 2026**

### Day 16 (Monday, March 24)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **User authentication & multi-tenancy** | Implement user login (email/password or Google SSO). Add workspace/team concept (clients see only their projects). Build role-based access: Admin (all projects) vs Client (own projects only). Implement API key authentication for programmatic access. Add session management and security headers. |
| PM | Tool 2 | **Google Drive permissions & sharing** | Handle various Drive sharing models (viewer, editor, owner). Support "Anyone with link" shared folders. Handle Google Workspace (business) vs personal Drive. Add Drive connection management (connect/disconnect/reauthorize). Implement token refresh and expiration handling. |

### Day 17 (Tuesday, March 25)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Unified dashboard - merge both tools** | Create single dashboard that supports both input types (PDF and Drive). Add "New Project" flow with input type selection: Upload PDF or Connect Drive. Unified project list showing source type icon (PDF icon / Drive icon). Shared deployment, analytics, and lead management across both types. Consistent UX regardless of content source. |
| PM | Tool 2 | **Unified dashboard - Drive integration** | Integrate Drive project flow into shared dashboard. Ensure Drive projects share same template system as PDF projects. Merge analytics pipeline (same Supabase schema, same dashboard). Test cross-tool workflows: start with Drive, re-upload as PDF, etc. |

### Day 18 (Wednesday, March 26)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **End-to-end testing & QA** | Test complete workflow: upload -> generate -> preview -> edit -> deploy. Test with 10+ diverse PDFs (different industries, formats, qualities). Verify analytics tracking on all deployed pages. Test form submissions (Buyer Profile, NDA) and lead pipeline. Cross-browser testing (Chrome, Safari, Firefox, Edge). |
| PM | Tool 2 | **End-to-end testing & QA** | Test complete workflow: Drive link -> parse -> generate -> deploy. Test with 5+ diverse Drive folders. Verify all file types parsed correctly (Docs, Sheets, Slides, Images, PDFs). Test Drive sync / change detection. Verify generated pages match quality of PDF-generated pages. |

### Day 19 (Thursday, March 27)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Tool 1 | **Documentation & onboarding** | Write user guide: how to use the PDF tool step-by-step. Create video walkthrough script (record tool demo). Document API endpoints for programmatic usage. Write troubleshooting guide (common issues + fixes). Create sample PDF templates for testing. |
| PM | Tool 2 | **Documentation & onboarding** | Write user guide: how to use the Drive tool step-by-step. Document Drive folder structure best practices (how to organize files for best results). Create sample Drive folder template. Write comparison guide: when to use PDF tool vs Drive tool. Document Google API quotas and limitations. |

### Day 20 (Friday, March 28)

| Block | Tool | Task | Details |
|-------|------|------|---------|
| AM | Both | **Production deployment & monitoring** | Deploy unified application to production (Vercel). Configure production environment variables. Set up error monitoring (Sentry or similar). Set up uptime monitoring. Configure rate limiting and abuse prevention. |
| PM | Both | **Launch readiness & handoff** | Final QA pass on both tools. Create client demo environment with sample projects. Prepare launch checklist. Conduct walkthrough with stakeholders. Document known limitations and future enhancement roadmap. |

---

## Deliverables Summary

### End of Week 1
- [ ] PDF tool: Template system with 3 base templates, improved PDF processing
- [ ] Drive tool: Google API connected, parsers for Docs, Sheets, Slides working

### End of Week 2
- [ ] PDF tool: Live preview editor, analytics auto-provisioning, form builder
- [ ] Drive tool: Full content extraction pipeline, landing page generation, project wizard UI

### End of Week 3
- [ ] PDF tool: Admin dashboard, lead management, chart system, mobile optimization
- [ ] Drive tool: Drive sync, multi-folder support, Sheets-to-chart mapping, error handling

### End of Week 4
- [ ] Both tools: Unified dashboard, user auth, full testing, documentation, production deployment

---

## Technical Architecture

```
+--------------------------------------------------+
|              Unified Dashboard (Next.js)          |
|                                                   |
|  +-------------------+  +---------------------+  |
|  | Tool 1: PDF Input |  | Tool 2: Drive Input |  |
|  |                   |  |                     |  |
|  | - PDF Upload      |  | - OAuth Connect     |  |
|  | - PDF Parse       |  | - Folder Traverse   |  |
|  | - Text Extract    |  | - Docs Parser       |  |
|  +--------+----------+  | - Sheets Parser     |  |
|           |              | - Slides Parser     |  |
|           |              | - Image Extract     |  |
|           |              +----------+----------+  |
|           |                         |             |
|           v                         v             |
|  +--------------------------------------------+  |
|  |       Content Normalization Layer           |  |
|  |  (Unified JSON: sections, metrics, media)   |  |
|  +---------------------+----------------------+  |
|                         |                         |
|                         v                         |
|  +--------------------------------------------+  |
|  |         Claude AI Generation Engine         |  |
|  |  - Data Extraction Prompt                   |  |
|  |  - Template Selection                       |  |
|  |  - HTML Generation                          |  |
|  |  - Chart Configuration                      |  |
|  +---------------------+----------------------+  |
|                         |                         |
|                         v                         |
|  +--------------------------------------------+  |
|  |         Preview & Editing Layer             |  |
|  |  - Live Preview    - Section Editor         |  |
|  |  - Code Editor     - Chart Configurator     |  |
|  +---------------------+----------------------+  |
|                         |                         |
|                         v                         |
|  +--------------------------------------------+  |
|  |         Deployment Pipeline                 |  |
|  |  - GitHub Repo     - Vercel Deploy          |  |
|  |  - Custom Domain   - Analytics Setup        |  |
|  |  - Form Injection  - Supabase Tables        |  |
|  +--------------------------------------------+  |
+--------------------------------------------------+
```

---

## Risk & Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| Google API rate limits | Drive tool slows down | Implement caching, batch requests, respect quotas |
| Claude AI output inconsistency | Landing page quality varies | Structured prompts, validation layer, regeneration option |
| Large Drive folders (100+ files) | Timeout / memory issues | Pagination, selective parsing, file size limits |
| PDF quality variations | Poor text extraction | OCR fallback, manual content editor |
| Vercel deployment limits | Can't deploy new pages | Monitor usage, implement queuing system |

---

## Resource Requirements

| Resource | Purpose |
|----------|---------|
| Anthropic API Key | Claude AI for content analysis & generation |
| Google Cloud Project | Drive API, Docs API, Sheets API, Slides API |
| GitHub Account | Repository creation for each landing page |
| Vercel Account | Hosting & deployment for generated pages |
| Supabase Instance | Analytics & lead management database |

---

*This document will be updated as development progresses. Weekly status reviews are recommended to track progress and adjust priorities.*
