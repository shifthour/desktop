# Anahata App Deployment Workflow

This document explains the workflow for deploying the Anahata landing page within the Ishtika Homes portfolio site.

## Overview

The Anahata landing page is a separate Next.js application that needs to be built and integrated into the main portfolio site before deployment.

## Directory Structure

- **Anahata Next.js Source**: `/Users/safestorage/Desktop/anhata-landing/`
- **Portfolio Site**: `/Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/`
- **Anahata Build Output**: `/Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/public/anahata/`

## Deployment Steps

### 1. Build the Anahata Next.js App

First, navigate to the Anahata source directory and build the app:

```bash
cd /Users/safestorage/Desktop/anhata-landing/
npm run build
```

This will generate static HTML files in the `out/` directory.

### 2. Copy Built Files to Portfolio

Copy the built static files from the Anahata `out/` directory to the portfolio's public directory:

```bash
rsync -av --delete /Users/safestorage/Desktop/anhata-landing/out/ /Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/public/anahata/
```

The `--delete` flag ensures old build artifacts are removed, keeping the deployment clean.

### 3. Commit Changes

Navigate to the portfolio directory and commit the changes:

```bash
cd /Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs
git add -A
git commit -m "Update anahata landing page build"
```

### 4. Deploy to Vercel

Deploy the updated portfolio to Vercel:

```bash
vercel --prod --yes
```

## Important Notes

- **Build Order**: Always build the Anahata app BEFORE deploying the portfolio
- **File Size**: The anahata directory should be around 131MB. If it exceeds this significantly, check for:
  - Duplicate nested directories (like `public/anahata/anahata/`)
  - Large backup files (like `.zip` files)
  - Duplicate video files
- **Vercel Configuration**:
  - The `.vercelignore` file excludes the `public/` directory from the serverless function bundle
  - Static files in `public/` are served separately via the `@vercel/static` build config in `vercel.json`
- **URL Structure**: The anahata app is accessible at `https://your-domain.com/anahata/`

## Troubleshooting

### Vercel Size Limit Errors

If you see errors like "Serverless Function has exceeded the maximum size of 250 MB":

1. Check the size of `public/anahata/`: `du -sh public/anahata`
2. Look for large files: `find public/anahata -type f -size +10M -exec ls -lh {} \;`
3. Ensure `.vercelignore` includes `public/` directory
4. Remove any unnecessary backup files or duplicate directories

### Explaining to Claude in Future Sessions

When working with Claude in future sessions, you can provide this context:

> "The Anahata landing page is a separate Next.js app located at `/Users/safestorage/Desktop/anhata-landing/`.
> Before deploying the portfolio, you need to:
> 1. Build the anahata app: `cd /Users/safestorage/Desktop/anhata-landing/ && npm run build`
> 2. Copy the built files: `rsync -av --delete /Users/safestorage/Desktop/anhata-landing/out/ /Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/public/anahata/`
> 3. Then deploy the portfolio from `/Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/`
>
> Please refer to the ANAHATA-DEPLOYMENT-WORKFLOW.md file for the complete workflow."

## Quick Command Reference

```bash
# Full deployment workflow
cd /Users/safestorage/Desktop/anhata-landing && npm run build
rsync -av --delete /Users/safestorage/Desktop/anhata-landing/out/ /Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs/public/anahata/
cd /Users/safestorage/Desktop/Istika-Branding/ishtika-portfolio-nodejs
git add -A && git commit -m "Update anahata build"
vercel --prod --yes
```

## Production URL

After deployment, the site will be available at your Vercel production URL (e.g., https://ishtika-branding-g5gligb5s-shifthourjobs-gmailcoms-projects.vercel.app)
