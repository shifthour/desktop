import { NextRequest, NextResponse } from 'next/server';

/**
 * Vercel Cron Job Endpoint for Housing.com Lead Fetching
 *
 * This endpoint is triggered automatically by Vercel Cron daily at 9 AM UTC
 * to fetch new leads from Housing.com and sync them to the CRM.
 *
 * Schedule: 0 9 * * * (daily at 9 AM UTC)
 * Configured in: vercel.json
 *
 * Security: Uses Vercel CRON_SECRET for authentication
 */

export async function GET(request: NextRequest) {
  try {
    // Verify this request is from Vercel Cron
    const authHeader = request.headers.get('authorization');
    const cronSecret = process.env.CRON_SECRET;

    if (!cronSecret) {
      console.error('CRON_SECRET not configured');
      return NextResponse.json(
        { error: 'Cron job not configured properly' },
        { status: 500 }
      );
    }

    // Vercel Cron sends: Authorization: Bearer <CRON_SECRET>
    if (authHeader !== `Bearer ${cronSecret}`) {
      console.error('Unauthorized cron request');
      return NextResponse.json(
        { error: 'Unauthorized' },
        { status: 401 }
      );
    }

    console.log('🕐 Cron job triggered - fetching Housing.com leads...');

    // Call our existing fetch-leads endpoint
    const baseUrl = process.env.NEXT_PUBLIC_APP_URL || 'https://flatrix-68ymyewwv-shifthourjobs-gmailcoms-projects.vercel.app';
    const fetchUrl = `${baseUrl}/api/integrations/housing/fetch-leads`;

    const response = await fetch(fetchUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': process.env.HOUSING_FETCH_API_KEY || '',
        'x-cron-trigger': 'true', // Mark this as a cron-triggered request
      },
      body: JSON.stringify({}), // Empty body = use last sync time logic
    });

    const data = await response.json();

    if (!response.ok) {
      console.error('❌ Cron job failed:', data);
      return NextResponse.json(
        {
          success: false,
          error: data.error || 'Failed to fetch leads',
          timestamp: new Date().toISOString()
        },
        { status: response.status }
      );
    }

    console.log('✅ Cron job completed successfully:', data);

    return NextResponse.json({
      success: true,
      message: 'Housing.com leads fetched successfully',
      data,
      timestamp: new Date().toISOString(),
    });

  } catch (error: any) {
    console.error('❌ Cron job error:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Internal server error',
        timestamp: new Date().toISOString()
      },
      { status: 500 }
    );
  }
}

// Optionally support POST as well
export async function POST(request: NextRequest) {
  return GET(request);
}
