import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';
import crypto from 'crypto';

// Initialize Supabase client with service role key for admin access
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Housing.com API configuration
const HOUSING_API_URL = 'https://pahal.housing.com/api/v0/get-builder-leads';
const HOUSING_PROFILE_ID = process.env.HOUSING_PROFILE_ID!;
const HOUSING_ENCRYPTION_KEY = process.env.HOUSING_ENCRYPTION_KEY!;

// Middleware to validate API key for this endpoint
function validateApiKey(request: NextRequest): boolean {
  const apiKey = request.headers.get('x-api-key') || request.headers.get('authorization')?.replace('Bearer ', '');
  const validApiKey = process.env.HOUSING_FETCH_API_KEY;

  console.log('[HOUSING AUTH] Validating API key:', {
    hasApiKey: !!apiKey,
    hasValidApiKey: !!validApiKey,
    apiKeyLength: apiKey?.length || 0,
    validApiKeyLength: validApiKey?.length || 0,
    apiKeyPreview: apiKey?.substring(0, 10) + '...',
    validApiKeyPreview: validApiKey?.substring(0, 10) + '...',
    matches: apiKey === validApiKey
  });

  if (!validApiKey) {
    console.error('[HOUSING AUTH] HOUSING_FETCH_API_KEY not configured in environment variables');
    return false;
  }

  if (!apiKey) {
    console.error('[HOUSING AUTH] No API key provided in request headers');
    return false;
  }

  const matches = apiKey === validApiKey;
  if (!matches) {
    console.error('[HOUSING AUTH] API key mismatch');
  }

  return matches;
}

// Validate Vercel Cron secret
function validateCronSecret(request: NextRequest): boolean {
  const authHeader = request.headers.get('authorization');
  const cronSecret = process.env.CRON_SECRET;

  if (!cronSecret) {
    // If no cron secret is set, allow (for backwards compatibility)
    return true;
  }

  return authHeader === `Bearer ${cronSecret}`;
}

// Generate HMAC SHA256 hash as per Housing.com documentation
function generateHash(currentTime: string, encryptionKey: string): string {
  return crypto
    .createHmac('sha256', encryptionKey)
    .update(currentTime)
    .digest('hex');
}

// Get last sync time from database
async function getLastSyncTime(): Promise<number | null> {
  try {
    const { data, error } = await supabase
      .from('flatrix_integration_sync_log')
      .select('last_sync_time')
      .eq('integration_name', 'Housing.com')
      .single();

    if (error || !data) {
      console.log('No previous sync found, will fetch last 24 hours');
      return null;
    }

    const lastSync = new Date(data.last_sync_time).getTime() / 1000;
    console.log('Last sync time:', new Date(data.last_sync_time).toISOString());
    return lastSync;
  } catch (error) {
    console.log('Sync log table might not exist, using 24h default');
    return null;
  }
}

// Update last sync time in database
async function updateLastSyncTime(leadsProcessed: number, leadsCreated: number, leadsUpdated: number, status: string = 'success', errorMessage?: string) {
  try {
    const now = new Date().toISOString();

    const { error } = await supabase
      .from('flatrix_integration_sync_log')
      .upsert({
        integration_name: 'Housing.com',
        last_sync_time: now,
        leads_fetched: leadsProcessed,
        leads_created: leadsCreated,
        leads_updated: leadsUpdated,
        status,
        error_message: errorMessage || null,
        updated_at: now
      }, {
        onConflict: 'integration_name'
      });

    if (error) {
      console.error('Failed to update sync time:', error);
    } else {
      console.log('✓ Sync time updated successfully');
    }
  } catch (error) {
    console.error('Error updating sync time:', error);
  }
}

// Interface for Housing.com lead response
interface HousingLead {
  lead_name: string;
  lead_phone: string;
  lead_email?: string | null;
  project_id?: number;
  flat_id?: number;
  project_name?: string;
  locality?: string;
  lead_date: string; // Epoch timestamp
  pg_name?: string;
  service_type?: string; // rent / resale / new-projects
  apartment_names?: string;
}

interface HousingApiResponse {
  data?: HousingLead[];
  apiErrors?: any;
}

// POST endpoint to fetch leads from Housing.com
export async function POST(request: NextRequest) {
  try {
    // Validate API key OR Cron secret
    const isValidApiKey = validateApiKey(request);
    const isValidCron = validateCronSecret(request);

    if (!isValidApiKey && !isValidCron) {
      return NextResponse.json(
        {
          success: false,
          error: 'Unauthorized - Invalid API key or cron secret',
          message: 'Please provide a valid API key in x-api-key header'
        },
        { status: 401 }
      );
    }

    // Parse request body for optional parameters
    const body = await request.json().catch(() => ({}));
    const {
      start_date,
      end_date,
      per_page = 100,
      project_ids,
      apartment_names,
      force_timerange = false // Allow manual override of timerange
    } = body;

    // Get system user ID
    const systemUserId = process.env.SYSTEM_USER_ID;
    if (!systemUserId) {
      return NextResponse.json(
        {
          success: false,
          error: 'Configuration error',
          message: 'SYSTEM_USER_ID not configured'
        },
        { status: 500 }
      );
    }

    // Validate Housing.com credentials
    if (!HOUSING_PROFILE_ID || !HOUSING_ENCRYPTION_KEY) {
      return NextResponse.json(
        {
          success: false,
          error: 'Configuration error',
          message: 'Housing.com credentials not configured (HOUSING_PROFILE_ID or HOUSING_ENCRYPTION_KEY missing)'
        },
        { status: 500 }
      );
    }

    // Calculate timestamps
    const currentTime = Math.floor(Date.now() / 1000).toString(); // Current time in epoch
    let startTime: string;
    let endTime: string;

    if (force_timerange && start_date && end_date) {
      // Manual timerange override
      startTime = start_date;
      endTime = end_date;
      console.log('Using manual timerange override');
    } else {
      // Always fetch last 24 hours (sync log logic causes Housing.com API 400 errors)
      // Housing.com API handles deduplication on their end
      startTime = (parseInt(currentTime) - 86400).toString();
      endTime = currentTime;
      console.log('Fetching last 24 hours');
    }

    // Generate hash
    const hash = generateHash(currentTime, HOUSING_ENCRYPTION_KEY);

    // Build API URL
    const params = new URLSearchParams({
      start_date: startTime,
      end_date: endTime,
      current_time: currentTime,
      hash: hash,
      id: HOUSING_PROFILE_ID,
      per_page: per_page.toString()
    });

    if (project_ids) {
      params.append('project_ids', project_ids);
    }

    if (apartment_names) {
      params.append('apartment_names', apartment_names);
    }

    const apiUrl = `${HOUSING_API_URL}?${params.toString()}`;

    console.log('Fetching leads from Housing.com API...');
    console.log('Time range:', new Date(parseInt(startTime) * 1000).toISOString(), 'to', new Date(parseInt(endTime) * 1000).toISOString());

    // Fetch leads from Housing.com
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'cache-control': 'no-cache'
      }
    });

    if (!response.ok) {
      console.error('Housing.com API error:', response.status, response.statusText);
      await updateLastSyncTime(0, 0, 0, 'error', `API returned status ${response.status}`);

      return NextResponse.json(
        {
          success: false,
          error: 'Housing.com API error',
          message: `API returned status ${response.status}: ${response.statusText}`
        },
        { status: response.status }
      );
    }

    const housingData: HousingApiResponse = await response.json();

    // Check for API errors
    if (housingData.apiErrors) {
      console.error('Housing.com API errors:', housingData.apiErrors);
      await updateLastSyncTime(0, 0, 0, 'error', JSON.stringify(housingData.apiErrors));

      return NextResponse.json(
        {
          success: false,
          error: 'Housing.com API error',
          message: 'API returned errors',
          details: housingData.apiErrors
        },
        { status: 400 }
      );
    }

    const leads = housingData.data || [];
    console.log(`Received ${leads.length} leads from Housing.com`);

    if (leads.length === 0) {
      await updateLastSyncTime(0, 0, 0, 'success');

      return NextResponse.json({
        success: true,
        message: 'No new leads found',
        leadsProcessed: 0,
        leadsCreated: 0,
        leadsUpdated: 0,
        timeRange: {
          start: new Date(parseInt(startTime) * 1000).toISOString(),
          end: new Date(parseInt(endTime) * 1000).toISOString()
        }
      });
    }

    let leadsCreated = 0;
    let leadsUpdated = 0;
    const errors: any[] = [];

    // Process each lead
    for (const housingLead of leads) {
      try {
        // Validate required fields
        if (!housingLead.lead_phone) {
          console.warn('Skipping lead without phone number:', housingLead);
          continue;
        }

        const phone = housingLead.lead_phone.trim();
        const name = housingLead.lead_name || 'Unknown Lead';
        const email = housingLead.lead_email || null;

        // Notes are intentionally NOT created for Housing.com leads
        // Data is synced every 10 minutes via cron, and notes should remain empty

        // Determine property type and location
        const preferredLocation = housingLead.locality || null;
        const projectName = housingLead.project_name || 'Not Specified';

        // Map apartment_names to interested_in
        const interestedIn: string[] = [];
        if (housingLead.apartment_names) {
          const apartmentTypes = housingLead.apartment_names.toLowerCase();
          if (apartmentTypes.includes('1 bhk') || apartmentTypes.includes('2 bhk') || apartmentTypes.includes('3 bhk') || apartmentTypes.includes('bhk')) {
            interestedIn.push('APARTMENT');
          }
          if (apartmentTypes.includes('villa')) {
            interestedIn.push('VILLA');
          }
          if (apartmentTypes.includes('plot')) {
            interestedIn.push('PLOT');
          }
        }

        // Check if lead with same phone already exists
        const { data: existingLeads, error: searchError } = await supabase
          .from('flatrix_leads')
          .select('*')
          .eq('phone', phone)
          .limit(1);

        if (searchError) {
          console.error('Error searching for existing lead:', searchError);
          errors.push({ phone, error: 'Failed to search for existing lead' });
          continue;
        }

        const existingLead = existingLeads && existingLeads.length > 0 ? existingLeads[0] : null;

        if (existingLead) {
          // Update existing lead (skip notes update to avoid duplicates)
          const { error: updateError } = await supabase
            .from('flatrix_leads')
            .update({
              name,
              email: email || existingLead.email,
              preferred_location: preferredLocation || existingLead.preferred_location,
              interested_in: interestedIn.length > 0 ? interestedIn : existingLead.interested_in,
              project_name: projectName,
              // Notes are NOT updated for existing leads to avoid duplication
              source: 'Housing.com',
              updated_at: new Date().toISOString(),
            })
            .eq('id', existingLead.id);

          if (updateError) {
            console.error('Error updating lead:', updateError);
            errors.push({ phone, error: 'Failed to update lead' });
            continue;
          }

          // Log activity
          await supabase
            .from('flatrix_activities')
            .insert({
              type: 'LEAD_UPDATED',
              description: 'Lead updated from Housing.com integration',
              lead_id: existingLead.id,
              user_id: systemUserId,
              metadata: housingLead,
              created_at: new Date().toISOString(),
            });

          leadsUpdated++;
        } else {
          // Get next user for round-robin assignment
          const { data: nextUserId, error: rotationError } = await supabase
            .rpc('get_next_assignee');

          // Create new lead (without notes - intentionally left empty)
          const { data: newLead, error: createError } = await supabase
            .from('flatrix_leads')
            .insert({
              name,
              phone,
              email,
              source: 'Housing.com',
              status: 'NEW',
              status_of_save: 'Housing.com',
              // notes field is intentionally omitted - will remain NULL
              interested_in: interestedIn.length > 0 ? interestedIn : null,
              preferred_location: preferredLocation,
              project_name: projectName,
              created_by_id: systemUserId,
              assigned_to_id: nextUserId || null, // Auto-assign to next user in rotation
              created_at: new Date().toISOString(),
              updated_at: new Date().toISOString(),
            })
            .select()
            .single();

          if (createError) {
            console.error('Error creating lead:', createError);
            errors.push({ phone, error: `Failed to create lead: ${createError.message}` });
            continue;
          }

          // Log activity
          await supabase
            .from('flatrix_activities')
            .insert({
              type: 'LEAD_CREATED',
              description: `New lead received from Housing.com integration${nextUserId ? ' and auto-assigned' : ''}`,
              lead_id: newLead.id,
              user_id: systemUserId,
              metadata: housingLead,
              created_at: new Date().toISOString(),
            });

          leadsCreated++;
        }
      } catch (leadError: any) {
        console.error('Error processing lead:', leadError);
        errors.push({ phone: housingLead.lead_phone, error: leadError.message });
      }
    }

    // Update sync log with results
    await updateLastSyncTime(leads.length, leadsCreated, leadsUpdated, 'success');

    return NextResponse.json({
      success: true,
      message: 'Leads fetched and processed successfully',
      leadsProcessed: leads.length,
      leadsCreated,
      leadsUpdated,
      errors: errors.length > 0 ? errors : undefined,
      timeRange: {
        start: new Date(parseInt(startTime) * 1000).toISOString(),
        end: new Date(parseInt(endTime) * 1000).toISOString()
      }
    });

  } catch (error: any) {
    console.error('Error fetching Housing.com leads:', error);
    await updateLastSyncTime(0, 0, 0, 'error', error.message);

    return NextResponse.json(
      {
        success: false,
        error: 'Internal server error',
        message: error.message || 'Failed to fetch leads'
      },
      { status: 500 }
    );
  }
}

// GET endpoint for testing/status
export async function GET(request: NextRequest) {
  // Validate API key
  if (!validateApiKey(request)) {
    return NextResponse.json(
      {
        success: false,
        error: 'Unauthorized',
        message: 'Invalid API key'
      },
      { status: 401 }
    );
  }

  // Get last sync info
  let lastSyncInfo = null;
  try {
    const { data } = await supabase
      .from('flatrix_integration_sync_log')
      .select('*')
      .eq('integration_name', 'Housing.com')
      .single();

    lastSyncInfo = data;
  } catch (error) {
    lastSyncInfo = { message: 'Sync log table not found - will be created on first fetch' };
  }

  return NextResponse.json({
    success: true,
    message: 'Housing.com lead fetching endpoint is active',
    endpoint: '/api/integrations/housing/fetch-leads',
    method: 'POST',
    description: 'Pull-based integration: Fetches leads from Housing.com API',
    configuration: {
      profileId: HOUSING_PROFILE_ID ? 'Configured' : 'Missing',
      encryptionKey: HOUSING_ENCRYPTION_KEY ? 'Configured' : 'Missing',
      systemUserId: process.env.SYSTEM_USER_ID ? 'Configured' : 'Missing',
      cronSecret: process.env.CRON_SECRET ? 'Configured' : 'Not set (optional)'
    },
    lastSync: lastSyncInfo,
    requestBody: {
      start_date: 'Optional - Epoch timestamp (auto-calculated from last sync)',
      end_date: 'Optional - Epoch timestamp (default: now)',
      per_page: 'Optional - Number of records (default: 100, max: 1000)',
      project_ids: 'Optional - Comma-separated project IDs',
      apartment_names: 'Optional - Apartment types (e.g., "1 BHK, 2 BHK")',
      force_timerange: 'Optional - Set to true to use manual start_date/end_date'
    }
  });
}
