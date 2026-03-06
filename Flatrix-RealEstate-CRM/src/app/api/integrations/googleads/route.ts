import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

// Initialize Supabase client with service role key for admin access
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Middleware to validate API key
function validateApiKey(request: NextRequest): boolean {
  const apiKey = request.headers.get('x-api-key') || request.headers.get('authorization')?.replace('Bearer ', '');
  const validApiKey = process.env.GOOGLE_ADS_API_KEY;

  if (!validApiKey) {
    console.error('GOOGLE_ADS_API_KEY not configured in environment variables');
    return false;
  }

  return apiKey === validApiKey;
}

// Interface for Google Ads lead payload
interface GoogleAdsLead {
  // Common fields from Google Ads Lead Form Extensions
  name?: string;
  first_name?: string;
  last_name?: string;
  email?: string;
  phone?: string;
  phone_number?: string;
  message?: string;
  comments?: string;
  property_type?: string;
  location?: string;
  city?: string;
  project_name?: string;
  campaign_name?: string;
  ad_group_name?: string;
  form_name?: string;
  gclid?: string;
  // Additional metadata
  lead_id?: string;
  source?: string;
  timestamp?: string;
  [key: string]: any; // Allow additional fields
}

// POST endpoint to receive leads from Google Ads
export async function POST(request: NextRequest) {
  try {
    // Validate API key
    if (!validateApiKey(request)) {
      return NextResponse.json(
        {
          success: false,
          error: 'Unauthorized - Invalid API key',
          message: 'Please provide a valid API key in x-api-key header or Authorization header'
        },
        { status: 401 }
      );
    }

    // Parse request body
    const body: GoogleAdsLead = await request.json();

    // Validate required fields
    if (!body.phone && !body.phone_number) {
      return NextResponse.json(
        {
          success: false,
          error: 'Validation failed',
          message: 'Phone number is required'
        },
        { status: 400 }
      );
    }

    // Extract and normalize data
    const phone = body.phone || body.phone_number || '';
    const fullName = body.name ||
                     (body.first_name && body.last_name ? `${body.first_name} ${body.last_name}` : body.first_name) ||
                     'Unknown Lead';
    const email = body.email || null;
    const location = body.location || body.city || null;
    const projectName = body.project_name || null;

    // Determine source based on form name, campaign name, or default to Google Ads
    let sourceDetail = 'Google Ads';
    if (body.form_name) {
      sourceDetail = body.form_name;
    } else if (body.campaign_name) {
      sourceDetail = `Google Ads - ${body.campaign_name}`;
    }

    // Prepare notes with Google Ads metadata
    const notesArray: string[] = [];
    if (body.message) notesArray.push(`Message: ${body.message}`);
    if (body.comments) notesArray.push(`Comments: ${body.comments}`);
    if (body.campaign_name) notesArray.push(`Campaign: ${body.campaign_name}`);
    if (body.ad_group_name) notesArray.push(`Ad Group: ${body.ad_group_name}`);
    if (body.form_name) notesArray.push(`Form: ${body.form_name}`);
    if (body.gclid) notesArray.push(`GCLID: ${body.gclid}`);
    if (body.lead_id) notesArray.push(`Google Ads Lead ID: ${body.lead_id}`);

    // Add timestamp
    const timestamp = body.timestamp || new Date().toISOString();
    notesArray.push(`Received from Google Ads at: ${timestamp}`);

    // Add any additional metadata
    const additionalFields = Object.keys(body)
      .filter(key => !['name', 'first_name', 'last_name', 'email', 'phone', 'phone_number',
                       'message', 'comments', 'property_type', 'location', 'city',
                       'lead_id', 'source', 'timestamp', 'project_name', 'campaign_name',
                       'ad_group_name', 'form_name', 'gclid'].includes(key))
      .map(key => `${key}: ${body[key]}`)
      .filter(item => item.length > 0);

    if (additionalFields.length > 0) {
      notesArray.push('Additional Info: ' + additionalFields.join(', '));
    }

    const notes = notesArray.join('\n');

    // Get system user ID
    const systemUserId = process.env.SYSTEM_USER_ID;

    if (!systemUserId) {
      return NextResponse.json(
        {
          success: false,
          error: 'Configuration error',
          message: 'SYSTEM_USER_ID not configured. Please set up a system user for lead imports.'
        },
        { status: 500 }
      );
    }

    // Check if lead with same phone already exists
    const { data: existingLeads, error: searchError } = await supabase
      .from('flatrix_leads')
      .select('*')
      .eq('phone', phone)
      .limit(1);

    if (searchError) {
      console.error('Error searching for existing lead:', searchError);
      throw new Error('Failed to search for existing lead');
    }

    const existingLead = existingLeads && existingLeads.length > 0 ? existingLeads[0] : null;
    let lead;

    if (existingLead) {
      // Update existing lead with new information
      const updatedNotes = existingLead.notes
        ? `${existingLead.notes}\n\n--- Updated from Google Ads ---\n${notes}`
        : notes;

      const { data: updatedLead, error: updateError } = await supabase
        .from('flatrix_leads')
        .update({
          name: fullName,
          email: email || existingLead.email,
          preferred_location: location || existingLead.preferred_location,
          project_name: projectName || existingLead.project_name,
          notes: updatedNotes,
          source: 'Google Ads',
          updated_at: new Date().toISOString(),
        })
        .eq('id', existingLead.id)
        .select()
        .single();

      if (updateError) {
        console.error('Error updating lead:', updateError);
        throw new Error('Failed to update lead');
      }

      lead = updatedLead;

      // Log activity for the update
      await supabase
        .from('flatrix_activities')
        .insert({
          type: 'LEAD_UPDATED',
          description: 'Lead updated from Google Ads integration',
          lead_id: lead.id,
          user_id: systemUserId,
          metadata: body,
          created_at: new Date().toISOString(),
        });

      return NextResponse.json(
        {
          success: true,
          message: 'Lead updated successfully',
          leadId: lead.id,
          action: 'updated'
        },
        { status: 200 }
      );
    } else {
      // Get next agent for round-robin assignment
      const { data: nextUserId, error: rotationError } = await supabase
        .rpc('get_next_assignee');

      if (rotationError) {
        console.error('Error getting next assignee:', rotationError);
      }

      // Create new lead
      const { data: newLead, error: createError } = await supabase
        .from('flatrix_leads')
        .insert({
          name: fullName,
          phone,
          email,
          source: 'Google Ads',
          status: 'NEW',
          status_of_save: sourceDetail,
          notes,
          preferred_location: location,
          project_name: projectName || 'Not Specified',
          created_by_id: systemUserId,
          assigned_to_id: nextUserId || null,  // Auto-assign to next agent in rotation
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString(),
        })
        .select()
        .single();

      if (createError) {
        console.error('Error creating lead:', createError);
        throw new Error(`Failed to create lead: ${createError.message}`);
      }

      lead = newLead;

      // Log activity for the new lead
      await supabase
        .from('flatrix_activities')
        .insert({
          type: 'LEAD_CREATED',
          description: `New lead received from Google Ads integration${nextUserId ? ' and auto-assigned' : ''}`,
          lead_id: lead.id,
          user_id: systemUserId,
          metadata: body,
          created_at: new Date().toISOString(),
        });

      return NextResponse.json(
        {
          success: true,
          message: 'Lead created successfully',
          leadId: lead.id,
          action: 'created'
        },
        { status: 201 }
      );
    }
  } catch (error: any) {
    console.error('Error processing Google Ads lead:', error);

    return NextResponse.json(
      {
        success: false,
        error: 'Internal server error',
        message: error.message || 'Failed to process lead'
      },
      { status: 500 }
    );
  }
}

// GET endpoint for testing/verification
export async function GET(request: NextRequest) {
  // Validate API key for GET requests too
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

  return NextResponse.json({
    success: true,
    message: 'Google Ads integration endpoint is active',
    endpoint: '/api/integrations/googleads',
    method: 'POST',
    requiredHeaders: {
      'Content-Type': 'application/json',
      'x-api-key': 'your-api-key'
    },
    requiredFields: ['phone or phone_number'],
    optionalFields: [
      'name', 'first_name', 'last_name', 'email', 'message',
      'comments', 'property_type', 'location', 'city',
      'project_name', 'campaign_name', 'ad_group_name',
      'form_name', 'gclid', 'lead_id'
    ],
    features: [
      'Auto-assignment to agents using round-robin',
      'Duplicate detection by phone number',
      'Campaign and GCLID tracking',
      'Activity logging'
    ],
    databaseSchema: 'flatrix_leads table with Supabase'
  });
}
