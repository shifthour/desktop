import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

// Initialize Supabase client with service role key for admin access
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

// Middleware to validate API key
function validateApiKey(request: NextRequest): boolean {
  const apiKey = request.headers.get('x-api-key') || request.headers.get('authorization')?.replace('Bearer ', '');
  const validApiKey = process.env.MAGICBRICKS_API_KEY;

  if (!validApiKey) {
    console.error('MAGICBRICKS_API_KEY not configured in environment variables');
    return false;
  }

  return apiKey === validApiKey;
}

// Interface for MagicBricks lead payload
interface MagicBricksLead {
  // Common fields from MagicBricks
  name?: string;
  first_name?: string;
  last_name?: string;
  email?: string;
  phone?: string;
  mobile?: string;
  alternate_phone?: string;
  message?: string;
  comments?: string;
  budget?: number | string;
  budget_min?: number | string;
  budget_max?: number | string;
  property_type?: string;
  location?: string;
  preferred_location?: string;
  city?: string;
  project_name?: string;
  // Additional metadata
  lead_id?: string;
  source?: string;
  timestamp?: string;
  [key: string]: any; // Allow additional fields
}

// POST endpoint to receive leads from MagicBricks
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
    const body: MagicBricksLead = await request.json();

    // Validate required fields
    if (!body.phone && !body.mobile) {
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
    const phone = body.phone || body.mobile || '';
    const fullName = body.name ||
                     (body.first_name && body.last_name ? `${body.first_name} ${body.last_name}` : body.first_name) ||
                     'Unknown Lead';
    const email = body.email || null;
    const budgetValue = body.budget ? parseFloat(String(body.budget)) : null;
    const budgetMin = body.budget_min ? parseFloat(String(body.budget_min)) : budgetValue;
    const budgetMax = body.budget_max ? parseFloat(String(body.budget_max)) : budgetValue;
    const preferredLocation = body.preferred_location || body.location || body.city || null;
    const projectName = body.project_name || null;

    // Map property type to PropertyType enum
    const propertyTypeMapping: { [key: string]: string } = {
      'apartment': 'APARTMENT',
      'flat': 'APARTMENT',
      'villa': 'VILLA',
      'independent house': 'VILLA',
      'plot': 'PLOT',
      'land': 'PLOT',
      'commercial': 'COMMERCIAL',
      'office': 'OFFICE_SPACE',
      'office space': 'OFFICE_SPACE'
    };

    const interestedIn: string[] = [];
    if (body.property_type) {
      const normalizedType = body.property_type.toLowerCase();
      const mappedType = propertyTypeMapping[normalizedType];
      if (mappedType) {
        interestedIn.push(mappedType);
      }
    }

    // Prepare notes from message/comments
    const notesArray: string[] = [];
    if (body.message) notesArray.push(`Message: ${body.message}`);
    if (body.comments) notesArray.push(`Comments: ${body.comments}`);
    if (body.lead_id) notesArray.push(`MagicBricks Lead ID: ${body.lead_id}`);

    // Add timestamp
    const timestamp = body.timestamp || new Date().toISOString();
    notesArray.push(`Received from MagicBricks at: ${timestamp}`);

    // Add any additional metadata
    const additionalFields = Object.keys(body)
      .filter(key => !['name', 'first_name', 'last_name', 'email', 'phone', 'mobile',
                       'alternate_phone', 'message', 'comments', 'budget', 'budget_min',
                       'budget_max', 'property_type', 'location', 'preferred_location',
                       'city', 'lead_id', 'source', 'timestamp', 'project_name'].includes(key))
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
        ? `${existingLead.notes}\n\n--- Updated from MagicBricks ---\n${notes}`
        : notes;

      const { data: updatedLead, error: updateError } = await supabase
        .from('flatrix_leads')
        .update({
          name: fullName,
          email: email || existingLead.email,
          budget_min: budgetMin || existingLead.budget_min,
          budget_max: budgetMax || existingLead.budget_max,
          preferred_location: preferredLocation || existingLead.preferred_location,
          interested_in: interestedIn.length > 0 ? interestedIn : existingLead.interested_in,
          project_name: projectName || existingLead.project_name,
          notes: updatedNotes,
          source: 'MagicBricks',
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
          description: 'Lead updated from MagicBricks integration',
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
          source: 'MagicBricks',
          status: 'NEW',
          status_of_save: 'MagicBricks',
          budget_min: budgetMin,
          budget_max: budgetMax,
          notes,
          interested_in: interestedIn.length > 0 ? interestedIn : null,
          preferred_location: preferredLocation,
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
          description: 'New lead received from MagicBricks integration',
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
    console.error('Error processing MagicBricks lead:', error);

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
    message: 'MagicBricks integration endpoint is active',
    endpoint: '/api/integrations/magicbricks',
    method: 'POST',
    requiredHeaders: {
      'Content-Type': 'application/json',
      'x-api-key': 'your-api-key'
    },
    requiredFields: ['phone'],
    optionalFields: [
      'name', 'first_name', 'last_name', 'email', 'mobile',
      'alternate_phone', 'message', 'comments', 'budget',
      'budget_min', 'budget_max', 'property_type', 'location',
      'preferred_location', 'city', 'project_name'
    ],
    databaseSchema: 'flatrix_leads table with Supabase'
  });
}
