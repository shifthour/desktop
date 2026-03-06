import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

// API key for 99acres integration
const API_KEY = process.env.ACRES99_API_KEY || 'flatrix_99acres_2024_secure_key';

console.log('[99ACRES] API Key configured:', {
  hasEnvVar: !!process.env.ACRES99_API_KEY,
  keyPrefix: API_KEY.substring(0, 20) + '...',
  keyLength: API_KEY.length
});

/**
 * Webhook endpoint for receiving leads from 99acres
 * POST /api/integrations/99acres
 *
 * Headers:
 * - x-api-key: API key for authentication
 *
 * Expected payload format (adjust based on 99acres documentation):
 * {
 *   "name": "Full Name",
 *   "email": "email@example.com",
 *   "phone": "9876543210",
 *   "project": "Project Name",
 *   "message": "User message/query",
 *   "budget": "50L-1Cr",
 *   "propertyType": "Apartment/Villa/etc"
 * }
 */
export async function POST(request: NextRequest) {
  try {
    // Parse request body first
    const body = await request.json();

    console.log('[99ACRES] Received request:', {
      body,
      headers: {
        'content-type': request.headers.get('content-type'),
        'x-api-key': request.headers.get('x-api-key') ? 'present' : 'missing'
      }
    });

    // Simple authentication check - accept API key from header OR body
    const headerApiKey = request.headers.get('x-api-key')?.trim();
    const bodyApiKey = body.apiKey || body.api_key;
    const apiKey = headerApiKey || bodyApiKey;

    console.log('[99ACRES] API Key check:', {
      fromHeader: !!headerApiKey,
      fromBody: !!bodyApiKey,
      receivedKey: apiKey,
      expectedKey: API_KEY,
      matches: apiKey === API_KEY,
      lengths: {
        received: apiKey?.length,
        expected: API_KEY.length
      }
    });

    if (!apiKey || apiKey !== API_KEY) {
      console.error('[99ACRES] Authentication failed:', {
        receivedKey: apiKey,
        expectedKey: API_KEY
      });
      return NextResponse.json(
        {
          success: false,
          error: 'Unauthorized',
          message: 'Invalid or missing API key. Please provide x-api-key header or apiKey in request body'
        },
        { status: 401 }
      );
    }

    console.log('[99ACRES] Authentication successful. Processing lead:', body.name, body.phone);

    // Extract lead information
    const {
      name,
      email,
      phone,
      project,
      message,
      budget,
      propertyType,
      city,
      requirement,
      source = '99acres'
    } = body;

    // Validate required fields
    if (!phone) {
      return NextResponse.json(
        {
          success: false,
          error: 'Validation error',
          message: 'Phone number is required'
        },
        { status: 400 }
      );
    }

    // Split name into first and last name
    const nameParts = (name || '').trim().split(' ');
    const firstName = nameParts[0] || 'Unknown';
    const lastName = nameParts.slice(1).join(' ') || '';

    // Check if lead already exists (by phone)
    const { data: existingLead } = await supabase
      .from('flatrix_leads')
      .select('id, name, email, notes')
      .eq('phone', phone)
      .single();

    if (existingLead) {
      console.log('[99ACRES] Lead already exists:', existingLead.id);

      // Update existing lead with new information
      const { error: updateError } = await supabase
        .from('flatrix_leads')
        .update({
          email: email || existingLead.email || null,
          notes: `${existingLead.notes || ''}\n\n99acres Lead Update (${new Date().toLocaleString()}):\nProject: ${project || 'N/A'}\nMessage: ${message || 'N/A'}\nBudget: ${budget || 'N/A'}\nProperty Type: ${propertyType || 'N/A'}`.trim(),
          updated_at: new Date().toISOString()
        })
        .eq('id', existingLead.id);

      if (updateError) {
        console.error('[99ACRES] Error updating lead:', updateError);
        throw updateError;
      }

      return NextResponse.json({
        success: true,
        message: 'Lead updated successfully',
        leadId: existingLead.id,
        action: 'updated'
      });
    }

    // Create new lead
    const leadData = {
      name: name || 'Unknown',
      email: email || null,
      phone: phone,
      source: source,
      status: 'NEW',
      project_name: project || null,
      budget_min: budget ? parseBudget(budget).min : null,
      budget_max: budget ? parseBudget(budget).max : null,
      notes: `99acres Lead (${new Date().toLocaleString()}):\nMessage: ${message || 'N/A'}\nBudget: ${budget || 'N/A'}\nProperty Type: ${propertyType || 'N/A'}`,
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    const { data: newLead, error: insertError } = await supabase
      .from('flatrix_leads')
      .insert(leadData)
      .select()
      .single();

    if (insertError) {
      console.error('[99ACRES] Error creating lead:', insertError);
      throw insertError;
    }

    console.log('[99ACRES] Lead created successfully:', newLead.id);

    return NextResponse.json({
      success: true,
      message: 'Lead created successfully',
      leadId: newLead.id,
      action: 'created'
    });

  } catch (error: any) {
    console.error('[99ACRES] Error processing webhook:', error);
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

/**
 * Parse budget string to min and max values
 * Examples: "50L-1Cr", "1-2Cr", "50-75L"
 */
function parseBudget(budgetStr: string): { min: number | null; max: number | null } {
  if (!budgetStr) return { min: null, max: null };

  try {
    const cleaned = budgetStr.toUpperCase().replace(/[^0-9.\-LCR]/g, '');
    const parts = cleaned.split('-');

    const parseValue = (val: string): number | null => {
      if (!val) return null;

      const num = parseFloat(val.replace(/[LCR]/g, ''));

      if (val.includes('CR')) {
        return num * 10000000; // Crore
      } else if (val.includes('L')) {
        return num * 100000; // Lakh
      }

      return num;
    };

    return {
      min: parseValue(parts[0]),
      max: parts[1] ? parseValue(parts[1]) : parseValue(parts[0])
    };
  } catch (error) {
    console.error('[99ACRES] Error parsing budget:', budgetStr, error);
    return { min: null, max: null };
  }
}

/**
 * GET endpoint to check integration status
 */
export async function GET(request: NextRequest) {
  return NextResponse.json({
    success: true,
    integration: '99acres',
    status: 'active',
    endpoints: {
      webhook: '/api/integrations/99acres'
    },
    authentication: 'x-api-key header required',
    note: 'Use POST method to submit leads'
  });
}
