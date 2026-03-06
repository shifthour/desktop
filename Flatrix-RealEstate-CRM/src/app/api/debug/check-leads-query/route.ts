import { NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET() {
  try {
    const leadIds = [
      '90a1d950-8f80-475c-9ed2-43ffe4f74f2c', // Naresh
      '1c489852-dd11-4f47-b535-40568f6ed4a5', // Aparna
      'd0768aa6-daf4-448c-9dcf-1f7f227ac1ed'  // Sujeet
    ];

    // Test 1: Simple query without joins
    const { data: simpleLeads, error: simpleError } = await supabase
      .from('flatrix_leads')
      .select('*')
      .in('id', leadIds)
      .order('created_at', { ascending: false });

    if (simpleError) {
      console.error('Simple query error:', simpleError);
    }

    // Test 2: Query with joins (same as useLeads hook)
    const { data: joinedLeads, error: joinedError } = await supabase
      .from('flatrix_leads')
      .select(`
        *,
        assigned_to:flatrix_users!assigned_to_id(name),
        created_by:flatrix_users!created_by_id(name),
        channel_partner:flatrix_channel_partners!channel_partner_id(company_name)
      `)
      .in('id', leadIds)
      .order('created_at', { ascending: false });

    if (joinedError) {
      console.error('Joined query error:', joinedError);
    }

    // Test 3: Check if users exist
    const allUserIds = [
      ...(simpleLeads?.map(l => l.assigned_to_id).filter(Boolean) || []),
      ...(simpleLeads?.map(l => l.created_by_id).filter(Boolean) || [])
    ];
    const userIds = Array.from(new Set(allUserIds));

    const { data: users, error: usersError } = await supabase
      .from('flatrix_users')
      .select('id, name, email')
      .in('id', userIds);

    if (usersError) {
      console.error('Users query error:', usersError);
    }

    // Test 4: Check channel partners
    const cpIds = simpleLeads?.map(l => l.channel_partner_id).filter(Boolean) || [];

    const { data: channelPartners, error: cpError } = cpIds.length > 0 ? await supabase
      .from('flatrix_channel_partners')
      .select('id, company_name')
      .in('id', cpIds) : { data: [], error: null };

    if (cpError) {
      console.error('Channel partners query error:', cpError);
    }

    return NextResponse.json({
      success: true,
      tests: {
        simpleQuery: {
          count: simpleLeads?.length || 0,
          leads: simpleLeads?.map(l => ({
            id: l.id,
            name: l.name,
            assigned_to_id: l.assigned_to_id,
            created_by_id: l.created_by_id,
            channel_partner_id: l.channel_partner_id
          })),
          error: simpleError
        },
        joinedQuery: {
          count: joinedLeads?.length || 0,
          leads: joinedLeads?.map(l => ({
            id: l.id,
            name: (l as any).name,
            assigned_to: (l as any).assigned_to,
            created_by: (l as any).created_by,
            channel_partner: (l as any).channel_partner
          })),
          error: joinedError
        },
        relatedData: {
          users: users?.map(u => ({ id: u.id, name: u.name })),
          channelPartners: channelPartners?.map(cp => ({ id: cp.id, company_name: cp.company_name })),
          usersError,
          cpError
        }
      }
    });

  } catch (error: any) {
    console.error('[DEBUG] Error:', error);
    return NextResponse.json({
      success: false,
      error: error.message
    }, { status: 500 });
  }
}
