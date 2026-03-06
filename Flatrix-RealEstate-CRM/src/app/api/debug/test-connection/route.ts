import { NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

export async function GET() {
  try {
    // Check environment variables
    const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
    const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
    const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY;

    console.log('[TEST] Environment check:', {
      hasUrl: !!supabaseUrl,
      hasServiceKey: !!supabaseServiceKey,
      hasAnonKey: !!supabaseAnonKey,
      urlValue: supabaseUrl?.substring(0, 30) + '...' || 'MISSING',
    });

    if (!supabaseUrl || !supabaseServiceKey) {
      return NextResponse.json({
        success: false,
        error: 'Missing environment variables',
        details: {
          hasUrl: !!supabaseUrl,
          hasServiceKey: !!supabaseServiceKey,
          hasAnonKey: !!supabaseAnonKey
        }
      }, { status: 500 });
    }

    // Try to create Supabase client
    const supabase = createClient(supabaseUrl, supabaseServiceKey);

    // Test 1: Count users
    const { count: userCount, error: userError } = await supabase
      .from('flatrix_users')
      .select('*', { count: 'exact', head: true });

    // Test 2: Count leads
    const { count: leadCount, error: leadError } = await supabase
      .from('flatrix_leads')
      .select('*', { count: 'exact', head: true });

    // Test 3: Count deals
    const { count: dealCount, error: dealError } = await supabase
      .from('flatrix_deals')
      .select('*', { count: 'exact', head: true });

    // Test 4: Get one user
    const { data: sampleUser, error: sampleUserError } = await supabase
      .from('flatrix_users')
      .select('id, email, name, role')
      .limit(1)
      .single();

    return NextResponse.json({
      success: true,
      environment: {
        hasUrl: !!supabaseUrl,
        hasServiceKey: !!supabaseServiceKey,
        hasAnonKey: !!supabaseAnonKey,
        nodeEnv: process.env.NODE_ENV,
      },
      databaseTests: {
        users: {
          count: userCount,
          error: userError?.message || null
        },
        leads: {
          count: leadCount,
          error: leadError?.message || null
        },
        deals: {
          count: dealCount,
          error: dealError?.message || null
        },
        sampleUser: {
          found: !!sampleUser,
          email: sampleUser?.email || null,
          error: sampleUserError?.message || null
        }
      }
    });

  } catch (error: any) {
    console.error('[TEST] Error:', error);
    return NextResponse.json({
      success: false,
      error: error.message,
      stack: error.stack
    }, { status: 500 });
  }
}
