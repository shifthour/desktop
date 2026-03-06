import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

// GET - List message logs with pagination and filters
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const page = parseInt(searchParams.get('page') || '1');
    const limit = parseInt(searchParams.get('limit') || '50');
    const status = searchParams.get('status');
    const dateFrom = searchParams.get('date_from');
    const dateTo = searchParams.get('date_to');

    const offset = (page - 1) * limit;

    let query = supabase
      .from('flatrix_whatsapp_logs')
      .select('*', { count: 'exact' })
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1);

    if (status) {
      query = query.eq('status', status);
    }
    if (dateFrom) {
      query = query.gte('created_at', dateFrom);
    }
    if (dateTo) {
      query = query.lte('created_at', dateTo);
    }

    const { data, error, count } = await query;

    if (error) throw error;

    // Get summary stats
    const { data: stats } = await supabase
      .from('flatrix_whatsapp_logs')
      .select('status')
      .gte('created_at', new Date(new Date().setDate(1)).toISOString());

    const summary = {
      total: stats?.length || 0,
      sent: stats?.filter(s => s.status === 'sent').length || 0,
      delivered: stats?.filter(s => s.status === 'delivered').length || 0,
      read: stats?.filter(s => s.status === 'read').length || 0,
      failed: stats?.filter(s => s.status === 'failed').length || 0,
    };

    return NextResponse.json({
      success: true,
      logs: data,
      pagination: {
        page,
        limit,
        total: count || 0,
        totalPages: Math.ceil((count || 0) / limit),
      },
      summary,
    });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}
