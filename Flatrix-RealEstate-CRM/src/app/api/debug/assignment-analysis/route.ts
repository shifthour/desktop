import { NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

export async function GET() {
  try {
    // Get Dinki and Prakash user IDs
    const { data: users, error: usersError } = await supabase
      .from('flatrix_users')
      .select('id, email, name')
      .in('email', ['dinki@flatrix.com', 'prakash@flatrix.com']);

    if (usersError || !users || users.length < 2) {
      return NextResponse.json({
        success: false,
        error: 'Required users not found'
      }, { status: 500 });
    }

    const dinki = users.find(u => u.email === 'dinki@flatrix.com');
    const prakash = users.find(u => u.email === 'prakash@flatrix.com');

    if (!dinki || !prakash) {
      return NextResponse.json({
        success: false,
        error: 'Could not find Dinki or Prakash user'
      }, { status: 500 });
    }

    // Get TODAY's start time
    const todayStart = new Date();
    todayStart.setHours(0, 0, 0, 0);

    // Get all leads assigned to Dinki and Prakash
    const { data: allLeads, error: leadsError } = await supabase
      .from('flatrix_leads')
      .select('id, name, project_name, assigned_to_id, created_at, source')
      .in('assigned_to_id', [dinki.id, prakash.id])
      .order('created_at', { ascending: false });

    // Get TODAY's leads only
    const { data: todaysLeads } = await supabase
      .from('flatrix_leads')
      .select('id, name, project_name, assigned_to_id, created_at, source')
      .in('assigned_to_id', [dinki.id, prakash.id])
      .gte('created_at', todayStart.toISOString())
      .order('created_at', { ascending: false });

    if (leadsError) {
      return NextResponse.json({
        success: false,
        error: leadsError.message
      }, { status: 500 });
    }

    // Categorize TODAY's leads
    const todaysAnahataLeads = (todaysLeads || []).filter(l => {
      const proj = (l.project_name || '').toLowerCase().trim();
      return proj === 'anahata' ||
             proj === 'ishtika anahata' ||
             proj === 'isthika anahata';
    });

    const todaysNonAnahataLeads = (todaysLeads || []).filter(l => {
      const proj = (l.project_name || '').toLowerCase().trim();
      return proj !== 'anahata' &&
             proj !== 'ishtika anahata' &&
             proj !== 'isthika anahata';
    });

    // Count TODAY's assignments
    const dinkiAnahataToday = todaysAnahataLeads.filter(l => l.assigned_to_id === dinki.id).length;
    const prakashAnahataToday = todaysAnahataLeads.filter(l => l.assigned_to_id === prakash.id).length;

    const dinkiNonAnahataToday = todaysNonAnahataLeads.filter(l => l.assigned_to_id === dinki.id).length;
    const prakashNonAnahataToday = todaysNonAnahataLeads.filter(l => l.assigned_to_id === prakash.id).length;

    // Get the last 10 assignments for analysis
    const recentAssignments = (allLeads || []).slice(0, 10).map(l => ({
      id: l.id,
      name: l.name || 'Unknown',
      project: l.project_name || 'N/A',
      assignedTo: l.assigned_to_id === dinki.id ? 'Dinki' : 'Prakash',
      isAnahata: (() => {
        const proj = (l.project_name || '').toLowerCase().trim();
        return proj === 'anahata' ||
               proj === 'ishtika anahata' ||
               proj === 'isthika anahata';
      })(),
      createdAt: l.created_at,
      source: l.source
    }));

    // Calculate expected ratio for TODAY's non-Anahata
    const totalNonAnahataToday = dinkiNonAnahataToday + prakashNonAnahataToday;
    const expectedDinkiToday = Math.floor(totalNonAnahataToday / 3);
    const expectedPrakashToday = totalNonAnahataToday - expectedDinkiToday;

    // Calculate what the next assignment should be using simple modulo logic (for today only)
    const totalTodayAssignments = dinkiNonAnahataToday + prakashNonAnahataToday;
    const position = (totalTodayAssignments + 1) % 3;
    const nextAssignment = position === 1 ? 'Dinki' : 'Prakash';

    return NextResponse.json({
      success: true,
      users: {
        dinki: { id: dinki.id, name: dinki.name },
        prakash: { id: prakash.id, name: prakash.name }
      },
      todaysDate: todayStart.toISOString().split('T')[0],
      todaysAnahataProjects: {
        total: todaysAnahataLeads.length,
        dinki: dinkiAnahataToday,
        prakash: prakashAnahataToday,
        note: 'All Anahata projects should go to Prakash'
      },
      todaysNonAnahataProjects: {
        total: totalNonAnahataToday,
        dinki: dinkiNonAnahataToday,
        prakash: prakashNonAnahataToday,
        expected: {
          dinki: expectedDinkiToday,
          prakash: expectedPrakashToday,
          note: '1:2 ratio (Dinki:Prakash) - out of every 3, 1 to Dinki and 2 to Prakash'
        },
        difference: {
          dinki: dinkiNonAnahataToday - expectedDinkiToday,
          prakash: prakashNonAnahataToday - expectedPrakashToday
        }
      },
      nextAssignment: {
        willGoTo: nextAssignment,
        reason: `Position ${position} in today's cycle (${totalTodayAssignments} today + 1) % 3 = ${position}`,
        note: 'Position 1 → Dinki, Position 0 or 2 → Prakash'
      },
      recentAssignments,
      analysis: {
        anahataCorrect: dinkiAnahataToday === 0,
        ratioCorrect: Math.abs(dinkiNonAnahataToday - expectedDinkiToday) <= 1,
        message: dinkiAnahataToday === 0
          ? `Today's assignment is correct (all Anahata to Prakash)`
          : `WARNING: ${dinkiAnahataToday} Anahata leads assigned to Dinki today (should be 0)`
      }
    });

  } catch (error: any) {
    console.error('[DEBUG] Error analyzing assignments:', error);
    return NextResponse.json({
      success: false,
      error: error.message
    }, { status: 500 });
  }
}
