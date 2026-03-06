import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

// Initialize Supabase client
const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL!;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY!;
const supabase = createClient(supabaseUrl, supabaseServiceKey);

/**
 * API endpoint to auto-assign leads to users in round-robin fashion
 * POST /api/leads/auto-assign
 *
 * Body:
 * {
 *   "leadId": "uuid" // Optional - assign specific lead
 *   "assignAll": boolean // Optional - assign all unassigned leads
 * }
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json().catch(() => ({}));
    const { leadId, assignAll } = body;

    let leadsToAssign: string[] = [];

    if (leadId) {
      // Assign specific lead
      leadsToAssign = [leadId];
    } else if (assignAll) {
      // Get all unassigned leads
      const { data: unassignedLeads, error } = await supabase
        .from('flatrix_leads')
        .select('id')
        .is('assigned_to_id', null)
        .order('created_at', { ascending: true });

      if (error) throw error;
      leadsToAssign = unassignedLeads?.map(lead => lead.id) || [];
    } else {
      return NextResponse.json(
        {
          success: false,
          error: 'Invalid request',
          message: 'Please provide either leadId or assignAll parameter'
        },
        { status: 400 }
      );
    }

    if (leadsToAssign.length === 0) {
      return NextResponse.json({
        success: true,
        message: 'No leads to assign',
        assigned: 0
      });
    }

    let successCount = 0;
    let errorCount = 0;
    const assignments: { leadId: string; userId: string | null; assignedTo: string; reason: string }[] = [];

    // Get Dinki and Prakash user IDs
    const { data: users, error: usersError } = await supabase
      .from('flatrix_users')
      .select('id, email, name')
      .in('email', ['dinki@flatrix.com', 'prakash@flatrix.com']);

    if (usersError || !users || users.length < 2) {
      return NextResponse.json(
        {
          success: false,
          error: 'Required users not found',
          message: 'Could not find Dinki and Prakash users'
        },
        { status: 500 }
      );
    }

    const dinki = users.find(u => u.email === 'dinki@flatrix.com');
    const prakash = users.find(u => u.email === 'prakash@flatrix.com');

    if (!dinki || !prakash) {
      return NextResponse.json(
        {
          success: false,
          error: 'Required users not found',
          message: 'Could not find Dinki or Prakash user'
        },
        { status: 500 }
      );
    }

    console.log('[AUTO-ASSIGN] Found users:', {
      dinki: { id: dinki.id, name: dinki.name },
      prakash: { id: prakash.id, name: prakash.name }
    });

    // Assign each lead using custom rules
    for (const currentLeadId of leadsToAssign) {
      try {
        // Get the lead details to check project name
        const { data: lead, error: leadError } = await supabase
          .from('flatrix_leads')
          .select('id, project_name')
          .eq('id', currentLeadId)
          .single();

        if (leadError || !lead) {
          console.error('Error fetching lead:', leadError);
          errorCount++;
          continue;
        }

        let assignedUserId: string;
        let assignedUserName: string;
        let assignmentReason: string;

        // Normalize project name for comparison
        const projectName = (lead.project_name || '').toLowerCase().trim();

        // Rule 1: Anahata projects go to Prakash
        if (projectName === 'anahata' ||
            projectName === 'ishtika anahata' ||
            projectName === 'isthika anahata') {
          assignedUserId = prakash.id;
          assignedUserName = prakash.name || 'Prakash';
          assignmentReason = 'Anahata project - assigned to Prakash';
          console.log(`[AUTO-ASSIGN] ${currentLeadId} - Anahata project, assigning to Prakash`);
        } else {
          // Rule 2: For other projects - 1:2 ratio (Dinki:Prakash)
          // ONLY count assignments from TODAY (ignore all historical data)
          const todayStart = new Date();
          todayStart.setHours(0, 0, 0, 0);

          const { data: todaysLeads } = await supabase
            .from('flatrix_leads')
            .select('id, assigned_to_id, project_name, created_at')
            .in('assigned_to_id', [dinki.id, prakash.id])
            .gte('created_at', todayStart.toISOString());

          // Filter out Anahata project leads from the count
          const todaysNonAnahataLeads = (todaysLeads || []).filter(l => {
            const proj = (l.project_name || '').toLowerCase().trim();
            return proj !== 'anahata' &&
                   proj !== 'ishtika anahata' &&
                   proj !== 'isthika anahata';
          });

          // Count how many each person has TODAY for non-Anahata projects
          const dinkiCountToday = todaysNonAnahataLeads.filter(l => l.assigned_to_id === dinki.id).length;
          const prakashCountToday = todaysNonAnahataLeads.filter(l => l.assigned_to_id === prakash.id).length;

          console.log(`[AUTO-ASSIGN] Today's non-Anahata assignments - Dinki: ${dinkiCountToday}, Prakash: ${prakashCountToday}`);

          // 1:2 ratio logic - For every 1 lead Dinki gets, Prakash should get 2
          // Prakash should ALWAYS have roughly 2x the leads of Dinki

          // If Prakash has less than double Dinki's count, assign to Prakash
          // Otherwise, assign to Dinki
          const shouldAssignToPrakash = prakashCountToday < (dinkiCountToday * 2);

          if (shouldAssignToPrakash) {
            // Assign to Prakash - he needs to catch up to 2x ratio
            assignedUserId = prakash.id;
            assignedUserName = prakash.name || 'Prakash';
            assignmentReason = `Maintaining 1:2 ratio - Prakash needs more (Today - Dinki:${dinkiCountToday}, Prakash:${prakashCountToday})`;
            console.log(`[AUTO-ASSIGN] ${currentLeadId} - Prakash has ${prakashCountToday}, Dinki has ${dinkiCountToday}, assigning to Prakash`);
          } else {
            // Assign to Dinki - Prakash has 2x or more
            assignedUserId = dinki.id;
            assignedUserName = dinki.name || 'Dinki';
            assignmentReason = `Maintaining 1:2 ratio - Dinki needs more (Today - Dinki:${dinkiCountToday}, Prakash:${prakashCountToday})`;
            console.log(`[AUTO-ASSIGN] ${currentLeadId} - Prakash has ${prakashCountToday}, Dinki has ${dinkiCountToday}, assigning to Dinki`);
          }
        }

        // Assign the lead to the user
        const { error: updateError } = await supabase
          .from('flatrix_leads')
          .update({
            assigned_to_id: assignedUserId,
            updated_at: new Date().toISOString()
          })
          .eq('id', currentLeadId);

        if (updateError) {
          console.error('Error assigning lead:', updateError);
          errorCount++;
          continue;
        }

        assignments.push({
          leadId: currentLeadId,
          userId: assignedUserId,
          assignedTo: assignedUserName,
          reason: assignmentReason
        });
        successCount++;

      } catch (leadError: any) {
        console.error('Error processing lead assignment:', leadError);
        errorCount++;
      }
    }

    return NextResponse.json({
      success: true,
      message: `Assigned ${successCount} leads successfully`,
      assigned: successCount,
      failed: errorCount,
      assignments
    });

  } catch (error: any) {
    console.error('Error in auto-assign endpoint:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Internal server error',
        message: error.message || 'Failed to assign leads'
      },
      { status: 500 }
    );
  }
}

/**
 * GET endpoint to check rotation status
 */
export async function GET(request: NextRequest) {
  try {
    // Get rotation status
    const { data: rotation, error: rotationError } = await supabase
      .from('flatrix_lead_rotation')
      .select('*')
      .eq('id', '00000000-0000-0000-0000-000000000001')
      .single();

    if (rotationError) throw rotationError;

    // Get active users eligible for assignment
    const { data: activeUsers, error: usersError } = await supabase
      .from('flatrix_users')
      .select('id, name, email, role')
      .eq('is_active', true)
      .in('role', ['ADMIN', 'SALES_MANAGER', 'AGENT'])
      .order('created_at', { ascending: true });

    if (usersError) throw usersError;

    // Get unassigned leads count
    const { count: unassignedCount } = await supabase
      .from('flatrix_leads')
      .select('*', { count: 'exact', head: true })
      .is('assigned_to_id', null);

    return NextResponse.json({
      success: true,
      rotation: {
        lastAssignedUserId: rotation?.last_assigned_user_id,
        lastAssignmentAt: rotation?.last_assignment_at,
        totalAssignments: rotation?.total_assignments || 0
      },
      activeUsers: activeUsers || [],
      activeUsersCount: activeUsers?.length || 0,
      unassignedLeadsCount: unassignedCount || 0
    });

  } catch (error: any) {
    console.error('Error getting rotation status:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Internal server error',
        message: error.message || 'Failed to get rotation status'
      },
      { status: 500 }
    );
  }
}
