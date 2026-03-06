import { NextRequest, NextResponse } from 'next/server';
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

// GET - List all templates
export async function GET() {
  try {
    const { data, error } = await supabase
      .from('flatrix_whatsapp_templates')
      .select('*, flatrix_projects(name)')
      .eq('is_active', true)
      .order('created_at', { ascending: false });

    if (error) throw error;

    return NextResponse.json({ success: true, templates: data });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

// POST - Create a new template
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();

    const {
      project_id,
      project_name_pattern,
      greeting_template_name,
      greeting_body,
      brochure_template_name,
      brochure_url,
      video_template_name,
      video_url,
      template_language,
      is_default,
    } = body;

    if (!project_name_pattern || !greeting_template_name) {
      return NextResponse.json(
        { success: false, error: 'project_name_pattern and greeting_template_name are required' },
        { status: 400 }
      );
    }

    // If marking as default, unset any existing default
    if (is_default) {
      await supabase
        .from('flatrix_whatsapp_templates')
        .update({ is_default: false })
        .eq('is_default', true);
    }

    const { data, error } = await supabase
      .from('flatrix_whatsapp_templates')
      .insert({
        project_id: project_id || null,
        project_name_pattern,
        greeting_template_name,
        greeting_body: greeting_body || null,
        brochure_template_name: brochure_template_name || null,
        brochure_url: brochure_url || null,
        video_template_name: video_template_name || null,
        video_url: video_url || null,
        template_language: template_language || 'en',
        is_default: is_default || false,
        is_active: true,
      })
      .select()
      .single();

    if (error) throw error;

    return NextResponse.json({ success: true, template: data }, { status: 201 });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

// PUT - Update a template
export async function PUT(request: NextRequest) {
  try {
    const body = await request.json();
    const { id, ...updateFields } = body;

    if (!id) {
      return NextResponse.json(
        { success: false, error: 'Template id is required' },
        { status: 400 }
      );
    }

    // If marking as default, unset any existing default
    if (updateFields.is_default) {
      await supabase
        .from('flatrix_whatsapp_templates')
        .update({ is_default: false })
        .eq('is_default', true)
        .neq('id', id);
    }

    const { data, error } = await supabase
      .from('flatrix_whatsapp_templates')
      .update({ ...updateFields, updated_at: new Date().toISOString() })
      .eq('id', id)
      .select()
      .single();

    if (error) throw error;

    return NextResponse.json({ success: true, template: data });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}

// DELETE - Soft-delete a template
export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const id = searchParams.get('id');

    if (!id) {
      return NextResponse.json(
        { success: false, error: 'Template id is required' },
        { status: 400 }
      );
    }

    const { error } = await supabase
      .from('flatrix_whatsapp_templates')
      .update({ is_active: false, updated_at: new Date().toISOString() })
      .eq('id', id);

    if (error) throw error;

    return NextResponse.json({ success: true, message: 'Template deactivated' });
  } catch (error: any) {
    return NextResponse.json(
      { success: false, error: error.message },
      { status: 500 }
    );
  }
}
