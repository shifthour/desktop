import { NextRequest, NextResponse } from 'next/server'
import { createServerClient } from '@/lib/supabase'

/**
 * Templates API - List and manage message templates
 */
export async function GET(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)

    const channel = searchParams.get('channel') || ''
    const type = searchParams.get('type') || ''
    const activeOnly = searchParams.get('activeOnly') === 'true'

    // Build query
    let query = supabase
      .from('jc_message_templates')
      .select('*')

    // Apply filters
    if (channel) {
      query = query.eq('channel', channel)
    }

    if (type) {
      query = query.eq('notification_type', type)
    }

    if (activeOnly) {
      query = query.eq('is_active', true)
    }

    // Apply sorting
    query = query.order('notification_type', { ascending: true })
      .order('channel', { ascending: true })
      .order('language', { ascending: true })

    const { data: templates, error } = await query

    if (error) {
      throw error
    }

    return NextResponse.json({ templates })
  } catch (error: any) {
    console.error('Templates API error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}

/**
 * Create a new template
 */
export async function POST(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    const {
      template_code,
      template_name,
      notification_type,
      channel,
      language,
      subject,
      body: templateBody,
      variables,
      whatsapp_template_name,
      whatsapp_namespace,
      is_active,
    } = body

    // Validate required fields
    if (!template_code || !template_name || !notification_type || !channel || !templateBody) {
      return NextResponse.json(
        { error: 'Missing required fields: template_code, template_name, notification_type, channel, body' },
        { status: 400 }
      )
    }

    const { data: template, error } = await (supabase
      .from('jc_message_templates') as any)
      .insert({
        template_code,
        template_name,
        notification_type,
        channel,
        language: language || 'en',
        subject,
        body: templateBody,
        variables: variables || [],
        whatsapp_template_name,
        whatsapp_namespace,
        is_active: is_active !== false,
      })
      .select()
      .single()

    if (error) {
      throw error
    }

    return NextResponse.json({ template }, { status: 201 })
  } catch (error: any) {
    console.error('Create template error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}

/**
 * Update a template
 */
export async function PUT(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const body = await request.json()

    const { id, ...updates } = body

    if (!id) {
      return NextResponse.json(
        { error: 'Template ID required' },
        { status: 400 }
      )
    }

    const { data: template, error } = await (supabase
      .from('jc_message_templates') as any)
      .update(updates)
      .eq('id', id)
      .select()
      .single()

    if (error) {
      throw error
    }

    return NextResponse.json({ template })
  } catch (error: any) {
    console.error('Update template error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}

/**
 * Delete a template
 */
export async function DELETE(request: NextRequest) {
  try {
    const supabase = createServerClient()
    const { searchParams } = new URL(request.url)
    const id = searchParams.get('id')

    if (!id) {
      return NextResponse.json(
        { error: 'Template ID required' },
        { status: 400 }
      )
    }

    const { error } = await (supabase
      .from('jc_message_templates') as any)
      .delete()
      .eq('id', id)

    if (error) {
      throw error
    }

    return NextResponse.json({ success: true })
  } catch (error: any) {
    console.error('Delete template error:', error)
    return NextResponse.json(
      { error: error.message },
      { status: 500 }
    )
  }
}
