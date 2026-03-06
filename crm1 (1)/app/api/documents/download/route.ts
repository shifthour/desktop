import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

// GET - Download a document
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const documentId = searchParams.get('id')
    const userId = searchParams.get('user_id')
    const userName = searchParams.get('user_name')
    
    if (!documentId) {
      return NextResponse.json({ error: 'Document ID is required' }, { status: 400 })
    }

    // Get document details
    const { data: document, error } = await supabase
      .from('documents')
      .select('*')
      .eq('id', documentId)
      .eq('status', 'Active')
      .single()

    if (error || !document) {
      return NextResponse.json({ error: 'Document not found' }, { status: 404 })
    }

    // Check if document is locked
    if (document.is_locked) {
      return NextResponse.json({ 
        error: 'Document is locked and cannot be downloaded' 
      }, { status: 403 })
    }

    // Update download count and last accessed info
    await supabase
      .from('documents')
      .update({
        download_count: (document.download_count || 0) + 1,
        last_accessed_at: new Date().toISOString(),
        last_accessed_by: userId
      })
      .eq('id', documentId)

    // Log the download action
    if (userId) {
      await supabase
        .from('document_access_logs')
        .insert({
          document_id: documentId,
          user_id: userId,
          user_name: userName,
          action: 'download',
          company_id: document.company_id
        })
    }

    // Return download URL
    return NextResponse.json({ 
      download_url: document.file_url,
      filename: document.original_filename,
      size: document.file_size,
      type: document.file_type
    })

  } catch (error) {
    console.error('Error in GET /api/documents/download:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// POST - Batch download multiple documents
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const { document_ids, user_id, user_name } = body

    if (!document_ids || !Array.isArray(document_ids) || document_ids.length === 0) {
      return NextResponse.json({ error: 'Document IDs array is required' }, { status: 400 })
    }

    // Get all requested documents
    const { data: documents, error } = await supabase
      .from('documents')
      .select('*')
      .in('id', document_ids)
      .eq('status', 'Active')

    if (error) {
      return NextResponse.json({ error: 'Failed to fetch documents' }, { status: 500 })
    }

    if (!documents || documents.length === 0) {
      return NextResponse.json({ error: 'No valid documents found' }, { status: 404 })
    }

    // Filter out locked documents
    const availableDocuments = documents.filter(doc => !doc.is_locked)

    if (availableDocuments.length === 0) {
      return NextResponse.json({ 
        error: 'All requested documents are locked' 
      }, { status: 403 })
    }

    // Update download counts and log access for each document
    const downloadPromises = availableDocuments.map(async (document) => {
      // Update download count
      await supabase
        .from('documents')
        .update({
          download_count: (document.download_count || 0) + 1,
          last_accessed_at: new Date().toISOString(),
          last_accessed_by: user_id
        })
        .eq('id', document.id)

      // Log download action
      if (user_id) {
        await supabase
          .from('document_access_logs')
          .insert({
            document_id: document.id,
            user_id: user_id,
            user_name: user_name,
            action: 'download',
            company_id: document.company_id
          })
      }

      return {
        id: document.id,
        filename: document.original_filename,
        download_url: document.file_url,
        size: document.file_size,
        type: document.file_type
      }
    })

    const downloadData = await Promise.all(downloadPromises)

    return NextResponse.json({ 
      documents: downloadData,
      total: downloadData.length,
      message: `${downloadData.length} documents ready for download`
    })

  } catch (error) {
    console.error('Error in POST /api/documents/download:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}