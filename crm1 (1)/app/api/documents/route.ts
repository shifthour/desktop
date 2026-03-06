import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

// GET - Fetch all documents for a company
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('company_id') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
    const folderId = searchParams.get('folder_id')
    const search = searchParams.get('search')
    const fileType = searchParams.get('file_type')
    const status = searchParams.get('status') || 'Active'
    const limit = parseInt(searchParams.get('limit') || '50')
    const offset = parseInt(searchParams.get('offset') || '0')
    
    let query = supabase
      .from('documents')
      .select(`
        id,
        document_name,
        original_filename,
        file_extension,
        file_size,
        file_type,
        file_url,
        folder_id,
        folder_path,
        description,
        tags,
        version,
        status,
        access_level,
        download_count,
        last_accessed_at,
        uploaded_by,
        uploaded_by_name,
        approved_by,
        approved_at,
        expiry_date,
        is_locked,
        created_at,
        updated_at,
        document_folders!inner(
          id,
          folder_name,
          folder_color
        )
      `)
      .eq('company_id', companyId)
      .eq('status', status)
      .order('created_at', { ascending: false })
      .range(offset, offset + limit - 1)

    // Filter by folder if specified
    if (folderId) {
      query = query.eq('folder_id', folderId)
    }

    // Filter by file type if specified
    if (fileType && fileType !== 'All') {
      query = query.eq('file_extension', fileType.toLowerCase())
    }

    // Search functionality
    if (search) {
      query = query.or(`document_name.ilike.%${search}%, original_filename.ilike.%${search}%, description.ilike.%${search}%`)
    }

    const { data: documents, error } = await query

    if (error) {
      console.error('Error fetching documents:', error)
      return NextResponse.json({ error: 'Failed to fetch documents' }, { status: 500 })
    }

    // Get total count for pagination
    let countQuery = supabase
      .from('documents')
      .select('id', { count: 'exact' })
      .eq('company_id', companyId)
      .eq('status', status)

    if (folderId) {
      countQuery = countQuery.eq('folder_id', folderId)
    }

    if (fileType && fileType !== 'All') {
      countQuery = countQuery.eq('file_extension', fileType.toLowerCase())
    }

    if (search) {
      countQuery = countQuery.or(`document_name.ilike.%${search}%, original_filename.ilike.%${search}%, description.ilike.%${search}%`)
    }

    const { count: totalCount } = await countQuery

    return NextResponse.json({ 
      documents: documents || [],
      total: totalCount || 0,
      limit,
      offset
    })

  } catch (error) {
    console.error('Error in GET /api/documents:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// POST - Upload a new document
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const {
      document_name,
      original_filename,
      file_extension,
      file_size,
      file_type,
      file_url,
      folder_id,
      description,
      tags,
      access_level = 'Company',
      uploaded_by,
      uploaded_by_name,
      company_id = 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
    } = body

    if (!document_name || !original_filename || !file_url) {
      return NextResponse.json({ 
        error: 'Document name, original filename, and file URL are required' 
      }, { status: 400 })
    }

    // Get folder path if folder_id is provided
    let folder_path = '/'
    if (folder_id) {
      const { data: folder } = await supabase
        .from('document_folders')
        .select('folder_path')
        .eq('id', folder_id)
        .single()
      
      if (folder) {
        folder_path = folder.folder_path
      }
    }

    const { data: newDocument, error } = await supabase
      .from('documents')
      .insert({
        document_name,
        original_filename,
        file_extension: file_extension.toLowerCase(),
        file_size,
        file_type,
        file_url,
        folder_id,
        folder_path,
        description,
        tags,
        access_level,
        uploaded_by,
        uploaded_by_name,
        company_id,
        created_by: uploaded_by,
        updated_by: uploaded_by
      })
      .select(`
        *,
        document_folders(
          id,
          folder_name,
          folder_color
        )
      `)
      .single()

    if (error) {
      console.error('Error creating document:', error)
      return NextResponse.json({ error: 'Failed to create document' }, { status: 500 })
    }

    // Log the upload action
    await supabase
      .from('document_access_logs')
      .insert({
        document_id: newDocument.id,
        user_id: uploaded_by,
        user_name: uploaded_by_name,
        action: 'upload',
        company_id
      })

    return NextResponse.json({ 
      document: newDocument,
      message: 'Document uploaded successfully' 
    })

  } catch (error) {
    console.error('Error in POST /api/documents:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// PUT - Update a document
export async function PUT(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const documentId = searchParams.get('id')
    
    if (!documentId) {
      return NextResponse.json({ error: 'Document ID is required' }, { status: 400 })
    }

    const body = await request.json()
    const {
      document_name,
      description,
      tags,
      access_level,
      folder_id,
      status,
      updated_by,
      updated_by_name
    } = body

    // Get folder path if folder_id is changing
    let folder_path
    if (folder_id) {
      const { data: folder } = await supabase
        .from('document_folders')
        .select('folder_path')
        .eq('id', folder_id)
        .single()
      
      if (folder) {
        folder_path = folder.folder_path
      }
    }

    const updateData: any = {
      updated_by
    }

    if (document_name !== undefined) updateData.document_name = document_name
    if (description !== undefined) updateData.description = description
    if (tags !== undefined) updateData.tags = tags
    if (access_level !== undefined) updateData.access_level = access_level
    if (folder_id !== undefined) {
      updateData.folder_id = folder_id
      updateData.folder_path = folder_path
    }
    if (status !== undefined) updateData.status = status

    const { data: updatedDocument, error } = await supabase
      .from('documents')
      .update(updateData)
      .eq('id', documentId)
      .select(`
        *,
        document_folders(
          id,
          folder_name,
          folder_color
        )
      `)
      .single()

    if (error) {
      console.error('Error updating document:', error)
      return NextResponse.json({ error: 'Failed to update document' }, { status: 500 })
    }

    // Log the edit action
    await supabase
      .from('document_access_logs')
      .insert({
        document_id: documentId,
        user_id: updated_by,
        user_name: updated_by_name,
        action: 'edit',
        company_id: updatedDocument.company_id
      })

    return NextResponse.json({ 
      document: updatedDocument,
      message: 'Document updated successfully' 
    })

  } catch (error) {
    console.error('Error in PUT /api/documents:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// DELETE - Delete a document (soft delete)
export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const documentId = searchParams.get('id')
    const userId = searchParams.get('user_id')
    const userName = searchParams.get('user_name')
    
    if (!documentId) {
      return NextResponse.json({ error: 'Document ID is required' }, { status: 400 })
    }

    // Soft delete by changing status to 'Deleted'
    const { data: deletedDocument, error } = await supabase
      .from('documents')
      .update({ 
        status: 'Deleted',
        updated_by: userId
      })
      .eq('id', documentId)
      .select('company_id')
      .single()

    if (error) {
      console.error('Error deleting document:', error)
      return NextResponse.json({ error: 'Failed to delete document' }, { status: 500 })
    }

    // Log the delete action
    await supabase
      .from('document_access_logs')
      .insert({
        document_id: documentId,
        user_id: userId,
        user_name: userName,
        action: 'delete',
        company_id: deletedDocument.company_id
      })

    return NextResponse.json({ message: 'Document deleted successfully' })

  } catch (error) {
    console.error('Error in DELETE /api/documents:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}