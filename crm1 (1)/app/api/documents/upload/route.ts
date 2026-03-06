import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

// POST - Handle file upload
export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData()
    const file = formData.get('file') as File
    const folder_id = formData.get('folder_id') as string
    const description = formData.get('description') as string
    const tags = formData.get('tags') as string
    const access_level = formData.get('access_level') as string || 'Company'
    const uploaded_by = formData.get('uploaded_by') as string
    const uploaded_by_name = formData.get('uploaded_by_name') as string
    const company_id = formData.get('company_id') as string || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'

    if (!file) {
      return NextResponse.json({ error: 'No file provided' }, { status: 400 })
    }

    // Validate file size (max 10MB for now)
    const maxSize = 10 * 1024 * 1024 // 10MB
    if (file.size > maxSize) {
      return NextResponse.json({ 
        error: 'File size too large. Maximum size is 10MB.' 
      }, { status: 400 })
    }

    // Get file extension and validate
    const fileExtension = file.name.split('.').pop()?.toLowerCase()
    if (!fileExtension) {
      return NextResponse.json({ error: 'File must have an extension' }, { status: 400 })
    }

    // Allowed file types
    const allowedTypes = [
      'pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx',
      'txt', 'csv', 'jpg', 'jpeg', 'png', 'gif', 'bmp',
      'zip', 'rar', '7z', 'mp4', 'avi', 'mov', 'wmv'
    ]

    if (!allowedTypes.includes(fileExtension)) {
      return NextResponse.json({ 
        error: `File type .${fileExtension} is not allowed` 
      }, { status: 400 })
    }

    // Create unique filename
    const timestamp = Date.now()
    const randomString = Math.random().toString(36).substring(2, 15)
    const fileName = `${timestamp}_${randomString}.${fileExtension}`
    const filePath = `documents/${company_id}/${fileName}`

    // Convert file to buffer
    const fileBuffer = await file.arrayBuffer()
    const uint8Array = new Uint8Array(fileBuffer)

    // Upload to Supabase Storage
    const { data: uploadData, error: uploadError } = await supabase.storage
      .from('documents')
      .upload(filePath, uint8Array, {
        contentType: file.type,
        duplex: 'half'
      })

    if (uploadError) {
      console.error('Error uploading file to storage:', uploadError)
      return NextResponse.json({ 
        error: 'Failed to upload file to storage' 
      }, { status: 500 })
    }

    // Get public URL for the uploaded file
    const { data: urlData } = supabase.storage
      .from('documents')
      .getPublicUrl(filePath)

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

    // Parse tags if provided
    let parsedTags = []
    if (tags) {
      try {
        parsedTags = JSON.parse(tags)
      } catch (e) {
        // If not valid JSON, split by comma
        parsedTags = tags.split(',').map(tag => tag.trim()).filter(Boolean)
      }
    }

    // Save document metadata to database
    const { data: newDocument, error: dbError } = await supabase
      .from('documents')
      .insert({
        document_name: file.name.replace(`.${fileExtension}`, ''),
        original_filename: file.name,
        file_extension: fileExtension,
        file_size: file.size,
        file_type: file.type,
        file_url: urlData.publicUrl,
        file_path: filePath,
        folder_id: folder_id || null,
        folder_path,
        description,
        tags: parsedTags,
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

    if (dbError) {
      console.error('Error saving document to database:', dbError)
      // Clean up uploaded file if database insert fails
      await supabase.storage
        .from('documents')
        .remove([filePath])
      
      return NextResponse.json({ 
        error: 'Failed to save document metadata' 
      }, { status: 500 })
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
      message: 'File uploaded successfully' 
    })

  } catch (error) {
    console.error('Error in POST /api/documents/upload:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}