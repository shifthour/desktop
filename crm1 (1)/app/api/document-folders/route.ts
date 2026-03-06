import { NextRequest, NextResponse } from 'next/server'
import { createClient } from '@supabase/supabase-js'

const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
)

// GET - Fetch all folders for a company
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const companyId = searchParams.get('company_id') || 'de19ccb7-e90d-4507-861d-a3aecf5e3f29'
    const parentId = searchParams.get('parent_id')
    
    let query = supabase
      .from('document_folders')
      .select(`
        id,
        folder_name,
        folder_description,
        parent_folder_id,
        folder_path,
        folder_level,
        folder_color,
        is_system_folder,
        permissions,
        tags,
        created_by,
        created_at,
        updated_at
      `)
      .eq('company_id', companyId)
      .order('folder_name', { ascending: true })

    // If parent_id is provided, get subfolders, otherwise get root folders
    if (parentId) {
      query = query.eq('parent_folder_id', parentId)
    } else {
      query = query.is('parent_folder_id', null)
    }

    const { data: folders, error } = await query

    if (error) {
      console.error('Error fetching folders:', error)
      return NextResponse.json({ error: 'Failed to fetch folders' }, { status: 500 })
    }

    // Get document count for each folder
    const foldersWithCounts = await Promise.all(
      folders.map(async (folder) => {
        const { count } = await supabase
          .from('documents')
          .select('id', { count: 'exact' })
          .eq('folder_id', folder.id)
          .eq('status', 'Active')

        return {
          ...folder,
          document_count: count || 0
        }
      })
    )

    return NextResponse.json({ 
      folders: foldersWithCounts,
      total: foldersWithCounts.length 
    })

  } catch (error) {
    console.error('Error in GET /api/document-folders:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// POST - Create a new folder
export async function POST(request: NextRequest) {
  try {
    const body = await request.json()
    const {
      folder_name,
      folder_description,
      parent_folder_id,
      folder_color = '#3B82F6',
      permissions,
      tags,
      company_id = 'de19ccb7-e90d-4507-861d-a3aecf5e3f29',
      created_by
    } = body

    if (!folder_name) {
      return NextResponse.json({ error: 'Folder name is required' }, { status: 400 })
    }

    // Check if folder name already exists in the same parent
    const { data: existingFolder } = await supabase
      .from('document_folders')
      .select('id')
      .eq('folder_name', folder_name)
      .eq('company_id', company_id)
      .eq('parent_folder_id', parent_folder_id || null)
      .single()

    if (existingFolder) {
      return NextResponse.json({ error: 'Folder name already exists in this location' }, { status: 409 })
    }

    // Calculate folder level and path
    let folder_level = 0
    let folder_path = `/${folder_name}`

    if (parent_folder_id) {
      const { data: parentFolder } = await supabase
        .from('document_folders')
        .select('folder_level, folder_path')
        .eq('id', parent_folder_id)
        .single()

      if (parentFolder) {
        folder_level = parentFolder.folder_level + 1
        folder_path = `${parentFolder.folder_path}/${folder_name}`
      }
    }

    const { data: newFolder, error } = await supabase
      .from('document_folders')
      .insert({
        folder_name,
        folder_description,
        parent_folder_id,
        folder_path,
        folder_level,
        folder_color,
        is_system_folder: false,
        permissions,
        tags,
        company_id,
        created_by,
        updated_by: created_by
      })
      .select()
      .single()

    if (error) {
      console.error('Error creating folder:', error)
      return NextResponse.json({ error: 'Failed to create folder' }, { status: 500 })
    }

    return NextResponse.json({ 
      folder: { ...newFolder, document_count: 0 },
      message: 'Folder created successfully' 
    })

  } catch (error) {
    console.error('Error in POST /api/document-folders:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// PUT - Update a folder
export async function PUT(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const folderId = searchParams.get('id')
    
    if (!folderId) {
      return NextResponse.json({ error: 'Folder ID is required' }, { status: 400 })
    }

    const body = await request.json()
    const {
      folder_name,
      folder_description,
      folder_color,
      permissions,
      tags,
      updated_by
    } = body

    const { data: updatedFolder, error } = await supabase
      .from('document_folders')
      .update({
        folder_name,
        folder_description,
        folder_color,
        permissions,
        tags,
        updated_by
      })
      .eq('id', folderId)
      .select()
      .single()

    if (error) {
      console.error('Error updating folder:', error)
      return NextResponse.json({ error: 'Failed to update folder' }, { status: 500 })
    }

    return NextResponse.json({ 
      folder: updatedFolder,
      message: 'Folder updated successfully' 
    })

  } catch (error) {
    console.error('Error in PUT /api/document-folders:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}

// DELETE - Delete a folder (only if empty)
export async function DELETE(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url)
    const folderId = searchParams.get('id')
    
    if (!folderId) {
      return NextResponse.json({ error: 'Folder ID is required' }, { status: 400 })
    }

    // Check if folder has documents
    const { count: documentCount } = await supabase
      .from('documents')
      .select('id', { count: 'exact' })
      .eq('folder_id', folderId)
      .eq('status', 'Active')

    if (documentCount && documentCount > 0) {
      return NextResponse.json({ 
        error: 'Cannot delete folder that contains documents. Please move or delete all documents first.' 
      }, { status: 409 })
    }

    // Check if folder has subfolders
    const { count: subfolderCount } = await supabase
      .from('document_folders')
      .select('id', { count: 'exact' })
      .eq('parent_folder_id', folderId)

    if (subfolderCount && subfolderCount > 0) {
      return NextResponse.json({ 
        error: 'Cannot delete folder that contains subfolders. Please delete all subfolders first.' 
      }, { status: 409 })
    }

    // Check if it's a system folder
    const { data: folder } = await supabase
      .from('document_folders')
      .select('is_system_folder')
      .eq('id', folderId)
      .single()

    if (folder?.is_system_folder) {
      return NextResponse.json({ 
        error: 'Cannot delete system folders' 
      }, { status: 403 })
    }

    const { error } = await supabase
      .from('document_folders')
      .delete()
      .eq('id', folderId)

    if (error) {
      console.error('Error deleting folder:', error)
      return NextResponse.json({ error: 'Failed to delete folder' }, { status: 500 })
    }

    return NextResponse.json({ message: 'Folder deleted successfully' })

  } catch (error) {
    console.error('Error in DELETE /api/document-folders:', error)
    return NextResponse.json({ error: 'Internal server error' }, { status: 500 })
  }
}