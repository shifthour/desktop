import { NextRequest, NextResponse } from 'next/server'
import { supabaseAdmin } from '@/lib/supabase'

/**
 * POST /api/upload
 * Upload a file (video/image) to Supabase Storage
 * Returns the public URL
 */
export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData()
    const file = formData.get('file') as File

    if (!file) {
      return NextResponse.json(
        { error: 'No file provided' },
        { status: 400 }
      )
    }

    console.log('[Upload] File received:', file.name, file.type, file.size)

    // Validate file type
    const allowedTypes = ['video/mp4', 'video/webm', 'video/quicktime', 'image/jpeg', 'image/png', 'image/gif']
    if (!allowedTypes.includes(file.type)) {
      return NextResponse.json(
        { error: 'Invalid file type. Allowed: MP4, WebM, MOV, JPEG, PNG, GIF' },
        { status: 400 }
      )
    }

    // Limit file size (50MB for videos)
    const maxSize = 50 * 1024 * 1024 // 50MB
    if (file.size > maxSize) {
      return NextResponse.json(
        { error: 'File too large. Maximum size is 50MB' },
        { status: 400 }
      )
    }

    // Generate unique filename
    const timestamp = Date.now()
    const randomStr = Math.random().toString(36).substring(7)
    const extension = file.name.split('.').pop() || 'mp4'
    const filename = `whatsapp-media/${timestamp}-${randomStr}.${extension}`

    // Convert File to ArrayBuffer then to Uint8Array
    const arrayBuffer = await file.arrayBuffer()
    const uint8Array = new Uint8Array(arrayBuffer)

    console.log('[Upload] Uploading to Supabase Storage, filename:', filename)

    // Upload to Supabase Storage using admin client (bypasses RLS)
    const { data, error } = await supabaseAdmin.storage
      .from('media') // bucket name
      .upload(filename, uint8Array, {
        contentType: file.type,
        upsert: false,
      })

    if (error) {
      console.error('[Upload] Supabase upload error:', error)

      // Provide helpful error messages
      if (error.message?.includes('Bucket not found') || error.message?.includes('not found')) {
        return NextResponse.json(
          { error: 'Storage bucket "media" not found. Please create it in Supabase Dashboard → Storage → New Bucket (name: media, public: yes)' },
          { status: 500 }
        )
      }

      if (error.message?.includes('row-level security') || error.message?.includes('policy')) {
        return NextResponse.json(
          { error: 'Storage permission denied. Please make the "media" bucket public in Supabase.' },
          { status: 500 }
        )
      }

      return NextResponse.json(
        { error: error.message || 'Upload failed' },
        { status: 500 }
      )
    }

    console.log('[Upload] Upload successful:', data)

    // Get public URL
    const { data: urlData } = supabaseAdmin.storage
      .from('media')
      .getPublicUrl(filename)

    console.log('[Upload] Public URL:', urlData.publicUrl)

    return NextResponse.json({
      success: true,
      url: urlData.publicUrl,
      filename: filename,
      size: file.size,
      type: file.type,
    })
  } catch (error: any) {
    console.error('[Upload] Error:', error)
    return NextResponse.json(
      { error: error.message || 'Upload failed' },
      { status: 500 }
    )
  }
}
