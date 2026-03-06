import { NextRequest, NextResponse } from 'next/server'
import { writeFile, readFile, unlink } from 'fs/promises'
import { join } from 'path'
import { tmpdir } from 'os'

export async function GET(
  request: NextRequest,
  { params }: { params: { filename: string } }
) {
  try {
    const { filename } = params
    
    if (!filename) {
      return NextResponse.json({ error: 'Filename is required' }, { status: 400 })
    }

    // Sanitize filename to prevent directory traversal
    const safeFilename = filename.replace(/[^a-zA-Z0-9._-]/g, '_')
    const tempDir = tmpdir()
    const filePath = join(tempDir, 'crm-reports', safeFilename)

    try {
      // Read the file
      const fileBuffer = await readFile(filePath)
      const fileExtension = safeFilename.split('.').pop()?.toLowerCase()

      // Set appropriate headers based on file type
      const headers: Record<string, string> = {
        'Content-Disposition': `attachment; filename="${safeFilename}"`,
      }

      switch (fileExtension) {
        case 'pdf':
          headers['Content-Type'] = 'application/pdf'
          break
        case 'xlsx':
        case 'xls':
          headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
          break
        case 'csv':
          headers['Content-Type'] = 'text/csv'
          break
        case 'json':
          headers['Content-Type'] = 'application/json'
          break
        default:
          headers['Content-Type'] = 'application/octet-stream'
      }

      // Clean up the temporary file after serving it
      setTimeout(async () => {
        try {
          await unlink(filePath)
        } catch (error) {
          console.log('Cleanup error:', error)
        }
      }, 5000) // Clean up after 5 seconds

      return new NextResponse(fileBuffer, {
        status: 200,
        headers
      })

    } catch (fileError) {
      console.error('File read error:', fileError)
      return NextResponse.json({ 
        error: 'File not found or has expired' 
      }, { status: 404 })
    }

  } catch (error) {
    console.error('Download error:', error)
    return NextResponse.json({ 
      error: 'Failed to download file' 
    }, { status: 500 })
  }
}