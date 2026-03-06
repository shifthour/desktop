"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { useToast } from "@/hooks/use-toast"
import { Upload } from "lucide-react"

export function SimpleUploadTest() {
  const { toast } = useToast()
  const [uploading, setUploading] = useState(false)

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files
    if (!files || files.length === 0) return

    const file = files[0]
    setUploading(true)

    try {
      const formData = new FormData()
      formData.append('file', file)
      formData.append('folder_id', '')
      formData.append('description', 'Test upload')
      formData.append('tags', '')
      formData.append('access_level', 'Company')
      formData.append('uploaded_by', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29')
      formData.append('uploaded_by_name', 'Test User')

      const response = await fetch('/api/documents/simple-upload', {
        method: 'POST',
        body: formData,
      })

      if (response.ok) {
        const data = await response.json()
        toast({
          title: "Success",
          description: "File uploaded successfully!"
        })
        console.log('Upload result:', data)
      } else {
        const errorData = await response.json()
        toast({
          title: "Error",
          description: errorData.error || "Upload failed",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Upload error:', error)
      toast({
        title: "Error",
        description: "Upload failed",
        variant: "destructive"
      })
    } finally {
      setUploading(false)
    }
  }

  return (
    <div className="p-4 border border-dashed border-gray-300 rounded-lg">
      <h3 className="text-lg font-medium mb-4">Simple Upload Test</h3>
      <div className="space-y-4">
        <Input
          type="file"
          onChange={handleFileUpload}
          disabled={uploading}
          accept=".pdf,.doc,.docx,.xls,.xlsx,.txt,.csv,.jpg,.jpeg,.png"
        />
        <Button disabled={uploading}>
          <Upload className="w-4 h-4 mr-2" />
          {uploading ? 'Uploading...' : 'Upload File'}
        </Button>
      </div>
    </div>
  )
}