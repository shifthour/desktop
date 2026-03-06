"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Upload, X } from "lucide-react"
import { useToast } from "@/hooks/use-toast"

interface SimpleCsvImportProps {
  isOpen: boolean
  onClose: () => void
  onImport: (data: any[]) => void
}

export function SimpleCsvImport({ isOpen, onClose, onImport }: SimpleCsvImportProps) {
  const [file, setFile] = useState<File | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const { toast } = useToast()

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = event.target.files?.[0]
    if (selectedFile) {
      console.log("File selected:", selectedFile.name)
      setFile(selectedFile)
    }
  }

  const handleUpload = () => {
    if (!file) {
      toast({
        title: "No file selected",
        description: "Please select a CSV file to upload",
        variant: "destructive"
      })
      return
    }

    setIsLoading(true)
    
    try {
      const reader = new FileReader()
      
      reader.onload = (e) => {
        try {
          const content = e.target?.result as string
          console.log("File content loaded")
          
          // Parse CSV manually
          const lines = content.split('\n').filter(line => line.trim())
          if (lines.length < 2) {
            throw new Error("CSV file must have at least a header row and one data row")
          }
          
          const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''))
          console.log("Headers:", headers)
          
          const data = lines.slice(1).map(line => {
            const values = line.split(',').map(v => v.trim().replace(/"/g, ''))
            const row: any = {}
            headers.forEach((header, index) => {
              row[header] = values[index] || ''
            })
            return row
          })
          
          console.log("Parsed data:", data.length, "rows")
          
          // Call the import function
          onImport(data)
          
          // Close modal and reset
          setFile(null)
          onClose()
          
          toast({
            title: "File uploaded successfully",
            description: `Processed ${data.length} records from ${file.name}`
          })
          
        } catch (parseError) {
          console.error("Parse error:", parseError)
          toast({
            title: "Error parsing file",
            description: parseError instanceof Error ? parseError.message : "Failed to parse CSV file",
            variant: "destructive"
          })
        } finally {
          setIsLoading(false)
        }
      }
      
      reader.onerror = () => {
        console.error("FileReader error")
        toast({
          title: "Error reading file",
          description: "Failed to read the selected file",
          variant: "destructive"
        })
        setIsLoading(false)
      }
      
      console.log("Starting to read file...")
      reader.readAsText(file)
      
    } catch (error) {
      console.error("Upload error:", error)
      toast({
        title: "Upload failed",
        description: "An unexpected error occurred",
        variant: "destructive"
      })
      setIsLoading(false)
    }
  }

  const handleClose = () => {
    setFile(null)
    onClose()
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Import CSV File</DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          <Card>
            <CardHeader>
              <CardTitle className="text-base">Select CSV File</CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <input
                  type="file"
                  accept=".csv"
                  onChange={handleFileSelect}
                  className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
                />
              </div>
              
              {file && (
                <div className="p-3 bg-green-50 border border-green-200 rounded-lg">
                  <p className="text-sm font-medium text-green-800">
                    Selected: {file.name}
                  </p>
                  <p className="text-xs text-green-600">
                    Size: {(file.size / 1024).toFixed(2)} KB
                  </p>
                </div>
              )}
            </CardContent>
          </Card>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleClose} disabled={isLoading}>
            <X className="w-4 h-4 mr-2" />
            Cancel
          </Button>
          <Button 
            onClick={handleUpload} 
            disabled={!file || isLoading}
            className="bg-blue-600 hover:bg-blue-700"
          >
            <Upload className="w-4 h-4 mr-2" />
            {isLoading ? "Uploading..." : "Upload"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}