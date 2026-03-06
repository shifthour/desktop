"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter } from "@/components/ui/dialog"
import { Upload, X, FileText, FileSpreadsheet, Download, Loader2 } from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import { Progress } from "@/components/ui/progress"

interface SimpleFileImportProps {
  isOpen: boolean
  onClose: () => void
  onImport: (data: any[]) => void
  isImporting?: boolean
  importProgress?: { current: number; total: number }
}

export function SimpleFileImport({ 
  isOpen, 
  onClose, 
  onImport, 
  isImporting = false,
  importProgress = { current: 0, total: 0 }
}: SimpleFileImportProps) {
  const [file, setFile] = useState<File | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const { toast } = useToast()

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = event.target.files?.[0]
    if (selectedFile) {
      console.log("File selected:", selectedFile.name, selectedFile.type)
      setFile(selectedFile)
    }
  }

  const parseCSV = (content: string): any[] => {
    const lines = content.split('\n').filter(line => line.trim())
    if (lines.length < 2) {
      throw new Error("File must have at least a header row and one data row")
    }
    
    // Parse CSV line properly handling quoted values with commas
    const parseCSVLine = (line: string): string[] => {
      const result: string[] = []
      let current = ''
      let inQuotes = false
      
      for (let i = 0; i < line.length; i++) {
        const char = line[i]
        
        if (char === '"') {
          if (inQuotes && line[i + 1] === '"') {
            // Handle escaped quotes
            current += '"'
            i++ // Skip next quote
          } else {
            // Toggle quote state
            inQuotes = !inQuotes
          }
        } else if (char === ',' && !inQuotes) {
          // Field separator outside quotes
          result.push(current.trim())
          current = ''
        } else {
          current += char
        }
      }
      
      // Add the last field
      result.push(current.trim())
      
      return result
    }
    
    const headers = parseCSVLine(lines[0]).map(h => h.replace(/"/g, ''))
    console.log("CSV Headers:", headers)
    
    const data = lines.slice(1).map(line => {
      const values = parseCSVLine(line).map(v => v.replace(/"/g, ''))
      const row: any = {}
      headers.forEach((header, index) => {
        row[header] = values[index] || ''
      })
      return row
    })
    
    return data
  }

  const parseExcel = async (file: File): Promise<any[]> => {
    return new Promise(async (resolve, reject) => {
      try {
        console.log("Loading XLSX library...")
        const XLSX = await import('xlsx')
        console.log("XLSX library loaded successfully")
        
        const reader = new FileReader()
        
        reader.onload = (e) => {
          try {
            console.log("Reading Excel file...")
            const data = e.target?.result as ArrayBuffer
            
            if (!data) {
              throw new Error("No data received from file")
            }
            
            const workbook = XLSX.read(data, { type: 'array' })
            console.log("Workbook loaded, sheets:", workbook.SheetNames)
            
            if (!workbook.SheetNames || workbook.SheetNames.length === 0) {
              throw new Error("No worksheets found in Excel file")
            }
            
            // Get first sheet
            const sheetName = workbook.SheetNames[0]
            const worksheet = workbook.Sheets[sheetName]
            
            // Convert to JSON
            const jsonData = XLSX.utils.sheet_to_json(worksheet, { header: 1 }) as any[][]
            console.log("Excel data extracted:", jsonData.length, "rows")
            
            if (jsonData.length < 2) {
              throw new Error("Excel file must have at least a header row and one data row")
            }
            
            // Extract headers and data
            const headers = jsonData[0].map(h => String(h || '').trim())
            console.log("Excel Headers:", headers)
            
            const processedData = jsonData.slice(1)
              .filter(row => row && row.some(cell => cell))
              .map(row => {
                const rowData: any = {}
                headers.forEach((header, index) => {
                  rowData[header] = String(row[index] || '').trim()
                })
                return rowData
              })
            
            resolve(processedData)
          } catch (error) {
            console.error("Excel parsing error:", error)
            reject(error)
          }
        }
        
        reader.onerror = () => {
          reject(new Error("Failed to read Excel file"))
        }
        
        reader.readAsArrayBuffer(file)
        
      } catch (error) {
        console.error("Excel import error:", error)
        reject(error)
      }
    })
  }

  const handleUpload = async () => {
    if (!file) {
      toast({
        title: "No file selected",
        description: "Please select a CSV or Excel file to upload",
        variant: "destructive"
      })
      return
    }

    setIsLoading(true)
    
    try {
      const fileExtension = file.name.split('.').pop()?.toLowerCase()
      console.log("Processing file type:", fileExtension)
      
      if (fileExtension === 'csv') {
        // Handle CSV files
        console.log("Processing CSV file...")
        const reader = new FileReader()
        
        reader.onload = (e) => {
          try {
            const content = e.target?.result as string
            console.log("CSV content loaded")
            
            const csvData = parseCSV(content)
            console.log("CSV parsed:", csvData.length, "rows")
            
            // Process the import
            onImport(csvData)
            
            // Success cleanup (don't close yet - wait for import completion)
            setFile(null)
            
          } catch (error) {
            console.error("CSV parse error:", error)
            toast({
              title: "Error parsing CSV file",
              description: error instanceof Error ? error.message : "Failed to parse CSV file",
              variant: "destructive"
            })
          } finally {
            setIsLoading(false)
          }
        }
        
        reader.onerror = () => {
          toast({
            title: "Error reading CSV file",
            description: "Failed to read the selected CSV file",
            variant: "destructive"
          })
          setIsLoading(false)
        }
        
        reader.readAsText(file)
        
      } else if (fileExtension === 'xlsx' || fileExtension === 'xls') {
        // Handle Excel files
        console.log("Processing Excel file...")
        
        try {
          const excelData = await parseExcel(file)
          console.log("Excel parsed:", excelData.length, "rows")
          
          // Process the import
          onImport(excelData)
          
          // Success cleanup (don't close yet - wait for import completion)
          setFile(null)
          
        } catch (error) {
          console.error("Excel processing error:", error)
          toast({
            title: "Error processing Excel file",
            description: error instanceof Error ? error.message : "Failed to process Excel file",
            variant: "destructive"
          })
        } finally {
          setIsLoading(false)
        }
        
      } else {
        toast({
          title: "Unsupported file type",
          description: "Please select a CSV (.csv) or Excel (.xlsx, .xls) file",
          variant: "destructive"
        })
        setIsLoading(false)
      }
      
    } catch (error) {
      console.error("Upload error:", error)
      toast({
        title: "Upload failed",
        description: "An unexpected error occurred during file upload",
        variant: "destructive"
      })
      setIsLoading(false)
    }
  }

  const downloadTemplate = () => {
    // Account fields template - matching the exact field names used in import mapping
    const headers = [
      'Account Name',    // maps to: item['Account Name'] || item.accountName
      'Contact Name',    // maps to: item['Contact Name'] || item.contactName  
      'Industry',        // maps to: item.industry || item.Industry  
      'Contact Phone',   // maps to: item['Contact Phone'] || item.phone
      'Email',           // maps to: item.email || item.Email || item['Email Address']
      'Website',         // maps to: item.website || item.Website
      'Address',         // maps to: item.address || item.Address
      'City',            // maps to: item.city || item.City
      'State',           // maps to: item.state || item.State  
      'Country',         // maps to: item.country || item.Country
      'Assigned To'      // maps to: item['Assigned To'] || item.assignedTo
    ]
    
    // Sample data for accounts import (with address column added)
    const sampleData = [
      'ABC Corporation,John Smith,Biotech Company,+91 9876543210,john.smith@abccorp.com,www.abccorp.com,123 MG Road Koramangala,Mumbai,Maharashtra,India,Sales Rep 1',
      'XYZ Labs,Sarah Johnson,Pharmaceutical,+91 9876543211,sarah.johnson@xyzlabs.com,www.xyzlabs.com,456 Brigade Road,Bangalore,Karnataka,India,Sales Rep 2', 
      'Tech Solutions Ltd,Mike Brown,Research,+91 9876543212,mike.brown@techsolutions.com,www.techsolutions.com,789 FC Road,Pune,Maharashtra,India,Sales Rep 1',
      'BioMed Industries,Lisa Davis,Molecular Diagnostics,+91 9876543213,lisa.davis@biomed.com,www.biomed.com,321 Anna Salai,Chennai,Tamil Nadu,India,Sales Rep 3',
      'Research University,Dr. Kumar,Universities,+91 9876543214,dr.kumar@researchuni.edu,www.researchuni.edu,654 CP Connaught Place,Delhi,Delhi,India,Sales Rep 2',
      'Food Corp,Priya Sharma,Food and Beverages,+91 9876543215,priya.sharma@foodcorp.com,www.foodcorp.com,987 Park Street,Kolkata,West Bengal,India,Sales Rep 1'
    ]
    
    // Create CSV content
    const csvContent = headers.join(',') + '\n' + sampleData.join('\n')
    
    // Create and download file
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' })
    const link = document.createElement('a')
    const url = URL.createObjectURL(blob)
    link.setAttribute('href', url)
    link.setAttribute('download', 'accounts_import_template.csv')
    link.style.visibility = 'hidden'
    document.body.appendChild(link)
    link.click()
    document.body.removeChild(link)
    
    toast({
      title: "Template downloaded",
      description: "Use this CSV template to format your account data"
    })
  }

  const handleClose = () => {
    setFile(null)
    onClose()
  }

  const getFileIcon = () => {
    if (!file) return <FileText className="w-6 h-6 text-gray-400" />
    
    const extension = file.name.split('.').pop()?.toLowerCase()
    if (extension === 'csv') {
      return <FileText className="w-6 h-6 text-green-600" />
    } else if (extension === 'xlsx' || extension === 'xls') {
      return <FileSpreadsheet className="w-6 h-6 text-blue-600" />
    }
    return <FileText className="w-6 h-6 text-gray-400" />
  }

  return (
    <Dialog open={isOpen} onOpenChange={handleClose}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Import Data File</DialogTitle>
        </DialogHeader>

        <div className="space-y-4">
          {/* Template Download Section */}
          <Card className="border-blue-200 bg-blue-50">
            <CardContent className="p-4">
              <div className="flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-medium text-blue-900">Need a template?</h3>
                  <p className="text-xs text-blue-700">Download our sample CSV template with the correct format</p>
                </div>
                <Button 
                  variant="outline" 
                  size="sm"
                  onClick={downloadTemplate}
                  className="bg-white hover:bg-blue-100 border-blue-300"
                >
                  <Download className="w-4 h-4 mr-2" />
                  Download Template
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="text-base flex items-center space-x-2">
                {getFileIcon()}
                <span>Select File</span>
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <div>
                <input
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  onChange={handleFileSelect}
                  className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-full file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
                />
                <p className="text-xs text-gray-500 mt-2">
                  Supported formats: CSV (.csv), Excel (.xlsx, .xls)
                </p>
              </div>
              
              {file && (
                <div className="p-3 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-center space-x-2">
                    {getFileIcon()}
                    <div>
                      <p className="text-sm font-medium text-green-800">
                        {file.name}
                      </p>
                      <p className="text-xs text-green-600">
                        Size: {(file.size / 1024).toFixed(2)} KB • Type: {file.name.split('.').pop()?.toUpperCase()}
                      </p>
                    </div>
                  </div>
                </div>
              )}
            </CardContent>
          </Card>

          {/* Import Progress Indicator */}
          {isImporting && (
            <Card className="border-orange-200 bg-orange-50">
              <CardContent className="p-4">
                <div className="flex items-center space-x-3">
                  <Loader2 className="h-5 w-5 animate-spin text-orange-600" />
                  <div className="flex-1">
                    <div className="flex justify-between items-center mb-2">
                      <span className="text-sm font-medium text-orange-900">
                        Importing accounts...
                      </span>
                      <span className="text-sm text-orange-700">
                        {importProgress.current} of {importProgress.total}
                      </span>
                    </div>
                    <Progress 
                      value={importProgress.total > 0 ? (importProgress.current / importProgress.total) * 100 : 0}
                      className="h-2"
                    />
                  </div>
                </div>
                <p className="text-xs text-orange-600 mt-2">
                  Please wait while your data is being imported. Do not close this dialog.
                </p>
              </CardContent>
            </Card>
          )}
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleClose} disabled={isLoading || isImporting}>
            <X className="w-4 h-4 mr-2" />
            Cancel
          </Button>
          <Button 
            onClick={handleUpload} 
            disabled={!file || isLoading || isImporting}
            className="bg-blue-600 hover:bg-blue-700"
          >
            {isLoading || isImporting ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                {isImporting ? "Importing..." : "Processing..."}
              </>
            ) : (
              <>
                <Upload className="w-4 h-4 mr-2" />
                Import Data
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}