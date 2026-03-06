"use client"

import { useState, useEffect } from "react"
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogDescription, DialogFooter } from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Upload, Download, FileSpreadsheet, AlertCircle, CheckCircle, X } from "lucide-react"
import { useToast } from "@/hooks/use-toast"
import * as XLSX from 'xlsx'

interface FieldConfig {
  field_name: string
  field_label: string
  field_type: string
  is_mandatory: boolean
  is_enabled: boolean
  field_options?: string[]
}

interface DynamicImportModalProps {
  isOpen: boolean
  onClose: () => void
  onImport: (data: any[]) => Promise<void>
  moduleType: 'accounts' | 'contacts' | 'products' | 'leads' | 'deals'
  isImporting?: boolean
  importProgress?: { current: number; total: number }
}

export function DynamicImportModal({
  isOpen,
  onClose,
  onImport,
  moduleType,
  isImporting = false,
  importProgress = { current: 0, total: 0 }
}: DynamicImportModalProps) {
  const { toast } = useToast()
  const [fieldConfigs, setFieldConfigs] = useState<FieldConfig[]>([])
  const [selectedFile, setSelectedFile] = useState<File | null>(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    if (isOpen) {
      loadFieldConfigurations()
    }
  }, [isOpen, moduleType])

  const loadFieldConfigurations = async () => {
    setLoading(true)
    try {
      const user = localStorage.getItem('user')
      if (!user) return

      const parsedUser = JSON.parse(user)
      const companyId = parsedUser.company_id

      const apiEndpoint = `/api/admin/${moduleType === 'accounts' ? 'account' : moduleType === 'contacts' ? 'contact' : moduleType === 'products' ? 'product' : moduleType === 'deals' ? 'deal' : 'lead'}-fields?companyId=${companyId}`

      const response = await fetch(apiEndpoint)
      if (response.ok) {
        const data: FieldConfig[] = await response.json()
        // Only include enabled fields
        const enabledFields = data.filter(f => f.is_enabled)
        setFieldConfigs(enabledFields)
      }
    } catch (error) {
      console.error('Error loading field configs:', error)
    } finally {
      setLoading(false)
    }
  }

  const handleDownloadTemplate = () => {
    try {
      // Filter out system-generated fields like account_id, lead_id, contact_id, product_id
      const templateFields = fieldConfigs.filter(field =>
        !['account_id', 'lead_id', 'contact_id', 'product_id'].includes(field.field_name)
      )

      // Add product columns for leads
      if (moduleType === 'leads') {
        templateFields.push({
          field_name: 'productNames',
          field_label: 'Product Names (comma-separated)',
          field_type: 'text',
          is_mandatory: false,
          is_enabled: true
        })
        templateFields.push({
          field_name: 'productQuantities',
          field_label: 'Product Quantities (comma-separated)',
          field_type: 'text',
          is_mandatory: false,
          is_enabled: true
        })
      }

      // Create headers from field configurations
      const headers = templateFields.map(field => field.field_label)

      // Create sample data row with placeholders
      const sampleRow = templateFields.map(field => {
        // Special handling for product fields
        if (field.field_name === 'productNames') {
          return 'Headphones, Mouse, Keyboard'
        } else if (field.field_name === 'productQuantities') {
          return '2, 5, 3'
        } else if (field.field_name === 'assigned_to') {
          return 'Account Name (must exist in your accounts)'
        } else if (field.field_type === 'select' && field.field_options && field.field_options.length > 0) {
          return field.field_options[0]
        } else if (field.field_type === 'email') {
          return 'example@company.com'
        } else if (field.field_type === 'phone' || field.field_type === 'tel') {
          return '+91 98765 43210'
        } else if (field.field_type === 'number') {
          return '0'
        } else if (field.field_type === 'date') {
          return new Date().toISOString().split('T')[0]
        } else {
          return `Sample ${field.field_label}`
        }
      })

      // Create worksheet
      const ws = XLSX.utils.aoa_to_sheet([headers, sampleRow])

      // Set column widths
      ws['!cols'] = templateFields.map(() => ({ wch: 20 }))

      // Create workbook
      const wb = XLSX.utils.book_new()
      XLSX.utils.book_append_sheet(wb, ws, moduleType.charAt(0).toUpperCase() + moduleType.slice(1))

      // Download file
      XLSX.writeFile(wb, `${moduleType}_import_template_${new Date().toISOString().split('T')[0]}.xlsx`)

      toast({
        title: "Template downloaded",
        description: "Fill in the template and upload it to import data."
      })
    } catch (error) {
      console.error('Error generating template:', error)
      toast({
        title: "Error",
        description: "Failed to generate template",
        variant: "destructive"
      })
    }
  }

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    if (file) {
      setSelectedFile(file)
    }
  }

  const handleImport = async () => {
    if (!selectedFile) {
      toast({
        title: "No file selected",
        description: "Please select a file to import",
        variant: "destructive"
      })
      return
    }

    try {
      const reader = new FileReader()

      reader.onload = async (e) => {
        try {
          const data = new Uint8Array(e.target?.result as ArrayBuffer)
          const workbook = XLSX.read(data, { type: 'array' })
          const firstSheet = workbook.Sheets[workbook.SheetNames[0]]
          const jsonData = XLSX.utils.sheet_to_json(firstSheet, { header: 1 }) as any[][]

          if (jsonData.length < 2) {
            toast({
              title: "Empty file",
              description: "The file doesn't contain any data rows",
              variant: "destructive"
            })
            return
          }

          // Map Excel data to field names
          const headers = jsonData[0] as string[]
          const rows = jsonData.slice(1)

          // Create mapping from field label to field name
          const labelToFieldName = new Map<string, string>()
          fieldConfigs.forEach(config => {
            labelToFieldName.set(config.field_label.toLowerCase(), config.field_name)
          })

          // Add product fields mapping for leads
          if (moduleType === 'leads') {
            labelToFieldName.set('product names (comma-separated)', 'productNames')
            labelToFieldName.set('product quantities (comma-separated)', 'productQuantities')
          }

          const mappedData = rows
            .filter(row => row.some(cell => cell !== undefined && cell !== null && cell !== ''))
            .map(row => {
              const obj: Record<string, any> = {}
              headers.forEach((header, index) => {
                const fieldName = labelToFieldName.get(header.toLowerCase())
                if (fieldName && row[index] !== undefined && row[index] !== null) {
                  obj[fieldName] = row[index]
                }
              })
              return obj
            })

          console.log('Mapped import data:', mappedData)

          if (mappedData.length === 0) {
            toast({
              title: "No valid data",
              description: "No valid rows found in the file",
              variant: "destructive"
            })
            return
          }

          // Call the import handler
          await onImport(mappedData)
          setSelectedFile(null)

          // Close the modal after import completes
          setTimeout(() => {
            onClose()
          }, 500)
        } catch (error) {
          console.error('Error parsing file:', error)
          toast({
            title: "Error",
            description: "Failed to parse the file. Please ensure it's a valid Excel file.",
            variant: "destructive"
          })
        }
      }

      reader.readAsArrayBuffer(selectedFile)
    } catch (error) {
      console.error('Error importing:', error)
      toast({
        title: "Import failed",
        description: "An error occurred while importing the file",
        variant: "destructive"
      })
    }
  }

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <DialogTitle className="flex items-center space-x-2">
            <Upload className="w-5 h-5" />
            <span>Import {moduleType.charAt(0).toUpperCase() + moduleType.slice(1)}</span>
          </DialogTitle>
          <DialogDescription>
            Download the template with your configured fields, fill it with data, and upload to import.
          </DialogDescription>
        </DialogHeader>

        {loading ? (
          <div className="py-8 text-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mx-auto mb-4"></div>
            <p className="text-sm text-gray-600">Loading field configuration...</p>
          </div>
        ) : (
          <div className="space-y-6">
            {/* Step 1: Download Template */}
            <div className="border rounded-lg p-4">
              <div className="flex items-start space-x-3">
                <div className="flex-shrink-0">
                  <div className="w-8 h-8 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center font-semibold">
                    1
                  </div>
                </div>
                <div className="flex-1">
                  <h3 className="font-semibold mb-2">Download Template</h3>
                  <p className="text-sm text-gray-600 mb-3">
                    Download the Excel template with {fieldConfigs.length} configured fields ({fieldConfigs.filter(f => f.is_mandatory).length} required)
                  </p>
                  <Button onClick={handleDownloadTemplate} variant="outline" size="sm">
                    <Download className="w-4 h-4 mr-2" />
                    Download Template
                  </Button>
                </div>
              </div>
            </div>

            {/* Step 2: Fill Template */}
            <div className="border rounded-lg p-4">
              <div className="flex items-start space-x-3">
                <div className="flex-shrink-0">
                  <div className="w-8 h-8 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center font-semibold">
                    2
                  </div>
                </div>
                <div className="flex-1">
                  <h3 className="font-semibold mb-2">Fill in Your Data</h3>
                  <p className="text-sm text-gray-600">
                    Open the template in Excel and fill in your data. Required fields are marked in the sample row.
                  </p>
                  {fieldConfigs.filter(f => f.is_mandatory).length > 0 && (
                    <div className="mt-2 p-2 bg-amber-50 border border-amber-200 rounded text-xs">
                      <p className="font-medium text-amber-900">Required fields:</p>
                      <p className="text-amber-800">
                        {fieldConfigs.filter(f => f.is_mandatory).map(f => f.field_label).join(', ')}
                      </p>
                    </div>
                  )}
                  {moduleType === 'accounts' && (
                    <div className="mt-2 p-2 bg-blue-50 border border-blue-200 rounded text-xs">
                      <p className="font-medium text-blue-900">For Distributor accounts:</p>
                      <p className="text-blue-800">
                        If Account Type is "Distributor", you can add multiple industries by separating them with commas.
                        Example: <span className="font-mono">Pharma Biopharma, Chemicals Petrochemicals</span>
                      </p>
                    </div>
                  )}
                </div>
              </div>
            </div>

            {/* Step 3: Upload File */}
            <div className="border rounded-lg p-4">
              <div className="flex items-start space-x-3">
                <div className="flex-shrink-0">
                  <div className="w-8 h-8 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center font-semibold">
                    3
                  </div>
                </div>
                <div className="flex-1">
                  <h3 className="font-semibold mb-2">Upload Completed File</h3>
                  <p className="text-sm text-gray-600 mb-3">
                    Select your completed Excel file to import the data
                  </p>
                  <div className="space-y-3">
                    <input
                      type="file"
                      accept=".xlsx,.xls"
                      onChange={handleFileSelect}
                      className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
                      disabled={isImporting}
                    />
                    {selectedFile && (
                      <div className="flex items-center space-x-2 text-sm">
                        <FileSpreadsheet className="w-4 h-4 text-green-600" />
                        <span className="text-gray-700">{selectedFile.name}</span>
                        <span className="text-gray-500">({(selectedFile.size / 1024).toFixed(1)} KB)</span>
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>

            {/* Import Progress */}
            {isImporting && (
              <div className="border rounded-lg p-4 bg-blue-50">
                <div className="flex items-center space-x-3">
                  <div className="animate-spin rounded-full h-5 w-5 border-b-2 border-blue-600"></div>
                  <div className="flex-1">
                    <p className="font-medium text-blue-900">Importing...</p>
                    <p className="text-sm text-blue-700">
                      Processing {importProgress.current} of {importProgress.total} records
                    </p>
                  </div>
                </div>
                <div className="mt-3 bg-blue-200 rounded-full h-2 overflow-hidden">
                  <div
                    className="bg-blue-600 h-full transition-all duration-300"
                    style={{ width: `${importProgress.total > 0 ? (importProgress.current / importProgress.total) * 100 : 0}%` }}
                  />
                </div>
              </div>
            )}
          </div>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={onClose} disabled={isImporting}>
            <X className="w-4 h-4 mr-2" />
            Cancel
          </Button>
          <Button
            onClick={handleImport}
            disabled={!selectedFile || isImporting || loading}
            className="bg-blue-600 hover:bg-blue-700"
          >
            {isImporting ? (
              <>
                <div className="w-4 h-4 mr-2 border-2 border-white border-t-transparent rounded-full animate-spin" />
                Importing...
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
