"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"

export function SimpleExcelTest() {
  const [result, setResult] = useState<string>("")
  
  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    try {
      const file = event.target.files?.[0]
      if (!file) return
      
      setResult(`File selected: ${file.name}, Size: ${file.size} bytes, Type: ${file.type}`)
      
      // Try dynamic import to avoid build-time issues
      try {
        const XLSX = await import('xlsx')
        setResult(prev => prev + "\nXLSX library loaded successfully")
        
        const reader = new FileReader()
        
        reader.onload = (e) => {
          try {
            setResult(prev => prev + "\nFileReader onload triggered")
            
            const data = e.target?.result as ArrayBuffer
            if (!data) {
              setResult(prev => prev + "\nError: No data from FileReader")
              return
            }
            
            setResult(prev => prev + `\nArrayBuffer size: ${data.byteLength} bytes`)
            
            const uint8Array = new Uint8Array(data)
            const workbook = XLSX.read(uint8Array, { type: 'array' })
            
            setResult(prev => prev + `\nWorkbook loaded with ${workbook.SheetNames.length} sheets: ${workbook.SheetNames.join(', ')}`)
            
            // Get first sheet
            const firstSheetName = workbook.SheetNames[0]
            const firstSheet = workbook.Sheets[firstSheetName]
            const jsonData = XLSX.utils.sheet_to_json(firstSheet, { header: 1 })
            
            setResult(prev => prev + `\nFound ${jsonData.length} rows in sheet '${firstSheetName}'`)
            
            if (jsonData.length > 0) {
              const headers = jsonData[0] as any[]
              setResult(prev => prev + `\nHeaders: ${headers.join(', ')}`)
            }
            
            setResult(prev => prev + "\n✅ Excel processing completed successfully!")
            
          } catch (parseError) {
            setResult(prev => prev + `\n❌ Error parsing Excel: ${parseError instanceof Error ? parseError.message : String(parseError)}`)
            console.error("Excel parsing error:", parseError)
          }
        }
        
        reader.onerror = (error) => {
          setResult(prev => prev + `\n❌ FileReader error: ${error}`)
          console.error("FileReader error:", error)
        }
        
        setResult(prev => prev + "\nStarting to read file as ArrayBuffer...")
        reader.readAsArrayBuffer(file)
        
      } catch (importError) {
        setResult(prev => prev + `\n❌ Error loading XLSX library: ${importError instanceof Error ? importError.message : String(importError)}`)
        console.error("XLSX import error:", importError)
      }
      
    } catch (outerError) {
      const errorMessage = outerError instanceof Error ? outerError.message : String(outerError)
      setResult(`❌ Outer error: ${errorMessage}`)
      console.error("Outer error:", outerError)
    }
  }
  
  return (
    <Card>
      <CardHeader>
        <CardTitle>Simple Excel Test</CardTitle>
      </CardHeader>
      <CardContent>
        <input
          type="file"
          accept=".xlsx,.xls"
          onChange={handleFileUpload}
        />
        <pre className="mt-4 p-4 bg-gray-100 rounded">{result}</pre>
      </CardContent>
    </Card>
  )
}