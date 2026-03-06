"use client"

import { useState, useEffect, useRef, useCallback } from "react"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Badge } from "@/components/ui/badge"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { useToast } from "@/hooks/use-toast"
import { EnhancedCard } from "@/components/ui/enhanced-card"
import { 
  Search, Upload, FolderOpen, File, Download, Eye, Trash2, FolderPlus, 
  Plus, FileX, Clock, User, Building, X, FileText,
  HardDrive, Users, TrendingUp
} from "lucide-react"

const fileTypes = ["All", "pdf", "docx", "xlsx", "pptx", "jpg", "png", "txt", "csv"]

export function DocLibraryContent() {
  const { toast } = useToast()
  const fileInputRef = useRef<HTMLInputElement>(null)
  
  // State management
  const [folders, setFolders] = useState<any[]>([])
  const [documents, setDocuments] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [searchTerm, setSearchTerm] = useState("")
  const [selectedFolder, setSelectedFolder] = useState("All")
  const [selectedFileType, setSelectedFileType] = useState("All")
  const [viewMode, setViewMode] = useState<"folders" | "documents">("folders")
  const [selectedFolderId, setSelectedFolderId] = useState<string | null>(null)
  
  // Dialog states
  const [showNewFolderDialog, setShowNewFolderDialog] = useState(false)
  const [showDeleteDialog, setShowDeleteDialog] = useState(false)
  const [itemToDelete, setItemToDelete] = useState<any>(null)
  
  // Upload states
  const [uploading, setUploading] = useState(false)
  
  // Form states
  const [newFolderData, setNewFolderData] = useState({
    folder_name: "",
    folder_description: "",
    folder_color: "#3B82F6"
  })
  

  // Stats state
  const [stats, setStats] = useState({
    totalDocuments: 0,
    totalFolders: 0,
    storageUsed: 0,
    recentUploads: 0
  })

  // Load data on component mount
  useEffect(() => {
    loadFolders()
    loadDocuments()
  }, [])

  // Load folders from API
  const loadFolders = async () => {
    try {
      const response = await fetch('/api/document-folders')
      if (response.ok) {
        const data = await response.json()
        setFolders(data.folders || [])
        
        // Update folder count in stats
        setStats(prevStats => ({
          ...prevStats,
          totalFolders: data.folders?.length || 0
        }))
      } else {
        console.error('Failed to fetch folders')
        toast({
          title: "Error",
          description: "Failed to load folders",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error loading folders:', error)
      toast({
        title: "Error",
        description: "Failed to load folders",
        variant: "destructive"
      })
    }
  }

  // Load documents from API
  const loadDocuments = async () => {
    try {
      setLoading(true)
      let url = '/api/documents'
      
      const params = new URLSearchParams()
      if (selectedFolderId) {
        params.append('folder_id', selectedFolderId)
      }
      if (searchTerm) {
        params.append('search', searchTerm)
      }
      if (selectedFileType !== "All") {
        params.append('file_type', selectedFileType)
      }
      
      if (params.toString()) {
        url += `?${params.toString()}`
      }

      const response = await fetch(url)
      if (response.ok) {
        const data = await response.json()
        setDocuments(data.documents || [])
        
        // Update stats
        setStats(prevStats => ({
          ...prevStats,
          totalDocuments: data.total || 0,
          storageUsed: calculateStorageUsed(data.documents || []),
          recentUploads: calculateRecentUploads(data.documents || [])
        }))
      } else {
        console.error('Failed to fetch documents')
        toast({
          title: "Error",
          description: "Failed to load documents",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error loading documents:', error)
      toast({
        title: "Error",
        description: "Failed to load documents",
        variant: "destructive"
      })
    } finally {
      setLoading(false)
    }
  }

  // Reload documents when filters change
  useEffect(() => {
    if (folders.length > 0) { // Only load documents after folders are loaded
      const timeoutId = setTimeout(() => {
        loadDocuments()
      }, 300) // Debounce search

      return () => clearTimeout(timeoutId)
    }
  }, [searchTerm, selectedFolder, selectedFileType, selectedFolderId, folders.length])

  // Calculate storage used
  const calculateStorageUsed = (docs: any[]) => {
    const totalBytes = docs.reduce((sum, doc) => sum + (doc.file_size || 0), 0)
    return Math.round(totalBytes / (1024 * 1024 * 1024) * 100) / 100 // GB
  }

  // Calculate recent uploads (last 7 days)
  const calculateRecentUploads = (docs: any[]) => {
    const sevenDaysAgo = new Date()
    sevenDaysAgo.setDate(sevenDaysAgo.getDate() - 7)
    return docs.filter(doc => new Date(doc.created_at) > sevenDaysAgo).length
  }

  // Handle folder creation
  const handleCreateFolder = async () => {
    if (!newFolderData.folder_name.trim()) {
      toast({
        title: "Error",
        description: "Folder name is required",
        variant: "destructive"
      })
      return
    }

    try {
      const response = await fetch('/api/document-folders', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          ...newFolderData,
          parent_folder_id: selectedFolderId,
          created_by: 'de19ccb7-e90d-4507-861d-a3aecf5e3f29' // Using company ID as placeholder
        }),
      })

      if (response.ok) {
        const data = await response.json()
        toast({
          title: "Success",
          description: "Folder created successfully"
        })
        setShowNewFolderDialog(false)
        setNewFolderData({ folder_name: "", folder_description: "", folder_color: "#3B82F6" })
        loadFolders()
      } else {
        const errorData = await response.json()
        toast({
          title: "Error",
          description: errorData.error || "Failed to create folder",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error creating folder:', error)
      toast({
        title: "Error",
        description: "Failed to create folder",
        variant: "destructive"
      })
    }
  }


  // Handle file input change - simplified direct upload
  const handleFileInputChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = e.target.files
    if (!files || files.length === 0) return

    setUploading(true)

    try {
      const uploadPromises = Array.from(files).map(async (file) => {
        const formData = new FormData()
        formData.append('file', file)
        formData.append('folder_id', selectedFolderId || '')
        formData.append('description', 'Uploaded via Doc Library')
        formData.append('tags', '')
        formData.append('access_level', 'Company')
        formData.append('uploaded_by', 'de19ccb7-e90d-4507-861d-a3aecf5e3f29')
        formData.append('uploaded_by_name', 'Current User')

        const response = await fetch('/api/documents/simple-upload', {
          method: 'POST',
          body: formData,
        })

        if (!response.ok) {
          const errorData = await response.json()
          throw new Error(errorData.error || 'Upload failed')
        }

        return response.json()
      })

      await Promise.all(uploadPromises)

      toast({
        title: "Success",
        description: `${files.length} file(s) uploaded successfully`
      })

      loadDocuments()

    } catch (error) {
      console.error('Upload error:', error)
      toast({
        title: "Error",
        description: error instanceof Error ? error.message : "Failed to upload files",
        variant: "destructive"
      })
    } finally {
      setUploading(false)
      // Reset the file input
      if (e.target) {
        e.target.value = ''
      }
    }
  }


  // Handle document download
  const handleDownload = async (documentId: string) => {
    // For demo purposes, just show a toast since we don't have actual file storage
    const document = documents.find(doc => doc.id === documentId)
    toast({
      title: "Download Started",
      description: `File: ${document?.original_filename || 'Unknown'} (File storage not implemented yet)`
    })
  }

  // Handle delete confirmation
  const confirmDelete = (item: any, type: 'folder' | 'document') => {
    setItemToDelete({ ...item, type })
    setShowDeleteDialog(true)
  }

  // Handle actual deletion
  const handleDelete = async () => {
    if (!itemToDelete) return

    try {
      const endpoint = itemToDelete.type === 'folder' ? '/api/document-folders' : '/api/documents'
      const response = await fetch(`${endpoint}?id=${itemToDelete.id}&user_id=de19ccb7-e90d-4507-861d-a3aecf5e3f29&user_name=Current User`, {
        method: 'DELETE'
      })

      if (response.ok) {
        toast({
          title: "Success",
          description: `${itemToDelete.type === 'folder' ? 'Folder' : 'Document'} deleted successfully`
        })
        
        setShowDeleteDialog(false)
        setItemToDelete(null)
        
        if (itemToDelete.type === 'folder') {
          loadFolders()
        } else {
          loadDocuments()
        }
      } else {
        const errorData = await response.json()
        toast({
          title: "Error",
          description: errorData.error || "Failed to delete item",
          variant: "destructive"
        })
      }
    } catch (error) {
      console.error('Error deleting item:', error)
      toast({
        title: "Error",
        description: "Failed to delete item",
        variant: "destructive"
      })
    }
  }

  // Get file type color
  const getFileTypeColor = (type: string) => {
    switch (type.toLowerCase()) {
      case "pdf":
        return "bg-red-100 text-red-800"
      case "docx":
      case "doc":
        return "bg-blue-100 text-blue-800"
      case "xlsx":
      case "xls":
        return "bg-green-100 text-green-800"
      case "pptx":
      case "ppt":
        return "bg-orange-100 text-orange-800"
      case "jpg":
      case "jpeg":
      case "png":
      case "gif":
        return "bg-purple-100 text-purple-800"
      default:
        return "bg-gray-100 text-gray-800"
    }
  }

  // Format file size
  const formatFileSize = (bytes: number) => {
    if (bytes === 0) return '0 Bytes'
    const k = 1024
    const sizes = ['Bytes', 'KB', 'MB', 'GB']
    const i = Math.floor(Math.log(bytes) / Math.log(k))
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
  }

  // Format date
  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleDateString('en-IN', {
      day: '2-digit',
      month: 'short',
      year: 'numeric'
    })
  }

  // Enhanced stats for the cards
  const statsCards = [
    {
      title: "Total Documents",
      value: stats.totalDocuments.toString(),
      change: "Files in library",
      icon: FileText,
      iconColor: "text-blue-600",
      iconBg: "bg-blue-100"
    },
    {
      title: "Folders",
      value: stats.totalFolders.toString(),
      change: "Organization units",
      icon: FolderOpen,
      iconColor: "text-green-600",
      iconBg: "bg-green-100"
    },
    {
      title: "Storage Used",
      value: `${stats.storageUsed} GB`,
      change: "of available storage",
      icon: HardDrive,
      iconColor: "text-purple-600",
      iconBg: "bg-purple-100"
    },
    {
      title: "Recent Uploads",
      value: stats.recentUploads.toString(),
      change: "This week",
      icon: TrendingUp,
      iconColor: "text-orange-600",
      iconBg: "bg-orange-100"
    }
  ]

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div className="flex justify-between items-center">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Document Library</h1>
          <p className="text-gray-600">Organize and manage your documents and files</p>
        </div>
        <div className="flex space-x-2">
          <Button 
            variant="outline" 
            onClick={() => setViewMode(viewMode === "folders" ? "documents" : "folders")}
          >
            {viewMode === "folders" ? "View Documents" : "View Folders"}
          </Button>
          <Button 
            variant="outline"
            onClick={() => setShowNewFolderDialog(true)}
          >
            <FolderPlus className="w-4 h-4 mr-2" />
            New Folder
          </Button>
          <Button 
            onClick={() => fileInputRef.current?.click()}
            disabled={uploading}
          >
            <Upload className="w-4 h-4 mr-2" />
            {uploading ? "Uploading..." : "Upload Document"}
          </Button>
        </div>
      </div>

      {/* Hidden file input */}
      <input
        ref={fileInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={handleFileInputChange}
        accept=".pdf,.doc,.docx,.xls,.xlsx,.ppt,.pptx,.txt,.csv,.jpg,.jpeg,.png,.gif"
      />

      {/* Enhanced Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        {statsCards.map((stat) => (
          <EnhancedCard 
            key={stat.title} 
            title={stat.title} 
            value={stat.value} 
            description={stat.change}
            icon={stat.icon}
            iconColor={stat.iconColor}
            iconBg={stat.iconBg}
          />
        ))}
      </div>


      {/* Search & Filters */}
      <Card>
        <CardHeader>
          <CardTitle>Search & Filters</CardTitle>
          <CardDescription>Find documents and folders quickly</CardDescription>
        </CardHeader>
        <CardContent>
          <div className="flex flex-col md:flex-row gap-4">
            <div className="flex-1">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400 w-4 h-4" />
                <Input
                  placeholder="Search documents by name, folder, or tags..."
                  value={searchTerm}
                  onChange={(e) => setSearchTerm(e.target.value)}
                  className="pl-10"
                />
              </div>
            </div>
            {viewMode === "documents" && (
              <>
                <Select value={selectedFolder} onValueChange={setSelectedFolder}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="Folder" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="All">All Folders</SelectItem>
                    {folders.map((folder) => (
                      <SelectItem key={folder.id} value={folder.id}>
                        {folder.folder_name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <Select value={selectedFileType} onValueChange={setSelectedFileType}>
                  <SelectTrigger className="w-48">
                    <SelectValue placeholder="File Type" />
                  </SelectTrigger>
                  <SelectContent>
                    {fileTypes.map((type) => (
                      <SelectItem key={type} value={type}>
                        {type.toUpperCase()}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </>
            )}
          </div>
        </CardContent>
      </Card>


      {/* Content Area */}
      {viewMode === "folders" ? (
        <Card>
          <CardHeader>
            <CardTitle>Folders</CardTitle>
            <CardDescription>Organize your documents in folders</CardDescription>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="text-center py-12">
                <p className="text-gray-500">Loading folders...</p>
              </div>
            ) : folders.length === 0 ? (
              <div className="text-center py-12">
                <FolderOpen className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No folders found</p>
                <Button 
                  variant="outline" 
                  className="mt-4"
                  onClick={() => setShowNewFolderDialog(true)}
                >
                  Create First Folder
                </Button>
              </div>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                {folders.map((folder) => (
                  <Card 
                    key={folder.id} 
                    className="hover:shadow-md transition-shadow cursor-pointer"
                    onClick={() => {
                      setSelectedFolderId(folder.id)
                      setViewMode("documents")
                    }}
                  >
                    <CardContent className="p-4">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center space-x-3 flex-1">
                          <FolderOpen 
                            className="w-8 h-8" 
                            style={{ color: folder.folder_color || '#3B82F6' }}
                          />
                          <div className="flex-1 min-w-0">
                            <h3 className="font-medium truncate">{folder.folder_name}</h3>
                            <p className="text-sm text-gray-500">{folder.document_count || 0} files</p>
                            <p className="text-xs text-gray-400">
                              Updated {formatDate(folder.updated_at)}
                            </p>
                          </div>
                        </div>
                        <div className="flex items-center space-x-1">
                          <Button 
                            variant="ghost" 
                            size="sm"
                            onClick={(e) => {
                              e.stopPropagation()
                              confirmDelete(folder, 'folder')
                            }}
                            className="text-red-600 hover:text-red-800 hover:bg-red-50"
                          >
                            <Trash2 className="w-4 h-4" />
                          </Button>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      ) : (
        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Documents</CardTitle>
                <CardDescription>
                  {selectedFolderId 
                    ? `Documents in ${folders.find(f => f.id === selectedFolderId)?.folder_name || 'Selected Folder'}`
                    : "All documents in your library"
                  }
                </CardDescription>
              </div>
              {selectedFolderId && (
                <Button 
                  variant="outline" 
                  onClick={() => {
                    setSelectedFolderId(null)
                    setSelectedFolder("All")
                  }}
                >
                  <X className="w-4 h-4 mr-2" />
                  Show All Documents
                </Button>
              )}
            </div>
          </CardHeader>
          <CardContent>
            {loading ? (
              <div className="text-center py-12">
                <p className="text-gray-500">Loading documents...</p>
              </div>
            ) : documents.length === 0 ? (
              <div className="text-center py-12">
                <FileX className="w-12 h-12 text-gray-300 mx-auto mb-4" />
                <p className="text-gray-500">No documents found</p>
                {searchTerm && (
                  <p className="text-sm text-gray-400 mt-2">
                    Try adjusting your search terms
                  </p>
                )}
              </div>
            ) : (
              <div className="overflow-x-auto">
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Document Name</TableHead>
                      <TableHead>Folder</TableHead>
                      <TableHead>Type</TableHead>
                      <TableHead>Size</TableHead>
                      <TableHead>Uploaded By</TableHead>
                      <TableHead>Upload Date</TableHead>
                      <TableHead>Downloads</TableHead>
                      <TableHead>Actions</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {documents.map((doc) => (
                      <TableRow key={doc.id}>
                        <TableCell>
                          <div>
                            <p className="font-medium">{doc.document_name}</p>
                            {doc.description && (
                              <p className="text-sm text-gray-500 truncate max-w-xs">
                                {doc.description}
                              </p>
                            )}
                          </div>
                        </TableCell>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            <FolderOpen 
                              className="w-4 h-4" 
                              style={{ color: doc.document_folders?.folder_color || '#3B82F6' }}
                            />
                            <span>{doc.document_folders?.folder_name || 'Root'}</span>
                          </div>
                        </TableCell>
                        <TableCell>
                          <Badge className={getFileTypeColor(doc.file_extension)}>
                            {doc.file_extension.toUpperCase()}
                          </Badge>
                        </TableCell>
                        <TableCell>{formatFileSize(doc.file_size)}</TableCell>
                        <TableCell>{doc.uploaded_by_name}</TableCell>
                        <TableCell>{formatDate(doc.created_at)}</TableCell>
                        <TableCell>{doc.download_count || 0}</TableCell>
                        <TableCell>
                          <div className="flex space-x-1">
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              title="View"
                              onClick={() => {
                                toast({
                                  title: "File Preview",
                                  description: `File: ${doc.original_filename} (File storage not implemented yet)`
                                })
                              }}
                            >
                              <Eye className="w-4 h-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              title="Download"
                              onClick={() => handleDownload(doc.id)}
                            >
                              <Download className="w-4 h-4" />
                            </Button>
                            <Button 
                              variant="ghost" 
                              size="sm" 
                              title="Delete"
                              onClick={() => confirmDelete(doc, 'document')}
                              className="text-red-600 hover:text-red-800 hover:bg-red-50"
                            >
                              <Trash2 className="w-4 h-4" />
                            </Button>
                          </div>
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* New Folder Dialog */}
      <Dialog open={showNewFolderDialog} onOpenChange={setShowNewFolderDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create New Folder</DialogTitle>
            <DialogDescription>
              Create a new folder to organize your documents.
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div>
              <Label htmlFor="folder-name">Folder Name</Label>
              <Input
                id="folder-name"
                value={newFolderData.folder_name}
                onChange={(e) => setNewFolderData(prev => ({ ...prev, folder_name: e.target.value }))}
                placeholder="Enter folder name"
              />
            </div>
            <div>
              <Label htmlFor="folder-description">Description (Optional)</Label>
              <Textarea
                id="folder-description"
                value={newFolderData.folder_description}
                onChange={(e) => setNewFolderData(prev => ({ ...prev, folder_description: e.target.value }))}
                placeholder="Enter folder description"
                rows={3}
              />
            </div>
            <div>
              <Label htmlFor="folder-color">Folder Color</Label>
              <div className="flex items-center space-x-2">
                <Input
                  id="folder-color"
                  type="color"
                  value={newFolderData.folder_color}
                  onChange={(e) => setNewFolderData(prev => ({ ...prev, folder_color: e.target.value }))}
                  className="w-20 h-10"
                />
                <span className="text-sm text-gray-500">{newFolderData.folder_color}</span>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowNewFolderDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreateFolder}>
              Create Folder
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>


      {/* Delete Confirmation Dialog */}
      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Confirm Deletion</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete this {itemToDelete?.type}? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          {itemToDelete && (
            <div className="p-4 bg-gray-50 rounded-lg">
              <p className="font-medium">
                {itemToDelete.type === 'folder' ? itemToDelete.folder_name : itemToDelete.document_name}
              </p>
              {itemToDelete.type === 'folder' && (
                <p className="text-sm text-gray-600">
                  Contains {itemToDelete.document_count || 0} documents
                </p>
              )}
            </div>
          )}
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowDeleteDialog(false)}>
              Cancel
            </Button>
            <Button variant="destructive" onClick={handleDelete}>
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}