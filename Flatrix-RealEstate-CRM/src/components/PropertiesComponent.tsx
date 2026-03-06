'use client'

import { useState } from 'react'
import { 
  Plus, 
  MapPin,
  Building2,
  X,
  Edit,
  Calendar,
  Users,
  Home,
  Phone,
  User,
  MoreVertical,
  Eye
} from 'lucide-react'
import { useProjects } from '@/hooks/useDatabase'
import { supabase } from '@/lib/supabase'

// Custom WhatsApp Icon Component
const WhatsAppIcon = ({ className }: { className?: string }) => (
  <svg className={className} viewBox="0 0 24 24" fill="currentColor">
    <path d="M12.031 2c-5.523 0-10.031 4.507-10.031 10.031 0 1.74.443 3.387 1.288 4.816l-1.288 4.153 4.232-1.245c1.389.761 2.985 1.214 4.661 1.214h.004c5.522 0 10.031-4.507 10.031-10.031 0-2.672-1.041-5.183-2.93-7.071-1.892-1.892-4.405-2.937-7.071-2.937zm.004 18.375h-.003c-1.465 0-2.899-.394-4.148-1.14l-.297-.177-3.085.808.821-2.998-.193-.307c-.821-1.302-1.253-2.807-1.253-4.358 0-4.516 3.677-8.192 8.193-8.192 2.186 0 4.239.852 5.784 2.397 1.545 1.545 2.397 3.598 2.397 5.783-.003 4.518-3.68 8.194-8.196 8.194zm4.5-6.143c-.247-.124-1.463-.722-1.69-.805-.227-.083-.392-.124-.558.124-.166.248-.642.805-.786.969-.145.165-.29.186-.537.062-.247-.124-1.045-.385-1.99-1.227-.736-.657-1.233-1.468-1.378-1.715-.145-.247-.016-.381.109-.504.112-.111.247-.29.371-.434.124-.145.166-.248.248-.413.083-.166.042-.31-.021-.434-.062-.124-.558-1.343-.765-1.839-.201-.479-.407-.414-.558-.422-.145-.007-.31-.009-.476-.009-.166 0-.435.062-.662.31-.227.248-.866.847-.866 2.067 0 1.22.889 2.395 1.013 2.56.124.166 1.75 2.667 4.24 3.74.592.256 1.055.408 1.415.523.594.189 1.135.162 1.563.098.476-.071 1.463-.598 1.669-1.175.207-.577.207-1.071.145-1.175-.062-.104-.227-.165-.476-.289z"/>
  </svg>
)

export default function PropertiesComponent() {
  const { data: projects, loading: projectsLoading, refetch } = useProjects()
  const [showAddModal, setShowAddModal] = useState(false)
  const [showEditModal, setShowEditModal] = useState(false)
  const [editingProject, setEditingProject] = useState<any>(null)
  const [isSubmitting, setIsSubmitting] = useState(false)
  const [formData, setFormData] = useState({
    name: '',
    location: '',
    city: '',
    developer_name: '',
    total_units: '',
    available_units: '',
    price_range: '',
    completion_date: '',
    description: '',
    project_stage: 'Planning',
    start_date: '',
    number_of_floors: '',
    number_of_towers: '',
    rera_number: '',
    primary_contact_name: '',
    primary_contact_phone: '',
    alternate_contact_name: '',
    alternate_contact_phone: '',
    project_size_acres: '',
    cover_image_url: ''
  })

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement | HTMLTextAreaElement | HTMLSelectElement>) => {
    const { name, value } = e.target
    setFormData(prev => ({ ...prev, [name]: value }))
  }

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (file) {
      const reader = new FileReader()
      reader.onloadend = () => {
        setFormData(prev => ({ ...prev, cover_image_url: reader.result as string }))
      }
      reader.readAsDataURL(file)
    }
  }

  const handleEdit = (project: any) => {
    setEditingProject(project)
    setFormData({
      name: project.name || '',
      location: project.location || '',
      city: project.city || '',
      developer_name: project.developer_name || '',
      total_units: project.total_units?.toString() || '',
      available_units: project.available_units?.toString() || '',
      price_range: project.price_range || '',
      completion_date: project.completion_date || '',
      description: project.description || '',
      project_stage: project.project_stage || 'Planning',
      start_date: project.start_date || '',
      number_of_floors: project.number_of_floors?.toString() || '',
      number_of_towers: project.number_of_towers?.toString() || '',
      rera_number: project.rera_number || '',
      primary_contact_name: project.primary_contact_name || '',
      primary_contact_phone: project.primary_contact_phone || '',
      alternate_contact_name: project.alternate_contact_name || '',
      alternate_contact_phone: project.alternate_contact_phone || '',
      project_size_acres: project.project_size_acres?.toString() || '',
      cover_image_url: project.cover_image_url || ''
    })
    setShowEditModal(true)
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsSubmitting(true)

    try {
      const projectData = {
        name: formData.name,
        location: formData.location,
        city: formData.city,
        developer_name: formData.developer_name,
        total_units: parseInt(formData.total_units) || 0,
        available_units: parseInt(formData.available_units) || 0,
        price_range: formData.price_range || null,
        completion_date: formData.completion_date || null,
        description: formData.description || null,
        project_stage: formData.project_stage as 'Planning' | 'Under Construction' | 'Completed',
        start_date: formData.start_date || null,
        number_of_floors: parseInt(formData.number_of_floors) || null,
        number_of_towers: parseInt(formData.number_of_towers) || null,
        rera_number: formData.rera_number || null,
        primary_contact_name: formData.primary_contact_name || null,
        primary_contact_phone: formData.primary_contact_phone || null,
        alternate_contact_name: formData.alternate_contact_name || null,
        alternate_contact_phone: formData.alternate_contact_phone || null,
        project_size_acres: parseFloat(formData.project_size_acres) || null,
        cover_image_url: formData.cover_image_url || null
      }

      let error;
      if (editingProject) {
        // Update existing project
        const result = await supabase
          .from('flatrix_projects')
          .update(projectData)
          .eq('id', editingProject.id)
        error = result.error
      } else {
        // Insert new project
        const result = await supabase
          .from('flatrix_projects')
          .insert([projectData])
        error = result.error
      }

      if (error) throw error

      // Reset form and close modal
      setFormData({
        name: '',
        location: '',
        city: '',
        developer_name: '',
        total_units: '',
        available_units: '',
        price_range: '',
        completion_date: '',
        description: '',
        project_stage: 'Planning',
        start_date: '',
        number_of_floors: '',
        number_of_towers: '',
        rera_number: '',
        primary_contact_name: '',
        primary_contact_phone: '',
        alternate_contact_name: '',
        alternate_contact_phone: '',
        project_size_acres: '',
        cover_image_url: ''
      })
      setShowAddModal(false)
      setShowEditModal(false)
      setEditingProject(null)
      refetch()
    } catch (error) {
      console.error('Error adding project:', error)
      const errorMessage = error instanceof Error ? error.message : 'An unknown error occurred'
      alert(`Failed to add project: ${errorMessage}`)
    } finally {
      setIsSubmitting(false)
    }
  }

  if (projectsLoading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-lg text-gray-600">Loading projects...</div>
      </div>
    )
  }

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Project Management</h1>
        <p className="text-gray-600 mt-2">Manage real estate projects and developments</p>
      </div>

      <div className="bg-white rounded-lg shadow mb-6">
        <div className="p-6 border-b border-gray-200">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <h2 className="text-xl font-semibold text-gray-900">Projects ({projects?.length || 0})</h2>
            <button 
              onClick={() => setShowAddModal(true)}
              className="flex items-center space-x-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition"
            >
              <Plus className="h-5 w-5" />
              <span>Add Project</span>
            </button>
          </div>
        </div>

        <div className="p-6">
          {projects?.length === 0 ? (
            <div className="text-center text-gray-500 py-8">
              No projects found. Add your first project to get started.
            </div>
          ) : (
            <div className="space-y-6">
              {projects?.map((project) => (
                <div key={project.id} className="group bg-white rounded-2xl shadow-sm border border-gray-100 overflow-hidden hover:shadow-xl hover:border-gray-200 transition-all duration-300">
                  <div className="flex">
                    {/* Image Section */}
                    <div className="relative w-80 h-64 overflow-hidden flex-shrink-0">
                      {project.cover_image_url ? (
                        <img 
                          src={project.cover_image_url} 
                          alt={project.name}
                          className="w-full h-full object-cover group-hover:scale-105 transition-transform duration-300"
                        />
                      ) : (
                        <div className="w-full h-full bg-gradient-to-br from-blue-50 to-indigo-100 flex items-center justify-center">
                          <Building2 className="h-20 w-20 text-blue-300" />
                        </div>
                      )}
                      
                      {/* Status Badge */}
                      <div className="absolute top-4 left-4">
                        <span className={`inline-flex items-center px-3 py-1 rounded-full text-xs font-medium backdrop-blur-sm ${
                          project.project_stage === 'Planning' 
                            ? 'bg-blue-100/90 text-blue-800 border border-blue-200/50' :
                          project.project_stage === 'Under Construction' 
                            ? 'bg-yellow-100/90 text-yellow-800 border border-yellow-200/50' :
                            'bg-green-100/90 text-green-800 border border-green-200/50'
                        }`}>
                          {project.project_stage || 'Planning'}
                        </span>
                      </div>
                    </div>

                    {/* Content Section */}
                    <div className="flex-1 p-6">
                      {/* Header with Title and RERA */}
                      <div className="flex justify-between items-start mb-3">
                        <div className="flex-1">
                          <div className="flex items-center gap-4 mb-2">
                            <h3 className="text-2xl font-bold text-gray-900">{project.name}</h3>
                            {project.rera_number && (
                              <span className="text-sm text-gray-500">RERA: {project.rera_number}</span>
                            )}
                          </div>
                          <div className="flex items-center text-gray-600">
                            <MapPin className="h-4 w-4 mr-2 text-gray-400" />
                            <span className="text-sm">{project.location}, {project.city}</span>
                          </div>
                        </div>
                        
                        {/* Actions Menu */}
                        <div className="flex space-x-2">
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              handleEdit(project);
                            }}
                            className="p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-full transition-colors"
                            title="Edit Project"
                          >
                            <Edit className="h-5 w-5" />
                          </button>
                          <button className="p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-full transition-colors">
                            <MoreVertical className="h-5 w-5" />
                          </button>
                        </div>
                      </div>

                      {/* Metrics Row */}
                      <div className="grid grid-cols-4 gap-4 mb-5">
                        <div className="bg-gray-50 rounded-lg p-3 text-center">
                          <div className="flex items-center justify-center mb-1">
                            <Users className="h-5 w-5 text-gray-400" />
                          </div>
                          <div className="text-2xl font-bold text-gray-900">{project.total_units}</div>
                          <div className="text-xs text-gray-600 font-medium">Total Units</div>
                        </div>
                        
                        <div className="bg-green-50 rounded-lg p-3 text-center">
                          <div className="flex items-center justify-center mb-1">
                            <Home className="h-5 w-5 text-green-500" />
                          </div>
                          <div className="text-2xl font-bold text-green-600">{project.available_units}</div>
                          <div className="text-xs text-gray-600 font-medium">Available</div>
                        </div>

                        <div className="bg-blue-50 rounded-lg p-3 text-center">
                          <div className="text-lg font-bold text-blue-700">{project.price_range || 'Price on Request'}</div>
                          <div className="text-xs text-blue-600 font-medium">Price Range</div>
                        </div>

                        {project.project_size_acres && (
                          <div className="bg-purple-50 rounded-lg p-3 text-center">
                            <div className="text-lg font-bold text-purple-700">{project.project_size_acres}</div>
                            <div className="text-xs text-purple-600 font-medium">Acres</div>
                          </div>
                        )}
                      </div>

                      {/* Configuration and Completion Row */}
                      <div className="grid grid-cols-2 gap-6 pb-4 border-b border-gray-100">
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-gray-600">Configuration</span>
                          <span className="font-semibold text-gray-900 text-sm">2BHK, 2.5BHK, 3BHK2T, 3BHK3T</span>
                        </div>

                        {project.completion_date && (
                          <div className="flex items-center justify-between">
                            <div className="flex items-center text-sm text-gray-600">
                              <Calendar className="h-4 w-4 mr-1" />
                              <span>Completion</span>
                            </div>
                            <span className="font-semibold text-gray-900">
                              {new Date(project.completion_date).toLocaleDateString('en-US', { 
                                month: 'short', 
                                year: 'numeric' 
                              })}
                            </span>
                          </div>
                        )}
                      </div>

                      {/* Contact Information Row */}
                      <div className="grid grid-cols-2 gap-6 pt-4">
                        {project.primary_contact_name && (
                          <div className="flex items-center justify-between">
                            <div className="flex items-center text-sm text-gray-600">
                              <User className="h-4 w-4 mr-1" />
                              <span>Primary Contact</span>
                            </div>
                            <div className="flex items-center gap-3">
                              <div className="text-right">
                                <div className="font-semibold text-gray-900 text-sm">{project.primary_contact_name}</div>
                                {project.primary_contact_phone && (
                                  <div className="text-xs text-gray-500">{project.primary_contact_phone}</div>
                                )}
                              </div>
                              {project.primary_contact_phone && (
                                <div className="flex gap-1">
                                  <a
                                    href={`tel:${project.primary_contact_phone}`}
                                    className="p-1.5 bg-blue-100 hover:bg-blue-200 rounded-full transition-colors"
                                    title="Call"
                                  >
                                    <Phone className="h-3.5 w-3.5 text-blue-600" />
                                  </a>
                                  <a
                                    href={`https://wa.me/${project.primary_contact_phone.replace(/[^0-9]/g, '')}`}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="p-1.5 bg-green-100 hover:bg-green-200 rounded-full transition-colors"
                                    title="WhatsApp"
                                  >
                                    <WhatsAppIcon className="h-3.5 w-3.5 text-green-600" />
                                  </a>
                                </div>
                              )}
                            </div>
                          </div>
                        )}

                        {project.alternate_contact_name && (
                          <div className="flex items-center justify-between">
                            <div className="flex items-center text-sm text-gray-600">
                              <User className="h-4 w-4 mr-1" />
                              <span>Secondary Contact</span>
                            </div>
                            <div className="flex items-center gap-3">
                              <div className="text-right">
                                <div className="font-semibold text-gray-900 text-sm">{project.alternate_contact_name}</div>
                                {project.alternate_contact_phone && (
                                  <div className="text-xs text-gray-500">{project.alternate_contact_phone}</div>
                                )}
                              </div>
                              {project.alternate_contact_phone && (
                                <div className="flex gap-1">
                                  <a
                                    href={`tel:${project.alternate_contact_phone}`}
                                    className="p-1.5 bg-blue-100 hover:bg-blue-200 rounded-full transition-colors"
                                    title="Call"
                                  >
                                    <Phone className="h-3.5 w-3.5 text-blue-600" />
                                  </a>
                                  <a
                                    href={`https://wa.me/${project.alternate_contact_phone.replace(/[^0-9]/g, '')}`}
                                    target="_blank"
                                    rel="noopener noreferrer"
                                    className="p-1.5 bg-green-100 hover:bg-green-200 rounded-full transition-colors"
                                    title="WhatsApp"
                                  >
                                    <WhatsAppIcon className="h-3.5 w-3.5 text-green-600" />
                                  </a>
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                      </div>

                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {/* Add/Edit Project Modal */}
      {(showAddModal || showEditModal) && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg max-w-2xl w-full mx-4 max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between p-6 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">{editingProject ? 'Edit Project' : 'Add New Project'}</h3>
              <button
                onClick={() => {
                  setShowAddModal(false)
                  setShowEditModal(false)
                  setEditingProject(null)
                }}
                className="p-2 hover:bg-gray-100 rounded-lg transition"
              >
                <X className="h-5 w-5" />
              </button>
            </div>
            
            <form onSubmit={handleSubmit} className="p-6 space-y-4">
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Project Name *
                  </label>
                  <input
                    type="text"
                    name="name"
                    value={formData.name}
                    onChange={handleInputChange}
                    required
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter project name"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Location *
                  </label>
                  <input
                    type="text"
                    name="location"
                    value={formData.location}
                    onChange={handleInputChange}
                    required
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter location"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    City *
                  </label>
                  <input
                    type="text"
                    name="city"
                    value={formData.city}
                    onChange={handleInputChange}
                    required
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter city"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Developer Name
                  </label>
                  <input
                    type="text"
                    name="developer_name"
                    value={formData.developer_name}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter developer name"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Total Units
                  </label>
                  <input
                    type="number"
                    name="total_units"
                    value={formData.total_units}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter total units"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Available Units
                  </label>
                  <input
                    type="number"
                    name="available_units"
                    value={formData.available_units}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter available units"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Price Range
                  </label>
                  <input
                    type="text"
                    name="price_range"
                    value={formData.price_range}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="e.g., ₹50L - ₹1.2Cr"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Completion Date
                  </label>
                  <input
                    type="date"
                    name="completion_date"
                    value={formData.completion_date}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Project Stage
                  </label>
                  <select
                    name="project_stage"
                    value={formData.project_stage}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  >
                    <option value="Planning">Planning</option>
                    <option value="Under Construction">Under Construction</option>
                    <option value="Completed">Completed</option>
                  </select>
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Start Date
                  </label>
                  <input
                    type="date"
                    name="start_date"
                    value={formData.start_date}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Number of Floors
                  </label>
                  <input
                    type="number"
                    name="number_of_floors"
                    value={formData.number_of_floors}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter number of floors"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Number of Towers
                  </label>
                  <input
                    type="number"
                    name="number_of_towers"
                    value={formData.number_of_towers}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter number of towers"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    RERA Number
                  </label>
                  <input
                    type="text"
                    name="rera_number"
                    value={formData.rera_number}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter RERA registration number"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Primary Contact Name
                  </label>
                  <input
                    type="text"
                    name="primary_contact_name"
                    value={formData.primary_contact_name}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter primary contact name"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Primary Contact Phone
                  </label>
                  <input
                    type="tel"
                    name="primary_contact_phone"
                    value={formData.primary_contact_phone}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter primary contact phone"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Alternate Contact Name
                  </label>
                  <input
                    type="text"
                    name="alternate_contact_name"
                    value={formData.alternate_contact_name}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter alternate contact name"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Alternate Contact Phone
                  </label>
                  <input
                    type="tel"
                    name="alternate_contact_phone"
                    value={formData.alternate_contact_phone}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter alternate contact phone"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Project Size (Acres)
                  </label>
                  <input
                    type="number"
                    step="0.01"
                    name="project_size_acres"
                    value={formData.project_size_acres}
                    onChange={handleInputChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                    placeholder="Enter project size in acres"
                  />
                </div>
                
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Cover Image
                  </label>
                  <input
                    type="file"
                    accept="image/*"
                    onChange={handleFileChange}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                  {formData.cover_image_url && (
                    <div className="mt-2">
                      <img 
                        src={formData.cover_image_url} 
                        alt="Cover preview" 
                        className="h-20 w-32 object-cover rounded border"
                      />
                    </div>
                  )}
                </div>
              </div>
              
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Description
                </label>
                <textarea
                  name="description"
                  value={formData.description}
                  onChange={handleInputChange}
                  rows={3}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  placeholder="Enter project description"
                ></textarea>
              </div>
              
              <div className="flex justify-end space-x-3 pt-4 border-t border-gray-200">
                <button
                  type="button"
                  onClick={() => {
                    setShowAddModal(false)
                    setShowEditModal(false)
                    setEditingProject(null)
                  }}
                  className="px-4 py-2 border border-gray-300 rounded-lg hover:bg-gray-50 transition"
                  disabled={isSubmitting}
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  disabled={isSubmitting}
                  className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition disabled:opacity-50"
                >
                  {isSubmitting ? (editingProject ? 'Updating...' : 'Adding...') : (editingProject ? 'Update Project' : 'Add Project')}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  )
}