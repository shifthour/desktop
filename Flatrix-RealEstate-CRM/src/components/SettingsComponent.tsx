'use client'

import { useState, useEffect, useCallback } from 'react'
import { supabase } from '@/lib/supabase'
import toast from 'react-hot-toast'
import {
  MessageSquare,
  Settings,
  FileText,
  Play,
  Send,
  Plus,
  Pencil,
  Trash2,
  RefreshCw,
  Power,
  PowerOff,
  CheckCircle,
  XCircle,
  ChevronLeft,
  ChevronRight,
} from 'lucide-react'

type Tab = 'config' | 'templates' | 'logs' | 'test'

interface WhatsAppConfig {
  id: string
  is_enabled: boolean
  phone_number_id: string | null
  business_account_id: string | null
  api_version: string
  env_configured: boolean
  phone_number_id_set: boolean
  access_token_set: boolean
}

interface WhatsAppTemplate {
  id: string
  project_id: string | null
  project_name_pattern: string
  greeting_template_name: string
  greeting_body: string | null
  brochure_template_name: string | null
  brochure_url: string | null
  video_template_name: string | null
  video_url: string | null
  template_language: string
  is_active: boolean
  is_default: boolean
  created_at: string
  flatrix_projects: { name: string } | null
}

interface WhatsAppLog {
  id: string
  lead_name: string | null
  lead_phone: string
  project_name: string | null
  message_type: string
  wa_template_name: string | null
  status: string
  error_message: string | null
  created_at: string
  sent_at: string | null
}

interface Project {
  id: string
  name: string
}

const STATUS_COLORS: Record<string, string> = {
  pending: 'bg-yellow-100 text-yellow-800',
  sent: 'bg-blue-100 text-blue-800',
  delivered: 'bg-green-100 text-green-800',
  read: 'bg-emerald-100 text-emerald-800',
  failed: 'bg-red-100 text-red-800',
}

export default function SettingsComponent() {
  const [activeTab, setActiveTab] = useState<Tab>('config')

  return (
    <div>
      <div className="mb-8">
        <h1 className="text-3xl font-bold text-gray-900">Settings</h1>
        <p className="text-gray-600 mt-2">WhatsApp automation & application settings</p>
      </div>

      {/* Tab Navigation */}
      <div className="border-b border-gray-200 mb-6">
        <nav className="-mb-px flex space-x-8">
          {[
            { id: 'config' as Tab, label: 'Configuration', icon: Settings },
            { id: 'templates' as Tab, label: 'Templates', icon: FileText },
            { id: 'logs' as Tab, label: 'Message Logs', icon: MessageSquare },
            { id: 'test' as Tab, label: 'Test Send', icon: Send },
          ].map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center gap-2 py-4 px-1 border-b-2 text-sm font-medium transition-colors ${
                activeTab === tab.id
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              <tab.icon className="h-4 w-4" />
              {tab.label}
            </button>
          ))}
        </nav>
      </div>

      {activeTab === 'config' && <ConfigTab />}
      {activeTab === 'templates' && <TemplatesTab />}
      {activeTab === 'logs' && <LogsTab />}
      {activeTab === 'test' && <TestSendTab />}
    </div>
  )
}

// ============================================
// Configuration Tab
// ============================================
function ConfigTab() {
  const [config, setConfig] = useState<WhatsAppConfig | null>(null)
  const [loading, setLoading] = useState(true)
  const [toggling, setToggling] = useState(false)

  const fetchConfig = useCallback(async () => {
    try {
      const res = await fetch('/api/whatsapp/config')
      const data = await res.json()
      if (data.success) setConfig(data.config)
    } catch {
      toast.error('Failed to load config')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { fetchConfig() }, [fetchConfig])

  const toggleEnabled = async () => {
    if (!config) return
    setToggling(true)
    try {
      const res = await fetch('/api/whatsapp/config', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ is_enabled: !config.is_enabled }),
      })
      const data = await res.json()
      if (data.success) {
        setConfig({ ...config, is_enabled: !config.is_enabled })
        toast.success(`WhatsApp automation ${!config.is_enabled ? 'enabled' : 'disabled'}`)
      }
    } catch {
      toast.error('Failed to update config')
    } finally {
      setToggling(false)
    }
  }

  if (loading) return <div className="text-center py-8 text-gray-500">Loading configuration...</div>

  return (
    <div className="space-y-6">
      {/* Master Toggle */}
      <div className="bg-white rounded-lg shadow p-6">
        <div className="flex items-center justify-between">
          <div>
            <h3 className="text-lg font-semibold text-gray-900">WhatsApp Automation</h3>
            <p className="text-sm text-gray-500 mt-1">
              When enabled, new leads will automatically receive WhatsApp messages
            </p>
          </div>
          <button
            onClick={toggleEnabled}
            disabled={toggling}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg font-medium transition-colors ${
              config?.is_enabled
                ? 'bg-green-100 text-green-700 hover:bg-green-200'
                : 'bg-gray-100 text-gray-600 hover:bg-gray-200'
            }`}
          >
            {config?.is_enabled ? (
              <>
                <Power className="h-5 w-5" />
                Enabled
              </>
            ) : (
              <>
                <PowerOff className="h-5 w-5" />
                Disabled
              </>
            )}
          </button>
        </div>
      </div>

      {/* Connection Status */}
      <div className="bg-white rounded-lg shadow p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Connection Status</h3>
        <div className="space-y-3">
          <StatusRow label="WhatsApp Access Token" ok={config?.access_token_set || false} />
          <StatusRow label="Phone Number ID" ok={config?.phone_number_id_set || false} />
          <StatusRow label="API Configuration" ok={config?.env_configured || false} />
        </div>
      </div>

      {/* Setup Instructions */}
      <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-blue-900 mb-3">Setup Guide</h3>
        <ol className="list-decimal list-inside space-y-2 text-sm text-blue-800">
          <li>Create a Meta Developer App at developers.facebook.com</li>
          <li>Add the WhatsApp product to your app</li>
          <li>Register your business phone number</li>
          <li>Create message templates (greeting, brochure, video) in WhatsApp Manager</li>
          <li>Get a permanent System User access token</li>
          <li>Add environment variables: WHATSAPP_PHONE_NUMBER_ID, WHATSAPP_ACCESS_TOKEN</li>
          <li>Configure webhook URL: your-domain.com/api/whatsapp/webhook</li>
          <li>Add templates in the Templates tab with matching Meta template names</li>
          <li>Enable automation using the toggle above</li>
        </ol>
      </div>
    </div>
  )
}

function StatusRow({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div className="flex items-center justify-between py-2 border-b border-gray-100 last:border-0">
      <span className="text-sm text-gray-700">{label}</span>
      {ok ? (
        <span className="flex items-center gap-1 text-sm text-green-600">
          <CheckCircle className="h-4 w-4" /> Configured
        </span>
      ) : (
        <span className="flex items-center gap-1 text-sm text-red-500">
          <XCircle className="h-4 w-4" /> Not Set
        </span>
      )}
    </div>
  )
}

// ============================================
// Templates Tab
// ============================================
function TemplatesTab() {
  const [templates, setTemplates] = useState<WhatsAppTemplate[]>([])
  const [projects, setProjects] = useState<Project[]>([])
  const [loading, setLoading] = useState(true)
  const [showForm, setShowForm] = useState(false)
  const [editingId, setEditingId] = useState<string | null>(null)
  const [formData, setFormData] = useState({
    project_id: '',
    project_name_pattern: '',
    greeting_template_name: '',
    greeting_body: '',
    brochure_template_name: '',
    brochure_url: '',
    video_template_name: '',
    video_url: '',
    template_language: 'en',
    is_default: false,
  })

  const fetchTemplates = useCallback(async () => {
    try {
      const res = await fetch('/api/whatsapp/templates')
      const data = await res.json()
      if (data.success) setTemplates(data.templates)
    } catch {
      toast.error('Failed to load templates')
    } finally {
      setLoading(false)
    }
  }, [])

  const fetchProjects = useCallback(async () => {
    const { data } = await supabase
      .from('flatrix_projects')
      .select('id, name')
      .eq('is_active', true)
      .order('name')
    if (data) setProjects(data)
  }, [])

  useEffect(() => {
    fetchTemplates()
    fetchProjects()
  }, [fetchTemplates, fetchProjects])

  const resetForm = () => {
    setFormData({
      project_id: '',
      project_name_pattern: '',
      greeting_template_name: '',
      greeting_body: '',
      brochure_template_name: '',
      brochure_url: '',
      video_template_name: '',
      video_url: '',
      template_language: 'en',
      is_default: false,
    })
    setEditingId(null)
    setShowForm(false)
  }

  const handleEdit = (t: WhatsAppTemplate) => {
    setFormData({
      project_id: t.project_id || '',
      project_name_pattern: t.project_name_pattern,
      greeting_template_name: t.greeting_template_name,
      greeting_body: t.greeting_body || '',
      brochure_template_name: t.brochure_template_name || '',
      brochure_url: t.brochure_url || '',
      video_template_name: t.video_template_name || '',
      video_url: t.video_url || '',
      template_language: t.template_language,
      is_default: t.is_default,
    })
    setEditingId(t.id)
    setShowForm(true)
  }

  const handleSave = async () => {
    if (!formData.project_name_pattern || !formData.greeting_template_name) {
      toast.error('Project name pattern and greeting template name are required')
      return
    }

    try {
      const method = editingId ? 'PUT' : 'POST'
      const body = editingId ? { id: editingId, ...formData } : formData

      const res = await fetch('/api/whatsapp/templates', {
        method,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })

      const data = await res.json()
      if (data.success) {
        toast.success(editingId ? 'Template updated' : 'Template created')
        resetForm()
        fetchTemplates()
      } else {
        toast.error(data.error || 'Failed to save')
      }
    } catch {
      toast.error('Failed to save template')
    }
  }

  const handleDelete = async (id: string) => {
    if (!confirm('Are you sure you want to delete this template?')) return

    try {
      const res = await fetch(`/api/whatsapp/templates?id=${id}`, { method: 'DELETE' })
      const data = await res.json()
      if (data.success) {
        toast.success('Template deleted')
        fetchTemplates()
      }
    } catch {
      toast.error('Failed to delete')
    }
  }

  const handleProjectChange = (projectId: string) => {
    const project = projects.find(p => p.id === projectId)
    setFormData(prev => ({
      ...prev,
      project_id: projectId,
      project_name_pattern: project?.name || prev.project_name_pattern,
    }))
  }

  if (loading) return <div className="text-center py-8 text-gray-500">Loading templates...</div>

  return (
    <div className="space-y-6">
      {/* Add button */}
      <div className="flex justify-end">
        <button
          onClick={() => { resetForm(); setShowForm(true) }}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          <Plus className="h-4 w-4" />
          Add Template
        </button>
      </div>

      {/* Template Form Modal */}
      {showForm && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
          <div className="bg-white rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto p-6 mx-4">
            <h3 className="text-lg font-semibold mb-4">
              {editingId ? 'Edit Template' : 'Add New Template'}
            </h3>

            <div className="space-y-4">
              {/* Project Selection */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">Project</label>
                <select
                  value={formData.project_id}
                  onChange={(e) => handleProjectChange(e.target.value)}
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                >
                  <option value="">-- Select Project --</option>
                  {projects.map(p => (
                    <option key={p.id} value={p.id}>{p.name}</option>
                  ))}
                </select>
              </div>

              {/* Project Name Pattern */}
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Project Name Pattern *
                </label>
                <input
                  type="text"
                  value={formData.project_name_pattern}
                  onChange={(e) => setFormData(prev => ({ ...prev, project_name_pattern: e.target.value }))}
                  placeholder="e.g., Anahata (used for matching lead's project name)"
                  className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                />
              </div>

              <hr className="my-2" />

              {/* Greeting Template */}
              <div className="bg-green-50 p-4 rounded-lg">
                <h4 className="text-sm font-semibold text-green-800 mb-3">Message 1: Greeting</h4>
                <div className="space-y-3">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Meta Template Name *
                    </label>
                    <input
                      type="text"
                      value={formData.greeting_template_name}
                      onChange={(e) => setFormData(prev => ({ ...prev, greeting_template_name: e.target.value }))}
                      placeholder="e.g., anahata_welcome"
                      className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">
                      Message Body (preview only)
                    </label>
                    <textarea
                      value={formData.greeting_body}
                      onChange={(e) => setFormData(prev => ({ ...prev, greeting_body: e.target.value }))}
                      placeholder={'Hello {{1}}, thank you for your interest...'}
                      rows={3}
                      className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                    <p className="text-xs text-gray-500 mt-1">For reference only. Actual template is managed in Meta WhatsApp Manager.</p>
                  </div>
                </div>
              </div>

              {/* Brochure Template */}
              <div className="bg-blue-50 p-4 rounded-lg">
                <h4 className="text-sm font-semibold text-blue-800 mb-3">Message 2: Brochure PDF</h4>
                <div className="space-y-3">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Meta Template Name</label>
                    <input
                      type="text"
                      value={formData.brochure_template_name}
                      onChange={(e) => setFormData(prev => ({ ...prev, brochure_template_name: e.target.value }))}
                      placeholder="e.g., anahata_brochure"
                      className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Brochure PDF URL</label>
                    <input
                      type="url"
                      value={formData.brochure_url}
                      onChange={(e) => setFormData(prev => ({ ...prev, brochure_url: e.target.value }))}
                      placeholder="https://example.com/brochure.pdf"
                      className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                  </div>
                </div>
              </div>

              {/* Video Template */}
              <div className="bg-purple-50 p-4 rounded-lg">
                <h4 className="text-sm font-semibold text-purple-800 mb-3">Message 3: Video</h4>
                <div className="space-y-3">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Meta Template Name</label>
                    <input
                      type="text"
                      value={formData.video_template_name}
                      onChange={(e) => setFormData(prev => ({ ...prev, video_template_name: e.target.value }))}
                      placeholder="e.g., anahata_video"
                      className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Video URL</label>
                    <input
                      type="url"
                      value={formData.video_url}
                      onChange={(e) => setFormData(prev => ({ ...prev, video_url: e.target.value }))}
                      placeholder="https://example.com/video.mp4"
                      className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                    />
                  </div>
                </div>
              </div>

              {/* Language & Default */}
              <div className="flex gap-4">
                <div className="flex-1">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Language</label>
                  <select
                    value={formData.template_language}
                    onChange={(e) => setFormData(prev => ({ ...prev, template_language: e.target.value }))}
                    className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
                  >
                    <option value="en">English</option>
                    <option value="hi">Hindi</option>
                  </select>
                </div>
                <div className="flex items-end pb-2">
                  <label className="flex items-center gap-2 cursor-pointer">
                    <input
                      type="checkbox"
                      checked={formData.is_default}
                      onChange={(e) => setFormData(prev => ({ ...prev, is_default: e.target.checked }))}
                      className="h-4 w-4 text-blue-600 rounded border-gray-300"
                    />
                    <span className="text-sm font-medium text-gray-700">Default template</span>
                  </label>
                </div>
              </div>
            </div>

            {/* Modal Actions */}
            <div className="flex justify-end gap-3 mt-6 pt-4 border-t">
              <button
                onClick={resetForm}
                className="px-4 py-2 text-gray-700 bg-gray-100 rounded-lg hover:bg-gray-200 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleSave}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                {editingId ? 'Update' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Templates Table */}
      {templates.length === 0 ? (
        <div className="bg-white rounded-lg shadow p-8 text-center">
          <FileText className="h-12 w-12 text-gray-300 mx-auto mb-3" />
          <p className="text-gray-500">No templates configured yet</p>
          <p className="text-sm text-gray-400 mt-1">Add a template to start automating WhatsApp messages</p>
        </div>
      ) : (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Project</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Greeting</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Brochure</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Video</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Default</th>
                <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Actions</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {templates.map((t) => (
                <tr key={t.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 text-sm font-medium text-gray-900">
                    {t.flatrix_projects?.name || t.project_name_pattern}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">{t.greeting_template_name}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    {t.brochure_template_name ? (
                      <span className="flex items-center gap-1">
                        <FileText className="h-3 w-3" />
                        {t.brochure_template_name}
                      </span>
                    ) : (
                      <span className="text-gray-400">--</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm text-gray-600">
                    {t.video_template_name ? (
                      <span className="flex items-center gap-1">
                        <Play className="h-3 w-3" />
                        {t.video_template_name}
                      </span>
                    ) : (
                      <span className="text-gray-400">--</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-sm">
                    {t.is_default && (
                      <span className="px-2 py-1 bg-blue-100 text-blue-700 rounded text-xs font-medium">Default</span>
                    )}
                  </td>
                  <td className="px-4 py-3 text-right">
                    <div className="flex justify-end gap-2">
                      <button
                        onClick={() => handleEdit(t)}
                        className="p-1.5 text-gray-400 hover:text-blue-600 transition-colors"
                        title="Edit"
                      >
                        <Pencil className="h-4 w-4" />
                      </button>
                      <button
                        onClick={() => handleDelete(t.id)}
                        className="p-1.5 text-gray-400 hover:text-red-600 transition-colors"
                        title="Delete"
                      >
                        <Trash2 className="h-4 w-4" />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

// ============================================
// Logs Tab
// ============================================
function LogsTab() {
  const [logs, setLogs] = useState<WhatsAppLog[]>([])
  const [loading, setLoading] = useState(true)
  const [statusFilter, setStatusFilter] = useState('')
  const [page, setPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)
  const [summary, setSummary] = useState({ total: 0, sent: 0, delivered: 0, read: 0, failed: 0 })

  const fetchLogs = useCallback(async () => {
    setLoading(true)
    try {
      const params = new URLSearchParams({ page: page.toString(), limit: '30' })
      if (statusFilter) params.set('status', statusFilter)

      const res = await fetch(`/api/whatsapp/logs?${params}`)
      const data = await res.json()
      if (data.success) {
        setLogs(data.logs)
        setTotalPages(data.pagination.totalPages)
        setSummary(data.summary)
      }
    } catch {
      toast.error('Failed to load logs')
    } finally {
      setLoading(false)
    }
  }, [page, statusFilter])

  useEffect(() => { fetchLogs() }, [fetchLogs])

  return (
    <div className="space-y-6">
      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
        {[
          { label: 'Total (This Month)', value: summary.total, color: 'bg-gray-100 text-gray-800' },
          { label: 'Sent', value: summary.sent, color: 'bg-blue-100 text-blue-800' },
          { label: 'Delivered', value: summary.delivered, color: 'bg-green-100 text-green-800' },
          { label: 'Read', value: summary.read, color: 'bg-emerald-100 text-emerald-800' },
          { label: 'Failed', value: summary.failed, color: 'bg-red-100 text-red-800' },
        ].map((s) => (
          <div key={s.label} className={`${s.color} rounded-lg p-4 text-center`}>
            <p className="text-2xl font-bold">{s.value}</p>
            <p className="text-xs mt-1">{s.label}</p>
          </div>
        ))}
      </div>

      {/* Filters */}
      <div className="flex items-center gap-4">
        <select
          value={statusFilter}
          onChange={(e) => { setStatusFilter(e.target.value); setPage(1) }}
          className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500"
        >
          <option value="">All Statuses</option>
          <option value="pending">Pending</option>
          <option value="sent">Sent</option>
          <option value="delivered">Delivered</option>
          <option value="read">Read</option>
          <option value="failed">Failed</option>
        </select>
        <button
          onClick={fetchLogs}
          className="flex items-center gap-1 px-3 py-2 text-sm text-gray-600 hover:text-gray-900 transition-colors"
        >
          <RefreshCw className="h-4 w-4" />
          Refresh
        </button>
      </div>

      {/* Logs Table */}
      {loading ? (
        <div className="text-center py-8 text-gray-500">Loading logs...</div>
      ) : logs.length === 0 ? (
        <div className="bg-white rounded-lg shadow p-8 text-center">
          <MessageSquare className="h-12 w-12 text-gray-300 mx-auto mb-3" />
          <p className="text-gray-500">No messages sent yet</p>
        </div>
      ) : (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Lead</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Phone</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Project</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Type</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Error</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-200">
              {logs.map((log) => (
                <tr key={log.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 text-sm text-gray-600">
                    {new Date(log.created_at).toLocaleString('en-IN', {
                      day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit',
                    })}
                  </td>
                  <td className="px-4 py-3 text-sm font-medium text-gray-900">{log.lead_name || '--'}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">{log.lead_phone}</td>
                  <td className="px-4 py-3 text-sm text-gray-600">{log.project_name || '--'}</td>
                  <td className="px-4 py-3 text-sm capitalize">{log.message_type}</td>
                  <td className="px-4 py-3 text-sm">
                    <span className={`px-2 py-1 rounded text-xs font-medium ${STATUS_COLORS[log.status] || 'bg-gray-100'}`}>
                      {log.status}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-sm text-red-600 max-w-[200px] truncate" title={log.error_message || ''}>
                    {log.error_message || ''}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between px-4 py-3 border-t border-gray-200">
              <p className="text-sm text-gray-500">Page {page} of {totalPages}</p>
              <div className="flex gap-2">
                <button
                  onClick={() => setPage(p => Math.max(1, p - 1))}
                  disabled={page === 1}
                  className="p-2 rounded hover:bg-gray-100 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <ChevronLeft className="h-4 w-4" />
                </button>
                <button
                  onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                  disabled={page === totalPages}
                  className="p-2 rounded hover:bg-gray-100 disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  <ChevronRight className="h-4 w-4" />
                </button>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

// ============================================
// Test Send Tab
// ============================================
function TestSendTab() {
  const [phone, setPhone] = useState('')
  const [name, setName] = useState('')
  const [projectName, setProjectName] = useState('')
  const [projects, setProjects] = useState<Project[]>([])
  const [sending, setSending] = useState(false)
  const [result, setResult] = useState<{ success: boolean; message: string } | null>(null)

  useEffect(() => {
    supabase
      .from('flatrix_projects')
      .select('id, name')
      .eq('is_active', true)
      .order('name')
      .then(({ data }) => { if (data) setProjects(data) })
  }, [])

  const handleSend = async () => {
    if (!phone) {
      toast.error('Phone number is required')
      return
    }

    setSending(true)
    setResult(null)

    try {
      const res = await fetch('/api/whatsapp/test-send', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phone, name, project_name: projectName || null }),
      })

      const data = await res.json()
      setResult({ success: data.success, message: data.message || data.error })

      if (data.success) {
        toast.success('Test messages sent! Check Logs tab for status.')
      } else {
        toast.error(data.error || 'Failed to send')
      }
    } catch {
      toast.error('Network error')
    } finally {
      setSending(false)
    }
  }

  return (
    <div className="max-w-lg">
      <div className="bg-white rounded-lg shadow p-6 space-y-4">
        <h3 className="text-lg font-semibold text-gray-900">Send Test Message</h3>
        <p className="text-sm text-gray-500">
          Send a test WhatsApp message to verify your setup. All 3 messages (greeting, brochure, video) will be sent.
        </p>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Phone Number *</label>
          <input
            type="tel"
            value={phone}
            onChange={(e) => setPhone(e.target.value)}
            placeholder="e.g., 9876543210 or +919876543210"
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Name</label>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            placeholder="Test User"
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          />
        </div>

        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Project</label>
          <select
            value={projectName}
            onChange={(e) => setProjectName(e.target.value)}
            className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500"
          >
            <option value="">-- Default Template --</option>
            {projects.map(p => (
              <option key={p.id} value={p.name}>{p.name}</option>
            ))}
          </select>
        </div>

        <button
          onClick={handleSend}
          disabled={sending || !phone}
          className="w-full flex items-center justify-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
        >
          {sending ? (
            <RefreshCw className="h-4 w-4 animate-spin" />
          ) : (
            <Send className="h-4 w-4" />
          )}
          {sending ? 'Sending...' : 'Send Test Messages'}
        </button>

        {result && (
          <div className={`p-4 rounded-lg text-sm ${
            result.success ? 'bg-green-50 text-green-800' : 'bg-red-50 text-red-800'
          }`}>
            {result.message}
          </div>
        )}
      </div>
    </div>
  )
}
