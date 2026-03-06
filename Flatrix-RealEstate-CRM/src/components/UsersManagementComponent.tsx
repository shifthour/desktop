'use client'

import { useState, useEffect } from 'react'
import {
  Users,
  Plus,
  Edit2,
  Trash2,
  Search,
  X,
  Phone,
  Mail,
  UserCheck,
  UserX,
  RefreshCw,
  Shield,
  User
} from 'lucide-react'
import { supabase } from '@/lib/supabase'
import toast from 'react-hot-toast'
import bcrypt from 'bcryptjs'

interface CRMUser {
  id: string
  email: string
  name: string
  phone: string | null
  role: 'super_admin' | 'ADMIN' | 'SALES_MANAGER' | 'AGENT' | 'CHANNEL_PARTNER'
  is_active: boolean
  created_at: string
  updated_at: string
}

const roleOptions = [
  { value: 'ADMIN', label: 'Admin' },
  { value: 'SALES_MANAGER', label: 'Sales Manager' },
  { value: 'AGENT', label: 'Agent' },
]

const roleColors = {
  super_admin: 'bg-purple-100 text-purple-800 border-purple-200',
  ADMIN: 'bg-blue-100 text-blue-800 border-blue-200',
  SALES_MANAGER: 'bg-green-100 text-green-800 border-green-200',
  AGENT: 'bg-gray-100 text-gray-800 border-gray-200',
  CHANNEL_PARTNER: 'bg-yellow-100 text-yellow-800 border-yellow-200'
}

const roleIcons = {
  super_admin: Shield,
  ADMIN: UserCheck,
  SALES_MANAGER: Users,
  AGENT: User,
  CHANNEL_PARTNER: User
}

export default function UsersManagementComponent() {
  const [users, setUsers] = useState<CRMUser[]>([])
  const [loading, setLoading] = useState(true)
  const [searchTerm, setSearchTerm] = useState('')
  const [showAddModal, setShowAddModal] = useState(false)
  const [showEditModal, setShowEditModal] = useState(false)
  const [selectedUser, setSelectedUser] = useState<CRMUser | null>(null)

  const [formData, setFormData] = useState({
    email: '',
    name: '',
    phone: '',
    password: '',
    role: 'AGENT' as const,
    is_active: true
  })

  useEffect(() => {
    fetchUsers()
  }, [])

  const fetchUsers = async () => {
    try {
      setLoading(true)
      const { data, error } = await supabase
        .from('flatrix_users')
        .select('*')
        .order('created_at', { ascending: false })

      if (error) throw error
      setUsers(data || [])
    } catch (error: any) {
      console.error('Error fetching users:', error)
      toast.error('Failed to load users')
    } finally {
      setLoading(false)
    }
  }

  const handleAddUser = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!formData.email || !formData.name || !formData.password) {
      toast.error('Please fill in all required fields')
      return
    }

    try {
      // Hash password
      const hashedPassword = await bcrypt.hash(formData.password, 10)

      const { data, error } = await supabase
        .from('flatrix_users')
        .insert([{
          email: formData.email,
          name: formData.name,
          phone: formData.phone || null,
          password: hashedPassword,
          role: formData.role,
          is_active: formData.is_active,
          created_at: new Date().toISOString(),
          updated_at: new Date().toISOString()
        }])
        .select()
        .single()

      if (error) throw error

      toast.success('User added successfully!')
      setShowAddModal(false)
      setFormData({
        email: '',
        name: '',
        phone: '',
        password: '',
        role: 'AGENT',
        is_active: true
      })
      fetchUsers()
    } catch (error: any) {
      console.error('Error adding user:', error)
      if (error.code === '23505') {
        toast.error('Email already exists')
      } else {
        toast.error('Failed to add user')
      }
    }
  }

  const handleEditUser = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!selectedUser) return

    try {
      const updateData: any = {
        name: formData.name,
        phone: formData.phone || null,
        role: formData.role,
        is_active: formData.is_active,
        updated_at: new Date().toISOString()
      }

      // Only update password if a new one is provided
      if (formData.password) {
        updateData.password = await bcrypt.hash(formData.password, 10)
      }

      const { error } = await supabase
        .from('flatrix_users')
        .update(updateData)
        .eq('id', selectedUser.id)

      if (error) throw error

      toast.success('User updated successfully!')
      setShowEditModal(false)
      setSelectedUser(null)
      fetchUsers()
    } catch (error: any) {
      console.error('Error updating user:', error)
      toast.error('Failed to update user')
    }
  }

  const handleDeleteUser = async (userId: string) => {
    if (!confirm('Are you sure you want to delete this user? This action cannot be undone.')) {
      return
    }

    try {
      const { error } = await supabase
        .from('flatrix_users')
        .delete()
        .eq('id', userId)

      if (error) throw error

      toast.success('User deleted successfully!')
      fetchUsers()
    } catch (error: any) {
      console.error('Error deleting user:', error)
      toast.error('Failed to delete user')
    }
  }

  const handleToggleActive = async (user: CRMUser) => {
    try {
      const { error } = await supabase
        .from('flatrix_users')
        .update({
          is_active: !user.is_active,
          updated_at: new Date().toISOString()
        })
        .eq('id', user.id)

      if (error) throw error

      toast.success(`User ${!user.is_active ? 'activated' : 'deactivated'} successfully!`)
      fetchUsers()
    } catch (error: any) {
      console.error('Error toggling user status:', error)
      toast.error('Failed to update user status')
    }
  }

  const openEditModal = (user: CRMUser) => {
    setSelectedUser(user)
    setFormData({
      email: user.email,
      name: user.name,
      phone: user.phone || '',
      password: '', // Don't pre-fill password
      role: user.role as any,
      is_active: user.is_active
    })
    setShowEditModal(true)
  }

  const filteredUsers = users.filter(user => {
    const searchLower = searchTerm.toLowerCase()
    return (
      user.name.toLowerCase().includes(searchLower) ||
      user.email.toLowerCase().includes(searchLower) ||
      user.phone?.includes(searchTerm) ||
      user.role.toLowerCase().includes(searchLower)
    )
  })

  const activeUsers = users.filter(u => u.is_active).length
  const inactiveUsers = users.filter(u => !u.is_active).length
  const adminUsers = users.filter(u => u.role === 'ADMIN' || u.role === 'super_admin').length

  if (loading) {
    return (
      <div className="flex items-center justify-center min-h-screen">
        <RefreshCw className="animate-spin text-blue-600" size={40} />
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Users Management</h1>
          <p className="text-sm text-gray-600 mt-1">Manage CRM users and their roles</p>
        </div>
        <button
          onClick={() => {
            setFormData({
              email: '',
              name: '',
              phone: '',
              password: '',
              role: 'AGENT',
              is_active: true
            })
            setShowAddModal(true)
          }}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          <Plus size={20} />
          Add User
        </button>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Total Users</p>
              <p className="text-3xl font-bold text-gray-900 mt-2">{users.length}</p>
            </div>
            <div className="p-3 rounded-full bg-blue-100">
              <Users size={24} className="text-blue-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Active Users</p>
              <p className="text-3xl font-bold text-green-600 mt-2">{activeUsers}</p>
            </div>
            <div className="p-3 rounded-full bg-green-100">
              <UserCheck size={24} className="text-green-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Inactive Users</p>
              <p className="text-3xl font-bold text-red-600 mt-2">{inactiveUsers}</p>
            </div>
            <div className="p-3 rounded-full bg-red-100">
              <UserX size={24} className="text-red-600" />
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg border border-gray-200 p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-sm font-medium text-gray-600">Admins</p>
              <p className="text-3xl font-bold text-purple-600 mt-2">{adminUsers}</p>
            </div>
            <div className="p-3 rounded-full bg-purple-100">
              <Shield size={24} className="text-purple-600" />
            </div>
          </div>
        </div>
      </div>

      {/* Search */}
      <div className="bg-white rounded-lg border border-gray-200 p-4">
        <div className="relative">
          <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" size={20} />
          <input
            type="text"
            placeholder="Search users by name, email, phone, or role..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
        </div>
      </div>

      {/* Users Table */}
      <div className="bg-white rounded-lg border border-gray-200 overflow-hidden">
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  User
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Contact
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Role
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Status
                </th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Created
                </th>
                <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                  Actions
                </th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {filteredUsers.length === 0 ? (
                <tr>
                  <td colSpan={6} className="px-6 py-12 text-center text-gray-500">
                    No users found
                  </td>
                </tr>
              ) : (
                filteredUsers.map((user) => {
                  const RoleIcon = roleIcons[user.role] || User
                  return (
                    <tr key={user.id} className="hover:bg-gray-50">
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center">
                          <div className="flex-shrink-0 h-10 w-10 rounded-full bg-blue-100 flex items-center justify-center">
                            <span className="text-blue-600 font-medium text-sm">
                              {user.name.charAt(0).toUpperCase()}
                            </span>
                          </div>
                          <div className="ml-4">
                            <div className="text-sm font-medium text-gray-900">{user.name}</div>
                          </div>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-sm text-gray-900 flex items-center gap-2">
                          <Mail size={14} className="text-gray-400" />
                          {user.email}
                        </div>
                        {user.phone && (
                          <div className="text-sm text-gray-500 flex items-center gap-2 mt-1">
                            <Phone size={14} className="text-gray-400" />
                            {user.phone}
                          </div>
                        )}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`inline-flex items-center gap-1 px-3 py-1 rounded-full text-xs font-medium border ${roleColors[user.role]}`}>
                          <RoleIcon size={14} />
                          {user.role.replace('_', ' ')}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <button
                          onClick={() => handleToggleActive(user)}
                          className={`inline-flex items-center gap-1 px-3 py-1 rounded-full text-xs font-medium border ${
                            user.is_active
                              ? 'bg-green-100 text-green-800 border-green-200'
                              : 'bg-gray-100 text-gray-800 border-gray-200'
                          }`}
                        >
                          {user.is_active ? <UserCheck size={14} /> : <UserX size={14} />}
                          {user.is_active ? 'Active' : 'Inactive'}
                        </button>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                        {new Date(user.created_at).toLocaleDateString()}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">
                        <div className="flex items-center justify-end gap-2">
                          <button
                            onClick={() => openEditModal(user)}
                            className="text-blue-600 hover:text-blue-900"
                          >
                            <Edit2 size={18} />
                          </button>
                          {user.role !== 'super_admin' && (
                            <button
                              onClick={() => handleDeleteUser(user.id)}
                              className="text-red-600 hover:text-red-900"
                            >
                              <Trash2 size={18} />
                            </button>
                          )}
                        </div>
                      </td>
                    </tr>
                  )
                })
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Add User Modal */}
      {showAddModal && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg max-w-md w-full p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-bold text-gray-900">Add New User</h2>
              <button onClick={() => setShowAddModal(false)} className="text-gray-400 hover:text-gray-600">
                <X size={24} />
              </button>
            </div>

            <form onSubmit={handleAddUser} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  required
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Email <span className="text-red-500">*</span>
                </label>
                <input
                  type="email"
                  required
                  value={formData.email}
                  onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Phone
                </label>
                <input
                  type="tel"
                  value={formData.phone}
                  onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Password <span className="text-red-500">*</span>
                </label>
                <input
                  type="password"
                  required
                  value={formData.password}
                  onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="Minimum 6 characters"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Role <span className="text-red-500">*</span>
                </label>
                <select
                  required
                  value={formData.role}
                  onChange={(e) => setFormData({ ...formData, role: e.target.value as any })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                >
                  {roleOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </div>

              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="is_active"
                  checked={formData.is_active}
                  onChange={(e) => setFormData({ ...formData, is_active: e.target.checked })}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
                <label htmlFor="is_active" className="ml-2 block text-sm text-gray-700">
                  Active user
                </label>
              </div>

              <div className="flex gap-3 pt-4">
                <button
                  type="button"
                  onClick={() => setShowAddModal(false)}
                  className="flex-1 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                >
                  Add User
                </button>
              </div>
            </form>
          </div>
        </div>
      )}

      {/* Edit User Modal */}
      {showEditModal && selectedUser && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-lg max-w-md w-full p-6">
            <div className="flex items-center justify-between mb-4">
              <h2 className="text-xl font-bold text-gray-900">Edit User</h2>
              <button onClick={() => setShowEditModal(false)} className="text-gray-400 hover:text-gray-600">
                <X size={24} />
              </button>
            </div>

            <form onSubmit={handleEditUser} className="space-y-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Name <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  required
                  value={formData.name}
                  onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Email (cannot be changed)
                </label>
                <input
                  type="email"
                  disabled
                  value={formData.email}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg bg-gray-100 text-gray-600"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  Phone
                </label>
                <input
                  type="tel"
                  value={formData.phone}
                  onChange={(e) => setFormData({ ...formData, phone: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  New Password (leave blank to keep current)
                </label>
                <input
                  type="password"
                  value={formData.password}
                  onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                  className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  placeholder="Minimum 6 characters"
                />
              </div>

              {selectedUser.role !== 'super_admin' && (
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">
                    Role <span className="text-red-500">*</span>
                  </label>
                  <select
                    required
                    value={formData.role}
                    onChange={(e) => setFormData({ ...formData, role: e.target.value as any })}
                    className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent"
                  >
                    {roleOptions.map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </div>
              )}

              <div className="flex items-center">
                <input
                  type="checkbox"
                  id="edit_is_active"
                  checked={formData.is_active}
                  onChange={(e) => setFormData({ ...formData, is_active: e.target.checked })}
                  className="h-4 w-4 text-blue-600 focus:ring-blue-500 border-gray-300 rounded"
                />
                <label htmlFor="edit_is_active" className="ml-2 block text-sm text-gray-700">
                  Active user
                </label>
              </div>

              <div className="flex gap-3 pt-4">
                <button
                  type="button"
                  onClick={() => setShowEditModal(false)}
                  className="flex-1 px-4 py-2 border border-gray-300 text-gray-700 rounded-lg hover:bg-gray-50"
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                >
                  Save Changes
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  )
}
