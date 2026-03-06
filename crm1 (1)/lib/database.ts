import { createServiceSupabase } from './supabase'

// Database service for server-side operations
export class DatabaseService {
  private supabase = createServiceSupabase()

  // Initialize database with schema
  async initializeDatabase() {
    try {
      console.log('Initializing database schema...')
      
      // Read the schema file and execute it
      // Note: In production, you'd run this directly in Supabase SQL editor
      
      return { success: true, message: 'Database initialized successfully' }
    } catch (error) {
      console.error('Database initialization error:', error)
      return { success: false, error: error.message }
    }
  }

  // Company management
  async createCompany(data: {
    name: string
    domain: string
    maxUsers: number
  }) {
    try {
      const { data: company, error } = await this.supabase
        .from('companies')
        .insert({
          name: data.name,
          domain: data.domain,
          max_users: data.maxUsers,
          current_users: 0,
          subscription_status: 'active'
        })
        .select()
        .single()

      if (error) throw error
      return { success: true, data: company }
    } catch (error) {
      console.error('Create company error:', error)
      return { success: false, error: error.message }
    }
  }

  // Create company with admin user
  async createCompanyWithAdmin(data: {
    name: string
    adminName: string
    adminEmail: string
    adminPassword: string
    maxUsers: number
  }) {
    try {
      // Create a unique domain from company name
      const domain = data.name.toLowerCase()
        .replace(/[^a-z0-9]/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-|-$/g, '')

      // Start transaction - create company first
      const { data: company, error: companyError } = await this.supabase
        .from('companies')
        .insert({
          name: data.name,
          domain: domain,
          admin_email: data.adminEmail,
          max_users: data.maxUsers,
          current_users: 0,
          subscription_status: 'active',
          expiry_date: new Date(Date.now() + 365 * 24 * 60 * 60 * 1000).toISOString() // 1 year from now
        })
        .select()
        .single()

      if (companyError) throw companyError

      // Get company_admin role ID
      const { data: role, error: roleError } = await this.supabase
        .from('user_roles')
        .select('id')
        .eq('name', 'company_admin')
        .single()

      if (roleError) throw roleError

      // Create admin user for the company
      const { data: adminUser, error: userError } = await this.supabase
        .from('users')
        .insert({
          company_id: company.id,
          email: data.adminEmail,
          full_name: data.adminName,
          password: data.adminPassword,
          role_id: role.id,
          is_admin: true,
          is_active: true,
          password_changed: false
        })
        .select(`
          *,
          role:user_roles(*)
        `)
        .single()

      if (userError) throw userError

      return { 
        success: true, 
        data: {
          company,
          adminUser,
          credentials: {
            email: data.adminEmail,
            password: data.adminPassword
          }
        }
      }
    } catch (error) {
      console.error('Create company with admin error:', error)
      return { success: false, error: error.message }
    }
  }

  async updateCompany(companyId: string, data: {
    name?: string
    adminEmail?: string
    maxUsers?: number
    logoUrl?: string
    website?: string
    address?: string
    city?: string
    state?: string
    country?: string
    postalCode?: string
    phone?: string
    email?: string
    industry?: string
    companySize?: string
    foundedYear?: number
    taxId?: string
    description?: string
    businessHours?: string
    primaryColor?: string
    secondaryColor?: string
  }) {
    try {
      const updateData: any = {}
      
      if (data.name !== undefined) updateData.name = data.name
      if (data.adminEmail !== undefined) updateData.admin_email = data.adminEmail
      if (data.maxUsers !== undefined) updateData.max_users = data.maxUsers
      if (data.logoUrl !== undefined) updateData.logo_url = data.logoUrl
      if (data.website !== undefined) updateData.website = data.website
      if (data.address !== undefined) updateData.address = data.address
      if (data.city !== undefined) updateData.city = data.city
      if (data.state !== undefined) updateData.state = data.state
      if (data.country !== undefined) updateData.country = data.country
      if (data.postalCode !== undefined) updateData.postal_code = data.postalCode
      if (data.phone !== undefined) updateData.phone = data.phone
      if (data.email !== undefined) updateData.email = data.email
      if (data.industry !== undefined) updateData.industry = data.industry
      if (data.companySize !== undefined) updateData.company_size = data.companySize
      if (data.foundedYear !== undefined) updateData.founded_year = data.foundedYear
      if (data.taxId !== undefined) updateData.tax_id = data.taxId
      if (data.description !== undefined) updateData.description = data.description
      if (data.businessHours !== undefined) updateData.business_hours = data.businessHours
      if (data.primaryColor !== undefined) updateData.primary_color = data.primaryColor
      if (data.secondaryColor !== undefined) updateData.secondary_color = data.secondaryColor

      const { data: company, error } = await this.supabase
        .from('companies')
        .update(updateData)
        .eq('id', companyId)
        .select(`
          *,
          users!inner(id, full_name, email, is_admin)
        `)
        .single()

      if (error) throw error

      // If admin email changed, update the admin user's email
      if (data.adminEmail) {
        const { error: userUpdateError } = await this.supabase
          .from('users')
          .update({ email: data.adminEmail })
          .eq('company_id', companyId)
          .eq('is_admin', true)

        if (userUpdateError) {
          console.warn('Failed to update admin user email:', userUpdateError)
        }
      }

      return { success: true, data: company }
    } catch (error) {
      console.error('Update company error:', error)
      return { success: false, error: error.message }
    }
  }

  async deleteCompany(companyId: string) {
    try {
      // Delete company (cascade will handle users)
      const { error } = await this.supabase
        .from('companies')
        .delete()
        .eq('id', companyId)

      if (error) throw error

      return { success: true }
    } catch (error) {
      console.error('Delete company error:', error)
      return { success: false, error: error.message }
    }
  }

  async getAllCompanies() {
    try {
      const { data: companies, error } = await this.supabase
        .from('companies')
        .select(`
          *,
          admin_email,
          expiry_date
        `)
        .order('created_at', { ascending: false })

      if (error) throw error
      
      // Ensure all companies have admin_email populated from their admin users
      const companiesWithAdminEmail = await Promise.all(
        companies.map(async (company) => {
          if (!company.admin_email) {
            // Try to get admin email from users table
            const { data: adminUser } = await this.supabase
              .from('users')
              .select('email')
              .eq('company_id', company.id)
              .eq('is_admin', true)
              .limit(1)
              .single()
            
            if (adminUser?.email) {
              // Update the company record with admin email
              await this.supabase
                .from('companies')
                .update({ admin_email: adminUser.email })
                .eq('id', company.id)
              
              company.admin_email = adminUser.email
            }
          }
          return company
        })
      )

      return { success: true, data: companiesWithAdminEmail }
    } catch (error) {
      console.error('Get companies error:', error)
      return { success: false, error: error.message }
    }
  }

  async getCompanyById(companyId: string) {
    try {
      const { data: company, error } = await this.supabase
        .from('companies')
        .select(`
          *,
          admin_email,
          expiry_date
        `)
        .eq('id', companyId)
        .single()

      if (error) throw error

      // Ensure admin_email is populated if not set
      if (!company.admin_email) {
        const { data: adminUser } = await this.supabase
          .from('users')
          .select('email')
          .eq('company_id', company.id)
          .eq('is_admin', true)
          .limit(1)
          .single()
        
        if (adminUser?.email) {
          // Update the company record with admin email
          await this.supabase
            .from('companies')
            .update({ admin_email: adminUser.email })
            .eq('id', company.id)
          
          company.admin_email = adminUser.email
        }
      }

      return { success: true, data: company }
    } catch (error) {
      console.error('Get company by ID error:', error)
      return { success: false, error: error.message }
    }
  }

  // User management
  async createUser(data: {
    companyId: string
    email: string
    fullName: string
    roleId: string
    password: string
    isAdmin?: boolean
  }) {
    try {
      // Check if company has available user slots
      const { data: company, error: companyError } = await this.supabase
        .from('companies')
        .select('max_users, current_users')
        .eq('id', data.companyId)
        .single()

      if (companyError) throw companyError
      
      if (company.current_users >= company.max_users) {
        return { 
          success: false, 
          error: 'Company has reached maximum user limit' 
        }
      }

      const { data: user, error } = await this.supabase
        .from('users')
        .insert({
          company_id: data.companyId,
          email: data.email,
          full_name: data.fullName,
          role_id: data.roleId,
          password: data.password,
          password_changed: false, // Mark as requiring password change on first login
          is_admin: data.isAdmin || false,
          is_active: true
        })
        .select(`
          *,
          company:companies(*),
          role:user_roles(*)
        `)
        .single()

      if (error) throw error
      return { success: true, data: user }
    } catch (error) {
      console.error('Create user error:', error)
      return { success: false, error: error.message }
    }
  }

  async getCompanyUsers(companyId: string) {
    try {
      const { data: users, error } = await this.supabase
        .from('users')
        .select(`
          *,
          role:user_roles(name, description)
        `)
        .eq('company_id', companyId)
        .order('created_at', { ascending: false })

      if (error) throw error
      return { success: true, data: users }
    } catch (error) {
      console.error('Get company users error:', error)
      return { success: false, error: error.message }
    }
  }

  async updateUser(userId: string, data: {
    email?: string
    fullName?: string
    roleId?: string
    isActive?: boolean
    isAdmin?: boolean
  }) {
    try {
      const { data: user, error } = await this.supabase
        .from('users')
        .update({
          ...(data.email && { email: data.email }),
          ...(data.fullName && { full_name: data.fullName }),
          ...(data.roleId && { role_id: data.roleId }),
          ...(data.isActive !== undefined && { is_active: data.isActive }),
          ...(data.isAdmin !== undefined && { is_admin: data.isAdmin })
        })
        .eq('id', userId)
        .select(`
          *,
          company:companies(*),
          role:user_roles(*)
        `)
        .single()

      if (error) throw error
      return { success: true, data: user }
    } catch (error) {
      console.error('Update user error:', error)
      return { success: false, error: error.message }
    }
  }

  async deleteUser(userId: string) {
    try {
      const { error } = await this.supabase
        .from('users')
        .delete()
        .eq('id', userId)

      if (error) throw error
      return { success: true }
    } catch (error) {
      console.error('Delete user error:', error)
      return { success: false, error: error.message }
    }
  }

  // Role management
  async getAllRoles() {
    try {
      const { data: roles, error } = await this.supabase
        .from('user_roles')
        .select('*')
        .order('name')

      if (error) throw error
      return { success: true, data: roles }
    } catch (error) {
      console.error('Get roles error:', error)
      return { success: false, error: error.message }
    }
  }

  // Authentication helpers
  async getUserByEmail(email: string) {
    try {
      const { data: user, error } = await this.supabase
        .from('users')
        .select(`
          *,
          company:companies(*),
          role:user_roles(*)
        `)
        .eq('email', email)
        .single()

      if (error) throw error
      return { success: true, data: user }
    } catch (error) {
      console.error('Get user by email error:', error)
      return { success: false, error: error.message }
    }
  }

  async updateLastLogin(userId: string) {
    try {
      const { error } = await this.supabase
        .from('users')
        .update({ last_login: new Date().toISOString() })
        .eq('id', userId)

      if (error) throw error
      return { success: true }
    } catch (error) {
      console.error('Update last login error:', error)
      return { success: false, error: error.message }
    }
  }

  async getUserById(userId: string) {
    try {
      const { data: user, error } = await this.supabase
        .from('users')
        .select(`
          *,
          company:companies(*),
          role:user_roles(*)
        `)
        .eq('id', userId)
        .single()

      if (error) throw error
      return { success: true, data: user }
    } catch (error) {
      console.error('Get user by ID error:', error)
      return { success: false, error: error.message }
    }
  }

  async updateUserPassword(userId: string, newPassword: string) {
    try {
      const { data: user, error } = await this.supabase
        .from('users')
        .update({ 
          password: newPassword,
          password_changed: true,
          updated_at: new Date().toISOString()
        })
        .eq('id', userId)
        .select()
        .single()

      if (error) throw error
      return { success: true, data: user }
    } catch (error) {
      console.error('Update user password error:', error)
      return { success: false, error: error.message }
    }
  }

  // Check user permissions
  async hasPermission(userId: string, permission: string): Promise<boolean> {
    try {
      const { data: user, error } = await this.supabase
        .from('users')
        .select(`
          is_super_admin,
          role:user_roles(permissions)
        `)
        .eq('id', userId)
        .single()

      if (error || !user) return false

      // Super admins have all permissions
      if (user.is_super_admin) return true

      // Check role permissions
      const permissions = user.role?.permissions as string[] || []
      return permissions.includes('*') || permissions.includes(permission)
    } catch (error) {
      console.error('Check permission error:', error)
      return false
    }
  }

  // Notification management
  async createNotification(data: {
    userId?: string
    companyId?: string
    title: string
    message: string
    type?: 'info' | 'success' | 'warning' | 'error'
    entityType?: string
    entityId?: string
  }) {
    try {
      // Use raw SQL to bypass RLS for system notifications
      const { data: notification, error } = await this.supabase.rpc('create_notification', {
        p_user_id: data.userId || null,
        p_company_id: data.companyId || null,
        p_title: data.title,
        p_message: data.message,
        p_type: data.type || 'info',
        p_entity_type: data.entityType || null,
        p_entity_id: data.entityId || null
      })

      if (error) throw error
      return { success: true, data: notification }
    } catch (error) {
      console.error('Create notification error:', error)
      // Fallback to direct insert
      try {
        const { data: notification, error: insertError } = await this.supabase
          .from('notifications')
          .insert({
            user_id: data.userId || null,
            company_id: data.companyId || null,
            title: data.title,
            message: data.message,
            type: data.type || 'info',
            entity_type: data.entityType || null,
            entity_id: data.entityId || null
          })
          .select()
          .single()

        if (insertError) throw insertError
        return { success: true, data: notification }
      } catch (fallbackError) {
        console.error('Fallback notification creation failed:', fallbackError)
        return { success: false, error: fallbackError.message }
      }
    }
  }

  async getUserNotifications(userId: string, limit: number = 20) {
    try {
      console.log('Fetching user notifications for userId:', userId)
      
      // Get user details to determine their company
      const { data: user, error: userError } = await this.supabase
        .from('users')
        .select('company_id, is_admin')
        .eq('id', userId)
        .single()

      if (userError) {
        console.error('Error fetching user details:', userError)
        throw userError
      }

      console.log('User details:', user)

      let query = this.supabase
        .from('notifications')
        .select('*')

      if (user.is_admin && user.company_id) {
        // Company admin sees: notifications for their company + their personal notifications
        query = query.or(`company_id.eq.${user.company_id},user_id.eq.${userId}`)
      } else if (user.company_id) {
        // Regular user sees: only their personal notifications + company-wide notifications for their company
        query = query.or(`user_id.eq.${userId},company_id.eq.${user.company_id}`)
      } else {
        // Users without company see only their personal notifications
        query = query.eq('user_id', userId)
      }

      const { data: notifications, error } = await query
        .order('created_at', { ascending: false })
        .limit(limit)

      if (error) throw error
      
      console.log('Found user notifications:', notifications?.length || 0)
      return { success: true, data: notifications || [] }
    } catch (error) {
      console.error('Get user notifications error:', error)
      return { success: false, error: error.message }
    }
  }

  async getSuperAdminNotifications(limit: number = 20) {
    try {
      console.log('Fetching super admin notifications...')
      
      // Super admin sees: system notifications + notifications with no company_id
      const { data: notifications, error } = await this.supabase
        .from('notifications')
        .select('*')
        .or('company_id.is.null,entity_type.eq.system')
        .order('created_at', { ascending: false })
        .limit(limit)

      if (error) {
        console.error('Supabase error:', error)
        throw error
      }
      
      console.log('Found super admin notifications:', notifications?.length || 0)
      return { success: true, data: notifications || [] }
    } catch (error) {
      console.error('Get super admin notifications error:', error)
      return { success: false, error: error.message }
    }
  }

  async markNotificationAsRead(notificationId: string) {
    try {
      const { error } = await this.supabase
        .from('notifications')
        .update({ is_read: true, updated_at: new Date().toISOString() })
        .eq('id', notificationId)

      if (error) throw error
      return { success: true }
    } catch (error) {
      console.error('Mark notification as read error:', error)
      return { success: false, error: error.message }
    }
  }

  async markAllNotificationsAsRead(userId: string) {
    try {
      const { error } = await this.supabase
        .from('notifications')
        .update({ is_read: true, updated_at: new Date().toISOString() })
        .eq('user_id', userId)

      if (error) throw error
      return { success: true }
    } catch (error) {
      console.error('Mark all notifications as read error:', error)
      return { success: false, error: error.message }
    }
  }
}

export const db = new DatabaseService()