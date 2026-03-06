import React from 'react'
import { render, screen, waitFor } from '@testing-library/react'
import { ProtectedRoute } from '@/components/protected-route'
import { useRouter } from 'next/navigation'

// Mock the next/navigation module
jest.mock('next/navigation', () => ({
  useRouter: jest.fn(),
}))

describe('ProtectedRoute', () => {
  const mockPush = jest.fn()
  const mockRouter = {
    push: mockPush,
    replace: jest.fn(),
    prefetch: jest.fn(),
    back: jest.fn(),
  }

  beforeEach(() => {
    jest.clearAllMocks()
    ;(useRouter as jest.Mock).mockReturnValue(mockRouter)
    localStorage.clear()
  })

  it('should render loading state when no user is stored', () => {
    render(
      <ProtectedRoute>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    expect(screen.getByText('Checking authentication...')).toBeInTheDocument()
  })

  it('should redirect to login when no user is stored', async () => {
    render(
      <ProtectedRoute>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    await waitFor(() => {
      expect(mockPush).toHaveBeenCalledWith('/login')
    })
  })

  it('should render children when valid user is stored', () => {
    const user = {
      id: '123',
      email: 'test@example.com',
      is_active: true,
      is_admin: false,
      is_super_admin: false,
    }
    localStorage.setItem('user', JSON.stringify(user))

    render(
      <ProtectedRoute>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })

  it('should redirect to unauthorized when super admin is required but user is not super admin', async () => {
    const user = {
      id: '123',
      email: 'test@example.com',
      is_active: true,
      is_admin: false,
      is_super_admin: false,
    }
    localStorage.setItem('user', JSON.stringify(user))

    render(
      <ProtectedRoute requireSuperAdmin={true}>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    await waitFor(() => {
      expect(mockPush).toHaveBeenCalledWith('/unauthorized')
    })
  })

  it('should allow access when super admin is required and user is super admin', () => {
    const user = {
      id: '123',
      email: 'test@example.com',
      is_active: true,
      is_admin: false,
      is_super_admin: true,
    }
    localStorage.setItem('user', JSON.stringify(user))

    render(
      <ProtectedRoute requireSuperAdmin={true}>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })

  it('should redirect to unauthorized when admin is required but user is neither admin nor super admin', async () => {
    const user = {
      id: '123',
      email: 'test@example.com',
      is_active: true,
      is_admin: false,
      is_super_admin: false,
    }
    localStorage.setItem('user', JSON.stringify(user))

    render(
      <ProtectedRoute requireAdmin={true}>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    await waitFor(() => {
      expect(mockPush).toHaveBeenCalledWith('/unauthorized')
    })
  })

  it('should allow access when admin is required and user is company admin', () => {
    const user = {
      id: '123',
      email: 'test@example.com',
      is_active: true,
      is_admin: true,
      is_super_admin: false,
    }
    localStorage.setItem('user', JSON.stringify(user))

    render(
      <ProtectedRoute requireAdmin={true}>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    expect(screen.getByText('Protected Content')).toBeInTheDocument()
  })

  it('should redirect to account-disabled when user is not active', async () => {
    const user = {
      id: '123',
      email: 'test@example.com',
      is_active: false,
      is_admin: false,
      is_super_admin: false,
    }
    localStorage.setItem('user', JSON.stringify(user))

    render(
      <ProtectedRoute>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    await waitFor(() => {
      expect(mockPush).toHaveBeenCalledWith('/account-disabled')
    })
  })

  it('should redirect to login when user data is invalid JSON', async () => {
    localStorage.setItem('user', 'invalid-json')

    render(
      <ProtectedRoute>
        <div>Protected Content</div>
      </ProtectedRoute>
    )

    await waitFor(() => {
      expect(mockPush).toHaveBeenCalledWith('/login')
      expect(localStorage.getItem('user')).toBeNull()
    })
  })
})
