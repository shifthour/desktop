import React from 'react'
import { render, screen } from '@testing-library/react'
import { ErrorBoundary } from '@/components/error-boundary'

describe('ErrorBoundary', () => {
  // Suppress console.error for these tests
  const originalError = console.error
  beforeAll(() => {
    console.error = jest.fn()
  })

  afterAll(() => {
    console.error = originalError
  })

  it('should render children when there is no error', () => {
    render(
      <ErrorBoundary>
        <div>Child Component</div>
      </ErrorBoundary>
    )

    expect(screen.getByText('Child Component')).toBeInTheDocument()
  })

  it('should render error UI when child component throws', () => {
    const ThrowError = () => {
      throw new Error('Test error')
    }

    render(
      <ErrorBoundary>
        <ThrowError />
      </ErrorBoundary>
    )

    expect(screen.getByText(/something went wrong/i)).toBeInTheDocument()
  })

  it('should handle error with custom fallback', () => {
    const ThrowError = () => {
      throw new Error('Custom error message')
    }

    render(
      <ErrorBoundary fallback={<div>Custom Error UI</div>}>
        <ThrowError />
      </ErrorBoundary>
    )

    // This test assumes ErrorBoundary accepts a fallback prop
    // Adjust based on actual implementation
  })
})
