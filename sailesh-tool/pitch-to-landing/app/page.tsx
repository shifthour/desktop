'use client'

import { useState, useCallback } from 'react'

type Status = 'idle' | 'uploading' | 'parsing' | 'analyzing' | 'generating' | 'deploying' | 'complete' | 'error'

interface ProgressStep {
  status: Status
  label: string
  description: string
}

const steps: ProgressStep[] = [
  { status: 'uploading', label: 'Uploading', description: 'Uploading your PDF...' },
  { status: 'parsing', label: 'Parsing', description: 'Extracting content from PDF...' },
  { status: 'analyzing', label: 'Analyzing', description: 'AI is analyzing your pitch deck...' },
  { status: 'generating', label: 'Generating', description: 'Creating interactive landing page...' },
  { status: 'deploying', label: 'Deploying', description: 'Deploying to Vercel...' },
]

export default function Home() {
  const [file, setFile] = useState<File | null>(null)
  const [projectName, setProjectName] = useState('')
  const [status, setStatus] = useState<Status>('idle')
  const [error, setError] = useState<string | null>(null)
  const [deployedUrl, setDeployedUrl] = useState<string | null>(null)
  const [dragOver, setDragOver] = useState(false)

  const handleDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    setDragOver(false)
    const droppedFile = e.dataTransfer.files[0]
    if (droppedFile?.type === 'application/pdf') {
      setFile(droppedFile)
      setError(null)
    } else {
      setError('Please upload a PDF file')
    }
  }, [])

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0]
    if (selectedFile) {
      if (selectedFile.type === 'application/pdf') {
        setFile(selectedFile)
        setError(null)
      } else {
        setError('Please upload a PDF file')
      }
    }
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()

    if (!file || !projectName.trim()) {
      setError('Please upload a PDF and enter a project name')
      return
    }

    // Validate project name (lowercase, no spaces, alphanumeric with hyphens)
    const cleanProjectName = projectName.toLowerCase().replace(/[^a-z0-9-]/g, '-').replace(/-+/g, '-')

    setError(null)
    setDeployedUrl(null)
    setStatus('uploading')

    try {
      const formData = new FormData()
      formData.append('pdf', file)
      formData.append('projectName', cleanProjectName)

      const response = await fetch('/api/generate', {
        method: 'POST',
        body: formData,
      })

      const reader = response.body?.getReader()
      const decoder = new TextDecoder()

      if (!reader) throw new Error('No response stream')

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        const text = decoder.decode(value)
        const lines = text.split('\n').filter(line => line.trim())

        for (const line of lines) {
          try {
            const data = JSON.parse(line)

            if (data.status) {
              setStatus(data.status)
            }

            if (data.error) {
              setError(data.error)
              setStatus('error')
              return
            }

            if (data.url) {
              setDeployedUrl(data.url)
              setStatus('complete')
            }
          } catch {
            // Skip non-JSON lines
          }
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred')
      setStatus('error')
    }
  }

  const getCurrentStepIndex = () => {
    return steps.findIndex(step => step.status === status)
  }

  const resetForm = () => {
    setFile(null)
    setProjectName('')
    setStatus('idle')
    setError(null)
    setDeployedUrl(null)
  }

  return (
    <main className="min-h-screen">
      {/* Header */}
      <header className="gradient-bg text-white py-6 px-4">
        <div className="max-w-4xl mx-auto">
          <h1 className="text-3xl font-bold">
            <span className="text-[#D4A84B]">Pitch</span> to Landing
          </h1>
          <p className="text-gray-300 mt-1">Convert pitch deck PDFs to interactive landing pages</p>
        </div>
      </header>

      {/* Main Content */}
      <div className="max-w-4xl mx-auto px-4 py-12">
        {status === 'complete' && deployedUrl ? (
          /* Success State */
          <div className="bg-white rounded-2xl shadow-xl p-8 text-center">
            <div className="w-20 h-20 bg-green-100 rounded-full flex items-center justify-center mx-auto mb-6">
              <svg className="w-10 h-10 text-green-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
              </svg>
            </div>
            <h2 className="text-2xl font-bold text-gray-800 mb-2">Landing Page Deployed!</h2>
            <p className="text-gray-600 mb-6">Your interactive landing page is now live.</p>

            <div className="bg-gray-50 rounded-lg p-4 mb-6">
              <p className="text-sm text-gray-500 mb-2">Your URL:</p>
              <a
                href={deployedUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="text-[#1a1a5e] font-semibold hover:text-[#D4A84B] break-all"
              >
                {deployedUrl}
              </a>
            </div>

            <div className="flex gap-4 justify-center">
              <a
                href={deployedUrl}
                target="_blank"
                rel="noopener noreferrer"
                className="btn-primary text-white px-6 py-3 rounded-lg font-semibold"
              >
                View Landing Page
              </a>
              <button
                onClick={resetForm}
                className="bg-gray-200 text-gray-700 px-6 py-3 rounded-lg font-semibold hover:bg-gray-300 transition"
              >
                Create Another
              </button>
            </div>
          </div>
        ) : status !== 'idle' && status !== 'error' ? (
          /* Progress State */
          <div className="bg-white rounded-2xl shadow-xl p-8">
            <h2 className="text-xl font-bold text-gray-800 mb-8 text-center">Creating Your Landing Page...</h2>

            <div className="space-y-4">
              {steps.map((step, index) => {
                const currentIndex = getCurrentStepIndex()
                const isComplete = index < currentIndex
                const isCurrent = index === currentIndex
                const isPending = index > currentIndex

                return (
                  <div key={step.status} className="flex items-center gap-4">
                    <div className={`
                      w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0
                      ${isComplete ? 'bg-green-500' : isCurrent ? 'bg-[#D4A84B]' : 'bg-gray-200'}
                    `}>
                      {isComplete ? (
                        <svg className="w-5 h-5 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M5 13l4 4L19 7" />
                        </svg>
                      ) : isCurrent ? (
                        <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full spinner" />
                      ) : (
                        <span className="text-gray-500 font-semibold">{index + 1}</span>
                      )}
                    </div>
                    <div className="flex-1">
                      <p className={`font-semibold ${isCurrent ? 'text-[#1a1a5e]' : isComplete ? 'text-green-600' : 'text-gray-400'}`}>
                        {step.label}
                      </p>
                      {isCurrent && (
                        <p className="text-sm text-gray-500 animate-pulse-slow">{step.description}</p>
                      )}
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        ) : (
          /* Upload Form */
          <form onSubmit={handleSubmit} className="bg-white rounded-2xl shadow-xl p-8">
            <div className="mb-8">
              <label className="block text-sm font-semibold text-gray-700 mb-3">
                Upload Pitch Deck PDF
              </label>
              <div
                onDrop={handleDrop}
                onDragOver={(e) => { e.preventDefault(); setDragOver(true) }}
                onDragLeave={() => setDragOver(false)}
                className={`upload-zone rounded-xl p-8 text-center cursor-pointer ${dragOver ? 'drag-over' : ''}`}
                onClick={() => document.getElementById('file-input')?.click()}
              >
                <input
                  id="file-input"
                  type="file"
                  accept=".pdf"
                  onChange={handleFileChange}
                  className="hidden"
                />
                {file ? (
                  <div className="flex items-center justify-center gap-3">
                    <svg className="w-8 h-8 text-[#D4A84B]" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                    </svg>
                    <div className="text-left">
                      <p className="font-semibold text-gray-800">{file.name}</p>
                      <p className="text-sm text-gray-500">{(file.size / 1024 / 1024).toFixed(2)} MB</p>
                    </div>
                    <button
                      type="button"
                      onClick={(e) => { e.stopPropagation(); setFile(null) }}
                      className="ml-4 text-gray-400 hover:text-red-500"
                    >
                      <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  </div>
                ) : (
                  <>
                    <svg className="w-12 h-12 text-gray-400 mx-auto mb-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                    </svg>
                    <p className="text-gray-600 font-medium">Drag and drop your PDF here</p>
                    <p className="text-gray-400 text-sm mt-1">or click to browse</p>
                  </>
                )}
              </div>
            </div>

            <div className="mb-8">
              <label htmlFor="project-name" className="block text-sm font-semibold text-gray-700 mb-3">
                Project Name
              </label>
              <input
                id="project-name"
                type="text"
                value={projectName}
                onChange={(e) => setProjectName(e.target.value)}
                placeholder="e.g., my-company-pitch"
                className="w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-[#D4A84B] focus:border-transparent outline-none transition"
              />
              <p className="text-xs text-gray-500 mt-2">
                This will be used in your Vercel URL: {projectName ? `${projectName.toLowerCase().replace(/[^a-z0-9-]/g, '-')}.vercel.app` : 'your-project.vercel.app'}
              </p>
            </div>

            {error && (
              <div className="mb-6 p-4 bg-red-50 border border-red-200 rounded-lg">
                <p className="text-red-600 text-sm">{error}</p>
              </div>
            )}

            <button
              type="submit"
              disabled={!file || !projectName.trim()}
              className={`
                w-full py-4 rounded-lg font-semibold text-lg transition
                ${file && projectName.trim()
                  ? 'btn-primary text-white'
                  : 'bg-gray-200 text-gray-400 cursor-not-allowed'}
              `}
            >
              Generate Landing Page
            </button>
          </form>
        )}

        {/* How it works */}
        <div className="mt-12 grid md:grid-cols-3 gap-6">
          {[
            { icon: '📄', title: 'Upload PDF', desc: 'Upload your pitch deck or financial document' },
            { icon: '🤖', title: 'AI Analysis', desc: 'Claude AI extracts and structures your content' },
            { icon: '🚀', title: 'Auto Deploy', desc: 'Interactive landing page deployed to Vercel' },
          ].map((item, i) => (
            <div key={i} className="bg-white rounded-xl p-6 text-center shadow-md">
              <div className="text-4xl mb-3">{item.icon}</div>
              <h3 className="font-semibold text-gray-800 mb-2">{item.title}</h3>
              <p className="text-gray-500 text-sm">{item.desc}</p>
            </div>
          ))}
        </div>
      </div>
    </main>
  )
}
