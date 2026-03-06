"use client"

import { useState, useEffect } from "react"

export function InstallationsContentSimple() {
  console.log("=== SIMPLE INSTALLATIONS CONTENT RENDERING ===")
  const [message, setMessage] = useState("Loading...")

  useEffect(() => {
    console.log("=== Simple useEffect running ===")
    setMessage("Loaded successfully!")
  }, [])

  console.log("=== About to render simple component ===")

  return (
    <div className="p-6">
      <h1 className="text-2xl font-bold">Installations Management</h1>
      <p>Status: {message}</p>
      <p>This is a simplified component to test rendering.</p>
    </div>
  )
}