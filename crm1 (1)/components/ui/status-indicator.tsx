"use client"

import { cn } from "@/lib/utils"
import { Badge } from "@/components/ui/badge"

interface StatusIndicatorProps {
  status: string
  variant?: "default" | "success" | "warning" | "error" | "info"
  size?: "sm" | "md" | "lg"
  showDot?: boolean
}

const statusVariants = {
  default: "bg-gray-100 text-gray-800 border-gray-200",
  success: "bg-green-100 text-green-800 border-green-200",
  warning: "bg-yellow-100 text-yellow-800 border-yellow-200",
  error: "bg-red-100 text-red-800 border-red-200",
  info: "bg-blue-100 text-blue-800 border-blue-200",
}

const dotVariants = {
  default: "bg-gray-400",
  success: "bg-green-400",
  warning: "bg-yellow-400",
  error: "bg-red-400",
  info: "bg-blue-400",
}

export function StatusIndicator({ status, variant = "default", size = "md", showDot = true }: StatusIndicatorProps) {
  return (
    <Badge
      variant="outline"
      className={cn(
        "inline-flex items-center gap-1.5",
        statusVariants[variant],
        size === "sm" && "text-xs px-2 py-0.5",
        size === "md" && "text-sm px-2.5 py-1",
        size === "lg" && "text-base px-3 py-1.5",
      )}
    >
      {showDot && (
        <div
          className={cn(
            "rounded-full",
            dotVariants[variant],
            size === "sm" && "h-1.5 w-1.5",
            size === "md" && "h-2 w-2",
            size === "lg" && "h-2.5 w-2.5",
          )}
        />
      )}
      {status}
    </Badge>
  )
}
