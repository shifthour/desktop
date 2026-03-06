"use client"

import type * as React from "react"
import { cn } from "@/lib/utils"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { MoreHorizontal, TrendingUp, TrendingDown } from "lucide-react"

interface EnhancedCardProps extends React.ComponentProps<typeof Card> {
  title: string
  description?: string
  value: string | number
  change?: {
    value: string
    type: "positive" | "negative" | "neutral"
    period?: string
  }
  icon?: React.ComponentType<{ className?: string }>
  iconColor?: string
  iconBg?: string
  actions?: React.ReactNode
  trend?: "up" | "down" | "neutral"
}

export function EnhancedCard({
  title,
  description,
  value,
  change,
  icon: Icon,
  iconColor,
  iconBg,
  actions,
  trend,
  className,
  ...props
}: EnhancedCardProps) {
  return (
    <Card className={cn("relative overflow-hidden transition-all hover:shadow-lg", className)} {...props}>
      <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
        <div className="space-y-1">
          <CardTitle className="text-sm font-medium text-muted-foreground">{title}</CardTitle>
          {description && <CardDescription className="text-xs">{description}</CardDescription>}
        </div>
        <div className="flex items-center space-x-2">
          {Icon && (
            <div className={cn(
              "h-8 w-8 rounded-lg flex items-center justify-center",
              iconBg || "bg-primary/10"
            )}>
              <Icon className={cn(
                "h-4 w-4",
                iconColor || "text-primary"
              )} />
            </div>
          )}
          {actions && (
            <Button variant="ghost" size="sm">
              <MoreHorizontal className="h-4 w-4" />
            </Button>
          )}
        </div>
      </CardHeader>
      <CardContent>
        <div className="space-y-2">
          <div className="text-2xl font-bold tracking-tight">{value}</div>
          {change && (
            <div className="flex items-center space-x-2">
              <Badge
                variant="outline"
                className={cn(
                  "text-xs",
                  change.type === "positive" && "border-green-200 bg-green-50 text-green-700",
                  change.type === "negative" && "border-red-200 bg-red-50 text-red-700",
                  change.type === "neutral" && "border-gray-200 bg-gray-50 text-gray-700",
                )}
              >
                {change.type === "positive" && <TrendingUp className="h-3 w-3 mr-1" />}
                {change.type === "negative" && <TrendingDown className="h-3 w-3 mr-1" />}
                {change.value}
              </Badge>
              {change.period && <span className="text-xs text-muted-foreground">{change.period}</span>}
            </div>
          )}
        </div>
      </CardContent>
      {trend && (
        <div
          className={cn(
            "absolute bottom-0 left-0 right-0 h-1",
            trend === "up" && "bg-gradient-to-r from-green-400 to-green-600",
            trend === "down" && "bg-gradient-to-r from-red-400 to-red-600",
            trend === "neutral" && "bg-gradient-to-r from-gray-400 to-gray-600",
          )}
        />
      )}
    </Card>
  )
}
