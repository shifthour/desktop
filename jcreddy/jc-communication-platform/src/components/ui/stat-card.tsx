'use client'

import { cn } from '@/lib/utils'
import { LucideIcon, TrendingUp, TrendingDown } from 'lucide-react'

interface StatCardProps {
  title: string
  value: string | number
  change?: number
  changeLabel?: string
  icon: LucideIcon
  iconColor?: string
  iconBgColor?: string
}

export function StatCard({
  title,
  value,
  change,
  changeLabel,
  icon: Icon,
  iconColor = 'text-blue-600',
  iconBgColor = 'bg-blue-100',
}: StatCardProps) {
  const isPositive = change && change > 0
  const isNegative = change && change < 0

  return (
    <div className="stat-card group">
      <div className="flex items-start justify-between">
        <div className="space-y-2">
          <p className="text-sm font-medium text-gray-500">{title}</p>
          <p className="text-3xl font-bold text-gray-900">{value}</p>
          {change !== undefined && (
            <div className="flex items-center gap-1">
              {isPositive && (
                <TrendingUp className="h-4 w-4 text-green-500" />
              )}
              {isNegative && (
                <TrendingDown className="h-4 w-4 text-red-500" />
              )}
              <span
                className={cn(
                  'text-sm font-medium',
                  isPositive && 'text-green-600',
                  isNegative && 'text-red-600',
                  !isPositive && !isNegative && 'text-gray-500'
                )}
              >
                {isPositive && '+'}
                {change}%
              </span>
              {changeLabel && (
                <span className="text-sm text-gray-400">{changeLabel}</span>
              )}
            </div>
          )}
        </div>
        <div
          className={cn(
            'flex h-12 w-12 items-center justify-center rounded-xl transition-transform group-hover:scale-110',
            iconBgColor
          )}
        >
          <Icon className={cn('h-6 w-6', iconColor)} />
        </div>
      </div>

      {/* Decorative gradient */}
      <div className="absolute bottom-0 left-0 right-0 h-1 bg-gradient-to-r from-blue-600 to-indigo-600 opacity-0 transition-opacity group-hover:opacity-100" />
    </div>
  )
}
