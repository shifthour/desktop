"use client";

import * as React from "react";
import { cva, type VariantProps } from "class-variance-authority";
import { cn } from "@/lib/utils";

const badgeVariants = cva(
  "inline-flex items-center rounded-md border px-2.5 py-0.5 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2",
  {
    variants: {
      variant: {
        default: "border-transparent bg-blue-600 text-white",
        secondary: "border-transparent bg-gray-100 text-gray-900",
        destructive: "border-transparent bg-red-600 text-white",
        success: "border-transparent bg-green-600 text-white",
        warning: "border-transparent bg-yellow-500 text-white",
        outline: "border-gray-300 text-gray-700",
        // Status badges for clinical trial
        draft: "border-transparent bg-gray-100 text-gray-700",
        active: "border-transparent bg-green-100 text-green-800",
        completed: "border-transparent bg-blue-100 text-blue-800",
        locked: "border-transparent bg-purple-100 text-purple-800",
        open: "border-transparent bg-red-100 text-red-800",
        responded: "border-transparent bg-yellow-100 text-yellow-800",
        closed: "border-transparent bg-green-100 text-green-800",
      },
    },
    defaultVariants: {
      variant: "default",
    },
  }
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

function Badge({ className, variant, ...props }: BadgeProps) {
  return (
    <div className={cn(badgeVariants({ variant }), className)} {...props} />
  );
}

export { Badge, badgeVariants };
