import React from "react";
import { Star } from "lucide-react";
import { cn } from "@/lib/utils";

export default function StarRating({ rating, size = "sm", interactive = false, onRate }) {
  const sizes = {
    xs: "w-3 h-3",
    sm: "w-4 h-4",
    md: "w-5 h-5",
    lg: "w-6 h-6",
  };

  return (
    <div className="flex items-center gap-0.5">
      {[1, 2, 3, 4, 5].map(star => (
        <button
          key={star}
          type="button"
          disabled={!interactive}
          onClick={() => interactive && onRate?.(star)}
          className={cn(
            interactive && "cursor-pointer hover:scale-110 transition-transform",
            !interactive && "cursor-default"
          )}
        >
          <Star
            className={cn(
              sizes[size],
              star <= (rating || 0) ? "text-amber-400 fill-amber-400" : "text-gray-200"
            )}
          />
        </button>
      ))}
    </div>
  );
}