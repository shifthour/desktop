import { HTMLAttributes } from "react";

interface CardProps extends HTMLAttributes<HTMLDivElement> {
  hover?: boolean;
}

export function Card({
  hover = false,
  className = "",
  children,
  ...props
}: CardProps) {
  return (
    <div
      className={`glass-card rounded-2xl p-6 ${
        hover
          ? "transition-all duration-300 hover:border-brand-purple/30 hover:shadow-lg hover:shadow-brand-purple/5 hover:-translate-y-0.5"
          : ""
      } ${className}`}
      {...props}
    >
      {children}
    </div>
  );
}
