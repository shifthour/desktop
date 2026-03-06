"use client";

interface PromptEditorProps {
  value: string;
  onChange: (value: string) => void;
  disabled?: boolean;
  placeholder?: string;
}

export function PromptEditor({
  value,
  onChange,
  disabled = false,
  placeholder = "Describe how you want the landing page to look...",
}: PromptEditorProps) {
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <label className="text-sm font-semibold text-gray-300">
          AI Prompt
        </label>
        <span className="text-[10px] text-gray-600">
          {value.length} characters
        </span>
      </div>
      <div className="relative">
        <textarea
          value={value}
          onChange={(e) => onChange(e.target.value)}
          disabled={disabled}
          rows={8}
          placeholder={placeholder}
          className="w-full resize-y rounded-xl border border-dark-border bg-dark-bg/50 px-4 py-3 text-sm text-gray-200 placeholder-gray-600 transition-all focus:border-brand-purple/50 focus:outline-none focus:ring-1 focus:ring-brand-purple/30 disabled:opacity-50 font-mono leading-relaxed"
        />
        <div className="absolute bottom-3 right-3">
          <span className="text-[9px] uppercase tracking-wider text-gray-700">
            editable
          </span>
        </div>
      </div>
      <p className="text-[11px] text-gray-600">
        This prompt guides the AI in generating your landing page. Edit it to customize the output.
      </p>
    </div>
  );
}
