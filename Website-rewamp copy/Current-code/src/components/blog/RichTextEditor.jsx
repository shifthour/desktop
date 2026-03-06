import React, { useEffect, useRef, useState, useCallback } from 'react';
import ReactQuill from 'react-quill';
import 'react-quill/dist/quill.snow.css';

const editorStyles = `
  .rich-text-editor-wrapper .ql-container {
    min-height: 350px; font-size: 16px; border: none;
    border-bottom-left-radius: 0.375rem; border-bottom-right-radius: 0.375rem;
  }
  .rich-text-editor-wrapper .ql-toolbar {
    background-color: #f9fafb; border: none; border-bottom: 1px solid #d1d5db;
    border-top-left-radius: 0.375rem; border-top-right-radius: 0.375rem;
    position: sticky; top: 0; z-index: 10;
  }
  .rich-text-editor-wrapper .ql-editor {
    min-height: 350px; cursor: text; background: white;
  }
  .rich-text-editor-wrapper .ql-editor.ql-blank::before {
    font-style: normal; color: #9ca3af;
  }
`;

export default function RichTextEditor({ value, onChange, editorKey }) {
  const [internalValue, setInternalValue] = useState(value || '');
  const [showHtmlEditor, setShowHtmlEditor] = useState(false);
  const quillRef = useRef(null);
  const timeoutRef = useRef(null);

  useEffect(() => {
    setInternalValue(value || '');
  }, [editorKey]);

  const handleChange = useCallback((content) => {
    setInternalValue(content);
    if (timeoutRef.current) clearTimeout(timeoutRef.current);
    timeoutRef.current = setTimeout(() => onChange(content), 500);
  }, [onChange]);

  useEffect(() => {
    return () => { if (timeoutRef.current) clearTimeout(timeoutRef.current); };
  }, []);

  const modules = {
    toolbar: [
      [{ 'header': [1, 2, 3, 4, 5, 6, false] }],
      ['bold', 'italic', 'underline', 'strike'],
      [{ 'list': 'ordered'}, { 'list': 'bullet' }],
      ['link', 'image'],
      [{ 'align': [] }],
      ['blockquote', 'code-block'],
      [{ 'color': [] }, { 'background': [] }],
      ['clean']
    ],
    clipboard: { matchVisual: false }
  };

  return (
    <>
      <style>{editorStyles}</style>
      <div className="rich-text-editor-wrapper rounded-md border border-gray-300 bg-white">
        {showHtmlEditor ? (
          <div className="p-4">
            <textarea
              value={internalValue}
              onChange={(e) => handleChange(e.target.value)}
              className="w-full min-h-[350px] p-3 font-mono text-sm border border-gray-300 rounded"
              placeholder="Edit HTML directly..."
            />
          </div>
        ) : (
          <ReactQuill
            ref={quillRef}
            theme="snow"
            value={internalValue}
            onChange={handleChange}
            modules={modules}
            placeholder="Write your content here..."
          />
        )}
        <div className="p-3 border-t border-gray-200 bg-gray-50 flex items-center justify-end">
          <button
            onClick={() => setShowHtmlEditor(!showHtmlEditor)}
            className="px-3 py-1 bg-gray-700 text-white rounded hover:bg-gray-800 transition-colors text-xs"
          >
            {showHtmlEditor ? 'Visual Editor' : '< /> HTML Editor'}
          </button>
        </div>
      </div>
    </>
  );
}
