import React from 'react';

export default function EditableText({
  textKey,
  defaultContent,
  as: Component = 'p',
  multiline = false,
  className = '',
  ...props
}) {
  return (
    <Component className={className} {...props}>
      {defaultContent}
    </Component>
  );
}
