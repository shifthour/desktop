import React from 'react';

export default function EditableBackgroundImage({
  imageKey,
  src,
  className = '',
  children,
  ...props
}) {
  return (
    <div
      className={className}
      style={{
        backgroundImage: `url(${src})`,
        ...props.style
      }}
      {...props}
    >
      {children}
    </div>
  );
}
