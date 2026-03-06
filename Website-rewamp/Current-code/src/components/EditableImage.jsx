import React from 'react';

export default function EditableImage({
  imageKey,
  src,
  alt,
  className = '',
  ...props
}) {
  return (
    <img
      src={src}
      alt={alt}
      className={className}
      {...props}
    />
  );
}
