import React, { useState, useRef } from 'react';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Trash2, Plus } from 'lucide-react';
import { toast } from 'sonner';

export default function ImageManager({ images, onUpdate, category = '' }) {
  const [newImageUrl, setNewImageUrl] = useState('');
  const [newImageTitle, setNewImageTitle] = useState('');

  const handleAddImage = () => {
    if (!newImageUrl.trim()) {
      toast.error('Please enter an image URL');
      return;
    }
    onUpdate([...images, {
      url: newImageUrl,
      title: newImageTitle || 'New Image',
      category: category
    }]);
    setNewImageUrl('');
    setNewImageTitle('');
    toast.success('Image added');
  };

  const handleDeleteImage = (index) => {
    onUpdate(images.filter((_, i) => i !== index));
    toast.success('Image removed');
  };

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        <Input placeholder="Image URL" value={newImageUrl} onChange={(e) => setNewImageUrl(e.target.value)} />
        <Input placeholder="Title (optional)" value={newImageTitle} onChange={(e) => setNewImageTitle(e.target.value)} />
        <Button onClick={handleAddImage}>
          <Plus className="w-4 h-4 mr-2" />Add
        </Button>
      </div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {images.map((image, index) => (
          <div key={index} className="relative group">
            <img src={image.url} alt={image.title || `Image ${index + 1}`} className="w-full h-32 object-cover rounded-lg" />
            <button onClick={() => handleDeleteImage(index)} className="absolute top-2 right-2 bg-red-500 text-white p-2 rounded-full opacity-0 group-hover:opacity-100 transition-opacity">
              <Trash2 className="w-4 h-4" />
            </button>
          </div>
        ))}
      </div>
    </div>
  );
}
