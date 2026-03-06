import React, { useState, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { base44 } from '@/api/base44Client';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Textarea } from '@/components/ui/textarea';
import { Label } from '@/components/ui/label';
import ReactQuill from 'react-quill';
import 'react-quill/dist/quill.snow.css';
import { ArrowLeft, Save, Plus, Trash2 } from 'lucide-react';
import { Link } from 'react-router-dom';
import { createPageUrl } from '@/utils';

export default function AdminBlogEditor() {
  const [selectedPostId, setSelectedPostId] = useState(null);
  const [formData, setFormData] = useState({
    title: '',
    excerpt: '',
    image: '',
    featuredImageAlt: '',
    date: new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }),
    readTime: '',
    author: '',
    category: '',
    content: '',
    h1Tag: '',
    metaTitle: '',
    metaDescription: ''
  });

  const queryClient = useQueryClient();

  const { data: blogPosts = [] } = useQuery({
    queryKey: ['admin-blog-posts'],
    queryFn: () => base44.entities.Blog.list('-date')
  });

  const createMutation = useMutation({
    mutationFn: (data) => base44.entities.Blog.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-blog-posts'] });
      alert('Blog post created successfully!');
      resetForm();
    }
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }) => base44.entities.Blog.update(id, data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-blog-posts'] });
      alert('Blog post updated successfully!');
    }
  });

  const deleteMutation = useMutation({
    mutationFn: (id) => base44.entities.Blog.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-blog-posts'] });
      alert('Blog post deleted successfully!');
      resetForm();
    }
  });

  const resetForm = () => {
    setFormData({
      title: '',
      excerpt: '',
      image: '',
      featuredImageAlt: '',
      date: new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' }),
      readTime: '',
      author: '',
      category: '',
      content: '',
      h1Tag: '',
      metaTitle: '',
      metaDescription: ''
    });
    setSelectedPostId(null);
  };

  const loadPost = (post) => {
    setSelectedPostId(post.id);
    setFormData({
      title: post.title || '',
      excerpt: post.excerpt || '',
      image: post.image || '',
      featuredImageAlt: post.featuredImageAlt || '',
      date: post.date || '',
      readTime: post.readTime || '',
      author: post.author || '',
      category: post.category || '',
      content: post.content || '',
      h1Tag: post.h1Tag || '',
      metaTitle: post.metaTitle || '',
      metaDescription: post.metaDescription || ''
    });
  };

  const handleSave = () => {
    if (selectedPostId) {
      updateMutation.mutate({ id: selectedPostId, data: formData });
    } else {
      createMutation.mutate(formData);
    }
  };

  const handleDelete = () => {
    if (selectedPostId && confirm('Are you sure you want to delete this post?')) {
      deleteMutation.mutate(selectedPostId);
    }
  };

  const modules = {
    toolbar: [
      [{ 'header': [1, 2, 3, 4, 5, 6, false] }],
      ['bold', 'italic', 'underline', 'strike'],
      [{ 'list': 'ordered'}, { 'list': 'bullet' }],
      [{ 'indent': '-1'}, { 'indent': '+1' }],
      ['link', 'image'],
      [{ 'align': [] }],
      ['blockquote', 'code-block'],
      [{ 'color': [] }, { 'background': [] }],
      ['clean']
    ]
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto p-6">
        {/* Header */}
        <div className="mb-6 flex items-center justify-between">
          <div className="flex items-center gap-4">
            <Link to={createPageUrl('Blog')}>
              <Button variant="outline" size="icon">
                <ArrowLeft className="w-4 h-4" />
              </Button>
            </Link>
            <h1 className="text-3xl font-bold text-gray-800">Blog Editor</h1>
          </div>
          <div className="flex gap-2">
            <Button variant="outline" onClick={resetForm}>
              <Plus className="w-4 h-4 mr-2" />
              New Post
            </Button>
            {selectedPostId && (
              <Button variant="destructive" onClick={handleDelete} disabled={deleteMutation.isPending}>
                <Trash2 className="w-4 h-4 mr-2" />
                Delete
              </Button>
            )}
            <Button onClick={handleSave} disabled={createMutation.isPending || updateMutation.isPending}>
              <Save className="w-4 h-4 mr-2" />
              {selectedPostId ? 'Update' : 'Create'}
            </Button>
          </div>
        </div>

        <div className="grid lg:grid-cols-[300px_1fr] gap-6">
          {/* Sidebar - Post List */}
          <div className="bg-white rounded-lg shadow p-4 h-fit">
            <h2 className="font-semibold mb-4 text-gray-700">All Posts</h2>
            <div className="space-y-2">
              {blogPosts.map((post) => (
                <button
                  key={post.id}
                  onClick={() => loadPost(post)}
                  className={`w-full text-left p-3 rounded-lg transition-colors ${
                    selectedPostId === post.id
                      ? 'bg-orange-50 border-2 border-orange-500'
                      : 'bg-gray-50 hover:bg-gray-100'
                  }`}
                >
                  <div className="font-medium text-sm text-gray-800 line-clamp-2">
                    {post.title}
                  </div>
                  <div className="text-xs text-gray-500 mt-1">{post.date}</div>
                </button>
              ))}
            </div>
          </div>

          {/* Editor Form */}
          <div className="bg-white rounded-lg shadow p-6 space-y-6">
            {/* Basic Info */}
            <div className="space-y-4">
              <div>
                <Label>Title *</Label>
                <Input
                  value={formData.title}
                  onChange={(e) => setFormData({...formData, title: e.target.value})}
                  placeholder="Blog post title"
                />
              </div>

              <div>
                <Label>Excerpt *</Label>
                <Textarea
                  value={formData.excerpt}
                  onChange={(e) => setFormData({...formData, excerpt: e.target.value})}
                  placeholder="Short summary"
                  rows={3}
                />
              </div>

              <div className="grid md:grid-cols-2 gap-4">
                <div>
                  <Label>Featured Image URL *</Label>
                  <Input
                    value={formData.image}
                    onChange={(e) => setFormData({...formData, image: e.target.value})}
                    placeholder="https://..."
                  />
                </div>
                <div>
                  <Label>Image Alt Text</Label>
                  <Input
                    value={formData.featuredImageAlt}
                    onChange={(e) => setFormData({...formData, featuredImageAlt: e.target.value})}
                    placeholder="Describe the image"
                  />
                </div>
              </div>

              <div className="grid md:grid-cols-2 gap-4">
                <div>
                  <Label>Author *</Label>
                  <Input
                    value={formData.author}
                    onChange={(e) => setFormData({...formData, author: e.target.value})}
                    placeholder="Author name"
                  />
                </div>
                <div>
                  <Label>Category *</Label>
                  <Input
                    value={formData.category}
                    onChange={(e) => setFormData({...formData, category: e.target.value})}
                    placeholder="Market Insights, Buying Guide, etc."
                  />
                </div>
              </div>

              <div className="grid md:grid-cols-2 gap-4">
                <div>
                  <Label>Date *</Label>
                  <Input
                    value={formData.date}
                    onChange={(e) => setFormData({...formData, date: e.target.value})}
                    placeholder="Feb 4, 2026"
                  />
                </div>
                <div>
                  <Label>Read Time *</Label>
                  <Input
                    value={formData.readTime}
                    onChange={(e) => setFormData({...formData, readTime: e.target.value})}
                    placeholder="5 min read"
                  />
                </div>
              </div>
            </div>

            {/* Rich Text Editor */}
            <div>
              <Label>Content *</Label>
              <div className="mt-2 border rounded-lg overflow-hidden">
                <ReactQuill
                  theme="snow"
                  value={formData.content}
                  onChange={(value) => setFormData({...formData, content: value})}
                  modules={modules}
                  placeholder="Paste your content from Google Docs here..."
                  style={{ minHeight: '400px' }}
                />
              </div>
              <p className="text-xs text-gray-500 mt-2">
                Paste content from Google Docs (tables & images preserved). Use the image button to add image URLs, or edit the HTML directly to add alt text to images.
              </p>
            </div>

            {/* SEO Fields */}
            <div className="space-y-4 border-t pt-6">
              <h3 className="font-semibold text-gray-700">SEO Settings</h3>
              
              <div>
                <Label>H1 Tag</Label>
                <Input
                  value={formData.h1Tag}
                  onChange={(e) => setFormData({...formData, h1Tag: e.target.value})}
                  placeholder="Main heading for SEO"
                />
              </div>

              <div>
                <Label>Meta Title</Label>
                <Input
                  value={formData.metaTitle}
                  onChange={(e) => setFormData({...formData, metaTitle: e.target.value})}
                  placeholder="SEO title (60 chars max)"
                />
              </div>

              <div>
                <Label>Meta Description</Label>
                <Textarea
                  value={formData.metaDescription}
                  onChange={(e) => setFormData({...formData, metaDescription: e.target.value})}
                  placeholder="SEO description (160 chars max)"
                  rows={3}
                />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}