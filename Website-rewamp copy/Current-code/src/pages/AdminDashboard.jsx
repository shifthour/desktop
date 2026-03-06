import React, { useState, useEffect } from 'react';
import { base44 } from '@/api/base44Client';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import { Trash2, Plus, FileEdit } from 'lucide-react';
import Navbar from '@/components/Navbar';
import Footer from '@/components/home/Footer';
import RichTextEditor from '@/components/blog/RichTextEditor';
import { toast, Toaster } from 'sonner';
import { useLocation } from 'react-router-dom';

export default function AdminDashboard() {
  const [user, setUser] = useState(null);
  const [selectedPost, setSelectedPost] = useState(null);
  const [formData, setFormData] = useState({
    title: '',
    excerpt: '',
    image: '',
    featuredImageAlt: '',
    date: new Date().toISOString().split('T')[0],
    readTime: '',
    author: '',
    category: '',
    content: '',
    h1Tag: '',
    metaTitle: '',
    metaDescription: ''
  });

  const queryClient = useQueryClient();
  const location = useLocation();

  useEffect(() => {
    base44.auth.me().then(u => {
      setUser(u);
      if (u?.role !== 'admin') {
        window.location.href = '/';
      }
    }).catch(() => window.location.href = '/');
  }, []);

  const { data: posts = [] } = useQuery({
    queryKey: ['admin-blog-posts'],
    queryFn: () => base44.entities.Blog.list('-updated_date'),
    enabled: !!user,
  });

  const loadPost = React.useCallback((post) => {
    setSelectedPost(post);
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
  }, []);

  useEffect(() => {
    const urlParams = new URLSearchParams(window.location.search);
    const editId = urlParams.get('edit');
    if (editId) {
      // Check if post data is passed via location state (faster)
      if (location.state?.postData) {
        loadPost(location.state.postData);
      } else if (posts && posts.length > 0) {
        // Fallback to finding from posts list
        const postToEdit = posts.find(p => p.id === editId);
        if (postToEdit) {
          loadPost(postToEdit);
        }
      }
    }
  }, [posts, loadPost, location.state]);

  const createMutation = useMutation({
    mutationFn: (data) => base44.entities.Blog.create(data),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-blog-posts'] });
      toast.success('Post created successfully!');
      resetForm();
    },
  });

  const updateMutation = useMutation({
    mutationFn: ({ id, data }) => base44.entities.Blog.update(id, data),
    onSuccess: (updatedPost) => {
      queryClient.invalidateQueries({ queryKey: ['admin-blog-posts'] });
      toast.success('Post updated successfully!');
      // Keep the form in edit mode with updated data
      setSelectedPost(updatedPost);
      setFormData({
        title: updatedPost.title || '',
        excerpt: updatedPost.excerpt || '',
        image: updatedPost.image || '',
        featuredImageAlt: updatedPost.featuredImageAlt || '',
        date: updatedPost.date || '',
        readTime: updatedPost.readTime || '',
        author: updatedPost.author || '',
        category: updatedPost.category || '',
        content: updatedPost.content || '',
        h1Tag: updatedPost.h1Tag || '',
        metaTitle: updatedPost.metaTitle || '',
        metaDescription: updatedPost.metaDescription || ''
      });
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (id) => base44.entities.Blog.delete(id),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin-blog-posts'] });
      toast.success('Post deleted successfully!');
      if (selectedPost) resetForm();
    },
  });

  const resetForm = () => {
    setSelectedPost(null);
    setFormData({
      title: '',
      excerpt: '',
      image: '',
      featuredImageAlt: '',
      date: new Date().toISOString().split('T')[0],
      readTime: '',
      author: '',
      category: '',
      content: '',
      h1Tag: '',
      metaTitle: '',
      metaDescription: ''
    });
  };

  const handleSave = () => {
    if (selectedPost) {
      updateMutation.mutate({ id: selectedPost.id, data: formData });
    } else {
      createMutation.mutate(formData);
    }
  };

  const handleDelete = (post) => {
    if (window.confirm(`Delete "${post.title}"?`)) {
      deleteMutation.mutate(post.id);
    }
  };



  if (!user) return null;

  return (
    <div className="bg-gray-50 min-h-screen pt-24">
      <Toaster position="top-right" richColors />
      <Navbar />
      
      <div className="max-w-7xl mx-auto px-4 py-8">
        <div className="flex justify-between items-center mb-8">
          <h1 className="text-3xl font-bold text-gray-900">Blog Admin Dashboard</h1>
          <Button onClick={resetForm} className="bg-orange-500 hover:bg-orange-600">
            <Plus className="w-4 h-4 mr-2" />
            New Post
          </Button>
        </div>

        <div className="grid lg:grid-cols-4 gap-6">
          {/* Posts List */}
          <div className="lg:col-span-1 bg-white rounded-lg shadow p-4 max-h-[calc(100vh-200px)] overflow-y-auto">
            <h2 className="font-semibold text-gray-800 mb-4">All Posts</h2>
            <div className="space-y-2">
              {posts.map((post) => (
                <div
                  key={post.id}
                  className={`p-3 rounded border cursor-pointer hover:bg-gray-50 ${
                    selectedPost?.id === post.id ? 'bg-orange-50 border-orange-300' : 'border-gray-200'
                  }`}
                >
                  <div className="flex items-start justify-between gap-2">
                    <div className="flex-1 min-w-0" onClick={() => loadPost(post)}>
                      <p className="text-sm font-medium text-gray-900 truncate">{post.title}</p>
                      <p className="text-xs text-gray-500">{post.date}</p>
                    </div>
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDelete(post);
                      }}
                      className="text-red-500 hover:text-red-700 hover:bg-red-50 p-1 h-auto"
                    >
                      <Trash2 className="w-4 h-4" />
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          </div>

          {/* Editor Form */}
          <div className="lg:col-span-3 bg-white rounded-lg shadow p-6">
            <>
                <h2 className="text-xl font-semibold mb-6 flex items-center gap-2">
                  <FileEdit className="w-5 h-5" />
                  {selectedPost ? 'Edit Post' : 'Create New Post'}
                </h2>

            <div className="space-y-6">
              {/* Basic Info */}
              <div className="grid md:grid-cols-2 gap-4">
                <div>
                  <Label>Title *</Label>
                  <Input
                    value={formData.title}
                    onChange={(e) => setFormData({...formData, title: e.target.value})}
                    placeholder="Blog post title"
                  />
                </div>
                <div>
                  <Label>Category *</Label>
                  <Input
                    value={formData.category}
                    onChange={(e) => setFormData({...formData, category: e.target.value})}
                    placeholder="e.g., Real Estate, Home Buying"
                  />
                </div>
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
                  <div className="flex gap-2">
                    <Input
                      value={formData.image}
                      onChange={(e) => setFormData({...formData, image: e.target.value})}
                      placeholder="https://..."
                    />
                    <Button
                      type="button"
                      variant="outline"
                      onClick={async () => {
                        const input = document.createElement('input');
                        input.type = 'file';
                        input.accept = 'image/*';
                        input.onchange = async (e) => {
                          const file = e.target.files?.[0];
                          if (!file) return;
                          try {
                            const { file_url } = await base44.integrations.Core.UploadFile({ file });
                            setFormData({...formData, image: file_url});
                          } catch (error) {
                            alert('Upload failed');
                          }
                        };
                        input.click();
                      }}
                    >
                      Upload
                    </Button>
                  </div>
                </div>
                <div>
                  <Label>Featured Image Alt Text</Label>
                  <Input
                    value={formData.featuredImageAlt}
                    onChange={(e) => setFormData({...formData, featuredImageAlt: e.target.value})}
                    placeholder="Describe the image for SEO"
                  />
                </div>
              </div>

              <div className="grid md:grid-cols-3 gap-4">
                <div>
                  <Label>Author *</Label>
                  <Input
                    value={formData.author}
                    onChange={(e) => setFormData({...formData, author: e.target.value})}
                    placeholder="Author name"
                  />
                </div>
                <div>
                  <Label>Date *</Label>
                  <Input
                    type="date"
                    value={formData.date}
                    onChange={(e) => setFormData({...formData, date: e.target.value})}
                  />
                </div>
                <div>
                  <Label>Read Time *</Label>
                  <Input
                    value={formData.readTime}
                    onChange={(e) => setFormData({...formData, readTime: e.target.value})}
                    placeholder="e.g., 5 min read"
                  />
                </div>
              </div>

              {/* Content Editor */}
              <div className="pb-8">
                <Label>Content *</Label>
                <div className="mt-2">
                  <RichTextEditor
                    value={formData.content}
                    onChange={(value) => setFormData({...formData, content: value})}
                    editorKey={selectedPost?.id || 'new'}
                  />
                </div>
                <p className="text-xs text-gray-500 mt-2">
                  Use the toolbar to format text, add images with alt text, and paste tables directly from Google Docs or Excel. Full HTML support enabled.
                </p>
              </div>

              {/* SEO Fields */}
              <div className="pt-6 border-t">
                <h3 className="font-semibold text-gray-800 mb-4">SEO Settings</h3>
                <div className="space-y-4">
                  <div>
                    <Label>H1 Tag</Label>
                    <Input
                      value={formData.h1Tag}
                      onChange={(e) => setFormData({...formData, h1Tag: e.target.value})}
                      placeholder="Main heading (defaults to title)"
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

              {/* Actions */}
              <div className="flex gap-4 pt-4">
                <Button onClick={handleSave} className="bg-orange-500 hover:bg-orange-600">
                  {selectedPost ? 'Update Post' : 'Create Post'}
                </Button>
                <Button variant="outline" onClick={resetForm}>
                  Cancel
                </Button>
              </div>
            </div>
            </>
          </div>
        </div>
      </div>

      <Footer />
    </div>
  );
}