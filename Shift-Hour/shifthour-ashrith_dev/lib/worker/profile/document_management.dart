import 'dart:io';
import 'package:flutter/material.dart';
import 'package:shifthour/worker/worker_dashboard.dart';
import 'package:supabase_flutter/supabase_flutter.dart';
import 'package:file_picker/file_picker.dart';
import 'package:path/path.dart' as path;

class DocumentManagementScreen extends StatefulWidget {
  const DocumentManagementScreen({Key? key}) : super(key: key);

  @override
  State<DocumentManagementScreen> createState() => _DocumentUploadScreenState();
}

class _DocumentUploadScreenState extends State<DocumentManagementScreen> {
  bool _isUploading = false;
  List<Map<String, dynamic>> _documents = [];
  String? _errorMessage;

  @override
  void initState() {
    super.initState();
    _loadDocuments();
  }

  // Add this method to your _DocumentUploadScreenState class
  Future<void> _viewDocument(Map<String, dynamic> document) async {
    final imageUrl = document['image_url'];
    final fileName = document['name'] as String;
    final fileExtension = path.extension(fileName).toLowerCase();

    if (imageUrl == null) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(
          content: Text('Document URL not available'),
          backgroundColor: Colors.red,
        ),
      );
      return;
    }

    // For PDFs, we could integrate a PDF viewer, but for now we'll just show the URL
    if (fileExtension == '.pdf') {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('PDF viewer not implemented. URL: $imageUrl'),
          action: SnackBarAction(label: 'Close', onPressed: () {}),
        ),
      );
      return;
    }

    // For images, show the image in a dialog
    if (fileExtension == '.jpg' ||
        fileExtension == '.jpeg' ||
        fileExtension == '.png') {
      showDialog(
        context: context,
        builder: (BuildContext context) {
          return Dialog(
            backgroundColor: Colors.transparent,
            insetPadding: const EdgeInsets.all(16),
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                // Header with document name and close button
                Container(
                  padding: const EdgeInsets.symmetric(
                    horizontal: 16,
                    vertical: 12,
                  ),
                  decoration: BoxDecoration(
                    color: Colors.white,
                    borderRadius: const BorderRadius.only(
                      topLeft: Radius.circular(12),
                      topRight: Radius.circular(12),
                    ),
                  ),
                  child: Row(
                    mainAxisAlignment: MainAxisAlignment.spaceBetween,
                    children: [
                      Expanded(
                        child: Text(
                          fileName,
                          style: const TextStyle(
                            fontWeight: FontWeight.bold,
                            fontSize: 16,
                          ),
                          overflow: TextOverflow.ellipsis,
                        ),
                      ),
                      IconButton(
                        icon: const Icon(Icons.close),
                        onPressed: () => Navigator.of(context).pop(),
                        padding: EdgeInsets.zero,
                        constraints: const BoxConstraints(),
                      ),
                    ],
                  ),
                ),

                // Document image with loading indicator
                Flexible(
                  child: Container(
                    constraints: BoxConstraints(
                      maxHeight: MediaQuery.of(context).size.height * 0.7,
                    ),
                    decoration: const BoxDecoration(
                      color: Colors.white,
                      borderRadius: BorderRadius.only(
                        bottomLeft: Radius.circular(12),
                        bottomRight: Radius.circular(12),
                      ),
                    ),
                    child: ClipRRect(
                      borderRadius: const BorderRadius.only(
                        bottomLeft: Radius.circular(12),
                        bottomRight: Radius.circular(12),
                      ),
                      child: InteractiveViewer(
                        minScale: 0.5,
                        maxScale: 3.0,
                        child: Image.network(
                          imageUrl,
                          fit: BoxFit.contain,
                          loadingBuilder: (context, child, loadingProgress) {
                            if (loadingProgress == null) return child;
                            return Center(
                              child: CircularProgressIndicator(
                                value:
                                    loadingProgress.expectedTotalBytes != null
                                        ? loadingProgress
                                                .cumulativeBytesLoaded /
                                            loadingProgress.expectedTotalBytes!
                                        : null,
                              ),
                            );
                          },
                          errorBuilder: (context, error, stackTrace) {
                            return Center(
                              child: Column(
                                mainAxisAlignment: MainAxisAlignment.center,
                                children: [
                                  const Icon(
                                    Icons.error_outline,
                                    color: Colors.red,
                                    size: 48,
                                  ),
                                  const SizedBox(height: 16),
                                  Text(
                                    'Failed to load image',
                                    style: TextStyle(
                                      color: Colors.red.shade700,
                                    ),
                                  ),
                                  const SizedBox(height: 8),
                                  TextButton(
                                    onPressed:
                                        () => Navigator.of(context).pop(),
                                    child: const Text('Close'),
                                  ),
                                ],
                              ),
                            );
                          },
                        ),
                      ),
                    ),
                  ),
                ),
              ],
            ),
          );
        },
      );
    }
  }

  void checkAuth() {
    final user = Supabase.instance.client.auth.currentUser;
    print('Current user: ${user?.id}');
    print('Is authenticated: ${user != null}');
  }

  Future<void> _loadDocuments() async {
    try {
      setState(() => _isUploading = true);

      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('User not authenticated');
      }

      // Try to get documents from storage
      final List<FileObject> files = await Supabase.instance.client.storage
          .from('documents')
          .list(path: user.id);

      // Transform files into document objects and ensure they have image_url
      final storageDocuments =
          files.map((file) {
            // Generate public URL for each file
            final imageUrl = Supabase.instance.client.storage
                .from('documents')
                .getPublicUrl('${user.id}/${file.name}');

            return {
              'name': file.name,
              'created_at': file.createdAt,
              'size': file.metadata?['size'] ?? 'Unknown size',
              'path': '${user.id}/${file.name}',
              'image_url': imageUrl, // Add URL directly from storage
            };
          }).toList();

      // Also try to get documents from the database
      try {
        final response = await Supabase.instance.client
            .from('documents')
            .select()
            .eq('user_id', user.id);

        // Merge database records with storage files, using filename as key
        final databaseDocuments =
            (response as List<dynamic>).map((record) {
              final String name = record['name'] ?? 'Unknown';
              final String path = '${user.id}/$name';

              // Ensure image_url exists - if not in database, generate it
              String imageUrl = record['image_url'];
              if (imageUrl == null || imageUrl.isEmpty) {
                imageUrl = Supabase.instance.client.storage
                    .from('documents')
                    .getPublicUrl(path);
              }

              return {
                'name': name,
                'created_at': record['created_at'],
                'path': path,
                'id': record['id'],
                'image_url': imageUrl,
              };
            }).toList();

        setState(() {
          _documents = databaseDocuments;
          _errorMessage = null;
        });
      } catch (dbError) {
        print('Database error: $dbError - Falling back to storage files');
        setState(() {
          _documents = storageDocuments;
          _errorMessage = null;
        });
      }

      print('Loaded ${_documents.length} documents');
    } catch (e) {
      setState(() {
        _errorMessage = 'Error loading documents: ${e.toString()}';
      });
      print('Error loading documents: $e');
    } finally {
      setState(() => _isUploading = false);
    }
  }

  Future<void> _uploadDocument() async {
    try {
      // Check authentication
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('User not authenticated');
      }

      // Pick file
      FilePickerResult? result = await FilePicker.platform.pickFiles(
        type: FileType.custom,
        allowedExtensions: ['pdf', 'jpg', 'png'],
      );

      if (result == null || result.files.isEmpty) {
        // User canceled the picker
        return;
      }

      final file = result.files.first;
      if (file.path == null) {
        throw Exception('File path is null');
      }

      // Show document type selection dialog
      final documentType = await _showDocumentTypeSelectionDialog();
      if (documentType == null) {
        // User canceled the dialog
        return;
      }

      setState(() => _isUploading = true);

      // Use document type as the filename
      final fileName = '$documentType${path.extension(file.name)}';
      final filePath = '${user.id}/$fileName';

      // Print debug information
      print('File name: $fileName');
      print('User ID: ${user.id}');
      print('Upload path: $filePath');

      // 1. Upload file to storage
      final uploadResult = await Supabase.instance.client.storage
          .from('documents')
          .upload(
            filePath,
            File(file.path!),
            fileOptions: const FileOptions(cacheControl: '3600', upsert: true),
          );

      print('Storage upload successful: $uploadResult');

      // 2. Get the public URL for the file
      final fileUrl = Supabase.instance.client.storage
          .from('documents')
          .getPublicUrl(filePath);

      print('File URL: $fileUrl');

      // 3. Insert record into documents table
      try {
        final response = await Supabase.instance.client
            .from('documents')
            .insert({
              'name': fileName,
              'image_url': fileUrl,
              'user_id': user.id, // Add user_id to the insert
            });

        print('Insert response: $response');
      } catch (dbError) {
        print('Database insert error: $dbError');
        // Continue execution even if database insert fails
      }

      // Refresh document list
      await _loadDocuments();

      // Show success message
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Document uploaded successfully'),
            backgroundColor: Colors.green,
          ),
        );
      }
    } catch (e) {
      setState(() {
        _errorMessage = 'Error uploading document: ${e.toString()}';
      });
      print('Error uploading document: $e');

      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('Error uploading document: ${e.toString()}'),
            backgroundColor: Colors.red.shade700,
          ),
        );
      }
    } finally {
      setState(() => _isUploading = false);
    }
  }

  Future<String?> _showDocumentTypeSelectionDialog() async {
    return showDialog<String>(
      context: context,
      builder: (BuildContext context) {
        final isDarkMode = Theme.of(context).brightness == Brightness.dark;

        return AlertDialog(
          title: const Text('Select Document Type'),
          backgroundColor: isDarkMode ? Colors.grey.shade800 : Colors.white,
          content: SingleChildScrollView(
            child: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                _buildDocumentTypeOption(
                  context,
                  'Driver\'s License',
                  Icons.directions_car,
                ),
                _buildDocumentTypeOption(
                  context,
                  'Birth Certificate',
                  Icons.child_care,
                ),
                _buildDocumentTypeOption(context, 'Passport', Icons.flight),
                _buildDocumentTypeOption(
                  context,
                  'Social Security Card',
                  Icons.security,
                ),
                _buildDocumentTypeOption(
                  context,
                  'Insurance Card',
                  Icons.health_and_safety,
                ),
                _buildDocumentTypeOption(
                  context,
                  'Pan Card',
                  Icons.credit_card,
                ),
                _buildDocumentTypeOption(
                  context,
                  'Voter ID',
                  Icons.how_to_vote,
                ),
                _buildDocumentTypeOption(context, 'Other', Icons.description),
              ],
            ),
          ),
          actions: [
            TextButton(
              onPressed: () => Navigator.of(context).pop(),
              child: const Text('Cancel'),
            ),
          ],
        );
      },
    );
  }

  Widget _buildDocumentTypeOption(
    BuildContext context,
    String type,
    IconData icon,
  ) {
    final isDarkMode = Theme.of(context).brightness == Brightness.dark;

    return InkWell(
      onTap: () => Navigator.of(context).pop(type),
      child: Padding(
        padding: const EdgeInsets.symmetric(vertical: 8.0),
        child: Row(
          children: [
            Container(
              padding: const EdgeInsets.all(8),
              decoration: BoxDecoration(
                color: Colors.blue.shade100,
                borderRadius: BorderRadius.circular(8),
              ),
              child: Icon(icon, color: Colors.blue.shade800),
            ),
            const SizedBox(width: 16),
            Text(
              type,
              style: TextStyle(
                fontSize: 16,
                color: isDarkMode ? Colors.white : Colors.black,
              ),
            ),
          ],
        ),
      ),
    );
  }

  Future<void> _deleteDocument(String filePath, {String? recordId}) async {
    try {
      final user = Supabase.instance.client.auth.currentUser;
      if (user == null) {
        throw Exception('User not authenticated');
      }

      setState(() => _isUploading = true);

      // 1. Delete file from storage
      await Supabase.instance.client.storage.from('documents').remove([
        filePath,
      ]);
      print('File deleted from storage: $filePath');

      // 2. Delete record from database if ID is provided
      if (recordId != null) {
        try {
          final response = await Supabase.instance.client
              .from('documents')
              .delete()
              .eq('id', recordId)
              .eq('user_id', user.id); // Add user_id filter for security

          print('Database delete response: $response');
        } catch (dbError) {
          print('Error deleting from database by ID: $dbError');
        }
      } else {
        // If no record ID provided, try to find by filename
        final fileName = filePath.split('/').last;
        try {
          final response = await Supabase.instance.client
              .from('documents')
              .delete()
              .eq('name', fileName)
              .eq('user_id', user.id); // Add user_id filter for security

          print('Database delete by name response: $response');
        } catch (dbError) {
          print('Error deleting from database by name: $dbError');
        }
      }

      await _loadDocuments();

      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Document deleted successfully'),
            backgroundColor: Colors.green,
          ),
        );
      }
    } catch (e) {
      print('Error deleting document: $e');

      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('Error deleting document: ${e.toString()}'),
            backgroundColor: Colors.red.shade700,
          ),
        );
      }
    } finally {
      setState(() => _isUploading = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    final isDarkMode = Theme.of(context).brightness == Brightness.dark;

    return Scaffold(
      backgroundColor: isDarkMode ? Colors.grey.shade900 : Colors.grey.shade50,
      appBar: AppBar(
        backgroundColor: isDarkMode ? Colors.grey.shade800 : Colors.white,
        elevation: 0,
        leading: IconButton(
          icon: const Icon(Icons.arrow_back, color: Colors.black12),
          onPressed: () {
            Navigator.pushReplacement(
              context,
              MaterialPageRoute(builder: (context) => WorkerDashboard()),
            );
          },
        ),
        title: Row(
          mainAxisSize: MainAxisSize.min,
          children: [
            Container(
              height: 32,
              width: 32,
              decoration: BoxDecoration(
                color: Colors.blue.shade600.withOpacity(0.1),
                borderRadius: BorderRadius.circular(16),
              ),
              child: Icon(
                Icons.description,
                size: 16,
                color: Colors.blue.shade600,
              ),
            ),
            const SizedBox(width: 8),
            Text(
              'Document Management',
              style: TextStyle(
                fontWeight: FontWeight.bold,
                color: isDarkMode ? Colors.white : Colors.black,
              ),
            ),
          ],
        ),
        centerTitle: true,
        shadowColor: Colors.black.withOpacity(0.05),
      ),
      body: SafeArea(
        child:
            _isUploading
                ? const Center(child: CircularProgressIndicator())
                : Column(
                  children: [
                    // Upload section
                    Padding(
                      padding: const EdgeInsets.all(16.0),
                      child: Column(
                        crossAxisAlignment: CrossAxisAlignment.start,
                        children: [
                          Text(
                            'Upload New Document',
                            style: TextStyle(
                              fontSize: 18,
                              fontWeight: FontWeight.bold,
                              color: isDarkMode ? Colors.white : Colors.black,
                            ),
                          ),
                          const SizedBox(height: 16),
                          InkWell(
                            onTap: _uploadDocument,
                            child: Container(
                              height: 150,
                              decoration: BoxDecoration(
                                color:
                                    isDarkMode
                                        ? Colors.grey.shade800
                                        : Colors.white,
                                borderRadius: BorderRadius.circular(12),
                                border: Border.all(
                                  color: Colors.blue.shade200,
                                  width: 1,
                                  style: BorderStyle.solid,
                                ),
                              ),
                              child: Center(
                                child: Column(
                                  mainAxisAlignment: MainAxisAlignment.center,
                                  children: [
                                    Icon(
                                      Icons.cloud_upload,
                                      size: 48,
                                      color: Colors.blue.shade400,
                                    ),
                                    const SizedBox(height: 8),
                                    Text(
                                      'Click to upload or drag and drop',
                                      style: TextStyle(
                                        color:
                                            isDarkMode
                                                ? Colors.white
                                                : Colors.black87,
                                        fontWeight: FontWeight.w500,
                                      ),
                                    ),
                                    const SizedBox(height: 8),
                                    Text(
                                      'Supported formats: PDF, JPG, PNG',
                                      style: TextStyle(
                                        fontSize: 12,
                                        color:
                                            isDarkMode
                                                ? Colors.grey.shade400
                                                : Colors.grey.shade600,
                                      ),
                                    ),
                                  ],
                                ),
                              ),
                            ),
                          ),
                          if (_errorMessage != null) ...[
                            const SizedBox(height: 16),
                            Container(
                              padding: const EdgeInsets.all(12),
                              decoration: BoxDecoration(
                                color: Colors.red.shade100,
                                borderRadius: BorderRadius.circular(8),
                                border: Border.all(color: Colors.red.shade300),
                              ),
                              child: Row(
                                children: [
                                  Icon(
                                    Icons.error_outline,
                                    color: Colors.red.shade700,
                                  ),
                                  const SizedBox(width: 8),
                                  Expanded(
                                    child: Text(
                                      _errorMessage!,
                                      style: TextStyle(
                                        color: Colors.red.shade700,
                                      ),
                                    ),
                                  ),
                                ],
                              ),
                            ),
                          ],
                        ],
                      ),
                    ),

                    // Documents list
                    Padding(
                      padding: const EdgeInsets.symmetric(horizontal: 16.0),
                      child: Row(
                        children: [
                          Text(
                            'Your Documents',
                            style: TextStyle(
                              fontSize: 18,
                              fontWeight: FontWeight.bold,
                              color: isDarkMode ? Colors.white : Colors.black,
                            ),
                          ),
                          const SizedBox(width: 8),
                          Container(
                            padding: const EdgeInsets.symmetric(
                              horizontal: 8,
                              vertical: 2,
                            ),
                            decoration: BoxDecoration(
                              color: Colors.blue.shade100,
                              borderRadius: BorderRadius.circular(12),
                            ),
                            child: Text(
                              '${_documents.length}',
                              style: TextStyle(
                                fontSize: 12,
                                fontWeight: FontWeight.bold,
                                color: Colors.blue.shade800,
                              ),
                            ),
                          ),
                          const Spacer(),
                          IconButton(
                            icon: Icon(
                              Icons.refresh,
                              color: Colors.blue.shade600,
                            ),
                            onPressed: _loadDocuments,
                          ),
                        ],
                      ),
                    ),

                    Expanded(
                      child:
                          _documents.isEmpty
                              ? Center(
                                child: Text(
                                  'No documents uploaded yet',
                                  style: TextStyle(
                                    color:
                                        isDarkMode
                                            ? Colors.grey.shade400
                                            : Colors.grey.shade600,
                                  ),
                                ),
                              )
                              : ListView.builder(
                                padding: const EdgeInsets.all(16),
                                itemCount: _documents.length,
                                itemBuilder: (context, index) {
                                  final document = _documents[index];
                                  final fileName = document['name'] as String;
                                  final fileExtension =
                                      path.extension(fileName).toLowerCase();

                                  IconData fileIcon;
                                  Color iconColor;

                                  if (fileExtension == '.pdf') {
                                    fileIcon = Icons.picture_as_pdf;
                                    iconColor = Colors.red.shade400;
                                  } else if (fileExtension == '.jpg' ||
                                      fileExtension == '.jpeg' ||
                                      fileExtension == '.png') {
                                    fileIcon = Icons.image;
                                    iconColor = Colors.green.shade400;
                                  } else {
                                    fileIcon = Icons.insert_drive_file;
                                    iconColor = Colors.blue.shade400;
                                  }

                                  return Card(
                                    margin: const EdgeInsets.only(bottom: 12),
                                    shape: RoundedRectangleBorder(
                                      borderRadius: BorderRadius.circular(12),
                                    ),
                                    elevation: 2,
                                    color:
                                        isDarkMode
                                            ? Colors.grey.shade800
                                            : Colors.white,
                                    child: ListTile(
                                      leading: Container(
                                        width: 40,
                                        height: 40,
                                        decoration: BoxDecoration(
                                          color: iconColor.withOpacity(0.1),
                                          borderRadius: BorderRadius.circular(
                                            8,
                                          ),
                                        ),
                                        child: Icon(
                                          fileIcon,
                                          color: iconColor,
                                          size: 24,
                                        ),
                                      ),
                                      title: Text(
                                        fileName,
                                        style: const TextStyle(
                                          fontWeight: FontWeight.w500,
                                        ),
                                      ),
                                      subtitle: Text(
                                        'Uploaded: ${document['created_at'] ?? 'Unknown date'}',
                                        style: TextStyle(
                                          fontSize: 12,
                                          color:
                                              isDarkMode
                                                  ? Colors.grey.shade400
                                                  : Colors.grey.shade600,
                                        ),
                                      ),
                                      trailing: IconButton(
                                        icon: Icon(
                                          Icons.delete_outline,
                                          color: Colors.red.shade400,
                                        ),
                                        onPressed:
                                            () => _deleteDocument(
                                              document['path'],
                                              recordId: document['id'],
                                            ),
                                      ),
                                      onTap: () {
                                        _viewDocument(document);
                                        // View document
                                      },
                                    ),
                                  );
                                },
                              ),
                    ),
                  ],
                ),
      ),
    );
  }
}
