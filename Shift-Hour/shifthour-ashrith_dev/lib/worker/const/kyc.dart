import 'dart:io';
import 'package:flutter/material.dart';
import 'package:image_picker/image_picker.dart';
import 'package:supabase_flutter/supabase_flutter.dart';

class StandaloneVerificationForm extends StatefulWidget {
  final VoidCallback? onComplete;

  const StandaloneVerificationForm({Key? key, this.onComplete})
    : super(key: key);

  @override
  State<StandaloneVerificationForm> createState() =>
      _StandaloneVerificationFormState();
}

class _StandaloneVerificationFormState
    extends State<StandaloneVerificationForm> {
  final _formKey = GlobalKey<FormState>();
  final TextEditingController _aadharController = TextEditingController();

  File? _aadharPhoto;
  File? _photoImage;

  bool _isUploading = false;
  String _errorMessage = '';

  final ImagePicker _picker = ImagePicker();

  @override
  void dispose() {
    _aadharController.dispose();
    super.dispose();
  }

  void _showImageSourceDialog(String photoType) {
    showDialog(
      context: context,
      builder: (BuildContext context) {
        return AlertDialog(
          title: Text("Select Image Source"),
          content: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              ListTile(
                leading: Icon(Icons.photo_camera),
                title: Text("Take a photo"),
                onTap: () {
                  Navigator.pop(context);
                  _pickImageDirectly(ImageSource.camera, photoType);
                },
              ),
              ListTile(
                leading: Icon(Icons.photo_library),
                title: Text("Choose from gallery"),
                onTap: () {
                  Navigator.pop(context);
                  _pickImageDirectly(ImageSource.gallery, photoType);
                },
              ),
            ],
          ),
        );
      },
    );
  }

  Future<void> _pickImageDirectly(ImageSource source, String photoType) async {
    try {
      // Show loading dialog
      showDialog(
        context: context,
        barrierDismissible: false,
        builder: (BuildContext context) {
          return AlertDialog(
            content: Column(
              mainAxisSize: MainAxisSize.min,
              children: [
                CircularProgressIndicator(),
                SizedBox(height: 16),
                Text(
                  source == ImageSource.camera
                      ? "Taking photo..."
                      : "Selecting from gallery...",
                ),
              ],
            ),
          );
        },
      );

      final XFile? pickedFile = await _picker.pickImage(
        source: source,
        maxWidth: 1200,
        maxHeight: 1200,
        imageQuality: 85,
      );

      // Close the loading dialog
      if (mounted) Navigator.of(context).pop();

      if (pickedFile != null && mounted) {
        setState(() {
          switch (photoType) {
            case 'aadhar':
              _aadharPhoto = File(pickedFile.path);
              break;
            case 'photo':
              _photoImage = File(pickedFile.path);
              break;
          }
        });
      }
    } catch (e) {
      // Close the loading dialog if there's an error
      if (mounted) Navigator.of(context).pop();

      setState(() {
        _errorMessage = 'Error picking image: $e';
      });
      print('Error picking image: $e');
    }
  }

  bool _validateFields() {
    // Check if Aadhar number is provided
    if (_aadharController.text.trim().isEmpty) {
      setState(() {
        _errorMessage = 'Please enter your Aadhar number';
      });
      return false;
    }

    // Check if Aadhar photo is uploaded
    if (_aadharPhoto == null) {
      setState(() {
        _errorMessage = 'Please upload your Aadhar card photo';
      });
      return false;
    }

    // Check if photo image is uploaded
    if (_photoImage == null) {
      setState(() {
        _errorMessage = 'Please upload your photo';
      });
      return false;
    }

    // Clear any previous error message if validation passes
    setState(() {
      _errorMessage = '';
    });
    return true;
  }

  Future<void> _uploadDocuments() async {
    // Validate all fields first
    if (!_validateFields()) {
      // Scroll to the error message
      Future.delayed(Duration(milliseconds: 100), () {
        if (mounted) {
          Scrollable.ensureVisible(
            _formKey.currentContext!,
            alignment: 0.5,
            duration: Duration(milliseconds: 300),
          );
        }
      });
      return;
    }

    try {
      setState(() {
        _isUploading = true;
      });

      final user = Supabase.instance.client.auth.currentUser;
      print('Current User: ${user?.id}'); // Debug print
      if (user == null) {
        throw Exception('No authenticated user found');
      }

      // Get user's email
      final userEmail = user.email;
      print('User Email: $userEmail');

      // Generate unique file names for uploads
      final aadharPhotoName =
          'aadhar_${user.id}_${DateTime.now().millisecondsSinceEpoch}.jpg';
      final photoImageName =
          'photo_${user.id}_${DateTime.now().millisecondsSinceEpoch}.jpg';

      // Upload Aadhar photo
      final aadharPhotoPath = await _uploadFile(
        _aadharPhoto!,
        'documents',
        aadharPhotoName,
      );

      // Upload photo image
      final photoImagePath = await _uploadFile(
        _photoImage!,
        'documents',
        photoImageName,
      );

      // First check if a record exists
      final existingRecord =
          await Supabase.instance.client
              .from('documents')
              .select()
              .eq('email', userEmail!)
              .maybeSingle();

      // Then either update or insert
      if (existingRecord != null) {
        // Update existing record
        await Supabase.instance.client
            .from('documents')
            .update({
              'aadhar_number': _aadharController.text,
              'aadhar_image_url': aadharPhotoPath,
              'photo_image_url': photoImagePath,
              'status': 'Inprogress',
              'updated_at': DateTime.now().toIso8601String(),
            })
            .eq('email', userEmail);
      } else {
        // Insert new record
        await Supabase.instance.client.from('documents').insert({
          'user_id': user.id,
          'email': userEmail,
          'aadhar_number': _aadharController.text,
          'aadhar_image_url': aadharPhotoPath,
          'photo_image_url': photoImagePath,
          'status': 'Inprogress',
          'created_at': DateTime.now().toIso8601String(),
          'updated_at': DateTime.now().toIso8601String(),
        });
      }

      // Show success message and close the form
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          const SnackBar(
            content: Text('Verification documents uploaded successfully!'),
          ),
        );

        // Call onComplete callback if provided
        if (widget.onComplete != null) {
          widget.onComplete!();
        }

        _closeForm();
      }
    } catch (e, stackTrace) {
      // Log the detailed error for developers
      print('Database Error: $e');
      print('Stack Trace: $stackTrace');

      if (mounted) {
        // Show a user-friendly message
        setState(() {
          _isUploading = false;
          _errorMessage =
              'We couldn\'t save your information. Please try again later.';
        });

        // Show a visual notification
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(
            content: Text('Verification didn\'t complete. Please try again.'),
            backgroundColor: Colors.red,
            duration: Duration(seconds: 3),
          ),
        );
      }
    }
  }

  Future<String> _uploadFile(File file, String bucket, String fileName) async {
    try {
      final bytes = await file.readAsBytes();

      // Upload binary data
      final uploadResponse = await Supabase.instance.client.storage
          .from(bucket)
          .uploadBinary(
            fileName,
            bytes,
            fileOptions: FileOptions(upsert: true),
          );

      print('Upload Response: $uploadResponse'); // Debug print

      // Get the public URL
      final publicUrl = Supabase.instance.client.storage
          .from(bucket)
          .getPublicUrl(fileName);

      print('Public URL: $publicUrl'); // Debug print

      return publicUrl;
    } catch (e) {
      print('File Upload Error: $e');
      rethrow;
    }
  }

  void _closeForm() {
    Navigator.of(context).pop();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        backgroundColor: Colors.indigo,
        foregroundColor: Colors.white,
        title: const Text('KYC Verification'),
        elevation: 0,
        leading: IconButton(
          icon: Icon(Icons.arrow_back),
          onPressed: _closeForm,
        ),
      ),
      body: Form(
        key: _formKey,
        child: ListView(
          padding: const EdgeInsets.all(16),
          children: [
            // Info text
            Container(
              padding: const EdgeInsets.all(16),
              decoration: BoxDecoration(
                color: Colors.blue.shade50,
                borderRadius: BorderRadius.circular(12),
                border: Border.all(color: Colors.blue.shade200),
              ),
              child: Column(
                crossAxisAlignment: CrossAxisAlignment.start,
                children: [
                  Row(
                    children: [
                      Icon(Icons.info_outline, color: Colors.blue.shade700),
                      const SizedBox(width: 8),
                      Text(
                        'KYC Verification',
                        style: TextStyle(
                          fontWeight: FontWeight.bold,
                          color: Colors.blue.shade700,
                          fontSize: 16,
                        ),
                      ),
                    ],
                  ),
                  const SizedBox(height: 8),
                  Text(
                    'Please provide your Aadhar number and upload required documents to verify your identity.',
                    style: TextStyle(color: Colors.blue.shade700),
                  ),
                ],
              ),
            ),

            const SizedBox(height: 24),

            // Aadhar Number field
            TextFormField(
              controller: _aadharController,
              keyboardType: TextInputType.number,
              maxLength: 12,
              decoration: InputDecoration(
                labelText: 'Aadhar Number',
                border: OutlineInputBorder(
                  borderRadius: BorderRadius.circular(12),
                ),
                prefixIcon: const Icon(Icons.credit_card),
                counterText: '', // Hide character counter
              ),
              validator: (value) {
                if (value == null || value.isEmpty) {
                  return 'Please enter your Aadhar number';
                }
                if (value.length != 12) {
                  return 'Aadhar number must be 12 digits';
                }
                return null;
              },
            ),

            const SizedBox(height: 24),

            // Aadhar Photo upload
            Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Aadhar Card Photo',
                  style: TextStyle(fontWeight: FontWeight.bold, fontSize: 16),
                ),
                const SizedBox(height: 8),
                GestureDetector(
                  onTap: () => _showImageSourceDialog('aadhar'),
                  child: Container(
                    height: 150,
                    decoration: BoxDecoration(
                      color: Colors.grey.shade100,
                      borderRadius: BorderRadius.circular(12),
                      border: Border.all(
                        color:
                            _errorMessage.contains('Aadhar card photo')
                                ? Colors.red.shade300
                                : Colors.grey.shade300,
                      ),
                    ),
                    child:
                        _aadharPhoto != null
                            ? ClipRRect(
                              borderRadius: BorderRadius.circular(12),
                              child: Image.file(
                                _aadharPhoto!,
                                fit: BoxFit.cover,
                                width: double.infinity,
                              ),
                            )
                            : Center(
                              child: Column(
                                mainAxisAlignment: MainAxisAlignment.center,
                                children: [
                                  Icon(
                                    Icons.upload_file,
                                    size: 40,
                                    color:
                                        _errorMessage.contains(
                                              'Aadhar card photo',
                                            )
                                            ? Colors.red.shade300
                                            : Colors.grey.shade400,
                                  ),
                                  const SizedBox(height: 8),
                                  Text(
                                    'Upload Aadhar Card Photo',
                                    style: TextStyle(
                                      color:
                                          _errorMessage.contains(
                                                'Aadhar card photo',
                                              )
                                              ? Colors.red.shade500
                                              : Colors.grey.shade600,
                                    ),
                                  ),
                                ],
                              ),
                            ),
                  ),
                ),
              ],
            ),

            const SizedBox(height: 24),

            // Photo Image upload
            Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                const Text(
                  'Your Photo',
                  style: TextStyle(fontWeight: FontWeight.bold, fontSize: 16),
                ),
                const SizedBox(height: 8),
                GestureDetector(
                  onTap: () => _showImageSourceDialog('photo'),
                  child: Container(
                    height: 150,
                    decoration: BoxDecoration(
                      color: Colors.grey.shade100,
                      borderRadius: BorderRadius.circular(12),
                      border: Border.all(
                        color:
                            _errorMessage.contains('your photo')
                                ? Colors.red.shade300
                                : Colors.grey.shade300,
                      ),
                    ),
                    child:
                        _photoImage != null
                            ? ClipRRect(
                              borderRadius: BorderRadius.circular(12),
                              child: Image.file(
                                _photoImage!,
                                fit: BoxFit.cover,
                                width: double.infinity,
                              ),
                            )
                            : Center(
                              child: Column(
                                mainAxisAlignment: MainAxisAlignment.center,
                                children: [
                                  Icon(
                                    Icons.person,
                                    size: 40,
                                    color:
                                        _errorMessage.contains('your photo')
                                            ? Colors.red.shade300
                                            : Colors.grey.shade400,
                                  ),
                                  const SizedBox(height: 8),
                                  Text(
                                    'Upload Your Photo',
                                    style: TextStyle(
                                      color:
                                          _errorMessage.contains('your photo')
                                              ? Colors.red.shade500
                                              : Colors.grey.shade600,
                                    ),
                                  ),
                                ],
                              ),
                            ),
                  ),
                ),
              ],
            ),

            // Error message
            if (_errorMessage.isNotEmpty) ...[
              const SizedBox(height: 16),
              Container(
                padding: const EdgeInsets.all(12),
                decoration: BoxDecoration(
                  color: Colors.red.shade50,
                  borderRadius: BorderRadius.circular(8),
                  border: Border.all(color: Colors.red.shade200),
                ),
                child: Text(
                  _errorMessage,
                  style: TextStyle(color: Colors.red.shade800),
                ),
              ),
            ],

            const SizedBox(height: 32),

            // Submit button
            ElevatedButton(
              onPressed: _isUploading ? null : _uploadDocuments,
              style: ElevatedButton.styleFrom(
                backgroundColor: Colors.blue.shade600,
                foregroundColor: Colors.white,
                padding: const EdgeInsets.symmetric(vertical: 16),
                shape: RoundedRectangleBorder(
                  borderRadius: BorderRadius.circular(12),
                ),
                elevation: 2,
              ),
              child:
                  _isUploading
                      ? const Row(
                        mainAxisAlignment: MainAxisAlignment.center,
                        children: [
                          SizedBox(
                            width: 20,
                            height: 20,
                            child: CircularProgressIndicator(
                              color: Colors.white,
                              strokeWidth: 2,
                            ),
                          ),
                          SizedBox(width: 12),
                          Text('Uploading...'),
                        ],
                      )
                      : const Text('Submit Documents'),
            ),
          ],
        ),
      ),
    );
  }
}
