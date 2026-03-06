import { Router } from 'express';
import multer from 'multer';
import path from 'path';
import { v4 as uuid } from 'uuid';
import supabase from '../supabase.js';

const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 5 * 1024 * 1024 } });

const router = Router();

// POST /api/upload
router.post('/', upload.single('file'), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ error: 'No file uploaded' });

    const ext = path.extname(req.file.originalname);
    const fileName = `${uuid()}${ext}`;

    const { error } = await supabase.storage
      .from('physioconnect-uploads')
      .upload(fileName, req.file.buffer, {
        contentType: req.file.mimetype,
      });

    if (error) throw error;

    const { data: urlData } = supabase.storage
      .from('physioconnect-uploads')
      .getPublicUrl(fileName);

    res.json({ file_url: urlData.publicUrl });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
