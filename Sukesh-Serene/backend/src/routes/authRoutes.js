import express from 'express';
import { register, login, getCurrentUser, changePassword } from '../controllers/authController.js';
import { authenticate } from '../middleware/auth.js';

const router = express.Router();

router.post('/register', register);
router.post('/login', login);
router.get('/me', authenticate, getCurrentUser);
router.post('/change-password', authenticate, changePassword);

export default router;
