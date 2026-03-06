import express from 'express';
import { tenantLogin, changePassword, resetPassword } from '../controllers/tenantAuthController.js';

const router = express.Router();

router.post('/login', tenantLogin);
router.post('/reset-password', resetPassword);
router.post('/change-password/:tenantId', changePassword);

export default router;
