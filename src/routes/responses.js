/**
 * Responses API 路由
 * 处理 /v1/responses 端点
 */

import { Router } from 'express';
import { handleResponsesRequest } from '../server/handlers/responses.js';

const router = Router();

/**
 * POST /v1/responses
 * 处理 OpenAI Responses 格式请求
 */
router.post('/responses', handleResponsesRequest);
router.post('/responses/compact', handleResponsesRequest);

export default router;
