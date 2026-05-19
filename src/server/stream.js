/**
 * SSE 流式响应和心跳机制工具模块
 */

import config from '../config/config.js';
import memoryManager, { registerMemoryPoolCleanup } from '../utils/memoryManager.js';
import { DEFAULT_HEARTBEAT_INTERVAL } from '../constants/index.js';

export { with429Retry } from './handlers/common/retry.js';

// ==================== 心跳机制（防止 CF 超时） ====================
const HEARTBEAT_INTERVAL = config.server.heartbeatInterval || DEFAULT_HEARTBEAT_INTERVAL;
const SSE_HEARTBEAT = Buffer.from(': heartbeat\n\n');

/**
 * 创建心跳定时器
 * @param {Response} res - Express响应对象
 * @returns {NodeJS.Timeout} 定时器
 */
export const createHeartbeat = (res) => {
  const timer = setInterval(() => {
    if (!res.writableEnded) {
      res.write(SSE_HEARTBEAT);
    } else {
      clearInterval(timer);
    }
  }, HEARTBEAT_INTERVAL);

  res.on('close', () => clearInterval(timer));
  res.on('finish', () => clearInterval(timer));

  return timer;
};

// ==================== 预编译的常量字符串（避免重复创建） ====================
const SSE_PREFIX = Buffer.from('data: ');
const SSE_SUFFIX = Buffer.from('\n\n');
const SSE_DONE = Buffer.from('data: [DONE]\n\n');

/**
 * 生成响应元数据
 * @returns {{id: string, created: number}}
 */
export const createResponseMeta = () => ({
  id: `chatcmpl-${Date.now()}`,
  created: Math.floor(Date.now() / 1000)
});

/**
 * 设置流式响应头
 * @param {Response} res - Express响应对象
 */
export const setStreamHeaders = (res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();
};

// ==================== 对象池（减少 GC） ====================
const chunkPool = [];

/**
 * 从对象池获取 chunk 对象
 * @returns {Object}
 */
export const getChunkObject = () => chunkPool.pop() || { choices: [{ index: 0, delta: {}, finish_reason: null }] };

/**
 * 释放 chunk 对象回对象池
 * @param {Object} obj
 */
export const releaseChunkObject = (obj) => {
  const maxSize = memoryManager.getPoolSizes().chunk;
  if (chunkPool.length < maxSize) chunkPool.push(obj);
};

registerMemoryPoolCleanup(chunkPool, () => memoryManager.getPoolSizes().chunk);

/**
 * 获取当前对象池大小（用于监控）
 * @returns {number}
 */
export const getChunkPoolSize = () => chunkPool.length;

/**
 * 清空对象池
 */
export const clearChunkPool = () => {
  chunkPool.length = 0;
};

/**
 * 零拷贝写入流式数据
 * @param {Response} res - Express响应对象
 * @param {Object} data - 要发送的数据
 */
export const writeStreamData = (res, data) => {
  const json = JSON.stringify(data);
  res.write(SSE_PREFIX);
  res.write(json);
  res.write(SSE_SUFFIX);
  if (typeof res.flush === 'function') {
    res.flush();
  }
};

/**
 * 结束流式响应
 * @param {Response} res - Express响应对象
 */
export const endStream = (res, isWriteDone = true) => {
  if (res.writableEnded) return;
  if (isWriteDone) res.write(SSE_DONE);
  res.end();
};
