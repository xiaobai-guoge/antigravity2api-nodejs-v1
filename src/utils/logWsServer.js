/**
 * WebSocket 日志服务模块
 * 提供实时日志推送和日志文件管理
 */
import { WebSocketServer } from 'ws';
import fs from 'fs';
import path from 'path';
import { getDataDir } from './paths.js';

// 默认配置
const DEFAULT_LOG_MAX_SIZE_MB = 10;   // 单个日志文件最大 10MB
const DEFAULT_LOG_MAX_FILES = 5;      // 保留 5 个历史文件
const DEFAULT_LOG_MAX_MEMORY = 500;   // 内存中保留 500 条日志

// 日志目录
const dataDir = getDataDir();
const LOG_DIR = path.join(dataDir, 'logs');
const LOG_FILE = path.join(LOG_DIR, 'app.log');

// 确保日志目录存在
if (!fs.existsSync(LOG_DIR)) {
    fs.mkdirSync(LOG_DIR, { recursive: true });
}

class LogWebSocketServer {
    constructor() {
        this.wss = null;
        this.clients = new Set();
        this.logStore = [];
        this.currentLogSize = 0;

        // 配置（可在运行时更新）
        this.maxSizeMB = DEFAULT_LOG_MAX_SIZE_MB;
        this.maxFiles = DEFAULT_LOG_MAX_FILES;
        this.maxMemory = DEFAULT_LOG_MAX_MEMORY;

        // 初始化日志文件大小
        this._initLogFileSize();

        // 写入缓冲（避免频繁写入）
        this.writeBuffer = [];
        this.flushTimer = null;
        this.FLUSH_INTERVAL = 1000; // 1秒刷新一次
    }

    /**
     * 初始化获取当前日志文件大小
     */
    _initLogFileSize() {
        try {
            if (fs.existsSync(LOG_FILE)) {
                const stats = fs.statSync(LOG_FILE);
                this.currentLogSize = stats.size;
            }
        } catch (error) {
            this.currentLogSize = 0;
        }
    }

    /**
     * 更新配置
     */
    updateConfig(config) {
        if (config.logMaxSizeMB !== undefined) {
            this.maxSizeMB = config.logMaxSizeMB;
        }
        if (config.logMaxFiles !== undefined) {
            this.maxFiles = config.logMaxFiles;
        }
        if (config.logMaxMemory !== undefined) {
            this.maxMemory = config.logMaxMemory;
        }
    }

    /**
     * 初始化 WebSocket 服务器
     * @param {http.Server} server - HTTP 服务器实例
     */
    initialize(server) {
        this.wss = new WebSocketServer({ noServer: true });

        this.wss.on('connection', (ws, req) => {
            this.clients.add(ws);

            // 发送最近的日志历史
            const recentLogs = this.logStore.slice(-50);
            if (recentLogs.length > 0) {
                ws.send(JSON.stringify({
                    type: 'history',
                    logs: recentLogs
                }));
            }

            ws.on('close', () => {
                this.clients.delete(ws);
            });

            ws.on('error', () => {
                this.clients.delete(ws);
            });
        });
    }

    /**
     * 广播日志到所有客户端
     */
    broadcast(entry) {
        const message = JSON.stringify({
            type: 'log',
            log: entry
        });

        for (const client of this.clients) {
            if (client.readyState === 1) { // OPEN
                try {
                    client.send(message);
                } catch (e) {
                    this.clients.delete(client);
                }
            }
        }
    }

    /**
     * 存储日志条目
     */
    storeLog(level, message) {
        const entry = {
            id: Date.now() + '-' + Math.random().toString(36).substr(2, 9),
            timestamp: new Date().toISOString(),
            level,
            message
        };

        // 存储到内存
        this.logStore.push(entry);
        while (this.logStore.length > this.maxMemory) {
            this.logStore.shift();
        }

        // 广播到 WebSocket 客户端
        this.broadcast(entry);

        // 添加到写入缓冲
        this._bufferWrite(entry);

        return entry;
    }

    /**
     * 缓冲写入（减少磁盘 I/O）
     */
    _bufferWrite(entry) {
        const line = `${entry.timestamp} [${entry.level}] ${entry.message}\n`;
        this.writeBuffer.push(line);

        // 设置定时刷新
        if (!this.flushTimer) {
            this.flushTimer = setTimeout(() => {
                this._flushBuffer();
            }, this.FLUSH_INTERVAL);
        }
    }

    /**
     * 刷新缓冲到文件
     */
    _flushBuffer() {
        if (this.writeBuffer.length === 0) {
            this.flushTimer = null;
            return;
        }

        const content = this.writeBuffer.join('');
        this.writeBuffer = [];
        this.flushTimer = null;

        const contentSize = Buffer.byteLength(content, 'utf8');

        // 检查是否需要轮转
        if (this.currentLogSize + contentSize > this.maxSizeMB * 1024 * 1024) {
            this._rotateLog();
        }

        // 追加写入
        try {
            fs.appendFileSync(LOG_FILE, content, 'utf8');
            this.currentLogSize += contentSize;
        } catch (error) {
            console.error('写入日志文件失败:', error.message);
        }
    }

    /**
     * 日志轮转
     */
    _rotateLog() {
        try {
            // 删除最旧的文件
            for (let i = this.maxFiles - 1; i >= 1; i--) {
                const oldFile = `${LOG_FILE}.${i}`;
                const newFile = `${LOG_FILE}.${i + 1}`;
                if (fs.existsSync(oldFile)) {
                    if (i === this.maxFiles - 1) {
                        fs.unlinkSync(oldFile);
                    } else {
                        fs.renameSync(oldFile, newFile);
                    }
                }
            }

            // 重命名当前文件
            if (fs.existsSync(LOG_FILE)) {
                fs.renameSync(LOG_FILE, `${LOG_FILE}.1`);
            }

            this.currentLogSize = 0;
        } catch (error) {
            console.error('日志轮转失败:', error.message);
        }
    }

    /**
     * 获取日志（API 查询）
     */
    getLogs(options = {}) {
        const { level, search, limit = 100, offset = 0 } = options;

        let filtered = [...this.logStore];

        // 过滤分隔符
        filtered = filtered.filter(log => !this._isSeparator(log.message));

        if (level && level !== 'all') {
            filtered = filtered.filter(log => log.level === level);
        }

        if (search) {
            const searchLower = search.toLowerCase();
            filtered = filtered.filter(log =>
                log.message.toLowerCase().includes(searchLower)
            );
        }

        filtered.reverse();

        return {
            logs: filtered.slice(offset, offset + limit),
            total: filtered.length
        };
    }

    /**
     * 判断是否为分隔符
     */
    _isSeparator(message) {
        if (!message || typeof message !== 'string') return false;
        const trimmed = message.trim();
        if (trimmed.length < 3) return false;
        return /^[═─=\-*_~]+$/.test(trimmed);
    }

    /**
     * 清空日志
     */
    clearLogs() {
        this.logStore.length = 0;
        // 广播清空事件
        for (const client of this.clients) {
            if (client.readyState === 1) {
                try {
                    client.send(JSON.stringify({ type: 'clear' }));
                } catch (e) { }
            }
        }
    }

    /**
     * 获取统计
     */
    getLogStats() {
        const stats = { total: 0, info: 0, warn: 0, error: 0, request: 0, debug: 0 };

        for (const log of this.logStore) {
            if (this._isSeparator(log.message)) continue;
            stats.total++;
            if (stats[log.level] !== undefined) {
                stats[log.level]++;
            }
        }

        return stats;
    }

    /**
     * 关闭服务
     */
    close() {
        // 刷新剩余缓冲
        if (this.flushTimer) {
            clearTimeout(this.flushTimer);
            this._flushBuffer();
        }

        // 关闭 WebSocket
        if (this.wss) {
            for (const client of this.clients) {
                client.close();
            }
            this.wss.close();
        }
    }
}

// 单例
export const logWsServer = new LogWebSocketServer();
export default logWsServer;
