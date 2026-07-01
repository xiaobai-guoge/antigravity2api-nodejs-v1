import { WebSocketServer } from 'ws';
import logger from '../utils/logger.js';
import config from '../config/config.js';
import { handleResponsesRequest } from './handlers/responses.js';

export const responsesWss = new WebSocketServer({ noServer: true });

responsesWss.on('connection', (ws, request) => {
  logger.info('WebSocket connection established on /v1/responses');

  ws.on('message', async (message) => {
    try {
      const msgStr = message.toString();
      const event = JSON.parse(msgStr);
      
      if (event.type === 'response.create') {
        const payload = event.response || event || {};
        payload.stream = true;
        
        // Mock req and res
        const mockReq = {
          body: payload,
          path: '/responses',
          originalUrl: '/v1/responses',
          headers: request.headers,
          setTimeout: () => {}
        };

        const mockRes = {
          isWebSocket: true,
          writableEnded: false,
          headersSent: false,
          sendEvent(eventType, data) {
            if (this.writableEnded) return;
            try {
              ws.send(JSON.stringify({
                type: eventType,
                ...data
              }));
            } catch (err) {
              logger.error('WS send event failed:', err.message);
            }
          },
          status(code) {
            return this;
          },
          setHeader(name, value) {
            return this;
          },
          flushHeaders() {
            return this;
          },
          setTimeout(timeout) {
            return this;
          },
          write(chunk) {
            return true;
          },
          on(event, callback) {
            if (!this.listeners) this.listeners = {};
            if (!this.listeners[event]) this.listeners[event] = [];
            this.listeners[event].push(callback);
            return this;
          },
          emit(event) {
            if (this.listeners && this.listeners[event]) {
              this.listeners[event].forEach(cb => cb());
            }
          },
          end() {
            if (this.writableEnded) return;
            this.writableEnded = true;
            this.emit('finish');
            this.emit('close');
            setTimeout(() => {
              try {
                ws.close();
              } catch (e) {}
            }, 50);
          },
          json(data) {
            if (this.writableEnded) return;
            if (data && data.error) {
              this.sendEvent('error', { error: data.error });
            } else {
              this.sendEvent('response.completed', { response: data });
            }
            this.end();
          }
        };

        try {
          await handleResponsesRequest(mockReq, mockRes);
        } catch (err) {
          logger.error('Error handling responses WS request:', err.message);
          mockRes.json({ error: { message: err.message } });
        }
      }
    } catch (err) {
      logger.error('Error parsing WS message:', err.message);
    }
  });

  ws.on('close', () => {
    logger.info('WebSocket connection closed on /v1/responses');
  });

  ws.on('error', (err) => {
    logger.error('WebSocket connection error:', err.message);
  });
});

export function handleUpgrade(request, socket, head) {
  logger.info(`responsesWs: handleUpgrade method=${request.method} version=${request.httpVersion} path=${request.url} headers=${JSON.stringify(request.headers)}`);
  // Verify API Key
  const apiKey = config.security?.apiKey;
  if (apiKey) {
    const authHeader = request.headers.authorization || request.headers['x-api-key'] || getAuthFromUrl(request.url);
    const providedKey = authHeader?.startsWith('Bearer ') ? authHeader.slice(7) : authHeader;
    if (providedKey !== apiKey) {
      logger.warn(`WS API Key 验证失败: ${request.url}`);
      socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
      socket.destroy();
      return;
    }
  }

  responsesWss.handleUpgrade(request, socket, head, (ws) => {
    responsesWss.emit('connection', ws, request);
  });
}

function getAuthFromUrl(urlStr) {
  try {
    const url = new URL(urlStr, 'http://localhost');
    return url.searchParams.get('token') || url.searchParams.get('key');
  } catch (e) {
    return null;
  }
}
