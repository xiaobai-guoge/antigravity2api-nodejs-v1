/**
 * Responses 格式处理器
 * 处理 /v1/responses 请求，支持流式和非流式响应
 */

import { generateAssistantResponse, generateAssistantResponseNoStream, getModelsWithQuotas } from '../../api/client.js';
import { generateRequestBody, prepareImageRequest } from '../../utils/utils.js';
import { buildOpenAIErrorPayload } from '../../utils/errors.js';
import logger from '../../utils/logger.js';
import config from '../../config/config.js';
import tokenManager from '../../auth/token_manager.js';
import quotaManager from '../../auth/quota_manager.js';
import { getSafeRetries } from './common/retry.js';
import {
  setStreamHeaders,
  createHeartbeat,
  endStream,
  with429Retry
} from '../stream.js';

// ==================== ID 生成辅助函数 ====================
export function generateResponsesID() {
  return `resp_${Math.random().toString(36).slice(2, 11)}${Math.random().toString(36).slice(2, 11)}`;
}

export function generateItemID() {
  return `item_${Math.random().toString(36).slice(2, 11)}${Math.random().toString(36).slice(2, 11)}`;
}

// ==================== SSE 写入辅助函数 ====================
export const writeResponsesStreamData = (res, eventType, data) => {
  if (res.writableEnded) return;
  res.write(`event: ${eventType}\n`);
  res.write(`data: ${JSON.stringify(data)}\n\n`);
  if (typeof res.flush === 'function') {
    res.flush();
  }
};

// ==================== 输入转换逻辑 ====================

function extractReasoningText(item) {
  if (!item) return '';
  const collect = (val) => {
    if (!val) return [];
    if (Array.isArray(val)) {
      return val.map(p => p.text || '').filter(Boolean);
    }
    if (typeof val === 'string') return [val];
    return [];
  };
  
  let parts = collect(item.summary);
  if (parts.length === 0) {
    parts = collect(item.content);
  }
  return parts.join('\n');
}

export function responsesInputToChatMessages(instructions, input) {
  const messages = [];
  if (instructions && typeof instructions === 'string' && instructions.trim() !== '') {
    messages.push({ role: 'system', content: instructions });
  }

  if (!input) {
    return messages;
  }

  // 单纯字符串输入作为 user 消息
  if (typeof input === 'string') {
    messages.push({ role: 'user', content: input });
    return messages;
  }

  // 数组形式的输入项
  if (Array.isArray(input)) {
    let pendingReasoning = '';
    
    for (const item of input) {
      if (!item) continue;
      
      const role = item.role || 'user';
      const itemType = item.type;
      
      if (itemType === 'reasoning') {
        const text = extractReasoningText(item);
        if (text) {
          pendingReasoning = text;
        }
        continue;
      }
      
      if (itemType === 'function_call') {
        let args = item.arguments;
        if (typeof args !== 'string') {
          args = JSON.stringify(args || {});
        }
        if (!args.trim()) {
          args = '{}';
        }
        
        const toolCall = {
          id: item.call_id || generateItemID(),
          type: 'function',
          function: {
            name: item.name,
            arguments: args
          }
        };
        
        // 合并连续的 tool calls 到上一个 assistant 消息中
        if (messages.length > 0 && messages[messages.length - 1].role === 'assistant') {
          const lastMsg = messages[messages.length - 1];
          if (!lastMsg.tool_calls) lastMsg.tool_calls = [];
          lastMsg.tool_calls.push(toolCall);
          if (!lastMsg.reasoning_content) {
            lastMsg.reasoning_content = pendingReasoning;
          }
        } else {
          messages.push({
            role: 'assistant',
            tool_calls: [toolCall],
            reasoning_content: pendingReasoning
          });
        }
        pendingReasoning = '';
        continue;
      }
      
      if (itemType === 'function_call_output') {
        messages.push({
          role: 'tool',
          tool_call_id: item.call_id,
          content: typeof item.output === 'string' ? item.output : JSON.stringify(item.output || '')
        });
        pendingReasoning = '';
        continue;
      }
      
      if (itemType === 'input_text' || itemType === 'text') {
        messages.push({
          role: 'user',
          content: item.text || ''
        });
        pendingReasoning = '';
        continue;
      }
      
      if (itemType === 'input_image') {
        const imageURL = item.image_url;
        messages.push({
          role: 'user',
          content: [
            {
              type: 'image_url',
              image_url: {
                url: typeof imageURL === 'string' ? imageURL : (imageURL?.url || '')
              }
            }
          ]
        });
        pendingReasoning = '';
        continue;
      }
      
      // 消息类型（或无类型）
      if (itemType === 'message' || !itemType) {
        let content = item.content;
        if (content === undefined || content === null) {
          content = item.text || '';
        }
        
        const mappedRole = role === 'developer' ? 'system' : role;
        
        let chatContent;
        if (Array.isArray(content)) {
          chatContent = [];
          for (const part of content) {
            if (part.type === 'input_text' || part.type === 'output_text' || part.type === 'text') {
              chatContent.push({ type: 'text', text: part.text || '' });
            } else if (part.type === 'input_image' || part.type === 'image_url') {
              const url = part.image_url?.url || part.image_url || '';
              chatContent.push({ type: 'image_url', image_url: { url } });
            }
          }
          if (chatContent.every(p => p.type === 'text')) {
            chatContent = chatContent.map(p => p.text).join('\n\n');
          }
        } else {
          chatContent = content;
        }
        
        messages.push({
          role: mappedRole,
          content: chatContent
        });
        
        if (mappedRole !== 'assistant') {
          pendingReasoning = '';
        }
      }
    }
  }
  
  return messages;
}

function isBlankChatContent(content) {
  if (content === undefined || content === null) return true;
  if (typeof content === 'string') return content.trim() === '';
  if (Array.isArray(content)) {
    return !content.some(p => p.type === 'text' && p.text && p.text.trim() !== '');
  }
  return false;
}

export function normalizeChatMessages(messages) {
  const replies = {};
  for (const m of messages) {
    if (m.role === 'tool' && m.tool_call_id) {
      replies[m.tool_call_id] = m;
    }
  }

  const out = [];
  for (const m of messages) {
    if (m.role === 'tool') {
      if (!m.tool_call_id) {
        out.push(m);
      }
      continue;
    }
    if (m.tool_calls && m.tool_calls.length > 0) {
      const kept = [];
      for (const tc of m.tool_calls) {
        if (!tc.id) continue;
        if (replies[tc.id]) {
          kept.push(tc);
        }
      }
      if (kept.length === 0) {
        if (isBlankChatContent(m.content)) {
          continue;
        }
        const mCopy = { ...m };
        delete mCopy.tool_calls;
        out.push(mCopy);
        continue;
      }
      const mCopy = { ...m, tool_calls: kept };
      out.push(mCopy);
      for (const tc of kept) {
        out.push(replies[tc.id]);
      }
    } else {
      out.push(m);
    }
  }
  return out;
}

export function responsesToolsToChatTools(tools) {
  if (!tools || !Array.isArray(tools)) return undefined;
  return tools.map(tool => {
    if (tool.type !== 'function') return null;
    return {
      type: 'function',
      function: {
        name: tool.name,
        description: tool.description,
        parameters: tool.parameters,
        strict: tool.strict === true
      }
    };
  }).filter(Boolean);
}

export function responsesToolChoiceToChatToolChoice(toolChoice) {
  if (!toolChoice) return undefined;
  if (typeof toolChoice === 'string') return toolChoice;
  if (toolChoice.type === 'function') {
    return {
      type: 'function',
      function: {
        name: toolChoice.name || toolChoice.function?.name
      }
    };
  }
  return toolChoice;
}

// ==================== 流式状态类 ====================

export class StreamState {
  constructor(model, id) {
    this.responseID = id || generateResponsesID();
    this.model = model;
    this.created = Math.floor(Date.now() / 1000);
    this.sequenceNumber = 0;
    
    this.createdSent = false;
    this.completedSent = false;
    this.nextOutputIndex = 0;
    
    // Reasoning
    this.reasoningItemID = null;
    this.reasoningIndex = -1;
    this.reasoningOpen = false;
    this.reasoningDone = false;
    
    // Message
    this.messageItemID = null;
    this.messageIndex = -1;
    this.textPartOpen = false;
    
    this.text = '';
    this.reasoning = '';
    
    // Tool calls
    this.toolCalls = {};       // index -> toolCall
    this.toolItemIDs = {};     // index -> itemID
    this.toolOutputIndex = {}; // index -> output_index
    
    this.finishReason = null;
    this.usage = null;
  }
  
  allocOutputIndex() {
    const idx = this.nextOutputIndex;
    this.nextOutputIndex++;
    return idx;
  }
  
  getChatOutput() {
    const outputs = [];
    if (this.reasoning.length > 0) {
      outputs.push({
        type: 'reasoning',
        id: this.reasoningItemID || generateItemID(),
        summary: [{
          type: 'summary_text',
          text: this.reasoning
        }]
      });
    }
    
    if (this.messageItemID || Object.keys(this.toolCalls).length === 0) {
      outputs.push({
        type: 'message',
        id: this.messageItemID || generateItemID(),
        role: 'assistant',
        content: [{
          type: 'output_text',
          text: this.text
        }],
        status: 'completed'
      });
    }
    
    const toolIndices = Object.keys(this.toolCalls).map(Number).sort((a, b) => a - b);
    for (const idx of toolIndices) {
      const tc = this.toolCalls[idx];
      if (!tc) continue;
      let args = tc.function.arguments;
      if (!args.trim()) args = '{}';
      outputs.push({
        type: 'function_call',
        id: this.toolItemIDs[idx] || generateItemID(),
        call_id: tc.id,
        name: tc.function.name,
        arguments: args,
        status: 'completed'
      });
    }
    return outputs;
  }
}

// ==================== 流式事件映射辅助 ====================

export function ensureCreated(state, res) {
  if (state.createdSent) return;
  state.createdSent = true;
  writeResponsesStreamData(res, 'response.created', {
    type: 'response.created',
    sequence_number: state.sequenceNumber++,
    response: {
      id: state.responseID,
      object: 'response',
      model: state.model,
      status: 'in_progress',
      output: []
    }
  });
}

export function closeReasoningItem(state, res) {
  if (!state.reasoningOpen) return;
  state.reasoningOpen = false;
  state.reasoningDone = true;
  
  writeResponsesStreamData(res, 'response.reasoning_summary_text.done', {
    type: 'response.reasoning_summary_text.done',
    sequence_number: state.sequenceNumber++,
    output_index: state.reasoningIndex,
    summary_index: 0,
    item_id: state.reasoningItemID,
    text: state.reasoning
  });
  
  writeResponsesStreamData(res, 'response.reasoning_summary_part.done', {
    type: 'response.reasoning_summary_part.done',
    sequence_number: state.sequenceNumber++,
    output_index: state.reasoningIndex,
    summary_index: 0,
    item_id: state.reasoningItemID,
    part: {
      type: 'summary_text',
      text: state.reasoning
    }
  });
  
  writeResponsesStreamData(res, 'response.output_item.done', {
    type: 'response.output_item.done',
    sequence_number: state.sequenceNumber++,
    output_index: state.reasoningIndex,
    item: {
      type: 'reasoning',
      id: state.reasoningItemID,
      status: 'completed',
      summary: [{
        type: 'summary_text',
        text: state.reasoning
      }]
    }
  });
}

export function processChunkData(data, state, res) {
  ensureCreated(state, res);
  
  if (data.type === 'usage') {
    state.usage = {
      input_tokens: data.usage.prompt_tokens,
      output_tokens: data.usage.completion_tokens,
      total_tokens: data.usage.total_tokens || (data.usage.prompt_tokens + data.usage.completion_tokens)
    };
    if (data.usage.prompt_tokens_details?.cached_tokens) {
      state.usage.input_tokens_details = {
        cached_tokens: data.usage.prompt_tokens_details.cached_tokens
      };
    }
  } else if (data.type === 'reasoning') {
    const reasoningText = data.reasoning_content || '';
    if (reasoningText !== '') {
      if (!state.reasoningOpen && !state.reasoningDone) {
        state.reasoningOpen = true;
        state.reasoningItemID = generateItemID();
        state.reasoningIndex = state.allocOutputIndex();
        
        writeResponsesStreamData(res, 'response.output_item.added', {
          type: 'response.output_item.added',
          sequence_number: state.sequenceNumber++,
          output_index: state.reasoningIndex,
          item: {
            type: 'reasoning',
            id: state.reasoningItemID,
            status: 'in_progress'
          }
        });
        
        writeResponsesStreamData(res, 'response.reasoning_summary_part.added', {
          type: 'response.reasoning_summary_part.added',
          sequence_number: state.sequenceNumber++,
          output_index: state.reasoningIndex,
          summary_index: 0,
          item_id: state.reasoningItemID,
          part: {
            type: 'summary_text'
          }
        });
      }
      
      state.reasoning += reasoningText;
      writeResponsesStreamData(res, 'response.reasoning_summary_text.delta', {
        type: 'response.reasoning_summary_text.delta',
        sequence_number: state.sequenceNumber++,
        output_index: state.reasoningIndex,
        summary_index: 0,
        item_id: state.reasoningItemID,
        delta: reasoningText
      });
    }
  } else if (data.type === 'tool_calls') {
    closeReasoningItem(state, res);
    
    for (const toolCall of data.tool_calls) {
      const idx = toolCall.index !== undefined ? toolCall.index : 0;
      let stored = state.toolCalls[idx];
      if (!stored) {
        stored = {
          id: toolCall.id || generateItemID(),
          type: 'function',
          function: {
            name: toolCall.function?.name || '',
            arguments: ''
          }
        };
        state.toolCalls[idx] = stored;
        state.toolItemIDs[idx] = generateItemID();
        state.toolOutputIndex[idx] = state.allocOutputIndex();
        
        writeResponsesStreamData(res, 'response.output_item.added', {
          type: 'response.output_item.added',
          sequence_number: state.sequenceNumber++,
          output_index: state.toolOutputIndex[idx],
          item: {
            type: 'function_call',
            id: state.toolItemIDs[idx],
            call_id: stored.id,
            name: stored.function.name,
            status: 'in_progress'
          }
        });
      } else {
        if (toolCall.id) stored.id = toolCall.id;
        if (toolCall.function?.name) stored.function.name = toolCall.function.name;
      }
      
      if (toolCall.function?.arguments) {
        stored.function.arguments += toolCall.function.arguments;
        writeResponsesStreamData(res, 'response.function_call_arguments.delta', {
          type: 'response.function_call_arguments.delta',
          sequence_number: state.sequenceNumber++,
          output_index: state.toolOutputIndex[idx],
          item_id: state.toolItemIDs[idx],
          delta: toolCall.function.arguments,
          call_id: stored.id,
          name: stored.function.name
        });
      }
    }
  } else {
    // text content
    const textContent = data.content || '';
    if (textContent !== '') {
      closeReasoningItem(state, res);
      
      if (!state.messageItemID) {
        state.messageItemID = generateItemID();
        state.messageIndex = state.allocOutputIndex();
        
        writeResponsesStreamData(res, 'response.output_item.added', {
          type: 'response.output_item.added',
          sequence_number: state.sequenceNumber++,
          output_index: state.messageIndex,
          item: {
            type: 'message',
            id: state.messageItemID,
            role: 'assistant',
            status: 'in_progress',
            content: [{ type: 'output_text' }]
          }
        });
      }
      
      if (!state.textPartOpen) {
        state.textPartOpen = true;
        writeResponsesStreamData(res, 'response.content_part.added', {
          type: 'response.content_part.added',
          sequence_number: state.sequenceNumber++,
          output_index: state.messageIndex,
          content_index: 0,
          item_id: state.messageItemID,
          part: {
            type: 'output_text',
            text: ''
          }
        });
      }
      
      state.text += textContent;
      writeResponsesStreamData(res, 'response.output_text.delta', {
        type: 'response.output_text.delta',
        sequence_number: state.sequenceNumber++,
        output_index: state.messageIndex,
        content_index: 0,
        item_id: state.messageItemID,
        delta: textContent
      });
    }
  }
}

export function finalizeStream(state, res) {
  if (state.completedSent) return;
  
  ensureCreated(state, res);
  closeReasoningItem(state, res);
  
  // reasoning fallback
  if (!state.messageItemID && state.text.length === 0 && state.reasoning.length > 0 && Object.keys(state.toolCalls).length === 0) {
    const fallbackText = state.reasoning;
    if (fallbackText.trim() !== '') {
      state.messageItemID = generateItemID();
      state.messageIndex = state.allocOutputIndex();
      
      writeResponsesStreamData(res, 'response.output_item.added', {
        type: 'response.output_item.added',
        sequence_number: state.sequenceNumber++,
        output_index: state.messageIndex,
        item: {
          type: 'message',
          id: state.messageItemID,
          role: 'assistant',
          status: 'in_progress',
          content: [{ type: 'output_text' }]
        }
      });
      
      state.textPartOpen = true;
      writeResponsesStreamData(res, 'response.content_part.added', {
        type: 'response.content_part.added',
        sequence_number: state.sequenceNumber++,
        output_index: state.messageIndex,
        content_index: 0,
        item_id: state.messageItemID,
        part: {
          type: 'output_text',
          text: ''
        }
      });
      
      state.text = fallbackText;
      writeResponsesStreamData(res, 'response.output_text.delta', {
        type: 'response.output_text.delta',
        sequence_number: state.sequenceNumber++,
        output_index: state.messageIndex,
        content_index: 0,
        item_id: state.messageItemID,
        delta: fallbackText
      });
    }
  }
  
  // Close message
  if (state.messageItemID) {
    if (state.textPartOpen) {
      writeResponsesStreamData(res, 'response.output_text.done', {
        type: 'response.output_text.done',
        sequence_number: state.sequenceNumber++,
        output_index: state.messageIndex,
        content_index: 0,
        item_id: state.messageItemID,
        text: state.text
      });
      
      writeResponsesStreamData(res, 'response.content_part.done', {
        type: 'response.content_part.done',
        sequence_number: state.sequenceNumber++,
        output_index: state.messageIndex,
        content_index: 0,
        item_id: state.messageItemID,
        part: {
          type: 'output_text',
          text: state.text
        }
      });
    }
    
    writeResponsesStreamData(res, 'response.output_item.done', {
      type: 'response.output_item.done',
      sequence_number: state.sequenceNumber++,
      output_index: state.messageIndex,
      item: {
        type: 'message',
        id: state.messageItemID,
        role: 'assistant',
        content: [{ type: 'output_text', text: state.text }],
        status: 'completed'
      }
    });
  }
  
  // Close tools
  const toolIndices = Object.keys(state.toolCalls).map(Number).sort((a, b) => a - b);
  for (const idx of toolIndices) {
    const tc = state.toolCalls[idx];
    if (!tc) continue;
    const itemID = state.toolItemIDs[idx];
    if (!itemID) continue;
    
    let args = tc.function.arguments;
    if (!args.trim()) args = '{}';
    const outputIndex = state.toolOutputIndex[idx];
    
    writeResponsesStreamData(res, 'response.function_call_arguments.done', {
      type: 'response.function_call_arguments.done',
      sequence_number: state.sequenceNumber++,
      output_index: outputIndex,
      item_id: itemID,
      call_id: tc.id,
      name: tc.function.name,
      arguments: args
    });
    
    writeResponsesStreamData(res, 'response.output_item.done', {
      type: 'response.output_item.done',
      sequence_number: state.sequenceNumber++,
      output_index: outputIndex,
      item: {
        type: 'function_call',
        id: itemID,
        call_id: tc.id,
        name: tc.function.name,
        arguments: args,
        status: 'completed'
      }
    });
  }
  
  let status = 'completed';
  let incompleteDetails = null;
  if (state.finishReason === 'length') {
    status = 'incomplete';
    incompleteDetails = { reason: 'max_output_tokens' };
  }
  
  state.completedSent = true;
  
  const responsePayload = {
    id: state.responseID,
    object: 'response',
    model: state.model,
    status: status,
    output: state.getChatOutput(),
    usage: state.usage
  };
  
  if (incompleteDetails) {
    responsePayload.incomplete_details = incompleteDetails;
  }
  
  writeResponsesStreamData(res, 'response.completed', {
    type: 'response.completed',
    sequence_number: state.sequenceNumber++,
    response: responsePayload
  });
}

function createResponsesNonStreamResponse(state) {
  let status = 'completed';
  let incompleteDetails = null;
  if (state.finishReason === 'length') {
    status = 'incomplete';
    incompleteDetails = { reason: 'max_output_tokens' };
  }
  
  const responsePayload = {
    id: state.responseID,
    object: 'response',
    model: state.model,
    status: status,
    output: state.getChatOutput(),
    usage: state.usage
  };
  
  if (incompleteDetails) {
    responsePayload.incomplete_details = incompleteDetails;
  }
  
  return responsePayload;
}

// ==================== 主请求处理器 ====================

/**
 * 处理 Responses 格式的聊天请求
 * @param {Request} req - Express请求对象
 * @param {Response} res - Express响应对象
 */
export const handleResponsesRequest = async (req, res) => {
  const body = req.body || {};
  const { input, instructions, model, stream = false, tools, tool_choice, max_output_tokens, temperature, top_p } = body;

  try {
    if (!body || typeof body !== 'object') {
      return res.status(400).json({ error: 'request body is required' });
    }
    if (!model || typeof model !== 'string') {
      return res.status(400).json({ error: 'model is required' });
    }
    if (input === undefined || input === null) {
      return res.status(400).json({ error: 'input is required' });
    }

    // 1. 将 input + instructions 转为 standard openaiMessages
    const rawMessages = responsesInputToChatMessages(instructions, input);
    const messages = normalizeChatMessages(rawMessages);

    // 2. 将 tools / tool_choice / parameters 转换为 OpenAI 参数结构
    const openaiTools = responsesToolsToChatTools(tools);
    const params = {};
    if (max_output_tokens !== undefined) params.max_tokens = max_output_tokens;
    if (temperature !== undefined) params.temperature = temperature;
    if (top_p !== undefined) params.top_p = top_p;
    if (tool_choice !== undefined) {
      params.tool_choice = responsesToolChoiceToChatToolChoice(tool_choice);
    }

    const isImageModel = model.includes('-image');
    let token = null;
    let tokenId = null;
    let requestBody = null;

    const applyTokenState = async (nextToken) => {
      if (!nextToken) return false;

      token = nextToken;
      tokenId = await tokenManager.getTokenId(token);
      requestBody = generateRequestBody(messages, model, params, openaiTools, token);
      if (isImageModel) {
        prepareImageRequest(requestBody);
      }
      return true;
    };

    if (!await applyTokenState(await tokenManager.getToken(model))) {
      throw new Error('没有可用的token，请运行 npm run login 获取token');
    }

    const refreshQuota = async () => {
      if (!tokenId || !token) return;
      const quotas = await getModelsWithQuotas(token);
      quotaManager.updateQuota(tokenId, quotas);
    };

    const createRetryOptions = (prefix) => ({
      loggerPrefix: prefix,
      onAttempt: () => tokenManager.recordRequest(token, model),
      getTokenId: () => tokenId,
      modelId: model,
      refreshQuota,
      tokenManager,
      getToken: () => token,
      onBeforeRetry: async ({ previousTokenId }) => {
        const nextToken = await tokenManager.getTokenForRetry(model, previousTokenId);
        return applyTokenState(nextToken);
      }
    });

    const responseID = generateResponsesID();
    const state = new StreamState(model, responseID);
    const safeRetries = getSafeRetries(config.retryTimes);

    if (stream) {
      setStreamHeaders(res);
      const heartbeatTimer = createHeartbeat(res);

      try {
        if (isImageModel) {
          const { content, usage } = await with429Retry(
            (attempt, shouldUseCredits) => {
              const actualRequestBody = shouldUseCredits 
                ? { ...requestBody, enabledCreditTypes: ["GOOGLE_ONE_AI"] }
                : requestBody;
              return generateAssistantResponseNoStream(actualRequestBody, token);
            },
            safeRetries,
            createRetryOptions('responses.stream.image ')
          );
          
          if (usage) {
            state.usage = {
              input_tokens: usage.prompt_tokens,
              output_tokens: usage.completion_tokens,
              total_tokens: usage.total_tokens || (usage.prompt_tokens + usage.completion_tokens)
            };
          }
          
          processChunkData({ type: 'text', content: content || '' }, state, res);
          finalizeStream(state, res);
        } else {
          await with429Retry(
            (attempt, shouldUseCredits) => {
              const actualRequestBody = shouldUseCredits 
                ? { ...requestBody, enabledCreditTypes: ["GOOGLE_ONE_AI"] }
                : requestBody;
              return generateAssistantResponse(actualRequestBody, token, (data) => {
                processChunkData(data, state, res);
              });
            },
            safeRetries,
            createRetryOptions('responses.stream ')
          );

          finalizeStream(state, res);
        }

        clearInterval(heartbeatTimer);
        endStream(res);
      } catch (error) {
        clearInterval(heartbeatTimer);
        if (!res.writableEnded) {
          const statusCode = error.statusCode || error.status || 500;
          const errPayload = buildOpenAIErrorPayload(error, statusCode);
          
          writeResponsesStreamData(res, 'response.failed', {
            type: 'response.failed',
            sequence_number: state.sequenceNumber++,
            response: {
              id: state.responseID,
              object: 'response',
              model: state.model,
              status: 'failed',
              error: errPayload.error
            }
          });
          endStream(res);
        }
        logger.error('Responses 生成响应失败:', error.message);
        return;
      }
    } else {
      // 非流式请求
      req.setTimeout(0);
      res.setTimeout(0);

      const { content, reasoningContent, toolCalls, usage } = await with429Retry(
        (attempt, shouldUseCredits) => {
          const actualRequestBody = shouldUseCredits 
            ? { ...requestBody, enabledCreditTypes: ["GOOGLE_ONE_AI"] }
            : requestBody;
          return generateAssistantResponseNoStream(actualRequestBody, token);
        },
        safeRetries,
        createRetryOptions('responses.no_stream ')
      );

      if (reasoningContent && reasoningContent.length > 0) {
        state.reasoning = reasoningContent;
        state.reasoningItemID = generateItemID();
      }
      
      if (content && content.length > 0) {
        state.text = content;
        state.messageItemID = generateItemID();
      }
      
      if (toolCalls && toolCalls.length > 0) {
        toolCalls.forEach((tc, idx) => {
          state.toolCalls[idx] = {
            id: tc.id,
            type: 'function',
            function: {
              name: tc.function.name,
              arguments: tc.function.arguments
            }
          };
          state.toolItemIDs[idx] = generateItemID();
        });
      }

      if (usage) {
        state.usage = {
          input_tokens: usage.prompt_tokens,
          output_tokens: usage.completion_tokens,
          total_tokens: usage.total_tokens || (usage.prompt_tokens + usage.completion_tokens)
        };
        if (usage.prompt_tokens_details?.cached_tokens) {
          state.usage.input_tokens_details = {
            cached_tokens: usage.prompt_tokens_details.cached_tokens
          };
        }
      }

      res.json(createResponsesNonStreamResponse(state));
    }
  } catch (error) {
    logger.error('Responses 生成响应失败:', error.message);
    if (res.headersSent) return;
    const statusCode = error.statusCode || error.status || 500;
    return res.status(statusCode).json(buildOpenAIErrorPayload(error, statusCode));
  }
};
