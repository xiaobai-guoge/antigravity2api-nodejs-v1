/**
 * Responses 格式处理器
 * 处理 /v1/responses 请求，支持流式和非流式响应
 */

import axios from 'axios';
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
import crypto from 'crypto';

// Encryption/decryption helper for context compaction
const COMPACT_SECRET = process.env.JWT_SECRET || 'default_compaction_secret_key_123456';
const ALGORITHM = 'aes-256-cbc';
const KEY = crypto.scryptSync(COMPACT_SECRET, 'salt', 32);

function encrypt(text) {
  const iv = crypto.randomBytes(16);
  const cipher = crypto.createCipheriv(ALGORITHM, KEY, iv);
  const encrypted = Buffer.concat([
    cipher.update(text, 'utf8'),
    cipher.final()
  ]);
  const combined = Buffer.concat([iv, encrypted]);
  return combined.toString('base64');
}

function decrypt(text) {
  try {
    if (text.includes(':')) {
      const parts = text.split(':');
      if (parts.length < 2) return '';
      const iv = Buffer.from(parts.shift(), 'hex');
      const encryptedText = Buffer.from(parts.join(':'), 'hex');
      const decipher = crypto.createDecipheriv(ALGORITHM, KEY, iv);
      let decrypted = decipher.update(encryptedText, 'hex', 'utf8');
      decrypted += decipher.final('utf8');
      return decrypted;
    } else {
      const combined = Buffer.from(text, 'base64');
      if (combined.length < 16) return '';
      const iv = combined.subarray(0, 16);
      const encryptedText = combined.subarray(16);
      const decipher = crypto.createDecipheriv(ALGORITHM, KEY, iv);
      const decrypted = decipher.update(encryptedText);
      return Buffer.concat([decrypted, decipher.final()]).toString('utf8');
    }
  } catch (err) {
    logger.error('解密压缩历史记录失败:', err.message);
    return '';
  }
}


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
  if (res.isWebSocket) {
    res.sendEvent(eventType, data);
    return;
  }
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
    let systemPrompt = instructions;
    if (!systemPrompt.includes('Chinese')) {
      systemPrompt += '\n\nIMPORTANT: If the user communicates or asks questions in Chinese, you MUST reply in Chinese. (如果用户使用中文交流或提问，请务必使用中文回复。更多的时候用户需要中文回复)';
    }
    messages.push({ role: 'system', content: systemPrompt });
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
      
      if (itemType === 'compaction_summary' || itemType === 'compaction') {
        const decrypted = decrypt(item.encrypted_content);
        if (decrypted) {
          try {
            const data = JSON.parse(decrypted);
            if (data.summary) {
              messages.push({
                role: 'system',
                content: `[System]: Here is a summary of the preceding conversation:\n${data.summary}`
              });
            }
          } catch (e) {
            messages.push({
              role: 'system',
              content: `[System]: Here is a summary of the preceding conversation:\n${decrypted}`
            });
          }
        }
        continue;
      }

      if (itemType === 'compaction_trigger') {
        messages.push({
          role: 'user',
          content: 'Summarize the conversation so far. Please write the summary in Chinese. Focus on technical details, plans, decisions, and outcomes. Keep it structured and concise. (请使用中文对目前的对话进行总结，重点关注技术细节、计划、决定和结果，保持结构化和简洁。)'
        });
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

    // NOTE: We intentionally do NOT add a separate 'reasoning' output item to the output array.
    // The reasoning/thinking block is correctly emitted via streaming SSE events
    // (response.output_item.added / response.output_item.done for type:'reasoning'),
    // but including it in the final output array causes Codex CLI's remote compaction v2
    // to fail with "expected exactly one compaction output item, got 0 from N output items"
    // because compaction expects a single message item with non-empty text.

    // When the model returns only reasoning and no text (or whitespace-only text),
    // use the reasoning as fallback text so the message item is always non-empty.
    // Use trim() to treat whitespace-only text as empty (some models return '\n' etc).
    const effectiveText = this.text.trim().length > 0 ? this.text : this.reasoning;

    if (this.messageItemID || Object.keys(this.toolCalls).length === 0) {
      outputs.push({
        type: 'message',
        id: this.messageItemID || generateItemID(),
        role: 'assistant',
        content: [{
          type: 'output_text',
          text: effectiveText
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

export function finalizeStream(state, res, isCompact = false) {
  if (state.completedSent) return;
  
  // DEBUG: Log state at finalization to diagnose compaction issues
  logger.info(`[finalizeStream] DEBUG model=${state.model} text.len=${state.text.length} reasoning.len=${state.reasoning.length} messageItemID=${state.messageItemID} toolCalls=${Object.keys(state.toolCalls).length}`);
  
  ensureCreated(state, res);
  closeReasoningItem(state, res);
  
  // Unified fallback: fires when no message item was opened during streaming
  // (model returned only thinking, nothing at all, or an empty stream).
  // Without this, no SSE response.output_item.done is emitted, causing Codex
  // compaction to fail with "got 0 from 0 output items" because Codex counts
  // from SSE events, NOT from response.completed.response.output.
  if (!state.messageItemID && Object.keys(state.toolCalls).length === 0) {
    // Determine fallback text:
    //   Case 1 (model returned only reasoning): use reasoning as summary text
    //   Case 2 (model returned absolutely nothing): use placeholder so Codex
    //           compaction gets 1 valid item instead of crashing
    const fallbackText = state.reasoning.trim().length > 0
      ? state.reasoning
      : '[Context summary unavailable: model returned empty response. Please retry.]';

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
  
  // Close message
  if (state.messageItemID) {
    // Use effectiveText (same as getChatOutput) for the final output_item.done event so
    // Codex compaction sees a non-empty text even when state.text is whitespace-only.
    // This matches the case where model streams '\n' as text but has non-empty reasoning:
    // the streamed delta is '\n', but the finalized item should contain the reasoning content.
    const effectiveText = state.text.trim().length > 0 ? state.text : state.reasoning;
    
    if (state.textPartOpen) {
      writeResponsesStreamData(res, 'response.output_text.done', {
        type: 'response.output_text.done',
        sequence_number: state.sequenceNumber++,
        output_index: state.messageIndex,
        content_index: 0,
        item_id: state.messageItemID,
        text: effectiveText
      });
      
      writeResponsesStreamData(res, 'response.content_part.done', {
        type: 'response.content_part.done',
        sequence_number: state.sequenceNumber++,
        output_index: state.messageIndex,
        content_index: 0,
        item_id: state.messageItemID,
        part: {
          type: 'output_text',
          text: effectiveText
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
        content: [{ type: 'output_text', text: effectiveText }],
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
  
  if (isCompact) {
    const encrypted = encrypt(JSON.stringify({ summary: state.text }));
    const compactionItemID = generateItemID();
    const compactionIndex = state.allocOutputIndex();
    
    writeResponsesStreamData(res, 'response.output_item.added', {
      type: 'response.output_item.added',
      sequence_number: state.sequenceNumber++,
      output_index: compactionIndex,
      item: {
        type: 'compaction_summary',
        id: compactionItemID,
        encrypted_content: encrypted
      }
    });
    
    writeResponsesStreamData(res, 'response.output_item.done', {
      type: 'response.output_item.done',
      sequence_number: state.sequenceNumber++,
      output_index: compactionIndex,
      item: {
        type: 'compaction_summary',
        id: compactionItemID,
        encrypted_content: encrypted,
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
  
  const responsePayload = isCompact
    ? createResponsesNonStreamResponse(state, true)
    : {
        id: state.responseID,
        object: 'response',
        model: state.model,
        status: status,
        output: state.getChatOutput(),
        usage: state.usage
      };
  
  if (!isCompact && incompleteDetails) {
    responsePayload.incomplete_details = incompleteDetails;
  }
  
  writeResponsesStreamData(res, 'response.completed', {
    type: 'response.completed',
    sequence_number: state.sequenceNumber++,
    response: responsePayload
  });
}

function createResponsesNonStreamResponse(state, isCompact = false) {
  let status = 'completed';
  let incompleteDetails = null;
  if (state.finishReason === 'length') {
    status = 'incomplete';
    incompleteDetails = { reason: 'max_output_tokens' };
  }
  
  if (isCompact) {
    const encrypted = encrypt(JSON.stringify({ summary: state.text }));
    const output = [
      {
        id: generateItemID(),
        type: 'message',
        status: 'completed',
        role: 'user',
        content: [{ type: 'input_text', text: 'Compacted conversation history' }]
      },
      {
        id: state.messageItemID || generateItemID(),
        type: 'message',
        status: 'completed',
        role: 'assistant',
        phase: 'final_answer',
        content: [{ type: 'output_text', text: state.text }]
      },
      {
        id: generateItemID(),
        type: 'compaction_summary',
        encrypted_content: encrypted
      }
    ];

    return {
      id: state.responseID,
      object: 'response.compaction',
      created_at: Math.floor(Date.now() / 1000),
      status: status,
      output: output,
      usage: state.usage
    };
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
  let { model } = body;
  const bodyKeys = Object.keys(body);
  const inputLength = body.input ? body.input.length : 0;
  const hasTools = !!body.tools;
  const instructionsSnippet = body.instructions ? body.instructions.slice(0, 150) : '';
  let isCompact = (typeof model === 'string' && model.endsWith('-openai-compact')) || req.path.endsWith('/compact');
  if (!isCompact && body.client_metadata) {
    let turnMeta = body.client_metadata['x-codex-turn-metadata'];
    if (typeof turnMeta === 'string') {
      try {
        const metadata = JSON.parse(turnMeta);
        if (metadata && metadata.request_kind === 'compaction') {
          isCompact = true;
        }
      } catch (e) {
        // ignore parse error
      }
    }
  }
  if (!isCompact && req.headers['x-codex-turn-metadata']) {
    try {
      const metadata = JSON.parse(req.headers['x-codex-turn-metadata']);
      if (metadata && metadata.request_kind === 'compaction') {
        isCompact = true;
      }
    } catch (e) {
      // ignore parse error
    }
  }
  if (!isCompact && typeof body.instructions === 'string' && body.instructions.includes('Summarize the conversation')) {
    isCompact = true;
  }
  if (isCompact) {
    model = 'gemini-3.5-flash-low';
  }
  logger.info(`[responses] INCOMING REQUEST path=${req.path} originalUrl=${req.originalUrl} model=${model} isCompact=${isCompact} bodyKeys=${JSON.stringify(bodyKeys)} inputLength=${inputLength} hasTools=${hasTools} instructionsSnippet="${instructionsSnippet}" headers=${JSON.stringify(req.headers)}`);
  const { input, instructions, tools, tool_choice, max_output_tokens, temperature, top_p, reasoning } = body;
  const stream = (body.stream === true);

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

    // 0. 判断是否为非反重力模型（即 GPT 模型等，需要代理到 sub2api）
    let cleanModel = model;
    if (model.endsWith('-openai-compact')) {
      cleanModel = model.slice(0, -15);
    }
    const isAntigravity = cleanModel.startsWith('gemini') || 
                          cleanModel.startsWith('claude') || 
                          cleanModel === 'rev19-uic3-1p' || 
                          cleanModel === 'gpt-oss-120b-medium';
    
    if (!isAntigravity) {
      logger.info(`非反重力模型 ${model}，代理到 sub2api`);
      const sub2ApiKey = process.env.SUB2API_KEY || 'sk-2f4e9c13844bdac1a3a174152ae6d6ae9c1cbf474909176240db2ee513322e88';
      const headers = {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${sub2ApiKey}`
      };
      
      try {
        const targetUrl = isCompact ? 'https://sub2.jinyus.top/v1/responses/compact' : `https://sub2.jinyus.top${req.originalUrl}`;
        const sub2Response = await axios({
          method: 'post',
          url: targetUrl,
          data: body,
          headers: headers,
          timeout: 300000,
          responseType: stream ? 'stream' : 'json'
        });
        
        if (stream) {
          res.status(sub2Response.status);
          for (const [k, v] of Object.entries(sub2Response.headers)) {
            res.setHeader(k, v);
          }
          sub2Response.data.pipe(res);
        } else {
          return res.status(sub2Response.status).json(sub2Response.data);
        }
        return;
      } catch (err) {
        logger.error(`代理到 sub2api 失败: ${err.message}`);
        const statusCode = err.response?.status || 500;
        return res.status(statusCode).json(err.response?.data || { error: { message: err.message } });
      }
    }

    // 否则是反重力模型，我们本地处理
    model = cleanModel;

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

    // 3. Map Responses API 'reasoning.effort' → thinking_budget.
    //    Codex sends reasoning.effort for normal chat (from model_reasoning_effort config).
    //    Compaction requests typically do NOT include reasoning field.
    //
    //    IMPORTANT: Do NOT set thinking_budget=0 for compaction (no-reasoning) requests.
    //    gemini-* models (e.g. gemini-3.5-flash-low) REQUIRE thinking to generate text.
    //    With thinkingBudget:0 the API disables thinking entirely and the model returns
    //    empty output → state.text='' → Codex rejects: "got 0 from 1 output items".
    //
    //    Instead: when no reasoning field is present (compaction), leave thinking_budget
    //    unset so toGenerationConfig uses the config default (small budget, includeThoughts:true).
    //    The thinking content goes to state.reasoning; getChatOutput() returns the
    //    message item using state.text (the actual compaction summary), giving Codex
    //    exactly 1 valid output item.
    const effort = reasoning?.effort || body.reasoning_effort;
    if (effort !== undefined) {
      const EFFORT_BUDGET_MAP = { low: 1024, medium: 4096, high: 8192, xhigh: 16000 };
      params.thinking_budget = EFFORT_BUDGET_MAP[effort] ?? 4096;
    }
    // else: no thinking_budget set → toGenerationConfig uses config default → thinking enabled

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
          finalizeStream(state, res, isCompact);
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

          finalizeStream(state, res, isCompact);
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

      logger.info(`[nostream] DEBUG model=${state.model} text.len=${state.text.length} reasoning.len=${state.reasoning.length} messageItemID=${state.messageItemID}`);
      res.json(createResponsesNonStreamResponse(state, isCompact));
    }
  } catch (error) {
    logger.error('Responses 生成响应失败:', error.message);
    if (res.headersSent) return;
    const statusCode = error.statusCode || error.status || 500;
    return res.status(statusCode).json(buildOpenAIErrorPayload(error, statusCode));
  }
};
