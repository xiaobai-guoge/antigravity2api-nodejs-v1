import {
  responsesInputToChatMessages,
  normalizeChatMessages,
  responsesToolsToChatTools,
  responsesToolChoiceToChatToolChoice
} from '../src/server/handlers/responses.js';

// 1. 测试输入转换为 chat messages
const instructions = "You are a coding assistant.";
const input = [
  {
    role: "user",
    type: "text",
    text: "Hello, model!"
  },
  {
    role: "assistant",
    type: "reasoning",
    summary: "Thinking about hello..."
  },
  {
    role: "assistant",
    type: "message",
    content: [
      {
        type: "output_text",
        text: "Hi! How can I help you today?"
      }
    ]
  },
  {
    role: "user",
    type: "text",
    text: "Call get_weather for Beijing."
  },
  {
    role: "assistant",
    type: "reasoning",
    summary: "Need to call weather tool..."
  },
  {
    role: "assistant",
    type: "function_call",
    call_id: "call_weather_1",
    name: "get_weather",
    arguments: JSON.stringify({ city: "Beijing" })
  },
  {
    role: "tool",
    type: "function_call_output",
    call_id: "call_weather_1",
    output: "Sunny, 22°C"
  }
];

console.log("=== Testing Input Conversion ===");
const convertedMessages = responsesInputToChatMessages(instructions, input);
console.log("Converted Messages:");
console.log(JSON.stringify(convertedMessages, null, 2));

console.log("\n=== Testing Message Normalization ===");
const normalizedMessages = normalizeChatMessages(convertedMessages);
console.log("Normalized Messages:");
console.log(JSON.stringify(normalizedMessages, null, 2));

// 验证转换正确性
const systemMsg = normalizedMessages[0];
if (systemMsg.role !== "system" || systemMsg.content !== instructions) {
  console.error("FAIL: System message conversion failed");
  process.exit(1);
}

const userMsg = normalizedMessages[1];
if (userMsg.role !== "user" || userMsg.content !== "Hello, model!") {
  console.error("FAIL: User text message conversion failed");
  process.exit(1);
}

const assistantMsg = normalizedMessages[2];
if (assistantMsg.role !== "assistant" || assistantMsg.content !== "Hi! How can I help you today?") {
  console.error("FAIL: Assistant message content conversion failed");
  process.exit(1);
}

const assistantToolCallMsg = normalizedMessages[4];
if (assistantToolCallMsg.role !== "assistant" || !assistantToolCallMsg.tool_calls || assistantToolCallMsg.tool_calls[0].id !== "call_weather_1" || assistantToolCallMsg.reasoning_content !== "Need to call weather tool...") {
  console.error("FAIL: Tool call message or reasoning merge failed", assistantToolCallMsg);
  process.exit(1);
}

const toolMsg = normalizedMessages[5];
if (toolMsg.role !== "tool" || toolMsg.tool_call_id !== "call_weather_1" || toolMsg.content !== "Sunny, 22°C") {
  console.error("FAIL: Tool response message conversion failed", toolMsg);
  process.exit(1);
}

console.log("\n✓ All conversion tests PASSED successfully!");
