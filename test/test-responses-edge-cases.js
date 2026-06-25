import {
  responsesInputToChatMessages,
  normalizeChatMessages,
  responsesToolsToChatTools,
  responsesToolChoiceToChatToolChoice
} from '../src/server/handlers/responses.js';

console.log("=== Running Edge Cases and Parameter Translation Tests ===\n");

// 1. 测试图片多模态输入转换
const multimodalInput = [
  {
    role: "user",
    type: "message",
    content: [
      {
        type: "input_text",
        text: "Describe this image:"
      },
      {
        type: "input_image",
        image_url: {
          url: "data:image/png;base64,iVBORw0KGgoAAA..."
        }
      }
    ]
  }
];

const imgMessages = responsesInputToChatMessages(null, multimodalInput);
console.log("✓ 多模态图片输入转译测试:");
console.log(JSON.stringify(imgMessages, null, 2));

if (imgMessages[0].role !== 'user' || !Array.isArray(imgMessages[0].content)) {
  console.error("FAIL: Multimodal image translation failed");
  process.exit(1);
}
if (imgMessages[0].content[0].type !== 'text' || imgMessages[0].content[1].type !== 'image_url') {
  console.error("FAIL: Multimodal parts mapping failed");
  process.exit(1);
}

// 2. 测试工具选择 (tool_choice) 的翻译
// 场景 A: string "auto"
const choiceAuto = responsesToolChoiceToChatToolChoice("auto");
console.log("✓ tool_choice = 'auto' 映射:", choiceAuto);
if (choiceAuto !== "auto") {
  console.error("FAIL: tool_choice auto mapping failed");
  process.exit(1);
}

// 场景 B: string "none"
const choiceNone = responsesToolChoiceToChatToolChoice("none");
console.log("✓ tool_choice = 'none' 映射:", choiceNone);
if (choiceNone !== "none") {
  console.error("FAIL: tool_choice none mapping failed");
  process.exit(1);
}

// 场景 C: 对象形式 {"type": "function", "function": {"name": "my_tool"}}
const choiceObj = responsesToolChoiceToChatToolChoice({
  type: "function",
  function: { name: "my_tool" }
});
console.log("✓ tool_choice = 对象 映射:", JSON.stringify(choiceObj));
if (choiceObj.type !== "function" || choiceObj.function.name !== "my_tool") {
  console.error("FAIL: tool_choice object mapping failed");
  process.exit(1);
}

// 3. 边界测试: 极简/空输入情况
const emptyInput = [];
const emptyMessages = responsesInputToChatMessages("", emptyInput);
console.log("✓ 空输入转换测试 (应该返回空数组):", JSON.stringify(emptyMessages));
if (emptyMessages.length !== 0) {
  console.error("FAIL: Empty input should yield empty messages list");
  process.exit(1);
}

// 4. 测试 normalize 对悬挂 tool 响应 (orphan tool responses) 的处理
const danglingMessages = [
  {
    role: "user",
    content: "Hi"
  },
  {
    role: "tool",
    tool_call_id: "call_orphan",
    content: "Result of orphan tool"
  }
];

const normalizedDangling = normalizeChatMessages(danglingMessages);
console.log("✓ 悬空 Tool 结果清理测试:");
console.log(JSON.stringify(normalizedDangling, null, 2));

// 悬空的 tool 响应应该被过滤掉以防 Gemini 报错，只保留 user 消息
if (normalizedDangling.length !== 1 || normalizedDangling[0].role !== "user") {
  console.error("FAIL: Orphan tool response filtering failed");
  process.exit(1);
}

console.log("\n✓ All edge case tests PASSED successfully!");
