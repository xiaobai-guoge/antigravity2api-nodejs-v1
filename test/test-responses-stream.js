import {
  StreamState,
  processChunkData,
  finalizeStream
} from '../src/server/handlers/responses.js';

class MockResponse {
  constructor() {
    this.written = [];
    this.ended = false;
  }
  
  write(data) {
    this.written.push(data.toString());
  }
  
  end() {
    this.ended = true;
  }
}

const res = new MockResponse();
const state = new StreamState("claude-sonnet-4-5", "mock_resp_123");

// Simulating chunks
const chunks = [
  { type: 'reasoning', reasoning_content: "Checking " },
  { type: 'reasoning', reasoning_content: "weather info." },
  { type: 'tool_calls', tool_calls: [{ index: 0, id: "call_w1", function: { name: "get_weather", arguments: '{"ci' } }] },
  { type: 'tool_calls', tool_calls: [{ index: 0, function: { arguments: 'ty":"Beijing"}' } }] },
  { type: 'text', content: "It is " },
  { type: 'text', content: "sunny." },
  { type: 'usage', usage: { prompt_tokens: 15, completion_tokens: 10, total_tokens: 25 } }
];

console.log("=== Simulating Stream Chunks ===");
for (const chunk of chunks) {
  processChunkData(chunk, state, res);
}
finalizeStream(state, res);

console.log("\n=== Captured SSE Events ===");
console.log(res.written.join(""));

// Verify events
const lines = res.written.join("").split("\n\n");
const events = [];
for (const line of lines) {
  if (!line.trim()) continue;
  const matchEvent = line.match(/^event: (.+)\ndata: (.*)$/s);
  if (matchEvent) {
    events.push({
      type: matchEvent[1],
      data: JSON.parse(matchEvent[2])
    });
  }
}

console.log(`\nParsed ${events.length} SSE Events:`);
events.forEach((evt, idx) => {
  console.log(`[${idx}] Type: ${evt.type}`);
  if (evt.type === 'response.completed') {
    console.log("    Completed Payload:", JSON.stringify(evt.data, null, 2));
  }
});

// Verification assertions
if (events[0].type !== "response.created") {
  console.error("FAIL: First event should be response.created");
  process.exit(1);
}

const completedEvent = events.find(e => e.type === "response.completed");
if (!completedEvent) {
  console.error("FAIL: response.completed event not found");
  process.exit(1);
}

const response = completedEvent.data.response;
if (response.id !== "mock_resp_123" || response.status !== "completed") {
  console.error("FAIL: Invalid response fields in response.completed", response);
  process.exit(1);
}

if (response.output.length !== 3) {
  console.error(`FAIL: Expected 3 output items (reasoning, function_call, message), got ${response.output.length}`);
  process.exit(1);
}

if (response.output[0].type !== 'reasoning' || response.output[0].summary[0].text !== 'Checking weather info.') {
  console.error("FAIL: Reasoning output mismatch", response.output[0]);
  process.exit(1);
}

if (response.output[1].type !== 'message' || response.output[1].content[0].text !== 'It is sunny.') {
  console.error("FAIL: Message output mismatch", response.output[1]);
  process.exit(1);
}

if (response.output[2].type !== 'function_call' || response.output[2].arguments !== '{"city":"Beijing"}') {
  console.error("FAIL: Function call output mismatch", response.output[2]);
  process.exit(1);
}

console.log("\n✓ Mock SSE Stream mapping tests PASSED successfully!");
