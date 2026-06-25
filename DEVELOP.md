# Antigravity2API 开发者指南

本文件记录了本项目的二次开发流程、与上游仓库同步的方法，以及更新部署至服务器的操作步骤。

---

## 1. 本地二次开发流程

### 环境要求
- Node.js >= 18.0.0

### 开发运行
1. 安装依赖：
   ```bash
   npm install
   ```
2. 启动本地开发服务（支持热重载）：
   ```bash
   npm run dev
   ```
   服务默认在本地 `http://localhost:8045` 启动。

### 运行测试
项目包含针对 `/v1/responses` 协议转换与 SSE 推送的测试用例：
```bash
# 运行全部转换与 SSE 协议用例
node test/test-responses.js
node test/test-responses-stream.js
node test/test-responses-edge-cases.js
```

### 二次开发设计原则
为了能够轻松同上游保持同步，请务必遵守**高内聚、低耦合**的设计：
- **新增接口与转换逻辑**：存放在独立的新文件中（如 `src/routes/responses.js` 和 `src/server/handlers/responses.js`）。
- **已有文件修改**：只在必要的地方（如 `src/server/index.js`）挂载新路由，尽量不要修改原有的系统逻辑文件。这样在合并上游更新时几乎不会产生代码冲突。

---

## 2. 与原作者上游同步代码

原作者仓库（Upstream）：`liuw1535/antigravity2api-nodejs`  
您的 Fork 仓库（Origin）：`xiaobai-guoge/antigravity2api-nodejs-v1`

### 首次配置上游源（仅需执行一次）
在您的本地工作区中添加原作者仓库作为 `upstream` 远程源：
```bash
git remote add upstream https://github.com/liuw1535/antigravity2api-nodejs.git
```

### 同步上游更新步骤
当原作者发布了新功能或修复了 Bug，您希望同步到您的代码中时：

1. **拉取上游最新代码**：
   ```bash
   git fetch upstream
   ```
2. **将上游更新合并到您的主分支**（假设当前在 `main` 或 `master`）：
   ```bash
   git checkout main
   git merge upstream/main
   ```
3. **解决可能的冲突**：
   由于我们添加的 `/v1/responses` 接口逻辑高度模块化，只有 `src/server/index.js` 有少许改动，即使有冲突也极易解决。
4. **验证测试**：
   合并后在本地运行测试套件，确保老接口与新加的 `/v1/responses` 接口均正常工作：
   ```bash
   npm test
   node test/test-responses.js
   ```
5. **推送至您的 Fork 仓库**：
   ```bash
   git push origin main
   ```

---

## 3. 服务器部署与更新流程

当本地开发完毕并推送至 GitHub 后，按照以下步骤在您的服务器上进行更新：

### 步骤 1：拉取最新代码
SSH 登录您的服务器，导航至项目部署目录，拉取最新的 GitHub 提交：
```bash
cd /opt/antigravity2api-nodejs-v1
git pull origin main
```

### 步骤 2：构建本地 Docker 镜像
基于拉取的最新代码，重新构建用于运行容器的本地镜像：
```bash
docker build -t antigravity2api-nodejs-local:latest .
```

### 步骤 3：重新部署容器
为了应用新镜像，需要重启容器。执行以下命令停止并移除当前容器，使用新镜像启动，并重新加入自定义网络（如 `new-api_default`）：

```bash
# 1. 停止当前运行的容器
docker stop antigravity2api-nodejs

# 2. 移除旧容器
docker rm antigravity2api-nodejs

# 3. 运行新容器 (请根据实际情况替换环境变量的值)
docker run -d --name antigravity2api-nodejs \
  -p 127.0.0.1:8045:8045 \
  -v /opt/antigravity2api-data:/app/data \
  -v /opt/antigravity2api-data/config.json:/app/config.json \
  --env JWT_SECRET=your_jwt_secret_here \
  --env ADMIN_USERNAME=your_admin_username_here \
  --env ADMIN_PASSWORD=your_admin_password_here \
  --env API_KEY=your_api_key_here \
  --restart unless-stopped \
  antigravity2api-nodejs-local:latest

# 4. 重新加入 new-api 的 Docker 网络（若有使用）
docker network connect new-api_default antigravity2api-nodejs
```

### 步骤 4：查看运行日志
检查容器日志，确保服务已成功启动：
```bash
docker logs -f antigravity2api-nodejs
```

### 步骤 5：测试接口可用性
在服务器或有访问权限的客户端上测试 `/v1/responses` 接口是否能正常提供流式响应：
```bash
curl -i -X POST http://127.0.0.1:8045/v1/responses \
  -H "Authorization: Bearer <你的API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gemini-2.5-flash",
    "input": [{"role": "user", "content": [{"type": "text", "text": "ping"}]}],
    "stream": true
  }'
```
正常情况下会看到响应以 `Content-Type: text/event-stream` 返回，且包含以 `event:` 开头的流式事件块。
