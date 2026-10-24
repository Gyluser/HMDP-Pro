package com.hmdp.utils;

import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.data.message.AiMessage;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.message.ChatMessageSerializer;
import dev.langchain4j.data.message.SystemMessage;
import dev.langchain4j.data.message.ToolExecutionResultMessage;
import dev.langchain4j.memory.ChatMemory;
import dev.langchain4j.model.chat.ChatModel;
import dev.langchain4j.store.memory.chat.ChatMemoryStore;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Redis 持久化的会话记忆，超过窗口后先调用模型压缩旧消息。
 */
@Slf4j
public class SummarizingChatMemory implements ChatMemory {

    private static final int MAX_MESSAGES = 20;
    private static final int RECENT_MESSAGES_TO_KEEP = 8;
    private static final int SUMMARY_TIMEOUT_SECONDS = 15;
    private static final String SUMMARY_PREFIX = "[AI_CONVERSATION_SUMMARY] ";
    private static final ConcurrentHashMap<String, Object> LOCKS = new ConcurrentHashMap<>();
    private static final ExecutorService SUMMARY_EXECUTOR = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "ai-memory-summary");
        thread.setDaemon(true);
        return thread;
    });

    private final Object id;
    private final ChatMemoryStore store;
    private final ChatModel summaryModel;

    public SummarizingChatMemory(Object id, ChatMemoryStore store, ChatModel summaryModel) {
        this.id = id;
        this.store = store;
        this.summaryModel = summaryModel;
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public void add(ChatMessage message) {
        Object lock = LOCKS.computeIfAbsent(String.valueOf(id), ignored -> new Object());
        synchronized (lock) {
            // LangChain4j 会先写入 AiMessage(tool_calls)，再写入对应的
            // ToolExecutionResultMessage。保存第二条消息时，前一条工具调用
            // 仍然是“未配对完成”的中间态，不能在这里被清理掉，否则 Redis
            // 最终只剩 role=tool 的孤立消息，下一次请求就会被模型拒绝。
            List<ChatMessage> messages = new ArrayList<>(messagesForAppend(message));
            messages.add(message);
            if (messages.size() > MAX_MESSAGES) {
                messages = summarizeOrTrim(messages);
            }
            store.updateMessages(id, messages);
        }
    }

    @Override
    public List<ChatMessage> messages() {
        Object lock = lock();
        synchronized (lock) {
            return Collections.unmodifiableList(new ArrayList<>(safeMessages()));
        }
    }

    @Override
    public void clear() {
        Object lock = lock();
        synchronized (lock) {
            store.deleteMessages(id);
        }
    }

    private Object lock() {
        return LOCKS.computeIfAbsent(String.valueOf(id), ignored -> new Object());
    }

    private List<ChatMessage> safeMessages() {
        List<ChatMessage> messages = store.getMessages(id);
        if (messages == null || messages.isEmpty()) {
            return Collections.emptyList();
        }

        // 工具调用必须保持 AiMessage(tool_calls) -> ToolExecutionResultMessage 的完整顺序。
        // 旧版本按固定下标截断，可能只留下 tool 消息，导致模型拒绝整个请求。
        List<ChatMessage> repaired = repairToolMessagePairs(messages);
        if (repaired.size() != messages.size() || !repaired.equals(messages)) {
            log.warn("修复不完整的 AI 工具调用消息，memoryId={}, 原消息数={}, 修复后={}",
                    id, messages.size(), repaired.size());
            store.updateMessages(id, repaired);
        }
        return repaired;
    }

    private List<ChatMessage> messagesForAppend(ChatMessage message) {
        List<ChatMessage> raw = store.getMessages(id);
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyList();
        }

        // 工具结果到达时保留尚未完成的 AiMessage(tool_calls)，让本次追加
        // 能形成完整的 assistant(tool_calls) -> tool(result) 消息对。
        if (message instanceof ToolExecutionResultMessage) {
            return repairToolMessagePairs(raw, true);
        }
        return repairToolMessagePairs(raw, false);
    }

    private List<ChatMessage> summarizeOrTrim(List<ChatMessage> messages) {
        int recentStart = safeRecentStart(messages,
                Math.max(0, messages.size() - RECENT_MESSAGES_TO_KEEP));
        List<ChatMessage> oldMessages = new ArrayList<>(messages.subList(0, recentStart));
        List<ChatMessage> recentMessages = new ArrayList<>(messages.subList(recentStart, messages.size()));
        if (oldMessages.isEmpty()) {
            return trimToLimit(messages);
        }

        try {
            String prompt = "请把下面的餐饮助手历史对话压缩成一段简洁的中文摘要。"
                    + "保留用户偏好、已确认的店铺、时间、预约、优惠券和未完成事项；"
                    + "不要编造信息，不要输出标题，只输出摘要。\n历史对话：\n"
                    + ChatMessageSerializer.messagesToJson(oldMessages);
            Future<String> summaryTask = SUMMARY_EXECUTOR.submit(() -> summaryModel.chat(prompt));
            String summary;
            try {
                summary = summaryTask.get(SUMMARY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                summaryTask.cancel(true);
                log.warn("AI 会话摘要超时，回退到安全截断，memoryId={}", id);
                return trimToLimit(messages);
            } catch (InterruptedException e) {
                summaryTask.cancel(true);
                Thread.currentThread().interrupt();
                log.warn("AI 会话摘要被中断，回退到安全截断，memoryId={}", id);
                return trimToLimit(messages);
            } catch (ExecutionException e) {
                throw new IllegalStateException("摘要模型调用失败", e.getCause());
            }
            if (summary != null && !summary.isBlank()) {
                List<ChatMessage> result = new ArrayList<>(recentMessages.size() + 1);
                result.add(SystemMessage.from(SUMMARY_PREFIX + summary.trim()));
                result.addAll(recentMessages);
                return trimToLimit(result);
            }
        } catch (Exception e) {
            // 摘要失败不能让正常聊天失败，回退到普通窗口截断。
            log.warn("AI 会话摘要失败，回退到最近消息，memoryId={}", id, e);
        }
        return trimToLimit(messages);
    }

    private List<ChatMessage> trimToLimit(List<ChatMessage> messages) {
        if (messages.size() <= MAX_MESSAGES) {
            return messages;
        }
        int start = safeRecentStart(messages, messages.size() - MAX_MESSAGES);
        return new ArrayList<>(messages.subList(start, messages.size()));
    }

    /**
     * 从完整的消息边界开始保留最近消息，不能从工具调用链中间切断。
     */
    private int safeRecentStart(List<ChatMessage> messages, int proposedStart) {
        int start = Math.max(0, proposedStart);
        while (start > 0) {
            ChatMessage current = messages.get(start);
            ChatMessage previous = messages.get(start - 1);
            if (isToolResult(current)
                    || isToolResult(previous)
                    || isToolCall(current)) {
                start--;
                continue;
            }
            break;
        }
        return start;
    }

    /**
     * 删除历史 Redis 中已经存在的孤立 tool 消息或未完成的工具调用链。
     * 完整的工具调用链原样保留，避免破坏 LangChain4j 的消息协议。
     */
    private List<ChatMessage> repairToolMessagePairs(List<ChatMessage> messages) {
        return repairToolMessagePairs(messages, false);
    }

    private List<ChatMessage> repairToolMessagePairs(List<ChatMessage> messages,
                                                       boolean keepIncompleteToolCalls) {
        List<ChatMessage> repaired = new ArrayList<>();
        for (int i = 0; i < messages.size(); i++) {
            ChatMessage message = messages.get(i);

            if (message instanceof AiMessage aiMessage && aiMessage.hasToolExecutionRequests()) {
                List<ToolExecutionRequest> requests = aiMessage.toolExecutionRequests();
                List<ToolExecutionResultMessage> results = new ArrayList<>();
                int next = i + 1;
                while (next < messages.size()
                        && messages.get(next) instanceof ToolExecutionResultMessage resultMessage) {
                    results.add(resultMessage);
                    next++;
                }

                // LangChain4j 会按 tool call 的顺序逐条追加结果。不能只用
                // result.id == request.id 判断是否完整：Redis 序列化/反序列化
                // 或兼容 OpenAI 的模型可能让结果 ID 发生变化，但相邻消息的
                // 顺序和数量仍然代表同一轮工具调用。
                boolean complete = !requests.isEmpty()
                        // 只要相邻结果数量与请求数量一致，就按原始顺序配对。
                        && results.size() == requests.size();
                if (complete) {
                    repaired.add(aiMessage);
                    repaired.addAll(results);
                } else if (keepIncompleteToolCalls) {
                    // 这是工具调用和工具结果之间的合法中间态；结果消息
                    // 将在本次 add() 后紧接着写入。已有结果也必须保留，
                    // 否则多工具调用逐条写入时会丢掉前面的结果。
                    repaired.add(aiMessage);
                    repaired.addAll(results);
                }
                // 无论完整与否，都跳过这段旧工具调用，避免重复加入孤立消息。
                i = next - 1;
                continue;
            }

            if (isToolResult(message)) {
                // 没有紧邻合法 AiMessage(tool_calls) 的 tool 结果不能发送给模型。
                continue;
            }
            repaired.add(message);
        }
        return repaired;
    }

    private boolean isToolResult(ChatMessage message) {
        return message instanceof ToolExecutionResultMessage;
    }

    private boolean isToolCall(ChatMessage message) {
        return message instanceof AiMessage aiMessage && aiMessage.hasToolExecutionRequests();
    }
}
