package com.hmdp.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * 生成并校验 AI 会话的服务端身份。
 *
 * <p>前端只提交逻辑会话号，真正的记忆 ID 和 Redis key 由服务端生成，
 * 并且始终绑定当前登录用户。</p>
 */
public final class AiConversationKeys {

    private static final String MEMORY_ID_PREFIX = "ai-user:";
    private static final String REDIS_KEY_PREFIX = "ai:chat:memory:";

    private AiConversationKeys() {
    }

    public static String createMemoryId(Long userId, String clientConversationId) {
        if (userId == null) {
            throw new IllegalArgumentException("用户未登录");
        }
        String conversationId = clientConversationId == null
                ? ""
                : clientConversationId.trim();
        if (conversationId.isEmpty()) {
            conversationId = UUID.randomUUID().toString();
        }
        if (conversationId.length() > 128) {
            conversationId = conversationId.substring(0, 128);
        }
        return MEMORY_ID_PREFIX + userId + ":" + sha256(conversationId);
    }

    public static Long userIdFromMemoryId(Object memoryId) {
        String value = requireMemoryId(memoryId);
        int separator = value.indexOf(':', MEMORY_ID_PREFIX.length());
        if (separator < 0) {
            throw new IllegalArgumentException("无效的 AI 会话身份");
        }
        try {
            return Long.valueOf(value.substring(MEMORY_ID_PREFIX.length(), separator));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("无效的 AI 会话身份", e);
        }
    }

    public static String redisKey(Object memoryId) {
        return REDIS_KEY_PREFIX + requireMemoryId(memoryId);
    }

    private static String requireMemoryId(Object memoryId) {
        String value = String.valueOf(memoryId);
        if (!value.startsWith(MEMORY_ID_PREFIX)
                || value.indexOf(':', MEMORY_ID_PREFIX.length()) < 0) {
            throw new IllegalArgumentException("无效的 AI 会话身份");
        }
        return value;
    }

    private static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] bytes = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                result.append(String.format("%02x", b));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JDK 不支持 SHA-256", e);
        }
    }
}
