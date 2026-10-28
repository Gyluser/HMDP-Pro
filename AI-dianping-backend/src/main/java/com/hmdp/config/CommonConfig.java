package com.hmdp.config;

import dev.langchain4j.data.document.Document;
import dev.langchain4j.data.document.loader.ClassPathDocumentLoader;
import dev.langchain4j.memory.chat.ChatMemoryProvider;
import dev.langchain4j.model.chat.StreamingChatModel;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import dev.langchain4j.rag.content.retriever.ContentRetriever;
import dev.langchain4j.rag.content.retriever.EmbeddingStoreContentRetriever;
import dev.langchain4j.service.AiServices;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.EmbeddingStoreIngestor;
import dev.langchain4j.store.embedding.inmemory.InMemoryEmbeddingStore;
import dev.langchain4j.store.memory.chat.ChatMemoryStore;
import com.hmdp.service.ConsultantService;
import com.hmdp.utils.ReservationTool;
import com.hmdp.utils.SummarizingChatMemory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.List;

@Configuration
public class CommonConfig {

    private final OpenAiChatModel model;

    private final EmbeddingModel embeddingModel;

    private final ChatMemoryStore redisChatMemoryStore;

    public CommonConfig(OpenAiChatModel model,
                        EmbeddingModel embeddingModel,
                        ChatMemoryStore redisChatMemoryStore) {
        this.model = model;
        this.embeddingModel = embeddingModel;
        this.redisChatMemoryStore = redisChatMemoryStore;
    }

    @Bean
    public ChatMemoryProvider chatMemoryProvider(){
        return memoryId -> new SummarizingChatMemory(memoryId, redisChatMemoryStore, model);
    }

    @Bean
    public ConsultantService consultantService(
            StreamingChatModel streamingChatModel,
            ChatMemoryProvider chatMemoryProvider,
            ContentRetriever contentRetriever,
            ReservationTool reservationTool) {
        return AiServices.builder(ConsultantService.class)
                .chatModel(model)
                .streamingChatModel(streamingChatModel)
                .chatMemoryProvider(chatMemoryProvider)
                .contentRetriever(contentRetriever)
                .tools(reservationTool)
                // 模型误重复调用工具时，最多允许 5 次，避免请求无限占用
                // 后端线程并最终被 Nginx 返回 504。
                .maxSequentialToolsInvocations(5)
                .build();
    }

    @Bean
    public EmbeddingStore store(){        // 返回向量存储组件对象
        // 1.加载文档进内存
        List<Document> documents = ClassPathDocumentLoader.loadDocuments("content");
        // 2.创建向量存储组件对象
        InMemoryEmbeddingStore store = new InMemoryEmbeddingStore();
        // 3.把文档转换为向量并存储到向量存储组件对象中
        EmbeddingStoreIngestor ingestor = EmbeddingStoreIngestor
                .builder()
                .embeddingModel(embeddingModel)
                .embeddingStore(store)
                .build();
        ingestor.ingest(documents);
        return store;
    }

    @Bean
    public ContentRetriever contentRetriever(EmbeddingStore store){ // 检索器构建
        return EmbeddingStoreContentRetriever.builder()
                .embeddingStore(store)//设置向量数据库操作对象
                .embeddingModel(embeddingModel)
                .minScore(0.5)//设置最小分数
                .maxResults(3)//设置最大片段数量
                .build();
    }
}
