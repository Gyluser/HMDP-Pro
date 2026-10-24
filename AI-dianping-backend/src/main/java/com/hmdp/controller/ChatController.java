package com.hmdp.controller;

import com.hmdp.dto.UserDTO;
import com.hmdp.service.ConsultantService;
import com.hmdp.utils.AiConversationKeys;
import com.hmdp.utils.UserHolder;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/chat")
public class ChatController {

    private final ConsultantService consultantService;

    public ChatController(ConsultantService consultantService) {
        this.consultantService = consultantService;
    }

    @GetMapping(produces = "text/html;charset=utf-8")
    public Flux<String> chat(
            @RequestParam("message") String message,
            @RequestParam(value = "conversationId", required = false) String conversationId,
            @RequestParam(value = "memoryId", required = false) String legacyConversationId) {
        UserDTO user = UserHolder.getUser();
        if (user == null || user.getId() == null) {
            return Flux.error(new ResponseStatusException(HttpStatus.UNAUTHORIZED, "请先登录"));
        }
        String clientConversationId = conversationId == null ? legacyConversationId : conversationId;
        String serverMemoryId = AiConversationKeys.createMemoryId(user.getId(), clientConversationId);
        return consultantService.chat(serverMemoryId, message);
    }

}
