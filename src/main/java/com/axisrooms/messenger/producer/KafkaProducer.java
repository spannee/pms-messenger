package com.axisrooms.messenger.producer;

import com.axisrooms.messenger.util.CompletableFutureUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Async("threadPoolTaskExecutor")
    public CompletableFuture<SendResult<String, String>> publish(String topic, String message) {
        ListenableFuture<SendResult<String, String>> listenableFuture = kafkaTemplate.send(topic, message);

        CompletableFuture<SendResult<String, String>> completableFuture = CompletableFutureUtil.buildCompletableFuture(listenableFuture);

        return completableFuture;
    }
}
