package com.axisrooms.messenger.controller;

import com.axisrooms.messenger.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Slf4j
@RestController
public class PMSController {

    @Autowired
    private KafkaProducer kafkaPublisherImpl;

    @RequestMapping(path="/api/daywisePrice", method= RequestMethod.POST)
    public DeferredResult<String> asyncDaywisePricePublisher(@RequestBody String daywisePrice) {
        DeferredResult<String> response = new DeferredResult<>();

        log.info("Started processing the following request {}", daywisePrice);
        kafkaPublisherImpl.publish("daywisePrice", daywisePrice).
                whenCompleteAsync((result, throwable) -> {
                    if(throwable == null)
                        response.setResult("The request has been received");
                    else
                        response.setResult("The request has not been received");
                });
        log.info("Servlet thread released for the request {}", daywisePrice);

        return response;
    }

    @RequestMapping(path="/api/bulkPriceUpdate", method= RequestMethod.POST)
    public DeferredResult<String> asyncBulkPricePublisher(@RequestBody String bulkPrice) {
        DeferredResult<String> response = new DeferredResult<>();

        log.info("Started processing the following request {}", bulkPrice);
        kafkaPublisherImpl.publish("bulkPrice", bulkPrice).
                whenCompleteAsync((result, throwable) -> {
                    if(throwable == null)
                        response.setResult("The request has been received");
                    else
                        response.setResult("The request has not been received");
                });
        log.info("Servlet thread released for the request {}", bulkPrice);

        return response;
    }

    @RequestMapping(path="/api/daywiseInventory", method= RequestMethod.POST)
    public DeferredResult<String> asyncDaywiseInventoryPublisher(@RequestBody String daywiseInventory) {
        DeferredResult<String> response = new DeferredResult<>();

        log.info("Started processing the following request {}", daywiseInventory);
        kafkaPublisherImpl.publish("daywiseInventory", daywiseInventory).
                whenCompleteAsync((result, throwable) -> response.setResult("The request has been received"));
        log.info("Servlet thread released for the request {}", daywiseInventory);

        return response;
    }

    @RequestMapping(path="/api/bulkInventory", method= RequestMethod.POST)
    public DeferredResult<String> asyncBulkInventoryPublisher(@RequestBody String bulkInventory) {
        DeferredResult<String> response = new DeferredResult<>();

        log.info("Started processing the following request {}", bulkInventory);
        kafkaPublisherImpl.publish("bulkInventory", bulkInventory).
                whenCompleteAsync((result, throwable) -> response.setResult("The request has been received"));
        log.info("Servlet thread released for the request {}", bulkInventory);

        return response;
    }

    @RequestMapping(path="/api/blockChannel", method= RequestMethod.POST)
    public DeferredResult<String> asyncBlockChannelPublisher(@RequestBody String blockChannelReq) {
        DeferredResult<String> response = new DeferredResult<>();

        log.info("Started processing the following request {}", blockChannelReq);
        kafkaPublisherImpl.publish("blockChannel", blockChannelReq).
                whenCompleteAsync((result, throwable) -> response.setResult("The request has been received"));
        log.info("Servlet thread released for the request {}", blockChannelReq);

        return response;
    }

    @RequestMapping(path="/api/unblockChannel", method= RequestMethod.POST)
    public DeferredResult<String> asyncUnblockChannelPublisher(@RequestBody String unblockChannelReq) {
        DeferredResult<String> response = new DeferredResult<>();

        log.info("Started processing the following request {}", unblockChannelReq);
        kafkaPublisherImpl.publish("unblockChannel", unblockChannelReq).
                whenCompleteAsync((result, throwable) -> response.setResult("The request has been received"));
        log.info("Servlet thread released for the request {}", unblockChannelReq);

        return response;
    }

    @RequestMapping(path="/api/failedRequests", method= RequestMethod.POST)
    @Async("threadPoolTaskExecutor")
    public DeferredResult<String> asyncFailedRequestsPublisher(@RequestBody String failedRequest) {
        DeferredResult<String> response = new DeferredResult<>();

        log.info("Started processing the following request {}", failedRequest);
        kafkaPublisherImpl.publish("failedRequests", failedRequest).
                whenCompleteAsync((result, throwable) -> response.setResult("The request has been received"));
        log.info("Servlet thread released for the request {}", failedRequest);

        return response;
    }

}
