package com.axisrooms.messenger.controller;

import com.axisrooms.messenger.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

@Slf4j
@RestController
public class PMSController {

    @Autowired
    KafkaProducer kafkaPublisherImpl;

    @RequestMapping(path="/api/dayWisePrice", method= RequestMethod.POST)
    @Async("threadPoolTaskExecutor")
    public DeferredResult<String> asyncPricePublisher(@RequestBody String priceRequest) {
        DeferredResult<String> response = new DeferredResult<>();

        try {
            log.info("Started processing the following request {}", priceRequest);
            kafkaPublisherImpl.publish("dayWisePrice", priceRequest, response);
        } catch (Throwable e) {
            log.error("Unable to process the following request {}. Exception [" + e + "]", priceRequest);
            response.setErrorResult("Unable to process the following request due to [" + e.getMessage() + "]");
        }

        return response;
    }

    @RequestMapping(path="/api/failedRequests", method= RequestMethod.POST)
    @Async("threadPoolTaskExecutor")
    public DeferredResult<String> asyncFailedRequestsPublisher(@RequestBody String failedRequest) {
        DeferredResult<String> response = new DeferredResult<>();

        try {
            log.info("Started processing the following request {}", failedRequest);
            kafkaPublisherImpl.publish("failedRequests", failedRequest, response);
        } catch (Throwable e) {
            log.error("Unable to process the following request {}. Exception [" + e + "]", failedRequest);
            response.setErrorResult("Unable to process the following request due to [" + e.getMessage() + "]");
        }

        return response;
    }

}
