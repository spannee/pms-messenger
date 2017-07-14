package com.axisrooms.messenger.util;


import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class CompletableFutureUtil {

    public static CompletableFuture<SendResult<String, String>> buildCompletableFuture(final ListenableFuture<SendResult<String, String>> listenableFuture) {
        CompletableFuture<SendResult<String, String>> completable = new CompletableFuture();

        listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                completable.complete(result);
            }

            @Override
            public void onFailure(Throwable t) {
                completable.completeExceptionally(t);
            }
        });

        return completable;
    }
}
