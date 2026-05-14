package com.example.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Map;

public class AuctionLambda implements RequestHandler<Map<String, Object>, String> {

    private static final String SQS_QUEUE_URL = System.getenv("SQS_QUEUE_URL");
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final AmazonSQS sqsClient = AmazonSQSClientBuilder.defaultClient();

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        try {
            Long auctionId = Long.valueOf(event.get("auctionId").toString());
            String action = event.get("action").toString();
            String targetTimeStr = event.get("targetTime").toString(); // 경매시작/종료시간

            LocalDateTime targetTime = LocalDateTime.parse(targetTimeStr);

            long millis = Duration.between(LocalDateTime.now(), targetTime).toMillis();
            long delaySeconds = millis <= 0 ? 0 : (millis + 999) / 1000;

            delaySeconds = Math.max(0, Math.min(delaySeconds, 900));

            String payload = objectMapper.writeValueAsString(Map.of(
                    "auctionId", auctionId,
                    "action", action
            ));

            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(SQS_QUEUE_URL)
                    .withMessageBody(payload)
                    .withDelaySeconds((int) delaySeconds));

            return "OK";

        } catch (Exception e) {
            context.getLogger().log("[SQS] 발행 실패: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }
}