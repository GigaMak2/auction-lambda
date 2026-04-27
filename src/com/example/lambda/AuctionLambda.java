package com.example.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Map;

// sqs 발행만 담당
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
            // 두 시간 차이의 초 계산(지금시간, 경매시작/종료시간)
            long delaySeconds = ChronoUnit.SECONDS.between(LocalDateTime.now(), targetTime);
            // 5분전 트리거라서 900을 넘을 일은 없지만 안전장치로 설정
            // 이미 지났으면 0으로, 최대 시간(15분) 초과면 15분(900)으로
            delaySeconds = Math.max(0, Math.min(delaySeconds, 900));

            String payload = objectMapper.writeValueAsString(Map.of(
                    "auctionId", auctionId,
                    "action", action
            ));

            sqsClient.sendMessage(new SendMessageRequest()
                    .withQueueUrl(SQS_QUEUE_URL)
                    .withMessageBody(payload)
                    .withDelaySeconds((int) delaySeconds));

            context.getLogger().log("[SQS] 메시지 발행: auctionId=" + auctionId
                    + ", action=" + action + ", delaySeconds=" + delaySeconds);
            return "OK";
        } catch (Exception e) {
            context.getLogger().log("[SQS] 발행 실패: " + e.getMessage());
            throw new RuntimeException(e);
        }
        }
    }