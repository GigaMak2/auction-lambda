package com.example.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AuctionSqsLambda implements RequestHandler<SQSEvent, SQSBatchResponse> {

    private static final String DB_URL = System.getenv("DB_URL");
    private static final String DB_USERNAME = System.getenv("DB_USERNAME");
    private static final String DB_PASSWORD = System.getenv("DB_PASSWORD");
    static final String REDIS_HOST = System.getenv("REDIS_HOST");
    static final int REDIS_PORT = 6379;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final JedisPool jedisPool = new JedisPool(
            new JedisPoolConfig(), REDIS_HOST, REDIS_PORT, 2000, true
    );

    private static final int EVICT_CACHE_LOOP_BATCH_SIZE = 100;
    private static final int EVICT_CACHE_LOOP_UPPER_BOUND = 100;

    private static final Properties DB_PROPS = new Properties();
    static {
        DB_PROPS.setProperty("user", DB_USERNAME);
        DB_PROPS.setProperty("password", DB_PASSWORD);
        DB_PROPS.setProperty("assumeMinServerVersion", "9.0");
    }

    private static final String ES_URL = System.getenv("ELASTICSEARCH_URIS");
    private static final String ES_AUCTION_INDEX = System.getenv("ELASTICSEARCH_AUCTION_INDEX");
    private static final String ES_USERNAME = System.getenv("ELASTICSEARCH_USERNAME");
    private static final String ES_PASSWORD = System.getenv("ELASTICSEARCH_PASSWORD");

    private static final String ES_BASIC_AUTH = "Basic " + java.util.Base64.getEncoder()
        .encodeToString((ES_USERNAME + ":" + ES_PASSWORD).getBytes(java.nio.charset.StandardCharsets.UTF_8));

    private static final HttpClient httpClient = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(2))
        .build();

    @Override
    public SQSBatchResponse handleRequest(SQSEvent event, Context context) {

        List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                Map<String, Object> body = objectMapper.readValue(message.getBody(), Map.class);
                Long auctionId = Long.valueOf(body.get("auctionId").toString());
                String action = body.get("action").toString();

                long dbStart = System.currentTimeMillis();

                try (Connection conn = DriverManager.getConnection(DB_URL, DB_PROPS)) {
                    if ("START".equals(action)) {
                        startAuction(conn, auctionId, context);
                    } else if ("END".equals(action)) {
                        endAuction(conn, auctionId, context);
                    }
                }
            } catch (Exception e) {
                context.getLogger().log("[Lambda] Error: " + e.getMessage());

                failures.add(SQSBatchResponse.BatchItemFailure.builder()
                        .withItemIdentifier(message.getMessageId())
                        .build());
            }
        }
        return SQSBatchResponse.builder()
                .withBatchItemFailures(failures)
                .build();
    }

    private void startAuction(Connection conn, Long auctionId, Context context) throws Exception {
        String sql = "UPDATE auctions SET auction_status = 'ACTIVE' WHERE id = ? AND auction_status = 'READY'";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, auctionId);

            long queryStart = System.currentTimeMillis();
            int updated = ps.executeUpdate();

            if (updated > 0) {
                long redisStart = System.currentTimeMillis();
                publishToRedis(auctionId, "AUCTION_STARTED", context);

                String selectSql = "SELECT item_name, user_id FROM auctions WHERE id = ?";
                try (PreparedStatement selectPs = conn.prepareStatement(selectSql)) {
                    selectPs.setLong(1, auctionId);
                    var rs = selectPs.executeQuery();

                    if (rs.next()) {
                        Long receiverId = rs.getLong("user_id");
                        String itemName = rs.getString("item_name");

                        publishNotification("AUCTION_STARTED", receiverId, auctionId, itemName, context);
                    }
                }

                long evictionStart = System.currentTimeMillis();
                evictCache("auction::getAuction::%s".formatted(auctionId), context);
                evictCache("auction::getManyAuctionsPublic::*", context);

                updateElasticsearchStatus(auctionId, "ACTIVE", context);
            } else {
                context.getLogger().log("[SKIP] 이미 처리된 경매: auctionId=" + auctionId);
            }
        }
    }

    private void endAuction(Connection conn, Long auctionId, Context context) throws Exception {
        String checkSql = "SELECT id, user_id, price FROM bids WHERE auction_id = ? ORDER BY price ASC LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
            ps.setLong(1, auctionId);
            var rs = ps.executeQuery();

            if (rs.next()) {
                // 낙찰 처리
                Long winnerId = rs.getLong("user_id");
                Long winnerBidId = rs.getLong("id");
                java.math.BigDecimal price = rs.getBigDecimal("price");

                String updateAuction = "UPDATE auctions SET auction_status = 'DONE' WHERE id = ? AND auction_status = 'ACTIVE'";
                try (PreparedStatement ps2 = conn.prepareStatement(updateAuction)) {
                    ps2.setLong(1, auctionId);

                    long queryStart = System.currentTimeMillis();

                    int updated = ps2.executeUpdate();

                    if (updated > 0) {
                        // 경매 결과 저장
                        String insertResult = "INSERT INTO auction_results (price, auction_id, buyer_id, seller_id, bid_id) " +
                                "SELECT ?, ?, user_id, ?, ? FROM auctions WHERE id = ? " +
                                "AND NOT EXISTS (SELECT 1 FROM auction_results WHERE auction_id = ?)";
                        try (PreparedStatement ps3 = conn.prepareStatement(insertResult)) {
                            ps3.setBigDecimal(1, price);
                            ps3.setLong(2, auctionId);
                            ps3.setLong(3, winnerId);
                            ps3.setLong(4, winnerBidId);
                            ps3.setLong(5, auctionId);
                            ps3.setLong(6, auctionId);
                            ps3.executeUpdate();
                        }

                        // 입찰 상태 CLOSED로 변경
                        String updateBids = "UPDATE bids SET status = 'CLOSED' WHERE auction_id = ?";
                        try (PreparedStatement ps4 = conn.prepareStatement(updateBids)) {
                            ps4.setLong(1, auctionId);
                            ps4.executeUpdate();
                        }

                        long redisStart = System.currentTimeMillis();
                        publishToRedis(auctionId, "AUCTION_ENDED", context);

                        String selectSql = "SELECT item_name, user_id FROM auctions WHERE id = ?";
                        try (PreparedStatement selectPs = conn.prepareStatement(selectSql)) {
                            selectPs.setLong(1, auctionId);
                            var auctionRs = selectPs.executeQuery();
                            if (auctionRs.next()) {
                                String itemName = auctionRs.getString("item_name");
                                Long sellerId = auctionRs.getLong("user_id");
                                publishNotification("AUCTION_CLOSED_WIN", winnerId, auctionId, itemName, context);
                                publishNotification("AUCTION_CLOSED_BUYER", sellerId, auctionId, itemName, context);
                            }
                        }
                        long evictionStart = System.currentTimeMillis();
                        evictCache("auction::getAuction::%s".formatted(auctionId), context);
                        evictCache("auction::getManyAuctionsPublic::*", context);

                        updateElasticsearchStatus(auctionId, "DONE", context);
                    } else {
                        context.getLogger().log("[SKIP] 이미 처리된 경매: auctionId=" + auctionId);
                    }
                }

            } else {
                // 유찰 처리
                String updateAuction = "UPDATE auctions SET auction_status = 'NO_BID' WHERE id = ? AND auction_status = 'ACTIVE'";
                try (PreparedStatement ps2 = conn.prepareStatement(updateAuction)) {
                    ps2.setLong(1, auctionId);

                    long queryStart = System.currentTimeMillis();
                    int updated = ps2.executeUpdate();

                    if (updated > 0) {
                        String updateBids = "UPDATE bids SET status = 'CLOSED' WHERE auction_id = ?";
                        try (PreparedStatement ps4 = conn.prepareStatement(updateBids)) {
                            ps4.setLong(1, auctionId);
                            ps4.executeUpdate();
                        }

                        long redisStart = System.currentTimeMillis();
                        publishToRedis(auctionId, "AUCTION_NO_BID", context);

                        String selectSql = "SELECT item_name, user_id FROM auctions WHERE id = ?";
                        try (PreparedStatement selectPs = conn.prepareStatement(selectSql)) {
                            selectPs.setLong(1, auctionId);
                            var auctionRs = selectPs.executeQuery();
                            if (auctionRs.next()) {
                                String itemName = auctionRs.getString("item_name");
                                Long sellerId = auctionRs.getLong("user_id");
                                publishNotification("AUCTION_NO_BID", sellerId, auctionId, itemName, context);
                            }
                        }

                        long evictionStart = System.currentTimeMillis();
                        evictCache("auction::getAuction::%s".formatted(auctionId), context);
                        evictCache("auction::getManyAuctionsPublic::*", context);

                        updateElasticsearchStatus(auctionId, "NO_BID", context);
                    } else {
                        // skip된 경우 로그
                        context.getLogger().log("[SKIP] 이미 처리된 경매 NO_BID: auctionId=" + auctionId);
                    }
                }
            }
        }
    }

    private void publishToRedis(Long auctionId, String eventType, Context context) {
        try (Jedis jedis = jedisPool.getResource()) {
            Map<String, Object> message = Map.of(
                    "auctionId", auctionId,
                    "eventType", eventType
            );
            String payload = objectMapper.writeValueAsString(message);
            jedis.publish("auction-events", payload);
        } catch (Exception e) {
            context.getLogger().log("[Redis] 발행 실패: " + e.getMessage());
        }
    }

    // 알림 이벤트 발행
    private void publishNotification(String type, Long receiverId, Long auctionId, String itemName, Context context) {
        try (Jedis jedis = jedisPool.getResource()){
            Map<String, Object> message = Map.of(
                    "type", type,
                    "receiverId", receiverId,
                    "auctionId", auctionId,
                    "itemName", itemName
            );
            String payload = objectMapper.writeValueAsString(message);
            jedis.publish("auction:notification", payload);
        } catch (Exception e) {
            context.getLogger().log("[Notification] 발행 실패: " + e.getMessage());
        }
    }

    private void evictCache(String pattern, Context context) {

        try (Jedis jedis = jedisPool.getResource()){
            String cursor = ScanParams.SCAN_POINTER_START;
            ScanParams scanParams = new ScanParams().match(pattern).count(EVICT_CACHE_LOOP_BATCH_SIZE);

            for(int i=0; i<EVICT_CACHE_LOOP_UPPER_BOUND; i++) {
                ScanResult<String> scan = jedis.scan(cursor, scanParams);

                List<String> scanResults = scan.getResult();

                if (!scanResults.isEmpty()) {
                    jedis.unlink(scanResults.toArray(new String[0]));
                }

                if (i == EVICT_CACHE_LOOP_UPPER_BOUND - 1 && !scan.getCursor().equals("0")) {
                    context.getLogger().log("[Cache-Evict] key (%s) cache eviction을 다 하기 전에 제한 횟수에 도달했습니다.".formatted(
                            pattern
                    ));
                }
                if (scan.getCursor().equals("0")) {
                    break;
                }

                cursor = scan.getCursor();
            }
        } catch (Exception e) {
            context.getLogger().log("[Cache-Evict] key (%s) cache eviction 실패: %s".formatted(
                    pattern, e.getMessage()
            ));
        }
    }

    private void updateElasticsearchStatus(Long auctionId, String status, Context context) {
        try {
            Map<String, Object> body = Map.of("doc", Map.of("status", status));
            String payload = objectMapper.writeValueAsString(body);

            String endpoint = "%s/%s/_update/%d".formatted(ES_URL, ES_AUCTION_INDEX, auctionId);

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(endpoint))
                    .timeout(Duration.ofSeconds(3))
                    .header("Content-Type", "application/json")
                    .header("Authorization", ES_BASIC_AUTH)
                    .POST(HttpRequest.BodyPublishers.ofString(payload))
                    .build();

            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());

            if (resp.statusCode() >= 300) {
                context.getLogger().log("[ES] status update 실패: auctionId=%d, code=%d, body=%s".formatted(
                        auctionId, resp.statusCode(), resp.body()));
            }
        } catch (Exception e) {
            context.getLogger().log("[ES] status update 예외: auctionId=%d, %s".formatted(
                    auctionId, e.getMessage()));
        }
    }
}
