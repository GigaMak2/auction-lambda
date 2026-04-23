package com.example.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Map;

public class AuctionLambda implements RequestHandler<Map<String, Object>, String> {

    private static final String DB_URL = System.getenv("DB_URL");
    private static final String DB_USERNAME = System.getenv("DB_USERNAME");
    private static final String DB_PASSWORD = System.getenv("DB_PASSWORD");
    private static final String REDIS_HOST = System.getenv("REDIS_HOST");
    private static final int REDIS_PORT = 6379;

    private static final ObjectMapper objectMapper = new ObjectMapper();
    // static 으로 만들어 재사용
    private static final Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT, true);

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {

        context.getLogger().log("[시간] Lambda 시작: " + System.currentTimeMillis());

        long start = System.currentTimeMillis();

        Long auctionId = Long.valueOf(event.get("auctionId").toString());
        String action = event.get("action").toString();

        long dbStart = System.currentTimeMillis();

        try (Connection conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD)) {

            context.getLogger().log("[시간] DB 연결: " + (System.currentTimeMillis() - dbStart) + "ms");

            if ("START".equals(action)) {
                startAuction(conn, auctionId, context);
            } else if ("END".equals(action)) {
                endAuction(conn, auctionId, context);
            } else {
                return "Invalid action: " + action;
            }
            return "OK";
        } catch (Exception e) {
            context.getLogger().log("[Lambda] Error: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    // context: 람다 실행환경 정보를 담고 있음. aws가 주입
    private void startAuction(Connection conn, Long auctionId, Context context) throws Exception {
        String sql = "UPDATE auctions SET auction_status = 'ACTIVE' WHERE id = ? AND auction_status = 'READY'";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setLong(1, auctionId);
            // db가 실제로 업데이트됐을 때만 레디스

            long queryStart = System.currentTimeMillis();

            int updated = ps.executeUpdate();

            context.getLogger().log("[시간] DB 쿼리: " + (System.currentTimeMillis() - queryStart) + "ms");

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

                context.getLogger().log("[시간] Redis 발행: " + (System.currentTimeMillis() - redisStart) + "ms");
            }
        }
    }

    private void endAuction(Connection conn, Long auctionId, Context context) throws Exception {
        // 낙찰자 있는지 확인
        String checkSql = "SELECT id, user_id, price FROM bids WHERE auction_id = ? ORDER BY price ASC LIMIT 1";
        try (PreparedStatement ps = conn.prepareStatement(checkSql)) {
            ps.setLong(1, auctionId);
            var rs = ps.executeQuery();

            if (rs.next()) {
                // 낙찰 처리
                Long winnerId = rs.getLong("user_id");
                Long winnerBidId = rs.getLong("id");
                java.math.BigDecimal price = rs.getBigDecimal("price");

                // 경매 상태 DONE으로 변경
                String updateAuction = "UPDATE auctions SET auction_status = 'DONE' WHERE id = ?";
                try (PreparedStatement ps2 = conn.prepareStatement(updateAuction)) {
                    ps2.setLong(1, auctionId);
                    ps2.executeUpdate();
                }

                // 경매 결과 저장
                String insertResult = "INSERT INTO auction_results (price, auction_id, buyer_id, seller_id, bid_id) " +
                        "SELECT ?, ?, user_id, ?, ? FROM auctions WHERE id = ?";
                try (PreparedStatement ps3 = conn.prepareStatement(insertResult)) {
                    ps3.setBigDecimal(1, price);
                    ps3.setLong(2, auctionId);
                    ps3.setLong(3, winnerId);
                    ps3.setLong(4, winnerBidId);
                    ps3.setLong(5, auctionId);
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
                        Long buyerId = auctionRs.getLong("user_id");

                        publishNotification("AUCTION_CLOSED_WIN", winnerId, auctionId, itemName, context);
                        publishNotification("AUCTION_CLOSED_BUYER", buyerId, auctionId, itemName, context);
                    }
                }

                context.getLogger().log("[시간] Redis 발행: " + (System.currentTimeMillis() - redisStart) + "ms");
            } else {
                // 유찰 처리
                String updateAuction = "UPDATE auctions SET auction_status = 'NO_BID' WHERE id = ?";
                try (PreparedStatement ps2 = conn.prepareStatement(updateAuction)) {
                    ps2.setLong(1, auctionId);
                    ps2.executeUpdate();
                }

                long redisStart = System.currentTimeMillis();

                publishToRedis(auctionId, "AUCTION_NO_BID", context);

                String selectSql = "SELECT item_name, user_id FROM auctions WHERE id = ?";
                try (PreparedStatement selectPs = conn.prepareStatement(selectSql)) {
                    selectPs.setLong(1, auctionId);
                    var auctionRs = selectPs.executeQuery();
                    if (auctionRs.next()) {
                        String itemName = auctionRs.getString("item_name");
                        Long buyerId = auctionRs.getLong("user_id");

                        publishNotification("AUCTION_NO_BID", buyerId, auctionId, itemName, context);
                    }
                }

                context.getLogger().log("[시간] Redis 발행: " + (System.currentTimeMillis() - redisStart) + "ms");
            }
        }
    }


    private void publishToRedis(Long auctionId, String eventType, Context context) {
        // jedis: 자바에서 레디스에 접속하고 명령어 쓸 수 있게 해주는 도구. spring의 redistemplate 나 redisson 같은 것
        try  {
            Map<String, Object> message = Map.of(
                    "auctionId", auctionId,
                    "eventType", eventType
            );
            String payload = objectMapper.writeValueAsString(message);
            jedis.publish("auction-events", payload);
            context.getLogger().log("[Redis] 이벤트 발행: " + payload);
        } catch (Exception e) {
            context.getLogger().log("[Redis] 발행 실패: " + e.getMessage());
        }
    }

    private void publishNotification(String type, Long receiverId, Long auctionId, String itemName, Context context) {
        try (Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT, true)) {
            Map<String, Object> message = Map.of(
                    "type", type,
                    "receiverId", receiverId,
                    "auctionId", auctionId,
                    "itemName", itemName
            );
            String payload = objectMapper.writeValueAsString(message);
            jedis.publish("auction:notification", payload);
            context.getLogger().log("[Notification] 이벤트 발행: " + payload);
        } catch (Exception e) {
            context.getLogger().log("[Notification] 발행 실패: " + e.getMessage());
        }
    }
}