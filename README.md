# auction-lambda

> 역경매 서비스의 **경매 상태 자동화**를 담당하는 AWS Lambda 모듈입니다.
> EventBridge Scheduler 트리거를 받아 SQS 지연 메시지를 발행하고, SQS 메시지를 소비하여 RDS 상태 변경 및 Redis Pub/Sub 이벤트를 발행합니다.

---

## 처리 흐름

```
EventBridge Scheduler
        │  (경매 시작/종료 5분 전 트리거)
        ▼
AuctionLambda (auction-sqs-handler)
        │  SQS 지연 메시지 발행 (최대 900초 지연)
        ▼
    Amazon SQS
        │  정확한 시각에 트리거
        ▼
AuctionSqsLambda (auction-sqs-consumer)
        ├── RDS  →  auction_status 변경
        └── Redis →  auction-events / auction:notification 발행
```

---

## Lambda 함수

### 1. `AuctionLambda` — SQS 메시지 발행

| 항목 | 내용 |
|------|------|
| 트리거 | Amazon EventBridge Scheduler |
| 배포 함수명 | `auction-sqs-handler` |
| 역할 | EventBridge 이벤트 수신 후 SQS 지연 메시지 발행 |

**입력 이벤트 (EventBridge)**

```json
{
  "auctionId": 123,
  "action": "START",
  "targetTime": "2026-05-13T10:00:00"
}
```

**동작**

- `targetTime`과 현재 시각의 차이를 초 단위로 계산
- `delaySeconds`를 `[0, 900]` 범위로 클램핑하여 SQS 지연 메시지 발행
- EventBridge가 5분 전에 트리거하므로 900초를 넘을 일은 없으나 안전장치로 설정

---

### 2. `AuctionSqsLambda` — RDS 상태 변경 + Redis 발행

| 항목 | 내용 |
|------|------|
| 트리거 | Amazon SQS |
| 배포 함수명 | `auction-sqs-consumer` |
| 역할 | 경매 상태 변경, 낙찰 결과 저장, Redis 이벤트 발행 |

**START 처리**

```
auctions.auction_status: READY → ACTIVE
  └── Redis auction-events     : AUCTION_STARTED 발행
  └── Redis auction:notification: AUCTION_STARTED 알림 (구매자)
  └── Redis 캐시 무효화
```

**END 처리 — 낙찰 (입찰 존재)**

```
auctions.auction_status: ACTIVE → DONE
  └── auction_results 삽입 (낙찰가, 낙찰자, 판매자, 입찰 ID)
  └── bids.status: → CLOSED
  └── Redis auction-events     : AUCTION_ENDED 발행
  └── Redis auction:notification: AUCTION_CLOSED_WIN (낙찰자)
  └── Redis auction:notification: AUCTION_CLOSED_BUYER (판매자)
  └── Redis 캐시 무효화
```

**END 처리 — 유찰 (입찰 없음)**

```
auctions.auction_status: ACTIVE → NO_BID
  └── bids.status: → CLOSED
  └── Redis auction-events     : AUCTION_NO_BID 발행
  └── Redis auction:notification: AUCTION_NO_BID 알림 (구매자)
  └── Redis 캐시 무효화
```

**SQS 부분 실패 처리**

배치 내 메시지 처리 중 일부 실패 시 `SQSBatchResponse`의 `BatchItemFailure`에 실패한 메시지 ID만 반환하여 해당 메시지만 재시도합니다.

---

### 3. `AuctionLambdaLegacy` — Legacy (직접 처리)

| 항목 | 내용 |
|------|------|
| 트리거 | Amazon EventBridge Scheduler |
| 배포 함수명 | `auction-scheduler` |
| 역할 | EventBridge 직접 트리거 → RDS 상태 변경 + Redis 발행 (SQS 없음) |

SQS 도입 이전 방식으로, `AuctionSqsLambda`와 동일한 로직을 수행하지만 SQS 중간 단계 없이 EventBridge에서 직접 실행됩니다.

---

## Redis 채널

| 채널 | 발행 이벤트 | 구독 서버 |
|------|------------|-----------|
| `auction-events` | `AUCTION_STARTED`, `AUCTION_ENDED`, `AUCTION_NO_BID` | auction (메인 서버) |
| `auction:notification` | `AUCTION_STARTED`, `AUCTION_CLOSED_WIN`, `AUCTION_CLOSED_BUYER`, `AUCTION_NO_BID` | auction-notification (알림 서버) |

---

## 구현 포인트

**멱등성 보장**

상태 변경 SQL에 현재 상태 조건(`AND auction_status = 'READY'` / `AND auction_status = 'ACTIVE'`)을 포함하고, `updated > 0`인 경우에만 후속 처리를 실행합니다. 중복 트리거 시 Redis 발행이 일어나지 않습니다.

**RDS Proxy 핀닝 방지**

PostgreSQL JDBC의 `assumeMinServerVersion=9.0` 속성을 설정하여 세션 단위 SET 명령 발행을 억제합니다. 이로써 RDS Proxy의 연결 핀닝을 방지하고 커넥션 풀을 효율적으로 활용합니다.

**Redis 캐시 무효화**

`SCAN` + `UNLINK`를 사용하여 `auction::getAuction::{id}`, `auction::getManyAuctionsPublic::*` 패턴의 캐시를 일괄 삭제합니다. `UNLINK`는 비동기로 메모리를 해제하여 블로킹을 최소화합니다.

**Jedis Pool 재사용**

`JedisPool`을 `static`으로 선언하여 Lambda 실행 환경(Execution Context) 재사용 시 커넥션을 재활용합니다.

---

## 패키지 구조

```
src/com/example/lambda/
├── AuctionLambda.java         # EventBridge 트리거 → SQS 지연 메시지 발행
├── AuctionSqsLambda.java      # SQS 트리거 → RDS 상태 변경 + Redis Pub/Sub 발행
└── AuctionLambdaLegacy.java   # Legacy: EventBridge 직접 트리거 처리
```

---

## 환경변수

| 변수명 | 설명 | 사용 Lambda |
|--------|------|------------|
| `SQS_QUEUE_URL` | SQS 큐 URL | AuctionLambda |
| `DB_URL` | PostgreSQL JDBC URL | AuctionSqsLambda, Legacy |
| `DB_USERNAME` | DB 사용자명 | AuctionSqsLambda, Legacy |
| `DB_PASSWORD` | DB 비밀번호 | AuctionSqsLambda, Legacy |
| `REDIS_HOST` | Redis 호스트 | AuctionSqsLambda, Legacy |

---

## 빌드 및 배포

### 빌드

```bash
./gradlew jar
```

`build/libs/auction-lambda-0.0.1-SNAPSHOT.jar` (Fat JAR)가 생성됩니다.

### CI/CD

`main` / `dev` 브랜치에 push 시 GitHub Actions가 자동으로 실행됩니다.

```
push → ./gradlew jar → AWS OIDC 인증 → Lambda 함수 코드 업데이트 (3개)
```

AWS 인증은 OIDC(키 없이 역할 기반)로 처리합니다.

### 수동 배포

```bash
aws lambda update-function-code \
  --function-name auction-sqs-consumer \
  --zip-file fileb://build/libs/auction-lambda-0.0.1-SNAPSHOT.jar
```

---

## 기술 스택

<div align="center">
<img src="https://img.shields.io/badge/java-007396?style=for-the-badge&logo=openjdk&logoColor=white">
<img src="https://img.shields.io/badge/AWS%20Lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white">
<img src="https://img.shields.io/badge/Amazon%20SQS-FF4F8B?style=for-the-badge&logo=amazonsqs&logoColor=white">
<img src="https://img.shields.io/badge/Amazon%20EventBridge-FF4F8B?style=for-the-badge&logo=amazonaws&logoColor=white">
<img src="https://img.shields.io/badge/Amazon%20RDS-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white">
<img src="https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white">
<img src="https://img.shields.io/badge/Github%20Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white">
</div>
