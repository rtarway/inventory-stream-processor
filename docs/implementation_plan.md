# Documentation & Configuration Enhancement

## Goal
1.  Make the metadata dimension limit (currently hardcoded to 3) configurable via `application.yml`.
2.  Create a comprehensive `README.md` describing the business use case (Multi-Channel Inventory Stream), event structure, and dynamic aggregation logic.

## Proposed Changes

### 1. Processor Configuration
#### [MODIFY] `KafkaStreamsConfig.java`
- Inject `@Value("${app.processor.metadata-limit:3}") int metadataLimit;`
- Replace `.limit(3)` with `.limit(metadataLimit)`.

#### [MODIFY] `processor/src/main/resources/application.yml`
- Add `app.processor.metadata-limit: 3` (default).

### 2. Documentation
#### [NEW] `README.md`
- **Introduction**: Explain the system as a "Real-time Inventory Position Engine" for retailers.
- **Event Structure**: Document the JSON schema (`id`, `value`, `type`, `metadata`).
- **Logic**: Explain how `FlatMap` generates multiple "virtual" streams (Item Total, Item+Store Total, Item+Group Total) from a single event.
- **Architecture**: Diagram/Text explaining Kafka -> Processor -> Redis Sink -> API.
- **Configuration**: List `grace-period-ms` and `metadata-limit`.
- **Run Instructions**: `deploy_infra.sh`, `kubectl apply`, etc.

### 3. Verification
- Rebuild processor.
- Set `metadata-limit` to 5.
- Send event with 5 metadata tags.
- Verify 6 not 4 entries in output (1 base + 5 dims).
