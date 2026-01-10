# Task: Design and Implement Kafka Stream Processor

- [x] **Phase 1: System Design & Planning**
    - [x] Create detailed system design document (implementation_plan.md) <!-- id: 0 -->

- [x] **Phase 2: Project Setup**
    - [x] Initialize project structure (Maven) <!-- id: 4 -->

- [x] **Phase 3: Core Implementation**
    - [x] Implement JSON Serdes & Event Models <!-- id: 6 -->
    - [x] Implement **Fan-Out Logic** (Multi-dim keys) <!-- id: 18 -->
    - [x] Implement **Resequencing Processor** (Buffering) <!-- id: 19 -->
    - [x] Implement Core Logic (State Store, Reset, late-check, Halt) <!-- id: 8 -->
    - [x] Implement Audit Producer <!-- id: 10 -->

- [x] **Phase 4: Read Layer (CQRS)**
    - [x] Implement Output Sink (Total Updates) <!-- id: 20 -->
    - [x] Setup Redis Writer (RedisSinkApp) <!-- id: 21 -->
    - [x] Implement Read API (ReadApiApp) <!-- id: 22 -->

- [x] **Phase 5: Verification & Testing**
    - [x] Write implementation code <!-- id: 13 -->
    - [x] Debug pipeline (Sink Offset, Extractor, Host Networking) <!-- id: 25 -->
    - [x] Create walkthrough.md <!-- id: 15 -->
    - [x] **Final Verified Success** (Total 210.0)
        - [x] Optimization: Implemented Hybrid Idle Flush (Wall-clock punctuator) to release stuck events in sparse streams.

- [x] **Phase 6: Refactoring**
    - [x] Refactor to Spring Boot Architecture (Profiles: processor, sink, api)
    - [x] Implement SLF4J Logging
    - [x] Verify Refactoring Success (Total 210)

- [x] **Phase 7: Microservices & Kubernetes**
    - [x] Refactor Projects to Multi-Module Maven
    - [x] Create Dockerfiles for Microservices
    - [x] Create K8s Manifests (Deployment/Service)
    - [x] Update Scripts (deploy, clean, validate)
    - [x] Deploy Infrastructure (Redis, Kafka with Zookeeper)
    - [x] Deploy Apps (Processor, Sink, API)
    - [x] End-to-End Verification on K8s (Success: Total 210)

- [x] **Phase 8: Late Reset Enhancement**
    - [x] Implement History Buffer in ProcessingState
    - [x] Implement Late Reset Logic in CoreLogicProcessor
    - [x] Verify with test_late_reset.sh (Success: 105)

- [x] **Phase 9: Configurable Grace Period**
    - [x] Make Grace Period Configurable in KafkaStreamsConfig
    - [x] Implement Zero-Grace Logic in ResequencingProcessor
- [x] **Phase 10: Documentation & Config Enhancement**
    - [x] Make Metadata Limit Configurable in KafkaStreamsConfig
    - [x] Create Detailed README.md (Business Context, Event Structure, Deployment)
    - [x] Verify Configurable Limit (Build Verified)
    - [x] Add Comprehensive Deployment Guide (Local K8s, GKE, External Services)
