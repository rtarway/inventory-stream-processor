FROM eclipse-temurin:17-jre
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app
WORKDIR /app
# Build from repository root: docker build -f api/Dockerfile .
COPY --chown=app:app api/target/api-1.0-SNAPSHOT.jar app.jar
USER app
# Non-root, no new privileges (compose/k8s can add read_only + tmpfs for /tmp if needed)
ENTRYPOINT ["java", "-jar", "app.jar"]
