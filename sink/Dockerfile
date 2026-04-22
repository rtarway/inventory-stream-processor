FROM eclipse-temurin:17-jre
RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app
WORKDIR /app
# Build from repository root: docker build -f sink/Dockerfile .
COPY --chown=app:app sink/target/sink-1.0-SNAPSHOT.jar app.jar
USER app
ENTRYPOINT ["java", "-jar", "app.jar"]
