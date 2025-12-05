FROM amazoncorretto:21-alpine

# Установка curl для health checks
RUN apk add --no-cache curl

# Создание пользователя для запуска приложения
RUN addgroup -S spring && adduser -S spring -G spring

# Установка рабочей директории
WORKDIR /app

# Копирование jar файла из target (собранного в CI/CD)
COPY target/*.jar app.jar

# Изменение владельца файлов
RUN chown -R spring:spring /app

# Переключение на непривилегированного пользователя
USER spring:spring

# Настройка JVM для контейнера
ENV JAVA_OPTS="-XX:+UseContainerSupport -XX:MaxRAMPercentage=75.0"

# Включение production режима для Vaadin
ENV VAADIN_PRODUCTION_MODE=true

# Expose порт приложения
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=60s --retries=3 \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

# Запуск приложения
ENTRYPOINT ["sh", "-c", "java $JAVA_OPTS -jar app.jar"]
