FROM eclipse-temurin:17-jdk-alpine
#RUN apk add --update fontconfig freetype

#RUN apk --no-cache add msttcorefonts-installer fontconfig && update-ms-fonts && fc-cache -f

# Refer to Maven build -> finalName
ARG JAR_FILE=target/rabbitmq-0.0.1-SNAPSHOT.jar

# cd /opt/app
WORKDIR /opt/app

# cp target/spring-boot-web.jar /opt/app/app.jar
COPY ${JAR_FILE} app.jar

# java -jar /opt/app/app.jar
#ENTRYPOINT ["java", "-Dspring.profiles.active=dev", "-jar","app.jar"]

#ENTRYPOINT ["java",  " $JAVA_OPTS -Dfile.encoding=UTF-8 -jar","app.jar"]

ENTRYPOINT ["sh", "-c", "java ${JAVA_OPTS}  -Dfile.encoding=UTF-8 -jar app.jar"]