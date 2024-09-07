FROM --platform=linux/amd64 openjdk
LABEL authors="Hrushikesh Vazurkar"
VOLUME /tmp
EXPOSE 8080
COPY target/acp2-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar","/app.jar"]