FROM openjdk:16-jdk-alpine
VOLUME /tmp
VOLUME /app
COPY ./app/app.jar /app/app.jar
COPY ./app/tretailb2b-bc7bdb32f362.json /app/tretailb2b-bc7bdb32f362.json
COPY ./app/tretailprod-379815-e527ca074abc.json /app/tretailprod-379815-e527ca074abc.json
ENTRYPOINT ["java","-jar","/app/app.jar"]