FROM maven:3.2-jdk-7-onbuild

ENTRYPOINT java -jar target/website-upload-processor-1.0-SNAPSHOT.jar config/log4j.properties config/main.properties