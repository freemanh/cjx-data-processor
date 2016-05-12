FROM maven:3.3.9-jdk-8

ADD pom.xml /tmp/build/
RUN cd /tmp/build && mvn -q dependency:resolve

ADD src /tmp/build/src
RUN cd /tmp/build && mvn clean package && mv target/*.jar /app.jar \
    && cd / && rm -rf /tmp/build
CMD ["java", "-jar","app.jar"]
