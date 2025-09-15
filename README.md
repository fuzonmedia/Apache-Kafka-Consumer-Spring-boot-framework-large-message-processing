# Fuzomedia Kafka Consumer

## Build and run this application

1. Install amazon selling partner api java sdk jar file in your local maven repository by this command.

`mvn install:install-file -Dfile=[path to JAR file in "target" folder] -DgroupId=com.amazon.sellingpartnerapi -DartifactId=sellingpartnerapi-aa-java -Dversion=1.0 -Dpackaging=jar`

2. Run this command to build **app.jar** file.
`./mvnw clean install -Dmaven.test.skip=true`

3. Copy the **app.jar** from target *folder* file to the *docker/app* folder.

4. Finally run this command inside  *docker* folder to build the docker image and run it in a docker container.
`docker-compose up`
