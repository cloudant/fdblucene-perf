FROM alpine:3.9
RUN apk --update add openjdk8-jre
ENV FDB_VERSION 6.0.18
#ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/fdblucene-perf/benchmarks.jar", "-jvmArgs", "-server -Ddir=/tmp/foo"]
RUN wget -q https://www.foundationdb.org/downloads/$FDB_VERSION/linux/libfdb_c_$FDB_VERSION.so -O /usr/local/lib/libfdb_c_$FDB_VERSION.so
RUN ldconfig /usr/local/lib/
ADD target/benchmarks.jar /usr/share/fdblucene-perf/benchmarks.jar
