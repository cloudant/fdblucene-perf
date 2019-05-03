FROM openjdk:11-stretch
ENV FDB_VERSION 6.0.18
ENTRYPOINT ["/usr/bin/java", "-jar", "/usr/share/fdblucene-perf/benchmarks.jar", "-jvmArgs", "-server -Ddir=/tmp/foo"]
RUN wget -q https://www.foundationdb.org/downloads/$FDB_VERSION/linux/libfdb_c_$FDB_VERSION.so -O /usr/local/lib/libfdb_c_$FDB_VERSION.so
RUN ldconfig
ADD target/benchmarks.jar /usr/share/fdblucene-perf/benchmarks.jar
