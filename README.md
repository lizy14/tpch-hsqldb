# tpch-hsqldb
TPC Benchmark H for HyperSQL

## Build
```
git clone https://github.com/lizy14/tpch-hsqldb.git
git clone https://github.com/electrum/tpch-dbgen.git
cp tpch-dbgen/* tpch-hsqldb/
cd tpch-hsqldb/
make
./generate-sql.sh
cp test-skipping.sql.example test-skipping.sql
```

## Install
```
export HSQLDB_SRC_ROOT="~/hsqldb-2.3.0" # to be modified
mkdir -p $HSQLDB_SRC_ROOT/data/tpch/
cp *.sql $HSQLDB_SRC_ROOT/data/tpch/
cp *.java $HSQLDB_SRC_ROOT/src/org/hsqldb/sample/
```

## Run
Run `src/org/hsqldb/sample/TpchTestdb.java` in your IDE or
```
javac $HSQLDB_SRC_ROOT/src/org/hsqldb/sample/TpchTestdb.java 
cd $HSQLDB_SRC_ROOT/data/
java -classpath "../src/;../lib/hsqldb.jar" org.hsqldb.sample.TpchTestdb
```
