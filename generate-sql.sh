./dbgen -v -s 1 # *.tbl
python pop.py # *.tbl.pop.sql
cat *.tbl.pop.sql > data.sql

./dbgen -v -U 1 -s 1 # *.tbl.u1 delete.1

python uf1.py # *.tbl.u1.sql
cat orders.tbl.u1.sql lineitem.tbl.u1.sql > rf1.sql

python uf2.py # rf2.sql

