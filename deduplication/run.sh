

# Step 0 : Initialize
echo Drop dedup index
curl -XDELETE http://localhost:9200/dedup

echo Drop and recreate input record
./bin/hbase <<!!!
disable 'inputRecord'
drop 'inputRecord'
create 'inputRecord', 'nbk'
list
!!!

# Step 1 : Load corpus -- test or live or whatevs


