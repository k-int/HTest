#!groovy


// https://mvnrepository.com/artifact/org.apache.hbase/hbase
@Grapes([
  // @GrabResolver(name='mvnRepository', root='http://central.maven.org/maven2/'),
  @Grab(group='org.apache.hbase', module='hbase-client', version='1.2.1'),
  @Grab(group='org.apache.hbase', module='hbase-common', version='1.2.1'),
  @Grab(group='org.apache.hadoop', module='hadoop-common', version='2.7.2')
])


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.HColumnDescriptor
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import groovy.time.*

println("NBK Ingest Phase 1 Adapter for hathitrust");

addRecord([:]);

System.exit(0);


def addRecord(record) {

  // We are assuming a table created in the hbase shell using 
  // create 'sourceRecord', 'nbk'

  // Instantiating Configuration class
  Configuration config = HBaseConfiguration.create();

  // Instantiating HTable class
  HTable htable = new HTable(config, "sourceRecord");

  try {
    def recordid = java.util.UUID.randomUUID().toString();
    Put p = new Put(Bytes.toBytes(recordid))
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("sourceid"), Bytes.toBytes("hathitrust") )
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("timestamp"), Bytes.toBytes("${System.currentTimeMillis()}".toString()))
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("canonical"), Bytes.toBytes("CanonicalRecord") )
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("raw"), Bytes.toBytes("RawRecord") )
    htable.put(p);
    htable.flushCommits()
    htable.close()
  }
  catch ( Exception e ) {
    e.printStackTrace()
  }
}
