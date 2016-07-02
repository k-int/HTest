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

System.exit(0);
