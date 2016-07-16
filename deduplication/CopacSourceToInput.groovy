#!groovy

// This Mapper takes raw copac records and extracts the original source components so we can compare our
// deduplication with the local

// Final hint in getting this going came from 
// http://stackoverflow.com/questions/8542324/type-mismatch-in-key-from-map-expected-text-received-longwritable

// https://mvnrepository.com/artifact/org.apache.hbase/hbase
@Grapes([
  // @GrabResolver(name='mvnRepository', root='http://central.maven.org/maven2/'),
  @Grab(group='org.apache.hbase', module='hbase-client', version='1.2.2'),
  @Grab(group='org.apache.hbase', module='hbase-server', version='1.2.2'),
  @Grab(group='org.apache.hbase', module='hbase-common', version='1.2.2'),
  @Grab(group='org.apache.hbase', module='hbase-hadoop2-compat', version='1.2.2'),
  @Grab(group='org.apache.hbase', module='hbase-hadoop-compat', version='1.2.2'),
  @Grab(group='org.apache.hadoop', module='hadoop-common', version='2.7.2'),
  @Grab(group='org.apache.hadoop', module='hadoop-mapreduce-client-core', version='2.7.2'),
  @Grab(group='org.apache.hadoop', module='hadoop-mapreduce-client-jobclient', version='2.7.2'),
  @GrabExclude('org.codehaus.groovy:groovy-all')
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
import java.util.Properties
import static groovy.json.JsonOutput.*
import groovy.json.JsonSlurper
import java.security.MessageDigest

import groovy.xml.XmlUtil  
import org.apache.hadoop.hbase.mapreduce.*
import org.apache.hadoop.io.*
import org.apache.hadoop.hbase.io.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.Mapper.Context
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.mapred.lib.NullOutputFormat

Configuration config = HBaseConfiguration.create();
Job job = new Job(config,'ExampleSummary');
job.setJarByClass(CopacSourceToInput.class);     // class that contains mapper and reducer -- for the groovy scriplet -- this

// Scan scan = new Scan();
// In this version, we only process 1 row
// Try a hathitrust record
// Scan scan = new Scan(Bytes.toBytes('oai:quod.lib.umich.edu:MIU01-003496759'), Bytes.toBytes('oai:quod.lib.umich.edu:MIU01-003496759'));
// try a copac record
Scan scan = new Scan(Bytes.toBytes('1200'), Bytes.toBytes('1200'));


scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs


TableMapReduceUtil.initTableMapperJob(
  'sourceRecord',        // input table
  scan,               // Scan instance to control CF and attribute selection
  MapToModsMapper.class,     // mapper class
  null,
  null,
  job);

TableMapReduceUtil.initTableReducerJob(
  'inputRecord',        // output table
  null,                 // NO reducer
  job);

job.setMapOutputKeyClass(ImmutableBytesWritable.class);
job.setMapOutputValueClass(Put.class);

// job.setMapperClass(MapToModsMapper.class);
// job.setReducerClass(PrimeNumberReduce.class);
job.setNumReduceTasks(1);   // at least one, adjust as required
// job.setOutputFormatClass(NullOutputFormat.class);

boolean b = job.waitForCompletion(true);
if (!b) {
  throw new IOException('error with job!');
}


// <KEYOUT>,<VALUEOUT>
public class MapToModsMapper extends TableMapper<ImmutableBytesWritable, Put>  {

  private static final String MARCXML2MODS_XSLT="http://www.loc.gov/standards/mods/v3/MARC21slim2MODS3.xsl";

  @Override
  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
    // this example is just copying the data from the source table...
    // We take the input value, parse it and extract and source records
    // raw record is in nbk:raw
    String record_xml_as_text = new String(value.getValue(Bytes.toBytes("nbk"), Bytes.toBytes("raw")));
    def parsed_xml = new XmlSlurper().parseText(record_xml_as_text)
    //
    // Take each mods/extension/modsCollection and create a new record
    //

    parsed_xml.extension.modsCollection.mods.each { m ->
      // Generate single records
      StringWriter sw = new StringWriter()
      XmlUtil xmlUtil = new XmlUtil()
      xmlUtil.serialize(m, sw)
      def atomic_record=sw.toString();
      def new_record_uuid = UUID.randomUUID().toString()

      context.write(new ImmutableBytesWritable(new_record_uuid.getBytes()), 
                    getContributorRecord(new_record_uuid, atomic_record, 'mods', null, null));
    }


  }

  private static Put getContributorRecord(String contributor_record_id, String contributor_record, String recsyn, String work_hash, String instance_hash) throws IOException {
    Put put = new Put(Bytes.toBytes(contributor_record_id));
    put.add( Bytes.toBytes("nbk"), Bytes.toBytes("raw"), Bytes.toBytes(contributor_record) )
    put.add( Bytes.toBytes("nbk"), Bytes.toBytes("recsyn"), Bytes.toBytes(recsyn) )
    return put;
  }


  private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
    Put put = new Put(key.get());
    for (KeyValue kv : result.raw()) {
      put.add(kv);
    }
    return put;
  }
}
