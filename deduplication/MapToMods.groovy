#!groovy


// https://mvnrepository.com/artifact/org.apache.hbase/hbase
@Grapes([
  // @GrabResolver(name='mvnRepository', root='http://central.maven.org/maven2/'),
  @Grab(group='org.apache.hbase', module='hbase-client', version='1.2.1'),
  @Grab(group='org.apache.hbase', module='hbase-server', version='1.2.1'),
  @Grab(group='org.apache.hbase', module='hbase-common', version='1.2.1'),
  @Grab(group='org.apache.hbase', module='hbase-hadoop2-compat', version='1.2.1'),
  @Grab(group='org.apache.hbase', module='hbase-hadoop-compat', version='1.2.1'),
  @Grab(group='org.apache.hadoop', module='hadoop-common', version='2.7.2'),
  @Grab(group='org.apache.hadoop', module='hadoop-mapreduce-client-core', version='2.7.2'),
  @Grab(group='org.apache.hadoop', module='hadoop-mapreduce-client-jobclient', version='2.7.2'),
  @Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7.2'),
  @Grab(group='org.apache.httpcomponents', module='httpclient', version='4.5.2'),
  @Grab(group='org.apache.httpcomponents', module='httpmime', version='4.5.2'),
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
import groovyx.net.http.HTTPBuilder
import org.apache.http.entity.mime.MultipartEntity
import org.apache.http.entity.mime.HttpMultipartMode
import org.apache.http.entity.mime.content.InputStreamBody
import org.apache.http.entity.mime.content.StringBody
import groovyx.net.http.*
import org.apache.http.entity.mime.MultipartEntityBuilder /* we'll use the new builder strategy */
import org.apache.http.entity.mime.content.ByteArrayBody /* this will encapsulate our file uploads */
import org.apache.http.entity.mime.content.StringBody /* this will encapsulate string params */
import static groovyx.net.http.ContentType.*
import static groovyx.net.http.Method.*
import groovyx.net.http.*

import groovy.xml.XmlUtil  
import org.apache.hadoop.hbase.mapreduce.*
import org.apache.hadoop.io.*
import org.apache.hadoop.hbase.io.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.Mapper.Context

Configuration config = HBaseConfiguration.create();
Job job = new Job(config,'ExampleSummary');
job.setJarByClass(MapToMods.class);     // class that contains mapper and reducer -- for the groovy scriplet -- this

// Scan scan = new Scan();
// In this version, we only process 1 row
Scan scan = new Scan(Bytes.toBytes('oai:quod.lib.umich.edu:MIU01-003496759'),
                     Bytes.toBytes('oai:quod.lib.umich.edu:MIU01-003496759'));
scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

TableMapReduceUtil.initTableMapperJob(
  'sourceRecord',        // input table
  scan,               // Scan instance to control CF and attribute selection
  MapToModsMapper.class,     // mapper class
  Text.class,         // mapper output key
  IntWritable.class,  // mapper output value
  job);

TableMapReduceUtil.initTableReducerJob(
  'inputRecord',        // output table
  null,                 // NO reducer
  job);

job.setNumReduceTasks(1);   // at least one, adjust as required

boolean b = job.waitForCompletion(true);
if (!b) {
  throw new IOException('error with job!');
}


public class MapToModsMapper extends TableMapper<Text, Put>  {

  private static final String MARCXML2MODS_XSLT="http://www.loc.gov/standards/mods/v3/MARC21slim2MODS3.xsl";

  private final IntWritable ONE = new IntWritable(1);

  private Text text = new Text();

  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
       // Get sourceRecord.raw -- which should be a marcxml record in this case
       byte[] val_bytes = value.getValue(Bytes.toBytes('nbk'), Bytes.toBytes('raw'))

       // Split the input into source records, and put the new inputRecord
  }
}
