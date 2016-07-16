#!groovy


// See
// http://hbase.apache.org/book.html#mapreduce.example
// https://bighadoop.wordpress.com/2012/07/06/hbase-and-hadoop/
// https://mvnrepository.com/artifact/org.apache.hbase/hbase
//
// Don't forget to hbase shell
// create 'countResult', 'nbk'
//

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
job.setJarByClass(mr1.class);     // class that contains mapper and reducer

Scan scan = new Scan();
scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs

TableMapReduceUtil.initTableMapperJob(
  'sourceRecord',        // input table
  scan,               // Scan instance to control CF and attribute selection
  MyMapper.class,     // mapper class
  Text.class,         // mapper output key
  IntWritable.class,  // mapper output value
  job);

org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/mySummaryFile"));
job.setReducerClass(SimpleReducer.class);

// TableMapReduceUtil.initTableReducerJob(
//   'countResult',        // output table
//    MyTableReducer.class,    // reducer class
//    job);

job.setNumReduceTasks(1);   // at least one, adjust as required

boolean b = job.waitForCompletion(true);
if (!b) {
  throw new IOException('error with job!');
}


// <KEYOUT>,<VALUEOUT>
public class MyMapper extends TableMapper<Text, IntWritable>  {

  private final IntWritable ONE = new IntWritable(1);

  private Text text = new Text();

  @Override
  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
       // Get sourceRecord.raw -- which should be a marcxml record in this case
       byte[] val_bytes = value.getValue(Bytes.toBytes('nbk'), Bytes.toBytes('raw'))
       if ( val_bytes ) {
         String val = new String('asterisk')
         // Emit "asterisk", 1 for every input record
         text.set(val);     // we can only emit Writables...
         context.write(text, ONE);
       }
       else {
         println("val_bytes null");
       }
  }
}

// public class MyTableReducer extends TableReducer<Text, IntWritable, ImmutableBytesWritable>  {
// <KEYIN>,<VALUEIN>,<KEYOUT>
public class MyTableReducer extends org.apache.hadoop.hbase.mapreduce.TableReducer<Text, IntWritable, ImmutableBytesWritable>  {

   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

      println("\n\n**Reduce**\n\n");

        int i = 0;
        // Gather all the 'asterisk' values and count up each "1"
        for (IntWritable val : values) {
          i += val.get();
        }
        Put put = new Put(Bytes.toBytes(key.toString()));
        put.add(Bytes.toBytes('nbk'), Bytes.toBytes('count'), Bytes.toBytes(i));

        context.write(null, put);
     }
}

public class SimpleReducer extends Reducer<Text, IntWritable, Text, IntWritable>  {

  public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int i = 0;
    for (IntWritable val : values) {
      i += val.get();
    }
    context.write(key, new IntWritable(i));
  }
}
