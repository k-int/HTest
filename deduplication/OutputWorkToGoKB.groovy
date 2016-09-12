#!groovy

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
	  @Grab(group='org.codehaus.groovy.modules.http-builder', module='http-builder', version='0.7.2'),
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.Path
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
import java.security.MessageDigest
import groovy.xml.StreamingMarkupBuilder

import groovyx.net.http.HTTPBuilder
import groovyx.net.http.URIBuilder
import groovyx.net.http.*
import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.Method.GET
import static groovyx.net.http.Method.POST
import java.text.Normalizer
import java.nio.charset.StandardCharsets

Configuration config = HBaseConfiguration.create();
Job job = new Job(config,'Job')
job.setJarByClass(OutputWorkToGoKB.class);

Scan scan = new Scan();
scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
scan.setCacheBlocks(false);  // don't set to true for MR jobs

TableMapReduceUtil.initTableMapperJob(
        'outputWork',        // input table
        scan,               // Scan instance to control CF and attribute selection
        MapOutputWorkToGoKBMapper.class,     // mapper class
        ImmutableBytesWritable.class,
        Text.class,
        job);

job.setReducerClass(MapOutputWorkToGoKBReducer.class)

FileOutputFormat.setOutputPath(job, new Path("/tmp/records/output"));
job.setNumReduceTasks(1);   // at least one, adjust as required

boolean b = job.waitForCompletion(true);
if (!b) {
    throw new IOException('error with job!');
}


public class MapOutputWorkToGoKBMapper extends TableMapper<ImmutableBytesWritable, Text> {

    public static byte[] NBK_FAMILY = Bytes.toBytes('nbk');
    public static byte[] TITLE_COL = Bytes.toBytes('title')
    public static byte[] AUTHOR_COL = Bytes.toBytes('author')
    public static byte[] IDENTIFIERS_COL = Bytes.toBytes('identifiers')

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        String title = null;
	String author = null;
	String identifiers = null;
	
        byte[] title_bytes = value.getValue(NBK_FAMILY, TITLE_COL)
        if (title_bytes) title = new String(title_bytes);

	byte[] author_bytes = value.getValue(NBK_FAMILY, AUTHOR_COL)
        if (author_bytes) author = new String(author_bytes);

	byte[] identifiers_bytes = value.getValue(NBK_FAMILY, IDENTIFIERS_COL)
	if (identifiers_bytes) identifiers = new String (identifiers_bytes);

	String rowstring = new String(row.get(), StandardCharsets.UTF_8);

	 def resourceFieldMap = [ : ]
	 resourceFieldMap['title'] = title
	 resourceFieldMap['medium'] = ""
	 resourceFieldMap['identifiers'] = Eval.me(identifiers)
	 resourceFieldMap['publisherHistory'] = []
	 resourceFieldMap['publishedFrom'] = ""
	 resourceFieldMap['publishedTo'] = ""
	 resourceFieldMap['continuingSeries'] = ""
	 resourceFieldMap['OAStatus'] = ""
	 resourceFieldMap['imprint'] = ""
	 resourceFieldMap['issuer'] = ""
	 resourceFieldMap['variantNames'] = []
	 resourceFieldMap['historyEvents'] = []
	 resourceFieldMap['type'] = 'Serial'

	 resourceFieldMap.identifiers.add([type:"bucket_hash", value:rowstring])

	 Text mapText = new Text(resourceFieldMap.inspect())
	 context.write(row, mapText)
	 
    }
}

public class MapOutputWorkToGoKBReducer extends Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>  {

    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {

      def config = null;
	def cfg_file = new File('./sync-gokb-titles-cfg.json')
	
	if ( cfg_file.exists() ) {
	  config = new JsonSlurper().parseText(cfg_file.text);
	}
	else {
	  config=[:]
	}
	
	def httpbuilder = new HTTPBuilder( 'http://localhost:8080' )
	httpbuilder.auth.basic 'admin','admin'

	//config.uploadUser, config.uploadPass

        for (Text val : values) {
	    def body = addToGoKB(true, httpbuilder,val)
	    context.write(new ImmutableBytesWritable(key),val)
	}
    }
    

  def addToGoKB(dryrun, gokb, mapText){
    
    if ( dryrun ) {
      println(Eval.me(mapText.toString()))
    }
    else {

      def resourceFieldMap = Eval.me(mapText.toString())
    				     
      gokb.request(Method.POST) { req ->
	uri.path='/gokb/integration/crossReferenceTitle'
	body = resourceFieldMap
	requestContentType = ContentType.JSON
	
	response.success = { resp ->
	  println "Success! ${resp.status}"
	}

	response.failure = { resp ->
	  println "Request failed with status ${resp.status}"
	  println (title_data)
	}
	
      } 
    }
  }
}
