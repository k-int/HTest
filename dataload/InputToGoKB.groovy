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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.*
import org.apache.hadoop.hbase.io.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.hbase.mapreduce.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Mapper.Context
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.mapred.lib.NullOutputFormat

import static groovy.json.JsonOutput.*
import groovy.json.JsonSlurper
import groovy.time.*
import groovy.xml.StreamingMarkupBuilder
import groovy.xml.XmlUtil
import groovyx.net.http.HTTPBuilder
import groovyx.net.http.URIBuilder
import groovyx.net.http.*
import static groovyx.net.http.ContentType.JSON
import static groovyx.net.http.Method.GET
import static groovyx.net.http.Method.POST

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.text.Normalizer
import java.util.Properties

Configuration config = HBaseConfiguration.create();
Job job = new Job(config,'Job')
job.setJarByClass(InputToGoKB.class);

Scan scan = new Scan();
scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
scan.setCacheBlocks(false);  // don't set to true for MR jobs

TableMapReduceUtil.initTableMapperJob(
        'inputRecord',        // input table
        scan,               // Scan instance to control CF and attribute selection
        MapInputToGoKBMapper.class,     // mapper class
        ImmutableBytesWritable.class,
        Text.class,
        job);

job.setReducerClass(MapInputToGoKBReducer.class)

FileOutputFormat.setOutputPath(job, new Path("/tmp/records/output"));
job.setNumReduceTasks(1);   // at least one, adjust as required

boolean b = job.waitForCompletion(true);
if (!b) {
    throw new IOException('error with job!');
}


public class MapInputToGoKBMapper extends TableMapper<ImmutableBytesWritable, Text> {

    public static byte[] NBK_FAMILY = Bytes.toBytes('nbk');
    public static byte[] SYN_COL = Bytes.toBytes('recsyn');
    public static byte[] WORK_COL = Bytes.toBytes('work_hash')
    
    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        String recsyn = null;
        String recwork = null;
        
        byte[] syn_bytes = value.getValue(NBK_FAMILY, SYN_COL)
        if (syn_bytes) recsyn = new String(syn_bytes);
        byte[] work_bytes = value.getValue(NBK_FAMILY, WORK_COL)
        if (work_bytes) recwork = new String(work_bytes);
        
        if (recsyn?.equals('mods') && (recwork != null)) {

            String record_xml_as_text = new String(value.getValue(NBK_FAMILY, Bytes.toBytes('raw')));
            def parsed_xml = new XmlSlurper().parseText(record_xml_as_text).declareNamespace(tag0: "tag0");

            String title = new String (parsed_xml.titleInfo.title?.text());
	    String subtitle = new String(parsed_xml.titleInfo.subTitle?.text());
	    String edition = new String(parsed_xml.classification?.@edition.text())
	    
	    def name_node = parsed_xml.'**'.find { name ->
	      name.@type == 'personal'
	    }

	    def identifiers = parsed_xml.'*'.findAll { node ->
	      node.name() == 'identifier' && node.@type.text().length() > 0
	    }

	    def recordIdentifiers = parsed_xml.'**'.findAll { node ->
	      node.name() == 'recordIdentifier'
	    }

	    String name = name_node ? new String(name_node?.namePart?.text()) : ""

	    String rowstring = new String(row.get(), StandardCharsets.UTF_8);

	    def resourceFieldMap = [ : ]
	    resourceFieldMap['title'] = title
	    resourceFieldMap['medium'] = ""
	    resourceFieldMap['identifiers'] = []
	    resourceFieldMap['publisherHistory'] = []
	    resourceFieldMap['publishedFrom'] = ""
	    resourceFieldMap['publishedTo'] = ""
	    resourceFieldMap['continuingSeries'] = ""
	    resourceFieldMap['OAStatus'] = ""
	    resourceFieldMap['imprint'] = ""
	    resourceFieldMap['issuer'] = ""
	    resourceFieldMap['variantNames'] = []
	    resourceFieldMap['historyEvents'] = []
	    resourceFieldMap['type'] = 'Monograph'

	     identifiers.each{
	       if(it.text().length() > 0 )
		 resourceFieldMap.identifiers.add([type:it.@type.text(), value:it.text()])
	     }

	     recordIdentifiers.each{
	       if(it.text().length() > 0 )
		 resourceFieldMap.identifiers.add([type:it.@source.text(), value:it.text()])
	     }
	     
	    resourceFieldMap.identifiers.add([type:"bucket_hash", value:recwork])

	    Text mapText = new Text(resourceFieldMap.inspect())
	    
            context.write(new ImmutableBytesWritable(work_bytes), mapText);
	}
    }
}

public class MapInputToGoKBReducer extends Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>  {

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

      for (Text val : values) {
	def body = addToGoKB(false, httpbuilder,val)
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
	  println body
	}

	response.failure = { resp ->
	  println "Request failed with status ${resp.status}"
	  println body
	  
	}
	
      } 
    }
  }
}
