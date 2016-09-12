
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
import java.security.MessageDigest
import groovy.xml.StreamingMarkupBuilder
import java.text.Normalizer


Configuration config = HBaseConfiguration.create();
Job job = new Job(config,'Job')
job.setJarByClass(InputToOutputWork.class);

Scan scan = new Scan();
scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
scan.setCacheBlocks(false);  // don't set to true for MR jobs

TableMapReduceUtil.initTableMapperJob(
        'inputRecord',        // input table
        scan,               // Scan instance to control CF and attribute selection
        MapInputToOutputMapper.class,     // mapper class
        ImmutableBytesWritable.class,
        Put.class,
        job);

//org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/records/input"));


TableMapReduceUtil.initTableReducerJob(
        'outputWork',        // output table
        InputToOutputReducer.class, //reducer class
        job);

job.setNumReduceTasks(1);   // at least one, adjust as required

boolean b = job.waitForCompletion(true);
if (!b) {
    throw new IOException('error with job!');
}


public class MapInputToOutputMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public static byte[] NBK_FAMILY = Bytes.toBytes('nbk');
    public static byte[] SYN_COL = Bytes.toBytes('recsyn');
    public static byte[] WORK_COL = Bytes.toBytes('work_hash')
    public static byte[] TITLE_COL = Bytes.toBytes('title_hash_str')

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        String recsyn = null;
        String recwork = null;
        String title = null;

        byte[] syn_bytes = value.getValue(NBK_FAMILY, SYN_COL)
        if (syn_bytes) recsyn = new String(syn_bytes);
        byte[] work_bytes = value.getValue(NBK_FAMILY, WORK_COL)
        if (work_bytes) recwork = new String(work_bytes);
        byte[] title_bytes = value.getValue(NBK_FAMILY, TITLE_COL)
        if (title_bytes) title = new String(title_bytes);

        if (recsyn?.equals('mods') && (recwork != null)) {

            String record_xml_as_text = new String(value.getValue(NBK_FAMILY, Bytes.toBytes('raw')));
            def parsed_xml = new XmlSlurper().parseText(record_xml_as_text).declareNamespace(tag0: "tag0");
            def discriminator = null;

            String title_hash_str = new String (parsed_xml.titleInfo.title?.text());

	    String subtitle = new String(parsed_xml.titleInfo.subTitle?.text());
	    
	    def name_node = parsed_xml.'**'.find { name ->
	      name.@type == 'personal'
	    }

	    def identifiers = parsed_xml.'*'.findAll { node ->
	      node.name() == 'identifier' && node.@type.text().length() > 0
	    }

	    String edition = new String(parsed_xml.classification?.@edition.text())

	    String name = name_node ? new String(name_node?.namePart?.text()) : ''
	    /*
	    println(">>>>")
	    println("title: " + title_hash_str)
	    
	    if(name.length() > 0){
	      println("name: " + name)
	    }
	    if(subtitle.length() > 0){
	      println("subtitle: " + subtitle)
	    }
	     if(edition.length() > 0){
	      println("edition: " + edition)
	    }

	    */
	    def identifierMap = []
	    
	     identifiers.each{
	       if(it.text().length() > 0 )
		 identifierMap.add([type:it.@type, value:it.text()])
	     }

	     def identifier_str = identifierMap.inspect()
	     
	     def key = new ImmutableBytesWritable(work_bytes)
	     Put put = new Put(key.get());
             put.add(Bytes.toBytes("nbk"), Bytes.toBytes("title"), Bytes.toBytes(title_hash_str));
	     put.add(Bytes.toBytes("nbk"), Bytes.toBytes("subtitle"), Bytes.toBytes(subtitle));
	     put.add(Bytes.toBytes("nbk"), Bytes.toBytes("author"), Bytes.toBytes(name));
	     put.add(Bytes.toBytes("nbk"), Bytes.toBytes("edition"), Bytes.toBytes(edition));
	     put.add(Bytes.toBytes("nbk"), Bytes.toBytes("identifiers"), Bytes.toBytes(identifier_str));

//            def after_hash = normalise(['BKM', parsed_xml.titleInfo.title?.text(), parsed_xml.titleInfo.subTitle?.text(), discriminator])
            context.write(new ImmutableBytesWritable(work_bytes), put);
            }

        }
/*
    // For use if string needs to be normalised
    private static String normalise(List components) {
        def sw = new StringWriter();
        def first = true;
        components.each { c ->
            if ( c ) {
                if ( first ) { first = false; } else { sw.write (' '); }
                sw.write(c)
            }
        }

        return norm2(sw.toString()).trim().toLowerCase()
    }

    public static List STOPWORDS = []

    public static String norm2(String s) {


        // Ensure s is not null.
        if (!s) s = "";

        // Normalize to the D Form and then remove diacritical marks.
        s = Normalizer.normalize(s, Normalizer.Form.NFD)
        s = s.replaceAll("\\p{InCombiningDiacriticalMarks}+","");

        // lowercase.
        s = s.toLowerCase();

        // Break apart the string.
        String[] components = s.split("\\s");

        // Re-piece the array back into a string.
        String normstring = "";
        components.each { String piece ->
            if ( !STOPWORDS.contains(piece)) {

                // Remove all unnecessary characters.
                normstring += piece.replaceAll("[^a-z0-9]", " ") + " ";
            }
        }

        // normstring.trim().replaceAll(" +", " ")
        // Do spaces really add anything for our purposes here, or are random spaces more likely to creep in to the
        // source records and throw the matching? Suspect the latter, kill them for now
        normstring.trim().replaceAll(' ', '')
    }
    */
}

public class InputToOutputReducer extends TableReducer<ImmutableBytesWritable, Put, ImmutableBytesWritable>
{
    @Override
    public void reduce(ImmutableBytesWritable key, Iterable<Put> values, org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
      
      context.write(null, values.first());

    }
}
