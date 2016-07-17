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
import java.security.MessageDigest

import java.text.Normalizer



Configuration config = HBaseConfiguration.create();
Job job = new Job(config,'ExampleSummary');
job.setJarByClass(Stats.class);     // class that contains mapper and reducer -- for the groovy scriplet -- this

Scan scan = new Scan();
// In this version, we only process 1 row
// Try a hathitrust record
// Scan scan = new Scan(Bytes.toBytes('oai:quod.lib.umich.edu:MIU01-003496759'), Bytes.toBytes('oai:quod.lib.umich.edu:MIU01-003496759'));
// try a copac record
// Scan scan = new Scan(Bytes.toBytes('1200'), Bytes.toBytes('1200'));


scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
scan.setCacheBlocks(false);  // don't set to true for MR jobs
// set other scan attrs


TableMapReduceUtil.initTableMapperJob(
  'inputRecord',        // input table
  scan,               // Scan instance to control CF and attribute selection
  InputRecordsMapper.class,     // mapper class
  ImmutableBytesWritable.class,
  ImmutableBytesWritable.class,
  job);

job.setMapOutputKeyClass(ImmutableBytesWritable.class);
job.setMapOutputValueClass(ImmutableBytesWritable.class);
job.setCombinerClass(MyCombiner.class);
// job.setMapperClass(MapToModsMapper.class);
job.setReducerClass(MyReducer.class);
job.setNumReduceTasks(1);   // at least one, adjust as required
// job.setOutputFormatClass(NullOutputFormat.class);

job.setOutputKeyClass(IntWritable.class);
job.setOutputValueClass(IntWritable.class);
		
org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/copacStats"));

boolean b = job.waitForCompletion(true);
if (!b) {
  throw new IOException('error with job!');
}


// <KEYOUT>,<VALUEOUT>
public class InputRecordsMapper extends TableMapper<ImmutableBytesWritable, Put>  {

  private static final String MARCXML2MODS_XSLT="http://www.loc.gov/standards/mods/v3/MARC21slim2MODS3.xsl";
  public static List STOPWORDS = []
  public static byte[] NBK_FAMILY = Bytes.toBytes('nbk');
  public static byte[] SYN_COL = Bytes.toBytes('recsyn')
  public static byte[] SRC_COL = Bytes.toBytes('sourceid')
  public static byte[] COPAC_RECORD_ID_COL = Bytes.toBytes('copac_record_id')
  public static byte[] WORK_HASH_COL = Bytes.toBytes('work_hash')

  @Override
  public void map(ImmutableBytesWritable row, 
                  Result value, 
                  Context context) throws IOException, InterruptedException {

    String copac_record_id = null;
    String work_hash = null;

    byte[] copac_record_id_bytes = value.getValue(NBK_FAMILY, COPAC_RECORD_ID_COL)
    byte[] work_hash_bytes = value.getValue(NBK_FAMILY, WORK_HASH_COL)

    if ( copac_record_id_bytes && work_hash_bytes ) {
      context.write(new ImmutableBytesWritable((byte[])copac_record_id_bytes), new ImmutableBytesWritable((byte[])work_hash_bytes));
    }
  }

  private static Put getContributorRecord(String contributor_record_id, 
                                          String contributor_record, 
                                          String recsyn, 
                                          String title_hash_str, 
                                          String title_hash, 
                                          String work_hash, 
                                          String instance_hash) throws IOException {

    Put put = new Put(Bytes.toBytes(contributor_record_id));
    put.add( Bytes.toBytes("nbk"), Bytes.toBytes("raw"), Bytes.toBytes(contributor_record) )
    put.add( Bytes.toBytes("nbk"), Bytes.toBytes("recsyn"), Bytes.toBytes(recsyn) )
  
    if ( title_hash_str ) 
      put.add( Bytes.toBytes("nbk"), Bytes.toBytes("title_hash_str"), Bytes.toBytes(title_hash_str) )

    if ( title_hash ) 
      put.add( Bytes.toBytes("nbk"), Bytes.toBytes("title_hash"), Bytes.toBytes(title_hash) )

    if ( work_hash ) 
      put.add( Bytes.toBytes("nbk"), Bytes.toBytes("work_hash"), Bytes.toBytes(work_hash) )

    if ( instance_hash ) 
      put.add( Bytes.toBytes("nbk"), Bytes.toBytes("instance_hash"), Bytes.toBytes(instance_hash) )

    return put;
  }


  private static Put resultToPut(ImmutableBytesWritable key, Result result) throws IOException {
    Put put = new Put(key.get());
    for (KeyValue kv : result.raw()) {
      put.add(kv);
    }
    return put;
  }

  private static String getBucket(String s) {
    MessageDigest.getInstance("MD5").digest(s.bytes).encodeHex().toString()
  }

  private static String normalise(List components) {
    def sw = new StringWriter()
    def first = true;
    components.each { c ->
      if ( c ) {
        if ( first ) { first = false; } else { sw.write (' '); }
        sw.write(c)
      }
    }

    return norm2(sw.toString()).trim().toLowerCase()
  }

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

}

public class MyReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>  {


 	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           IntWritable result = new IntWritable();
           int i = 0;
           // Gather all the 'asterisk' values and count up each "1"
           for (IntWritable val : values) {
                i += val.get();
           }

           result.set(i)
           context.write(key, result);
   	}
}

// Turn a list of copac-record-id, work hash into a sum of the number of unique work hashes and a 1 after that - so if there are 2, return 2,1 -- so we can count up the total
public class MyCombiner extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, IntWritable, IntWritable>  {

        private final IntWritable ONE = new IntWritable(1);

        public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
                IntWritable result = new IntWritable();
                int i = 0;
                def unique_work_ids = []
                for (ImmutableBytesWritable val : values) {
                  String v = new String(val.get());
                  if (unique_work_ids.contains( v ) ) {
                  }
                  else {
                    unique_work_ids.add(v);
                  }
                }

                result.set(unique_work_ids.size())
                context.write(result, ONE);
        }
}


