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
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.*
import org.apache.hadoop.io.*
import org.apache.hadoop.hbase.io.*
import org.apache.hadoop.hbase.client.*
import org.apache.hadoop.mapreduce.*
import org.apache.hadoop.mapreduce.Mapper.Context
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat

Configuration config = HBaseConfiguration.create();
Job job = new Job(config,'ExampleSummary');
job.setJarByClass(Stats.class);     // class that contains mapper and reducer -- for the groovy scriplet -- this

Scan scan = new Scan();
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
job.setReducerClass(MyReducer.class);
job.setNumReduceTasks(1);   // at least one, adjust as required
job.setOutputKeyClass(IntWritable.class);
job.setOutputValueClass(IntWritable.class);
		
org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/records/input"));

boolean b = job.waitForCompletion(true);
if (!b) {
  throw new IOException('error with job!');
}

Job job2 = new Job(new Configuration());
job2.setJarByClass(Stats.class);
job2.setJobName("Build Histogram");

org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job2, new org.apache.hadoop.fs.Path("/tmp/records/input"));
org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job2, new org.apache.hadoop.fs.Path("/tmp/records/output"));

job2.setMapperClass(MyMapper.class);
job2.setReducerClass(MyReducer2.class);
job2.setInputFormatClass(KeyValueTextInputFormat.class);
job2.setMapOutputKeyClass(IntWritable.class);
job2.setMapOutputValueClass(IntWritable.class);
job2.setOutputKeyClass(IntWritable.class);
job2.setOutputValueClass(IntWritable.class);

boolean b2 = job2.waitForCompletion(true);
if (!b2) {
    throw new IOException('error with job2!');
}

// <KEYOUT>,<VALUEOUT>
public class InputRecordsMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>  {

  public static byte[] NBK_FAMILY = Bytes.toBytes('nbk');
  public static byte[] COPAC_RECORD_ID_COL = Bytes.toBytes('copac_record_id')
  public static byte[] WORK_HASH_COL = Bytes.toBytes('work_hash')

  @Override
  public void map(ImmutableBytesWritable row, 
                  Result value, 
                  Context context) throws IOException, InterruptedException {

    byte[] copac_record_id_bytes = value.getValue(NBK_FAMILY, COPAC_RECORD_ID_COL)
    byte[] work_hash_bytes = value.getValue(NBK_FAMILY, WORK_HASH_COL)

    if ( copac_record_id_bytes && work_hash_bytes ) {
      context.write(new ImmutableBytesWritable((byte[])copac_record_id_bytes), new ImmutableBytesWritable((byte[])work_hash_bytes));
    }
  }


}

// Turn a list of copac-record-id, work hash into a sum of the number of unique work hashes and a 1 after that - so if there are 2, return 2,1 -- so we can count up the total
public class MyReducer extends Reducer<ImmutableBytesWritable, ImmutableBytesWritable, IntWritable, IntWritable>  {

        private final IntWritable ONE = new IntWritable(1);

        @Override
        public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {
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


public class MyMapper extends Mapper< Text, Text, IntWritable, IntWritable> {

    IntWritable newKey = new IntWritable();
    IntWritable newVal = new IntWritable();
    @Override
    public void map(Text key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
            throws IOException, InterruptedException {

         newVal.set(Integer.parseInt(value.toString()));
        newKey.set(Integer.parseInt(key.toString()));
        context.write(newKey, newVal);
    }
}

public class MyReducer2 extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>  {

    //Thomas
    // Had to explicitly refer to context as ...Reducer.Context as it was using the Mapper.Context
    // This is why the @Override tag was failing saying one didn't exist in the superclass
    // and why it was just only writing the bytes named about in the context.write() in the Mapper class
    // as the reduce in MyReducer and MyCombiner were never being run

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values,  org.apache.hadoop.mapreduce.Reducer.Context  context) throws IOException, InterruptedException {
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


