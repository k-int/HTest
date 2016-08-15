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
job.setNumReduceTasks(1);   // at least one, adjust as required

boolean b = job.waitForCompletion(true);
if (!b) {
    throw new IOException('error with job!');
}


public class MapOutputWorkToGoKBMapper extends TableMapper<ImmutableBytesWritable, Text> {

    public static byte[] NBK_FAMILY = Bytes.toBytes('nbk');
    public static byte[] TITLE_COL = Bytes.toBytes('title')

    @Override
    public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

        String title = null;

        byte[] title_bytes = value.getValue(NBK_FAMILY, TITLE_COL)
        if (title_bytes) title = new String(title_bytes);

        context.write(row, new Text(title));

    }
}

public class MapOutputWorkToGoKBReducer extends Reducer<ImmutableBytesWritable, Text, ImmutableBytesWritable, Text>  {

    public void reduce(ImmutableBytesWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Text text = null

        for (Text val : values) {
            text = val
        }
        
    }
}