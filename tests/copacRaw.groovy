#!groovy


// https://mvnrepository.com/artifact/org.apache.hbase/hbase
@Grapes([
  // @GrabResolver(name='mvnRepository', root='http://central.maven.org/maven2/'),
  @Grab(group='org.apache.hbase', module='hbase-client', version='1.2.1'),
  @Grab(group='org.apache.hbase', module='hbase-common', version='1.2.1'),
  @Grab(group='org.apache.hadoop', module='hadoop-common', version='2.7.2'),
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

config = null;
cfg_file = new File('./sync-copac-config.json')
if ( cfg_file.exists() ) {
  config = new JsonSlurper().parseText(cfg_file.text);
}
else {
  config=[:]
}

println("Using config ${config}");

println("NBK Ingest Phase 1 Adapter for COPAC::RAW");

println("Pulling latest messages");
pullLatest(config, cfg_file);
println("All done");

println("Updating config");
cfg_file.delete()
cfg_file << toJson(config);


// addRecord([:]);

System.exit(0);



def addRecord(recordid, recsyn, raw, htable) {

  // We are assuming a table created in the hbase shell using 
  // create 'sourceRecord', 'nbk'

  // Instantiating Configuration class
  // Configuration config = HBaseConfiguration.create();
  // Instantiating HTable class
  // HTable htable = new HTable(config, "sourceRecord");

  try {
    // def recordid = java.util.UUID.randomUUID().toString();
    Put p = new Put(Bytes.toBytes(recordid))
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("sourceid"), Bytes.toBytes("copac") )
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("timestamp"), Bytes.toBytes("${System.currentTimeMillis()}".toString()))
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("recsyn"), Bytes.toBytes(recsyn) )
    // p.add( Bytes.toBytes("nbk"), Bytes.toBytes("canonical"), Bytes.toBytes("CanonicalRecord") )
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("raw"), Bytes.toBytes(raw) )
    htable.put(p);
    // htable.flushCommits()
    // htable.close()
  }
  catch ( Exception e ) {
    e.printStackTrace()
  }
}



def pullLatest(config, cfg_file) {

  def bad_seq = 0;
  def throttle_counter = 50
  def REST_SECONDS = 2;

  // open hbase table for writing a page of results
  Configuration hbase_config = HBaseConfiguration.create();
  HTable htable = new HTable(hbase_config, "sourceRecord");

  int MAX_CTR = 15000000;
  for ( int i=(config.copacCursor?:1); i<MAX_CTR; i++ ) {
    def rec = getRecord(i, config, cfg_file);
    if ( rec == null ) {
      bad_seq++
      println("**BADROW** ${i} ${bad_seq}");
    }
    else {
      bad_seq = 0;

      StringWriter sw = new StringWriter()
      XmlUtil xmlUtil = new XmlUtil()
      xmlUtil.serialize(rec, sw)
      def mods_xml_record=sw.toString();
      def record_id_str = rec.recordInfo.recordIdentifier.text()

      // println("Store record id ${record_id_str} ${mods_xml_record}");
      println("Store record id ${record_id_str} (Ctr ${i})");

      addRecord(record_id_str, 'mods', mods_xml_record, htable);
    }

    if ( bad_seq > 1500 )
      break

    if ( i % throttle_counter == 0 ) {
      println("Throttle rest ${new Date()}");
      htable.flushCommits()
      synchronized(this) {
      println("Updating config");
      cfg_file.delete()
      config.copacCursor=i
      cfg_file << toJson(config);
        Thread.sleep(REST_SECONDS*1000)
      }
    }
  }

  htable.flushCommits()
  htable.close()

}


def getRecord(recno, config, cfg_file) {

  def result = null;

  // def host = http://copac.jisc.ac.uk/id/1500000?style=xml
  def host = 'http://copac.jisc.ac.uk'
  def http = new HTTPBuilder( host )
  def qry = [style:'xml']

  http.ignoreSSLIssues()
  http.contentType = XML
  http.headers = [Accept : 'application/xml']

  http.request( GET, XML ) { req ->

    uri.path = "/id/${recno}"
    uri.query = qry 
    contentType=XML

    // response handler for a success response code:
    response.success = { resp, xml ->
      println("Success ${resp}");
      result = xml;
    }

    response.failure = { resp ->
      println("failure ${resp}");
    }
  }

  result
}

