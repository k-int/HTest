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

config = null;
cfg_file = new File('./sync-hathitrust-config.json')
if ( cfg_file.exists() ) {
  config = new JsonSlurper().parseText(cfg_file.text);
}
else {
  config=[:]
}

println("Using config ${config}");

println("NBK Ingest Phase 1 Adapter for hathitrust");

println("Pulling latest messages");
pullLatest(config);
println("All done");

println("Updating config");
cfg_file.delete()
cfg_file << toJson(config);


// addRecord([:]);

System.exit(0);


def addRecord(record) {

  // We are assuming a table created in the hbase shell using 
  // create 'sourceRecord', 'nbk'

  // Instantiating Configuration class
  Configuration config = HBaseConfiguration.create();

  // Instantiating HTable class
  HTable htable = new HTable(config, "sourceRecord");

  try {
    def recordid = java.util.UUID.randomUUID().toString();
    Put p = new Put(Bytes.toBytes(recordid))
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("sourceid"), Bytes.toBytes("hathitrust") )
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("timestamp"), Bytes.toBytes("${System.currentTimeMillis()}".toString()))
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("canonical"), Bytes.toBytes("CanonicalRecord") )
    p.add( Bytes.toBytes("nbk"), Bytes.toBytes("raw"), Bytes.toBytes("RawRecord") )
    htable.put(p);
    htable.flushCommits()
    htable.close()
  }
  catch ( Exception e ) {
    e.printStackTrace()
  }
}



def pullLatest(config) {

  // def cursor = SyncCursor.findByActivity('KBPlusTitles') ?: new SyncCursor(activity:'KBPlusTitles', lastTimestamp:0).save(flush:true, failOnError:true);
  if ( config.cursor == null ) {
    config.cursor = [ lastTimestamp : 0 ];
  }

  def cursor = config.cursor

  // log.debug("Got cursor ${cursor}");

  // http://quod.lib.umich.edu/cgi/o/oai/oai?verb=ListRecords&metadataPrefix=marc21&set=hathitrust
  doSync('http://quod.lib.umich.edu','/cgi/o/oai/oai', 'marc21', 'hathitrust', cursor) { r ->

    def result = [:]

    try {
      def sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

      def record_ts_str = r.header.datestamp.text()
      result.ts = sdf.parse(record_ts_str)?.getTime()

      def identifiers = []
      println("Process record ${r}");
      // def title_id = r.metadata.kbplus.title.@id.text()
    }
    catch ( Exception e ) {
      result.message = e.message;
    }

    result
  }
}

public doSync(host, path, prefix, set, cursor, notificationTarget) {
  println("Get latest changes ${host} ${path} ${prefix} ${cursor}");

  def http = new HTTPBuilder( host )
  http.ignoreSSLIssues()
  http.contentType = XML
  http.headers = [Accept : 'application/xml']
  def lastTimestamp = 0;

  def more = true
  println("Attempt get...");

  def resumption=null

  // perform a GET request, expecting XML response data
  while ( more ) {

    println("Make request....");

    def qry = null
    if ( resumption ) {
        println("Processing resumption");
        qry = [ verb:'ListRecords', resumptionToken: resumption ]
    }
    else {
        println("Fetch all records since ${(cursor?.lastTimestamp)?:0}");
        def the_date = new Date(cursor.lastTimestamp)
        def sdf = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        def from=sdf.format(the_date)
        println("Requesting records since ${cursor.lastTimestamp} :: ${from}");
        qry = [ verb:'ListRecords', metadataPrefix: prefix, from:from, set:set ]
    }

    println("Query params : ${qry} ");

    http.request( GET, XML ) { req ->

      uri.path = path
      uri.query = qry 
      contentType=XML

      // response handler for a success response code:
      response.success = { resp, xml ->
        int ctr=0
        println("In response handler");
        println("Status ${resp.statusLine}")
        println("xml response object :: ${xml?.class.name}");

        // def slurper = new groovy.util.XmlSlurper()
        // def parsed_xml = slurper.parseText(xml.text)
        // def parsed_xml = slurper.parse(xml)

        xml?.'ListRecords'?.'record'.each { r ->
          def clr = notificationTarget(r)
          println(clr);
          ctr++
          if ( clr.ts > lastTimestamp )
            lastTimestamp = clr.ts
        }

        if ( ctr > 0 ) {
          more=true
        }
        else {
          more=false
        }

        resumption = xml?.'ListRecords'?.'resumptionToken'
        println("Complete ${ctr} ${more} ${resumption}");
      }

      response.error = { err ->
        println(err)
      }
    }
    // update cursor
    cursor.lastTimestamp = lastTimestamp
    // cursor.save(flush:true, failOnError:true);
  }
}

