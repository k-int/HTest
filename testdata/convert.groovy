import java.io.InputStream
import java.io.FileInputStream
import org.marc4j.marc.Record
import org.marc4j.MarcXmlReader
import org.marc4j.MarcStreamWriter

this.getClass().classLoader.rootLoader.addURL(new File("marc4j-2.7.0.jar").toURL())

println("Converting ${args[0]}")

// Convert input file marcxml to output file marc21
InputStream is = new FileInputStream(args[0])
MarcXmlReader reader = new MarcXmlReader(is);
OutputStream os = new FileOutputStream(args[1]);
MarcStreamWriter writer = new MarcStreamWriter(os);

println("Reading records...");
while (reader.hasNext()) {
  println("Record");
  Record record = reader.next();
  println(record.toString() + "\n************\n");
  writer.write(record);
}
println("Done converting...");
