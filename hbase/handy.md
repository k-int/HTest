

In hbase shell

list - list tables
count 'inputRecord' - how many rows in input record
describe 'inputRecord' - Describe table
get 'inputRecord','fff6e866-0878-431b-b773-1facce269fa8'   -- Get an example row



To get MODS records out of hbase

export the raw column and grab the output

./hbase shell <<< "scan 'inputRecord',{COLUMNS=>'nbk:raw'}" > /tmp/copac_data.tsv

Pull out only the data rows
cat /tmp/copac_data.tsv | grep "column=" > /tmp/copac_data2.tsv

Trm off all the unwanted crap from the start of the line
sed s/^.*value=// < ./copac_data2.tsv > copac_data3.tsv


We now have a file with 1 record per line

ctr=0
mkdir mods_records
mkdir marcxml_records
while read in
do 
  echo "hello $ctr"
  ctr=`expr $ctr + 1`
  echo $in | sed s/x0A/\\n/g > mods_records/$ctr.mods
  xsltproc ./MODS3-4_MARC21slim_XSLT1-0.xsl mods_records/$ctr.mods > marcxml_records/$ctr.marcxml
done < copac_data3.tsv

