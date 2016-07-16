# HTest
HTest


Both scripts have grape requirements

edit ~/.groovy/grapeConfig.xml

<?xml version="1.0"?> <ivysettings> <settings defaultResolver="downloadGrapes"/> <resolvers> <chain name="downloadGrapes"> <!-- todo add 'endorsed groovy extensions' resolver here --> <ibiblio name="central" root="http://central.maven.org/maven2/" m2compatible="true"/> <ibiblio name="local" root="file:${user.home}/.m2/repository/" m2compatible="true"/> <filesystem name="cachedGrapes"> <ivy pattern="${user.home}/.groovy/grapes/[organisation]/[module]/ivy-[revision].xml"/> <artifact pattern="${user.home}/.groovy/grapes/[organisation]/[module]/[type]s/[artifact]-[revision].[ext]"/> </filesystem> <ibiblio name="codehaus" root="http://repository.codehaus.org/" m2compatible="true"/> <ibiblio name="ibiblio" m2compatible="true"/> <ibiblio name="java.net2" root="http://download.java.net/maven/2/" m2compatible="true"/> </chain> </resolvers> </ivysettings>




hbase shell example commands


get 'sourceRecord', 'oai:quod.lib.umich.edu:MIU01-003496759'



https://github.com/zepheira/pybibframe

cd /home/ibbo/spikes/bibframe
source ./bin/activate
cd /home/ibbo/spikes/bibframe/pybibframe

Output rdfxml
/home/ibbo/spikes/bibframe/bin/marc2bf  --rdfxml resources.xml /home/ibbo/spikes/bibframe/pybibframe/test/resource/egyptskulls.mrx 



export: http://hbase.apache.org/0.94/book/ops_mgt.html#export

http://stackoverflow.com/questions/16542310/hbase-migration-from-standalone-mode-to-fully-distributed-mode
