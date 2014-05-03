WikipediaXMLHadoopParser
========================
<h2>Input :</h2> 
Wikipedia dump, metadata history XML files (XXwiki-latest-stub-meta-historyXX.xml.gz) from http://dumps.wikimedia.org/

<h2>Output :</h2>
flats files with the following structure
ArticleTitle +"\t" + EditorUsername + "\t" +EditorType+"\t" + ArticleSizeAfterModification + "\t"+ IsMinorChange + "\t" + EditorComment+"\t"+ModificationTimestamp

One file per year (year partitionner)


<h2>How to use the job :</h2>
hadoop jar WikipediaXmlHadoopParser hadoop.wikipedia.parse.job.WikiParseDriver InputFolder OutputFolder

<h2>Use output file with Hive : </h2>
hive> create EXTERNAL table wikipedia(title String, user String, typeUser String,   size Int, typeModification String, comment String, date Timestamp) PARTITIONED  BY (YEAR int) ROW FORMAT DELIMITED FIELDS TERMINATED BY "\t" LOCATION "/";

hive> ALTER TABLE WIKIPEDIA ADD PARTITION (YEAR="2001");
....
ALTER TABLE WIKIPEDIA ADD PARTITION (YEAR="2014");

Move corresponding output files in the right folder (hadoop dfs -mv  /OutputFile/2001 /year=2001....) 
