# QCon São Paulo - Flink + OpenNLP Example

Code for the talk **Sistemas de Processamento de Linguagem Natural na Prática**.

This code was forked from https://github.com/thygesen/DC-FlinkMeetup and was also used in:

* The 2nd Washington DC Flink Meetup on Nov 19, 2015 at CapitalOne, Vienna, VA
* Big Data Madrid 2017, November 16-17 

# Instructions

* Check the docker howto in the docker folder

* Log in to Kibana at http://localhost:5601

* Create the index (refer to `PUT my-index{...}` at `docker/mappings.txt`)

* Run `org.apache.opennlp.flink.examples.NewsPipeline` passing a file with -file (one document per line) and the desired parallelism.

* Create some visualization and dashboards in Kibana

