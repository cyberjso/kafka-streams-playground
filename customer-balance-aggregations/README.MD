# Kafka streams - Observability - Customer Balance Aggregation

This is a kafka stream application that aggregates transactions per customer. Main intention f this poc is to show <b>how to extract metrics from a kafka streams pipeline</b> using <b>micrometer</b> 

## Requirements to run this application in your local environment (what needs to be installed) 

<ul>
<li>running on linux or some sort of bash console</li>
<li>jdk 1.11+</li>
<li>docker 20+</li>
<li>docker compose 20+</li>
</ul>

## Steps to run  this application

1. Compile the stream application and spin up the whole environment.
<pre>./run.sh</pre>

2. Check kafka, zookeeper and the stream application are up and running
<pre>
docker-compose ps
</pre>

3. The stream application prints on the console all metrics being monitored. Micrometer has built in support to monitor kafka streams pipelines. In order to see all metrics being watched run the following command:
<pre>docker-compose logs customer-balance-aggregations | grep MeterId</pre>
This is approach is not recommended for production. Ideally these metrics sent via statsd to some sort of agent running on the environment. This last should then forward these metrics to a proper observability platform.