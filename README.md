# Duplog #
Duplog will deduplicate messages from multiple similar streams.  This can be used to take syslog messages from multiple redundant rsyslog servers, strip out duplicates between the streams, and produce a complete record of log messages for an application like Splunk.

## Building ##
You need Apache Ant and a Java JDK installed.  Then run

    % ant

to fetch all of the project's dependencies and compile the source.  An executable jar file can be built with

    % ant jar

## Running ##

*   On each rsyslog server, create a file such as:

        % cat /usr/local/libexec/rsyslog/send-to-rabbitmq 
        #!/bin/sh
        exec /usr/bin/java -jar /path/to/duplog.jar inject

    Then define a configuration in rsyslog such as:

        $ModLoad omprog
        $ActionOMProgBinary /usr/local/libexec/rsyslog/send-to-rabbitmq
        *.* :omprog:

    Finally, make sure [RabbitMQ](http://www.rabbitmq.com/) is running locally on the default port.

*   On the destination server, where deduplicated log messages are required, simply run

        % java -jar /path/to/duplog.jar extract [-o OUTPUT_FILE] [-r REDIS_SERVER] syslog_server [syslog_server ...]

    where `syslog_server` is the hostname of a syslog server running RabbitMQ as above.  A [Redis](http://redis.io/) server must be available to perform deduplication.  It should be running on the default port with the following parameters set in `/etc/redis/redis.conf`:

        maxmemory <bytes>    # each unique message will consume about 100 bytes; configure based on messaging rate and available memory
        maxmemory-policy allkeys-lru
