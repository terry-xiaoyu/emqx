#!/bin/sh

#sed '/^[[:blank:]]*#/d;s/#.*//' config_examples/node.conf config_examples/cluster.conf config_examples/log.conf config_examples/rpc.conf config_examples/broker.conf config_examples/listeners.conf config_examples/sysmon.conf config_examples/alarm.conf | grep . > ./emqx.conf
sed '/^[[:blank:]]*#/d;s/#.*//' config_examples/emqx.conf | grep . > ./emqx.conf

sed '/^[[:blank:]]*#/d;s/#.*//' config_examples/mqtt.conf | grep . > ./mqtt.conf
