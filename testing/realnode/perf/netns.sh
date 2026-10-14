#!/bin/sh
set -eu
for namespace in perf-client perf-node perf-target perf-bottleneck; do
    ip netns add "$namespace"
    ip -n "$namespace" link set lo up
done
ip link add perf-cn type veth peer name perf-bc
ip link add perf-nc type veth peer name perf-bn
ip link add perf-nt type veth peer name perf-tn
ip link set perf-cn netns perf-client
ip link set perf-nc netns perf-node
ip link set perf-bc netns perf-bottleneck
ip link set perf-bn netns perf-bottleneck
ip link set perf-nt netns perf-node
ip link set perf-tn netns perf-target
ip -n perf-client addr add 10.202.1.1/24 dev perf-cn
source=101
while [ "$source" -le 164 ]; do
    ip -n perf-client addr add "10.202.1.$source/24" dev perf-cn
    source=$((source + 1))
done
ip -n perf-node addr add 10.202.1.2/24 dev perf-nc
ip -n perf-node addr add 10.202.2.1/24 dev perf-nt
ip -n perf-target addr add 10.202.2.2/24 dev perf-tn
ip -n perf-bottleneck link add perf-br type bridge
ip -n perf-bottleneck link set perf-bc master perf-br
ip -n perf-bottleneck link set perf-bn master perf-br
for pair in 'perf-client perf-cn' 'perf-node perf-nc' 'perf-node perf-nt' 'perf-target perf-tn' 'perf-bottleneck perf-bc' 'perf-bottleneck perf-bn' 'perf-bottleneck perf-br'; do
    ip -n "${pair% *}" link set "${pair#* }" up
done
for pair in 'perf-client perf-cn' 'perf-node perf-nc' 'perf-node perf-nt' 'perf-target perf-tn' 'perf-bottleneck perf-bc' 'perf-bottleneck perf-bn'; do
    ip -n "${pair% *}" link set "${pair#* }" gso_max_size 1500 gro_max_size 1500
done
ip netns exec perf-client tc qdisc add dev perf-cn root netem delay 15ms limit 4096
ip netns exec perf-target tc qdisc add dev perf-tn root netem delay 15ms limit 4096
ip netns exec perf-node tc qdisc add dev perf-nc root fq limit 4096
ip netns exec perf-node tc qdisc add dev perf-nt root fq limit 4096
ip netns exec perf-bottleneck tc qdisc add dev perf-bc root netem rate 1gbit limit 4096
ip netns exec perf-bottleneck tc qdisc add dev perf-bn root netem rate 1gbit limit 4096
exec "$@"
