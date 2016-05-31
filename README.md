# pybgpstream-spark-examples

Spark Hello-World Script:


The Hello-World script computes the number of elems and records for each BGP speaker over time.

For each VP, the script parses all the RIB and Updates dumps and it provides information aggregated per day.
Specifically, it extracts the number of elems, and the number of records, and it outputs:

* the number elems
* the number of records

Source code and documentation are available here.


Spark RoutingTable Script:


The RoutingTable script computes the size of the IPv4 and IPv6 routing tables of each BGP speaker over time, i.e., the number of unique prefixes and unique ASNs observed in the Adj-RIB-out of each VP.

For each VP, the script parses all the RIB and Updates dumps and it provides information aggregated per day.
Specifically, it extracts the set of prefixes announced, and the set of AS numbers observed in each AS path, and
it outputs:

* the number of unique IPv4 prefixes
* the number of unique IPv6 prefixes
* the number of unique ASNs observed in AS paths associated with IPv4 prefixes
* the number of unique ASNs observed in AS paths associated with IPv6 prefixes

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

Spark Moas Script:


The MOAS (Multi Origin AS) script detects for each each BGP speaker the set of MOASes for IPv4 and IPv6 over time, i.e.,
the number of unique IPv4 and IPv6 prefixes involved in a MOAS as well as the multiple origin AS numbers announcing such prefixes.

For each VP, the script parses all the RIB and Updates dumps and it provides information aggregated per day.
Specifically, it extracts all the routed prefixes, and the subset of prefixes involved in a MOAS along with the list of unique
origin ASNs sets.

It outputs:

* the number of unique IPv4 prefixes
* the number of unique IPv6 prefixes
* the number of unique IPv4 prefixes involved in a MOAS
* the number of unique IPv6 prefixes involved in a MOAS
* the number of unique sets of origin ASNs involved in a MOAS, associated with IPv4 prefixes
* the number of unique sets of origin ASNs involved in a MOAS, associated with IPv6 prefixes

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

Spark Transit Script:


The Transit script computes the number of transit ASes observed for both IPv4 and IPv6 routes from each BGP speaker over time.
We define an AS as transit for a specific prefix if it appears in the middle of its AS path, in other words, we
assume the AS to forward part of the traffic related to the prefix.

For each VP, the script parses all the RIB and Updates dumps and it provides information aggregated per day.
Specifically, it extracts the set of prefixes announced, the set of prefixes which have at least one transit AS,
the set of transit ASes, and some statistics about the number of prefixes for the transit ASes.
The Transit script produces as output:

* the number of unique IPv4 prefixes
* the number of unique IPv6 prefixes
* the number of unique IPv4 prefixes that have at least one transit AS
* the number of unique IPv6 prefixes that have at least one transit AS
* the number of unique transit ASes observed in AS paths associated with IPv4 prefixes
* the number of unique transit ASes observed in AS paths associated with IPv6 prefixes


* the minimum number of unique IPv4 prefixes for which a transit AS number forwards traffic
* the average number of unique IPv4 prefixes for which a transit AS number forwards traffic
* the maximum number of uniq IPv4 prefixes for which a transit AS number forwards traffic
* the standard deviation of the number of unique IPv4 prefixes for which a transit AS number forwards traffic

* the minimum number of unique IPv6 prefixes for which a transit AS number forwards traffic
* the average number of unique IPv6 prefixes for which a transit AS number forwards traffic
* the maximum number of unique IPv6 prefixes for which a transit AS number forwards traffic
* the standard deviation of the number of unique IPv6 prefixes for which a transit AS number forwards traffic

-------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------

Spark Community Script:


The Community script computes statistics about the BGP community attribute visibility and propagation.

For each VP, the script parses all the RIB and Updates dumps and it provides information aggregated per day.
Specifically, it extracts:
the set of prefixes announced,
the set of prefixes which are associated with at least one community,
the set of prefixes which are associated with at least one community "originated" by the VP,
the set of prefixes which are associated with at least one community which is not "originated" by the VP,
the set of AS numbers that "originate" communities,
the set of communities

The Community script produce as output:

* the number of unique IPv4 prefixes
* the number of unique IPv6 prefixes
* the number of unique IPv4 prefixes associated with at least one community
* the number of unique IPv6 prefixes associated with at least one community
* the number of unique IPv4 prefixes associated with at least one community "originated" by the VP
* the number of unique IPv6 prefixes associated with at least one community "originated" by the VP
* the number of unique IPv4 prefixes associated with at least one community which is not "originated" by the VP
* the number of unique IPv6 prefixes associated with at least one community which is not "originated" by the VP
* the number of unique ASes that "originate" communities associated with IPv4 prefixes
* the number of unique ASes that "originate" communities associated with IPv6 prefixes
* the number of unique communities associated with IPv4 prefixes
* the number of unique communities associated with IPv6 prefixes
