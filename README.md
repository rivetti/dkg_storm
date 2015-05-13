# dgk_storm
Distribution-aware Key Grouping (DKG) implementation for Apache Storm

NOTICE: The source code will be uploaded shortly

This repository contains the implementation of the Distribution-aware Key Grouping (DKG) algorithm targeting Apache Storm (through a class extending a the CustomStreamGrouping inteface).

We present the details of this algorithm in the paper Efficient Key Grouping for Near-Optimal Load Balancing in Stream Processing Systems, Proceedings of the 9th ACM International Conference on Distributed Event-Based Systems, DEBS, 2015.

Abstract:
Key grouping is a technique used by stream processing frameworks to simplify the development of parallel stateful operators. Through key grouping a stream of tuples is partitioned in several disjoint sub-streams depending on the values contained in the tuples themselves. Each operator instance target of one sub-stream is guaranteed to receive all the tuples containing a specific key value. A common solution to implement key grouping is through hash functions that, however, are known to cause load imbalances on the target operator instances when the input data stream is characterized by a skewed value distribution. In this paper we present DKG, a novel approach to key grouping that provides near-optimal load distribution for input streams with skewed value distribution. DKG starts from the simple observation that with such inputs the load balance is strongly driven by the most frequent values; it identifies such values and explicitly maps them to sub-streams together with groups of less frequent items to achieve a near-optimal load balance. We provide theoretical approximation bounds for the quality of the mapping derived by DKG and show, through both simulations and a  running prototype, its impact on stream processing applications.


