# Graph metrics

Graph metrics quantify the characteristics of a graph. A simple example of a graph metric is cluster size, which is the number of nodes in a cluster.

For data linking with Splink, it is useful to sort graph metrics into three categories:
* cluster metrics, 
* node metrics, and 
* edge metrics

Each of these are defined below together with examples and how they can be applied to linked data to evaluate cluster quality. The examples given are of all metrics currently available in Splink.

!!! note

    It is important to bear in mind that whilst graph metrics can be very useful for assessing linkage quality, they are rarely definitive, especially when taken in isolation. A more comprehensive picture can be built by considering various metrics in conjunction with one another.

    It is important to consider metrics within the context of their distribution and the underlying dataset. For example: a cluster density (see below) of 0.4 might seem low but could actually be above average for the dataset in question; a cluster of size 80 might be suspiciously large for one dataset but not for another.

## :fontawesome-solid-circle-nodes: Cluster metrics

Cluster metrics refer to the characteristics of a cluster as a whole, rather than the individual nodes and edges it contains.

### Example: cluster size

Cluster size is defined as the number of nodes within a cluster.

When thinking about cluster size, one important thing to consider is the size of the biggest clusters produced and to ask - does this seem reasonable for the dataset being linked? For example, does it make sense that one person is appearing hundreds of times in the data resulting in a cluster of size 100+ nodes? If the answer is no, then false positives links are probably being formed. This could be due to having blocking rules which are too loose or the clustering threshold which is too low.

If you don't have an intuition of what seems reasonable, then it is worth inspecting a sample of the largest clusters in Splink Cluster Studio to validate or invalidate links[link to guidance]. From there you can develop an understanding of what maximum cluster size to expect.

There also might be an expected cutoff on minimum cluster size. For example, when linking two datasets in which you know people appear least once in each, the minimum expected size of cluster will be 2. Clusters smaller than the minimum size indicate links have been missed. This could be due to blocking rules not letting through comparisons on true matches.

Lisewise, the modal cluster size...bimodal distributions.


### Example: cluster density

The density of a cluster is given by the number of edges it contains divided by the maximum possible number of edges. Density ranges from 0 to 1. A density of 1 means that all nodes are connected to all other nodes in a cluster.

[picture: edges vs max possible edges]

When evaluating clusters, a high density (close to 1) is generally considered good as it means there are many edges in support of the records in a cluster being linked.

A low density could indicate links being missed. This could happen for example if blocking rules are too tight or the clustering threshold is too high.
A sample of low density clusters can be inspected in Splink Cluster Studio by choosing [inser option here]. Ask yourself the question: why aren't more links being formed between records?

It is important to consider cluster density within the context of cluster size. Bigger clusters can have a greater range of densities than smaller ones
This is why `sampling_method = "lowest_density_clusters_by_size"` performs a stratified sample...]

[Explain the relationship between density and cluster size and it's consequences. Stratified sampling in cluster studio.]

### Example: cluster centralisation

[TBC]

## ⚫️ Node metrics

Node metrics quantify the properties of the nodes within clusters.

### Example: node degree

A node degree is the number of edges (links) connected to a node.

within clusters or across clusters?

High node degree also places more pressure on a node to be a legitimate member of a cluster as its removal could dramatically change the cluster’s structure. Therefore... 

Low node degree

[TBC]

## 🔗 Edge metrics

Edge metrics quantify the properties of edges within a cluster. 

### Example: 'is bridge'

An edge is classified as a bridge if its removal splits a cluster into two smaller clusters.

[insert picture]

Bridges can be signalers of false positives in linked data, especially when joining two highly connected clusters. Examining bridges can shed light on potential errors in the linking process leading to false positive links.