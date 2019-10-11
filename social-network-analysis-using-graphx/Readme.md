# Analyzing Social Networks using GraphX

In this project, we used Spark GraphX to analyze social network data. The 'ego-Twitter' dataset was chosen from the SNAP repository: https://snap.stanford.edu/data/index.html#socnets

### Details about the dataset:

The edges in the graph are directed. There are 81,306 nodes and 1,768,149 edges in the dataset which represent social circles from Twitter. 

## Analysis details

The following high-level analysis was done on the twitter data:

a. To find the top 5 nodes with the highest outdegree and find the count of the number of outgoing
edges in each

*Analysis: 3359851 Node has highest out-degree. This implies that this node follows maximum number of other twitter profiles.*

b. To find the top 5 nodes with the highest indegree and find the count of the number of incoming edges
in each

*Analysis: 40981798 Node has the highest in-degree. This implies that this is the sought after profile which might have a heavy following*

c. To calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank
values. 

*Analysis: Node 115485051 has the highest pagerank. This implies that this profile is of very high importance and it plays a vital role in influencing with its tweets*

d. To run the connected components algorithm on it and find the top 5 components with the largest
number of nodes.

*Analysis: Since there is only one component, we can say that the entire graph is connected. We don’t have any separate components in the graph. This implies that everyone on twitter is connected to each other via one or another nodes.*

e. To run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the
largest triangle count. In case of ties, randomly select the top 5 vertices.

*Analysis: Node 40981798 has a highest triangle count of 774424 and this implies that many people have this user in common in their mutually 3node connected networks*
