# Blueprint-execution

Service for automatic execution of Notebooks. The service gets an execution graph 
from the `blueprint` service, that is, a directionaly, acyclic graph (DAG) where 
the single leaf node is the notebook creating the desired output dataset(s) and
the nodes are the notebooks producing the datasets used as input for the next 
level in the graph.

The DAG is used to create an execution plan in form of a sequence of Kubernetes jobs,
which in turn is executed in the Kubernetes cluster. 

## Usage 

Run `BlueprintExecutionApplication` to start the service locally.
