## Project #2 – Redis / Key-Value Stores
*Due Date: May 5th, 2019*

### Installation

### Functions
*Details on each function can be found in the Python files*
- **Create_KLStore** *(name, data-source, query-string, position1, position2, direction)*
- **Filter_KLStore** *(name1, expression)*
- **Apply_KLStore** *(name1, func)*
- **Aggr_KLStore** *(name1, aggr)*
- **LookUp_KLStore** *(name1, name2)*
- **ProjSel_KLStore** *(output_name, pname1, pname2, …, pnamek, expression)*

### Description
A key-value store is a system that manages a collection of *(key,value)* pairs, where key is unique in this universe. Redis – and other systems – allow the value to be a single value (e.g. string, number), a set of values, a list of values, a hash, etc.

Assume a collection of (key, list) pairs, i.e. the value is a list of values, namely strings. Assume that key is a string as well. Let’s call such a collection a *Key-List Store (KL Store)*. This is a special case of a multi-map data structure, where several values are mapped to a key.

**Example:**

| Key 	| List                   	|
|-----	|------------------------	|
| 12  	| [t12, t67]             	|
| 34  	| [t87, t12, t98]        	|
| ... 	| ...                    	|
| 76  	| [t121, t72, t99, t179] 	|

Assume two domains of values **D1** and **D2**  
e.g. D1 = {all possible customer ids}, D2 = {all possible transaction ids}

Assume that there is a process P that generates a collection of value1, value2 pairs:

**S = { (u,v): u ∈ D1 , v ∈ D2}**

Examples of such processes:
- SELECT custID, transID FROM SALES
- Reading a CSV file and getting for each line forming a pair using columns i and j.
- Running any program that produces a stream of pairs of values

Given a collection S described as above, one can define two KLStores, **KL1(S)** and **KL2(S)** as follows:

- KL1(S) = {(x, Lx), ∀x ∈ U = {u: (u,v) ∈ S}, Lx = the list of values v, such that (x,v) ∈ S}

- KL2(S) = {(x, Lx), ∀x ∈ V = {v: (u,v) ∈ S}, Lx = the list of values u, such that (u,x) ∈ S}

We want to implement in Python the following functions/methods that get one or more KL stores and “return” (or update) a KL store. All these KL stores should exist in Redis.
