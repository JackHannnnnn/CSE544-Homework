# CSE544 Homework
## Homework 1: Data Analytics Pipeline
### Objectives
To get familiar with the main components of the data analytic pipeline: schema design, data acquisition, data transformation, querying and visualizing.
### Assignment tools
Postgres, excel (or some other tool for visualization)
### What to turn in
These files: pubER.pdf, solution.sql, graph.pdf. Your solution.sql file should be executable using the command psql -f solution.sql
### Process
1. Schema Design
2. Data Acquisition
3. Data Transformation
4. Queries
5. Using a DBMS from Python or Java and Data Visualization

## Homework 2: SimpleDB
### Objectives
To get experience implementing the internals of a DBMS.
### Assignment tools
Apache ant and Eclipse

### SimpleDB Architecture

- Classes that represent fields, tuples, and tuple schemas
- Classes that apply predicates and conditions to tuples
- One or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples of those relations
- A collection of operator classes (e.g., select, join, insert, delete, etc.) that process tuples
- A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions
- A catalog that stores information about available tables and their schemas

### Implemented Classes

- simpledb/TupleDesc.java
- simpledb/Tuple.java
- simpledb/Catalog.java
- simpledb/BufferPool.java
- simpledb/HeapPageId.java
- simpledb/RecordId.java
- simpledb/HeapPage.java
- simpledb/HeapFile.java
- simpledb/SeqScan.java
- simpledb/Predicate.java
- simpledb/JoinPredicate.java
- simpledb/Filter.java
- simpledb/Join.java
- simpledb/IntegerAggregator.java
- simpledb/StringAggregator.java
- simpledb/Aggregate.java

