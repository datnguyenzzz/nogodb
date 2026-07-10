The queye engine provides support for the SQL query language, and is the main `nogodb` database interface. The SQL engine itself consists of below components that form a pipeline:

```
+-------------------------- SQL session ------------------------------------+  +--- NogoDB ---+     
| Client -> Session -> Lexer -> Parser -> Planner -> Optimizer -> Executor -|--|->  Storage   |  
+---------------------------------------------------------------------------+  +--------------+  
```