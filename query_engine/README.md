The queye engine provides support for the SQL query language, and is the main `nogodb` database interface. The SQL engine itself consists of below components that form a pipeline:

```
+-------------------------- SQL session ------------------------------------+  +--- NogoDB ---+     
| Client -> Session -> Lexer -> Parser -> Planner -> Optimizer -> Executor -|--|->  Storage   |  
+---------------------------------------------------------------------------+  +--------------+  
```

NogoDB Query Engine supports ANSI SQL dialect: https://ronsavage.github.io/SQL/sql-2003-2.bnf.html