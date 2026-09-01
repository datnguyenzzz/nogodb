The queye engine provides support for the SQL query language, and is the main `nogodb` database interface. The SQL engine itself consists of below components that form a pipeline:

```
+-------------------------- SQL session ------------------------------------+    +--- NogoDB (LSM) ----+     
| Client -> Session -> Lexer -> Parser -> Planner -> Optimizer -> Executor -|----|-> (gRPC) Storage    |  
+---------------------------------------------------------------------------+ |  +---------------------+  
                                                                              |
                                                                              |  +-- NogoDB (Non LSM)--+  
                                                                              └--|-> (syscal) Storage  |
                                                                                 +---------------------+
``` 

NogoDB Query Engine supports ANSI SQL dialect: https://ronsavage.github.io/SQL/sql-2003-2.bnf.html

Example of pipelines 
```
┌[3]------------┐                         Dependencies: [1] < [2] < [3]
│ QUERY         │
└---------------┘   
        |          
┌[3]------------┐               
│ PROJECTION    │            
└---------------┘  
        |                
┌[3]-------------┐                   
━┿━HASH_GROUP_BY━┿━
└[2]-------------┘
        |
┌-------------┐ 
│ PROJECTION  │ 
└[2]----------┘
        |
   ┌---------╂---------┐
   │  PROBE  ┃  BUILD  ├─────┐ 
   └[2]------╂------[1]┘     │ 
        |                    |
┌--------------┐      ┌-------------┐ 
│ TABLE_SCAN   │      |    FILTER   │
└[2]-----------┘      └----------[1]┘ 
 lineitem                    |
                      ┌-------------┐
                      │ TABLE_SCAN  │
                      └----------[1]┘
                           orders
 ```