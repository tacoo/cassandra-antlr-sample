create table if not exists test_keyspace.my_table
(field1 text, 
 field2 int, 
 field3 set<ascii>,
 field4 map<ascii, text>,
 primary key (field1)
);