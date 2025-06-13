
Purpose: This script creates a new database named 'datawarehouse. Additional, 3 schemas were created: 'bronze', 'silver', 'gold'. 

use master; 

create database datawarehouse 

use datawarehouse 

create schema bronze
create schema silver;
go
create schema gold; 
go
