/*
==========================================================
Create Database and Schemas
==========================================================

Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All data in the database will be permanently deleted. Proceed with caution
    and ensure you have proper backups before running this script.
*/

-- Create a new Database call DataWarehouse
drop database if exists "DataWarehouse" (FORCE);
create database "DataWarehouse"; 

-- Remember to navigate the location to the new database environment.
-- Create Schema in the target database. 
create schema if not exists bronze;
create schema if not exists silver; 
create schema if not exists gold; 
