#!/bin/bash 

psql -c "CREATE DATABASE ftxtest"
psql -d ftxtest -c "CREATE TABLE highres_mixed ( \
	startTime timestamp UNIQUE NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32) NOT NULL, \
	close DECIMAL ( 32 )  NOT NULL, \
	high DECIMAL (32) NOT NULL, \
    low DECIMAL (32) NOT NULL, \
    volume DECIMAL (32) NOT NULL \
    );"

psql -d ftxtest -c "CREATE TABLE highres_historical ( \
	startTime timestamp UNIQUE NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32) NOT NULL, \
	close DECIMAL ( 32 )  NOT NULL, \
	high DECIMAL (32) NOT NULL, \
    low DECIMAL (32) NOT NULL, \
    volume DECIMAL (32) NOT NULL \
    );"

psql -d ftxtest -c "CREATE TABLE highres_stream ( \
	startTime timestamp UNIQUE NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32) NOT NULL, \
	close DECIMAL ( 32 )  NOT NULL, \
	high DECIMAL (32) NOT NULL, \
    low DECIMAL (32) NOT NULL, \
    volume DECIMAL (32) NOT NULL \
    );"