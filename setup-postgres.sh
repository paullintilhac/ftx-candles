#!/bin/bash 

psql -c "CREATE DATABASE ftxtest"
psql -d ftxtest -c "CREATE TABLE mixed ( \
	startTime timestamp NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 )  NOT NULL, \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL, \
	pair VARCHAR(16) NOT NULL, \
	exchange VARCHAR(16) NOT NULL, \
	res_secs BIGINT NOT NULL, \
	is_streamed BIT NOT NULL \
    );"

psql -d ftxtest -c "CREATE TABLE hist ( \
	startTime timestamp NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 )  NOT NULL, \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL, \
	pair VARCHAR(16) NOT NULL, \
	exchange VARCHAR(16) NOT NULL, \
	res_secs BIGINT NOT NULL,
	is_streamed BIT NOT NULL \
    );"

psql -d ftxtest -c "CREATE TABLE diff ( \
	startTime timestamp NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 )  NOT NULL, \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL, \
	pair VARCHAR(16) NOT NULL, \
	exchange VARCHAR(16) NOT NULL, \
	res_secs BIGINT NOT NULL \
    );"