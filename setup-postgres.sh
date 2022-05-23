#!/bin/bash 

psql -c "CREATE DATABASE ftxtest"
psql -d ftxtest -c "CREATE TABLE mixed_15_sec ( \
	startTime timestamp NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 )  NOT NULL, \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL \
    );"

psql -d ftxtest -c "CREATE TABLE historical_15_sec ( \
	startTime timestamp NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 )  NOT NULL, \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL \
    );"

psql -d ftxtest -c "CREATE TABLE stream_15_sec ( \
	startTime timestamp NOT NULL,\
	time BIGINT UNIQUE NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 )  NOT NULL, \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL \
    );"