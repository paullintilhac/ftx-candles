#!/bin/bash 

psql -c "CREATE DATABASE ftxtest"

psql -d ftxtest -c "CREATE TABLE mixed ( \
	startTime timestamp NOT NULL,\
	time BIGINT NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 ) NOT NULL , \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL, \
	pair VARCHAR(16) NOT NULL, \
	exchange VARCHAR(16) NOT NULL, \
	res_secs BIGINT NOT NULL, \
	is_streamed BIT NOT NULL, \
	open_interest DECIMAL (32,8), \
	CONSTRAINT mixed_id UNIQUE (time, pair,exchange,res_secs)   \
    );"

# psql -d ftxtest -c "CREATE UNIQUE index \
# 	id_mixed \
# 	ON mixed(time, pair, exchange, res_secs);"

psql -d ftxtest -c "CREATE TABLE hist ( \
	startTime timestamp NOT NULL,\
	time BIGINT NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 )  NOT NULL, \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL, \
	pair VARCHAR(16) NOT NULL, \
	exchange VARCHAR(16) NOT NULL, \
	res_secs BIGINT NOT NULL, \
	is_streamed BIT NOT NULL, \
	CONSTRAINT hist_id UNIQUE (time, pair,exchange,res_secs)   \
    );"

# psql -d ftxtest -c "CREATE UNIQUE index \
# 	id_hist \
# 	ON hist(time, pair, exchange, res_secs);"

psql -d ftxtest -c "CREATE TABLE diff ( \
	startTime timestamp NOT NULL,\
	time BIGINT NOT NULL, \
	open DECIMAL (32,8) NOT NULL, \
	close DECIMAL ( 32,8 )  NOT NULL, \
	high DECIMAL (32,8) NOT NULL, \
    low DECIMAL (32,8) NOT NULL, \
    volume DECIMAL (32,8) NOT NULL, \
	pair VARCHAR(16) NOT NULL, \
	exchange VARCHAR(16) NOT NULL, \
	res_secs BIGINT NOT NULL, \
	CONSTRAINT diff_id UNIQUE (time, pair,exchange,res_secs)   \
    );"

# psql -d ftxtest -c "CREATE UNIQUE index \
# 	id_diff \
# 	ON diff(time, pair, exchange, res_secs);"
