# IoT Device Data 2024may
source code for 2024 May

## Setup

Database setup:
```
mysql> DESCRIBE T_DATA;
+-------------------------+--------------+------+-----+---------+----------------+
| Field                   | Type         | Null | Key | Default | Extra          |
+-------------------------+--------------+------+-----+---------+----------------+
| DATA_ID                 | int          | NO   | PRI | NULL    | auto_increment |
| TAKEN_AT                | datetime     | YES  |     | NULL    |                |
| SENSOR_ID               | varchar(50)  | YES  |     | NULL    |                |
| SENSOR_READING          | double       | YES  |     | NULL    |                |
| ORIGINAL_SENSOR_READING | varchar(255) | YES  |     | NULL    |                |
| CREATED_AT              | datetime     | YES  |     | NULL    |                |
| PAYLOAD                 | varchar(255) | YES  |     | NULL    |                |
| VALIDITY                | varchar(1)   | YES  |     | NULL    |                |
+-------------------------+--------------+------+-----+---------+----------------+
8 rows in set (0.00 sec)

mysql> DESCRIBE T_SENSOR;
+-------------+--------------+------+-----+---------+-------+
| Field       | Type         | Null | Key | Default | Extra |
+-------------+--------------+------+-----+---------+-------+
| SENSOR_ID   | varchar(50)  | NO   | PRI | NULL    |       |
| SENSOR_NAME | varchar(255) | YES  |     | NULL    |       |
| CREATED_AT  | datetime     | YES  |     | NULL    |       |
| PAYLOAD     | varchar(255) | YES  |     | NULL    |       |
| LOCATION    | varchar(255) | YES  |     | NULL    |       |
+-------------+--------------+------+-----+---------+-------+
5 rows in set (0.00 sec)
```

For the table of sensor, we might add
```
alter table T_DATA ADD CONSTRAINT UK_TIME_EMIT UNIQUE (SENSOR_ID, TAKEN_AT);
```

but this risks making INSERT statement (which is by design multiple rows) fails and useful records would be lost.

The database configuration for the program is set on (1) config.yaml , where the meaning of each key is intuitive; (2) line 58 variable $dsn (data source name) of the program, currently using mysql, please refer to https://metacpan.org/pod/DBD::ODBC#SYNOPSIS or https://metacpan.org/pod/DBD::Pg#SYNOPSIS, etc., for other database systems.



Program setup:

To run the program, Perl version 5.26 or above is recommended.

To setup the program, firstly run the following in environment:

```
yum -y install cpanm gcc perl perl-App-cpanminus # ensure gcc, cpanminus and perl is installed
# replace yum with appropriate command like apt-get for different Linux versions
cpanm Text::CSV_XS Config::Any DBI DateTime DateTime::Format::ISO8601 Crypt::PRNG
```

Finally run
```
perl submission.pl [directory location] [optional:no-of-threads]
```

For example,
```
perl submission.pl "./case/tinyfile"
```

Or if you prefer,
```
chmod +x submission.pl
./submission.pl "./case/tinyfile"
```

---
# Performance

As stated, there is an optional parameter number of threads can be set. The default is 6. Inside the program we can also set the number of rows for each INSERT statement; the default is 100,000. These numbers being too large may cause program error, like aborting due to "double free or corruption", or some database insertion exeution fails due to "DBD::mysql::st execute failed: Lost connection to MySQL server during query".

Some trial data:

```
20 files, 5.4MB each
real	1m49.025s
user	7m58.745s
sys	0m3.215s

100 files, 1.1MB each
real	1m43.169s
user	8m35.975s
sys	0m3.463s

100 files, 5.4MB each (./case/tinyfile in this repository)
real	11m6.531s
user	54m58.987s
sys	0m19.870s
```

Consider approximately-linearity of the program, this suggests that the program needs one and a half day to process 100 1GB files into a local MySQL database on my average laptop computer.

## Note - my laptop spec
```
Architecture:                       x86_64
CPU op-mode(s):                     32-bit, 64-bit
Byte Order:                         Little Endian
Address sizes:                      43 bits physical, 48 bits virtual
CPU(s):                             8
On-line CPU(s) list:                0-7
Thread(s) per core:                 2
Core(s) per socket:                 4
Socket(s):                          1
NUMA node(s):                       1
Vendor ID:                          AuthenticAMD
CPU family:                         23
Model:                              24
Model name:                         AMD Ryzen 5 3500U with Radeon Vega Mobile Gfx
Stepping:                           1
Frequency boost:                    enabled
CPU MHz:                            1197.385
CPU max MHz:                        2100.0000
CPU min MHz:                        1400.0000
```
