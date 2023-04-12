
CREATE TABLE IF NOT EXISTS `test`.`sensor`(
  `ts`          TIMESTAMP,
  `pm25_aqi`    INT,
  `pm10_aqi`    INT,
  `no2_aqi`     INT,
  `temperature` INT,
  `pressure`    INT,
  `humidity`    INT,
  `wind`        INT,
  `weather`     INT)
TAGS (
  `region` NCHAR(16)
);

CREATE TABLE test.beijing USING test.sensor TAGS("beijing");
CREATE TABLE test.shanghai USING test.sensor TAGS("shanghai");

INSERT INTO test.beijing VALUES(now, 11, 70, 21, -6, 1025, 48, 3, 1);
INSERT INTO test.beijing VALUES(now, 14, 71, 31, -1, 925, 46, 4, 2);
INSERT INTO test.beijing VALUES(now, 15, 71, 31, -1, 925, 46, 4, 2);
INSERT INTO test.shanghai VALUES(now, 11, 72, 31, -1, 925, 46, 4, 2);
INSERT INTO test.shanghai VALUES(now, 21, 74, 32, -2, 998, 48, 5, 3);
INSERT INTO test.shanghai VALUES(now, 31, 76, 35, -4, 977, 57, 2, 4);
INSERT INTO test.shanghai VALUES(now, 11, 77, 37,  0, 928, 39, 4, 3);
INSERT INTO test.shanghai VALUES(now, 43, 79, 31, -2, 911, 49, 3, 3);
INSERT INTO test.shanghai VALUES(now, 61, 80, 30, -1, 900, 85, 5, 2);

CREATE TOPIC topic_sensor_data AS SELECT * FROM `test`.`sensor`;

DROP TOPIC topic_sensor_data;
DROP TABLE `test`.`sensor`;
