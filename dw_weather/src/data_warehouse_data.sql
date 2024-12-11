/*
 Navicat Premium Data Transfer

 Source Server         : mysql
 Source Server Type    : MySQL
 Source Server Version : 100432
 Source Host           : localhost:3306
 Source Schema         : data_warehouse

 Target Server Type    : MySQL
 Target Server Version : 100432
 File Encoding         : 65001

 Date: 11/12/2024 11:17:15
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for control_data_config
-- ----------------------------
DROP TABLE IF EXISTS `control_data_config`;
CREATE TABLE `control_data_config`  (
  `id` int NOT NULL,
  `name` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `decription` text CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL,
  `url_main_web` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `url_web` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `location` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `create_by` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `email_report` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `pass_email` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `email_sent` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for country
-- ----------------------------
DROP TABLE IF EXISTS `country`;
CREATE TABLE `country`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `values_country` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1483 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for date_dim
-- ----------------------------
DROP TABLE IF EXISTS `date_dim`;
CREATE TABLE `date_dim`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `date_values` datetime NULL DEFAULT NULL,
  `day` int NULL DEFAULT NULL,
  `month` int NULL DEFAULT NULL,
  `year` int NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 15 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for fact_table
-- ----------------------------
DROP TABLE IF EXISTS `fact_table`;
CREATE TABLE `fact_table`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `country_id` int NOT NULL DEFAULT 0,
  `location_id` int NULL DEFAULT NULL,
  `weather_id` int NULL DEFAULT NULL,
  `date_id` int NULL DEFAULT NULL,
  `report_id` int NULL DEFAULT NULL,
  `temperature` double NULL DEFAULT NULL,
  `visibility` double NULL DEFAULT NULL,
  `pressure` double NULL DEFAULT NULL,
  `humidity` int NULL DEFAULT NULL,
  `dew_point` double NULL DEFAULT NULL,
  `dead_time` datetime NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `FK_fact_table_country`(`country_id` ASC) USING BTREE,
  INDEX `FK_fact_table_location`(`location_id` ASC) USING BTREE,
  INDEX `FK_fact_table_weather_description`(`weather_id` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 5723 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for latesreport
-- ----------------------------
DROP TABLE IF EXISTS `latesreport`;
CREATE TABLE `latesreport`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `time` varchar(10) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for location
-- ----------------------------
DROP TABLE IF EXISTS `location`;
CREATE TABLE `location`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `values_location` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1481 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for log
-- ----------------------------
DROP TABLE IF EXISTS `log`;
CREATE TABLE `log`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `status` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `note` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  `log_date` datetime NULL DEFAULT NULL,
  `process` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 3577 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for staging
-- ----------------------------
DROP TABLE IF EXISTS `staging`;
CREATE TABLE `staging`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `nation` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `temperature` double NOT NULL,
  `weather_status` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `location` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `currentTime` datetime NOT NULL,
  `latestReport` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `visibility` double NOT NULL,
  `pressure` double NOT NULL,
  `humidity` int NOT NULL,
  `dew_point` double NOT NULL,
  `dead_time` datetime NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 4845 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Table structure for weather_description
-- ----------------------------
DROP TABLE IF EXISTS `weather_description`;
CREATE TABLE `weather_description`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `values_weather` varchar(250) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 274 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Procedure structure for CheckCountryCount
-- ----------------------------
DROP PROCEDURE IF EXISTS `CheckCountryCount`;
delimiter ;;
CREATE PROCEDURE `CheckCountryCount`(IN p_values_country VARCHAR(255), OUT p_count INT)
BEGIN
    SELECT COUNT(*) INTO p_count
    FROM country
    WHERE values_country = p_values_country;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for CheckLatesReportCount
-- ----------------------------
DROP PROCEDURE IF EXISTS `CheckLatesReportCount`;
delimiter ;;
CREATE PROCEDURE `CheckLatesReportCount`(IN p_time DATETIME, OUT p_count INT)
BEGIN
    SELECT COUNT(*) INTO p_count
    FROM latesReport
    WHERE time = p_time;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for CheckLocationCount
-- ----------------------------
DROP PROCEDURE IF EXISTS `CheckLocationCount`;
delimiter ;;
CREATE PROCEDURE `CheckLocationCount`(IN p_values_location VARCHAR(255), OUT p_count INT)
BEGIN
    SELECT COUNT(*) INTO p_count
    FROM location
    WHERE values_location = p_values_location;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for CheckWeatherDescriptionCount
-- ----------------------------
DROP PROCEDURE IF EXISTS `CheckWeatherDescriptionCount`;
delimiter ;;
CREATE PROCEDURE `CheckWeatherDescriptionCount`(IN p_values_weather VARCHAR(255), OUT p_count INT)
BEGIN
    SELECT COUNT(*) INTO p_count
    FROM weather_description
    WHERE values_weather = p_values_weather;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetControlDataConfig
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetControlDataConfig`;
delimiter ;;
CREATE PROCEDURE `GetControlDataConfig`()
BEGIN
    SELECT url_main_web, url_web, location, email_report, pass_email, email_sent 
    FROM control_data_config 
    LIMIT 1;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetControlDataConfigLocation
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetControlDataConfigLocation`;
delimiter ;;
CREATE PROCEDURE `GetControlDataConfigLocation`()
BEGIN
    SELECT location FROM control_data_config LIMIT 1;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetCountryMap
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetCountryMap`;
delimiter ;;
CREATE PROCEDURE `GetCountryMap`()
BEGIN
    SELECT id, values_country FROM country;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetDateDim
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetDateDim`;
delimiter ;;
CREATE PROCEDURE `GetDateDim`()
BEGIN
    SELECT id, date_values FROM date_dim;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetEmailReportConfig
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetEmailReportConfig`;
delimiter ;;
CREATE PROCEDURE `GetEmailReportConfig`()
BEGIN
    SELECT email_report, pass_email, email_sent FROM control_data_config LIMIT 1;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetLatestReport
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetLatestReport`;
delimiter ;;
CREATE PROCEDURE `GetLatestReport`()
BEGIN
    SELECT id, time FROM latesreport;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetLocationMap
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetLocationMap`;
delimiter ;;
CREATE PROCEDURE `GetLocationMap`()
BEGIN
    SELECT id, values_location FROM location;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetStagingData
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetStagingData`;
delimiter ;;
CREATE PROCEDURE `GetStagingData`()
BEGIN
    SELECT nation, temperature, weather_status, location, currentTime, latestReport, 
           visibility, pressure, humidity, dew_point, dead_time
    FROM staging;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for GetWeatherDescription
-- ----------------------------
DROP PROCEDURE IF EXISTS `GetWeatherDescription`;
delimiter ;;
CREATE PROCEDURE `GetWeatherDescription`()
BEGIN
    SELECT id, values_weather FROM weather_description;
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for InsertCountry
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertCountry`;
delimiter ;;
CREATE PROCEDURE `InsertCountry`(IN p_values_country VARCHAR(255))
BEGIN
    INSERT INTO country (values_country)
    VALUES (p_values_country);
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for InsertDateDim
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertDateDim`;
delimiter ;;
CREATE PROCEDURE `InsertDateDim`(IN p_date_values DATE, IN p_day INT, IN p_month INT, IN p_year INT)
BEGIN
    INSERT INTO date_dim (date_values, day, month, year) 
    VALUES (p_date_values, p_day, p_month, p_year);
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for InsertDimLocation
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertDimLocation`;
delimiter ;;
CREATE PROCEDURE `InsertDimLocation`(IN p_values_location VARCHAR(255))
BEGIN
    INSERT INTO location (values_location) 
    VALUES (p_values_location);
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for InsertFactTable
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertFactTable`;
delimiter ;;
CREATE PROCEDURE `InsertFactTable`(IN p_country_id INT, IN p_location_id INT, IN p_weather_id INT, 
    IN p_date_id INT, IN p_report_id INT, IN p_temperature FLOAT,
    IN p_visibility FLOAT, IN p_pressure FLOAT, IN p_humidity FLOAT, 
    IN p_dew_point FLOAT, IN p_dead_time DATETIME)
BEGIN
    INSERT INTO fact_table (
        country_id, location_id, weather_id, date_id, report_id, temperature,
        visibility, pressure, humidity, dew_point, dead_time
    ) 
    VALUES (
        p_country_id, p_location_id, p_weather_id, p_date_id, p_report_id, p_temperature,
        p_visibility, p_pressure, p_humidity, p_dew_point, p_dead_time
    );
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for InsertLatesReport
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertLatesReport`;
delimiter ;;
CREATE PROCEDURE `InsertLatesReport`(IN p_time DATETIME)
BEGIN
    INSERT INTO latesReport (time)
    VALUES (p_time);
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for InsertLog
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertLog`;
delimiter ;;
CREATE PROCEDURE `InsertLog`(IN p_status VARCHAR(50),
    IN p_note TEXT,
    IN p_process VARCHAR(100),
    IN p_log_date DATETIME)
BEGIN
    INSERT INTO log (status, note, process, log_date)
    VALUES (p_status, p_note, p_process, IFNULL(p_log_date, NOW()));
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for InsertStagingData
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertStagingData`;
delimiter ;;
CREATE PROCEDURE `InsertStagingData`(IN p_nation VARCHAR(255), IN p_temperature FLOAT, IN p_weather_status VARCHAR(255), 
    IN p_location VARCHAR(255), IN p_currentTime DATETIME, IN p_latestReport DATETIME, 
    IN p_visibility FLOAT, IN p_pressure FLOAT, IN p_humidity FLOAT, 
    IN p_dew_point FLOAT, IN p_dead_time DATETIME)
BEGIN
    INSERT INTO staging 
    (nation, temperature, weather_status, location, currentTime, latestReport, 
     visibility, pressure, humidity, dew_point, dead_time) 
    VALUES 
    (p_nation, p_temperature, p_weather_status, p_location, p_currentTime, p_latestReport, 
     p_visibility, p_pressure, p_humidity, p_dew_point, p_dead_time);
END
;;
delimiter ;

-- ----------------------------
-- Procedure structure for InsertWeatherDescription
-- ----------------------------
DROP PROCEDURE IF EXISTS `InsertWeatherDescription`;
delimiter ;;
CREATE PROCEDURE `InsertWeatherDescription`(IN p_values_weather VARCHAR(255))
BEGIN
    INSERT INTO weather_description (values_weather)
    VALUES (p_values_weather);
END
;;
delimiter ;

SET FOREIGN_KEY_CHECKS = 1;
