-- Delete the existing database if it exists
DROP DATABASE IF EXISTS crow;

CREATE DATABASE crow;

USE crow;

-- Resources
CREATE TABLE `resources` (
    `protocol` varchar(5) NOT NULL,
    `host` varchar(128) NOT NULL,
    `port` smallint(5) UNSIGNED NOT NULL,
    `path` varchar(512) NOT NULL,
    `hash` varchar(32) NOT NULL,
    `type` varchar(255) NOT NULL,
    `last_changed` datetime NOT NULL,
    `last_retrieved` datetime NOT NULL,
    PRIMARY KEY (`protocol`, `host`, `port`, `path`)
) DEFAULT CHARSET=utf8mb4;

-- Queue
CREATE TABLE `queue` (
    `protocol` varchar(5) NOT NULL,
    `host` varchar(128) NOT NULL,
    `port` smallint(5) UNSIGNED NOT NULL,
    `path` varchar(512) NOT NULL,
    `queued` datetime NOT NULL,
    PRIMARY KEY (`protocol`, `host`, `port`, `path`)
) DEFAULT CHARSET=utf8mb4;

-- Misses (destinations that returned errors)
CREATE TABLE `misses` (
    `protocol` varchar(5) NOT NULL,
    `host` varchar(128) NOT NULL,
    `port` smallint(5) UNSIGNED NOT NULL,
    `path` varchar(512) NOT NULL,
    `reason` varchar(255) NOT NULL,
    `last_miss` datetime NOT NULL,
    PRIMARY KEY (`protocol`, `host`, `port`, `path`)
) DEFAULT CHARSET=utf8mb4;
