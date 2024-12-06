-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Máy chủ: 127.0.0.1
-- Thời gian đã tạo: Th12 03, 2024 lúc 09:38 AM
-- Phiên bản máy phục vụ: 10.4.28-MariaDB
-- Phiên bản PHP: 8.0.28

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Cơ sở dữ liệu: `data_warehouse`
--

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `control_data_config`
--

CREATE TABLE `control_data_config` (
  `id` int(11) NOT NULL,
  `name` varchar(1000) DEFAULT NULL,
  `decription` text DEFAULT NULL,
  `url_main_web` varchar(1000) DEFAULT NULL,
  `url_web` varchar(1000) DEFAULT NULL,
  `location` varchar(1000) DEFAULT NULL,
  `create_by` varchar(255) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

--
-- Đang đổ dữ liệu cho bảng `control_data_config`
--

INSERT INTO `control_data_config` (`id`, `name`, `decription`, `url_main_web`, `url_web`, `location`, `create_by`) VALUES
(1, 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa', 'aaaa');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `country`
--

CREATE TABLE `country` (
  `id` int(11) NOT NULL,
  `values_country` varchar(250) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

--
-- Đang đổ dữ liệu cho bảng `country`
--

INSERT INTO `country` (`id`, `values_country`) VALUES
(366, 'Viet Nam');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `location`
--

CREATE TABLE `location` (
  `id` int(11) NOT NULL,
  `values_location` varchar(250) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

--
-- Đang đổ dữ liệu cho bảng `location`
--

INSERT INTO `location` (`id`, `values_location`) VALUES
(366, 'TP Hồ Chí Minh');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `log`
--

CREATE TABLE `log` (
  `id` int(11) NOT NULL,
  `status` varchar(255) DEFAULT NULL,
  `note` varchar(255) DEFAULT NULL,
  `log_date` datetime DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

--
-- Đang đổ dữ liệu cho bảng `log`
--

INSERT INTO `log` (`id`, `status`, `note`, `log_date`) VALUES
(53, 'aaaa', 'aaaa', NULL);

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `staging`
--

CREATE TABLE `staging` (
  `id` int(11) NOT NULL,
  `nation` varchar(250) NOT NULL,
  `temperature` double NOT NULL,
  `weather_status` varchar(250) NOT NULL,
  `location` varchar(250) NOT NULL,
  `currentTime` datetime NOT NULL,
  `latestReport` datetime NOT NULL,
  `visibility` double NOT NULL,
  `pressure` double NOT NULL,
  `humidity` int(11) NOT NULL,
  `dew_point` double NOT NULL,
  `dead_time` datetime NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

--
-- Đang đổ dữ liệu cho bảng `staging`
--

INSERT INTO `staging` (`id`, `nation`, `temperature`, `weather_status`, `location`, `currentTime`, `latestReport`, `visibility`, `pressure`, `humidity`, `dew_point`, `dead_time`) VALUES
(1, 'Vietnam', 30.5, 'Sunny', 'Hanoi', '2024-11-30 10:00:00', '0000-00-00 00:00:00', 10, 1015, 65, 23.4, '2024-11-30 12:00:00'),
(2, 'Vietnam', 28.3, 'Cloudy', 'Ho Chi Minh City', '2024-11-30 11:00:00', '0000-00-00 00:00:00', 8, 1012, 70, 21.2, '2024-11-30 13:00:00'),
(3, 'China', 25, 'Rainy', 'Beijing', '2024-11-30 09:00:00', '0000-00-00 00:00:00', 5, 1013, 85, 20.1, '2024-11-30 10:30:00'),
(4, 'Japan', 18.6, 'Windy', 'Tokyo', '2024-11-30 08:00:00', '0000-00-00 00:00:00', 15, 1018, 55, 17.8, '2024-11-30 09:30:00'),
(5, 'South Korea', 22.2, 'Sunny', 'Seoul', '2024-11-30 07:00:00', '0000-00-00 00:00:00', 12, 1016, 60, 19.5, '2024-11-30 08:30:00'),
(6, 'USA', 15, 'Cloudy', 'New York', '2024-11-30 10:30:00', '0000-00-00 00:00:00', 20, 1020, 50, 13.4, '2024-11-30 12:00:00'),
(7, 'Germany', 12.4, 'Snowy', 'Berlin', '2024-11-30 06:00:00', '0000-00-00 00:00:00', 3, 1009, 80, 10.2, '2024-11-30 07:30:00'),
(8, 'India', 35.2, 'Sunny', 'New Delhi', '2024-11-30 11:30:00', '0000-00-00 00:00:00', 25, 1014, 45, 28.1, '2024-11-30 13:00:00'),
(9, 'Australia', 22, 'Clear', 'Sydney', '2024-11-30 09:30:00', '0000-00-00 00:00:00', 30, 1021, 40, 20.7, '2024-11-30 11:00:00'),
(10, 'Brazil', 28, 'Rainy', 'Rio de Janeiro', '2024-11-30 08:30:00', '0000-00-00 00:00:00', 6, 1010, 90, 26.3, '2024-11-30 10:00:00'),
(1851, 'aaaaaa', 11, 'aaaaaa', 'aaaaaa', '2024-11-30 08:32:13', '2024-11-30 08:32:13', 11, 11, 11, 11, '2024-11-30 08:32:13');

-- --------------------------------------------------------

--
-- Cấu trúc bảng cho bảng `weather_description`
--

CREATE TABLE `weather_description` (
  `id` int(11) NOT NULL,
  `values_weather` varchar(250) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

--
-- Đang đổ dữ liệu cho bảng `weather_description`
--

INSERT INTO `weather_description` (`id`, `values_weather`) VALUES
(71, 'aaaaaa');

--
-- Chỉ mục cho các bảng đã đổ
--

--
-- Chỉ mục cho bảng `control_data_config`
--
ALTER TABLE `control_data_config`
  ADD PRIMARY KEY (`id`) USING BTREE;

--
-- Chỉ mục cho bảng `country`
--
ALTER TABLE `country`
  ADD PRIMARY KEY (`id`) USING BTREE;

--
-- Chỉ mục cho bảng `location`
--
ALTER TABLE `location`
  ADD PRIMARY KEY (`id`) USING BTREE;

--
-- Chỉ mục cho bảng `log`
--
ALTER TABLE `log`
  ADD PRIMARY KEY (`id`) USING BTREE;

--
-- Chỉ mục cho bảng `staging`
--
ALTER TABLE `staging`
  ADD PRIMARY KEY (`id`) USING BTREE;

--
-- Chỉ mục cho bảng `weather_description`
--
ALTER TABLE `weather_description`
  ADD PRIMARY KEY (`id`) USING BTREE;

--
-- AUTO_INCREMENT cho các bảng đã đổ
--

--
-- AUTO_INCREMENT cho bảng `control_data_config`
--
ALTER TABLE `control_data_config`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=2;

--
-- AUTO_INCREMENT cho bảng `country`
--
ALTER TABLE `country`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=367;

--
-- AUTO_INCREMENT cho bảng `location`
--
ALTER TABLE `location`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=367;

--
-- AUTO_INCREMENT cho bảng `log`
--
ALTER TABLE `log`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=54;

--
-- AUTO_INCREMENT cho bảng `staging`
--
ALTER TABLE `staging`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=1852;

--
-- AUTO_INCREMENT cho bảng `weather_description`
--
ALTER TABLE `weather_description`
  MODIFY `id` int(11) NOT NULL AUTO_INCREMENT, AUTO_INCREMENT=72;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
