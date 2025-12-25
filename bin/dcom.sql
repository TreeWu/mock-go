/*
 Navicat Premium Data Transfer

 Source Server         : 21.80.10.87
 Source Server Type    : MySQL
 Source Server Version : 80033 (8.0.33-v24-txsql-22.4.1-20230926)
 Source Host           : 21.80.10.87:15022
 Source Schema         : dcom

 Target Server Type    : MySQL
 Target Server Version : 80033 (8.0.33-v24-txsql-22.4.1-20230926)
 File Encoding         : 65001

 Date: 24/12/2025 19:41:05
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for auth_setting
-- ----------------------------
DROP TABLE IF EXISTS `auth_setting`;
CREATE TABLE `auth_setting`  (
  `set_type` varchar(36) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '配置项类型',
  `content` varchar(15000) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '配置项内容',
  PRIMARY KEY (`set_type`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of auth_setting
-- ----------------------------
INSERT INTO `auth_setting` VALUES ('menu_version', '1');

-- ----------------------------
-- Table structure for history_password
-- ----------------------------
DROP TABLE IF EXISTS `history_password`;
CREATE TABLE `history_password`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `account` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '用户账号',
  `password` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '用户密码',
  `create_time` bigint NOT NULL DEFAULT 0 COMMENT '创建时间',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '备注',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '历史密码记录表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of history_password
-- ----------------------------

-- ----------------------------
-- Table structure for itom_base_config
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_config`;
CREATE TABLE `itom_base_config`  (
  `account` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
  `config_id` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '资源id',
  `config` longtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '资源内容',
  PRIMARY KEY (`account`, `config_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_config
-- ----------------------------
INSERT INTO `itom_base_config` VALUES ('admin', 'customColumns', '{\"checkedCols\":[\"level\",\"alarm_description\",\"born_description\",\"location\",\"object\",\"born_timestamp\",\"accept_timestamp\",\"process\",\"accept_status\",\"confirm_status\",\"accept_source\",\"accept_description\",\"recover_timestamp\",\"recover_status\",\"recover_snapshot\",\"recover_type\",\"suggest\",\"type\",\"born_snapshot\",\"real_value\",\"threshold\",\"produce_reason\",\"confirm_timestamp\",\"confirm_source\",\"confirm_description\",\"alarm_duration\"],\"checkedExtraCols\":[\"multiSelect\",\"operator\"],\"fixedCols\":[],\"sortedCols\":[\"level\",\"alarm_description\",\"born_description\",\"location\",\"object\",\"born_timestamp\",\"accept_timestamp\",\"process\",\"accept_status\",\"confirm_status\",\"accept_source\",\"accept_description\",\"recover_timestamp\",\"recover_status\",\"recover_snapshot\",\"recover_type\",\"suggest\",\"type\",\"born_snapshot\",\"real_value\",\"threshold\",\"produce_reason\",\"confirm_timestamp\",\"confirm_source\",\"confirm_description\",\"alarm_duration\"]}');
INSERT INTO `itom_base_config` VALUES ('admin', 'customTools', '{\"clearAudio\":false,\"lockedRefresh\":false,\"checkedCols\":[\"level\",\"alarm_description\",\"born_description\",\"location\",\"object\",\"born_timestamp\",\"update_timestamp\",\"accept_timestamp\",\"event_process\",\"accept_status\",\"confirm_status\",\"accept_source\",\"accept_description\",\"recover_timestamp\",\"recover_status\",\"recover_snapshot\",\"recover_type\",\"suggest\",\"event_type\",\"born_snapshot\",\"real_value\",\"threshold\"],\"checkedAlerts\":[],\"voiceAlarm\":\"voiceEnable\",\"layouts\":\"UD\",\"checkedExtraCols\":[\"multiSelect\",\"operator\"],\"fixedCols\":[],\"sortedCols\":[\"level\",\"alarm_description\",\"born_description\",\"location\",\"object\",\"born_timestamp\",\"update_timestamp\",\"accept_timestamp\",\"event_process\",\"accept_status\",\"confirm_status\",\"accept_source\",\"accept_description\",\"recover_timestamp\",\"recover_status\",\"recover_snapshot\",\"recover_type\",\"suggest\",\"event_type\",\"born_snapshot\",\"real_value\",\"threshold\"]}');

-- ----------------------------
-- Table structure for itom_base_department
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_department`;
CREATE TABLE `itom_base_department`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '部门名称',
  `remark` varchar(1000) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '备注',
  `parent_id` int NULL DEFAULT NULL COMMENT '部门层级',
  `default_dep` int NOT NULL DEFAULT 20 COMMENT '默认部门（10为默认部门，不可删除，20为可删除部门）',
  `row_status` int NOT NULL DEFAULT 10 COMMENT '删除标识符',
  `version` int NOT NULL COMMENT '版本号',
  `type` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '1' COMMENT '层级类型',
  `orders` int NOT NULL DEFAULT 0 COMMENT '编号顺序',
  `create_time` bigint NOT NULL COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 20 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = COMPACT;

-- ----------------------------
-- Records of itom_base_department
-- ----------------------------
INSERT INTO `itom_base_department` VALUES (1, '新疆中行', 'GU', 0, 10, 10, 0, '0', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (2, '阿勒泰分行', '', 1, 20, 20, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (3, '新疆分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (4, '克拉玛依分行', '', 1, 20, 20, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (5, '喀什分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (6, '克拉玛依分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (7, '塔城分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (8, '哈密分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (9, '昌吉分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (10, '吐鲁番分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (11, '博州分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (12, '伊犁分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (13, '巴州分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (14, '克州分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (15, '阿勒泰分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (16, '阿克苏分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (17, '石河子分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (18, '和田分行', '', 1, 20, 10, 0, '1', 0, 1766142415);
INSERT INTO `itom_base_department` VALUES (19, '建设路六楼外呼中心', '', 1, 20, 10, 0, '1', 0, 1766142415);

-- ----------------------------
-- Table structure for itom_base_menu
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_menu`;
CREATE TABLE `itom_base_menu`  (
  `id` varchar(128) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  `parent_id` varchar(128) CHARACTER SET ascii COLLATE ascii_general_ci NULL DEFAULT NULL COMMENT '父项ID',
  `used` tinyint NOT NULL DEFAULT 1 COMMENT '是否启用菜单？1启用',
  `display_order` int NOT NULL DEFAULT 0 COMMENT '显示顺序大到小',
  `internal` int NOT NULL DEFAULT 0 COMMENT '是否内部菜单？内部菜单不允许删除',
  `owner_id` bigint NULL DEFAULT NULL COMMENT '私人菜单的用户ID关联',
  `license` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '菜单授权ID',
  `name` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '菜单名称，用于多语言',
  `title` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `url` varchar(1024) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tag` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '菜单标签，用于特殊场景',
  `target` enum('appzone','_blank','popup','top') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'appzone' COMMENT '目标框架',
  `operation_id` bigint NULL DEFAULT NULL COMMENT '权限控制，操作项ID',
  `row_status` int NOT NULL DEFAULT 10 COMMENT '删除标识符',
  `extend` varchar(1024) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '{}' COMMENT '菜单图标',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `operation_id`(`operation_id` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_menu
-- ----------------------------
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1', '{0000002F-0000-0000-C000-000000000046}', 1, 4800, 1, NULL, NULL, '门禁管理', '门禁管理', '', NULL, 'appzone', 769, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883', 'bf5d36594c3efcb1', 1, 800, 1, NULL, NULL, '权限配置', '权限配置', '', NULL, 'appzone', 805, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883.341c86804b81bbb4', 'bf5d36594c3efcb1.5abdc5ed65401883', 1, 200, 1, NULL, NULL, '时间段', '时间段', '../xweb_newaccesscontrol/index.html?r=period', NULL, 'appzone', 813, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883.521320fd3c9c09f7', 'bf5d36594c3efcb1.5abdc5ed65401883', 1, 110, 1, NULL, NULL, '节假日', '节假日', '../xweb_newaccesscontrol/index.html?r=holiday', NULL, 'appzone', 820, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883.96955ec871858f86', 'bf5d36594c3efcb1.5abdc5ed65401883', 1, 300, 1, NULL, NULL, '权限组', '权限组', '../xweb_newaccesscontrol/index.html?r=authority', NULL, 'appzone', 810, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883.ef7096b1c8f0970e', 'bf5d36594c3efcb1.5abdc5ed65401883', 1, 400, 1, NULL, NULL, '人员信息', '人员信息', '../xweb_newaccesscontrol/index.html?r=person', NULL, 'appzone', 819, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.9f46c296d73593c4', 'bf5d36594c3efcb1', 1, 900, 1, NULL, NULL, '门禁监控', '门禁监控', '', NULL, 'appzone', 790, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.9f46c296d73593c4.7b0730624e2d8018', 'bf5d36594c3efcb1.9f46c296d73593c4', 1, 300, 1, NULL, NULL, '门状态', '门状态', '../xweb_newaccesscontrol/index.html?r=status', NULL, 'appzone', 794, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.9f46c296d73593c4.bf69b24de7a5bf31', 'bf5d36594c3efcb1.9f46c296d73593c4', 1, 200, 1, NULL, NULL, '历史记录', '历史记录', '../xweb_newaccesscontrol/index.html?r=history', NULL, 'appzone', 796, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.9f46c296d73593c4.e56688ec5f14a1f7', 'bf5d36594c3efcb1.9f46c296d73593c4', 1, 100, 1, NULL, NULL, '告警记录', '告警记录', '../xweb_newaccesscontrol/index.html?r=alarm', NULL, 'appzone', 792, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.f236b205b5fdb2e0', 'bf5d36594c3efcb1', 1, 700, 1, NULL, NULL, '设备管理', '设备管理', '', NULL, 'appzone', 772, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.f236b205b5fdb2e0.53f20519272c8b7f', 'bf5d36594c3efcb1.f236b205b5fdb2e0', 1, 100, 1, NULL, NULL, '门管理', '门管理', '../xweb_newaccesscontrol/index.html?r=door', NULL, 'appzone', 781, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('bf5d36594c3efcb1.f236b205b5fdb2e0.81970ca803f32070', 'bf5d36594c3efcb1.f236b205b5fdb2e0', 1, 200, 1, NULL, NULL, '控制器', '控制器', '../xweb_newaccesscontrol/index.html?r=controller', NULL, 'appzone', 773, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('dataQuality', '{0000002F-0000-0000-C000-000000000046}', 1, 4999, 1, NULL, NULL, '数据质量', '数据质量', '', NULL, 'appzone', 449, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('dataQuality.alarmConfig', 'dataQuality', 1, 5000, 1, NULL, NULL, '数据质量告警配置', '数据质量告警配置', '../xweb_reportformv2/#/data-quality-alarm-config', NULL, 'appzone', 458, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('dataQuality.exceptionList', 'dataQuality', 1, 4999, 1, NULL, NULL, '数据质量异常列表', '数据质量异常列表', '../xweb_reportformv2/#/data-quality-exception', NULL, 'appzone', 453, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('energyPowerMonitor.extremeDataQuery', 'reportForm.commonForm', 1, 4998, 1, NULL, NULL, '运行极值查询', '运行极值查询', '../xweb_reportformv2/#/extreme-value-query', NULL, 'appzone', 268, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('energyPowerMonitor.originDataQuery', 'reportForm.commonForm', 1, 4999, 1, NULL, NULL, '原始数据查询', '原始数据查询', '../xweb_reportformv2/#/origin-data', NULL, 'appzone', 254, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message', 'message_center', 1, 1, 1, NULL, NULL, '系统消息', '系统消息', '', NULL, 'appzone', 167, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center', '{0000002F-0000-0000-C000-000000000046}', 1, 0, 1, NULL, NULL, '消息中心', '消息中心', '', NULL, 'appzone', 760, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center.message.list', 'message', 1, 1, 1, NULL, NULL, '消息总览', '消息总览', '/page/xweb_messagecenter/message.html#/message/list', NULL, 'appzone', 179, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center.message.setting', 'message', 1, 0, 1, NULL, NULL, '消息配置', '消息配置', '/page/xweb_messagecenter/message.html#/message/setting', NULL, 'appzone', 169, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center.notice.all', 'notice', 1, 1, 1, NULL, NULL, '全部公告', '全部公告', '/page/xweb_messagecenter/message.html#/notice/admin-post-list', NULL, 'appzone', 142, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center.notice.create', 'notice', 1, 4, 1, NULL, NULL, '创建公告', '创建公告', '/page/xweb_messagecenter/message.html#/notice/add', NULL, 'appzone', 134, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center.notice.draft', 'notice', 1, 3, 1, NULL, NULL, '草稿箱', '草稿箱', '/page/xweb_messagecenter/message.html#/notice/draft-list', NULL, 'appzone', 152, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center.notice.list', 'notice', 1, 5, 1, NULL, NULL, '公告总览', '公告总览', '/page/xweb_messagecenter/message.html#/notice/list', NULL, 'appzone', 162, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center.notice.send', 'notice', 1, 2, 1, NULL, NULL, '已发公告', '已发公告', '/page/xweb_messagecenter/message.html#/notice/post-list', NULL, 'appzone', 125, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('message_center.notice.setting', 'notice', 1, 0, 1, NULL, NULL, '公告配置', '公告配置', '/page/xweb_messagecenter/message.html#/notice/setting', NULL, 'appzone', 133, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('monitorInfo', '{0000002F-0000-0000-C000-000000000046}', 1, 5000, 1, NULL, NULL, '设施监控', '设施监控', '../ht_view/ht_view.html', NULL, 'appzone', 467, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('notice', 'message_center', 1, 0, 1, NULL, NULL, '通知公告', '通知公告', '', NULL, 'appzone', 119, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration', '{0000002F-0000-0000-C000-000000000046}', 1, 4600, 1, NULL, NULL, '工程配置', '工程配置', '', NULL, 'appzone', 472, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmDescription', 'projectConfiguration', 1, 4300, 1, NULL, NULL, '告警描述配置', '告警描述配置', '../alarm_description_config/alarm_description_config.html', NULL, 'appzone', 510, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmMasking', 'projectConfiguration', 1, 4800, 1, NULL, NULL, '告警屏蔽', '告警屏蔽', '', NULL, 'appzone', 519, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmMasking.blockingRules', 'projectConfiguration.alarmMasking', 1, 5000, 1, NULL, NULL, '屏蔽规则', '屏蔽规则', '/page/alarm_shielding_rule/alarm_shielding_rule.html', NULL, 'appzone', 531, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmMasking.timeManagement', 'projectConfiguration.alarmMasking', 1, 4900, 1, NULL, NULL, '时段管理', '时段管理', '/page/shielding_times/shielding_times.html', NULL, 'appzone', 521, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmNotify', 'projectConfiguration', 1, 5000, 1, NULL, NULL, '告警告知', '告警告知', '', NULL, 'appzone', 633, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmNotify.alarmStrategy', 'projectConfiguration.alarmNotify', 1, 5000, 1, NULL, NULL, '告警策略', '告警策略', '../ht_gjgz/ht_gjgz.html', NULL, 'appzone', 653, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmNotify.messageRecordQuery', 'projectConfiguration.alarmNotify', 1, 4500, 1, NULL, NULL, '信息查询', '信息查询', '../message_record_query/message_record_query.html', NULL, 'appzone', 643, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmNotify.messageRouting', 'projectConfiguration.alarmNotify', 1, 4900, 1, NULL, NULL, '信息发送', '信息发送', '../message_send_service/message_send_main.html', NULL, 'appzone', 660, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmNotify.messageTemplate', 'projectConfiguration.alarmNotify', 1, 4600, 1, NULL, NULL, '信息模板', '信息模板', '../xweb_cep/#/messageTemplate', NULL, 'appzone', 667, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmNotify.regularySecurity', 'projectConfiguration.alarmNotify', 1, 4800, 1, NULL, NULL, '定时报平安', '定时报平安', '../regular_security/regular_security.html', NULL, 'appzone', 670, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmNotify.stormWarning', 'projectConfiguration.alarmNotify', 1, 4400, 1, NULL, NULL, '风暴预警', '风暴预警', '../xweb_cep/#/stormWarningConfig', NULL, 'appzone', 665, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.alarmSetting', 'projectConfiguration', 1, 4900, 1, NULL, NULL, '告警配置', '告警配置', '../alarm_config/alarm_config.html', NULL, 'appzone', 603, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.carousel', 'projectConfiguration', 1, 3700, 1, NULL, NULL, '大屏管理', '大屏管理', '../carousel', NULL, 'appzone', 631, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.driversManager', 'projectConfiguration', 1, 3800, 1, NULL, NULL, '驱动管理', '驱动管理', '', NULL, 'appzone', 576, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.driversManager.collection', 'projectConfiguration.driversManager', 1, 5001, 1, NULL, NULL, '采集组件', '采集组件', '/tools/#/home_au/collection', NULL, 'appzone', 578, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.driversManager.devNode', 'projectConfiguration.driversManager', 1, 5003, 1, NULL, NULL, '设备点表', '设备点表', '/tools/#/home_au/devNode', NULL, 'appzone', 594, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.driversManager.protocolFile', 'projectConfiguration.driversManager', 1, 5002, 1, NULL, NULL, '协议文件', '协议文件', '/tools/#/home_au/protocolFile', NULL, 'appzone', 587, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.hardwareConfiguration.portWorkPattern', 'projectConfiguration.hardwareConfiguration', 0, 5000, 1, NULL, NULL, '端口工作模式', '端口工作模式', '../hardware_config/hardware_config.html', NULL, 'appzone', 118, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.nussi', 'projectConfiguration', 1, 4700, 1, NULL, NULL, '北向连接', '北向连接', '../north_interface/north_interface.html', NULL, 'appzone', 566, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.policyLinkage', 'projectConfiguration', 1, 4500, 1, NULL, NULL, '联动策略', '联动策略', '', NULL, 'appzone', 536, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.policyLinkage.functionModule', 'projectConfiguration.policyLinkage', 1, 4900, 1, NULL, NULL, '函数模型', '函数模型', '../funcmodel_list_detail/funcmodel_list.html', NULL, 'appzone', 550, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.policyLinkage.strategyList', 'projectConfiguration.policyLinkage', 1, 5100, 1, NULL, NULL, '后台联动', '后台联动', '../xweb_projectconfiguration_v2/strategy.html#/strategy/strategyList', NULL, 'appzone', 543, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.policyLinkage.strategyOverview', 'projectConfiguration.policyLinkage', 1, 5200, 1, NULL, NULL, '策略总览', '策略总览', '../xweb_projectconfiguration_v2/strategy.html#/strategy/strategyOverview', NULL, 'appzone', 559, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.storageSetting', 'projectConfiguration', 1, 4600, 1, NULL, NULL, '存储配置', '存储配置', '', NULL, 'appzone', 481, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.storageSetting.storageEngine', 'projectConfiguration.storageSetting', 1, 4800, 1, NULL, NULL, '存储引擎', '存储引擎', '../storage_engine/storage_engine.html', NULL, 'appzone', 488, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.storageSetting.storagePolicy', 'projectConfiguration.storageSetting', 1, 4900, 1, NULL, NULL, '存储策略', '存储策略', '../storage_policy/storage_policy.html', NULL, 'appzone', 498, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.systemTools', 'projectConfiguration', 1, 4000, 1, NULL, NULL, '系统工具', '系统工具', '', NULL, 'appzone', 610, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.systemTools.configImportExport', 'projectConfiguration.systemTools', 1, 4900, 1, NULL, NULL, '告警规则管理', '告警规则管理', '../config_import_export/config_import_export.html', NULL, 'appzone', 622, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.systemTools.networkDbugTools', 'projectConfiguration.systemTools', 1, 4800, 1, NULL, NULL, '网络调试工具', '网络调试工具', '../network_debugging_tools/network_debugging_tools.html', NULL, 'appzone', 616, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.systemTools.serialPortTool', 'projectConfiguration.systemTools', 1, 5000, 1, NULL, NULL, '串口工具', '串口工具', '../serial_port_tool/serial_port_tool.html', NULL, 'appzone', 614, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.XECEP', 'projectConfiguration', 1, 4100, 1, NULL, NULL, '智能告警', '智能告警', '', NULL, 'appzone', 677, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.XECEP.Custom', 'projectConfiguration.XECEP', 1, 4200, 1, NULL, NULL, 'CEP2.0', 'CEP2.0', '../xweb_cep/cep.html', NULL, 'appzone', 691, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('projectConfiguration.XECEP.RepeatNoise', 'projectConfiguration.XECEP', 1, 4100, 1, NULL, NULL, '规则列表', '规则列表', '../xweb_cep/#/cepMainPage', NULL, 'appzone', 681, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm', '{0000002F-0000-0000-C000-000000000046}', 1, 4900, 1, NULL, NULL, '报表报告', '报表报告', '', NULL, 'appzone', 196, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.commonForm', 'reportForm', 1, 4500, 1, NULL, NULL, '数据查询', '数据查询', '', NULL, 'appzone', 240, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.commonForm.dataSearch', 'reportForm.commonForm', 1, 5000, 1, NULL, NULL, '参数查询', '参数查询', '../xweb_reportformv2/#/params-query', NULL, 'appzone', 245, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.commonForm.instantQuery', 'reportForm.commonForm', 1, 4800, 1, NULL, NULL, '即时查询', '即时查询', '../xweb_reportformv2/#/instant-query', NULL, 'appzone', 262, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.commonForm.querySchemeConfig', 'reportForm.commonForm', 1, 4700, 1, NULL, NULL, '查询方案配置', '查询方案配置', '../xweb_reportformv2/#/query-scheme-config', NULL, 'appzone', 253, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.commonForm.warnSearchV2', 'reportForm.commonForm', 1, 4900, 1, NULL, NULL, '告警查询', '告警查询', '/page/xweb_messagecenter/historyEvent.html#/alarm-query', NULL, 'appzone', 269, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.reportOverview', 'reportForm', 1, 4800, 1, NULL, NULL, '报表总览', '报表总览', '../xweb_reportformv2/#/report-overview', NULL, 'appzone', 211, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.storageStatistics', 'projectConfiguration.storageSetting', 1, 5000, 1, NULL, NULL, '存储总览', '存储总览', '../xweb_projectconfiguration_v2/storageStatistics.html#/storageStatistics', NULL, 'appzone', 504, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.taskManagement', 'reportForm', 1, 4600, 1, NULL, NULL, '任务管理', '任务管理', '../xweb_reportformv2/#/task-management', NULL, 'appzone', 201, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('reportForm.templateManagement', 'reportForm', 1, 4700, 1, NULL, NULL, '模板管理', '模板管理', '../xweb_reportformv2/#/template-management', NULL, 'appzone', 207, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement', '{0000002F-0000-0000-C000-000000000046}', 1, 4700, 1, NULL, NULL, '系统管理', '系统管理', '', NULL, 'appzone', 283, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.authorityManagement', 'systemManagement', 1, 5000, 1, NULL, NULL, '权限管理', '权限管理', '', NULL, 'appzone', 361, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.authorityManagement.accountRecycle', 'systemManagement.authorityManagement', 1, 4500, 1, NULL, NULL, '已删除用户', '已删除用户', '../authority/account_recycle/account_recycle.html', NULL, 'appzone', 385, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.authorityManagement.departmentManagement', 'systemManagement.authorityManagement', 1, 5000, 1, NULL, NULL, '部门管理', '部门管理', '../authority/department_manager/department_manager.html', NULL, 'appzone', 371, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.authorityManagement.ipWhiteList', 'systemManagement.authorityManagement', 1, 4300, 1, NULL, NULL, 'IP白名单', 'IP白名单', '../authority/white_list/white_list.html', NULL, 'appzone', 389, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.authorityManagement.onlineUser', 'systemManagement.authorityManagement', 1, 4600, 1, NULL, NULL, '在线用户', '在线用户', '../authority/online_users/online_users.html', NULL, 'appzone', 399, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.authorityManagement.roleManagement', 'systemManagement.authorityManagement', 1, 4800, 1, NULL, NULL, '角色管理', '角色管理', '../authority/role_manager/role_manager.html', NULL, 'appzone', 406, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.authorityManagement.timePeriodManagement', 'systemManagement.authorityManagement', 1, 4700, 1, NULL, NULL, '时间段管理', '时间段管理', '../authority/time_period_manager/time_period_manager.html', NULL, 'appzone', 382, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.authorityManagement.userManagement', 'systemManagement.authorityManagement', 1, 4900, 1, NULL, NULL, '用户管理', '用户管理', '../authority/employee_manager/employee_manager.html', NULL, 'appzone', 379, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.backupRestore', 'systemManagement', 1, 4500, 1, NULL, NULL, '备份还原', '备份还原', '', NULL, 'appzone', 293, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.backupRestore.autoBackup', 'systemManagement.backupRestore', 1, 4800, 1, NULL, NULL, '自动备份配置', '自动备份配置', '../auto_backup/auto_backup.html', NULL, 'appzone', 306, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.backupRestore.pageBackupRestore', 'systemManagement.backupRestore', 1, 4900, 1, NULL, NULL, '页面备份还原', '页面备份还原', '../page_backup_restore/page_backup_restore.html', NULL, 'appzone', 299, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.backupRestore.systemBackupRestore', 'systemManagement.backupRestore', 1, 5000, 1, NULL, NULL, '系统备份还原', '系统备份还原', '../backup_restore/backup_restore.html', NULL, 'appzone', 308, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.pageSettingV2', 'systemManagement', 1, 4700, 1, NULL, NULL, '页面配置', '页面配置', '', NULL, 'appzone', 416, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.pageSettingV2.homepage', 'systemManagement.pageSettingV2', 1, 4900, 1, NULL, NULL, '首页配置', '首页配置', '/page/xweb_projectconfiguration_v2/pageConfig.html#/pageConfig/home', NULL, 'appzone', 432, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.pageSettingV2.login', 'systemManagement.pageSettingV2', 1, 5000, 1, NULL, NULL, '登录页配置', '登录页配置', '/page/xweb_projectconfiguration_v2/pageConfig.html#/', NULL, 'appzone', 423, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.pageSettingV2.style', 'systemManagement.pageSettingV2', 1, 4800, 1, NULL, NULL, '风格配置', '风格配置', '/page/xweb_projectconfiguration_v2/pageConfig.html#/pageConfig/style', NULL, 'appzone', 446, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.pageSettingV2.view', 'systemManagement.pageSettingV2', 1, 4700, 1, NULL, NULL, '组态配置', '组态配置', '/page/xweb_projectconfiguration_v2/pageConfig.html#/pageConfig/ht', NULL, 'appzone', 442, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemAuthorization', 'systemManagement', 1, 4400, 1, NULL, NULL, '系统授权', '系统授权', '../system_authorization/system_authorization.html', NULL, 'appzone', 322, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemLog', 'systemManagement', 1, 4100, 1, NULL, NULL, '系统日志', '系统日志', '', NULL, 'appzone', 329, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemLog.linkageRecord', 'systemManagement.systemLog', 1, 4100, 1, NULL, NULL, '联动日志', '联动日志', '../linkage_record/linkage_record.html', NULL, 'appzone', 337, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemLog.operationLog', 'systemManagement.systemLog', 1, 4200, 1, NULL, NULL, '操作日志', '操作日志', '../operation_record/operation_record.html', NULL, 'appzone', 333, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemSetting', 'systemManagement', 1, 5000, 1, NULL, NULL, '系统配置', '系统配置', '../system_configuration/system_configuration.html', NULL, 'appzone', 327, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemSkinPeelerV2', 'systemManagement', 1, 4300, 1, NULL, NULL, '主题配置', '主题配置', '/page/xweb_projectconfiguration_v2/systemSkinChange.html#/systemSkinChange', NULL, 'appzone', 407, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemUpgrade', 'systemManagement', 1, 4600, 1, NULL, NULL, '系统升级', '系统升级', '', NULL, 'appzone', 338, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemUpgrade.systemUpgrade', 'systemManagement.systemUpgrade', 1, 5000, 1, NULL, NULL, '系统升级', '系统升级', '../upgrade/upgrade.html', NULL, 'appzone', 347, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.systemUpgrade.upgradeRecord', 'systemManagement.systemUpgrade', 1, 4900, 1, NULL, NULL, '升级记录', '升级记录', '../upgrade_record/upgrade_record.html', NULL, 'appzone', 352, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('systemManagement.viewManagement', 'systemManagement', 0, 4900, 1, NULL, NULL, '视图管理', '视图管理', '', NULL, 'appzone', 312, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('viewManagement.logicManagement', 'systemManagement.viewManagement', 0, 4900, 1, NULL, NULL, '逻辑视图', '逻辑视图', '', NULL, 'appzone', 321, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('xbTool', '{0000002F-0000-0000-C000-000000000046}', 1, 4500, 1, NULL, NULL, '工程组态', '工程组态', '../engineering/engineering.html', NULL, 'appzone', 185, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('{0000002F-0000-0000-C000-000000000046}', NULL, 1, 1, 1, NULL, NULL, 'ROOT', 'ROOT', '', NULL, 'appzone', NULL, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}');
INSERT INTO `itom_base_menu` VALUES ('{0000002F-0000-0000-C000-000000000047}', '{0000002F-0000-0000-C000-000000000046}', 1, 10000, 1, NULL, NULL, '快速访问', '快速访问', '', NULL, 'appzone', 186, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}');

-- ----------------------------
-- Table structure for itom_base_menu_v2
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_menu_v2`;
CREATE TABLE `itom_base_menu_v2`  (
  `id` varchar(128) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  `parent_id` varchar(128) CHARACTER SET ascii COLLATE ascii_general_ci NULL DEFAULT NULL COMMENT '父项ID',
  `used` tinyint NOT NULL DEFAULT 1 COMMENT '是否启用菜单？1启用',
  `display_order` int NOT NULL DEFAULT 0 COMMENT '显示顺序大到小',
  `internal` int NOT NULL DEFAULT 0 COMMENT '是否内部菜单？内部菜单不允许删除',
  `owner_id` bigint NULL DEFAULT NULL COMMENT '私人菜单的用户ID关联',
  `license` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '菜单授权ID',
  `name` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `title` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `url` varchar(1024) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `tag` varchar(64) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '菜单标签，用于特殊场景',
  `target` enum('appzone','_blank','popup','top') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'appzone' COMMENT '目标框架',
  `operation_id` bigint NULL DEFAULT NULL COMMENT '权限控制，操作项ID',
  `row_status` int NOT NULL DEFAULT 10 COMMENT '删除标识符',
  `extend` varchar(1024) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '{}' COMMENT '菜单图标',
  `declared` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' COMMENT '菜单说明',
  `navigation_icon` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' COMMENT '导航图标url',
  `font_icon` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' COMMENT '字体图标url',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `operation_id`(`operation_id` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_menu_v2
-- ----------------------------
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1', 'facilityMonitor', 1, 4800, 1, NULL, NULL, '门禁管理', '门禁管理', '', NULL, 'appzone', 735, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/menjinguanli.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883', 'bf5d36594c3efcb1', 1, 800, 1, NULL, NULL, '权限配置', '权限配置', '', NULL, 'appzone', 737, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883.341c86804b81bbb4', 'bf5d36594c3efcb1.5abdc5ed65401883', 1, 200, 1, NULL, NULL, '时间段', '时间段', '../xweb_newaccesscontrol/index.html?r=period', NULL, 'appzone', 754, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883.521320fd3c9c09f7', 'bf5d36594c3efcb1.5abdc5ed65401883', 1, 110, 1, NULL, NULL, '节假日', '节假日', '../xweb_newaccesscontrol/index.html?r=holiday', NULL, 'appzone', 757, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883.96955ec871858f86', 'bf5d36594c3efcb1.5abdc5ed65401883', 1, 300, 1, NULL, NULL, '权限组', '权限组', '../xweb_newaccesscontrol/index.html?r=authority', NULL, 'appzone', 755, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.5abdc5ed65401883.ef7096b1c8f0970e', 'bf5d36594c3efcb1.5abdc5ed65401883', 1, 400, 1, NULL, NULL, '人员信息', '人员信息', '../xweb_newaccesscontrol/index.html?r=person', NULL, 'appzone', 745, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.9f46c296d73593c4', 'bf5d36594c3efcb1', 1, 900, 1, NULL, NULL, '门禁监控', '门禁监控', '', NULL, 'appzone', 761, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.9f46c296d73593c4.7b0730624e2d8018', 'bf5d36594c3efcb1.9f46c296d73593c4', 1, 300, 1, NULL, NULL, '门状态', '门状态', '../xweb_newaccesscontrol/index.html?r=status', NULL, 'appzone', 775, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.9f46c296d73593c4.bf69b24de7a5bf31', 'bf5d36594c3efcb1.9f46c296d73593c4', 1, 200, 1, NULL, NULL, '历史记录', '历史记录', '../xweb_newaccesscontrol/index.html?r=history', NULL, 'appzone', 782, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.9f46c296d73593c4.e56688ec5f14a1f7', 'bf5d36594c3efcb1.9f46c296d73593c4', 1, 100, 1, NULL, NULL, '告警记录', '告警记录', '../xweb_newaccesscontrol/index.html?r=alarm', NULL, 'appzone', 770, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.f236b205b5fdb2e0', 'bf5d36594c3efcb1', 1, 700, 1, NULL, NULL, '设备管理', '设备管理', '', NULL, 'appzone', 785, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.f236b205b5fdb2e0.53f20519272c8b7f', 'bf5d36594c3efcb1.f236b205b5fdb2e0', 1, 100, 1, NULL, NULL, '门管理', '门管理', '../xweb_newaccesscontrol/index.html?r=door', NULL, 'appzone', 792, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('bf5d36594c3efcb1.f236b205b5fdb2e0.81970ca803f32070', 'bf5d36594c3efcb1.f236b205b5fdb2e0', 1, 200, 1, NULL, NULL, '控制器', '控制器', '../xweb_newaccesscontrol/index.html?r=controller', NULL, 'appzone', 796, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('dataAnalysis', '{0000002F-0000-0000-C000-000000000050}', 1, 4900, 1, NULL, NULL, '数据分析', '数据分析', '', NULL, 'appzone', 530, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('dataQuality', 'dataAnalysis', 1, 4999, 1, NULL, NULL, '数据质量', '数据质量', '', NULL, 'appzone', 636, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/moren.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('dataQuality.alarmConfig', 'dataQuality', 1, 5000, 1, NULL, NULL, '数据质量告警配置', '数据质量告警配置', '../xweb_reportformv2/#/data-quality-alarm-config', NULL, 'appzone', 639, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('dataQuality.exceptionList', 'dataQuality', 1, 4999, 1, NULL, NULL, '数据质量异常列表', '数据质量异常列表', '../xweb_reportformv2/#/data-quality-exception', NULL, 'appzone', 647, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('energyPowerMonitor.extremeDataQuery', 'reportForm.commonForm', 1, 4998, 1, NULL, NULL, '运行极值查询', '运行极值查询', '../xweb_reportformv2/#/extreme-value-query', NULL, 'appzone', 603, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('energyPowerMonitor.originDataQuery', 'reportForm.commonForm', 1, 4999, 1, NULL, NULL, '原始数据查询', '原始数据查询', '../xweb_reportformv2/#/origin-data', NULL, 'appzone', 583, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('facilityMonitor', '{0000002F-0000-0000-C000-000000000050}', 1, 5000, 1, NULL, NULL, '设施监控', '设施监控', '', NULL, 'appzone', 360, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('facilityMonitor.alarmManagement', 'facilityMonitor', 1, 4900, 1, NULL, NULL, '告警管理', '告警管理', '', NULL, 'appzone', 365, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/gaojingguanli.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('integratedSystem', '{0000002F-0000-0000-C000-000000000050}', 1, 4400, 1, NULL, NULL, '集成系统', '集成系统', '', NULL, 'appzone', 652, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message', 'message_center', 1, 1, 1, NULL, NULL, '系统消息', '系统消息', '', NULL, 'appzone', 512, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center', 'facilityMonitor', 1, 4800, 1, NULL, NULL, '消息中心', '消息中心', '', NULL, 'appzone', 681, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/xiaoxizhongxin.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center.message.list', 'message', 1, 1, 1, NULL, NULL, '消息总览', '消息总览', '/page/xweb_messagecenter/message.html#/message/list', NULL, 'appzone', 522, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center.message.setting', 'message', 1, 0, 1, NULL, NULL, '消息配置', '消息配置', '/page/xweb_messagecenter/message.html#/message/setting', NULL, 'appzone', 516, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center.notice.all', 'notice', 1, 1, 1, NULL, NULL, '全部公告', '全部公告', '/page/xweb_messagecenter/message.html#/notice/admin-post-list', NULL, 'appzone', 493, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center.notice.create', 'notice', 1, 4, 1, NULL, NULL, '创建公告', '创建公告', '/page/xweb_messagecenter/message.html#/notice/add', NULL, 'appzone', 503, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center.notice.draft', 'notice', 1, 3, 1, NULL, NULL, '草稿箱', '草稿箱', '/page/xweb_messagecenter/message.html#/notice/draft-list', NULL, 'appzone', 507, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center.notice.list', 'notice', 1, 5, 1, NULL, NULL, '公告总览', '公告总览', '/page/xweb_messagecenter/message.html#/notice/list', NULL, 'appzone', 484, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center.notice.send', 'notice', 1, 2, 1, NULL, NULL, '已发公告', '已发公告', '/page/xweb_messagecenter/message.html#/notice/post-list', NULL, 'appzone', 486, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('message_center.notice.setting', 'notice', 1, 0, 1, NULL, NULL, '公告配置', '公告配置', '/page/xweb_messagecenter/message.html#/notice/setting', NULL, 'appzone', 494, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('monitorInfo', 'facilityMonitor', 1, 5100, 1, NULL, NULL, '基础设施监控', '基础设施监控', '../ht_view/ht_view.html', NULL, 'appzone', 467, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/sheshijiankong.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('notice', 'message_center', 1, 0, 1, NULL, NULL, '通知公告', '通知公告', '', NULL, 'appzone', 483, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration', '{0000002F-0000-0000-C000-000000000050}', 1, 4700, 1, NULL, NULL, '工程配置', '工程配置', '', NULL, 'appzone', 252, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmDescription', 'facilityMonitor.alarmManagement', 1, 4300, 1, NULL, NULL, '告警描述配置', '告警描述配置', '../alarm_description_config/alarm_description_config.html', NULL, 'appzone', 370, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmMasking', 'facilityMonitor.alarmManagement', 1, 4800, 1, NULL, NULL, '告警屏蔽', '告警屏蔽', '', NULL, 'appzone', 438, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmMasking.blockingRules', 'projectConfiguration.alarmMasking', 1, 5000, 1, NULL, NULL, '屏蔽规则', '屏蔽规则', '/page/alarm_shielding_rule/alarm_shielding_rule.html', NULL, 'appzone', 444, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmMasking.timeManagement', 'projectConfiguration.alarmMasking', 1, 4900, 1, NULL, NULL, '时段管理', '时段管理', '/page/shielding_times/shielding_times.html', NULL, 'appzone', 442, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmNotify', 'facilityMonitor.alarmManagement', 1, 5000, 1, NULL, NULL, '告警告知', '告警告知', '', NULL, 'appzone', 377, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmNotify.alarmStrategy', 'projectConfiguration.alarmNotify', 1, 5000, 1, NULL, NULL, '告警策略', '告警策略', '../ht_gjgz/ht_gjgz.html', NULL, 'appzone', 405, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmNotify.messageRecordQuery', 'projectConfiguration.alarmNotify', 1, 4500, 1, NULL, NULL, '信息查询', '信息查询', '../message_record_query/message_record_query.html', NULL, 'appzone', 425, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmNotify.messageRouting', 'projectConfiguration.alarmNotify', 1, 4900, 1, NULL, NULL, '信息发送', '信息发送', '../message_send_service/message_send_main.html', NULL, 'appzone', 395, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmNotify.messageTemplate', 'projectConfiguration.alarmNotify', 1, 4600, 1, NULL, NULL, '信息模板', '信息模板', '../xweb_cep/#/messageTemplate', NULL, 'appzone', 382, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmNotify.regularySecurity', 'projectConfiguration.alarmNotify', 1, 4800, 1, NULL, NULL, '定时报平安', '定时报平安', '../regular_security/regular_security.html', NULL, 'appzone', 415, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmNotify.stormWarning', 'projectConfiguration.alarmNotify', 1, 4400, 1, NULL, NULL, '风暴预警', '风暴预警', '../xweb_cep/#/stormWarningConfig', NULL, 'appzone', 389, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.alarmSetting', 'facilityMonitor.alarmManagement', 1, 4900, 1, NULL, NULL, '告警配置', '告警配置', '../alarm_config/alarm_config.html', NULL, 'appzone', 435, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.carousel', 'facilityMonitor', 1, 4850, 1, NULL, NULL, '大屏监控', '大屏监控', '../carousel', NULL, 'appzone', 473, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/dapingguanli.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.driversManager', 'projectConfiguration', 1, 4500, 1, NULL, NULL, '驱动管理', '驱动管理', '', NULL, 'appzone', 259, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/qudongguanli.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.driversManager.collection', 'projectConfiguration.driversManager', 1, 5001, 1, NULL, NULL, '采集组件', '采集组件', '/tools/#/home_au/collection', NULL, 'appzone', 285, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.driversManager.devNode', 'projectConfiguration.driversManager', 1, 5003, 1, NULL, NULL, '设备点表', '设备点表', '/tools/#/home_au/devNode', NULL, 'appzone', 277, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.driversManager.protocolFile', 'projectConfiguration.driversManager', 1, 5002, 1, NULL, NULL, '协议文件', '协议文件', '/tools/#/home_au/protocolFile', NULL, 'appzone', 269, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.nussi', 'projectConfiguration', 1, 5000, 1, NULL, NULL, '北向连接', '北向连接', '../north_interface/north_interface.html', NULL, 'appzone', 307, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/beixianglianjie.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.policyLinkage', 'projectConfiguration', 1, 4800, 1, NULL, NULL, '联动策略', '联动策略', '', NULL, 'appzone', 336, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/liandongcelue.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.policyLinkage.functionModule', 'projectConfiguration.policyLinkage', 1, 4900, 1, NULL, NULL, '函数模型', '函数模型', '../funcmodel_list_detail/funcmodel_list.html', NULL, 'appzone', 345, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.policyLinkage.strategyList', 'projectConfiguration.policyLinkage', 1, 5100, 1, NULL, NULL, '后台联动', '后台联动', '../xweb_projectconfiguration_v2/strategy.html#/strategy/strategyList', NULL, 'appzone', 355, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.policyLinkage.strategyOverview', 'projectConfiguration.policyLinkage', 1, 5200, 1, NULL, NULL, '策略总览', '策略总览', '../xweb_projectconfiguration_v2/strategy.html#/strategy/strategyOverview', NULL, 'appzone', 356, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.storageSetting', 'projectConfiguration', 1, 4900, 1, NULL, NULL, '存储配置', '存储配置', '', NULL, 'appzone', 317, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/cunchupeizhi.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.storageSetting.storageEngine', 'projectConfiguration.storageSetting', 1, 4800, 1, NULL, NULL, '存储引擎', '存储引擎', '../storage_engine/storage_engine.html', NULL, 'appzone', 320, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.storageSetting.storagePolicy', 'projectConfiguration.storageSetting', 1, 4900, 1, NULL, NULL, '存储策略', '存储策略', '../storage_policy/storage_policy.html', NULL, 'appzone', 319, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.systemTools', 'projectConfiguration', 1, 4600, 1, NULL, NULL, '系统工具', '系统工具', '', NULL, 'appzone', 291, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/xitonggongju.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.systemTools.configImportExport', 'projectConfiguration.systemTools', 1, 4900, 1, NULL, NULL, '告警规则管理', '告警规则管理', '../config_import_export/config_import_export.html', NULL, 'appzone', 302, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.systemTools.networkDbugTools', 'projectConfiguration.systemTools', 1, 4800, 1, NULL, NULL, '网络调试工具', '网络调试工具', '../network_debugging_tools/network_debugging_tools.html', NULL, 'appzone', 293, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.systemTools.serialPortTool', 'projectConfiguration.systemTools', 1, 5000, 1, NULL, NULL, '串口工具', '串口工具', '../serial_port_tool/serial_port_tool.html', NULL, 'appzone', 299, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.XECEP', 'facilityMonitor.alarmManagement', 1, 4600, 1, NULL, NULL, '智能告警', '智能告警', '', NULL, 'appzone', 448, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.XECEP.Custom', 'projectConfiguration.XECEP', 1, 4200, 1, NULL, NULL, 'CEP2.0', 'CEP2.0', '../xweb_cep/cep.html', NULL, 'appzone', 463, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('projectConfiguration.XECEP.RepeatNoise', 'projectConfiguration.XECEP', 1, 4100, 1, NULL, NULL, '规则列表', '规则列表', '../xweb_cep/#/cepMainPage', NULL, 'appzone', 458, 10, '{\"icon\":\"\",\"mode\":0,\"type\":2,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm', 'dataAnalysis', 1, 5000, 1, NULL, NULL, '报表报告', '报表报告', '', NULL, 'appzone', 540, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/baobiaobaogao.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.commonForm', 'reportForm', 1, 4500, 1, NULL, NULL, '数据查询', '数据查询', '', NULL, 'appzone', 563, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.commonForm.dataSearch', 'reportForm.commonForm', 1, 5000, 1, NULL, NULL, '参数查询', '参数查询', '../xweb_reportformv2/#/params-query', NULL, 'appzone', 575, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.commonForm.instantQuery', 'reportForm.commonForm', 1, 4800, 1, NULL, NULL, '即时查询', '即时查询', '../xweb_reportformv2/#/instant-query', NULL, 'appzone', 569, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.commonForm.querySchemeConfig', 'reportForm.commonForm', 1, 4700, 1, NULL, NULL, '查询方案配置', '查询方案配置', '../xweb_reportformv2/#/query-scheme-config', NULL, 'appzone', 579, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.commonForm.warnSearchOtherV2', 'reportForm.commonForm', 1, 4900, 1, NULL, NULL, '告警查询', '告警查询', '/page/xweb_messagecenter/historyEvent.html#/alarm-query', NULL, 'appzone', 600, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.commonForm.warnSearchV2', 'facilityMonitor.alarmManagement', 1, 4500, 1, NULL, NULL, '告警查询', '告警查询', '/page/xweb_messagecenter/historyEvent.html#/alarm-query', NULL, 'appzone', 432, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.reportOverview', 'reportForm', 1, 4800, 1, NULL, NULL, '报表总览', '报表总览', '../xweb_reportformv2/#/report-overview', NULL, 'appzone', 623, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.storageStatistics', 'projectConfiguration.storageSetting', 1, 5000, 1, NULL, NULL, '存储总览', '存储总览', '../xweb_projectconfiguration_v2/storageStatistics.html#/storageStatistics', NULL, 'appzone', 324, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.taskManagement', 'reportForm', 1, 4600, 1, NULL, NULL, '任务管理', '任务管理', '../xweb_reportformv2/#/task-management', NULL, 'appzone', 545, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('reportForm.templateManagement', 'reportForm', 1, 4700, 1, NULL, NULL, '模板管理', '模板管理', '../xweb_reportformv2/#/template-management', NULL, 'appzone', 554, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement', '{0000002F-0000-0000-C000-000000000050}', 1, 4800, 1, NULL, NULL, '系统管理', '系统管理', '', NULL, 'appzone', 100, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.authorityManagement', 'systemManagement', 1, 5000, 1, NULL, NULL, '权限管理', '权限管理', '', NULL, 'appzone', 107, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/quanxianguanli.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.authorityManagement.accountRecycle', 'systemManagement.authorityManagement', 1, 4500, 1, NULL, NULL, '已删除用户', '已删除用户', '../authority/account_recycle/account_recycle.html', NULL, 'appzone', 126, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.authorityManagement.departmentManagement', 'systemManagement.authorityManagement', 1, 5000, 1, NULL, NULL, '部门管理', '部门管理', '../authority/department_manager/department_manager.html', NULL, 'appzone', 116, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.authorityManagement.ipWhiteList', 'systemManagement.authorityManagement', 1, 4300, 1, NULL, NULL, 'IP白名单', 'IP白名单', '../authority/white_list/white_list.html', NULL, 'appzone', 142, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.authorityManagement.onlineUser', 'systemManagement.authorityManagement', 1, 4600, 1, NULL, NULL, '在线用户', '在线用户', '../authority/online_users/online_users.html', NULL, 'appzone', 135, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.authorityManagement.roleManagement', 'systemManagement.authorityManagement', 1, 4800, 1, NULL, NULL, '角色管理', '角色管理', '../authority/role_manager/role_manager.html', NULL, 'appzone', 133, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.authorityManagement.timePeriodManagement', 'systemManagement.authorityManagement', 1, 4700, 1, NULL, NULL, '时间段管理', '时间段管理', '../authority/time_period_manager/time_period_manager.html', NULL, 'appzone', 145, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.authorityManagement.userManagement', 'systemManagement.authorityManagement', 1, 4900, 1, NULL, NULL, '用户管理', '用户管理', '../authority/employee_manager/employee_manager.html', NULL, 'appzone', 154, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.backupRestore', 'systemManagement', 1, 4500, 1, NULL, NULL, '备份还原', '备份还原', '', NULL, 'appzone', 208, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/beifenhuanyuan.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.backupRestore.autoBackup', 'systemManagement.backupRestore', 1, 4800, 1, NULL, NULL, '自动备份配置', '自动备份配置', '../auto_backup/auto_backup.html', NULL, 'appzone', 221, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.backupRestore.pageBackupRestore', 'systemManagement.backupRestore', 1, 4900, 1, NULL, NULL, '页面备份还原', '页面备份还原', '../page_backup_restore/page_backup_restore.html', NULL, 'appzone', 214, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.backupRestore.systemBackupRestore', 'systemManagement.backupRestore', 1, 5000, 1, NULL, NULL, '系统备份还原', '系统备份还原', '../backup_restore/backup_restore.html', NULL, 'appzone', 210, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.pageSettingV2', 'systemManagement', 1, 4700, 1, NULL, NULL, '页面配置', '页面配置', '', NULL, 'appzone', 177, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/xitongpeizhi.png', 'icon-xb-s-nav-2xitongpeizhi');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.pageSettingV2.homepage', 'systemManagement.pageSettingV2', 1, 4900, 1, NULL, NULL, '首页配置', '首页配置', '/page/xweb_projectconfiguration_v2/pageConfig.html#/pageConfig/home', NULL, 'appzone', 203, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.pageSettingV2.login', 'systemManagement.pageSettingV2', 1, 5000, 1, NULL, NULL, '登录页配置', '登录页配置', '/page/xweb_projectconfiguration_v2/pageConfig.html#/', NULL, 'appzone', 189, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.pageSettingV2.style', 'systemManagement.pageSettingV2', 1, 4800, 1, NULL, NULL, '风格配置', '风格配置', '/page/xweb_projectconfiguration_v2/pageConfig.html#/pageConfig/style', NULL, 'appzone', 195, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.pageSettingV2.view', 'systemManagement.pageSettingV2', 1, 4700, 1, NULL, NULL, '组态配置', '组态配置', '/page/xweb_projectconfiguration_v2/pageConfig.html#/pageConfig/ht', NULL, 'appzone', 183, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemAuthorization', 'systemManagement', 1, 4400, 1, NULL, NULL, '系统授权', '系统授权', '../system_authorization/system_authorization.html', NULL, 'appzone', 156, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/xitongshouquan.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemLog', 'systemManagement', 1, 4200, 1, NULL, NULL, '系统日志', '系统日志', '', NULL, 'appzone', 161, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/xitongrizhi.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemLog.linkageRecord', 'systemManagement.systemLog', 1, 4100, 1, NULL, NULL, '联动日志', '联动日志', '../linkage_record/linkage_record.html', NULL, 'appzone', 173, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemLog.operationLog', 'systemManagement.systemLog', 1, 4200, 1, NULL, NULL, '操作日志', '操作日志', '../operation_record/operation_record.html', NULL, 'appzone', 169, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemSetting', 'systemManagement', 1, 4900, 1, NULL, NULL, '系统配置', '系统配置', '../system_configuration/system_configuration.html', NULL, 'appzone', 226, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/xitongpeizhi.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemSkinPeelerV2', 'systemManagement', 1, 4300, 1, NULL, NULL, '主题配置', '主题配置', '/page/xweb_projectconfiguration_v2/systemSkinChange.html#/systemSkinChange', NULL, 'appzone', 223, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemUpgrade', 'systemManagement', 1, 4600, 1, NULL, NULL, '系统升级', '系统升级', '', NULL, 'appzone', 231, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/xitongshengji.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemUpgrade.systemUpgrade', 'systemManagement.systemUpgrade', 1, 5000, 1, NULL, NULL, '系统升级', '系统升级', '../upgrade/upgrade.html', NULL, 'appzone', 242, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('systemManagement.systemUpgrade.upgradeRecord', 'systemManagement.systemUpgrade', 1, 4900, 1, NULL, NULL, '升级记录', '升级记录', '../upgrade_record/upgrade_record.html', NULL, 'appzone', 234, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '', '');
INSERT INTO `itom_base_menu_v2` VALUES ('xbTool', 'projectConfiguration', 1, 4400, 1, NULL, NULL, '工程组态', '工程组态', '../engineering/engineering.html', NULL, 'appzone', 328, 10, '{\"icon\":\"\",\"mode\":0,\"type\":0,\"out_link\":0,\"change_title\":0}', '', '/api/v2/files/xmenu/gongchengzutai.png', '');
INSERT INTO `itom_base_menu_v2` VALUES ('{0000002F-0000-0000-C000-000000000050}', NULL, 1, 1, 1, NULL, NULL, 'ROOT', 'ROOT', '', NULL, 'appzone', NULL, 10, '{\"icon\":\"\",\"mode\":0,\"type\":1,\"out_link\":0,\"change_title\":0}', '', '', '');

-- ----------------------------
-- Table structure for itom_base_operations
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_operations`;
CREATE TABLE `itom_base_operations`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `pid` bigint NULL DEFAULT 0,
  `module` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '所属模块',
  `title` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '操作项名称',
  `resource` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '资源路径',
  `remark` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' COMMENT '备注',
  `row_status` int NOT NULL DEFAULT 10 COMMENT '删除标识符',
  `version` int NOT NULL DEFAULT 0 COMMENT '版本控制位',
  `comment` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `stoppage` int NULL DEFAULT 10,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_module`(`module` ASC) USING BTREE,
  INDEX `idx_title`(`title` ASC) USING BTREE,
  INDEX `idx_resource`(`resource` ASC) USING BTREE,
  INDEX `idx_remark`(`remark` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1110313010 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of itom_base_operations
-- ----------------------------
INSERT INTO `itom_base_operations` VALUES (-2, -1, '告警查询', 'reportForm:commonForm:warnSearchOther', 'reportForm/commonForm/warnSearchOther', 'warnSearchOther', 10, 2, '操作项', 10);
INSERT INTO `itom_base_operations` VALUES (1, 0, '操作项', 'operation:home', 'operation/home', 'operationItem', 10, 2, '操作项', 10);
INSERT INTO `itom_base_operations` VALUES (6, 1, '', 'historyEvent', 'historyEvent', '历史告警&事件操作', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (7, 6, '', 'historyEvent:gatherAlarmAssign', 'historyEvent\\gatherAlarmAssign', '告警合并派单', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (12, 6, '', 'historyEvent:eventConfirm', 'historyEvent\\eventConfirm', '事件确认', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (18, 6, '', 'historyEvent:eventExport', 'historyEvent\\eventExport', '告警导出', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (26, 6, '', 'historyEvent:alarmConfirm', 'historyEvent\\alarmConfirm', '告警确认', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (36, 6, '', 'historyEvent:alarmAccept', 'historyEvent\\alarmAccept', '告警受理', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (44, 6, '', 'historyEvent:alarmAssign', 'historyEvent\\alarmAssign', '告警单独派单', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (45, 1, '', 'realEvent', 'realEvent', '实时告警&事件操作', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (50, 45, '', 'realEvent:alarmConfirm', 'realEvent\\alarmConfirm', '告警确认', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (55, 45, '', 'realEvent:gatherAlarmAssign', 'realEvent\\gatherAlarmAssign', '告警合并派单', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (62, 45, '', 'realEvent:alarmGroup', 'realEvent\\alarmGroup', '告警分组类型', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (68, 45, '', 'realEvent:alarmAccept', 'realEvent\\alarmAccept', '告警受理', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (74, 45, '', 'realEvent:eventConfirm', 'realEvent\\eventConfirm', '事件确认', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (75, 45, '', 'realEvent:alarmAssign', 'realEvent\\alarmAssign', '告警单独派单', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (78, 1, '', 'device:home', 'device\\home', '空间设备操作', 10, 0, '空间设备权限', 10);
INSERT INTO `itom_base_operations` VALUES (81, 78, '', 'device:ctrl', 'device\\ctrl', '资源权限', 10, 0, '空间设备权限', 10);
INSERT INTO `itom_base_operations` VALUES (87, 6, '', 'historyEvent:quicklyFilter', 'historyEvent\\quicklyFilter', '告警快捷屏蔽', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (94, 6, '', 'historyEvent:alarmView', 'historyEvent\\alarmView', '告警视图类型', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (104, 6, '', 'historyEvent:manualRecover', 'historyEvent\\manualRecover', '告警手动恢复', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (110, 45, '', 'realEvent:quicklyFilter', 'realEvent\\quicklyFilter', '告警快捷屏蔽', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (114, 45, '', 'realEvent:manualRecover', 'realEvent\\manualRecover', '告警手动恢复', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations` VALUES (118, 1, '', 'projectConfiguration:hardwareConfiguration:portWorkPattern', 'projectConfiguration\\hardwareConfiguration\\portWorkPattern', '端口工作模式', 10, 0, '端口工作模式', 10);
INSERT INTO `itom_base_operations` VALUES (119, 1, '', 'notice', 'notice', '通知公告', 10, 0, '通知公告', 10);
INSERT INTO `itom_base_operations` VALUES (125, 119, '', 'message_center:notice:send', 'message_center\\notice\\send', '已发公告', 10, 0, '已发公告', 10);
INSERT INTO `itom_base_operations` VALUES (133, 119, '', 'message_center:notice:setting', 'message_center\\notice\\setting', '公告配置', 10, 0, '公告配置', 10);
INSERT INTO `itom_base_operations` VALUES (134, 119, '', 'message_center:notice:create', 'message_center\\notice\\create', '创建公告', 10, 0, '创建公告', 10);
INSERT INTO `itom_base_operations` VALUES (142, 119, '', 'message_center:notice:all', 'message_center\\notice\\all', '全部公告', 10, 0, '全部公告', 10);
INSERT INTO `itom_base_operations` VALUES (152, 119, '', 'message_center:notice:draft', 'message_center\\notice\\draft', '草稿箱', 10, 0, '草稿箱', 10);
INSERT INTO `itom_base_operations` VALUES (162, 119, '', 'message_center:notice:list', 'message_center\\notice\\list', '公告总览', 10, 0, '公告总览', 10);
INSERT INTO `itom_base_operations` VALUES (167, 1, '', 'message', 'message', '系统消息', 10, 0, '系统消息', 10);
INSERT INTO `itom_base_operations` VALUES (169, 167, '', 'message_center:message:setting', 'message_center\\message\\setting', '消息配置', 10, 0, '消息配置', 10);
INSERT INTO `itom_base_operations` VALUES (179, 167, '', 'message_center:message:list', 'message_center\\message\\list', '消息总览', 10, 0, '消息总览', 10);
INSERT INTO `itom_base_operations` VALUES (185, 1, '', 'xbTool', 'xbTool', '工程组态', 10, 0, '工程组态', 10);
INSERT INTO `itom_base_operations` VALUES (186, 1, '', '{0000002F-0000-0000-C000-000000000047}', '{0000002F-0000-0000-C000-000000000047}', '快速访问', 10, 0, '快速访问', 10);
INSERT INTO `itom_base_operations` VALUES (196, 1, '', 'reportForm', 'reportForm', '报表报告', 10, 0, '报表报告', 10);
INSERT INTO `itom_base_operations` VALUES (201, 196, '', 'reportForm:taskManagement', 'reportForm\\taskManagement', '任务管理', 10, 0, '任务管理', 10);
INSERT INTO `itom_base_operations` VALUES (207, 196, '', 'reportForm:templateManagement', 'reportForm\\templateManagement', '模板管理', 10, 0, '模板管理', 10);
INSERT INTO `itom_base_operations` VALUES (211, 196, '', 'reportForm:reportOverview', 'reportForm\\reportOverview', '报表总览', 10, 0, '报表总览', 10);
INSERT INTO `itom_base_operations` VALUES (240, 196, '', 'reportForm:commonForm', 'reportForm\\commonForm', '数据查询', 10, 0, '数据查询', 10);
INSERT INTO `itom_base_operations` VALUES (245, 240, '', 'reportForm:commonForm:dataSearch', 'reportForm\\commonForm\\dataSearch', '参数查询', 10, 0, '参数查询', 10);
INSERT INTO `itom_base_operations` VALUES (253, 240, '', 'reportForm:commonForm:querySchemeConfig', 'reportForm\\commonForm\\querySchemeConfig', '查询方案配置', 10, 0, '查询方案配置', 10);
INSERT INTO `itom_base_operations` VALUES (254, 240, '', 'energyPowerMonitor:originDataQuery', 'energyPowerMonitor\\originDataQuery', '原始数据查询', 10, 0, '原始数据查询', 10);
INSERT INTO `itom_base_operations` VALUES (262, 240, '', 'reportForm:commonForm:instantQuery', 'reportForm\\commonForm\\instantQuery', '即时查询', 10, 0, '即时查询', 10);
INSERT INTO `itom_base_operations` VALUES (268, 240, '', 'energyPowerMonitor:extremeDataQuery', 'energyPowerMonitor\\extremeDataQuery', '运行极值查询', 10, 0, '运行极值查询', 10);
INSERT INTO `itom_base_operations` VALUES (269, 240, '', 'reportForm:commonForm:warnSearchV2', 'reportForm\\commonForm\\warnSearchV2', '告警查询', 10, 0, '告警查询', 10);
INSERT INTO `itom_base_operations` VALUES (283, 1, '', 'systemManagement', 'systemManagement', '系统管理', 10, 0, '系统管理', 10);
INSERT INTO `itom_base_operations` VALUES (293, 283, '', 'systemManagement:backupRestore', 'systemManagement\\backupRestore', '备份还原', 10, 0, '备份还原', 10);
INSERT INTO `itom_base_operations` VALUES (299, 293, '', 'systemManagement:backupRestore:pageBackupRestore', 'systemManagement\\backupRestore\\pageBackupRestore', '页面备份还原', 10, 0, '页面备份还原', 10);
INSERT INTO `itom_base_operations` VALUES (306, 293, '', 'systemManagement:backupRestore:autoBackup', 'systemManagement\\backupRestore\\autoBackup', '自动备份配置', 10, 0, '自动备份配置', 10);
INSERT INTO `itom_base_operations` VALUES (308, 293, '', 'systemManagement:backupRestore:systemBackupRestore', 'systemManagement\\backupRestore\\systemBackupRestore', '系统备份还原', 10, 0, '系统备份还原', 10);
INSERT INTO `itom_base_operations` VALUES (312, 283, '', 'systemManagement:viewManagement', 'systemManagement\\viewManagement', '视图管理', 10, 0, '视图管理', 10);
INSERT INTO `itom_base_operations` VALUES (321, 312, '', 'viewManagement:logicManagement', 'viewManagement\\logicManagement', '逻辑视图', 10, 0, '逻辑视图', 10);
INSERT INTO `itom_base_operations` VALUES (322, 283, '', 'systemManagement:systemAuthorization', 'systemManagement\\systemAuthorization', '系统授权', 10, 0, '系统授权', 10);
INSERT INTO `itom_base_operations` VALUES (327, 283, '', 'systemManagement:systemSetting', 'systemManagement\\systemSetting', '系统配置', 10, 0, '系统配置', 10);
INSERT INTO `itom_base_operations` VALUES (329, 283, '', 'systemManagement:systemLog', 'systemManagement\\systemLog', '系统日志', 10, 0, '系统日志', 10);
INSERT INTO `itom_base_operations` VALUES (333, 329, '', 'systemManagement:systemLog:operationLog', 'systemManagement\\systemLog\\operationLog', '操作日志', 10, 0, '操作日志', 10);
INSERT INTO `itom_base_operations` VALUES (337, 329, '', 'systemManagement:systemLog:linkageRecord', 'systemManagement\\systemLog\\linkageRecord', '联动日志', 10, 0, '联动日志', 10);
INSERT INTO `itom_base_operations` VALUES (338, 283, '', 'systemManagement:systemUpgrade', 'systemManagement\\systemUpgrade', '系统升级', 10, 0, '系统升级', 10);
INSERT INTO `itom_base_operations` VALUES (347, 338, '', 'systemManagement:systemUpgrade:systemUpgrade', 'systemManagement\\systemUpgrade\\systemUpgrade', '系统升级', 10, 0, '系统升级', 10);
INSERT INTO `itom_base_operations` VALUES (352, 338, '', 'systemManagement:systemUpgrade:upgradeRecord', 'systemManagement\\systemUpgrade\\upgradeRecord', '升级记录', 10, 0, '升级记录', 10);
INSERT INTO `itom_base_operations` VALUES (361, 283, '', 'systemManagement:authorityManagement', 'systemManagement\\authorityManagement', '权限管理', 10, 0, '权限管理', 10);
INSERT INTO `itom_base_operations` VALUES (371, 361, '', 'systemManagement:authorityManagement:departmentManagement', 'systemManagement\\authorityManagement\\departmentManagement', '部门管理', 10, 0, '部门管理', 10);
INSERT INTO `itom_base_operations` VALUES (379, 361, '', 'systemManagement:authorityManagement:userManagement', 'systemManagement\\authorityManagement\\userManagement', '用户管理', 10, 0, '用户管理', 10);
INSERT INTO `itom_base_operations` VALUES (382, 361, '', 'systemManagement:authorityManagement:timePeriodManagement', 'systemManagement\\authorityManagement\\timePeriodManagement', '时间段管理', 10, 0, '时间段管理', 10);
INSERT INTO `itom_base_operations` VALUES (385, 361, '', 'systemManagement:authorityManagement:accountRecycle', 'systemManagement\\authorityManagement\\accountRecycle', '已删除用户', 10, 0, '已删除用户', 10);
INSERT INTO `itom_base_operations` VALUES (389, 361, '', 'systemManagement:authorityManagement:ipWhiteList', 'systemManagement\\authorityManagement\\ipWhiteList', 'IP白名单', 10, 0, 'IP白名单', 10);
INSERT INTO `itom_base_operations` VALUES (399, 361, '', 'systemManagement:authorityManagement:onlineUser', 'systemManagement\\authorityManagement\\onlineUser', '在线用户', 10, 0, '在线用户', 10);
INSERT INTO `itom_base_operations` VALUES (406, 361, '', 'systemManagement:authorityManagement:roleManagement', 'systemManagement\\authorityManagement\\roleManagement', '角色管理', 10, 0, '角色管理', 10);
INSERT INTO `itom_base_operations` VALUES (407, 283, '', 'systemManagement:systemSkinPeelerV2', 'systemManagement\\systemSkinPeelerV2', '主题配置', 10, 0, '主题配置', 10);
INSERT INTO `itom_base_operations` VALUES (416, 283, '', 'systemManagement:pageSettingV2', 'systemManagement\\pageSettingV2', '页面配置', 10, 0, '页面配置', 10);
INSERT INTO `itom_base_operations` VALUES (423, 416, '', 'systemManagement:pageSettingV2:login', 'systemManagement\\pageSettingV2\\login', '登录页配置', 10, 0, '登录页配置', 10);
INSERT INTO `itom_base_operations` VALUES (432, 416, '', 'systemManagement:pageSettingV2:homepage', 'systemManagement\\pageSettingV2\\homepage', '首页配置', 10, 0, '首页配置', 10);
INSERT INTO `itom_base_operations` VALUES (442, 416, '', 'systemManagement:pageSettingV2:view', 'systemManagement\\pageSettingV2\\view', '组态配置', 10, 0, '组态配置', 10);
INSERT INTO `itom_base_operations` VALUES (446, 416, '', 'systemManagement:pageSettingV2:style', 'systemManagement\\pageSettingV2\\style', '风格配置', 10, 0, '风格配置', 10);
INSERT INTO `itom_base_operations` VALUES (449, 1, '', 'dataQuality', 'dataQuality', '数据质量', 10, 0, '数据质量', 10);
INSERT INTO `itom_base_operations` VALUES (453, 449, '', 'dataQuality:exceptionList', 'dataQuality\\exceptionList', '数据质量异常列表', 10, 0, '数据质量异常列表', 10);
INSERT INTO `itom_base_operations` VALUES (458, 449, '', 'dataQuality:alarmConfig', 'dataQuality\\alarmConfig', '数据质量告警配置', 10, 0, '数据质量告警配置', 10);
INSERT INTO `itom_base_operations` VALUES (467, 1, '', 'monitorInfo', 'monitorInfo', '设施监控', 10, 0, '设施监控', 10);
INSERT INTO `itom_base_operations` VALUES (472, 1, '', 'projectConfiguration', 'projectConfiguration', '工程配置', 10, 0, '工程配置', 10);
INSERT INTO `itom_base_operations` VALUES (481, 472, '', 'projectConfiguration:storageSetting', 'projectConfiguration\\storageSetting', '存储配置', 10, 0, '存储配置', 10);
INSERT INTO `itom_base_operations` VALUES (488, 481, '', 'projectConfiguration:storageSetting:storageEngine', 'projectConfiguration\\storageSetting\\storageEngine', '存储引擎', 10, 0, '存储引擎', 10);
INSERT INTO `itom_base_operations` VALUES (498, 481, '', 'projectConfiguration:storageSetting:storagePolicy', 'projectConfiguration\\storageSetting\\storagePolicy', '存储策略', 10, 0, '存储策略', 10);
INSERT INTO `itom_base_operations` VALUES (504, 481, '', 'reportForm:storageStatistics', 'reportForm\\storageStatistics', '存储总览', 10, 0, '存储总览', 10);
INSERT INTO `itom_base_operations` VALUES (510, 472, '', 'projectConfiguration:alarmDescription', 'projectConfiguration\\alarmDescription', '告警描述配置', 10, 0, '告警描述配置', 10);
INSERT INTO `itom_base_operations` VALUES (519, 472, '', 'projectConfiguration:alarmMasking', 'projectConfiguration\\alarmMasking', '告警屏蔽', 10, 0, '告警屏蔽', 10);
INSERT INTO `itom_base_operations` VALUES (521, 519, '', 'projectConfiguration:alarmMasking:timeManagement', 'projectConfiguration\\alarmMasking\\timeManagement', '时段管理', 10, 0, '时段管理', 10);
INSERT INTO `itom_base_operations` VALUES (531, 519, '', 'projectConfiguration:alarmMasking:blockingRules', 'projectConfiguration\\alarmMasking\\blockingRules', '屏蔽规则', 10, 0, '屏蔽规则', 10);
INSERT INTO `itom_base_operations` VALUES (536, 472, '', 'projectConfiguration:policyLinkage', 'projectConfiguration\\policyLinkage', '联动策略', 10, 0, '联动策略', 10);
INSERT INTO `itom_base_operations` VALUES (543, 536, '', 'projectConfiguration:policyLinkage:strategyList', 'projectConfiguration\\policyLinkage\\strategyList', '后台联动', 10, 0, '后台联动', 10);
INSERT INTO `itom_base_operations` VALUES (550, 536, '', 'projectConfiguration:policyLinkage:functionModule', 'projectConfiguration\\policyLinkage\\functionModule', '函数模型', 10, 0, '函数模型', 10);
INSERT INTO `itom_base_operations` VALUES (559, 536, '', 'projectConfiguration:policyLinkage:strategyOverview', 'projectConfiguration\\policyLinkage\\strategyOverview', '策略总览', 10, 0, '策略总览', 10);
INSERT INTO `itom_base_operations` VALUES (566, 472, '', 'projectConfiguration:nussi', 'projectConfiguration\\nussi', '北向连接', 10, 0, '北向连接', 10);
INSERT INTO `itom_base_operations` VALUES (576, 472, '', 'projectConfiguration:driversManager', 'projectConfiguration\\driversManager', '驱动管理', 10, 0, '驱动管理', 10);
INSERT INTO `itom_base_operations` VALUES (578, 576, '', 'projectConfiguration:driversManager:collection', 'projectConfiguration\\driversManager\\collection', '采集组件', 10, 0, '采集组件', 10);
INSERT INTO `itom_base_operations` VALUES (587, 576, '', 'projectConfiguration:driversManager:protocolFile', 'projectConfiguration\\driversManager\\protocolFile', '协议文件', 10, 0, '协议文件', 10);
INSERT INTO `itom_base_operations` VALUES (594, 576, '', 'projectConfiguration:driversManager:devNode', 'projectConfiguration\\driversManager\\devNode', '设备点表', 10, 0, '设备点表', 10);
INSERT INTO `itom_base_operations` VALUES (603, 472, '', 'projectConfiguration:alarmSetting', 'projectConfiguration\\alarmSetting', '告警配置', 10, 0, '告警配置', 10);
INSERT INTO `itom_base_operations` VALUES (610, 472, '', 'projectConfiguration:systemTools', 'projectConfiguration\\systemTools', '系统工具', 10, 0, '系统工具', 10);
INSERT INTO `itom_base_operations` VALUES (614, 610, '', 'projectConfiguration:systemTools:serialPortTool', 'projectConfiguration\\systemTools\\serialPortTool', '串口工具', 10, 0, '串口工具', 10);
INSERT INTO `itom_base_operations` VALUES (616, 610, '', 'projectConfiguration:systemTools:networkDbugTools', 'projectConfiguration\\systemTools\\networkDbugTools', '网络调试工具', 10, 0, '网络调试工具', 10);
INSERT INTO `itom_base_operations` VALUES (622, 610, '', 'projectConfiguration:systemTools:configImportExport', 'projectConfiguration\\systemTools\\configImportExport', '告警规则管理', 10, 0, '告警规则管理', 10);
INSERT INTO `itom_base_operations` VALUES (631, 472, '', 'projectConfiguration:carousel', 'projectConfiguration\\carousel', '大屏管理', 10, 0, '大屏管理', 10);
INSERT INTO `itom_base_operations` VALUES (633, 472, '', 'projectConfiguration:alarmNotify', 'projectConfiguration\\alarmNotify', '告警告知', 10, 0, '告警告知', 10);
INSERT INTO `itom_base_operations` VALUES (643, 633, '', 'projectConfiguration:alarmNotify:messageRecordQuery', 'projectConfiguration\\alarmNotify\\messageRecordQuery', '信息查询', 10, 0, '信息查询', 10);
INSERT INTO `itom_base_operations` VALUES (653, 633, '', 'projectConfiguration:alarmNotify:alarmStrategy', 'projectConfiguration\\alarmNotify\\alarmStrategy', '告警策略', 10, 0, '告警策略', 10);
INSERT INTO `itom_base_operations` VALUES (660, 633, '', 'projectConfiguration:alarmNotify:messageRouting', 'projectConfiguration\\alarmNotify\\messageRouting', '信息发送', 10, 0, '信息发送', 10);
INSERT INTO `itom_base_operations` VALUES (665, 633, '', 'projectConfiguration:alarmNotify:stormWarning', 'projectConfiguration\\alarmNotify\\stormWarning', '风暴预警', 10, 0, '风暴预警', 10);
INSERT INTO `itom_base_operations` VALUES (667, 633, '', 'projectConfiguration:alarmNotify:messageTemplate', 'projectConfiguration\\alarmNotify\\messageTemplate', '信息模板', 10, 0, '信息模板', 10);
INSERT INTO `itom_base_operations` VALUES (670, 633, '', 'projectConfiguration:alarmNotify:regularySecurity', 'projectConfiguration\\alarmNotify\\regularySecurity', '定时报平安', 10, 0, '定时报平安', 10);
INSERT INTO `itom_base_operations` VALUES (677, 472, '', 'projectConfiguration:XECEP', 'projectConfiguration\\XECEP', '智能告警', 10, 0, '智能告警', 10);
INSERT INTO `itom_base_operations` VALUES (681, 677, '', 'projectConfiguration:XECEP:RepeatNoise', 'projectConfiguration\\XECEP\\RepeatNoise', '规则列表', 10, 0, '规则列表', 10);
INSERT INTO `itom_base_operations` VALUES (691, 677, '', 'projectConfiguration:XECEP:Custom', 'projectConfiguration\\XECEP\\Custom', 'CEP2.0', 10, 0, 'CEP2.0', 10);
INSERT INTO `itom_base_operations` VALUES (695, 270, '', 'reportForm:xbReportFormOutline:edit', 'reportForm\\xbReportFormOutline\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations` VALUES (699, 270, '', 'reportForm:xbReportFormOutline:view', 'reportForm\\xbReportFormOutline\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations` VALUES (703, 230, '', 'reportForm:xbReportFormCustom:xbReportOperateManage:edit', 'reportForm\\xbReportFormCustom\\xbReportOperateManage\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations` VALUES (709, 230, '', 'reportForm:xbReportFormCustom:xbReportOperateManage:view', 'reportForm\\xbReportFormCustom\\xbReportOperateManage\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations` VALUES (719, 276, '', 'reportForm:xbReportManage:view', 'reportForm\\xbReportManage\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations` VALUES (724, 238, '', 'reportForm:xbReportFormCustom:xbReportTemplateMan:edit', 'reportForm\\xbReportFormCustom\\xbReportTemplateMan\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations` VALUES (726, 276, '', 'reportForm:xbReportManage:edit', 'reportForm\\xbReportManage\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations` VALUES (732, 243, '', 'reportForm:commonForm:templateImmediate:edit', 'reportForm\\commonForm\\templateImmediate\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations` VALUES (736, 243, '', 'reportForm:commonForm:templateImmediate:view', 'reportForm\\commonForm\\templateImmediate\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations` VALUES (745, 119, '', '消息中心:通知公告:创建公告', '消息中心\\通知公告\\创建公告', '转发', 10, 0, '转发', 10);
INSERT INTO `itom_base_operations` VALUES (750, 543, '', 'projectConfiguration:policyLinkage:strategyList:edit', 'projectConfiguration\\policyLinkage\\strategyList\\edit', '策略编辑', 10, 0, '策略编辑', 10);
INSERT INTO `itom_base_operations` VALUES (754, 238, '', 'reportForm:xbReportFormCustom:xbReportTemplateMan:view', 'reportForm\\xbReportFormCustom\\xbReportTemplateMan\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations` VALUES (760, 1, '', '消息中心', '消息中心', '消息中心', 10, 0, '公告配置', 10);
INSERT INTO `itom_base_operations` VALUES (767, 760, '', '消息中心:通知公告:撤回公告', '消息中心\\通知公告\\撤回公告', '撤回公告', 10, 0, '公告配置', 10);
INSERT INTO `itom_base_operations` VALUES (769, 1, '', 'bf5d36594c3efcb1', 'bf5d36594c3efcb1', '门禁管理', 10, 0, '门禁管理', 10);
INSERT INTO `itom_base_operations` VALUES (772, 769, '', 'bf5d36594c3efcb1:f236b205b5fdb2e0', 'bf5d36594c3efcb1\\f236b205b5fdb2e0', '设备管理', 10, 0, '设备管理', 10);
INSERT INTO `itom_base_operations` VALUES (773, 772, '', 'bf5d36594c3efcb1:f236b205b5fdb2e0:81970ca803f32070', 'bf5d36594c3efcb1\\f236b205b5fdb2e0\\81970ca803f32070', '控制器', 10, 0, '控制器', 10);
INSERT INTO `itom_base_operations` VALUES (781, 772, '', 'bf5d36594c3efcb1:f236b205b5fdb2e0:53f20519272c8b7f', 'bf5d36594c3efcb1\\f236b205b5fdb2e0\\53f20519272c8b7f', '门管理', 10, 0, '门管理', 10);
INSERT INTO `itom_base_operations` VALUES (790, 769, '', 'bf5d36594c3efcb1:9f46c296d73593c4', 'bf5d36594c3efcb1\\9f46c296d73593c4', '门禁监控', 10, 0, '门禁监控', 10);
INSERT INTO `itom_base_operations` VALUES (792, 790, '', 'bf5d36594c3efcb1:9f46c296d73593c4:e56688ec5f14a1f7', 'bf5d36594c3efcb1\\9f46c296d73593c4\\e56688ec5f14a1f7', '告警记录', 10, 0, '告警记录', 10);
INSERT INTO `itom_base_operations` VALUES (794, 790, '', 'bf5d36594c3efcb1:9f46c296d73593c4:7b0730624e2d8018', 'bf5d36594c3efcb1\\9f46c296d73593c4\\7b0730624e2d8018', '门状态', 10, 0, '门状态', 10);
INSERT INTO `itom_base_operations` VALUES (796, 790, '', 'bf5d36594c3efcb1:9f46c296d73593c4:bf69b24de7a5bf31', 'bf5d36594c3efcb1\\9f46c296d73593c4\\bf69b24de7a5bf31', '历史记录', 10, 0, '历史记录', 10);
INSERT INTO `itom_base_operations` VALUES (805, 769, '', 'bf5d36594c3efcb1:5abdc5ed65401883', 'bf5d36594c3efcb1\\5abdc5ed65401883', '权限配置', 10, 0, '权限配置', 10);
INSERT INTO `itom_base_operations` VALUES (810, 805, '', 'bf5d36594c3efcb1:5abdc5ed65401883:96955ec871858f86', 'bf5d36594c3efcb1\\5abdc5ed65401883\\96955ec871858f86', '权限组', 10, 0, '权限组', 10);
INSERT INTO `itom_base_operations` VALUES (813, 805, '', 'bf5d36594c3efcb1:5abdc5ed65401883:341c86804b81bbb4', 'bf5d36594c3efcb1\\5abdc5ed65401883\\341c86804b81bbb4', '时间段', 10, 0, '时间段', 10);
INSERT INTO `itom_base_operations` VALUES (819, 805, '', 'bf5d36594c3efcb1:5abdc5ed65401883:ef7096b1c8f0970e', 'bf5d36594c3efcb1\\5abdc5ed65401883\\ef7096b1c8f0970e', '人员信息', 10, 0, '人员信息', 10);
INSERT INTO `itom_base_operations` VALUES (820, 805, '', 'bf5d36594c3efcb1:5abdc5ed65401883:521320fd3c9c09f7', 'bf5d36594c3efcb1\\5abdc5ed65401883\\521320fd3c9c09f7', '节假日', 10, 0, '节假日', 10);

-- ----------------------------
-- Table structure for itom_base_operations_v2
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_operations_v2`;
CREATE TABLE `itom_base_operations_v2`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `pid` bigint NULL DEFAULT 0,
  `module` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '所属模块',
  `title` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '操作项名称',
  `resource` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '资源路径',
  `remark` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT '' COMMENT '备注',
  `row_status` int NOT NULL DEFAULT 10 COMMENT '删除标识符',
  `version` int NOT NULL DEFAULT 0 COMMENT '版本控制位',
  `comment` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `stoppage` int NULL DEFAULT 10,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_module`(`module` ASC) USING BTREE,
  INDEX `idx_title`(`title` ASC) USING BTREE,
  INDEX `idx_resource`(`resource` ASC) USING BTREE,
  INDEX `idx_remark`(`remark` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1110313010 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_operations_v2
-- ----------------------------
INSERT INTO `itom_base_operations_v2` VALUES (1, 0, '操作项', 'operation:home', 'operation/home', 'operationItem', 10, 2, '操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (4, 1, '', 'realEvent', 'realEvent', '实时告警&事件操作', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (5, 4, '', 'realEvent:alarmGroup', 'realEvent\\alarmGroup', '告警分组类型', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (11, 4, '', 'realEvent:alarmAssign', 'realEvent\\alarmAssign', '告警单独派单', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (18, 4, '', 'realEvent:gatherAlarmAssign', 'realEvent\\gatherAlarmAssign', '告警合并派单', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (20, 4, '', 'realEvent:alarmConfirm', 'realEvent\\alarmConfirm', '告警确认', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (28, 4, '', 'realEvent:eventConfirm', 'realEvent\\eventConfirm', '事件确认', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (30, 4, '', 'realEvent:alarmAccept', 'realEvent\\alarmAccept', '告警受理', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (34, 1, '', 'historyEvent', 'historyEvent', '历史告警&事件操作', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (35, 34, '', 'historyEvent:alarmAssign', 'historyEvent\\alarmAssign', '告警单独派单', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (43, 34, '', 'historyEvent:gatherAlarmAssign', 'historyEvent\\gatherAlarmAssign', '告警合并派单', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (44, 34, '', 'historyEvent:alarmConfirm', 'historyEvent\\alarmConfirm', '告警确认', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (53, 34, '', 'historyEvent:eventConfirm', 'historyEvent\\eventConfirm', '事件确认', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (54, 34, '', 'historyEvent:alarmAccept', 'historyEvent\\alarmAccept', '告警受理', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (62, 34, '', 'historyEvent:eventExport', 'historyEvent\\eventExport', '告警导出', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (63, 1, '', 'device:home', 'device\\home', '空间设备操作', 10, 0, '空间设备权限', 10);
INSERT INTO `itom_base_operations_v2` VALUES (71, 63, '', 'device:ctrl', 'device\\ctrl', '资源权限', 10, 0, '空间设备权限', 10);
INSERT INTO `itom_base_operations_v2` VALUES (73, 34, '', 'historyEvent:manualRecover', 'historyEvent\\manualRecover', '告警手动恢复', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (81, 34, '', 'historyEvent:alarmView', 'historyEvent\\alarmView', '告警视图类型', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (82, 34, '', 'historyEvent:quicklyFilter', 'historyEvent\\quicklyFilter', '告警快捷屏蔽', 10, 0, '历史告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (88, 4, '', 'realEvent:manualRecover', 'realEvent\\manualRecover', '告警手动恢复', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (97, 4, '', 'realEvent:quicklyFilter', 'realEvent\\quicklyFilter', '告警快捷屏蔽', 10, 0, '实时告警新增操作项', 10);
INSERT INTO `itom_base_operations_v2` VALUES (100, 1, '', 'systemManagement', 'systemManagement', '系统管理', 10, 0, '系统管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (107, 100, '', 'systemManagement:authorityManagement', 'systemManagement\\authorityManagement', '权限管理', 10, 0, '权限管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (116, 107, '', 'systemManagement:authorityManagement:departmentManagement', 'systemManagement\\authorityManagement\\departmentManagement', '部门管理', 10, 0, '部门管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (126, 107, '', 'systemManagement:authorityManagement:accountRecycle', 'systemManagement\\authorityManagement\\accountRecycle', '已删除用户', 10, 0, '已删除用户', 10);
INSERT INTO `itom_base_operations_v2` VALUES (133, 107, '', 'systemManagement:authorityManagement:roleManagement', 'systemManagement\\authorityManagement\\roleManagement', '角色管理', 10, 0, '角色管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (135, 107, '', 'systemManagement:authorityManagement:onlineUser', 'systemManagement\\authorityManagement\\onlineUser', '在线用户', 10, 0, '在线用户', 10);
INSERT INTO `itom_base_operations_v2` VALUES (142, 107, '', 'systemManagement:authorityManagement:ipWhiteList', 'systemManagement\\authorityManagement\\ipWhiteList', 'IP白名单', 10, 0, 'IP白名单', 10);
INSERT INTO `itom_base_operations_v2` VALUES (145, 107, '', 'systemManagement:authorityManagement:timePeriodManagement', 'systemManagement\\authorityManagement\\timePeriodManagement', '时间段管理', 10, 0, '时间段管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (154, 107, '', 'systemManagement:authorityManagement:userManagement', 'systemManagement\\authorityManagement\\userManagement', '用户管理', 10, 0, '用户管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (156, 100, '', 'systemManagement:systemAuthorization', 'systemManagement\\systemAuthorization', '系统授权', 10, 0, '系统授权', 10);
INSERT INTO `itom_base_operations_v2` VALUES (161, 100, '', 'systemManagement:systemLog', 'systemManagement\\systemLog', '系统日志', 10, 0, '系统日志', 10);
INSERT INTO `itom_base_operations_v2` VALUES (169, 161, '', 'systemManagement:systemLog:operationLog', 'systemManagement\\systemLog\\operationLog', '操作日志', 10, 0, '操作日志', 10);
INSERT INTO `itom_base_operations_v2` VALUES (173, 161, '', 'systemManagement:systemLog:linkageRecord', 'systemManagement\\systemLog\\linkageRecord', '联动日志', 10, 0, '联动日志', 10);
INSERT INTO `itom_base_operations_v2` VALUES (177, 100, '', 'systemManagement:pageSettingV2', 'systemManagement\\pageSettingV2', '页面配置', 10, 0, '页面配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (183, 177, '', 'systemManagement:pageSettingV2:view', 'systemManagement\\pageSettingV2\\view', '组态配置', 10, 0, '组态配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (189, 177, '', 'systemManagement:pageSettingV2:login', 'systemManagement\\pageSettingV2\\login', '登录页配置', 10, 0, '登录页配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (195, 177, '', 'systemManagement:pageSettingV2:style', 'systemManagement\\pageSettingV2\\style', '风格配置', 10, 0, '风格配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (203, 177, '', 'systemManagement:pageSettingV2:homepage', 'systemManagement\\pageSettingV2\\homepage', '首页配置', 10, 0, '首页配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (208, 100, '', 'systemManagement:backupRestore', 'systemManagement\\backupRestore', '备份还原', 10, 0, '备份还原', 10);
INSERT INTO `itom_base_operations_v2` VALUES (210, 208, '', 'systemManagement:backupRestore:systemBackupRestore', 'systemManagement\\backupRestore\\systemBackupRestore', '系统备份还原', 10, 0, '系统备份还原', 10);
INSERT INTO `itom_base_operations_v2` VALUES (214, 208, '', 'systemManagement:backupRestore:pageBackupRestore', 'systemManagement\\backupRestore\\pageBackupRestore', '页面备份还原', 10, 0, '页面备份还原', 10);
INSERT INTO `itom_base_operations_v2` VALUES (221, 208, '', 'systemManagement:backupRestore:autoBackup', 'systemManagement\\backupRestore\\autoBackup', '自动备份配置', 10, 0, '自动备份配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (223, 100, '', 'systemManagement:systemSkinPeelerV2', 'systemManagement\\systemSkinPeelerV2', '主题配置', 10, 0, '主题配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (226, 100, '', 'systemManagement:systemSetting', 'systemManagement\\systemSetting', '系统配置', 10, 0, '系统配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (231, 100, '', 'systemManagement:systemUpgrade', 'systemManagement\\systemUpgrade', '系统升级', 10, 0, '系统升级', 10);
INSERT INTO `itom_base_operations_v2` VALUES (234, 231, '', 'systemManagement:systemUpgrade:upgradeRecord', 'systemManagement\\systemUpgrade\\upgradeRecord', '升级记录', 10, 0, '升级记录', 10);
INSERT INTO `itom_base_operations_v2` VALUES (242, 231, '', 'systemManagement:systemUpgrade:systemUpgrade', 'systemManagement\\systemUpgrade\\systemUpgrade', '系统升级', 10, 0, '系统升级', 10);
INSERT INTO `itom_base_operations_v2` VALUES (252, 1, '', 'projectConfiguration', 'projectConfiguration', '工程配置', 10, 0, '工程配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (259, 252, '', 'projectConfiguration:driversManager', 'projectConfiguration\\driversManager', '驱动管理', 10, 0, '驱动管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (269, 259, '', 'projectConfiguration:driversManager:protocolFile', 'projectConfiguration\\driversManager\\protocolFile', '协议文件', 10, 0, '协议文件', 10);
INSERT INTO `itom_base_operations_v2` VALUES (277, 259, '', 'projectConfiguration:driversManager:devNode', 'projectConfiguration\\driversManager\\devNode', '设备点表', 10, 0, '设备点表', 10);
INSERT INTO `itom_base_operations_v2` VALUES (285, 259, '', 'projectConfiguration:driversManager:collection', 'projectConfiguration\\driversManager\\collection', '采集组件', 10, 0, '采集组件', 10);
INSERT INTO `itom_base_operations_v2` VALUES (291, 252, '', 'projectConfiguration:systemTools', 'projectConfiguration\\systemTools', '系统工具', 10, 0, '系统工具', 10);
INSERT INTO `itom_base_operations_v2` VALUES (293, 291, '', 'projectConfiguration:systemTools:networkDbugTools', 'projectConfiguration\\systemTools\\networkDbugTools', '网络调试工具', 10, 0, '网络调试工具', 10);
INSERT INTO `itom_base_operations_v2` VALUES (299, 291, '', 'projectConfiguration:systemTools:serialPortTool', 'projectConfiguration\\systemTools\\serialPortTool', '串口工具', 10, 0, '串口工具', 10);
INSERT INTO `itom_base_operations_v2` VALUES (302, 291, '', 'projectConfiguration:systemTools:configImportExport', 'projectConfiguration\\systemTools\\configImportExport', '告警规则管理', 10, 0, '告警规则管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (307, 252, '', 'projectConfiguration:nussi', 'projectConfiguration\\nussi', '北向连接', 10, 0, '北向连接', 10);
INSERT INTO `itom_base_operations_v2` VALUES (317, 252, '', 'projectConfiguration:storageSetting', 'projectConfiguration\\storageSetting', '存储配置', 10, 0, '存储配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (319, 317, '', 'projectConfiguration:storageSetting:storagePolicy', 'projectConfiguration\\storageSetting\\storagePolicy', '存储策略', 10, 0, '存储策略', 10);
INSERT INTO `itom_base_operations_v2` VALUES (320, 317, '', 'projectConfiguration:storageSetting:storageEngine', 'projectConfiguration\\storageSetting\\storageEngine', '存储引擎', 10, 0, '存储引擎', 10);
INSERT INTO `itom_base_operations_v2` VALUES (324, 317, '', 'reportForm:storageStatistics', 'reportForm\\storageStatistics', '存储总览', 10, 0, '存储总览', 10);
INSERT INTO `itom_base_operations_v2` VALUES (328, 252, '', 'xbTool', 'xbTool', '工程组态', 10, 0, '工程组态', 10);
INSERT INTO `itom_base_operations_v2` VALUES (336, 252, '', 'projectConfiguration:policyLinkage', 'projectConfiguration\\policyLinkage', '联动策略', 10, 0, '联动策略', 10);
INSERT INTO `itom_base_operations_v2` VALUES (345, 336, '', 'projectConfiguration:policyLinkage:functionModule', 'projectConfiguration\\policyLinkage\\functionModule', '函数模型', 10, 0, '函数模型', 10);
INSERT INTO `itom_base_operations_v2` VALUES (355, 336, '', 'projectConfiguration:policyLinkage:strategyList', 'projectConfiguration\\policyLinkage\\strategyList', '后台联动', 10, 0, '后台联动', 10);
INSERT INTO `itom_base_operations_v2` VALUES (356, 336, '', 'projectConfiguration:policyLinkage:strategyOverview', 'projectConfiguration\\policyLinkage\\strategyOverview', '策略总览', 10, 0, '策略总览', 10);
INSERT INTO `itom_base_operations_v2` VALUES (360, 1, '', 'facilityMonitor', 'facilityMonitor', '设施监控', 10, 0, '设施监控', 10);
INSERT INTO `itom_base_operations_v2` VALUES (365, 360, '', 'facilityMonitor:alarmManagement', 'facilityMonitor\\alarmManagement', '告警管理', 10, 0, '告警管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (370, 365, '', 'projectConfiguration:alarmDescription', 'projectConfiguration\\alarmDescription', '告警描述配置', 10, 0, '告警描述配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (377, 365, '', 'projectConfiguration:alarmNotify', 'projectConfiguration\\alarmNotify', '告警告知', 10, 0, '告警告知', 10);
INSERT INTO `itom_base_operations_v2` VALUES (382, 377, '', 'projectConfiguration:alarmNotify:messageTemplate', 'projectConfiguration\\alarmNotify\\messageTemplate', '信息模板', 10, 0, '信息模板', 10);
INSERT INTO `itom_base_operations_v2` VALUES (389, 377, '', 'projectConfiguration:alarmNotify:stormWarning', 'projectConfiguration\\alarmNotify\\stormWarning', '风暴预警', 10, 0, '风暴预警', 10);
INSERT INTO `itom_base_operations_v2` VALUES (395, 377, '', 'projectConfiguration:alarmNotify:messageRouting', 'projectConfiguration\\alarmNotify\\messageRouting', '信息发送', 10, 0, '信息发送', 10);
INSERT INTO `itom_base_operations_v2` VALUES (405, 377, '', 'projectConfiguration:alarmNotify:alarmStrategy', 'projectConfiguration\\alarmNotify\\alarmStrategy', '告警策略', 10, 0, '告警策略', 10);
INSERT INTO `itom_base_operations_v2` VALUES (415, 377, '', 'projectConfiguration:alarmNotify:regularySecurity', 'projectConfiguration\\alarmNotify\\regularySecurity', '定时报平安', 10, 0, '定时报平安', 10);
INSERT INTO `itom_base_operations_v2` VALUES (425, 377, '', 'projectConfiguration:alarmNotify:messageRecordQuery', 'projectConfiguration\\alarmNotify\\messageRecordQuery', '信息查询', 10, 0, '信息查询', 10);
INSERT INTO `itom_base_operations_v2` VALUES (432, 365, '', 'reportForm:commonForm:warnSearchV2', 'reportForm\\commonForm\\warnSearchV2', '告警查询', 10, 0, '告警查询', 10);
INSERT INTO `itom_base_operations_v2` VALUES (435, 365, '', 'projectConfiguration:alarmSetting', 'projectConfiguration\\alarmSetting', '告警配置', 10, 0, '告警配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (438, 365, '', 'projectConfiguration:alarmMasking', 'projectConfiguration\\alarmMasking', '告警屏蔽', 10, 0, '告警屏蔽', 10);
INSERT INTO `itom_base_operations_v2` VALUES (442, 438, '', 'projectConfiguration:alarmMasking:timeManagement', 'projectConfiguration\\alarmMasking\\timeManagement', '时段管理', 10, 0, '时段管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (444, 438, '', 'projectConfiguration:alarmMasking:blockingRules', 'projectConfiguration\\alarmMasking\\blockingRules', '屏蔽规则', 10, 0, '屏蔽规则', 10);
INSERT INTO `itom_base_operations_v2` VALUES (448, 365, '', 'projectConfiguration:XECEP', 'projectConfiguration\\XECEP', '智能告警', 10, 0, '智能告警', 10);
INSERT INTO `itom_base_operations_v2` VALUES (458, 448, '', 'projectConfiguration:XECEP:RepeatNoise', 'projectConfiguration\\XECEP\\RepeatNoise', '规则列表', 10, 0, '规则列表', 10);
INSERT INTO `itom_base_operations_v2` VALUES (463, 448, '', 'projectConfiguration:XECEP:Custom', 'projectConfiguration\\XECEP\\Custom', 'CEP2.0', 10, 0, 'CEP2.0', 10);
INSERT INTO `itom_base_operations_v2` VALUES (467, 360, '', 'monitorInfo', 'monitorInfo', '基础设施监控', 10, 0, '基础设施监控', 10);
INSERT INTO `itom_base_operations_v2` VALUES (473, 360, '', 'projectConfiguration:carousel', 'projectConfiguration\\carousel', '大屏监控', 10, 0, '大屏监控', 10);
INSERT INTO `itom_base_operations_v2` VALUES (483, 1, '', 'notice', 'notice', '通知公告', 10, 0, '通知公告', 10);
INSERT INTO `itom_base_operations_v2` VALUES (484, 483, '', 'message_center:notice:list', 'message_center\\notice\\list', '公告总览', 10, 0, '公告总览', 10);
INSERT INTO `itom_base_operations_v2` VALUES (486, 483, '', 'message_center:notice:send', 'message_center\\notice\\send', '已发公告', 10, 0, '已发公告', 10);
INSERT INTO `itom_base_operations_v2` VALUES (493, 483, '', 'message_center:notice:all', 'message_center\\notice\\all', '全部公告', 10, 0, '全部公告', 10);
INSERT INTO `itom_base_operations_v2` VALUES (494, 483, '', 'message_center:notice:setting', 'message_center\\notice\\setting', '公告配置', 10, 0, '公告配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (503, 483, '', 'message_center:notice:create', 'message_center\\notice\\create', '创建公告', 10, 0, '创建公告', 10);
INSERT INTO `itom_base_operations_v2` VALUES (507, 483, '', 'message_center:notice:draft', 'message_center\\notice\\draft', '草稿箱', 10, 0, '草稿箱', 10);
INSERT INTO `itom_base_operations_v2` VALUES (512, 1, '', 'message', 'message', '系统消息', 10, 0, '系统消息', 10);
INSERT INTO `itom_base_operations_v2` VALUES (516, 512, '', 'message_center:message:setting', 'message_center\\message\\setting', '消息配置', 10, 0, '消息配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (522, 512, '', 'message_center:message:list', 'message_center\\message\\list', '消息总览', 10, 0, '消息总览', 10);
INSERT INTO `itom_base_operations_v2` VALUES (530, 1, '', 'dataAnalysis', 'dataAnalysis', '数据分析', 10, 0, '数据分析', 10);
INSERT INTO `itom_base_operations_v2` VALUES (540, 530, '', 'reportForm', 'reportForm', '报表报告', 10, 0, '报表报告', 10);
INSERT INTO `itom_base_operations_v2` VALUES (545, 540, '', 'reportForm:taskManagement', 'reportForm\\taskManagement', '任务管理', 10, 0, '任务管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (554, 540, '', 'reportForm:templateManagement', 'reportForm\\templateManagement', '模板管理', 10, 0, '模板管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (563, 540, '', 'reportForm:commonForm', 'reportForm\\commonForm', '数据查询', 10, 0, '数据查询', 10);
INSERT INTO `itom_base_operations_v2` VALUES (569, 563, '', 'reportForm:commonForm:instantQuery', 'reportForm\\commonForm\\instantQuery', '即时查询', 10, 0, '即时查询', 10);
INSERT INTO `itom_base_operations_v2` VALUES (575, 563, '', 'reportForm:commonForm:dataSearch', 'reportForm\\commonForm\\dataSearch', '参数查询', 10, 0, '参数查询', 10);
INSERT INTO `itom_base_operations_v2` VALUES (579, 563, '', 'reportForm:commonForm:querySchemeConfig', 'reportForm\\commonForm\\querySchemeConfig', '查询方案配置', 10, 0, '查询方案配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (583, 563, '', 'energyPowerMonitor:originDataQuery', 'energyPowerMonitor\\originDataQuery', '原始数据查询', 10, 0, '原始数据查询', 10);
INSERT INTO `itom_base_operations_v2` VALUES (600, 563, '', 'reportForm:commonForm:warnSearchOtherV2', 'reportForm\\commonForm\\warnSearchOtherV2', '告警查询', 10, 0, '告警查询', 10);
INSERT INTO `itom_base_operations_v2` VALUES (603, 563, '', 'energyPowerMonitor:extremeDataQuery', 'energyPowerMonitor\\extremeDataQuery', '运行极值查询', 10, 0, '运行极值查询', 10);
INSERT INTO `itom_base_operations_v2` VALUES (623, 540, '', 'reportForm:reportOverview', 'reportForm\\reportOverview', '报表总览', 10, 0, '报表总览', 10);
INSERT INTO `itom_base_operations_v2` VALUES (636, 530, '', 'dataQuality', 'dataQuality', '数据质量', 10, 0, '数据质量', 10);
INSERT INTO `itom_base_operations_v2` VALUES (639, 636, '', 'dataQuality:alarmConfig', 'dataQuality\\alarmConfig', '数据质量告警配置', 10, 0, '数据质量告警配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (647, 636, '', 'dataQuality:exceptionList', 'dataQuality\\exceptionList', '数据质量异常列表', 10, 0, '数据质量异常列表', 10);
INSERT INTO `itom_base_operations_v2` VALUES (652, 1, '', 'integratedSystem', 'integratedSystem', '集成系统', 10, 0, '集成系统', 10);
INSERT INTO `itom_base_operations_v2` VALUES (662, 630, '', 'reportForm:xbReportManage:edit', 'reportForm\\xbReportManage\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations_v2` VALUES (669, 355, '', 'projectConfiguration:policyLinkage:strategyList:edit', 'projectConfiguration\\policyLinkage\\strategyList\\edit', '策略编辑', 10, 0, '策略编辑', 10);
INSERT INTO `itom_base_operations_v2` VALUES (674, 607, '', 'reportForm:xbReportFormCustom:xbReportOperateManage:view', 'reportForm\\xbReportFormCustom\\xbReportOperateManage\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations_v2` VALUES (676, 630, '', 'reportForm:xbReportManage:view', 'reportForm\\xbReportManage\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations_v2` VALUES (680, 590, '', 'reportForm:commonForm:templateImmediate:edit', 'reportForm\\commonForm\\templateImmediate\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations_v2` VALUES (681, 360, '', '消息中心', '消息中心', '消息中心', 10, 0, '公告配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (689, 681, '', '消息中心:通知公告:撤回公告', '消息中心\\通知公告\\撤回公告', '撤回公告', 10, 0, '公告配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (694, 541, '', 'reportForm:xbReportFormOutline:edit', 'reportForm\\xbReportFormOutline\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations_v2` VALUES (695, 607, '', 'reportForm:xbReportFormCustom:xbReportOperateManage:edit', 'reportForm\\xbReportFormCustom\\xbReportOperateManage\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations_v2` VALUES (703, 615, '', 'reportForm:xbReportFormCustom:xbReportTemplateMan:edit', 'reportForm\\xbReportFormCustom\\xbReportTemplateMan\\edit', '编辑', 10, 0, 'edit', 10);
INSERT INTO `itom_base_operations_v2` VALUES (711, 615, '', 'reportForm:xbReportFormCustom:xbReportTemplateMan:view', 'reportForm\\xbReportFormCustom\\xbReportTemplateMan\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations_v2` VALUES (719, 483, '', '消息中心:通知公告:创建公告', '消息中心\\通知公告\\创建公告', '转发', 10, 0, '转发', 10);
INSERT INTO `itom_base_operations_v2` VALUES (725, 541, '', 'reportForm:xbReportFormOutline:view', 'reportForm\\xbReportFormOutline\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations_v2` VALUES (732, 590, '', 'reportForm:commonForm:templateImmediate:view', 'reportForm\\commonForm\\templateImmediate\\view', '查看', 10, 0, 'view', 10);
INSERT INTO `itom_base_operations_v2` VALUES (735, 360, '', 'bf5d36594c3efcb1', 'bf5d36594c3efcb1', '门禁管理', 10, 0, '门禁管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (737, 735, '', 'bf5d36594c3efcb1:5abdc5ed65401883', 'bf5d36594c3efcb1\\5abdc5ed65401883', '权限配置', 10, 0, '权限配置', 10);
INSERT INTO `itom_base_operations_v2` VALUES (745, 737, '', 'bf5d36594c3efcb1:5abdc5ed65401883:ef7096b1c8f0970e', 'bf5d36594c3efcb1\\5abdc5ed65401883\\ef7096b1c8f0970e', '人员信息', 10, 0, '人员信息', 10);
INSERT INTO `itom_base_operations_v2` VALUES (754, 737, '', 'bf5d36594c3efcb1:5abdc5ed65401883:341c86804b81bbb4', 'bf5d36594c3efcb1\\5abdc5ed65401883\\341c86804b81bbb4', '时间段', 10, 0, '时间段', 10);
INSERT INTO `itom_base_operations_v2` VALUES (755, 737, '', 'bf5d36594c3efcb1:5abdc5ed65401883:96955ec871858f86', 'bf5d36594c3efcb1\\5abdc5ed65401883\\96955ec871858f86', '权限组', 10, 0, '权限组', 10);
INSERT INTO `itom_base_operations_v2` VALUES (757, 737, '', 'bf5d36594c3efcb1:5abdc5ed65401883:521320fd3c9c09f7', 'bf5d36594c3efcb1\\5abdc5ed65401883\\521320fd3c9c09f7', '节假日', 10, 0, '节假日', 10);
INSERT INTO `itom_base_operations_v2` VALUES (761, 735, '', 'bf5d36594c3efcb1:9f46c296d73593c4', 'bf5d36594c3efcb1\\9f46c296d73593c4', '门禁监控', 10, 0, '门禁监控', 10);
INSERT INTO `itom_base_operations_v2` VALUES (770, 761, '', 'bf5d36594c3efcb1:9f46c296d73593c4:e56688ec5f14a1f7', 'bf5d36594c3efcb1\\9f46c296d73593c4\\e56688ec5f14a1f7', '告警记录', 10, 0, '告警记录', 10);
INSERT INTO `itom_base_operations_v2` VALUES (775, 761, '', 'bf5d36594c3efcb1:9f46c296d73593c4:7b0730624e2d8018', 'bf5d36594c3efcb1\\9f46c296d73593c4\\7b0730624e2d8018', '门状态', 10, 0, '门状态', 10);
INSERT INTO `itom_base_operations_v2` VALUES (782, 761, '', 'bf5d36594c3efcb1:9f46c296d73593c4:bf69b24de7a5bf31', 'bf5d36594c3efcb1\\9f46c296d73593c4\\bf69b24de7a5bf31', '历史记录', 10, 0, '历史记录', 10);
INSERT INTO `itom_base_operations_v2` VALUES (785, 735, '', 'bf5d36594c3efcb1:f236b205b5fdb2e0', 'bf5d36594c3efcb1\\f236b205b5fdb2e0', '设备管理', 10, 0, '设备管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (792, 785, '', 'bf5d36594c3efcb1:f236b205b5fdb2e0:53f20519272c8b7f', 'bf5d36594c3efcb1\\f236b205b5fdb2e0\\53f20519272c8b7f', '门管理', 10, 0, '门管理', 10);
INSERT INTO `itom_base_operations_v2` VALUES (796, 785, '', 'bf5d36594c3efcb1:f236b205b5fdb2e0:81970ca803f32070', 'bf5d36594c3efcb1\\f236b205b5fdb2e0\\81970ca803f32070', '控制器', 10, 0, '控制器', 10);

-- ----------------------------
-- Table structure for itom_base_periods
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_periods`;
CREATE TABLE `itom_base_periods`  (
  `id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `name` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `year_from` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `year_end` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `month_from` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `month_end` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `day_from` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `day_end` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `weekday_from` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `weekday_end` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `time_from` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  `time_end` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `name`(`name` ASC) USING BTREE,
  UNIQUE INDEX `id`(`id` ASC) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_periods
-- ----------------------------

-- ----------------------------
-- Table structure for itom_base_role_menu_collect
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_role_menu_collect`;
CREATE TABLE `itom_base_role_menu_collect`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `account` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT ' ' COMMENT '用户',
  `menu_id` varchar(256) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '菜单ID',
  `create_time` datetime NOT NULL COMMENT '创建时间',
  `update_time` datetime NOT NULL COMMENT '删除时间',
  `deleted` tinyint NOT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 205 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_role_menu_collect
-- ----------------------------
INSERT INTO `itom_base_role_menu_collect` VALUES (7, 'admin', 'dataQuality', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (8, 'admin', 'dataQuality.alarmConfig', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (9, 'admin', 'dataQuality.exceptionList', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (10, 'admin', 'energyPowerMonitor.extremeDataQuery', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (11, 'admin', 'energyPowerMonitor.originDataQuery', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (12, 'admin', 'facilityMonitor.alarmManagement', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (13, 'admin', 'message', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (14, 'admin', 'message_center', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (15, 'admin', 'message_center.message.list', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (16, 'admin', 'message_center.message.setting', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (17, 'admin', 'message_center.notice.all', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (18, 'admin', 'message_center.notice.create', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (19, 'admin', 'message_center.notice.draft', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (20, 'admin', 'message_center.notice.list', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (21, 'admin', 'message_center.notice.send', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (22, 'admin', 'message_center.notice.setting', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (23, 'admin', 'monitorInfo', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (24, 'admin', 'notice', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (25, 'admin', 'projectConfiguration.alarmDescription', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (26, 'admin', 'projectConfiguration.alarmMasking', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (27, 'admin', 'projectConfiguration.alarmMasking.blockingRules', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (28, 'admin', 'projectConfiguration.alarmMasking.timeManagement', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (29, 'admin', 'projectConfiguration.alarmNotify', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (30, 'admin', 'projectConfiguration.alarmNotify.alarmStrategy', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (31, 'admin', 'projectConfiguration.alarmNotify.messageRecordQuery', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (32, 'admin', 'projectConfiguration.alarmNotify.messageRouting', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (33, 'admin', 'projectConfiguration.alarmNotify.messageTemplate', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (34, 'admin', 'projectConfiguration.alarmNotify.regularySecurity', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (35, 'admin', 'projectConfiguration.alarmNotify.stormWarning', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (36, 'admin', 'projectConfiguration.alarmSetting', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (37, 'admin', 'projectConfiguration.carousel', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (38, 'admin', 'projectConfiguration.driversManager', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (39, 'admin', 'projectConfiguration.driversManager.collection', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (40, 'admin', 'projectConfiguration.driversManager.devNode', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (41, 'admin', 'projectConfiguration.driversManager.protocolFile', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (42, 'admin', 'projectConfiguration.nussi', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (43, 'admin', 'projectConfiguration.policyLinkage', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (44, 'admin', 'projectConfiguration.policyLinkage.functionModule', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (45, 'admin', 'projectConfiguration.policyLinkage.strategyList', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (46, 'admin', 'projectConfiguration.policyLinkage.strategyOverview', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (47, 'admin', 'projectConfiguration.storageSetting', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (48, 'admin', 'projectConfiguration.storageSetting.storageEngine', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (49, 'admin', 'projectConfiguration.storageSetting.storagePolicy', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (50, 'admin', 'projectConfiguration.systemTools', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (51, 'admin', 'projectConfiguration.systemTools.configImportExport', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (52, 'admin', 'projectConfiguration.systemTools.networkDbugTools', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (53, 'admin', 'projectConfiguration.systemTools.serialPortTool', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (54, 'admin', 'projectConfiguration.XECEP', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (55, 'admin', 'projectConfiguration.XECEP.Custom', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (56, 'admin', 'projectConfiguration.XECEP.RepeatNoise', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (57, 'admin', 'reportForm', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (58, 'admin', 'reportForm.commonForm', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (59, 'admin', 'reportForm.commonForm.dataSearch', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (60, 'admin', 'reportForm.commonForm.instantQuery', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (61, 'admin', 'reportForm.commonForm.querySchemeConfig', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (62, 'admin', 'reportForm.commonForm.templateImmediate', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (63, 'admin', 'reportForm.commonForm.warnSearchOtherV2', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (64, 'admin', 'reportForm.commonForm.warnSearchV2', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (65, 'admin', 'reportForm.reportOverview', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (66, 'admin', 'reportForm.storageStatistics', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (67, 'admin', 'reportForm.taskManagement', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (68, 'admin', 'reportForm.templateManagement', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (69, 'admin', 'reportForm.xbReportFormCustom', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (70, 'admin', 'reportForm.xbReportFormCustom.xbReportOperateManage', '2025-12-18 16:06:54', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (71, 'admin', 'reportForm.xbReportFormCustom.xbReportTemplateMan', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (72, 'admin', 'reportForm.xbReportFormOutline', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (73, 'admin', 'reportForm.xbReportManage', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (74, 'admin', 'systemManagement.authorityManagement', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (75, 'admin', 'systemManagement.authorityManagement.accountRecycle', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (76, 'admin', 'systemManagement.authorityManagement.departmentManagement', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (77, 'admin', 'systemManagement.authorityManagement.ipWhiteList', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (78, 'admin', 'systemManagement.authorityManagement.onlineUser', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (79, 'admin', 'systemManagement.authorityManagement.roleManagement', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (80, 'admin', 'systemManagement.authorityManagement.timePeriodManagement', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (81, 'admin', 'systemManagement.authorityManagement.userManagement', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (82, 'admin', 'systemManagement.backupRestore', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (83, 'admin', 'systemManagement.backupRestore.autoBackup', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (84, 'admin', 'systemManagement.backupRestore.pageBackupRestore', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (85, 'admin', 'systemManagement.backupRestore.systemBackupRestore', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (86, 'admin', 'systemManagement.pageSettingV2', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (87, 'admin', 'systemManagement.pageSettingV2.homepage', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (88, 'admin', 'systemManagement.pageSettingV2.login', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (89, 'admin', 'systemManagement.pageSettingV2.style', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (90, 'admin', 'systemManagement.pageSettingV2.view', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (91, 'admin', 'systemManagement.systemAuthorization', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (92, 'admin', 'systemManagement.systemLog', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (93, 'admin', 'systemManagement.systemLog.linkageRecord', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (94, 'admin', 'systemManagement.systemLog.operationLog', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (95, 'admin', 'systemManagement.systemSetting', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (96, 'admin', 'systemManagement.systemSkinPeelerV2', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (97, 'admin', 'systemManagement.systemUpgrade', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (98, 'admin', 'systemManagement.systemUpgrade.systemUpgrade', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (99, 'admin', 'systemManagement.systemUpgrade.upgradeRecord', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (100, 'admin', 'xbTool', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (101, 'admin', '{0000002F-0000-0000-C000-000000000050}', '2025-12-18 16:06:55', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (102, 'admin', 'bf5d36594c3efcb1', '2025-12-18 16:17:27', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (103, 'dhjk', 'bf5d36594c3efcb1', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (104, 'dhjk', 'bf5d36594c3efcb1.5abdc5ed65401883', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (105, 'dhjk', 'bf5d36594c3efcb1.5abdc5ed65401883.341c86804b81bbb4', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (106, 'dhjk', 'bf5d36594c3efcb1.5abdc5ed65401883.521320fd3c9c09f7', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (107, 'dhjk', 'bf5d36594c3efcb1.5abdc5ed65401883.96955ec871858f86', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (108, 'dhjk', 'bf5d36594c3efcb1.5abdc5ed65401883.ef7096b1c8f0970e', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (109, 'dhjk', 'bf5d36594c3efcb1.9f46c296d73593c4', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (110, 'dhjk', 'bf5d36594c3efcb1.9f46c296d73593c4.7b0730624e2d8018', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (111, 'dhjk', 'bf5d36594c3efcb1.9f46c296d73593c4.bf69b24de7a5bf31', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (112, 'dhjk', 'bf5d36594c3efcb1.9f46c296d73593c4.e56688ec5f14a1f7', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (113, 'dhjk', 'bf5d36594c3efcb1.f236b205b5fdb2e0', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (114, 'dhjk', 'bf5d36594c3efcb1.f236b205b5fdb2e0.53f20519272c8b7f', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (115, 'dhjk', 'bf5d36594c3efcb1.f236b205b5fdb2e0.81970ca803f32070', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (116, 'dhjk', 'dataQuality', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (117, 'dhjk', 'dataQuality.alarmConfig', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (118, 'dhjk', 'dataQuality.exceptionList', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (119, 'dhjk', 'energyPowerMonitor.extremeDataQuery', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (120, 'dhjk', 'energyPowerMonitor.originDataQuery', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (121, 'dhjk', 'facilityMonitor.alarmManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (122, 'dhjk', 'message', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (123, 'dhjk', 'message_center', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (124, 'dhjk', 'message_center.message.list', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (125, 'dhjk', 'message_center.message.setting', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (126, 'dhjk', 'message_center.notice.all', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (127, 'dhjk', 'message_center.notice.create', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (128, 'dhjk', 'message_center.notice.draft', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (129, 'dhjk', 'message_center.notice.list', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (130, 'dhjk', 'message_center.notice.send', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (131, 'dhjk', 'message_center.notice.setting', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (132, 'dhjk', 'monitorInfo', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (133, 'dhjk', 'notice', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (134, 'dhjk', 'projectConfiguration.alarmDescription', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (135, 'dhjk', 'projectConfiguration.alarmMasking', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (136, 'dhjk', 'projectConfiguration.alarmMasking.blockingRules', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (137, 'dhjk', 'projectConfiguration.alarmMasking.timeManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (138, 'dhjk', 'projectConfiguration.alarmNotify', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (139, 'dhjk', 'projectConfiguration.alarmNotify.alarmStrategy', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (140, 'dhjk', 'projectConfiguration.alarmNotify.messageRecordQuery', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (141, 'dhjk', 'projectConfiguration.alarmNotify.messageRouting', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (142, 'dhjk', 'projectConfiguration.alarmNotify.messageTemplate', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (143, 'dhjk', 'projectConfiguration.alarmNotify.regularySecurity', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (144, 'dhjk', 'projectConfiguration.alarmNotify.stormWarning', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (145, 'dhjk', 'projectConfiguration.alarmSetting', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (146, 'dhjk', 'projectConfiguration.carousel', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (147, 'dhjk', 'projectConfiguration.driversManager', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (148, 'dhjk', 'projectConfiguration.driversManager.collection', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (149, 'dhjk', 'projectConfiguration.driversManager.devNode', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (150, 'dhjk', 'projectConfiguration.driversManager.protocolFile', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (151, 'dhjk', 'projectConfiguration.nussi', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (152, 'dhjk', 'projectConfiguration.policyLinkage', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (153, 'dhjk', 'projectConfiguration.policyLinkage.functionModule', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (154, 'dhjk', 'projectConfiguration.policyLinkage.strategyList', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (155, 'dhjk', 'projectConfiguration.policyLinkage.strategyOverview', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (156, 'dhjk', 'projectConfiguration.storageSetting', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (157, 'dhjk', 'projectConfiguration.storageSetting.storageEngine', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (158, 'dhjk', 'projectConfiguration.storageSetting.storagePolicy', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (159, 'dhjk', 'projectConfiguration.systemTools', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (160, 'dhjk', 'projectConfiguration.systemTools.configImportExport', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (161, 'dhjk', 'projectConfiguration.systemTools.networkDbugTools', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (162, 'dhjk', 'projectConfiguration.systemTools.serialPortTool', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (163, 'dhjk', 'projectConfiguration.XECEP', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (164, 'dhjk', 'projectConfiguration.XECEP.Custom', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (165, 'dhjk', 'projectConfiguration.XECEP.RepeatNoise', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (166, 'dhjk', 'reportForm', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (167, 'dhjk', 'reportForm.commonForm', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (168, 'dhjk', 'reportForm.commonForm.dataSearch', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (169, 'dhjk', 'reportForm.commonForm.instantQuery', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (170, 'dhjk', 'reportForm.commonForm.querySchemeConfig', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (171, 'dhjk', 'reportForm.commonForm.warnSearchOtherV2', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (172, 'dhjk', 'reportForm.commonForm.warnSearchV2', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (173, 'dhjk', 'reportForm.reportOverview', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (174, 'dhjk', 'reportForm.storageStatistics', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (175, 'dhjk', 'reportForm.taskManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (176, 'dhjk', 'reportForm.templateManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (177, 'dhjk', 'systemManagement.authorityManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (178, 'dhjk', 'systemManagement.authorityManagement.accountRecycle', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (179, 'dhjk', 'systemManagement.authorityManagement.departmentManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (180, 'dhjk', 'systemManagement.authorityManagement.ipWhiteList', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (181, 'dhjk', 'systemManagement.authorityManagement.onlineUser', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (182, 'dhjk', 'systemManagement.authorityManagement.roleManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (183, 'dhjk', 'systemManagement.authorityManagement.timePeriodManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (184, 'dhjk', 'systemManagement.authorityManagement.userManagement', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (185, 'dhjk', 'systemManagement.backupRestore', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (186, 'dhjk', 'systemManagement.backupRestore.autoBackup', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (187, 'dhjk', 'systemManagement.backupRestore.pageBackupRestore', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (188, 'dhjk', 'systemManagement.backupRestore.systemBackupRestore', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (189, 'dhjk', 'systemManagement.pageSettingV2', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (190, 'dhjk', 'systemManagement.pageSettingV2.homepage', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (191, 'dhjk', 'systemManagement.pageSettingV2.login', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (192, 'dhjk', 'systemManagement.pageSettingV2.style', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (193, 'dhjk', 'systemManagement.pageSettingV2.view', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (194, 'dhjk', 'systemManagement.systemAuthorization', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (195, 'dhjk', 'systemManagement.systemLog', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (196, 'dhjk', 'systemManagement.systemLog.linkageRecord', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (197, 'dhjk', 'systemManagement.systemLog.operationLog', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (198, 'dhjk', 'systemManagement.systemSetting', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (199, 'dhjk', 'systemManagement.systemSkinPeelerV2', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (200, 'dhjk', 'systemManagement.systemUpgrade', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (201, 'dhjk', 'systemManagement.systemUpgrade.systemUpgrade', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (202, 'dhjk', 'systemManagement.systemUpgrade.upgradeRecord', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (203, 'dhjk', 'xbTool', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);
INSERT INTO `itom_base_role_menu_collect` VALUES (204, 'dhjk', '{0000002F-0000-0000-C000-000000000050}', '2025-12-22 09:21:37', '0000-00-00 00:00:00', 0);

-- ----------------------------
-- Table structure for itom_base_role_operation
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_role_operation`;
CREATE TABLE `itom_base_role_operation`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `role_id` bigint NOT NULL DEFAULT 1 COMMENT '角色ID',
  `operation_id` bigint NOT NULL COMMENT '操作项ID',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1930 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_role_operation
-- ----------------------------
INSERT INTO `itom_base_role_operation` VALUES (773, 21, 268);
INSERT INTO `itom_base_role_operation` VALUES (774, 21, 254);
INSERT INTO `itom_base_role_operation` VALUES (775, 21, 6);
INSERT INTO `itom_base_role_operation` VALUES (776, 21, 36);
INSERT INTO `itom_base_role_operation` VALUES (777, 21, 26);
INSERT INTO `itom_base_role_operation` VALUES (778, 21, 94);
INSERT INTO `itom_base_role_operation` VALUES (779, 21, 12);
INSERT INTO `itom_base_role_operation` VALUES (780, 21, 18);
INSERT INTO `itom_base_role_operation` VALUES (781, 21, 104);
INSERT INTO `itom_base_role_operation` VALUES (782, 21, 87);
INSERT INTO `itom_base_role_operation` VALUES (783, 21, 467);
INSERT INTO `itom_base_role_operation` VALUES (784, 21, 1);
INSERT INTO `itom_base_role_operation` VALUES (785, 21, 45);
INSERT INTO `itom_base_role_operation` VALUES (786, 21, 68);
INSERT INTO `itom_base_role_operation` VALUES (787, 21, 50);
INSERT INTO `itom_base_role_operation` VALUES (788, 21, 62);
INSERT INTO `itom_base_role_operation` VALUES (789, 21, 74);
INSERT INTO `itom_base_role_operation` VALUES (790, 21, 114);
INSERT INTO `itom_base_role_operation` VALUES (791, 21, 110);
INSERT INTO `itom_base_role_operation` VALUES (792, 21, 196);
INSERT INTO `itom_base_role_operation` VALUES (793, 21, 240);
INSERT INTO `itom_base_role_operation` VALUES (794, 21, 245);
INSERT INTO `itom_base_role_operation` VALUES (795, 21, 262);
INSERT INTO `itom_base_role_operation` VALUES (796, 21, 253);
INSERT INTO `itom_base_role_operation` VALUES (1108, 5, 268);
INSERT INTO `itom_base_role_operation` VALUES (1109, 5, 254);
INSERT INTO `itom_base_role_operation` VALUES (1110, 5, 6);
INSERT INTO `itom_base_role_operation` VALUES (1111, 5, 36);
INSERT INTO `itom_base_role_operation` VALUES (1112, 5, 26);
INSERT INTO `itom_base_role_operation` VALUES (1113, 5, 94);
INSERT INTO `itom_base_role_operation` VALUES (1114, 5, 12);
INSERT INTO `itom_base_role_operation` VALUES (1115, 5, 18);
INSERT INTO `itom_base_role_operation` VALUES (1116, 5, 104);
INSERT INTO `itom_base_role_operation` VALUES (1117, 5, 87);
INSERT INTO `itom_base_role_operation` VALUES (1118, 5, 467);
INSERT INTO `itom_base_role_operation` VALUES (1119, 5, 1);
INSERT INTO `itom_base_role_operation` VALUES (1120, 5, 45);
INSERT INTO `itom_base_role_operation` VALUES (1121, 5, 68);
INSERT INTO `itom_base_role_operation` VALUES (1122, 5, 50);
INSERT INTO `itom_base_role_operation` VALUES (1123, 5, 62);
INSERT INTO `itom_base_role_operation` VALUES (1124, 5, 74);
INSERT INTO `itom_base_role_operation` VALUES (1125, 5, 114);
INSERT INTO `itom_base_role_operation` VALUES (1126, 5, 110);
INSERT INTO `itom_base_role_operation` VALUES (1127, 5, 196);
INSERT INTO `itom_base_role_operation` VALUES (1128, 5, 240);
INSERT INTO `itom_base_role_operation` VALUES (1129, 5, 245);
INSERT INTO `itom_base_role_operation` VALUES (1130, 5, 262);
INSERT INTO `itom_base_role_operation` VALUES (1131, 5, 253);
INSERT INTO `itom_base_role_operation` VALUES (1132, 27, 268);
INSERT INTO `itom_base_role_operation` VALUES (1133, 27, 254);
INSERT INTO `itom_base_role_operation` VALUES (1134, 27, 6);
INSERT INTO `itom_base_role_operation` VALUES (1135, 27, 36);
INSERT INTO `itom_base_role_operation` VALUES (1136, 27, 26);
INSERT INTO `itom_base_role_operation` VALUES (1137, 27, 94);
INSERT INTO `itom_base_role_operation` VALUES (1138, 27, 12);
INSERT INTO `itom_base_role_operation` VALUES (1139, 27, 18);
INSERT INTO `itom_base_role_operation` VALUES (1140, 27, 104);
INSERT INTO `itom_base_role_operation` VALUES (1141, 27, 87);
INSERT INTO `itom_base_role_operation` VALUES (1142, 27, 467);
INSERT INTO `itom_base_role_operation` VALUES (1143, 27, 1);
INSERT INTO `itom_base_role_operation` VALUES (1144, 27, 45);
INSERT INTO `itom_base_role_operation` VALUES (1145, 27, 68);
INSERT INTO `itom_base_role_operation` VALUES (1146, 27, 50);
INSERT INTO `itom_base_role_operation` VALUES (1147, 27, 62);
INSERT INTO `itom_base_role_operation` VALUES (1148, 27, 74);
INSERT INTO `itom_base_role_operation` VALUES (1149, 27, 114);
INSERT INTO `itom_base_role_operation` VALUES (1150, 27, 110);
INSERT INTO `itom_base_role_operation` VALUES (1151, 27, 196);
INSERT INTO `itom_base_role_operation` VALUES (1152, 27, 240);
INSERT INTO `itom_base_role_operation` VALUES (1153, 27, 245);
INSERT INTO `itom_base_role_operation` VALUES (1154, 27, 262);
INSERT INTO `itom_base_role_operation` VALUES (1155, 27, 253);
INSERT INTO `itom_base_role_operation` VALUES (1156, 15, 268);
INSERT INTO `itom_base_role_operation` VALUES (1157, 15, 254);
INSERT INTO `itom_base_role_operation` VALUES (1158, 15, 6);
INSERT INTO `itom_base_role_operation` VALUES (1159, 15, 36);
INSERT INTO `itom_base_role_operation` VALUES (1160, 15, 26);
INSERT INTO `itom_base_role_operation` VALUES (1161, 15, 94);
INSERT INTO `itom_base_role_operation` VALUES (1162, 15, 12);
INSERT INTO `itom_base_role_operation` VALUES (1163, 15, 18);
INSERT INTO `itom_base_role_operation` VALUES (1164, 15, 104);
INSERT INTO `itom_base_role_operation` VALUES (1165, 15, 87);
INSERT INTO `itom_base_role_operation` VALUES (1166, 15, 467);
INSERT INTO `itom_base_role_operation` VALUES (1167, 15, 1);
INSERT INTO `itom_base_role_operation` VALUES (1168, 15, 45);
INSERT INTO `itom_base_role_operation` VALUES (1169, 15, 68);
INSERT INTO `itom_base_role_operation` VALUES (1170, 15, 50);
INSERT INTO `itom_base_role_operation` VALUES (1171, 15, 62);
INSERT INTO `itom_base_role_operation` VALUES (1172, 15, 74);
INSERT INTO `itom_base_role_operation` VALUES (1173, 15, 114);
INSERT INTO `itom_base_role_operation` VALUES (1174, 15, 110);
INSERT INTO `itom_base_role_operation` VALUES (1175, 15, 196);
INSERT INTO `itom_base_role_operation` VALUES (1176, 15, 240);
INSERT INTO `itom_base_role_operation` VALUES (1177, 15, 245);
INSERT INTO `itom_base_role_operation` VALUES (1178, 15, 262);
INSERT INTO `itom_base_role_operation` VALUES (1179, 15, 253);
INSERT INTO `itom_base_role_operation` VALUES (1180, 16, 268);
INSERT INTO `itom_base_role_operation` VALUES (1181, 16, 254);
INSERT INTO `itom_base_role_operation` VALUES (1182, 16, 6);
INSERT INTO `itom_base_role_operation` VALUES (1183, 16, 36);
INSERT INTO `itom_base_role_operation` VALUES (1184, 16, 26);
INSERT INTO `itom_base_role_operation` VALUES (1185, 16, 94);
INSERT INTO `itom_base_role_operation` VALUES (1186, 16, 12);
INSERT INTO `itom_base_role_operation` VALUES (1187, 16, 18);
INSERT INTO `itom_base_role_operation` VALUES (1188, 16, 104);
INSERT INTO `itom_base_role_operation` VALUES (1189, 16, 87);
INSERT INTO `itom_base_role_operation` VALUES (1190, 16, 467);
INSERT INTO `itom_base_role_operation` VALUES (1191, 16, 1);
INSERT INTO `itom_base_role_operation` VALUES (1192, 16, 45);
INSERT INTO `itom_base_role_operation` VALUES (1193, 16, 68);
INSERT INTO `itom_base_role_operation` VALUES (1194, 16, 50);
INSERT INTO `itom_base_role_operation` VALUES (1195, 16, 62);
INSERT INTO `itom_base_role_operation` VALUES (1196, 16, 74);
INSERT INTO `itom_base_role_operation` VALUES (1197, 16, 114);
INSERT INTO `itom_base_role_operation` VALUES (1198, 16, 110);
INSERT INTO `itom_base_role_operation` VALUES (1199, 16, 196);
INSERT INTO `itom_base_role_operation` VALUES (1200, 16, 240);
INSERT INTO `itom_base_role_operation` VALUES (1201, 16, 245);
INSERT INTO `itom_base_role_operation` VALUES (1202, 16, 262);
INSERT INTO `itom_base_role_operation` VALUES (1203, 16, 253);
INSERT INTO `itom_base_role_operation` VALUES (1204, 17, 268);
INSERT INTO `itom_base_role_operation` VALUES (1205, 17, 254);
INSERT INTO `itom_base_role_operation` VALUES (1206, 17, 6);
INSERT INTO `itom_base_role_operation` VALUES (1207, 17, 36);
INSERT INTO `itom_base_role_operation` VALUES (1208, 17, 26);
INSERT INTO `itom_base_role_operation` VALUES (1209, 17, 94);
INSERT INTO `itom_base_role_operation` VALUES (1210, 17, 12);
INSERT INTO `itom_base_role_operation` VALUES (1211, 17, 18);
INSERT INTO `itom_base_role_operation` VALUES (1212, 17, 104);
INSERT INTO `itom_base_role_operation` VALUES (1213, 17, 87);
INSERT INTO `itom_base_role_operation` VALUES (1214, 17, 467);
INSERT INTO `itom_base_role_operation` VALUES (1215, 17, 1);
INSERT INTO `itom_base_role_operation` VALUES (1216, 17, 45);
INSERT INTO `itom_base_role_operation` VALUES (1217, 17, 68);
INSERT INTO `itom_base_role_operation` VALUES (1218, 17, 50);
INSERT INTO `itom_base_role_operation` VALUES (1219, 17, 62);
INSERT INTO `itom_base_role_operation` VALUES (1220, 17, 74);
INSERT INTO `itom_base_role_operation` VALUES (1221, 17, 114);
INSERT INTO `itom_base_role_operation` VALUES (1222, 17, 110);
INSERT INTO `itom_base_role_operation` VALUES (1223, 17, 196);
INSERT INTO `itom_base_role_operation` VALUES (1224, 17, 240);
INSERT INTO `itom_base_role_operation` VALUES (1225, 17, 245);
INSERT INTO `itom_base_role_operation` VALUES (1226, 17, 262);
INSERT INTO `itom_base_role_operation` VALUES (1227, 17, 253);
INSERT INTO `itom_base_role_operation` VALUES (1228, 6, 268);
INSERT INTO `itom_base_role_operation` VALUES (1229, 6, 254);
INSERT INTO `itom_base_role_operation` VALUES (1230, 6, 6);
INSERT INTO `itom_base_role_operation` VALUES (1231, 6, 36);
INSERT INTO `itom_base_role_operation` VALUES (1232, 6, 26);
INSERT INTO `itom_base_role_operation` VALUES (1233, 6, 94);
INSERT INTO `itom_base_role_operation` VALUES (1234, 6, 12);
INSERT INTO `itom_base_role_operation` VALUES (1235, 6, 18);
INSERT INTO `itom_base_role_operation` VALUES (1236, 6, 104);
INSERT INTO `itom_base_role_operation` VALUES (1237, 6, 87);
INSERT INTO `itom_base_role_operation` VALUES (1238, 6, 467);
INSERT INTO `itom_base_role_operation` VALUES (1239, 6, 1);
INSERT INTO `itom_base_role_operation` VALUES (1240, 6, 45);
INSERT INTO `itom_base_role_operation` VALUES (1241, 6, 68);
INSERT INTO `itom_base_role_operation` VALUES (1242, 6, 50);
INSERT INTO `itom_base_role_operation` VALUES (1243, 6, 62);
INSERT INTO `itom_base_role_operation` VALUES (1244, 6, 74);
INSERT INTO `itom_base_role_operation` VALUES (1245, 6, 114);
INSERT INTO `itom_base_role_operation` VALUES (1246, 6, 110);
INSERT INTO `itom_base_role_operation` VALUES (1247, 6, 196);
INSERT INTO `itom_base_role_operation` VALUES (1248, 6, 240);
INSERT INTO `itom_base_role_operation` VALUES (1249, 6, 245);
INSERT INTO `itom_base_role_operation` VALUES (1250, 6, 262);
INSERT INTO `itom_base_role_operation` VALUES (1251, 6, 253);
INSERT INTO `itom_base_role_operation` VALUES (1252, 7, 268);
INSERT INTO `itom_base_role_operation` VALUES (1253, 7, 254);
INSERT INTO `itom_base_role_operation` VALUES (1254, 7, 6);
INSERT INTO `itom_base_role_operation` VALUES (1255, 7, 36);
INSERT INTO `itom_base_role_operation` VALUES (1256, 7, 26);
INSERT INTO `itom_base_role_operation` VALUES (1257, 7, 94);
INSERT INTO `itom_base_role_operation` VALUES (1258, 7, 12);
INSERT INTO `itom_base_role_operation` VALUES (1259, 7, 18);
INSERT INTO `itom_base_role_operation` VALUES (1260, 7, 104);
INSERT INTO `itom_base_role_operation` VALUES (1261, 7, 87);
INSERT INTO `itom_base_role_operation` VALUES (1262, 7, 467);
INSERT INTO `itom_base_role_operation` VALUES (1263, 7, 1);
INSERT INTO `itom_base_role_operation` VALUES (1264, 7, 45);
INSERT INTO `itom_base_role_operation` VALUES (1265, 7, 68);
INSERT INTO `itom_base_role_operation` VALUES (1266, 7, 50);
INSERT INTO `itom_base_role_operation` VALUES (1267, 7, 62);
INSERT INTO `itom_base_role_operation` VALUES (1268, 7, 74);
INSERT INTO `itom_base_role_operation` VALUES (1269, 7, 114);
INSERT INTO `itom_base_role_operation` VALUES (1270, 7, 110);
INSERT INTO `itom_base_role_operation` VALUES (1271, 7, 196);
INSERT INTO `itom_base_role_operation` VALUES (1272, 7, 240);
INSERT INTO `itom_base_role_operation` VALUES (1273, 7, 245);
INSERT INTO `itom_base_role_operation` VALUES (1274, 7, 262);
INSERT INTO `itom_base_role_operation` VALUES (1275, 7, 253);
INSERT INTO `itom_base_role_operation` VALUES (1276, 8, 268);
INSERT INTO `itom_base_role_operation` VALUES (1277, 8, 254);
INSERT INTO `itom_base_role_operation` VALUES (1278, 8, 6);
INSERT INTO `itom_base_role_operation` VALUES (1279, 8, 36);
INSERT INTO `itom_base_role_operation` VALUES (1280, 8, 26);
INSERT INTO `itom_base_role_operation` VALUES (1281, 8, 94);
INSERT INTO `itom_base_role_operation` VALUES (1282, 8, 12);
INSERT INTO `itom_base_role_operation` VALUES (1283, 8, 18);
INSERT INTO `itom_base_role_operation` VALUES (1284, 8, 104);
INSERT INTO `itom_base_role_operation` VALUES (1285, 8, 87);
INSERT INTO `itom_base_role_operation` VALUES (1286, 8, 467);
INSERT INTO `itom_base_role_operation` VALUES (1287, 8, 1);
INSERT INTO `itom_base_role_operation` VALUES (1288, 8, 45);
INSERT INTO `itom_base_role_operation` VALUES (1289, 8, 68);
INSERT INTO `itom_base_role_operation` VALUES (1290, 8, 50);
INSERT INTO `itom_base_role_operation` VALUES (1291, 8, 62);
INSERT INTO `itom_base_role_operation` VALUES (1292, 8, 74);
INSERT INTO `itom_base_role_operation` VALUES (1293, 8, 114);
INSERT INTO `itom_base_role_operation` VALUES (1294, 8, 110);
INSERT INTO `itom_base_role_operation` VALUES (1295, 8, 196);
INSERT INTO `itom_base_role_operation` VALUES (1296, 8, 240);
INSERT INTO `itom_base_role_operation` VALUES (1297, 8, 245);
INSERT INTO `itom_base_role_operation` VALUES (1298, 8, 262);
INSERT INTO `itom_base_role_operation` VALUES (1299, 8, 253);
INSERT INTO `itom_base_role_operation` VALUES (1300, 9, 268);
INSERT INTO `itom_base_role_operation` VALUES (1301, 9, 254);
INSERT INTO `itom_base_role_operation` VALUES (1302, 9, 6);
INSERT INTO `itom_base_role_operation` VALUES (1303, 9, 36);
INSERT INTO `itom_base_role_operation` VALUES (1304, 9, 26);
INSERT INTO `itom_base_role_operation` VALUES (1305, 9, 94);
INSERT INTO `itom_base_role_operation` VALUES (1306, 9, 12);
INSERT INTO `itom_base_role_operation` VALUES (1307, 9, 18);
INSERT INTO `itom_base_role_operation` VALUES (1308, 9, 104);
INSERT INTO `itom_base_role_operation` VALUES (1309, 9, 87);
INSERT INTO `itom_base_role_operation` VALUES (1310, 9, 467);
INSERT INTO `itom_base_role_operation` VALUES (1311, 9, 1);
INSERT INTO `itom_base_role_operation` VALUES (1312, 9, 45);
INSERT INTO `itom_base_role_operation` VALUES (1313, 9, 68);
INSERT INTO `itom_base_role_operation` VALUES (1314, 9, 50);
INSERT INTO `itom_base_role_operation` VALUES (1315, 9, 62);
INSERT INTO `itom_base_role_operation` VALUES (1316, 9, 74);
INSERT INTO `itom_base_role_operation` VALUES (1317, 9, 114);
INSERT INTO `itom_base_role_operation` VALUES (1318, 9, 110);
INSERT INTO `itom_base_role_operation` VALUES (1319, 9, 196);
INSERT INTO `itom_base_role_operation` VALUES (1320, 9, 240);
INSERT INTO `itom_base_role_operation` VALUES (1321, 9, 245);
INSERT INTO `itom_base_role_operation` VALUES (1322, 9, 262);
INSERT INTO `itom_base_role_operation` VALUES (1323, 9, 253);
INSERT INTO `itom_base_role_operation` VALUES (1324, 10, 268);
INSERT INTO `itom_base_role_operation` VALUES (1325, 10, 254);
INSERT INTO `itom_base_role_operation` VALUES (1326, 10, 6);
INSERT INTO `itom_base_role_operation` VALUES (1327, 10, 36);
INSERT INTO `itom_base_role_operation` VALUES (1328, 10, 26);
INSERT INTO `itom_base_role_operation` VALUES (1329, 10, 94);
INSERT INTO `itom_base_role_operation` VALUES (1330, 10, 12);
INSERT INTO `itom_base_role_operation` VALUES (1331, 10, 18);
INSERT INTO `itom_base_role_operation` VALUES (1332, 10, 104);
INSERT INTO `itom_base_role_operation` VALUES (1333, 10, 87);
INSERT INTO `itom_base_role_operation` VALUES (1334, 10, 467);
INSERT INTO `itom_base_role_operation` VALUES (1335, 10, 1);
INSERT INTO `itom_base_role_operation` VALUES (1336, 10, 45);
INSERT INTO `itom_base_role_operation` VALUES (1337, 10, 68);
INSERT INTO `itom_base_role_operation` VALUES (1338, 10, 50);
INSERT INTO `itom_base_role_operation` VALUES (1339, 10, 62);
INSERT INTO `itom_base_role_operation` VALUES (1340, 10, 74);
INSERT INTO `itom_base_role_operation` VALUES (1341, 10, 114);
INSERT INTO `itom_base_role_operation` VALUES (1342, 10, 110);
INSERT INTO `itom_base_role_operation` VALUES (1343, 10, 196);
INSERT INTO `itom_base_role_operation` VALUES (1344, 10, 240);
INSERT INTO `itom_base_role_operation` VALUES (1345, 10, 245);
INSERT INTO `itom_base_role_operation` VALUES (1346, 10, 262);
INSERT INTO `itom_base_role_operation` VALUES (1347, 10, 253);
INSERT INTO `itom_base_role_operation` VALUES (1348, 11, 268);
INSERT INTO `itom_base_role_operation` VALUES (1349, 11, 254);
INSERT INTO `itom_base_role_operation` VALUES (1350, 11, 6);
INSERT INTO `itom_base_role_operation` VALUES (1351, 11, 36);
INSERT INTO `itom_base_role_operation` VALUES (1352, 11, 26);
INSERT INTO `itom_base_role_operation` VALUES (1353, 11, 94);
INSERT INTO `itom_base_role_operation` VALUES (1354, 11, 12);
INSERT INTO `itom_base_role_operation` VALUES (1355, 11, 18);
INSERT INTO `itom_base_role_operation` VALUES (1356, 11, 104);
INSERT INTO `itom_base_role_operation` VALUES (1357, 11, 87);
INSERT INTO `itom_base_role_operation` VALUES (1358, 11, 467);
INSERT INTO `itom_base_role_operation` VALUES (1359, 11, 1);
INSERT INTO `itom_base_role_operation` VALUES (1360, 11, 45);
INSERT INTO `itom_base_role_operation` VALUES (1361, 11, 68);
INSERT INTO `itom_base_role_operation` VALUES (1362, 11, 50);
INSERT INTO `itom_base_role_operation` VALUES (1363, 11, 62);
INSERT INTO `itom_base_role_operation` VALUES (1364, 11, 74);
INSERT INTO `itom_base_role_operation` VALUES (1365, 11, 114);
INSERT INTO `itom_base_role_operation` VALUES (1366, 11, 110);
INSERT INTO `itom_base_role_operation` VALUES (1367, 11, 196);
INSERT INTO `itom_base_role_operation` VALUES (1368, 11, 240);
INSERT INTO `itom_base_role_operation` VALUES (1369, 11, 245);
INSERT INTO `itom_base_role_operation` VALUES (1370, 11, 262);
INSERT INTO `itom_base_role_operation` VALUES (1371, 11, 253);
INSERT INTO `itom_base_role_operation` VALUES (1372, 4, 268);
INSERT INTO `itom_base_role_operation` VALUES (1373, 4, 254);
INSERT INTO `itom_base_role_operation` VALUES (1374, 4, 6);
INSERT INTO `itom_base_role_operation` VALUES (1375, 4, 36);
INSERT INTO `itom_base_role_operation` VALUES (1376, 4, 26);
INSERT INTO `itom_base_role_operation` VALUES (1377, 4, 94);
INSERT INTO `itom_base_role_operation` VALUES (1378, 4, 12);
INSERT INTO `itom_base_role_operation` VALUES (1379, 4, 18);
INSERT INTO `itom_base_role_operation` VALUES (1380, 4, 104);
INSERT INTO `itom_base_role_operation` VALUES (1381, 4, 87);
INSERT INTO `itom_base_role_operation` VALUES (1382, 4, 467);
INSERT INTO `itom_base_role_operation` VALUES (1383, 4, 1);
INSERT INTO `itom_base_role_operation` VALUES (1384, 4, 45);
INSERT INTO `itom_base_role_operation` VALUES (1385, 4, 68);
INSERT INTO `itom_base_role_operation` VALUES (1386, 4, 50);
INSERT INTO `itom_base_role_operation` VALUES (1387, 4, 62);
INSERT INTO `itom_base_role_operation` VALUES (1388, 4, 74);
INSERT INTO `itom_base_role_operation` VALUES (1389, 4, 114);
INSERT INTO `itom_base_role_operation` VALUES (1390, 4, 110);
INSERT INTO `itom_base_role_operation` VALUES (1391, 4, 196);
INSERT INTO `itom_base_role_operation` VALUES (1392, 4, 240);
INSERT INTO `itom_base_role_operation` VALUES (1393, 4, 245);
INSERT INTO `itom_base_role_operation` VALUES (1394, 4, 262);
INSERT INTO `itom_base_role_operation` VALUES (1395, 4, 253);
INSERT INTO `itom_base_role_operation` VALUES (1396, 12, 268);
INSERT INTO `itom_base_role_operation` VALUES (1397, 12, 254);
INSERT INTO `itom_base_role_operation` VALUES (1398, 12, 6);
INSERT INTO `itom_base_role_operation` VALUES (1399, 12, 36);
INSERT INTO `itom_base_role_operation` VALUES (1400, 12, 26);
INSERT INTO `itom_base_role_operation` VALUES (1401, 12, 94);
INSERT INTO `itom_base_role_operation` VALUES (1402, 12, 12);
INSERT INTO `itom_base_role_operation` VALUES (1403, 12, 18);
INSERT INTO `itom_base_role_operation` VALUES (1404, 12, 104);
INSERT INTO `itom_base_role_operation` VALUES (1405, 12, 87);
INSERT INTO `itom_base_role_operation` VALUES (1406, 12, 467);
INSERT INTO `itom_base_role_operation` VALUES (1407, 12, 1);
INSERT INTO `itom_base_role_operation` VALUES (1408, 12, 45);
INSERT INTO `itom_base_role_operation` VALUES (1409, 12, 68);
INSERT INTO `itom_base_role_operation` VALUES (1410, 12, 50);
INSERT INTO `itom_base_role_operation` VALUES (1411, 12, 62);
INSERT INTO `itom_base_role_operation` VALUES (1412, 12, 74);
INSERT INTO `itom_base_role_operation` VALUES (1413, 12, 114);
INSERT INTO `itom_base_role_operation` VALUES (1414, 12, 110);
INSERT INTO `itom_base_role_operation` VALUES (1415, 12, 196);
INSERT INTO `itom_base_role_operation` VALUES (1416, 12, 240);
INSERT INTO `itom_base_role_operation` VALUES (1417, 12, 245);
INSERT INTO `itom_base_role_operation` VALUES (1418, 12, 262);
INSERT INTO `itom_base_role_operation` VALUES (1419, 12, 253);
INSERT INTO `itom_base_role_operation` VALUES (1420, 13, 268);
INSERT INTO `itom_base_role_operation` VALUES (1421, 13, 254);
INSERT INTO `itom_base_role_operation` VALUES (1422, 13, 6);
INSERT INTO `itom_base_role_operation` VALUES (1423, 13, 36);
INSERT INTO `itom_base_role_operation` VALUES (1424, 13, 26);
INSERT INTO `itom_base_role_operation` VALUES (1425, 13, 94);
INSERT INTO `itom_base_role_operation` VALUES (1426, 13, 12);
INSERT INTO `itom_base_role_operation` VALUES (1427, 13, 18);
INSERT INTO `itom_base_role_operation` VALUES (1428, 13, 104);
INSERT INTO `itom_base_role_operation` VALUES (1429, 13, 87);
INSERT INTO `itom_base_role_operation` VALUES (1430, 13, 467);
INSERT INTO `itom_base_role_operation` VALUES (1431, 13, 1);
INSERT INTO `itom_base_role_operation` VALUES (1432, 13, 45);
INSERT INTO `itom_base_role_operation` VALUES (1433, 13, 68);
INSERT INTO `itom_base_role_operation` VALUES (1434, 13, 50);
INSERT INTO `itom_base_role_operation` VALUES (1435, 13, 62);
INSERT INTO `itom_base_role_operation` VALUES (1436, 13, 74);
INSERT INTO `itom_base_role_operation` VALUES (1437, 13, 114);
INSERT INTO `itom_base_role_operation` VALUES (1438, 13, 110);
INSERT INTO `itom_base_role_operation` VALUES (1439, 13, 196);
INSERT INTO `itom_base_role_operation` VALUES (1440, 13, 240);
INSERT INTO `itom_base_role_operation` VALUES (1441, 13, 245);
INSERT INTO `itom_base_role_operation` VALUES (1442, 13, 262);
INSERT INTO `itom_base_role_operation` VALUES (1443, 13, 253);
INSERT INTO `itom_base_role_operation` VALUES (1444, 14, 268);
INSERT INTO `itom_base_role_operation` VALUES (1445, 14, 254);
INSERT INTO `itom_base_role_operation` VALUES (1446, 14, 6);
INSERT INTO `itom_base_role_operation` VALUES (1447, 14, 36);
INSERT INTO `itom_base_role_operation` VALUES (1448, 14, 26);
INSERT INTO `itom_base_role_operation` VALUES (1449, 14, 94);
INSERT INTO `itom_base_role_operation` VALUES (1450, 14, 12);
INSERT INTO `itom_base_role_operation` VALUES (1451, 14, 18);
INSERT INTO `itom_base_role_operation` VALUES (1452, 14, 104);
INSERT INTO `itom_base_role_operation` VALUES (1453, 14, 87);
INSERT INTO `itom_base_role_operation` VALUES (1454, 14, 467);
INSERT INTO `itom_base_role_operation` VALUES (1455, 14, 1);
INSERT INTO `itom_base_role_operation` VALUES (1456, 14, 45);
INSERT INTO `itom_base_role_operation` VALUES (1457, 14, 68);
INSERT INTO `itom_base_role_operation` VALUES (1458, 14, 50);
INSERT INTO `itom_base_role_operation` VALUES (1459, 14, 62);
INSERT INTO `itom_base_role_operation` VALUES (1460, 14, 74);
INSERT INTO `itom_base_role_operation` VALUES (1461, 14, 114);
INSERT INTO `itom_base_role_operation` VALUES (1462, 14, 110);
INSERT INTO `itom_base_role_operation` VALUES (1463, 14, 196);
INSERT INTO `itom_base_role_operation` VALUES (1464, 14, 240);
INSERT INTO `itom_base_role_operation` VALUES (1465, 14, 245);
INSERT INTO `itom_base_role_operation` VALUES (1466, 14, 262);
INSERT INTO `itom_base_role_operation` VALUES (1467, 14, 253);
INSERT INTO `itom_base_role_operation` VALUES (1468, 18, 268);
INSERT INTO `itom_base_role_operation` VALUES (1469, 18, 254);
INSERT INTO `itom_base_role_operation` VALUES (1470, 18, 6);
INSERT INTO `itom_base_role_operation` VALUES (1471, 18, 36);
INSERT INTO `itom_base_role_operation` VALUES (1472, 18, 26);
INSERT INTO `itom_base_role_operation` VALUES (1473, 18, 94);
INSERT INTO `itom_base_role_operation` VALUES (1474, 18, 12);
INSERT INTO `itom_base_role_operation` VALUES (1475, 18, 18);
INSERT INTO `itom_base_role_operation` VALUES (1476, 18, 104);
INSERT INTO `itom_base_role_operation` VALUES (1477, 18, 87);
INSERT INTO `itom_base_role_operation` VALUES (1478, 18, 467);
INSERT INTO `itom_base_role_operation` VALUES (1479, 18, 1);
INSERT INTO `itom_base_role_operation` VALUES (1480, 18, 45);
INSERT INTO `itom_base_role_operation` VALUES (1481, 18, 68);
INSERT INTO `itom_base_role_operation` VALUES (1482, 18, 50);
INSERT INTO `itom_base_role_operation` VALUES (1483, 18, 62);
INSERT INTO `itom_base_role_operation` VALUES (1484, 18, 74);
INSERT INTO `itom_base_role_operation` VALUES (1485, 18, 114);
INSERT INTO `itom_base_role_operation` VALUES (1486, 18, 110);
INSERT INTO `itom_base_role_operation` VALUES (1487, 18, 196);
INSERT INTO `itom_base_role_operation` VALUES (1488, 18, 240);
INSERT INTO `itom_base_role_operation` VALUES (1489, 18, 245);
INSERT INTO `itom_base_role_operation` VALUES (1490, 18, 262);
INSERT INTO `itom_base_role_operation` VALUES (1491, 18, 253);
INSERT INTO `itom_base_role_operation` VALUES (1492, 19, 268);
INSERT INTO `itom_base_role_operation` VALUES (1493, 19, 254);
INSERT INTO `itom_base_role_operation` VALUES (1494, 19, 6);
INSERT INTO `itom_base_role_operation` VALUES (1495, 19, 36);
INSERT INTO `itom_base_role_operation` VALUES (1496, 19, 26);
INSERT INTO `itom_base_role_operation` VALUES (1497, 19, 94);
INSERT INTO `itom_base_role_operation` VALUES (1498, 19, 12);
INSERT INTO `itom_base_role_operation` VALUES (1499, 19, 18);
INSERT INTO `itom_base_role_operation` VALUES (1500, 19, 104);
INSERT INTO `itom_base_role_operation` VALUES (1501, 19, 87);
INSERT INTO `itom_base_role_operation` VALUES (1502, 19, 467);
INSERT INTO `itom_base_role_operation` VALUES (1503, 19, 1);
INSERT INTO `itom_base_role_operation` VALUES (1504, 19, 45);
INSERT INTO `itom_base_role_operation` VALUES (1505, 19, 68);
INSERT INTO `itom_base_role_operation` VALUES (1506, 19, 50);
INSERT INTO `itom_base_role_operation` VALUES (1507, 19, 62);
INSERT INTO `itom_base_role_operation` VALUES (1508, 19, 74);
INSERT INTO `itom_base_role_operation` VALUES (1509, 19, 114);
INSERT INTO `itom_base_role_operation` VALUES (1510, 19, 110);
INSERT INTO `itom_base_role_operation` VALUES (1511, 19, 196);
INSERT INTO `itom_base_role_operation` VALUES (1512, 19, 240);
INSERT INTO `itom_base_role_operation` VALUES (1513, 19, 245);
INSERT INTO `itom_base_role_operation` VALUES (1514, 19, 262);
INSERT INTO `itom_base_role_operation` VALUES (1515, 19, 253);
INSERT INTO `itom_base_role_operation` VALUES (1516, 20, 268);
INSERT INTO `itom_base_role_operation` VALUES (1517, 20, 254);
INSERT INTO `itom_base_role_operation` VALUES (1518, 20, 6);
INSERT INTO `itom_base_role_operation` VALUES (1519, 20, 36);
INSERT INTO `itom_base_role_operation` VALUES (1520, 20, 26);
INSERT INTO `itom_base_role_operation` VALUES (1521, 20, 94);
INSERT INTO `itom_base_role_operation` VALUES (1522, 20, 12);
INSERT INTO `itom_base_role_operation` VALUES (1523, 20, 18);
INSERT INTO `itom_base_role_operation` VALUES (1524, 20, 104);
INSERT INTO `itom_base_role_operation` VALUES (1525, 20, 87);
INSERT INTO `itom_base_role_operation` VALUES (1526, 20, 467);
INSERT INTO `itom_base_role_operation` VALUES (1527, 20, 1);
INSERT INTO `itom_base_role_operation` VALUES (1528, 20, 45);
INSERT INTO `itom_base_role_operation` VALUES (1529, 20, 68);
INSERT INTO `itom_base_role_operation` VALUES (1530, 20, 50);
INSERT INTO `itom_base_role_operation` VALUES (1531, 20, 62);
INSERT INTO `itom_base_role_operation` VALUES (1532, 20, 74);
INSERT INTO `itom_base_role_operation` VALUES (1533, 20, 114);
INSERT INTO `itom_base_role_operation` VALUES (1534, 20, 110);
INSERT INTO `itom_base_role_operation` VALUES (1535, 20, 196);
INSERT INTO `itom_base_role_operation` VALUES (1536, 20, 240);
INSERT INTO `itom_base_role_operation` VALUES (1537, 20, 245);
INSERT INTO `itom_base_role_operation` VALUES (1538, 20, 262);
INSERT INTO `itom_base_role_operation` VALUES (1539, 20, 253);
INSERT INTO `itom_base_role_operation` VALUES (1540, 1, 769);
INSERT INTO `itom_base_role_operation` VALUES (1541, 1, 805);
INSERT INTO `itom_base_role_operation` VALUES (1542, 1, 813);
INSERT INTO `itom_base_role_operation` VALUES (1543, 1, 820);
INSERT INTO `itom_base_role_operation` VALUES (1544, 1, 810);
INSERT INTO `itom_base_role_operation` VALUES (1545, 1, 819);
INSERT INTO `itom_base_role_operation` VALUES (1546, 1, 790);
INSERT INTO `itom_base_role_operation` VALUES (1547, 1, 794);
INSERT INTO `itom_base_role_operation` VALUES (1548, 1, 796);
INSERT INTO `itom_base_role_operation` VALUES (1549, 1, 792);
INSERT INTO `itom_base_role_operation` VALUES (1550, 1, 772);
INSERT INTO `itom_base_role_operation` VALUES (1551, 1, 781);
INSERT INTO `itom_base_role_operation` VALUES (1552, 1, 773);
INSERT INTO `itom_base_role_operation` VALUES (1553, 1, 449);
INSERT INTO `itom_base_role_operation` VALUES (1554, 1, 458);
INSERT INTO `itom_base_role_operation` VALUES (1555, 1, 453);
INSERT INTO `itom_base_role_operation` VALUES (1556, 1, 81);
INSERT INTO `itom_base_role_operation` VALUES (1557, 1, 78);
INSERT INTO `itom_base_role_operation` VALUES (1558, 1, 268);
INSERT INTO `itom_base_role_operation` VALUES (1559, 1, 254);
INSERT INTO `itom_base_role_operation` VALUES (1560, 1, 6);
INSERT INTO `itom_base_role_operation` VALUES (1561, 1, 36);
INSERT INTO `itom_base_role_operation` VALUES (1562, 1, 26);
INSERT INTO `itom_base_role_operation` VALUES (1563, 1, 94);
INSERT INTO `itom_base_role_operation` VALUES (1564, 1, 12);
INSERT INTO `itom_base_role_operation` VALUES (1565, 1, 18);
INSERT INTO `itom_base_role_operation` VALUES (1566, 1, 104);
INSERT INTO `itom_base_role_operation` VALUES (1567, 1, 87);
INSERT INTO `itom_base_role_operation` VALUES (1568, 1, 167);
INSERT INTO `itom_base_role_operation` VALUES (1569, 1, 179);
INSERT INTO `itom_base_role_operation` VALUES (1570, 1, 169);
INSERT INTO `itom_base_role_operation` VALUES (1571, 1, 142);
INSERT INTO `itom_base_role_operation` VALUES (1572, 1, 134);
INSERT INTO `itom_base_role_operation` VALUES (1573, 1, 152);
INSERT INTO `itom_base_role_operation` VALUES (1574, 1, 162);
INSERT INTO `itom_base_role_operation` VALUES (1575, 1, 125);
INSERT INTO `itom_base_role_operation` VALUES (1576, 1, 133);
INSERT INTO `itom_base_role_operation` VALUES (1577, 1, 467);
INSERT INTO `itom_base_role_operation` VALUES (1578, 1, 119);
INSERT INTO `itom_base_role_operation` VALUES (1579, 1, 1);
INSERT INTO `itom_base_role_operation` VALUES (1580, 1, 472);
INSERT INTO `itom_base_role_operation` VALUES (1581, 1, 510);
INSERT INTO `itom_base_role_operation` VALUES (1582, 1, 519);
INSERT INTO `itom_base_role_operation` VALUES (1583, 1, 531);
INSERT INTO `itom_base_role_operation` VALUES (1584, 1, 521);
INSERT INTO `itom_base_role_operation` VALUES (1585, 1, 633);
INSERT INTO `itom_base_role_operation` VALUES (1586, 1, 653);
INSERT INTO `itom_base_role_operation` VALUES (1587, 1, 643);
INSERT INTO `itom_base_role_operation` VALUES (1588, 1, 660);
INSERT INTO `itom_base_role_operation` VALUES (1589, 1, 667);
INSERT INTO `itom_base_role_operation` VALUES (1590, 1, 670);
INSERT INTO `itom_base_role_operation` VALUES (1591, 1, 665);
INSERT INTO `itom_base_role_operation` VALUES (1592, 1, 603);
INSERT INTO `itom_base_role_operation` VALUES (1593, 1, 631);
INSERT INTO `itom_base_role_operation` VALUES (1594, 1, 576);
INSERT INTO `itom_base_role_operation` VALUES (1595, 1, 578);
INSERT INTO `itom_base_role_operation` VALUES (1596, 1, 594);
INSERT INTO `itom_base_role_operation` VALUES (1597, 1, 587);
INSERT INTO `itom_base_role_operation` VALUES (1598, 1, 566);
INSERT INTO `itom_base_role_operation` VALUES (1599, 1, 536);
INSERT INTO `itom_base_role_operation` VALUES (1600, 1, 550);
INSERT INTO `itom_base_role_operation` VALUES (1601, 1, 543);
INSERT INTO `itom_base_role_operation` VALUES (1602, 1, 750);
INSERT INTO `itom_base_role_operation` VALUES (1603, 1, 559);
INSERT INTO `itom_base_role_operation` VALUES (1604, 1, 481);
INSERT INTO `itom_base_role_operation` VALUES (1605, 1, 488);
INSERT INTO `itom_base_role_operation` VALUES (1606, 1, 498);
INSERT INTO `itom_base_role_operation` VALUES (1607, 1, 610);
INSERT INTO `itom_base_role_operation` VALUES (1608, 1, 622);
INSERT INTO `itom_base_role_operation` VALUES (1609, 1, 616);
INSERT INTO `itom_base_role_operation` VALUES (1610, 1, 614);
INSERT INTO `itom_base_role_operation` VALUES (1611, 1, 677);
INSERT INTO `itom_base_role_operation` VALUES (1612, 1, 691);
INSERT INTO `itom_base_role_operation` VALUES (1613, 1, 681);
INSERT INTO `itom_base_role_operation` VALUES (1614, 1, 45);
INSERT INTO `itom_base_role_operation` VALUES (1615, 1, 68);
INSERT INTO `itom_base_role_operation` VALUES (1616, 1, 50);
INSERT INTO `itom_base_role_operation` VALUES (1617, 1, 62);
INSERT INTO `itom_base_role_operation` VALUES (1618, 1, 74);
INSERT INTO `itom_base_role_operation` VALUES (1619, 1, 114);
INSERT INTO `itom_base_role_operation` VALUES (1620, 1, 110);
INSERT INTO `itom_base_role_operation` VALUES (1621, 1, 196);
INSERT INTO `itom_base_role_operation` VALUES (1622, 1, 240);
INSERT INTO `itom_base_role_operation` VALUES (1623, 1, 245);
INSERT INTO `itom_base_role_operation` VALUES (1624, 1, 262);
INSERT INTO `itom_base_role_operation` VALUES (1625, 1, 253);
INSERT INTO `itom_base_role_operation` VALUES (1626, 1, 269);
INSERT INTO `itom_base_role_operation` VALUES (1627, 1, 211);
INSERT INTO `itom_base_role_operation` VALUES (1628, 1, 504);
INSERT INTO `itom_base_role_operation` VALUES (1629, 1, 201);
INSERT INTO `itom_base_role_operation` VALUES (1630, 1, 207);
INSERT INTO `itom_base_role_operation` VALUES (1631, 1, 283);
INSERT INTO `itom_base_role_operation` VALUES (1632, 1, 361);
INSERT INTO `itom_base_role_operation` VALUES (1633, 1, 385);
INSERT INTO `itom_base_role_operation` VALUES (1634, 1, 371);
INSERT INTO `itom_base_role_operation` VALUES (1635, 1, 389);
INSERT INTO `itom_base_role_operation` VALUES (1636, 1, 399);
INSERT INTO `itom_base_role_operation` VALUES (1637, 1, 406);
INSERT INTO `itom_base_role_operation` VALUES (1638, 1, 382);
INSERT INTO `itom_base_role_operation` VALUES (1639, 1, 379);
INSERT INTO `itom_base_role_operation` VALUES (1640, 1, 293);
INSERT INTO `itom_base_role_operation` VALUES (1641, 1, 306);
INSERT INTO `itom_base_role_operation` VALUES (1642, 1, 299);
INSERT INTO `itom_base_role_operation` VALUES (1643, 1, 308);
INSERT INTO `itom_base_role_operation` VALUES (1644, 1, 416);
INSERT INTO `itom_base_role_operation` VALUES (1645, 1, 432);
INSERT INTO `itom_base_role_operation` VALUES (1646, 1, 423);
INSERT INTO `itom_base_role_operation` VALUES (1647, 1, 446);
INSERT INTO `itom_base_role_operation` VALUES (1648, 1, 442);
INSERT INTO `itom_base_role_operation` VALUES (1649, 1, 322);
INSERT INTO `itom_base_role_operation` VALUES (1650, 1, 329);
INSERT INTO `itom_base_role_operation` VALUES (1651, 1, 337);
INSERT INTO `itom_base_role_operation` VALUES (1652, 1, 333);
INSERT INTO `itom_base_role_operation` VALUES (1653, 1, 327);
INSERT INTO `itom_base_role_operation` VALUES (1654, 1, 407);
INSERT INTO `itom_base_role_operation` VALUES (1655, 1, 338);
INSERT INTO `itom_base_role_operation` VALUES (1656, 1, 347);
INSERT INTO `itom_base_role_operation` VALUES (1657, 1, 352);
INSERT INTO `itom_base_role_operation` VALUES (1658, 1, 185);
INSERT INTO `itom_base_role_operation` VALUES (1659, 1, 760);
INSERT INTO `itom_base_role_operation` VALUES (1660, 1, 745);
INSERT INTO `itom_base_role_operation` VALUES (1661, 1, 767);
INSERT INTO `itom_base_role_operation` VALUES (1662, 2, 268);
INSERT INTO `itom_base_role_operation` VALUES (1663, 2, 254);
INSERT INTO `itom_base_role_operation` VALUES (1664, 2, 6);
INSERT INTO `itom_base_role_operation` VALUES (1665, 2, 36);
INSERT INTO `itom_base_role_operation` VALUES (1666, 2, 26);
INSERT INTO `itom_base_role_operation` VALUES (1667, 2, 94);
INSERT INTO `itom_base_role_operation` VALUES (1668, 2, 12);
INSERT INTO `itom_base_role_operation` VALUES (1669, 2, 18);
INSERT INTO `itom_base_role_operation` VALUES (1670, 2, 104);
INSERT INTO `itom_base_role_operation` VALUES (1671, 2, 87);
INSERT INTO `itom_base_role_operation` VALUES (1672, 2, 467);
INSERT INTO `itom_base_role_operation` VALUES (1673, 2, 1);
INSERT INTO `itom_base_role_operation` VALUES (1674, 2, 45);
INSERT INTO `itom_base_role_operation` VALUES (1675, 2, 68);
INSERT INTO `itom_base_role_operation` VALUES (1676, 2, 50);
INSERT INTO `itom_base_role_operation` VALUES (1677, 2, 62);
INSERT INTO `itom_base_role_operation` VALUES (1678, 2, 74);
INSERT INTO `itom_base_role_operation` VALUES (1679, 2, 114);
INSERT INTO `itom_base_role_operation` VALUES (1680, 2, 110);
INSERT INTO `itom_base_role_operation` VALUES (1681, 2, 196);
INSERT INTO `itom_base_role_operation` VALUES (1682, 2, 240);
INSERT INTO `itom_base_role_operation` VALUES (1683, 2, 245);
INSERT INTO `itom_base_role_operation` VALUES (1684, 2, 262);
INSERT INTO `itom_base_role_operation` VALUES (1685, 2, 253);
INSERT INTO `itom_base_role_operation` VALUES (1686, 3, 268);
INSERT INTO `itom_base_role_operation` VALUES (1687, 3, 254);
INSERT INTO `itom_base_role_operation` VALUES (1688, 3, 6);
INSERT INTO `itom_base_role_operation` VALUES (1689, 3, 36);
INSERT INTO `itom_base_role_operation` VALUES (1690, 3, 26);
INSERT INTO `itom_base_role_operation` VALUES (1691, 3, 94);
INSERT INTO `itom_base_role_operation` VALUES (1692, 3, 12);
INSERT INTO `itom_base_role_operation` VALUES (1693, 3, 18);
INSERT INTO `itom_base_role_operation` VALUES (1694, 3, 104);
INSERT INTO `itom_base_role_operation` VALUES (1695, 3, 87);
INSERT INTO `itom_base_role_operation` VALUES (1696, 3, 467);
INSERT INTO `itom_base_role_operation` VALUES (1697, 3, 1);
INSERT INTO `itom_base_role_operation` VALUES (1698, 3, 45);
INSERT INTO `itom_base_role_operation` VALUES (1699, 3, 68);
INSERT INTO `itom_base_role_operation` VALUES (1700, 3, 50);
INSERT INTO `itom_base_role_operation` VALUES (1701, 3, 62);
INSERT INTO `itom_base_role_operation` VALUES (1702, 3, 74);
INSERT INTO `itom_base_role_operation` VALUES (1703, 3, 114);
INSERT INTO `itom_base_role_operation` VALUES (1704, 3, 110);
INSERT INTO `itom_base_role_operation` VALUES (1705, 3, 196);
INSERT INTO `itom_base_role_operation` VALUES (1706, 3, 240);
INSERT INTO `itom_base_role_operation` VALUES (1707, 3, 245);
INSERT INTO `itom_base_role_operation` VALUES (1708, 3, 262);
INSERT INTO `itom_base_role_operation` VALUES (1709, 3, 253);
INSERT INTO `itom_base_role_operation` VALUES (1710, 22, 268);
INSERT INTO `itom_base_role_operation` VALUES (1711, 22, 254);
INSERT INTO `itom_base_role_operation` VALUES (1712, 22, 6);
INSERT INTO `itom_base_role_operation` VALUES (1713, 22, 36);
INSERT INTO `itom_base_role_operation` VALUES (1714, 22, 26);
INSERT INTO `itom_base_role_operation` VALUES (1715, 22, 94);
INSERT INTO `itom_base_role_operation` VALUES (1716, 22, 12);
INSERT INTO `itom_base_role_operation` VALUES (1717, 22, 18);
INSERT INTO `itom_base_role_operation` VALUES (1718, 22, 104);
INSERT INTO `itom_base_role_operation` VALUES (1719, 22, 87);
INSERT INTO `itom_base_role_operation` VALUES (1720, 22, 467);
INSERT INTO `itom_base_role_operation` VALUES (1721, 22, 1);
INSERT INTO `itom_base_role_operation` VALUES (1722, 22, 45);
INSERT INTO `itom_base_role_operation` VALUES (1723, 22, 68);
INSERT INTO `itom_base_role_operation` VALUES (1724, 22, 50);
INSERT INTO `itom_base_role_operation` VALUES (1725, 22, 62);
INSERT INTO `itom_base_role_operation` VALUES (1726, 22, 74);
INSERT INTO `itom_base_role_operation` VALUES (1727, 22, 114);
INSERT INTO `itom_base_role_operation` VALUES (1728, 22, 110);
INSERT INTO `itom_base_role_operation` VALUES (1729, 22, 196);
INSERT INTO `itom_base_role_operation` VALUES (1730, 22, 240);
INSERT INTO `itom_base_role_operation` VALUES (1731, 22, 245);
INSERT INTO `itom_base_role_operation` VALUES (1732, 22, 262);
INSERT INTO `itom_base_role_operation` VALUES (1733, 22, 253);
INSERT INTO `itom_base_role_operation` VALUES (1904, 26, 268);
INSERT INTO `itom_base_role_operation` VALUES (1905, 26, 254);
INSERT INTO `itom_base_role_operation` VALUES (1906, 26, 6);
INSERT INTO `itom_base_role_operation` VALUES (1907, 26, 36);
INSERT INTO `itom_base_role_operation` VALUES (1908, 26, 26);
INSERT INTO `itom_base_role_operation` VALUES (1909, 26, 94);
INSERT INTO `itom_base_role_operation` VALUES (1910, 26, 12);
INSERT INTO `itom_base_role_operation` VALUES (1911, 26, 18);
INSERT INTO `itom_base_role_operation` VALUES (1912, 26, 104);
INSERT INTO `itom_base_role_operation` VALUES (1913, 26, 87);
INSERT INTO `itom_base_role_operation` VALUES (1914, 26, 467);
INSERT INTO `itom_base_role_operation` VALUES (1915, 26, 1);
INSERT INTO `itom_base_role_operation` VALUES (1916, 26, 472);
INSERT INTO `itom_base_role_operation` VALUES (1917, 26, 45);
INSERT INTO `itom_base_role_operation` VALUES (1918, 26, 68);
INSERT INTO `itom_base_role_operation` VALUES (1919, 26, 50);
INSERT INTO `itom_base_role_operation` VALUES (1920, 26, 62);
INSERT INTO `itom_base_role_operation` VALUES (1921, 26, 74);
INSERT INTO `itom_base_role_operation` VALUES (1922, 26, 114);
INSERT INTO `itom_base_role_operation` VALUES (1923, 26, 110);
INSERT INTO `itom_base_role_operation` VALUES (1924, 26, 196);
INSERT INTO `itom_base_role_operation` VALUES (1925, 26, 240);
INSERT INTO `itom_base_role_operation` VALUES (1926, 26, 245);
INSERT INTO `itom_base_role_operation` VALUES (1927, 26, 262);
INSERT INTO `itom_base_role_operation` VALUES (1928, 26, 253);
INSERT INTO `itom_base_role_operation` VALUES (1929, 26, 185);

-- ----------------------------
-- Table structure for itom_base_role_operation_default
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_role_operation_default`;
CREATE TABLE `itom_base_role_operation_default`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `role_id` bigint NOT NULL COMMENT '角色ID',
  `operation_id` bigint NOT NULL COMMENT '操作项ID',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 10 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_role_operation_default
-- ----------------------------
INSERT INTO `itom_base_role_operation_default` VALUES (1, 1, 1);
INSERT INTO `itom_base_role_operation_default` VALUES (4, 1, 770);
INSERT INTO `itom_base_role_operation_default` VALUES (5, 1, 1024);
INSERT INTO `itom_base_role_operation_default` VALUES (6, 1, 2048);

-- ----------------------------
-- Table structure for itom_base_role_operation_v2
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_role_operation_v2`;
CREATE TABLE `itom_base_role_operation_v2`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `role_id` bigint NOT NULL DEFAULT 1 COMMENT '角色ID',
  `operation_id` bigint NOT NULL COMMENT '操作项ID',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 2184 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_role_operation_v2
-- ----------------------------
INSERT INTO `itom_base_role_operation_v2` VALUES (901, 21, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (902, 21, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (903, 21, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (904, 21, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (905, 21, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (906, 21, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (907, 21, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (908, 21, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (909, 21, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (910, 21, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (911, 21, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (912, 21, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (913, 21, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (914, 21, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (915, 21, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (916, 21, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (917, 21, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (918, 21, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (919, 21, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (920, 21, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (921, 21, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (922, 21, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (923, 21, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (924, 21, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (925, 21, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (926, 21, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (927, 21, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1270, 5, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1271, 5, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1272, 5, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1273, 5, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1274, 5, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1275, 5, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1276, 5, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1277, 5, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1278, 5, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1279, 5, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1280, 5, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1281, 5, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1282, 5, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1283, 5, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1284, 5, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1285, 5, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1286, 5, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1287, 5, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1288, 5, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1289, 5, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1290, 5, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1291, 5, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1292, 5, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1293, 5, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1294, 5, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1295, 5, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1296, 5, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1297, 27, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1298, 27, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1299, 27, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1300, 27, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1301, 27, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1302, 27, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1303, 27, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1304, 27, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1305, 27, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1306, 27, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1307, 27, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1308, 27, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1309, 27, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1310, 27, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1311, 27, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1312, 27, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1313, 27, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1314, 27, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1315, 27, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1316, 27, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1317, 27, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1318, 27, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1319, 27, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1320, 27, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1321, 27, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1322, 27, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1323, 27, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1324, 15, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1325, 15, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1326, 15, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1327, 15, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1328, 15, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1329, 15, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1330, 15, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1331, 15, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1332, 15, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1333, 15, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1334, 15, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1335, 15, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1336, 15, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1337, 15, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1338, 15, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1339, 15, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1340, 15, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1341, 15, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1342, 15, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1343, 15, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1344, 15, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1345, 15, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1346, 15, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1347, 15, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1348, 15, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1349, 15, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1350, 15, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1351, 16, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1352, 16, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1353, 16, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1354, 16, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1355, 16, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1356, 16, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1357, 16, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1358, 16, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1359, 16, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1360, 16, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1361, 16, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1362, 16, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1363, 16, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1364, 16, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1365, 16, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1366, 16, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1367, 16, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1368, 16, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1369, 16, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1370, 16, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1371, 16, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1372, 16, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1373, 16, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1374, 16, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1375, 16, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1376, 16, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1377, 16, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1378, 17, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1379, 17, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1380, 17, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1381, 17, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1382, 17, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1383, 17, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1384, 17, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1385, 17, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1386, 17, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1387, 17, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1388, 17, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1389, 17, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1390, 17, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1391, 17, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1392, 17, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1393, 17, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1394, 17, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1395, 17, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1396, 17, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1397, 17, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1398, 17, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1399, 17, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1400, 17, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1401, 17, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1402, 17, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1403, 17, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1404, 17, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1405, 6, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1406, 6, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1407, 6, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1408, 6, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1409, 6, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1410, 6, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1411, 6, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1412, 6, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1413, 6, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1414, 6, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1415, 6, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1416, 6, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1417, 6, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1418, 6, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1419, 6, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1420, 6, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1421, 6, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1422, 6, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1423, 6, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1424, 6, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1425, 6, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1426, 6, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1427, 6, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1428, 6, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1429, 6, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1430, 6, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1431, 6, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1432, 7, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1433, 7, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1434, 7, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1435, 7, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1436, 7, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1437, 7, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1438, 7, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1439, 7, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1440, 7, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1441, 7, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1442, 7, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1443, 7, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1444, 7, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1445, 7, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1446, 7, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1447, 7, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1448, 7, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1449, 7, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1450, 7, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1451, 7, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1452, 7, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1453, 7, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1454, 7, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1455, 7, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1456, 7, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1457, 7, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1458, 7, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1459, 8, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1460, 8, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1461, 8, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1462, 8, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1463, 8, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1464, 8, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1465, 8, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1466, 8, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1467, 8, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1468, 8, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1469, 8, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1470, 8, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1471, 8, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1472, 8, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1473, 8, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1474, 8, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1475, 8, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1476, 8, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1477, 8, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1478, 8, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1479, 8, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1480, 8, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1481, 8, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1482, 8, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1483, 8, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1484, 8, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1485, 8, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1486, 9, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1487, 9, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1488, 9, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1489, 9, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1490, 9, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1491, 9, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1492, 9, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1493, 9, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1494, 9, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1495, 9, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1496, 9, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1497, 9, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1498, 9, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1499, 9, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1500, 9, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1501, 9, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1502, 9, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1503, 9, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1504, 9, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1505, 9, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1506, 9, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1507, 9, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1508, 9, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1509, 9, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1510, 9, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1511, 9, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1512, 9, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1513, 10, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1514, 10, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1515, 10, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1516, 10, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1517, 10, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1518, 10, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1519, 10, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1520, 10, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1521, 10, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1522, 10, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1523, 10, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1524, 10, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1525, 10, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1526, 10, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1527, 10, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1528, 10, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1529, 10, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1530, 10, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1531, 10, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1532, 10, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1533, 10, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1534, 10, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1535, 10, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1536, 10, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1537, 10, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1538, 10, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1539, 10, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1540, 11, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1541, 11, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1542, 11, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1543, 11, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1544, 11, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1545, 11, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1546, 11, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1547, 11, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1548, 11, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1549, 11, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1550, 11, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1551, 11, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1552, 11, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1553, 11, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1554, 11, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1555, 11, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1556, 11, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1557, 11, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1558, 11, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1559, 11, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1560, 11, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1561, 11, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1562, 11, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1563, 11, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1564, 11, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1565, 11, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1566, 11, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1567, 4, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1568, 4, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1569, 4, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1570, 4, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1571, 4, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1572, 4, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1573, 4, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1574, 4, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1575, 4, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1576, 4, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1577, 4, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1578, 4, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1579, 4, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1580, 4, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1581, 4, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1582, 4, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1583, 4, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1584, 4, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1585, 4, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1586, 4, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1587, 4, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1588, 4, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1589, 4, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1590, 4, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1591, 4, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1592, 4, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1593, 4, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1594, 12, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1595, 12, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1596, 12, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1597, 12, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1598, 12, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1599, 12, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1600, 12, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1601, 12, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1602, 12, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1603, 12, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1604, 12, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1605, 12, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1606, 12, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1607, 12, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1608, 12, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1609, 12, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1610, 12, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1611, 12, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1612, 12, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1613, 12, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1614, 12, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1615, 12, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1616, 12, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1617, 12, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1618, 12, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1619, 12, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1620, 12, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1621, 13, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1622, 13, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1623, 13, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1624, 13, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1625, 13, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1626, 13, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1627, 13, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1628, 13, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1629, 13, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1630, 13, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1631, 13, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1632, 13, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1633, 13, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1634, 13, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1635, 13, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1636, 13, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1637, 13, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1638, 13, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1639, 13, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1640, 13, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1641, 13, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1642, 13, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1643, 13, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1644, 13, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1645, 13, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1646, 13, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1647, 13, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1648, 14, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1649, 14, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1650, 14, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1651, 14, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1652, 14, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1653, 14, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1654, 14, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1655, 14, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1656, 14, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1657, 14, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1658, 14, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1659, 14, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1660, 14, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1661, 14, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1662, 14, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1663, 14, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1664, 14, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1665, 14, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1666, 14, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1667, 14, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1668, 14, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1669, 14, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1670, 14, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1671, 14, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1672, 14, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1673, 14, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1674, 14, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1675, 18, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1676, 18, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1677, 18, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1678, 18, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1679, 18, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1680, 18, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1681, 18, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1682, 18, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1683, 18, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1684, 18, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1685, 18, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1686, 18, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1687, 18, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1688, 18, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1689, 18, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1690, 18, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1691, 18, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1692, 18, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1693, 18, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1694, 18, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1695, 18, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1696, 18, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1697, 18, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1698, 18, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1699, 18, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1700, 18, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1701, 18, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1702, 19, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1703, 19, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1704, 19, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1705, 19, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1706, 19, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1707, 19, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1708, 19, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1709, 19, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1710, 19, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1711, 19, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1712, 19, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1713, 19, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1714, 19, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1715, 19, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1716, 19, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1717, 19, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1718, 19, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1719, 19, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1720, 19, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1721, 19, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1722, 19, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1723, 19, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1724, 19, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1725, 19, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1726, 19, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1727, 19, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1728, 19, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1729, 20, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1730, 20, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1731, 20, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1732, 20, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1733, 20, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1734, 20, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1735, 20, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1736, 20, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1737, 20, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1738, 20, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1739, 20, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1740, 20, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1741, 20, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1742, 20, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1743, 20, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1744, 20, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1745, 20, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1746, 20, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1747, 20, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1748, 20, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1749, 20, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1750, 20, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1751, 20, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1752, 20, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1753, 20, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1754, 20, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1755, 20, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1756, 1, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1757, 1, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1758, 1, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1759, 1, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1760, 1, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1761, 1, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1762, 1, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1763, 1, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1764, 1, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1765, 1, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1766, 1, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1767, 1, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1768, 1, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1769, 1, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1770, 1, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1771, 1, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1772, 1, 63);
INSERT INTO `itom_base_role_operation_v2` VALUES (1773, 1, 71);
INSERT INTO `itom_base_role_operation_v2` VALUES (1774, 1, 100);
INSERT INTO `itom_base_role_operation_v2` VALUES (1775, 1, 107);
INSERT INTO `itom_base_role_operation_v2` VALUES (1776, 1, 116);
INSERT INTO `itom_base_role_operation_v2` VALUES (1777, 1, 126);
INSERT INTO `itom_base_role_operation_v2` VALUES (1778, 1, 133);
INSERT INTO `itom_base_role_operation_v2` VALUES (1779, 1, 135);
INSERT INTO `itom_base_role_operation_v2` VALUES (1780, 1, 142);
INSERT INTO `itom_base_role_operation_v2` VALUES (1781, 1, 145);
INSERT INTO `itom_base_role_operation_v2` VALUES (1782, 1, 154);
INSERT INTO `itom_base_role_operation_v2` VALUES (1783, 1, 156);
INSERT INTO `itom_base_role_operation_v2` VALUES (1784, 1, 161);
INSERT INTO `itom_base_role_operation_v2` VALUES (1785, 1, 169);
INSERT INTO `itom_base_role_operation_v2` VALUES (1786, 1, 173);
INSERT INTO `itom_base_role_operation_v2` VALUES (1787, 1, 177);
INSERT INTO `itom_base_role_operation_v2` VALUES (1788, 1, 183);
INSERT INTO `itom_base_role_operation_v2` VALUES (1789, 1, 189);
INSERT INTO `itom_base_role_operation_v2` VALUES (1790, 1, 195);
INSERT INTO `itom_base_role_operation_v2` VALUES (1791, 1, 203);
INSERT INTO `itom_base_role_operation_v2` VALUES (1792, 1, 208);
INSERT INTO `itom_base_role_operation_v2` VALUES (1793, 1, 210);
INSERT INTO `itom_base_role_operation_v2` VALUES (1794, 1, 214);
INSERT INTO `itom_base_role_operation_v2` VALUES (1795, 1, 221);
INSERT INTO `itom_base_role_operation_v2` VALUES (1796, 1, 223);
INSERT INTO `itom_base_role_operation_v2` VALUES (1797, 1, 226);
INSERT INTO `itom_base_role_operation_v2` VALUES (1798, 1, 231);
INSERT INTO `itom_base_role_operation_v2` VALUES (1799, 1, 234);
INSERT INTO `itom_base_role_operation_v2` VALUES (1800, 1, 242);
INSERT INTO `itom_base_role_operation_v2` VALUES (1801, 1, 252);
INSERT INTO `itom_base_role_operation_v2` VALUES (1802, 1, 259);
INSERT INTO `itom_base_role_operation_v2` VALUES (1803, 1, 269);
INSERT INTO `itom_base_role_operation_v2` VALUES (1804, 1, 277);
INSERT INTO `itom_base_role_operation_v2` VALUES (1805, 1, 285);
INSERT INTO `itom_base_role_operation_v2` VALUES (1806, 1, 291);
INSERT INTO `itom_base_role_operation_v2` VALUES (1807, 1, 293);
INSERT INTO `itom_base_role_operation_v2` VALUES (1808, 1, 299);
INSERT INTO `itom_base_role_operation_v2` VALUES (1809, 1, 302);
INSERT INTO `itom_base_role_operation_v2` VALUES (1810, 1, 307);
INSERT INTO `itom_base_role_operation_v2` VALUES (1811, 1, 317);
INSERT INTO `itom_base_role_operation_v2` VALUES (1812, 1, 319);
INSERT INTO `itom_base_role_operation_v2` VALUES (1813, 1, 320);
INSERT INTO `itom_base_role_operation_v2` VALUES (1814, 1, 324);
INSERT INTO `itom_base_role_operation_v2` VALUES (1815, 1, 328);
INSERT INTO `itom_base_role_operation_v2` VALUES (1816, 1, 336);
INSERT INTO `itom_base_role_operation_v2` VALUES (1817, 1, 345);
INSERT INTO `itom_base_role_operation_v2` VALUES (1818, 1, 355);
INSERT INTO `itom_base_role_operation_v2` VALUES (1819, 1, 669);
INSERT INTO `itom_base_role_operation_v2` VALUES (1820, 1, 356);
INSERT INTO `itom_base_role_operation_v2` VALUES (1821, 1, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1822, 1, 365);
INSERT INTO `itom_base_role_operation_v2` VALUES (1823, 1, 370);
INSERT INTO `itom_base_role_operation_v2` VALUES (1824, 1, 377);
INSERT INTO `itom_base_role_operation_v2` VALUES (1825, 1, 382);
INSERT INTO `itom_base_role_operation_v2` VALUES (1826, 1, 389);
INSERT INTO `itom_base_role_operation_v2` VALUES (1827, 1, 395);
INSERT INTO `itom_base_role_operation_v2` VALUES (1828, 1, 405);
INSERT INTO `itom_base_role_operation_v2` VALUES (1829, 1, 415);
INSERT INTO `itom_base_role_operation_v2` VALUES (1830, 1, 425);
INSERT INTO `itom_base_role_operation_v2` VALUES (1831, 1, 432);
INSERT INTO `itom_base_role_operation_v2` VALUES (1832, 1, 435);
INSERT INTO `itom_base_role_operation_v2` VALUES (1833, 1, 438);
INSERT INTO `itom_base_role_operation_v2` VALUES (1834, 1, 442);
INSERT INTO `itom_base_role_operation_v2` VALUES (1835, 1, 444);
INSERT INTO `itom_base_role_operation_v2` VALUES (1836, 1, 448);
INSERT INTO `itom_base_role_operation_v2` VALUES (1837, 1, 458);
INSERT INTO `itom_base_role_operation_v2` VALUES (1838, 1, 463);
INSERT INTO `itom_base_role_operation_v2` VALUES (1839, 1, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1840, 1, 473);
INSERT INTO `itom_base_role_operation_v2` VALUES (1841, 1, 681);
INSERT INTO `itom_base_role_operation_v2` VALUES (1842, 1, 689);
INSERT INTO `itom_base_role_operation_v2` VALUES (1843, 1, 735);
INSERT INTO `itom_base_role_operation_v2` VALUES (1844, 1, 737);
INSERT INTO `itom_base_role_operation_v2` VALUES (1845, 1, 745);
INSERT INTO `itom_base_role_operation_v2` VALUES (1846, 1, 754);
INSERT INTO `itom_base_role_operation_v2` VALUES (1847, 1, 755);
INSERT INTO `itom_base_role_operation_v2` VALUES (1848, 1, 757);
INSERT INTO `itom_base_role_operation_v2` VALUES (1849, 1, 761);
INSERT INTO `itom_base_role_operation_v2` VALUES (1850, 1, 770);
INSERT INTO `itom_base_role_operation_v2` VALUES (1851, 1, 775);
INSERT INTO `itom_base_role_operation_v2` VALUES (1852, 1, 782);
INSERT INTO `itom_base_role_operation_v2` VALUES (1853, 1, 785);
INSERT INTO `itom_base_role_operation_v2` VALUES (1854, 1, 792);
INSERT INTO `itom_base_role_operation_v2` VALUES (1855, 1, 796);
INSERT INTO `itom_base_role_operation_v2` VALUES (1856, 1, 483);
INSERT INTO `itom_base_role_operation_v2` VALUES (1857, 1, 484);
INSERT INTO `itom_base_role_operation_v2` VALUES (1858, 1, 486);
INSERT INTO `itom_base_role_operation_v2` VALUES (1859, 1, 493);
INSERT INTO `itom_base_role_operation_v2` VALUES (1860, 1, 494);
INSERT INTO `itom_base_role_operation_v2` VALUES (1861, 1, 503);
INSERT INTO `itom_base_role_operation_v2` VALUES (1862, 1, 507);
INSERT INTO `itom_base_role_operation_v2` VALUES (1863, 1, 719);
INSERT INTO `itom_base_role_operation_v2` VALUES (1864, 1, 512);
INSERT INTO `itom_base_role_operation_v2` VALUES (1865, 1, 516);
INSERT INTO `itom_base_role_operation_v2` VALUES (1866, 1, 522);
INSERT INTO `itom_base_role_operation_v2` VALUES (1867, 1, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1868, 1, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1869, 1, 545);
INSERT INTO `itom_base_role_operation_v2` VALUES (1870, 1, 554);
INSERT INTO `itom_base_role_operation_v2` VALUES (1871, 1, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1872, 1, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1873, 1, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1874, 1, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1875, 1, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1876, 1, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1877, 1, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1878, 1, 623);
INSERT INTO `itom_base_role_operation_v2` VALUES (1879, 1, 636);
INSERT INTO `itom_base_role_operation_v2` VALUES (1880, 1, 639);
INSERT INTO `itom_base_role_operation_v2` VALUES (1881, 1, 647);
INSERT INTO `itom_base_role_operation_v2` VALUES (1882, 1, 652);
INSERT INTO `itom_base_role_operation_v2` VALUES (1883, 2, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1884, 2, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1885, 2, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1886, 2, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1887, 2, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1888, 2, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1889, 2, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1890, 2, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1891, 2, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1892, 2, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1893, 2, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1894, 2, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1895, 2, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1896, 2, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1897, 2, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1898, 2, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1899, 2, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1900, 2, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1901, 2, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1902, 2, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1903, 2, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1904, 2, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1905, 2, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1906, 2, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1907, 2, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1908, 2, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1909, 2, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1910, 3, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1911, 3, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1912, 3, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1913, 3, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1914, 3, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1915, 3, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1916, 3, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1917, 3, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1918, 3, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1919, 3, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1920, 3, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1921, 3, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1922, 3, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1923, 3, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1924, 3, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1925, 3, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1926, 3, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1927, 3, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1928, 3, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1929, 3, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1930, 3, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1931, 3, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1932, 3, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1933, 3, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1934, 3, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1935, 3, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1936, 3, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (1937, 22, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (1938, 22, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (1939, 22, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (1940, 22, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (1941, 22, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (1942, 22, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (1943, 22, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (1944, 22, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (1945, 22, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (1946, 22, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (1947, 22, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (1948, 22, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (1949, 22, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (1950, 22, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (1951, 22, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (1952, 22, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (1953, 22, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (1954, 22, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (1955, 22, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (1956, 22, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (1957, 22, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (1958, 22, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (1959, 22, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (1960, 22, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (1961, 22, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (1962, 22, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (1963, 22, 603);
INSERT INTO `itom_base_role_operation_v2` VALUES (2155, 26, 1);
INSERT INTO `itom_base_role_operation_v2` VALUES (2156, 26, 4);
INSERT INTO `itom_base_role_operation_v2` VALUES (2157, 26, 5);
INSERT INTO `itom_base_role_operation_v2` VALUES (2158, 26, 20);
INSERT INTO `itom_base_role_operation_v2` VALUES (2159, 26, 28);
INSERT INTO `itom_base_role_operation_v2` VALUES (2160, 26, 30);
INSERT INTO `itom_base_role_operation_v2` VALUES (2161, 26, 88);
INSERT INTO `itom_base_role_operation_v2` VALUES (2162, 26, 97);
INSERT INTO `itom_base_role_operation_v2` VALUES (2163, 26, 34);
INSERT INTO `itom_base_role_operation_v2` VALUES (2164, 26, 44);
INSERT INTO `itom_base_role_operation_v2` VALUES (2165, 26, 53);
INSERT INTO `itom_base_role_operation_v2` VALUES (2166, 26, 54);
INSERT INTO `itom_base_role_operation_v2` VALUES (2167, 26, 62);
INSERT INTO `itom_base_role_operation_v2` VALUES (2168, 26, 73);
INSERT INTO `itom_base_role_operation_v2` VALUES (2169, 26, 81);
INSERT INTO `itom_base_role_operation_v2` VALUES (2170, 26, 82);
INSERT INTO `itom_base_role_operation_v2` VALUES (2171, 26, 252);
INSERT INTO `itom_base_role_operation_v2` VALUES (2172, 26, 328);
INSERT INTO `itom_base_role_operation_v2` VALUES (2173, 26, 360);
INSERT INTO `itom_base_role_operation_v2` VALUES (2174, 26, 467);
INSERT INTO `itom_base_role_operation_v2` VALUES (2175, 26, 530);
INSERT INTO `itom_base_role_operation_v2` VALUES (2176, 26, 540);
INSERT INTO `itom_base_role_operation_v2` VALUES (2177, 26, 563);
INSERT INTO `itom_base_role_operation_v2` VALUES (2178, 26, 569);
INSERT INTO `itom_base_role_operation_v2` VALUES (2179, 26, 575);
INSERT INTO `itom_base_role_operation_v2` VALUES (2180, 26, 579);
INSERT INTO `itom_base_role_operation_v2` VALUES (2181, 26, 583);
INSERT INTO `itom_base_role_operation_v2` VALUES (2182, 26, 600);
INSERT INTO `itom_base_role_operation_v2` VALUES (2183, 26, 603);

-- ----------------------------
-- Table structure for itom_base_roles
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_roles`;
CREATE TABLE `itom_base_roles`  (
  `default_role` int NOT NULL DEFAULT 20 COMMENT '默认角色标识符,20为普通角色，可删除，10为默认角色，不可删除',
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '角色名称',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '角色说明',
  `row_status` int NOT NULL DEFAULT 10 COMMENT '删除标识符',
  `version` int NOT NULL DEFAULT 0 COMMENT '版本控制位',
  `modify_status` int NULL DEFAULT NULL,
  `super_role` int NULL DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `idx_name`(`name` ASC) USING BTREE,
  INDEX `idx_remark`(`remark` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 28 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of itom_base_roles
-- ----------------------------
INSERT INTO `itom_base_roles` VALUES (10, 1, '系统管理员', '负责系统的管理工作', 10, 0, 10, 10);
INSERT INTO `itom_base_roles` VALUES (20, 2, '坐席主管', '管理坐席人员', 10, 0, 10, 20);
INSERT INTO `itom_base_roles` VALUES (20, 3, '坐席', '服务台坐席人员', 10, 0, 20, 20);
INSERT INTO `itom_base_roles` VALUES (20, 4, '工程师', '日常运维工作的实际执行者', 10, 0, 20, 20);
INSERT INTO `itom_base_roles` VALUES (20, 5, '工程师主管', '管理运维工程师', 10, 0, 10, 20);
INSERT INTO `itom_base_roles` VALUES (20, 6, '阿克苏分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 7, '阿勒泰分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 8, '巴州分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 9, '博州分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 10, '昌吉分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 11, '电信二枢纽灾备机房', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 12, '哈密分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 13, '和田分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 14, '鲤鱼山路档案库房', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 15, '喀什分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 16, '克拉玛依分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 17, '克州分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 18, '石河子分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 19, '塔城分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 20, '吐鲁番分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 21, '伊犁分行', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 22, '中心机房', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 23, '外来人员', '', 20, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 24, 'zbb', '', 20, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 25, '值班', '', 20, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 26, '值班监控', '', 10, 0, NULL, 20);
INSERT INTO `itom_base_roles` VALUES (20, 27, '建设路六楼外呼中心', '', 10, 0, NULL, 20);

-- ----------------------------
-- Table structure for itom_base_sessions
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_sessions`;
CREATE TABLE `itom_base_sessions`  (
  `session` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
  `expire_time` bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (`session`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_sessions
-- ----------------------------
INSERT INTO `itom_base_sessions` VALUES ('XSS_6-8-iXpzrggDKOtz8CTkISF-56eC9RQ3xj-6CwJqKOV3zcE', 1766737994);
INSERT INTO `itom_base_sessions` VALUES ('XSS_Afl-q90iU7MkaOTxtU7f7zOWPMYZaRxnIAt01Yv8bgQy3zAU', 1769168464);
INSERT INTO `itom_base_sessions` VALUES ('XSS_evL3eV1hosg0CW1vjaHcD60s4ThlzgRvVHw9jIrhv8c-L22X', 1768997384);
INSERT INTO `itom_base_sessions` VALUES ('XSS_gsiMUgsh-_NKKfiqAnLvayQ--0k-t6fhRblWjiqooGrCDYmn', 1768958511);
INSERT INTO `itom_base_sessions` VALUES ('XSS_hkkZDhJbcaKR4GUmAAWX1iSKJk1vDMpXEDLdXJXdoEhkEp4', 1766629428);
INSERT INTO `itom_base_sessions` VALUES ('XSS_I0nn_z-HfgQtMSYkt1sgLuHOpVL0v2fawEXarfcJk7rgcSTC', 1768992156);
INSERT INTO `itom_base_sessions` VALUES ('XSS_iFZenaF9WASnXxhqhyerCDxeeGa01Ox7DjuQeEps5j6-9ZY', 1766585220);
INSERT INTO `itom_base_sessions` VALUES ('XSS_k1mx2PoHF6MX_uTNUlhF2GE1hfr2wpnluznF2ausR_DEl4dZ', 1769159634);
INSERT INTO `itom_base_sessions` VALUES ('XSS_mpsL9LwFosnCL8Y_Fzjgs_l2NXAZV6dVkZGYU7jEd1VQ8bc', 1766719328);
INSERT INTO `itom_base_sessions` VALUES ('XSS_q8LfDGia-jwiOhth1XecwNzT23FhEfOZq7PCQbTgf07mZo0', 1766718846);
INSERT INTO `itom_base_sessions` VALUES ('XSS_sh9NDhvMK93q_8ENqD4XfVEG9UmOXYN2HTadnvstT-we9o8', 1766749005);
INSERT INTO `itom_base_sessions` VALUES ('XSS_SrXYWgzjJrq-i2lAl41bWPaPO66FSCMgf9Ur6dX05Jm-wfom', 1769001637);
INSERT INTO `itom_base_sessions` VALUES ('XSS_TZ1SYrll07CbPIxoJ47d2QRCmMYvxdhckAeMW_r6bOzEEcw', 1766625757);
INSERT INTO `itom_base_sessions` VALUES ('XSS_vD2SmB3dwGe6WBNds09L-05-S1k0ZYraXvz-onedAwGJBxI', 1766835016);
INSERT INTO `itom_base_sessions` VALUES ('XSS_xmlFSvNq-x8LOvvC6V0UJJSsXcT046NhtdlReJNyRzRwS0A', 1766741891);
INSERT INTO `itom_base_sessions` VALUES ('XSS_YCQ3LcnV60uoX0oOSfkyb5LiAOj--fI_qJ6azEcSE9VOxp4', 1766666467);
INSERT INTO `itom_base_sessions` VALUES ('XSS_yv6tBX_wGZcAxAVDx4TbqMNMOYTJqZYe_i3M5yrRFPKSufAC', 1769168274);
INSERT INTO `itom_base_sessions` VALUES ('XSS_ze763GxoEAqYcG7lLaW0U_PdlEBgbkU7fIGwfLiB7xWFbZw', 1766750902);
INSERT INTO `itom_base_sessions` VALUES ('XSS__9nSKZnT6IY04pcpMq3olbXGVr9qE8r_n0tc3A7w4qWv2xw', 1766652170);

-- ----------------------------
-- Table structure for itom_base_user_department
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_user_department`;
CREATE TABLE `itom_base_user_department`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '编号',
  `user_id` bigint NOT NULL COMMENT '系统用户编号',
  `department_id` bigint NOT NULL COMMENT '部门编号',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1290 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = COMPACT;

-- ----------------------------
-- Records of itom_base_user_department
-- ----------------------------
INSERT INTO `itom_base_user_department` VALUES (190, 38, 3);
INSERT INTO `itom_base_user_department` VALUES (191, 39, 3);
INSERT INTO `itom_base_user_department` VALUES (332, 43, 3);
INSERT INTO `itom_base_user_department` VALUES (360, 55, 3);
INSERT INTO `itom_base_user_department` VALUES (506, 240, 1);
INSERT INTO `itom_base_user_department` VALUES (510, 244, 1);
INSERT INTO `itom_base_user_department` VALUES (520, 254, 1);
INSERT INTO `itom_base_user_department` VALUES (525, 259, 1);
INSERT INTO `itom_base_user_department` VALUES (531, 265, 1);
INSERT INTO `itom_base_user_department` VALUES (550, 278, 9);
INSERT INTO `itom_base_user_department` VALUES (560, 284, 15);
INSERT INTO `itom_base_user_department` VALUES (561, 285, 16);
INSERT INTO `itom_base_user_department` VALUES (562, 282, 13);
INSERT INTO `itom_base_user_department` VALUES (565, 275, 6);
INSERT INTO `itom_base_user_department` VALUES (566, 283, 14);
INSERT INTO `itom_base_user_department` VALUES (567, 276, 7);
INSERT INTO `itom_base_user_department` VALUES (568, 279, 10);
INSERT INTO `itom_base_user_department` VALUES (569, 281, 12);
INSERT INTO `itom_base_user_department` VALUES (570, 274, 5);
INSERT INTO `itom_base_user_department` VALUES (571, 280, 11);
INSERT INTO `itom_base_user_department` VALUES (572, 1, 1);
INSERT INTO `itom_base_user_department` VALUES (596, 298, 3);
INSERT INTO `itom_base_user_department` VALUES (603, 301, 1);
INSERT INTO `itom_base_user_department` VALUES (614, 49, 3);
INSERT INTO `itom_base_user_department` VALUES (631, 309, 7);
INSERT INTO `itom_base_user_department` VALUES (685, 344, 5);
INSERT INTO `itom_base_user_department` VALUES (714, 18, 14);
INSERT INTO `itom_base_user_department` VALUES (715, 19, 17);
INSERT INTO `itom_base_user_department` VALUES (718, 350, 6);
INSERT INTO `itom_base_user_department` VALUES (720, 50, 3);
INSERT INTO `itom_base_user_department` VALUES (721, 351, 9);
INSERT INTO `itom_base_user_department` VALUES (723, 10, 15);
INSERT INTO `itom_base_user_department` VALUES (724, 12, 7);
INSERT INTO `itom_base_user_department` VALUES (725, 17, 13);
INSERT INTO `itom_base_user_department` VALUES (726, 15, 11);
INSERT INTO `itom_base_user_department` VALUES (727, 11, 8);
INSERT INTO `itom_base_user_department` VALUES (728, 335, 12);
INSERT INTO `itom_base_user_department` VALUES (730, 20, 17);
INSERT INTO `itom_base_user_department` VALUES (731, 330, 5);
INSERT INTO `itom_base_user_department` VALUES (732, 22, 5);
INSERT INTO `itom_base_user_department` VALUES (759, 8, 6);
INSERT INTO `itom_base_user_department` VALUES (765, 337, 11);
INSERT INTO `itom_base_user_department` VALUES (766, 142, 9);
INSERT INTO `itom_base_user_department` VALUES (768, 219, 12);
INSERT INTO `itom_base_user_department` VALUES (771, 128, 11);
INSERT INTO `itom_base_user_department` VALUES (773, 141, 9);
INSERT INTO `itom_base_user_department` VALUES (774, 148, 18);
INSERT INTO `itom_base_user_department` VALUES (775, 323, 9);
INSERT INTO `itom_base_user_department` VALUES (776, 338, 11);
INSERT INTO `itom_base_user_department` VALUES (778, 327, 9);
INSERT INTO `itom_base_user_department` VALUES (779, 209, 12);
INSERT INTO `itom_base_user_department` VALUES (780, 307, 9);
INSERT INTO `itom_base_user_department` VALUES (781, 129, 11);
INSERT INTO `itom_base_user_department` VALUES (783, 321, 9);
INSERT INTO `itom_base_user_department` VALUES (784, 322, 9);
INSERT INTO `itom_base_user_department` VALUES (785, 324, 9);
INSERT INTO `itom_base_user_department` VALUES (786, 196, 7);
INSERT INTO `itom_base_user_department` VALUES (787, 320, 9);
INSERT INTO `itom_base_user_department` VALUES (788, 326, 9);
INSERT INTO `itom_base_user_department` VALUES (789, 336, 11);
INSERT INTO `itom_base_user_department` VALUES (790, 353, 15);
INSERT INTO `itom_base_user_department` VALUES (807, 362, 16);
INSERT INTO `itom_base_user_department` VALUES (824, 79, 1);
INSERT INTO `itom_base_user_department` VALUES (825, 365, 9);
INSERT INTO `itom_base_user_department` VALUES (848, 372, 5);
INSERT INTO `itom_base_user_department` VALUES (849, 277, 8);
INSERT INTO `itom_base_user_department` VALUES (851, 374, 3);
INSERT INTO `itom_base_user_department` VALUES (852, 375, 3);
INSERT INTO `itom_base_user_department` VALUES (853, 376, 3);
INSERT INTO `itom_base_user_department` VALUES (854, 377, 3);
INSERT INTO `itom_base_user_department` VALUES (855, 29, 18);
INSERT INTO `itom_base_user_department` VALUES (856, 223, 3);
INSERT INTO `itom_base_user_department` VALUES (857, 251, 1);
INSERT INTO `itom_base_user_department` VALUES (858, 229, 1);
INSERT INTO `itom_base_user_department` VALUES (859, 267, 1);
INSERT INTO `itom_base_user_department` VALUES (860, 268, 1);
INSERT INTO `itom_base_user_department` VALUES (861, 238, 1);
INSERT INTO `itom_base_user_department` VALUES (862, 245, 1);
INSERT INTO `itom_base_user_department` VALUES (863, 269, 1);
INSERT INTO `itom_base_user_department` VALUES (864, 255, 1);
INSERT INTO `itom_base_user_department` VALUES (865, 252, 1);
INSERT INTO `itom_base_user_department` VALUES (866, 266, 1);
INSERT INTO `itom_base_user_department` VALUES (867, 237, 1);
INSERT INTO `itom_base_user_department` VALUES (868, 263, 1);
INSERT INTO `itom_base_user_department` VALUES (869, 257, 1);
INSERT INTO `itom_base_user_department` VALUES (870, 262, 1);
INSERT INTO `itom_base_user_department` VALUES (871, 227, 1);
INSERT INTO `itom_base_user_department` VALUES (872, 246, 1);
INSERT INTO `itom_base_user_department` VALUES (873, 248, 1);
INSERT INTO `itom_base_user_department` VALUES (874, 232, 1);
INSERT INTO `itom_base_user_department` VALUES (875, 234, 1);
INSERT INTO `itom_base_user_department` VALUES (876, 250, 1);
INSERT INTO `itom_base_user_department` VALUES (877, 224, 1);
INSERT INTO `itom_base_user_department` VALUES (878, 236, 1);
INSERT INTO `itom_base_user_department` VALUES (879, 239, 1);
INSERT INTO `itom_base_user_department` VALUES (880, 261, 1);
INSERT INTO `itom_base_user_department` VALUES (881, 270, 1);
INSERT INTO `itom_base_user_department` VALUES (882, 258, 1);
INSERT INTO `itom_base_user_department` VALUES (883, 221, 3);
INSERT INTO `itom_base_user_department` VALUES (884, 378, 3);
INSERT INTO `itom_base_user_department` VALUES (885, 379, 3);
INSERT INTO `itom_base_user_department` VALUES (886, 380, 3);
INSERT INTO `itom_base_user_department` VALUES (887, 381, 3);
INSERT INTO `itom_base_user_department` VALUES (888, 382, 3);
INSERT INTO `itom_base_user_department` VALUES (889, 383, 3);
INSERT INTO `itom_base_user_department` VALUES (890, 384, 3);
INSERT INTO `itom_base_user_department` VALUES (891, 385, 3);
INSERT INTO `itom_base_user_department` VALUES (892, 386, 3);
INSERT INTO `itom_base_user_department` VALUES (893, 387, 3);
INSERT INTO `itom_base_user_department` VALUES (894, 388, 3);
INSERT INTO `itom_base_user_department` VALUES (895, 389, 3);
INSERT INTO `itom_base_user_department` VALUES (896, 390, 3);
INSERT INTO `itom_base_user_department` VALUES (897, 391, 3);
INSERT INTO `itom_base_user_department` VALUES (898, 392, 3);
INSERT INTO `itom_base_user_department` VALUES (899, 393, 3);
INSERT INTO `itom_base_user_department` VALUES (900, 394, 3);
INSERT INTO `itom_base_user_department` VALUES (901, 395, 3);
INSERT INTO `itom_base_user_department` VALUES (902, 396, 3);
INSERT INTO `itom_base_user_department` VALUES (903, 397, 3);
INSERT INTO `itom_base_user_department` VALUES (904, 398, 3);
INSERT INTO `itom_base_user_department` VALUES (905, 399, 3);
INSERT INTO `itom_base_user_department` VALUES (906, 400, 3);
INSERT INTO `itom_base_user_department` VALUES (907, 401, 3);
INSERT INTO `itom_base_user_department` VALUES (908, 402, 3);
INSERT INTO `itom_base_user_department` VALUES (909, 403, 3);
INSERT INTO `itom_base_user_department` VALUES (910, 404, 3);
INSERT INTO `itom_base_user_department` VALUES (911, 405, 3);
INSERT INTO `itom_base_user_department` VALUES (912, 406, 3);
INSERT INTO `itom_base_user_department` VALUES (913, 407, 3);
INSERT INTO `itom_base_user_department` VALUES (914, 408, 3);
INSERT INTO `itom_base_user_department` VALUES (915, 409, 3);
INSERT INTO `itom_base_user_department` VALUES (916, 410, 3);
INSERT INTO `itom_base_user_department` VALUES (918, 412, 3);
INSERT INTO `itom_base_user_department` VALUES (919, 413, 3);
INSERT INTO `itom_base_user_department` VALUES (920, 414, 3);
INSERT INTO `itom_base_user_department` VALUES (921, 415, 3);
INSERT INTO `itom_base_user_department` VALUES (922, 416, 3);
INSERT INTO `itom_base_user_department` VALUES (923, 417, 3);
INSERT INTO `itom_base_user_department` VALUES (924, 418, 3);
INSERT INTO `itom_base_user_department` VALUES (925, 419, 3);
INSERT INTO `itom_base_user_department` VALUES (926, 420, 3);
INSERT INTO `itom_base_user_department` VALUES (927, 421, 3);
INSERT INTO `itom_base_user_department` VALUES (928, 422, 3);
INSERT INTO `itom_base_user_department` VALUES (929, 423, 3);
INSERT INTO `itom_base_user_department` VALUES (930, 424, 3);
INSERT INTO `itom_base_user_department` VALUES (931, 425, 3);
INSERT INTO `itom_base_user_department` VALUES (932, 426, 3);
INSERT INTO `itom_base_user_department` VALUES (933, 427, 3);
INSERT INTO `itom_base_user_department` VALUES (934, 428, 3);
INSERT INTO `itom_base_user_department` VALUES (935, 429, 3);
INSERT INTO `itom_base_user_department` VALUES (936, 430, 3);
INSERT INTO `itom_base_user_department` VALUES (937, 431, 3);
INSERT INTO `itom_base_user_department` VALUES (938, 432, 3);
INSERT INTO `itom_base_user_department` VALUES (939, 433, 3);
INSERT INTO `itom_base_user_department` VALUES (940, 434, 3);
INSERT INTO `itom_base_user_department` VALUES (941, 435, 3);
INSERT INTO `itom_base_user_department` VALUES (942, 436, 3);
INSERT INTO `itom_base_user_department` VALUES (943, 437, 3);
INSERT INTO `itom_base_user_department` VALUES (944, 438, 3);
INSERT INTO `itom_base_user_department` VALUES (945, 439, 3);
INSERT INTO `itom_base_user_department` VALUES (946, 440, 3);
INSERT INTO `itom_base_user_department` VALUES (947, 441, 3);
INSERT INTO `itom_base_user_department` VALUES (948, 442, 3);
INSERT INTO `itom_base_user_department` VALUES (949, 443, 3);
INSERT INTO `itom_base_user_department` VALUES (950, 444, 3);
INSERT INTO `itom_base_user_department` VALUES (952, 446, 3);
INSERT INTO `itom_base_user_department` VALUES (957, 215, 12);
INSERT INTO `itom_base_user_department` VALUES (958, 447, 8);
INSERT INTO `itom_base_user_department` VALUES (959, 448, 8);
INSERT INTO `itom_base_user_department` VALUES (960, 449, 8);
INSERT INTO `itom_base_user_department` VALUES (961, 450, 8);
INSERT INTO `itom_base_user_department` VALUES (962, 451, 8);
INSERT INTO `itom_base_user_department` VALUES (963, 452, 8);
INSERT INTO `itom_base_user_department` VALUES (964, 453, 11);
INSERT INTO `itom_base_user_department` VALUES (965, 454, 11);
INSERT INTO `itom_base_user_department` VALUES (966, 455, 11);
INSERT INTO `itom_base_user_department` VALUES (967, 456, 11);
INSERT INTO `itom_base_user_department` VALUES (968, 457, 11);
INSERT INTO `itom_base_user_department` VALUES (969, 458, 11);
INSERT INTO `itom_base_user_department` VALUES (970, 459, 11);
INSERT INTO `itom_base_user_department` VALUES (971, 460, 11);
INSERT INTO `itom_base_user_department` VALUES (972, 461, 11);
INSERT INTO `itom_base_user_department` VALUES (973, 462, 16);
INSERT INTO `itom_base_user_department` VALUES (974, 463, 16);
INSERT INTO `itom_base_user_department` VALUES (975, 464, 16);
INSERT INTO `itom_base_user_department` VALUES (976, 465, 16);
INSERT INTO `itom_base_user_department` VALUES (977, 466, 16);
INSERT INTO `itom_base_user_department` VALUES (978, 467, 16);
INSERT INTO `itom_base_user_department` VALUES (979, 468, 16);
INSERT INTO `itom_base_user_department` VALUES (980, 469, 16);
INSERT INTO `itom_base_user_department` VALUES (981, 470, 16);
INSERT INTO `itom_base_user_department` VALUES (982, 471, 16);
INSERT INTO `itom_base_user_department` VALUES (983, 478, 1);
INSERT INTO `itom_base_user_department` VALUES (984, 479, 1);
INSERT INTO `itom_base_user_department` VALUES (985, 480, 1);
INSERT INTO `itom_base_user_department` VALUES (986, 481, 1);
INSERT INTO `itom_base_user_department` VALUES (987, 482, 1);
INSERT INTO `itom_base_user_department` VALUES (988, 483, 1);
INSERT INTO `itom_base_user_department` VALUES (989, 484, 1);
INSERT INTO `itom_base_user_department` VALUES (990, 485, 1);
INSERT INTO `itom_base_user_department` VALUES (991, 486, 1);
INSERT INTO `itom_base_user_department` VALUES (992, 487, 1);
INSERT INTO `itom_base_user_department` VALUES (993, 488, 1);
INSERT INTO `itom_base_user_department` VALUES (994, 489, 1);
INSERT INTO `itom_base_user_department` VALUES (995, 490, 1);
INSERT INTO `itom_base_user_department` VALUES (996, 491, 1);
INSERT INTO `itom_base_user_department` VALUES (997, 492, 1);
INSERT INTO `itom_base_user_department` VALUES (998, 493, 1);
INSERT INTO `itom_base_user_department` VALUES (999, 494, 1);
INSERT INTO `itom_base_user_department` VALUES (1000, 495, 1);
INSERT INTO `itom_base_user_department` VALUES (1001, 496, 1);
INSERT INTO `itom_base_user_department` VALUES (1002, 497, 1);
INSERT INTO `itom_base_user_department` VALUES (1003, 498, 1);
INSERT INTO `itom_base_user_department` VALUES (1004, 499, 1);
INSERT INTO `itom_base_user_department` VALUES (1005, 500, 1);
INSERT INTO `itom_base_user_department` VALUES (1006, 501, 1);
INSERT INTO `itom_base_user_department` VALUES (1007, 502, 1);
INSERT INTO `itom_base_user_department` VALUES (1008, 503, 1);
INSERT INTO `itom_base_user_department` VALUES (1009, 504, 1);
INSERT INTO `itom_base_user_department` VALUES (1010, 505, 1);
INSERT INTO `itom_base_user_department` VALUES (1011, 506, 1);
INSERT INTO `itom_base_user_department` VALUES (1012, 507, 1);
INSERT INTO `itom_base_user_department` VALUES (1013, 508, 1);
INSERT INTO `itom_base_user_department` VALUES (1014, 509, 1);
INSERT INTO `itom_base_user_department` VALUES (1015, 510, 1);
INSERT INTO `itom_base_user_department` VALUES (1016, 511, 1);
INSERT INTO `itom_base_user_department` VALUES (1017, 512, 1);
INSERT INTO `itom_base_user_department` VALUES (1018, 513, 1);
INSERT INTO `itom_base_user_department` VALUES (1019, 514, 1);
INSERT INTO `itom_base_user_department` VALUES (1020, 515, 1);
INSERT INTO `itom_base_user_department` VALUES (1021, 516, 1);
INSERT INTO `itom_base_user_department` VALUES (1022, 517, 1);
INSERT INTO `itom_base_user_department` VALUES (1023, 518, 1);
INSERT INTO `itom_base_user_department` VALUES (1024, 519, 1);
INSERT INTO `itom_base_user_department` VALUES (1025, 520, 1);
INSERT INTO `itom_base_user_department` VALUES (1026, 521, 1);
INSERT INTO `itom_base_user_department` VALUES (1027, 522, 1);
INSERT INTO `itom_base_user_department` VALUES (1028, 523, 1);
INSERT INTO `itom_base_user_department` VALUES (1029, 524, 1);
INSERT INTO `itom_base_user_department` VALUES (1030, 525, 1);
INSERT INTO `itom_base_user_department` VALUES (1031, 526, 1);
INSERT INTO `itom_base_user_department` VALUES (1032, 527, 1);
INSERT INTO `itom_base_user_department` VALUES (1033, 528, 1);
INSERT INTO `itom_base_user_department` VALUES (1034, 529, 1);
INSERT INTO `itom_base_user_department` VALUES (1035, 530, 1);
INSERT INTO `itom_base_user_department` VALUES (1036, 531, 1);
INSERT INTO `itom_base_user_department` VALUES (1037, 532, 1);
INSERT INTO `itom_base_user_department` VALUES (1038, 533, 1);
INSERT INTO `itom_base_user_department` VALUES (1039, 534, 1);
INSERT INTO `itom_base_user_department` VALUES (1040, 535, 1);
INSERT INTO `itom_base_user_department` VALUES (1041, 536, 1);
INSERT INTO `itom_base_user_department` VALUES (1042, 537, 1);
INSERT INTO `itom_base_user_department` VALUES (1043, 538, 1);
INSERT INTO `itom_base_user_department` VALUES (1044, 539, 1);
INSERT INTO `itom_base_user_department` VALUES (1045, 540, 1);
INSERT INTO `itom_base_user_department` VALUES (1046, 541, 1);
INSERT INTO `itom_base_user_department` VALUES (1047, 542, 1);
INSERT INTO `itom_base_user_department` VALUES (1048, 543, 1);
INSERT INTO `itom_base_user_department` VALUES (1049, 544, 1);
INSERT INTO `itom_base_user_department` VALUES (1050, 545, 1);
INSERT INTO `itom_base_user_department` VALUES (1051, 546, 1);
INSERT INTO `itom_base_user_department` VALUES (1052, 547, 1);
INSERT INTO `itom_base_user_department` VALUES (1053, 548, 1);
INSERT INTO `itom_base_user_department` VALUES (1054, 549, 1);
INSERT INTO `itom_base_user_department` VALUES (1055, 550, 1);
INSERT INTO `itom_base_user_department` VALUES (1056, 551, 1);
INSERT INTO `itom_base_user_department` VALUES (1057, 552, 1);
INSERT INTO `itom_base_user_department` VALUES (1058, 553, 1);
INSERT INTO `itom_base_user_department` VALUES (1059, 554, 1);
INSERT INTO `itom_base_user_department` VALUES (1060, 555, 1);
INSERT INTO `itom_base_user_department` VALUES (1061, 556, 1);
INSERT INTO `itom_base_user_department` VALUES (1062, 557, 1);
INSERT INTO `itom_base_user_department` VALUES (1063, 558, 1);
INSERT INTO `itom_base_user_department` VALUES (1064, 559, 1);
INSERT INTO `itom_base_user_department` VALUES (1065, 560, 1);
INSERT INTO `itom_base_user_department` VALUES (1066, 561, 1);
INSERT INTO `itom_base_user_department` VALUES (1067, 562, 1);
INSERT INTO `itom_base_user_department` VALUES (1068, 563, 1);
INSERT INTO `itom_base_user_department` VALUES (1069, 564, 1);
INSERT INTO `itom_base_user_department` VALUES (1070, 565, 1);
INSERT INTO `itom_base_user_department` VALUES (1071, 566, 1);
INSERT INTO `itom_base_user_department` VALUES (1072, 567, 1);
INSERT INTO `itom_base_user_department` VALUES (1073, 568, 1);
INSERT INTO `itom_base_user_department` VALUES (1074, 569, 1);
INSERT INTO `itom_base_user_department` VALUES (1075, 570, 1);
INSERT INTO `itom_base_user_department` VALUES (1076, 571, 1);
INSERT INTO `itom_base_user_department` VALUES (1077, 572, 1);
INSERT INTO `itom_base_user_department` VALUES (1078, 573, 1);
INSERT INTO `itom_base_user_department` VALUES (1079, 574, 1);
INSERT INTO `itom_base_user_department` VALUES (1080, 575, 1);
INSERT INTO `itom_base_user_department` VALUES (1081, 576, 1);
INSERT INTO `itom_base_user_department` VALUES (1083, 14, 10);
INSERT INTO `itom_base_user_department` VALUES (1109, 590, 19);
INSERT INTO `itom_base_user_department` VALUES (1110, 286, 17);
INSERT INTO `itom_base_user_department` VALUES (1114, 591, 5);
INSERT INTO `itom_base_user_department` VALUES (1115, 592, 5);
INSERT INTO `itom_base_user_department` VALUES (1116, 593, 5);
INSERT INTO `itom_base_user_department` VALUES (1117, 594, 17);
INSERT INTO `itom_base_user_department` VALUES (1119, 595, 18);
INSERT INTO `itom_base_user_department` VALUES (1120, 596, 12);
INSERT INTO `itom_base_user_department` VALUES (1121, 597, 1);
INSERT INTO `itom_base_user_department` VALUES (1135, 601, 1);
INSERT INTO `itom_base_user_department` VALUES (1136, 610, 1);
INSERT INTO `itom_base_user_department` VALUES (1137, 598, 1);
INSERT INTO `itom_base_user_department` VALUES (1138, 599, 1);
INSERT INTO `itom_base_user_department` VALUES (1139, 600, 1);
INSERT INTO `itom_base_user_department` VALUES (1140, 602, 1);
INSERT INTO `itom_base_user_department` VALUES (1141, 603, 1);
INSERT INTO `itom_base_user_department` VALUES (1142, 604, 1);
INSERT INTO `itom_base_user_department` VALUES (1143, 605, 1);
INSERT INTO `itom_base_user_department` VALUES (1144, 606, 1);
INSERT INTO `itom_base_user_department` VALUES (1145, 607, 1);
INSERT INTO `itom_base_user_department` VALUES (1146, 608, 1);
INSERT INTO `itom_base_user_department` VALUES (1147, 609, 1);
INSERT INTO `itom_base_user_department` VALUES (1180, 615, 3);
INSERT INTO `itom_base_user_department` VALUES (1181, 616, 16);
INSERT INTO `itom_base_user_department` VALUES (1182, 617, 3);
INSERT INTO `itom_base_user_department` VALUES (1196, 52, 3);
INSERT INTO `itom_base_user_department` VALUES (1209, 624, 1);
INSERT INTO `itom_base_user_department` VALUES (1218, 625, 1);
INSERT INTO `itom_base_user_department` VALUES (1219, 618, 3);
INSERT INTO `itom_base_user_department` VALUES (1225, 619, 3);
INSERT INTO `itom_base_user_department` VALUES (1227, 626, 1);
INSERT INTO `itom_base_user_department` VALUES (1230, 41, 3);
INSERT INTO `itom_base_user_department` VALUES (1231, 54, 3);
INSERT INTO `itom_base_user_department` VALUES (1233, 354, 3);
INSERT INTO `itom_base_user_department` VALUES (1236, 578, 3);
INSERT INTO `itom_base_user_department` VALUES (1238, 63, 3);
INSERT INTO `itom_base_user_department` VALUES (1240, 68, 3);
INSERT INTO `itom_base_user_department` VALUES (1241, 61, 3);
INSERT INTO `itom_base_user_department` VALUES (1242, 80, 3);
INSERT INTO `itom_base_user_department` VALUES (1243, 59, 3);
INSERT INTO `itom_base_user_department` VALUES (1246, 48, 3);
INSERT INTO `itom_base_user_department` VALUES (1248, 585, 3);
INSERT INTO `itom_base_user_department` VALUES (1249, 577, 3);
INSERT INTO `itom_base_user_department` VALUES (1250, 60, 3);
INSERT INTO `itom_base_user_department` VALUES (1251, 62, 3);
INSERT INTO `itom_base_user_department` VALUES (1252, 58, 3);
INSERT INTO `itom_base_user_department` VALUES (1253, 366, 3);
INSERT INTO `itom_base_user_department` VALUES (1254, 586, 3);
INSERT INTO `itom_base_user_department` VALUES (1255, 348, 3);
INSERT INTO `itom_base_user_department` VALUES (1256, 627, 1);
INSERT INTO `itom_base_user_department` VALUES (1257, 66, 3);
INSERT INTO `itom_base_user_department` VALUES (1258, 65, 3);
INSERT INTO `itom_base_user_department` VALUES (1259, 358, 3);
INSERT INTO `itom_base_user_department` VALUES (1260, 72, 3);
INSERT INTO `itom_base_user_department` VALUES (1263, 272, 1);
INSERT INTO `itom_base_user_department` VALUES (1265, 346, 3);
INSERT INTO `itom_base_user_department` VALUES (1267, 70, 3);
INSERT INTO `itom_base_user_department` VALUES (1268, 580, 3);
INSERT INTO `itom_base_user_department` VALUES (1269, 363, 3);
INSERT INTO `itom_base_user_department` VALUES (1270, 622, 3);
INSERT INTO `itom_base_user_department` VALUES (1271, 53, 3);
INSERT INTO `itom_base_user_department` VALUES (1272, 47, 3);
INSERT INTO `itom_base_user_department` VALUES (1273, 292, 1);
INSERT INTO `itom_base_user_department` VALUES (1274, 71, 3);
INSERT INTO `itom_base_user_department` VALUES (1276, 356, 3);
INSERT INTO `itom_base_user_department` VALUES (1280, 3, 3);
INSERT INTO `itom_base_user_department` VALUES (1281, 628, 1);
INSERT INTO `itom_base_user_department` VALUES (1282, 628, 3);
INSERT INTO `itom_base_user_department` VALUES (1283, 288, 1);
INSERT INTO `itom_base_user_department` VALUES (1284, 629, 3);
INSERT INTO `itom_base_user_department` VALUES (1285, 630, 3);
INSERT INTO `itom_base_user_department` VALUES (1286, 631, 3);
INSERT INTO `itom_base_user_department` VALUES (1287, 632, 3);
INSERT INTO `itom_base_user_department` VALUES (1288, 633, 3);
INSERT INTO `itom_base_user_department` VALUES (1289, 634, 3);

-- ----------------------------
-- Table structure for itom_base_user_period
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_user_period`;
CREATE TABLE `itom_base_user_period`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `user_id` bigint NOT NULL DEFAULT 59 COMMENT '用户ID',
  `time_period_id` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '时间段ID',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_user_period
-- ----------------------------

-- ----------------------------
-- Table structure for itom_base_user_role
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_user_role`;
CREATE TABLE `itom_base_user_role`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `user_id` bigint NOT NULL DEFAULT 59 COMMENT '用户ID',
  `role_id` bigint NOT NULL COMMENT '角色ID',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 736 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_user_role
-- ----------------------------
INSERT INTO `itom_base_user_role` VALUES (233, 240, 1);
INSERT INTO `itom_base_user_role` VALUES (237, 244, 1);
INSERT INTO `itom_base_user_role` VALUES (247, 254, 1);
INSERT INTO `itom_base_user_role` VALUES (252, 259, 1);
INSERT INTO `itom_base_user_role` VALUES (258, 265, 1);
INSERT INTO `itom_base_user_role` VALUES (272, 278, 10);
INSERT INTO `itom_base_user_role` VALUES (282, 284, 7);
INSERT INTO `itom_base_user_role` VALUES (283, 285, 6);
INSERT INTO `itom_base_user_role` VALUES (284, 282, 8);
INSERT INTO `itom_base_user_role` VALUES (287, 275, 16);
INSERT INTO `itom_base_user_role` VALUES (288, 283, 17);
INSERT INTO `itom_base_user_role` VALUES (289, 276, 19);
INSERT INTO `itom_base_user_role` VALUES (290, 279, 20);
INSERT INTO `itom_base_user_role` VALUES (291, 281, 21);
INSERT INTO `itom_base_user_role` VALUES (292, 274, 15);
INSERT INTO `itom_base_user_role` VALUES (293, 280, 9);
INSERT INTO `itom_base_user_role` VALUES (294, 1, 1);
INSERT INTO `itom_base_user_role` VALUES (308, 298, 1);
INSERT INTO `itom_base_user_role` VALUES (318, 309, 19);
INSERT INTO `itom_base_user_role` VALUES (367, 3, 1);
INSERT INTO `itom_base_user_role` VALUES (368, 18, 17);
INSERT INTO `itom_base_user_role` VALUES (369, 19, 18);
INSERT INTO `itom_base_user_role` VALUES (371, 350, 16);
INSERT INTO `itom_base_user_role` VALUES (373, 50, 1);
INSERT INTO `itom_base_user_role` VALUES (374, 351, 10);
INSERT INTO `itom_base_user_role` VALUES (376, 10, 7);
INSERT INTO `itom_base_user_role` VALUES (377, 12, 19);
INSERT INTO `itom_base_user_role` VALUES (378, 17, 8);
INSERT INTO `itom_base_user_role` VALUES (379, 15, 9);
INSERT INTO `itom_base_user_role` VALUES (380, 11, 12);
INSERT INTO `itom_base_user_role` VALUES (381, 335, 21);
INSERT INTO `itom_base_user_role` VALUES (383, 20, 18);
INSERT INTO `itom_base_user_role` VALUES (384, 330, 15);
INSERT INTO `itom_base_user_role` VALUES (385, 22, 15);
INSERT INTO `itom_base_user_role` VALUES (409, 8, 16);
INSERT INTO `itom_base_user_role` VALUES (415, 337, 9);
INSERT INTO `itom_base_user_role` VALUES (416, 142, 10);
INSERT INTO `itom_base_user_role` VALUES (418, 219, 21);
INSERT INTO `itom_base_user_role` VALUES (421, 128, 9);
INSERT INTO `itom_base_user_role` VALUES (423, 141, 10);
INSERT INTO `itom_base_user_role` VALUES (424, 148, 13);
INSERT INTO `itom_base_user_role` VALUES (425, 323, 10);
INSERT INTO `itom_base_user_role` VALUES (426, 338, 9);
INSERT INTO `itom_base_user_role` VALUES (428, 327, 10);
INSERT INTO `itom_base_user_role` VALUES (429, 209, 21);
INSERT INTO `itom_base_user_role` VALUES (430, 307, 10);
INSERT INTO `itom_base_user_role` VALUES (431, 129, 9);
INSERT INTO `itom_base_user_role` VALUES (433, 321, 10);
INSERT INTO `itom_base_user_role` VALUES (434, 322, 10);
INSERT INTO `itom_base_user_role` VALUES (435, 324, 10);
INSERT INTO `itom_base_user_role` VALUES (436, 196, 19);
INSERT INTO `itom_base_user_role` VALUES (437, 320, 10);
INSERT INTO `itom_base_user_role` VALUES (438, 326, 10);
INSERT INTO `itom_base_user_role` VALUES (439, 336, 9);
INSERT INTO `itom_base_user_role` VALUES (440, 353, 7);
INSERT INTO `itom_base_user_role` VALUES (442, 362, 6);
INSERT INTO `itom_base_user_role` VALUES (445, 79, 1);
INSERT INTO `itom_base_user_role` VALUES (446, 365, 10);
INSERT INTO `itom_base_user_role` VALUES (449, 277, 12);
INSERT INTO `itom_base_user_role` VALUES (451, 374, 3);
INSERT INTO `itom_base_user_role` VALUES (452, 375, 14);
INSERT INTO `itom_base_user_role` VALUES (453, 376, 14);
INSERT INTO `itom_base_user_role` VALUES (454, 377, 14);
INSERT INTO `itom_base_user_role` VALUES (455, 29, 13);
INSERT INTO `itom_base_user_role` VALUES (456, 372, 15);
INSERT INTO `itom_base_user_role` VALUES (457, 223, 1);
INSERT INTO `itom_base_user_role` VALUES (458, 251, 1);
INSERT INTO `itom_base_user_role` VALUES (459, 229, 1);
INSERT INTO `itom_base_user_role` VALUES (460, 267, 1);
INSERT INTO `itom_base_user_role` VALUES (461, 268, 1);
INSERT INTO `itom_base_user_role` VALUES (462, 238, 1);
INSERT INTO `itom_base_user_role` VALUES (463, 245, 1);
INSERT INTO `itom_base_user_role` VALUES (464, 269, 1);
INSERT INTO `itom_base_user_role` VALUES (465, 255, 1);
INSERT INTO `itom_base_user_role` VALUES (466, 252, 1);
INSERT INTO `itom_base_user_role` VALUES (467, 266, 1);
INSERT INTO `itom_base_user_role` VALUES (468, 237, 1);
INSERT INTO `itom_base_user_role` VALUES (469, 263, 1);
INSERT INTO `itom_base_user_role` VALUES (470, 257, 1);
INSERT INTO `itom_base_user_role` VALUES (471, 262, 1);
INSERT INTO `itom_base_user_role` VALUES (472, 227, 1);
INSERT INTO `itom_base_user_role` VALUES (473, 246, 1);
INSERT INTO `itom_base_user_role` VALUES (474, 248, 1);
INSERT INTO `itom_base_user_role` VALUES (475, 232, 1);
INSERT INTO `itom_base_user_role` VALUES (476, 234, 1);
INSERT INTO `itom_base_user_role` VALUES (477, 250, 1);
INSERT INTO `itom_base_user_role` VALUES (478, 224, 1);
INSERT INTO `itom_base_user_role` VALUES (479, 236, 1);
INSERT INTO `itom_base_user_role` VALUES (480, 239, 1);
INSERT INTO `itom_base_user_role` VALUES (481, 261, 1);
INSERT INTO `itom_base_user_role` VALUES (482, 270, 1);
INSERT INTO `itom_base_user_role` VALUES (483, 258, 1);
INSERT INTO `itom_base_user_role` VALUES (484, 221, 1);
INSERT INTO `itom_base_user_role` VALUES (485, 378, 1);
INSERT INTO `itom_base_user_role` VALUES (486, 379, 1);
INSERT INTO `itom_base_user_role` VALUES (487, 380, 1);
INSERT INTO `itom_base_user_role` VALUES (488, 381, 1);
INSERT INTO `itom_base_user_role` VALUES (489, 382, 1);
INSERT INTO `itom_base_user_role` VALUES (490, 383, 1);
INSERT INTO `itom_base_user_role` VALUES (491, 384, 1);
INSERT INTO `itom_base_user_role` VALUES (492, 385, 1);
INSERT INTO `itom_base_user_role` VALUES (493, 386, 1);
INSERT INTO `itom_base_user_role` VALUES (494, 387, 1);
INSERT INTO `itom_base_user_role` VALUES (495, 388, 1);
INSERT INTO `itom_base_user_role` VALUES (496, 389, 1);
INSERT INTO `itom_base_user_role` VALUES (497, 390, 1);
INSERT INTO `itom_base_user_role` VALUES (498, 391, 1);
INSERT INTO `itom_base_user_role` VALUES (499, 392, 1);
INSERT INTO `itom_base_user_role` VALUES (500, 393, 1);
INSERT INTO `itom_base_user_role` VALUES (501, 394, 1);
INSERT INTO `itom_base_user_role` VALUES (502, 395, 1);
INSERT INTO `itom_base_user_role` VALUES (503, 396, 1);
INSERT INTO `itom_base_user_role` VALUES (504, 397, 1);
INSERT INTO `itom_base_user_role` VALUES (505, 398, 1);
INSERT INTO `itom_base_user_role` VALUES (506, 399, 1);
INSERT INTO `itom_base_user_role` VALUES (507, 400, 1);
INSERT INTO `itom_base_user_role` VALUES (508, 401, 1);
INSERT INTO `itom_base_user_role` VALUES (509, 402, 1);
INSERT INTO `itom_base_user_role` VALUES (510, 403, 1);
INSERT INTO `itom_base_user_role` VALUES (511, 404, 1);
INSERT INTO `itom_base_user_role` VALUES (512, 405, 1);
INSERT INTO `itom_base_user_role` VALUES (513, 406, 1);
INSERT INTO `itom_base_user_role` VALUES (514, 407, 1);
INSERT INTO `itom_base_user_role` VALUES (515, 408, 1);
INSERT INTO `itom_base_user_role` VALUES (516, 409, 1);
INSERT INTO `itom_base_user_role` VALUES (517, 410, 1);
INSERT INTO `itom_base_user_role` VALUES (519, 412, 1);
INSERT INTO `itom_base_user_role` VALUES (520, 413, 1);
INSERT INTO `itom_base_user_role` VALUES (521, 414, 1);
INSERT INTO `itom_base_user_role` VALUES (522, 415, 1);
INSERT INTO `itom_base_user_role` VALUES (523, 416, 1);
INSERT INTO `itom_base_user_role` VALUES (524, 417, 1);
INSERT INTO `itom_base_user_role` VALUES (525, 418, 1);
INSERT INTO `itom_base_user_role` VALUES (526, 419, 1);
INSERT INTO `itom_base_user_role` VALUES (527, 420, 1);
INSERT INTO `itom_base_user_role` VALUES (528, 421, 1);
INSERT INTO `itom_base_user_role` VALUES (529, 422, 1);
INSERT INTO `itom_base_user_role` VALUES (530, 423, 1);
INSERT INTO `itom_base_user_role` VALUES (531, 424, 1);
INSERT INTO `itom_base_user_role` VALUES (532, 425, 1);
INSERT INTO `itom_base_user_role` VALUES (533, 426, 1);
INSERT INTO `itom_base_user_role` VALUES (534, 427, 1);
INSERT INTO `itom_base_user_role` VALUES (535, 428, 1);
INSERT INTO `itom_base_user_role` VALUES (536, 429, 1);
INSERT INTO `itom_base_user_role` VALUES (537, 430, 1);
INSERT INTO `itom_base_user_role` VALUES (538, 431, 1);
INSERT INTO `itom_base_user_role` VALUES (539, 432, 1);
INSERT INTO `itom_base_user_role` VALUES (540, 433, 1);
INSERT INTO `itom_base_user_role` VALUES (541, 434, 1);
INSERT INTO `itom_base_user_role` VALUES (542, 435, 1);
INSERT INTO `itom_base_user_role` VALUES (543, 436, 1);
INSERT INTO `itom_base_user_role` VALUES (544, 437, 1);
INSERT INTO `itom_base_user_role` VALUES (545, 438, 1);
INSERT INTO `itom_base_user_role` VALUES (546, 439, 1);
INSERT INTO `itom_base_user_role` VALUES (547, 440, 1);
INSERT INTO `itom_base_user_role` VALUES (548, 441, 1);
INSERT INTO `itom_base_user_role` VALUES (549, 442, 1);
INSERT INTO `itom_base_user_role` VALUES (550, 443, 1);
INSERT INTO `itom_base_user_role` VALUES (551, 444, 1);
INSERT INTO `itom_base_user_role` VALUES (553, 446, 1);
INSERT INTO `itom_base_user_role` VALUES (555, 215, 21);
INSERT INTO `itom_base_user_role` VALUES (556, 447, 12);
INSERT INTO `itom_base_user_role` VALUES (557, 448, 12);
INSERT INTO `itom_base_user_role` VALUES (558, 449, 12);
INSERT INTO `itom_base_user_role` VALUES (559, 450, 12);
INSERT INTO `itom_base_user_role` VALUES (560, 451, 12);
INSERT INTO `itom_base_user_role` VALUES (561, 452, 12);
INSERT INTO `itom_base_user_role` VALUES (562, 453, 9);
INSERT INTO `itom_base_user_role` VALUES (563, 454, 9);
INSERT INTO `itom_base_user_role` VALUES (564, 455, 9);
INSERT INTO `itom_base_user_role` VALUES (565, 456, 9);
INSERT INTO `itom_base_user_role` VALUES (566, 457, 9);
INSERT INTO `itom_base_user_role` VALUES (567, 458, 9);
INSERT INTO `itom_base_user_role` VALUES (568, 459, 9);
INSERT INTO `itom_base_user_role` VALUES (569, 460, 9);
INSERT INTO `itom_base_user_role` VALUES (570, 461, 9);
INSERT INTO `itom_base_user_role` VALUES (571, 462, 6);
INSERT INTO `itom_base_user_role` VALUES (572, 463, 6);
INSERT INTO `itom_base_user_role` VALUES (573, 464, 6);
INSERT INTO `itom_base_user_role` VALUES (574, 465, 6);
INSERT INTO `itom_base_user_role` VALUES (575, 466, 6);
INSERT INTO `itom_base_user_role` VALUES (576, 467, 6);
INSERT INTO `itom_base_user_role` VALUES (577, 468, 6);
INSERT INTO `itom_base_user_role` VALUES (578, 469, 6);
INSERT INTO `itom_base_user_role` VALUES (579, 470, 6);
INSERT INTO `itom_base_user_role` VALUES (580, 471, 6);
INSERT INTO `itom_base_user_role` VALUES (581, 478, 1);
INSERT INTO `itom_base_user_role` VALUES (582, 479, 1);
INSERT INTO `itom_base_user_role` VALUES (583, 480, 1);
INSERT INTO `itom_base_user_role` VALUES (584, 481, 1);
INSERT INTO `itom_base_user_role` VALUES (585, 482, 1);
INSERT INTO `itom_base_user_role` VALUES (586, 483, 1);
INSERT INTO `itom_base_user_role` VALUES (587, 484, 1);
INSERT INTO `itom_base_user_role` VALUES (588, 485, 1);
INSERT INTO `itom_base_user_role` VALUES (589, 486, 1);
INSERT INTO `itom_base_user_role` VALUES (590, 487, 1);
INSERT INTO `itom_base_user_role` VALUES (591, 488, 1);
INSERT INTO `itom_base_user_role` VALUES (592, 489, 1);
INSERT INTO `itom_base_user_role` VALUES (593, 490, 1);
INSERT INTO `itom_base_user_role` VALUES (594, 491, 1);
INSERT INTO `itom_base_user_role` VALUES (595, 492, 1);
INSERT INTO `itom_base_user_role` VALUES (596, 493, 1);
INSERT INTO `itom_base_user_role` VALUES (597, 494, 1);
INSERT INTO `itom_base_user_role` VALUES (598, 495, 1);
INSERT INTO `itom_base_user_role` VALUES (599, 496, 1);
INSERT INTO `itom_base_user_role` VALUES (600, 497, 1);
INSERT INTO `itom_base_user_role` VALUES (601, 498, 1);
INSERT INTO `itom_base_user_role` VALUES (602, 499, 1);
INSERT INTO `itom_base_user_role` VALUES (603, 500, 1);
INSERT INTO `itom_base_user_role` VALUES (604, 501, 1);
INSERT INTO `itom_base_user_role` VALUES (605, 502, 1);
INSERT INTO `itom_base_user_role` VALUES (606, 503, 1);
INSERT INTO `itom_base_user_role` VALUES (607, 504, 1);
INSERT INTO `itom_base_user_role` VALUES (608, 505, 1);
INSERT INTO `itom_base_user_role` VALUES (609, 506, 1);
INSERT INTO `itom_base_user_role` VALUES (610, 507, 1);
INSERT INTO `itom_base_user_role` VALUES (611, 508, 1);
INSERT INTO `itom_base_user_role` VALUES (612, 509, 1);
INSERT INTO `itom_base_user_role` VALUES (613, 510, 1);
INSERT INTO `itom_base_user_role` VALUES (614, 511, 1);
INSERT INTO `itom_base_user_role` VALUES (615, 512, 1);
INSERT INTO `itom_base_user_role` VALUES (616, 513, 1);
INSERT INTO `itom_base_user_role` VALUES (617, 514, 1);
INSERT INTO `itom_base_user_role` VALUES (618, 515, 1);
INSERT INTO `itom_base_user_role` VALUES (619, 516, 1);
INSERT INTO `itom_base_user_role` VALUES (620, 517, 1);
INSERT INTO `itom_base_user_role` VALUES (621, 518, 1);
INSERT INTO `itom_base_user_role` VALUES (622, 519, 1);
INSERT INTO `itom_base_user_role` VALUES (623, 520, 1);
INSERT INTO `itom_base_user_role` VALUES (624, 521, 1);
INSERT INTO `itom_base_user_role` VALUES (625, 522, 1);
INSERT INTO `itom_base_user_role` VALUES (626, 523, 1);
INSERT INTO `itom_base_user_role` VALUES (627, 524, 1);
INSERT INTO `itom_base_user_role` VALUES (628, 525, 1);
INSERT INTO `itom_base_user_role` VALUES (629, 526, 1);
INSERT INTO `itom_base_user_role` VALUES (630, 527, 1);
INSERT INTO `itom_base_user_role` VALUES (631, 528, 1);
INSERT INTO `itom_base_user_role` VALUES (632, 529, 1);
INSERT INTO `itom_base_user_role` VALUES (633, 530, 1);
INSERT INTO `itom_base_user_role` VALUES (634, 531, 1);
INSERT INTO `itom_base_user_role` VALUES (635, 532, 1);
INSERT INTO `itom_base_user_role` VALUES (636, 533, 1);
INSERT INTO `itom_base_user_role` VALUES (637, 534, 1);
INSERT INTO `itom_base_user_role` VALUES (638, 535, 1);
INSERT INTO `itom_base_user_role` VALUES (639, 536, 1);
INSERT INTO `itom_base_user_role` VALUES (640, 537, 1);
INSERT INTO `itom_base_user_role` VALUES (641, 538, 1);
INSERT INTO `itom_base_user_role` VALUES (642, 539, 1);
INSERT INTO `itom_base_user_role` VALUES (643, 540, 1);
INSERT INTO `itom_base_user_role` VALUES (644, 541, 1);
INSERT INTO `itom_base_user_role` VALUES (645, 542, 1);
INSERT INTO `itom_base_user_role` VALUES (646, 543, 1);
INSERT INTO `itom_base_user_role` VALUES (647, 544, 1);
INSERT INTO `itom_base_user_role` VALUES (648, 545, 1);
INSERT INTO `itom_base_user_role` VALUES (649, 546, 1);
INSERT INTO `itom_base_user_role` VALUES (650, 547, 1);
INSERT INTO `itom_base_user_role` VALUES (651, 548, 1);
INSERT INTO `itom_base_user_role` VALUES (652, 549, 1);
INSERT INTO `itom_base_user_role` VALUES (653, 550, 1);
INSERT INTO `itom_base_user_role` VALUES (654, 551, 1);
INSERT INTO `itom_base_user_role` VALUES (655, 552, 1);
INSERT INTO `itom_base_user_role` VALUES (656, 553, 1);
INSERT INTO `itom_base_user_role` VALUES (657, 554, 1);
INSERT INTO `itom_base_user_role` VALUES (658, 555, 1);
INSERT INTO `itom_base_user_role` VALUES (659, 556, 1);
INSERT INTO `itom_base_user_role` VALUES (660, 557, 1);
INSERT INTO `itom_base_user_role` VALUES (661, 558, 1);
INSERT INTO `itom_base_user_role` VALUES (662, 559, 1);
INSERT INTO `itom_base_user_role` VALUES (663, 560, 1);
INSERT INTO `itom_base_user_role` VALUES (664, 561, 1);
INSERT INTO `itom_base_user_role` VALUES (665, 562, 1);
INSERT INTO `itom_base_user_role` VALUES (666, 563, 1);
INSERT INTO `itom_base_user_role` VALUES (667, 564, 1);
INSERT INTO `itom_base_user_role` VALUES (668, 565, 1);
INSERT INTO `itom_base_user_role` VALUES (669, 566, 1);
INSERT INTO `itom_base_user_role` VALUES (670, 567, 1);
INSERT INTO `itom_base_user_role` VALUES (671, 568, 1);
INSERT INTO `itom_base_user_role` VALUES (672, 569, 1);
INSERT INTO `itom_base_user_role` VALUES (673, 570, 1);
INSERT INTO `itom_base_user_role` VALUES (674, 571, 1);
INSERT INTO `itom_base_user_role` VALUES (675, 572, 1);
INSERT INTO `itom_base_user_role` VALUES (676, 573, 1);
INSERT INTO `itom_base_user_role` VALUES (677, 574, 1);
INSERT INTO `itom_base_user_role` VALUES (678, 575, 1);
INSERT INTO `itom_base_user_role` VALUES (679, 576, 1);
INSERT INTO `itom_base_user_role` VALUES (681, 14, 20);
INSERT INTO `itom_base_user_role` VALUES (685, 590, 27);
INSERT INTO `itom_base_user_role` VALUES (686, 286, 18);
INSERT INTO `itom_base_user_role` VALUES (687, 591, 15);
INSERT INTO `itom_base_user_role` VALUES (688, 592, 15);
INSERT INTO `itom_base_user_role` VALUES (689, 593, 15);
INSERT INTO `itom_base_user_role` VALUES (690, 594, 18);
INSERT INTO `itom_base_user_role` VALUES (691, 595, 13);
INSERT INTO `itom_base_user_role` VALUES (692, 596, 21);
INSERT INTO `itom_base_user_role` VALUES (693, 597, 1);
INSERT INTO `itom_base_user_role` VALUES (707, 601, 1);
INSERT INTO `itom_base_user_role` VALUES (708, 610, 1);
INSERT INTO `itom_base_user_role` VALUES (709, 598, 1);
INSERT INTO `itom_base_user_role` VALUES (710, 599, 1);
INSERT INTO `itom_base_user_role` VALUES (711, 600, 1);
INSERT INTO `itom_base_user_role` VALUES (712, 602, 1);
INSERT INTO `itom_base_user_role` VALUES (713, 603, 1);
INSERT INTO `itom_base_user_role` VALUES (714, 604, 1);
INSERT INTO `itom_base_user_role` VALUES (715, 605, 1);
INSERT INTO `itom_base_user_role` VALUES (716, 606, 1);
INSERT INTO `itom_base_user_role` VALUES (717, 607, 1);
INSERT INTO `itom_base_user_role` VALUES (718, 608, 1);
INSERT INTO `itom_base_user_role` VALUES (719, 609, 1);
INSERT INTO `itom_base_user_role` VALUES (722, 615, 1);
INSERT INTO `itom_base_user_role` VALUES (723, 616, 6);
INSERT INTO `itom_base_user_role` VALUES (724, 617, 14);
INSERT INTO `itom_base_user_role` VALUES (728, 628, 1);
INSERT INTO `itom_base_user_role` VALUES (729, 288, 26);
INSERT INTO `itom_base_user_role` VALUES (730, 629, 3);
INSERT INTO `itom_base_user_role` VALUES (731, 630, 1);
INSERT INTO `itom_base_user_role` VALUES (732, 631, 1);
INSERT INTO `itom_base_user_role` VALUES (733, 632, 1);
INSERT INTO `itom_base_user_role` VALUES (734, 633, 1);
INSERT INTO `itom_base_user_role` VALUES (735, 634, 1);

-- ----------------------------
-- Table structure for itom_base_user_third_key
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_user_third_key`;
CREATE TABLE `itom_base_user_third_key`  (
  `user_id` int NOT NULL,
  `appid` int NOT NULL,
  `thirdy_key` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  PRIMARY KEY (`user_id`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_base_user_third_key
-- ----------------------------

-- ----------------------------
-- Table structure for itom_base_users
-- ----------------------------
DROP TABLE IF EXISTS `itom_base_users`;
CREATE TABLE `itom_base_users`  (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '标识符',
  `name` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '用户姓名',
  `account` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '用户帐号',
  `salt` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `password` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '用户密码',
  `encrypt` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '密码自定义规则加密',
  `position` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL,
  `email` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '用户Email',
  `phone` varchar(50) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '办公电话',
  `fax` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '传真',
  `mobile` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '移动电话',
  `version` int NOT NULL COMMENT '版本控制位',
  `user_type` int NOT NULL DEFAULT 10 COMMENT '用户类型(默认为系统用户)',
  `row_status` int NOT NULL DEFAULT 10 COMMENT '删除标识符',
  `status` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '是否可用(CODE:common_usable)',
  `engineer_level` int NULL DEFAULT NULL COMMENT '运维工程师的级别（code:engineer_level）',
  `remark` varchar(200) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '备注',
  `default_user` int NOT NULL DEFAULT 20 COMMENT '默认用户标识符,20为普通用户，可删除，10为默认用户，不可删除',
  `userno` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL DEFAULT NULL COMMENT '工号',
  `user_level` int NULL DEFAULT NULL COMMENT '用户等级，用以判断是否超级用户即管理员，10是超级用户，30普通用户',
  `user_status` int NULL DEFAULT NULL,
  `identify_type` char(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `identify_id` char(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `extend` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL,
  `namespace` varchar(100) CHARACTER SET utf8 COLLATE utf8_bin NOT NULL COMMENT '权限空间域',
  `send_type` bigint NOT NULL DEFAULT 0,
  `create_time` bigint NULL DEFAULT NULL COMMENT '创建时间',
  `allow_ips` text CHARACTER SET utf8 COLLATE utf8_unicode_ci NULL COMMENT '用户终端IP绑定，设置后其他IP无法登录当前账号',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `idx_account`(`account` ASC) USING BTREE,
  INDEX `idx_name`(`name` ASC) USING BTREE,
  INDEX `idx_email`(`email` ASC) USING BTREE,
  INDEX `idx_phone`(`phone` ASC) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 635 CHARACTER SET = utf8 COLLATE = utf8_unicode_ci ROW_FORMAT = DYNAMIC;

-- ----------------------------
-- Records of itom_base_users
-- ----------------------------
INSERT INTO `itom_base_users` VALUES (1, '系统管理员', 'admin', '70f25a7240d84607', 'cd8056e87f0a3dd066b0dfb0b9ae3b9169461e0e', '', '系统管理员', 'admin@xxx.com', '', '', '00000000000', 8, 10, 10, '1', 10, '', 10, NULL, 10, 0, '{\n\"login_type\": \"0\",', '', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (3, '杨立智', 'ylz', '70f25a7240d84607', 'a455fdc640efc0e2523d4b1bb94e30246523d8ef', NULL, NULL, 'yanglz_xj@mail.notes.bank-of-china.com', '', '', '5459073', 0, 10, 10, '1', NULL, '', 20, NULL, 10, NULL, '{\n\"login_type\": \"0\",', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'ke;xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (4, '张波', 'zb', '70f25a7240d84607', '4e641a0a81d24e40141f528bc0d66ed7dbc4abf5', NULL, NULL, '', '', '', '17789177201', 0, 10, 20, '1', NULL, '', 20, NULL, 10, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (5, '韩金刚', 'hjg', '70f25a7240d84607', '8895e27f1e574f9ffae57304094345dbca3994af', NULL, NULL, '', '', '', '13899952895', 0, 10, 20, '1', NULL, '', 20, NULL, 10, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (6, '值班', 'zhiban', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18099688074', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (7, '马涛', 'matao', '70f25a7240d84607', '21456243385ab7224b705745c02619fbd782e6b9', NULL, NULL, '', '', '', '5255523', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (8, '刘国生', 'liuguosheng', '70f25a7240d84607', '68eb82103e8c0891cbbd65b441d0a37ec15dba06', NULL, NULL, '', '', '', '6871889', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (9, '王小艳', 'wangxiaoyan', '70f25a7240d84607', '3add021db09dd452d9f2b689fded230309b42a3c', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (10, '彭光宁', 'pengguangning', '70f25a7240d84607', '728c3554de306680c1997f93b38d1e927bf00856', NULL, NULL, '', '', '', '4523359', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (11, '王涛', 'wangtao', '70f25a7240d84607', '2c5f5aa18cd83769573fe8954ee9438cc80c6683', NULL, NULL, '', '', '', '2362968', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (12, '邳晓新', 'pixiaoxin', '70f25a7240d84607', 'b2468862027ce55ff7b25c295d7642f741fc3dcf', NULL, NULL, '', '', '', '3216881', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (13, '朱明', 'zhuming', '70f25a7240d84607', '2564078e97edc60bdab96fefebdd87fd93c3a151', NULL, NULL, '', '', '', '18690871616', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (14, '郑少先', 'zhengshaoxian', '70f25a7240d84607', 'b05751501013c861842904a429077762a643f492', NULL, NULL, '', '', '', '0869107', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (15, '王成', 'wangcheng', '70f25a7240d84607', '437412acbb999a6510c0f01728ad3e535be0f76d', NULL, NULL, '', '', '', '6101791', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (16, '常维鸣', 'changweiming', '70f25a7240d84607', '4b6369699452db140be7f28f525f5676e4f4957c', NULL, NULL, 'y@mail.notes.bank-of-china.com', '', '', '13309999366', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (17, '秦弘剑', 'qinhongjian', '70f25a7240d84607', 'd108e00a49d74dd73086b7792185c5c4e775469f', NULL, NULL, '', '', '', '2085816', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (18, '巴哈提努尔', 'baha', '70f25a7240d84607', 'bdcc6da6a09a4740ca247f78657337a744b47fdc', NULL, NULL, '', '', '', '5148258', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (19, '陈杰', 'chenjie', '70f25a7240d84607', 'fedeb6a956e8226a7c7c478b510b392ca44ebbd0', NULL, NULL, '', '', '', '3015046', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (20, '张立春', 'zhanglichun', '70f25a7240d84607', '1d700a5074b60322235fc520b40f2261f371c219', NULL, NULL, '', '', '', '2264555', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (21, '李永翠', 'liyongcui', '70f25a7240d84607', 'cbf7c3bbc9a27a0c4964a6ceb6aed7c7b24e876b', NULL, NULL, '', '', '', '18999091595', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (22, '曾广新', 'zengguangxin', '70f25a7240d84607', '0b398a78c5d5ed7e1ecefb790247d419bb2065dc', NULL, NULL, '', '', '', '3839654', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (23, '测试1', '1', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (24, '测试2', '2', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (25, '密码', '3', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (26, '张波', '4', '70f25a7240d84607', '4e641a0a81d24e40141f528bc0d66ed7dbc4abf5', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (27, '张波', '110', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (28, 'zb', '111', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (29, '何伟', '112', '70f25a7240d84607', 'dddf3d8ac6be57b0763458332604d4ba0740c04d', NULL, NULL, '', '', '', '5898388', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (30, 'ceshi', '113', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (31, '1996', '1996', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (32, '杨立智', '1001', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (33, '机房临时卡1033', '1033', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (35, '机房临时卡1033--密码', '2033', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (36, '机房临时卡1034', '1000', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (37, '机房临时卡--1035', '1035', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (38, '超级管理员密码', '1999', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (39, '超级管理员密码1', '1998', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (40, '测试01', '9001', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (41, '楚梦婷', '101', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (43, '应急', '9003', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (44, '韩金刚', '1006', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (45, '韩金刚', '102', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (46, '杨立智', '103', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (47, '机房值班1', '104', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (48, '蔡远鹏', '105', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (49, '莫智敏', '106', '70f25a7240d84607', 'ab42feb16abd1b9640657170ad6d694f922bb179', NULL, NULL, 'xxkjmzm_xj@mail.notes.bank-of-china.com', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (50, '李曼', '107', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '4609797', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (51, '向明', '108', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (52, '姜丽', '109', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (53, '高磊', '114', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (54, '董玲', '115', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (55, '郝俊', '116', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (56, '冶冬', '117', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (57, '崔绪亮', '118', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (58, '赵晶', '119', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (59, '聂晨', '120', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (60, '郑洁', '121', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (61, '曹亮', '122', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (62, '刘洁', '123', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (63, '孙叶彬', '124', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (64, '黄建华', '125', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (65, '刘芳', '126', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (66, '闫军', '127', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (68, '王杰敏', '129', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (70, '王鑫馨', '131', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (71, '张丽君', '132', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (72, '赵飞翔', '133', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (73, '消防1', '134', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (74, '消防2', '135', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (77, '何为', '1111', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (78, '黄建华-密码', '225', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (79, '姜敬芝', '3802320850', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '5972232', '', '13999234755', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (80, '姜敬芝', '137', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (81, 'zb', 'bb', '70f25a7240d84607', '4e641a0a81d24e40141f528bc0d66ed7dbc4abf5', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (82, '外来人员', 'wlry', '70f25a7240d84607', '86ea90b41d9dad048e3ec6da88a5493229aced90', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (83, 'zbb', 'zbb', '70f25a7240d84607', '339700d37187cc97ae8961554dd44c6ce20f1469', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (85, '杨立智--密码', '203', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (86, '韩金刚--密码', '202', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (88, 'ID测试01', '8001', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (89, '崔绪亮--密码', '218', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (90, '向明--密码', '208', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (97, '冶冬--密码', '217', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (99, '456', '1234', '70f25a7240d84607', 'fa8d8271c718871e96d67cff265bb890cd64210d', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (100, '杨立智', '100', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (101, '崔绪亮', '6001', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (102, '何为', '8002', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (103, '王洁明IC', '9005', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (104, '蒋佰华', '3001', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15770025566', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (105, '李鹏', '3002', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15569202000', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (106, '王倍蕾', '3003', '70f25a7240d84607', '68eb82103e8c0891cbbd65b441d0a37ec15dba06', NULL, NULL, '', '', '', '15886839113', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (107, '王倍蕾', '3004', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15886839113', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (108, '王忠', '3005', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13095178322', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (109, '周婕', '3006', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15999421868', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (110, '彭光宁', '3007', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13319768580', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (111, '国义', '3009', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13899998196', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (112, '李梅', '3010', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13565187824', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (113, '帕丽达', '3011', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18690615881', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (114, '孙江龙', '3012', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18509960909', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (115, '李晶', '3013', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13709953311', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (116, '王斌', '3014', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18909969779', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (117, '武成龙', '3015', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13779339522', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (118, '周俊', '3016', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15001565672', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (119, '高茁', '3017', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13899006155', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (120, '李国志', '3018', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13309960321', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (121, '蒋云鹏', '3019', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13709960070', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (122, '张甜', '3020', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15909962255', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (123, '刘影', '3021', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18209968609', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (124, '李晨光', '3023', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13999625518', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (125, '雍素东', '3024', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13899000573', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (126, '胡峰', '3028', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13779017175', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (127, '马杰', '3029', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18609092840', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (128, '侯新征', '3030', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '8724251', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (129, '唐慧琴', '3031', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '4222006', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (130, '张鑫', '3032', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13779022829', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (131, '陈乐', '3033', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13565503063', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (132, '伊明霞', '3034', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13209099009', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (133, '焦璐琳', '3035', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13519978130', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (134, '董玉杰', '3036', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13565509445', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (135, '苗学巍', '3037', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '8388134', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (136, '刘晓威', '3039', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15001681570', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (137, '王丽波', '3041', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18690629322', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (138, '白静', '3043', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18509948208', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (139, '刘吉斌', '3045', '70f25a7240d84607', '441b71b1728cd1664c40c10ec33ae8e7f6488802', NULL, NULL, '', '', '', '18399907654', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (140, '李玉红', '3048', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '4626320', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (141, '李勇', '3051', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '6034534', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (142, '郭济福', '3052', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '8085712', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (143, '彭璐', '3054', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15299555650', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (144, '王英', '3055', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15699020060', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (145, '王梓涵', '3056', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15509027986', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (146, '王忠凯', '3057', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13899360567', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (147, '薛峰', '3059', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18099056980', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (148, '李凤', '3060', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '8467345', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (149, '马小艳', '3062', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15729978777', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (150, '张志刚', '3063', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13899150168', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (151, '朱云涛', '3064', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18509989686', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (152, '王芳', '3065', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13657513662', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (153, '张倩', '3066', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18999091569', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (154, '张蕾', '3067', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15099006688', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (155, '王雪梅', '3068', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13031284567', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (156, '王晓龙', '3069', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15292919190', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (157, '杨晋斌', '3070', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13779895005', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (158, '张超超', '3071', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15894012588', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (159, '万韬', '3072', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18999091517', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (160, '杨建华', '3073', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '15199800108', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (161, '张远', '3074', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13899151181', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (162, '姜虎', '3075', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18999308659', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (163, '王晓霞', '3076', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18799900019', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (164, '王琳', '3077', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '19909928188', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (165, '戴军', '3078', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13709923377', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (166, '刘平', '3079', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18999308656', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (167, '李世琴', '3080', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13999523005', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (168, '相婧', '3081', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18999300021', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (169, '赵玉兰', '3082', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '13519915672', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (170, '田忠', '3083', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18909905689', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (171, '赵丽娜', '3084', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18999308663', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (172, '于丽', '3085', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18999306366', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (173, '彭莉莉', '3087', '70f25a7240d84607', 'ddf8cd7c9cac5827ff76fed1356f36d5cf079b61', NULL, NULL, '', '', '', '18909908977', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (174, '王闯', 'ancd3088', '70f25a7240d84607', 'eb919808b5b29d604d24be5541986cd54f25d2ae', NULL, NULL, '', '', '', '18099000913', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (175, '黄艳林', 'ancd3089', '70f25a7240d84607', '1579e35e2e0af8b2423fb5903dfb713b9ea9df1f', NULL, NULL, '', '', '', '18099081221', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (176, '赵永军', 'ancd3090', '70f25a7240d84607', '31e0be935f3749b3b5804331661a108c834291ca', NULL, NULL, '', '', '', '18609083777', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (177, '于立晨', 'ancd3091', '70f25a7240d84607', 'a1850c8162a99ebca7553f766a3d4f1c56fbd8b7', NULL, NULL, '', '', '', '15509935568', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (178, '李鸽', 'ancd3092', '70f25a7240d84607', 'f5be61d2e3aae17b82108556b18f5b25fd47a112', NULL, NULL, '', '', '', '13909932345', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (179, '何玉新', 'ancd3093', '70f25a7240d84607', 'd4f793cdd1cfb43c73d6e22c0eae24caf4578d39', NULL, NULL, '', '', '', '18609931366', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (180, '马丽娜', 'ancd3094', '70f25a7240d84607', 'a7cf5d022fbe898a8bf18862342295eec086b9a3', NULL, NULL, '', '', '', '13565737307', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (181, '王晓燕', 'ancd3095', '70f25a7240d84607', '2c11bb0754b7559d32c88ce2ad25af110aeed502', NULL, NULL, '', '', '', '13909937361', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (182, '高昆峰', 'ancd3096', '70f25a7240d84607', '11670845544d4d1fcd1931aee7d8afd040205157', NULL, NULL, '', '', '', '18909931020', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (183, '曹舒翔', 'ancd3097', '70f25a7240d84607', 'e805f4a6c2d615d680c57c59474cd914acbc629b', NULL, NULL, '', '', '', '18699333883', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (184, '吕鹏', 'ancd3098', '70f25a7240d84607', '1a3e96326df6429e0505ed3898658b2f0cd0bab6', NULL, NULL, '', '', '', '18509930800', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (185, '郭峰', 'ancd3099', '70f25a7240d84607', 'bdffb51b60fe2d85d063542f7983cedcc2f44d22', NULL, NULL, '', '', '', '18509930827', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (186, '田业蕾', 'ancd3100', '70f25a7240d84607', 'dae8c18b3ce43d0abcdcd11b22b70f328b0ef6c0', NULL, NULL, '', '', '', '18509935266', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (187, '陈勇', 'ancd3101', '70f25a7240d84607', 'fa27479f24358caf4d462294bf2687c9c49a351a', NULL, NULL, '', '', '', '13579456366', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (188, '郭丽红', 'ancd3102', '70f25a7240d84607', '927ae4ed8429f2eac52e2a061224aa07eaff483b', NULL, NULL, '', '', '', '18609930166', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (189, '窦建民', 'ancd3103', '70f25a7240d84607', 'd71852373033072a9bad23212f67aa43d7fa3301', NULL, NULL, '', '', '', '18609933009', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (190, '于桂新', 'ancd3104', '70f25a7240d84607', '7a5911d5cf819a0dd9c49392ede17ca65b3ded7d', NULL, NULL, '', '', '', '15809018585', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (191, '王歆旖', 'ancd3105', '70f25a7240d84607', '1315663d1548973c760ba21ecdffd85896f0e17a', NULL, NULL, '', '', '', '15199499350', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (192, '张东海', 'ancd3106', '70f25a7240d84607', '14582315a5766e5fb4b3aaef59fa860dcf39da71', NULL, NULL, '', '', '', '13779203622', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (193, '张明祥', 'ancd3107', '70f25a7240d84607', '18e256ba78b22db8c77481b2ebc81930a617c799', NULL, NULL, '', '', '', '13579457999', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (194, '史小燕', 'ancd3108', '70f25a7240d84607', '424ffc1d361422a47a62f20363ecfa97b520b4b5', NULL, NULL, '', '', '', '15699018855', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (195, '李  强', 'ancd3109', '70f25a7240d84607', '20a01b8806d180d8ac766a66f09b45526420e7df', NULL, NULL, '', '', '', '15509925668', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (196, '詹生华', 'ancd3110', '70f25a7240d84607', 'e40a1bb091079bb7fd94f6b7269c5d5ed51edfe6', NULL, NULL, '', '', '', '1139815', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (197, '李雯', 'ancd3112', '70f25a7240d84607', 'ec4e5a791e51e05794c73fba665be2f07f7de95c', NULL, NULL, '', '', '', '18690110067', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (198, '朱传照', 'ancd3113', '70f25a7240d84607', '40d1695bf5732b18eab0da6d3b65deae115a8212', NULL, NULL, '', '', '', '15199052443', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (199, '樊江江', 'ancd3114', '70f25a7240d84607', '810914ecfd9805587ae51c38549460349bd6ba9f', NULL, NULL, '', '', '', '15026261139', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (200, '雷霞', 'ancd3115', '70f25a7240d84607', 'd14b44a4ee4a08bc660417044c4675677f84ebd6', NULL, NULL, '', '', '', '15299886316', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (201, '杜雪峰', 'ancd3116', '70f25a7240d84607', 'd92e12cc24badbcf9e9b0b5bd729f608043ed80c', NULL, NULL, '', '', '', '18509950788', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (202, '徐世荣', 'ancd3118', '70f25a7240d84607', '1485a2e6eb9936809008a91b17f66aa082a574fd', NULL, NULL, '', '', '', '13699902200', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (203, '张志东', 'ancd3120', '70f25a7240d84607', '618c67a46bc67a843d60c0d2e08df10ae81f484e', NULL, NULL, '', '', '', '15886975029', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (204, '木合亚提', 'ancd3121', '70f25a7240d84607', '17186f243c3d728c870ce55f73f689905a63e87e', NULL, NULL, '', '', '', '13679939328', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (205, '贾佳', 'ancd3122', '70f25a7240d84607', '6654591114dcff9eac2ea86f7e3682e5489f4b84', NULL, NULL, '', '', '', '18709998188', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (206, '张静', 'ancd3123', '70f25a7240d84607', 'e5a2eddf076bd508fb21ae81e29a33f08db5a0d4', NULL, NULL, '', '', '', '18509997588', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (207, '马雪莹', 'ancd3124', '70f25a7240d84607', '806f0d0d65a7ce285682ad9de4f978625e3c065f', NULL, NULL, '', '18699955133', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (208, '李翔', 'ancd3125', '70f25a7240d84607', '97d04e0a4958db0a9befeb5aa70c766684522254', NULL, NULL, '', '', '', '13579709020', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (209, '宋俞', 'ancd3126', '70f25a7240d84607', '68e8c9236432eb950a2aa430c8d677eec45704b2', NULL, NULL, '', '', '', '0540929', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (210, '杨雨迟', 'ancd3127', '70f25a7240d84607', '4d286a068ef5fc9a04b261f1f0042b5f0ad3e19e', NULL, NULL, '', '', '', '13779110097', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (211, '亓涛', 'ancd3128', '70f25a7240d84607', 'e7cd0a0321a63c0af4ee76947b6aff7fb6165b64', NULL, NULL, '', '', '', '13909990252', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (212, '张建', 'ancd3129', '70f25a7240d84607', '4578b789cf586cce3e628474d83b9e23fa5a5621', NULL, NULL, '', '', '', '15599690321', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (213, '程婕', 'ancd3130', '70f25a7240d84607', 'c0ac2333540e5c7c93f261842af3c121497f0b6f', NULL, NULL, '', '', '', '18209920925', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (214, '李永泽', 'ancd3131', '70f25a7240d84607', 'c1889b7fc2a919e28631f6a1a060bc4a0b02451e', NULL, NULL, '', '', '', '18935866355', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (215, '陈晓军', 'ancd3132', '70f25a7240d84607', 'eff9830e8f4942d6aa18726c0c6aed3a4b3f4061', NULL, NULL, '', '', '', '4901723', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (216, '魏荣荣', 'ancd3133', '70f25a7240d84607', '34cef21db9c490ad6db9e9f912424322eb8714f7', NULL, NULL, '', '', '', '13319929920', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (217, '牛建勇', 'ancd3134', '70f25a7240d84607', 'c4387130f5106d2fa19c1a70abef88ba3b434e70', NULL, NULL, '', '', '', '13909990767', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (218, '张菊丽', 'ancd3135', '70f25a7240d84607', '983d4bfde9175506334cfd7aabbff3ea405ea2a2', NULL, NULL, '', '', '', '18999573096', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (219, '高岚', 'ancd3136', '70f25a7240d84607', '45032b788e2331b81f6d8c24c79f04abb6332e6a', NULL, NULL, '', '', '', '3771319', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (220, '韩晶', 'ancd3137', '70f25a7240d84607', '7190e533ad3195b0b3dc57e2d3e06727b1af169c', NULL, NULL, '', '', '', '18999588590', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (221, '万丽', 'ancd3139', '70f25a7240d84607', 'dab8a6a62896332047cb0c65a6445c9a62b8d25c', NULL, NULL, '', '', '', '7247569', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (222, '贺瑛翔', 'ancd3140', '70f25a7240d84607', '0fed33a986cf92e820c5a6da3177d0c9bf8c9bf9', NULL, NULL, '', '', '', '18699181519', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (223, '阿克拜江', 'ancd3141', '70f25a7240d84607', '68eb82103e8c0891cbbd65b441d0a37ec15dba06', NULL, NULL, '', '', '', '0335044', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (224, '张新军', 'ancd3142', '70f25a7240d84607', '0c3835c5e033a13023d5d33b7d6a9ad5c4c472bd', NULL, NULL, '', '', '', '7337770', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (225, '张佩', 'ancd3143', '70f25a7240d84607', 'b92908310aaa99cc6e4499d02e419aa21a7126c5', NULL, NULL, '', '', '', '13579943638', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (226, '周东', 'ancd3144', '70f25a7240d84607', 'b4d61e5e04f1015ccb4d474fc22140e4e7ea1b99', NULL, NULL, '', '', '', '13109905859', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (227, '王鹏飞', 'ancd3145', '70f25a7240d84607', '61cd2bca24855f8448f8c75da6740e63c31e4a3a', NULL, NULL, '', '', '', '2013888', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (228, '白峰', 'ancd3146', '70f25a7240d84607', '7922275423942018cc434f5eaadcfc9d256fcef2', NULL, NULL, '', '', '', '13609922662', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (229, '陈蕊超', 'ancd3147', '70f25a7240d84607', '09d1454e33274dd74e0956b4c4bd800e156c8b20', NULL, NULL, '', '', '', '7326076', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (230, '侯本勇', 'ancd3148', '70f25a7240d84607', 'b8311184fba6cd39cc2412ff1fc4314acd358653', NULL, NULL, '', '', '', '18999213858', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (231, '张鑫新', 'ancd3149', '70f25a7240d84607', '7add367f00a368f73d177c0a802ddf9f0cba753b', NULL, NULL, '', '', '', '13999991665', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (232, '徐希', 'ancd3150', '70f25a7240d84607', '7037bc47de4e2c5ee272d3e7d2c7573d54cb0fc3', NULL, NULL, '', '', '', '2289717', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (233, '韩斌', 'ancd3151', '70f25a7240d84607', 'cf5bd08b44eb88e2c5482414c73af9c11b4fe6fb', NULL, NULL, '', '', '', '13999271012', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (234, '闫乐', 'ancd3152', '70f25a7240d84607', '84e103950b45d2ac04a93e952a380f7e372b30ae', NULL, NULL, '', '', '', '5600277', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (235, '江海', 'ancd3153', '70f25a7240d84607', '8efb6f9e48f71160a5556eba87528a9e91525671', NULL, NULL, '', '', '', '13579847732', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (236, '赵丽', 'ancd3154', '70f25a7240d84607', 'cb30675be8a8d74c6323afb46249adeed4a67fa9', NULL, NULL, '', '', '', '6333065', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (237, '彭颖', 'ancd3155', '70f25a7240d84607', 'cd3f8125b080418a3a67675a81620af133bcae43', NULL, NULL, '', '', '', '4343249', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (238, '郭原', 'ancd3156', '70f25a7240d84607', '29011de2e0bdc5da246a6ecbcbd2a9578c53c671', NULL, NULL, '', '', '', '1930389', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (239, '张超', 'ancd3157', '70f25a7240d84607', 'a9a14e569391a260897873c7e4ac7bdb558fa163', NULL, NULL, '', '', '', '8804915', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (240, '李莹', 'ancd3158', '70f25a7240d84607', '8bcf248a4c79596c660f16a800c6ae700cd51652', NULL, NULL, '', '', '', '15022917686', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (241, '林辉', 'ancd3159', '70f25a7240d84607', '45aee5134d197eb569778d687c85e4acb2a74491', NULL, NULL, '', '', '', '13999103636', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (242, '李莹', 'ancd3160', '70f25a7240d84607', '474445f1d779d5ef26e2b4a0560713acc9eed1f2', NULL, NULL, '', '', '', '15022917686', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (243, '李卫军', 'ancd3161', '70f25a7240d84607', '50c17dab3533b957922a8046b2eab722475e09be', NULL, NULL, '', '', '', '13899900989', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (244, '李豫敏', 'ancd3163', '70f25a7240d84607', '47b2aca43842f4d173a98c160884b53c82a8f6e3', NULL, NULL, '', '', '', '15999133370', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (245, '韩勇', 'ancd3164', '70f25a7240d84607', 'e288c653091b3622f3cb11c81ee85a2a739b70af', NULL, NULL, '', '', '', '6355985', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (246, '卫勇', 'ancd3165', '70f25a7240d84607', '06f10afc0df225ea9c913f7ae81a49744b7cb96f', NULL, NULL, '', '', '', '8609099', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (247, '周磊', 'ancd3166', '70f25a7240d84607', '61646893c0f05c761b355d183dfdbc51045fe2bf', NULL, NULL, '', '', '', '13999401770', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (248, '王成', 'ancd3167', '70f25a7240d84607', '7a154842eb502ffd3daf70cb1cac1f98974a4f13', NULL, NULL, '', '', '', '3195523', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (249, '杨礼冬', 'ancd3168', '70f25a7240d84607', '745eb7432edf942061d3e7a3193702411de4372e', NULL, NULL, '', '', '', '18699180299', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (250, '杨治', 'ancd3169', '70f25a7240d84607', 'cc26748c259e661ff1ff8a7caf364998d61ea4c5', NULL, NULL, '', '', '', '4876359', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (251, '艾克帕尔', 'ancd3170', '70f25a7240d84607', 'bb5ea3c92823879116fdf7b299e11de1348ba5c3', NULL, NULL, '', '', '', '5146863', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (252, '林芳', 'ancd3171', '70f25a7240d84607', '9380840de012a84cf1804fc19de5f4d66b413f1a', NULL, NULL, '', '', '', '5777520', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (253, '怀策', 'ancd3172', '70f25a7240d84607', 'f94337fff3d90f8fdccb1127cc06b18a437194ec', NULL, NULL, '', '', '', '18609916695', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (254, '杨瑞', 'ancd3173', '70f25a7240d84607', '7ba302047a7230a70fb2033620cb65e14493ef02', NULL, NULL, '', '', '', '15209915088', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (255, '蒋奎', 'ancd3174', '70f25a7240d84607', 'c7bb508d690d3d88869ebd0a458fe7a04aef1994', NULL, NULL, '', '', '', '2013999', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (256, '李玉', 'ancd3175', '70f25a7240d84607', '0e0c9a623d505c117be4d4b8622cabb1db43fc8b', NULL, NULL, '', '', '', '13999910364', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (257, '孙立', 'ancd3176', '70f25a7240d84607', '1683effb6d9b5203ba250fc17601f7f788aa7399', NULL, NULL, '', '', '', '3922752', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (258, '刘晓妍', 'ancd3177', '70f25a7240d84607', 'cd3d3f7f6eaf68806a13cc5cfc16996b111fbc1e', NULL, NULL, '', '', '', '1192903', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (259, '崔静', 'ancd3179', '70f25a7240d84607', '3ee2e2f9730739e70818e6777b2c1695ca919a9a', NULL, NULL, '', '', '', '13999886119', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (260, '胡艳慧', 'ancd3180', '70f25a7240d84607', '4deff163027b3319eabc53810cb859e5811a2899', NULL, NULL, '', '', '', '13899910968', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (261, '张娜', 'ancd3181', '70f25a7240d84607', '63a6282825e580925d198fe92a31c577650ebb17', NULL, NULL, '', '', '', '4255863', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (262, '谈峰', 'ancd3182', '70f25a7240d84607', '7c4b770d807c5e28da470d758405ed16941547e0', NULL, NULL, '', '', '', '8265987', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (263, '庞晓东', 'ancd3183', '70f25a7240d84607', '55adc8fbc226fa37f39d8468540ae8c06f54e5b4', NULL, NULL, '', '', '', '8574403', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (264, '刘苗', 'ancd3184', '70f25a7240d84607', '5da5ba89c2c31acae93ef1edf92d3b0003d02477', NULL, NULL, '', '', '', '18690172105', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (265, '刘晓文', 'ancd3185', '70f25a7240d84607', '9df56c30faa4a5c85fd9d543d2251a2afa17b12b', NULL, NULL, '', '', '', '15109916089', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (266, '刘雪琳', 'ancd3186', '70f25a7240d84607', '2e287221a837dc2396a4337903b9175177dcbdf3', NULL, NULL, '', '', '', '3565726', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (267, '陈实', 'ancd3187', '70f25a7240d84607', '0bce6c4fd50d5433c5b79022652d6a34db5ada10', NULL, NULL, '', '', '', '1989830', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (268, '蔡栩', 'ancd3188', '70f25a7240d84607', '52a9341c4b1dd887ca339f5c33a0124abd57c722', NULL, NULL, '', '', '', '4510778', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (269, '何江', 'ancd3189', '70f25a7240d84607', '68eb82103e8c0891cbbd65b441d0a37ec15dba06', NULL, NULL, '', '', '', '6758034', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (270, '周磊', 'ancd3190', '70f25a7240d84607', '261a8afb39b1bcdf66250f2d736bb5e1e0d5f25e', NULL, NULL, '', '', '', '7352121', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (272, '应急', '9006', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (274, '喀什账号', 'dhjk0998', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (275, '克拉玛依账号', 'dhjk0990', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (276, '塔城账号', 'dhjk0901', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (277, '哈密账号', 'dhjk0902', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (278, '昌吉账号', 'dhjk0994', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (279, '吐鲁番账号', 'dhjk0995', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (280, '博州账号', 'dhjk0909', '70f25a7240d84607', 'f19b3363f52a816808ed71314e03aa8d54d1c8aa', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (281, '伊犁账号', 'dhjk0999', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (282, '巴州账号', 'dhjk0996', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (283, '克州账号', 'dhjk0908', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (284, '阿勒泰账号', 'dhjk0906', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (285, '阿克苏账号', 'dhjk0997', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (286, '石河子账号', 'dhjk0993', '70f25a7240d84607', '284edca45831cd7419514e4dd0c4622a4d3b87f7', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (288, '值班监控', 'dhjk', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', 'dhjk', '{\n\"login_type\": \"0\",\n\"last_change_password_time\": 1766409387,\n\"timeout\": {\n\"type\": \"0\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"project_root\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (292, '杨鎏', '139', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (297, '梁平', 'liangping', '70f25a7240d84607', '6e5f1d1dc79355a60b2c790311a34d04e9e7d5ad', NULL, NULL, '', '', '', '13565901867', 0, 10, 20, '1', NULL, '', 20, NULL, 10, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (298, '值班', 'zhiban2020', '70f25a7240d84607', 'ad76860dedd9e3cdffe638dd49fca5615cb6d31d', NULL, NULL, '', '', '', '18099688074', 0, 10, 10, '1', NULL, '', 20, NULL, 10, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (299, '王智峰', 'wangzhifeng', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13779018088', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (300, '曹亮--密码', '222', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (301, '张国芳', '140', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (302, '赵小宇', 'zxy', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '15099580566', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (303, '李捷新', 'lijiexin', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18999091562', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (305, '马靖疆', 'mjj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18999091585', 0, 10, 20, '1', NULL, '20201113', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (307, '孙绍杰', 'sunsj', '70f25a7240d84607', '86a544b0ce9fff6d2c3a775447c65df597de2f88', NULL, NULL, '', '', '', '3776621', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (308, '王亮', 'wangl', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '7941358', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (309, '孙斌', 'sunbin', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18699222908', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (310, '俞幸聚', 'yuxj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '15909940132', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (311, '詹生华', 'zhansh', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '17709929760', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (312, '李凤美', 'lifm', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '15739160669', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (313, '王立树', 'wangls', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18609988563', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (314, '党小乔', 'dangxq', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '15569586565', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (315, '刘慧娟', 'liuhj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13565653765', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (316, '王琰琰', 'wangyy', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13999099187', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (317, '王香香', 'wangxx', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '15886890169', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (318, '王飞', 'wangfei', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13899336062', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (319, '何飞', 'hefei', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18509948282', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (320, '周陆欣', 'zhoulx', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '2049102', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (321, '汪秀梅', 'wangxm', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '1902568', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (322, '杨文静', 'yangwj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '36622731', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (323, '李玉红', 'liyh', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '4626320', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (324, '晏晨', 'yanchen', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '0281252', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (325, '李杨', 'liyang', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13999345850', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (326, '张晶晶', 'zhangjj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8416866', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (327, '苗学巍', 'miaoxw', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8388134', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (328, '李铭挚', 'limz', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18099949266', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (329, '刘英', 'liuying', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13139872108', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (330, '周洪斌', 'zhouhb', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8137237', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (331, '周鑫', 'zhouxin', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13999010007', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (332, '刘平', 'liuping', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18999308656', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (333, '李旭敬', 'lixj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '18099659098', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (334, '张宁欢', 'zhangnh', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13565520118', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (335, '魏唯', 'weiw', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8961063', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (336, '张哲', 'zhangz', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '7432983', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (337, '董洁', 'dongj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '4025245', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (338, '吕娟', 'lvjuan', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '0627988', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (339, '尚米娜', 'shmn', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '15299963623', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (340, '许倩茹', 'xuqr', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13677561210', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (341, '杨栩', 'yangxu', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '15001621062', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (342, '张楠', 'zhangn', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '13909936927', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (343, '曹荣林', 'crl', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (344, '孙姣姣', 'sjj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (345, '张勇', 'zhangy', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (346, '伍奇杰', '141', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (348, '龚鑫', '142', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (350, '蒋志强', 'jiangzhiqiang', '70f25a7240d84607', '3b67dc20609f9133cc97552b56bed855b5615a7e', NULL, NULL, '', '', '', '8189159', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (351, '刘志娇', 'liuzj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '1926844', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (353, '王长伟', 'wangcw', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '4267680', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (354, '汪涛', '143', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (356, '任雪松', '144', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (358, '贾鑫磊', '145', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (360, '巴宗轲', '146', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (361, '巴宗轲-密码', '246', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (362, '张靳蕾', 'zhangjl', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8658879', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (363, '郭姝晗', '147', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (365, '黄磊', 'huangl', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '3891361', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (366, '王涛', '148', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (369, '郭维莉', 'guoweili', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8136587', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (372, '曾广新', 'zenggx', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '3839654', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (373, '楚梦婷', 'chumt', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (374, '张毅勇', 'zhangyy', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8533474', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (375, '张峥', 'zhangzheng', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '0015518', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (376, '卢育', 'luyu', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '6707369', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (377, '董素娟', 'dongsj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '2522125', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (378, '韩玉', 'ancd3192', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3222574', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (379, '童悦', 'ancd3193', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7118721', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (380, '盛艳', 'ancd3194', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7374424', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (381, '李成', 'ancd3195', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2188783', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (382, '张磊', 'ancd3196', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6516993', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (383, '王馨月', 'ancd3197', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5858620', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (384, '徐斌', 'ancd3198', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6967620', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (385, '李霞', 'ancd3199', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2287301', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (386, '赖青', 'ancd3200', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1202422', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (387, '黄磊', 'ancd3201', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5840097', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (388, '闫玲', 'ancd3202', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6053223', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (389, '谌萍', 'ancd3203', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5994995', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (390, '刘亦卿', 'ancd3204', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5899959', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (391, '付寅嫣', 'ancd3205', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1436551', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (392, '刘晨璐', 'ancd3206', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3119958', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (393, '杜嘉馨', 'ancd3207', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8692252', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (394, '周璇', 'ancd3208', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4959035', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (395, '李婷婷', 'ancd3209', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5009256', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (396, '李志红', 'ancd3210', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3459297', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (397, '惠勇', 'ancd3211', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8404901', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (398, '韩晓茹', 'ancd3212', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2674358', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (399, '崔巧娟', 'ancd3213', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6893353', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (400, '彭欢', 'ancd3214', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4187854', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (401, '綦贵侠', 'ancd3215', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2213596', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (402, '彭岚', 'ancd3216', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3562063', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (403, '邹真', 'ancd3217', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4093725', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (404, '魏荣荣', 'ancd3218', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1603586', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (405, '吴苏江', 'ancd3219', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4521523', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (406, '王钰', 'ancd3220', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7162116', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (407, '宋文娟', 'ancd3221', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6017899', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (408, '梁海峰', 'ancd3222', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2014660', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (409, '单英', 'ancd3223', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0858022', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (410, '刘兆梅', 'ancd3224', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3211621', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (411, '郭原', 'ancd3225', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1930389', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (412, '吴靖', 'ancd3226', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7438036', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (413, '韦丽丽', 'ancd3227', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1052691', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (414, '黄文博', 'ancd3228', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5148690', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (415, '王敏星', 'ancd3229', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6558463', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (416, '闫子微', 'ancd3230', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7835935', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (417, '赵婧', 'ancd3231', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2983861', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (418, '程政博', 'ancd3232', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1947239', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (419, '张丽', 'ancd3233', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6776330', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (420, '李倩', 'ancd3234', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7116844', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (421, '朱雅晖', 'ancd3235', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3155069', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (422, '季晨', 'ancd3236', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4899099', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (423, '杨远', 'ancd3237', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4000994', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (424, '于烈', 'ancd3238', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0366330', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (425, '王新丽', 'ancd3239', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2130873', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (426, '魏梓秣', 'ancd3240', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2142953', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (427, '梁亚萍', 'ancd3241', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1409512', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (428, '李锐', 'ancd3242', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5533279', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (429, '郭婷婷', 'ancd3243', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1084795', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (430, '于娜', 'ancd3244', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4668166', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (431, '周速进', 'ancd3245', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2567967', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (432, '王艺', 'ancd3246', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8831518', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (433, '辛长洁', 'ancd3247', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3649273', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (434, '闫召文', 'ancd3248', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2395879', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (435, '张庆芳', 'ancd3249', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0719517', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (436, '巴勒根', 'ancd3250', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2014568', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (437, '苏菲', 'ancd3251', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7564585', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (438, '王莎莎', 'ancd3252', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1180009', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (439, '张文莉', 'ancd3253', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3031537', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (440, '陈龙', 'ancd3254', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2014113', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (441, '侯雅楠', 'ancd3255', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8176247', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (442, '赵勤', 'ancd3256', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3030019', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (443, '樊博', 'ancd3257', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1787810', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (444, '郑东', 'ancd3258', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8763499', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (445, '陈实', 'ancd3259', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1989830', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (446, '马慧', 'ancd3260', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8629016', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (447, '吕焰', 'ancd3261', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8816257', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (448, '陈秀荣', 'ancd3262', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8250985', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (449, '鲁茹', 'ancd3263', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1853745', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (450, '刘雪娇', 'ancd3264', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7759977', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (451, '魏新捷', 'ancd3265', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7136586', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (452, '王忠凯', 'ancd3266', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1125896', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (453, '侯新征', 'ancd3267', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8724251', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (454, '张鑫', 'ancd3268', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2634950', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (455, '马雪梅', 'ancd3269', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7590002', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (456, '唐慧琴', 'ancd3270', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4222006', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (457, '张哲', 'ancd3271', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7432983', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (458, '谭睦林', 'ancd3272', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3655477', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (459, '薛莹', 'ancd3273', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1726655', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (460, '郭江腾', 'ancd3274', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1052460', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (461, '马杰', 'ancd3275', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5842084', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (462, '李鹏', 'ancd3276', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8613323', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (463, '李鹏鹄', 'ancd3277', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7618552', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (464, '魏小燕', 'ancd3278', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7501571', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (465, '高永永', 'ancd3279', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7333885', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (466, '张泉', 'ancd3280', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1934284', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (467, '张志鑫', 'ancd3281', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6843874', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (468, '王忠', 'ancd3282', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7089171', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (469, '宋杰', 'ancd3283', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4404275', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (470, '詹斯尧', 'ancd3284', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5579009', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (471, '赵瑞', 'ancd3285', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4600042', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (472, '荆文洁', 'ancd3286', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4576779', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (473, '徐鑫鑫', 'ancd3287', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4256993', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (474, '秦晓飞', 'ancd3288', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4015775', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (475, '赵丽娜', 'ancd3289', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7213252', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (476, '金晓彤', 'ancd3290', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8600294', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (477, '郝者强', 'ancd3291', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7975501', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (478, '姜虎', 'ancd3292', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0977272', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (479, '戴军', 'ancd3293', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7734063', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (480, '容婧婧', 'ancd3294', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5000952', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (481, '李世琴', 'ancd3296', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2067246', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (482, '王亚琪', 'ancd3297', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3185500', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (483, '宗非非', 'ancd3298', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2760924', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (484, '余冰', 'ancd3299', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8557510', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (485, '王琳', 'ancd3300', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5421143', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (486, '段西银', 'ancd3301', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6852623', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (487, '赵永军', 'ancd3302', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5844610', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (488, '侯明', 'ancd3303', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6015933', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (489, '温思雨', 'ancd3304', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8912548', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (490, '张东海', 'ancd3305', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5721410', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (491, '马永顺', 'ancd3306', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3407648', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (492, '王亮', 'ancd3307', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7941358', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (493, '康亚男', 'ancd3308', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5360293', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (494, '詹生华', 'ancd3309', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1139815', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (495, '孙斌', 'ancd3310', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4413233', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (496, '王歆旖', 'ancd3311', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2868408', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (497, '马雪莹', 'ancd3312', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8047829', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (498, '邓欢', 'ancd3313', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2545727', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (499, '菲尔东', 'ancd3314', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4174292', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (500, '朱胜杰', 'ancd3315', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6721537', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (501, '木合亚提', 'ancd3316', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6792642', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (502, '胡侨迪', 'ancd3317', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7778800', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (503, '周祥', 'ancd3319', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7679952', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (504, '赵伟', 'ancd3320', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3495016', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (505, '王静', 'ancd3321', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8744325', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (506, '陈晓军', 'ancd3322', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4901723', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (507, '董长慧', 'ancd3323', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0410148', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (508, '张丽娜', 'ancd3324', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3501945', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (509, '申高成', 'ancd3325', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7257387', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (510, '张菊丽', 'ancd3326', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0623238', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (511, '杨晓伊', 'ancd3327', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7553857', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (512, '张碧池', 'ancd3328', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1833451', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (513, '杨春', 'ancd3329', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2244387', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (514, '郭振宇', 'ancd3330', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1025857', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (515, '程静', 'ancd3331', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8080715', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (516, '樊梅', 'ancd3332', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3292030', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (517, '朱传照', 'ancd3333', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7174114', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (518, '刘丰', 'ancd3334', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6705394', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (519, '孟留阳', 'ancd3335', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2698432', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (520, '尚米娜', 'ancd3336', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7052084', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (521, '姜莉莉', 'ancd3337', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1868921', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (522, '何玉新', 'ancd3338', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5743551', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (523, '郭俊杰', 'ancd3339', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7511701', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (524, '彭楚贤', 'ancd3340', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1076314', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (525, '许倩茹', 'ancd3342', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8942309', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (526, '毛艺璇', 'ancd3343', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1636503', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (527, '高巍', 'ancd3344', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0178248', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (528, '杨栩', 'ancd3345', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2561408', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (529, '鹿斌', 'ancd3346', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1568986', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (530, '袁亚玲', 'ancd3347', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6928371', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (531, '吕燕', 'ancd3348', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7423167', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (532, '王香香', 'ancd3349', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2534010', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (533, '周园园', 'ancd3350', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4894022', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (534, '杨少成', 'ancd3351', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2419888', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (535, '王小艳', 'ancd3352', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8419306', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (536, '唐黎', 'ancd3353', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1767917', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (537, '王晓龙', 'ancd3354', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1858663', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (538, '杨晋斌', 'ancd3355', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6286066', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (539, '褚杰', 'ancd3356', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1731338', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (540, '张志刚', 'ancd3357', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8578963', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (541, '高孟琦', 'ancd3358', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2540451', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (542, '朱云涛', 'ancd3359', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5211061', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (543, '党小乔', 'ancd3360', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0900521', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (544, '万韬', 'ancd3361', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5699732', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (545, '孙绍杰', 'ancd3362', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3776621', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (546, '李勇', 'ancd3363', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6034534', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (547, '李玉红', 'ancd3364', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4626320', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (548, '汪秀梅', 'ancd3365', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1902568', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (549, '郭济福', 'ancd3366', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8085712', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (550, '张梦娇', 'ancd3367', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3750216', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (551, '铁春', 'ancd3368', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1830147', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (552, '周陆欣', 'ancd3369', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2049102', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (553, '唐国忠', 'ancd3370', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3337366', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (554, '李玮', 'ancd3371', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1019418', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (555, '李沛源', 'ancd3372', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1861408', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (556, '刘胜男', 'ancd3373', '70f25a7240d84607', '', NULL, NULL, '', '', '', '2415185', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (557, '杨文静', 'ancd3374', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3662273', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (558, '王凯', 'ancd3375', '70f25a7240d84607', '', NULL, NULL, '', '', '', '7242454', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (559, '朱万强', 'ancd3376', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3016671', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (560, '赵文智', 'ancd3377', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4803719', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (561, '阿迪力', 'ancd3378', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4705735', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (562, '薛峰', 'ancd3379', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6690008', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (563, '李旭敬', 'ancd3380', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0922209', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (564, '陈辉', 'ancd3381', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6669201', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (565, '江博塔', 'ancd3382', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3588888', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (566, '蒋云鹏', 'ancd3383', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3447113', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (567, '李晨光', 'ancd3384', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3602518', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (568, '刘斐', 'ancd3385', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8966917', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (569, '刘万祥', 'ancd3386', '70f25a7240d84607', '', NULL, NULL, '', '', '', '4963592', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (570, '刘影', 'ancd3387', '70f25a7240d84607', '', NULL, NULL, '', '', '', '5487698', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (571, '孙江龙', 'ancd3388', '70f25a7240d84607', '', NULL, NULL, '', '', '', '3859223', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (572, '王斌', 'ancd3389', '70f25a7240d84607', '', NULL, NULL, '', '', '', '6804940', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (573, '张甜', 'ancd3390', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0961795', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (574, '张文洁', 'ancd3391', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1126812', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (575, '张雪', 'ancd3392', '70f25a7240d84607', '', NULL, NULL, '', '', '', '1303502', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (576, '郑寒冰', 'ancd3393', '70f25a7240d84607', '', NULL, NULL, '', '', '', '8488676', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (577, '梁平', '149', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (578, '孟卓超', '150', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (580, '周路凡', '152', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (585, '文君豪', '153', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (586, '丁永进', '154', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (588, '薛震', '156', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (589, '薛震-密码', '256', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (590, '王君君', 'wangjunjun', '70f25a7240d84607', '421a812eb95e38e01ed5d048b2b81f2d8476e13a', NULL, NULL, '', '', '', '5452618', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (591, '张志刚', 'Zhangszg', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8578963', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (592, '王小艳', 'wangxy', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8419306', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (593, '冉利军', 'ranlj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '6146994', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (594, '苗颖', 'miaoying', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '5526891', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (595, '朱万成', 'zhuwc', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '4381787', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (596, '邹小强', 'zouxq', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '3353575', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (597, '柴丽', 'ancd3394', '70f25a7240d84607', '', NULL, NULL, '', '', '', '0091766', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (598, '崔思良', 'ancd3395', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '5892299', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (599, '傅炜', 'ancd3396', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '4564644', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (600, '郭宇星', 'ancd3397', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '7740473', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (601, '李慧', 'ancd3398', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '1110282', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (602, '刘艺', 'ancd3399', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '3430826', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (603, '王廉钦', 'ancd3400', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '5067836', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (604, '杨莺', 'ancd3401', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '4136766', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (605, '余星慧', 'ancd3402', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '2289254', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (606, '糟晓倩', 'ancd3403', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '7076809', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (607, '张翼龙', 'ancd3404', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '1309765', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (608, '赵筱璿', 'ancd3405', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '6303125', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (609, '朱明月', 'ancd3406', '70f25a7240d84607', 'f3796e0fe1bca2ddb7f638744550b4915484aaf1', NULL, NULL, '', '', '', '7847140', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (610, '王浩', 'ancd3407', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '1543759', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (611, '薛震', '155', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 20, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '', '', 'xaccessgo,ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (612, '薛震', '157', '70f25a7240d84607', 'd52d8ff51fd9e59464c7ac0fc77b0bc86f943671', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (613, '薛震-密码', '158', '70f25a7240d84607', 'd52d8ff51fd9e59464c7ac0fc77b0bc86f943671', NULL, NULL, '', '', '', '', 0, 10, 20, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (615, '李俊强', 'lijq', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '1004494', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (616, '马涛', 'mtao', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '5255523', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (617, '李厉', 'lili', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', NULL, NULL, '', '', '', '8239276', 0, 10, 10, '1', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"0\",', '0', '', 'ke', 0, NULL, NULL);
INSERT INTO `itom_base_users` VALUES (618, '牛俊豪', '0155', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (619, '王筱璇', '0156', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccess_v2', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (622, '丁亚涛', '0158', '70f25a7240d84607', '', NULL, NULL, '', '', '', '', 0, 10, 10, '0', NULL, '', 20, NULL, 30, NULL, '{\n\"login_type\": \"\",\n', '0', '{\n\"login_type\": \"\",\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": false\n}', 'xaccessgo,ke', 0, NULL, '[]');
INSERT INTO `itom_base_users` VALUES (624, '666', '48255', '70f25a7240d84607', '8cf2c782716a0f202fb622da999811dde5d9f784', '', '', '', '', '', '', 0, 10, 10, '0', 0, '', 20, '', 30, NULL, '', '', '{\n\"login_type\": \"\",\n\"last_change_password_time\": 1766226094,\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'xaccess_v2', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (625, '莫智敏', '25214', '70f25a7240d84607', '3b44cc6007cf161e90c7bb5b033351b6a40d7b5d', '', '', '', '', '', '', 0, 10, 10, '0', 0, '', 20, '', 30, NULL, '', '', '{\n\"login_type\": \"\",\n\"last_change_password_time\": 1766311990,\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'xaccess_v2', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (626, '薛震', '51746', '70f25a7240d84607', 'b4abf0332271979d0ed9d2fb9954e3fd40e93383', '', '', '', '', '', '', 0, 10, 10, '0', 0, '', 20, '', 30, NULL, '', '', '{\n\"login_type\": \"\",\n\"last_change_password_time\": 1766312645,\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'xaccess_v2', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (627, '李曼', '35708', '70f25a7240d84607', '8d06d841e8b92cf792a20cd97279709011da5b6c', '', '', '', '', '', '', 0, 10, 10, '0', 0, '', 20, '', 30, NULL, '', '', '{\n\"login_type\": \"\",\n\"last_change_password_time\": 1766313601,\n\"timeout\": {\n\"type\": \"\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'xaccess_v2', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (628, '贾鑫磊', 'jxl', '70f25a7240d84607', 'a8cc9935717fa5bac17c5763b448f80c98b46200', '', '', '', '', '', '8115963', 0, 10, 10, '1', 0, '', 20, '', 10, NULL, '0', '', '{\n\"login_type\": \"0\",\n\"last_change_password_time\": 1766400978,\n\"timeout\": {\n\"type\": \"0\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'ke', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (629, '马瑞睿', 'marr', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', '', '', '', '', '', '3889142', 0, 10, 10, '1', 0, '', 20, '', 30, NULL, '0', '', '{\n\"login_type\": \"0\",\n\"last_change_password_time\": 1766567658,\n\"timeout\": {\n\"type\": \"0\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'ke', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (630, '龚雅洁', 'gongyj', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', '', '', '', '', '', '7659036', 0, 10, 10, '1', 0, '', 20, '', 30, NULL, '0', '', '{\n\"login_type\": \"0\",\n\"last_change_password_time\": 1766568091,\n\"timeout\": {\n\"type\": \"0\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'ke', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (631, '李哲', 'lizhe', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', '', '', '', '', '', '3760825', 0, 10, 10, '1', 0, '', 20, '', 30, NULL, '0', '', '{\n\"login_type\": \"0\",\n\"last_change_password_time\": 1766569342,\n\"timeout\": {\n\"type\": \"0\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'ke', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (632, '许晓莉', 'xuxl', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', '', '', '', '', '', '1393137', 0, 10, 10, '1', 0, '', 20, '', 30, NULL, '0', '', '{\n\"login_type\": \"0\",\n\"last_change_password_time\": 1766570103,\n\"timeout\": {\n\"type\": \"0\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'ke', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (633, '于翔', 'yuxiang', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', '', '', '', '', '', '3950085', 0, 10, 10, '1', 0, '', 20, '', 30, NULL, '0', '', '{\n\"login_type\": \"0\",\n\"last_change_password_time\": 1766570587,\n\"timeout\": {\n\"type\": \"0\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'ke', 0, 0, '[]');
INSERT INTO `itom_base_users` VALUES (634, '马俊楠', 'majn', '70f25a7240d84607', '29db3dfdcd14d69ea2c0f8da4a539c94bcca72b0', '', '', '', '', '', '7466134', 0, 10, 10, '1', 0, '', 20, '', 30, NULL, '0', '', '{\n\"login_type\": \"0\",\n\"last_change_password_time\": 1766570924,\n\"timeout\": {\n\"type\": \"0\",\n\"time\": \"\"\n},\n\"page_setting\": {\n\"id\": \"\",\n\"location\": \"\"\n},\n\"alarm_filter\": null,\n\"menu\": {\n\"showIds\": null,\n\"hideIds\": null\n},\n\"wechat\": \"\",\n\"single_terminal_login\": false,\n\"person_type\": 0,\n\"first_login\": true\n}', 'ke', 0, 0, '[]');

-- ----------------------------
-- Table structure for itom_role_resource_perms
-- ----------------------------
DROP TABLE IF EXISTS `itom_role_resource_perms`;
CREATE TABLE `itom_role_resource_perms`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `role_id` bigint NOT NULL,
  `resource_id` varchar(64) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  `resource_type` enum('ci','inventory','sub_system','access') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'ci' COMMENT 'ci:CI项；inventory:库存资产；sub_system:子系统; access:门禁',
  `perms` int UNSIGNED NOT NULL DEFAULT 1 COMMENT '0:未选取,1:选取',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE INDEX `role_resource_unique`(`role_id` ASC, `resource_id` ASC, `resource_type` ASC) USING BTREE,
  CONSTRAINT `itom_role_resource_perms_ibfk_1` FOREIGN KEY (`role_id`) REFERENCES `itom_base_roles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE = InnoDB AUTO_INCREMENT = 860 CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of itom_role_resource_perms
-- ----------------------------
INSERT INTO `itom_role_resource_perms` VALUES (3, 1, '0_0', 'access', 1);
INSERT INTO `itom_role_resource_perms` VALUES (4, 1, '0_1', 'access', 1);
INSERT INTO `itom_role_resource_perms` VALUES (65, 21, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (66, 21, '0_117', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (67, 21, '14', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (68, 21, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (118, 27, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (119, 27, '0_361', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (120, 27, '1', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (121, 27, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (122, 15, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (123, 15, '0_112', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (124, 15, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (125, 15, '10', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (126, 16, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (127, 16, '0_108', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (128, 16, '5', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (129, 16, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (130, 17, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (131, 17, '0_114', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (132, 17, '8', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (133, 17, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (134, 6, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (135, 6, '0_110', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (136, 6, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (137, 6, '7', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (138, 7, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (139, 7, '0_104', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (140, 7, '3', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (141, 7, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (142, 8, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (143, 8, '0_115', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (144, 8, '12', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (145, 8, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (146, 9, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (147, 9, '0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (148, 9, '6', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (149, 9, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (150, 10, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (151, 10, '0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (152, 10, '2', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (153, 10, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (154, 11, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (155, 11, '0_282', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (156, 11, '21', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (157, 11, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (158, 12, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (159, 12, '0_118', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (160, 12, '15', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (161, 12, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (162, 13, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (163, 13, '0_113', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (164, 13, '11', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (165, 13, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (166, 14, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (167, 14, '0_357', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (168, 14, '17', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (169, 14, '18', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (170, 14, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (171, 18, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (172, 18, '0_111', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (173, 18, '9', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (174, 18, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (175, 19, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (176, 19, '0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (177, 19, '4', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (178, 19, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (179, 20, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (180, 20, '0_116', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (181, 20, '13', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (182, 20, 'link_root', 'sub_system', 2);
INSERT INTO `itom_role_resource_perms` VALUES (183, 1, 'project_root', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (184, 1, 'link_root', 'sub_system', 1);
INSERT INTO `itom_role_resource_perms` VALUES (767, 26, '12_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (768, 26, '6_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (769, 26, '1_0_103', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (770, 26, '0_106', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (771, 26, '4_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (772, 26, '13_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (773, 26, '14_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (774, 26, '15_0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (775, 26, '10_0_107', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (776, 26, '8_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (777, 26, '9_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (778, 26, '7_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (779, 26, '0_102', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (780, 26, '0_113', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (781, 26, '1_0_107', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (782, 26, '0_249', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (783, 26, '7_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (784, 26, '7_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (785, 26, '0_104', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (786, 26, '0_115', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (787, 26, '0_112', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (788, 26, '5_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (789, 26, '12_0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (790, 26, '0_240', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (791, 26, '0_242', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (792, 26, '15_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (793, 26, '0_114', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (794, 26, '9_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (795, 26, '0_110', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (796, 26, '4_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (797, 26, '0_250', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (798, 26, '14_0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (799, 26, '15_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (800, 26, '10_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (801, 26, '10_0_108', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (802, 26, '4_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (803, 26, 'project_root', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (804, 26, '12_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (805, 26, '0_243', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (806, 26, '0_362', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (807, 26, '0_247', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (808, 26, '8_0_153', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (809, 26, '4_0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (810, 26, '0_116', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (811, 26, '0_282', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (812, 26, '2_0_115', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (813, 26, '5_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (814, 26, '0_248', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (815, 26, '0_357', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (816, 26, '0_111', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (817, 26, '9_0_110', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (818, 26, '0_162', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (819, 26, '0_119', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (820, 26, '12_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (821, 26, '0_241', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (822, 26, '10_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (823, 26, '5_0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (824, 26, '9_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (825, 26, '14_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (826, 26, '14_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (827, 26, '6_0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (828, 26, '1_0_104', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (829, 26, '8_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (830, 26, '8_0_147', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (831, 26, '13_0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (832, 26, '7_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (833, 26, '0_233', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (834, 26, '6_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (835, 26, '6_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (836, 26, '2_0_119', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (837, 26, '0_118', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (838, 26, '9_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (839, 26, '7_0_106', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (840, 26, '12_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (841, 26, '2_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (842, 26, '2_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (843, 26, '10_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (844, 26, '0_246', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (845, 26, '0_108', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (846, 26, '5_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (847, 26, '6_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (848, 26, '5_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (849, 26, '8_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (850, 26, '4_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (851, 26, '13_0_101', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (852, 26, '13_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (853, 26, '13_0_109', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (854, 26, '0_117', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (855, 26, '0_109', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (856, 26, '15_0_102', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (857, 26, '0_361', 'ci', 2);
INSERT INTO `itom_role_resource_perms` VALUES (858, 26, '14_0_105', 'ci', 1);
INSERT INTO `itom_role_resource_perms` VALUES (859, 26, '0_251', 'ci', 1);

-- ----------------------------
-- Table structure for user_face_info
-- ----------------------------
DROP TABLE IF EXISTS `user_face_info`;
CREATE TABLE `user_face_info`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `account` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '用户账号',
  `face_id` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '人脸识别后的face_id',
  `create_time` bigint NOT NULL DEFAULT 0 COMMENT '创建时间',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '备注',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '人脸信息记录表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of user_face_info
-- ----------------------------

-- ----------------------------
-- Table structure for user_login_info
-- ----------------------------
DROP TABLE IF EXISTS `user_login_info`;
CREATE TABLE `user_login_info`  (
  `guid` varchar(40) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `account` varchar(32) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `ip` varchar(128) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
  `type` int NOT NULL,
  `first_login_ts` bigint NOT NULL,
  `last_login_ts` bigint NOT NULL,
  `status` tinyint NOT NULL,
  `created_at` datetime(3) NULL DEFAULT NULL,
  `updated_at` datetime(3) NULL DEFAULT NULL,
  `deleted` tinyint NOT NULL,
  PRIMARY KEY (`guid`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of user_login_info
-- ----------------------------
INSERT INTO `user_login_info` VALUES ('026cb845-cdb5-4f53-a8a2-cc3207381f1a', 'admin', '22.80.66.142', 1, 1766326024, 1766392973, 2, '2025-12-21 22:07:04.019', '2025-12-23 12:34:36.400', 0);
INSERT INTO `user_login_info` VALUES ('0755071f-69fa-4cd4-9f15-3ca1f441ab89', 'admin', '21.80.66.218', 1, 1766152943, 1766564458, 1, '2025-12-19 22:02:23.293', '2025-12-24 16:20:58.952', 0);
INSERT INTO `user_login_info` VALUES ('076e24a3-3cde-4382-b745-133641cdce73', 'dhjk', '22.80.45.83', 1, 1766366500, 1766409592, 2, '2025-12-22 09:21:40.118', '2025-12-22 21:21:11.972', 0);
INSERT INTO `user_login_info` VALUES ('207957a9-f46f-4f86-aa1d-85696c66baca', 'admin', '21.80.66.108', 1, 1766459647, 1766459647, 2, '2025-12-23 11:14:07.568', '2025-12-23 16:00:07.921', 0);
INSERT INTO `user_login_info` VALUES ('329ae89b-8033-437a-975d-49344246a7ba', 'dhjk', '21.80.66.143', 1, 1766394811, 1766566564, 2, '2025-12-22 17:13:31.344', '2025-12-24 17:06:15.031', 0);
INSERT INTO `user_login_info` VALUES ('53742c43-351e-457d-ada9-2a1997cdc052', 'admin', '22.80.45.83', 1, 1766366559, 1766366559, 2, '2025-12-22 09:22:39.307', '2025-12-22 09:23:25.785', 0);
INSERT INTO `user_login_info` VALUES ('71396648-df3b-49cb-ace8-071bc7a6461a', 'admin', '22.80.7.91', 1, 1766460129, 1766460129, 2, '2025-12-23 11:22:09.924', '2025-12-23 11:23:43.393', 0);
INSERT INTO `user_login_info` VALUES ('842fd99b-fddf-4d83-b043-1a6dd89b58d0', 'admin', '21.80.66.102', 1, 1766489806, 1766497103, 2, '2025-12-23 19:36:46.993', '2025-12-23 22:04:46.490', 0);
INSERT INTO `user_login_info` VALUES ('96ba3f86-b374-4b72-8fb9-0f0bd861238c', 'dhjk', '22.80.66.142', 1, 1766385556, 1766443759, 2, '2025-12-22 14:39:16.777', '2025-12-23 06:49:39.678', 0);
INSERT INTO `user_login_info` VALUES ('be2a5fbe-99b9-4f2a-9665-0cfb469c24d5', 'admin', '21.80.66.143', 1, 1766045215, 1766567046, 2, '2025-12-18 16:06:55.496', '2025-12-24 19:26:22.670', 0);

-- ----------------------------
-- Table structure for web_user_face_info
-- ----------------------------
DROP TABLE IF EXISTS `web_user_face_info`;
CREATE TABLE `web_user_face_info`  (
  `id` int NOT NULL AUTO_INCREMENT,
  `account` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '用户账号',
  `face_id` text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL COMMENT '人脸识别后的face_id',
  `create_time` bigint NOT NULL DEFAULT 0 COMMENT '创建时间',
  `remark` varchar(255) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '' COMMENT '备注',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8 COLLATE = utf8_general_ci COMMENT = '人脸信息记录表' ROW_FORMAT = Dynamic;

-- ----------------------------
-- Records of web_user_face_info
-- ----------------------------

SET FOREIGN_KEY_CHECKS = 1;
