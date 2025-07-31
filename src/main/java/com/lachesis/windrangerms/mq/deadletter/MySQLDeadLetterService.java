package com.lachesis.windrangerms.mq.deadletter;

import com.alibaba.fastjson.JSON;
import com.lachesis.windrangerms.mq.config.MQConfig;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterStatusEnum;
import com.lachesis.windrangerms.mq.deadletter.model.RetryHistory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * MySQL实现的死信队列服务
 */
@Slf4j
@Service
public class MySQLDeadLetterService extends AbstractDeadLetterService {

    private JdbcTemplate jdbcTemplate;

    @Autowired(required = false)
    private Map<String, DataSource> dataSourceMap;

    private String deadLetterTableName;
    private String retryHistoryTableName;

    /**
     * 初始化数据源和表名
     */
    @PostConstruct
    public void init() {
        // 检查dataSourceMap是否可用
        if (dataSourceMap == null || dataSourceMap.isEmpty()) {
            log.warn("No DataSource available, MySQL dead letter service will not be initialized");
            return;
        }

        // 获取配置的表名
        deadLetterTableName = mqConfig.getDeadLetter().getMysql().getDeadLetterTableName();
        retryHistoryTableName = mqConfig.getDeadLetter().getMysql().getRetryHistoryTableName();

        // 获取数据源
        String dataSourceName = mqConfig.getDeadLetter().getMysql().getDataSourceName();
        DataSource dataSource;

        if (StringUtils.hasText(dataSourceName)) {
            dataSource = dataSourceMap.get(dataSourceName);
            if (dataSource == null) {
                log.error("DataSource not found with name: {}", dataSourceName);
                throw new IllegalStateException("DataSource not found: " + dataSourceName);
            }
        } else {
            // 使用默认数据源
            dataSource = dataSourceMap.values().iterator().next();
        }

        jdbcTemplate = new JdbcTemplate(dataSource);

        // 如果配置了自动创建表，则创建表
        if (mqConfig.getDeadLetter().getMysql().isAutoCreateTable()) {
            createTablesIfNotExist();
        }
    }

    /**
     * 创建必要的表
     */
    private void createTablesIfNotExist() {
        try {
            // 创建死信队列表
            jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + deadLetterTableName + " ("
                    + "id VARCHAR(64) PRIMARY KEY,"
                    + "original_message_id VARCHAR(64),"
                    + "topic VARCHAR(255) NOT NULL,"
                    + "tag VARCHAR(255),"
                    + "body LONGTEXT NOT NULL,"
                    + "properties TEXT,"
                    + "mq_type VARCHAR(50) NOT NULL,"
                    + "failure_reason TEXT,"
                    + "retry_count INT DEFAULT 0,"
                    + "status VARCHAR(20) NOT NULL,"
                    + "create_timestamp BIGINT NOT NULL,"
                    + "update_timestamp BIGINT NOT NULL,"
                    + "INDEX idx_status (status),"
                    + "INDEX idx_create_timestamp (create_timestamp)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

            // 创建重试历史表
            jdbcTemplate.execute("CREATE TABLE IF NOT EXISTS " + retryHistoryTableName + " ("
                    + "id VARCHAR(64) PRIMARY KEY,"
                    + "message_id VARCHAR(64) NOT NULL,"
                    + "retry_timestamp BIGINT NOT NULL,"
                    + "success BOOLEAN NOT NULL,"
                    + "failure_reason TEXT,"
                    + "retry_count INT NOT NULL,"
                    + "INDEX idx_message_id (message_id)"
                    + ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");

            log.info("Created dead letter queue tables: {} and {}", deadLetterTableName, retryHistoryTableName);
        } catch (Exception e) {
            log.error("Failed to create dead letter queue tables", e);
            throw new RuntimeException("Failed to create dead letter queue tables", e);
        }
    }

    /**
     * 将消息保存到死信队列
     *
     * @param message 死信消息
     * @return 是否保存成功
     */
    @Override
    public boolean saveDeadLetterMessage(DeadLetterMessage message) {
        try {
            if (message.getId() == null) {
                message.setId(generateId());
            }

            if (message.getCreateTimestamp() <= 0) {
                message.setCreateTimestamp(System.currentTimeMillis());
            }

            if (message.getStatusEnum() == null) {
                message.setStatusEnum(DeadLetterStatusEnum.PENDING);
            }

            message.setUpdateTimestamp(System.currentTimeMillis());

            String properties = message.getProperties() != null ? JSON.toJSONString(message.getProperties()) : null;

            jdbcTemplate.update(
                    "INSERT INTO " + deadLetterTableName
                            + " (id, original_message_id, topic, tag, body, properties, mq_type, failure_reason, "
                            + "retry_count, status, create_timestamp, update_timestamp) "
                            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    message.getId(),
                    message.getOriginalMessageId(),
                    message.getTopic(),
                    message.getTag(),
                    message.getBody(),
                    properties,
                    message.getMqType(),
                    message.getFailureReason(),
                    message.getRetryCount(),
                    message.getStatusEnum().name(),
                    message.getCreateTimestamp(),
                    message.getUpdateTimestamp()
            );

            log.info("Saved dead letter message: {}", message.getId());
            return true;
        } catch (Exception e) {
            log.error("Failed to save dead letter message", e);
            return false;
        }
    }

    /**
     * 获取死信队列中的消息列表
     *
     * @param offset 偏移量
     * @param limit  限制数量
     * @return 死信消息列表
     */
    @Override
    public List<DeadLetterMessage> listDeadLetterMessages(int offset, int limit) {
        try {
            return jdbcTemplate.query(
                    "SELECT * FROM " + deadLetterTableName + " ORDER BY create_timestamp DESC LIMIT ? OFFSET ?",
                    new DeadLetterMessageRowMapper(),
                    limit, offset
            );
        } catch (Exception e) {
            log.error("Failed to list dead letter messages", e);
            return Collections.emptyList();
        }
    }

    /**
     * 获取死信队列中的消息总数
     *
     * @return 消息总数
     */
    @Override
    public long countDeadLetterMessages() {
        try {
            return jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + deadLetterTableName, Long.class);
        } catch (Exception e) {
            log.error("Failed to count dead letter messages", e);
            return 0;
        }
    }

    /**
     * 根据ID获取死信消息
     *
     * @param id 消息ID
     * @return 死信消息
     */
    @Override
    public DeadLetterMessage getDeadLetterMessage(String id) {
        try {
            List<DeadLetterMessage> messages = jdbcTemplate.query(
                    "SELECT * FROM " + deadLetterTableName + " WHERE id = ?",
                    new DeadLetterMessageRowMapper(),
                    id
            );

            return messages.isEmpty() ? null : messages.get(0);
        } catch (Exception e) {
            log.error("Failed to get dead letter message: {}", id, e);
            return null;
        }
    }

    /**
     * 删除死信消息
     *
     * @param id 消息ID
     * @return 是否删除成功
     */
    @Override
    public boolean deleteDeadLetterMessage(String id) {
        try {
            // 删除消息
            int deletedRows = jdbcTemplate.update("DELETE FROM " + deadLetterTableName + " WHERE id = ?", id);

            // 删除重试历史
            jdbcTemplate.update("DELETE FROM " + retryHistoryTableName + " WHERE message_id = ?", id);

            log.info("Deleted dead letter message: {}", id);
            return deletedRows > 0;
        } catch (Exception e) {
            log.error("Failed to delete dead letter message: {}", id, e);
            return false;
        }
    }

    /**
     * 批量删除死信消息
     *
     * @param ids 消息ID列表
     * @return 成功删除的消息数量
     */
    @Override
    public int deleteDeadLetterMessages(List<String> ids) {
        int successCount = 0;
        for (String id : ids) {
            if (deleteDeadLetterMessage(id)) {
                successCount++;
            }
        }
        return successCount;
    }

    /**
     * 清理过期的死信消息
     * 定时任务，根据配置的清理周期执行
     */
    @Scheduled(fixedDelayString = "${mq.dead-letter.cleanup.cleanup-interval:86400000}")
    public void scheduledCleanup() {
        if (!mqConfig.getDeadLetter().getCleanup().isEnabled()) {
            return;
        }

        // 检查jdbcTemplate是否已初始化
        if (jdbcTemplate == null) {
            log.debug("MySQL dead letter service not initialized, skipping cleanup");
            return;
        }

        long expireTime = System.currentTimeMillis() - mqConfig.getDeadLetter().getCleanup().getMessageRetentionTime();
        int count = cleanupExpiredMessages(expireTime);
        log.info("Scheduled cleanup: removed {} expired dead letter messages", count);
    }

    /**
     * 清理过期消息
     *
     * @return 清理的消息数量
     */
    @Override
    public int cleanupExpiredMessages() {
        if (jdbcTemplate == null) {
            log.debug("MySQL dead letter service not initialized, cannot cleanup expired messages");
            return 0;
        }
        long expireTime = System.currentTimeMillis() - mqConfig.getDeadLetter().getCleanup().getMessageRetentionTime();
        return cleanupExpiredMessages(expireTime);
    }

    /**
     * 清理过期的死信消息
     *
     * @param expireTime 过期时间（毫秒）
     * @return 清理的消息数量
     */
    public int cleanupExpiredMessages(long expireTime) {
        if (jdbcTemplate == null) {
            log.debug("MySQL dead letter service not initialized, cannot cleanup expired messages");
            return 0;
        }
        
        try {
            // 获取过期消息ID
            List<String> expiredIds = jdbcTemplate.queryForList(
                    "SELECT id FROM " + deadLetterTableName + " WHERE create_timestamp < ?",
                    String.class,
                    expireTime
            );

            // 删除过期消息
            return deleteDeadLetterMessages(expiredIds);
        } catch (Exception e) {
            log.error("Failed to cleanup expired messages", e);
            return 0;
        }
    }

    /**
     * 记录重试历史
     *
     * @param messageId 消息ID
     * @param result    重试结果
     * @param reason    失败原因（如果失败）
     * @return 是否记录成功
     */
    @Override
    public boolean recordRetryHistory(String messageId, boolean result, String reason) {
        try {
            DeadLetterMessage message = getDeadLetterMessage(messageId);
            if (message == null) {
                log.warn("Cannot record retry history for non-existent message: {}", messageId);
                return false;
            }

            String id = generateId();
            long timestamp = System.currentTimeMillis();

            jdbcTemplate.update(
                    "INSERT INTO " + retryHistoryTableName
                            + " (id, message_id, retry_timestamp, success, failure_reason, retry_count) "
                            + "VALUES (?, ?, ?, ?, ?, ?)",
                    id,
                    messageId,
                    timestamp,
                    result,
                    reason,
                    message.getRetryCount()
            );

            return true;
        } catch (Exception e) {
            log.error("Failed to record retry history for message: {}", messageId, e);
            return false;
        }
    }

    /**
     * 获取消息的重试历史
     *
     * @param messageId 消息ID
     * @return 重试历史列表
     */
    @Override
    public List<RetryHistory> getRetryHistory(String messageId) {
        try {
            return jdbcTemplate.query(
                    "SELECT * FROM " + retryHistoryTableName + " WHERE message_id = ? ORDER BY retry_timestamp DESC",
                    new RetryHistoryRowMapper(),
                    messageId
            );
        } catch (Exception e) {
            log.error("Failed to get retry history for message: {}", messageId, e);
            return Collections.emptyList();
        }
    }

    /**
     * 更新死信消息
     *
     * @param message 死信消息
     * @return 是否更新成功
     */
    @Override
    protected boolean updateDeadLetterMessage(DeadLetterMessage message) {
        try {
            message.setUpdateTimestamp(System.currentTimeMillis());
            String properties = message.getProperties() != null ? JSON.toJSONString(message.getProperties()) : null;

            int updatedRows = jdbcTemplate.update(
                    "UPDATE " + deadLetterTableName + " SET "
                            + "topic = ?, tag = ?, body = ?, properties = ?, mq_type = ?, "
                            + "failure_reason = ?, retry_count = ?, status = ?, update_timestamp = ? "
                            + "WHERE id = ?",
                    message.getTopic(),
                    message.getTag(),
                    message.getBody(),
                    properties,
                    message.getMqType(),
                    message.getFailureReason(),
                    message.getRetryCount(),
                    message.getStatusEnum().name(),
                    message.getUpdateTimestamp(),
                    message.getId()
            );

            return updatedRows > 0;
        } catch (Exception e) {
            log.error("Failed to update dead letter message: {}", message.getId(), e);
            return false;
        }
    }

    /**
     * 死信消息行映射器
     */
    private static class DeadLetterMessageRowMapper implements RowMapper<DeadLetterMessage> {
        @Override
        public DeadLetterMessage mapRow(ResultSet rs, int rowNum) throws SQLException {
            DeadLetterMessage message = new DeadLetterMessage();
            message.setId(rs.getString("id"));
            message.setOriginalMessageId(rs.getString("original_message_id"));
            message.setTopic(rs.getString("topic"));
            message.setTag(rs.getString("tag"));
            message.setBody(rs.getString("body"));

            String propertiesJson = rs.getString("properties");
            if (StringUtils.hasText(propertiesJson)) {
                message.setProperties(JSON.parseObject(propertiesJson, Map.class));
            }

            message.setMqType(rs.getString("mq_type"));
            message.setFailureReason(rs.getString("failure_reason"));
            message.setRetryCount(rs.getInt("retry_count"));
            message.setStatusEnum(DeadLetterStatusEnum.valueOf(rs.getString("status")));
            message.setCreateTimestamp(rs.getLong("create_timestamp"));
            message.setUpdateTimestamp(rs.getLong("update_timestamp"));

            return message;
        }
    }

    /**
     * 重试历史行映射器
     */
    private static class RetryHistoryRowMapper implements RowMapper<RetryHistory> {
        @Override
        public RetryHistory mapRow(ResultSet rs, int rowNum) throws SQLException {
            RetryHistory history = new RetryHistory();
            history.setId(rs.getString("id"));
            history.setMessageId(rs.getString("message_id"));
            history.setRetryTimestamp(rs.getLong("retry_timestamp"));
            history.setSuccess(rs.getBoolean("success"));
            history.setFailureReason(rs.getString("failure_reason"));
            history.setRetryCount(rs.getInt("retry_count"));

            return history;
        }
    }
}