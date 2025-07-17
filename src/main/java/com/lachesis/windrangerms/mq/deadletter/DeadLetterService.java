package com.lachesis.windrangerms.mq.deadletter;

import com.lachesis.windrangerms.mq.deadletter.model.DeadLetterMessage;
import com.lachesis.windrangerms.mq.deadletter.model.RetryHistory;

import java.util.List;

/**
 * 死信队列服务接口
 * 定义死信队列的基本操作
 */
public interface DeadLetterService {

    /**
     * 保存死信消息
     *
     * @param message 死信消息
     * @return 是否保存成功
     */
    boolean saveDeadLetterMessage(DeadLetterMessage message);

    /**
     * 获取死信消息
     *
     * @param id 消息ID
     * @return 死信消息
     */
    DeadLetterMessage getDeadLetterMessage(String id);

    /**
     * 删除死信消息
     *
     * @param id 消息ID
     * @return 是否删除成功
     */
    boolean deleteDeadLetterMessage(String id);

    /**
     * 批量删除死信消息
     *
     * @param ids 消息ID列表
     * @return 成功删除的消息数量
     */
    int deleteDeadLetterMessages(List<String> ids);

    /**
     * 列出死信消息
     *
     * @param offset 偏移量
     * @param limit  限制数量
     * @return 死信消息列表
     */
    List<DeadLetterMessage> listDeadLetterMessages(int offset, int limit);

    /**
     * 统计死信消息数量
     *
     * @return 消息数量
     */
    long countDeadLetterMessages();

    /**
     * 重新投递死信消息
     *
     * @param id 消息ID
     * @return 是否重新投递成功
     */
    boolean redeliverMessage(String id);

    /**
     * 批量重新投递死信消息
     *
     * @param ids 消息ID列表
     * @return 成功重新投递的消息数量
     */
    int redeliverMessages(List<String> ids);

    /**
     * 获取重试历史
     *
     * @param messageId 消息ID
     * @return 重试历史列表
     */
    List<RetryHistory> getRetryHistory(String messageId);

    /**
     * 记录重试历史
     *
     * @param messageId 消息ID
     * @param success   是否成功
     * @param errorMsg  错误信息
     * @return 是否记录成功
     */
    boolean recordRetryHistory(String messageId, boolean success, String errorMsg);

    /**
     * 清理过期消息
     *
     * @return 清理的消息数量
     */
    int cleanupExpiredMessages();
}