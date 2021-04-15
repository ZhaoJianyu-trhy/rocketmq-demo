package com.itheima.mq.rocketmq.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.concurrent.TimeUnit;

/**
 * 发送同步消息
 */
public class Producer {
    public static void main(String[] args) throws Exception {
//        1.创建消息生产者producer，并制定生产者组名
        TransactionMQProducer mqProducer = new TransactionMQProducer("group5");
//        2.指定Nameserver地址
        mqProducer.setNamesrvAddr("192.168.137.20:9876;192.168.137.21:9876");

        //设置事务监听器
        mqProducer.setTransactionListener(new TransactionListener() {
            /**
             * 在本地先执行一个事务
             * @param message
             * @param o
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                if (StringUtils.equals("tagA", message.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (StringUtils.equals("tagB", message.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else {
                    return LocalTransactionState.UNKNOW;
                }
            }

            /**
             * 检查事务的状态
             * @param messageExt
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("消息的tag：" + messageExt.getTags());
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });

//        3.启动producer
        mqProducer.start();
        String[] tags = {"tagA", "tagB", "tagC"};
//        4.创建消息对象，指定主题Topic、Tag和消息体
        for (int i = 0; i < 3; i++) {
            Message message = new Message("transactionTopic", tags[i % 3], ("hello world" + i).getBytes());
            //        5.发送消息
            SendResult result = mqProducer.sendMessageInTransaction(message, null);
            System.out.println("发送结果：" + result);
            //线程睡眠1s
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
