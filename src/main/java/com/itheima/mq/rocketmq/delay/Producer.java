package com.itheima.mq.rocketmq.delay;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

public class Producer {
    public static void main(String[] args) throws Exception {
//        1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer mqProducer = new DefaultMQProducer("group1");
//        2.指定Nameserver地址
        mqProducer.setNamesrvAddr("192.168.137.20:9876;192.168.137.21:9876");
//        3.启动producer
        mqProducer.start();
//        4.创建消息对象，指定主题Topic、Tag和消息体
        for (int i = 0; i < 10; i++) {
            Message message = new Message("delay", "tag1", ("hello world" + i).getBytes());
            //设置延时时间
            message.setDelayTimeLevel(2);
            //        5.发送消息
            SendResult result = mqProducer.send(message);
            //发送状态
            SendStatus sendStatus = result.getSendStatus();
            //消息id
            String msgId = result.getMsgId();
            //消息队列id
            int queueId = result.getMessageQueue().getQueueId();
            System.out.println("发送结果：" + result);
            //线程睡眠1s
            TimeUnit.SECONDS.sleep(1);
        }
//        6.关闭生产者producer
        mqProducer.shutdown();
    }
}
