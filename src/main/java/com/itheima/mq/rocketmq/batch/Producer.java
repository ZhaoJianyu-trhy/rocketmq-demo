package com.itheima.mq.rocketmq.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.ArrayList;
import java.util.List;

/**
 * 发送同步消息
 */
public class Producer {
    public static void main(String[] args) throws Exception {
//        1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer mqProducer = new DefaultMQProducer("group1");
//        2.指定Nameserver地址
        mqProducer.setNamesrvAddr("192.168.137.20:9876;192.168.137.21:9876");
//        3.启动producer
        mqProducer.start();
        List<Message> msgs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Message message = new Message("batch", "tag1", ("hello world" + i).getBytes());
            msgs.add(message);
        }

        SendResult result = mqProducer.send(msgs);
        System.out.println("发送结果：" + result);

//        6.关闭生产者producer
        mqProducer.shutdown();
    }
}
