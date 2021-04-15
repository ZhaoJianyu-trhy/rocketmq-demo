package com.itheima.mq.rocketmq.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * 发送异步消息
 */
public class AsyncProducer {
    public static void main(String[] args) throws Exception {
//        1.创建消息生产者producer，并制定生产者组名
        DefaultMQProducer mqProducer = new DefaultMQProducer("group1");
//        2.指定Nameserver地址
        mqProducer.setNamesrvAddr("192.168.137.20:9876;192.168.137.21:9876");
//        3.启动producer
        mqProducer.start();
//        4.创建消息对象，指定主题Topic、Tag和消息体
        for (int i = 0; i < 10; i++) {
            Message message = new Message("base", "tag2", ("hello world" + i).getBytes());
            //        5.发送消息
            mqProducer.send(message, new SendCallback() {
                public void onSuccess(SendResult sendResult) {
                    //发送成功回调函数
                    System.out.println("发送成功：" + sendResult);
                }

                public void onException(Throwable throwable) {
                    //发送失败回调函数
                    System.out.println("发送失败：" + throwable);
                }
            });
            //线程睡眠1s
            TimeUnit.SECONDS.sleep(1);
        }
//        6.关闭生产者producer
        mqProducer.shutdown();
    }
}
