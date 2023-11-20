package ru.job4j.pooh;

import java.util.concurrent.*;

public class QueueSchema implements Schema {

    // контейнер  для регистрации потребителей
    private final ConcurrentHashMap<String, CopyOnWriteArrayList<Receiver>> receivers = new ConcurrentHashMap<>();

     // контейнер используется для аккумуляции сообщений от поставщика
    private final ConcurrentHashMap<String, BlockingQueue<String>> data = new ConcurrentHashMap<>();

    // многопоточный блокирующий флаг. С помощью этого флага мы регулируем работу очередей в контейнере data
    private final Condition condition = new Condition();

    @Override
    public void addReceiver(Receiver receiver) {
        receivers.putIfAbsent(receiver.name(), new CopyOnWriteArrayList<>());
        receivers.get(receiver.name()).add(receiver);
        condition.on();
    }

    @Override
    public void publish(Message message) {
        data.putIfAbsent(message.name(), new LinkedBlockingQueue<>());
        data.get(message.name()).add(message.text());
        condition.on();
    }

    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            // перебираем всех зарегистрированных потребителей
            for (var queueKey : receivers.keySet()) {
                // возвращаем очередь сообщений от поставщика, либо пустую очередь
                var queue = data.getOrDefault(queueKey, new LinkedBlockingQueue<>());
                // возвращаем очередь сообщений от потребителя
                var receiversByQueue = receivers.get(queueKey);
                var it = receiversByQueue.iterator();
                while (it.hasNext()) {
                    var data = queue.poll();
                    if (data != null) {
                        it.next().receive(data);
                    }
                    if (data == null) {
                        break;
                    }
                    if (!it.hasNext()) {
                        it = receiversByQueue.iterator();
                    }
                }
            }
            condition.off();
            try {
                condition.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
