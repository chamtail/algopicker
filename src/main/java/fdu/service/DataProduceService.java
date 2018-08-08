package fdu.service;

import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
public class DataProduceService {

    private ExecutorService controllers;

    private ExecutorService producers;

    private volatile boolean running;

    private Map<Integer, LinkedBlockingQueue<String>> portDataMap;

    private String[] urls = new String[]{"www.sogou.com", "www.baidu.com", "www.google.com", "www.yahoo.com", "www.taobao.com",
            "www.jd.com", "www.pinduoduo.com", "www.apple.com", "www.tmall.com", "www.tencent.com"};

    @PostConstruct
    private void init() {
        controllers = new ThreadPoolExecutor(1, 1, 0,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> new Thread(r, "data-controller"));
        producers = new ThreadPoolExecutor(2, 2, 0,
                TimeUnit.SECONDS, new LinkedBlockingQueue<>(), r -> new Thread(r, "data-producer"));
        portDataMap = new ConcurrentHashMap<>(3, 1);
        portDataMap.put(9996, new LinkedBlockingQueue<>());
        portDataMap.put(9997, new LinkedBlockingQueue<>());
    }

    private static String randomIp() {
        Random random = new Random();
        return String.format("%s.%s.%s.%s", random.nextInt(256), random.nextInt(256),
                random.nextInt(256), random.nextInt(256));
    }

    public void startProducer() {
        init();
        running = true;
        controllers.submit(() -> {
            while (running) {
                try {
                    Thread.sleep(50L);
                    int rowCount = urls.length;
                    String line = String.format("%s %s %s", System.currentTimeMillis(), randomIp(),
                            urls[new Random().nextInt(rowCount)]);
                    portDataMap.values().forEach(queue -> {
                        try {
                            queue.put(line);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    });
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        for (int port = 9997; port >= 9996; --port) {
            int finalPort = port;
            producers.submit(() -> {
                try {
                    try (ServerSocket listener = new ServerSocket(finalPort);
                         Socket produceSocket = listener.accept();
                         PrintWriter out = new PrintWriter(produceSocket.getOutputStream(), true)) {
                        while (running) {
                            String content = portDataMap.get(finalPort).take();
                            out.write(content + '\n');
                            out.flush();
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    public void stopProducer() {
        running = false;
        controllers.shutdownNow();
        portDataMap.values().forEach(LinkedBlockingQueue::clear);
        portDataMap.clear();
        producers.shutdownNow();
    }
}
