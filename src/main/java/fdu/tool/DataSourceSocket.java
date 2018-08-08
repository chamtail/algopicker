package fdu.tool;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class DataSourceSocket {

    private static String[] urls;

    static {
        urls = new String[]{"www.sogou.com", "www.baidu.com", "www.google.com", "www.yahoo.com", "www.taobao.com",
                "www.jingdong.com", "www.pingduoduo.com", "www.apple.com", "www.tmall.com", "www.tencent.com"};
    }

    private static String randomIp() {
        Random random = new Random();
        return String.format("%s.%s.%s.%s", random.nextInt(256), random.nextInt(256),
                random.nextInt(256), random.nextInt(256));
    }

//    public static void main(String[] args) throws IOException {
//        if (args.length != 3) {
//            System.err.println("Usage: <filename> <port> <millisecond>");
//            System.exit(1);
//        }
//
//        int rowCount = urls.length;
//
//        ServerSocket listener = new ServerSocket(Integer.parseInt(args[1]));
//        while (true) {
//            System.out.println("start accept......");
//            Socket socket = listener.accept();
//            System.out.println("start run......");
//            System.out.println("Got client connected from: " + socket.getInetAddress());
//            try (PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
//                while (true) {
//                    try {
//                        Thread.sleep(Long.parseLong(args[2]));
//                        String content = String.format("%s %s %s", System.currentTimeMillis(), randomIp(), urls[new Random().nextInt(rowCount)]);
//                        System.out.println(content);
//                        out.write(content + '\n');
//                        out.flush();
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//    }
}
