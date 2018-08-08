package fdu.bean.executor;

import java.io.*;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * Created by slade on 2016/11/28.
 */

public class ShellExecutor {
	private Process p;
	private Reader reader;
	private Writer writer;
	private Reader error;
//	private static final String sparkHome = "/opt/spark";
	private static final String sparkHome = "/home/hadoop/bigdata/spark-2.0.0-bin-hadoop2.7";
	private static final String hiveHome = "/home/hadoop/bigdata/apache-hive-1.2.2-bin";

	class Reader extends Thread {
		private BufferedReader reader;

		Reader(InputStream inputStream) {
			this.reader = new BufferedReader(new InputStreamReader(inputStream));
		}

		public void run() {
			String line;
			try {
//				while (!isInterrupted() && (line = reader.readLine()) != null) {
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	class Writer extends Thread {
		private PrintWriter writer;
		private BlockingQueue<String> queue;

		public Writer(OutputStream outputStream) {
			writer = new PrintWriter(new OutputStreamWriter(outputStream));
			queue = new LinkedBlockingQueue<>(1);
		}

		public void write(String s) throws InterruptedException {
			queue.put(s);
		}

		public void run() {
			while (!isInterrupted()) {
				try {
					String s = queue.take();
					writer.println(s);
					writer.flush();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public void init() throws IOException {
	}

	public void destroy() {
		reader.interrupt();
		writer.interrupt();
		error.interrupt();
		p.destroy();
	}

	public String executeCommand(String[] command, String masterip, String masterport) throws IOException {
		if (masterip == null && masterport == null) {
			p = Runtime.getRuntime().exec(command);
		} else {
			String sparkShellCommand = sparkHome + "/bin/spark-shell --master spark://" + masterip + ':' + masterport
					+ " --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0" +
					" --master local[4] " +
					" --jars " + hiveHome + "/lib/mysql-connector-java-5.1.6.jar";
			System.out.println(sparkShellCommand);
			p = Runtime.getRuntime().exec(sparkShellCommand);
		}
		
		reader = new Reader(p.getInputStream());
		writer = new Writer(p.getOutputStream());
		error = new Reader(p.getErrorStream());

		reader.start();
		writer.start();
		error.start();

		try {
			writer.write(Arrays.stream(command).collect(Collectors.joining(" ")));
//			Thread.sleep(1000*600);
//			destroy();
		} catch (InterruptedException e) {
			StringWriter writer = new StringWriter();
			e.printStackTrace(new PrintWriter(writer));
//			destroy();
			return writer.toString();
		}
		return "submitted";
	}
}
