package org.zeromq.examples;

import java.util.Random;

import org.apache.log4j.Logger;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;
import org.zeromq.examples.loadbalancer.Master;
import org.zeromq.examples.loadbalancer.Worker;

public class LoadBalancerExample {
	private static final Logger log = Logger.getLogger(LoadBalancerExample.class);
	private static Random rand = new Random(System.currentTimeMillis());
	
	public static void main(String[] args) {
		new LoadBalancerExample().run();
	}
	
	private void run() {
		Master master = new Master();
		Worker worker1 = new Worker();
		Worker worker2 = new Worker();
		Worker worker3 = new Worker();
		
		worker1.setRandomIdentity();
		worker2.setRandomIdentity();
		worker3.setRandomIdentity();
		
		// start background threads
		new Thread(master).start();
		new Thread(worker1).start();
		new Thread(worker2).start();
		new Thread(worker3).start();
		try {
			Thread.sleep(100);
		} catch (InterruptedException ignored) {
		}
		new WorkerThread(worker1.createSocket()).start();
		new WorkerThread(worker1.createSocket()).start();
		new WorkerThread(worker1.createSocket()).start();
		new WorkerThread(worker2.createSocket()).start();
		new WorkerThread(worker3.createSocket()).start();
//		new MasterThread(master.createSocket()).start();
		
		// do some work
		Socket socket = master.createSocket();
		socket.send("Task #1");
		socket.send("Task #2");
		socket.send("Task #3");
		socket.send("Task #4");
		socket.send("Task #5");
		
		try {
			Thread.sleep(250);
		} catch (InterruptedException ignored) {
		}
		
		socket.send("Task #6");
		socket.send("Task #7");
		socket.send("Task #8");
		socket.send("Task #9");
		socket.send("Task #10");
		
		try {
			Thread.sleep(250);
		} catch (InterruptedException ignored) {
		}
		
		socket.send("Task #11");
		socket.send("Task #12");
		socket.send("Task #13");
		socket.send("Task #14");
		socket.send("Task #15");
		
		try {
			Thread.sleep(250);
		} catch (InterruptedException ignored) {
		}
		
		socket.send("Task #16");
		socket.send("Task #17");
		socket.send("Task #18");
		socket.send("Task #19");
		socket.send("Task #20");
		
		try {
			Thread.sleep(2500);
		} catch (InterruptedException ignored) {
		}
		
		log.info("Done with work");
		
		master.destroy();
		worker1.destroy();
		worker2.destroy();
		worker3.destroy();
		
		log.info("Finished");
	}
	
	private static class WorkerThread extends Thread {
		private static int workerIndex = 0;
		private Socket socket;
		private Integer workerId;
		
		public WorkerThread(Socket socket) {
			this.socket = socket;
			this.workerId = Integer.valueOf(++workerIndex);
		}
		
		@Override
		public void run() {
			Poller poller = new Poller(1);
			poller.register(socket, Poller.POLLIN);
			while (!Thread.currentThread().isInterrupted()) {
				socket.send("READY");
				if (poller.poll() < 0)
					break;
				
				String work = socket.recvStr();
				log.info(String.format("WorkerThread %s: Working on %s", workerId, work));
				try {
					long sleep = (long) (rand.nextDouble() * 200);
					Thread.sleep(sleep);
				} catch (InterruptedException ignored) {
				}
				
				log.info(String.format("WorkerThread %s: Finished with %s", workerId, work));
			}
			
			log.info(String.format("WorkerThread %s has exited", workerId));
		}
	}
}
