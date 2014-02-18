package org.zeromq.examples.loadbalancer;

import java.util.LinkedList;
import java.util.Queue;

import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Poller;
import org.zeromq.ZMQ.Socket;

/**
 * ZeroMQ Master that accepts READY messages as requests from workers and sends
 * tasks to available workers via a ROUTER socket.
 * 
 * @author Stephen Riesenberg
 */
public class Master implements Runnable {
	private static final Logger log = Logger.getLogger(Master.class);
	private static final byte[] EMPTY_FRAME = new byte[0];
	
	private ZContext context;
	private Socket frontend;
	private Socket backend;
	
	private String endpoint = "tcp://*:5555";
	private Queue<String> workerQueue = new LinkedList<String>();
	
	/**
	 * Constructor.
	 */
	public Master() {
		this.context = new ZContext();
	}
	
	/**
	 * Constructor.
	 * 
	 * @param context An existing ZContext to reuse (shadow)
	 */
	public Master(ZContext context) {
		this.context = ZContext.shadow(context);
	}
	
	public void setEndpoint(String endpoint) {
		this.endpoint = endpoint;
	}
	
	public void setIoThreads(int ioThreads) {
		context.setIoThreads(ioThreads);
	}
	
	/**
	 * Create and return a pre-configured PUSH socket for communicating with
	 * this Master's frontend PULL socket, which sends work to Workers.
	 * 
	 * @return An inproc PUSH socket
	 */
	public Socket createSocket() {
		String inproc = String.format("inproc://frontend-%d", Integer.valueOf(hashCode()));
		log.info(String.format("F: Connecting PUSH socket to %s", inproc));
		Socket push = context.createSocket(ZMQ.PUSH);
		push.connect(inproc);
		
		return push;
	}
	
	@Override
	public void run() {
		// bind frontend socket for inproc worker threads
		String inproc = String.format("inproc://frontend-%d", Integer.valueOf(hashCode()));
		log.info(String.format("F: Binding PULL socket to %s", inproc));
		frontend = context.createSocket(ZMQ.PULL);
		frontend.bind(inproc);
		
		// bind backend socket to local endpoint
		log.info(String.format("B: Binding ROUTER socket to %s", endpoint));
		backend = context.createSocket(ZMQ.ROUTER);
		backend.bind(endpoint);
		
		// start main loop
		Poller poller = new Poller(2);
		poller.register(backend, Poller.POLLIN);
		while (!Thread.currentThread().isInterrupted()) {
			// only pull a frontend message if a worker is available
			if (!workerQueue.isEmpty())
				poller.register(frontend, ZMQ.Poller.POLLIN);
			
			if (poller.poll() < 0)
				break;
			
			// enqueue a worker
			if (poller.pollin(0))
				receiveFromWorker();
			
			// dequeue a worker and task with work
			if (poller.pollin(1))
				receiveFromThread();
			
			// reset poller for next iteration
			poller.unregister(frontend);
		}
		
		log.info("Master has shut down");
	}
	
	private void receiveFromWorker() {
		String workerAddress = backend.recvStr();
		backend.recv(); // empty frame
		String command = backend.recvStr();
		switch (command) {
			case "READY":
				workerQueue.add(workerAddress);
				break;
		}
	}
	
	private void receiveFromThread() {
		String workerAddress = workerQueue.poll();
		backend.send(workerAddress, ZMQ.SNDMORE);
		backend.send(EMPTY_FRAME, ZMQ.SNDMORE);
		backend.send(frontend.recv(), 0);
	}
	
	/**
	 * Destroy this Master's ZMQ context.
	 */
	public void destroy() {
		context.destroy();
	}
}
