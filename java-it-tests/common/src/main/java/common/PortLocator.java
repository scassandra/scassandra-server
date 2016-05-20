package common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Random;

public class PortLocator {
    private static final int MIN_PORT = 1024;
    private static final int MAX_PORT = 65535;
    private static final int MAX_PORT_FAILURES = 10;
    private static final int CONNECT_TIMEOUT_MS = 2000;
    private static final Random random = new Random(System.currentTimeMillis());

    public static int findFreePort() {
        for (int attempts = 1; attempts <= MAX_PORT_FAILURES; attempts++) {
            int port = randomPort();
            if (portFree(port)) {
                return port;
            }
        }
        throw new IllegalStateException("Not possible to find any free port");
    }

    private static int randomPort() {
        int range = MAX_PORT - MIN_PORT;
        return MIN_PORT + random.nextInt(range + 1);
    }

    private static boolean portFree(int port) {
        try {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress("localhost", port), CONNECT_TIMEOUT_MS);
            socket.close();
            return false;
        } catch (IOException e) {
            return true;
        }
    }
}
