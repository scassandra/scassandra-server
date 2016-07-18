package common;

import java.io.IOException;
import java.net.ServerSocket;

public class PortLocator {

    public static int findFreePort() {
        ServerSocket socket = null;
        try {
            // let the system pick an ephemeral port.
            socket = new ServerSocket(0);
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        } catch (IOException e) {
            // throw as RuntimeException, this would be completely
            // unexpected and we don't expect callers to have a
            // strategy to handle this.
            throw new RuntimeException(e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
