package edu.viosng.data.storage.sample;

import edu.viosng.data.storage.example.GreeterGrpc;
import edu.viosng.data.storage.example.HelloReply;
import edu.viosng.data.storage.example.HelloRequest;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.time.LocalDateTime;

/**
 * Created by viosn_000 on 09.09.2016.
 */
@Service
public class HelloWorldServer {
    private static final Logger logger = LoggerFactory.getLogger(HelloWorldServer.class.getName());

    /* The port on which the server should run */
    private int port = 50051;
    private Server server;

    @PostConstruct
    private void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new GreeterImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
    }


    @PreDestroy
    private void stop() {
        // Use stderr here since the logger may have been reset by its JVM shutdown hook.
        logger.error("*** shutting down gRPC server since JVM is shutting down");
        if (server != null) {
            server.shutdown();
        }
        logger.error("*** server shut down");
    }

    private class GreeterImpl extends GreeterGrpc.GreeterImplBase {

        @Override
        public void sayHello(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder().setMessage("Hello " + req.getName()).build();
            logger.info("Said hello to {}", req.getName());
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void sendTime(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            HelloReply reply = HelloReply.newBuilder().setMessage(LocalDateTime.now().toString()).build();
            logger.info("Said time to {}", request.getName());
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }
    }
}
