import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.hazelcast.jet.pipeline.SourceBuilder;
import com.hazelcast.jet.pipeline.StreamSource;
import com.pubnub.api.PNConfiguration;
import com.pubnub.api.PubNub;
import com.pubnub.api.callbacks.PNCallback;
import com.pubnub.api.callbacks.SubscribeCallback;
import com.pubnub.api.enums.PNStatusCategory;
import com.pubnub.api.models.consumer.PNPublishResult;
import com.pubnub.api.models.consumer.PNStatus;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;
import com.pubnub.api.models.consumer.pubsub.PNPresenceEventResult;

import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class PubNubSource {
    public static StreamSource<PNMessageResult> PubNubSource() {
        return SourceBuilder.timestampedStream("twitter-source", x -> new MySourceGenerator())
                            .fillBufferFn(MySourceGenerator::fillBuffer).build();
    }

    private static class MySourceGenerator {
        LinkedBlockingQueue<PNMessageResult> queue = new LinkedBlockingQueue<>();

        public MySourceGenerator() {
            PNConfiguration pnConfiguration = new PNConfiguration();
            pnConfiguration.setSubscribeKey("sub-c-78806dd4-42a6-11e4-aed8-02ee2ddab7fe");
            pnConfiguration.setSecure(false);
            final String channelName = "pubnub-twitter";
            PubNub pubnub = new PubNub(pnConfiguration);

            pubnub.addListener(new SubscribeCallback() {
                @Override
                public void status(PubNub pubnub, PNStatus status) {
                    if (status.getOperation() != null) {
                        switch (status.getOperation()) {
                            // let's combine unsubscribe and subscribe handling for ease of use
                            case PNSubscribeOperation:
                            case PNUnsubscribeOperation:
                                System.out.println("subscribed");
                                // note: subscribe statuses never have traditional
                                // errors, they just have categories to represent the
                                // different issues or successes that occur as part of subscribe
                                switch (status.getCategory()) {
                                    case PNConnectedCategory:
                                        System.out.println("PNConnectedCategory");
                                        // this is expected for a subscribe, this means there is no error or issue whatsoever
                                    case PNReconnectedCategory:
                                        // this usually occurs if subscribe temporarily fails but reconnects. This means
                                        // there was an error but there is no longer any issue
                                    case PNDisconnectedCategory:
                                        // this is the expected category for an unsubscribe. This means there
                                        // was no error in unsubscribing from everything
                                    case PNUnexpectedDisconnectCategory:
                                        // this is usually an issue with the internet connection, this is an error, handle appropriately
                                    case PNAccessDeniedCategory:
                                        System.out.println("PNAccessDeniedCategory");
                                        // this means that PAM does allow this client to subscribe to this
                                        // channel and channel group configuration. This is another explicit error
                                    default:
                                        // More errors can be directly specified by creating explicit cases for other
                                        // error categories of `PNStatusCategory` such as `PNTimeoutCategory` or `PNMalformedFilterExpressionCategory` or `PNDecryptionErrorCategory`
                                }

                            case PNHeartbeatOperation:
                                // heartbeat operations can in fact have errors, so it is important to check first for an error.
                                // For more information on how to configure heartbeat notifications through the status
                                // PNObjectEventListener callback, consult <link to the PNCONFIGURATION heartbeart config>
                                if (status.isError()) {
                                    // There was an error with the heartbeat operation, handle here
                                } else {
                                    // heartbeat operation was successful
                                }
                            default: {
                                // Encountered unknown status type
                            }
                        }
                    } else {
                        // After a reconnection see status.getCategory()
                    }
                }

                @Override
                public void message(PubNub pubnub, PNMessageResult message) {
                    String messagePublisher = message.getPublisher();
                    //                    System.out.println("Message publisher: " + messagePublisher);
                    //                    System.out.println("Message Payload: " + message.getMessage());
                    //                    System.out.println("Message Subscription: " + message.getSubscription());
                    //                    System.out.println("Message Channel: " + message.getChannel());
                    //                    System.out.println("Message timetoken: " + message.getTimetoken());
                    queue.offer(message);
                }

                @Override
                public void presence(PubNub pubnub, PNPresenceEventResult presence) {

                }
            });

            pubnub.subscribe().channels(Arrays.asList(channelName)).execute();
        }

        void fillBuffer(SourceBuilder.TimestampedSourceBuffer<PNMessageResult> buffer) {
            PNMessageResult p;
            while ((p = queue.poll()) != null) {
                buffer.add(p);
            }

        }
    }
}
