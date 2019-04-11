import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.pubnub.api.models.consumer.pubsub.PNMessageResult;

public class HackathonMain {
    public static void main(String[] args) {
        JetInstance jet = Jet.newJetInstance();

        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(PubNubSource.PubNubSource())
                //                .withoutTimestamps()
                .withTimestamps(PNMessageResult::getTimetoken, 10000)
//                .map(PNMessageResult::getMessage)
//                .map(m -> m.getAsJsonObject().get("text"))
//                .filter(m -> m.getAsJsonObject().get("country") != null)

//                .filter(a -> !"hello".equals(a))

                .drainTo(Sinks.logger());

        jet.newJob(pipeline).join();
    }
}
