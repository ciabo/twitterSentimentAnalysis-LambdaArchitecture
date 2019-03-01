package masterdataset;

import com.backtype.hadoop.pail.PailStructure;

import java.io.*;
import java.util.Collections;
import java.util.List;

public class TweetStructure implements PailStructure<Tweet> {
    public Class getType() {
        return Tweet.class;
    }

    public byte[] serialize(Tweet tweet) {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(byteOut);
        try {
            dataOut.writeInt(tweet.getDate().getBytes().length);
            dataOut.write(tweet.getDate().getBytes());
            dataOut.writeInt(tweet.getTimestamp().getBytes().length);
            dataOut.write(tweet.getTimestamp().getBytes());
            dataOut.writeInt(tweet.getTweet().getBytes().length);
            dataOut.write(tweet.getTweet().getBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOut.toByteArray();
    }

    public Tweet deserialize(byte[] serialized) {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(serialized);
        DataInputStream dataIn = new DataInputStream(byteIn);
        try {
            byte[] date = new byte[dataIn.readInt()];
            dataIn.read(date);
            byte[] timeStamp = new byte[dataIn.readInt()];
            dataIn.read(timeStamp);
            byte[] tweet = new byte[dataIn.readInt()];
            dataIn.read(tweet);
            return new Tweet(new String(date), new String(timeStamp), new String(tweet));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> getTarget(Tweet obj) {
        return Collections.EMPTY_LIST;
    }

    public boolean isValidTarget(String... dirs) {
        return true;
    }
}