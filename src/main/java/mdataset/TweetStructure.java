package mdataset;

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
            dataOut.writeInt(tweet.getTweet().getBytes().length);
            dataOut.write(tweet.getTweet().getBytes());
            dataOut.writeInt(tweet.getDate());
            dataOut.writeInt(tweet.getTimestamp());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return byteOut.toByteArray();
    }

    public Tweet deserialize(byte[] serialized) {
        ByteArrayInputStream byteIn = new ByteArrayInputStream(serialized);
        DataInputStream dataIn = new DataInputStream(byteIn);
        try {
            byte[] tweet = new byte[dataIn.readInt()];
            dataIn.read(tweet);
            return new Tweet(new String(tweet), dataIn.readInt(), dataIn.readInt());
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

