package batchlayer.pail;

public class Tweet {
    private String tweet;
    private String date;
    private String timestamp;

    public Tweet(String date, String timestamp, String tweet) {
        this.tweet = tweet;
        this.date = date;
        this.timestamp = timestamp;
    }

    public String getTweet() {
        return tweet;
    }

    String getDate() {
        return date;
    }

    String getTimestamp() {
        return timestamp;
    }
}

