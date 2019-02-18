package masterdataset;

public class Tweet {
    private String tweet;
    private int date;
    private int timestamp;

    public Tweet(String tweet, int date, int timestamp) {
        this.tweet = tweet;
        this.date = date;
        this.timestamp = timestamp;
    }

    public String getTweet() {
        return tweet;
    }

    public int getDate() {
        return date;
    }

    public int getTimestamp() {
        return timestamp;
    }
}
