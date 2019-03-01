package fastlayer.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class SentimentRepository {
    private Session session;
    private static SentimentRepository instance = null;

    private SentimentRepository(Session session) {
        this.session = session;
    }

    public static SentimentRepository getInstance(Session session){
        if(instance == null){
            instance = new SentimentRepository(session);
        }
        return instance;
    }

    public static SentimentRepository getInstance(){
        return instance;
    }

    public static boolean isSet(){
        if(instance==null){
            return false;
        }
        return true;
    }

    public void createTable(String tablename) {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(tablename).append("(")
                .append("keyword text, ")
                .append("sentiment int,")
                .append("count counter,")
                .append("PRIMARY KEY (keyword, sentiment) );");
        final String query = sb.toString();
        session.execute(query);
    }

    public int selectCountFromKey(String tablename,String keyword, int sentiment) {
        StringBuilder sb = new StringBuilder("SELECT count FROM ")
                .append(tablename)
                .append(" WHERE keyword = '")
                .append(keyword)
                .append("' AND sentiment = ")
                .append(sentiment)
                .append(";");
        final String query = sb.toString();
        ResultSet rs = session.execute(query);
        Row r = rs.one();
        if(r==null)
             return 0;
        else
            return r.getInt("count");
    }

    public void updateCount(String tablename, String keyword, int sentiment, int newCount) {
        StringBuilder sb = new StringBuilder("UPDATE ")
                .append(tablename)
                .append(" SET count = ")
                .append(newCount)
                .append(" WHERE keyword = '")
                .append(keyword)
                .append("' AND sentiment = ")
                .append(sentiment)
                .append(";");
        final String query = sb.toString();
        session.execute(query);
    }

    public void addToCount(String tablename, String keyword, int sentiment, int value){
        StringBuilder sb = new StringBuilder("UPDATE ")
                .append(tablename)
                .append(" SET count = count + ")
                .append(value)
                .append(" WHERE keyword = '")
                .append(keyword)
                .append("' AND sentiment = ")
                .append(sentiment)
                .append(";");
        final String query = sb.toString();
        session.execute(query);
    }

    public void substractToCount(String tablename, String keyword, int sentiment, int value){
        StringBuilder sb = new StringBuilder("UPDATE ")
                .append(tablename)
                .append(" SET count = count - ")
                .append(value)
                .append(" WHERE keyword = '")
                .append(keyword)
                .append("' AND sentiment = ")
                .append(sentiment)
                .append(";");
        final String query = sb.toString();
        session.execute(query);
    }

    public void deleteTable(String tablename) {
        StringBuilder sb = new StringBuilder("DROP TABLE IF EXISTS ").append(tablename);
        final String query = sb.toString();
        session.execute(query);
    }
}
