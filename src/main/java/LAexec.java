import utils.Utils;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class LAexec {
    private String filename;
    private List batch = new ArrayList();
    private List fast = new ArrayList();

    public LAexec(String filename) {
        this.filename = filename;
    }

    public void startLA(int endbatch) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filename));
//            BufferedWriter bwB = new BufferedWriter(new FileWriter("dbBatch.txt"));
//            BufferedWriter bwF = new BufferedWriter(new FileWriter("dbFast.txt"));
            String line;
            int count = 0;
            while ((line = br.readLine()) != null && count < 15) {
                String[] split = line.split(",");
                String tweet = Utils.createTweet(Arrays.copyOfRange(split, 2, split.length));
                if (count < endbatch) {
                    batch.add(Arrays.asList(tweet));
//                    bwB.append(tweet+"\n");
                } else {
                    fast.add(Arrays.asList(tweet));
//                    bwF.append(tweet+"\n");
                }
                count++;
            }
        } catch (IOException e) {
            System.out.println("Error with file %s" + filename);
        }
    }

    public List<String> getBatch() {
        return batch;
    }

    public List<String> getFast() {
        return fast;
    }

}
