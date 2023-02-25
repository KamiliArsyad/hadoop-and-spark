/*
ENTER YOUR NAME HERE
NAME: Arsyad Ibadurrahman Kamili
MATRICULATION NUMBER: A0244135M
*/
import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileSplit;


public class TopkCommonWords {

    /**
     * This mapper takes in a document, tokenize it, and returns the key-value pair
     * <word, document> where document is the document ID (either one or two).
     *
     * The output will be processed by the reducer.
     * */
    public static class DualTokenMapper
        extends Mapper<Object, Text, Text, IntWritable> {

      private final static String FIRST_FILE = "task1-input1.txt";
      private IntWritable document = new IntWritable(); // 1 for document 1, 2 for doc 2
      private Text word = new Text();
      private Set<String> listOfStopWords;

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
        String filename = context.getConfiguration().get("stop words");
        File f = new File(path);
        BufferedReader buf = new BufferedReader(new FileReader(f));
        
        listOfStopWords = new HashSet<>();
        String stopWord = buf.readLine();

        while (stopWord != null) {
          listOfStopWords.add(stopWord);
          stopWord = buf.readLine();
        }
      }

      @Override
      public void map(Object key, Text val, Context context
          ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(val.toString());
        document.set(
            ((FileSplit) context.getInputSplit())
              .getPath()
              .getName() == FIRST_FILE ? 1 : 2);
        while (itr.hasMoreTokens()) {
          /**
           * Making sure that the next word is not a stop word.
           */
          String nextWord = itr.nextToken();
          if (!listOfStopWords.contains(nextWord)) {
            word.set(nextWord);
            context.write(word, document);
          }
        }
      }
    }

    public static class DocWordCountReducer
         extends Reducer<Text, IntWritable, IntWritable, Text> {
      private Integer res;
      private Map<Integer, Text> orderedResult;
      /* Stores the frequency of common word */
      private Map<String, Integer> wordCountMap; 

      @Override
      protected void setup(Context context) {
        orderedResult = new TreeMap<>();
        wordCountMap = new HashMap<>();
      }

      @Override
      public void reduce(Text key, Iterable<IntWritable> documents, Context context
                    ) throws IOException, InterruptedException {
        int s1 = 0;
        int s2 = 0;
        int count = wordCountMap.getOrDefault(key.toString(), 0); 
        boolean existsInFirst = false;
        boolean existsInSecond = false;

        for (IntWritable document : documents) {
          if (document.get() == 1) {
            existsInFirst = true;
            s1 += 1;
          } else {
            existsInSecond = true;
            s2 += 1;
          }
        }

        count += Math.min(s1, s2);

        // Is the word a common word?
        if (existsInFirst && existsInSecond) {
          /**
           * Reduce will only be called once for each key. In other words,
           * the frequency of the word will be
           * */
        } 
      }

    }
    

    public static void main(String[] args){

    } 
}
