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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopkCommonWords {

    /**
     * This mapper takes in a document, tokenize it, and returns the key-value pair
     * <word, document> where document is the document ID (either one or two).
     *
     * The output will be processed by the reducer.
     * */
    public static class DualTokenMapper
        extends Mapper<Object, Text, Text, IntWritable> {

      private final static String FIRST_FILE = "Task1-input1.txt";
      private IntWritable document = new IntWritable(); // 1 for document 1, 2 for doc 2
      private Text word = new Text();
      private Set<String> listOfStopWords;

      @Override
      protected void setup(Context context) throws IOException, InterruptedException {
        String filename = context.getConfiguration().get("stop words");
        File f = new File(filename);
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
        String name = ((FileSplit) context.getInputSplit()).getPath().getName();
        document.set(name.contains("input1") ? 1 : 2);
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
      /* Stores the frequency of common word */
      private Map<String, Integer> wordCountMap; 

      @Override
      protected void setup(Context context) {
        wordCountMap = new HashMap<>();
      }

      @Override
      public void reduce(Text key, Iterable<IntWritable> documents, Context context
                    ) throws IOException, InterruptedException {
        int s1 = 0;
        int s2 = 0;
        int frequency = wordCountMap.getOrDefault(key.toString(), 0); 
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

        frequency += Math.min(s1, s2);

        // Is the word a common word and longer than 4 characters?
        if (existsInFirst && existsInSecond && key.toString().length() > 4) {
          // context.write(new IntWritable(frequency), key);
          wordCountMap.put(key.toString(), frequency);
        } 
      }

      /**
       * Output the top k common words. This method will be more efficient as opposed to using a tree map
       * and creating an arraylist for every frequency if all the words' frequency can be stored in the cache.
       * @param context
       * @throws IOException
       * @throws InterruptedException
       */
      @Override
      protected void cleanup(Context context) throws IOException, InterruptedException {
        // Sort the map by value and make sure that if they have the same value,
        // the key is sorted in lexicographical order.
        List<Map.Entry<String, Integer>> list = new ArrayList<>(wordCountMap.entrySet());
        list.sort((o1, o2) -> o1.getValue() == o2.getValue() 
                    ? o1.getKey().compareTo(o2.getKey()) 
                    : o2.getValue() - o1.getValue());
                
        // Get the top k common words
        int k = context.getConfiguration().getInt("k", 0);
        for (int i = 0; i < k && i < list.size(); i++) {
          Map.Entry<String, Integer> entry = list.get(i);
          context.write(new IntWritable(entry.getValue()), new Text(entry.getKey()));
        }
      }
    }
    

    public static void main(String[] args) throws Exception {
      // Input format: <input file 1> <input file 2> <stop words> <output> <k>
      Path stopWords = new Path(args[2]);
      int k = Integer.parseInt(args[4]);

      Configuration conf = new Configuration();
      conf.set("stop words", stopWords.toString());
      conf.setInt("k", k);

      Job job = Job.getInstance(conf, "Top k common words");
      job.setJarByClass(TopkCommonWords.class);
      job.setMapperClass(DualTokenMapper.class);
      job.setReducerClass(DocWordCountReducer.class);
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(IntWritable.class);
      job.setOutputKeyClass(IntWritable.class);
      job.setOutputValueClass(Text.class);
      
      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileInputFormat.addInputPath(job, new Path(args[1]));

      FileOutputFormat.setOutputPath(job, new Path(args[3]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    } 
}
