package edu.ensias.hadoop.hdfslab;
 import java.io.IOException;
 import org.apache.hadoop.conf.Configuration;
 import org.apache.hadoop.fs.Path;
 import org.apache.hadoop.io.*;
 import org.apache.hadoop.mapreduce.Job;
 import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WordCount {

   public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
       if (args.length != 2) {
           System.err.println("Usage: WordCount <input path> <output path>");
           System.exit(-1);
       }

       Configuration conf = new Configuration();
       Job job = Job.getInstance(conf, "word count");
       job.setJarByClass(WordCount.class);

       // Set Mapper Class
       job.setMapperClass(TokenizerMapper.class);

       // Set Shuffle and Reduce Class
       job.setCombinerClass(IntSumReducer.class);
       job.setReducerClass(IntSumReducer.class);
    
       // Set Output Key and Value Class
       job.setOutputKeyClass(Text.class);
       job.setOutputValueClass(IntWritable.class);

       // Set Input and Output Path
       FileInputFormat.addInputPath(job, new Path(args[0]));
       FileOutputFormat.setOutputPath(job, new Path(args[1]));
       System.exit(job.waitForCompletion(true) ? 0 : 1);
   }

}
