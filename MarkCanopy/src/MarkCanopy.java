/**
 * Created by ZhouDavid on 2016/7/12.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;


import java.io.*;
import java.util.*;


public class MarkCanopy {
    public static class MyOutputFormat extends FileOutputFormat<Text,Text>{
        @Override
        public RecordWriter<Text,Text>getRecordWriter(TaskAttemptContext job)
                throws IOException,InterruptedException{
            Configuration conf = job.getConfiguration();
            Path path=getDefaultWorkFile(job,"");
            FileSystem fs = path.getFileSystem(conf);
            FSDataOutputStream fout = fs.create(path,false);
            return new MyWriter(fout);
        }
    }
    public static class MyWriter extends RecordWriter<Text,Text> {
        protected DataOutputStream out;
        private String KeyValueSep;
        public static final String NEW_LINE = "\r\n";

        public MyWriter(DataOutputStream out, String KeyValueSep){
            this.out = out;
            this.KeyValueSep = KeyValueSep;
        }
        public MyWriter(DataOutputStream out){
            this(out,"->");
        }

        @Override
        public void write(Text title,Text link_list)throws IOException,InterruptedException{
            if(title!=null){
                out.write(title.toString().getBytes());
                out.write(this.KeyValueSep.getBytes());
            }
            if(link_list!=null){
                out.write(link_list.getBytes(),0,link_list.getLength());
                out.write(NEW_LINE.getBytes());
            }
        }

        @Override
        public void close(TaskAttemptContext context)throws IOException,InterruptedException{
            out.close();
        }
    }

    public static class CanopyMapper extends Mapper<Object,Text,NullWritable,Text>{
        String centerPath = "E:\\HadoopLab3-MovieCluster\\large_data\\large_canopyCenter_data\\large_canopyCenter_data";
        HashMap<String,HashSet<String>> canopyCenters = new HashMap<String,HashSet<String>>();
        public Integer canopy_num = 0;
        int lthreshold = 2;

        @Override
        public void setup(Context context) throws FileNotFoundException,IOException{
            //读入canopyCenter 文件
            BufferedReader reader = new BufferedReader(new FileReader(centerPath));
            String line = null;
            line = reader.readLine();
            while(line!=null){
                int end = line.indexOf("-");
                canopyCenters.put(line.substring(0,end),genRaterIDs(line,end+2));
                line = reader.readLine();
            }
        }
        public HashSet<String>genRaterIDs(String r,int begin){
            HashSet<String>ans = new HashSet<>();
            int start = begin;
            int end = -1;
            while((end = r.indexOf(":",start))!=-1){
                ans.add(new String(r.substring(start,end)));
                start = r.indexOf(",",end);
                if(start==-1) break;
                start++;
            }
            return ans;
        }
        public static int calSimilarity(HashSet<String> s1,HashSet<String>s2){
            int count =0;
            for(Iterator it = s1.iterator();it.hasNext();){
                if(s2.contains(it.next())){
                    count++;
                }
            }
            return count;
        }

        @Override
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
            String record = value.toString();
            int end = record.indexOf("-");
            HashSet<String> vec = genRaterIDs(record,end+2);
            record+="|||";
            Iterator it = canopyCenters.entrySet().iterator();
            while(it.hasNext()){
                Map.Entry e = (Map.Entry)it.next();
                int share = calSimilarity(vec,(HashSet<String>)e.getValue());
                if(share>=lthreshold){
                    record+=(String)e.getKey()+",";
                }
            }
            record=record.substring(0,record.length()-1);
            context.write(NullWritable.get(),new Text(record));
        }
    }

    public static class CanopyReducer extends Reducer<NullWritable,Text,NullWritable,Text>{
        @Override
        public void reduce(NullWritable key,Iterable<Text>values,Context context)throws IOException, InterruptedException{
            for(Text val:values){
                context.write(NullWritable.get(),val);
            }
        }
    }
    public static void main(String args[])throws Exception{
        System.err.println("reading inverted_list....");
        System.err.println("start mapreduce....");
        String inPath = "E:\\HadoopLab3-MovieCluster\\large_data\\large_reprocessed_data\\large_reprocessed_data";
        String outPath = "E:\\HadoopLab3-MovieCluster\\large_data\\large_canopy_data";
        System.setProperty("hadoop.home.dir","E:\\share\\yarn\\hadoop-2.7.1");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "mark");
        job.setJarByClass(MarkCanopy.class);
        job.setMapperClass(CanopyMapper.class);
        job.setReducerClass(CanopyReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //job.setOutputFormatClass(MyOutputFormat.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job,new Path(inPath));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
