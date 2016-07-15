/**
 * Created by ZhouDavid on 2016/7/12.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.hash.Hash;


import java.io.*;
import java.util.*;

public class GetCanopyCenter {
    public static class CanopyMapper extends Mapper<Object,Text,Text,Text>{
        Vector<HashSet<String>> canopyCenters = new Vector<HashSet<String>>();
        public HashMap<String,Integer>counter = new HashMap<>();
        public Integer canopy_num = 0;
        int uthreshold = 8;
        int lthreshold = 2;
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
            String line = value.toString();
            int start = line.indexOf('-');
            start+=2;
            HashSet<String> rids = genRaterIDs(line,start);
            if(canopyCenters.size()==0){
                canopyCenters.add(rids);
                context.write(new Text(canopy_num.toString()),new Text(line.substring(start)));
                canopy_num++;
            }
            else{
                boolean stronglyMark = false;
                for(Integer i = 0;i<canopyCenters.size();i++){
                    int share = calSimilarity(rids,canopyCenters.get(i));
                    if(share>=uthreshold){
                        context.write(new Text(i.toString()),new Text(line.substring(start)));
                        stronglyMark = true;
                        break;
                    }
                    else if(share>=lthreshold){
                        stronglyMark = true;
                    }
                }
                if(stronglyMark == false){
                    canopyCenters.add(rids);
                    context.write(new Text(canopy_num.toString()),new Text(line.substring(start)));
                    canopy_num++;
                }
            }
        }
    }

    public static class CanopyReducer extends Reducer<Text,Text,Text,Text>{
        public void reduce(Text key,Iterable<Text>values){

        }
    }
    public static void main(String args[])throws Exception{
        System.err.println("reading inverted_list....");
        System.err.println("start mapreduce....");
        String inPath = "E:\\HadoopLab3-MovieCluster\\small_data\\small_reprocessed_data";
        String outPath = "E:\\HadoopLab3-MovieCluster\\small_data\\small_canopyCenter_data";
        System.setProperty("hadoop.home.dir","E:\\share\\yarn\\hadoop-2.7.1");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "get");
        job.setJarByClass(GetCanopyCenter.class);
        job.setMapperClass(CanopyMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job,new Path(inPath));
        FileOutputFormat.setOutputPath(job,new Path(outPath));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
