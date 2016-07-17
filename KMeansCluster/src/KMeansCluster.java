/**
 * Created by ZhouDavid on 2016/7/13.
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

import static org.apache.commons.math3.util.FastMath.sqrt;
class KCenter{
    HashMap<String,Double>vec;
    String kid;
    double sum = 0;
    double calSum(){
        double ans = 0;
        Iterator it = vec.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry e = (Map.Entry)it.next();
            Double v = (Double)e.getValue();
            ans+=v*v;
        }
        return ans;
    }
    KCenter(String kid,HashMap<String,Double>vec){
        this.kid = kid;
        this.vec = vec;
        this.sum = calSum();
    }
}
public class KMeansCluster {
    public static BufferedWriter writer =null;
    public static class KMeansMapper extends Mapper<Object,Text,Text,Text>{
        public int setup_time = 0;
        public HashMap<String, KCenter> canopy2KCenter = new HashMap<>();

        public static Vector<String> getCanopyIDs(String line){
            int start = line.indexOf("|||");
            start+=3;
            Vector<String>cids = new Vector<>();
            int end = -1;
            while((end=line.indexOf(",",start))!=-1){
                cids.add(line.substring(start,end));
                start=end+1;
            }
            cids.add(line.substring(start));
            return cids;
        }

        public static String getKid(String line){
            int end = line.indexOf("-");
            return line.substring(0,end);
        }

        public static HashMap<String,Double> getVec(String line){
            HashMap<String,Double> vec = new HashMap<>();
            int start = line.indexOf(">"); start++;
            int end = line.indexOf("|");
            String v = line.substring(start,end);
            end = -1;
            start = 0;
            boolean is_end = false;
            while((end=v.indexOf(":",start))!=-1){
                String rid = v.substring(start,end);
                start = end+1;
                end = v.indexOf(",",start);
                Double rate = null;
                if(end == -1){
                    rate = Double.parseDouble(v.substring(start));
                    is_end = true;
                }
                else{
                    rate = Double.parseDouble(v.substring(start,end));
                }
                vec.put(rid,rate);
                if(is_end == true)break;
                start = end+1;
            }
            return vec;
        }

        public static double cosineDistance(HashMap<String,Double>v1,HashMap<String,Double>v2,double sum2){
            Iterator iter = v1.entrySet().iterator();
            double sum =0,sum1 = 0;
            //计算点乘
            while(iter.hasNext()){
                Map.Entry e =(Map.Entry)iter.next();
                String rid1 = (String)e.getKey();
                Double rate1 = (Double)e.getValue();
                if(v2.containsKey(rid1)){
                    sum+=rate1*v2.get(rid1);
                }
                sum1+=rate1*rate1;
            }
            return sum/(sqrt(sum1)*sqrt(sum2));

        }

        @Override
        public void setup(Context context)throws IOException{
            System.err.println("mapper start setup....");
            String kCenterFileName = context.getConfiguration().get("kcenter");
            String kMeansFileName = context.getConfiguration().get("kmeans");
            if(writer==null){
                writer = new BufferedWriter(new FileWriter(kMeansFileName));
            }
            BufferedReader reader = new BufferedReader(new FileReader(kCenterFileName));
            String line = reader.readLine();
            while(line!=null){
                Vector<String> cids = getCanopyIDs(line);
                String kid = getKid(line);
                HashMap<String,Double> vec = getVec(line);
                for(int i  =0;i<cids.size();i++){
                    canopy2KCenter.put(cids.get(i),new KCenter(kid,vec));
                }
                line = reader.readLine();
            }
            System.err.println("mapper end setup....");
        }

        @Override
        public void map(Object key,Text value,Context context)throws IOException,InterruptedException{
            String record = value.toString();
            Vector<String> cids = getCanopyIDs(record);
            HashMap<String,Double> vec = getVec(record);
            double max_dist = -1;
            String kid ="";
            boolean hasCommonCanopy = false;
            for(int i = 0;i<cids.size();i++){
                if(canopy2KCenter.containsKey(cids.get(i))){
                    hasCommonCanopy = true;
                    String rid = cids.get(i);
                    HashMap<String,Double>vec2 = canopy2KCenter.get(rid).vec;
                    double sum2 = canopy2KCenter.get(rid).sum;
                    double dist = cosineDistance(vec,vec2,sum2);
                    if(dist>max_dist){
                        max_dist = dist;
                        kid = canopy2KCenter.get(rid).kid;
                    }
                }
            }
            if(!hasCommonCanopy){
                System.err.println("bug!");
            }
            record+=("@"+kid);
            writer.write(record);
            writer.newLine();

            int start = record.indexOf(">"); start++;
            int end = record.indexOf("|");
            String v = record.substring(start,end);
            context.write(new Text(kid),new Text(v));
        }
    }
    public static void main(String args[])throws Exception{
        System.setProperty("hadoop.home.dir","E:\\share\\yarn\\hadoop-2.7.1");
        String in = "E:\\HadoopLab3-MovieCluster\\large_data\\large_canopy_data\\large_canopy_data";
        String out = "E:\\HadoopLab3-MovieCluster\\large_data\\large_kmeans_data";
        String KCenterFile = "E:\\HadoopLab3-MovieCluster\\large_data\\large_KCenter_data";
        String newKCenterFile = "E:\\HadoopLab3-MovieCluster\\large_data\\large_KCenter_data1";
        String CanopyCenterFile = "E:\\HadoopLab3-MovieCluster\\large_data\\large_canopyCenter_data\\large_canopyCenter_data";
        Configuration conf = new Configuration();
        conf.set("kcenter",KCenterFile);
        conf.set("kmeans",out);
        conf.set("canopycenter",CanopyCenterFile);
        Job job = Job.getInstance(conf,"kmeans");
        job.setJarByClass(KMeansCluster.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job,new Path(in));
        FileOutputFormat.setOutputPath(job,new Path(newKCenterFile));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
