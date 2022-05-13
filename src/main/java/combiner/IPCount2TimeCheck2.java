package combiner;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 컴바니이너가 적용 안된 멥리듀스 실행시간 체크
 */
@Log4j
public class IPCount2TimeCheck2 extends Configuration implements Tool {
    public static void main(String[] args) throws Exception {
        if(args.length != 2){
            System.exit(-1);
        }
        //컴바이너가 적용된 맵리두스 실행 전 시간
        long startTime = System.nanoTime();
        int exitCode = ToolRunner.run(new IPCount2TimeCheck2(), args);
        //컴바이너가 적용된 맵리듀스 실행 완료 시간
        long endTime = System.nanoTime();
        log.info("time : " + (endTime - startTime) + "ns");
        System.exit(exitCode);
    }
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        String appName = conf.get("AppName");
        log.info("appName : " + appName);
        //맵리듀스 실행을 위한 잡 객체를 가져오기
        //하둡이 실행되면, 기본적으로 잡 객체를 메모리에 올림
        Job job = Job.getInstance(conf);
        job.setJarByClass(IPCount2TimeCheck2.class);
        job.setJobName("Combiner IP Count2");
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(IPCount2Mapper.class);
        job.setReducerClass(IPCount2Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set("AppName", "Combiner Test");
    }

    @Override
    public Configuration getConf() {
        //맵리듀스 전체에 적용될 변수를 정의할 때 사용
        Configuration conf = new Configuration();
        //변수 정의
        this.setConf(conf);
        return conf;
    }
}
