package success;

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

@Log4j
public class ResultCount2 extends Configuration implements Tool {
    // 멥리듀스 실행 함수
    public static void main(String[] args) throws Exception{
        //파라미터는 분석할 파일(폴더)과 분석결과가 저장될 파일(폴더), 전송 결과 코드 3개를 받음
        if(args.length != 3){
            System.exit(-1);
        }
    int exitCode = ToolRunner.run(new ResultCount2(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        conf.set("resultCode", args[2]);
        String appName = conf.get("AppName");
        Job job = Job.getInstance(conf);
        job.setJarByClass(ResultCount2.class);
        job.setJobName(appName);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(ResultCountMapper.class);
        job.setReducerClass(ResultCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean success = job.waitForCompletion(true);
        return (success ? 0 : 1);
    }

    @Override
    public void setConf(Configuration configuration) {
        //App 이름 정의
        configuration.set("AppName", "Send Result2");
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
