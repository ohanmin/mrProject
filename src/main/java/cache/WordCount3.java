package cache;


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

import java.net.URI;

@Log4j
public class WordCount3 extends Configuration implements Tool {
    public static void main(String[] args) throws Exception{
        if(args.length != 1){
            System.exit(-1);
        }
        //Cache로 로그파일을 올리고, 맵리듀스 실행 전 시간

        long startTime = System.nanoTime();
        int exitCode = ToolRunner.run(new WordCount3(), args);
        //Cache로 로그파일을 올리고, 맵리듀스 실행 완료 시간
        long endTime = System.nanoTime();
        log.info("Cache time : " + (endTime - startTime) + "ns");
        System.exit(exitCode);
    }
    @Override
    public int run(String[] args) throws Exception {
        //캐시메모리에 올릴 분석 파일
        String analysisFile = "/comdies";
        Configuration conf = this.getConf();
        String appName= conf.get("AppName");
        log.info("appName : " + appName);
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount3.class);
        job.setJobName(appName);
        job.addCacheFile(new Path(analysisFile).toUri());
        //캐시 메로리에 저장된 파일 정보를 가져오기
        URI[] cacheFiles = job.getCacheFiles();
        //파일 이름 가져오기
        for(URI cacheFile : cacheFiles){
            Path uploadFile = new Path(cacheFile.getPath());
        //캐시 메모리에 업로드된 파일명 확인
            log.info("Uploaded CacheFile : "+ uploadFile);
        }
        FileInputFormat.setInputPaths(job, analysisFile);
        //분석 결과가 저장되는 폴더(파일) -- 두번째 파라미터
        FileOutputFormat.setOutputPath(job, new Path(args[0]));
        //맵리듀스의 맵 역할을 수행하는 Mapper 자바 파일 설정
        job.setMapperClass(WordCount3Mapper.class);
        //맵리듀스의 리듀스 역할을 수행하는 Reducer 자바 파일 설정
        job.setReducerClass(WordCount3Reducer.class);

        //분석 결과가 저장될 때 사용될 키의 데이터 타입
        job.setOutputKeyClass(Text.class);
        //분석 결과가 저장될 때 사용될 값의 데이터 타입
        job.setOutputValueClass(IntWritable.class);
        // 맵리듀스 실행
        boolean success = job.waitForCompletion(true);

        return (success ? 0 : 1);
    }

    @Override
    public void setConf(Configuration configuration) {
        configuration.set("AppName", "Cache Test");
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
