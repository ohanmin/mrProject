package cc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Locale;

/**
 * 맵 역할을 수행하기 위해서는 Mapper 자바 파일을 상속받아야함
 * Mapper 파일의 앞의 2개 데이터 타입(LongWritable, Text)은 분석할 파일의 키와 값의 데이터 타입
 * Mapper 파일의 두의 2개 데이터 탕비(Text, IntWritable)은 리듀스에 보낼 키와 값의 데이터 타입
 */
public class CharCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        String line = value.toString();
        for(String word : line.split("\\W+")){
            if(word.length() > 3){
                word = word.toLowerCase();
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
}
