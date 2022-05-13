package wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 부모 Reducer 자바 파일에 작성된 reduce 함수를 덮어쓰기 수행
     * reduce 함수는 Suffle and Sort로 처리된 데이터마다 실행됨
     * 처리된 데이터의 수가 500개라면, reduce함수는 500번 실햄됨
     * Reducer 객체는 기본값이 1개로 1개의 쓰레디로 처리함
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        //단어별 빈도수를 계산하기 위한 변수
        int wordCount = 0;
        // Suffle and Sort로 인해 단어별로 데이터들의 값들이 List 구조로 저장됨
        //모든 값은 1이이게 모두 더하기 해도 됨
        for(IntWritable value : values){
            //값을 모두 더하기
            wordCount += value.get();
        }
        //분석 결과 파일에 데이터 저장하기
        context.write(key ,new IntWritable(wordCount));
    }
}
