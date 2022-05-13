package success;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ResultCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * 부모 Reducer 자바 파일에 자성된 reduce 함수를 덮어쓰기 수행
     * reduce 함수는 Suffle and Sort로 처리된 데이터마다 실행됨
     * 처리된 데이터의 수가 500개 라면, reduce 함수는 500번 실행됨
     * <p>
     * Reducer 객체는 기본값이 1개로 1개의 쓰레드로 처리함
     *
     */
    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
        //IP별 빈도수를 계산하기 위한 변수
        int resultCodeCount = 0;
        //Suffle and Sort로 인해 단어별로 데이터들의 값들이 List 구조로 저장됨
        //200 : {1,1,1,1,1,1,1,1,1,1,1,1,1}
        //모든 값은 1이이게 모두 더하기 해도 됨
        for(IntWritable value : values){
             resultCodeCount += value.get();

        }
        //분석 결과 파일에 데이터 저장하기
        context.write(key, new IntWritable(resultCodeCount));
    }
}
