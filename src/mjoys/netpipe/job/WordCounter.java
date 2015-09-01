package mjoys.netpipe.job;

import java.util.HashMap;
import java.util.Map;

import mjoys.netpipe.pipe.InPipe;
import mjoys.netpipe.pipe.Job;
import mjoys.netpipe.pipe.OutPipe;
import mjoys.netpipe.pipe.Task;

@Job(name="wordcounter")
public class WordCounter {
    /*
     * taskA: InPipeA1, InPipeA2
     * taskB: InPipeA1, InPipeA2, ...  OutPipeB1, OutPipeB2 ...
     * taskC: InPipeB1, InPipeB2, ...  OutPipeC1
     * taskD: InPipeD1
     * taskE: InPipeE1
     * taskF: InPipeC1, InPipeD1, InPipeE1, OutPipeF1
     * 
     * 
     * A(A1,A2) ---> B(B1,B2) ---> C(C1) ---> F(F1)
     *                             D(D1) ---> F(F1)
     *                             E(E1) ---> F(F1)
     * */
    private Map<String, Integer> wordCount = new HashMap<String, Integer>();
    
    @Task(name="text", out={"lines"})
    public void textSource(OutPipe<String> linesPipe) {
        linesPipe.write("skdfjs\r\n");
    }
    
    @Task(in={"lines"}, out={"words"})
    public void splitWords(InPipe<String> linesPipe, OutPipe<String> wordsPipe) {
        String line;
        while (true) {
            line = linesPipe.read();
            String words[] = line.split("\\s");
            for (String word : words) {
                wordsPipe.write(word);
            }
        }
    }
    
    @Task(in={"words"})
    public void count(InPipe<String> wordsPipe) {
        while (true) {
            String word = wordsPipe.read();
            if (wordCount.get(word) == null) {
                wordCount.put(word, new Integer(1));
            } else {
                wordCount.put(word, new Integer((Integer)wordCount.get(word) + 1));
            }
        }
    }
}