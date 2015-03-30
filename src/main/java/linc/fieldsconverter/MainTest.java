package linc.fieldsconverter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

/**
 * Created by ihainan on 3/21/15.
 */
public class MainTest {
    public static void main(String[] args){
        // 写入文件
        try {
            Writer output = new BufferedWriter(new FileWriter("file.txt", true));
            output.append("Hello World");
            output.close();
        } catch (IOException e) {
            System.err.println("Err: 打开文件失败");
        }
    }
}
