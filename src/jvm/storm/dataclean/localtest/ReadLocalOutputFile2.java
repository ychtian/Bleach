package storm.dataclean.localtest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by tian on 18/03/2016.
 */
public class ReadLocalOutputFile2 {

    public BufferedReader br;

    public int error_count;
    public int count;

    public static void main(String[] args) throws IOException {

//        FileReader fr =

        ReadLocalOutputFile2 aa = new ReadLocalOutputFile2();
        aa.run();
    }

    public void run() throws IOException {
        br = new BufferedReader(new FileReader("outputdata/kid_dirty_data_output"));
        error_count = 0;
        count = 0;
//        String line = getLine();
        String line = br.readLine();
        while(line!=null && !line.equals("\n")){
            analyze_Line(line);
            line = br.readLine();
        }


        System.out.println("finish: error count = " + error_count);
    }

    public void analyze_Line(String l) throws IOException {
//        String[] values = br.readLine().split("\\[")[1].split(",");
        if(l.split("\\[").length < 1){
            System.err.println("count " + count + ", line: " + l);
        }
        String[] aa = l.split("\\[");

//        System.out.println(aa.length);


        String[] values = aa[1].split(",");
        String tid = values[0];

        if(tid.equals("10822")){
            System.err.println("debug");
        }


        String ckey = values[1];
        String nkey = values[7];
        String nation = values[6];
        count++;
        if(nkey.startsWith(" _")){
//            System.out.println(tid + ", " + ckey + ", " + nkey + ", " + nation);
            System.out.println(tid + ", " + nkey + ", count="+error_count);
            error_count ++;
        }
//        if(error_count > 500){
//            System.exit(0);
//        }

//        return br.readLine().split("\\[")[1];
    }

}
