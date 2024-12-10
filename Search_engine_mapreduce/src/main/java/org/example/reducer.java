package org.example;
import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<Integer,HashSet<Integer>> positionalindex=new HashMap<Integer,HashSet<Integer>>();

        for(Text value: values){
            String[] DocIDAndPostions=value.toString().split(":");
            int DocID=Integer.parseInt(DocIDAndPostions[0]);
            int position=Integer.parseInt(DocIDAndPostions[1]);

            positionalindex.put(DocID,new HashSet<Integer>());
            positionalindex.get(DocID).add(position);


        }
        ArrayList<Integer> sortedDocID=new ArrayList<Integer>(positionalindex.keySet());
        Collections.sort(sortedDocID);

        StringBuilder result =new StringBuilder();
        for (int docID : sortedDocID) {
            ArrayList<Integer> sortedPositions=new ArrayList<Integer>(positionalindex.get(docID));
            Collections.sort(sortedPositions);
            result.append(docID +":" +positionalindex.get(docID).toString()+";");
        }
        context.write(key, new Text(result.toString().trim()));
    }
}
