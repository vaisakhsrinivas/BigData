package filter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;

public class FilterMapper extends Mapper<Object, Text, Text, NullWritable> {

    Funnel<Player> p = new Funnel<Player>() {

        public void funnel(Player person, Sink into) {
            into.putString(person.playername, Charsets.UTF_8);
        }
    };

    private BloomFilter<Player> player = BloomFilter.create(p, 500, 0.01);

    @Override
    public void setup(Context context) throws IOException, InterruptedException {

        
        String pname;
        
       
        
        try {
            Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if (files != null && files.length > 0) {

                for (Path file : files) {

                    try {
                        File myFile = new File(file.toUri());
                        BufferedReader bufferedReader = new BufferedReader(new FileReader(myFile.toString()));
                        String line = null;
                        while ((line = bufferedReader.readLine()) != null) {
                            
                            if (line.equals("Roger Federer"))
                            {
                            
                            Player p = new Player(line);
                            player.put(p);
                        }
                        }
                    } catch (IOException ex) {
                        System.err.println("Exception while reading  file: " + ex.getMessage());
                    }
                }
            }
        } catch (IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }

    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String values[] = value.toString().split(",");
        Player p = new Player(values[10]);
        Player q = new Player(values[20]);
        
        if (player.mightContain(p) || player.mightContain(q)) {
            context.write(value, NullWritable.get());
        }

    }

}
