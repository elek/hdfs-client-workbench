package net.anzix.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.htrace.HTraceConfiguration;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;
import org.apache.htrace.impl.AlwaysSampler;
import org.apache.htrace.impl.ZipkinSpanReceiver;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HdfsCli {
    public static void main(String[] args) throws IOException {
        new HdfsCli().run();
    }

    private void run() throws IOException {
        Configuration conf = new Configuration();
        Map<String, String> configMap = new HashMap<>();
        configMap.put("spanreceiver.classes", "org.apache.htrace.impl.ZipkinSpanReceiver");
        HTraceConfiguration htraceConf = HTraceConfiguration.fromMap(configMap);

        Trace.addReceiver(new ZipkinSpanReceiver(htraceConf));

        TraceScope something = Trace.startSpan("hdfsclient", new AlwaysSampler(htraceConf), null);
        System.out.println(Trace.isTracing());
        conf.set("fs.default.name", "hdfs://localhost:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/tmp/test.txt" + new Date().getTime()));
        for (int i = 0; i < 10000; i++) {
            fsDataOutputStream.write(("/tmp/test2" + new Date().getTime()).getBytes());
        }
        fsDataOutputStream.close();
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), false);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            System.out.println(locatedFileStatusRemoteIterator.next());
        }
        fileSystem.close();
        something.close();

    }
}
