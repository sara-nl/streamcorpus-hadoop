/*
 * Copyright 2015 SURFsara B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package nl.surfsara.streamcorpus;

import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;
import streamcorpus.StreamItem;

import java.io.IOException;

public class StreamCorpusRecordReader extends RecordReader<Text, ThriftWritable<StreamItem>> {

    private FileSplit split;
    private Configuration conf;

    private TProtocol tp;

    private StreamItem item;
    private Text key;
    private ThriftWritable<StreamItem> value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        split = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();
        final Path file = split.getPath();
        FSDataInputStream in = file.getFileSystem(conf).open(file);
        tp = new TBinaryProtocol.Factory().getProtocol(new TIOStreamTransport(in));
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new Text();
        }
        key.set(split.getPath().toString());
        if (item == null) {
            item = new StreamItem();
        }
        if (value == null) {
            value = new ThriftWritable<>();
        }
        try {
            item.read(tp);
            value.set(item);
        } catch (TException e) {
            if (e instanceof TTransportException && ((TTransportException) e).getType() == TTransportException.END_OF_FILE) {
                return false;
            } else {
                e.printStackTrace();
                throw new IOException();
            }
        }
        return true;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public ThriftWritable<StreamItem> getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0f;
    }

    @Override
    public void close() throws IOException {
        if (tp != null && tp.getTransport() != null) {
            tp.getTransport().close();
        }
    }
}
