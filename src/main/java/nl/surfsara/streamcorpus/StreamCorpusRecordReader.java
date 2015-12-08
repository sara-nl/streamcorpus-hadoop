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
import org.apache.hadoop.fs.FileSystem;
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
import streamcorpus.StreamItem;

import java.io.IOException;

public class StreamCorpusRecordReader extends RecordReader<Text, ThriftWritable<StreamItem>> {

    private FileSplit fileSplit;
    private Configuration conf;

    private TProtocol tProtocol;

    private Text key;
    private ThriftWritable<StreamItem> value;
    private StreamItem streamItem;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit = (FileSplit) inputSplit;
        conf = taskAttemptContext.getConfiguration();

        key = new Text();
        value = new ThriftWritable<>();
        streamItem = new StreamItem();

        final Path file = fileSplit.getPath();
        FileSystem fs = file.getFileSystem(conf);
        TIOStreamTransport in = new TIOStreamTransport(fs.open(file));
        tProtocol = new TBinaryProtocol.Factory().getProtocol(in);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        key.set(fileSplit.getPath().toString());

        try {
            streamItem.read(tProtocol);
            value.set(streamItem);
        } catch (TException e) {
            throw new IOException();
        }

        if (streamItem == null) {
            return false;
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
        tProtocol.getTransport().close();
    }
}
